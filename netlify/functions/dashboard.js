// Netlify Function — PKO Sales Dashboard backend
//
// Hits TWO data sources:
//   1. ClickHouse (bonito_analytics) for sales line items + product catalog
//   2. Postgres read-replica for project_details (project_value, project_type)
//
// Env vars needed:
//   CH_HOST, CH_USER, CH_PASS         (ClickHouse cloud)
//   PG_HOST, PG_USER, PG_PASS, PG_DB  (Postgres read replica)

const https = require('https');
const { Pool } = require('pg');

/* ----------------------------- Postgres pool ---------------------------- */
let pgPool = null;
function getPgPool(){
  if(pgPool) return pgPool;
  pgPool = new Pool({
    host: process.env.PG_HOST,
    user: process.env.PG_USER,
    password: process.env.PG_PASS,
    database: process.env.PG_DB,
    port: 5432,
    ssl: { rejectUnauthorized: false },
    max: 3,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 8000,
  });
  return pgPool;
}

/* --------------------------- ClickHouse call ---------------------------- */
function clickhouseQuery(sql){
  return new Promise((resolve, reject) => {
    const auth = Buffer.from(`${process.env.CH_USER}:${process.env.CH_PASS}`).toString('base64');
    const body = sql + ' FORMAT JSONEachRow';
    const req = https.request({
      hostname: process.env.CH_HOST,
      port: 8443,
      path: '/',
      method: 'POST',
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'text/plain',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 55000,
    }, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if(res.statusCode >= 400){
          return reject(new Error(`ClickHouse ${res.statusCode}: ${data.slice(0, 500)}`));
        }
        const rows = data.split('\n').filter(l => l.trim()).map(l => JSON.parse(l));
        resolve(rows);
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(new Error('ClickHouse request timeout')); });
    req.write(body);
    req.end();
  });
}

/* ----------------------------- Query cache ------------------------------ */
const CACHE_TTL_MS = 60 * 1000;
const cache = new Map();
function getCached(key){
  const entry = cache.get(key);
  if(!entry) return null;
  if(Date.now() - entry.ts > CACHE_TTL_MS){ cache.delete(key); return null; }
  return entry.data;
}
function setCached(key, data){ cache.set(key, { ts: Date.now(), data }); }

/* ============================================================
   QUERIES — use LIMIT 1 BY id ORDER BY _peerdb_version DESC
   for dedup (matches DM Review dashboard pattern)
   ============================================================ */

/* --- Query 1: Sales line items (big one, aggregated) ---
   Join path: line_items -> quotes -> projects
              line_items -> variants -> products -> brands + categories
   Scoped to FY25 onwards to keep response size sane. */
const Q_SALES = `
WITH
quotes_dedup AS (
  SELECT id, quotable_id
  FROM bonito_analytics.pulse_quotes
  WHERE quotable_type = 'Project' AND _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
projects_dedup AS (
  SELECT id, project_no
  FROM bonito_analytics.pulse_projects
  WHERE _peerdb_is_deleted = 0 AND project_no IS NOT NULL AND project_no > 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
variants_dedup AS (
  SELECT id, erp_code, name AS variant_name, product_id
  FROM bonito_analytics.pulse_commerce_product_variants
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
products_dedup AS (
  SELECT id, name AS product_name, erp_code AS p_erp_code,
         product_brand_id, product_category_id, backend_product_type
  FROM bonito_analytics.pulse_commerce_products
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
brands_dedup AS (
  SELECT id, name AS brand_name
  FROM bonito_analytics.pulse_commerce_product_brands
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
cats_dedup AS (
  SELECT id, name AS category_name
  FROM bonito_analytics.pulse_commerce_product_categories
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
line_items_dedup AS (
  SELECT id, quote_id, product_variant_id, unit_price,
         total_price_with_discount AS amount,
         created_at AS line_created_at
  FROM bonito_analytics.pulse_commerce_project_line_items
  WHERE _peerdb_is_deleted = 0
    AND created_at >= '2025-04-01'
    AND total_price_with_discount > 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
)
SELECT
  lp.project_no AS project_no,
  li.line_created_at AS pko_date,
  COALESCE(v.erp_code, p.p_erp_code, '') AS erp_code,
  COALESCE(p.product_name, v.variant_name, '') AS product_name,
  v.variant_name AS variant_name,
  COALESCE(b.brand_name, '') AS brand,
  COALESCE(c.category_name, '') AS category,
  p.backend_product_type AS backend_product_type,
  CASE p.backend_product_type
    WHEN 46 THEN 'type_modular'
    WHEN 47 THEN 'type_modular'
    WHEN 48 THEN 'type_neo_modular'
    WHEN 49 THEN 'type_bonito_studio'
    WHEN 50 THEN 'type_service'
    ELSE 'type_other'
  END AS default_category,
  CASE WHEN li.unit_price > 0
       THEN round(li.amount / li.unit_price, 3)
       ELSE 0
  END AS qty,
  li.amount AS amount,
  li.unit_price AS unit_price
FROM line_items_dedup li
INNER JOIN quotes_dedup q ON q.id = li.quote_id
INNER JOIN projects_dedup lp ON lp.id = q.quotable_id
LEFT JOIN variants_dedup v ON v.id = li.product_variant_id
LEFT JOIN products_dedup p ON p.id = v.product_id
LEFT JOIN brands_dedup b ON b.id = p.product_brand_id
LEFT JOIN cats_dedup c ON c.id = p.product_category_id
LIMIT 500000
`;

/* --- Query 2: Studio product catalog --- */
const Q_STUDIO = `
WITH
variants_dedup AS (
  SELECT id, erp_code, name AS variant_name, product_id, status AS v_status
  FROM bonito_analytics.pulse_commerce_product_variants
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
products_dedup AS (
  SELECT id, name AS product_name, erp_code AS p_erp_code,
         product_brand_id, product_category_id,
         backend_product_type, bom_type, created_at, status
  FROM bonito_analytics.pulse_commerce_products
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
brands_dedup AS (
  SELECT id, name AS brand_name
  FROM bonito_analytics.pulse_commerce_product_brands
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
cats_dedup AS (
  SELECT id, name AS category_name
  FROM bonito_analytics.pulse_commerce_product_categories
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
prices_dedup AS (
  SELECT id, code AS pl_code, sell_price,
         commerce_product_variant_id AS variant_id
  FROM bonito_analytics.pulse_commerce_product_variant_prices
  WHERE _peerdb_is_deleted = 0 AND sell_price > 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
-- Latest BNG price per variant (take highest price id = most recent)
bng_prices AS (
  SELECT variant_id, argMax(sell_price, id) AS bng_sp
  FROM prices_dedup
  WHERE pl_code LIKE 'PL-BNG-%' OR pl_code LIKE 'BLR%' OR pl_code LIKE 'Price_BLR%'
  GROUP BY variant_id
),
-- Latest MUM price per variant
mum_prices AS (
  SELECT variant_id, argMax(sell_price, id) AS mum_sp
  FROM prices_dedup
  WHERE pl_code LIKE 'PL-MUM-%' OR pl_code LIKE 'MUM%'
  GROUP BY variant_id
)
SELECT
  v.erp_code AS erp_code,
  p.product_name AS product_name,
  v.variant_name AS variant_name,
  COALESCE(b.brand_name, '') AS brand,
  COALESCE(c.category_name, '') AS category,
  CASE p.backend_product_type
    WHEN 46 THEN 'type_modular'
    WHEN 47 THEN 'type_modular'
    WHEN 48 THEN 'type_neo_modular'
    WHEN 49 THEN 'type_bonito_studio'
    WHEN 50 THEN 'type_service'
    ELSE 'type_other'
  END AS default_category,
  p.backend_product_type AS backend_product_type,
  p.bom_type AS bom_type,
  p.created_at AS created_date,
  p.status AS status,
  COALESCE(bng.bng_sp, 0) AS bng_sp,
  COALESCE(mum.mum_sp, 0) AS mum_sp
FROM variants_dedup v
INNER JOIN products_dedup p ON p.id = v.product_id
LEFT JOIN brands_dedup b ON b.id = p.product_brand_id
LEFT JOIN cats_dedup c ON c.id = p.product_category_id
LEFT JOIN bng_prices bng ON bng.variant_id = v.id
LEFT JOIN mum_prices mum ON mum.variant_id = v.id
WHERE v.erp_code IS NOT NULL AND v.erp_code != ''
  AND p.status = 1
`;

/* --- Query 3: Postgres project metadata --- */
const Q_PROJECTS_PG = `
  SELECT project_no, project_type, project_value,
         branch, status, current_milestone, p_date, campaign
  FROM project_details
  WHERE project_no IS NOT NULL
`;

/* ---------------------------- Handler ----------------------------------- */
exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
  };

  if(event.httpMethod === 'OPTIONS'){
    return { statusCode: 200, headers, body: '' };
  }

  const view = (event.queryStringParameters || {}).view;
  if(!view){
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'missing ?view= param' }) };
  }

  try {
    const cached = getCached(view);
    if(cached){
      return {
        statusCode: 200,
        headers: { ...headers, 'X-Cache': 'HIT' },
        body: JSON.stringify(cached),
      };
    }

    let data;
    if(view === 'sales'){
      data = await clickhouseQuery(Q_SALES);
    } else if(view === 'studio'){
      data = await clickhouseQuery(Q_STUDIO);
    } else if(view === 'projects'){
      const client = await getPgPool().connect();
      try {
        const result = await client.query(Q_PROJECTS_PG);
        data = result.rows;
      } finally {
        client.release();
      }
    } else {
      return { statusCode: 400, headers, body: JSON.stringify({ error: `unknown view: ${view}` }) };
    }

    setCached(view, data);
    return {
      statusCode: 200,
      headers: { ...headers, 'X-Cache': 'MISS', 'X-Row-Count': String(data.length) },
      body: JSON.stringify(data),
    };
  } catch (err) {
    console.error(`[${view}] query failed:`, err);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: err.message, view }),
    };
  }
};
