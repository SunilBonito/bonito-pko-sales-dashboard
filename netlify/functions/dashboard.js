// Netlify Function — PKO Sales Dashboard backend
//
// Hits TWO data sources:
//   1. ClickHouse (bonito_analytics) for sales line items + product catalog
//      - Mirrored from Postgres by PeerDB, lag ~1 minute
//      - Fast aggregations across 12M line items
//   2. Postgres read-replica for project_details (project_value, project_type)
//      - OLTP, straight lookup
//      - Small table (~19K rows), instant
//
// Pattern cloned from the DM Review dashboard: query whitelist, 60s cache,
// Basic auth to ClickHouse HTTPS port, pg for Postgres.
//
// Env vars needed in Netlify dashboard:
//   CH_HOST, CH_USER, CH_PASS   (ClickHouse cloud)
//   PG_HOST, PG_USER, PG_PASS, PG_DB (Postgres read replica)

const https = require('https');
const { Pool } = require('pg');

/* ----------------------------- Postgres pool ---------------------------- */
// Lazy-init so cold starts don't open a connection unless we need one.
let pgPool = null;
function getPgPool(){
  if(pgPool) return pgPool;
  pgPool = new Pool({
    host: process.env.PG_HOST,
    user: process.env.PG_USER,
    password: process.env.PG_PASS,
    database: process.env.PG_DB,
    port: 5432,
    ssl: { rejectUnauthorized: false },  // RDS uses self-signed on default endpoint
    max: 3,                 // Netlify functions are short-lived, keep low
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 8000,
  });
  return pgPool;
}

/* --------------------------- ClickHouse call ---------------------------- */
// Mirrors the DM Review dashboard's pattern: HTTPS POST with SQL as body,
// Basic auth header, TabSeparatedWithNames as output format.
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
        // JSONEachRow returns one JSON object per line
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
// Simple in-memory cache per Netlify function instance. Instances recycle
// every ~5 min so cache naturally expires. We keep a hard TTL too.
const CACHE_TTL_MS = 60 * 1000;  // 60 seconds
const cache = new Map();  // key = view name, value = {ts, data}

function getCached(key){
  const entry = cache.get(key);
  if(!entry) return null;
  if(Date.now() - entry.ts > CACHE_TTL_MS){
    cache.delete(key);
    return null;
  }
  return entry.data;
}
function setCached(key, data){
  cache.set(key, { ts: Date.now(), data });
}

/* ============================================================
   QUERIES — whitelisted so only known views can be fetched
   ============================================================ */

/* --- Query 1: Sales line items (the big one, 12M rows aggregated) ---
   Flattens line_items + quotes + projects + variants + products + brands + categories
   into one row per line item with the fields the dashboard needs. Scoped to FY25
   onwards to keep response size sane (~200K rows). */
const Q_SALES = `
WITH latest_quotes AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(quotable_id, _peerdb_version) AS project_id,
         argMax(category, _peerdb_version) AS quote_category
  FROM bonito_analytics.pulse_quotes
  WHERE quotable_type = 'Project' AND _peerdb_is_deleted = 0
  GROUP BY id
),
latest_projects AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(project_no, _peerdb_version) AS project_no,
         argMax(created_at, _peerdb_version) AS project_created_at
  FROM bonito_analytics.pulse_projects
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
latest_variants AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(erp_code, _peerdb_version) AS v_erp_code,
         argMax(name, _peerdb_version) AS v_name,
         argMax(product_id, _peerdb_version) AS product_id
  FROM bonito_analytics.pulse_commerce_product_variants
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
latest_products AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(name, _peerdb_version) AS p_name,
         argMax(erp_code, _peerdb_version) AS p_erp_code,
         argMax(product_brand_id, _peerdb_version) AS product_brand_id,
         argMax(product_category_id, _peerdb_version) AS product_category_id,
         argMax(backend_product_type, _peerdb_version) AS backend_product_type,
         argMax(created_at, _peerdb_version) AS product_created_at,
         argMax(status, _peerdb_version) AS product_status
  FROM bonito_analytics.pulse_commerce_products
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
latest_brands AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(name, _peerdb_version) AS brand_name
  FROM bonito_analytics.pulse_commerce_product_brands
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
latest_cats AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(name, _peerdb_version) AS category_name
  FROM bonito_analytics.pulse_commerce_product_categories
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
line_items_dedup AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(quote_id, _peerdb_version) AS quote_id,
         argMax(product_variant_id, _peerdb_version) AS product_variant_id,
         argMax(unit_price, _peerdb_version) AS unit_price,
         argMax(total_price_with_discount, _peerdb_version) AS amount,
         argMax(created_at, _peerdb_version) AS line_created_at
  FROM bonito_analytics.pulse_commerce_project_line_items
  WHERE _peerdb_is_deleted = 0
    AND created_at >= '2025-04-01'
  GROUP BY id
)
SELECT
  lp.project_no AS project_no,
  li.line_created_at AS pko_date,
  COALESCE(v.v_erp_code, p.p_erp_code, '') AS erp_code,
  COALESCE(p.p_name, v.v_name, '') AS product_name,
  v.v_name AS variant_name,
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
  li.unit_price AS unit_price,
  p.product_created_at AS product_created_date,
  p.product_status AS status
FROM line_items_dedup li
INNER JOIN latest_quotes q ON q.id = li.quote_id
INNER JOIN latest_projects lp ON lp.id = q.project_id
LEFT JOIN latest_variants v ON v.id = li.product_variant_id
LEFT JOIN latest_products p ON p.id = v.product_id
LEFT JOIN latest_brands b ON b.id = p.product_brand_id
LEFT JOIN latest_cats c ON c.id = p.product_category_id
WHERE lp.project_no IS NOT NULL AND lp.project_no > 0
  AND li.amount > 0
LIMIT 500000
`;

/* --- Query 2: Studio product catalog ---
   The "Studio Dump" equivalent. All sellable product variants with brand,
   category, latest BNG and MUM prices, created_at (for age calc). */
const Q_STUDIO = `
WITH latest_variants AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(erp_code, _peerdb_version) AS erp_code,
         argMax(name, _peerdb_version) AS variant_name,
         argMax(product_id, _peerdb_version) AS product_id,
         argMax(status, _peerdb_version) AS v_status
  FROM bonito_analytics.pulse_commerce_product_variants
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
latest_products AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(name, _peerdb_version) AS product_name,
         argMax(erp_code, _peerdb_version) AS p_erp_code,
         argMax(product_brand_id, _peerdb_version) AS product_brand_id,
         argMax(product_category_id, _peerdb_version) AS product_category_id,
         argMax(backend_product_type, _peerdb_version) AS backend_product_type,
         argMax(bom_type, _peerdb_version) AS bom_type,
         argMax(created_at, _peerdb_version) AS created_at,
         argMax(status, _peerdb_version) AS status
  FROM bonito_analytics.pulse_commerce_products
  WHERE _peerdb_is_deleted = 0
  GROUP BY id
),
latest_brands AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(name, _peerdb_version) AS brand_name
  FROM bonito_analytics.pulse_commerce_product_brands
  WHERE _peerdb_is_deleted = 0 GROUP BY id
),
latest_cats AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(name, _peerdb_version) AS category_name
  FROM bonito_analytics.pulse_commerce_product_categories
  WHERE _peerdb_is_deleted = 0 GROUP BY id
),
latest_prices AS (
  SELECT argMax(id, _peerdb_version) AS id,
         argMax(code, _peerdb_version) AS pl_code,
         argMax(sell_price, _peerdb_version) AS sell_price,
         argMax(commerce_product_variant_id, _peerdb_version) AS variant_id
  FROM bonito_analytics.pulse_commerce_product_variant_prices
  WHERE _peerdb_is_deleted = 0 GROUP BY id
),
-- Latest BNG price per variant
bng_prices AS (
  SELECT variant_id,
         argMax(sell_price, id) AS bng_sp
  FROM latest_prices
  WHERE (pl_code LIKE 'PL-BNG-%' OR pl_code LIKE 'BLR%' OR pl_code LIKE 'Price_BLR%')
    AND sell_price > 0
  GROUP BY variant_id
),
-- Latest MUM price per variant
mum_prices AS (
  SELECT variant_id,
         argMax(sell_price, id) AS mum_sp
  FROM latest_prices
  WHERE (pl_code LIKE 'PL-MUM-%' OR pl_code LIKE 'MUM%')
    AND sell_price > 0
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
FROM latest_variants v
INNER JOIN latest_products p ON p.id = v.product_id
LEFT JOIN latest_brands b ON b.id = p.product_brand_id
LEFT JOIN latest_cats c ON c.id = p.product_category_id
LEFT JOIN bng_prices bng ON bng.variant_id = v.id
LEFT JOIN mum_prices mum ON mum.variant_id = v.id
WHERE v.erp_code IS NOT NULL AND v.erp_code != ''
  AND p.status = 1  -- active products only
`;

/* --- Query 3: Project metadata from Postgres ---
   project_no -> project_value, project_type, branch. One row per PID. */
const Q_PROJECTS_PG = `
  SELECT project_no,
         project_type,
         project_value,
         branch,
         status,
         current_milestone,
         p_date,
         campaign
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
      // Postgres pull — the Project Value / Project Type lookup
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
