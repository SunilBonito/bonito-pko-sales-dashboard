// Netlify Function — PKO Sales Dashboard backend
// ClickHouse (sales + studio) + Postgres (project_details)

const https = require('https');
const zlib = require('zlib');
const { Pool, types } = require('pg');

// Make Postgres return decimals as numbers, not strings. 1700 = DECIMAL/NUMERIC.
types.setTypeParser(1700, v => v === null ? null : parseFloat(v));
types.setTypeParser(701,  v => v === null ? null : parseFloat(v));
types.setTypeParser(23,   v => v === null ? null : parseInt(v, 10));

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
      path: '/?enable_http_compression=1',
      method: 'POST',
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'text/plain',
        'Accept-Encoding': 'gzip',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 55000,
    }, (res) => {
      const chunks = [];
      const stream = res.headers['content-encoding'] === 'gzip'
        ? res.pipe(zlib.createGunzip())
        : res;
      stream.on('data', chunk => { chunks.push(chunk); });
      stream.on('end', () => {
        const data = Buffer.concat(chunks).toString('utf8');
        chunks.length = 0; // free early
        if(res.statusCode >= 400){
          return reject(new Error(`ClickHouse ${res.statusCode}: ${data.slice(0, 500)}`));
        }
        // Parse line by line, pushing into array — don't hold the big string + array both
        const lines = data.split('\n');
        const rows = [];
        for(let i = 0; i < lines.length; i++){
          const l = lines[i];
          if(l) rows.push(JSON.parse(l));
          lines[i] = null;  // let GC reclaim parsed lines
        }
        resolve(rows);
      });
      stream.on('error', reject);
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
   QUERIES
   ============================================================ */

/* --- Query 1: SLIM sales line items aggregated by (project_no, erp_code).
   Only columns the dashboard actually uses in sales rows:
     project_no, erp_code, pko_date (min line date), amount, qty.
   Everything else (product name, brand, category, backend type) is
   joined on the FRONTEND by looking up erp_code in the studio catalog.
   This cuts response size by ~70%. */
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
  SELECT id, erp_code
  FROM bonito_analytics.pulse_commerce_product_variants
  WHERE _peerdb_is_deleted = 0 AND erp_code IS NOT NULL AND erp_code != ''
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
    AND created_at < '2026-11-01'
    AND total_price_with_discount > 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
)
SELECT
  lp.project_no AS p,
  v.erp_code AS e,
  toDate(min(li.line_created_at)) AS d,
  round(sum(li.amount), 2) AS a,
  round(sum(CASE WHEN li.unit_price > 0 THEN li.amount / li.unit_price ELSE 0 END), 3) AS q
FROM line_items_dedup li
INNER JOIN quotes_dedup qd ON qd.id = li.quote_id
INNER JOIN projects_dedup lp ON lp.id = qd.quotable_id
INNER JOIN variants_dedup v ON v.id = li.product_variant_id
GROUP BY lp.project_no, v.erp_code
`;

/* --- Query 2: Studio product catalog (full detail, lives per-ERP once) ---
   Carries product_name, brand, category, backend_product_type, prices.
   Sales rows reference ERPs that get fat-join'd on the browser. */
const Q_STUDIO = `
WITH
variants_dedup AS (
  SELECT id, erp_code, name AS variant_name, product_id
  FROM bonito_analytics.pulse_commerce_product_variants
  WHERE _peerdb_is_deleted = 0 AND erp_code IS NOT NULL AND erp_code != ''
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
products_dedup AS (
  SELECT id, name AS product_name,
         product_brand_id, product_category_id,
         backend_product_type, bom_type, created_at, status
  FROM bonito_analytics.pulse_commerce_products
  WHERE _peerdb_is_deleted = 0 AND status = 1
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
bng_prices AS (
  SELECT variant_id, argMax(sell_price, id) AS bng_sp
  FROM prices_dedup
  WHERE pl_code LIKE 'PL-BNG-%' OR pl_code LIKE 'BLR%' OR pl_code LIKE 'Price_BLR%'
  GROUP BY variant_id
),
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
  toString(p.created_at) AS created_date,
  p.status AS status,
  COALESCE(bng.bng_sp, 0) AS bng_sp,
  COALESCE(mum.mum_sp, 0) AS mum_sp
FROM variants_dedup v
INNER JOIN products_dedup p ON p.id = v.product_id
LEFT JOIN brands_dedup b ON b.id = p.product_brand_id
LEFT JOIN cats_dedup c ON c.id = p.product_category_id
LEFT JOIN bng_prices bng ON bng.variant_id = v.id
LEFT JOIN mum_prices mum ON mum.variant_id = v.id
`;

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

  const acceptEncoding = (event.headers && (event.headers['accept-encoding'] || event.headers['Accept-Encoding'])) || '';
  const wantsGzip = /\bgzip\b/.test(acceptEncoding);

  try {
    let data = getCached(view);
    let cacheHit = !!data;

    if(!data){
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
    }

    const bodyStr = JSON.stringify(data);

    if(wantsGzip && bodyStr.length > 10000){
      const compressed = zlib.gzipSync(bodyStr);
      return {
        statusCode: 200,
        headers: {
          ...headers,
          'Content-Encoding': 'gzip',
          'X-Cache': cacheHit ? 'HIT' : 'MISS',
          'X-Row-Count': String(data.length),
          'X-Bytes': String(bodyStr.length),
        },
        body: compressed.toString('base64'),
        isBase64Encoded: true,
      };
    }

    return {
      statusCode: 200,
      headers: {
        ...headers,
        'X-Cache': cacheHit ? 'HIT' : 'MISS',
        'X-Row-Count': String(data.length),
      },
      body: bodyStr,
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
