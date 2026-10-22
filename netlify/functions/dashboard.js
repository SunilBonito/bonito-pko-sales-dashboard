// Netlify Function — PKO Sales Dashboard backend
// ClickHouse (sales + studio) + Postgres (project_details)

const https = require('https');
const zlib = require('zlib');
const { Pool, types } = require('pg');

types.setTypeParser(1700, v => v === null ? null : parseFloat(v));
types.setTypeParser(701,  v => v === null ? null : parseFloat(v));
types.setTypeParser(23,   v => v === null ? null : parseInt(v, 10));

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
        chunks.length = 0;
        if(res.statusCode >= 400){
          return reject(new Error(`ClickHouse ${res.statusCode}: ${data.slice(0, 500)}`));
        }
        const lines = data.split('\n');
        const rows = [];
        for(let i = 0; i < lines.length; i++){
          const l = lines[i];
          if(l) rows.push(JSON.parse(l));
          lines[i] = null;
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

const CACHE_TTL_MS = 60 * 1000;
const cache = new Map();
function getCached(key){
  const entry = cache.get(key);
  if(!entry) return null;
  if(Date.now() - entry.ts > CACHE_TTL_MS){ cache.delete(key); return null; }
  return entry.data;
}
function setCached(key, data){ cache.set(key, { ts: Date.now(), data }); }

const PAGE_SIZE = 40000;

/* --- Query 1: Sales line items — PAGINATED and FIXED ---
   Key fixes vs previous version:
    a) Added `current = true` filter on quotes (each project had 2-20+
       versions of its quote, all with line items — this was the 15-20x
       revenue inflation).
    b) Normalize ERP code to upperUTF8(trim(...)) so Map lookups on the
       frontend match the studio catalog's ERP spelling. */
function buildSalesQuery(page){
  const offset = page * PAGE_SIZE;
  return `
WITH
quotes_dedup AS (
  SELECT id, quotable_id, current
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
  upperUTF8(trim(v.erp_code)) AS e,
  toDate(min(li.line_created_at)) AS d,
  round(sum(li.amount), 2) AS a,
  round(sum(CASE WHEN li.unit_price > 0 THEN li.amount / li.unit_price ELSE 0 END), 3) AS q
FROM line_items_dedup li
INNER JOIN quotes_dedup qd ON qd.id = li.quote_id AND qd.current = true
INNER JOIN projects_dedup lp ON lp.id = qd.quotable_id
INNER JOIN variants_dedup v ON v.id = li.product_variant_id
GROUP BY lp.project_no, upperUTF8(trim(v.erp_code))
ORDER BY lp.project_no, upperUTF8(trim(v.erp_code))
LIMIT ${PAGE_SIZE} OFFSET ${offset}
`;
}

/* --- Query 2: Studio catalog — PAGINATED and FIXED ---
   Key fixes:
    a) Removed `status = 1` filter (killed almost everything; status enum
       meaning is non-standard in this schema).
    b) Walk the category parent tree up to 5 levels to find the ROOT
       category name (Modular / Neo Modular / Studio / Services), then
       map to type_modular / type_neo_modular / type_bonito_studio /
       type_service for the dashboard's existing logic.
    c) Normalize ERP to upperUTF8(trim) to match sales. */
function buildStudioQuery(page){
  const offset = page * PAGE_SIZE;
  return `
WITH
cats_dedup AS (
  SELECT id, name AS cat_name, parent_category_id
  FROM bonito_analytics.pulse_commerce_product_categories
  WHERE _peerdb_is_deleted = 0
  ORDER BY _peerdb_version DESC
  LIMIT 1 BY id
),
-- Walk the category tree up to 5 levels to find the top-level ancestor.
cat_roots AS (
  SELECT c1.id AS cat_id,
         c1.cat_name AS leaf_name,
         COALESCE(c5.cat_name, c4.cat_name, c3.cat_name, c2.cat_name, c1.cat_name) AS root_name
  FROM cats_dedup c1
  LEFT JOIN cats_dedup c2 ON c2.id = c1.parent_category_id
  LEFT JOIN cats_dedup c3 ON c3.id = c2.parent_category_id
  LEFT JOIN cats_dedup c4 ON c4.id = c3.parent_category_id
  LEFT JOIN cats_dedup c5 ON c5.id = c4.parent_category_id
),
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
  upperUTF8(trim(v.erp_code)) AS erp_code,
  COALESCE(p.product_name, '') AS product_name,
  COALESCE(v.variant_name, '') AS variant_name,
  COALESCE(b.brand_name, '') AS brand,
  COALESCE(cr.leaf_name, '') AS category,
  COALESCE(cr.root_name, '') AS main_category,
  CASE lower(COALESCE(cr.root_name, ''))
    WHEN 'modular' THEN 'type_modular'
    WHEN 'neo modular' THEN 'type_neo_modular'
    WHEN 'bonito studio' THEN 'type_bonito_studio'
    WHEN 'studio' THEN 'type_bonito_studio'
    WHEN 'services' THEN 'type_service'
    WHEN 'service' THEN 'type_service'
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
LEFT JOIN cat_roots cr ON cr.cat_id = p.product_category_id
LEFT JOIN bng_prices bng ON bng.variant_id = v.id
LEFT JOIN mum_prices mum ON mum.variant_id = v.id
ORDER BY upperUTF8(trim(v.erp_code))
LIMIT ${PAGE_SIZE} OFFSET ${offset}
`;
}

const Q_PROJECTS_PG = `
  SELECT project_no, project_type, project_value,
         branch, status, current_milestone, p_date, campaign
  FROM project_details
  WHERE project_no IS NOT NULL
`;

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

  const page = (view === 'sales' || view === 'studio')
    ? parseInt((event.queryStringParameters || {}).page || '0', 10)
    : 0;
  const cacheKey = (view === 'sales' || view === 'studio') ? `${view}:p${page}` : view;

  const acceptEncoding = (event.headers && (event.headers['accept-encoding'] || event.headers['Accept-Encoding'])) || '';
  const wantsGzip = /\bgzip\b/.test(acceptEncoding);

  try {
    let data = getCached(cacheKey);
    let cacheHit = !!data;

    if(!data){
      if(view === 'sales'){
        data = await clickhouseQuery(buildSalesQuery(page));
      } else if(view === 'studio'){
        data = await clickhouseQuery(buildStudioQuery(page));
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
      setCached(cacheKey, data);
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
