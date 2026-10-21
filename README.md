# PKO Sales Dashboard

Live-data version of the Ilab Inventory Dashboard. Same UI, no more CSV pushes.

## Data sources

- **ClickHouse** (`bonito_analytics`) — sales line items, product catalog, brands, categories, prices
  - Mirrored from Postgres by PeerDB, ~1 min lag
  - Handles the heavy aggregations across 12M line items
- **Postgres read-replica** — `project_details` table for Project Value, Project Type, branch
  - Fast lookup (~19K rows)

## Deploy

1. Create a new GitHub repo named `bonito-pko-sales-dashboard`
2. Push these files
3. In Netlify: New site from Git, point at the repo
4. **Set env vars** in Site settings > Environment variables:
   - `CH_HOST` — ClickHouse hostname (e.g. `qp25599mkl.ap-south-1.aws.clickhouse.cloud`)
   - `CH_USER` — ClickHouse username
   - `CH_PASS` — ClickHouse password
   - `PG_HOST` — `bonitoapp-read-replica.cnxne33fjape.ap-south-1.rds.amazonaws.com`
   - `PG_USER` — `postgres_read`
   - `PG_PASS` — Postgres password
   - `PG_DB` — `bonito`
5. Deploy

## Architecture

```
Browser
   │
   ├─ GET /api/dashboard?view=sales   → ClickHouse (line items)
   ├─ GET /api/dashboard?view=studio  → ClickHouse (product catalog)
   └─ GET /api/dashboard?view=projects → Postgres (project_value, project_type)
```

Each query is cached in the function for 60 seconds, so refreshes within a minute are instant.

## Local dev

```bash
npm install
npx netlify dev
```

Opens on `http://localhost:8888` with the API routed through the local function runner.

## Changing a query

All SQL lives in `netlify/functions/dashboard.js` at the top. Only whitelisted views can be fetched — no SQL from the frontend.
