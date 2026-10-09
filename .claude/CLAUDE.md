# KoreanStory Sales Bot - Project Documentation

## Project Overview
Automated sales reporting Telegram bot for KoreanStory with interactive web dashboard, Docker containerization, and CI/CD auto-deployment to Hetzner VPS. Uses KeyCRM as data source.

## Architecture

Selective — only the entries worth naming. `core/` alone holds 43 modules; the
listing below is not a directory dump and should not be read as one.

```
key-api-bot/
├── core/                         # Shared modules (bot + web), 43 files
│   ├── config.py                # Shared configuration
│   ├── keycrm.py                # Unified async KeyCRM client
│   ├── duckdb_store.py          # The analytics store; repositories/ mix into it
│   ├── repositories/            # Nine mixins composed into DuckDBStore
│   ├── models.py                # Data models (Order, Product, Category, Buyer)
│   ├── filters.py               # Date period parsing (DateRange, parse_period)
│   ├── scheduler.py             # APScheduler jobs, addressed by string id
│   └── prediction_service.py    # LightGBM revenue prediction (train, predict, forecast)
│
├── bot/                          # Telegram bot package
│   ├── config.py                # Configuration, constants, enums
│   ├── services.py              # Business logic (sales aggregation, reports)
│   ├── handlers_legacy.py       # Every command/callback handler actually lives here
│   ├── handlers/__init__.py     # Re-export facade over handlers_legacy; adds nothing
│   ├── keyboards.py             # Keyboards; bound to handlers only by callback_data
│   └── main.py                  # Bot entry point; registers handlers imperatively
│
├── web/                          # Web dashboard (FastAPI + React)
│   ├── main.py                  # FastAPI app entry point
│   ├── routes/
│   │   ├── api/                 # Package, 16 modules; __init__ include_routers them
│   │   ├── auth.py              # Authentication routes
│   │   └── pages.py             # HTML page routes (React SPA)
│   ├── services/
│   │   └── dashboard_service.py # Data transformations (async)
│   ├── frontend/                # React Dashboard (TypeScript + Vite)
│   │   └── src/
│   │       ├── api/             # API client with error handling
│   │       ├── components/      # ALL components, flat; co-located *.stories.tsx (Storybook)
│   │       ├── hooks/           # TanStack Query hooks
│   │       ├── store/           # Zustand filter store
│   │       └── utils/           # Formatters, colors
│   ├── static/                  # Static assets
│   └── static-v2/               # Built React app (generated, gitignored)
│
├── scripts/                      # Utility scripts. Whole dir is COPYied into the
│   │                            # web image — the compact sidecar runs from it.
│   ├── compact_duckdb.py        # Host cron, Sunday 02:00 UTC. Do not touch.
│   ├── weekly_compact.sh        # The cron entry itself
│   ├── check_date.py            # Compare DuckDB vs KeyCRM for date
│   ├── check_turbosms_signature.py  # Is TURBOSMS_WEBHOOK_SECRET the one the gateway signs with?
│   ├── utm_reclassify_dryrun.py # What a UTM reclassify would move; read-only, snapshots the verdicts
│   └── force_resync.py          # Force rebuild DuckDB from API
│
├── deploy/                       # Reached only from host cron, never imported
│   ├── daily_offsite.sh         # 02:45 daily — snapshot + ship off-site
│   ├── offsite_check.sh         # 09:00 daily — freshness, in-app watchdog liveness
│   └── restore_from_export.py   # The only restore path. Never delete.
│
├── tests/                        # 67 files. pytest.ini sets testpaths, so only
│   │                            # this tree is collected — scripts/ is not.
│   └── test_data_consistency.py # KeyCRM vs DuckDB; marked `external`,
│                                # deselected by default, run with -m external
│
├── nginx/nginx.conf             # Reverse proxy configuration
├── .github/workflows/deploy.yml # GitHub Actions auto-deployment
├── Dockerfile                   # Bot container
├── Dockerfile.web               # Web dashboard container (multi-stage with Node.js)
└── docker-compose.yml           # Container orchestration
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Python | 3.14 |
| Bot | python-telegram-bot v22.0 |
| Web | FastAPI + Uvicorn |
| Frontend | React 19 + TypeScript + Vite 7 |
| Charts | Recharts |
| State | TanStack Query 5 + Zustand 5 |
| Styling | Tailwind CSS 4 |
| Database | DuckDB (analytics store) |
| ML | LightGBM + scikit-learn |
| Hosting | Hetzner VPS |

## Configuration

### Environment Variables (.env)
```
BOT_TOKEN=<telegram_bot_token>
KEYCRM_API_KEY=<keycrm_api_key>
ADMIN_USER_IDS=123456789,987654321
DASHBOARD_URL=https://ksanalytics.duckdns.org
```

**Copies of `.env` on the server go to `/root/env-backups/key-api-bot/`, never
into the repository root.** A copy carries every secret and the repository is
public. On 2026-09-16 the server's tree root held twelve `.env.bak*` files, all
world-readable, none ignored — one `git add .` from being published. Ten were
moved there; `.env.*` is now in `.gitignore`, as the second line of defence and
not the first. Make a copy with

```
install -m 600 /opt/key-api-bot/.env \
  /root/env-backups/key-api-bot/.env.bak-<what>-$(date -u +%Y%m%d-%H%M%S)
```

and keep `.env` itself at mode 600. Everything that reads it — compose, the
root crontab, `ks-alert-agent.service` — runs as root, and no container mounts
the file.

### Key Constants
```python
# bot/config.py
DEFAULT_TIMEZONE = "Europe/Kyiv"
RETURN_STATUS_IDS = [15, 18, 19, 21, 22, 23]   # KeyCRM lost/cancel group (6)
SOURCE_MAPPING = {1: 'Instagram', 2: 'Telegram', 3: 'Opencart', 4: 'Shopify'}

# core/duckdb_constants.py
B2B_MANAGER_ID = 15                             # the wholesale manager
RETAIL_MANAGER_IDS = [4, 8, 11, 16, 17, 19, 22]  # seeds managers.is_retail for NEW rows only
KNOWN_SALES_TYPES = ("retail", "b2b", "internal")
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/health` | Health check (status, version, uptime, cache stats); `degraded` while `duckdb.fatal` is set, and `degraded_by` says why |
| `/api/summary` | Summary statistics |
| `/api/revenue/trend` | Revenue time series (+ `include_forecast=true` for ML predictions) |
| `/api/revenue/forecast` | ML revenue forecast for current month |
| `/api/revenue/forecast/train` | Manually trigger model training (POST) |
| `/api/revenue/forecast/evaluate` | Walk-forward CV evaluation with baselines (GET) |
| `/api/revenue/forecast/tune` | Hyperparameter grid search (POST) |
| `/api/sales/by-source` | Sales breakdown by source |
| `/api/products/top` | Top 10 products by quantity |
| `/api/products/performance` | Top by revenue, category breakdown |
| `/api/categories` | Root categories list |
| `/api/categories/{id}/children` | Subcategories for parent |
| `/api/brands` | All brands list |
| `/api/brands/analytics` | Top brands by revenue and quantity |
| `/api/customers/insights` | New vs returning, AOV trend, repeat rate |
| `/api/customers/sms-segments` | Campaign audience — one arm or one per value level (needs `sms` view; customer rows need `sms` edit) |
| `/api/customers/sms-segments/export/csv` | Campaign list as CSV, holdout excluded (needs `sms` edit) |
| `/api/customers/sms-campaigns` | Freeze the audience as a campaign, no CSV needed (POST, needs `sms` edit) |
| `/api/customers/sms-audience-presets` | Saved audiences; PUT/DELETE by name (needs `sms` view/edit) |
| `/api/admin/users/{id}/features` | Which tabs one account may open (PATCH, admin) |
| `/api/managers` | Managers with sales_type and 365d revenue (admin only) |
| `/api/managers/{id}/retail-status` | Classify a manager, marks warehouse dirty (POST, admin) |
| `/api/health/data-quality` | Latest run per layer — integrity, reconciliation, mirror_landing, reconciliation_pg, reconciliation_ch — with issues/diffs (за сессией) |
| `/api/warehouse/status` | Last refresh, checksums, validation_passed; `cutover` — step 13's unmet preconditions (DN-28) |
| `/api/warehouse/refresh` | Force a FULL rebuild of Silver + Gold (POST, admin) |
| `/api/mirror/backfill/orders` | Ship the orders Postgres is missing; idempotent (POST, admin) |
| `/api/mirror/backfill/buyers` | Re-ship every buyer DuckDB holds, with its contacts, detached; chain 4's pre-flip lever (POST, admin) |
| `/api/mirror/backfill/catalogue` | Carry the catalogue rows only DuckDB holds and KeyCRM retired (product 1055) into Postgres; dry run by default; chain 6's pre-flip lever (POST, admin) |
| `/api/reconcile` | Run `dq_reconciliation` over any window now, all three stores (POST, admin) |
| `/api/jobs` | Scheduler jobs with live next_run and history |
| `/api/jobs/{job_id}/trigger` | Run a job now (POST, admin) |

**Retired by the owner's decision OD-10 (2026-09-30)**, and kept retired by
`tests/unit/test_od10_retired_doors.py`: `GET /api/buyers/{id}`,
`/api/orders/{id}`, `/api/products/{id}`, `/api/buyers/stats`,
`/api/debug/stale-returns`, `/api/debug/order-status/{id}`,
`/api/reconciliation`, and `POST /api/reconciliation/run`,
`/api/duckdb/purge-orders`. See "OD-10: the DuckDB-only doors". A retired
GET answers a JSON 404, as does any `/api/` GET no route matches. The SPA
catch-all used to serve those the shell with a 200, which a script reads as
success.

**Query params**: `period` (today/yesterday/week/last_week/month/last_month) or `start_date` + `end_date`, `category_id`, `brand`, `sales_type`

## DuckDB Schema

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    categories   │       │    products     │       │  expense_types  │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ id          PK  │◄──────│ category_id FK  │       │ id          PK  │
│ name            │       │ id          PK  │       │ name            │
│ parent_id   FK  │       │ name, brand     │       │ alias           │
└─────────────────┘       │ sku, price      │       └────────┬────────┘
                          └────────┬────────┘                │
┌─────────────────┐       ┌────────┴────────┐       ┌────────┴────────┐
│     orders      │       │ order_products  │       │    expenses     │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ id          PK  │◄──────│ order_id    FK  │       │ id          PK  │
│ source_id       │       │ product_id  FK  │       │ order_id    FK  │
│ status_id       │       │ name, quantity  │       │ expense_type_id │
│ grand_total     │       │ price_sold      │       │ amount, status  │
│ ordered_at      │       └─────────────────┘       └─────────────────┘
│ buyer_id        │
│ manager_id      │       ┌──────────────────────┐
└─────────────────┘       │  gold_daily_revenue  │
                          ├──────────────────────┤
                          │ date, sales_type (PK)│
                          │ revenue, orders_count│
                          │ new/returning_custs  │
                          └──────────────────────┘
```

**Key tables:**
- `orders` - Core order facts with source_id, status_id, `status_group_id`, grand_total, ordered_at, buyer_id, manager_id
- `order_products` - Line items with product_id, quantity, price_sold
- `products` - Product catalog with category_id, brand, sku, price
- `categories` - Hierarchical categories (parent_id for tree structure)
- `expenses` - Order-level expenses (delivery, commission, etc.)
- `gold_daily_revenue` - Pre-aggregated daily revenue by sales_type (the real aggregate; `daily_stats` was declared but never written and has been dropped)
- `revenue_predictions` - ML forecast predictions (date, sales_type, predicted_revenue, model metrics)
- `data_quality_runs` / `_issues` / `_diffs` - one row per check run plus its findings; the digest and `/api/health/data-quality` read these. A failed run writes a row too, with `error_message` set — never treat row-existence as "the check ran"
- `warehouse_refreshes` - ~65k rows back to 2026-03-14, the best forensic trail in the system
- `order_backfill_misses` - ids KeyCRM cannot supply, or supplies without line items; skipped for 30 days so a repair job cannot loop on them forever
- `weekly_report_sends` - one row per week delivered by `weekly_report`; what keeps a daily job to one message a week and makes a missed Monday recoverable

**Source filtering:**
- Included: Instagram (1), Telegram (2), Shopify (4)
- Excluded: Opencart (3) - deprecated

## Deployment

### GitHub Secrets
Names only — this repository is public. Host, user, registry account and key
live in the repo's Actions secrets and in the owner's private notes.

```
DOCKER_USERNAME   DOCKER_PASSWORD
VPS_HOST          VPS_USER          EC2_SSH_KEY
```

### Auto-Deployment
Push to `main` → GitHub Actions builds images → pushes to Docker Hub → SSHs to
the VPS → pulls and restarts. Concurrency group `deploy-production`,
`cancel-in-progress: false` — deploys queue instead of racing.

**A merge reaches live containers on its own.** The `deploy` job names
`environment: production`, but that environment has **no required reviewers** —
the owner removed them on 2026-08-09 — so nothing waits for a human. Verified
2026-08-12: merging #70 went from merge to `completed/success` in one step. Read
the declaration as a label, not a gate. If a run ever does sit in `waiting`,
that is a reviewer requirement someone added back.

`paths-ignore` covers `**/*.md`, `.claude/**`, `.planning/**`, `tests/**` — a
commit touching only those does not deploy. A commit touching code *and* a doc
deploys as normal, because the skip needs every changed file to match.

### Manual Deployment
```bash
ssh <vps>                       # host and user: private notes
cd /opt/key-api-bot
docker compose pull && docker compose up -d
```

## Docker Commands

```bash
# Run all services
docker-compose up -d

# View logs
docker-compose logs -f web
docker-compose logs -f bot

# Restart specific service
docker-compose restart web

# Rebuild
docker-compose up -d --build
```

### Local Development
```bash
# Terminal 1: FastAPI backend
uvicorn web.main:app --host 0.0.0.0 --port 8080 --reload

# Terminal 2: Vite dev server (with proxy to backend)
cd web/frontend && npm run dev

# Build frontend for production
cd web/frontend && npm run build

# Component library (Storybook): dev on :6006, static build to storybook-static/
cd web/frontend && npm run storybook
cd web/frontend && npm run build-storybook
```

## Monitoring

```bash
docker-compose ps
docker-compose logs --tail=50 bot
docker-compose logs --tail=50 web
docker-compose logs --tail=50 nginx
```

## Testing

`pytest.ini` sets `testpaths`, `asyncio_mode = strict`, and deselects the
`external` and `slow` markers by default, so a bare `pytest` is the safe run —
no paths to remember, nothing reaching the network.

**Baseline as of 2026-08-14 (`d81b194`): 1297 passed, 7 deselected, 39s.**
Any change that lowers the passing count is a regression until explained.

**CI runs the suite against a real PostgreSQL** since 2026-09-08. Without one,
every differential test — the ones proving DuckDB and Postgres answer the same
question the same way — skipped itself on `KS_PG_DSN`, so **60 checks never
ran on a pull request**, including all 20 guarding the /traffic port. That tab
reads Postgres in production behind a silent fallback to DuckDB, so a query
broken in one engine only would have kept the page working and left an ERROR
in a log nobody reads. `.github/workflows/ci.yml` now starts
`postgres:17.2-alpine` after checkout with `postgres/initdb` mounted (a
`services:` block starts *before* the checkout that would supply those
scripts), applies `alembic upgrade head`, and **fails the job if any test
still skips for want of PostgreSQL** — the arrangement breaking silently is
the state it replaced. Baseline with the store up: **3 771 passed, 8 skipped**
(ClickHouse, which CI still has no container for). `tests/unit/test_ci_workflow.py`
pins all of it, including that the image matches `docker-compose.yml` and
`deploy/gate_with_stores.sh`.

```bash
# The suite. No network, no production data.
pytest -q

# What CI runs. The store tests need a database; `deploy/gate_with_stores.sh`
# builds one on the VPS, and locally any throwaway will do:
#   docker run -d --name pg -p 5432:5432 -e POSTGRES_USER=postgres \
#     -e POSTGRES_PASSWORD=x -e POSTGRES_DB=ks \
#     -v "$PWD/postgres/initdb:/docker-entrypoint-initdb.d:ro" postgres:17.2-alpine
#   docker exec pg psql -U postgres -tAc "ALTER ROLE ks_app WITH PASSWORD 'x'"
#   KS_PG_DSN=postgresql://ks_app:x@127.0.0.1:5432/ks alembic upgrade head
KS_PG_DSN=... pytest -q

# On a machine that is not UTC, `test_expenses_two_engines.py` fails three
# ways: it compares *rendered* timestamps, so it only passes where the
# renderer agrees with Postgres. CI and the production image are UTC.
TZ=UTC KS_PG_DSN=... pytest -q

# The external tests, deliberately: these reach the live KeyCRM API and the
# production DuckDB file.
pytest -m external -v

# Check specific date
PYTHONPATH=. python tests/test_data_consistency.py 2025-12-07

# Force resync DuckDB
python scripts/force_resync.py --days 365
```

## Caching

**There is no server-side response cache, and there never has been in
production.** There was a Redis client (`core/cache.py`) for months, but no
Redis container ever existed in `docker-compose.yml` (`git log -S redis --
docker-compose.yml` is empty), so it was a no-op in every environment it ever
ran in. It was deleted 2026-08-10 along with the `redis` dependency, the
startup connection attempt, and the TTL/warming config knobs for a job that
never existed.

This section used to describe a 5-minute TTL and warming every 4 minutes. It
was describing a system that has never run, and that fiction is the reason a
later audit rated the "missing" cache as the standing cause of dashboard
latency. Measured on prod 2026-08-10 instead: at the TTL the code actually
used, a cache would hit **0.1%** of requests, and it would hit **none** of the
slow ones — the slow requests come from a user stepping through dates, where
every URL is distinct. The real costs are elsewhere; see
`.planning/redis-cache-investigation/`.

**If caching is ever warranted again** — a wall-mounted dashboard, several
people on one shared link — the shape is a bounded in-process TTL cache with a
TTL checked against the frontend's refetch cadence, not a datastore. Do not
reintroduce one without a hit-rate measurement on real traffic first.

What does exist:
- **Small in-process caches where evidence demanded them** — `/api/health`
  stats (60s), product categories, product brands
- **Client-side caching** — TanStack Query, 2 min for realtime data
  (`web/frontend/src/hooks/useApi.ts`), which absorbs most repeats before the
  server sees them
- **Gzip compression** - ~70% smaller responses
- **orjson** - Fast JSON serialization

## Key Features

### Web Dashboard
- Revenue trend with daily breakdown
- Sales by source (bar + doughnut)
- Top products by quantity and revenue
- Category drill-down
- Brand analytics
- Customer insights (new vs returning, AOV trend)
- MilestoneProgress with goal tracking and celebrations
- ML revenue prediction (forecast bars for remaining month days)
- Responsive design with mobile optimizations

### Revenue Prediction (LightGBM)
- Trains on ~780 days of historical daily revenue data from `gold_daily_revenue`
- **31 engineered features** across 10 groups:
  - Calendar (4): day_of_week, month, day_of_month, week_of_year
  - Cyclical (4): month_sin/cos, dow_sin/cos
  - Lags (5): 1d, 7d, 14d, 28d, 365d
  - Rolling (4): mean_7d, mean_14d, mean_28d, std_7d
  - Trend + Momentum (4): yoy_ratio, trend_index, momentum_7d_28d, revenue_growth_7d
  - Events (1): days_to_nearest_event (holidays, promos, Black Friday)
  - Payday (1): days_to_payday (distance to nearest 1st/15th, capped at 7)
  - DOW-specific (2): rolling_mean_4w_same_dow, rolling_std_4w_same_dow
  - AOV + Orders (3): rolling_mean_7d_aov, lag_7d_orders, rolling_mean_7d_orders
  - Customer mix (3): new_cust_ratio, returning_ratio, return_rate (all 7d rolling)
- **Tuned hyperparameters** (saved to `data/lgbm_best_params.json`): num_leaves=31, learning_rate=0.01, min_child_samples=5, reg_alpha=0.1, subsample=0.8
- **DOW residual corrections**: computed on last 180 days, clamped [0.70, 1.30], saved to `data/dow_corrections.json`
- LightGBM with 500 rounds, early stopping (patience=50), time-series validation (last 60 days)
- **Performance** (6-fold walk-forward CV): WAPE=27.66%, R²=0.066, beats best baseline by 1.53%
- Retrains twice weekly, Mon and Thu 03:30 Kyiv (`REVENUE_TRAIN_SCHEDULE` in `core/scheduler.py`), and at a start with no model artefact on disk
- Predictions stored in `revenue_predictions` — DuckDB, or Postgres `app.revenue_predictions` under chain 7b-3 (`KS_WRITE_FORECAST`, off)
- Frontend shows forecast bars (lighter opacity) on Revenue Trend chart when period=month
- "Predicted: ₴X" badge in chart header
- Graceful degradation: chart works normally if model unavailable
- Model saved to `data/revenue_model.joblib`, auto-rejected on load if feature count mismatches
- **Known limitation**: Thu/Fri have ~₴30K underprediction bias due to bimodal distribution (promo spike days ₴300-600K are unpredictable from lagged features)

### Telegram Bot
- Sales summary reports by source
- Excel reports
- TOP-10 products
- Date filtering (today, yesterday, week, month, custom)
- Dashboard link

## Important Notes

### UI component architecture (frontend)
Every visual style lives inside its component; nothing cosmetic crosses a
component boundary. Concretely:

- **No `style`/`className` props on components.** Consumers express intent via
  semantic props only (`variant`, `tone`, `surface`, `size`, `hideBelow`, ...).
  Internal `className`/inline styles for data-driven values (a bar width, a
  virtual-list offset) are implementation, not API.
- **Layout between siblings is the parent's job, via `Wrapper`** — the one
  component allowed to carry `gap`/`padding`/`margin`/`flex` props. Page-level
  chrome comes from `PageShell` (feature | dashboard | admin), chart pages
  compose `ChartSection`/`ChartGrid`.
- **Icons pass as components, never as styled elements**: `icon={Users}`, typed
  `IconComponent` (`components/icons.tsx`); the owning component sets size and
  colour. Lucide icons satisfy the contract as-is.
- **All components live flat in `src/components/`** — pages included; no local
  UI components inside page files. Check for an existing component before
  writing a new one.
- **Storybook is the component library** (`npm run storybook`): stories are
  co-located `X.stories.tsx`. A new or reshaped component gets a story in the
  same change.
- **Spacing has two levels, one token each, same on every tab.** The page
  rhythm — between top-level blocks (a chart panel, a summary row, a table) —
  is 16px → 24px on sm+, owned only by `PageShell` (vertical) and `ChartGrid`
  (side-by-side panels). Tile grids — metric cards in a row, inside or outside
  a panel — are 12px → 16px on sm+, owned by `TileGrid`. Pages and chart
  components never write their own `space-y`/`gap` for these; if two tabs
  disagree about a gap, something is bypassing one of the three owners.
  Spacing *inside* a card (padding, header margins) stays the component's own.

### Timezone Handling
KeyCRM API stores timestamps in +04:00 (server timezone), but UI displays in Europe/Kyiv. DuckDB queries use `_date_in_kyiv()` helper to convert before extracting dates.

### Retail / B2B / Internal
`sales_type` is materialised into `silver_orders` by one CASE in
`refresh_warehouse_layers`, and every dashboard endpoint defaults to
`Query("retail")`:

```
manager_id IS NULL                        → retail    (Shopify, no manager)
manager_id = B2B_MANAGER_ID               → b2b       (the wholesale manager)
manager_id in managers.is_retail = TRUE   → retail
otherwise                                 → internal
```

**Retail is a fixed list of retail managers and b2b is the wholesale manager;
nobody else is mixed into either.** `internal` is everyone else — staff whose
work is neither: their own sales, and shipments to bloggers that carry line
items and no money at all. It is admin-only, enforced in `api_gate`.
`sales_type=all` spans every category and is not gated.

`api_gate` reads `sales_type` from the **query string**. That was sufficient
once `/api/dashboard/batch` was deleted — it was the only route that took
`sales_type` in a request body, and it needed its own check for exactly that
reason. Any new POST accepting `sales_type` in a body reopens the hole and
needs the same treatment.

`managers.is_retail` is seeded from `RETAIL_MANAGER_IDS` for managers the
warehouse has never seen and **never overwritten afterwards** — it used to be
recomputed on every sync, which made a human's classification impossible to
keep. Set it via `POST /api/managers/{id}/retail-status`, which also marks the
warehouse dirty, because `sales_type` only changes on a rebuild.

### The audience of an SMS campaign
Until 2026-08-26 the page had exactly one cohort: three value tiers over a
270-day window, from six thresholds hardcoded in the API. Two knobs were on
screen (LTV basis, holdout), the rest were invisible, and creating a campaign
meant downloading a CSV — `freeze_sms_campaign` was only reachable from the
export route.

An audience is now **grouping + filters**, and both travel as flat query
parameters — not a JSON body — because the CSV download is a link the browser
follows and `api_gate` reads `sales_type` from the query string.

**Grouping** (`grouping=rfm|single`) decides the arms, and `single` is the
default. `rfm` is the three value levels, and anyone in none of them is
dropped — which removes most one-order buyers, so it must never be a default.
`single` puts everyone the filters kept into one arm named `ALL`.

**The value level is a filter, not the split.** It is computed for everyone and
`tier=VIP` narrows the audience under either grouping — "send to VIP only,
measured as one group" is the commonest campaign there is. Tying the two
together is what made three tier cards read as three audiences. Splitting also
costs sensitivity that these audiences do not have to spare: the whole retail
base measured as one arm sees a lift from 1.07 pp, and as three arms from 1.83,
2.41 and 2.59 — 2.24 to 3.20 with Holm. The only campaign there has been moved
conversion by about 2 pp.

**Filters** are two families and they read the customer differently:

- *aggregate* — recency window, order count, LTV, average order, first-order
  date, city. Predicates over the customer's own history.
- *content* — brand, category, source, promocode, optionally within
  `bought_within_days`. An `EXISTS` over the customer's order lines, **never a
  join**: filtering the line items would recompute LTV from the matching lines
  alone, and a customer's value is not "what they spent on this brand".

A category filter matches the branch (`category_id` *or* `parent_category_id`);
picking a parent and getting nothing because every product hangs off a child is
the kind of empty result nobody debugs.

The filters live in one `ok_filters` column in the same pass that flags every
other eligibility rule, so the funnel gains a `filtered` stage and an empty
audience can say which rule emptied it. An empty filter set is not a predicate:
it selects exactly what the tier rules alone selected, so campaigns built the
old way stay reproducible.

**Presets** (`sms_audience_presets`) are the wizard's form state under a name,
and are **never executed** — the page reads one, fills its controls, and sends
the values back through the same validated parameters. The built-ins live in
code (`BUILTIN_AUDIENCE_PRESETS`), so "RFM tiers" cannot be edited into
something that no longer means what past campaigns meant.

`POST /api/customers/sms-campaigns` freezes the roster without a CSV. It
refuses the placeholder name `default`, an empty audience, and a truncated one.
The wizard builds the preview and the freeze from the same query string, so
what is recorded is what was on screen.
### Two axes: a level and an area

Access is a **pair**, and conflating the two halves is the mistake this section
exists to record. `marketer` was a *role* — and it was an **area wearing a
level's clothes**: a viewer's depth plus `sms` edit. So "a marketer who may
only read campaign results" could not be said at all; the only marketer there
could be was one who may send. The owner put it plainly: viewer and editor are
one category, marketer is another, and a person may be marketer-viewer or
marketer-editor.

* **level** — `viewer`, `editor`, `admin` (`Role`). How deep, nothing else.
  The stored matrix carries this and is now **uniform across features**: every
  level may view what it is asked about, `edit` starts at editor, `delete` at
  admin, and `user_management` — the access system itself, not a tab — stays
  with admin alone.
* **area** — `dashboard_users.allowed_features`. Which tabs, nothing else.

`view` is "the tab is in my set"; `edit` is that **and** a level of editor or
above. So "viewer over the marketing tabs" and "editor over the same tabs" are
one pair each, and `ACCESS_PRESETS["marketer"]` is that area by name.

**`DEFAULT_TABS` is load-bearing and arrived with this.** While the matrix
carried areas, an account with no override saw whatever its role granted, and
a viewer's row simply did not grant `margin`, `expenses` or `sms`. With depth
uniform, "no override" without a default would have handed all sixteen viewers
the margin tab, the expenses block and the SMS roster the moment it shipped.
The default is written down instead: viewer gets `standard`, editor adds
`expenses` (what the editor row granted before), admin is not narrowed at all —
which is what `None` means there.

**Revision 0023 carries the data**, because `seed_default_permissions` fills
pairs the table lacks and never rewrites one it has: the stored rows still
described the old world, where `viewer/sms = false` would make "marketer
viewer" somebody holding the SMS tab and seeing nothing on it. Safe to rewrite
because it was measured first — every stored pair equalled the code defaults,
so nobody had ever customised the matrix. The one `marketer` account became
`editor` with that preset's tabs, keeping every ability it had. Executed
against a production-shaped copy before shipping, not only read.

It does not downgrade: going back would have to invent which editor used to be
the marketer, and guessing that is guessing who may spend money on SMS.

### Which tabs one person may open

Access used to be a **role** and nothing else: four roles, a stored
`role × feature` matrix, and — because only `expenses` and `sms` were ever
gated on the server — an approved account could read every other page's API
whatever the sidebar showed it. "Open only /traffic to this person" could not
be said at all: `traffic`, `products`, `marketing` and `margin` were not
features, and `/margin` was behind the admin role, which meant granting the
profit numbers also granted user management and the warehouse controls.

It is now **role + tab set**, and the sentence that decides everything is:
**the tab set decides what is visible, the role decides what may be done
inside it.**

- `TAB_FEATURES` (`core/permissions.py`) is the grantable list, in the
  sidebar's order: dashboard, products, traffic, inventory, reports,
  marketing, margin, expenses, sms. The admin checklist, the bot's keyboard
  and the route table are all built from it, so a new tab is added once.
- `user_management` is deliberately **not** in it. The admin pages stay on the
  admin role: a checkbox that hands somebody the access system itself would
  make every other rule here advisory. `analytics` and `customers` are absent
  too — they are stored matrix rows that gate nothing.
- `apply_feature_override` is the rule, and it is pure. A ticked tab is
  viewable even where the role would not show it — that is how "traffic only"
  works without inventing a role — while `edit` and `delete` still come from
  the role. **No override ever grants an action**: ticking `sms` for a viewer
  gives roster sizes, never the CSV of names and phone numbers nor the send.

**Three states, and the middle one is the one that gets lost.**
`dashboard_users.allowed_features` (revision 0021, and the DuckDB twin in
migration 0029) is NULL for "as the role" — what all 24 existing rows meant on
the day it shipped, so nobody's access moved — a comma-separated list for
"exactly these", and an **empty string** for "none of them", which is a real
choice an admin can make from the page. Folding the empty set into NULL turns
"see nothing" into "see everything the role shows".

**A column, not a table.** `_resolve_session` reads this row on every request
to every tab, and that read *is* revocation here — the session cookie is a
stateless signed token that cannot be withdrawn. A second table would mean a
second read on that path, or a cache, and a cache is a revocation that takes
effect later than the admin thinks it did.

**The server enforces it, or it is decoration.** Routers that serve exactly one
tab carry the gate at their include (`traffic`, `products`, `inventory`,
`margin`); modules that mix tabs carry it per endpoint (`reports` also serves
/marketing; `analytics` serves the dashboard and the two charts /marketing
shares with it). Three things stay open to any approved session and each is a
decision: `/api/health`, `/api/me`, and the filter lookups `/categories`,
`/brands`, `/promocodes`, which are the header's dropdowns on every page.
`/api/summary` is `require_any_permission(["dashboard", "marketing",
"traffic"])` — the revenue totals are the dashboard's cards *and* what the ROAS
block on /traffic and the ROI calculator on /marketing divide by, so gating it
on `dashboard` would leave a traffic-only account looking at empty tiles.
`tests/integration/test_tab_permissions.py` fails if a tab has no gate at all.

**`/margin` is a permission now, held by the admin role alone.** Nothing
changed about who can open it; what changed is that it can be ticked for one
person without making them an admin.

**Presets live in code** (`ACCESS_PRESETS`), like `BUILTIN_AUDIENCE_PRESETS`
and for the same reason: a stored preset can be edited into something that no
longer means what it meant when somebody was granted it, and the bot would
need a second store to read before it could draw a keyboard. `standard` is the
default an approval grants and equals what a viewer's role opened the day
before this existed — computed from `ROLE_PERMISSIONS` in the test, so
changing the role and forgetting the preset fails rather than quietly widening
every future approval.

**The bot chooses the tabs at the moment of approval.** ✅ Approve now grants
the dashboard row explicitly (`GRANT_ACCESS`, one statement — an insert
followed by an update would leave the person approved with somebody else's
tabs in the second the bot tells them they have access) and redraws the same
message as a checklist: a chip per tab, the presets, and Готово. Every tap is
written through immediately rather than accumulated and saved at the end — an
admin who taps three tabs and walks away has made three decisions. The first
tap on an account with no override starts from `standard`, not from an empty
set, or the first tick would silently revoke everything else.

Callback data is `atab:<id>:<key>` — a colon, because the neighbouring auth
handlers read ids with `split('_')[-1]` and a key containing an underscore
would be read as the id.

**The bot writes it through the store port**, not through a second connection:
`DashboardTabs` in `core/bot_store.py`, implemented against `app.dashboard_users`
in `bot/store_postgres.py`. The statements themselves are in
`core/dashboard_access.py`, one text with the `{users}`/`{self}` holes, run by
the web container through `core/repositories/users.py` and by the bot through
its own pool — a grant written two ways would drift the first time one of them
learned something the other did not.

`available()` is the honest half: it needs `KS_BOT_STORE=postgres` **and**
`KS_USER_STORE=postgres`. Under `KS_USER_STORE=duckdb` the dashboard's list is
in a file the web container holds the single writer for, so no bot process can
reach it — the approval still grants bot access and the message says the tabs
are set from Admin → Users, rather than showing a checklist that would do
nothing. Two variables in the same `.env`; if they ever disagree, a tab set
written by the bot would go to a table nothing reads.

One portability trap, found by running the statement rather than reading about
it: DuckDB cannot resolve a bare `CURRENT_TIMESTAMP` on the right of
`ON CONFLICT DO UPDATE SET` — it binds the name as a column. The value comes
out of `excluded` instead, which is what `create_user` and `set_permission`
already do here.

**The bot hands out the same numbers, so it asks the same question.** A
summary report *is* the dashboard's revenue in a Telegram message, and until
2026-09-08 approval alone decided who got one — granting somebody the traffic
tab and nothing else still sent them last week's revenue every Monday and let
`/report` answer in full. `core.permissions.BOT_SURFACES` now maps each thing
the bot produces to the tab it belongs to (summary and TOP-10 → `dashboard`,
Excel → `reports`, search → `dashboard`, the weekly push → `dashboard`), and
`@authorized(surface=...)` refuses the rest. **One table, not an argument per
handler**: spelled at fifteen call sites, "marketing sees marketing" and
"traffic sees traffic" would drift apart the first time somebody added a
report, and a test walks the handlers to catch a surface that is not in it.

Gated at the three *generators* rather than at `/report` — the menu offers
three branches and only the branch that produces data knows which tab it is.

**Permissive where it cannot know, and that is the opposite of the web's
answer on purpose.** `DashboardTabs.permissions()` returns None when the
dashboard's list is unreachable, when the person has no row there, or when
their row carries no override; all three mean nothing has been narrowed, and
the bot behaves as it did before tabs existed. The web fails *closed* on an
unreadable store because a narrowing exists there to be undone; a reporting
bot that answers "no" because a read threw would take the bot away from
everybody to protect a restriction nobody has.

`search` is the one conservative entry: the web's `/api/search*` is admin-only
while the bot has offered search to every approved person since long before
tabs. It is gated on `dashboard`, which keeps that promise for anyone who is
not narrowed; whether the two doors should agree on *admin* is a separate
decision and is deliberately not taken.

**The request itself is visible in the UI, and actionable there.** It was not
at first, and the gap was exactly the shape of the two lists: a person asking
for access lands in the **bot's** list as `pending`, and the dashboard's list
gains a row only when somebody approves — so `/admin/users`, which reads the
second list, had nothing to show until the decision had already been taken on
a phone. `GET /api/admin/access-requests` reads the bot's queue and the two
`POST .../approve|deny` endpoints write exactly the rows the bot's own buttons
write: the conditional `expected_status="pending"` (two admins must not both
win), then `grant_access` — the shared statement — and a message to the person
in their own language over the same HTTP transport the weekly report uses, so
the kill switch and the admin-only signature both still apply. The lists are
still not merged; this gives one decision a second door.

The tabs are picked **before** the grant on that card, not after it, which is
also the order the bot's keyboard uses. A denial writes no dashboard row at
all: a refusal is not a decision about somebody who is not there.

**The redirect could not stay a constant.** `RouteGuard` used to send a denied
visitor to `/`; an account granted /traffic alone is *denied* at `/`, so that
is now a redirect loop. It computes the first page the account can actually
open (`utils/access.firstAllowedPath`) and, when there is none, says so on the
screen instead of navigating.

**Access no longer touches DuckDB at all.** Revision 0016 moved the user list;
the matrix saying what each role may do stayed behind, cached per role and read
out of DuckDB — defensible while ~20 endpoints were gated. With a permission
dependency on ~120 of 140, every cache miss took DuckDB's **process-wide store
lock** on an authorisation path, and that lock is held by a warehouse rebuild
every two minutes (the measured cause of the SMS tab going 124 → 38 req/s).
Revision 0022 puts `role_permissions` in `app`, routed by `_perms_run` through
the same `KS_USER_STORE` switch, so one variable still answers "where does
access live" — splitting them would let an approval and the permissions behind
it disagree about which database is authoritative.

**Not ClickHouse**, asked and answered: these rows are read on every request,
written by a human, and authoritative. ClickHouse has no transactional update
and holds what can be rebuilt from source; access can be rebuilt from nothing.

**No copy step, because there was nothing to copy.** Measured on the 2026-09-07
snapshot before writing any: all 32 stored pairs equal the code defaults
exactly — nobody had ever edited the matrix — so `seed_default_permissions`
reproduces it. Verify after deploying rather than assuming; a permission
someone turns off in the window between the two stores stays in DuckDB.

**Three things the audit of this change moved**, each because putting a gate on
~120 endpoints instead of ~20 changed what a detail costs:

- **The session is resolved once per request**, cached on `request.state`.
  Two dependencies ask — `api_gate` at the include, then the permission gate on
  the route — and each ask was a signature check plus a read of the user list;
  under `KS_USER_STORE=postgres` that read carries `require_revision()`, so a
  doubled resolution was four pool acquisitions per request against a pool of
  five. Within one request the answer cannot change, so revocation is unmoved:
  it is still the *next* request that finds an account no longer approved.
- **An unreadable user list refuses the request** instead of falling back to a
  viewer with no override. An error and an absence used to share one exit, and
  that exit handed back every tab a viewer's role opens — silently undoing an
  admin's narrowing at the one moment nothing can be verified. The realistic
  trigger is `SchemaVersionError`: `web` deployed ahead of `migrate`, where
  this read raises while the bot's store, which checks the revision only in
  `initialise()`, keeps answering. A genuine absence still falls back, because
  an account with no row has no tab set to contradict.
- **A broadcast that carries money names its tab.** `ConnectionManager.broadcast`
  takes an optional `feature` and connections remember their viewer's tabs from
  the handshake. Most events name none on purpose — the room is how the whole
  UI learns a sync happened, and scoping `orders_synced {count}` would stop a
  traffic-only page refreshing itself for no gain. `goal_progress` is scoped,
  though **nothing emits it today**: it is the one payload that would be
  revenue, so whoever wires the emitter up inherits the answer.

### Who may run an SMS campaign
Sending is the one thing on this dashboard that spends money and reaches
customers on their phones, and the roster behind it is 6 000 names and phone
numbers. It used to be `require_admin` on all nine endpoints, so the only way
to let somebody run a campaign was to hand over user management, expenses,
margin and the internal sales_type with it.

It is a permission now — `sms` in `core/permissions.py` — with the split that
matters:

- **view** — roster sizes, past results, which channels are configured;
- **edit** — the CSV of names and phone numbers, `include_customers=true`, the
  test send, the send itself, marking a campaign sent, opt-outs.

`marketer` is the role that carries it: a viewer everywhere else, `sms` view
and edit. Admins keep it through the same matrix, and hardcoded admins
short-circuit it entirely. `/sms` in the frontend is gated on the permission,
not on the role — the sidebar link, the route guard and the API all read the
same answer.

`seed_default_permissions` fills **missing** (role, feature) rows on every
call, from `ROLE_PERMISSIONS`. It used to return the moment the table held a
row, which meant any feature added after the first deploy was denied to every
DB-backed role. Turning a permission off writes `false` rather than deleting
the row, so re-seeding cannot resurrect a decision.

### Order statuses
Revenue excludes KeyCRM's lost/cancel group (`status_group_id = 6`), verified
against the API one live order per status. `orders.status_group_id` is stored
and preferred wherever known; `OrderStatus.return_statuses()` is the fallback
for rows synced before the column existed and gives an identical answer on
current data. Enum members carry KeyCRM's labels; the names this codebase used
before 2026-08-09 are preserved in `LEGACY_STATUS_NAMES`.

Status 20 is «Прибув у відділення» (group 4) — a parcel at the branch, and
revenue. It appeared 2026-07-09 and went unnoticed for a month, which is the
whole reason the group is read from the source now.

### Expense API Limitation
- Order expenses available via `include=expenses`
- Global expenses (Facebook, taxes) NOT available via KeyCRM API

### Scheduled checks and when they run (Europe/Kyiv)

| Job | Trigger | What it does |
|---|---|---|
| `warehouse_refresh` | every 2 min | Silver + Gold rebuild, validation, cell guard; not registered under `KS_WRITE_WAREHOUSE=postgres` |
| `halfwritten_repair` | every 2 h | re-fetch orders with revenue and no line items |
| `dq_integrity_check` | 01, 07, 13, 19 | DB-only scans: PK/FK/NULL/domain, cross-metric |
| `dq_reconciliation` | 05:30 | compare 90 days against KeyCRM, per order — **all three stores** (DuckDB, PG, ClickHouse), one fetch; слои `reconciliation`, `reconciliation_pg`, `reconciliation_ch` |
| `dq_mirror_landing` | 07:30 | Reconciliation A: landing, Silver, Gold, the five `app.*` tables, `bot.db` — tolerance zero — then the order-version archive, which is a liveness check and not a comparison |
| `replicate_operational` | every 1 h | Copy the five irreplaceable tables and `data/bot.db` into Postgres |
| `ch_sync` | every 1 h | Ship silver → ClickHouse, derive gold there, append the archive (шаги 5–6); stands down without `KS_CH_URL` |
| `dq_digest` | 09:00 | one message with WARN+ findings and a delta |
| `weekly_report` | daily 09:30 | last complete week's numbers to every approved user — sends once, then quiet |
| `traffic_report` | daily 09:45 | last complete week's attribution to the admins — sends once, then quiet |
| `bot_memory_watch` | every 30 min (bot) | бот сторожит свои 512 МБ тем же evaluator'ом; предыдущий сэмпл — в `data/memory-bot-last.json` (OOM-счётчик ядра сбрасывается при recreate) |

The legacy 06:00 `reconciliation_check` is gone (OD-10, 2026-09-30):
`dq_reconciliation` is the reconciliation. It repairs only the orders we do
not hold. The one repair only the legacy job made now belongs to
`order_status_refresh` (05:15): an order dated into the last 30 days but
created before them, whose status or total moved without `updated_at`
changing. That job re-fetches these orders by id, before 05:30 looks at them.

**Never schedule anything at 05:00–05:05 Kyiv.** The host cron
`0 2 * * 0 weekly_compact.sh` is 02:00 UTC — the same instant — and it stops
both containers. A `CronTrigger` computes its next fire from registration, so
a scheduler that comes back at 02:00:51 sets the next run a day out: the job
is not late, it does not exist when it is due, and `misfire_grace_time` has
nothing to forgive. That cost `dq_reconciliation` every Sunday.

`BackgroundScheduler.start()` queues a one-off catch-up for any check whose
last *successful* verdict is older than its cadence (`CATCHUP_CHECKS`), which
also covers deploys landing on a cron instant. For `dq_reconciliation` that is
the oldest of the layers it writes in this process (`CATCHUP_SIBLING_LAYERS`:
`reconciliation_pg` with `KS_PG_DSN`, `reconciliation_ch` with `KS_CH_URL` as
well — a host without one never writes the layer, and would otherwise re-fetch
KeyCRM on every start). Every layer the canary pages on is read by a catch-up
whose limit (26 h) is under the page's (30 h), so the first probe after a
restart pages only a layer already past 30 h; a test holds both halves. A
catch-up cannot hold back a page that is owed: a run that compared nothing —
the ClickHouse arm against a copy over 3 h old included — is a failed run and
moves no age, so while ClickHouse stays down each restart past 26 h costs one
KeyCRM fetch and the page still fires at 30 h.

`weekly_report` solves the same problem a different way: it ticks **daily** and
reports the last *complete* Monday–Sunday week, recording each delivery in
`weekly_report_sends`. Six firings out of seven find the week already sent and
return quiet, and a Monday spent down becomes a Tuesday delivery instead of a
week nobody ever sees. It defers while `MAX(date)` in Gold is still behind the
week end, so no report is ever rendered mid-rebuild.

**Audience**: every approved bot user plus the admins, minus anyone who
switched `notifications_enabled` off — a toggle some messages ignore is worse
than no toggle. The report is `retail` only, which is exactly what an approved
user already sees on the dashboard, so the wider audience does not widen what
is disclosed. An unreadable `bot.db` falls back to the admins alone.

**It sends the rich form** (Bot API 10.1, June 2026), which is a document
rather than a run of text: `<h1>`, real `<table>`s, `<ul>`, `<details>`, a
`<figure>` between paragraphs, `<tg-button>`, `<tg-math>`, and a 32 768-char
budget instead of a caption's 1 024. `format_report_rich` renders it and
`send_rich_message_http` carries it, uploading every picture in the same
multipart request (`attach://<id>`, named in the HTML as
`tg://photo?id=<id>`).

**Three rungs, each a fallback for the one above**: the rich form, then the
card with the report as its caption, then plain text. A 400 from the API — a
tag Telegram does not know, a reader on a client too old — returns 0 from the
transport, and the next rung sends. That ladder is the whole safety story of
the rich form: it can cost the shape of the report, never the report.
`KS_WEEKLY_REPORT_RICH=0` on the web container is the rollback and needs no
deploy. Unlike `KS_BOT_STORE` an unrecognised value does **not** raise: that
variable decides where the approval list lives, this one decides what a
message looks like, and taking the weekly report down over a misspelling is
the worse failure. The ladder is three `chat_ids=` overrides in one job,
which is why `tests/unit/test_alert_recipients.py` expects three and names
them.

**Written for a reader who is not an analyst** (owner's brief, 2026-09-07):
a `<blockquote>` summary in three sentences — the verdict in words ("an
ordinary week", linking to a `<tg-reference>` footnote that holds z and σ, so
the jargon is one tap away and nowhere else), revenue against last week / the
12-week average / last year, then the one sentence that says why with the
lever in bold ("there were fewer orders, and the average check barely
moved"). Bold, never `<mark>`, whose highlight is invisible in the dark
theme. Every headline number sits **beside last week's** — "336 against 433"
is understood by everyone, "▼ 22.4%" by fewer. A collapsed "how to read this
report" is two `<tg-math>` formulas and four lines; the LaTeX is composed in
code from translated words, because braces in the i18n table read as
placeholders.

**Four pictures, drawn with Pillow** (`core/weekly_report_image.py`): the
card, seven grouped bars by day (this week against the *same weekday* a week
earlier, orders under each day), a waterfall (last week → order effect →
basket effect → this week, which closes exactly because the decomposition is
an identity) and one bar per channel (this week filled, last week outlined,
share and change beside). They come from `gold_daily_revenue` via
`fetch_daily`/`fetch_channels` on `WeeklyReport.days/previous_days/channels`;
empty lists render nothing. A chart replaces the table it stands for, or
folds it into a `<details>`. Shares were block-glyph bars in a table once —
Telegram sets table text in the reader's theme colour, so they were
monochrome by construction, and colour now lives only in pictures. Pictures
are rendered **once per language**, not per reader: their labels are
translated, their numbers are not.

**Everything drawn is in the brand's colours.** `core/brand.py` holds the
palette from the brand book (p. 13: bordeaux `#821532`, lime `#dcdf5d`,
off-white `#f5f4eb`, pink `#f7c9df`, plus beige `#e3d4d2` and plum
`#922146`), its contrast rules (p. 14: lime is never text on the light
canvas) and its type (p. 15/46: Libre Franklin for headings in upper case and
body, Instrument Serif for accent headings; left or centre aligned, never
right). The card is the brand's hero surface — bordeaux with a lime chip and
pink labels — and the charts sit on the off-white canvas. There is no red and
no green in the palette, so the arrow carries the sign and the colour carries
the brand. The flower and wordmark are greyscale masks in `assets/brand/`,
tinted at draw time. **Both images carry the fonts and the marks** — `core/`
travels into each, renderers included, so both COPY `assets/` and both apt
`fonts-dejavu-core`; the bot had neither until 2026-09-08 and nothing said
so. DejaVu is a dependency there, not decoration: it is the fallback face
for the arrows, and with the brand faces present but DejaVu absent the
arrows become empty boxes. That case now logs a warning once and still
draws, and `tests/unit/test_brand_fonts.py` parses both Dockerfiles so the
two cannot drift. `brand_font(role)`
finds the brand faces in `assets/fonts/`, where both now ship with their OFL
licences: Libre Franklin as Google Fonts' single **variable** file, so a
weight is an axis and not a second path — `_face()` sets Medium for body and
Bold for headings, and both loaders go through it, because calling
`truetype` directly gave every "bold" chart label the default Regular
instance. The renderers still fall back to DejaVu when the files are absent.

**Neither brand face covers everything, so the drawing is run-based.** Libre
Franklin has Latin, Cyrillic, digits and ₴ but not the arrows `▼▲`;
Instrument Serif is Latin and digits alone, which is why the headline number
is set in it and the ₴ beside it is not. Pillow has no font fallback and
draws a missing glyph as `.notdef` in silence, so `_text`/`_len` split a
string on `brand.FALLBACK_CHARS` and draw those characters from DejaVu at
the same size; where DejaVu *is* the primary face the run is the same file
and the output is identical, which is why it is unconditional rather than a
branch. `tests/unit/test_brand_fonts.py` computes the gap from the shipped
files and fails if `FALLBACK_CHARS` drifts either way. The brand book itself
is outside the repo; see the memory note `reference_brand_book`.

**The shop bot's voice rules apply to what the report says**, where they
translate: one emoji per screen and it is a pointer (the verdict's ✅/🚀/⚠️,
never the heading), no long dash inside a sentence, headings in upper case.
Pinned in `tests/unit/test_rich_report.py`, which also pins every tag the
report may use — anything new goes through the "Rich HTML style" section of
the Bot API docs first.

The card (`core/weekly_report_image.py`) carries revenue against the week
before, then orders, basket and the new/repeat split. Nothing else — what the
message already carries stays in the message. Pillow only, no matplotlib,
drawn at ~2× display size because Telegram re-encodes photos as JPEG. It needs
`fonts-dejavu-core` in the image (`Dockerfile.web`): `python:3.14-slim` ships
no fonts, and DejaVu is the one carrying both Cyrillic and ₴. No font, a
caption over Telegram's 1 024, or any render failure costs the picture and
nothing else — the text still goes out.

Preview any of it on a real phone with `scripts/weekly_report_preview.py`,
which honours the kill switch and writes nothing to the send ledger;
`--fixture` renders the 31.08–06.09.2026 week from Gold rows copied out of
Postgres, for a laptop whose DuckDB copy is stale.

**The instance signature is admins-only** (owner's decision, 2026-09-07).
`· prod-vps` answers an operator's question and only an admin can act on the
answer; the weekly report and the milestone broadcast reach every approved
user through the same transports, and to them it is a stray line under a
sales figure. `sign_for(text, chat_id)` and `sign_html_for` decide per
recipient inside the transports, so a business message and an alert share one
call and the sender chooses nothing. The caption budget is still measured
against the signed form: one verdict per send, never a picture for the admins
and text for everyone else.

### The other weekly message: where the orders came from

`core/traffic_report.py`, job `traffic_report`, daily 09:45 Kyiv. The sales
report answers "how much"; this answers "from where", and they are two
messages on purpose — burying attribution under a revenue headline is
attribution not being read.

**It reads through the repository the tab reads**, `get_traffic_analytics`
and `get_traffic_utm_campaigns`, with the tab's own `sales_type="retail"`.
Same question, same answer: a message that disagreed with the screen would
cost more trust than it delivers.

**No spend, no ROAS** (owner's call, 2026-09-08). The tab has a ROAS block,
but ad spend is not in KeyCRM and is typed in by hand, so a weekly figure
built on it would be as fresh as somebody remembered to be.

**Attribution quality is a headline, not a footnote.** Every share in the
message is a share of what could be attributed, so the orders that could not
be are what says whether the rest is worth reading. The summary's second
sentence is that share — `named_pct`, the orders whose **campaign** this
system can name, which is the `paid` bucket and only it, because a campaign
comes from `utm_campaign` and everything else is placed by inference. It is
stated **every week rather than above a threshold**, always beside what it was
the week before: a share that doubled matters more than the share itself, and
a threshold would have said nothing on the week it moved from 13% to 21%.
There is no `UNATTRIBUTED_WARN_PCT`; an earlier draft of this section named
one and it was never built.

**Orders with no attribution at all is a deferral, not a finding.** The UTM
rows land behind the orders, so a week whose attribution has not arrived
looks exactly like a week where nothing came from anywhere. It reports
`no_attribution` and tries again tomorrow.

**Admins, plus a named list.** "Everyone who can see the traffic tab" was the
obvious audience and the wrong one: measured 2026-09-08, seventeen of the
eighteen approved dashboard accounts hold that tab, because it is in the
default set for both `viewer` and `editor`. Narrowing later is a change of one
list; widening after a wrong number went out is not. Dashboard accounts are
reachable in any case — `app.dashboard_users.user_id` **is** the Telegram id,
since the dashboard signs in through Telegram.

So the widening is **written down rather than derived**:
`KS_TRAFFIC_REPORT_RECIPIENTS` is a comma-separated list of Telegram ids,
added to the admins by `audience()`, **empty by default** — nobody is put on a
weekly message to their phone by a code path that guessed. A curator can be
added without a deploy, and the choice stays a sentence somebody typed. A
malformed entry is dropped with a warning rather than raising: one bad
character must not cost everybody else their report. Language follows the
system rule with nothing to configure — Ukrainian unless the reader is an
admin or has chosen otherwise in the bot — so the send splits per language,
not per reader.

**`KS_TRAFFIC_REPORT_FIRST_WEEK`** names the earliest week the report may
deliver, as the Monday that week starts on, inclusive. A report that ships
mid-week finds last week complete and unsent and delivers it the next
morning — a week that ended before the report existed, arriving on a day
nobody expects a weekly message. The ledger cannot express "skip this one":
it records deliveries, marking that week delivered would be a lie in the one
table that answers whether a week went out, and backdating it is not
available anyway while DuckDB is held open by the running process. Checked
**before** the ledger, because nothing about that week changes by tomorrow,
and skipping a week does **not** record it as sent — the guard defers, it
does not consume. Unset means no floor, which is right for a report already
running; this exists for the first week of a new one. Set to `2026-09-07` in
production, so the first delivery is Monday 2026-09-14.

This is the **only** `chat_ids` override in the repository that can widen an
audience rather than narrow one, and `tests/unit/test_alert_recipients.py`
says so at the site of the guard.

Same shape as the sales report otherwise: last complete Monday–Sunday week, a
daily tick against its own ledger (`traffic_report_sends` — its own table,
because `weekly_report_sends.sales_type` means a sales type and "traffic" is
not one), the rich form with plain text under it, campaigns ranked by hryvnia
moved rather than by percent, and one picture — the sales report's channel
chart fed platform totals, so the two reports do not grow two visual
languages for one idea.

### The conversation must always be re-enterable
`/report`, `/search` and `/settings` — and the reply-keyboard buttons standing
in for them — are ConversationHandler **entry points**, and entry points are
only checked for users who are *not* already in a conversation. With
`allow_reentry=False` (the default), picking a report type and walking away
parked the user in `SELECTING_DATE_RANGE`, whose handlers are all
`CallbackQueryHandler`s: a text button matched nothing, no entry point could
fire, and "⚙️ Settings" reached nothing at all until `/cancel`. "ℹ️ Help" and
"📈 Dashboard" kept working the whole time because they are registered outside
the conversation — which is why it presented as one broken button.

`allow_reentry=True` and `conversation_timeout` are both set now. Do not remove
either; `tests/unit/test_conversation_reentry.py` pins them.

### Languages
`core/i18n.py` holds every translated string for three languages (en/uk/ru) in
one dict — no gettext, no extraction step, no .po files to drift. A missing key
or language falls back to English rather than raising: one odd line in a
delivered report beats no report. A test asserts every key carries every
language *and* the same `{placeholders}`, which is the failure a hand-written
table actually invites.

Dates are rendered numerically (`18.05 – 24.05.2026`) on purpose. Ukrainian and
Russian month names inflect — a date takes the genitive — so a table of
nominative month names is a table of wrong ones.

**Default: Ukrainian for everyone, English for admins** — one function,
`core/bot_prefs.default_language_for`. A stored choice overrides it for good,
in the interface and in the report alike. Telegram's `language_code` is
deliberately not consulted: the default is a decision about this company, not
about a phone's locale.

The choice lives in `user_preferences.language` in the bot's store, set from
the bot's settings screen. The weekly report runs in the **web** container and
reads it through `core/bot_prefs.py`, which asks the bot store port
(`get_bot_store()`, the same `KS_BOT_STORE` engine the bot writes) — never the
SQLite file directly. It did open `data/bot.db` until 2026-09-06, which was a
frozen copy from the day the bot moved to Postgres: revoked users kept
receiving the report and newly approved ones never did. An unreachable store
means one thing there: fall back to the admins and the default language, and
the report still goes. The job renders once per distinct language, not once
per reader.

**Everything user-facing is translated**: the weekly report and its card, and
the whole bot — `/start`, `/help`, the report builder and its date picker,
search, settings, the access-request flow and the admin panel. Handlers resolve
the reader's language at the point of use with `_lang(update)` rather than
threading a parameter through forty signatures.

**Reply-keyboard labels and their matchers come from the same table.** Telegram
sends a tap back as plain text, so `bot/main.py` builds each `MessageHandler`
filter with `button_filter(key)` over `all_translations(key)`. Never write one
of those labels into a regex by hand — a mismatch is a button that dies in
silence, which happened once over a single U+FE0F. A test draws the keyboard in
every language and feeds each label back through the handlers.

Month names exist in the table (`month.1`…`month.12`) for **standalone labels
only** — picker buttons, which are nominative. Dates stay numeric, because a
month after a day number takes the genitive in both Slavic languages.

**No flags in the language picker.** This is a Ukrainian company; a Russian
flag in its internal tooling is not a neutral act. Languages have names.

### Anything that accumulates declares its bound in the same change

Six separate things on this host grew without a limit and were each found by a
watchdog rather than declared at creation: the WAL archive (nine days of
segments with no base backup to replay them onto), anonymous Docker volumes
(173, from `docker rm` without `-v`), journald (3.9 GB, no `SystemMaxUse`),
pulled images (189 of one repo, one in use), the build cache, and `/var/log/btmp`
(397 MB of failed logins, still unbounded).

None was a bug in logic. The bound is simply never the job of the change that
creates the thing — you add `pg-receivewal` thinking about RPO, you tag images
`sha-<commit>` thinking about traceability — so it becomes somebody's later
problem, and "later" is a page at 03:00.

**Bind the bound where the thing is created.** `docker rm -v` in the gate that
starts the container; `--max-used-space` in the gate that fills the cache;
retention inside `pg_basebackup.sh`, which is also the only moment a WAL
deletion is provably safe. A cron that sweeps somebody else's mess is the
version of this that rots, because the sweeper does not know what the creator
meant to keep.

**Age is never the retention key here.** It is always tempting and it has been
wrong every time: deleting WAL older than N days removes segments a retained
base backup still needs; `docker volume prune` takes named volumes; old
`rollback-step03` is the tag you want most. Anchor retention to something that
means something — the oldest base backup's START WAL, what is running, a count
per repository — never to a date. The diagnostic agent recommended the age
form twice in one week and both would have destroyed a recovery point.

**A bound written into one script is not written into its siblings.**
`gate_with_stores.sh` got `docker rm -v` on 2026-09-09 and the test pinning it
named that file. `quick_gate.sh` runs the same `postgres:17.2-alpine`, removes
it the same way, and runs far more often — it kept leaking one 49 MB volume per
invocation for five days behind a green suite, and the diagnostic agent found
it on 2026-09-14 (65 dangling volumes, 3.52 GB) before the test did. The guard
now walks `deploy/`, `scripts/` and `.github/workflows/` instead of naming a
file, which is the same lesson as the mirror-spec guard: **a guard that names
its subjects only ever guards the ones you were already thinking about.**

The rule it enforces is flat because `-v` is safe everywhere: it removes
**only** anonymous volumes, so a named volume and a bind mount are untouched.
A script that mounts its own directory over the image's `VOLUME` — the PITR
drill, the weekly compact — loses nothing by carrying the flag and gains the
bound the day somebody drops the mount. An exception list here would be a list
of the places the next leak is allowed to appear.

**A one-off cleanup of something that rebounds manufactures the next alert.**
The disk watchdog differences at a fixed 168h lag, so emptying a cache that
returns to its natural size reads as growth a week later. The WARN standing on
2026-09-13 was made by a `docker builder prune` on 09-09, not by anything new.
Cap what rebounds; only delete what stays deleted.

### A killed DuckDB writer, and what the next start used to lose

DuckDB 1.5.5 drops index entries across a kill. The rows a killed writer left
in its WAL are replayed into every `CREATE INDEX` index (not the PK/UNIQUE
ones) as entries the index has not bound yet, and DuckDB's **own** checkpoint
— `close()`, or `wal_autocheckpoint` — writes those indexes without them.
From then on `WHERE col = ?` misses rows a scan still sees, and a write that
must take such a row out of the index is a FatalException ("Failed to delete
all rows from index") that invalidates the whole instance. Measured through
`DuckDBStore.connect()` then `close()`: 45 of 45 single-column indexes short,
`DELETE FROM t` a FATAL on 26 of 26 indexed tables, composite ones included.
Every OOM kill and every stop that outlives its grace is such a kill.

**The fix is a CHECKPOINT before anything else runs.**
`core.duckdb_switch.open_file` is the one read-write `duckdb.connect` of the
analytics file, and its first statement on a read-write open is `CHECKPOINT`,
which keeps every replayed entry. It is also the application's one opener
for the week of silence (`KS_DUCKDB`, below), whose refusal it runs first,
before the driver. The two guards were built apart — this one as
`core.duckdb_store.open_read_write` — and each walk demanded its own function
be the only opener, so they were made one at the stage-4 integration: neither
walk can now be satisfied by an opener the other does not see, and both name
the same `OPENER`. First, not after the SETs: a SET that fails leaves a valid
instance, and closing a valid instance is the lossy checkpoint. A guard
checkpoint that fails inside DuckDB is FATAL, so nothing is written on close
and the WAL waits for the next open. One that is **interrupted** is not —
`InterruptException`, or Ctrl-C in a CLI opening through the store, leaves a
valid instance and the WAL unapplied (measured on a 58 MB WAL), and closing
that is the lossy checkpoint again. `PRAGMA disable_checkpoint_on_shutdown`
on every failed exit is what stops it, and every failed exit closes: an
instance left to the exception's traceback holds the file's lock against
every other process. Free on a clean start; behind a kill, 1.5 / 3.6 /
8.9 s at 30 / 300 / 900 MB of WAL — the checkpoint the restart would have
taken anyway, moved to the open. **The store's memory limit and spill
directory are given to the open** (`open_file(config=...)`), not SET after
it: the open replays the WAL and the guard checkpoints it before any SET can
run, so as SETs they bounded neither, and both ran under DuckDB's default —
80% of detected memory, ~5.6 GB in web's 7 GB container — after exactly the
kill that leaves a large WAL (batch-E review; not measured at production WAL
sizes). A limit DuckDB refuses there raises before any instance exists, the
WAL whole for the next open. DuckDB refuses a second in-process open of one
file under a different configuration; only the store opens the live file in
web. A WAL found at open is logged as a WARNING,
so every kill is named. Read-only opens replay into memory and answer
correctly; they neither lose entries nor take the guard.

`tests/unit/test_duckdb_open_guard.py` walks `core/ web/ bot/ scripts/
deploy/`, not a list of callers: every `duckdb.connect` is read-only,
in-memory, the opener, or `compact_duckdb.py::phase3_validate` (the file
phase 2 just built and closed — no WAL — in a do-not-touch script),
including one written out in a string a module runs elsewhere (`python -c`,
a subprocess), which the AST never sees inside — the step-13 rehearsal's D1
DELETE hid there, merged in after the walk was written. Such a program is
read the way a module is when it parses, so the driver imported under an
alias and connected through the alias does not pass where the first form,
which searched for the literal call, let it through, and
the week of silence's walk reads it too: it runs in another process, past
the in-process count, and a read-only open changes no byte the hash sees
(batch-E review); the
`duckdb` module is never handed on as a value; and every ATTACH of a database
file carries the READ_ONLY *option* — parsed, because `AS read_only_copy`,
`(READ_ONLY false)` and a comment saying READ_ONLY all attach read-write, and
read through `%` and `+` as well as f-strings. It also reads every document
in the repository (Markdown, shell, YAML, SQL, Dockerfiles; not `.planning/`
or other worktrees) for a `duckdb.connect` call, an ATTACH and the `duckdb`
CLI, under the same rules: the analytics agent's instructions told it to open
`data/analytics.duckdb` with a bare read-write connect, which on a killed
writer's file is the lossy restart itself. (So this file, too, never writes
such a call out.) `test_duckdb_kill_guard.py` SIGKILLs
a child that wrote through `DuckDBStore`, and also pins **the defect without
the guard**: if that half fails after a DuckDB upgrade, re-measure before
deleting the guard — never just the test.

**web gets 120 s to stop** (`stop_grace_period`; Docker's default is 10 s):
uvicorn's drain (30 s, handlers run on past their 504), DuckDB work a
cancelled job left in flight (~30 s, a whole warehouse refresh), `close()`'s
checkpoint (~7 s at 900 MB) — ~67 s for an ordinary request. **Not for a slow
one**: the eight `SLOW_ENDPOINTS` (`web/middleware.py`) get 300 s, uvicorn
drains them too, and five write DuckDB (`/api/duckdb/resync`,
`/api/duckdb/refresh-statuses`, `/api/traffic/reclassify`, and — until chain
7b-3's `KS_WRITE_FORECAST=postgres` moves their tables — `/api/goals/recalculate`
and `/api/revenue/forecast/train`). The list was kept by hand and named three;
it is derived from the code now (`test_web_stop_grace.py` walks each handler
to a store method executing a write). One in flight at a stop is killed. Covering them (~340 s) would leave a deploy — pull, stop,
migrate, the 180 s health gate — no margin inside its 10-minute ssh timeout,
so the grace makes kills rarer and `open_file`'s checkpoint is what makes
them harmless. `test_web_stop_grace.py` pins both halves, the endpoints by name.
`weekly_compact.sh` keeps its `--timeout 30`; harmless now.

**`close()` is final.** Shutdown closes the store while a handler past its
504 or a job past its cancellation may still be queued on its lock. Such a
caller used to find no connection, take it for one a FATAL had dropped, and
open the file again — the schema and the migrations during shutdown, a write
after `close()`'s checkpoint, and an instance nothing would ever close. It
gets `StoreClosedError` now; only an explicit `connect()` reopens a store.
`get_store()` after `close_store()` still builds a new one — a new lifespan
and the tests rely on it — so a caller that asks the singleton afresh during
shutdown is not stopped; one holding the store is.

**A FATAL drops the instance instead of leaving web's DuckDB dead.**
`connection()` reconnected only when there was no connection, so one FATAL
failed every later use, the caller queued on the lock included, until a
restart. Now, whenever a block raises, the instance is asked (`SELECT 1`);
one DuckDB invalidated is dropped and the next use opens the file again —
through the guard, since an invalidated instance writes nothing on close and
leaves its WAL. Asked, not read off the exception: a caller may swallow the
FATAL and raise something else, and a FatalException can come from another
instance. **It does not heal**: an index short in the file fails the same
write after every reconnect — in the sync, an order batch every tick. So
`/api/health` publishes `duckdb.fatal` (`count`, `kinds` — `index` or `other`
— `last_at`; never the text) and `status: degraded` until web restarts;
otherwise the next read answering would resolve the `health_status` page over
a write that still fails. The existing page, not a new canary key — **but
named for what it is** (batch-E review): it read "Dashboard DOWN",
"status=degraded" and "a restart won't fix a migration" while web answered.
`/api/health` says why it is degraded (`degraded_by`: `duckdb`, `migrations`,
`duckdb_fatal`), and when the FATAL is the only cause the canary pages
`health_status` as **"DuckDB index short"** (CRITICAL, lever: the compaction)
for an index, and as a WARN for any other FATAL (the reconnect answered past
it; lever: web's log). **An index FATAL outlives the restart**: `_fatal` is
the process's memory and the damage is the file's, so a restart announced a
short index healed while the same write still failed. It is written down
beside the file (`data/.duckdb_index_short.json`, under the file's device and
inode) and published as `duckdb.fatal.index_short` until a compaction swaps
a new file in. A restore that copies over the file in place keeps the inode:
delete the marker then.

**DuckDB prints the rows it could not remove** — every column, so a buyer's
name, phone and email — after `\nChunk:` in the FATAL, and again in every
"invalidated" echo that quotes it. The store's own line was cut from the start,
but the exception went to the caller whole, and the buyers step logs a failed
write with its traceback, on every retry while the index stays short. Now
`connection()`, `checkpoint()` and `connect()` cut the dump out of what leaves
them, the whole exception chain included
(`core.observability.cut_row_dumps`); web's log handler (`RowDumpFilter`,
installed by `setup_logging`) cuts it from any record, for a handler that logs
inside its own block before the exception reaches the store's exit; and a
migration's or a view's error text — published on `/api/health`, which is
public — is cut where it is recorded. Not covered: text an in-block handler
writes anywhere but a log, and a script that logs through its own handler
(scripts still get the cut exception).

**A cancelled read waits for its thread.** `_fetch_one`/`_fetch_all` run the
query on the executor thread under the store lock. A timeout interrupted the
query and waited for the thread; a cancellation — the scheduler at shutdown,
a caller giving up — released the lock with the thread still inside
`conn.execute()`, and the next block that raised made the probe above wait
for that query on the event loop (2.2–2.9 s of every request stalled,
measured). Both now interrupt and wait (`_offload`); the probe never runs on
a cancellation. An aborted transaction is not a FATAL: the probe's
`TransactionException` keeps the instance.

The lever for `index` is rebuilding every index, which the compaction does —
Sunday's, or `weekly_compact.sh` by hand; a restart clears neither the damage
nor, since the marker, the status. Rebuilding automatically was measured and **not** built (3–3.5 s,
≤394 MB, DDL at startup, the file growing once by the index footprint) — the
owner's call. Whether production holds damage from before the guard is
unknown; the first compaction after it ships clears whatever there is.

**Not fixed: a WAL that cannot be replayed at all.** On 1.5.5 a WAL holding a
column-level `ALTER TABLE` — add, drop or rename a column, change its type,
set or drop a DEFAULT or a NOT NULL — on a table that, after it, has a column
DEFAULT calling a function (`nextval(...)`, `CURRENT_TIMESTAMP`, `now()`,
`uuid()`: nearly every table here, and an `ADD COLUMN ... DEFAULT nextval(...)`
makes one) fails every open, read-only included, guard or not: `INTERNAL
Error: Failure while replaying WAL file … GetDefaultDatabase with no default
database set`. Neither web nor the compaction opens the file until the WAL is
removed, and its contents with it. First seen as "a brand-new file killed
before its first checkpoint" (the first start creates `warehouse_refreshes`,
then ALTERs it), but production is exposed too: the first start after a
deploy that alters such a table, until the hourly checkpoint. What replays:
renaming the table, dropping the function DEFAULT itself, `CREATE INDEX`,
DML, and an `ADD COLUMN IF NOT EXISTS` of a column that exists (it writes
nothing). `test_duckdb_kill_guard.py::test_which_wal_cannot_be_replayed`
pins each case. Candidate fix, as a separate change: a second CHECKPOINT at
the end of `connect()`.

### How a failure reaches a human
- **`KS_ALERTS_DISABLED=1` глушит весь исходящий Telegram** (алерты, дайджест,
  watchdog'и, недельный отчёт, хендлерные уведомления, host-cron скрипты) —
  рубильник dev-инстансов: ноутбук на копии прод-бэкапа дважды слал админам
  «прод»-тревоги о самом себе. Прод не ставит переменную; локальный .env —
  ставит. Подавление тотально и громко. До 29.08 у заявления «весь» было
  пять дыр (memory monitor, два хендлера, четыре shell-скрипта) — закрыты
  шагами 00 и 07 переработки алертов; shell читает рубильник из .env через
  `deploy/notify.sh`.
- **Третья рука сверки** — `reconciliation_ch` (внутри `dq_reconciliation`):
  клик против того же снапшота KeyCRM, ноль лишних вызовов API. Шапочное
  зерно (silver без позиций), вотермарочное исключение считается в PG bronze
  (у клика нет `updated_at` — без этого ~1 400 force-переписанных в 05:15
  заказов были бы фантомами), копия старше 3 ч гейтится
  (`ch_reconcile_pending`), а не обвиняется — и пишется прогоном без
  вердикта (`error_message`), не успехом: до ревью OD-08 такой прогон
  сбрасывал возраст слоя, и лежащий ClickHouse пейджился не больше одного
  раза.
- **Агент-диагност** (трек Б): доставленный свежий инцидент кладёт задачу в
  `data/alert-tasks/pending/` (sticky 1777 — пишут контейнеры, дренит root);
  systemd path-unit на хосте запускает headless `claude -p` с ранбуком
  семейства (`deploy/agent/runbooks/`), инструменты — только чтение (docker
  logs, psql под SELECT-only ролью `ks_readonly` через local trust, curl,
  df/du/stats/free); бюджет 10/день, таймаут 5 мин. Диагноз — вторым
  сообщением через `deploy/notify.sh` и целиком в журнал
  (`event_type='diagnosed'`). Логи для агента — недоверенные данные; граница
  — allowlist, не промпт. bucket-less эмиттеры зовут его через
  `raise_alert(spool_as=...)`.
- **Память — по контейнерам**: ключи `memory:web:*` и `memory:bot:*` (лимиты
  7g и 512m — один ключ врал бы, чей cgroup голодает); кулдауны WARN 24ч /
  CRITICAL 6ч — задокументированное pre-OOM исключение из суточного
  затухания Gate; OOM не троттлится никогда и спулится агенту с
  `conditions=[]`.
- **Формат сообщения — три строки, английский**: человеческие слова +
  главное число (`Mirror check: rows differ between copies
  (mirror_row_values) — 891`), ≤3 деталей, одно «→» с однострочным
  императивом (REMEDIATION). Обоснования — здесь, описания — в журнале,
  возрасты — в /api/health, деталь приносит агент вторым сообщением.
  Правка владельца 30.08; «коротко» ≠ «по-русски» — язык шаблонов не
  менять без явной просьбы.
- **Получатели алертов — только админы**, запинено
  `tests/unit/test_alert_recipients.py`: chat_ids-override есть лишь у двух
  отчётов; алерт-модулям запрещён импорт списка пользователей.
  Бизнес-исключения: недельный отчёт продаж и веха-рассылка. С 08.09 к ним
  добавился **отчёт по трафику**, и он единственный, чей override
  *расширяет* аудиторию: `KS_TRAFFIC_REPORT_RECIPIENTS` — явный список
  Telegram-id поверх админов, пустой по умолчанию, применяется одной
  функцией `core.traffic_report.audience`. Правило не «нельзя расширять», а
  «нельзя расширять молча»: список пишет человек, а не выводит код из прав
  на вкладку.
- **Антишум**: health_*-блип канарейки ждёт второго зонда (`defer_flaky` —
  ежедневная 4.5-мин UTM-заморозка 05:15 и пересоздания при деплоях);
  «🫀» офсайта — еженедельно по пн; test:/deploy-test: бакеты не вызывают
  агента.
- **Агент-диагност**: см. блок выше; ранбуки знают, что «→»-строка — наш
  шаблон, не инъекция.
- **Условия и жизненный цикл**: словарь — `core/alerting.py` (REGISTRY,
  ~105 ключей, condition/event, exact-match; тест полноты вычисляет
  эмиттируемое множество из AST). Политика — AlertGate: громкий час (30 мин),
  затем суточное напоминание со стажем; кулдаун платится только доставкой;
  час тишины = новый инцидент. Журнал — `app.alert_series`/`alert_events`
  (ревизия 0012, доступ МИМО require_revision, fire-and-forget ≤1с, повторы
  счётчиком). Resolved — `resolve_group` на здоровом проходе эмиттера,
  только после доставленного fired. Эскалатор — в боте на тике канарейки:
  горящее+неузаконенное 6ч → один повтор за цикл, стоит при мёртвом web.
  Хвост дайджеста — из журнала; дельта находок осталась на
  data_quality_issues. Канарейка с шага 07 тоже на Gate (CanaryState умер).
- **Alert throttle** keys on the *condition* (`warehouse:validation_retrying`,
  `dq:{layer}:{severity}:{checks}`, `canary:{failing keys}`), never on message
  text — the validator embeds live checksums, so 3 119 failures once produced
  404 "identical" messages.
- **Daily digest** (`dq_digest`) is the only route by which WARN findings reach
  anyone. INFO findings ride along but never summon a digest on their own.
- **Run-age watchdog**: `/api/health` publishes the age of the last successful
  run per layer, keyed on `error_message IS NULL` — a failed run writes a row
  too, so row-existence alone reads green. `bot/canary.py` judges it from the
  *other* container every 15 min: 30 h for reconciliation, 12 h for integrity,
  30 h for mirror_landing, and since DN-21 30 h for
  reconciliation_pg — the Postgres half of the 05:30 job, and the comparison
  against KeyCRM meant to outlive DuckDB. It does not yet: the job runs it
  only once the DuckDB extraction has succeeded, and journals it in DuckDB,
  which is where its age comes from — so a DuckDB failure silences it too
  and, since DN-21, pages under both keys. Decoupling it belongs with step 13.
  A missing block or a layer that never succeeded both count as failures.
  `reconciliation_ch` joins them at 30 h since OD-08 (a) (2026-09-30), which
  made ClickHouse required for the parallel period: same job, same dependence
  on the DuckDB extraction, and a web without `KS_CH_URL` never writes it and
  pages `dq_never:reconciliation_ch`. A copy too old to reconcile (over 3 h)
  is gated, and written with `error_message` beside its `ch_reconcile_pending`:
  it compared nothing, so it moves no age. Written as a success, as it was
  until the OD-08 review, it reset the age every morning ClickHouse stayed
  down, and the page was announced resolved with ClickHouse still down. The
  history that watch rests on is `deploy/stage4_soak/22_reconciliation_ch_history.sql`,
  DN-21's bar for `reconciliation_pg` (20) applied to this arm, read the way
  the canary now reads it.
- **Every message is signed with `KS_INSTANCE`** (default `gethostname()`,
  `prod-vps` in compose on both services). Applied by the two HTTP transports
  and by the bot's Application path — three call sites, so nothing that merely
  *builds* a message has to remember — and idempotent, so the bot's fallback
  to HTTP cannot sign twice. Plain text, no markup: HTML and Markdown both
  ride this channel and a transport cannot know which. The photo caption is
  signed **before** the 1 024-char check, or the signature would silently cost
  the weekly card its picture.
- **A CRITICAL names its lever.** `REMEDIATION` in `core/data_quality.py` maps
  check name → what to do, matched by **longest prefix** because half the
  names are generated (`fk_orphan_*`, `freshness_*`). The canary, the disk
  watchdog and the three warehouse-validation alerts carry theirs inline.
  Anything unlisted gets an anchor to this section — which is the signal to
  add one.
- **The alert says what the machine already tried**, and still never triggers
  a repair. `machine_attempts_note()` reads the two ids-diff heal ledgers
  (24 h window) and is *passed into* `format_alert_message`, which stays pure
  but for one mode cached at boot: `KS_UTM_PARSE` picks which lever line the
  `pg_order_utm_*` findings carry (`REMEDIATION_UTM_PARSED_IN_POSTGRES`).
- **Mirror freshness**: `/api/health` publishes `mirrors` — `last_ok_at` age,
  `failures_since_ok` and a boolean `failing` for `bronze.orders`. The error
  *text* is deliberately not published; this endpoint is public and the text
  is a driver exception. Both numbers are needed: `mirror_orders` refuses to
  move the watermark when there was nothing to ship, so age alone cannot tell
  a quiet night from a dead mirror — measured, the orders watermark stands
  still from ~01:00 Kyiv to the 05:15 status refresh and again to ~07:00, so
  the canary's limit is **8 h**, not the hour first sketched. What makes 8 h
  affordable is `failures_since_ok`, which catches an actively failing mirror
  on the next sync tick. ClickHouse rows are **not** watched — that store's
  daily window of silence was accepted when step 4 shipped.
- **The suite is hermetic against the kill switch**: `tests/conftest.py`
  assigns `KS_ALERTS_DISABLED=0` (assigns, never pops — `load_dotenv` only
  declines to overwrite a name that is *present*). What stops tests reaching
  Telegram is the autouse fixture, never that variable.

### The buyers step, before chain 4 moves its writes

Chain 4 of the DuckDB exit will make the buyers step of the incremental sync
the **only** writer of buyers, contacts and gender. Its preparation (PR-1)
changed nothing about which store is written, and four things about how:

- **One parse** (`core.landing_rows.parse_buyers`) feeds both stores. A
  birthday reads exactly as DuckDB 1.5.5's CAST reads it — 80 shapes pinned
  against DuckDB itself — and one it refuses becomes NULL, logged by id,
  instead of rolling back the batch. NUL is stripped (Postgres refuses it); a
  blank or NUL-only name is `'Unknown'` (an empty name loops the selection).
- **The step cannot take the tick down.** It never raises; a failure holds the
  watermark and waits ten minutes, during which it asks neither store nor
  KeyCRM anything, and offers and stocks run regardless. A KeyCRM outage used
  to read as "nothing to fetch" and was stamped as a success; it is a failure
  now. `/api/health` publishes `buyer_sync` (ages, count, error *class*, never
  text), and the canary pages `buyer_sync_stalled` (WARN) on three failures in
  a row or no success for 90 minutes — naming "step not reached" when the
  whole tick stops before it.
- **Every buyer write mirrors, from one place**: `DuckDBStore.upsert_buyers`,
  in portions of 1 000, each committed then shipped outside the store lock. A
  portion whose ship failed is retried once at the end and then raised — the
  next portion's success would otherwise erase the trace. `sync-all-buyers`
  now mirrors too, and runs **detached** (the answer is `started`; the result
  is logged as `Full buyer sync:`). **Do not run it to see whether it works**:
  it is the only path fetching with `include=loyalty,shipping`, so it fills
  city and region for every buyer and moves the SMS city filter.
- **`reconcile_buyer_completeness`** (in `dq_mirror_landing`, Postgres alone):
  `buyers_without_verdict` — landed over 90 minutes ago with no verdict of the
  current rules — and `buyers_missing_for_orders` — named by an order over a
  day old and never landed. Running with the flag off is the point: its false
  positives are measured before the flip. It read zero of each on 2026-09-24.

PR-2 made the way back ready, still with no chain registered and nothing
routed differently:

- **One writer of buyer rows**, `core.pg_buyer_rows._write_buyer_rows` — the
  mirror and, since PR-3, the chain both run it. The rows go on the caller's
  transaction (the chain proves its first write by the owner row sharing an
  `xmin` with the row) and it never stamps the buyers' rows of
  `meta.mirror_state`, which stay the mirror's — a dropped derivation mark is
  recorded there, but under `meta.derivation_signal`, `core.pg_derivation`'s
  own row, on a connection outside the caller's transaction. It lives
  outside the chain module on purpose: the registry walk derives
  `BUYER_UNIT` from the function that writes it and skips chain modules.
- **`app.buyer_gender.decided_at` is compared** every morning (decision 7):
  the hourly derive restamps only the verdicts it writes and the copy ships
  the value as it stands. Per row now, so a re-derive is forgiven for the
  90-minute grace and no longer.
- **`buyer_contacts` left `snapshot_validation.MONOTONE`** for `MUST_BE_NONEMPTY`
  and a `BOUNDED_SHRINK` tier at 1%: its writers replace a buyer's contacts
  whole, so a small fall is a dropped number and not a loss — but an export
  that falls by more than 1% (~330 rows at 32 700), or to zero, is rejected,
  including after a copy-back that writes back noticeably fewer contacts.
- **The copy-back knows mirrored tables** — see "A write flag is no longer a
  rollback" — and `POST /api/mirror/backfill/buyers` is the lever its
  pre-flip handover names.

The chain itself, its rollback under the DN-06 latch and the owner's decisions
are in `.planning/DUCKDB_EXIT_CHAIN4_PLAN.md`, revised against today's main in
`.planning/DUCKDB_EXIT_CHAIN4_PLAN_REV2.md`.

### Chain 4: the buyers, written where they are read

`core/pg_buyers_write.py` is the fifth registered write chain (PR-3):
`bronze.buyers`, `bronze.buyer_contacts` and `app.buyer_gender`, moved together
by `KS_WRITE_BUYERS=postgres`. **Off in production.** The flip needs the
owner's own yes, not before 2026-09-30, Tuesday to Thursday 10:30–12:30 Kyiv.

**Held on DuckDB until its readers follow.** Three readers show buyers — the
SMS audience (`KS_SMS_STORE`), the search index (`KS_READ_SEARCH_INDEX`) and
the dashboard's buyer reads (`KS_READ_DASHBOARD`) — and each could still read
DuckDB, whose buyers stop the moment the chain writes Postgres.
`unmet_precondition()` names any of the three not on postgres (a typo in one is
unmet), and until all are an unlatched chain runs as duckdb whatever its flag
says: DN-26's hook, made generic in the registry for this chain. Production had
all three on postgres on 2026-09-25, so the hook is met there and changes
nothing. The warning is rate-limited to once an hour per reason — this chain's
key is read every incremental tick — and `/api/health` publishes it on every
read, where the canary warns `write_chain_precondition_unmet`.

**Every buyer's verdict is written with its name.** `upsert_buyers` classifies
the names it writes and writes the verdicts in the same transaction, in a
savepoint caught on `asyncpg.PostgresError` only, so the buyers land whatever
the gender does — and when that savepoint fails, the portion's verdicts are
marked stale (`rules_version = 0`, never over a human's) so the hourly
derivation re-decides them: a renamed buyer's old verdict at the current
version is one nothing would select again. `derive_gender_pg` (the hourly rider, and
`scripts/backfill_gender.py`) reads what is pending **before** it latches — an
hour with nothing to decide takes nothing — then share-locks the portion's
buyers and drops a verdict whose name moved. That closes a race the hourly
derivation alone would leave open: a derivation that read an old name and
wrote after a rename committed would store the old name's verdict at the
current rules version, and nothing would ever select that buyer again. Proved
by running it: without `FOR SHARE` the old verdict lands. A human override is
never overwritten; an equal verdict is not rewritten, so `decided_at` is when
the answer last changed — the web process's UTC clock, passed in, which is the
clock DuckDB's `CURRENT_TIMESTAMP` is and the one the copy-back orders by.
DuckDB never re-derives on a rename; this does, on purpose.

**A buyer Postgres would refuse is skipped, not raised.** Refused before the
latch where it can be seen (`_refusal`: an id missing or outside INTEGER, text
that is not UTF-8 — a lone surrogate — a value of the wrong type, a loyalty
figure over its `NUMERIC` via `core/pg_numeric.py`, shared with chain 7a), and
retried buyer by buyer on a `DataError` it could not foresee. Either way the
id is logged and counted in `buyer_sync.last_skipped_bad`. A batch `_refusal`
rejects whole leaves no marker; one Postgres refuses buyer by buyer has
already latched the chain, and the marker stands without an owner row until
the next write lands — `chain_latch_disagrees` the next morning if none does,
which is why `_refusal` asks everything the driver would refuse. A batch in
which every fetched buyer was refused is **not a success**: the step raises
`BuyersRefused`, a data error, so the watermark and the canary's clock stay
where they were. A cancelled statement is not bad data and raises at once. A
statement timeout and a two-minute deadline — counted from the latch, and
never before the first portion — bound the write.

**Every path asks the chain's one answer** (`pg_buyers_write.mode()`, never
raising):

- `DuckDBStore.upsert_buyers` hands the batch to the chain first, before the
  portion loop — so DuckDB's copy stops and the mirror has nothing to ship.
- `get_missing_buyer_ids` selects from Postgres whenever the chain is off
  DuckDB, before and regardless of `KS_READ_BUYER_SYNC` — a selection from a
  DuckDB that stopped would fetch the same buyers every hour. A latched chain
  with its flag set back routes both the selection and the write to Postgres;
  the copy-back's post-latch levers depend on exactly that, and one test pins
  the two together.
- A `KS_WRITE_BUYERS` nobody can read writes nowhere: the step refuses before
  KeyCRM is asked, both manual doors answer 409 before starting, the gender
  rider reports `stood_down`, and `get_stats` — `/health/detailed`,
  `/api/duckdb/stats` and the DuckDB block of `/api/health` — leaves the buyer
  counts out rather than show DuckDB's frozen ones; the live count is
  `bronze.buyers` (`pg_buyer_sync_read.count_buyers`).
- `scripts/backfill_gender.py` writes Postgres only for a chain web has
  already latched (exit 4 otherwise): the latch is a marker web reads once and
  caches, so a script taking the first write would leave web writing DuckDB
  until it restarted.
- `reconcile_buyer_completeness` also runs under the chain with the landing
  mirror off — it is the one Postgres check that the writer is alive.

`tests/unit/test_buyer_paths_consult_registry.py` walks the tree for every
DuckDB statement that writes the three tables and requires each path into it
to pass through a function that asks.

**Watched from three sides.** The canary pages `buyer_sync_stalled_chain`
(CRITICAL) when the step has not succeeded for three hours under the chain —
the WARN at 90 minutes stays — judged by the older of the step's own clock and
`buyer_sync.watermark_age_s`, the age of the stamp it writes to Postgres: the
first is floored at web's start, so on its own every recreate reset a stall
and announced it resolved. The standing watch (`core/pg_chain_invariants.py`)
files `chain_buyer_orphan_rows` (CRITICAL, from the handover on),
`chain_required_column_null` for a NULL `full_name` (DuckDB's is NOT NULL, so
the copy-back could not carry it back) and `chain_buyer_contact_missing` (WARN):
all three read zero on production on 2026-09-30. Its watermark is left to the
canary, which pages on the step's own state. `deploy/stage4_soak.sh` reads
B1–B5 — copies stood down, the watermark's stored value at 90 minutes,
verdicts and the override floor, the selection's backlog, integrity — and
knows a fifth state for this chain, `held`: flagged, unlatched and a reader
not on postgres, reported by B1 as the flip that did not move. B1 dates the
handover by the owner rows, then by `SOAK_BUYERS_FLIP_AT`, and with neither
reads UNKNOWN rather than FAIL a stamp inside its 75-minute window. The restore
drill counts all three tables, because once the chain writes them the nightly
dump is their only backup.

**Revision 0034 locks older images out** (PR-4, owner decision 16) and is
deployed directly before the flip. It changes no schema — three table comments
saying who writes each table — and moves `REQUIRED_REVISION` to
`0034_buyer_chain`, so an image without chain 4 refuses the database: its
replication would otherwise full-replace `app.buyer_gender` out of a DuckDB
that stopped, and its mirror ship DuckDB's buyers over the chain's. Two things
it does not do, both written down in the revision:

- **It does not hold images below v3.0.249.** Their buyers mirror and
  backfills never called `require_revision`. None of them is a rollback target
  after this revision, whatever the database says.
- **It is not "no writes" for an image that checks — it is an outage.** The
  session read raises under `KS_USER_STORE=postgres` (401 for everybody) and
  the bot refuses to start. An image-only rollback therefore needs `alembic
  downgrade 0033_derivation_signal` first, run with the new migrate image, and
  then all three images moved back — keycrm-migrate too, or `up -d` re-runs its
  `alembic upgrade head` and puts 0034 back. Only before the flip: after chain
  4 has written Postgres the way back is `scripts/chain_copy_back.py buyers`,
  never a downgrade below 0034.
- **A locked-out image still writes DuckDB.** From v3.0.249 on, a build without
  chain 4 cannot reach the buyer tables but its sync still commits buyers to
  DuckDB before the mirror refuses — and after the flip `--handover` then
  refuses on those ids. After the flip, no image rollback at all.

### Revision 0035: the same lock for the eight chains #280 registered

#280 registered chains 3, 5, 6, 7b-3, 9, 10, 11a and 11b, all off, and shipped
no revision, so a database at 0034 still admitted images 3.0.263 to 3.0.270 —
built between chain 4's lock and #280, and knowing none of these chains.
Image numbers here are what `/app/VERSION` and the Docker Hub tag say; the git
tag of the same code is one higher (`v3.0.249` above is a git tag).
`0035_batch_e_chains` is 0034's shape for all eight at once: twenty `COMMENT ON
TABLE` statements naming each table's chain, `KS_WRITE_*` and writing module,
and `REQUIRED_REVISION` moved to it, so every gated path in an older image
refuses the database.

**One revision, directly before the first of these flips.** The bump refuses
every older image whatever tables it comments, so eight revisions would buy
seven more deploys and seven more windows in which an image-only rollback
needs a downgrade, and lock nothing more. It goes out before the first of the
eight flips (planned from 2026-10-10) and not earlier — owner decision 16's
rule for 0034 — because from the day it lands, rolling an image back below it
for any reason takes the downgrade below.

**What it adds is an outage, not a wall.** In 3.0.263 to 3.0.270 every gated
path that writes one of these tables already stands down on an owner row read
as itself (DN-22a, DN-22b, both older than 0034): the operational copy, the
classification copy, chain 3's backfills, ids-diffs and comment ship. What the
lock adds is that such an image cannot run quietly beside a latched chain —
the dashboard answers 401 under `KS_USER_STORE=postgres`, and the bot, the
canary with it, refuses to start. What it does not hold, written in the
revision:

- **The per-tick landing mirrors never gated**: `upsert_orders` →
  `mirror_orders` → `write_orders` and `pg_landing._mirror` (expenses,
  products, categories). The sync runs without a session, so a locked-out
  image still ships its own copy over chain 3's and chain 6's rows — and the
  comparisons that would file `owner_row_without_marker` and
  `order_owner_row_without_marker` are gated, so its 07:30 run fails on the
  revision instead of naming the overwrite.
- **It still writes DuckDB**: the sync, the checks, the watchdogs, the reports,
  the goal and forecast jobs. After a flip those are rows the chain does not
  hold — `shadow_duckdb_only_rows` for the shadow chains, a handover that
  reports them for the rest.

**The way back below it.** An image-only rollback below the first image built
with 0035 (3.0.273 if it is the next merge to main) needs `alembic downgrade
0034_buyer_chain` first, run with the **new** migrate image — an older one
does not know 0035 — and then all three images moved back, keycrm-migrate with
web and bot, or `up -d` re-runs `alembic upgrade head` and puts 0035 back.
Only before the first of these chains latches: after one has written Postgres,
the way back is `scripts/chain_copy_back.py <chain>`, never a downgrade below
0035. It also locks out 3.0.271 and 3.0.272, which carry these chains — the
cost of a bump, not a hazard.

**Deploying it** is any revision's rule: CI rebuilds `keycrm-migrate` with
`keycrm-web` and the bot, compose runs `migrate` first and holds web and bot on
`service_completed_successfully`, and the workflow checks `docker wait
ks-migrate`. Each statement takes SHARE UPDATE EXCLUSIVE on its table
(measured on 17.2): reads and writes go on — the full replaces of these tables
are DELETE, not TRUNCATE — and it waits only behind a manual VACUUM or ANALYZE,
an index build or other DDL. After step 13's flip the restart re-asks step 13's
preconditions like any deploy's; `migrate` finishing first is what keeps
`pg_revision` met.

**Measured on a throwaway PostgreSQL 17.2**: 0034 → 0035, down to 0034 and up
again, each clean, with all 57 table comments equal to the replay of the
migrations at every step; origin/main's code (0034) refused the 0035 database
in `require_revision` and in the bot store's `initialise()`, and this code
refused the 0034 one. Held by `tests/unit/test_batch_e_chains_revision.py`
(each test names its mutation) and `tests/integration/test_batch_e_lock_pg.py`
(the round trip and the lock on a real server, in a transaction rolled back).
Both read what came before from `tests/migration_replay.py`, which runs every
revision against a recorder rather than restating its text. **And a chain
registered after it fails the first of them**: every registered chain must be
held by 0034 or 0035, so a later one needs a lock-out revision before its
flip, and the test's set says which.

### Chain 3: the orders, written where they are read (off)

`core/pg_orders_write.py` is the sixth registered write chain:
`bronze.orders`, `bronze.order_products`, `bronze.expenses` and
`app.order_backfill_misses`, moved together by `KS_WRITE_ORDERS=postgres`.
**Off in production, and held off by its own preconditions.** Owner decision
OD-13 (a): a **raising** writer. Under the chain a Postgres write *is* the
write; there is no DuckDB fallback when Postgres is down, and the way back is
`scripts/chain_copy_back.py orders`.

**What moves together, and why.** The two order tables are one unit (DN-22a).
The expenses come in the same payload (`include=…,expenses`) at the same call
site, so a page's orders and their costs commit in one Postgres transaction or
neither does. The misses ledger moves because the repair *selections* move: the
gap scan and the half-written scan stop re-asking KeyCRM for an id only through
a 30-day `NOT EXISTS` over it, and a ledger read where nothing writes it stops
nothing. `app.order_versions` is **not** a chain table: it has no DuckDB copy,
and its one writer runs in both modes, so the archive goes on across a flip and
across a way back with nothing to copy. `bronze.expense_types` is chain 6a's.

**One writer of the order tables stays one writer.** `pg_landing.write_orders`
became a thin acquire-and-transaction around `_write_order_rows`, which takes
the caller's connection (and `_write` around `_write_rows` for expenses). The
mirror runs the cores in their own transaction; the chain runs them in its own,
after `chain_latch.claim`, so the owner rows share an `xmin` with the order.
The version capture, both landing watermarks (the canary's `mirror_stale` and
`order_versions_stalled` keep meaning what they say) and the derivation mark go
with them unchanged.

**The decider read is the write decision.** DuckDB decided insert, update,
skip-as-unchanged and the header-only refresh's deferral from one read of
`(id, updated_at)` under its store lock. The chain reads the same pair
`FOR UPDATE`, in id order, inside the writing transaction, and decides with the
same `core.upsert_decider.should_update_order` — on UTC instants, and never
rewriting a stored order from a payload without `updated_at` (DuckDB reads
that stamp as NaT; a grid against DuckDB's own `upsert_orders` found it). A
batch that would write nothing is read before the latch and takes none. A
value Postgres would refuse (NUL in text, a number over `NUMERIC(12,2)`, an id
outside INTEGER, a NULL into NOT NULL) is refused before the latch, logged by
id, counted in `failed`. A `DataError` nobody foresaw retries the batch order
by order on the same connection, each with its own claim. Anything else raises.

**Every path asks the chain's one answer.** `SyncService._upsert_orders_with_expenses`
routes at its first statement; DuckDB's `upsert_orders` and
`upsert_expenses_batch` raise `ChainOwnsOrders` while it writes Postgres
(`upsert_expenses`, which had no caller, is deleted). `record_backfill_misses`
routes, dated by the web clock rather than `now()` — the clock the copy-back
orders the two stores by. The gap, backdated, half-written and checkpoint
selections read Postgres (`core/pg_orders_read.py`, no `except`), and so does
the boot's "do we hold any orders" gate — a latched host with an empty DuckDB
would otherwise full-sync 730 days on every start. The two comment backfills
write through `restore_manager_comments`: only NULL comments, headers only,
`kind='backfill'`. `tests/unit/test_order_paths_consult_chain.py` walks
`core/`, `web/` and `scripts/` for every DuckDB statement writing the four
tables.

**The order step cannot take the tick down.** Under the chain a failure that
is not KeyCRM's is recorded (`write_chains.pg_orders_write.sync_step`, the
error class only), `last_sync_orders` stays where it was, `meta.mirror_state`
marks `bronze.orders` failing, and products, managers, buyers and inventory run.
The watermark lives in `meta.chain_watermarks` and is read before the fetch, so
a dead Postgres spends no KeyCRM call. Until the first tick under the flag
writes it there, DuckDB's frozen stamp stands in
(`CHAIN_WATERMARK_INHERITS_DUCKDB`, read by the store's getter as well as the
freshness check): it is where the window starts, and read as absent it was
"an hour ago", so a flip after a day of KeyCRM failing skipped every order
updated in that day (found in review). The canary pages
**`orders_sync_failing`** (CRITICAL) on three failures in a row, no success for
15 minutes, or the step not reached for 20 — except while the tick waits
for the heavy-job lock (`sync_step.lock_wait_s`, recorded by the scheduler's
tick): the full sync, training, the backup and the 05:15 refresh hold it,
and a hold past 20 minutes paged with nothing at fault (found in review).
The wait excuses its own length, never what was stale before it, and pages
on its own past 90 minutes, chain 4's bound. How long those jobs hold the
lock has not been measured.

**The search index's Postgres cursor moves with it** (OD-15).
`last_sync_meilisearch_pg` is `MAX(mirrored_at)` over orders, buyers and
products, a Postgres clock, and it is the chain's second sync key: under the
chain the store's getter and setter keep it in `meta.chain_watermarks`,
DuckDB's frozen cursor stands in until the first index tick writes it there
(the same `CHAIN_WATERMARK_INHERITS_DUCKDB`, so that tick goes on
incrementally rather than re-indexing everything), and the copy-back carries
it into DuckDB and the release deletes it. Nothing judges its age: it is in no
`FRESHNESS_THRESHOLDS` entry, so a run that cannot read the moved watermarks
does not name it in `sync_watermarks_unwatched`. `last_sync_meilisearch`, the
DuckDB-built index's cursor, is a DuckDB clock and stays DuckDB's. Declared on
no chain until a review found it, while this file said chain 3 carried it.

**What stands down with it.** DuckDB's arm of `dq_reconciliation` (layer
`reconciliation`): against a DuckDB that no longer receives orders it would
page every morning and re-fetch every new order. The Postgres arm, from the
same KeyCRM snapshot, drives the repair; the stood-down arm journals no run.
`/api/health` marks the layer `stood_down` — after chain 9's `_journal_ages`,
which answers Postgres's block and ignores the one it is handed, so marking
first loses the mark — and the canary, the catch-up and the digest skip it.
Each of those is pinned by running it, not by reading its source. The ten
DuckDB integrity checks over the order tables (`ORDER_LANDING_CHECKS`) join
`stood_down_duckdb_checks()`, so DN-23's Postgres twins stand in alone;
`tests/unit/test_order_landing_stand_down.py` derives the list from the scan.
The standing watch adds `chain_expense_orphans` (an expense with no order past
a day) and a NULL `checked_at` in the misses.

**Preconditions, every one read locally** (`unmet_precondition()`, published
through the registry, `write_chain_precondition_unmet` on the canary):
`goals_bridge` (the goal calculators still read DuckDB orders until chain 7b),
`step13` (Postgres alone derives the warehouse), `chain1` (DuckDB's SKU rebuild
reads DuckDB order lines), the backup evidence — `pitr_drill` and
`remote_restore` under 8 days, `pg_offsite` under 36 h, from the markers the
host scripts now write on success only (`core/backup_evidence.py`; one provider,
OD-01 (c) cancelled 2026-10-01; their ages, and only their ages, are
`/api/health`'s `backups`) — and `landing_pages_clear`: no delivered page
under a condition the stand-down retires. While any is unmet an unlatched chain
runs as duckdb whatever its flag says. **And held is held until the process
ends** (`held_until_restart`): the markers age and are rewritten by the
host's drills, and a page resolves, inside a running web, and with the flag
set the Monday drill's fresh marker used to flip a running web on its next
write — no stopped window, no `--handover` (found in review). The first unmet
answer under the flag is remembered, from the start (`configure_modes()`)
on, so the flip happens only at a start that found everything met. **Except
`step13` before step 13 has decided** (`warehouse_cutover.verdict_reached()`):
step 13's start gathers the goal bridge's owners by asking this chain its
mode, before its own verdict exists, and remembering that "unmet" held chain
3 — and chain 5 through it — for good on every start that found step 13 in
force (batch-E review). It still keeps the chain on DuckDB; it is not held on.
`preflight` adds what Postgres says: the three bronze tables backfilled and
their mirrors not failing. It is asked beside chain 1's, never after it: each
is bounded at 5 s, and two in a row with Postgres hung were the canary's whole
10 s (found in review). And each is shielded, the sync tick's DN-05a form: a
bare `wait_for` waits for asyncpg's cancel round trip, and against a paused
Postgres the "5 s" answer came after 60 s, holding its cache lock — and every
later `/api/health` — meanwhile (batch-E review). Its `reasons` are one entry
per precondition by structure (`unmet_reasons()`), never the joined answer
split on "; ", which two of them carry.

**The way back** is the copy-back generalised from chain 4's mirrored tables:
the orders, expenses and line items each dated by their own `mirrored_at`,
KeyCRM's `updated_at` refusing a later DuckDB version, and the misses
replaced whole. **A line item is never dated by its order's header**: the
05:15 refresh and the comment restore write headers alone, so a line only
DuckDB holds under any refreshed order read as a basket the chain shrank and
`--execute` deleted it (found in review). It is the chain's only when
Postgres's line items of its order carry a stamp from after the latch. A
basket the chain emptied leaves no line to date and is refused, knowingly too
strict, with the per-id decision named. No allocator to carry. Proved against
a real Postgres in `tests/integration/test_chain_copy_back_orders.py`. **What
is not built**: the soak checks and the restore drill counting the four
tables. The lock-out for images without the chain is revision 0035, shared
with the other seven chains #280 registered (see "Revision 0035"); it holds
the backfills and the misses ledger's copy, and not the per-tick orders and
expenses mirrors, which never asked the revision.

### The Postgres mirror of landing
One parse, two stores. `core/landing_rows.py` turns a KeyCRM payload into typed
rows; DuckDB and Postgres each write the rows they are handed. Bookkeeping
columns are *not* shared — DuckDB stamps `synced_at`, Postgres `mirrored_at` —
because two correct copies differ on those by construction.

**The catalogue re-ships whole; orders ship a delta.** `mirror_products` sends
all 1,003 products every hour. `upsert_orders` sends `updated_ids` — what it
actually wrote — because a page of 500 orders usually changes a handful, and
re-sending the page every minute would be thousands of no-op writes. That
difference is why orders need `POST /api/mirror/backfill/orders` and the
catalogue never did.

Three things the orders path must keep doing, each with a reason that is not
obvious from the code:

- **`manager_comment` is `COALESCE(EXCLUDED, stored)`, never overwritten with
  NULL** — it carries UTM attribution a backfill restored, and 14 179 of 46 446
  orders have it NULL. A plain `EXCLUDED` would erase attribution on one store
  only, and the reconciliation would then report a difference the mirror had
  created.
- **Line items are deleted per order and re-inserted, never upserted.** Ids are
  `order_id * 1000 + position`, so an order dropping from three items to two
  leaves `…002` behind under an upsert.
- **The `order_products` watermark only moves when line items moved.**
  `skip_products=True` (the status-only refresh) rewrites headers and does not
  look at line items; stamping them as shipped there would tell the
  reconciliation they are current when nothing touched them.

The backfill has **no cursor**: it asks both stores which order ids they hold,
ships the difference in chunks, and stops. Interrupt it anywhere and the next
run resumes by recomputing. It does *not* repair an order that exists on both
sides and differs — that is the reconciliation's business, downstream of a
check that can say which rows disagree. Measured end to end against the
production backup on a throwaway Postgres 17.2: 46 446 orders and 147 508 line
items in **13.2 s**, then compared column by column — **0 differing rows on
both tables**.

### Replicated, not mirrored: the manager classification
`bronze.managers` and `app.manager_classifications` are the one pair that does
**not** come from a KeyCRM payload. `sales_type` is decided by
`silver_sales_type_case()`, four of whose six branches read these tables, and
KeyCRM has no idea whether a manager sells retail, wholesale, internally, or
ships to bloggers — **only a human does**. `upsert_managers` seeds `is_retail`
from `RETAIL_MANAGER_IDS` for managers it has never seen and never touches it
again, because recomputing it every sync made the column unfixable.

So `core/pg_replication.py` copies what DuckDB holds. A payload-fed mirror would
re-seed from the same constant, the two stores would classify differently, and
the Gold reconciliation would then be measuring the classification instead of
the migration.

**Full replace, both tables, one transaction.** An interval can be *deleted* — a
correction removes one — and an upsert would leave a ghost that silently
reclassifies past orders. It refuses an empty manager list: replacing a
populated table with nothing sends every affected order to `internal`, which is
admin-only, and empties the dashboard for everyone else.

Written on every manager sync **and** in `POST /api/managers/{id}/retail-status`,
because that endpoint is the reason the value cannot be re-derived and waiting
for the daily sync would leave Postgres a day behind.

In Reconciliation A the two carry `full_replace=True`, which removes the retired
category: a full replace writes every row it holds, so a missing row was lost,
not retired. Proven on the production classification — deleting one interval in
Postgres is reported CRITICAL with the manager id as the sample, where the
catalogue rule would have excused it as retired (seeded baselines carry
`valid_from = 1970-01-01`, so their `set_at` always predates the watermark).

### Chain 5: the classification, written where it is derived (off)

`core/pg_managers_write.py` is a registered write chain for `bronze.managers`
and `app.manager_classifications` — `pg_replication.MANAGER_UNIT`, whole —
under `KS_WRITE_MANAGERS=postgres`. **Off in production, and held even if
set**: `unmet_precondition()` names `goals_bridge` (the goal calculators read
DuckDB's classification until 7b-4 deletes the bridge — spelled as chain 3
spells it, so the tripwire in `test_goals_off_duckdb_silver.py` finds the
hold), `step13` (DuckDB would derive `sales_type` from a classification the
chain freezes), `chain3` (`update_manager_stats` reads the orders; a build
without chain 3 reads as unmet, not as an import error) and
`read_fallback_off`. Every fact is local, so `/api/health` answers with
Postgres down. Order of flips: 7b-4, step 13, chain 3, chain 5, with the
lock-out revision — 0035, shared by all eight of #280's chains — deployed
before the first of those eight flips (see "Revision 0035"). The way back runs
in reverse.

**Three writers, one advisory lock** (`CHAIN_LOCK_KEY`, 'ks' + 5), taken
first in every transaction. Measured before building: two classifications of
one manager in flight leave **two open intervals** without it, one with it.

- `upsert_managers` — from the shared parse (`landing_rows.manager_row`).
  `is_retail` is inserted for a new manager and **never in the conflict
  update**: writing the seed there reverts every human classification. The
  same transaction seeds a baseline for every manager without one — DuckDB
  does that only at the next boot (`_m0006`), which stands down once the
  chain is latched, because a DuckDB baseline after the latch is a key the
  copy-back refuses on.
- `update_manager_stats` — one body for both engines
  (`sql_dialect.manager_stats_sql`) with the date **spelled in Kyiv**.
  `DATE(ordered_at)` took the process's timezone: right in production only
  because web runs `TZ=Europe/Kyiv`, a day early under UTC — CI, a laptop, a
  Postgres session. Production's answer does not move. It raises no
  derivation signal: Silver reads none of the three columns.
- `set_manager_retail_status` — close, replace the same day, open, set
  `is_retail`, raise the signal: one transaction. **A backdate behind the
  manager's latest change is refused** (`BackdateBehindLatest`, 409) before
  the latch and again under the lock (OD-C5-1 (a)). DuckDB accepts one and
  writes two open intervals and an overlap, and the `sales_type` it then
  gives depends on the direction — that latent defect is left as it is, since
  changing what DuckDB accepts changes production. `set_at` is the web
  container's UTC clock, the copy-back's handover clock.

**Around it, with the flag off nothing moves**: the store routes the four
methods by `writes_postgres()` (or `reads_postgres()`); `get_all_managers`
and every statement carrying `{managers}` (the dashboard's returns list,
through `_dashboard_run`) read Postgres once the chain owns the tables, with
no fallback — a walk in `test_managers_chain.py` finds every such statement;
the incremental tick's managers step is contained under the chain, retried
after 10 minutes and published as `write_chains.pg_managers_write.sync_step`
(on DuckDB a raise there skipped the buyers, offers and stocks steps, and
still does); the retail-status route answers a `KS_WRITE_MANAGERS` typo with
409 before anything is read and a Postgres failure with 503;
`replicate_managers` already stood down on the flag and on an owner row
alone (DN-22b). `/api/health` asks chain 5's `preflight()` concurrently with
chain 1's — two in a row could cost the canary's whole timeout — and it
names every unmet precondition and, once those all hold, a replica failing or
over 26 h old and the shape the standing watch would file.

**Watched**: `chain_managers_empty` and `chain_manager_open_interval`
(CRITICAL), `chain_manager_unclassified` (WARN, from the latch on),
`chain_manager_intervals_broken` and `chain_manager_retail_disagrees` (WARN),
and a NULL `set_at` as `chain_required_column_null` — one read,
`pg_managers_write.SHAPE_SQL`, shared with the preflight. Soak checks M1 (the
replica stayed away) and M2 (the shape and `last_sync_managers` under 26 h);
`stage4_soak.sh` passes `managers_on=pending` for a flag the chain has not
latched under, since only web can evaluate the bridge. The restore drill
counts both tables once the chain owns them (`grows`: nothing deletes a
manager, and the writer's one DELETE is followed by the INSERT of the same
key).

**The way back** is `scripts/chain_copy_back.py managers`. `chain_specs`
derives the pair from a fourth source — `pg_replication.REPLICATED_SHAPES`
and `mirror_reconciliation.REPLICATED_TABLES` — and the handover differs from
an operational table's in two places: the pre-flip lever is
`replicate_managers` and `POST /api/jobs/manager_stats/trigger`, and after the
latch a key only DuckDB holds is a DuckDB write after it. Before a flip a key
only Postgres holds is **CRITICAL** (there is no next full replace to remove a
ghost interval) — this pair's rule first, and every full-replace operational
table's since F6 (review of #265). Expect one pre-flip CRITICAL naming a
`(manager, 1970-01-01)` baseline: the script's own connect runs `_m0006` for
a manager synced since web's last start; `up -d web` ships it. A chain
declaring `CHAIN_COPY_BACK_OWES_FULL_REBUILD` gets `warehouse_dirty='full'`
in the copy's own transaction — `sales_type` is materialised at rebuild time
and an incremental rebuild never re-derives an order whose classification
moved. **Step 13's way back is refused while this chain is latched**: a
start that would run as duckdb stays `postgres` and pages
`warehouse_way_back_refused` (see "Step 13"); copy chain 5 back first, then
chain 3, then take the way back.

**What the review moved (2026-10-08).** The ways back leave a state where this
chain is on DuckDB and chain 3 still owns the orders; DuckDB's
`update_manager_stats` then leaves the stats as the copy-back carried them
(`duckdb_orders_frozen`) rather than count frozen orders and ship
`last_order_date` and `order_count` backwards. A value Postgres would refuse —
a note with NUL, a `set_by` past INTEGER — is `ClassificationRefused`, which
the route answers **422**, not "Postgres did not answer". Every precondition's
reason starts with its key, an unreadable one too (`read_fallback_off`, not
`read_fallback`). With the flag off nothing is asked on the chain's behalf:
the preflight asks Postgres only once every local precondition holds — never
while chain 3 writes DuckDB, its default — and the restore drill counts the two tables only
once live `meta.chain_watermarks` holds an owner row naming one. And a walk in
`test_managers_chain.py` finds every statement naming DuckDB's `managers` or
`manager_classifications` **bare**, not only through the holes, and holds each
to the router that keeps it off the frozen copy.

### Postgres against KeyCRM — the other half of the criterion
Reconciliation A proves the two stores agree with each other. That is not the
same as being right: two copies can agree perfectly and both disagree with the
source. So step 05 closes on **two** things — zero differences between the
stores *and* a reconciliation against KeyCRM.

`dq_reconciliation` now compares the KeyCRM snapshot it already fetched against
**both** stores and writes two runs: layer `reconciliation` (DuckDB) and layer
`reconciliation_pg` (Postgres). **No extra API calls** — the fetch is the
expensive part and it has already happened, and comparing both stores to the
identical snapshot is what makes the two verdicts comparable at all.

Separate layers, not extra findings on one: a single layer would give the two
comparisons one age between them, and a Postgres half that stopped running
would hide behind a fresh DuckDB one.

**No repair path on the Postgres side.** The DuckDB layer re-fetches orders it
is missing, because a delta sync keyed on `updated_at` can never reach an order
it does not hold. Postgres has a different answer to the same problem — the
backfill — and re-fetching from KeyCRM there would repair the wrong store.

Gated on `backfilled_at`, like Reconciliation A and for the same reason.

`core.reconciliation_io.postgres_orders_in_window` is a deliberate
transliteration of the DuckDB extractor, not a rewrite — same columns, window,
source filter, watermark and exclusions. Verified over the production
catalogue: **44 352 orders across a 900-day window, 0 only on either side, 0
differing**, and the per-order facts compare `==` outright. `AT TIME ZONE
'Europe/Kyiv'` means the same thing in both engines, which matters because
1 007 orders have a Kyiv date different from their UTC one.

**The rollups still do not compare equal, and must not be made to.** With
identical per-order facts, the two databases return rows in different orders
and float addition is not associative: the largest observed gap is **2.1e-09**
on a ₴1 834 807.24 cell. The tolerance in `classify_discrepancies` is
load-bearing here — exact equality would buy a daily discrepancy of two
nanohryvnia.

### Reconciliation A — the two stores against each other
`dq_mirror_landing` (layer `mirror_landing`, daily 07:30) compares
`bronze.products` and `bronze.categories` in Postgres against `products` and
`categories` in DuckDB, **column by column with a tolerance of zero**. Landing
is a copy, not a computation — the same parsed tuple from `core/landing_rows.py`
goes to both stores in the same call — so any difference at all is a defect in
the mirror, and a tolerance would only be somewhere for one to hide. This is
step 05's closing criterion.

**A row DuckDB has and Postgres does not is two different things**, and
`meta.mirror_state.last_ok_at` is what tells them apart. Every successful mirror
ships the *whole* catalogue, so after a success at T, Postgres holds everything
KeyCRM served at T:

- `synced_at <= last_ok_at` → KeyCRM has **retired** the row. DuckDB keeps it
  because upsert never deletes; a payload-fed mirror can never learn of it
  again. INFO, counted. Production has exactly one, product 1055, last synced
  2026-06-13 — that is the whole 1004-vs-1003 gap.
- `synced_at > last_ok_at` → in flight inside the 15-minute grace, **lost**
  past it. CRITICAL.

A table with no `last_ok_at` at all reports `mirror_never_shipped` and
**suppresses the row-level findings**, so `bronze.categories` — written only by
the weekly full sync — does not report 28 missing rows every morning until
Sunday.

Report-only, like the Silver arc and the Gold cell check: a finding cannot
reach `validation_passed`, and a mirror that quietly re-shipped whatever it
noticed missing would destroy the signal.

**Orders are compared differently, because 46 487 rows and 147 648 line items
cannot be pulled whole every morning.** Both sides are folded into one
fingerprint per 1 000-id bucket — a count and one SUM per column — and only the
buckets that disagree are opened and compared row by row. ~47 rows come back
from each store; the full check costs **0.27 s** measured over the whole
catalogue of orders.

The fingerprint sees any change to a number, a timestamp, or a text column's
*length*. It cannot see a text edit of exactly equal length in a bucket where
nothing else moved — measured and confirmed, not assumed. The alternative is a
cross-engine row hash, which needs both databases to render numerics and
timestamps to text identically, and they do not.

**Not keyed on `updated_at`, which would be exact and cheap.** KeyCRM does not
bump it on a status change — that is why `upsert_orders` has `force_update` —
so an order can move from status 12 to 20 with `updated_at` untouched on both
sides. Verified: the fingerprint catches exactly that change.

**The gate is `meta.mirror_state.backfilled_at`.** `last_ok_at` licenses a
tolerance of zero for the catalogue because a catalogue mirror ships the whole
table. The orders mirror ships `updated_ids`, so it proves only that the last
delta landed. Until the backfill has carried history across, every order older
than the mirror looks exactly like a lost one — so the row-level comparison is
suppressed and one `mirror_backfill_pending` is reported instead. The column is
written by `core/pg_backfill.py` only on a run that finished with nothing
remaining. A partial backfill claiming completion would file tens of thousands
of CRITICALs.

`mirror_buckets_disagree` caps the drill-down: more than 20 disagreeing buckets
is a whole-table problem, and reading them all would turn a daily check into a
table scan of both stores. `mirror_landing` is in
`WATCHED_LAYERS` (digest section, layer age, catch-up), and since 2026-08-28 in
the canary's `DQ_MAX_AGE_S` too, paged at 30 h: the worry that kept it out —
the canary's first probe 90 s after the bot starts, before the catch-up can
finish — only bites a layer already past 30 h, and the 26 h catch-up sits
under that page (see "Scheduled checks").

### Gold in Postgres, and why it is not the same table

`gold.daily_revenue` (revision 0007) is the second thing Postgres *derives*
rather than receives. `core/pg_gold.py` writes it from `silver.orders`, in the
same call that rebuilds Silver and immediately after it — one floor, one tick,
so Gold can never aggregate a Silver the next statement is about to replace.
`TRUNCATE` then one `INSERT`, in one transaction, for `pg_silver`'s reasons.

**The measures are shared; the shape is not.** The eight aggregates live in
`GOLD_MEASURES` (`core/duckdb_store.py`) and are rendered verbatim into both
`GOLD_REVENUE_SELECT_SQL` and the Postgres projection — rule 1, with a test
per expression. What does not transfer is DuckDB's channel columns:
`instagram_revenue` and its five siblings can only name a channel somebody
wrote a column for. Source 5 (Виставка) arrived with #101 and is
**₴266,059.00 across 177 orders** that sits in `revenue` and in none of the
three columns; all of it in the `exhibition` rows, whose channel columns are
empty while their revenue is not. Postgres carries `source_id` as a dimension
instead — the grain `gold_daily_products` has had all along.

**The table holds both grains, and the roll-up is not redundancy.** Three
measures are `COUNT(DISTINCT buyer_id)` and distinct counts do not add up:
folding the per-source rows overstates `unique_customers` in 29 of 2,107
production cells, `new_customers` in 12, `returning_customers` in 17 — each by
one buyer who used two channels in a day. The criterion is zero, so an
approximation is not available, and the read switch cannot serve those columns
from the fine rows either. `GROUP BY GROUPING SETS` computes both grains in one
pass over one snapshot; `source_id IS NULL` is the roll-up. ClickHouse will
answer this with `AggregateFunction(uniqExact)` at step 06 — PostgreSQL has no
equivalent, and this is its answer.

An inactive source still gets a row, and it is all zeroes: every measure
filters `is_active_source`, so Opencart's 2,470 orders and ₴5.8M gross produce
zeros. That is `is_active_source` written out, and DuckDB already does the same
one grain up.

**`reconcile_gold` compares through a mapping, never by folding.** DuckDB's
fourteen measures each have an exact counterpart: the eight against the roll-up
row, the six channel columns against the fine rows for sources 1, 2 and 4 —
sound only while those are in `REVENUE_SOURCE_IDS`, which a test pins. An
absent fine row reads as zero, not as a missing row. Tolerance is zero
everywhere except `avg_order_value`, which may differ by one cent because
DuckDB's DECIMAL division promotes to DOUBLE — measured, not assumed: **5 cells
of 2,106 actually do**. `gold_rollup_mismatch` additionally checks the fine
rows add up to the roll-up for the four additive measures, which is the only
thing that sees sources 3 and 5 at all.

It runs inside `dq_mirror_landing`, on layer `mirror_landing`, after
`reconcile_silver`. **Not its own layer** — all three comparisons run in one
call, so they cannot have different ages and a fourth layer would invent one.
That is the opposite of `reconciliation_pg`, which is separate precisely
because it *can* stop running on its own.

Verified end to end 2026-08-27 on a throwaway Postgres 17.2 against the
production backup: 46,620 Silver rows → **5,681 Gold rows in 235 ms**, 2,106
roll-ups matching DuckDB's 2,106 cells exactly, and **29,484 values compared
with 0 discrepancies**.

**Deploying 0007 needs both images, migrate first.** `Dockerfile.migrate`
COPYs `migrations/`, so `keycrm-migrate` must be rebuilt and pushed alongside
`keycrm-web`.

Getting the order wrong does **not** crash the container — there is no startup
revision gate, and `web/main.py` never imports `core/pg.py`. What happens
instead: the mirror of landing keeps working, and `rebuild_silver`,
`rebuild_gold`, the backfill and all three reconciliations raise
`SchemaVersionError`. The rebuilds are swallowed and logged; the checks persist
failed runs, so the layer ages go stale and the 09:00 digest says so. Fails
closed, not silently — but you find out the next morning, not at deploy.

### The five tables with no source to be rebuilt from

`bronze.*` can be re-fetched from KeyCRM; Silver and Gold can be recomputed.
These cannot, which is why the plan's 2026-08-22 amendment routes them to
Postgres and **not** to ClickHouse — a store you would rebuild from source is a
cache, and none of these has a source.

| `app.*` (revision 0008) | rows | what it knows that KeyCRM does not |
|---|---|---|
| `stock_movements` | 50,386 | the only record that a quantity ever changed — a delta against the *previous* `offer_stocks`, so a movement not written when it happened is gone |
| `inventory_sku_history` | 143,274 | per-SKU daily snapshot since 2026-01-27 |
| `sku_inventory_status` | 887 | `first_seen_at`, carried forward out of the table's own previous contents |
| `inventory_history` | 188 | the same snapshot rolled up |
| `order_backfill_misses` | 43 | ids KeyCRM **could not supply** — the one fact an API can never be asked for |

**Replicated, not mirrored**, for `app.manager_classifications`' reason: all
five are derived from state only DuckDB holds, so a second computation here
would diverge the first time either side missed a sync. `core/pg_operational.py`
copies them as they stand.

**Two shipping shapes, chosen by how DuckDB writes each one.** The three small
tables are replaced whole — and `sku_inventory_status` is a `DELETE`+`INSERT`
on the DuckDB side too, so that is the same operation rather than a decision.
`inventory_sku_history` and `stock_movements` are **append-only** — verified by
parsing the repository for an `UPDATE` or `DELETE` against either, and pinned
by a test — so they ship what is above `MAX(date)` / `MAX(id)` read back out of
Postgres. **No stored cursor**, `core/pg_backfill.py`'s reason: nothing to be
wrong, and an interrupted run resumes by recomputing.

`stock_movements.id` is **carried from DuckDB, never generated here.** A
Postgres sequence would give the same movement two names and the comparison
would be meaningless before it began.

**One scheduled job, hourly, and exactly one call site.** Five code paths write
these tables and hooking each is how the sixth gets forgotten — which is
precisely what `update_manager_stats` did until `cf34e8b`. The cost is lag, so
`OPERATIONAL_GRACE_MINUTES` (90) is sized against the interval the way
`SILVER_GRACE_MINUTES` is against `KS_PG_SILVER_INTERVAL_S`; raise one and the
other moves with it.

**The hole a watermark cannot see.** A row lost from Postgres *below* the
watermark is invisible to `id > MAX(id)` and to `date >= MAX(date)` forever.
The daily comparison finds it; `replicate_operational(store, full=True)` puts
it back, upserting so a row that is present and *wrong* is corrected too. Never
on a schedule — Reconciliation A reports and does not repair, and that matters
most here, where a "repair" would be writing the only record of a stock change
from the only other record of it.

`reconcile_operational` runs last inside `dq_mirror_landing`: the three small
tables read whole with `full_replace=True`, the two large ones fingerprinted.
`inventory_sku_history` buckets by the **day**, not by an id — there are only
~900 offers, so dividing an id would put all 143,274 rows in one bucket and the
drill-down would scan the table to find one row.

Verified 2026-08-27 on a throwaway PostgreSQL 17.2 loaded from the production
backup. First replication **9.3 s**, incremental **116 ms**, whole comparison
**177–221 ms**. Then broken on purpose, seven ways: four deletions and three
value changes were each reported CRITICAL with the id; the incremental run
repaired the full-replace table and left the two append-only ones broken, as
documented; `full=True` cleared all of it.

### The archive of order versions, and why it was built first

`app.order_versions` (revision 0010) is the sixth thing here that nothing can
rebuild, and the only one that is *still being lost*. The five tables above at
least exist; yesterday's order status does not. KeyCRM serves current state and
has no history endpoint, so a transition nobody wrote down when it happened is
gone — the same sentence as `stock_movements`, which is why the order of work
was inverted to put this first.

**It is a side branch, not a layer.** Silver keeps feeding from the latest state
in bronze; nothing downstream reads the archive, and dropping it would not move
a number on any dashboard. That is what makes it safe to build first.

**Fed from `updated_ids`, never from a poll.** `upsert_orders` already computes
the exact set of orders it wrote — rows already in the desired state go to
`skipped_unchanged` instead — and until now discarded it. A scan keyed on
`updated_at` cannot work here and would fail on precisely the transitions this
exists for: KeyCRM does not bump `updated_at` on a status change, which is the
whole reason `force_update` exists.

**A version is a change, not an observation.** `force_update` bypasses the state
check by design, so `updated_ids` carries orders whose content did not move.
Measured on production before anything was written: `order_status_refresh` runs
daily at 05:15 over a 30-day `created_between` window and force-wrote **1,388**
orders in one hour on 26.08, while the reconciliation against KeyCRM reported
**0 discrepancies on 13 of the last 14 days** — the store already agreed with
the source, so those writes rewrote identical content. Without the comparison
the table gains ~500 K rows a year recording nothing; with it, ~100 a day
against ~48 orders created. `bronze_order_events` is the standing proof of the
alternative: it keyed on the observation, wrote ~150 K rows a day and took the
database to 43 GB.

Three decisions that read as details and are not:

- **The capture runs inside `write_orders`' transaction**, not in
  `mirror_orders`. The mirror never raises because the backfill can ship what it
  missed; **this has no backfill and cannot have one**, so it must not inherit
  that contract. The version and the row it describes land together or neither
  does.
- **It reads the stored row, never the payload.** `manager_comment` is upserted
  as `COALESCE(EXCLUDED, stored)` because it carries UTM attribution, and 32 437
  of 46 685 orders have a value in it. Comparing an incoming payload that omits
  the field would report "comment removed" on two orders in three, every tick,
  forever.
- **Header only.** The 05:15 refresh runs `skip_products=True` and carries no
  line items at all, so an archive that recorded them would minute the deletion
  of ~1 400 baskets every morning.

`updated_at` is stored and deliberately **not** compared: if it moves while every
stored column is identical, nothing this store holds has changed.

**A restored `manager_comment` is a fourth kind** (owner's decision OD-20 (b),
2026-09-17). The two `manager_comment` backfills — `POST /api/traffic/backfill-utm`
and `scripts/backfill_utm.py` — re-fetch a comment KeyCRM has always held and
fill it in where the column is NULL. The stored row genuinely changes, so it is
archived like any other; what would be false is calling it a `'change'`, which
dates a transition on the day an operator ran a script. It is written as
`kind = 'backfill'` instead — no migration, because `kind` has never carried a
CHECK — and the two liveness checks exclude it exactly as they exclude the
migration's `'baseline'`.

**The volume was measured before the exclusion was taken** (OD-20 asked for
that, production, 2026-09-17): **0** orders where Postgres holds a NULL comment
and DuckDB holds one, so nothing diverges today; 10 192 orders carry a NULL
comment in DuckDB inside the backfills' 730-day window, of which **26** are
website orders (`source_id = 4`) — the upper bound on what a KeyCRM re-fetch
could fill, since an order taken by hand in the Instagram inbox never carried a
tag. One run today is therefore ~26 rows against a threshold of 1 000, and the
flood is **not** at risk. The exclusion stands on what the count *means*: a
repair an operator deliberately started is not the content comparison having
stopped discriminating, which is the only thing `order_versions_flooding` can
honestly name — and the volume can move (the window is a `--days` argument, and
the July 2026 template break is what put 10 192 NULLs there) while the meaning
cannot. The **stall** check excludes it too, and that is the opposite of what it
does with `'baseline'` — a baseline exists only in the hours after a migration
and buys a fresh archive its first quiet day, while a backfill runs on a system
that has been writing for months, where counting it would certify the sync as
alive for a day after the one event that distracts everybody from watching it.

Both backfills reach Postgres through **`core.pg_backfill.ship_orders_by_id`**,
headers only. Until 2026-09-18 they wrote DuckDB and stopped, so a restored
comment never reached `bronze.orders` or `app.order_versions`. **That was never
a /traffic problem**, and an earlier draft of this paragraph said it was: the
tab reads `silver.orders LEFT JOIN silver.order_utm` (`core/pg_traffic_read.py`
mentions neither `bronze` nor `manager_comment`) and both backfills have shipped
the *parsed* UTM rows through `ship_after_reparse` since DN-04, so the screen
always saw the result. What the missing header actually costs is the daily
`mirror_landing` orders fingerprint, which compares `manager_comment` and
therefore reported every restored comment as a disagreeing bucket, and step 9's
Postgres UTM parser, which reads `bronze.orders.manager_comment` — which is why
this lands before that parser is wired up. The
hourly ids-diff cannot carry this and never could: it ships the orders Postgres
is *missing*, and these exist on both sides and merely differ. The route holds
`get_scheduler()._heavy_job_lock` across the UPDATE and the ship (the KeyCRM
fetch stays outside it); the CLI cannot — that lock lives in the web process —
so it logs `WEB_MUST_BE_STOPPED` and says so in its own `--help`.
`tests/unit/test_heavy_lock_coverage.py` walks `web/` and `scripts/` for any
function executing an `UPDATE orders`, and requires one of those two answers as
a *name the code evaluates*: the first draft read the source text and a comment
mentioning the constant satisfied it, and the second accepted the bare `import`
the deleted warning left behind, so `WEB_MUST_BE_STOPPED` now has to be an
argument to a call the function makes.

**The ship raises and both callers catch it, per chunk.**
`ship_orders_by_id` inherits `backfill_orders`' "stop loudly" contract, which
was written for a helper with no second job. These two have one: the DuckDB UTM
re-parse at the end of each run, which was Postgres-independent before this and
must stay so. So an unreachable Postgres — or `web` deployed ahead of `migrate`,
where `require_revision()` raises — costs the ship and nothing else. The ids are
recorded (`pg_failed_ids` in the endpoint's status, an ERROR naming them in the
CLI) and the endpoint's final status becomes `partial`, because a re-run cannot
find those rows again: the SELECT that chooses them only offers comments that
are still NULL.

**The check is liveness, because a healthy archive is quiet.** Comparing version
count against `updated_ids` — the obvious check — fails by construction, since
writing fewer rows than ids offered is the design. What breaks the tie is that a
brand-new order has no previous version and so always writes one, and production
creates ~48 a day: a whole day with no version is a broken writer, not a quiet
day. `reconcile_order_versions` runs inside `dq_mirror_landing` on the same
layer as the rest — one call, one age — and reports `order_versions_stalled`,
`order_versions_flooding`, `order_versions_missing` and `order_versions_empty`.
Report-only, and more absolutely than anything else in that module: a "repair"
would mean inventing the history this table is the only record of.

Append-only is enforced by there being no statement that is not an `INSERT`,
pinned by a test that **parses** the repository — not by a `REVOKE`. Migrations
run as `ks_app` (the `migrate` service chose that over `postgres` deliberately),
so `ks_app` owns the table and an owner can grant itself back anything it
revoked; a `REVOKE` here would look like a guarantee and be a speed bump.

Verified 2026-08-27 by execution on a throwaway PostgreSQL 17.11, because local
tests do not run SQL and revision 0009 died at `COMMENT ON` on a real server
after four tables had already been created. The migration applies and seeds a
baseline for every order already held; the real `write_orders` was driven
through six cases including the `manager_comment` one; the archive was broken
six ways and each was reported with the right severity and id. Cost at
production scale (130 K-row archive, 24 MB): **0.6 ms** for an ordinary tick,
**2.4 ms** for the 1 400-id 05:15 batch, **7.4 ms** for a 5 000-id backfill
chunk — ~190 bytes a row, so **~7 MB a year**.

**`bot` and `web` wait for `migrate` to finish** (`service_completed_successfully`,
added 2026-09-08). They did not until then, and `up -d` started all three at
once: the bot lost the race on every migration, refused to start on the
revision mismatch and came back on its restart policy — one crash per deploy,
seen in the log as a RuntimeError naming both revisions. `web` had no startup
gate at all, so it lost the same race silently and served 401 to everybody
while looking healthy, because the session read is a Postgres read. A failed
migration now stops both in one place instead.

**Deploying it needs all three images, migrate first.** `REQUIRED_REVISION`
moved to `0010_order_versions` at the time and `require_revision` raises on any
mismatch, ahead or behind. It is `0035_batch_e_chains` today, and every
revision since has inherited the same rule: rebuild and push `keycrm-migrate`
alongside `keycrm-web`, and let `docker wait ks-migrate` finish first. The bot
checks it too, but only in `initialise()`, so a running bot survives the
migration; only a restart before its image is replaced would fail.

### The port in front of the bot's state

`bot/database.py` was 717 lines of `sqlite3` with 46 call sites and no tests.
Step 03 needs the engine underneath it to change, so it now has a seam:

```
core/bot_store.py     the port — five Protocols, a registry, no SQL
bot/store_sqlite.py   the adapter — every statement, moved verbatim
bot/database.py       the facade — same names, same signatures, delegating
```

**Synchronous, and that is the one decision worth arguing.** Measured before
choosing: 38 of the 46 call sites are already inside `async def` and would take
an `await`. The 8 that would not include `bot/handlers_legacy._lang()`, which
resolves the reader's language and is called from about forty places precisely
so a parameter is not threaded through forty signatures. Against that, sync
SQLite from async code is already how `core/bot_prefs.py` and
`core/pg_bot_state.py` read these same 51 rows. **The cost is named rather than
discovered later:** a sync port cannot be backed by asyncpg directly, so a
Postgres adapter needs `psycopg` — a second driver — or asyncpg on a background
loop. That belongs with the adapter, on a measurement.

**A facade, not a caller migration.** Rewriting 46 sites to
`get_bot_store().access.approve(...)` buys nothing the engine switch needs, and
38 of them are in `handlers_legacy.py`. The layering cost is that callers see
the module rather than the port and nothing but a test forbids going round it —
`tests/unit/test_bot_store.py` is that test: it installs a store that records,
calls every public function, and fails on any that never reached the port.

**Storage only in the port.** `get_user_language` reads a preference and then
applies a rule about this company (Ukrainian for everyone, English for admins),
so the rule stays in the facade and an adapter never learns who the admins are.
`generate_cache_key` touches nothing and is not in the port at all.

**`revoke_user` is deliberately not a port method.** It is implemented as
`deny`, so revoking somebody's access increments their denial count and five
revocations freeze them for thirty days. Surprising enough to be pinned by
name; kept out of the port so that changing the decision adds a method rather
than redefining `deny`.

**Two clocks, and they must not be unified.** `authorized_users`,
`user_preferences` and `celebrated_milestones` carry timestamps written by
SQLite's `CURRENT_TIMESTAMP` — UTC, `'YYYY-MM-DD HH:MM:SS'`. The cache writes
`expires_at` itself with Python's *local* `datetime.now().isoformat()` and
reads it back the same way: self-consistent, different convention, and merging
them expires the cache by the wrong clock.

Two defects were found putting the first set under test, both on the same
comparison and both now fixed with `_utc_now()` / `_sqlite_stamp()`: the
container runs `TZ=Europe/Kyiv` so `datetime.now()` sat three hours ahead of
every stored value, and `cutoff.isoformat()` writes a `'T'` where SQLite writes
a space — `' '` is 0x20, `'T'` is 0x54, so every row whose activity fell on the
cutoff *date* sorted as older than the cutoff. Together the daily inactivity
sweep was up to twenty-seven hours too eager, on a job that takes access away.

Verified 2026-08-27 in both production images against a copy of the real
`bot.db`: the adapter satisfies every Protocol, all read paths answer, the
engine swaps and restores, and `web/services/auth_service.py` answers through
it. 86 tests — 44 characterising the behaviour, 42 on the seam.

**Seven functions here have no caller anywhere**: `has_pending_request`,
`is_user_frozen` (live only through `reset_to_pending`), `get_pending_requests`,
`cache_delete`, `generate_cache_key`, `get_database_stats`,
`get_celebrated_milestones`. They are ported rather than deleted — leaving them
un-ported would be code that silently reads SQLite after the engine moves —
but deleting them would make the port smaller and is the owner's call.

**`web/main.py._migrate_sqlite_users_to_duckdb` goes round the seam** and is
left alone: it is a startup one-shot copying `authorized_users` into DuckDB's
own `users` table, unrelated to what the port is for. It will need a decision
when the engine moves, because it opens `data/bot.db` directly.

### The Postgres adapter behind that port, and the switch

`bot/store_postgres.py` implements the same port against `app.*`, chosen by
`KS_BOT_STORE=postgres` (default `sqlite`; **an unknown value raises** — a typo
in the one variable deciding where the approval list lives should stop the bot,
not quietly point it at the old copy).

**One driver, on a loop of its own — decided by measurement.** `psycopg` was
already rejected once in writing (`requirements-dev.txt`: "two drivers'
behaviour to know, for one database"), so the bar was a number. On production
hardware, from inside a running event loop, 300 calls each:

| | read | write |
|---|---|---|
| SQLite, what the bot pays today | 0.252 ms | 0.259 ms |
| asyncpg through the bridge | 0.674 ms | 1.649 ms |
| asyncpg native `await` | 0.515 ms | — |

The bridge costs **0.16 ms**; Postgres costs about half a millisecond more per
call, against a Telegram round trip of ~100 ms. **It blocks the bot's event
loop** — 50 calls took 42 ms and a 1 ms ticker beside them ticked *zero* times
— and so does SQLite today; the change is duration, not nature. If traffic ever
makes that matter, the answer is an async port and forty `await`s, not a second
driver.

**The adapter returns SQLite's shapes on purpose.** Postgres disagrees on
exactly two things and both would have shipped silently: `notifications_enabled`
(handlers write `1 if enabled else 0`, asyncpg refuses an int for `BOOLEAN`) and
timestamps (aware `datetime` where the admin screens interpolate SQLite's text
straight into a message). Coerced on write, rendered on read.

**The proof is the same tests against both engines.** `test_bot_database.py`
is parametrised over sqlite and postgres, the latter skipped without
`KS_PG_DSN`. On the runtime: **87 passed, 1 skipped** — the skip being the
unparseable-timestamp case, which `TIMESTAMPTZ` makes impossible. A caller
cannot tell the two apart.

**Two guards, and they are the most dangerous code in the change.**
`replicate_bot_state` is an hourly *full replace* from `data/bot.db`; run after
the switch it would roll back every approval, once an hour, looking fine in
between. It refuses under `KS_BOT_STORE=postgres`, and `reconcile_bot_state`
stands down with it — `bot.db` becomes a frozen artefact, so every change since
the switch would read as a discrepancy the check itself created.

**The cache stays local under both engines** (revision 0009 left it out of
Postgres). `initialise()` therefore still creates the SQLite cache table — found
on a real database, where a missing `data/bot.db` made `cache_set` raise
`no such table: cache` and take a sales report with it.

**To switch**: `KS_BOT_STORE=postgres` on the bot **and** the web container —
`web/services/auth_service.py` reads the same store. Both need `KS_PG_DSN`; the
bot has none today. Roll back by removing the variable, but note that anything
written to Postgres in between does not come back to SQLite: the copy is
one-way and it was standing down the whole time.

### The bot's own state, and the only store with no backup

`data/bot.db` is the third store in this system. Measured 2026-08-27: it is in
**no backup at all** — `data/backups/` holds `analytics-*.duckdb`,
`deploy/daily_offsite.sh` globs the same pattern, and the Ark froze the
warehouse. One file, one disk, no copy, holding 24 approved users, 25
celebrated milestones and everyone's language choice. Nothing re-derives any of
it: an approval is a human's decision, a celebrated milestone is a message
already sent, a language is a preference nobody will re-enter.

Revision 0009 lands four of its five tables in `app` — `authorized_users`,
`user_preferences`, `celebrated_milestones`, `report_history` — which puts them
inside step 01's WAL archiving and nightly dump. That is worth having on its
own, before any question of switching the writer.

**`app`, not a schema of its own**, because `postgres/initdb/30-app.sql` already
decided it in writing, naming these exact rows. A `bot` schema would draw the
boundary along the writing container rather than the meaning, and this
repository's bot and web are one codebase, one image and one deploy — unlike
the shop, whose `tgbot` schema separates a genuinely different application.

**`cache` stays behind.** A ten-minute TTL cache holds no fact that can be
lost, and copying it would report a discrepancy every time an entry expired
between the copy and the comparison.

**It runs in the web container and does not touch the bot.** `core/bot_prefs.py`
opened this file read-only from there for the weekly report at the time (it
reads through the store port since 2026-09-06); the copy still does. So step 03's first half needs no environment variable on the
bot, no asyncpg on its path and no new way for it to fail — pinned by a test
that no module under `bot/` imports `core.pg`.

**Two conversions, both traps, both pinned.** SQLite has no boolean and no
timezone. `notifications_enabled` 1/0 becomes a real boolean — and NULL stays
NULL, because `core/bot_prefs.py` reads NULL as *on* and turning it into False
would mute everybody who has never opened settings. Timestamps are naive
strings from `CURRENT_TIMESTAMP`, **which is UTC by definition**; read as
anything else, all 24 approvals move by the container's offset, silently.
`core.pg_bot_state._convert` is the one home for both, and the comparison
imports it rather than converting a second time.

Full replace, all four in one transaction — the weekly report joins approvals
to mute settings to decide who is written to, so a half-applied pair computes
the audience from one snapshot's approvals and another's settings. `deny_user`
and `revoke_user` genuinely delete, so `full_replace=True` is right: a ghost in
`authorized_users` reads as an approved person who is not.

`reconcile_bot_state` reuses `compare_table` unchanged — it is pure and takes
two already-read dictionaries, so it does not care that one came out of SQLite.
Only the reader is new.

Verified 2026-08-27 against the production `bot.db` on a throwaway PostgreSQL
17.2: 51 rows copied in **35 ms**, comparison **6–9 ms**, zero findings. Then
broken seven ways — a deleted user, an invented milestone, and four changed
values including a timestamp shifted by three hours — each reported with the
column and the id; a second copy cleared all of it.

**A finding now says what its own table is.** `MirroredTable.origin_note`
carries the sentence explaining why a disagreement matters, because the shared
default was landing's story — "the same parsed tuple in the same call" — and
that is false for every replicated table. It was found by reading a real
finding, not by reading the code.

### Шаги 1–2 «Одной бронзы»: переключение чтения и строка на клиента

**Шаг 1 — `KS_READ_GOLD`** (`duckdb` по умолчанию | `postgres`; опечатка
роняет запрос, правило `KS_BOT_STORE`). **В проде `postgres` с 28.08** —
включён после разбора WARN-гейта (§32 хендоффа): summary и trend читают
Postgres, откат — переменная в .env и `up -d web`. Подменяет ровно два голдовых
примитива — итоги `/api/summary` и дневной ряд `/api/revenue/trend`
(`core/pg_gold_read.py`); сравнения, гранулярность, прогноз и вся логика выше
остаются в одном экземпляре. Перехват повторяет собственную маршрутизацию
DuckDB: источник без колонки канала (5) и линейные фильтры до Postgres не
доходят — флаг меняет движок и ничего больше. Ошибка чтения PG — fallback в
DuckDB с ERROR в логе; откат — вернуть переменную, без пересоздания.

**Шаг 2 — ревизия 0011**: `bronze.buyers`/`bronze.buyer_contacts` (ландинг —
`mirror_buyers` получает тот же разобранный батч, что `upsert_buyers`; синк
дельтовый, история — `backfill_buyers(store)`, ids-diff без курсора, сверка
гейтится на `backfilled_at`); `silver.order_lines` — VIEW, одно тело на два
движка в `core.sql_dialect.order_lines_select`, миграция заморозила
PG-рендер и тест сверяет их; `app.customer_profile` — витрина: строка на
retail-клиента, **факты без уровней** (пороги — политика кампаний и живут в
коде), пересобирается целиком тем же тиком, что Gold. Её сверка
(`reconcile_customer_profile`) пересобирает и проверяет материализацию об
тот же снимок Silver в одной REPEATABLE READ транзакции — допуск ноль
законен, потому что обе стороны видят один Silver. Ключ контактов —
натуральная тройка, sequence-id DuckDB намеренно не переносится. Куда витрине
переезжать (этот Postgres или магазина) — открытый вопрос владельца; она
derived, переезд стоит одну пересборку.

Проверено на одноразовом PostgreSQL 17.2 (миграции 0001→0011 + initdb):
читатели шага 1 сходятся с независимо посчитанной правдой по каждой ячейке;
витрина 38 клиентов, SUM(ltv) == Silver, сверка ноль; зеркало покупателей —
ноль находок на честной копии, три подложенных дефекта пойманы, лечится
зеркалом и бэкфиллом.

### Gold в ClickHouse — шаг 4 «Одной бронзы», копия на обкатку

`core/ch_gold.py` копирует `gold.daily_revenue` из Postgres в ClickHouse
(база `gold`, та же таблица) ежечасно и **ничего не вычисляет** — uniqExact и
настоящая деривация приезжают с шагом 5. Что обкатывается: связность (web
вошёл в сеть `ks-data`, где живёт `ks-clickhouse` без опубликованных портов),
форма доставки (staging + атомарный `EXCHANGE TABLES`, потому что транзакций
нет и TRUNCATE+INSERT по живой таблице дал бы читателю пустую), и суточная
сверка на слое `mirror_landing`.

- **Включается `KS_CH_URL`** (`http://ks-clickhouse:8123`); без него часовой
  шиппер стоит, а суточная сверка с OD-08 (a) не молчит: слой
  `mirror_landing` получает `gold_values_unwatched` (см. шаги 5–6). Пароль
  `ks_app` (пользователь уже заведён платформой с правами на `bronze/silver/
  gold.*`) — `KS_CH_PASSWORD` из `.env`; кредензии едут заголовками, не в URL.
- **Счётчик до обмена**: короткая заливка в staging не подменяет живую
  таблицу — EXCHANGE случается только при равенстве числа строк.
- **Сверка сначала отгружает, потом читает обратно** и сравнивает с тем же
  снимком в памяти: PG gold пересобирается каждые ~10 минут и не несёт
  таймстемпов строк, так что сверка «свежести» врала бы ежедневно. Свежесть —
  дело вотермарки (`meta.mirror_state`, строка `clickhouse.gold_daily_revenue`);
  сверка меряет верность копии. Допуск ноль **включая** `avg_order_value` —
  цент прощается двум движкам, которые считают, а не копии.
- Недоступный ClickHouse — WARN-находка, не исключение: одно хранилище не
  должно валить слой остальных сравнений. Рядом с ней — `gold_values_unwatched`.
- Проверено против настоящего ClickHouse 24.8.14.39 (версия прода): 10 950
  синтетических ячеек — отгрузка 0.28 с, чтение 0.06 с, чистый круг — ноль
  находок; три подложенных дефекта (цент, потерянная и выдуманная ячейка)
  пойманы с правильными именами; повторная отгрузка лечит.
- **Выкладка требует `--force-recreate web`** — членство в сети меняется
  только пересозданием контейнера, restart его не даёт (урок D3).

### Шаги 5–6: ClickHouse считает Gold сам, и архив наследуется

Шаг 4 (копия голда) превзойдён и оставлен фолбэком. Часовой `ch_sync` теперь:
`core/ch_silver.py` отгружает **silver целиком** (staging + EXCHANGE, копия —
допуск ноль на круге) и **деривит gold внутри ClickHouse** из этого silver;
`core/ch_history.py` дописывает `history.order_versions` поверх собственного
`MAX(id)` (append-only, курсора нет — вотермарка читается из самого CH).

**Третьего диалекта мер нет** — и это снимает возражение И1: `GOLD_MEASURES`
написаны как `COUNT(DISTINCT CASE WHEN …)` / `COALESCE(SUM(CASE …), 0)`, и
ClickHouse 24.8 понимает этот же текст с той же семантикой (проверено, не
предположено). `derive_gold_sql` рендерит выражения дословно из того же
словаря, что PG и DuckDB; тест провалится, если кто-то перепишет меру «под
ClickHouse».

Суточный вердикт в `dq_mirror_landing`: круг silver (ноль), **gold двух
движков друг против друга** — независимые агрегации одного silver, ноль
всюду, кроме документированного цента на `avg_order_value` (движки по-разному
делят DECIMAL), — и бакеты архива (ноль ниже вотермарки; находка о потерянной
строке **не лечится** — архив чинит человек). Проверено на живой паре
PG 17.2 + CH 24.8.14.39: 730 дней, 5 225 ячеек в обоих движках, **0
расхождений**, включая 374 дня, где uniqExact-свёртка законно не равна сумме
мелких строк; сверка 0.7 с; архив — 12 000 унаследовано + 40 инкрементом,
удалённая строка поймана и находка не исчезает при повторной сверке.

**ClickHouse обязателен на параллельный период** (OD-08 (a), 30.09.2026).
После шага 13 это единственная независимая переагрегация Gold: `reconcile_gold`
стоит, а `pg_gold_internal_check` спрашивает Postgres о нём самом. Поэтому
прогон, в котором двух Gold не сравнили, говорит это одним именем —
`gold_values_unwatched` на слое `mirror_landing`: без `KS_CH_URL`; при сбое
отгрузки или деривации (своего гейта возраста у этой сверки нет — она
отгружает то, что сравнивает, так что сбой отгрузки и есть устаревшая копия);
при сбое чтения обратно; при двух пустых Gold; при исключении или прогоне,
не дошедшем до сверки — эти два файлит сам джоб, сверке нечего вернуть.
**WARN, пока Gold DuckDB ещё сравнивается с Postgres, CRITICAL — когда нет**:
`warehouse_checks_stand_down()`, тот же предикат, на котором джоб снимает
`reconcile_gold` (Postgres деривит один, или путь назад держит сравнения), а
нечитаемый предикат — тоже CRITICAL. Такой прогон держит `ch_silver_roundtrip`
и `ch_engines_gold_*` как были, а не объявляет их решёнными; и архив так же:
прогон, не посчитавший бакеты (`ch_history_unreachable`, `[]` без
`KS_CH_URL`, исключение), держит `ch_history_buckets`, который закрывает
только человек (`history_unverified_conditions`). `KS_CH_URL` уже в условиях
шага 13 (`ch_url`); `reconciliation_ch` — в канарейке. В проде сегодня сверка
идёт каждое утро, так что находки нет, а будь она — WARN в дайджест, не
страница; а утро, на котором ClickHouse не сверили с KeyCRM, пейджит
канарейка через `reconciliation_ch` (см. «How a failure reaches a human»).
Что за 14 дней такого утра не было, говорит не снимок `/api/health`, а
`deploy/stage4_soak/22_reconciliation_ch_history.sql` — его прогоняют на хосте
до мержа. Соак D8 читает прогон, где на производных таблицах только «ClickHouse
не сравнивал» (`gold_values_unwatched` и WARN'ы самой сверки), как UNKNOWN, а
не как расхождение хранилищ.

**База `history` в прод-CH заведена и работает** (проверено 17.09.2026;
до этого здесь стояло «не существует», и запись читалась как «шаг 6 ещё не
включён»). Платформа выполнила те две строки — `CREATE DATABASE history` и тот
же GRANT, что у bronze/silver/gold. Замер: 52 448 строк, id 1…52 448 без
пропусков, 6.75 МиБ; вотермарка `clickhouse.order_versions` свежая,
`failures_since_ok = 0`, суточная `reconciliation_ch` — PASS.

**Где проходит граница с платформой.** `ks-clickhouse` — контейнер проекта
`ks-data-platform`, и на этом хосте живут ещё два чужих проекта. Но базы
`bronze`/`silver`/`gold`/`history` внутри — **наши**: грант `ks_app` несёт
полный набор (`SELECT, INSERT, ALTER, CREATE/DROP TABLE, TRUNCATE, OPTIMIZE`),
и владелец подтвердил 17.09, что право писать и удалять там за нами, как и
перезапуск самого контейнера. Что остаётся платформе — конфигурация сервера и
его тома. То есть граница между **сервером** и **нашими данными в нём**, а не
по контейнеру целиком.

`history.order_versions` — append-only и без верхней границы по замыслу. Это
не нарушение правила «всё, что копится, объявляет свой предел»: при ~100
строках в день и ~190 байтах на строку это ~7 МБ в год, и предел здесь —
арифметика, а не политика. Если темп когда-нибудь изменится, ретеншен тут наш
и делается на месте.

### Вкладка /traffic: слой, который убрали, а не скопировали

`KS_READ_TRAFFIC=postgres` переводит все пять методов вкладки. Две из них
читали `gold_daily_traffic`, которой в Postgres нет — **и не будет**.

Обе читающие функции этот Gold **сворачивают**: `SUM(orders_count)` и
`SUM(revenue)` по подмножеству его же первичного ключа. А `silver_order_utm`
ключевана `order_id`, то есть строго 1:1 с заказом, — значит заказ лежит ровно
в одной ячейке, и свёртка ячеек есть та же арифметика, что агрегат по строкам
под ними. Это совпадение **по построению**, а не «до копейки по замеру», как у
бренд-таблицы `/marketing`. Поэтому оба движка читают `silver_orders LEFT JOIN
silver_order_utm` (одно тело, правило 1), и хранилище не получило ни
производного слоя, ни деривера внутри голдового тика, ни сверки, которая бы его
стерегла. Слой, которого нет, не может протухнуть в одном движке и не протухнуть
в другом.

Видимое следствие: **у `gold_daily_traffic` не осталось ни одного читателя**,
а перестраивается она каждый складской тик. Это не безобидно — именно она была
крупнейшим источником роста файла DuckDB: 3.86 M хранимых строк за 5 781 живой,
667× амплификация, ~90 МБ в день до починки инкрементальной перестройки.
Выбросить её — решение владельца, здесь оно намеренно не принято.

**Пиксель не называет площадку.** `_fbp` и `ttp` — наши собственные
first-party куки: пиксели Meta и TikTok ставят их любому посетителю, кто бы
его ни привёл. До 08.09.2026 классификатор возвращал по ним `facebook` и
`tiktok` соответственно, то есть канал выбирался порядком двух `if`, а не
поведением посетителя. Замер на проде: **1 969 из 2 210** розничных
pixel-only заказов за 180 дней (**₴4.55 млн**) несут **обе** куки — вся эта
сумма лежала в слайсе «Facebook» на графике платформ и лежала бы в «TikTok»,
будь строки написаны в другом порядке. Теперь `pixel_only` отдаёт платформу
`other`; какой пиксель сработал, по-прежнему видно в колонке улик
(`_build_evidence`), где утверждению такой силы и место. Тип трафика не
тронут: прогон обеих версий по всем комментариям за 180 дней даёт 2 210
переносов платформы и **ноль** смен `traffic_type`.

**`unattributed` — не `other`, и это две разные незнания.** `other` —
источник, о котором нам *сказали*, но который мы не научились называть (`qr`,
`novaposhta`, `rivo`: ~26 заказов за 180 дней). `unattributed` — когда не
сказали ничего: сработали наши пиксели и ни один параметр не говорит,
откуда покупатель. Пока они делили один ключ, ₴1.36 млн «не знаем» лежали
под словом, читающимся как «прочие мелкие каналы». Ключ живёт в семи местах:
две ступени классификатора, `_PLATFORM_EXPR`, валидация платформ в API, два
списка фильтра на фронте и `traffic.platform.unattributed` для отчёта.

Подпись сектора и ось графика держат короткое «Источник не определён», а
скобку про пиксели несут легенда и подсказка — там есть место, а на секторе
круговой диаграммы его нет.

`platform` материализована в `silver_order_utm`, а перепарсинг трогает только
заказы с изменившимся `updated_at` — поэтому правка правил классификации
доезжает до экрана лишь после `POST /api/traffic/reclassify` (он же доносит
результат до Postgres через `reparse_router`). Без этого вызова старые
строки продолжают называть Facebook то, что классификатор уже так не считает.
Проверено на проде 09.09: со сменённым классификатором, но без перепарсинга,
`unattributed` показывает 25 заказов — ровно те, у кого строки атрибуции нет
вовсе и кого размечает SQL-подстановка, — а 600 Facebook и 45 TikTok стоят на
месте.

**Прежде чем переклассифицировать — прогон вхолостую и снимок** (DN-15, к
решению OD-06). Реклассификация и будущий полный разбор в Postgres (шаг 9)
перезаписывают единственную запись того, что вкладка и понедельничный отчёт
говорили о старых заказах, а истории вердиктов нет нигде. Парсер и
классификатор вынесены дословно в `core/utm_classify.py` — миксин держит те же
имена на те же функции, а `tests/unit/test_utm_classify_golden.py` хранит
выходы до переноса литералами: правка правила — это решение о
реклассификации, а не рефакторинг, и кортеж в голдене меняется в том же
коммите. `scripts/utm_reclassify_dryrun.py` читает `bronze.orders`,
`silver.order_utm` и `silver.orders` одной READ ONLY REPEATABLE READ
транзакцией, пересчитывает каждый вердикт в памяти и раскладывает изменения
по причинам: `rule_change` (достаёт только реклассификация — это и есть вопрос
OD-06), `orphaned` (комментарий опустел, DELETE уберёт строку навсегда),
`pending` и `unparsed` (их и так разберёт следующий тик) — с заказами и
гривнами за 12 полных недель, для `retail` и для всех. `--snapshot` сохраняет
таблицу целиком в gzip-CSV и печатает sha256. **Он не пишет ничего, кроме
этого файла**, и это держится не словами: `KS_PG_DSN` он не читает вовсе
(только `KS_PG_READONLY_DSN` или `KS_READONLY_PASSWORD` для `ks_readonly`),
логин с любым правом записи — своим или роли, в которую он может `SET ROLE`,
включая последовательности, `CREATEROLE` и `CREATEDB` — отвергается до первой
строки (что у `ks_readonly` после всех ревизий таких прав нет, CI спрашивает
каждым прогоном), а перед концом транзакции сервер спрашивают, выдал ли он ей
номер — номер выдаётся на первой записи, и выданный откатывает прогон. Тесты
гоняют весь `main()` в свежем интерпретаторе под audit-хуком
(`tests/write_audit.py`) с `KS_PG_DSN`, направленным в пустой порт, и требуют,
чтобы DuckDB — его записи хук не видит, они идут из C++ — не был даже
загружен; и заставляют скрипт писать в настоящий Postgres — сервер отказывает,
а без READ ONLY запись откатывает проверка номера.

**Первый оператор транзакции — `LOCK ... IN ACCESS SHARE MODE`** на все три
таблицы. Снимок REPEATABLE READ фиксирует первый *читающий* оператор, а
TRUNCATE не MVCC-безопасен: снимок, взятый до коммита тика Silver и
дождавшийся его, читает `silver.orders` или `silver.order_utm` пустыми.
Воспроизведено с настоящим `rebuild_silver`: exit 0, «no orders» за все
12 недель, а с `--snapshot` — файл на 0 строк, «сверенный» с таблицей.
LOCK снимка не фиксирует и пережидает тик; пустой Silver рядом с непустой
бронзой всё равно роняет прогон (exit 1). Платформы в отчёте — в именах
вкладки: Google делится на `google_ads`/`google_organic` одной функцией
`core.utm_classify.tab_platform`, которую зовёт и `get_traffic_analytics`.

**Проверен на той самой копии 31.08** (46 979 заказов, 32 656 вердиктов,
загружена в одноразовый PostgreSQL 17.2 и прочитана как `ks_readonly`): 3 769
вердиктов, ровно число из дизайна, все `rule_change` и все только по
платформе — 2 961 `pixel_only/facebook`, 318 `pixel_only/tiktok` и 490
`unknown/other` уходят в `unattributed`. За 12 недель это 1 392 розничных
заказа и ₴3.13 млн, переезжающие в «источник не определён». Снимок
удерживается 0.45–0.68 с, весь прогон ~1.8 с; файл побайтно одинаков между
прогонами и возвращается `COPY FROM` без единого расхождения. Цифры на сегодня
даёт только прогон на проде — это и есть вход для OD-06.

**Три решения на карточке ROAS, каждое — утверждение, которого не считали.**

- Выручку карточка берёт из **своего же** ответа (`blended.revenue`), а не из
  `/api/summary`. Тот применяет фильтры шапки, а `/api/traffic/roas` не
  принимает ни одного — при выбранном Instagram сверху стояло ₴1 356 330, а
  ROAS под ним считался от ₴3 602 560, вдвое с лишним. Сам ROAS остаётся
  общим по аккаунту (расходы вносятся по платформе, а не по источнику), и при
  активном фильтре источника карточка это говорит вслух.
- `bonus_tier` — **ключ** (`plus_30`…`none`) и `None`, когда делить было
  нечего. Была английская фраза, которую фронт печатал как есть и сравнивал с
  собственным *переведённым* ярлыком, так что подсветка строки работала только
  в английском; а дефолт «No bonus» при нулевых расходах — а они нулевые
  всегда — выводил на экран вердикт из пустоты.
- Ввод расходов ходит в `/api/expenses`, то есть во вкладку **expenses**, а
  страница гейтится на `traffic`. Форма, кнопка и корзина обёрнуты в
  `ProtectedSection` (`edit` и `delete` соответственно), а отказ записи теперь
  виден тостом: раньше единственная запись на вкладке падала молча.

**Фильтры шапки, которых вкладка не применяет, на ней не показываются.**
`utils/pageFilters.filtersForPath` — одна карта на два читателя: `FilterBar` не
рисует контрол, `useQueryParams` не кладёт параметр в запрос. Для `/traffic`
это `period`, `sales_type`, `source_id`; категория, бренд и промокод не
доезжали до эндпоинтов никогда (FastAPI молча роняет лишние query-параметры), и
выбор их не менял на странице ровно ничего. Остальные вкладки намеренно не
тронуты — сузить любую из них значит сперва проверить её эндпоинты.

**Ревизия 0018** привозит две таблицы, которых действительно не хватало:

- `silver.order_utm` — **отгружается, а не выводится**: её тело не SQL, а
  питоновский парсер (`refresh_utm_silver_layer` разбирает `manager_comment`
  регулярками, `_classify_traffic` выносит вердикт). Вывод в SQL был бы второй
  реализацией классификатора, расходящейся с первой в день правки любой из них
  — аргумент `app.manager_classifications` этажом ниже. Едет **тиком Silver**
  (`core/pg_order_utm.py`), а не часовой семьёй: отсутствующая строка UTM не
  читается как «нет данных», она проваливается через `COALESCE` в `organic` и
  двигает paid/organic-разрез на графике. Отгрузка **последняя в тике**, после
  всех деривации — из неё в Postgres ничего не выводится, и её отказ не должен
  стоить выручковый Gold, который `/summary` и `/marketing` читают с 28.08.
- `app.manual_expenses` — **ноль строк в проде** (замер 07.09.2026), то есть
  расходная половина ROAS никогда не имела входа: `has_spend_data` всегда false,
  `blended_roas` всегда null. Таблица заведена всё равно, потому что запрос не
  может присоединить несуществующую таблицу, и оба движка обязаны отвечать
  одинаковым «нет данных», а не ошибкой из одного из них. Едет часовой
  `replicate_operational`; час отставания на цифре, которую сейчас никто не
  вводит, — не проблема, а если владелец начнёт вводить, починка в одну строку:
  отгружать тиком Silver рядом с UTM.

**Сверка UTM читает обе стороны целиком, а не отпечатками.** Отпечаток
суммирует числа и *длины* текста — верный инструмент для 46 487 заказов, и
неверный здесь: таблица почти целиком текстовая, и переименование кампании
`spring` в `autumn` не меняет ни числа, ни длины. 32 905 строк, допуск ноль,
раз в сутки, слой `mirror_landing`.

**Четыре дефекта, найденных при переносе** — каждый настоящий и на DuckDB:
`any_value(u.utm_source)` по колонке, по которой не группируют; `SELECT
COUNT(*) FROM ( ... )` без имени производной таблицы; `ORDER BY` без добивки
под `LIMIT`/`OFFSET`, из-за чего две кампании с равной выручкой попадают на обе
страницы или ни на одну; и `SUM(revenue)` по выручковому Gold без предиката
свёртки — в Postgres это считает каждый заказ дважды и **делит ROAS пополам**.

**Перепарсинг UTM обязан отгружаться сам.** Три админских эндпоинта и
`scripts/backfill_utm.py` переписывают `silver_order_utm` в DuckDB, **не помечая
склад грязным**. Пока вкладка читала DuckDB, это было безобидно — она читала ту
самую таблицу; теперь она читает Postgres, и без отгрузки админ, только что
сменивший правила классификации, увидел бы на странице предыдущие. Все четыре
зовут `reparse_router` (DN-19), который при `KS_UTM_PARSE=duckdb` — по
умолчанию — и есть `ship_after_reparse`: под `PG_LAYER_LOCK` (планировщик
отгружает ту же таблицу, и две переплётшиеся TRUNCATE+INSERT оставят ту копию,
что закоммитилась позже, под свежей OK-вотермаркой — включая копию, прочитанную
*до* перепарсинга), с ограниченным ожиданием лока (под ним ходит сетевой
ClickHouse-шиппер, а три из четырёх вызывающих — HTTP-хендлеры) и никогда не
бросая, потому что работа в DuckDB уже удалась. Тест обходит AST: функция,
зовущая `refresh_utm_silver_layer`, обязана звать роутер.

**Отказ шиппера пишется в вотермарку** (`_record_failure` из `core/pg_landing.py`),
и внутрь гарда занесены `get_pool` с `require_revision`: `SchemaVersionError` от
деплоя web раньше migrate — именно та узнаваемая поломка, которую сверка иначе
доложила бы как тысячи расхождений строк, то есть симптомом вместо причины.
Соседние *деривированные* слои (`rebuild_silver`, `rebuild_gold`) так намеренно
не делают и правы: их можно пересчитать, а это копия состояния, которое есть
только у DuckDB.

**`KS_UTM_PARSE=postgres` переносит разбор в Postgres** (шаг 9, DN-19; по
умолчанию `duckdb`, и тогда не меняется ничего). Нужен `KS_PG_DERIVE=own`:
последний шаг деривации — `parse_incremental_locked` из `bronze.orders`, в
своём `try` после строки журнала, так что сбой Gold его не останавливает, а
его сбой пишется в вотермарку `silver.order_utm`, а не в вердикт деривации.
`PG_LAYER_LOCK` он берёт сам, уже после того как деривация его отпустила:
лок не реентерабелен, и внутри `async with` деривации разбор ждал бы сам себя
`LOCK_WAIT_S` и записывал отказ каждый прогон. Тик DuckDB перестаёт
отгружать, двери зовут `parse_full` (reclassify и CLI — те, что делают
DELETE) или `parse_incremental_locked`, а `/api/health` публикует
`silver.order_utm` среди наблюдаемых таблиц со своим `max_age_s`. Разбор
DuckDB не останавливается нигде, поэтому откат — снять переменную: следующий
тик отгрузит копию, которая всё это время была актуальной. Неизвестное
значение или `postgres` без `own` работает как `duckdb`, публикует ошибку в
`utm_parse` и будит канарейку `utm_parse_mode_invalid` — web не падает, он
единственный синкер. Рычаги находок `pg_order_utm_*` следуют режиму: при
`postgres` строка «→» — вотермарка `silver.order_utm`, затем
`POST /api/traffic/refresh` или `/api/warehouse/refresh` (деривация кончается
тем же разбором), а копию комментария в DuckDB она не предлагает — этот
разбор DuckDB не читает. При `duckdb` и строка, и описания прежние байт в
байт. Тест обходит `core/`, `web/`, `scripts/`, `bot/` и
`deploy/` до неподвижной точки и читает каждое имя через `import … as`,
которым его связали: ни одна функция вне `core/pg_order_utm.py` не доходит до
отгрузки иначе как из ветки `duckdb` проверки `parses_in_postgres()`. Чего
обход вызовов не видит — отгрузку, переданную значением без вызова, и имя
отгрузки строкой (`getattr`, импорт по пути), — запрещено отдельными тестами.
Имя, вычисленное во время работы, не увидит никакой обход.

**Индексов по `platform` и `traffic_type` на этой таблице нет** (ревизия 0019
сняла их). Каждый предикат вкладки оборачивает колонку в `COALESCE` с запасным
значением из `silver.orders.source_id` — такое выражение не есть `u.platform`, и
индекс по одной этой таблице его не обслужит. Проверено планом на проде
(`Seq Scan on order_utm`, фильтр после джойна) и грепом: сырых предикатов нет
нигде. `pg_stat_user_indexes` показывал 3 скана — это `count(*)` брал самый
маленький индекс для index-only scan, а не предикат.

### Вкладка /expenses: половина уже стояла, и один веер ценой ₴314K

`KS_READ_EXPENSES=postgres` переводит шесть читающих методов, и они делятся
надвое. **Три** (`list_expenses`, `get_ad_spend_by_platform`,
`get_expenses_summary`) читают только `manual_expenses`, которую привезла
ревизия 0018 ради ROAS на `/traffic`, — им миграция не понадобилась вовсе.
**Ревизия 0020** добирает две настоящие: `bronze.expenses` (~15 тыс. строк) и
`bronze.expense_types` (27). Обе из KeyCRM, поэтому **зеркалируются**, а не
реплицируются — противоположность `manual_expenses`, которая лежит схемой выше
именно потому, что число, введённое человеком, никем не выводится.

**Дефект, найденный переносом и живший на проде.** `get_profit_analysis` читал
`FROM orders o LEFT JOIN expenses e` и суммировал `o.grand_total` по строкам
джойна: заказ с двумя расходами давал выручку дважды. Таких 157. Замер на
прод-каталоге — **₴16 433 644.56 за 90 дней против истинных ₴16 119 279.06,
завышение ₴314 365.50, а 28.07 — ₴62 000 за один день**. Свёртка расходов до
джойна воспроизводит и выручку, и сумму расходов по каждому из 90 дней.
Починено в общем теле: переносить веер «как есть», чтобы два движка сошлись на
неверном числе, — худший из доступных исходов.

**Разбор пришлось перенести раньше зеркала.** `expense_types` — первая
ландинг-таблица, где разбор есть настоящее преобразование: KeyCRM отдаёт часть
имён ключами локализации (`dictionaries.expense_types.delivery`), и
отображаемое имя строится из алиаса. Зеркало, читающее payload заново, дало бы
двум хранилищам разные *названия* расходов — расхождение, которое суточная
сверка доложит, не умея сказать, кто прав. Перенесено в `core/landing_rows.py`
и сверено построчно со старым кодом; попутно закрыт отказ на `"expenses": null`.

**Оба тяжёлых метода читают Silver.** Они джойнились к сырым `orders` и
сужались `EXISTS`-ом к `silver_orders`, беря оттуда только киевскую дату и
источник — а Silver несёт и то, и другое колонками, плюс сам `sales_type`.
Замена `status_id NOT IN (15,18,19,21,22,23)` на `NOT is_return` проверена на
всём каталоге: **47 338 заказов, ноль расхождений**.

**Записи доезжают сразу, а не за час.** Форма пишет DuckDB, страница после неё
читает Postgres, поэтому отставание, которое ревизия 0018 сознательно приняла,
здесь перестаёт быть приемлемым. `add/update/delete_expense` зовут
`replicate_after_manual_expense` — 116 мс по замеру, **вне лока стора** и без
права упасть, потому что запись в DuckDB уже состоялась.

**Сверка**: `expense_types` идёт с каталогом (27 строк, отгружается целиком
каждый синк, так что первая успешная отгрузка и есть его история).
`bronze.expenses` едет дельтой, поэтому гейтится на `backfilled_at` как заказы,
имеет `core/pg_expense_backfill.py` и `POST /api/mirror/backfill/expenses`, и
**читается целиком, а не отпечатками**: 15 тыс. строк дёшево сравнить точно, а
`description` — текст, переписывание которого в ту же длину отпечаток не видит.
Часы сверки — `synced_at` (бухгалтерия DuckDB), не `created_at`: штамп KeyCRM
прочитал бы только что синканный старый расход как потерянный.

### Every read that falls back to DuckDB is counted

Every ported tab answers from DuckDB when Postgres (for cohorts, ClickHouse)
fails, and until DN-20a each said so in its own words — or, in
`pg_expenses_read.backfilled()`, in words no grep for "falling back to DuckDB"
could find. After step 13 each of those paths serves frozen Silver and Gold,
and after the first Sunday compaction empty ones, behind a page that looks as
if it works. So every such site calls `core.read_fallback.fall_back(surface,
exc)`: one uniform ERROR line carrying that phrase, and a per-surface counter
`/api/health` publishes as `read_fallbacks {surface: {count, last_at}}` — no
exception text, the endpoint is public. **Under the default, nothing a read
returns changed.**

**The week `off` waits for is proven by the canary, not by the log** (OD-07
(a), 2026-09-30). The condition for `KS_READ_FALLBACK=off` is a covered, clean
168 h with no read answered from DuckDB. The plan measured it by grepping web's
log from its oldest surviving line, but every deploy recreates web and loses
the counters and the log together, so a week that spans a deploy could not be
called covered at all. The evidence is now made by the process that outlives
web's and kept in Postgres. The canary pages two keys, both WARN, and the
Alert Gate journals a delivered page in `app.alert_series`/`alert_events`:

- **`read_fallback_used`** whenever `read_fallbacks` is non-empty — an engine
  failed and DuckDB answered — naming the surfaces and each `last_at`. The key
  is the condition alone, and the page stands until web restarts, because
  only a restart empties the counters.
- **`read_routed_to_duckdb`** whenever `read_fallback_mode.misconfigured` or
  `no_engine` is non-empty, under `duckdb` only. Those reads are served from
  DuckDB on every request and **counted by nothing**, so `read_fallbacks`
  stays empty over them, and `off` refuses every one. The review of OD-07
  reproduced the week passing over a lost `KS_CH_URL` while cohorts read
  ClickHouse. `no_engine` is the cohorts' switch not naming ClickHouse, their
  only engine under `off` — a route no missing address names, published by
  `read_fallback.no_engine_routes()` from `ENGINE_ONLY`. A test holds the two
  lists to exactly what `off` would refuse, for every switch and address.

**A probe that read nothing clears nothing.** The canary calls `resolve_group`
on every tick, and a probe that got no payload — a first-time blip the canary
holds back, the 05:15 freeze, a 10 s timeout — used to announce a standing
`read_fallback_used` resolved and page it again as a new incident on the next
probe, agent and all. `CanaryResult.unjudged_keys` names the OD-07 keys a
probe could not judge for want of their block, and `canary_job` keeps them in
`still_firing` — and the buyers step's two keys, `buyer_sync_stalled` when the
probe read no `buyer_sync` block and chain 4's CRITICAL also when it read no
entry for the chain: under chain 4 a stall of the only writer of buyers
outlives the freeze and every recreate, and both keys fire for it. Chain 3's
`orders_sync_failing` is held the same way when the probe read no entry for
the chain, or read it on Postgres with no `sync_step`: the Postgres hang
that fails the only writer of orders hangs `/api/health` too (batch-E
review). Every other payload-derived key keeps today's behaviour, though
they share the flaw.

A page proves a fallback, but it cannot prove that a quiet week was looked at.
So every probe that read the block also rewrites one row,
`watch:read_fallbacks` in `app.alert_series` (`core.alert_archive.record_watch`).
It is event-kind, not in the REGISTRY, and read by nothing else that reads the
series. `first_fired_at` is the moment since which every probe read no read
served from DuckDB. A probe that reads one restarts it at its own time.
**What can restart it otherwise is the unread tail of a replaced web process,
not the gap between probes.** The counters cover a process from its start, so
a probe of the same process reads everything since the last one, however long
the bot was away. Only a process replaced in between loses its counts after
the last probe, and that tail ends at the latest at the new process's start,
`now() - uptime_seconds`. Over 35 min restarts the run at the next clean
probe. The first form judged the gap, and the Sunday compaction alone —
last probe, stop, compaction, web's startup, the bot's first probe failing
before web answers — could pass 35 min every week. One row, so it declares
its own bound. `deploy/stage4_soak/22_f1_read_fallbacks.sql` (F1) judges the
page journal and the watch **a day at a time**, like every check in the
report:

- **FAIL**: a page of either key fired, escalated or resolved in the last
  24 h, or is still standing however old, or the watch's latest probe, inside
  the day, found a read served from DuckDB. The last one counts because the
  Gate journals only a delivered page.
- **UNKNOWN**: no watch row, a watch not written for 35 min, or one clean for
  less than the day.
- **PASS**: otherwise, and its detail says how many of the week's 168 h the
  clean run has covered. "Covered" is the licence for `off`. It was 168 h
  deep at first, which made F1 UNKNOWN for a week after every deploy and every
  reset, and one fallback a week of FAIL, in the daily report chain 1's soak
  reads too.

What it cannot see is a fallback in a web process after the canary's last
probe of it and before it was replaced: at most the tail above. **A refusal
under `off` is never paged as a fallback.** It served nothing from DuckDB, and
counting it would fail the soak that licenses the flip. It is `read_refused`
(WARN), judged on recency: a refusal in the last 30 minutes. A refusal left no
wrong number behind, only a 503 somebody saw, so the page stands while reads
are being refused, not until a restart. Production on 2026-09-30 published
`read_fallbacks: {}` and `misconfigured: []` under `duckdb`, with cohorts on
ClickHouse and its address, so the merge pages nothing at the deploy, and F1
reads UNKNOWN on the first report, before the watch holds a day. What happens
over a web process's life was not measured: both reads of `/api/health`
caught a process under two hours old, and the fallbacks of older ones went
with their logs. So a routine fallback — a Postgres timeout under a heavy
job — would page after the merge and hold F1's week at zero until explained.
The plan's grep over the oldest surviving log line answers it on the host.
The first one came on 2026-10-06 06:18 UTC, a cohort read: ClickHouse's
server is shared with other projects, its total memory limit was spent, and
its OvercommitTracker stopped our query with code 241. Our own hourly ship had
finished 18 minutes earlier. `ch_cohorts.fetch` now asks again after 1 s and
3 s on code 241 alone, and only the last refusal reaches the fallback.

`KS_READ_FALLBACK` (`duckdb` default | `off`) is read in `configure_modes()`,
before the boot sync. **Under `off`, `fall_back` raises `ReadUnavailable`**
(DN-20b) instead of letting its caller read DuckDB, and one exception handler
in `web/main.py` answers it, from any route, with a 503 carrying `surface` —
the cause stays in the log. One raise, so it refuses every router an HTTP
route reaches, the filter bar's lookups included, not only the tabs the plan
named. A refusal is not a fallback: counted apart, logged without the phrase
a fallback is grepped by, published as `read_fallback_mode.refused` under `off`
— and under `duckdb` only once a write chain's read was refused, below — so
`read_fallbacks` stays empty there and the block keeps its shape under
`duckdb`. `off` refuses a *failure*, never a switch left at `duckdb` —
except the cohorts, which have no Postgres body: under `off` a live
ClickHouse answers them or nobody does (`no_engine`). A switch naming an
engine this process has **no address** for (`KS_READ_TRAFFIC=postgres`
without `KS_PG_DSN`) counts as a failure there: the gate is simply false and
DuckDB answers with nothing to count, so every `enabled() and available()`
gate first calls `no_address(surface, switch)`, which refuses under `off` and
does nothing under `duckdb`. A lost DSN line is then a 503 on every tab, not
frozen numbers behind all of them. Chosen over having the canary page
`misconfigured` under `off`, which would have kept serving DuckDB until
somebody read the page. The trend's forecast
overlay, which already degrades to "no forecast" on any failure, drops under
a refusal and the chart answers; the forecast's own endpoint,
`/api/revenue/forecast`, is not an overlay and answers 503 — it used to read
an outage as "Forecast not available yet", a model nobody had trained.

**Everything else a refusal can reach answers it itself** (DN-20c), and
never with a DuckDB number. The two weekly reports defer: no message, no
ledger row, and tomorrow's tick asks again. The Monday goals job
(`seasonality_calc`) reads everything before it writes anything, in one
transaction (chain 7b-1), so a refusal at any of its reads defers it whole
instead of leaving new indices beside last week's `growth_metrics`. Training is skipped
and the previous model stands; `_train_impl` lets the refusal through its
`except Exception`, so `POST /api/revenue/forecast/train` answers 503 like
every route rather than 200 `"status": "error"`. The search index and the
buyers step skip their step for the tick with the watermark held, and the
tick goes on to offers and stocks; the boot contains each of them. The
assistant's tools return a named `data_unavailable` result, which is what
the model reads instead of numbers. A job puts
`{"reason": "read_unavailable", "surface"}` in its result
(`read_fallback.answered`), so `/api/jobs` shows a refusal, not a quiet run —
except the incremental sync, whose `stats` are summed and carry no strings:
the buyers step records the refusal and passes it on, and the tick names it
at the call and skips the step, so it shows in the log, in `buyer_sync`'s
error class and in `/api/health` `read_fallback_mode.refused`.
Several of these never had a fallback — the weekly report, the training
input, the buyers step and the index read Postgres or nothing — so for them
a Postgres failure is still their own error, as before; `off` adds only the
switch with no address. **That list is derived, not remembered** —
`tests/unit/test_read_fallback_consumers.py` walks up from every
`fall_back`/`no_address`/`no_engine` to the entry points nothing in the
repository calls (`NON_HTTP_CONSUMERS`), then back down through the `try`
each call sits in, and pins where every consumer's refusal stops
(`ANSWERS`): a refusal that could leave one as an exception fails it, and a
handler that answers rather than raises may sit only where no HTTP route
reaches but the assistant's two, `POST /api/chat` and `GET /api/chat/stream`,
which answer inside the conversation (`IN_BAND_ROUTES`). That rule first
passed by not seeing those routes — `service.chat(...)` names a method two
classes define — so the walk types an object by the annotated factory that
made it, and pins every call it still leaves unresolved under the name of a
function that reaches a refusal (`UNRESOLVED_NAMESAKES`). A remembered list
had missed the goals job. `POST /api/duckdb/sync-buyers` reaches the 503
too: it shares the buyers step, which used to answer the refusal in its own
`except Exception` and now passes it on (chain 4's PR-3). An
unknown value **runs as `duckdb` and never raises** — web is the only syncer,
so a crash loop over how a read degrades would stop order intake (OD-09); it
publishes `read_fallback_mode.error` and the canary warns
`read_fallback_mode_invalid`. The same call lists, under
`read_fallback_mode.misconfigured`, every `KS_READ_*=postgres` without
`KS_PG_DSN` and `=clickhouse` without `KS_CH_URL` — found by prefix in the
environment, not listed: under `duckdb` those reads serve DuckDB with nothing
failing to count, and under `off` exactly those are refused, by the same rule.
Beside it, `no_engine` names the cohorts when their switch does not name
ClickHouse: no address is missing, and `off` refuses them all the same. Under
`duckdb` the canary pages both as `read_routed_to_duckdb`.

`tests/unit/test_read_fallback_sites.py` walks `core/` and `web/` for the
shapes, never a list of routers: a "falling back to DuckDB" log; an
`enabled() and available()` gate whose handler carries on toward DuckDB; and
**every** handler in a function reaching another engine under
`core/repositories/` or `core/pg_*_read*.py`, because `backfilled()` returns
False and `_pg_gold_summary` returns None and it is the caller that then reads
DuckDB. Each must call `fall_back`, re-raise, or say in the same `try` what
`ReadUnavailable` means — `_get_ml_forecast_total` lets it through, so a
refusal is never turned into a goal computed without its signal. And every
gate's function must call `no_address` with the switch the gate consults (or
`no_engine`); the two Gold readers gained `pg_gold_read.available()` so they
take the gate's shape and the walk sees them.

That the refusal then *reaches* the 503 is proved by running it, not listing
it: `tests/unit/test_read_fallback_http.py` sweeps **every GET route under
`/api/` read off the app** — every switch on, once with an engine that fails
and once with no address — and fails on any request that left a refusal
counted without answering 503 naming it. Only `/api/chat/stream` is skipped,
and for the network. A service wrapping a router in `except Exception:
return {}` was invisible to the static walk and to a hand-kept list of
thirteen routes; the sweep finds it by the route. The static half checks
every module `web/` imports: a handler that names `ReadUnavailable` must end
in a bare `raise` or `raise <its name>` — `raise HTTPException(500)` turns a
503 naming the surface into a 500 naming nothing. DN-20c's answers are the
one exemption, by function: those pinned, each proved reached from the
non-HTTP consumers and the assistant's routes alone, and each required to
exist.

### A write flag is no longer a rollback

Stage 4 moves WRITES chain by chain, and each chain is chosen by a `KS_WRITE_*`
variable — `KS_WRITE_EXPENSES` (chain 8, on since 2026-09-17), `KS_WRITE_INVENTORY`
(chain 1, off), `KS_WRITE_GOALS` (chain 7a, off — `app.revenue_goals`, the three
goal amounts typed on /goals, whose POST wrote DuckDB while the GET read an
hourly copy in Postgres), `KS_WRITE_EXPENSE_TYPES` (chain 6a, off),
`KS_WRITE_BUYERS` (chain 4, off — the buyers, their contacts and their gender;
see "Chain 4: the buyers, written where they are read"), `KS_WRITE_ORDERS`
(chain 3, off — see "Chain 3: the orders, written where they are read"),
`KS_WRITE_MANAGERS` (chain 5, off — see "Chain 5: the classification, written
where it is derived"), `KS_WRITE_CATALOGUE` (chain 6, off — see "Chain 6: the
catalogue"), `KS_WRITE_FORECAST` (chain 7b-3, off — the three goal tables and
the forecast; see "Chain 7b-3"). Putting one back to `duckdb` reads like an
undo and is not one:
once rows have landed in Postgres, it starts a **second writer beside the
first** — a typed expense in the store the page does not read, DuckDB's
`seq_stock_movements_id` reissuing ids the Postgres sequence already handed out
(the state revision 0030 forbids), and the hourly full replace rolling the
Postgres rows back out of a frozen DuckDB, once an hour, looking healthy in
between.

So the first Postgres write a chain performs **latches** it (owner decision
OD-19 (a), `core/chain_latch.py`): `writes_postgres()` answers True from then
on whatever the variable says, and every consumer — the nine writers, the sync
keys, the hourly shipper, the daily comparison — reads that one answer. The
latch has two copies, a marker file under `data/write-chain-owners/` that
routes the writes without needing a database, and an `owner:<table>` row in
`meta.chain_watermarks` written inside the writing transaction; anything
already holding a Postgres connection stands down on either. The cost is
named: a latched chain whose Postgres is unreachable **fails its writes**
rather than writing DuckDB.

The disagreement is never silent — `/api/health` publishes `latched`,
`latched_at` and `mismatch` per chain, the canary pages `write_chain_flag_mismatch`
(WARN) within one probe, the shipper stamps failing the chain's tables it ships
itself (never a table another shipper carries — chain 4's bronze buyers are the
buyers mirror's, and a stamp there only that mirror's next success could clear),
and the daily comparison files `chain_latch_disagrees` and `chain_shipper_overwrote`
(both CRITICAL) — and `chain_owner_unregistered` for an owner row no chain in
the running build declares. Today nothing is latched: `app.manual_expenses` holds zero
rows, so chain 8's flag can still be moved freely, and the first typed expense
ends that.

**The way back is `scripts/chain_copy_back.py`** (DN-08), and it is the only
thing that may release a latch. It reads every table the chain owns out of
Postgres and then, in **one DuckDB transaction**, writes them, carries the
chain's `last_sync_*` values back into `sync_metadata`, moves the DuckDB id
allocator above every id either store has used, and compares both sides
**whole, with tolerance zero and no grace** — on the same connection, before
COMMIT, and on every column it wrote **except the two bookkeeping stamps the
daily spec forgives** (`ignore_columns`): `bronze.offers.synced_at` and
`app.sku_inventory_status.updated_at`. That is right, not a gap: each is one
value stamped across the whole table by one sync or one rebuild, so it says
when that writer last ran and nothing about an offer or a SKU, and the writer
restamps it wholesale on its next hourly run after `up -d` — a bad copy could
cost a wrong "as of" on /inventory until then. Keeping the daily list is
also what keeps the specs derived rather than a second opinion, and a unit
test computes the set so a third cannot join it quietly. Chain 7b-3 added four
more on purpose, with the reason in that test: the goal tables' `updated_at`
and `revenue_predictions.created_at`, each one stamp per writer run and read
by nothing in DuckDB. Two of them are **not** restamped by the next run —
`weekly_patterns` is stored only by `POST /api/goals/recalculate`, never by
the Monday job, and a training replaces only the range it predicts, so a past
day's `created_at` stands for good — so a mis-copy of either is seen by no
comparison and corrected by no run. What the copy carries is pinned by the
suite instead (`test_forecast_writer.py::TestTheCopyBack`, stamps that differ
by row).
`stock_movements.recorded_at` is the opposite case and **is** compared: the
daily check leaves it out only because it is that check's clock, but it dates
one movement for good, and a review shifted every copied value by an hour and
saw the latch released. It commits, and
releases the marker and the `owner:` rows, **only on zero findings**. A
difference is a ROLLBACK: DuckDB is left exactly as it was and Postgres stays
the writer. A ROLLBACK does not undo a sequence burn — measured on 1.5.5, the
burn stays in the process and reaches the file only if something commits
afterwards — so the allocator ends at or above where it was: a gap, never a
reissued id.

**After the COMMIT come a checkpoint and the release, and either can fail.**
That is exit 3, never exit 1: until it had its own code, an exception there
left through the interpreter's exit 1 — "rolled back" — with the copy in
DuckDB. The message reads which copies of the latch survived and names one
step: owner rows still standing → `--execute` again (its handover finds DuckDB
already equal, and it releases); owner rows UNREADABLE — Postgres gone
between the comparison and the release, so the read that would answer fails
too — → the same, once Postgres answers, and never a guess
either way; owner rows gone and the marker not → the marker-only steps below,
`--handover` first. The marker is read from disk, not the process's cache: an
unlink whose directory fsync raised has removed the file and not the entry.

**A comparison run after the copy cannot see what the copy destroyed.** A
full-replace table is DELETE+INSERT, and afterwards the two stores agree by
construction; the first draft released a latch exactly so, with an offer
DuckDB had catalogued after the last shipment deleted by the copy it then
"verified". So the handover question is asked first, of both stores as they
stand — in the dry run as well — and any CRITICAL refuses before anything is
written:

- a key only DuckDB holds — bar two the copy may delete, each INFO: a row
  older than anything Postgres holds on a table both stores sweep by age
  (DuckDB's sweep lagging the writer's), and after the latch a contact or
  line item the chain's rewrite of its owner dropped (decision 6, below);
- before a flip, a key only Postgres holds in a table the hourly copy replaces
  whole: a row DuckDB deleted after the copy last ran. The copy would remove
  it, but it stands down at the flip, so the flip would keep it in the store
  the page reads — the review of #265 (F6) reproduced a withdrawn expense back
  in the ad spend. The lever is the copy itself, with the chain writing DuckDB
  (the flag at duckdb, no marker). One exception, and only on a table both
  stores sweep by age (chain 10's samples): a row older than anything DuckDB
  still holds is its sweep, which Postgres's own writer repeats after a flip.
  A row's own clock is no sweep — an older withdrawn expense is a ghost too.
  An append table's key only Postgres holds, above DuckDB's watermark, stays
  INFO and is **not** refused: a DuckDB file restored from before the last
  copy and a writer round the copy look the same there, and refusing the
  first leaves no lever but deleting real history. The finding says the flip
  keeps them; whether it should refuse is the owner's to decide;
- in an append-only table, two different rows under one key. There is no
  "newer" there: they are two events, and the usual one is a movement id both
  allocators issued, because Postgres floors its sequence on its own MAX(id);
- in an append-only table, a Postgres row the copy can never read back: below
  DuckDB's watermark, or at it where the copy reads strictly above
  (`stock_movements`; `inventory_sku_history` re-reads its watermark day with
  `>=`, so a row on that day is carried);
- a DuckDB version later than Postgres's by a clock both stores carry as a value
  — derived per table from the daily spec (`_shared_clock`; `manual_expenses`
  and `inventory_history` first, then every chain that carries one, such as
  `revenue_goals`, whose two writers stamp `updated_at` from the web
  container's clock for exactly this reason), and for a buyer or an order
  KeyCRM's own `updated_at`. `TestTheClockIsDerived` lists every one.

Any other difference after the latch is taken as Postgres being newer, and that
is true by construction only when `--handover` was clean before the flip —
which is why the runbook puts it first. One refusal is knowingly too strict: an
expense DuckDB held before the flip and Postgres deleted after the latch looks
exactly like a stranded one (a row on one side has nothing to compare a clock
against). Unreachable today, since DuckDB holds no expenses; the refusal names
the ids and the two levers.

Its specs are derived rather than written out — `_FULL_REPLACE` and
`_APPEND_ABOVE` intersected with the chain's `CHAIN_TABLES`, plus the daily
comparison's own specs — and a chain table with no shipping shape or no
comparison spec **raises** instead of being skipped. Chain 4 added a third
source, `MIRRORED_LANDING_TABLES`: `bronze.buyers` and `bronze.buyer_contacts`
are shipped by the buyers mirror from the same parse as DuckDB and nothing
replaces them whole. Their handover is stricter because it can be — Postgres
dates every write of a buyer (`mirrored_at = now()`), and the owner row's
`updated_at` is `now()` in the latching transaction, one clock. Before a flip
any difference, either side, is CRITICAL. A buyer or contact DuckDB holds that
Postgres lacks or holds differently, and a contact only Postgres holds of a
buyer DuckDB holds, name `POST /api/mirror/backfill/buyers`, which re-ships
every buyer DuckDB holds with its contacts (detached, heavy lock per portion;
it moves every `mirrored_at`, so the search index re-indexes everything and
`buyers_without_verdict` is blind for 90 minutes). A buyer only Postgres holds
is a per-id decision — nothing deletes one. After the latch a difference, or
a row on one side only, is INFO only for a buyer the chain rewrote since the
latch and CRITICAL otherwise (decision 6); a buyer only DuckDB holds always
refuses — the chain never deletes one — and so does a buyer DuckDB holds in a
version KeyCRM's own `updated_at` dates later, with its contacts: a DuckDB
write after the latch (the marker lost with the flag back at duckdb) that the
rewrite clock alone would take for the chain's. Contacts are read in both
stores through a join to their own buyers, as the daily check reads DuckDB's,
so an orphan on either side is outside the landing rather than a refusal the
reship could never clear. For every chain, a Postgres row with NULL where
DuckDB declares NOT NULL (read from DuckDB's catalogue) is a handover CRITICAL
whose lever is correcting it in Postgres, so `--handover` refuses what
`--execute` would otherwise have died on at its first INSERT. The two big tables are
compared row by row rather than by fingerprint, because the fingerprint's one
blind spot — a text column rewritten to the same length — is affordable every
morning and not affordable in the comparison that releases a latch.

**Preconditions, and one deliberate absence.** Web must be stopped, which is
proved by DuckDB opening read-write at all: the file lock is exclusive, so a
check against the docker socket would only be a second opinion that can be
wrong. The chain's **owner rows** must exist — they are written inside every
Postgres write, so they are the proof that rows changed hands, marker or no
marker (a lost `./data` loses the marker and keeps them). A **marker without
owner rows** is a first write that failed, or a release that died between its
two deletes: Postgres received nothing, a copy would be a rewind, and it is
refused with the steps that clear it. **`--handover` is the first of them**:
with no owner rows it applies the pre-flip rule, and a CRITICAL there is a row
DuckDB wrote after the shipper stood down. Then the flag in `.env` — left at
`postgres` only on exit 0, or the next write latches the chain over that row
and every later copy-back refuses on it — then delete
`data/write-chain-owners/<chain>`, then `up -d`. `KS_WRITE_*` is **not** a
precondition: under OD-19 (a) it does not route writes while the chain is
latched, and it is put back afterwards, by the runbook.

**`--handover` comes first, in both directions.** It reads only and needs no
latch, but it does need the database file to itself — DuckDB will not let a
second process open a file web holds, not even read-only — so it runs in the
stopped window. Before a flip it is the gate; before a rollback it is the
preview, and its CRITICALs are exactly what `--execute` refuses on.

```bash
cd /opt/key-api-bot && docker compose stop web bot
docker compose run --rm --no-deps -T web \
    python /app/scripts/chain_copy_back.py inventory --handover
# BEFORE A FLIP: first, with web still up, /api/health must show
#   write_chains.pg_inventory_write.preflight.ok = true (DN-24). Then this:
#   exit 0, or do not flip. A CRITICAL is a row the flip would
#   strand, or one DuckDB deleted that the flip would keep: up -d web with the
#   chain writing DuckDB — KS_WRITE_INVENTORY=duckdb and no marker under
#   data/write-chain-owners — let replicate_operational ship
#   (POST /api/jobs/replicate_operational/trigger), stop, ask again. Not
#   "the flag unchanged": the handover asks the same pre-flip question with
#   the flag already at postgres and nothing latched, or with a marker whose
#   first write failed, and there the copy stays down and the next write
#   latches the chain over the row.
# TO ROLL BACK: the same command with --dry-run (also the default), then with
#   --execute. Exit 0 released. 1 not committed (a difference rolled back, or
#   a traceback before COMMIT): DuckDB as it was, latch kept. 2 refused before
#   writing. 3 COMMITTED, then the checkpoint or the release failed: DuckDB
#   HAS the copy; do what the message says for the latch it found. Only after
#   exit 0:
#   1. set KS_WRITE_INVENTORY=duckdb in .env   (the flag decides again)
#   2. docker compose up -d web bot
#   3. at +2 min: deploy/stage4_soak.sh — E1/E2 for chain 8, I1/I2/I3 for
#      chain 1; chains 7a, 6a and 7b-3 have no soak check yet, so meta.mirror_state
#      for app.revenue_goals or bronze.expense_types. The hourly copy must be
#      shipping the chain's tables again. Chain 4's B1-B5 read "not
#      applicable" once it is released, so read meta.mirror_state for its
#      three tables: the buyers mirror stamps the bronze two on its next
#      batch (POST /api/mirror/backfill/buyers now), replicate_operational
#      stamps app.buyer_gender within the hour.
```

**Run it as the web service, never as a bare `docker run --env-file .env`.**
`KS_PG_DSN` is not in `.env`: `docker-compose.yml` builds it in web's
`environment:` block, with the password interpolated from `.env`. The first
form of this runbook passed `--env-file .env` and a hand-picked network, and at
chain 1's flip (2026-09-30) the handover died on `KS_PG_DSN is not set` — safely,
exit 1 before anything was written, web back in 8 s. `docker compose run
--rm --no-deps -T web` gives the one-off exactly web's environment, volumes and
networks, so the DSN, `./data` and the route to Postgres are web's own and not
a second copy that can drift. On flip day pass the flip time to the soak
(`SOAK_INVENTORY_FLIP_AT='<latch time>' deploy/stage4_soak.sh`): without it I1
reads the last pre-flip copy as a lapse for 75 minutes, by construction. At
production's size — 162,883 history rows, 56,277 movements —
the whole copy-back is ~9 s on a laptop, because rows go in 1,000 to a
statement. `executemany` runs once per row in DuckDB's client: about eight
minutes for the history alone with no memory limit, and under the store's own
4 GB — what the one-off container gets — `OutOfMemoryException` in 17 s.

**Chain 7a moves the write, and the reads of its table follow it.**
`KS_WRITE_GOALS=postgres` routes `set_goal` — and `reset_goal_to_auto` through
it — to `core/pg_goals_write.py`. Every statement reading `{revenue_goals}` —
`get_goals`, `get_smart_goals`, the /marketing target line — then goes to
Postgres whatever `KS_READ_GOALS` or `KS_READ_MARKETING` say, and a Postgres
error there raises instead of falling back. The first version left those reads
on the two flags and called "both at `postgres`" a precondition of the flip.
It could not stay one: putting a read flag back is every read port's rollback,
and after the latch that would have shown the pre-flip goal out of a DuckDB
nothing writes, for good and in silence. The flags still choose the engine for
the rest of those pages. `tests/unit/test_goals_reads_follow_chain.py` walks
`core/` and `web/` for every goal statement and fails if its router does not
ask. `scripts/chain_copy_back.py goals` is the chain's way back; there is no
sequence and no sync key to carry.

### What the warehouse validation can and cannot see
`validation_passed` covers: Bronze→Silver row counts, Silver→Gold revenue
checksum, product revenue checksum, and the **cell guard** — the set of
`(date, sales_type)` cells must match between Silver and Gold. The guard is
what catches the August incident's shape: 100 → 90 → 84 missing cells with
*zero* value mismatches, invisible to every scalar.

It cannot see a lie that arrives from Bronze. Gold is built from Silver in the
same tick, so a consistent wrong value is reproduced on both sides — proven by
injection: ±1000 on two orders, `status 12→19`, `source 1→2` all pass. Only
reconciliation against KeyCRM sees those.

**And reconciliation cannot see data that stops meaning anything**, which is
the wider blind spot the July 2026 outage exposed. On 20–21 July the shop's
order-comment template changed and stopped carrying `utm_source`,
`utm_medium` and `utm_campaign`; website campaign coverage fell from 34% to
5.5% and stayed there **five weeks**, and nobody was told. Nothing here could
have told them: the reconciliations compare DuckDB against Postgres against
ClickHouse against KeyCRM, and all four faithfully recorded the absence, so
they agreed perfectly. `validation_passed` checksums revenue, which never
moved — the orders kept coming, only their labels stopped. The platform chart
hid it outright, because pixel-only orders were counted as Facebook until
2026-09-09, so ~600 Facebook orders a month stayed on screen while the real
number was 58. Every guard in this system watched whether data was
transported faithfully; not one asked whether it still said anything.

`_attribution_coverage_check` is the first that does, and it rides
`dq_integrity_check` (01, 07, 13, 19) so it reaches people through the 09:00
digest. It measures the share of **website** orders (`source_id = 4`) that
carry a `utm_campaign` — an order taken by hand in the Instagram inbox cannot
carry a tag and never will, so including those would measure the channel mix
instead. WARN below **15%** over 7 days, or when that share has **halved**
against the trailing 28 days; the floor alone would sleep through 34% → 16%,
which is the same failure caught early. Numbers from the twelve complete
weeks to 2026-09-06: healthy ran 20.5–34.3%, the outage 5.5–9.1%.
Back-tested day by day over that history: **it would have fired first on
2026-07-28**, one week after the break, and it is quiet today at 27.8%.

Its `REMEDIATION` deliberately names no lever in this repository. The tags
stop arriving at the website, so a warehouse rebuild would only recopy the
absence; the entry sends the reader to the shop's order-comment template.

The `sales_type` partition assertion (Gold known types == Silver total) is
deliberately **not** part of `validation_passed`: no rebuild can invent a
sales_type the code does not know, so it reports and stops rather than driving
a rebuild every two minutes.

### The standing watch on a moved chain's tables

Once a chain is latched or flagged, `replicate_operational` and
`reconcile_operational` both stand down for its tables — the comparison without
a finding — so `core/pg_chain_invariants.py` watches them on the integrity layer
instead (01, 07, 13, 19). It judges what is true of the Postgres copy alone: the
allocator above `MAX(id)`; no NULL where Postgres has no default
(`manual_expenses.created_at`, `stock_movements.recorded_at` and `.source`,
`revenue_goals.updated_at` and `.is_custom`);
chain 1's `last_sync_*` under 90 minutes; and from chain 1's handover on, no
burst of `initial` movements, no offer first seen after a day it was already
photographed, and both snapshots every day; for chain 7b-3, the four goal and
forecast tables complete and stored by their last scheduled slot (WARN, see
"Chain 7b-3"). Who is watched comes from
`chain_modes()`, a watched chain with no invariants written for it is reported
unwatched rather than clean, and nothing is repaired. **It is judged in the
integrity job beside the Postgres twins, not inside the DuckDB scan**: these
are Postgres facts about tables DuckDB no longer writes, so a DuckDB half that
raises neither loses a chain finding nor holds back its page, and the
`chain_*` conditions resolve on such a run the way the twins' `pg_*` ones do.
**Production reads chain 8 on every run**: `KS_WRITE_EXPENSES=postgres` is live
and nothing is latched, so each run reads `app.manual_expenses`' allocator and
NULL count, and a Postgres it cannot read is `chain_invariants_unwatched`
(WARN). Measured 2026-09-18, before chain 1's flip: 0 of 56 277
`stock_movements` rows carry a NULL `recorded_at` or `source`.

### Chain 1 before its flip (DN-24)

Three things that would bite the day `KS_WRITE_INVENTORY=postgres` is set,
closed while it is still off. The lock and the Postgres step path do not run
under today's flags. The preflight does, by design, since its question is
asked before the flip: every `/api/health` cache miss (once a minute) reads
Postgres — the revision check, then four reads on one web-pool connection —
bounded at 5 s, and publishes the answer.

- **One rebuild at a time.** The stock step holds the scheduler's heavy lock;
  the 01:00 `inventory_snapshot` job, its boot catch-up and
  `POST /api/inventory/snapshot` do not. DuckDB's store lock hid that. In
  Postgres two status rebuilds interleave, the second one's DELETE cannot see
  the first one's new rows, and its INSERT dies on `offer_id` — reproduced.
  So the rebuild and both snapshots take `pg_advisory_xact_lock(CHAIN_LOCK_KEY)`
  as their first statement (`pg_locks`: classid 1802698752, objid 1). The
  stock upsert does not: nothing but the stock step calls it.
- **The pre-flip questions are asked for you.** `/api/health` publishes
  `write_chains.pg_inventory_write.preflight` — `ok` and the `reasons` it is
  not: the writing role lacks TEMPORARY (every status rebuild would raise), one
  of the six tables was not copied within 50 min or its copy is failing, or
  the latest `mirror_landing` run — read from a journal copy under 75 min old
  — is over the canary's 30 h, filed anything against the six tables or the
  chain, or failed before comparing them: `reconcile_operational` itself
  raised, an exception fell between checks (`setup`), or the error is not in
  the job's format. Any other check raising — SMS, ClickHouse — leaves chain
  1's comparison complete, so it is a `note`, not a reason; the run still
  counts as failed everywhere else. `ok` is null once the chain writes
  Postgres. It is not a substitute for `chain_copy_back.py --handover`, which
  is still the gate: read the preflight first, then stop web and ask the
  handover.
- **A step failure is not the tick's end.** On the Postgres path the offers
  and stocks steps run in `SyncService._inventory_step_postgres`, which never
  raises: a failure (the watermark reads included — they are Postgres reads
  there) is published as `write_chains.pg_inventory_write.sync_step`, error
  class only; `last_sync_stocks` moves only after the upsert, the rebuild and
  both snapshots commit; a failed offers step skips stocks; and the next
  attempt waits 10 min, or the held watermark would refetch every stock from
  KeyCRM once a minute. The DuckDB path is unchanged, a failure escaping it
  included.

### Step 13: the switch is built, and not switched (DN-28, DN-29)

`KS_WRITE_WAREHOUSE` names who derives Silver, Gold and the UTM verdicts:
`duckdb` (default) or `postgres`. It is read in `configure_modes()`, before
the boot sync, and **production does not set it**. An unknown value runs as
`duckdb` and publishes the error on `/api/health` (`warehouse_writer_mode`),
where the canary warns `warehouse_mode_invalid`; it never raises, since web is
the only syncer. After a flip the same typo is the way back, so it pages as
one: `settle_writer`, finding `postgres` recorded, adds `value_understood` to
the unmet preconditions, and the canary's CRITICAL carries the way-back lever.

**What `postgres` does (DN-29)** — only when every precondition below holds;
the verdict is reached once per process, the Postgres revision read on a
connection of its own. DuckDB stops deriving: `warehouse_refresh` is not
registered, every production call of `refresh_warehouse_layers` stands behind
`duckdb_derives()` (a test walks `core/`, `web/`, `bot/`, `scripts/` and
`deploy/` for one that does not), `mark_warehouse_dirty` is a no-op and the id
list pending at the switch is abandoned, `POST /api/warehouse/refresh` and
rebuild-silver run the Postgres derivation alone. The five DuckDB integrity
checks over Silver, Gold and UTM stand down and the Postgres twins stand in;
in `dq_mirror_landing` `reconcile_silver`, `reconcile_order_utm` and
`reconcile_gold` stand down and `pg_gold_internal_check` asks
`gold_rollup_mismatch` in `reconcile_gold`'s place. ClickHouse's own
derivation is then the only independent check of Gold, so a run in which it
did not compare files `gold_values_unwatched` CRITICAL and pages (OD-08 (a)).
The first start records
`sync_metadata.warehouse_writer` in DuckDB and closes the `warehouse` alert
group once — its only resolver was the DuckDB tick — without a validating
tick. **One precondition unmet and it runs as `duckdb`** (OD-09 (b)):
`/api/health` lists the unmet keys under `warehouse_writer_mode` (keys only,
the endpoint is public) and the canary pages `warehouse_preconditions_unmet`,
CRITICAL — titled "Warehouse switch held back", since web serves throughout:
a canary title says "Dashboard DOWN" only for the three keys that mean web
did not answer or said it is unhealthy, and any other CRITICAL reads
"Dashboard critical".

**The way back costs a full DuckDB rebuild.** Unset the variable and
`up -d web`: a start under `duckdb` that finds `postgres` recorded marks the
warehouse dirty in full, before any job exists, and holds the stood-down
checks and the three comparisons down until a full tick validates — the Silver
they would read is as old as the switch, and an incremental rebuild over it
would validate — and a UTM parse has finished, in that tick or a later one:
the tick swallows a parse that raised and still reports a validated success,
and `attribution_coverage` and `reconcile_order_utm` read what that parse
left. Not a second full rebuild, which would rewrite Silver every two minutes
for as long as the parser failed. The tick that completes both writes
`duckdb` back. A hold that does not end — a parse that keeps raising, which
the tick logs at WARNING and reports as a validated success, or a full tick
whose parse raised with nothing dirty after it — is published as
`held_for_s`, and past two hours the canary warns `warehouse_hold_stuck`.
When DuckDB's
`silver_order_utm` is empty (a Sunday compaction ran in between) it publishes
`reclassify_needed`; the tick's own parse refills every commented order with
no verdict, so `POST /api/traffic/reclassify` is the lever only if that parse
fails. Every day spent under `postgres` is a day no independent derivation
proves Postgres Silver/Gold, and that cannot be re-verified afterwards. The
gate-stack rehearsal on the production backups — a container killed mid
Postgres rebuild, the owed state surviving — runs on the host before any flip.

**After a flip, a start with any precondition unmet IS the way back**, not
only an operator's unset: the same full rebuild and hold, and the next start
under `postgres` records a new `since` — the soak clock starts again — and
closes the `warehouse` group again without a validating tick. The canary's
lever says so first. So a Postgres read that failed (no answer in time, or an
exception, a connection lost in the middle of the SELECT included — never an
answer such as a wrong revision) is asked again before the verdict: three
asks, 2 s and 5 s apart, at most 37 s, once per process. The revision is read
strictly (`current_revision(strict=True)`): only a version table that is not
there reads as "never migrated", where the default reads any failure so.
The write-chain registry and the Alert Gate are read on the caller's thread,
outside that bound, so a start that could not reach Postgres names
`pg_revision` alone.

**Unless a write chain is latched over what DuckDB derives from** (chain 5's
review, 2026-10-08). A chain owning `bronze.orders`/`order_products` (chain 3)
or `bronze.managers`/`app.manager_classifications` (chain 5) froze DuckDB's
copy at its latch, and the full rebuild the way back owes would derive Silver
from it — every order of a manager reclassified since on the wrong
`sales_type`, with the Silver and Gold comparisons back to vouch for it. An
automatic way back follows no runbook, so `configure_mode` refuses it: a start
that would run as duckdb — unset, typo or an unmet precondition — stays
`postgres` while such a chain is latched (`DUCKDB_DERIVATION_SOURCES`, local
markers only), publishes the chains as `warehouse_writer_mode.way_back_refused`,
and the canary pages `warehouse_way_back_refused` (CRITICAL, "Warehouse way
back refused") in place of `warehouse_preconditions_unmet`. The lever: copy the
chains back first (chain 5, then chain 3), then the way back. Postgres derives
on its own signal only under `KS_PG_DERIVE=own` — under `piggyback` its
rebuild rides the DuckDB tick a `postgres` start does not run — so a refusal
with `own` gone leaves **nothing** deriving, screens frozen and correct up to
the start; still refused, because the alternative is a wrong `sales_type` that
rolled-back read switches would serve. The page says which, from the
`derivation` block. Never on a
deployment that has not flipped — both chains hold themselves behind `step13`,
which a test requires of every chain declaring one of those tables — and not
for chain 4: DuckDB's Silver reads none of the buyers.

**The UTM doors parse Postgres alone under `postgres`.** `POST
/api/traffic/refresh`, `/traffic/reclassify`, the `manager_comment` backfill
and `scripts/backfill_utm.py` neither empty nor re-parse DuckDB's
`silver_order_utm`: a DuckDB parse that raised used to leave the Postgres
reclassify — what /traffic reads — unrun behind a 500, and after a compaction
each door re-parsed every order in DuckDB first. The Postgres answer is the
door's (an error is a 500, a refused full parse a 409, a failed parse makes
the backfill `partial`). The reclassify and the CLI note the re-parse in the
recorded writer (`utm_reparsed_at`), and the way back then empties DuckDB's
`silver_order_utm`, so the full tick it owes re-parses every verdict under
the rules in force — an incremental parse would never notice a rule change on
an order whose `updated_at` has not moved.

`GET /api/warehouse/status` publishes `cutover`: the variable as read,
`switch_built: true`, and every unmet precondition by name, from
`evaluate_preconditions(env, facts)` — a `KS_WRITE_WAREHOUSE` this build
understands, `KS_PG_DERIVE=own`, the twins on,
`KS_UTM_PARSE=postgres`, `KS_READ_FALLBACK=off`, a DSN and the required
revision, the landing mirror on, every Silver/Gold/UTM read switch on
`postgres` (a test reads every string in `core/`, `web/` and `bot/` for a
`KS_READ_*` or `KS_*_STORE` name, so a new one has to be put on the list or
excluded by name — `KS_SMS_STORE` is read inline and the first walk missed
it), cohorts on ClickHouse with `KS_CH_URL`, **the goal calculators off the
DN-12 bridge** (`goals_bridge`: `KS_GOALS_HISTORY=silver`, see "Chain 7b"),
**`bronze.expenses` holding its history**
(`expenses_backfilled` — see "OD-10: the DuckDB-only doors"), **no door OD-10
has not answered** (`od10_doors`: `OD10_DOORS` is every function in `web/`
naming DuckDB's Silver, its order-lines view, Gold, UTM or an inventory view
over them (the set is derived from the view bodies, `{views}` hole included)
without asking `duckdb_derives()`, and a test walks `web/` and requires
exactly that list. It is **empty since 2026-09-30**, when the owner retired
all seven doors; a door added later joins it and holds the switch again),
and **no delivered page open under a condition only a stood-down check
reports** (`retired_conditions_clear`). A stood-down
check is not a raised one, so the integrity job does not hold its conditions,
and the first run after the switch would announce such a page "✅ Resolved"
with no check looking. Holding them instead would keep it open for as long as
Postgres derives, since nothing re-examines a retired check; so the switch
waits while the DuckDB check can still clear it. It reads the Alert Gate's
delivered map — what `resolve_group` announces from — not `app.alert_series`,
which can miss a delivered page (its fired row is fire-and-forget).
`gold_missing_cells` and `gold_orphan_cells` count as retired too, since
`reconcile_gold` goes with the switch. `reconcile_silver` and
`reconcile_order_utm` report under `mirror_*` names the comparisons that stay
up share, and the Gate keys a page by condition and group alone, so those
names cannot be retired — that would hold the switch over every
`bronze.orders` page, and after a flip run a restart as duckdb over one.
Instead each `dq_mirror_landing` run in which they ran marks the delivered
pages they were still reporting (`AlertGate.mark_delivered`, persisted with
the entry, gone when it resolves) and unmarks the rest once all three reached
a verdict; a marked page holds the switch. `preconditions_met: true` is a
checklist done, not a switch thrown. An exception reading any fact is
published by its class alone and logged whole: a driver's text names the
database user, host and port.

### The step-13 rehearsal (`deploy/step13_rehearsal.sh`)

The gate-stack rehearsal named above, as one command and one table. It
exercises PR #268's eight points on **copies** of production, on the host,
beside the live stack: P1 the flip with every precondition, P2 DuckDB frozen
while Postgres derives, P3 web killed inside the Postgres derivation (Gold's
`TRUNCATE` held by a lock, so the kill lands between Silver's commit and
Gold's), P4 the three mutations (a deleted Silver row, a landed order, an
unknown sales_type via a trigger on the copy), P5 both DQ jobs under the
stand-down, P6 restarts — graceful, after the kill, graceful — and one
resolve, P7 one precondition broken and the canary paging, P8 the way back.
P3, P6, P8 and D1 judge the stops the container shows, not the ones the
script meant: `docker inspect` after each stop and start (exit 0, or exit 137
after the rehearsal's `docker kill` and not the OOM killer), and P6 counts the
restart after the kill only when its SIGKILL is the one `p3.json` records.
Every stop of reh-web gets production's grace — web's `stop_grace_period`,
120 s since the DuckDB kill guard and compose's 10 s default before it
(`STOP_GRACE_S`, pinned to `docker-compose.yml`) — and records the
container's state after it, so one that outruns the grace is recorded as the
kill a deploy would have made: P6 reads F5s's and F7's, D1 phase 0's and
F7's, P8 the stop the way back starts from and its own last one, and a stop
that outran the grace is UNKNOWN there until somebody has read why web did
not stop in time. D1 reads those last two as well, because its DELETE comes
after them: a loss it finds behind one that was no deploy's stop is still
FAIL, and says that a kill after P3's may have cost it. Three
stops recorded nothing until 2026-10-08 — both around the way back, and the
one after a run that did not flip — while phase 0's had already taken 7 of
the 10 s it had then on 400 orders. A test walks the script for a stop of reh-web
without a state file, and for a record or a judge fed another phase's.
Plus D1 (what P3's kill cost DuckDB's indexes, below), K0 (KeyCRM never
called) and Z0 (no other container on the host moved). Z0 records every other container's
`StartedAt`, `RestartCount` and `OOMKilled`, not just its id: a restart
policy brings an OOM-killed live web back under the same id and name, so
only those say it happened, and one that did is a FAIL until somebody has
read why. A container gone or new is UNKNOWN — a deploy or a cron one-off,
or, on a laptop, other work. Exit 0 all PASS, 1 any FAIL, 2 UNKNOWN only,
3 not set up.

**What keeps it off production**, each pinned by parsing the script —
and the watchdog, the lock and the disk guard by running its own functions
against stubbed `docker`, `df` and `du`
(`tests/unit/test_step13_rehearsal_script.py`): every container, one-offs
included, and the network are named `reh-*`, and nothing else is ever acted
on — the one look at the rest is Z0's `docker ps` and `docker inspect`, and
no `docker compose`;
the network is `--internal`, so there is no route to KeyCRM, Telegram or any
live container; `--pull never` on every `docker run`, so the image the next
`up -d` starts is not changed; hard memory caps (web 1.5 g with DuckDB at
768 MB, Postgres 512 m, ClickHouse 1.5 g) and `--oom-score-adj 1000` on
every container — the caps bound what the rehearsal takes, but if the host
runs short anyway the kernel kills the largest RSS, the live web, unless the
rehearsal's processes ask to go first (at 0 the live one went, reproduced);
a start guard on `MemAvailable`, and one on the disk that projects every
copy onto both filesystems it lands on against 74 %, a point under the live
monitor's 75 % WARN, which would otherwise page production's admins about
the rehearsal's own files; a watchdog on both that kills the `reh-*`
containers itself and only then signals the shell, which runs its TERM trap
once its foreground command returns — 25 minutes for a `docker exec`,
unbounded for `pg_restore`; `/tmp/ks-gate.lock` taken with `flock -n` before
anything starts, and the watchdog launched with fd 9 closed and alive only
while the shell is — a SIGKILLed shell left it looping with the lock, every
gate waiting in `flock 9` and `--cleanup-only` refusing, and now it removes
what the shell started instead; the tree mounted read-only and never
written; cleanup on EXIT with `docker rm -f -v`, deaf to a second signal.
reh-web runs with `KS_ALERTS_DISABLED=1`, no `BOT_TOKEN`, and
`KEYCRM_BASE_URL` pointing at a
stub (`deploy/step13_rehearsal/keycrm_stub.py`) — nothing in the application
switches the sync off, so the base URL and the network are what keep the
quota untouched. The canary is `bot/canary.py` imported inside reh-web and
judged against its own `/api/health`, never sent. Jobs are triggered through
reh-web's own API with a session signed by a key minted for the run.

**Production enters as copies only**: the newest nightly `pg_dump` (streamed
into `reh-pg`, ownership kept, on roles initdb creates with per-run
passwords) and the newest `data/backups/analytics-*.duckdb` — never the live
file — copied into the rehearsal's own directory (mode 700), plus the
`KS_WRITE_*` chain flags read from `.env` by exact name. The copies go at the
end; only the table survives, in `step13-rehearsal-<stamp>.txt` beside that
directory, ids, counts and keys only. `--keep` leaves the copies and names
each in red: the directory, and the stopped `reh-pg` (a full restore of
production's Postgres, buyers and phones) and `reh-ch` in their anonymous
volumes; `--cleanup-only` removes what a killed run left. A window guard
refuses to start within 100 minutes of any reh-web or host cron
(`--any-hour` overrides): ~75 minutes on production's data.

`--local` runs it on a laptop over synthetic data, and is refused on the
host — root, or `/opt/key-api-bot` present — since it skips the window,
the memory guard and the watchdog: `seed_synthetic.py`
invents an account, the stub serves it, a reh-web over an empty directory
syncs it the way production once synced the real one, and the rehearsal
proper then restores that dump and that backup by the host's path. The
floor (`KS_PG_SILVER_INTERVAL_S`) is 120 s on the host and 60 s locally
instead of 600 s; every property is "within floor plus a tick", so its size
changes nothing proved. On 2026-10-02 a local run over 400 orders, on an
image built from this branch (3.0.261, revision 0033) with `--build`,
passed P1–P8, D1 and K0 in 22 minutes, exit 2 for Z0 alone, UNKNOWN for
other sessions' containers. P4a and P5 judged F4's runs as F5s read them,
both whole through `fetch_run_issues`; D1 swept 23 indexes at both stops
and found none short. That PASS said less than it read as. F5s's graceful
stop had checkpointed F4's runs before F6's kill, so they were out of its
reach; what the sweep asked within it was whatever reh-web wrote after that
checkpoint, the fixture's fourth version among it — landed by F6 before the
kill, and asked of the single-column indexes on `orders` and
`order_products` (`idx_orders_status` among them). That row survived
because the sync touches `orders` after a restart, not because the kill
could not reach it. And the sweep asked
single-column indexes only: 23 of the 57, the other 34 the 12 composite
ones and 22 left empty by the seed (counted on the 10-07 run, whose seed is
the same deterministic one; none was one-valued). Its P6 counted a `kill`
restart the script recorded whether or not F6 had killed anything; that
run's P3 shows
the kill landed, but P6 could not have told. The run the day before, on
the same image with P4a and P5 read after the kill, failed both on the
DuckDB defect below and on nothing else: the live process showed both
runs' findings at F4, and after F6's kill `fetch_run_issues` returned none
of them. An earlier note here said a run had passed everything on an image
of main at 3.0.264, revision 0034; the image it ran was built at revision
0033, and its P4a and P5 had read around the defect.

On 2026-10-07, with P6 and D1 reworked and main merged (an image built from
this branch, 3.0.267, revision 0034), the first local run did not flip:
chain 7b had added `goals_bridge` (`KS_GOALS_HISTORY=silver`) to the
preconditions and reh-web's list lacked it — and the script, deciding the
flip with a grep that `utm_parse`'s `"mode": "postgres"` matched, ran every
phase after F1 anyway. The script now asks the probe for the writer's mode
out of the snapshot P1 judges, and a test runs this tree's
`evaluate_preconditions` over the list: F1 must leave nothing unmet, phase
0 exactly `read_fallback_off`. The next run passed P1–P8 and K0 in 22
minutes and failed D1 as predicted: after the kill and F7's checkpoint,
`idx_dqi_run`, `idx_dqr_layer` and `idx_dqr_started_at` each missed F5w's
run, `fetch_run_issues` returned 0 of its 1 finding, and deleting the run's
rows from `data_quality_issues` and `data_quality_runs` was DuckDB's FATAL.
The fixture order and its line came out of every index on `orders` and
`order_products` that holds them — after a restart the sync touches
`orders`, and nothing touches the DQ journal — but that is two of the four
composite indexes on `orders`, not all four as this note first said: the
fixture has no buyer and no manager, and DuckDB keeps no index entry for a
row with a NULL in any key column, so `idx_orders_buyer_date`,
`idx_orders_manager_date` and their single-column twins never held it and
the DELETE asked them nothing. Z0 failed on another session's ClickHouse
container, stopped (exit 0, not the OOM
killer, no restart policy) and started again 12 s into the run. What the
local run cannot answer is production's own readiness — whether the copy's
preconditions hold, the way back's full rebuild inside 1.5 GB, and the
timings at 47 k orders — which is what the host run is for.

**P5 reads the stand-down off the job, not off its silence.**
`dq_mirror_landing` names every check it asked, verdict or raise, as
`checks_run` on its completion line, and P5 fails on a retired comparison
there and is UNKNOWN without the list. Absent findings could not fail it:
`compare_gold` excuses every cell whose orders synced inside its 20-minute
grace — every cell of the local seed, and on the host the fixture's — so a
review that ran `reconcile_gold` under the stand-down got a PASS.

**The local runs found two things outside the rehearsal.** With a
Silver row deleted and a Gold row bumped in one window, ClickHouse's
comparison raised instead of filing: `compare_gold_cells` sorts cell keys
whose roll-up `source_id` is NULL beside per-source ints, so one day that
differs in both grains is a `TypeError`, `gold_values_unwatched`, and never
`ch_engines_gold_mismatch`. The mutations now run one per DQ run.

**And DuckDB 1.5.5 drops index entries across a SIGKILL — production's
shape, not the rehearsal's.** Every index made by `CREATE INDEX` — all 57,
on 26 tables; `PRIMARY KEY` and `UNIQUE` constraints keep theirs, and so
does an index that was empty at the last checkpoint — loses the
rows a kill caught in the WAL when the first checkpoint after the restart is
one DuckDB takes on its own (at close, which is the next `docker stop`, or
past `wal_autocheckpoint`) and nothing wrote that table, or filtered it on a
value it holds, first. Replay buffers the entries in the still-unbound index;
that checkpoint writes the index without them, and they stay out. Measured
with production's own code on a fresh file: schema by `DuckDBStore.connect()`,
runs by `persist_run` in a process then SIGKILLed, the restart as
`connect()` then `close()` — `fetch_run_issues` answered 0 of 2 and 0 of 5,
`fetch_run_diffs` 0 of 3, and with rows in every indexed table each of the
45 single-column indexes answered for 2 of 5; with the hourly
`duckdb_checkpoint` job's explicit `CHECKPOINT` before the close, every one
answered whole. The same in web's image (Python 3.14, Linux) and on macOS.
Nothing the product runs at start binds them. A blind read
is the mildest of what follows:

- **reads through the index miss the rows** — `fetch_run_issues` and
  `fetch_run_diffs` (`run_id = ?`), so `/api/health/data-quality` and the
  09:00 digest list no finding under a run whose counts (read by its
  primary key, intact) say it has some;
- **a `DELETE` through it leaves them** — the sync's
  `DELETE FROM order_products WHERE order_id IN (?)` for a one-order batch
  and `DELETE FROM buyer_contacts WHERE buyer_id = ?`: stale line items and
  contacts;
- **the next write that must take a lost row out of an index is a DuckDB
  FATAL** ("Failed to delete all rows from index") — `UPDATE orders` on a
  status change, `INSERT OR REPLACE` into `order_products` or `buyers`, the
  samples' retention `DELETE`. A FATAL invalidates the instance: every later
  statement on web's connection fails and nothing in the store reconnects.
  A restart does not clear it — the index is still short, so the same write
  fails the same way.

The exposure is an OOM kill of web followed, within the hour before
`duckdb_checkpoint` first runs in the new process, by a deploy or any other
stop. The Sunday compaction rebuilds every index from its table (a row a
`DELETE` missed survives it as a row), and so does `DROP INDEX` +
`CREATE INDEX` with web stopped. A `CHECKPOINT` as the first statement after
`duckdb.connect()` kept every entry in the same measurement; whether the
store should take one is the owner's call, not this rehearsal's.

**The rehearsal's F6 is that kill, so nothing P4 and P5 judge is read after
it.** F5s stops reh-web gracefully after F5 — the checkpoint a deploy
takes — and reads F4's two runs there, scan and `fetch_run_issues` side by
side; P4a and P5 judge that read. The kill stays inside a live derivation
for P3, and F7 still reads after it for P2's frozen state and P6's gate
file.

**D1 asks what the kill cost of the rows it could cost** — those written
after F5s's checkpoint, since a graceful stop puts everything before it out
of a kill's reach: one integrity run F5w triggers, and the fixture's fourth
version F6 lands (an order and its line). An index holds no row with a NULL
in any key column — measured on 1.5.5 by losing every entry across a kill
and deleting each row: only a row with every key set is the FATAL — and the
fixture has no buyer and no manager, so it is in two of the four composite
indexes on `orders` (`idx_orders_source_date`, `idx_orders_status_date`),
and the buyer and manager ones cannot be asked about it. `window_rows`
reads which index holds how many of the window's rows (`held`), D1 counts
only those as asked and names the rest, and an index whose holding was not
read leaves the row UNKNOWN. After the kill and F7's graceful stop — the
checkpoint that writes the loss down; a read-only open before it still sees
every row the WAL holds — the run is read by `fetch_run_issues` beside a
scan. At the end, on the copy about to be removed, the window's rows are
deleted table by table in a process each, found by a predicate no index
serves: a `DELETE` must take each row out of every index on its table that
holds it, so a lost entry is DuckDB's FATAL, and that is the only question
a composite index answers — on 1.5.5 none serves a read. Each process opens
the copy through the image's own `duckdb_switch.open_file`, as the product's next
start would. A bare read-write open there, as D1 had it until the kill
guard was merged beside it, replays a WAL the last stop left and closes
lossily, so the next table's DELETE was a FATAL of D1's own making
(measured: a killed writer's rows in two tables, the first DELETE clean,
the second the FATAL; `test_the_ends_delete_costs_no_index_of_its_own_duckdb_1_5_5`).
An image without the guard cannot import it, and the DELETE is UNKNOWN.
The copy reaches that DELETE after the way back, through two more stops of
reh-web, and a loss found behind one that was not graceful (or not
recorded) may be that stop's kill: still FAIL, and the verdict names the
stop — a hedge written for an image whose restarts were lossy; through the
guard neither a restart nor D1's own open loses what a WAL holds. Every
single-column index is also swept at both
stops (lookup limits lifted, held to `count_if` over the table). A loss
after the kill is a FAIL labelled **DuckDB 1.5.5's known defect, not a
step-13 regression** — the switch writes no DuckDB index, and an OOM kill of
the live web costs the same; a loss already there before the kill is the
copy's own and is not given that label. Nothing the product runs at start
touches the DQ journal, so on 1.5.5 as the store opens today D1 fails on
the window's run — the 2026-10-07 run above did, on the DQ journal alone. A
`CHECKPOINT` of the replayed WAL before anything else runs —
`duckdb_switch.open_file`, the kill guard ("A killed DuckDB writer, and what the
next start used to lose"), merged after this was written — should turn it
PASS, and nothing here depends on it; the rehearsal has not yet been run
against an image that carries it.
Two earlier notes here called it one odd segment, then a DQ-journal matter,
and the judges first read P4a and P5 after the kill, which passed what the
product could not show anyone.
`test_the_sweep_finds_what_a_kill_costs_duckdb_1_5_5` holds the loss on
DuckDB itself: if an upgrade makes it zero, re-measure before changing it.

### OD-10: the DuckDB-only doors, retired (2026-09-30)

Step 13 waited on every door in `web/` that read DuckDB's Silver, Gold or UTM
with no read switch: after the flip each would have answered from a Silver as
old as the switch, and after the first Sunday compaction from none. OD-10 was
"port or retire", door by door. **The owner retired all of them, in one
change** — none had a caller, and most had stopped working long before:

- **The buyer, order and product cards** (`GET /api/buyers/{id}`,
  `/api/orders/{id}`, `/api/products/{id}`) and the assistant's
  `get_buyer_details` / `get_order_details`. `dict()` of a DuckDB tuple
  answered 404 for every real id from 2026-02-06 on, and the order card's
  `op.sku` named a column `order_products` never had. The model was being
  told real customers did not exist. The Meilisearch searches stay; a customer
  card, if one is wanted, is built new on Postgres behind a read switch.
- **The two debug routes** (`/api/debug/stale-returns`,
  `/api/debug/order-status/{id}`): DuckDB's Bronze against its Silver. The
  first checked four of the six return statuses; the second's KeyCRM half
  called a method the client never had. `pg_silver_row_values` asks the same
  question of the store the tabs read.
- **`/api/buyers/stats`**: counters that read "all synced" after a
  compaction. The number that mattered is the buyer step's own selection from
  Postgres; DuckDB's raw counts stay at `/api/duckdb/stats`.
- **`POST /api/duckdb/purge-orders`**: the April 2026 DuckDB 1.5 MVCC
  one-shot. It never reached Postgres, so it had stopped changing any number;
  deleting an order from Postgres is chain 3's to design.
- **The legacy 06:00 `reconciliation_check`**, `GET /api/reconciliation` and
  `POST /api/reconciliation/run`, with the comparator only they used
  (`reconcile_with_api`, `get_order_summaries_by_date`,
  `log_reconciliation`). `dq_reconciliation` compares 90 days per order
  against all three stores from one fetch. `reconciliation_log` had no other
  writer, so it is **history now** — OD-13's freeze — and the table, its
  hourly copy and its daily comparison stay; a test fails if anything writes
  it again. The manual levers are `POST /api/reconcile` (detection) and
  `POST /api/duckdb/refresh-statuses` (repair).
  **The legacy job made one repair nothing else made, and it was kept.** A
  B2B order's `ordered_at` can sit weeks after its `created_at`, and KeyCRM
  does not bump `updated_at` on a status change. So a moved status on an
  order created more than 30 days ago and ordered inside them was re-fetched
  by nothing else. `order_status_refresh` fetches by creation date, the
  weekly full sync skips the order as unchanged, and `dq_reconciliation`
  filed it as STATUS_DRIFT (CRITICAL) every morning but repairs only
  MISSING_IN_DK. The legacy fetch was padded by 30 days for exactly this.
  `refresh_order_statuses` now finishes by re-fetching those orders by id
  (`find_backdated_order_ids`, Kyiv dates): 156 ids over 183 daily runs on
  the 2026-08-31 copy, 0.85 a day and at most 5, each one API call
  (`tests/unit/test_status_refresh_backdated.py`).
- **`scripts/migrate_sqlite_to_duckdb.py`**: dead at both ends since the bot
  and the user list moved to Postgres, and it wrote roles from two hardcoded
  ids.

**Kept on purpose**: the DuckDB half of `POST /api/warehouse/rebuild-silver`
(DN-29 already stands it down, and the way back needs it; it goes with step
14's code deletion), the assistant's revenue tools (behind `KS_READ_CHAT`),
and `POST /api/duckdb/sync-all-buyers` (the only `include=loyalty,shipping`
fetch; chain 4 routes it). No table was dropped and none was touched in
`_init_schema` or the compaction's lists — OD-11 is still open.

**The sweep that closed OD-10 found one more reader**, and it is a
precondition now rather than a door. `_expenses_run` sends the /expenses
summary and profit analysis to Postgres only once
`meta.mirror_state.backfilled_at` is set for `bronze.expenses`, whatever
`KS_READ_EXPENSES` says — and a NULL there is an answer, not a failure, so
DuckDB's Silver serves them uncounted and unrefused even under
`KS_READ_FALLBACK=off`. Production has it set; a fresh host or a restore older
than the backfill would not, and after the switch that page would read HTTP
200 and zero expenses. So `expenses_backfilled` is read in the revision's
path — the cutover's own connection at a start, the pool for the status page,
one bound for both, retried with the revision, published by class — and only
of a Postgres that said its revision, so a start that cannot reach Postgres
still names `pg_revision` alone.

### Stage 5's two clocks: the parallel period and the week of silence (OD-17 (a))

Stage 5 — the end of DuckDB — waits for two things in a row (owner decision
OD-17 (a)): a **14-day parallel period** counted from the last `KS_WRITE_*`
flag, then a **7-day week of silence**. The period was 30 days until the
owner shortened it on 2026-10-08, with the soak after step 13 and after chain
3 cut from seven days to three and `KS_READ_FALLBACK=off` taken on the day of
a clean rehearsal; the week of silence and "no DROP before a week after full
completion, with a tested way back" were kept. Four things breach either one and
restart its count: web opens DuckDB, the file's hash changes, a rollback lever
is used, a read is served from DuckDB. None of the tooling below drops,
deletes or moves anything — OD-11 (a) forbids any DROP before the owner's week
after full completion, and schema removal, a `DERIVED_TABLES` addition and
deleting the file all count. Everything is off or read-only by default;
production behaves as before.

**Why two clocks, not one.** Until stage 5 decouples the code, web opens the
file on every boot and writes it all day, so "web opens DuckDB" and "the hash
changed" are true every minute under today's settings and measure nothing.
The parallel period is therefore judged on the two breaches that *can* be
measured beside a live DuckDB — levers and fallbacks — and the week of silence
on all four, with web running `KS_DUCKDB=off`.

**`KS_DUCKDB`** (`core/duckdb_switch.py`; `on` default, `off`). `open_file` is
the only reach for the driver in `core/`, `web/` and `bot/` — `connect`, and
every function on its default connection, which `ATTACH` points at any file.
A test walks the three trees, and the host tools in `scripts/`/`deploy/` are
an exemption map the walk must equal; every read-write open the kill guard
exempts from the checkpoint is one of them, under the same name. The same
function is the kill guard's checkpoint-first opener (above): the refusal,
then the CHECKPOINT, one driver call between them. It reads the driver however it is
reached: imported, re-exported (`from core.duckdb_store import duckdb`, or
`<module>.duckdb`), imported by its name (`import_module('duckdb')`,
`sys.modules`), through `getattr`, or handed on as a value. It cannot read a
module named by a variable, nor anything that is not Python; the first walk
read `duckdb.connect` alone and let four such spellings past (review of
02.10). The weekly compaction's phase 1 — the one scheduled process outside
web that opens the live file, read-only, which no hash can see — and the
nightly off-site's snapshot, which runs the same phase, open through the
switch: their sidecars start from `.env`, so under `off` both are refused
before the driver runs, each with one line saying its cron line is retired
at the start of the week. Under `off` an open is refused **before the
driver runs**, so the file is not even created;
counted **at the raise**, because dozens of callers wrap `get_store()` in
`except Exception` and a swallowed refusal is exactly the open the week must
not miss; logged CRITICAL; published as `duckdb_switch.opened_while_off`
(`{site: {count, last_at}}`, a site being `module:function`, at most 20, never
exception text); and paged **CRITICAL `duckdb_opened_while_off`**, whose lever
goes first because under `off` the outage beside it is its symptom. A typo
runs as `on` and warns `duckdb_mode_invalid` (OD-09: web is the only syncer).
**`off` does not make web work** — order intake stops and the dashboard
errors; startup contains the refusal only so `/api/health` answers and the
page can be seen. It is not set anywhere until the decoupling ships.

**The file's hash** — `deploy/duckdb_silence_check.sh`, a host tool, **not
installed**: hourly from root's crontab at the start of the week (the line is
in its header). sha256 of the file and its `.wal`, O_RDONLY and no lock, so it
can never be the change it reports; record in `/root/duckdb-silence/state`
(600, atomic, parsed and never sourced) and one history line per run, cut to
500 by the run that grows it. `--peek` hashes and writes nothing; `--status`
reads the record and hashes nothing — that is what the soak calls. A MISSING
file keeps the recorded hash as the reference, and the soak fails on it under
either mode: deleting the file is a DROP. The record also keeps `missing_at`,
the last check that found the file gone, through every later run: a file
moved away and back byte for byte reads UNCHANGED, and until the key existed
only the history file — which nothing reads — remembered the episode, so the
soak passed (review of 02.10). It cannot see a change and its exact reversal
inside one hour, nor a read-only open.

**A copy-back now leaves a trace.** `scripts/chain_copy_back.py` is the only
lever that gives a chain back to DuckDB, and it used to leave only an absence
— owner rows deleted, a marker unlinked, its report gone with its `--rm`
container. `release_chain` now writes one `app.alert_events` row
(`condition_key = 'lever:chain_copy_back'`, `event_type = 'lever_used'`,
`context` naming the chain and outcome) **inside the transaction that deletes
the owner rows** (`core/lever_journal.py`): a release whose record fails does
not commit. Exit 3 writes `committed_not_released` while the owner rows still
stand. No revision — `event_type` has no CHECK, and every reader of the journal
asks for `fired`/`escalated`/`resolved` by name. A copy-back container that
inherits `KS_DUCKDB=off` is refused (exit 2); `-e KS_DUCKDB=on` is the
decision to restart both clocks, said out loud.

**The soak checks**, `deploy/stage4_soak/50`–`53`, read-only as `ks_readonly`:

- **P1** the file record. FAIL on MISSING, or a `missing_at` inside the day,
  always; under `off`, FAIL on a change at the last check or inside the day,
  UNKNOWN with no record, one over 3 h old, or one younger than the day;
  under `on`, not applicable.
- **P2** levers in the day: a `lever_used` row, a `write_chain_flag_mismatch`
  or `warehouse_hold_stuck` page (fired, escalated, resolved or still
  standing), step 13 given back (`warehouse_writer` = duckdb with `since` in
  the day), and `warehouse_preconditions_unmet` — but only once a period is
  declared or web runs `off`, because before the step-13 flip that page means
  "held back", not "rolled back". And `warehouse_way_back_refused`, always:
  while chain 3 or 5 owns what DuckDB derives from, the way back is refused
  and pages that key *instead of* the one above — in the parallel period,
  with every chain latched, the only page a way back can raise — and it can
  only happen after a flip. UNKNOWN when nothing says a page could
  have been journaled: the pages reach `app.alert_events` only through the
  bot's fire-and-forget alert archive (nothing without `KS_PG_DSN`, standing
  down while Postgres is slow), whose proof of life is the
  `watch:read_fallbacks` row the same writer rewrites every probe — none, or
  none for 35 min, and an empty journal says nothing (review of 02.10: it used
  to PASS on one nobody wrote). A recorded copy-back FAILs without it.
- **P3** the parallel period. Starts at the latest of `SOAK_PARALLEL_FROM`
  (the operator declares the last flip — nothing in the database knows which
  flip the stage needed), the newest `owner:` row, step 13's switch, every
  breach (P2's and F1's pages), and the `watch:read_fallbacks` clean-since — an
  unwatched stretch is not a clean one. Covered at **720 h**.
- **P4** the week of silence, under `off` only. Starts at the latest of the
  `watch:duckdb_switch` clean-since (the canary writes it only while web runs
  `off`, on F1's 35-minute rule), the file's unchanged-since, the last edit of
  `.env`, every breach of all four kinds — the file's `missing_at` among them —
  and F1's watch. `.env` is asked because the Sunday compaction and the
  nightly off-site start their sidecars from it, not from web's environment,
  and only the switch in the sidecar refuses their read-only open: web `off` with `.env` not saying `off` the way
  `docker run --env-file` reads it (last line, quotes kept) is a FAIL, and any
  edit of the file restarts the week. Covered at **168 h** — a full week holds
  Sunday 05:00 and Monday 09:30 by construction — **and** an
  `app.weekly_report_sends.sent_at` after the start, because a week that never
  delivered a report has not shown the Monday path works without DuckDB.
  Today that ledger is written in DuckDB and copied, so under `off` it cannot
  be covered: correctly, since the decoupling has not happened.

These are the measurements, not the gate: every chain latched, source
reconciliation clean, `KS_READ_FALLBACK=off`, the second Ark and the rest of
the plan's stage-5 preconditions are their own checks.

### The order write path asks the registry too (DN-22a)

Until DN-22a nothing that ships orders out of DuckDB asked who owns the order
tables, so registering a chain for `bronze.orders` would have had the sync's
mirror overwrite the chain's rows and archive each overwrite in
`app.order_versions` as a change that never happened. Now every such path asks
`core.pg_landing.order_tables_stood_down()` **before** `write_orders`, never
inside its transaction: the sync's mirror in `upsert_orders` skips, the ids-diff
and header-only repair refuse, the hourly diff returns `stood_down`, the
comment ship reports `skipped`, `POST /api/mirror/backfill/orders` answers 409,
and the bucket comparison files `mirror_stood_down` (INFO). Either table stands
both down, because ownership of the order tables passes as a unit: a chain that
declares one declares both, which `tests/unit/test_write_chains.py` walks the
registry for. It is not that they cannot ship apart — the 05:15 refresh and the
comment ship send headers alone every day. The question is asked only of
chains that declare an order table (`write_chains.stood_down_among`) — none do
today, so it reads no variable and no file, and nothing changed in production.
`tests/unit/test_write_chains.py` walks `core/`, `web/` and `scripts/` for any
caller of `write_orders` or `mirror_orders` that does not ask.

The paths that already hold a pool — the ids-diff and its repair, the hourly
diff, the comment ship and the bucket comparison — then ask
`order_tables_stood_down_or_owned(pool)` too, which adds the `owner:` rows in
`meta.chain_watermarks`: DN-06's rule that anything holding a Postgres
connection stands down on either copy of the latch, so a lost marker cannot
make the chain's rows look like DuckDB's again. It is asked after
`require_revision()` and a read that fails raises into each path's own
handling, as in `replicate_operational`. Only the sync's per-tick mirror stays
on the local answer — the write path, where that read is the one to avoid.
An owner row naming an order table counts even when no chain in this build
declares it: after an image rollback to a build older than the orders chain,
`claimed_tables` alone would drop `owner:bronze.orders` and hand the tables
back to DuckDB, so the helper reads the row as itself too, and either order
table owned stands both down. The comparison files a different check for
each answer, because the two are not the same state. On the local answer the
sync's mirror has stopped too, and `mirror_stood_down` stays INFO — a decision
somebody took. On the owner rows alone it has not — the per-tick mirror asks
only the local answer — so every tick writes DuckDB's copy over the chain's
rows, and that is `order_owner_row_without_marker`, **CRITICAL**, whose lever
names `data/write-chain-owners` and `scripts/chain_copy_back.py` (or, with no
chain declared in this build, a redeploy of one that does). It was the same
INFO once, and a page or the digest prints only the check's label and lever,
never its description: all three said "not a defect" about the one state
here that is. Production today files neither: no chain declares an order
table and no owner row names one.
`POST /api/mirror/backfill/orders` asks the owner rows too, before anything
starts: a lost marker is a 409, not a "started" whose refusal lands in the web
log, and an owner read that fails — `SchemaVersionError` included — is a 503.
With `KS_MIRROR_LANDING` off it answers 409 before any of that, asking
Postgres nothing: the backfill refuses a switched-off mirror, and the route
used to say "started" (or 500 in the foreground) to the run it refused.

### Chain 6a: the expense-type dictionary (DN-26, off)

`bronze.expense_types` was the one catalogue table in chain 8's exact shape:
KeyCRM serves the 27 rows only to the weekly full sync, so DuckDB was its
source and the hourly full replace copied it. Under
`KS_WRITE_EXPENSE_TYPES=postgres` `upsert_expense_types` writes Postgres
directly (`core/pg_expense_types_write.py`), **after** `core.landing_rows`
has resolved the localisation-key names, so both stores are handed the same
names; `last_sync_expense_types` moves with it, and the hourly copy and the
daily comparison stand down. Three things differ from the chains before it:

- **`full_sync` contains the raise.** A Postgres fault, or a flag nobody can
  read, leaves the watermark where it was and the sync carries on with
  products and orders — one dictionary must not cost a week's orders, or the
  whole history on a boot with an empty DuckDB. With the chain on DuckDB the
  raise goes out as it always has.
- **Its watermark is not judged at 90 minutes.** It moves weekly;
  `CHAIN_WATERMARK_MAX_AGE_MIN = None` leaves it to `_freshness_check`'s
  192 h. The standing watch judges instead that the table is not empty
  (CRITICAL — /expenses collapses into "Other") and that no name is still a
  localisation key (WARN).
- **Its watermark is inherited across the flip.** Postgres holds no
  `last_sync_expense_types` until the Sunday full sync under the flag writes
  one, and a mid-week flip used to file "entity never synced" on every
  integrity run until then. `CHAIN_WATERMARK_INHERITS_DUCKDB` has the check
  judge DuckDB's frozen stamp for the absent key instead — the age of what
  the hourly copy last shipped — at the same 192 h, so a first sync that
  fails still pages. Chain 1 does not declare it: its keys move hourly.

**`KS_READ_EXPENSES=postgres` comes first, and the chain enforces it.** Every
reader of the dictionary goes through `_expenses_run`, so with the read flag
off a chain writing Postgres would leave each type KeyCRM adds in Postgres
alone, shown as "Other", with nothing to say so. `unmet_precondition()` names
the read flag; until it is on, an unlatched chain runs as duckdb whatever
`KS_WRITE_EXPENSE_TYPES` says — `core.write_chains` reads the same answer, so
the hourly copy keeps shipping — and the write_chains block in `/api/health`
publishes `unmet_precondition`, which the canary turns into
`write_chain_precondition_unmet` (WARN). A latched chain keeps writing
Postgres (OD-19 (a)) and the same warning says its readers are on the frozen
copy. DN-27's rule, generic in the registry. Chain 8 carries the same
assumption unenforced: it is live, so enforcing it is its own change.
Rollback is `scripts/chain_copy_back.py expense_types` once latched.

**The latch waits for a connection.** Every chain latches inside
`pool.acquire()`, not between `_pool()` and it. `core.pg.get_pool` sets no
acquire timeout, so a pool with nothing free — the Sunday sync holding all
five — is a wait, not a failure, and a latch taken before the acquire sat on
disk through the whole wait. What ends a wait without a connection: a Postgres
restart (or a reset that failed on release) leaves the connection handed back
dead and the reconnect is refused; a cancellation; a closed pool; or the
process stopped mid-wait. Each of those used to move the chain with nothing
written. Nothing in web cancels these writers: `RequestTimeoutMiddleware`
answers 504 and lets the handler run on (Starlette's `BaseHTTPMiddleware`,
checked on 1.6.0), so an expense can land after its 504. 6a was written that
way; chains 1, 8 and 7a latched before the acquire until 2026-09-25 and were
moved, chain 8 while live — only *when* its latch is taken changed.
`tests/unit/test_chain_latch.py` holds every registered chain to it by nesting,
not by line: each `_latch()` inside `async with …acquire()`, each `claim`
inside `async with …transaction()`. Its walk counts as a writer every public
async function of a chain module that reaches a connection or a writing
statement, through the module's private helpers and constants — the first walk
read only SQL spelled in the function itself and passed a writer whose
statement sat in a helper — bar the readers it names and holds to writing
nothing. A function handed its connection whose statement lives in another
module is still past it. `tests/integration/test_latch_waits_for_a_connection.py`
proves it per writer against a live pool: no marker while it waits, none and
no owner row after a refused reconnect, a cancellation or a closed pool; no
owner row after a write that rolls back; and a first write takes the marker
and claims its owner rows in the transaction that writes the row.

### Chain 6: the catalogue (off)

`core/pg_catalogue_write.py` is the seventh registered chain, after chain 3:
`bronze.products` and `bronze.categories`, moved by `KS_WRITE_CATALOGUE=postgres` (default
`duckdb`, which changes nothing). **Off in production.** It does not "delete
the DuckDB half", as the chain map once said: like 6a it inverts the write.
`DuckDBStore.upsert_products` / `upsert_categories` hand the rows
`core.landing_rows` parsed to the writer, so brand and price are read one way
whichever store is written, and DuckDB's two tables freeze in place — they stay
in `_init_schema` (OD-11 (a)).

**Retired and lost are told apart by one clock** (OD-15 (a), no new column).
Every write is the whole catalogue — every product hourly, every category on
the weekly full sync — upserted with `mirrored_at = now()`, and in the same
transaction `meta.mirror_state.last_ok_at` is stamped with the mirror's own
statement (`pg_landing.WATERMARK_OK_SQL`). So a row the last write carried has
`mirrored_at = last_ok_at` exactly, a row it left out is earlier — KeyCRM
retired it, and the writer never deletes — and a later one was written round
the chain. Checked on a real Postgres with the mirror's statements before it
was built: one transaction, one `now()`, for the rows, the owner rows and the
watermark alike (one `xmin`). The writer de-duplicates by id, last wins, and
sorts, so `last_rows` counts distinct rows and two overlapping writes take
their locks in one order. **Full-catalogue contract**: because each write
stamps the watermark, a writer handed part of the catalogue would make the
rest read retired, so a walk holds the writer to the two repository methods
and those to `full_sync` and the hourly products step.

**"Later than `last_ok_at`" was evidence for one hour only.** The next full
write moves the watermark past a row written round the chain, and a row KeyCRM
does not serve — a stray insert, a retired product edited — is not in its
payload: it read as retired, and the copy-back, whose rule was "`mirrored_at`
at or after the latch", carried it into DuckDB and released (reproduced by the
chain-6 review). So each write also keeps, in its own transaction, a record of
the instants the chain has written at — `writes:<table>` in
`meta.chain_watermarks`, microseconds since the epoch, pruned to the stamps
some row still carries, so bounded by the table and not by the number of
writes — locked before any product so two writes cannot lose each other's
stamp. From the latch on, "the chain wrote this row" means its `mirrored_at`
is a recorded stamp, for the watch and the copy-back alike; any other instant
at or after the latch stays CRITICAL until KeyCRM serves the row again or a
human deletes or restores it. What it cannot see, and says so: an edit that
leaves `mirrored_at` alone, or copies a recorded instant — on a retired row
only a trigger or a migration would date that. The copy-back releases the
record with the latch.

**The standing watch** reads both tables four times a day: empty and written
round (CRITICAL), rows lost (`last_rows − current − around`, CRITICAL, judged
only once the chain's own write stamped the watermark — the mirror's
`last_rows` counts a repeated payload id twice; "the chain's own" is
`last_ok_at` at or after the owner rows' `updated_at`, both Postgres's clock,
never the local marker's stamp, which a host clock a millisecond ahead of
Postgres put after its own latching write), retired (INFO, with ids —
product 1055 in production) and a short write (WARN when the last write left
out over 5% and 10 rows of what the write before it carried — the record's
`previous`: the sync's pagination stops on the first short page, so a
truncated catalogue reads as mass retirement. Measured per write, not as
retired against the table, which only rises because nothing deletes, and so
warned after every complete write once ordinary retirements passed 5%). Its watermarks are left to the freshness check (48
h, 192 h) and inherited from DuckDB until the first write under the flag.

**Two hazards 6a's template did not cover, closed before the flag.** The
products watermark was read at the top of the incremental tick, ahead of the
orders, where a flag typo or a Postgres outage would have stopped order
intake every minute; off DuckDB that read moves into the hourly products step
(`SyncService._catalogue_step_postgres`), bounded and shielded like the
buyers', and a failure — the read, a refused catalogue, Postgres down — is
recorded, published as `write_chains.pg_catalogue_write.sync_step` and retried
in ten minutes, during which neither KeyCRM nor Postgres is asked anything.
And `full_sync` wrote both tables uncontained; off DuckDB a failure there now
leaves the watermark, stamps `meta.mirror_state` failing and carries on with
the orders. A catalogue Postgres would refuse (a NULL name, a NUL, a price
outside `NUMERIC(12,2)`) is refused whole before the latch, never row by row —
a skipped row would read as retired.

**Held on DuckDB until its readers move.** `unmet_precondition()` names chain 1
not on Postgres (DuckDB's SKU status joins DuckDB's products), the read
fallback not `off`, and every `warehouse_cutover.WAREHOUSE_READERS` switch not
on postgres — read through `readers_not_on_postgres`, the one helper the
step-13 switch files its `reader:*` keys from, so the two cannot disagree.
Over-inclusive on purpose: the walked list beats a hand-picked one. The warning
is rate-limited to once an hour.

**Product 1055 is carried before the flip**, by
`POST /api/mirror/backfill/catalogue` (dry run by default;
`pg_landing.carry_retired_catalogue`). It ships only the rows the daily
comparison already calls retired — its own rule, `synced_at <= last_ok_at`,
pinned row for row against `compare_table` — with a `mirrored_at` before the
watermark, so they still read as retired, and never touches
`meta.mirror_state`. It refuses with the mirror off and on either copy of the
latch. Once carried, Postgres reads of an order line on 1055 name it as DuckDB
does, and the daily `mirror_retired_rows` INFO goes to zero.

**`last_sync_meilisearch_pg` is not this chain's.** Its value is a Postgres
clock this chain stamps exactly as the mirror did; by OD-15 it moves with
whichever of chains 3 and 6 lands last, which is chain 3, and chain 3 declares
it (see "Chain 3"). A test keeps it off every chain that does not own an order
table, and requires it on chain 3.

**The way back** is `scripts/chain_copy_back.py catalogue`. Both tables are
mirrored landing tables there, each its own rewrite clock (`mirrored_at` one
of the chain's recorded instants, at or after the owner row's `updated_at`),
compared whole at zero. Before a flip a row on one
side only, or differing, refuses and names the carry or the next products sync
(categories: `POST /api/jobs/full_sync_weekly/trigger`); after the latch a row
the chain re-stamped is the copy's work (INFO) and a retired row that differs,
one written round the chain since the latch, or one only DuckDB holds,
refuses. The chain map's "run a full sync" rollback
is wrong under OD-19 (a): a full sync under a latched chain writes Postgres
again. No lock-out of its own, and revision 0035 — shipped for the other
chains #280 registered — holds none of its writers: an image older than the
chain writes DuckDB from the payload and mirrors the same payload through
`pg_landing._mirror`, which never asks the revision, stamping the watermark in
one transaction. What 0035 changes is the evidence: such an image is an outage
(401, no bot), and its 07:30 comparison fails on the revision instead of
paging `owner_row_without_marker`.

Proved on PostgreSQL 17.2 (`tests/integration/test_catalogue_writer.py`): the
one-`xmin` write, retirement, a repeated id, two concurrent full writes with no
deadlock, three planted defects reaching the watch by name, and the carry →
handover → flip → copy-back round trip with 1055 landing in DuckDB. Since the
review: a write round the chain still CRITICAL after the next full write and
refused by the copy-back, the record locked before any product, the short
write judged per write (20 ordinary retirements quiet, a truncated write
warned and cleared), the acquire and statement bounds, and the carry's
`DO NOTHING` and `W − 1 µs` clamp.

### Chains 9, 10, 11a, 11b: the shadow state (OD-02 (c), all off)

The quality journal (`data_quality_runs`/`_issues`/`_diffs` and the digest's
beat, chain 9, `KS_WRITE_DQ_JOURNAL`), the watchdogs' samples (`disk_samples`,
`data_dir_samples`, `memory_samples`, chain 10, `KS_WRITE_WATCHDOGS`) and the
two send ledgers (chain 11a `KS_WRITE_WEEKLY_LEDGER`, 11b
`KS_WRITE_TRAFFIC_LEDGER`) move their writer to Postgres **and DuckDB still
receives every row**. All four default to `duckdb`, which runs the block that
stood at each call site, moved behind one door per chain (`core/dq_journal.py`,
`core/watchdog_samples.py`, `core/report_ledger.py`) and walked for anything
that goes round it. No schema revision: every table has been there since
stage 3; revision 0035 only comments them and locks images older than the four
chains out (see "Revision 0035"). `warehouse_refreshes` and
`reconciliation_log` are not ported — both are frozen at their writer (step
13, OD-10).

**A third registry state: the copy stops, the comparison continues.** A chain
module declares `CHAIN_SHADOW = True`. The hourly `replicate_operational`
stands its tables down like any moved chain's — its full replace out of DuckDB
would delete every row whose shadow write failed, and for a ledger that is a
delivered week the next tick sends again. `reconcile_operational` subtracts
`write_chains.compared_in_shadow()` from what it stands down and compares
those tables facing Postgres→DuckDB (`compare_shadow`): no watermark gate, each
side's own clock for the grace, and four findings — `shadow_duckdb_only_rows`
(CRITICAL: something wrote DuckDB round the chain), `shadow_row_values`
(CRITICAL), `shadow_missing_in_duckdb` (WARN: a shadow write failed) and
`shadow_pruned_rows` (INFO: DuckDB's prune ran a tick behind). Only
`mode == "postgres"` is compared; a typo writes nowhere and stands down with
`write_chain_flag_invalid` paging.

**Postgres commits first, then DuckDB is handed the same values**
(`core/shadow_writes.into_duckdb`), never inside `store.connection()`, in one
DuckDB transaction rolled back on any error. A DuckDB failure is counted and
never raised — published as `write_chains.<chain>.shadow_failures`, class
only. A Postgres failure raises and DuckDB is not written, so DuckDB ⊆
Postgres holds by construction. The latch (OD-19 (a)) is unchanged: a row whose
shadow failed exists in Postgres alone, so the flag is no more a rollback than
chain 8's, and the way back is `scripts/chain_copy_back.py dq_journal |
watchdog | weekly_ledger | traffic_ledger`. It carries `CHAIN_MARKER_KEYS` (the
digest's beat) with the sync keys, and its handover reads a DuckDB-only sample
older than everything Postgres still holds as a lagging prune, INFO.

**Readers follow the writer, with no fallback** — chain 7a's rule. Under
`postgres` the digest, `/api/health`'s layer ages, `/api/health/data-quality`
and the startup catch-up read Postgres; a Postgres that cannot answer fails the
digest and gives the canary no `data_quality` block (`dq_block_missing`), never
DuckDB's, which would read "journal fine" at the moment its writer failed. A
flag nobody can read keeps the readers on DuckDB, where the whole journal is.

- **Chain 9** allocates `run_id` as `MAX + 1` under
  `pg_advisory_xact_lock(RUN_ID_LOCK_KEY)` in the writing transaction — no
  sequence, so no revision; a test walks `core/` for a reused lock key. The beat
  lives in `meta.chain_watermarks` (the hourly replace of `app.sync_metadata`
  would wipe it) and is inherited from DuckDB while absent there, or the first
  digest after a flip restates every standing finding. A Postgres row is
  rendered in DuckDB's session zone, read from DuckDB itself. **One finding
  per `(check_name, table_name)` in a run**, which Postgres keys
  `app.data_quality_issues` on and DuckDB does not: `chain_invariants_unwatched`
  names the part it could not read — `(<chain>)`, `meta.chain_watermarks`,
  `(write chains)` for everything, and the table itself for chain 6's
  catalogue table with no full write recorded, which can be both tables in
  one run — and the journal's door folds any repeat
  that still arrives, with an ERROR. Two blind groups under one constant name
  had Postgres refuse the whole run, or, under `duckdb`, broke the hourly
  copy of the never-pruned journal for good.
- **Chain 10** runs each job's whole persistence — the differencing reads, the
  insert, the prune — as one Postgres transaction, so a read can never be left
  on the store the insert moved away from; every instant both stores see is
  computed once. A Postgres that refuses, or a typo, returns no history and the
  job **still judges capacity and memory**: when the disk fills, Postgres is
  the first thing to refuse writes. The standing watch adds
  `chain_samples_stale` and `chain_retention_unbounded`.
- **Chains 11a/11b are at least once, never twice** (OD-16 (a)). `sent_at` is
  taken once after the delivery; the record is tried three times (~12 s) and
  then spooled to `data/report-ledger-pending/<chain>/` (temp, fsync, rename).
  The gate drains the spool, counts a spooled week as sent, adopts a row only
  DuckDB holds rather than sending it again, and raises — no send — when
  Postgres cannot answer. The traffic floor is asked before the ledger.
  `/api/health` publishes `pending`, the canary warns `report_ledger_pending`,
  the standing watch adds `chain_report_week_missing` from Wednesday on. The
  residual: Postgres refusing three times **and** the spool unwritable raises,
  and the next tick sends again — today's exposure on the `duckdb` path, left
  as it is because closing it there changes the default. Flip and roll back
  Wednesday to Sunday. **A spool outlives the flag that wrote it**: the gate
  under `duckdb` lands it in DuckDB and counts a still-spooled week as sent
  (a flag put back after a record that never latched, or after a copy-back,
  sent the week twice before the review); the handover reports the spool —
  INFO, a file that will not parse CRITICAL — and `--execute` lands it in
  Postgres before it reads, refusing if anything is left.

**Soak:** H1 (`28_h1`) — the hourly copy stood down on each on-chain's tables
since its owner rows; H2 (`29_h2`) — the 07:30 run filed no `shadow_*` above
INFO. D8, 20, 21 and 22 stop gating on the journal's copy once chain 9 writes
it directly (the flag, the latch, or its owner row); chain 1's preflight and
chain 7b's goals dry run do the same on the flag or the latch. Every guard names its
mutation; a run over all of them found five that did not catch theirs, now
closed in the tests.

### Chain 7b: the goals read Silver, and a GET never writes (OD-14)

The goal calculators — seasonality, YoY, weekly patterns, the growth cap, and
a smart goal's last-year and recent-months reads — read the whole order
history, and since DN-12 they read it through a bridge: DuckDB `orders`
narrowed by `silver_sales_type_case` rendered over DuckDB's `managers` and
`manager_classifications`. Step 13 freezes all three. Two changes, neither of
which moves a number in production by itself.

**7b-1 — compute, then store once.** The calculators return and store
nothing; `recalculate_goal_tables` is the one writer of `seasonal_indices`,
`growth_metrics` and `weekly_patterns`, reached from the Monday job (the
first two, as always) and `POST /api/goals/recalculate` (all three, retail
only — the tables carry no `sales_type`, so the rows mean retail and any other
value is a 400). It reads everything first and writes in **one** DuckDB
transaction: they were twelve autocommitted upserts then twelve autocommitted
YoY updates, so a failure between them left this week's indices beside last
week's growth. `GET /api/goals/smart` used to recompute and store all three
whenever `seasonal_indices` held fewer than twelve rows, for any viewer and
with the viewer's `sales_type`; `GET /api/goals/forecast?recalculate=true`
did it on request. Neither does now: the smart goal reads only, a short
table falls back per month as it always did, and `recalculate=true` is a 400
naming the POST, for an admin too. **The trade, taken knowingly:**
`weekly_patterns` has no automatic writer now — the Monday job stores the
other two (OQ-2) and only the POST stores it — so on a host whose goal
tables start empty, milestone weeks use the default weights (0.23 a week,
0.08 for the fifth) until an admin POSTs, where the first page view used to
fill them. Production holds all three (12/60/1 rows), and the Sunday
compaction exports them: none is in its `DERIVED_TABLES`. A sweep runs every GET under `/api` with
`seasonal_indices` emptied and every optional boolean switched on, and fails
on any that changes a goal table. **The YoY crash it fixed** would have come on
2026-11-02: from the first order dated 1 November the running year has eleven
months and counted as "full", a second pair of years appeared, and the
recency weighting multiplied DuckDB `Decimal`s by float weights —
`TypeError`, in the Monday job and in `GET /api/goals/growth`. The yearly
read now never takes the current Kyiv year, and every term is a float.

**7b-2 — `KS_GOALS_HISTORY` chooses which orders count**: `bridge` (default,
today's) or `silver` — every history read over `{silver_orders}` through
`_goals_run`, so the engine is `KS_READ_GOALS`', and DN-20's counting and
refusal cover it. An unknown value raises at the read, never at import —
counting another set of orders in silence would be the worse failure — and
since every goal read then answers 500 and the Monday job fails, neither of
which pages anybody, `/api/health` publishes `goals_history {mode, error}`
and the canary pages `goals_history_mode_invalid`, CRITICAL like
`write_chain_flag_invalid`, with the variable as its lever. Not
`KS_READ_GOALS` reused: that is `postgres` in production already, so a reuse
would have moved the reads at the deploy; it names an engine, this names a
row set; and an engine switch put back must never change semantics. Not
`KS_READ_*` either — the step-13 walk reads those as engines.
**`is_active_source` is deliberately absent** (OQ-1, the owner's): source 3
is the 2024 website on Opencart — 2 055 retail orders, ₴5.0M, July to
December 2024, before Shopify took over — and dropping it shrinks 2024, so
retail YoY would move from 0.50 to 0.82 and October's retail goal from ₴4.0M
to ₴4.2M. Without it, Silver selects exactly the bridge's orders.

Measured with the built code on the 2026-08-31 production backup, as of
2026-10-01: the orders each side counts are identical for every sales type
(retail 42 054), and **1 207 numbers — every calculator for every sales type,
the Monday job's store and the smart goal for four months — show 0
differences**. The one rule that could differ is the return: KeyCRM's status
group decides before the status list (0 orders in production). On a real
Postgres the same bodies answer the bridge's numbers to 1e-6
(`tests/integration/test_goals_history_two_engines.py`), including with
DuckDB's orders, classification, Silver and Gold refused at the statement and
with DuckDB's Silver emptied.

**The flip**: `KS_GOALS_HISTORY=silver` with `KS_READ_GOALS=postgres`, after
`scripts/goals_semantics_dryrun.py --backup <that day's backup>` exits 0,
run as the web service (`docker compose run --rm --no-deps -T web`) once
that morning's 07:30 `mirror_landing` run has reached the journal copy
(hourly); not before 04:00 on a Monday, so the first Monday job under
`silver` is watched. **Exit 0 needs both halves.** The backup half runs the
real goal methods both ways over a read-only in-memory copy and files every
difference under its cause — but it reads DuckDB's Silver on both sides,
and the flip reads Postgres', so a Postgres Silver missing or misclassifying
orders would have read clean there (review of 7b-2). The Postgres half
reads, as `ks_readonly` through `utm_reclassify_dryrun.py`'s door (never
`KS_PG_DSN`), the verdict of the one comparison that sets the two Silvers
against each other at one instant: the latest `mirror_landing` run's
`reconcile_silver`, from Postgres' copy of the quality journal. Clean only
when the copy is under 75 min old and not failing — unless chain 9 writes the
journal in Postgres (`reads_postgres()`: its flag or its latch), when the run
is read from the writer and the stood-down copy's frozen mark is no reason,
as in chain 1's preflight — the run under 30 h (chain
1's preflight limits, read from it), the run did not fail in
`reconcile_silver`, `setup` or unparseably, it filed nothing against
`silver.orders`, and neither switch that stands `reconcile_silver` down is
set here — `KS_MIRROR_LANDING` off, or `KS_WRITE_WAREHOUSE` anything but
`duckdb` — since it files nothing then. A backup whose
`sync_metadata.warehouse_writer` reads `postgres` (switched, or a way back
still owed its validated full tick) is refused: its Silver is frozen and the
comparison stood down, so neither half could answer. Before the warehouse
switch neither can hold, since `goals_bridge` is one of its preconditions;
a re-flip after one is when they would. Exit 1 is a difference or Postgres Silver not proved,
each named; 2 a refusal (no read-only login, one that can write, Postgres
unreachable, a backup after the switch); `--backup-only` skips the Postgres half and exits 3 on a clean
backup, never 0. What the verdict cannot see is a Postgres Silver that went
wrong after that run, which is why it is the flip day's run. Rollback is unsetting the
variable and `up -d web`, number-neutral by the same measurement, while the
bridge exists. `goals_bridge` in step 13's readiness is **met only under
`silver`** — DuckDB's orders freeze at the switch whoever owns them — and its
detail names any write chain already owning a bridge table, which is when
retail goals start diverging. The CI tripwire in
`tests/unit/test_goals_off_duckdb_silver.py` stays until the bridge is
deleted (7b-4, after the flip's soak); chains 3 and 5 wait for that. The
three goal tables' writer moving to Postgres is 7b-3's, below.

### Chain 7b-3: the goal and forecast tables' writer (off)

`KS_WRITE_FORECAST` (`duckdb` default | `postgres`), `core/pg_forecast_write.py`:
`app.seasonal_indices`, `app.growth_metrics`, `app.weekly_patterns` and
`app.revenue_predictions` (revision 0025, ~313 rows; no migration). Until it
writes Postgres, DuckDB writes all four and the hourly full replace carries
them. Two writers, one chain: `persist_goal_tables` (the three goal tables —
the Monday `seasonality_calc`, which stores two, and `POST
/api/goals/recalculate`, which stores all three) and `store_predictions`
(every training). Routed in the repository, `_persist_goal_tables` and
`store_predictions`, where every caller arrives. Either writer's first write
latches the chain and claims all four owner rows: they move as a unit.

**One transaction, and the YoY must land.** The index upsert never touches
`yoy_growth`; the YoY is one `UPDATE` per month after it. Split or reordered,
that UPDATE matches nothing and says nothing, and `generate_smart_goals` then
reads NULL and silently uses the cap. So both run in one transaction, upsert
first, and every UPDATE must report `UPDATE 1` or the set rolls back
(`executemany` returns no per-row status, so it is a loop). One advisory lock
for the chain (`pg_locks` objid 31491) restores the serialisation DuckDB's
store lock gave: two `store_predictions` over one range otherwise die on the
primary key.

**Refused before the latch, NaN included.** DuckDB 1.5.5 refuses NaN, ±inf
and overflow in `DECIMAL(6,2)`; Postgres 17.2 refuses the last two and
**stores NaN** (measured). MAPE is unbounded (a zero-revenue validation day),
so every number goes through `core.pg_numeric.refusal` first, which refuses
NaN on purpose; and the model's ISO-string dates become `date`s there, since
asyncpg refuses a string where a DATE goes. `predict_month` swallows a failed
store, as always, so `_train_impl`'s result (the job's, and `POST
/api/revenue/forecast/train`'s) now carries `predictions_stored`.

**The reads follow the chain, and only the chain.** While it writes Postgres,
every statement reading one of the four goes there whatever `KS_READ_GOALS`
says, with no fallback (7a's rule). A failure there is a **counted refusal**
in either mode (`read_fallback.chain_refusal`): a 503 naming `goals`, and
`read_fallback_mode.refused` on `/api/health`, which the canary pages as
`read_refused` — what the same failure was before the flip under the
precondition's `off`. Raw, as 7a's reads still are, it reached two handlers
that let `ReadUnavailable` through and contain everything else:
`/api/revenue/forecast` answered 200 "Forecast not available yet" and the
smart goal dropped its ML signal, with nothing counted. While it does not,
each keeps today's engine: the forecast by `KS_READ_GOALS`, the three goal
tables from DuckDB.
The smart goal reads the three in **one** `UNION ALL` statement through
`_goal_tables_run` — one committed set in either engine, and never
`KS_READ_GOALS`, which in production would read the replica up to an hour
behind a POST. `CAST(NULL AS INTEGER)`, because Postgres types a UNION column
from its first branch. Weekly rows are sorted in Python: the residual goes to
`max(...)`, which breaks ties by insertion order.

**Held until its inputs are Postgres** (`unmet_precondition`, 6a's
arrangement): `KS_GOALS_HISTORY=silver`, `KS_READ_GOALS=postgres` with a DSN,
`KS_READ_FALLBACK=off` as configured at start, and `KS_READ_FORECAST_INPUT=
postgres` (the training frame; on in production since 2026-09-16, so it
costs nothing and stops a later rollback of that flag training on DuckDB Gold).
Unmet and unlatched, the chain runs as duckdb for every consumer — writer,
reads (unlike 7a's `reads_postgres`), shipper, comparison — and the canary
warns `write_chain_precondition_unmet`.

**Standing watch** (integrity layer, all WARN — every value is recomputable):
`chain_goal_tables_incomplete` (fewer than 12 months, a NULL `yoy_growth`, no
measured `yoy_overall`, or the 0.10 placeholder while `period_start` already
holds two full years — the 2026-08-31 backup's state), `chain_goal_tables_stale`
and `chain_forecast_stale`, judged against the last slot of the scheduler's
own constants plus 2 h 30 min, so a missed Monday or Thursday is in that
morning's 07:00 run. **Training is twice weekly, not daily**; a flat limit
would have to exceed the Thu→Mon gap.

**The goals dry run** (`scripts/goals_semantics_dryrun.py`) pins the chain's
two answers to DuckDB and makes its pool raise (`held_off_chain_7b3`): run as
the web service under a latched chain it would store into production Postgres
and latch from a one-off. Each wall is tested on its own — the pins under a
latched chain and under a flagged one whose precondition holds, the pool by
calling both writers inside the pins.

**The way back.** Flagged and not yet latched: unset, `up -d web`, free.
Latched: `scripts/chain_copy_back.py forecast` (specs derived, no sequence,
no sync key; the stamps forgiven as above), then the flag, then
`replicate_operational`. Recalculating and retraining are the cheaper way to
fix the *content* — they write wherever the chain writes — but only the
copy-back releases a latch. The model artefact (`data/revenue_model.joblib`
and its JSON siblings) is a file and does not move.

**The flip**, after `KS_GOALS_HISTORY=silver` with `KS_READ_FALLBACK=off`
live, one flag a day, outside Mon/Thu 03:20–04:40 Kyiv and ≥ 65 min after any
write to these tables, before step 13: `/api/health` shows the chain
`duckdb`/unlatched and `goals_history.mode` silver; record `/api/goals/smart`
and `/api/revenue/forecast`; `docker compose stop web bot`; `chain_copy_back.py
forecast --handover` exits 0 or no flip; set the flag; `up -d web bot`; then
trigger `seasonality_calc` and `revenue_prediction_train` (admin, `POST
/api/jobs/<id>/trigger`) so the first writes happen watched — the trigger
leaves `weekly_patterns` as they are, on purpose. Expect `retail_months: 12`,
`predictions_stored: true` and `latched: true`; at +65 min the copy lists the
four under `stood_down`.

### Every other shipper asks too, and a walk finds them (DN-22b)

The catalogue, the order-level expenses, the buyers and the manager
classification reached Postgres through paths that never asked who owns the
table — so registering chain 4, 5 or 6 would have had DuckDB's copy shipped
over the chain's rows, and for the classification a full replace deleting every
interval a human set in Postgres since the handover. Now the question has one
generic home, `core.pg_landing.tables_stood_down(unit)` (the local answer) and
`tables_stood_down_or_owned(pool, unit)` (plus the owner rows, read as
themselves too), and the order helpers are its `ORDER_UNIT` case.

- **The sync's shippers** skip with `stood down: a write chain owns …`.
  `_mirror` (products, categories, expenses, from every sync site) asks the
  local answer alone: it is the per-tick write path, like the orders mirror.
  `replicate_managers` and `mirror_buyers` ask it first and then, after
  `require_revision()`, the owner rows — both before DuckDB is read, the
  buyers only for a batch that has rows. The review reproduced why: with the
  marker lost, the classification copy's full replace deleted every interval
  a human had set in Postgres the first time it ran, hours before the page.
  It runs from the daily sync and stats job, at startup and on an admin
  click, so the per-tick argument never applied. An owner read that fails is
  a failure there like any other — nothing shipped, counted, stamped.
- **The backfills** (`backfill_buyers`, `backfill_expenses`) refuse; their
  **hourly diffs** return `{"stood_down": [...]}` without an ERROR;
  `POST /api/mirror/backfill/expenses` answers 409 (503 when the owner rows
  cannot be read). All of them hold a pool, so they read the owner rows too,
  after `require_revision()`.
- **The comparisons** — `reconcile_mirror`, `reconcile_expenses`,
  `reconcile_buyers` — file `mirror_stood_down` (INFO) per table on the local
  answer. On the owner rows alone the answer follows the shipper
  (`pg_landing.sync_reads_the_owner_rows`): the catalogue and expenses, whose
  per-tick mirror is still writing over the chain's rows, are
  `owner_row_without_marker` (CRITICAL); the buyers and the classification,
  whose shippers have stopped, are `mirror_stood_down` (INFO) naming the
  latch's own page. A test checks each claim against what its shipper does.
- **The operational pair reads each owner row as itself too.**
  `replicate_operational` and `reconcile_operational` expanded owner rows
  through the registered chains alone, so on an image older than chain 4 the
  hourly full replace put DuckDB's `app.buyer_gender` over the chain's
  verdicts, human overrides included, while the buyers' own paths stood down
  — and the two copies then agreed. Both now stand down on
  `chain_latch.owned_tables` (the one union every owner-reading path uses),
  and `reconcile_operational` files **`chain_owner_unregistered`**
  (CRITICAL, one finding) for owner rows no chain in the build declares. It is
  not stamped on the watermark: only a later shipment could clear it.
- **A unit stands down whole**: the buyers with their contacts, the managers
  with their classifications, the orders with their line items. The units are
  what one writer writes in one transaction; `tests/unit/test_write_chains.py`
  derives them from the writers, requires `pg_landing.shipping_units()` to
  equal them, and fails on a registered chain that splits one.

**The walk replaced a list.** The old test named the operational shipper and
its comparison, and guarded exactly those two. It now finds every function in
`core/`, `web/` and `scripts/` that executes a Postgres write naming a
`bronze.`/`app.` table — or a target it cannot read, which counts rather than
being assumed foreign — and requires each to ask the registry or be reached
only from functions that do (`write_orders`, `pg_buyers._write`,
`write_managers`). The destination side is exempt with a reason: the
registered chains, the alert journal, ClickHouse, the Postgres-derived layers,
and three replicators that stand down on a switch of their own (the walk checks
each evaluates it). Every comparison that reads a spec's Postgres copy of our
tables is walked the same way.

The review found two writers it passed, and the walk reads both now: a helper
that transforms the statement it is handed (`conn.execute(numbered(sql))`, or
hands it on — `_users_run` → `execute(rendered)`), which counts as an executor
when its statement carries one of its parameters, to a fixed point; and
`copy_records_to_table`/`copy_to_table`, read by `schema_name` (absent or
unresolved counts). That also made `_users_run`'s writers visible, so the three
store helpers that route by a switch of their own (`_sms_run`, `_users_run`,
`_perms_run`) exempt what goes only through them, each checked to read its
switch. And "holding a pool" means reaching `get_pool` through a callee too —
`replicate_managers` held it inside `write_managers`, where the walk did not
look — with `_mirror` and `upsert_orders` the two named per-tick exemptions.
Limits that remain: a target held on an object attribute
(`dialect.silver_orders`) reads as unknown, statements kept in a container
constant (a dict of SQL) are not rendered, and DuckDB SQL beside a Postgres
call in one function reads as Postgres's (why `copy_back` is exempt).

**The order watches do not learn the stand-down — chain 3 writes through
`write_orders`.** DN-22a's review asked what happens to the canary's
`mirror_stale:bronze.orders` and to `order_versions_stalled` once a chain owns
the order tables and the sync's mirror stops. Answer: the chain's writer ships
through `write_orders`, which moves that watermark and captures the version in
the row's own transaction, so both watches keep meaning what they say. The
alternative would blind the one liveness check on the one table nothing can
rebuild at the moment its writer changes hands. Pinned: the order tables and
the archive each have exactly one writer, a chain module included, and the two
watches do not ask the registry. Chain 3 runs `write_orders`' own core,
`_write_order_rows`, on its transaction, and its order step records a failure
with `_record_failure` as `mirror_orders` did, so `mirror_failing` keeps its
fast signal.

Production today stands nothing down: no chain declares any of these tables
and no owner row names one, so the per-tick shippers read no variable and no
file. What does run is an owner read after a revision check — in each hourly
diff and again in the backfill it calls, in each of the three daily
comparisons, in the expenses route, in every classification copy and in the
buyers mirror when a sync fetched new buyers — and none of them finds a row
that names these tables; `chain_owner_unregistered` has nothing to report. The
retail-status route now warns only on a replica that failed, not on one that
was skipped.

### What becomes of every DuckDB table (stage 5's manifest)

`core/duckdb_table_fates.py` names every table and view `analytics.duckdb` can
hold — the schema, a migration's scratch, and the names only the production
file still carries — with its fate: **moved** (the chain, its `KS_WRITE_*` and
the Postgres or ClickHouse successor), **derived** (rebuilt from Postgres),
**archive-only** (frozen where it stands), or **retired** with a reason; plus
what the weekly compaction does with it and whether it travels off-site.
`duckdb_written()` answers "which tables does DuckDB still write today" through
the readers the writers use — `chain_modes()` with its latch, the SMS and user
store switches, `KS_WRITE_WAREHOUSE` — and None where it cannot tell, which
for the warehouse is any process whose `configure_modes()` never ran (every
one but web's). Nothing in production imports it.

`tests/unit/test_duckdb_table_fates.py` does not trust it: the names come from
a fresh `DuckDBStore.connect()`, an AST walk of every `CREATE TABLE`/`VIEW`/
`RENAME TO` in `core/`, `web/`, `scripts/` and `deploy/`, and `DERIVED_TABLES`;
each entry is held to the real `phase1_export` (the off-site archive), every
tier of `snapshot_validation`, every DuckDB→Postgres pairing the shippers and
comparisons state, the migrated Postgres schema and the registered chains. **A
chain that registers names its `KS_WRITE_*` on the tables it takes in the same
change** — chain 4 was the first the guard stopped, and chains 3, 5, 6, 7b-3,
9, 10, 11a and 11b met it at the stage-4 integration. A shadow chain's tables
(OD-02 (c)) read DuckDB-written in either mode, because the shadow still hands
DuckDB every row. OD-11 is pinned there too:
`DERIVED_TABLES` and the set with no DDL are literals, so a DROP says so in
the diff.

**The DDL walk reads every shape or lists it**: a name joined with `+`, a
`%s` or a `{}` hole is a rendered site `RENDERED_DDL` must account for; the
relational API (`.create`, `.to_table`, `.create_view`, `.to_view`) is DDL;
a qualified name is the file's only as `main.x` or `analytics[.main].x`.
**Nothing outside the four trees opens DuckDB**, checked over every Python
file in the repository, import forms and `import_module` included. **Which
tables a store switch moves is read off the writers**, not declared: the
statements each router runs, rendered by the router itself, plus every
literal DuckDB write, and a table belongs to `KS_SMS_STORE`, `KS_USER_STORE`
or `KS_WRITE_WAREHOUSE` only if every writer runs on the branch where that
reader says DuckDB. A write naming its table as a `{hole}` — one text for two
engines, chain 10's watchdog statements — is rendered for DuckDB by its
module's own function, or says what fills the hole (`TEMPLATED_ELSEWHERE`):
the walk lost all three sample tables before it read them. `duckdb_written()` is held to the same writers, not to
`kind` — `schema_migrations` is retired at stage 5 and written until then.

`deploy/duckdb_table_fates_check.py` asks the same of a file: the newest
nightly backup, opened read-only, never the live database (refused by name, by
inode, and with a `.wal` beside it). Exit 0 every name declared, 1 a name to
decide, 2 refused. A copy of the 2026-08-31 production backup read 58 of 58.
It refuses what it cannot see, too: a newest backup over 48 h old (`--file`
reads an older one on purpose), and a file without `orders`, `sync_metadata`
and `schema_migrations` or holding under half of today's schema — an empty
file used to read "clean".

## TODO: Full DuckDB Resync Solution

### Overview
Reliable solution for completely re-uploading historical data to DuckDB from scratch.

### Requirements
- Downtime OK (5-10 min)
- Disk space OK (2x DB size ~200MB)
- Triggers: CLI script + API endpoint
- Verification: order counts + revenue totals
- Smart date detection: find MIN(ordered_at) from KeyCRM, sync from first order

### Flow
```
1. PREPARE
   • Create analytics_resync.duckdb (fresh)
   • Query KeyCRM for MIN(created_at) to find first order date
   • Calculate total days to sync

2. SYNC (into new DB)
   • Managers → Categories → Expense Types → Products
   • Orders in 90-day chunks (with progress %)
   • Expenses per order

3. VERIFY
   • Count orders in new DB vs KeyCRM API
   • Sum revenue for last 30 days vs KeyCRM
   • Flag if discrepancy > 1%
   • Abort if verification fails (keep old DB)

4. SWAP (atomic)
   • Stop background sync
   • Close DuckDB connection
   • mv analytics.duckdb → analytics_old.duckdb
   • mv analytics_resync.duckdb → analytics.duckdb
   • Reconnect & resume sync

5. CLEANUP (after 24h or manual)
   • Delete analytics_old.duckdb
```

### Files to Create

**Stale — this was built differently.** The admin endpoint exists as
`POST /duckdb/resync` in `web/routes/api/admin.py:21` and nginx already routes
it (`nginx.conf:103`). Neither `core/resync_service.py` nor
`scripts/full_resync.py` was ever created; the CLI equivalent is
`scripts/force_resync.py`. Kept for the design rationale below, not as a task
list — do not implement it a second time.

| File | Purpose | Status |
|------|---------|--------|
| `core/resync_service.py` | Core resync logic, verification | never created |
| `scripts/full_resync.py` | CLI interface | superseded by `scripts/force_resync.py` |
| `web/routes/api.py` | API endpoint (admin) | shipped as `web/routes/api/admin.py:21` |

### CLI Usage
```bash
# Full resync with verification
PYTHONPATH=. python scripts/full_resync.py

# Dry run (verify only, no swap)
PYTHONPATH=. python scripts/full_resync.py --dry-run

# Keep old DB file after swap
PYTHONPATH=. python scripts/full_resync.py --keep-old

# In Docker
docker exec keycrm-web python /app/scripts/full_resync.py
```

### API Endpoint
```
POST /api/admin/resync
Authorization: Bearer <admin_token>

GET /api/admin/resync/status/{job_id}
```

### Open Questions
1. Admin auth for API - use existing auth or simple API key?
2. Progress storage - file-based or in-memory?
3. Automatic cleanup - delete old DB after 24h, or manual only?

---

> **This file is public.** The repository is public, and 5 550 customer phone
> numbers had to be scrubbed from it once already. Keep server addresses,
> usernames, keys and people's names out of here — describe how the system
> works, not where it lives or who staffs it.

## Useful Links

- **Repository**: https://github.com/halloweex/key-api-bot
- **Dashboard**: https://ksanalytics.duckdns.org
- **Docker Hub**: images are published under the account in `DOCKER_USERNAME`

---

*Last updated: 2026-10-08*
