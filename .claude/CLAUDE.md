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
| `/api/health` | Health check (status, version, uptime, cache stats) |
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
| `/api/jobs` | Scheduler jobs with live next_run and history |
| `/api/jobs/{job_id}/trigger` | Run a job now (POST, admin) |

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
- Retrains daily at 3:30 AM via scheduler + on server startup
- Predictions stored in `revenue_predictions` DuckDB table
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
| `warehouse_refresh` | every 2 min | Silver + Gold rebuild, validation, cell guard |
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

**Never schedule anything at 05:00–05:05 Kyiv.** The host cron
`0 2 * * 0 weekly_compact.sh` is 02:00 UTC — the same instant — and it stops
both containers. A `CronTrigger` computes its next fire from registration, so
a scheduler that comes back at 02:00:51 sets the next run a day out: the job
is not late, it does not exist when it is due, and `misfire_grace_time` has
nothing to forgive. That cost `dq_reconciliation` every Sunday.

`BackgroundScheduler.start()` queues a one-off catch-up for any check whose
last *successful* verdict is older than its cadence (`CATCHUP_CHECKS`), which
also covers deploys landing on a cron instant.

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
  (`ch_reconcile_pending`), а не обвиняется.
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
  `reconciliation_ch` is published and digested but not paged on.
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
  (24 h window) and is *passed into* `format_alert_message`, which stays pure.
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
`WATCHED_LAYERS` (digest section, layer age, catch-up) but deliberately not yet
in the canary's `DQ_MAX_AGE_S` — that dict pages, and the canary's first probe
is 90 s after the bot starts, before the catch-up run can finish.

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
mismatch, ahead or behind. It is `0021_user_allowed_features` today, and every
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

- **Включается `KS_CH_URL`** (`http://ks-clickhouse:8123`); без него шиппер и
  сверка молча стоят — хост без ClickHouse работает без изменений. Пароль
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
- Недоступный ClickHouse — WARN-находка, не исключение: опциональное
  хранилище не должно валить слой обязательных сравнений.
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
единственный синкер. Тест обходит `core/`, `web/`, `scripts/`, `bot/` и
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
exception text, the endpoint is public. **Nothing a read returns changed.**

`KS_READ_FALLBACK` (`duckdb` default | `off`) is read in `configure_modes()`,
before the boot sync. `off` is validated and published but **not enforced**:
refusing is DN-20b (HTTP, a 503 naming the surface) and DN-20c (reports,
assistant, training, sync), which raise `ReadUnavailable` from `fall_back`. An
unknown value **runs as `duckdb` and never raises** — web is the only syncer,
so a crash loop over how a read degrades would stop order intake (OD-09); it
publishes `read_fallback_mode.error` and the canary warns
`read_fallback_mode_invalid`. The same call lists, under
`read_fallback_mode.misconfigured`, every `KS_READ_*=postgres` without
`KS_PG_DSN` and `=clickhouse` without `KS_CH_URL` — found by prefix in the
environment, not listed: those reads serve DuckDB with nothing failing to count.

`tests/unit/test_read_fallback_sites.py` walks `core/` and `web/` for the
shapes, never a list of routers: a "falling back to DuckDB" log; an
`enabled() and available()` gate whose handler carries on toward DuckDB; and
**every** handler in a function reaching another engine under
`core/repositories/` or `core/pg_*_read*.py`, because `backfilled()` returns
False and `_pg_gold_summary` returns None and it is the caller that then reads
DuckDB. Each must call `fall_back`, re-raise, or say in the same `try` what
`ReadUnavailable` means — `_get_ml_forecast_total` lets it through, so a
refusal is never turned into a goal computed without its signal.

### A write flag is no longer a rollback

Stage 4 moves WRITES chain by chain, and each chain is chosen by a `KS_WRITE_*`
variable — `KS_WRITE_EXPENSES` (chain 8, on since 2026-09-17), `KS_WRITE_INVENTORY`
(chain 1, off), `KS_WRITE_GOALS` (chain 7a, off — `app.revenue_goals`, the three
goal amounts typed on /goals, whose POST wrote DuckDB while the GET read an
hourly copy in Postgres), `KS_WRITE_EXPENSE_TYPES` (chain 6a, off). Putting one
back to `duckdb` reads like an undo and is not one:
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
(WARN) within one probe, the shipper stamps the chain's tables failing, and the
daily comparison files `chain_latch_disagrees` and `chain_shipper_overwrote`
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
test computes the set so a third cannot join it quietly.
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

- a key only DuckDB holds;
- in an append-only table, two different rows under one key. There is no
  "newer" there: they are two events, and the usual one is a movement id both
  allocators issued, because Postgres floors its sequence on its own MAX(id);
- in an append-only table, a Postgres row at or below DuckDB's watermark, which
  a copy reading only above it can never bring back;
- a DuckDB version later than Postgres's by a clock both stores carry as a value
  (`manual_expenses`, `inventory_history`, `revenue_goals` — whose two writers
  stamp `updated_at` from the web container's clock for exactly this reason).

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
comparison spec **raises** instead of being skipped. The two big tables are
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
docker run --rm --name chain-copy-back \
    -v /opt/key-api-bot/data:/app/data --env-file /opt/key-api-bot/.env \
    --network key-api-bot_default halloweex/keycrm-web:latest \
    python /app/scripts/chain_copy_back.py inventory --handover
# BEFORE A FLIP: first, with web still up, /api/health must show
#   write_chains.pg_inventory_write.preflight.ok = true (DN-24). Then this:
#   exit 0, or do not flip. A CRITICAL is a row the flip would
#   strand: up -d web with the flag unchanged, let replicate_operational ship
#   (POST /api/jobs/replicate_operational/trigger), stop, ask again.
# TO ROLL BACK: the same command with --dry-run (also the default), then with
#   --execute. Exit 0 released. 1 not committed (a difference rolled back, or
#   a traceback before COMMIT): DuckDB as it was, latch kept. 2 refused before
#   writing. 3 COMMITTED, then the checkpoint or the release failed: DuckDB
#   HAS the copy; do what the message says for the latch it found. Only after
#   exit 0:
#   1. set KS_WRITE_INVENTORY=duckdb in .env   (the flag decides again)
#   2. docker compose up -d web bot
#   3. at +2 min: deploy/stage4_soak.sh — E1/E2 for chain 8, I1/I2/I3 for
#      chain 1; chains 7a and 6a have no soak check yet, so meta.mirror_state
#      for app.revenue_goals or bronze.expense_types. The hourly copy must be
#      shipping the chain's tables again.
```

`--network key-api-bot_default` is observed, not derived: on 2026-09-18 web and
ks-postgres were both on it (and on `ks-data`). Postgres has no `ports:` key
and the DSN names the compose service alias, which the weekly-compact sidecar
never needed. At production's size — 162,883 history rows, 56,277 movements —
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
photographed, and both snapshots every day. Who is watched comes from
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

### Step 13 is published, not switched (DN-28)

`KS_WRITE_WAREHOUSE` names who derives Silver, Gold and the UTM verdicts once
DuckDB stops: `duckdb` (default) or `postgres`. It is read in
`configure_modes()`, before the boot sync, and **nothing in this build acts on
it**: `postgres` is published and still runs as `duckdb`, because the switch
itself — no DuckDB refresh, no dirty marks — is DN-29, and half of it would
stand DuckDB's checks down while DuckDB went on deriving. An unknown value runs
as `duckdb` and publishes the error on `/api/health` (`warehouse_writer_mode`),
where the canary warns `warehouse_mode_invalid` — a typo costs nothing in this
build, and the day it would is the flip; it never raises, since web is the only
syncer.

What the switch will need is in place and idle. `stood_down_duckdb_checks()`
(`core/warehouse_cutover.py`) names the five DuckDB integrity checks over
Silver, Gold and UTM; the scan skips them and `duckdb_looked` leaves them out,
so the Postgres twins stand in — at the counts the DN-14 pairing record has
been giving in shadow. `pg_gold_internal_check` asks `gold_rollup_mismatch` of
Postgres alone in `dq_mirror_landing`, and `reconcile_gold` leaves it out on the
same predicate, so it is asked once a run. Both are empty or unregistered while
the mode is `duckdb`, which today is always.

`GET /api/warehouse/status` publishes `cutover`: the variable as read,
`switch_built: false`, and every unmet precondition by name, from
`evaluate_preconditions(env, facts)` — `KS_PG_DERIVE=own`, the twins on,
`KS_UTM_PARSE=postgres`, `KS_READ_FALLBACK=off`, a DSN and the required
revision, the landing mirror on, every Silver/Gold/UTM read switch on
`postgres` (a test reads every string in `core/`, `web/` and `bot/` for a
`KS_READ_*` or `KS_*_STORE` name, so a new one has to be put on the list or
excluded by name — `KS_SMS_STORE` is read inline and the first walk missed
it), cohorts on ClickHouse with `KS_CH_URL`, no write chain owning a table
the goals bridge reads (DN-12), and **no delivered page open under a condition
only a stood-down check reports** (`retired_conditions_clear`). A stood-down
check is not a raised one, so the integrity job does not hold its conditions,
and the first run after the switch would announce such a page "✅ Resolved"
with no check looking. Holding them instead would keep it open for as long as
Postgres derives, since nothing re-examines a retired check; so the switch
waits while the DuckDB check can still clear it. It reads the Alert Gate's
delivered map — what `resolve_group` announces from — not `app.alert_series`,
which can miss a delivered page (its fired row is fire-and-forget).
`preconditions_met: true` is a checklist done, not a switch thrown. An
exception reading any fact is published by its class alone and logged whole:
a driver's text names the database user, host and port.

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

**The latch waits for a connection.** 6a latches inside `pool.acquire()`, not
between `_pool()` and it: an acquire that times out during the Sunday sync
must not move the chain with nothing written. Chains 1, 8 and 7a still latch
before the acquire; `tests/unit/test_chain_latch.py` names them in a strict
xfail ledger that can only shrink, and holds every other chain to the rule.

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
watches do not ask the registry. The chain's writer also owes one thing
`mirror_orders` does today — recording its failures with `_record_failure` —
or `mirror_failing` loses its fast signal.

Production today stands nothing down: no chain declares any of these tables
and no owner row names one, so the per-tick shippers read no variable and no
file. What does run is an owner read after a revision check — in each hourly
diff and again in the backfill it calls, in each of the three daily
comparisons, in the expenses route, in every classification copy and in the
buyers mirror when a sync fetched new buyers — and none of them finds a row
that names these tables; `chain_owner_unregistered` has nothing to report. The
retail-status route now warns only on a replica that failed, not on one that
was skipped.

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

*Last updated: 2026-09-08*
