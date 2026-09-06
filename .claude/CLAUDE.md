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
| `/api/managers` | Managers with sales_type and 365d revenue (admin only) |
| `/api/managers/{id}/retail-status` | Classify a manager, marks warehouse dirty (POST, admin) |
| `/api/health/data-quality` | Latest run per layer — integrity, reconciliation, mirror_landing, reconciliation_pg, reconciliation_ch — with issues/diffs (за сессией) |
| `/api/warehouse/status` | Last refresh, checksums, validation_passed |
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

```bash
# The suite. No network, no production data.
pytest -q

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

It sends a light PNG card (`core/weekly_report_image.py`) with the text report
as the caption: revenue against the week before, then orders, basket and the
new/repeat split. Nothing else — everything the caption already carries stays
in the caption. Pillow only, no matplotlib, drawn at ~2× display size because
Telegram re-encodes photos as JPEG. The card needs `fonts-dejavu-core` in the
image (`Dockerfile.web`): `python:3.14-slim` ships no fonts, and DejaVu is the
one carrying both Cyrillic and ₴. No font, a caption over Telegram's 1 024, or
any render failure costs the picture and nothing else — the text still goes
out.

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
- **Получатели — только админы**, запинено
  `tests/unit/test_alert_recipients.py`: единственный chat_ids-override —
  недельный отчёт; алерт-модулям запрещён импорт списка пользователей.
  Бизнес-исключения: недельный отчёт и веха-рассылка.
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
  30 h for mirror_landing. A missing block or a layer that never succeeded
  both count as failures.
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

**Deploying it needs all three images, migrate first.** `REQUIRED_REVISION`
moves to `0010_order_versions` and `require_revision` raises on any mismatch,
ahead or behind. The bot checks it too, but only in `initialise()`, so a running
bot survives the migration; only a restart before its image is replaced would
fail.

### The port in front of the bot's state

`bot/database.py` was 717 lines of `sqlite3` with 46 call sites and no tests.
Step 03 needs the engine underneath it to change, so it now has a seam:

```
core/bot_store.py     the port — four Protocols, a registry, no SQL
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

**База `history` в прод-CH не существует и в грантах ks_app её нет** — перед
включением платформа выполняет две строки: `CREATE DATABASE history` и тот же
GRANT, что у bronze/silver/gold. До этого каждый ship пишет failed-строку в
`meta.mirror_state` — громко, не молча.

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

The `sales_type` partition assertion (Gold known types == Silver total) is
deliberately **not** part of `validation_passed`: no rebuild can invent a
sales_type the code does not know, so it reports and stops rather than driving
a rebuild every two minutes.

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

*Last updated: 2026-08-30*
