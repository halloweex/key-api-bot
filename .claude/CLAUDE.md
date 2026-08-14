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
│   │   ├── batch.py             # POST /api/dashboard/batch (no client calls it)
│   │   └── pages.py             # HTML page routes (React SPA)
│   ├── services/
│   │   └── dashboard_service.py # Data transformations (async)
│   ├── frontend/                # React Dashboard (TypeScript + Vite)
│   │   └── src/
│   │       ├── api/             # API client with error handling
│   │       ├── components/      # React components (cards, charts, filters, ui)
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
| `/api/customers/sms-segments` | RFM segments for SMS campaigns, `ltv_basis=revenue\|margin` (admin only) |
| `/api/customers/sms-segments/export/csv` | Campaign list as CSV, holdout excluded (admin only) |
| `/api/managers` | Managers with sales_type and 365d revenue (admin only) |
| `/api/managers/{id}/retail-status` | Classify a manager, marks warehouse dirty (POST, admin) |
| `/api/health/data-quality` | Latest integrity + reconciliation run, with issues/diffs |
| `/api/warehouse/status` | Last refresh, checksums, validation_passed |
| `/api/warehouse/refresh` | Force a FULL rebuild of Silver + Gold (POST, admin) |
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
items and no money at all. It is admin-only, enforced in `api_gate` (and again
in `/api/dashboard/batch`, which carries `sales_type` in the body where the
gate cannot see it). `sales_type=all` spans every category and is not gated.

`managers.is_retail` is seeded from `RETAIL_MANAGER_IDS` for managers the
warehouse has never seen and **never overwritten afterwards** — it used to be
recomputed on every sync, which made a human's classification impossible to
keep. Set it via `POST /api/managers/{id}/retail-status`, which also marks the
warehouse dirty, because `sales_type` only changes on a rebuild.

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
| `dq_reconciliation` | 05:30 | compare 90 days against KeyCRM, per order |
| `dq_digest` | 09:00 | one message with WARN+ findings and a delta |
| `weekly_report` | daily 09:30 | last complete week's numbers to every approved user — sends once, then quiet |

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

The choice lives in `user_preferences.language` in the bot's SQLite, set from
the bot's settings screen. The weekly report runs in the **web** container and
reads it through `core/bot_prefs.py`, which opens `data/bot.db` read-only —
both containers bind-mount `./data`. A locked, missing, or pre-migration
database all mean the same thing there: fall back, and the report still goes.
The job renders once per distinct language, not once per reader.

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
- **Alert throttle** keys on the *condition* (`warehouse:validation_retrying`,
  `dq:{layer}:{severity}:{checks}`, `canary:{failing keys}`), never on message
  text — the validator embeds live checksums, so 3 119 failures once produced
  404 "identical" messages.
- **Daily digest** (`dq_digest`) is the only route by which WARN findings reach
  anyone. INFO findings ride along but never summon a digest on their own.
- **Run-age watchdog**: `/api/health` publishes the age of the last successful
  run per layer, keyed on `error_message IS NULL` — a failed run writes a row
  too, so row-existence alone reads green. `bot/canary.py` judges it from the
  *other* container every 15 min: 30 h for reconciliation, 12 h for integrity.
  A missing block or a layer that never succeeded both count as failures.

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

*Last updated: 2026-08-10*
