# KoreanStory Sales Bot - Project Documentation

## Project Overview
Automated sales reporting Telegram bot for KoreanStory with interactive web dashboard, Docker containerization, and CI/CD auto-deployment to Hetzner VPS. Uses KeyCRM as data source.

## Architecture

Selective — only the entries worth naming. `core/` alone holds 120 modules;
the listing below is not a directory dump and should not be read as one.

```
key-api-bot/
├── core/                         # Shared modules (bot + web), 120 files
│   ├── config.py                # Shared configuration
│   ├── keycrm.py                # Unified async KeyCRM client
│   ├── store.py                 # The analytics store (AnalyticsStore), over Postgres;
│   │                            # its old name's shim, duckdb_store.py, went at the final stage
│   ├── repositories/            # Nine mixins composed into AnalyticsStore
│   ├── pg_*_write.py            # The thirteen write chains, one writer per table set
│   ├── pg_derivation.py         # Silver, Gold and UTM derived in Postgres on a signal
│   ├── models.py                # Data models (Order, OrderProduct, Buyer, Manager) and the source/status enums
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
│   │                            # web image. The DuckDB compaction
│   │                            # (compact_duckdb.py, weekly_compact.sh) went in
│   │                            # step 14's S14-18; the pinned image keeps it.
│   ├── check_turbosms_signature.py  # Is TURBOSMS_WEBHOOK_SECRET the one the gateway signs with?
│   └── utm_reclassify_dryrun.py # What a UTM reclassify would move; read-only, snapshots the verdicts
│
├── deploy/                       # Reached only from host cron, never imported
│   ├── offsite_check.sh         # 09:00 daily — freshness, in-app watchdog liveness
│   ├── daily_soak.sh            # The daily soak report, by hand; checks in daily_soak/
│   ├── api_fingerprint.py       # sha256 of past-window API answers, before/after a deploy
│   └── restore_from_export.py   # The only restore path, on the pinned image. Never delete.
│
├── tests/                        # 326 test modules. pytest.ini sets testpaths, so only
│                                # this tree is collected — scripts/ is not.
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
| Database | PostgreSQL 17 (every store: landing, warehouse, operational) + ClickHouse 24.8 (second Gold, cohorts); DuckDB until step 14 |
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
SOURCE_MAPPING = {1: 'Instagram', 2: 'Telegram', 3: 'Opencart', 4: 'Shopify', 5: 'Виставка'}

# core/duckdb_constants.py
B2B_MANAGER_ID = 15                             # the wholesale manager
RETAIL_MANAGER_IDS = [4, 8, 11, 16, 17, 19, 22]  # seeds managers.is_retail for NEW rows only
EXHIBITION_SOURCE_ID = 5                        # the fair: sales_type exhibition, whoever staffed it
SYNCED_SOURCE_IDS = (1, 2, 3, 4, 5)             # what we pull from KeyCRM and reconcile
REVENUE_SOURCE_IDS = (1, 2, 4, 5)               # what counts as revenue (is_active_source)
KNOWN_SALES_TYPES = ("retail", "b2b", "internal", "exhibition")
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/health` | Health check (status, version, uptime, the blocks the canary judges); `status` is `healthy` and `degraded_by` empty since step 14's S14-17 took DuckDB's block, its ledger and their three causes out |
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
| `/api/customers/sms-segments` | Campaign audience — one arm or one per value level, one or more groups ORed (`or_groups`) (needs `sms` view; customer rows need `sms` edit); `follow_up_of=X` previews a follow-up |
| `/api/customers/sms-segments/export/csv` | Campaign list as CSV, holdout excluded (needs `sms` edit) |
| `/api/customers/sms-campaigns` | Freeze the audience as a campaign, no CSV needed (POST, needs `sms` edit); `follow_up_of=X` freezes a follow-up |
| `/api/customers/sms-campaigns/{c}/send` | Send through TurboSMS (POST, needs `sms` edit); requires `confirm_campaign`, refuses on balance or a recent overlap — see "What stops an SMS send" |
| `/api/customers/sms/channels` | Configured channels and tariff; `include_balance=true` adds the gateway's balance |
| `/api/customers/sms-audience-presets` | Saved audiences; PUT/DELETE by name (needs `sms` view/edit) |
| `/api/traffic/funnel` | The site's purchase funnel from GA4, live: the period, the one before and a year ago, by traffic type, channel group and device (tab `traffic`). `country=ua` (default: Ukraine and the visitors GA4 could not place; abroad is mostly bots) or `all`. 503 "not configured" without the key — see "Воронка сайта из GA4" |
| `/api/me/send-file` | A CSV the page already holds, sent by the bot to the requester's own chat — how every export works inside the Telegram Mini App, whose WebView saves no download (POST, any session; `utils/deliverFile.ts`) |
| `/api/admin/users/{id}/features` | Which tabs one account may open (PATCH, admin) |
| `/api/managers` | Managers with sales_type and 365d revenue (admin only) |
| `/api/managers/{id}/retail-status` | Classify a manager; raises the Postgres derivation signal (POST, admin) |
| `/api/health/data-quality` | Latest run of every watched layer (`data_quality.WATCHED_LAYERS`: integrity, mirror_landing, reconciliation_pg, reconciliation_ch) with its top 20 issues, or diffs for the two reconciliation arms (за сессией) (admin) |
| `/api/health/detailed` | Component status — Meilisearch, WebSocket rooms, the sync, the forecast model — and process metrics (admin, O8 (a), 2026-11-11) |
| `/api/metrics` | Uptime, request counts and timings per route template (admin, O8 (a), 2026-11-11) |
| `/api/warehouse/status` | The Postgres derivation: owed or not, when built, the last journalled run (`cutover` and `duckdb_frozen` went with step 13's switch in step 14's S14-15) |
| `/api/warehouse/refresh` | Force a FULL rebuild of Silver + Gold (POST, admin) |
| `/api/reconcile` | Run `dq_reconciliation` over any window now, all three stores (POST, admin) |
| `/api/jobs` | Scheduler jobs with live next_run and history |
| `/api/jobs/{job_id}/trigger` | Run a job now (POST, admin) |
| `/api/tg-bot/activity` | The customer Telegram bot's activity — daily/weekly active, new, registrations, WAU/MAU, activation, the entry funnel and the press table (which button people leave after) — for page `/tg-bot` (tab `tgbot`: admins and the marketing presets). `days` ∈ 30/90/180. Asked of ks-tg-bot's `GET /internal/activity` at `KS_TG_BOT_URL` (default `http://ks-tg-bot:8081`, the shared `key-api-bot_default` network) with bearer `KS_TG_BOT_TOKEN`; counts only, no customer data. Unset token → 503 "not configured". Last good answer kept 120 s per window and served `stale: true` while the bot is unreachable — `web/services/tg_bot_service.py` |

**Retired by the owner's decision OD-10 (2026-09-30)**, and kept retired by
`tests/unit/test_od10_retired_doors.py`: `GET /api/buyers/{id}`,
`/api/orders/{id}`, `/api/products/{id}`, `/api/buyers/stats`,
`/api/debug/stale-returns`, `/api/debug/order-status/{id}`,
`/api/reconciliation`, and `POST /api/reconciliation/run`,
`/api/duckdb/purge-orders`. See "History of the DuckDB exit". A retired
GET answers a JSON 404, as does any `/api/` GET no route matches. The SPA
catch-all used to serve those the shell with a 200, which a script reads as
success.

**Query params**: `period` (today/yesterday/week/last_week/month/last_month/last_7_days/last_28_days/all_time) or `start_date` + `end_date`, `category_id`, `brand`, `sales_type`

`all_time` («За весь час», 2026-11-05) runs from `core.filters.HISTORY_START`
— 2023-12-02, KeyCRM's first order, measured on the 2026-08-31 backup — to
today. It has nothing before it to compare with (`has_comparison`): its
previous period is zeros and a year back is two thirds the same orders. So
the trend reads no comparison, the marketing report and its CSV read neither
of theirs (`include_comparison=False`, the four dates and both periods
null), and the page hides the change column of the organic block. The page
asks the trend `granularity=monthly` for it: one bar a month, labelled
`MM.YYYY`, never a comparison or a forecast — by day it is a thousand bars
whose `dd.mm` repeats every year. `/api/customers/insights` takes the same
`granularity` for its AOV trend, a month's AOV being its revenue over its
orders (never a mean of daily AOVs; `core.filters.by_month` folds both), and
the traffic trend's additive days are folded on the page
(`utils/trafficTrend.trafficByMonth`). The GA4 funnel sees 14 months of it and
says so; the CRM's order count beside it starts at GA4's `earliest` too. Nine
digits of hryvnia do not fit a narrow tile: `StatCard` takes a
`shortFormatter` (`formatCurrencyShort`: `₴68,1 млн`, and no tenth from three
digits up, `₴109 млн`) and shows it only when the full figure, measured on a
hidden copy, does not fit — the full one stays in the tooltip and the label.
`TileGrid`'s five-tile row is three-up below `xl` (1280 px): beside the
sidebar at 1024 px a fifth of the row held 74 px of figure. The marketing report's range gives its start a year
when the two differ (`formatReportRange`). Its «Кол-во клиентов», new and
returning count **people**, by the dashboard's own statement over Silver
(`_INSIGHTS_PERIOD_SQL`; with a brand or category, over the matching lines),
in every window, the comparisons included (owner's decision, 2026-11-11).
They summed Gold's daily distinct buyers until then — buyer-days, within a
few of the orders on a month (October 2026: 1 295 against 1 299 orders and
1 231 people) and 42 880 against 19 425 people over the whole history. A
person new in the window who comes back in it is in both new and returning,
as on the dashboard. `tests/integration/test_marketing_counts_people.py`
holds the report to the dashboard's answer. Its brand table's «Заказов» is
the orders that contained the brand, each once, by the dashboard brand
card's statement (`_BRAND_ANALYTICS_SQL`; owner's decision, 2026-11-12), and
the brand's average check is its revenue over those. It summed DuckDB's
per-product cells until then, one order per product of the brand it held:
NEOGEN, October 2026, 1 429 against 748. These figures go to suppliers.
`test_marketing_two_engines.py` names the recorded values the decision moved
(`BRAND_ORDERS_SINCE_2026_11_12`) and holds the table to the brand card.

`start_date` and `end_date` are read by `strptime("%Y-%m-%d")` in
`core/filters.parse_period`, and only as a pair: one alone is ignored, and a
known `period` wins over both. The forms `strptime` forgives (`2026-1-5`,
`2026-01- 5`) stay accepted — decided 2026-10-21, refusing them would only
turn working requests into errors. A date it refuses is a 400 naming the field;
it was a 500 until then. Each date must also lie in 2000-01-01..2099-12-31
(`EARLIEST_DATE`, `LATEST_DATE`, the owner's decision of 2026-10-22), the
start judged before the end, or it is a 400 naming its field: `Date must be
between 2000-01-01 and 2099-12-31`. Python's calendar ends at 0001-01-01 and
9999-12-31, and eight routes stepped off it and answered 500 —
`end_date=9999-12-31` under the per-day loops, a start in year 1 under a
comparison, a backwards range whose previous period runs past 9999-12-31 —
while a date beyond the window answered zeros everywhere else. Inside it every
date a route derives lies between 1989-12-24 and 2199-12-30, which
`tests/unit/test_filters_parse_period.py` holds the comparison windows to.
29 February is an ordinary date: a year back from it is 28 February, by
`RevenueMixin._comparison_window`, which the marketing report shares with the
trend since its own `date.replace(year=…)` answered 500 on a range starting or
ending there. A range longer than `MAX_RANGE_DAYS` (3 660 days,
ten years and a week) is a 400 naming `date_range`: four endpoints build one
list entry per day of the range on web's event loop — the trend, its
comparison, expenses summary and profit, customer insights — and
0002-01-01..9999-12-30 cost the trend 6.8 s and 95 MB of JSON. The trend's
`include_forecast` shows at most `FORECAST_HORIZON_DAYS` (60) ahead, where
the date picker stops. Until 2026-11-04 it ran the model for the week and a
custom range, and eight years ahead had held the single ML thread for 70 s;
it reads the stored forecast now (see "Revenue Prediction").

An integer parameter past the width of the column it meets, or a NUL in an
`/api/` path or query string, is a 400 naming the field. Both used to reach
asyncpg and fail there, and the engine routers read that as the store failing:
a 503 naming the page and a `read_refused` page, for a request no page sends
(found by the Rust ports, #330–#332). Integers are held to their column where
the route reads them (`core.validators.validate_db_int`, and
`validate_category_id`'s int32 ceiling); a NUL is refused before routing by
`web.middleware.RejectNulMiddleware`. `tests/unit/test_input_bounds.py` walks
every `/api/` route: each integer parameter is bounded on both sides by
FastAPI (`ge`/`le`) or passed to a bounded validator, or is exempt there with
the reason it never reaches SQL.

## Postgres schema

PostgreSQL 17, one database, five schemas, every one migrated by Alembic
(`migrations/versions/`, `REQUIRED_REVISION` in `core/pg.py`; `migrate` runs
before web and the bot on every deploy). DuckDB held all of this until step
14 — see "History of the DuckDB exit".

```
bronze ── what KeyCRM serves, as one parse writes it        (the write chains)
  orders ─┬─ order_products      products ── categories      offers ── offer_stocks
          └─ expenses ── expense_types     buyers ── buyer_contacts     managers
silver ── derived from bronze by pg_warehouse_derive, on a signal
  orders (one row per order: sales_type, order_date in Kyiv, is_return …)
  order_utm (the parsed attribution)       order_lines (a view)
gold ──── derived from silver in the same tick
  daily_revenue (date × sales_type × source_id, plus the roll-up row)
  v_* (the inventory views over app.* and bronze.*)
app ───── what nothing can rebuild, and the application's own state
meta ──── the store's own bookkeeping
```

**Key tables:**
- `bronze.orders` - Core order facts with source_id, status_id, `status_group_id`, grand_total, ordered_at, buyer_id, manager_id, `manager_comment` (the UTM attribution's text)
- `bronze.order_products` - Line items with product_id, quantity, price_sold; ids are `order_id * 1000 + position`, replaced per order, never upserted
- `bronze.products` / `bronze.categories` - The catalogue (hierarchical categories by parent_id); a product KeyCRM retires stays, never deleted
- `bronze.expenses` / `bronze.expense_types` - Order-level expenses (delivery, commission, etc.)
- `silver.orders` - One row per order with `sales_type` materialised and the Kyiv `order_date`; what every tab reads
- `gold.daily_revenue` - Pre-aggregated daily revenue; `source_id IS NULL` is the roll-up, because distinct buyer counts do not add up across sources
- `app.order_versions` - The only record that an order ever changed; append-only, fed from what a write actually changed
- `app.stock_movements`, `app.inventory_sku_history`, `app.sku_inventory_status`, `app.inventory_history` - The stock history KeyCRM does not keep
- `app.manager_classifications` - Who sells retail, wholesale or internally, as of when; only a human knows
- `app.revenue_predictions`, `app.seasonal_indices`, `app.growth_metrics`, `app.weekly_patterns`, `app.revenue_goals` - The forecast and the goals
- `app.data_quality_runs` / `_issues` / `_diffs` - one row per check run plus its findings; the digest and `/api/health/data-quality` read these. A failed run writes a row too, with `error_message` set — never treat row-existence as "the check ran"
- `app.order_backfill_misses` - ids KeyCRM cannot supply, or supplies without line items, so a repair job cannot loop on them forever. The two kinds are skipped differently (`core/pg_orders_read.py`): an id the gap scan found absent upstream is never asked for again (`order_id_gaps` excludes every ledgered id, whatever its age), while an order KeyCRM served without line items is re-fetched by the half-written repair once its `checked_at` is 30 days old (`MISS_RETRY_DAYS`), and each such re-fetch that comes back empty restamps it
- `app.weekly_report_sends` / `app.traffic_report_sends` - one row per week delivered; what keeps a daily job to one message a week and makes a missed Monday recoverable
- `app.dashboard_users`, `app.role_permissions` - Access (see "Which tabs one person may open"); `app.authorized_users`, `app.user_preferences` - the bot's
- `app.sms_campaigns`, `app.sms_campaign_members`, `app.sms_dlr_events`, `app.marketing_optouts` - The SMS campaigns, their rosters, delivery reports and opt-outs
- `app.alert_series` / `app.alert_events` - The alert journal
- `meta.chain_watermarks` - Each chain's `last_sync_*` keys and its `owner:<table>` rows; `meta.mirror_state` - per-table write stamps; `meta.derivation_signal` / `meta.derivation_runs` - the derivation's request counter and journal
- History, written by nothing (OD-11): `app.sync_metadata`, `app.warehouse_refreshes` (~65k rows back to 2026-03-14, still the best forensic trail of the DuckDB warehouse), `app.reconciliation_log`

ClickHouse holds a second copy of Silver and derives its own Gold from it
(`core/ch_silver.py`, hourly `ch_sync`), the archive of order versions
(`history.order_versions`) and the cohorts' reads.

**Source filtering** (`REVENUE_SOURCE_IDS`, materialised as Silver's `is_active_source`):
- Included: Instagram (1), Telegram (2), Shopify (4), Виставка (5) — the exhibition, its own `sales_type`
- Excluded: Opencart (3) - deprecated; still synced and reconciled (`SYNCED_SOURCE_IDS`), never revenue

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

### Before any rollback to an image from before step 14

Every image built before S14-00 routes by the latch, the flags and the
step-13 and `KS_DUCKDB=off` verdicts: 3.0.292 up to the one production runs
when S14-00 deploys (3.0.298 on 2026-10-25), and every one of them requires
revision `0035_batch_e_chains`. Every step-14 image routes by none of them
(S14-00). **The target is `rollback-step14`, the last image before step 14,
pinned at S14-00's deploy** (the plan's H1; 3.0.292 is kept as
`rollback-step14-3.0.292`), or, for a later step-14 PR, the image before it
(`:s14-NN`, H8, which needs none of what follows). Not 3.0.292 by number:
the images after it carry fixes a rollback to it would undo, among them
`/api/health`'s `sync` block and the web memory watchdog's alerts (3.0.293),
a malformed dashboard date or an over-long range answered 400 (3.0.295), the
customer bot's page and its `tgbot` tab (3.0.296, 3.0.298), and a dashboard
date outside 2000-01-01..2099-12-31 answered 400 and a marketing report on
29 February answered rather than a 500 (3.0.297). Before tagging
`rollback-step14` back to `latest`:

1. **Markers against owner rows.** `ls data/write-chain-owners/` and, as
   `ks_readonly`, `SELECT key, updated_at FROM meta.chain_watermarks WHERE key
   LIKE 'owner:%'`: every chain with owner rows has its marker. An image from
   before step 14 routes a chain whose marker is lost by its flag and its
   preconditions, and an unmet one sends its writes to DuckDB. Restore the
   marker (its content is the latch's ISO time from the owner row) or do not
   roll back.
2. **`.env` by exact names** (the plan's H3): every one at production's value
   — that image reads them all again, and for many an absent value means
   `duckdb` (`KS_USER_STORE` absent is a 401 for everybody). Above all
   `KS_DUCKDB=off`: the final stage deleted the DuckDB file (2026-11-12), and
   that image runs without it only under `off`. `off` is necessary, not
   enough: that image falls back to `on` whenever a coherence key is unmet
   (OD-S5-1 (a)), a start that cannot reach Postgres included, and under `on`
   it creates an empty `data/analytics.duckdb` and starts the empty-DuckDB
   boot sync. Right after such a rollback, confirm `/api/health`'s
   `duckdb_switch.held_back` is empty, no `duckdb_off_held_back` page fired
   and no new `data/analytics.duckdb` appeared; if any did, stop that image.
3. **`meta.alembic_version`** is the revision that image requires
   (`0035_batch_e_chains` for every image built before S14-00), or roll back
   to the last image that requires the one there instead.

Fast: `docker tag …:rollback-step14 …:latest` for web, bot and migrate, then
`docker compose up -d` without `pull`. Proper: `git revert` the PR and let CI
deploy.

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

### The /api request log

`docker logs` goes with the container, and on 2026-10-29 the dead-route audit
found four hours of it: too little to call a lever run by hand once a month
dead. So nginx also writes one JSON line per HTTPS request under `/api/` to
`/var/log/ks-api/api.log` on the host (`nginx/nginx.conf`, `ks_api`): time,
method, path, status, bytes, both timings, the upstream by name and by
address. **No client address, no user agent, no query string**: the query of
a search is what somebody typed. The path is cut out of the request line as
it arrived, so a request answered with the 502 page keeps its own path.

The bound is the `api-log` service, declared beside the mount that creates
the log (`nginx/api-log/rotate.sh`, every five minutes): the file is cut at
the first pass after midnight UTC, or past 32 MiB, compressed, and the newest
62 cuts are kept. That is two months on an ordinary day, and in a flood never
much more than 62 × 32 MiB of text. Copy, then truncate, because nothing
there can signal nginx: a line written inside the copy can be lost.
`tests/unit/test_api_request_log.py` holds the path, the format and the bound
to each other and runs the script.

Which routes were asked, on the host:

```bash
zcat -f /var/log/ks-api/api.log /var/log/ks-api/api-*.log.gz | python3 -c '
import collections, json, sys
c = collections.Counter((r["m"], r["p"]) for r in map(json.loads, sys.stdin))
for (m, p), n in c.most_common(): print(n, m, p)'
```

## Testing

`pytest.ini` sets `testpaths`, `asyncio_mode = strict`, and deselects the
`external` and `slow` markers by default, so a bare `pytest` is the safe run —
no paths to remember, nothing reaching the network.

**Baseline as of 2026-11-05 (step 14's S14-20), with the store up: 9 288
passed, 10 skipped, in about 12 minutes** — the skips are ClickHouse (6), no
real gpg on the machine (3) and one TIMESTAMPTZ case. Any change that lowers
the passing count is a regression until explained. (It was 1 297 passed on
2026-08-14 without a store, and 3 771 with one on 2026-09-08.)

**CI runs the suite against a real PostgreSQL** since 2026-09-08. Without one,
every differential test — the ones proving DuckDB and Postgres answer the same
question the same way, and since step 14 Postgres the same as DuckDB's answers
recorded on their fixtures (`tests/integration/golden/`) — skipped itself on
`KS_PG_DSN`, so **60 checks never
ran on a pull request**, including all 20 guarding the /traffic port. That tab
reads Postgres in production behind a silent fallback to DuckDB, so a query
broken in one engine only would have kept the page working and left an ERROR
in a log nobody reads. `.github/workflows/ci.yml` now starts
`postgres:17.2-alpine` after checkout with `postgres/initdb` mounted (a
`services:` block starts *before* the checkout that would supply those
scripts), applies `alembic upgrade head`, and **fails the job if any test
still skips for want of PostgreSQL** — the arrangement breaking silently is
the state it replaced. ClickHouse still has no container in CI, so its
differential tests skip there. `tests/unit/test_ci_workflow.py`
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
```

`pytest -m external` runs nothing today: its one test, KeyCRM against the
production DuckDB file, went in step 14's S14-03 with `scripts/check_date.py`
and `scripts/force_resync.py`. `reconciliation_pg` makes that comparison every
morning, and a resync is `POST /api/sync/resync` (`/api/duckdb/resync`
until step 14's S14-17, then an alias until the final stage removed it on
2026-11-12; a POST there answers 405 now and a GET the JSON 404, as for any
path no route serves).

### Type checking

`basedpyright` is CI's second job (`typecheck`), beside the suite. It covers
`core web bot scripts deploy migrations`, everything the pyflakes guard walks
except `tests/`. It runs in `standard` mode and models Python 3.14 on Linux
(`pyrightconfig.json`). It fails on any error or warning that is not in
`.basedpyright/baseline.json`, and on a baseline that still lists a finding the
code no longer has. **The baseline only shrinks.**

pyflakes sees a name nobody defined; this sees a call that cannot succeed, on
lines no test runs. Its first run (2026-10-21) found 1 122 findings, and five
of them were three live defects:

- the web memory watchdog had raised AttributeError on every alert since
  2026-09-14;
- a language chosen in the bot's settings had been stored and never confirmed
  since #76;
- `/api/health` had published `sync: null` since 2026-02-12.

The other 1 117 are the baseline. Most of them are:

- python-telegram-bot's Optional attributes;
- DuckDB rows typed `tuple | None`;
- values the code knows are set and the checker cannot see;
- third-party stubs.

One is a latent defect in code nothing calls (`MeiliClient.get_stats` calls
`.get` on a pydantic model).

Run it the way CI does. The findings depend on the installed packages' own
annotations, and the laptop's fastapi and pandas are not production's:

```bash
docker run --rm -v "$PWD:/w" -w /w python:3.14-slim sh -c \
  'pip install -q -r requirements-dev.lock && basedpyright'
```

The baseline matches a finding by file, rule and column span, not by line.
Moving code leaves it alone, but editing the expression a baselined finding
sits on changes its span, and the finding then reads as new. When that
happens, fix it, or rewrite the baseline with `basedpyright --writebaseline`
in the same container, so that the moved entry shows in the diff. Rewrite the
baseline in the same commit as any fix that removes a finding, or the job
fails on the leftover. A basedpyright bump is a commit of its own, with the
baseline rewritten beside it.

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
- Predictions stored in Postgres `app.revenue_predictions`, written by chain 7b-3 (DuckDB's `revenue_predictions` until it latched)
- Frontend shows forecast bars (lighter opacity) on Revenue Trend chart for the month, the week and a custom range with future days
- **Every forecast a page shows is read from `revenue_predictions`; no request runs the model** (owner's decision, 2026-11-04 — the Rust council's decision 6). It is as of the last training (Mon/Thu), retail only — b2b and the other sales types show none — and `last_trained` (the Kyiv day of the rows' `created_at`) and `model_metrics` come from the same rows, so any process, the Rust read API included, can answer it. Until then the week and a custom range ran the model on the request over 780 days of Gold, and `last_trained` came from web's memory — null after every restart, until the next training
- "Predicted: ₴X" badge in chart header — what the bars add up to (`dashboard_service.stack_forecast_on_actuals`): the period's actual so far, plus on each forecast day what its forecast adds above that day's actual. Until 2026-11-05 the month added today's revenue so far to today's whole forecast (₴116 546 twice on 2026-11-04) and the week held only the days from today on
- **The comparison under the chart is the whole period's** (owner's decision, 2026-11-12). The month and the week end today (`parse_period`) and are drawn to their last day (`dashboard_service.last_day_of_period`). Their comparison is the window before the whole period, day by day: `previous_period` sets November beside 2–31 October, and a week beside the week before, Monday beside Monday. The dashed line therefore runs under the forecast days too. The growth badge adds up the days through today on both sides, so 1–12 November is set beside 2–13 October. A range reaching into the future counts only its days behind it, where it used to set its days so far beside the whole window. The store's trend takes `period_end` and `through`, and `dashboard_service.fit_trend` lays the window under the days drawn, by date. Until then the month compared its days so far with as many days before them, and set each forecast day beside the day as many days back as there were forecast days, read from Silver: under 19–30 November the line drew 1–12 November. `year_ago`, the chart's default, already went day by day
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
KeyCRM API stores timestamps in +04:00 (server timezone), but UI displays in Europe/Kyiv. Every date is taken in Kyiv before it is extracted: Silver carries `order_date` as the Kyiv date (`AT TIME ZONE 'Europe/Kyiv'`, rendered by `core.duckdb_constants._date_in_kyiv` through the dialect), which is what DuckDB's queries did too.

### Retail / B2B / Internal
`sales_type` is materialised into `silver.orders` by one CASE in the Silver
projection (`core.warehouse_sql.silver_select_sql`, derived by
`pg_warehouse_derive`; it was DuckDB's `refresh_warehouse_layers` until step
13), and every dashboard endpoint defaults to `Query("retail")`:

```
source_id = EXHIBITION_SOURCE_ID (5)      → exhibition (the fair, whoever staffed it)
manager_id IS NULL                        → retail    (Shopify, no manager)
manager_id = B2B_MANAGER_ID               → b2b       (the wholesale manager)
manager_id in managers.is_retail = TRUE   → retail
otherwise                                 → internal
```

The exhibition branch is first on purpose: the fair was staffed by a retail
manager, so a manager-based rule would pull a one-off channel into the retail
trend it would distort. It counts as revenue (source 5 is in
`REVENUE_SOURCE_IDS`) and is not gated like `internal`.

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
keep. Set it via `POST /api/managers/{id}/retail-status`, which raises the
derivation signal in the same transaction, because `sales_type` only changes
on a rebuild.

**"First purchase" has four definitions, kept on purpose** (owner's decision
O7, 2026-11-11). Silver's (`silver_recompute_ctes`, which Gold's
`new_customers` counts): the earliest non-return order on any source and of any
sales type, and `is_new_customer` only on an active-source order of that day.
`app.customer_profile`'s: retail orders on active sources (frozen since O6).
The SMS audience's: retail order lines on active sources, so an order without
lines does not count. The cohorts' (`core/sql_dialect.py`): the earliest
non-return order of the requested sales type on any source, so a retail
cohort takes in a buyer whose first order was internal, b2b or at the
exhibition, whom Silver counts as not new. Measured on production for the 365
days to 2026-11-11:
8 168 buyers made a first retail order, and Silver's rule counts 63 of them as
not new, exactly the gap to Gold's 8 105 — 40 because their first order was on
Opencart (intended: a purchase on a retired channel is still a purchase), 21
because it was an internal shipment (9 of them with no money), one b2b and one
at the exhibition. The SMS audience disagrees with the order-based rule for 2
of 19 413 retail buyers, both for want of line items.

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

**An audience is groups, ORed** (owner's decision, 2026-10-17). "Two purchases
in 2026, or anybody who bought in the last three months" could not be said:
filters were ANDed, `orders_min` counts the whole history, and the window was
one number for the whole audience. Now each group is a window plus a filter
set, ANDed inside the group, and a customer is in the audience if they match
any group (`SmsAudienceGroup`, at most `MAX_AUDIENCE_GROUPS` = 5).

- **Group 1 is the flat parameters, exactly as before**; groups 2..N travel in
  one query parameter, `or_groups`, a JSON array keyed by the flat names,
  refused strictly (`extra="forbid"`, strict types, a 400 that names the
  group). A test holds the JSON form to the flat one: same keys, same bounds.
- **One group selects what the audience selected before groups.**
  `tests/sms_segments_legacy.py` freezes the query as it was on main at
  5b4d1c61, and `test_sms_audience_groups_two_engines.py` runs it beside the
  current one for every filter: the same rows, on PostgreSQL — and on DuckDB
  too until step 14's S14-08 left Postgres the audience's only engine. On
  production on 2026-10-17, group B alone through both: zero rows different.
- **A group's window is its own.** `scored` reads everyone inside the widest
  window and a narrower group carries `recency <= ?` in its own flag, so a
  92-day group never cuts a 365-day one. The frozen `maxRecencyDays` is the
  widest window.
- **Orders in a period** (`period_orders_min` with `period_from` or
  `period_days`) is counted by Kyiv `order_date` in the same `cust` pass as
  `orders`, so returns, retired sources and other sales types never count.
  "Within N days" is spelled as `recency` is (`today - order_date <= N`), so it
  equals an N-day window, edge included. A period reaching past its group's
  window is cut by it, and the page says so.
- **Common to the campaign, applied to the union**: the sales type, the level
  and its cut-offs, phone validity, opt-outs, one SMS per phone, the grouping
  and one control drawn over the union (`core/sms_holdout.py`, unchanged).
- **What each group brings**: the response's `groups[]` — `matched` (its own
  conditions before the common rules, so a group that empties the audience
  says so), `recipients` (the audience's phones it reaches, which is what it
  would send to alone) and `onlyHere`. Counted per phone: `group_mask` is the
  number's, so a number shared by buyers in two groups counts for both. The
  funnel's `filtered` stage is "in any group".
- The frozen `criteria` keep `filters` (group 1, for readers from before
  groups) and add `groups`. A preset stores `groups` and mirrors group 1 at
  the top level, where an older page reads it. A follow-up refuses
  `or_groups` and the period parameters like every other audience parameter.

Measured on production on 2026-10-17 (retail, valid 380 numbers, no opt-outs):
A, two or more orders since 2026-01-01, 2 626 phones; B, last order within 92
days, 3 103; A ∪ B 4 138, of which 1 035 only through A. The store's query and
plain SQL agree on all three. Inside the default 270-day window A would be
2 610.

**The screen holds conditions, not parameters** (2026-10-17, after the owner's
first real campaign through the form typed 92 days into an order count, put a
period's end into its only date field, and lost January to a 270-day window
nobody had set). `utils/smsConditions.ts` is the model: a group is sentences —
«Сделал(а) [2+ заказа] [с 1 янв по 14 окт 2026]» — and `definitionToCriteria`
turns it into the window-and-filters shape above only to ask, so the
transport, `or_groups` and the freeze are unchanged.

- **Lifetime and in-period orders are one condition with a period** («за всё
  время | последние N дней | с … по …»). The server took two parameters for
  that: `period_to` (inclusive end; alone it is "up to a date", never beside
  `period_days`) and `period_orders_max` (≥ 0; alone it has no floor, so `0` is
  "bought, but not in the period"). "Since a date" and "within N days" render
  the SQL they rendered before, byte for byte.
- **The window is derived, never shown**: a group's "last order" upper bound,
  else 3650 days, which is all time — the server cap went 730 → 3650 for
  exactly this. Under 30 days it is the floor plus `recency_max`. "Last order
  in the last 270 days" sends exactly `max_recency_days=270`, which is how the
  built-in presets still reproduce.
- **Relative stays relative, dates stay dates**: presets and the draft
  (`sms.campaignDraft.v2`, reading v1 once) store `groups[].conditions` beside
  the old shape, which an older page still reads. Old presets, v1 drafts and
  past campaigns' frozen `criteria` (store field names) map onto conditions.
- **An unfinished condition is left out of the preview and holds the step** —
  never frozen with a condition the person believes is applied.
- **The preview carries the campaign name** once it is valid, because the
  holdout is drawn per (buyer, name): it previewed under "default" and showed
  375 for a campaign frozen with 429.
- Grouping rfm/single, the level thresholds and the ranking basis are step 2's;
  the value level as a filter stays in step 1, for the whole campaign.

On production on 2026-10-17, the new code's own SQL beside plain SQL: «2+ с
01.01 по 14.10» OR «1+ с 16.07 по 14.10» is 4 101 (2 612 / 3 076) both ways.

**What the step says about itself** (2026-10-18, the redesign's follow-ups):

- **"The last N days" is N + 1 calendar days**, and the line under the number
  says so: every relative time here is `today − date <= N` in Kyiv dates, so
  on 18 Oct the last 91 days are «= с 19 июля по 18 октября 2026,
  включительно». The label follows the query, never the other way round — the
  built-in presets were written against it. `utils/smsDates.lastDays` is the
  one place the page does that arithmetic; `tests/unit/test_sms_last_n_days_edge.py`
  runs the server's own fragment for the same literals the vitest states.
- **Each template chip carries its count**, and **an empty group names the
  condition that emptied it** («Никого: условие «Сделал(а) 92+ заказа за всё
  время» не подходит ни одному клиенту»). Both are previews of other
  audiences, asked by `useSmsAudienceCounts`: one query per distinct question,
  one at a time, cached 30 minutes under their own key (a send does not refetch
  nine chips at once), templates only once their row is on screen, a group's
  only once its count has settled and is zero — at most one preview per
  condition. Named on the page, not counted by the server: a per-condition
  count would split the predicate every campaign is frozen from.
- **Inside the Telegram Mini App** «Далее · N получателей» is Telegram's
  MainButton and the bar draws none; the BackButton closes the sheet or
  dialog on top (`utils/layerStack`), else steps back. Telegram's SDK is
  loaded only inside Telegram, and only drives buttons in a real client of
  Bot API 6.1+; outside it nothing changes.
- **A number the gateway rejected in two campaigns is left out** of every
  audience and of a follow-up (`ok_reachable`, funnel stage `reachable`).
  Measured before it was built: of 38 numbers ever rejected (REJECTD/Rejected),
  15 were messaged again later and all 15 were rejected again; none was ever
  delivered after a rejection. Read from the delivery reports at each preview,
  **never written as an opt-out** — `app.marketing_optouts` stays a list of
  people's own requests, one rejection can be a temporary block, and the rule
  can change without a clean-up. On production on 2026-10-18 it removed 17 of
  18 104 people from "all buyers".
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
  marketing, margin, expenses, sms, tgbot. The admin checklist, the bot's keyboard
  and the route table are all built from it, so a new tab is added once.
- `tgbot` (/tg-bot, the customer Telegram bot's activity) was admin-only on
  the day it shipped, 2026-10-25, and the owner opened it to the marketers the
  same day. It sits in the `marketer` and `marketing` presets and in no role's
  `DEFAULT_TABS`: a viewer with no override does not see it. Accounts that
  already held a preset kept their stored list, so the one marketer then
  approved (the `marketer` preset, editor) was given the tab by hand.
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

`available()` is the honest half: it needed `KS_BOT_STORE=postgres` **and**
`KS_USER_STORE=postgres`. Under `KS_USER_STORE=duckdb` the dashboard's list was
in a file the web container holds the single writer for, so no bot process
could reach it — the approval still granted bot access and the message said the
tabs are set from Admin → Users, rather than showing a checklist that would do
nothing. **Since step 14's S14-08 it asks only `KS_BOT_STORE`**, by the adapter
that variable chose: the Postgres one answers True, SQLite's False. The
dashboard's list has no store but Postgres (a constant since S14-00), so the
disagreement this guarded against cannot happen, and the bot's startup ERROR
about it went with it.

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
it disagree about which database is authoritative. **Since step 14's S14-08
neither `_users_run` nor `_perms_run` has a DuckDB tail**: both render into
`app.*` and ask nothing, and `KS_USER_STORE` routes nothing — `_ENV` left
`core/pg_dashboard_users.py`; the `.env` line stays while an image from before
step 14 can be a rollback target, which reads an absent value as `duckdb`.

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

### What stops an SMS send, and the one deliberate second message

In August 2026 a roster of 5 550 was partly sent twice: the stamp came after
the gateway call and a 10 s client timeout invited a second press. The claim
(`get_sms_campaign_targets`, one conditional UPDATE taken before anything
leaves) closed that. Four more things now stand between a press and the
gateway, and each refusal hands the claim back with nothing sent:

- **The name typed back is checked by the server** — `confirm_campaign` on
  `POST …/{campaign}/send`, compared after strip, a 409 before the claim. The
  dialog's disabled button was the only check and a replay skipped it.
- **Another campaign sent in the last day** (`SMS_RECENT_CAMPAIGN_HOURS`, a
  constant, not an env var) is asked after the claim, so two operators sending
  overlapping rosters at once each see the other's `sent_at` and both are
  refused. Matched on the phone. Another campaign's **control** is never
  messaged — the holdout is drawn per (buyer, campaign name), so a second
  campaign over the same audience reached ~10% of the first one's control and
  destroyed its measurement. Its **targets** only by a declared follow-up of it
  (or of the root they share).
- **The balance** (`TurboSmsClient.balance()`) must cover recipients × parts ×
  tariff × `SMS_BALANCE_MARGIN` (1.02): config says 1.28 ₴ and a top-up under
  10 000 ₴ bills 1.29, and the gateway bills batches it then fails to dispatch
  (−675 ₴ once). Viber is priced as SMS for everyone. An unreadable balance is
  a refusal, not a pass. Every refusal is a 409 whose JSON carries `reason`
  (`recent_campaign`, `balance_short`, `balance_unreadable`) and the figures.
- **A guard that raises** releases the claim before the error goes out.

**Since step 14's S14-08 `/sms` has one store, Postgres**, and the guards are
proved on it. `_sms_tx`, `_sms_run` and the audience lost their DuckDB
branches (`_DuckTx`, `sms_segments_select(DUCKDB)`), and the half-switch
guard `refuse_while_unported`/`UNPORTED` — empty since every path ran on both
engines — went with them; `sms_store_is_postgres()` stays a constant for its
last readers (the copy and its comparison, S14-12/13; the manifest, S14-18).
The tests that ran on a DuckDB fixture seed the tables the production reads
name (`tests/integration/pg_sms_seed.py`), and every guard above has a test
end to end over a real Postgres store — the typed name, the claim (two sends
at once, one goes), an overlap, a short or unreadable balance, a guard that
raises — that reads the claim back out of `app.sms_campaigns`
(`TestTheGuardsOverAPostgresStore`). The two-engine files compare with
DuckDB's answers recorded on their fixtures (`tests/integration/golden/`),
dates written back from today so a literal holds on any day.

**Every refusal on the campaign routes is named**, not only the three above:
the name typed back, the claim (`not_frozen`, `already_sent`), the gateway's
own answers, the freeze and every follow-up refusal. `detail` stays the English
sentence API callers read; beside it `reason` (one of
`core/sms_refusals.REFUSAL_REASONS`) and its figures, which the page says in
the reader's language from `sms.refusal.<reason>` (`utils/smsRefusal.ts`) — a
reason with no sentence falls back to `detail`, never to a key. Raised as
`SmsRefusal` (answered by a handler in `web/main.py`) or, from the store, as
`SmsCampaignRefused`, still a `ValueError`. `tests/unit/test_sms_refusal_reasons.py`
walks both modules: every reason raised is in the list and every name in the
list is raised; the locale test requires a sentence for each in three
languages.

**A follow-up** (owner's decision 2026-10-17) is the same people a second time
the same day — a "last day" push. As an ordinary campaign it would re-draw a
control and message part of the first one's. So `follow_up_of=X` on the
preview and the freeze makes the roster exactly X's target members whose
message was accepted, less current opt-outs, `holdout_pct` forced to 0. X must
be sent and not itself a follow-up (followers point at the root). Any audience
parameter beside it is a 400, not ignored. The link is `"followUpOf"` inside
the stored `criteria` JSON — no column, so no revision — read in Python, not
with a JSON operator, because the two engines spell those differently. Its
results say it has no control and point at X; X's results list its follow-ups,
because X's lift then includes the second message.

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
| `pg_warehouse_derive` | every 1 min | Postgres derives Silver and Gold when its signal says one is owed (or the hourly heartbeat), validates, journals, then parses the UTM verdicts. DuckDB's `warehouse_refresh`, not registered since step 13, went in step 14's S14-15 |
| `halfwritten_repair` | every 2 h | re-fetch orders with revenue and no line items |
| `dq_integrity_check` | 01, 07, 13, 19 | Postgres-only since step 14's S14-14: sync freshness, snapshot continuity, the `pg_` twins (Silver, Gold, UTM, order tables), the chain invariants |
| `dq_reconciliation` | 05:30 | compare 90 days against KeyCRM, per order — Postgres and ClickHouse, one fetch; слои `reconciliation_pg`, `reconciliation_ch`; re-fetches by id what Postgres lacks or holds wrong, an order's expenses included (Postgres only, O3 (a)) (DuckDB's arm and its layer `reconciliation` went in step 14's S14-12) |
| `dq_mirror_landing` | 07:30 | Postgres alone and ClickHouse against it, tolerance zero: the UTM verdicts' completeness, Gold's two grains, `bot.db` against its copy, the order-version archive's liveness, the buyers' completeness, ClickHouse's Silver round trip and Gold, its archive. Reconciliation A against DuckDB went in step 14's S14-12 |
| `replicate_operational` | every 1 h | `derive_gender_pg` (the one writer of new buyers' gender verdicts) and the copy of `data/bot.db`, which stands down under `KS_BOT_STORE=postgres`; the id is kept, the operational copy out of DuckDB went in step 14's S14-13 |
| `ch_sync` | every 1 h | Ship silver → ClickHouse, derive gold there, append the archive (шаги 5–6); stands down without `KS_CH_URL` |
| `dq_digest` | 09:00 | one message with WARN+ findings and a delta |
| `weekly_report` | daily 09:30 | last complete week's numbers to every approved user — sends once, then quiet |
| `traffic_report` | daily 09:45 | last complete week's attribution to the admins — sends once, then quiet |
| `bot_memory_watch` | every 30 min (bot) | бот сторожит свои 512 МБ тем же evaluator'ом; предыдущий сэмпл — в `data/memory-bot-last.json` (OOM-счётчик ядра сбрасывается при recreate) |

**`freshness_orders` judges when the order step last wrote, not the
watermark's value** (2026-10-29). `last_sync_orders` holds KeyCRM's newest
`updated_at` — where the next window starts — and stands still whenever
nobody touches an order. Judged at 6 h it paged CRITICAL on five quiet
nights from 2026-09-21 to 10-29, each age the length of that night's gap in
order activity, while the sync landed a batch every minute.
The integrity job reads `meta.chain_watermarks.updated_at` beside the value
(`read_values_and_stamps`, one statement) and judges that stamp for
`data_quality.JUDGED_BY_LAST_WRITE`; every other entity's value is the
sync's own `now()` and is judged as before. What it no longer sees is a
KeyCRM that answers while nobody orders: the longest such gap in the year
to 2026-10-29 was 12.2 h, overnight.

The legacy 06:00 `reconciliation_check` is gone (OD-10, 2026-09-30):
`dq_reconciliation` is the reconciliation. It re-fetches by id, from the
Postgres arm's verdict, the orders we do not hold (MISSING_IN_DK) and then
those we hold wrong (STATUS_DRIFT, VALUE_MISMATCH: a status moved on an order
created and ordered more than 30 days ago reaches no sync), at most
`REPAIR_BATCH_LIMIT` a run; never one KeyCRM no longer returns. The one
repair only the legacy job made now belongs to
`order_status_refresh` (05:15): an order dated into the last 30 days but
created before them, whose status or total moved without `updated_at`
changing. That job re-fetches these orders by id, before 05:30 looks at them.

**What nothing sees, accepted** (the owner's decision O4 (a), 2026-11-11):
an order older than the 90-day reconciliation window whose status changes
without KeyCRM moving its `updated_at`. No sync, no refresh and no comparison
reaches it. Accepted on a measurement of production that day: of the 14 070
status transitions `app.order_versions` captured as `change` from 2026-08-27
to 2026-11-11, every one came with `updated_at` moved, so the delta sync saw
it; 8 were on orders older than 90 days (6 at 91–365 days, 2 older), all with
`updated_at` moved; and `reconciliation_pg` compared 79 runs since 2026-08-25
with zero discrepancies of any class. The case stays unwatched by design;
the 05:15 refresh is unchanged.

**A job must not fire at the instant something restarts web.** The host cron
`0 2 * * 0 weekly_compact.sh`, 02:00 UTC — 05:00 Kyiv in summer, 04:00 in
winter — stopped both containers every Sunday until the week of silence
retired it (2026-10-16; step 14's S14-18 deleted the script). A `CronTrigger`
computes its next fire from registration, so a scheduler that comes back at
02:00:51 sets the next run a day out: the job is not late, it does not exist
when it is due, and `misfire_grace_time` has nothing to forgive. That cost
`dq_reconciliation` every Sunday, and a deploy on a cron instant does the
same today.

`BackgroundScheduler.start()` queues a one-off catch-up for any check whose
last *successful* verdict is older than its cadence (`CATCHUP_CHECKS`), which
also covers deploys landing on a cron instant. For `dq_reconciliation`, which
has no layer of its own since step 14's S14-12 deleted DuckDB's, that is the
oldest of the layers it writes in this process (`CATCHUP_SIBLING_LAYERS`:
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
which honours the kill switch and writes nothing to the send ledger. It reads
Postgres through the job's own readers (`build_report` and its `fetch_*`, or
the traffic report's), refuses without `KS_PG_DSN` (the switches
`KS_READ_WEEKLY` and `KS_READ_TRAFFIC` are not read since S14-07 and S14-05),
and hands them a store that refuses to connect, so it opens no DuckDB file (it
read a DuckDB backup until S14-03).
`--fixture` renders the 31.08–06.09.2026 week from Gold rows copied out of
Postgres, and reads nothing.

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
table that answers whether a week went out, and backdating it was not
available anyway while DuckDB was held open by the running process (the
ledger is Postgres's since chain 11b). Checked
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
  *other* container every 15 min: 12 h for integrity,
  30 h for mirror_landing (30 h for reconciliation, DuckDB's arm, until step
  14's S14-12 deleted it with its layer), and since DN-21 30 h for
  reconciliation_pg — the Postgres half of the 05:30 job, and the comparison
  against KeyCRM meant to outlive DuckDB, which it does: the DuckDB extraction
  it once waited on went with DuckDB's arm in S14-12, and its runs are
  journalled in Postgres (chain 9). A missing block or a layer that never
  succeeded both count as failures. `reconciliation_ch` joins them at 30 h
  since OD-08 (a) (2026-09-30), which made ClickHouse required: same job, and
  a web without `KS_CH_URL` never writes it and pages
  `dq_never:reconciliation_ch`. A copy too old to reconcile (over 3 h)
  is gated, and written with `error_message` beside its `ch_reconcile_pending`:
  it compared nothing, so it moves no age. Written as a success, as it was
  until the OD-08 review, it reset the age every morning ClickHouse stayed
  down, and the page was announced resolved with ClickHouse still down. The
  history that watch rests on is `deploy/daily_soak/22_reconciliation_ch_history.sql`,
  DN-21's bar for `reconciliation_pg` (20) applied to this arm, read the way
  the canary now reads it. The same for `integrity` since O10 (a)+(c)
  (2026-11-11): a run in which every Postgres twin and the chain invariants
  filed only their `*_unwatched` findings is journalled with `error_message`
  and moves no age (`scheduler.blind_integrity_run`), while the scan's own
  CRITICALs still page, and a blindness finding is news in every 09:00 digest
  it stands in (`data_quality.blindness_names`), not only on the weekly beat.
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
  a repair. `machine_attempts_note()` reads the ClickHouse archive's heal
  ledger (24 h window; the buyers' ids-diff was the other until step 14's
  S14-13 deleted it) and is *passed into* `format_alert_message`, which is
  pure: the `pg_order_utm_*` findings carry the Postgres parser's lever line,
  the one `KS_UTM_PARSE=postgres` picked (`REMEDIATION_UTM_PARSED_IN_POSTGRES`)
  until step 14's S14-15 removed the mode and the swap.
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

### Postgres against KeyCRM — the other half of the criterion
**Since step 14's S14-12 the job compares Postgres and ClickHouse only**:
DuckDB's arm (`reconciliation_io.duckdb_orders_in_window`, layer
`reconciliation`), standing down since chain 3's flip, is deleted, and
`MISSING_IN_DK`, `STATUS_DRIFT` and `VALUE_MISMATCH` in the Postgres arm's
verdict are what the job re-fetches by id. Since the owner's decision O3 (a)
(2026-11-11) the snapshot carries each order's expenses, and the Postgres arm
compares their count and sum with `bronze.expenses`, to the kopeck, as
`VALUE_MISMATCH` (WARN — a cost moves no revenue) on `n_expenses` and
`expense_amount`; an order fetched without the key is not compared. Both sides
count only what chain 3 stores, through the writer's own
`pg_orders_write.stored_expenses`: an entry it drops (no id) or refuses
(`expense_refusal`) is not counted from KeyCRM, and a stored row whose id
KeyCRM lists but the chain refuses is not counted from Postgres, because a
re-fetch keeps that row as it is. An order the chain refuses whole
(`pg_orders_write.payload_refusal`: the parse, its header or a line item) is
not compared for its expenses at all, since its listing deletes nothing. An
amount is the number asyncpg sends, rounded as `NUMERIC(12,2)` rounds it: a
float by its binary value, so 2.675 is stored as 2.67, and text as written,
so "2.675" is 2.68. An order whose expenses differ is re-fetched
last in the same batch, and its write deletes what KeyCRM no longer lists
(O2 (a)). History below.

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

### Gold in Postgres, and why it is not the same table

`gold.daily_revenue` (revision 0007) is the second thing Postgres *derives*
rather than receives. `core/pg_gold.py` writes it from `silver.orders`, in the
same call that rebuilds Silver and immediately after it — one floor, one tick,
so Gold can never aggregate a Silver the next statement is about to replace.
`DELETE` then one `INSERT`, in one transaction, under `SET LOCAL lock_timeout
= '5s'`, for `pg_silver`'s reasons — and Silver is the same shape: one `INSERT`
of `silver_recompute_ctes`, where it was an INSERT and a full-table pass-2
UPDATE. Both tables were cleared with `TRUNCATE` until the review's A23
(2026-11), and every read of them waited for the rebuild; a `DELETE` blocks
no reader, and a conflicting lock costs one failed, journalled run that the
next tick retries. The price, a dead version of every row for autovacuum, was
measured first: at production's pace (the 600-second floor) both tables
settle under twice a fresh build; back to back, which only forced refreshes
can do, they grow until vacuum catches up.

**The measures are shared; the shape is not.** The eight aggregates live in
`GOLD_MEASURES` (`core/warehouse_sql.py`) and are rendered verbatim into the
Postgres projection, ClickHouse's derivation and DuckDB's
`GOLD_REVENUE_SELECT_SQL` — rule 1, with a test per expression. They lived in
`core/duckdb_store.py`, beside Silver's projection, until step 14's S14-01:
every Postgres derivation tick imported the DuckDB store, and with it the
`duckdb` driver and pandas, to render a string. Both moved, unchanged
(`core/warehouse_sql.py`; the old names are re-exported for one release), and
`tests/unit/test_warehouse_sql.py` holds every Postgres and ClickHouse
rendering to the bytes 57fd7c22 produced. What does not transfer is DuckDB's
channel columns:
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

**Since step 14's S14-13 nothing copies them**: each is a write chain's, and
`core/pg_operational.py` is deleted with `replicate_after_manual_expense`, the
SMS state's copy (`replicate_sms`) and the dashboard users' copy
(`replicate_dashboard_users`). The hourly job keeps its id,
`replicate_operational` — `/api/jobs`' history and the catch-up read it — and
runs `derive_gender_pg` and `replicate_bot_state`. `app.sync_metadata`,
`app.warehouse_refreshes` and `app.reconciliation_log` are history (OD-S5-4).
History below.

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

**`web/main.py._migrate_sqlite_users_to_duckdb` went round the seam**: a
startup one-shot copying `authorized_users` into DuckDB's own `users` table,
unrelated to what the port is for. It was deleted on 2026-09-07, when the
dashboard's user list moved to Postgres.

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
**no backup at all** — `data/backups/` held `analytics-*.duckdb`,
`deploy/daily_offsite.sh` globbed the same pattern (both gone since step 14),
and the Ark froze the warehouse. One file, one disk, no copy, holding 24 approved users, 25
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

**С S14-06 шага 14 дашборд читает только Postgres**: `KS_READ_GOLD`,
`KS_READ_SILVER` и `KS_READ_DASHBOARD` движок больше не выбирают (до S14-15
их читала только готовность шага 13 на `/api/warehouse/status`), сбой —
отказ 503 `surface=dashboard` (у экрана менеджеров — `managers`).
`/api/summary` по источнику без колонки в DuckDB-Gold (3, 5) по-прежнему
отвечает нулями — константа по решению владельца 5 для шага 14; маршрут
`/api/summary` источник 5 и вовсе отвергает 400 до чтения. Ниже — история.

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
derived, переезд стоит одну пересборку. **С решения владельца O6
(2026-11-11) витрину не пересобирают и не сверяют** — её никто не читал: таблица
держит последнее содержимое, `meta.derivation_runs.profile_rows` пишется NULL,
а `customer_profile_mismatch` не переиспользуется.

Проверено на одноразовом PostgreSQL 17.2 (миграции 0001→0011 + initdb):
читатели шага 1 сходятся с независимо посчитанной правдой по каждой ячейке;
витрина 38 клиентов, SUM(ltv) == Silver, сверка ноль; зеркало покупателей —
ноль находок на честной копии, три подложенных дефекта пойманы, лечится
зеркалом и бэкфиллом.

### Gold в ClickHouse — шаг 4 «Одной бронзы», копия на обкатку

**Режима копии больше нет** (решение владельца O11 (a), 11.11.2026). Шаг 4
копировал `gold.daily_revenue` из Postgres в ClickHouse как есть, ежечасно, и
сверял копию; шаг 5 его превзошёл (Gold считает сам ClickHouse, см. ниже), и
с тех пор копию не запускало ничего. Шиппер, сверка обратным чтением и пять
условий `ch_gold_unreachable`, `ch_gold_ship_failed`, `ch_gold_row_missing`,
`ch_gold_row_extra`, `ch_gold_value_mismatch` удалены; имена не
переиспользуются. В `core/ch_gold.py` осталось то, что деривация шага 5
импортирует, а не повторяет: DDL таблицы (`_TABLE_DDL`), её колонки
(`COLUMNS` — тот же объект, что `core.pg_gold.GOLD_COLUMNS`) и TSV-кодек.
Что шаг 4 обкатал, живёт дальше в шаге 5:

- **Связность**: web в сети `ks-data`, где живёт `ks-clickhouse` без
  опубликованных портов. **Выкладка, меняющая членство, требует
  `--force-recreate web`** — restart его не даёт (урок D3).
- **Включается `KS_CH_URL`** (`http://ks-clickhouse:8123`); без него часовой
  `ch_sync` стоит, а суточная сверка с OD-08 (a) не молчит: слой
  `mirror_landing` получает `gold_values_unwatched` (см. шаги 5–6). Пароль
  `ks_app` (пользователь заведён платформой с правами на `bronze/silver/
  gold.*`) — `KS_CH_PASSWORD` из `.env`; кредензии едут заголовками, не в URL.
- **Форма доставки**: staging + атомарный `EXCHANGE TABLES` — транзакций нет,
  и TRUNCATE+INSERT по живой таблице дал бы читателю пустую.
- **Свежесть — дело вотермарки** (`meta.mirror_state`, строка
  `clickhouse.gold_daily_revenue`, её пишет деривация шага 5), а не сверки.

### Шаги 5–6: ClickHouse считает Gold сам, и архив наследуется

Шаг 4 (копия голда) превзойдён; его режим копии удалён (O11 (a)). Часовой `ch_sync` теперь:
`core/ch_silver.py` отгружает **silver целиком** (staging + EXCHANGE, копия —
допуск ноль на круге) и **деривит gold внутри ClickHouse** из этого silver;
`core/ch_history.py` дописывает в `history.order_versions` разность множеств
id — всё, что есть в `app.order_versions` и чего нет в копии (`ship_history`:
append-only, курсора нет, а не «поверх собственного `MAX(id)`»: BIGSERIAL
коммитится не по порядку, и отгрузка выше `MAX(id)` навсегда пропустила бы
версию, закоммиченную после большей). Строка, потерянная копией ниже её
`MAX(id)`, поэтому доезжает следующей же отгрузкой, с WARNING в логе и
INFO `ch_history_selfhealed` в суточной сверке следующих 24 ч (память
процесса: перезапуск web её теряет).

**Третьего диалекта мер нет** — и это снимает возражение И1: `GOLD_MEASURES`
написаны как `COUNT(DISTINCT CASE WHEN …)` / `COALESCE(SUM(CASE …), 0)`, и
ClickHouse 24.8 понимает этот же текст с той же семантикой (проверено, не
предположено). `derive_gold_sql` рендерит выражения дословно из того же
словаря, что PG и DuckDB; тест провалится, если кто-то перепишет меру «под
ClickHouse».

Суточный вердикт в `dq_mirror_landing`: круг silver (ноль), **gold двух
движков друг против друга** — независимые агрегации одного silver, ноль
всюду, кроме документированного цента на `avg_order_value` (движки по-разному
делят DECIMAL), — и бакеты архива (ноль ниже вотермарки; сама сверка ничего
не чинит: потерянную ниже вотермарки строку довозит следующая часовая
отгрузка по разнице id, и тогда сверка пишет INFO `ch_history_selfhealed`
вместо CRITICAL; человек нужен, только если бакет длиннее, чем в Postgres,
или версию потерял и сам Postgres). Проверено на живой паре
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
**CRITICAL всегда с S14-12 шага 14**: до него — WARN, пока Gold DuckDB ещё
сравнивался с Postgres, и CRITICAL, когда нет (`warehouse_checks_stand_down()`,
константа с S14-00); S14-12 удалил `reconcile_gold` и предикат вместе с ним. Такой прогон держит `ch_silver_roundtrip`
и `ch_engines_gold_*` как были, а не объявляет их решёнными; и архив так же:
прогон, не посчитавший бакеты (`ch_history_unreachable`, `[]` без
`KS_CH_URL`, исключение), держит `ch_history_buckets`, который закрывает
только человек (`history_unverified_conditions`). `KS_CH_URL` уже в условиях
шага 13 (`ch_url`); `reconciliation_ch` — в канарейке. В проде сегодня сверка
идёт каждое утро, так что находки нет, а будь она — WARN в дайджест, не
страница; а утро, на котором ClickHouse не сверили с KeyCRM, пейджит
канарейка через `reconciliation_ch` (см. «How a failure reaches a human»).
Что за 14 дней такого утра не было, говорит не снимок `/api/health`, а
`deploy/daily_soak/22_reconciliation_ch_history.sql` — его прогоняют на хосте
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

**С S14-05 шага 14 вкладка читает только Postgres** — каждый её метод,
разбивка органики и «Тільки піксель» тоже, одним роутером `_traffic_run`:
`KS_READ_TRAFFIC` движок больше не выбирает (до S14-15 его читала только
готовность шага 13 на `/api/warehouse/status`), сбой — отказ 503
`surface=traffic` (см. «A read whose engine fails is refused»).
Ниже — история переноса и блоки вкладки.

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
бронзой всё равно роняет прогон (exit 1). С A23 (11.11.2026) Silver и Gold
чистятся `DELETE`, а не `TRUNCATE`: LOCK тика больше не ждёт, снимок читает
предыдущий Silver целиком, а проверка пустого Silver остаётся страховкой. Платформы в отчёте — в именах
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
рисует контрол, `useQueryParams` не кладёт параметр в запрос. Категория, бренд
и промокод не доезжали до эндпоинтов `/traffic` никогда (FastAPI молча роняет
лишние query-параметры), и выбор их не менял на странице ровно ничего.
Остальные вкладки намеренно не тронуты — сузить любую из них значит сперва
проверить её эндпоинты.

**С 4.11.2026 вкладка — три раздела, у каждого свой путь** (`utils/trafficTabs`,
`SectionNav`; владелец оценил ленту из девяти блоков на 2 из 10): «Воронка»
(`/traffic`, Google Analytics), «Продажи по каналам» (`/traffic/sales`: итоги по
типам трафика, динамика, платформы, органика, «только пиксель») и «Кампании и
реклама» (`/traffic/campaigns`: кампании по меткам, заказы с метками,
окупаемость). Путь, а не состояние: раздел открывается ссылкой, и по пути
карта решает фильтры. Воронке — только `period`: GA4 не видит ни типа продаж,
ни источника заказа CRM, поэтому их нет ни в шапке, ни в запросе (`sales_type`
`useQueryParams` теперь тоже кладёт только там, где он есть в карте). Двум
другим — `period`, `sales_type`, `source_id`. Под ссылками раздела — период
словами («Период: 1–31 октября 2026») и одна фраза о том, какие фильтры
действуют — на воронке только когда в шапке выбран не розничный тип продаж
или источник, иначе её нечем удивить; даты считает `utils/periodDates` тем же
правилом, что `core/filters.parse_period`.

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

**С S14-15 шага 14 `silver.order_utm` пишет только разбор в Postgres.**
Копия из DuckDB (`core/pg_order_utm.py`, `ship_order_utm`,
`ship_after_reparse`), разбор DuckDB (`refresh_utm_silver_layer`) и режим
`KS_UTM_PARSE` удалены: деривация кончается `parse_incremental_locked`, четыре
двери зовут `reparse_router` без стора — `parse_full` для reclassify и CLI,
`parse_incremental_locked` для остальных, — блок `utm_parse` и канарейка
`utm_parse_mode_invalid` ушли, строка «→» находок `pg_order_utm_*` — та, что
была при `postgres`. Обходы дверей — `tests/unit/test_utm_parse_doors.py`;
разбор держится литерала того, что DuckDB писал на золотых фикстурах
(`tests/integration/golden/utm_parse.json`). Ниже — история.

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

**Новые клиенты по видам трафика — люди, а не заказы.** `/api/traffic/analytics`
отдаёт блок `new_customers` (`total`, `summary` по пяти карточкам,
`by_platform`, `by_traffic_type`): покупатели, чей первый заказ вообще попал в
период, на карточке того трафика, откуда пришёл этот заказ. «Новый» — это
`silver_orders.is_new_customer`, тот же флаг, что считает `new_customers` в
Gold, так что вкладка и карточки дашборда не спорят о том, кто новый. **Отдельный
блок, а не третье поле в бакетах**, потому что люди не складываются:
`is_new_customer` стоит на каждом заказе первого дня, и покупатель с двумя
заказами первого дня через два канала новый на обеих карточках и один в
`total`. Поэтому второй запрос отдаёт `DISTINCT (platform, traffic_type,
buyer_id)` и считается множествами в Python — свёртка счётчиков по группам, как
у заказов, посчитала бы такого человека дважды (та же причина, по которой Gold
держит строку-свёртку). Бакеты `{orders, revenue}` не тронуты: их целиком
сравнивают недельный отчёт по трафику и dry run реклассификации.

**Органика разложена по каналам** (`GET /api/traffic/organic`, блок «Органічний
трафік за каналами»). `organic` — один вердикт над очень разными вещами: замер на
проде 19.10.2026, 180 дней розницы, ₴14.1M органики — из них ₴9.0M заказы,
принятые менеджером в Instagram Direct без единой метки, ₴3.1M переходы по
ссылке в профиле, ₴0.9M email, ₴0.7M Telegram, ₴0.3M бесплатные объявления
Google. Канал — `core.utm_classify.organic_channel`, одиннадцать ключей
`ORGANIC_CHANNELS`; **вычисляется при чтении из хранимых меток и нигде не
хранится**, поэтому правка правила не требует реклассификации (в отличие от
`classify_traffic`). Без вердикта (`traffic_type IS NULL`) органика возможна
только через COALESCE на источниках 1 и 2 — это и есть Direct и Telegram. Email
делится на рассылки (Klaviyo: `utm_medium=klaviyo`/`campaign` или имя
«Campaign #N») и автоматические письма (всё прочее, включая Rivo и Shopify Email
— у последнего рассылку от цепочки по метке не отличить). Сторис —
`utm_medium`, начинающийся со `stories`.

У каждого канала: заказы, выручка, новые клиенты (множествами, по причине
абзаца выше), те же цифры за предыдущий период равной длины
(`_comparison_window`) и до 20 строк внутри **в форме отчёта GA4 «Привлечение
трафика»** — `source / medium`, под ним кампания и `utm_content`
(`organic_detail`; source и medium в нижнем регистре, кампания как написана);
`details_total` говорит, сколько их было всего. Заказ совсем без меток — своя
строка «без UTM-міток», а не пропуск: для Direct эта строка и есть ответ на
«что входит». Под названием канала — его группа в словаре GA4 (Organic Social,
Email, Organic Shopping, Referral; «поза сайтом» для заказов менеджера, которых
GA4 не видит), и подсказка «?» — таблица «канал → какие source / medium → группа».
Это **название, а не обещание**: сам GA4 раскладывает сырые метки по своим
правилам (`klaviyo / klaviyo` у него Unassigned), и подсказка это говорит.
Канал, продававший только в
прошлом периоде, остаётся в списке с нулями: это строка, которая нужна больше
всех. Итог блока равен карточке «Органічний» (тест держит). Метки сравниваются в
нижнем регистре **в Python, не в SQL**: `LOWER` в PostgreSQL зависит от
collation базы, в DuckDB — нет, и два движка сгруппировали бы кириллицу
по-разному. Имена каналов на фронте — интерполированные ключи
`traffic.organicChannel.*`/`…Desc.*`, и `test_frontend_locales.py` требует их
во всех трёх языках.

**«Тільки піксель» разложен по покупателю** (`GET /api/traffic/pixel-only`, блок
«Тільки піксель: хто ці покупці»). `pixel_only` — заказ сайта, до которого не
дошли UTM-метки, только кука пикселя Meta/TikTok; пиксель срабатывает и на 74%
органических заказов с метками, так что корзина значит «метка не дошла», а не
«привёл пиксель». Замер на проде 28.10.2026: в августе, когда шаблон
комментария потерял UTM, она выросла с ~30% заказов сайта до 70%, а органика
упала с 35% до 10%. Блок делит её на новых и повторных и для каждого покупателя
называет канал его **самого первого** заказа (`first_order_channel`: органика —
каналом органики, реклама — платформой вкладки, иначе `pixel_only`, `manager`,
`opencart`, `exhibition`, `unknown`). «Первый» — правило Silver для нового
клиента: любой источник и тип продаж, без отменённых, ничья — по id, поэтому
«новый» здесь совпадает с `is_new_customer`. Люди не складываются: покупатель с
пиксельным заказом в первый день и ещё одним позже есть и в новых, и в повторных.
За 30.04–27.10 розницы: 2 125 заказов, ₴4.94M; 689 новых (₴1.26M) и 1 006
повторных (₴3.68M), из них первыми пришли через Instagram Direct 193, ссылку в
профиле 99, рекламу Facebook 82.

**Воронка сайта из GA4** (`GET /api/traffic/funnel`, раздел «Воронка», `/traffic`).
Шаги — события GA4: начало сеанса → просмотр товара → корзина → оформление →
покупка; воронка закрытая и считает людей, поэтому дубли просмотров от
песочницы пикселя Shopify её не завышают. Только сайт: фильтры источника и
типа продаж сюда не доходят.

**Блок переделан 4.11.2026 по утверждённому владельцем макету** — первая
версия получила 2 из 10: «основной воронки как таковой нет (Вошли → товар →
корзина → оформление → покупка, Итог)». Арифметика — `utils/funnel`, фразы —
`utils/funnelText`, сам блок — `SiteFunnel`.

- **Сверху — конверсия и что она значит**, потом пять шагов полосами от общего
  левого края (синее — дошли, бледный хвост — были на прошлом шаге и не
  дошли), между полосами словами: «дошли 12,9 % · ушли 16 280 · больше
  всего», а под ними «▼ хуже обычного: было 48 %», только если шаг ушёл за
  колебания. «Было N» людей — только у первого и последнего шага, в конце
  строка «Итог». Изменение доли пишется «было X %», никогда относительными
  процентами.
- **«Хуже обычного» — z двух долей с поправкой на разброс GA4**:
  `OVERDISPERSION = 1.96` — дневные воронки сентября–октября 2026 разбросаны в
  1,96 раза шире биномиального (χ²/df). Без поправки тест назвал бы
  «заметными» 10 из 12 помесячных изменений конверсии. Ничего не утверждается
  при меньше чем 2 000 зашедших или 50 купивших (один день — ~1 100 и ~23,
  ±0,8 п.п. сам по себе), про группу меньше 200 человек и про долю из меньше
  чем 50 человек. Красный значит только «хуже», всегда со значком ▼.
- **Вывод — сначала факт, потом место, без «из-за»**: при конверсии в пределах
  колебаний — счётчики («Покупателей почти столько же, а зашедших на 13 %
  больше»), затем шаг, ушедший за колебания, и его цена в покупателях
  («корзина → оформление — минус около 100 покупателей»: доля тогда и сейчас
  × люди перед шагом × сколько прошедших его купило). При заметном изменении
  конверсии — шаг, сдвинувший больше всего покупателей (не самый «уверенный»:
  z растёт с числом людей), и состав трафика, если он дал не меньше пятой
  части изменения в ту же сторону (симметричное разложение по
  холодным/тёплым/другим). Состав говорится **долей сайта** («доля тёплых
  выросла с 33 % до 57 %»): до 11.11.2026 было «тёплых стало больше», а в
  октябре 2026 против 2025 тёплых стало меньше (21 940 → 19 592) при доле
  34 % → 56 %. Месяц в пределах колебаний и без сдвинувшихся шагов не
  получает ни строки: вердикт стоит рядом с числом.
- **«Купили» — люди, а не заказы**, и блок говорит это рядом с числом заказов
  сайта в CRM за те же дни (`useSiteOrders`: `/api/summary`, `source_id=4`,
  `sales_type=all`).
- **«Кто как проходит воронку»** — холодные и тёплые, каналы GA4 или
  устройства. ▼ в ячейке — доля шага заметно ниже, чем у остального сайта
  (сайт минус группа); «хуже обычного» в последней колонке — итог группы
  против её же итога тогда. Устройства меньше 1 % зашедших складываются в
  «Другие устройства» (смарт-ТВ — 0 корзин, похоже на ботов), строка не
  судится. Строки не складываются в «Весь сайт» (люди без канала, люди с
  заходами из двух каналов), и блок говорит, на сколько.
- **Состояния словами**: GA4 не подключён или не отвечает, данные устарели
  («показаны данные на 10:42»), последние два дня ещё досчитываются, год назад
  старше 14 месяцев, мало людей, период задом наперёд, нет визитов. В
  Storybook — по истории на каждое (`Traffic/SiteFunnel`), на условных
  числах (`test/funnelFixtures`), не на продовых.

**Меньше текста, 11.11.2026** (владелец: «слишком много текста на вкладке
воронка и ты теряешься, особенно если смотреть с телефона»). Каждый факт
говорится один раз. Вердикт конверсии — рядом с числом («было 2,33 % · в
пределах колебаний»), без заголовка-фразы, которая его повторяла. Фразы «где
уходит больше всего» нет: это слово «больше всего» у полосы. Что меняет
прочтение цифр (GA4 не отвечает, дни досчитываются, история короче периода,
мало людей) — строкой с иконкой в одном жёлтом блоке. Что читают один раз —
как считается воронка, кто холодный и тёплый, почему CRM не совпадает, — под
«?» или ссылкой (`HelpSheet`: снизу лист на телефоне, панель под кнопкой на
широком экране, цель 44 px). На телефоне разбивка — строки «конверсия · было»
с подписью группы, а ▼ в ячейке сказан словами («в корзину 9,5 % — ниже
остального сайта»); каналы GA4 остаются английскими, под ними наше слово
(`traffic.funnelChannelDesc.*`, `test_frontend_locales.py` требует его у
каждого канала, который сервис делит на холодные и тёплые). На макете при
375 px октябрь стал 206 слов вместо 368 и 1 277 px вместо 2 179.

**«Итог» на первом экране телефона, 11.11.2026.** Владелец выбрал вариант 4
макета. На проде «Итог» стоял на 954 px при экране 812 px.

На телефоне:
- сравнение — чип в строке заголовка (`CompareChip`, «⇄ 21–31 окт. ⌄»,
  подпись — `compareChip` в `utils/funnelText`); выбор — в листе с ролями
  radio;
- рамку теряет только ежедневное «ещё досчитывается» (`Notice.routine`);
  настоящие предупреждения остаются в рамке;
- цвета подписаны в первой строке «дошли · ушли», отдельной легенды нет;
- строка «Итог» говорит «из 1 000 зашедших купили N», в карточке числа — одно
  число;
- вывод уходит под «Итог» с заголовком «Что изменилось». Исключение — состав
  трафика: он одной строкой стоит под числом (`sayMixShort`). Без него октябрь
  против 2025 показывал на первом экране одни зелёные «лучше обычного», хотя
  покупателей стало меньше.

Широкий экран не менялся.

Замер на данных прода при 375 px, по низу букв «Итога»: вид по умолчанию —
975 → 690 px, октябрь против 2025 — 919 → 726. Видимая высота внутри Telegram
на телефоне 375×812 — оценка ~700–725 px, не замер.

/traffic теперь ведёт кнопку «Назад» Telegram, пока открыт лист
(`useTelegramBackClosesLayers`). Раньше «Закрыть» в шапке закрывало весь
дашборд вместо листа.

- **Читается вживую из GA4 Data API** (`core/ga4.py`, `runFunnelReport`
  v1alpha), без таблицы в базе. Доступ — сервисный аккаунт `ga4-reader` с
  ролью «Читатель» на ресурсе и без ролей в проекте Google Cloud; JSON-ключ
  лежит на хосте в `/root/secrets/key-api-bot/` (вне дерева — репозиторий
  публичный), права 640 с группой `appuser` контейнера, и монтируется в web
  папкой на `/run/secrets/ks` только на чтение. `KS_GA4_PROPERTY_ID` и
  `KS_GA4_KEY_FILE` — в compose. Без файла маршрут отвечает 503 «GA4 is not
  configured», и блок так и пишет; в Google ничего не уходит. Подпись токена —
  `cryptography`, единственная новая зависимость.
- **Три окна за раз**: период, такой же перед ним и те же даты год назад; на
  окно две воронки (по группе каналов сеанса, чья итоговая строка — весь сайт,
  и по устройству) — шесть запросов параллельно
  (`web/services/ga4_funnel_service.py`). Ответ держится час в процессе, не
  больше 64 ответов (по 32 периода на каждый охват страны), и при
  недоступном GA4 отдаётся последний с `stale: true` — ради квоты GA4 на
  ресурс в час, не ради скорости.
- **Тип трафика решается при чтении** (холодный: Paid Social, Cross-network,
  Paid Shopping/Other/Video, Display; тёплый: Direct, Email, Organic Social,
  Organic Search, Paid Search, Organic Shopping; остальное — «другой»), так
  что деление меняется без перезаписи.
- **Чьи заходы считать — фильтр самой воронки, по умолчанию Украина**
  (12.11.2026, владелец: «мы можем добавить фильтр»). Из-за границы заходят
  в основном боты. Замер в тот день: всё, кроме Украины и «(not set)», —
  17 % зашедших в сентябре, 24 % в октябре, 30 % за 1–10 ноября, покупки в
  0,4 % случаев. Это краулеры из США (открывают товары, не покупают), поток
  из Китая (открывает сайт и только) и с начала ноября заходы на 1–3
  секунды из Бразилии, Вьетнама, Мексики и ещё десятка стран. С ними
  «зашли» в октябре выросли на 13 % против 5 % по Украине, а конверсия за
  1–10 ноября была 1,85 % вместо 2,47 %.
  `country=ua` (по умолчанию) оставляет Украину и «(not set)» — тех, кого GA4
  не определил: они покупают (117 покупок в октябре против 938 по Украине).
  Настоящие покупатели из-за границы, около 5 %, уходят вместе с ботами.
  `country=all` — как в самом GA4. Фильтр — `dimensionFilter` на весь отчёт,
  а не условие первого шага: так каждый шаг равен разбивке по странам
  (1–10 ноября: 8 559 зашли и 212 купили против 8 567 и 212 в сумме
  разбивки; восемь — люди с обоими значениями).
  Переключатель стоит в строке «Период: …» под ссылками разделов
  (`FunnelCountryFilter`): чип с листом на телефоне (`ChoiceChip`, тот же,
  что у сравнения), два сегмента на широком экране. Отдельный ряд в шапке
  опустил бы «Итог» с первого экрана. Строка стала выше на 11 px (21 → 32);
  период через границу месяца в одну строку с чипом не помещается, и чип
  переносится (+40 px). Выбор живёт в сторе фильтров (`funnelCountry`) и
  входит в ключ кэша сервиса.
- **Воронки GA4 видят только 14 месяцев** — максимум для обычного ресурса.
  4.11.2026 они начинались с 2025-09-04 — ровно 14 месяцев назад, по дням, а
  не с начала месяца, — а январь–август 2025 приходили пустыми; агрегатные
  отчёты при этом хранят всю историю. Сравнение с
  прошлым годом блок прячет, если те даты старше `earliest`. О днях вне
  `earliest`…сегодня (по Киеву) GA4 не спрашивают вовсе (`asked_window`):
  ответ от этого не меняется, а окно без таких дней — нули без запроса.
  Дашборд пропускает диапазон задом наперёд и дни раньше 2015-08-14, первого
  дня GA4. На них GA4 отвечал 400 (замер 4.11.2026), и блок писал «не
  отвечает».
- **Трендовая воронка GA4 — не дневная**: на отрезке она считает человека
  один раз, в первый его день (за 01–02.10.2026 «вошли 2-го» 1 428 против
  1 552 у воронки одного дня). Ряд по дням — только воронкой на каждый день.
- **История в базе — следующий шаг**, после шага 14 (новая ревизия меняет
  откат). До тех пор помесячная и понедельная история с сентября 2025 по
  каналам и устройствам и дневная по каналам сохранены страховочной
  выгрузкой в файлах артефакта «Воронка сайта KoreanStory».

### Вкладка /expenses: половина уже стояла, и один веер ценой ₴314K

**С S14-07 шага 14 вкладка читает только Postgres**: `KS_READ_EXPENSES`
движок больше не выбирает, сбой — отказ 503 `surface=expenses`, и пустой
`backfilled_at` у `bronze.expenses` — тоже явный отказ, а не нули. Ниже —
история переноса.

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
права упасть, потому что запись в DuckDB уже состоялась. (С S14-11 пишет
цепочка 8, а с S14-13 копии нет вовсе.)

**Сверка**: `expense_types` идёт с каталогом (27 строк, отгружается целиком
каждый синк, так что первая успешная отгрузка и есть его история).
`bronze.expenses` едет дельтой, поэтому гейтится на `backfilled_at` как заказы,
имела `core/pg_expense_backfill.py` и `POST /api/mirror/backfill/expenses`
(оба удалены в S14-13), и
**читается целиком, а не отпечатками**: 15 тыс. строк дёшево сравнить точно, а
`description` — текст, переписывание которого в ту же длину отпечаток не видит.
Часы сверки — `synced_at` (бухгалтерия DuckDB), не `created_at`: штамп KeyCRM
прочитал бы только что синканный старый расход как потерянный.

### A read whose engine fails is refused

Every tab reads Postgres, the cohorts ClickHouse, and nothing else. When the
engine fails, the router's handler raises `core.read_fallback.refuse(surface,
exc)`: one `ReadUnavailable`, counted per surface, logged as "read refused:
<surface> — its engine failed…" with the cause, and answered by one handler
in `web/main.py` with a 503 carrying `surface` — the cause stays in the log,
because the endpoint is public. `/api/health` publishes the counts as
`read_fallback_mode.refused {surface: {count, last_at}}`, and the canary
pages `read_refused` (WARN) while a refusal is under 30 minutes old: a
refusal leaves no wrong number behind, only a 503 somebody saw.

Nothing that is not an HTTP request answers a refusal with a number either
(DN-20c): the two weekly reports defer to the next tick, the Monday goals
job and training are skipped with nothing written, the search index and the
buyers step skip the tick with the watermark held, the assistant's tools
return `data_unavailable`, and a job's result names
`{"reason": "read_unavailable", "surface"}`.
`tests/unit/test_read_fallback_consumers.py` derives where every refusal
stops, and `tests/unit/test_read_fallback_sites.py` holds every engine call
of a router inside a `try` whose handler is exactly that `raise`.

Until step 14 a failure was answered from DuckDB and counted
(`read_fallbacks`), and `KS_READ_FALLBACK=off` (production from 2026-10-09)
made it this refusal; since S14-16 the refusal is all there is and the
variable is not read. The tabs' numbers are held to literals DuckDB answered
on their fixtures before its side went (`tests/integration/golden/`).

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

`_attribution_coverage_check` was the first that does. Since step 14's
S14-14 deleted it with the DuckDB scan, its Postgres twin
`pg_attribution_coverage_website` is the check, at the same thresholds
(moved byte for byte to `core/pg_warehouse_dq.py`). It rides
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

**It judges the latch's two copies too, since step 14's S14-12** (decision
12): `chain_latch_disagrees`, CRITICAL, for every registered chain whose
`owner:` rows stand in `meta.chain_watermarks` with no marker under
`data/write-chain-owners/` — read afresh from disk on every run, never the
process's cache. A step-14 image routes by neither copy; an image from before
step 14 (the rollback target) routes by the marker and, without it, by its
flag and preconditions again. So the lever is: do not roll back until the
marker is restored (its content: `latched_at` = the earliest owner row), and
"Before any rollback to an image from before step 14". The other direction —
a marker with no owner row, a first write that failed — is safe and is not
filed. An unreadable marker directory is `chain_invariants_unwatched` on
`data/write-chain-owners`, which holds every condition. The daily
`reconcile_operational` judged both directions on `mirror_landing` until
S14-12 deleted it.

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
forecast tables complete and stored by their last scheduled slot (WARN).
Every registered chain is watched (the registry's `chain_modes()` chose
which until step 14's S14-11), a watched chain with no invariants written for
it is reported unwatched rather than clean, and nothing is repaired. **It is judged in the
integrity job beside the Postgres twins, not inside `check_internal_integrity`**
(the DuckDB scan until step 14's S14-14): these are Postgres facts about
tables DuckDB no longer writes, so a scan that raises neither loses a chain
finding nor holds back its page, and the `chain_*` conditions resolve on such
a run the way the twins' `pg_*` ones do.
**Chain 8 is read on every run**, though nothing typed has latched it: each
run reads `app.manual_expenses`' allocator and NULL count, and a Postgres it
cannot read is `chain_invariants_unwatched` (WARN). Measured 2026-09-18, before chain 1's flip: 0 of 56 277
`stock_movements` rows carry a NULL `recorded_at` or `source`.

## History of the DuckDB exit

DuckDB was this system's only store from the first dashboard until August
2026, and it is not in the code any more. This section is what is left of the
long account that stood in this file through the exit: what was done and in
what order, where each decision is written, what is still on disk, and how an
archive is brought back. The long form of every section it replaced is in git
— `git log -p -- .claude/CLAUDE.md`, the revisions before step 14's S14-20 —
and each PR's own description.

### What was done, in order

- **Stages 1–3, «Одна бронза» (2026-08-20 to 2026-09-14).** Postgres got a
  mirror of landing (`bronze.*`, one parse feeding both stores), a
  replication of the tables nothing can rebuild (`app.*`: stock movements,
  SKU history, the manager classification, the bot's state), its own Silver
  and Gold derivation, and the archive of order versions, which had no
  DuckDB copy at all. Each copy was compared daily at a tolerance of zero
  (Reconciliation A) and against KeyCRM (`reconciliation_pg`); ClickHouse
  derived a second Gold from a round-tripped Silver. Every tab then moved
  its reads to Postgres behind a `KS_READ_*` switch with a counted fallback
  to DuckDB (the dashboard first, on 2026-08-28; the other tabs through
  September).
- **Stage 4, the write chains (2026-09-17 to 2026-10-15).** Thirteen chains
  (1, 3, 4, 5, 6, 6a, 7a, 7b-3, 8, 9, 10, 11a, 11b), each a `KS_WRITE_*`
  flag moving a set of tables' writer to Postgres. A chain's first Postgres
  write *latched* it (owner decision OD-19 (a)): a marker under
  `data/write-chain-owners/` and `owner:<table>` rows in
  `meta.chain_watermarks`, after which the flag was no longer a rollback.
  The shadow chains (9, 10, 11a, 11b; OD-02 (c)) also kept handing DuckDB
  every row. Revisions 0034 and 0035 locked out images that did not know
  the chains. Eleven chains latched; 8 (`app.manual_expenses`) and 7a
  (`app.revenue_goals`) write Postgres with no first write yet.
- **The DuckDB-only doors, retired (OD-10, 2026-09-30).** The buyer, order
  and product cards, two debug routes, `/api/buyers/stats`,
  `POST /api/duckdb/purge-orders` and the legacy `reconciliation_check` with
  its routes: none had a caller, and most had stopped working long before.
  A retired GET answers a JSON 404 (`tests/unit/test_od10_retired_doors.py`).
  The one repair the legacy job made that nothing else did — an order
  ordered inside the last 30 days and created before them — belongs to
  `order_status_refresh` (05:15) since.
- **`KS_READ_FALLBACK=off` (2026-10-09 08:01 UTC)** made a failed read a
  503 naming the tab instead of a frozen DuckDB number; the soak that
  licensed it was shortened by the owner to a clean rehearsal.
- **Step 13 (2026-10-09 08:18 UTC).** `KS_WRITE_WAREHOUSE=postgres`:
  Postgres alone derives Silver, Gold and the UTM verdicts, after a
  rehearsal on copies of production passed eleven checks of eleven. DuckDB's
  warehouse checks stood down and their Postgres twins stood in.
- **Stage 5, the week of silence (2026-10-16 to 2026-10-20).** Web ran
  `KS_DUCKDB=off`: nothing opened `data/analytics.duckdb`, a tripwire
  counted any open, and an hourly hash on the host watched the file. The
  second Ark was frozen and shipped before it began. The owner closed the
  week at 105 hours of 168 and started step 14 on 2026-10-21.
- **Step 14 (2026-10-28 to 2026-11).** S14-00 made every mode, verdict and
  routing predicate the constant production published — every chain writes
  Postgres, web runs `off` whatever `KS_DUCKDB` says — so no later PR could
  change where anything reads or writes, only delete dead branches. S14-02
  pinned the archive tools to an image, and S14-01 to S14-18 deleted
  DuckDB's code: the unplanned tools, the way back (the copy-back), the read routers'
  fallbacks, /sms and access, the chains' routing and shadow copies, the
  comparisons, mirrors and copies, the DuckDB integrity scan, step 13's
  switch, the read fallback, DuckDB's `/api/health` blocks and the store's
  DuckDB half with the compaction. S14-19 took `duckdb` out of the images;
  S14-20 is this section. Deleting the file itself was the final stage, on
  the owner's separate yes, after a rehearsed restore from the second Ark.
- **The final stage (2026-11-11 to 2026-11-12).** The rehearsal (Z1,
  2026-11-11) fetched the second Ark's off-site copy, decrypted it with the
  escrowed passphrase, verified it (L0–L2 clean), found a declared fate for
  every table, and compared its history with Postgres: nothing in DuckDB was
  missing from Postgres. On the owner's yes the deletion (Z2, 2026-11-12)
  removed, on the host, `data/analytics.duckdb` (with any `.wal` beside
  it), its `.old`, the nightly backups `data/backups/analytics-*.duckdb`,
  `data/export_parquet/`, `data/duckdb_tmp/` and
  `data/.duckdb_index_short.json`, and the silence check's line from root's
  crontab. The pull request that followed deleted what only looked at the
  file or kept its old names: `deploy/duckdb_silence_check.sh` and the daily soak's
  P1, the `/api/duckdb/*` aliases of the sync levers (with their
  `SLOW_ENDPOINTS` entries and their part of nginx's long-timeout regex),
  the shim `core/duckdb_store.py` with the store's refusing `connection()`
  (owner decision O9 (b)), and `deploy/offsite_check.sh`'s judgement of the
  DuckDB off-site copy. Z3, retiring `rollback-step14` as a rollback target,
  is a later yes.

### Where the decisions are

The owner's decisions are numbered — OD-NN through stage 5, DN-NN for the
design notes, "decision N" in the step-14 plan — and each is cited where it
binds. The plans and handoffs live under `.planning/`, which is not in git
(the repository is public); the step-14 plan is
`.planning/DUCKDB_EXIT_STEP14_PLAN.md` and stage 5's register of every
DuckDB touch point `.planning/DUCKDB_EXIT_STAGE5_DECOUPLING.md`. What a PR
decided is also in its description, which names its deviations, its host
steps and the alert conditions it removed.

### What is left, and why

Deleted at the final stage (2026-11-12), after the rehearsal of 2026-11-11:
`data/analytics.duckdb` (unchanged since 2026-10-16, as the hourly hash
attested until then) and its `.old`, the nightly backups
`data/backups/analytics-*.duckdb`, `data/export_parquet/`,
`data/duckdb_tmp/` and `data/.duckdb_index_short.json`. Kept on disk: both
Arks (local and on the Storage Box), the off-site Parquet archive,
`data/write-chain-owners/` (the latch markers, until Z3),
`data/report-ledger-pending/`, `data/warehouse-utm-reparsed-at`, the `marks`
in `data/alert-gate-*.json`, and the hash check's record under
`/root/duckdb-silence`, as history. The image `rollback-step14` stays on the
host: it runs under `KS_DUCKDB=off` without the file.

Kept in Postgres as history, with no writer and no DROP (OD-11):
`app.sync_metadata`, `app.warehouse_refreshes` and `app.reconciliation_log`;
the `watch:read_fallbacks` and `watch:duckdb_switch` rows of
`app.alert_series` and the `lever_used` rows of `app.alert_events`. And live:
every `owner:*` row, chain 6's `writes:<table>` record and every `last_sync_*`
key in `meta.chain_watermarks`, under their old names — an image from before
step 14 continues its sync from them.

Kept in the code while `rollback-step14` can be a rollback target (owner
decision 8 of the plan: thirty days after S14-19's deploy without a
rollback, and not before the final stage): a chain's first write still takes
its marker and owner rows (decision 3), because that image routes by them;
each chain module keeps its `WRITE_ENV` name; no alert condition that image
treats as retired is reused or renamed (rule 15); and every `.env` line and
`environment:` line in `docker-compose.yml` it reads stays — for many of them
an absent value means `duckdb` there. What to check before such a rollback is
"Before any rollback to an image from before step 14", under Deployment.

### The week of silence, and its way back

What the week changed on the host stays changed: root's crontab no longer
runs the Sunday compaction (`scripts/weekly_compact.sh`) or the nightly
DuckDB snapshot (`deploy/daily_offsite.sh`) — both retired on 2026-10-16 and
deleted in step 14's S14-18. It ran the hourly hash of the file,
`deploy/duckdb_silence_check.sh`, from the week's start until the final
stage: on 2026-11-12, after the restore from the second Ark was rehearsed on
2026-11-11, its line came out of the crontab, the file was deleted, and the
script went from this checkout. Its record under `/root/duckdb-silence`
stays, as history. `.env` keeps `KS_DUCKDB=off`, written bare on a line of
its own: a step-14 web ignores it, `deploy/restore-test.sh` reads it
(`deploy/duckdb_env.sh`) and passes `-e KS_DUCKDB=on` to its own sidecar so
the drill still runs, and an image from before step 14 runs without the file
only under it. `deploy/offsite_check.sh` reported the DuckDB copy as frozen
on it until the final stage, which took every judgement of that copy out of
the check. The crontab changes were made by a person from the print of
`deploy/duckdb_silence_check.sh --crontab`, never by a script.

**On a step-14 image there is no way back to make**: leave `.env` at
`KS_DUCKDB=off` and do not re-add those three cron lines — the scripts are gone
from this checkout, and a step-14 web runs `off` whatever `.env` says. The
way back is a rollback to an image from before step 14 first ("Before any
rollback to an image from before step 14"), whose own code knows
`KS_DUCKDB=on`. If on that image the nightly snapshot's cron line is put
back, `deploy/backup.env` must set `BACKUP_RESTORE_IMAGE` to the image tagged
`rollback-step14`, and that tag must be on the host: the snapshot has had no
default image since S14-02, and without both it refuses every night (exit 2)
and pages "Daily snapshot FAILED". And since the final stage there is no
file for `on` to open: that image runs under `off`, and `on` would first need
a warehouse rebuilt from the second Ark (`deploy/restore_from_export.py`).

### What is left on disk, and how to restore it

The archives are read by DuckDB 1.5.5, the version that wrote every one, in
the image pinned as `keycrm-web:rollback-step14` — never `:latest`, which
carries no `duckdb` since S14-19. The second Ark is self-describing: the
schema, the Parquet, a `RESTORE.md` and its own `ark_freeze.py`. The restore
drill (`deploy/restore-test.sh`) pulls the newest off-site archive and
rebuilds a validated warehouse in a throwaway container of that image;
`deploy/restore_from_export.py` is the one restore path and is never deleted;
`deploy/duckdb_table_fates_check.py` reads a copy against the frozen
manifest — an Ark's byte copy, with `--file`, since the final stage left no
nightly backup to read. The two sections below are how those tools are held
to the image, and what each table became.

### The archive tools on a pinned image (step 14, S14-02)

Step 14 takes DuckDB out of the code, and since S14-19 out of the images:
`requirements.lock` installs no `duckdb`, so neither web's image nor the
bot's carries the driver. `tests/unit/test_images_carry_no_duckdb.py` holds
it: an AST walk of `core/`, `web/`, `bot/`, `scripts/`, `deploy/` and
`migrations/` finds no import of the driver in any form (aliased, inside a
function, `import_module`, `sys.modules`, a `python -c` program in a
string) but in the three host tools that run on the pinned image, and every
module under `core/`, `web/`, `bot/` and `scripts/` imports in a fresh
interpreter in which `duckdb` cannot be found. `requirements-dev.txt` names
`duckdb==1.5.5` on its own line, for those tools' tests and a few that
evaluate a SQL fragment in memory; it used to arrive there only through
`-r requirements.txt`. The
archives stay: the off-site Parquet archive, the nightly snapshots, two Arks.
What reads them is DuckDB 1.5.5, the version that wrote every one, with the
restore code they were proven against — `scripts/compact_duckdb.py`'s phases
2 and 3, which rebuild the schema from the application's own DDL. Both live in
every image built before step 14, and the host's `keycrm-web:rollback-step14`
is the last of them, pinned at S14-00's deploy (the plan's H1/H2; see "Before
any rollback to an image from before step 14"). A tag is never removed by the
deploy's `docker image prune -f`; `:latest` moves with every deploy and
carries no DuckDB since S14-19.

So `deploy/restore-test.sh` and `deploy/ark_ship.sh` — and
`deploy/daily_offsite.sh`, the nightly snapshot, until S14-18 deleted it —
no longer default to `:latest`. They take their image
from `BACKUP_RESTORE_IMAGE` (the Ark: `BACKUP_ARK_IMAGE`, else the same) in
`deploy/backup.env`, and ask it before anything is fetched, linked or frozen
(`deploy/archive_image.sh`): a tag that does not move (not `:latest`, not
untagged), present on the host (nothing pulls it, and `rollback-step14` is on
no registry), and `import duckdb` inside it saying 1.5.5. Any of them false is
exit 2 with a line naming `rollback-step14`; the nightly snapshot, which only
runs on the way back, pages it as a failed night, which is why the way back's
step 3 sets the variable before it puts the snapshot's cron line back. Every
container they start carries `--pull never`.

**What is mounted into it imports only what it carries.** The drill mounts
`restore_from_export.py`, the Ark `ark_freeze.py`, and the table-fate
check's recipe the check and `core/duckdb_table_fates.py` — files from the
checkout, run inside that pinned image (the snapshot mounted
`snapshot_export.py` and `core/snapshot_validation.py` until S14-18 deleted
all three). A name main gains after it is a name
the container does not have. `tests/unit/test_archive_image.py` derives the
mounts from the scripts and their headers' recipes, read as the shell reads
them (`${PWD}`, `$(pwd)`, variables, function arguments, the `DOCKER_RUN`
array; a checkout directory mounted whole is refused, and a source it cannot
resolve must be listed with a reason), and holds every import in them to a
frozen list of what the pinned image carries: the standard library, `duckdb`,
`compact_duckdb`'s eight names and the `core` modules they reach today —
read from 3.0.292's code and checked against 3.0.298's, where both modules it
names are unchanged. A step-14 PR that deletes `compact_duckdb.py` from main
changes nothing there; one that has a mounted file import something new fails
it.

**Response fingerprints.** `deploy/api_fingerprint.py` is how a read PR of
step 14 shows production answered the same before and after its deploy. It
asks a fixed list of GET routes, each with a window for one that is absolute
— by default the last whole month that ended at least 40 days ago, because
the 05:15 refresh rewrites the last 30 — and keeps each body's sha256 and
size, never the body. Routes the API offers no window for (cohorts, the SMS
preview with an absolute period, the stock pages, the goal tables and the
forecast, the header's filter lookups) are asked anyway and marked `live`.
So are routes that take a window and still answer something counted from
today: the smart goals for the window's month (one signal is counted back
from today), the customer insights (all-time lifetime value, purchase
frequency and repeat rate beside the window's figures, so any order moves
them) and the six /margin routes (the window's lines costed at today's
purchase price, which the hourly stock sync rewrites). A live route that
moved is a question the hashes cannot answer, so the "before" is taken just
before the merge and the "after" as soon as the deploy is done. An absolute
route still reads the catalogue as it is today: a brand renamed or a product
moved to another category in KeyCRM moves the past-window answers grouped by
brand or category.
Routes that need a production id or name in the request (a category's
drill-down, one campaign's results) are not asked. `compare`
exits 1 when a past window or any status moved, 3 when only live routes did,
2 when the two snapshots are not of one window. The admin session comes from
`KS_FINGERPRINT_SESSION` and nowhere else: never an argument, never printed
or written, sent only over https (or to loopback) and never after a redirect.
The token is unrevocable for seven days, so the owner runs it in their own
terminal (`read -rs`); the header has the commands. Take two "before"
snapshots first: a past-window route that moves between those is query
noise, not the deploy. Two "before"s say nothing about a live one.

### What becomes of every DuckDB table (stage 5's manifest)

**Since step 14's S14-18 the manifest is a frozen literal** (`FATES`, its
types and their vocabulary, standard library only). The schema, the
compaction, the snapshot validator and the switches' readers it was walked
against are deleted, and so are `duckdb_written()` and `STORE_SWITCHES`,
with the two store predicates they were the last to ask
(`pg_dashboard_users.user_store_is_postgres`, `core/pg_sms.py`). The test
holds the literal consistent with itself and with the chains' `CHAIN_TABLES`,
and `deploy/duckdb_table_fates_check.py` against a fixture file built from
it; the check, on the pinned image, and the final stage are its readers.
History below.

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
(OD-02 (c)) read DuckDB-written in either mode until step 14's S14-10 deleted
the shadow; they read as every latched chain's since, and the walk finds no
DuckDB writer of them. OD-11 is pinned there too:
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
reader says DuckDB. Since step 14's S14-08 the first two have no DuckDB
branch left to read: their sets are literals (`STORE_SWITCH_TABLES`, what the
walk derived the commit before) and the walk requires that no literal DuckDB
write of those tables is left. That cannot see a DuckDB tail back inside a
router, since the walk reads every routed statement as Postgres's, so the
four routers and `_PgTx`'s methods are held to their syntax instead
(`tests/store_reach.py`): nothing on `self` but what renders the statement,
no `super`, no way to the store or its file. A write naming its table as a
`{hole}` — one text for two engines, chain 10's watchdog statements — is
rendered for DuckDB by its module's own function, or says what fills the hole
(`TEMPLATED_ELSEWHERE`): the walk lost all three sample tables before it read
them. `duckdb_written()` is held to the same writers, not to `kind` —
`schema_migrations` is retired at stage 5 and written until then.

`deploy/duckdb_table_fates_check.py` asks the same of a file: the newest
nightly backup, opened read-only, never the live database (refused by name, by
inode, and with a `.wal` beside it). Exit 0 every name declared, 1 a name to
decide, 2 refused. A copy of the 2026-08-31 production backup read 58 of 58.
It refuses what it cannot see, too: a newest backup over 48 h old (`--file`
reads an older one on purpose), and a file without `orders`, `sync_metadata`
and `schema_migrations` or holding under half of today's schema — an empty
file used to read "clean". Since the final stage there is no nightly backup
and no live file to refuse: the rehearsal of 2026-11-11 read the second
Ark's byte copy with `--file`, and every name in it was declared.

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

*Last updated: 2026-11-05 (step 14, S14-20)*
