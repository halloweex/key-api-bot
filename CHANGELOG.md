# Changelog

All notable changes to this project will be documented in this file.

## 3.0.88

- Give the schema a ledger: migrations run once, and cannot fail in silence (#111)


## 3.0.87

- Move revenue, SMS segments and campaign results onto the level (#110)


## 3.0.86

- Move basket intelligence, chat tools and search onto the level (#109)


## 3.0.85

- Name the order line: silver_order_lines (#108)


## 3.0.84

- Resolve a manager's classification as of the order's date (#107)


## 3.0.83

- Give the Silver row one definition (#106)


## 3.0.82

- Move order_status_refresh off the compaction instant, and mark dirty in the method (#105)


## 3.0.81

- Drop every index on gold_daily_products (#104)


## 3.0.80

- A catalog change widens one scope, not four (#103)


## 3.0.79

- Rebuild gold_daily_traffic for the dates that moved (#102)


## 3.0.78

- Name KeyCRM source 5 «Виставка» and give it its own sales_type (#101)


## 3.0.77

- Bump the version again, after four months of green no-ops (#100)
- Say a standing DQ finding once, not every morning (#99)
- Fix the two defects the cleanup listed and left (#98)
- Remove 6151 lines of dead code, behind a suite that now runs (#97)
- Send technical alerts to every admin, not the first one (#96)
- Do not call a restarted watchdog a stopped one (#95)
- Let something outside the process say the watchdog is alive (#94)
- Watch the floor the compact leaves behind (#93)
- Watch the whole directory, and name what grew (#92)
- Document how a sub-account key actually gets installed (#91)
- Stop warning about the layers the snapshot deliberately omits (#90)
- Let the snapshot container write to its own work directory (#89)
- Take the off-site copy nightly instead of weekly (#88)
- Measure memory that has to fit, not memory the kernel lends back (#87)
- Keep two local backups, not seven (#86)
- Let the import finish when there is no source database (#85)
- Give the restore drill write access, not just read (#84)
- Let a non-root container read the restored archive (#83)
- Mount the drill script instead of baking it into the image (#82)
- Give sftp its own port flag (#81)
- Ship the warehouse export off the box every week (#78)
- Notice when a day of stock history goes missing (#79)
- Stop paging on a percentage that resets every compaction (#77)
- Stop publishing the session handoffs (#80)
- Put the two handoffs where the next session will find them
- Rebuild what changed, not what was looked at (#70)
- Speak the whole bot in the reader's language, not just the report (#76)
- Send the week to everyone, in the language each of them reads (#74)
- Weekly report, three languages, and a menu that stays reachable (#73)
- Record the Redis investigation and what came of it
- Delete the cache that was never there (#72)
- Log how long a request actually took (#68)
- Stop dropping every SMS delivery report (#67)
- Stop documenting a cache that has never run (#69)
- Do not hold a request open for ten minutes (#66)
- Let the reconciliation look further back than ninety days (#65)
- Check the thirteen columns nothing was checking (#64)
- Do not deploy a paragraph (#63)
- Track the project doc, minus the things a public repo should not carry (#62)
- The test suite must not be able to page anyone (#61)
- Compare the cells, and stop calling a job a defect (#60)
- Add the column to the table that already exists (#59)
- Ask KeyCRM what counts as a sale, instead of remembering (#58)
- Give the third category a name, and a door with a lock (#56)
- Run the check we were not alive for (#55)
- Recompute the date an order left, not only the one it arrived at (#54)
- Do not call the wholesale manager unclassified (#53)
- Let a human say who is retail, and notice revenue that is neither (#52)
- Report the next run APScheduler actually has (#51)
- Say the findings out loud, and say each one once (#50)
- Notice when a check stops producing verdicts (#49)
- Queue deploys instead of letting them fight over a container (#48)
- Let jobs run late instead of not at all (#47)
- Register three data agents, and give them what the incidents cost (#46)
- Give the integrity module the logger it was already calling (#45)
- Let the backfill finish: stop hiding failures and stop fearing a 404 (#44)
- Fetch back the orders no date-window sync can reach (#43)
- Compare orders, not just their monthly sums (#42)
- Count a customer as new only if they have never bought before (#41)
- Credit an order to the month it was ordered in, whoever finds it (#40)
- Let a stuck warehouse try again instead of waiting for a human (#39)
- Say it once — an alarm that repeats every two minutes is furniture (#38)
- Stop blaming the warehouse for the ruler's own bends (#37)
- Give the warehouse room to breathe, and a voice when it cannot (#36)
- Take the DuckDB patches that fix our own failure modes (#35)
- Take 5 550 customers' phone numbers out of a public repository (#34)
- Let the money look as unsettled as it is (#33)
- Stop certifying a campaign nobody has measured yet (#31) (#32)
- Read a campaign down a column, not across four cards (#30)
- Let the window match an offer that lasts two days (#29)
- Write the status the results already read (#28)
- Put every arm of a campaign on one axis (#27)
- Say what the result figures mean, and show revenue beside margin (#26)
- Keep the never-messaged out of the arm they never joined (#25)
- Measure from the moment the message went out (#24)
- Claim a campaign before sending it, not after (#23)
- Split a roster the gateway will not take in one call (#22)
- Count an emoji as the two units the operator bills (#21)
- Stop recreating containers the deploy did not change (#20)
- Let a campaign choose which tiers it is for (#19)
- Treat every TurboSMS success code as a success (#18)
- Send over Viber, with SMS as the fallback (#17)
- Report what the server said, not what the status code implies (#16)
- Rehearse an SMS before it becomes a campaign (#15)
- Say why the SMS list is the size it is (#14)
- Send a campaign from the dashboard (#12)
- Migrate delivery columns onto an existing sms_campaign_members (#10)
- TurboSMS integration: send, delivery reports, and opt-outs (#9)
- SMS campaigns UI: tiers, export, and results read against the control (#8)
- Measure a campaign against its holdout, with an interval (#6)
- Freeze the SMS campaign roster, and carry the last order into the export (#5)
- Data platform fixes (#3)
- docs: ultra-deep audit findings (parser, NULL-overwrite, 'other' bucket)
- upsert_orders: never overwrite manager_comment with NULL
- Harden UTM parser: recover metadata the strict format was dropping
- UTM Campaigns table: sortable columns + traffic type / platform filters
- docs: Traffic Attribution deep-dive (pipeline, classification, gaps)
- Traffic: UTM campaigns table, Google ads/organic split, fix TOF/MOF misattribution
- Disk growth watchdog: alert on capacity and 24h growth-rate breaches
- compact: auto-swap inside sidecar removes "operator died" outage class
- upsert_orders: skip-if-unchanged eliminates 1440x write amplification
- Data Quality framework: orchestrator + scheduler jobs + endpoint
- Data Quality framework: Layer 1 + Layer 2 foundations
- Bronze invariant watchdog: catch config drift in 6h, not 30 days
- Bronze alert: defense-in-depth mode gate
- Bronze shadow-write becomes opt-in in legacy mode
- Bronze prune: mode-aware retention policy
- Fix compact preflight: account for source DB already on disk
- Fix: UserRow falls back to generated avatar on photo_url 404
- Security followup 2 (commit D2): robustness — case-insensitive prod detection, clean stale defaults, surface dirty server tree before reset
- Security followup 2 (commit C2): close audit-invariant escape hatches
- Security followup 2 (commit B2): delete dead /v1 dashboard
- Security followup 2 (commit A2): fix WebApp auth — server-side HttpOnly cookie
- Security followup (commit I): drop brittle spelling-grep assertion
- Security followup (commit H): advisory pip-audit in build job
- Security followup (commit G): tighten rate-limit key, trim docstring
- Security followup (commit F): deploy pinned to approved commit SHA
- Security followup (commit E): chat router gated once at include level
- Security followup (commit D): tighten CSP — drop 'unsafe-inline' from script-src
- Security followup (commit C): nginx headers via include + expires
- Security followup (commit B): unify /api lockdown behind a single api_gate
- Security followup (commit A): Tier 1 bug fixes from review
- CI/CD hardening: split build/deploy, manual-approval gate, SHA-pin actions
- Fix Dockerfile.web: glob web/*.py so new modules are included
- Add security-hardening regression tests
- Security hardening: medium/low findings (#10-#16)
- Security hardening: lock down API surface, fix auth gaps (#1-#9)
- Refactor UI: enforce component visual boundaries (#1)
- Lower compact preflight margin 1.5x → 0.8x source size
- Add internal canary: HTTPS health probe + cert expiry watcher
- Add weekly_compact.sh: host-cron wrapper for automated compaction
- Reallocate memory budget: 7g container + 3g DuckDB buffer
- Traffic analytics: detect AI assistants (ChatGPT) as new 'ai' platform
- Hide cost basis from SKU table and Brand rotation reports
- SkuRotationTable: show retail value alongside cost basis
- SkuRotationTable: add Last sale column
- SKU rotation table: 4 action presets, smart suggestions, CSV export
- Dead stock deep analytics: cost basis, GMROI, NPV decision, brand rotation
- Add unit tests for Silver incremental refresh
- Silver incremental: clean orphan rows by including silver-side scope
- Silver incremental refresh: scope DELETE+INSERT to changed_ids + cascade
- Support automated compact: ship scripts/ + register bronze sequence
- Fix stale buyer_name in Meili orders index
- Reduce memory pressure: incremental Meili sync, hourly checkpoint, fix alert
- Add sync_mode field to HealthResponse schema
- H3 Phase 3-5: SYNC_MODE cutover, backfill, prune, replay, alerts
- H3 Phase 2: promotion job bronze → orders_v2 shadow table with diff
- H3 Phase 1: bronze_order_events audit log with dual-write shadow path
- Make reconciliation endpoint non-blocking by default
- Add compact_duckdb.py maintenance script
- Expand UTM classifier: Advantage+, fbsales, telegram, cpc* prefixes
- Add memory monitor job with Telegram alerts at 75%/90%
- Rewrite reconciliation to compare per-order, not counts
- Replace ON CONFLICT with explicit SELECT→UPDATE/INSERT in upsert_orders
- Fix gold_daily_traffic GROUP BY: repeat COALESCE expressions explicitly
- Fix gold_daily_traffic PK violation: GROUP BY COALESCE'd aliases, not raw NULLs
- Fix gold_daily_traffic PK conflict on incremental UTM rebuild
- Fix UTM refresh holding DB lock for 37K rows, blocking health checks
- Fix deadlock: mark_warehouse_dirty called inside connection lock
- Fix warehouse refresh destroying silver: always full rebuild + fix sequences
- Fix ImportError in rebuild-silver: import constants from duckdb_constants, not config
- Add /warehouse/rebuild-silver: DROP+CREATE+INSERT to bypass MVCC corruption
- Bump DuckDB 1.5.1 → 1.5.2 for race condition and index bugfixes
- Add admin endpoint to purge poisoned orders and CHECKPOINT
- Catch ConstraintException in upsert_orders, fallback to UPDATE
- Isolate poisoned rows in orders upsert: skip, don't block batch
- Stabilize DuckDB writes: drop RMW, raise WAL threshold, serialize warehouse refresh
- Fix orders upsert for DuckDB 1.5: autocommit per-row ON CONFLICT
- Fix PK violation: fallback to UPDATE when ON CONFLICT fails
- Fix orders upsert: use per-row execute instead of broken executemany
- Fix orders upsert: use ON CONFLICT instead of INSERT OR REPLACE
- Fix PK violation in upsert_orders: use INSERT OR REPLACE
- Serve React SPA at root (/) instead of /v2
- Add date filter support to marketing report (any period, not just months)
- Pin DuckDB <1.6.0 to prevent unexpected major version upgrades
- Fix DuckDB 1.5 NaN→INT32 conversion error breaking sync
- Fix login 500 error: update TemplateResponse for Starlette 1.0
- Add same month previous year (YoY) comparison to marketing report
- Fix SyntaxError: use list comprehension instead of generator for unpacking
- Add CSV export for marketing report (3 months side by side)
- Add monthly marketing report to Marketing tab
- Fix DuckDB upsert_orders write-write conflict and PK violations
- Fix CI version bump race condition on concurrent pushes


## 3.0.76

- Fix silver_orders OOM: two-pass rebuild, skip warehouse refresh on non-order changes
- Add all-products breakdown with search to Reports tab


## 3.0.75

- Increase DuckDB memory limit to 4GB (checkpoint OOMs at 2GB on 9GB DB)


## 3.0.74

- Fix OOM on startup: remove DEFAULT from ALTER TABLE migration


## 3.0.73

- Add Tier 3: validation retry, auto-resync on drift, dead code cleanup, reconciliation API


## 3.0.72

- Add warehouse robustness Tier 2: audit columns, reconciliation, dirty flag decoupling
- Fix data consistency: use UPDATE+INSERT for all order upserts, stop advancing empty checkpoint


## 3.0.71

- Prevent DuckDB OOM: skip unused self-join, enable disk spilling, serialize heavy jobs


## 3.0.70

- Fix stale returns: use UPDATE instead of INSERT OR REPLACE for status refresh


## 3.0.69

- Fix stale returns: skip product re-insertion during status refresh


## 3.0.68

- Add stale returns diagnostics and bump container mem_limit to 6g


## 3.0.67

- Bump container mem_limit to 5g — 4g OOM-killed during status refresh


## 3.0.66

- Fix margin page: render AppShell outside AdminGuard so sidebar shows


## 3.0.65

- Bump DuckDB memory_limit to 3GB — 2.5GB still OOMs on 60-day status refresh


## 3.0.64

- Fix status refresh OOM: incremental Silver rebuild + memory bump


## 3.0.63

- Restrict Margin Analysis page to admin users only
- Add Margin Analysis page with full frontend and backend


## 3.0.62

- Add /admin/* SPA route so admin page links work on direct access


## 3.0.61

- Fix user management: enforce viewer default, use DB-backed permissions


## 3.0.60

- Fix offer_stocks: add PK migration, switch to INSERT OR REPLACE


## 3.0.59

- Harden DuckDB: safe ROLLBACK, fix sync gaps, fix customer metrics


## 3.0.58

- Fix DuckDB write-write conflict: use INSERT OR REPLACE for order upsert


## 3.0.57

- Fix Today/Yesterday showing no data: use Kyiv timezone for date calculations


## 3.0.56

- Fix double-slash paths in sidebar navigation


## 3.0.55

- Fix DuckDB upsert: use temp table instead of registered DataFrame view


## 3.0.54

- Add INSERT OR IGNORE as safety net for duplicate order inserts


## 3.0.53

- Fix duplicate key crash in upsert_orders during startup sync


## 3.0.52

- Remove /v2 prefix from all navigation links


## 3.0.51

- Move Promocode Analytics from main dashboard to Marketing section


## 3.0.50

- Make User Management table header sticky


## 3.0.49

- Migrate SQLite users to DuckDB on startup


## 3.0.48

- Fix login denied for users not yet in DuckDB


## 3.0.47

- Fix revenue trend showing wrong data for sales_type=all


## 3.0.46

- Fix ML forecast total using actual + predicted revenue


## 3.0.45

- Fix NameError in get_product_performance — build top_by_revenue dict


## 3.0.44

- Add promocode performance overview to dashboard


## 3.0.43

- Fix hardcoded 365-day sync in web/main.py — change to 730 days


## 3.0.42

- Extend default sync to 730 days for accurate YoY comparisons


## 3.0.41

- Fix OOM during DuckDB sync — limit memory, reduce chunk size, increase container limit


## 3.0.40

- Fix DuckDB WAL corruption on aarch64 — add checkpoint after each sync chunk


## 3.0.39

- Add promocode filter to dashboard — full stack (API → DuckDB → React)


## 3.0.38

- Fix sku_inventory_status same PK issue — DELETE+INSERT with temp table


## 3.0.37

- Fix stale stock data — use DELETE+INSERT instead of INSERT OR REPLACE


## 3.0.36

- Update milestones — daily 300K, remove weekly 800K, add YoY comparison


## 3.0.35

- Fix DuckDB deadlock — pass conn to helper methods instead of re-acquiring lock


## 3.0.34

- Improve monthly goal algorithm — weighted blend, ML signal, dynamic caps


## 3.0.33

- Fix monthly goal using wrong YoY growth — exclude incomplete months/years


## 3.0.32

- Fix inflated avg check in Reports — grand_total was summed per product row


## 3.0.31

- Add sell-through velocity section title and description (i18n)


## 3.0.30

- Fix days-of-supply: use 90-day velocity instead of 30-day


## 3.0.29

- Add ABC classification descriptions


## 3.0.28

- Add overstocked status to Inventory Health


## 3.0.27

- Add clickable ABC cards with expandable SKU list


## 3.0.26

- Show current stock value in gauge labels


## 3.0.25

- Remove grey gap from stock gauge and add color legend


## 3.0.24

- Make optimal stock params configurable via API and UI


## 3.0.23

- Fix TS build errors in InventoryTurnoverChart
- Add new inventory
- Add inventory turnover & optimal stock analytics


## 3.0.22

- Fix Decimal TypeError in cohort insights and 429 retry storm


## 3.0.21

- Add cohort analysis enhancements: insights, skeletons, i18n, and controls


## 3.0.20

- Add SVG skeleton loaders and improve MilestoneProgress visuals


## 3.0.19

- Fix date filtering for product pairs & brand affinity endpoints


## 3.0.18

- Upgrade MilestoneProgress with rich SVG animations and Lottie confetti


## 3.0.17

- Add Vector


## 3.0.16

- Show user name and username next to avatar in expanded sidebar


## 3.0.15

- Fix i18n: hardcoded qty strings, fix 60+ bad DeepL translations


## 3.0.14

- i18n: translate MilestoneProgress, rename to Main Dashboard


## 3.0.13

- Move ROI Calculator to new Marketing page, reorder sidebar tabs


## 3.0.12

- Fix i18n: replace hardcoded English strings with t() calls in charts


## 3.0.11

- Sidebar UX: push content with smart formula, collapsed nav icons, language accordion


## 3.0.10

- Restyle language selector as vertical dropdown list with checkmark


## 3.0.9

- Change language selector from cycle to dropdown with all options


## 3.0.8

- Add multi-language support (EN/UK/RU) with react-i18next


## 3.0.7

- Add info popovers to Product Intelligence page metrics


## 3.0.6

- Bump web container memory to 2GB, remove DuckDB memory limit


## 3.0.5

- Fix OOM: use staged temp tables for gold_product_pairs, limit memory to 400MB


## 3.0.4

- Fix OOM crash: limit DuckDB memory + optimize gold_product_pairs query


## 3.0.3

- Add Product Intelligence page with basket analysis, pairs, and momentum


## 3.0.2

- Add winsorized LightGBM training to reduce promo spike distortion


## 3.0.1

- Bump major version to 3.0.0


## 2.0.2

- Fix CI bump: disable checkout credential helper override
- Fix VERSION file missing from Docker images
- Add WAPE metric to predictions and improve CSV export format
- Add auto-versioning with git tags, Docker tags & changelog
- Remove zero-gain dow_event_interaction feature (32→31)
- Fix undefined val_dows after DOW correction refactor
- Add DOW-specific features and expand DOW correction window
- Remove 2 zero-gain features: is_weekend, log_trend_index (31→29)
- Improve revenue prediction: expand to 31 features, widen DOW correction
- export csv added
- Fix inventory queries: convert to f-strings for INTERVAL interpolation
- Fix INTERVAL parameterization: DuckDB rejects ? for all INTERVAL types
- Fix cohort analysis: DuckDB INTERVAL parameterization for months
- Fix 4 critical ML prediction bugs causing wrong forecasts
- Fix goals crash + SQL parameterization + thread-safety improvements
- extra changes
- Fix bot report_service injection into correct module
- Add stock movement tracking and fix inventory data accuracy
- fonts fixed
- Fix data layer: OrderStatus import, BIGINT migration, validation, UTM & traffic


## 2.0.1

- Initial versioned release
- Auto-versioning with git tags and Docker image tagging
