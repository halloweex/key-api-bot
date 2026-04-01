# Changelog

All notable changes to this project will be documented in this file.

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
