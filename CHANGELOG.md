# Changelog

All notable changes to this project will be documented in this file.

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
