# Changelog

All notable changes to this project will be documented in this file.

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
