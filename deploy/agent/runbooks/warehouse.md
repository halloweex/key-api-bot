СЕМЕЙСТВО: деривация склада в Postgres (warehouse_pg:*) — Silver, Gold и UTM выводит pg_warehouse_derive из bronze; валидация, раскладка по sales_type, сигнал.

Ходы:
1. docker logs keycrm-web --since 30m | grep -iE "derivation|derive|validation|signal" — какая проверка не сошлась, какая попытка.
2. psql: SELECT condition_key, state, first_fired_at, fired_count FROM app.alert_series WHERE condition_key LIKE 'warehouse_pg%' ORDER BY last_fired_at DESC LIMIT 5;
3. psql: SELECT trigger, started_at, ended_at, validation_passed, left(error,160) FROM meta.derivation_runs ORDER BY id DESC LIMIT 6; и SELECT * FROM meta.derivation_signal; — идут ли прогоны, отвечены ли запросы (requested против built).
4. psql: SELECT date, sales_type, revenue FROM gold.daily_revenue ORDER BY date DESC LIMIT 5; — жив ли Gold.
5. curl /api/health: блоки derivation, data_quality, mirrors (возраст производных таблиц).
Знание: с шага 13 (09.10.2026) склад выводит только Postgres, с шага 14 в коде нет ни DuckDB-тика, ни его валидатора (warehouse:* — их ключи, больше не приходят). Ошибка, пришедшая ИЗ bronze, валидацией не видна — это к reconciliation_pg/_ch. Рычаг человека — POST /api/warehouse/refresh (деривация, после своего пола), не рестарт.
