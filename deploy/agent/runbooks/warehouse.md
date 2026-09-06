СЕМЕЙСТВО: склад (warehouse:*) — валидация Silver/Gold, пересборка, бэкап.

Ходы:
1. docker logs keycrm-web --since 30m | grep -iE "validation|rebuild|warehouse" — какие чексуммы/ячейки не сходятся, какая попытка.
2. psql: SELECT * FROM app.alert_series WHERE condition_key LIKE 'warehouse%' ORDER BY updated_at DESC LIMIT 5;
3. psql: SELECT date, sales_type, revenue FROM gold.daily_revenue ORDER BY date DESC LIMIT 5; — жив ли Gold в Postgres.
4. curl /api/health: блоки duckdb, data_quality, mirrors.
Знание: валидатор ретраит каждые 2 мин, после 3 неудач — один полный rebuild в 6 ч. «missing cells» — августовский класс (в Silver есть дни, которых нет в Gold). Ошибка, пришедшая ИЗ Bronze, валидатором не видна — это к reconciliation.
