СЕМЕЙСТВО: зеркало и свежесть данных (mirror_*, dq_*, ch_*, order_versions_*, freshness_*).

Ходы:
1. psql: SELECT table_name, last_ok_at, last_attempted_at, failures_since_ok, left(last_error,120) FROM meta.mirror_state ORDER BY last_attempted_at DESC;
2. docker logs keycrm-web --since 1h | grep -iE "mirror|sync|reconcil|backfill" | tail -40
3. psql: SELECT layer, status, started_at, error_message FROM ... — нет такой таблицы в PG; прогоны проверок в DuckDB (залочен). Вместо этого: curl /api/health → блок data_quality (возраст последнего успешного прогона на слой) и mirrors.
4. Если ch_*: KS_CH_URL опционален — стоит ли он вообще (логи скажут «standing down»).
Знание: вотермарка заказов legitimately стоит ночью (01:00–05:15 Kyiv, замерено). failures_since_ok > 0 — мирор реально падает, смотри last_error. bronze.categories пишется только воскресным полным синком.
