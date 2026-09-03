СЕМЕЙСТВО: зеркало и свежесть данных (mirror_*, dq_*, ch_*, order_versions_*, freshness_*).

Ходы:
1. psql: SELECT table_name, last_ok_at, last_attempted_at, failures_since_ok, left(last_error,120) FROM meta.mirror_state ORDER BY last_attempted_at DESC;
2. docker logs keycrm-web --since 1h | grep -iE "mirror|sync|reconcil|backfill" | tail -40
3. Находки прогона — в app.alert_events.context (см. ПЕРВЫЙ ХОД в общей части): какие колонки разошлись и на скольких строках. Это делай раньше логов. Возрасты слоёв по-прежнему в curl /api/health → data_quality и mirrors; сами прогоны лежат в DuckDB и оттуда недоступны, но context — их выжимка, снятая в момент алерта.
4. Если ch_*: KS_CH_URL опционален — стоит ли он вообще (логи скажут «standing down»).
Знание: вотермарка заказов legitimately стоит ночью (01:00–05:15 Kyiv, замерено). failures_since_ok > 0 — мирор реально падает, смотри last_error. bronze.categories пишется только воскресным полным синком.
