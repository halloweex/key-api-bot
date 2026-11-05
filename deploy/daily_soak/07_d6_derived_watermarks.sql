-- D6 — the derived tables' watermarks.
--
-- WHY IT EXISTS
-- `rebuild_silver` and `rebuild_gold` stamp `meta.mirror_state` on success, and
-- the canary pages on these two rows at 90 min (`DERIVED_MAX_AGE_S`). This is
-- the same judgement read directly, plus the one thing the canary cannot see:
-- whether Silver's row count is the count of the bronze it was built from.
--
-- WHAT A FAIL MEANS
-- - `failures_since_ok > 0`: the last rebuild of that table raised; D2 has the
--   stage and the error.
-- - age of 90 min or more: nothing has rebuilt it for longer than the canary
--   allows; D1 and D3 say whether the derivation is stalled or held.
-- - Silver's `last_rows` differs from the bronze count the latest error-free run
--   validated against: the watermark and the journal describe different
--   rebuilds, or the rebuild lost rows.
--
-- Not here, deliberately: `derivation.mode` from /api/health (a fallback to
-- piggyback shows up as D2's "no run"), and `silver.order_utm`'s day-on-day
-- row count, which needs yesterday's value and this table keeps one row. The
-- UTM gap check covers a partial copy directly.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
wanted (table_name) AS (
    VALUES ('silver.orders'), ('gold.daily_revenue')
),
m AS (
    SELECT w.table_name, s.table_name AS present, s.last_ok_at, s.failures_since_ok,
           s.last_rows,
           left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 160) AS last_error,
           floor(extract(epoch FROM clock.now - s.last_ok_at) / 60)::bigint AS age_min
    FROM wanted w
    CROSS JOIN clock
    LEFT JOIN meta.mirror_state s ON s.table_name = w.table_name
),
last_run AS (
    SELECT d.id, (d.validation ->> 'bronze_orders')::bigint AS bronze_orders, d.silver_rows
    FROM meta.derivation_runs d, clock
    WHERE d.layer = 'warehouse' AND d.error IS NULL AND d.started_at <= clock.now
    ORDER BY d.id DESC
    LIMIT 1
),
judged AS (
    SELECT m.*,
           CASE
               WHEN m.present IS NULL THEN 'never stamped'
               WHEN m.failures_since_ok > 0 THEN
                   format('failing, %s in a row: %s', m.failures_since_ok, m.last_error)
               WHEN m.last_ok_at IS NULL THEN 'never succeeded'
               WHEN m.age_min >= 90 THEN format('%s min old (limit 90)', m.age_min)
               WHEN m.table_name = 'silver.orders'
                    AND (SELECT bronze_orders FROM last_run) IS NOT NULL
                    AND m.last_rows IS DISTINCT FROM (SELECT bronze_orders FROM last_run) THEN
                   format('last_rows %s but run %s validated against %s bronze orders',
                          m.last_rows, (SELECT id FROM last_run),
                          (SELECT bronze_orders FROM last_run))
           END AS problem
    FROM m
)
SELECT 'D6 derived watermarks'::text AS "check",
       CASE WHEN count(*) FILTER (WHERE problem IS NOT NULL) > 0 THEN 'FAIL' ELSE 'PASS' END AS verdict,
       string_agg(format('%s: %s', table_name,
                         COALESCE(problem,
                                  format('%s min old, %s rows', age_min, last_rows))),
                  '; ' ORDER BY table_name) AS detail
FROM judged;
