-- Pairing — the Postgres Silver twins find what DuckDB's checks find (8a/8b).
--
-- Only under KS_DQ_PG_WAREHOUSE=on. deploy/stage4_soak.sh reads the flag from
-- the web container and passes it as the psql variable `dq_pg_warehouse_on`:
-- 1 (on), 0 (off or unset), invalid, unknown. With 0 this is a
-- "not applicable" PASS.
--
-- WHY IT EXISTS
-- When DuckDB stops deriving (step 13), the integrity scan's Silver checks
-- read a frozen table: the arc pages four times a day for rows that are fine,
-- and attribution goes vacuously green. The `pg_` twins replace them, and a
-- replacement is proven equal on live data before anything stands alone
-- (core/pg_warehouse_dq.py). This reads the last 26 h of integrity runs — four
-- or five — and the daily report accumulates the seven days.
--
-- HOW IT DECIDES, PER RUN
-- - the run completed (`error_message` is NULL);
-- - `pg_silver_missing_rows` / `pg_silver_orphan_rows` /
--   `pg_attribution_coverage_website` are present exactly when their DuckDB
--   names are. Counts are shown, not judged: they may differ by the orders
--   still in flight, which the journal does not record;
-- - no `pg_line_items_disagree`. The twins' `pg_headline_vs_line_items` and
--   `pg_goods_shipped_without_sale` are ABSENT while DuckDB looked, by design,
--   so their absence proves nothing and is not asked about;
-- - no `pg_*_unwatched` and no `pg_warehouse_dq_flag_invalid`: blindness is
--   reported, never `[]`, and a blind twin paired with a quiet DuckDB would
--   otherwise read as agreement.
--
-- THE EVIDENCE IS A COPY
-- As in D8: a copy of `app.data_quality_runs` 75 min old or more, or failing,
-- makes this UNKNOWN; so does a window with no integrity run in it.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'dq_pg_warehouse_on'::text AS state
),
dq_copy AS (
    SELECT CASE
               WHEN s.table_name IS NULL THEN
                   'app.data_quality_runs has never been copied into Postgres'
               WHEN s.failures_since_ok > 0 THEN
                   format('the hourly copy of app.data_quality_* is failing (%s in a row): %s',
                          s.failures_since_ok,
                          left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 160))
               WHEN s.last_ok_at IS NULL THEN
                   'the copy of app.data_quality_* has never succeeded'
               WHEN clock.now - s.last_ok_at >= interval '75 minutes' THEN
                   format('the copy of app.data_quality_* is %s min old (limit 75)',
                          floor(extract(epoch FROM clock.now - s.last_ok_at) / 60))
           END AS stale
    FROM clock
    LEFT JOIN meta.mirror_state s ON s.table_name = 'app.data_quality_runs'
),
runs AS (
    SELECT r.run_id, r.started_at,
           left(regexp_replace(r.error_message, '\s+', ' ', 'g'), 160) AS error_message
    FROM app.data_quality_runs r, clock
    WHERE r.layer = 'integrity'
      AND r.started_at > clock.now - interval '26 hours'
      AND r.started_at <= clock.now
),
pairs (duck, twin) AS (
    VALUES ('silver_missing_rows', 'pg_silver_missing_rows'),
           ('silver_orphan_rows', 'pg_silver_orphan_rows'),
           ('attribution_coverage_website', 'pg_attribution_coverage_website')
),
per_pair AS (
    SELECT r.run_id, r.started_at, p.duck, p.twin,
           (SELECT sum(i.count) FROM app.data_quality_issues i
             WHERE i.run_id = r.run_id AND i.check_name = p.duck) AS duck_count,
           (SELECT sum(i.count) FROM app.data_quality_issues i
             WHERE i.run_id = r.run_id AND i.check_name = p.twin) AS twin_count
    FROM runs r CROSS JOIN pairs p
    WHERE r.error_message IS NULL
),
problems AS (
    SELECT r.started_at, format('run %s errored: %s', r.run_id, r.error_message) AS what
    FROM runs r
    WHERE r.error_message IS NOT NULL
    UNION ALL
    SELECT pp.started_at,
           format('run %s: %s %s but %s %s', pp.run_id,
                  pp.duck, COALESCE(pp.duck_count::text, 'absent'),
                  pp.twin, COALESCE(pp.twin_count::text, 'absent'))
    FROM per_pair pp
    WHERE (pp.duck_count IS NULL) <> (pp.twin_count IS NULL)
    UNION ALL
    SELECT r.started_at, format('run %s: %s (%s)', r.run_id, i.check_name, i.count)
    FROM runs r
    JOIN app.data_quality_issues i ON i.run_id = r.run_id
    WHERE i.check_name = 'pg_line_items_disagree'
       OR i.check_name = 'pg_warehouse_dq_flag_invalid'
       OR (left(i.check_name, 3) = 'pg_' AND right(i.check_name, 10) = '_unwatched')
),
agg AS (
    SELECT (SELECT count(*) FROM runs) AS n_runs,
           (SELECT count(*) FROM problems) AS n_problems,
           (SELECT string_agg(what, '; ' ORDER BY started_at DESC) FROM problems) AS listed,
           (SELECT string_agg(DISTINCT format('%s=%s', pp.twin, pp.twin_count), ', ')
              FROM per_pair pp WHERE pp.twin_count IS NOT NULL) AS twins_seen
)
SELECT 'pairing (pg twins)'::text AS "check",
       CASE
           WHEN flag.state = '0' THEN 'PASS'
           WHEN flag.state = 'invalid' THEN 'FAIL'
           WHEN flag.state <> '1' THEN 'UNKNOWN'
           WHEN dq_copy.stale IS NOT NULL THEN 'UNKNOWN'
           WHEN agg.n_runs = 0 THEN 'UNKNOWN'
           WHEN agg.n_problems > 0 THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN flag.state = '0' THEN 'not applicable: KS_DQ_PG_WAREHOUSE is not on'
           WHEN flag.state = 'invalid' THEN
               'KS_DQ_PG_WAREHOUSE is set to a value it does not understand: the twins are off'
           WHEN flag.state = 'unknown' THEN 'could not read KS_DQ_PG_WAREHOUSE from the web container'
           WHEN flag.state <> '1' THEN
               format('dq_pg_warehouse_on=%s is not one of 0, 1, invalid, unknown', flag.state)
           WHEN dq_copy.stale IS NOT NULL THEN dq_copy.stale || '; re-run after the next replication'
           WHEN agg.n_runs = 0 THEN 'no integrity run in the last 26 h to pair'
           WHEN agg.n_problems > 0 THEN left(agg.listed, 600)
           ELSE format('%s integrity run(s) in 26 h, twins agree%s', agg.n_runs,
                       COALESCE(' (' || agg.twins_seen || ')', ''))
       END AS detail
FROM flag CROSS JOIN dq_copy CROSS JOIN agg;
