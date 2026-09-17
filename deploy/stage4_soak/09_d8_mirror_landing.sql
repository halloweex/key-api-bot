-- D8 — the 07:30 mirror_landing run, as far as the derived layers go.
--
-- WHY IT EXISTS
-- `dq_mirror_landing` compares Postgres' Silver, Gold, customer profile and UTM
-- copy against DuckDB's every morning with a tolerance of zero. Under own the
-- two stores derive independently for the first time, so this comparison is
-- the soak's actual criterion: zero findings on the derived tables, every day.
--
-- THE EVIDENCE IS A COPY, SO FIRST ASK WHETHER THE COPY IS CURRENT
-- The DQ journal is written in DuckDB and carried into Postgres by the hourly
-- `replicate_operational` (core/pg_operational.py). A replication that fails
-- raises nothing here — it only stops moving the watermark — and the journal in
-- Postgres then shows yesterday's clean run for as long as anyone reads it.
-- So this is UNKNOWN, not PASS, when:
-- - the copy of `app.data_quality_runs` is 75 min old or more, or failing;
-- - the copy predates 07:45 Kyiv on the day of the run being judged, so the
--   run cannot be in it yet. Before 07:45 the run judged is yesterday's.
--
-- WHAT A FAIL MEANS
-- - no mirror_landing run since that 07:30: the job did not fire (a deploy on
--   the cron instant, or the scheduler down); the catch-up only runs at start.
-- - the run carries `error_message`: it wrote a row and checked nothing.
-- - findings on the derived tables: the two stores' Silver or Gold disagree.
--   `app.data_quality_issues.description` names the columns.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
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
           END AS stale,
           s.last_ok_at
    FROM clock
    LEFT JOIN meta.mirror_state s ON s.table_name = 'app.data_quality_runs'
),
slot AS (
    -- The 07:30 run being judged: today's once its copy can exist, else yesterday's.
    SELECT ((CASE WHEN (clock.now AT TIME ZONE 'Europe/Kyiv')::time >= time '07:45'
                  THEN (clock.now AT TIME ZONE 'Europe/Kyiv')::date
                  ELSE (clock.now AT TIME ZONE 'Europe/Kyiv')::date - 1 END)
            + time '07:30') AT TIME ZONE 'Europe/Kyiv' AS starts
    FROM clock
),
runs AS (
    SELECT r.run_id, r.started_at, r.status, r.critical_count, r.warn_count,
           left(regexp_replace(r.error_message, '\s+', ' ', 'g'), 200) AS error_message
    FROM app.data_quality_runs r, slot, clock
    WHERE r.layer = 'mirror_landing'
      AND r.started_at >= slot.starts - interval '5 minutes'
      AND r.started_at <= clock.now
),
findings AS (
    SELECT i.run_id, i.check_name, i.table_name, i.severity, i.count
    FROM app.data_quality_issues i
    JOIN runs USING (run_id)
    WHERE i.table_name IN ('silver.orders', 'gold.daily_revenue',
                           'app.customer_profile', 'silver.order_utm')
       OR i.check_name IN ('mirror_buckets_disagree', 'gold_rollup_mismatch',
                           'gold_missing_cells', 'gold_orphan_cells',
                           'gold_cell_values', 'customer_profile_mismatch')
),
agg AS (
    SELECT (SELECT count(*) FROM runs) AS n_runs,
           (SELECT count(*) FROM runs WHERE error_message IS NOT NULL) AS errored,
           (SELECT string_agg(format('run %s: %s', run_id, error_message), '; ')
              FROM runs WHERE error_message IS NOT NULL) AS errors,
           (SELECT count(*) FROM findings) AS n_findings,
           (SELECT string_agg(format('%s %s on %s (%s)', check_name, severity, table_name, count),
                              ', ' ORDER BY check_name, table_name)
              FROM findings) AS listed
)
SELECT 'D8 mirror_landing (derived)'::text AS "check",
       CASE
           WHEN dq_copy.stale IS NOT NULL THEN 'UNKNOWN'
           WHEN dq_copy.last_ok_at < slot.starts + interval '15 minutes' THEN 'UNKNOWN'
           WHEN agg.n_runs = 0 THEN 'FAIL'
           WHEN agg.errored > 0 OR agg.n_findings > 0 THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN dq_copy.stale IS NOT NULL THEN dq_copy.stale || '; re-run after the next replication'
           WHEN dq_copy.last_ok_at < slot.starts + interval '15 minutes' THEN
               format('the copy was taken at %s Kyiv, before the %s run could be in it; re-run after the next replication',
                      to_char(dq_copy.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           WHEN agg.n_runs = 0 THEN
               format('no mirror_landing run since %s Kyiv',
                      to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           WHEN agg.errored > 0 THEN left(agg.errors, 400)
           WHEN agg.n_findings > 0 THEN left(format('%s finding(s) on the derived tables: %s',
                                                    agg.n_findings, agg.listed), 400)
           ELSE format('%s run(s) since %s Kyiv, no error, zero findings on the derived tables',
                       agg.n_runs, to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
       END AS detail
FROM dq_copy CROSS JOIN slot CROSS JOIN agg;
