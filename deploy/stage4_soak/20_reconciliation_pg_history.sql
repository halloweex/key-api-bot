-- reconciliation_pg — fourteen clean days against KeyCRM (precondition for DN-21).
--
-- WHY IT EXISTS
-- Reconciliation A proves Postgres agrees with DuckDB; `reconciliation_pg`
-- proves it agrees with KeyCRM, which is the half that stays true after DuckDB
-- is gone. DN-21 puts it under the canary, and a detector that pages from its
-- first day on a history nobody looked at would be switched off by the second.
-- So the history is read first: at least one successful run on each of the
-- last 14 Kyiv days, and no CRITICAL on any run in that window.
--
-- THE EVIDENCE IS A COPY
-- As in D8: the journal reaches Postgres through the hourly
-- `replicate_operational`, and a failed replication freezes it silently. A copy
-- 75 min old or more, or failing, makes this UNKNOWN.
--
-- WHAT A FAIL MEANS
-- A day with no successful run (the 05:30 job did not run, or every run
-- errored — CLAUDE.md's Sunday 05:00 trap is the known cause), or a CRITICAL
-- finding. Either restarts the 14-day count for DN-21. Today is not required:
-- its run may not have happened, or not been copied, yet.
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
           END AS stale
    FROM clock
    LEFT JOIN meta.mirror_state s ON s.table_name = 'app.data_quality_runs'
),
today AS (
    SELECT (clock.now AT TIME ZONE 'Europe/Kyiv')::date AS kday FROM clock
),
wanted AS (
    SELECT today.kday - k AS kday FROM today, generate_series(1, 14) AS k
),
runs AS (
    SELECT (r.started_at AT TIME ZONE 'Europe/Kyiv')::date AS kday,
           r.error_message, r.critical_count
    FROM app.data_quality_runs r, today, clock
    WHERE r.layer = 'reconciliation_pg'
      AND (r.started_at AT TIME ZONE 'Europe/Kyiv')::date >= today.kday - 14
      AND r.started_at <= clock.now
),
per_day AS (
    SELECT w.kday,
           count(r.kday) FILTER (WHERE r.error_message IS NULL) AS ok
    FROM wanted w
    LEFT JOIN runs r ON r.kday = w.kday
    GROUP BY w.kday
),
agg AS (
    SELECT (SELECT string_agg(to_char(kday, 'DD.MM'), ', ' ORDER BY kday)
              FROM per_day WHERE ok = 0) AS days_without_ok,
           (SELECT COALESCE(max(critical_count), 0) FROM runs) AS max_critical,
           (SELECT string_agg(DISTINCT to_char(kday, 'DD.MM'), ', ')
              FROM runs WHERE critical_count > 0) AS critical_days,
           (SELECT count(*) FROM runs) AS n_runs
)
SELECT 'reconciliation_pg history'::text AS "check",
       CASE
           WHEN dq_copy.stale IS NOT NULL THEN 'UNKNOWN'
           WHEN agg.days_without_ok IS NOT NULL OR agg.max_critical > 0 THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN dq_copy.stale IS NOT NULL THEN dq_copy.stale || '; re-run after the next replication'
           ELSE format('%s run(s) over the last 14 days%s%s',
                       agg.n_runs,
                       COALESCE('; no successful run on ' || agg.days_without_ok, ''),
                       COALESCE('; CRITICAL on ' || agg.critical_days, ''))
       END AS detail
FROM dq_copy CROSS JOIN agg;
