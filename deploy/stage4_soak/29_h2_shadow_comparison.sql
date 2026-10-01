-- H2 (chains 9, 10, 11a, 11b) — the 07:30 comparison of a shadow chain's two
-- copies found nothing.
--
-- Only while a shadow chain writes Postgres: the script passes the same four
-- states as H1 (dq_journal_direct, watchdogs_on, weekly_ledger_on,
-- traffic_ledger_on), and a chain whose owner rows stand is judged as on.
--
-- WHY IT EXISTS
-- Under OD-02 (c) Postgres writes first and DuckDB is handed the same row
-- after the commit, and `reconcile_operational` keeps comparing the two,
-- facing Postgres→DuckDB (`compare_shadow`). That comparison is the soak's
-- criterion for these chains — the runbook's "zero shadow_* findings":
--   shadow_duckdb_only_rows   CRITICAL — something wrote DuckDB round the chain
--   shadow_row_values         CRITICAL — the two stores disagree on a row
--   shadow_missing_in_duckdb  WARN     — a shadow write failed (Postgres holds it)
--   shadow_pruned_rows        INFO     — DuckDB's prune lagged; not counted here
--
-- THE EVIDENCE IS THE JOURNAL, SO FIRST ASK WHETHER IT IS CURRENT
-- As D8 does: until chain 9 writes the journal in Postgres directly, it
-- reaches Postgres through the hourly copy, and a copy that stopped would show
-- yesterday's clean run. Direct is the flag the script read, or the journal's
-- owner row (OD-19 (a)). Otherwise UNKNOWN when the copy is 75 min old or
-- failing, or predates the run being judged.
--
-- A RUN THAT DID NOT HAPPEN IS D8'S FAIL, NOT THIS ONE'S
-- No mirror_landing run since 07:30, or one that wrote only its error, says
-- nothing about the shadow: UNKNOWN here, and D8 reports it as the failure it
-- is.
--
-- WHAT A FAIL MEANS
-- Read `app.data_quality_issues.description` for the run: it names the table
-- and the rows. A shadow_duckdb_only_rows is the one that stops a flip in its
-- tracks — a DuckDB writer the chain did not route; find it in the web log
-- before the next tick writes another.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
chains (label, state, tables) AS (
    VALUES ('chain 9', :'dq_journal_direct',
            ARRAY['app.data_quality_runs', 'app.data_quality_issues',
                  'app.data_quality_diffs']),
           ('chain 10', :'watchdogs_on',
            ARRAY['app.disk_samples', 'app.data_dir_samples', 'app.memory_samples']),
           ('chain 11a', :'weekly_ledger_on', ARRAY['app.weekly_report_sends']),
           ('chain 11b', :'traffic_ledger_on', ARRAY['app.traffic_report_sends'])
),
on_chains AS (
    SELECT c.label
    FROM chains c
    WHERE c.state = '1'
       OR EXISTS (SELECT 1 FROM meta.chain_watermarks w
                  WHERE w.key LIKE 'owner:%' AND substring(w.key FROM 7) = ANY (c.tables))
),
journal AS (
    SELECT (:'dq_journal_direct' = '1'
            OR EXISTS (SELECT 1 FROM meta.chain_watermarks
                       WHERE key = 'owner:app.data_quality_runs')) AS direct
),
dq_copy AS (
    SELECT CASE
               WHEN journal.direct THEN NULL
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
           CASE WHEN journal.direct THEN clock.now ELSE s.last_ok_at END AS last_ok_at
    FROM clock
    CROSS JOIN journal
    LEFT JOIN meta.mirror_state s ON s.table_name = 'app.data_quality_runs'
),
slot AS (
    SELECT ((CASE WHEN (clock.now AT TIME ZONE 'Europe/Kyiv')::time >= time '07:45'
                  THEN (clock.now AT TIME ZONE 'Europe/Kyiv')::date
                  ELSE (clock.now AT TIME ZONE 'Europe/Kyiv')::date - 1 END)
            + time '07:30') AT TIME ZONE 'Europe/Kyiv' AS starts
    FROM clock
),
runs AS (
    SELECT r.run_id, r.error_message
    FROM app.data_quality_runs r, slot, clock
    WHERE r.layer = 'mirror_landing'
      AND r.started_at >= slot.starts - interval '5 minutes'
      AND r.started_at <= clock.now
),
findings AS (
    SELECT i.run_id, i.check_name, i.table_name, i.severity, i.count
    FROM app.data_quality_issues i
    JOIN runs USING (run_id)
    WHERE i.check_name LIKE 'shadow\_%' AND i.severity <> 'INFO'
),
agg AS (
    SELECT (SELECT count(*) FROM on_chains) AS n_on,
           (SELECT string_agg(label, ', ' ORDER BY label) FROM on_chains) AS on_listed,
           (SELECT count(*) FROM runs) AS n_runs,
           (SELECT count(*) FROM runs WHERE error_message IS NULL) AS n_clean,
           (SELECT count(*) FROM findings) AS n_findings,
           (SELECT string_agg(format('%s %s on %s (%s)', check_name, severity, table_name, count),
                              ', ' ORDER BY check_name, table_name)
              FROM findings) AS listed
)
SELECT 'H2 shadow comparison (mirror_landing)'::text AS "check",
       CASE
           WHEN agg.n_on = 0 THEN 'PASS'
           WHEN dq_copy.stale IS NOT NULL THEN 'UNKNOWN'
           WHEN dq_copy.last_ok_at < slot.starts + interval '15 minutes' THEN 'UNKNOWN'
           WHEN agg.n_findings > 0 THEN 'FAIL'
           WHEN agg.n_clean = 0 THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN agg.n_on = 0 THEN
               'not applicable: no shadow chain writes Postgres (chains 9, 10, 11a and 11b at duckdb, no latch, no owner row)'
           WHEN dq_copy.stale IS NOT NULL THEN dq_copy.stale || '; re-run after the next replication'
           WHEN dq_copy.last_ok_at < slot.starts + interval '15 minutes' THEN
               format('the copy was taken at %s Kyiv, before the %s run could be in it; re-run after the next replication',
                      to_char(dq_copy.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           WHEN agg.n_findings > 0 THEN left(format('%s shadow finding(s) for %s: %s',
                                                    agg.n_findings, agg.on_listed, agg.listed), 400)
           WHEN agg.n_runs = 0 THEN
               format('no mirror_landing run since %s Kyiv to judge %s by; D8 says why',
                      to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'), agg.on_listed)
           WHEN agg.n_clean = 0 THEN
               format('the mirror_landing run since %s Kyiv wrote only its error; D8 says which',
                      to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           ELSE format('%s run(s) since %s Kyiv, zero shadow findings for %s',
                       agg.n_runs, to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                       agg.on_listed)
       END AS detail
FROM dq_copy CROSS JOIN slot CROSS JOIN agg;
