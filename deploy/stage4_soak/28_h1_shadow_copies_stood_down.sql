-- H1 (chains 9, 10, 11a, 11b) — the hourly copy stays stood down for the
-- shadow chains' tables.
--
-- Only for a shadow chain that writes Postgres. deploy/stage4_soak.sh reads the
-- web container's flags and latch markers and passes, per chain, 1 (the flag
-- says postgres, or the chain is latched whatever the flag says — DN-06), 0,
-- invalid or unknown:
--   dq_journal_direct    chain 9,   KS_WRITE_DQ_JOURNAL
--   watchdogs_on         chain 10,  KS_WRITE_WATCHDOGS
--   weekly_ledger_on     chain 11a, KS_WRITE_WEEKLY_LEDGER
--   traffic_ledger_on    chain 11b, KS_WRITE_TRAFFIC_LEDGER
-- A chain whose owner rows stand in meta.chain_watermarks is judged as on
-- whatever was passed: the copy stands down on those rows too (a lost marker).
--
-- WHY IT EXISTS
-- Under OD-02 (c) these chains write Postgres first and hand DuckDB the same
-- row after the commit, so DuckDB is still a whole copy and the daily
-- comparison keeps comparing (`compare_shadow`). The hourly
-- `replicate_operational` must not: its shape for every one of these tables is
-- DELETE plus INSERT out of DuckDB, and a row whose shadow write failed exists
-- in Postgres alone — a quality run, a sample, a delivered week. If the
-- stand-down lapses, the copy deletes those rows once an hour, looking healthy
-- in between, and for a send ledger the next tick sends that week again.
--
-- HOW IT DECIDES
-- Per chain that is on: the handover is the chain's own owner rows (written in
-- its first writing transaction, on Postgres's clock — the clock the copy
-- stamps with), else the last 75 minutes. A `last_ok_at` after it, or a failure
-- attempted after it, is a copy that did not stand down — except with no owner
-- row, where the copy stamps every table until the flip and a healthy flip
-- reads a stamp inside the window until the chain's first write: UNKNOWN then.
--
-- A failure counts for the day it was stamped as well (O1's rule, chain 3's
-- review). Only a successful copy resets `failures_since_ok`, and under the
-- chain the copy never succeeds: it stamps failing while the flag and the
-- latch disagree, then stands down silently once they agree — so one episode
-- after the handover used to FAIL this check for ever. A `last_ok_at` after
-- the handover is DuckDB's copy already put over the chain's — every row
-- whose shadow write failed gone — which no later hour undoes, and it stays.
--
-- WHAT A FAIL MEANS
-- - a table written or failing after the handover: the copy is writing
--   DuckDB's rows over the chain's. Stop it before the next hour: put the
--   flag of a chain that never latched back, or read the web log for
--   "replicate_operational". A failure that reads "owned by Postgres since"
--   is the latch with its flag put back — not a rollback; the way back is
--   scripts/chain_copy_back.py.
-- - invalid: a KS_WRITE_* value no chain understands; that chain writes
--   nowhere (OD-18 (a)). Fix the .env line.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
chains (label, flag, state, tables) AS (
    VALUES ('chain 9', 'KS_WRITE_DQ_JOURNAL', :'dq_journal_direct',
            ARRAY['app.data_quality_runs', 'app.data_quality_issues',
                  'app.data_quality_diffs']),
           ('chain 10', 'KS_WRITE_WATCHDOGS', :'watchdogs_on',
            ARRAY['app.disk_samples', 'app.data_dir_samples', 'app.memory_samples']),
           ('chain 11a', 'KS_WRITE_WEEKLY_LEDGER', :'weekly_ledger_on',
            ARRAY['app.weekly_report_sends']),
           ('chain 11b', 'KS_WRITE_TRAFFIC_LEDGER', :'traffic_ledger_on',
            ARRAY['app.traffic_report_sends'])
),
owned AS (
    SELECT c.label, min(w.updated_at) AS at
    FROM chains c
    LEFT JOIN meta.chain_watermarks w
           ON w.key LIKE 'owner:%' AND substring(w.key FROM 7) = ANY (c.tables)
    GROUP BY c.label
),
judged_chain AS (
    SELECT c.label, c.flag, c.tables, o.at AS owner_at,
           CASE WHEN o.at IS NOT NULL THEN '1' ELSE c.state END AS state,
           COALESCE(o.at, clock.now - interval '75 minutes') AS since
    FROM chains c
    JOIN owned o USING (label)
    CROSS JOIN clock
),
problems AS (
    SELECT j.label, t.table_name,
           CASE
               WHEN s.failures_since_ok > 0 AND s.last_attempted_at > j.since
                    AND s.last_attempted_at > clock.now - interval '24 hours' THEN
                   format('%s failing (%s): %s', t.table_name, s.failures_since_ok,
                          left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 100))
               WHEN s.last_ok_at > j.since THEN
                   format('%s written by the copy at %s Kyiv', t.table_name,
                          to_char(s.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           END AS problem
    FROM judged_chain j
    CROSS JOIN clock
    CROSS JOIN LATERAL unnest(j.tables) AS t (table_name)
    LEFT JOIN meta.mirror_state s ON s.table_name = t.table_name
    WHERE j.state = '1'
),
per_chain AS (
    SELECT j.label, j.flag, j.state, j.owner_at,
           string_agg(p.problem, '; ' ORDER BY p.table_name)
               FILTER (WHERE p.problem IS NOT NULL) AS listed
    FROM judged_chain j
    LEFT JOIN problems p USING (label)
    GROUP BY j.label, j.flag, j.state, j.owner_at
),
verdicts AS (
    SELECT label,
           CASE
               WHEN state = '0' THEN 'off'
               WHEN state = 'invalid' THEN 'FAIL'
               WHEN state = '1' AND listed IS NOT NULL AND owner_at IS NOT NULL THEN 'FAIL'
               WHEN state = '1' AND listed IS NOT NULL THEN 'UNKNOWN'
               WHEN state = '1' THEN 'PASS'
               ELSE 'UNKNOWN'
           END AS v,
           CASE
               WHEN state = 'invalid' THEN
                   format('%s: %s is a value no chain understands, so it writes nowhere',
                          label, flag)
               WHEN state = '1' AND listed IS NOT NULL AND owner_at IS NOT NULL THEN
                   format('%s: %s since the handover', label, listed)
               WHEN state = '1' AND listed IS NOT NULL THEN
                   format('%s: %s in the last 75 min, and no owner row yet: a healthy flip reads this until the chain''s first write',
                          label, listed)
               WHEN state = '1' AND owner_at IS NOT NULL THEN
                   format('%s: no copy since the handover', label)
               WHEN state = '1' THEN format('%s: no copy in the last 75 min', label)
               WHEN state = 'unknown' THEN
                   format('%s: could not read %s from the web container', label, flag)
               WHEN state <> '0' THEN
                   format('%s: state %s is not one of 0, 1, invalid, unknown', label, state)
           END AS said
    FROM per_chain
),
agg AS (
    SELECT count(*) FILTER (WHERE v = 'FAIL') AS fails,
           count(*) FILTER (WHERE v = 'UNKNOWN') AS unknowns,
           count(*) FILTER (WHERE v <> 'off') AS judged,
           string_agg(said, '; ' ORDER BY label) FILTER (WHERE v = 'FAIL') AS failed,
           string_agg(said, '; ' ORDER BY label) FILTER (WHERE v <> 'off') AS all_said
    FROM verdicts
)
SELECT 'H1 shadow copies stood down'::text AS "check",
       CASE
           WHEN agg.fails > 0 THEN 'FAIL'
           WHEN agg.unknowns > 0 THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN agg.judged = 0 THEN
               'not applicable: chains 9, 10, 11a and 11b still write DuckDB (no KS_WRITE_* at postgres, no latch, no owner row)'
           WHEN agg.fails > 0 THEN left(agg.failed, 500)
           ELSE left(agg.all_said, 500)
       END AS detail
FROM agg;
