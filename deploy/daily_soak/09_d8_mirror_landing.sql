-- D8 — the 07:30 mirror_landing run, as far as the derived layers go.
--
-- WHY IT EXISTS
-- `dq_mirror_landing` judges Postgres' derived tables every morning with a
-- tolerance of zero: ClickHouse re-derives Gold from a round-tripped Silver
-- and the two engines' Gold are set against each other — the one independent
-- re-aggregation left — beside the Postgres-alone checks of the same tables
-- (`gold_rollup_mismatch`, `customer_profile_mismatch`, the UTM verdicts'
-- completeness). Zero findings on the derived tables, every day.
--
-- Until step 14's S14-12 the run also set Postgres' Silver, Gold and UTM
-- against DuckDB's (`reconcile_silver`, `reconcile_order_utm`,
-- `reconcile_gold`, standing down since step 13) and filed their names here:
-- `mirror_buckets_disagree`, `gold_missing_cells`, `gold_orphan_cells`.
-- S14-12 deleted those comparisons, so D8 reads ClickHouse's comparison and
-- the Postgres-alone checks only.
--
-- THE RUN JUDGED
-- Today's 07:30 run from 07:45 Kyiv, yesterday's before. The DQ journal is
-- written in Postgres by chain 9, unconditionally since step 14's S14-10, so
-- what is read is the journal itself. Until step 14's S14-20 this check also
-- knew it as an hourly copy out of DuckDB (`replicate_operational`), and was
-- UNKNOWN on a copy 75 min old or more, failing, or taken before the run could
-- be in it; nothing has copied the journal since S14-13.
--
-- WHAT A FAIL MEANS
-- - no mirror_landing run since that 07:30: the job did not fire (a deploy on
--   the cron instant, or the scheduler down); the catch-up only runs at start.
-- - the run carries `error_message`: it wrote a row and checked nothing.
-- - findings on the derived tables: Postgres' and ClickHouse's Gold disagree,
--   or a Postgres-alone check of them failed.
--   `app.data_quality_issues.description` names the columns.
--
-- A VERDICT STILL INSIDE ITS GRACE IS NOT A DISAGREEMENT EITHER
-- `pg_order_utm_in_flight` (INFO, on silver.order_utm) counts orders written
-- in the last 20 minutes whose UTM verdict has not landed yet, and its own
-- description says "Not a defect yet". Since KS_UTM_PARSE=postgres (30.09) the
-- 07:30 run files one nearly every morning, so counting it made D8 FAIL daily
-- on a state that is the design. It is excluded from the findings at INFO
-- only, and the PASS says how many there were; the "UTM gap" check judges
-- the same verdicts past their grace, and `pg_order_utm_missing` / `_stale`,
-- the CRITICALs past it, still FAIL here. A test holds the list to checks
-- core files at INFO and nothing else.
--
-- A COMPARISON THAT COULD NOT LOOK IS UNKNOWN, NOT A DISAGREEMENT
-- ClickHouse's Gold comparison files on these tables too, and three of its
-- findings say only that it did not compare: `gold_values_unwatched` (OD-08
-- (a): KS_CH_URL unset, a failed ship or read-back, two empty Golds) and the
-- two WARNs about ClickHouse itself beside it. Counted as findings they read
-- "the two engines' Gold disagree" about a run in which one engine
-- saw nothing. So with nothing else found they make the run UNKNOWN; any real
-- finding beside them is still a FAIL, since a disagreement outranks a blind
-- spot. A test holds the list to every WARN `reconcile_clickhouse` files.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
slot AS (
    -- The 07:30 run being judged: today's from 07:45, else yesterday's.
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
blind_checks AS (
    SELECT unnest(ARRAY['gold_values_unwatched', 'ch_silver_sync_failed',
                        'ch_silver_unreachable']) AS check_name
),
derived AS (
    SELECT i.run_id, i.check_name, i.table_name, i.severity, i.count
    FROM app.data_quality_issues i
    JOIN runs USING (run_id)
    WHERE i.table_name IN ('silver.orders', 'gold.daily_revenue',
                           'app.customer_profile', 'silver.order_utm')
       OR i.check_name IN ('gold_rollup_mismatch', 'customer_profile_mismatch')
),
grace_checks AS (
    SELECT unnest(ARRAY['pg_order_utm_in_flight']) AS check_name
),
graced AS (
    SELECT d.* FROM derived d
    WHERE d.check_name IN (SELECT check_name FROM grace_checks)
      AND d.severity = 'INFO'
),
findings AS (
    SELECT d.* FROM derived d
    WHERE d.check_name NOT IN (SELECT check_name FROM blind_checks)
      AND NOT (d.check_name IN (SELECT check_name FROM grace_checks)
               AND d.severity = 'INFO')
),
blind AS (
    SELECT d.* FROM derived d
    WHERE d.check_name IN (SELECT check_name FROM blind_checks)
),
agg AS (
    SELECT (SELECT count(*) FROM runs) AS n_runs,
           (SELECT count(*) FROM runs WHERE error_message IS NOT NULL) AS errored,
           (SELECT string_agg(format('run %s: %s', run_id, error_message), '; ')
              FROM runs WHERE error_message IS NOT NULL) AS errors,
           (SELECT count(*) FROM findings) AS n_findings,
           (SELECT string_agg(format('%s %s on %s (%s)', check_name, severity, table_name, count),
                              ', ' ORDER BY check_name, table_name)
              FROM findings) AS listed,
           (SELECT count(*) FROM blind) AS n_blind,
           (SELECT string_agg(format('%s %s', check_name, severity), ', '
                              ORDER BY check_name)
              FROM blind) AS blind_listed,
           (SELECT COALESCE(sum(count), 0) FROM graced) AS n_graced
)
SELECT 'D8 mirror_landing (derived)'::text AS "check",
       CASE
           WHEN agg.n_runs = 0 THEN 'FAIL'
           WHEN agg.errored > 0 OR agg.n_findings > 0 THEN 'FAIL'
           WHEN agg.n_blind > 0 THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN agg.n_runs = 0 THEN
               format('no mirror_landing run since %s Kyiv',
                      to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           WHEN agg.errored > 0 THEN left(agg.errors, 400)
           WHEN agg.n_findings > 0 THEN left(format('%s finding(s) on the derived tables: %s',
                                                    agg.n_findings, agg.listed), 400)
           WHEN agg.n_blind > 0 THEN left(format('ClickHouse did not compare Gold (%s): the Postgres-alone checks found nothing, and nothing re-aggregated Gold in a second engine; re-run after the next dq_mirror_landing',
                                                 agg.blind_listed), 400)
           ELSE format('%s run(s) since %s Kyiv, no error, zero findings on the derived tables',
                       agg.n_runs, to_char(slot.starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                || CASE WHEN agg.n_graced > 0
                        THEN format('; %s UTM verdict(s) in flight inside their grace (the UTM gap check judges them)',
                                    agg.n_graced)
                        ELSE '' END
       END AS detail
FROM slot CROSS JOIN agg;
