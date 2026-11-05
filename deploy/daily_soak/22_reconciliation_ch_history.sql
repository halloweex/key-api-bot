-- reconciliation_ch — would the canary have stayed quiet? (OD-08 (a))
--
-- WHY IT EXISTS
-- OD-08 (a) (2026-09-30) put `reconciliation_ch`, the ClickHouse arm of the
-- 05:30 comparison against KeyCRM, under the canary at 30 h. DN-21 set the
-- bar for doing that to `reconciliation_pg`: a successful run on each of the
-- last 14 days and no silence past the limit, read from the history rather
-- than from one /api/health — a detector that pages from its first day on a
-- history nobody looked at would be switched off by the second. The same bar
-- applies here, and the one snapshot of 30.09 did not meet it: the ClickHouse
-- arm fails on its own (a ClickHouse query or the Postgres exclusion set
-- raising while the copy is fresh), and how often it did is what this reads.
-- Run it on the host before the change that pages on the layer is merged.
-- Until then the host's checkout does not have this file: feed it from a
-- checkout on stdin to the same psql `deploy/daily_soak.sh` runs, as
-- `ks_readonly` with `default_transaction_read_only=on`.
--
-- THE SAME QUESTIONS AS 20, ONE DIFFERENCE IN WHAT COUNTS AS A SUCCESS
-- Everything but `layer_runs` and the label is
-- 20_reconciliation_pg_history.sql, held identical by a test, so the three
-- questions and the silence arithmetic are the ones documented there. What differs
-- is `ok`: the arm gates a ClickHouse copy more than 3 h old instead of
-- comparing it, and until the OD-08 review wrote that run as a success —
-- error NULL, a WARN `ch_reconcile_pending` beside it. The canary no longer
-- counts such a run (it is written with `error_message` now), so neither does
-- this: the history is judged as the canary that ships with it would judge it.
--
-- THE JOURNAL IS READ, NOT A COPY OF IT
-- As in 20: chain 9 writes the journal in Postgres, so a silence past the
-- limit now is a FAIL (until step 14's S14-20 a stale hourly copy of it made
-- this UNKNOWN).
--
-- WHAT A FAIL MEANS
-- A day with no run that compared, a silence past the limit, or a CRITICAL
-- finding (a discrepancy between ClickHouse's Silver and KeyCRM). Before the
-- canary watches this layer, a FAIL is a page it would have sent; after, it
-- is a page that went out.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
limits AS (
    SELECT interval '30 hours' AS canary_max_age
),
today AS (
    SELECT (clock.now AT TIME ZONE 'Europe/Kyiv')::date AS kday FROM clock
),
span AS (
    SELECT (today.kday - 14)::timestamp AT TIME ZONE 'Europe/Kyiv' AS starts FROM today
),
wanted AS (
    SELECT today.kday - k AS kday FROM today, generate_series(1, 14) AS k
),
layer_runs AS (
    -- Every run of the layer, and whether it produced a verdict. With the
    -- label, the one place this file and 20_reconciliation_pg_history.sql
    -- differ; a test holds the rest of the two identical. A run that gated a stale copy
    -- compared nothing, whether it was written before the review (error
    -- NULL, a `ch_reconcile_pending` beside it) or after (error set).
    SELECT r.run_id, r.started_at, r.ended_at, r.critical_count,
           r.error_message IS NULL
           AND NOT EXISTS (SELECT 1 FROM app.data_quality_issues i
                           WHERE i.run_id = r.run_id
                             AND i.check_name = 'ch_reconcile_pending') AS ok
    FROM app.data_quality_runs r
    WHERE r.layer = 'reconciliation_ch'
),
runs AS (
    SELECT (r.started_at AT TIME ZONE 'Europe/Kyiv')::date AS kday,
           r.ok, r.critical_count
    FROM layer_runs r, span, clock
    WHERE r.started_at >= span.starts
      AND r.started_at <= clock.now
),
per_day AS (
    SELECT w.kday, count(r.kday) FILTER (WHERE r.ok) AS ok
    FROM wanted w
    LEFT JOIN runs r ON r.kday = w.kday
    GROUP BY w.kday
),
successes AS (
    -- Not bounded by the window: the success before it opens the first silence.
    SELECT r.started_at, COALESCE(r.ended_at, r.started_at) AS visible_at
    FROM layer_runs r, clock
    WHERE r.ok
      AND r.started_at <= clock.now
),
silences AS (
    -- A layer with no success before the window was silent from its start.
    SELECT COALESCE(lag(s.started_at) OVER (ORDER BY s.started_at), span.starts) AS since,
           s.visible_at AS until
    FROM successes s, span
),
longest AS (
    SELECT si.since, si.until, si.until - si.since AS length
    FROM silences si, span
    WHERE si.until >= span.starts
    ORDER BY si.until - si.since DESC, si.until
    LIMIT 1
),
newest AS (
    SELECT (SELECT max(started_at) FROM successes) AS at
),
agg AS (
    SELECT (SELECT string_agg(to_char(kday, 'DD.MM'), ', ' ORDER BY kday)
              FROM per_day WHERE ok = 0) AS days_without_ok,
           (SELECT COALESCE(max(critical_count), 0) FROM runs) AS max_critical,
           (SELECT string_agg(DISTINCT to_char(kday, 'DD.MM'), ', ')
              FROM runs WHERE critical_count > 0) AS critical_days,
           (SELECT count(*) FROM runs WHERE ok) AS n_ok
),
judged AS (
    SELECT agg.*, clock.now AS clock_now, span.starts,
           limits.canary_max_age AS lim, newest.at AS newest_at,
           longest.since AS longest_since, longest.until AS longest_until,
           longest.length AS longest_length,
           longest.length > limits.canary_max_age AS silence_too_long,
           -- With no success at all, the silence still running began, for
           -- this window, when the window did.
           clock.now - COALESCE(newest.at, span.starts)
               > limits.canary_max_age AS silent_now
    FROM agg CROSS JOIN clock CROSS JOIN span
         CROSS JOIN limits CROSS JOIN newest
         LEFT JOIN longest ON true
)
SELECT 'reconciliation_ch history'::text AS "check",
       CASE
           WHEN days_without_ok IS NOT NULL OR max_critical > 0
                OR silence_too_long OR silent_now THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       format('%s successful run(s) since %s Kyiv; %s, %s (the canary pages past %s h)%s%s%s%s',
              n_ok,
              to_char(starts AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
              COALESCE('the longest silence '
                       || round(extract(epoch FROM longest_length) / 3600, 1) || ' h',
                       'no silence ended in the window'),
              COALESCE('the last success '
                       || round(extract(epoch FROM clock_now - newest_at) / 3600, 1) || ' h ago',
                       'no successful run on record'),
              floor(extract(epoch FROM lim) / 3600),
              COALESCE('; no successful run on ' || days_without_ok, ''),
              COALESCE('; CRITICAL on ' || critical_days, ''),
              CASE WHEN silence_too_long THEN
                  format('; silent for %s h, from %s to %s Kyiv',
                         round(extract(epoch FROM longest_length) / 3600, 1),
                         to_char(longest_since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                         to_char(longest_until AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
              ELSE '' END,
              CASE WHEN silent_now AND newest_at IS NOT NULL THEN
                  format('; no success since %s Kyiv, %s h ago',
                         to_char(newest_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                         round(extract(epoch FROM clock_now - newest_at) / 3600, 1))
              ELSE '' END) AS detail
FROM judged;
