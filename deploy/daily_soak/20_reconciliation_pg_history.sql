-- reconciliation_pg — would the canary have stayed quiet? (precondition for DN-21)
--
-- WHY IT EXISTS
-- Reconciliation A proves Postgres agrees with DuckDB; `reconciliation_pg`
-- proves it agrees with KeyCRM, the half meant to stay true once DuckDB is
-- gone. DN-21 puts it under the canary, and a detector that pages from its
-- first day on a history nobody looked at would be switched off by the second.
-- So the history is read first, over a window from midnight Kyiv fourteen days
-- ago up to now.
--
-- THREE QUESTIONS, BECAUSE NONE OF THEM IMPLIES ANOTHER
-- 1. A successful run on each of the 14 Kyiv days before today.
-- 2. No silence longer than the canary allows. The canary counts no days: it
--    pages when the newest successful run is more than `canary_max_age` old
--    (bot/canary.py, DQ_MAX_AGE_S["reconciliation_pg"]; a test holds the two
--    equal). So every silence that ends inside the window is measured — from
--    one success's `started_at`, which is what /api/health's age counts from,
--    to the `ended_at` of the next, the moment `persist_run` made it visible —
--    and so is the silence still running, from the newest success to now.
--    Runs at 00:10 and at 23:50 the next day cover both days and leave 47.7 h
--    unwatched; runs 25 h apart can straddle a day that has none. Hence 1 and
--    2 both. The first silence may have begun before the window, and is
--    measured whole: the canary would have been paging when the window opened.
-- 3. No CRITICAL on any run in the window, failed or not.
-- "Successful" is `error_message IS NULL`. `persist_run` writes status FAILED
-- exactly when it sets `error_message`, so this is the set /api/health takes
-- its age over, and it is the only kind of run the detail counts: a failed
-- run writes a row too, and checked nothing.
--
-- THE JOURNAL IS READ, NOT A COPY OF IT
-- Chain 9 writes the DQ journal in Postgres since its flip, unconditionally
-- since step 14's S14-10, so these rows are as current as the read and a
-- silence past the limit now is a FAIL. Until step 14's S14-20 this check
-- also knew the journal as an hourly copy out of DuckDB (`replicate_operational`)
-- and was UNKNOWN on a copy 75 min old or more, or one taken before a run could
-- be in it; nothing has copied the journal since S14-13.
--
-- WHAT A FAIL MEANS
-- A day with no successful run or a silence past the limit (the 05:30 job did
-- not run, or every run errored — CLAUDE.md's Sunday 05:00 trap is the known
-- cause), or a CRITICAL finding. Any of them restarts the 14-day count for
-- DN-21, and once DN-21 is live, a silence past the limit is a page that went
-- out. Today needs no run of its own, only a silence within the limit.
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
    -- label, the one place this file and 22_reconciliation_ch_history.sql
    -- differ; a test holds the rest of the two identical.
    SELECT r.run_id, r.started_at, r.ended_at, r.critical_count,
           r.error_message IS NULL AS ok
    FROM app.data_quality_runs r
    WHERE r.layer = 'reconciliation_pg'
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
SELECT 'reconciliation_pg history'::text AS "check",
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
