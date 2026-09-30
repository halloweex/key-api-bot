-- F1 — reads answered from DuckDB in the last 168 h, and whether anyone looked.
--
-- WHY IT EXISTS
-- KS_READ_FALLBACK=off may be set only after a covered, clean week with no read
-- answered from DuckDB (owner decision OD-07 (a)). The plan measured it by
-- grepping web's log, but every deploy recreates web and its log goes with it,
-- so a week that spans a deploy could not be called covered at all. The
-- evidence is now made by the canary (bot/canary.py, every 15 min), which
-- outlives web's process, and kept in Postgres:
--
-- - THE PAGE. The canary pages `read_fallback_used` (WARN) whenever
--   /api/health's `read_fallbacks` is non-empty, and the Alert Gate journals
--   the delivered page in app.alert_series and app.alert_events. The surfaces
--   are read back out of the page's line.
-- - THE WATCH. Every probe that read the block rewrites one row,
--   `watch:read_fallbacks` in app.alert_series (core/alert_archive.py): its
--   first_fired_at is the moment since which every probe, none more than
--   35 min after the one before, read the block empty; last_fired_at is the
--   latest probe; fired_count the clean probes since, 0 when the latest saw a
--   fallback. A page proves a fallback; only the watch proves a quiet week was
--   looked at.
--
-- No freshness precondition: both are written to Postgres directly, not copied
-- out of DuckDB (D9's reason).
--
-- WHAT A FAIL MEANS
-- `read_fallback_used` is firing, was paged or escalated inside the window,
-- was resolved inside it, or the watch's latest probe read a fallback (which
-- counts even when the page was not delivered, and so never journaled). Each
-- one is a read some page answered from DuckDB; it is explained and fixed
-- before OD-07, and the week starts again.
--
-- WHAT AN UNKNOWN MEANS
-- Nothing durable says the week was watched: no watch row (a bot older than
-- this build, or one without KS_PG_DSN), a watch not written for over 35 min
-- (the bot is down, or web is not answering it), or a watch clean for less than
-- the window. Re-run once it has covered 168 h.
--
-- WHAT IT CANNOT SEE
-- The counters live in web's process. A fallback in the last minutes of a web
-- process, after the canary's last probe of it and before a recreate, goes with
-- the process: at most one or two probe intervals per recreate, which is what
-- the 35-minute gap allows. A refusal under `off` is not a fallback and is not
-- counted here (the canary pages it as `read_refused`).
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
limits AS (
    SELECT interval '168 hours' AS span,
           interval '35 minutes' AS watch_gap
),
win AS (
    SELECT clock.now - limits.span AS starts, clock.now, limits.span, limits.watch_gap
    FROM clock CROSS JOIN limits
),
series AS (
    SELECT s.state, s.last_fired_at, s.resolved_at
    FROM app.alert_series s
    WHERE s.condition_key = 'read_fallback_used'
),
paged AS (
    -- A page's body rides the first condition's row of its raise; the others
    -- share its instant and instance, so the line is looked for across them.
    SELECT e.at, e.event_type,
           (SELECT substring(m.message FROM 'reads served from DuckDB: ([^\n]*)')
              FROM app.alert_events m
             WHERE m.at = e.at AND m.instance = e.instance
               AND m.event_type = 'fired' AND m.message IS NOT NULL
             ORDER BY m.id
             LIMIT 1) AS surfaces
    FROM app.alert_events e, win
    WHERE e.condition_key = 'read_fallback_used'
      AND e.event_type IN ('fired', 'escalated')
      AND e.at > win.starts
      AND e.at <= win.now
),
watch AS (
    SELECT w.first_fired_at AS clean_since, w.last_fired_at AS last_probe,
           w.fired_count AS clean_probes
    FROM app.alert_series w
    WHERE w.condition_key = 'watch:read_fallbacks'
),
judged AS (
    SELECT win.starts, win.now AS clock_now, win.span, win.watch_gap,
           (SELECT count(*) FROM paged) AS n_paged,
           (SELECT max(at) FROM paged) AS last_paged,
           (SELECT surfaces FROM paged WHERE surfaces IS NOT NULL
             ORDER BY at DESC LIMIT 1) AS surfaces,
           (SELECT state = 'firing' FROM series) AS firing,
           (SELECT last_fired_at FROM series) AS series_last_fired,
           (SELECT resolved_at FROM series) AS resolved_at,
           watch.clean_since, watch.last_probe, watch.clean_probes,
           watch.last_probe IS NOT NULL AS watched
    FROM win
    LEFT JOIN watch ON true
),
verdicts AS (
    SELECT j.*,
           COALESCE(j.firing, false) AS fail_firing,
           j.n_paged > 0 AS fail_paged,
           COALESCE(j.resolved_at > j.starts AND j.resolved_at <= j.clock_now, false)
               AS fail_resolved,
           COALESCE(j.clean_probes = 0 AND j.last_probe > j.starts, false) AS fail_watch,
           j.watched AND j.clock_now - j.last_probe > j.watch_gap AS watch_stale,
           j.watched AND j.clean_since > j.starts AS watch_short
    FROM judged j
)
SELECT 'F1 read fallbacks'::text AS "check",
       CASE
           WHEN fail_firing OR fail_paged OR fail_resolved OR fail_watch THEN 'FAIL'
           WHEN NOT watched OR watch_stale OR watch_short THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN fail_firing OR fail_paged OR fail_resolved OR fail_watch THEN left(
               concat_ws('; ',
                   CASE WHEN fail_firing THEN
                       format('read_fallback_used is still firing (last paged %s Kyiv)',
                              to_char(series_last_fired AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN fail_paged THEN
                       format('paged %s time(s) in %s h, last %s Kyiv%s', n_paged,
                              floor(extract(epoch FROM span) / 3600),
                              to_char(last_paged AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                              COALESCE(': ' || surfaces, ''))
                   END,
                   CASE WHEN fail_resolved AND NOT fail_paged THEN
                       format('read_fallback_used stood until %s Kyiv, inside the window',
                              to_char(resolved_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN fail_watch THEN
                       format('the canary''s probe at %s Kyiv read a fallback',
                              to_char(last_probe AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END)
               || '; explain each in web''s log before KS_READ_FALLBACK=off', 500)
           WHEN NOT watched THEN
               'no watch:read_fallbacks row: nothing durable says the canary read '
               || 'read_fallbacks (a bot older than this build, or one without KS_PG_DSN)'
           WHEN watch_stale THEN
               format('the canary last read read_fallbacks %s min ago (limit %s): the bot, '
                      || 'or web behind it, is not answering',
                      floor(extract(epoch FROM clock_now - last_probe) / 60),
                      floor(extract(epoch FROM watch_gap) / 60))
           WHEN watch_short THEN
               format('clean and watched only since %s Kyiv, %s h of %s; re-run once the '
                      || 'watch covers the window',
                      to_char(clean_since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      round(extract(epoch FROM clock_now - clean_since) / 3600, 1),
                      floor(extract(epoch FROM span) / 3600))
           ELSE
               format('no read fallback paged in %s h; the canary read read_fallbacks empty '
                      || 'every <= %s min since %s Kyiv (%s probes)',
                      floor(extract(epoch FROM span) / 3600),
                      floor(extract(epoch FROM watch_gap) / 60),
                      to_char(clean_since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      clean_probes)
       END AS detail
FROM verdicts;
