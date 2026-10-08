-- P3 — the 14-day parallel period: since when it has run clean (OD-17 (a),
-- amended by the owner on 2026-10-08 from 30 days).
--
-- WHY IT EXISTS
-- Stage 5 waits for a 14-day parallel period counted from the last write
-- flag, restarted by any breach (owner decision OD-17 (a)). Two of OD-17's
-- four breaches can be measured while DuckDB still runs beside Postgres: a
-- rollback lever used (P2's evidence) and a read served from DuckDB (F1's).
-- The other two — web opening DuckDB and the file changing — cannot: until
-- stage 5 decouples the code web opens the file on every boot and writes it
-- all day. They are the week of silence's (P4), which runs with KS_DUCKDB=off.
--
-- WHERE THE CLOCK STARTS
-- The latest of:
-- - SOAK_PARALLEL_FROM, the moment the last KS_WRITE_* flag flipped, declared
--   by the operator — `parallel_from`. Nothing in the database can say which
--   flip was the last one the stage needs; a person can;
-- - the newest `owner:` row in meta.chain_watermarks: a chain that latched
--   after the declared moment moved a write flag later;
-- - step 13's switch, `since` of the warehouse_writer record when it says
--   postgres;
-- - every breach: a lever recorded, a lever page fired, escalated or resolved
--   (P2), step 13 given back to DuckDB, a read-fallback page fired, escalated
--   or resolved (F1);
-- - the F1 watch's clean-since: the moment since which every probe read no
--   read served from DuckDB and no web process went unread for over 35 min.
--   A stretch nobody watched is not a clean one.
--
-- A DAY, AND THE PERIOD IN THE DETAIL
-- The verdict is the daily question, as everywhere in this report. FAIL: a
-- breach inside the day, a lever or fallback page still standing, the F1
-- watch's latest probe inside the day found a fallback, or step 13 is on
-- DuckDB. UNKNOWN: no F1 watch, a watch not written for 35 min, or one clean
-- for less than the day. PASS: otherwise, with how many of the 720 h are
-- behind it and what started the clock; "covered" is the period done.
-- Undeclared, the check is not applicable.
--
-- WHAT IT CANNOT SEE
-- Whatever P2 and F1 cannot. And it is not the stage-5 gate by itself: the
-- other preconditions (every chain latched, source reconciliation clean,
-- KS_READ_FALLBACK=off, the Ark) are their own checks.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
limits AS (
    SELECT interval '24 hours' AS span,
           interval '336 hours' AS period,
           interval '35 minutes' AS watch_gap
),
win AS (
    SELECT clock.now - limits.span AS starts, clock.now, limits.span,
           limits.period, limits.watch_gap
    FROM clock CROSS JOIN limits
),
declared AS (
    SELECT NULLIF(:'parallel_from', '')::timestamptz AS from_at
),
breach_keys AS (
    SELECT v.condition_key, v.kind
    FROM (VALUES ('write_chain_flag_mismatch', 'a lever page'),
                 ('warehouse_hold_stuck', 'a lever page'),
                 ('warehouse_preconditions_unmet', 'a lever page'),
                 ('warehouse_way_back_refused', 'a lever page'),
                 ('read_fallback_used', 'a read served from DuckDB'),
                 ('read_routed_to_duckdb', 'a read served from DuckDB'))
         AS v (condition_key, kind)
),
writer AS (
    SELECT substring(m.value FROM '"writer"\s*:\s*"([a-z]+)"') AS writer,
           CASE WHEN m.value ~ '"since"\s*:\s*"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})"'
                THEN substring(m.value FROM '"since"\s*:\s*"([^"]+)"')::timestamptz
           END AS since
    FROM app.sync_metadata m
    WHERE m.key = 'warehouse_writer'
),
breaches AS (
    SELECT e.at,
           concat_ws(' ', 'a rollback lever:', substr(e.condition_key, 7),
                     e.context->>'subject', e.context->>'outcome') AS what
    FROM app.alert_events e
    WHERE left(e.condition_key, 6) = 'lever:' AND e.event_type = 'lever_used'
    UNION ALL
    SELECT e.at, format('%s (%s %s)', k.kind, e.condition_key, e.event_type)
    FROM app.alert_events e JOIN breach_keys k USING (condition_key)
    WHERE e.event_type IN ('fired', 'escalated')
    UNION ALL
    SELECT s.resolved_at, format('%s (%s, resolved)', k.kind, s.condition_key)
    FROM app.alert_series s JOIN breach_keys k USING (condition_key)
    WHERE s.resolved_at IS NOT NULL
    UNION ALL
    SELECT w.since, 'step 13 given back to DuckDB' FROM writer w
    WHERE w.writer = 'duckdb' AND w.since IS NOT NULL
),
standing AS (
    SELECT string_agg(s.condition_key, ', ' ORDER BY s.condition_key) AS keys
    FROM app.alert_series s JOIN breach_keys k USING (condition_key)
    WHERE s.state = 'firing'
),
watch AS (
    SELECT w.first_fired_at AS clean_since, w.last_fired_at AS last_probe,
           w.fired_count AS clean_probes
    FROM app.alert_series w
    WHERE w.condition_key = 'watch:read_fallbacks'
),
starts AS (
    -- `rank` breaks a tie toward the breach: a breach and a watch restart at
    -- one instant are one event, and the breach is what to name.
    SELECT b.at, b.what, 0 AS rank FROM breaches b, win WHERE b.at <= win.now
    UNION ALL
    SELECT d.from_at, 'the declared last write flag', 1 FROM declared d
    UNION ALL
    SELECT max(c.updated_at), 'a chain latched', 1 FROM meta.chain_watermarks c
    WHERE left(c.key, 6) = 'owner:'
    UNION ALL
    SELECT w.since, 'step 13 switched to Postgres', 1 FROM writer w
    WHERE w.writer = 'postgres'
    UNION ALL
    SELECT watch.clean_since, 'reads watched clean since', 1 FROM watch
),
judged AS (
    SELECT win.starts, win.now AS clock_now, win.span, win.period, win.watch_gap,
           declared.from_at,
           (SELECT s.at FROM starts s WHERE s.at IS NOT NULL
             ORDER BY s.at DESC, s.rank LIMIT 1) AS start_at,
           (SELECT s.what FROM starts s WHERE s.at IS NOT NULL
             ORDER BY s.at DESC, s.rank LIMIT 1) AS start_what,
           (SELECT count(*) FROM breaches b
             WHERE b.at > win.starts AND b.at <= win.now) AS n_breaches,
           (SELECT b.what FROM breaches b WHERE b.at > win.starts AND b.at <= win.now
             ORDER BY b.at DESC LIMIT 1) AS last_breach,
           (SELECT max(b.at) FROM breaches b
             WHERE b.at > win.starts AND b.at <= win.now) AS last_breach_at,
           standing.keys AS standing_keys,
           (SELECT w.writer FROM writer w) AS writer,
           watch.clean_since, watch.last_probe, watch.clean_probes,
           watch.last_probe IS NOT NULL AS watched
    FROM win
    CROSS JOIN declared
    CROSS JOIN standing
    LEFT JOIN watch ON true
),
verdicts AS (
    SELECT j.*,
           j.from_at IS NULL AS undeclared,
           j.n_breaches > 0 AS fail_breach,
           j.standing_keys IS NOT NULL AS fail_standing,
           COALESCE(j.writer = 'duckdb', false) AS fail_writer,
           COALESCE(j.clean_probes = 0 AND j.last_probe > j.starts, false) AS fail_watch,
           j.watched AND j.clock_now - j.last_probe > j.watch_gap AS watch_stale,
           j.watched AND j.clean_since > j.starts AS watch_short,
           j.clock_now - j.start_at >= j.period AS covered
    FROM judged j
)
SELECT 'P3 parallel period'::text AS "check",
       CASE
           WHEN undeclared THEN 'PASS'
           WHEN fail_breach OR fail_standing OR fail_writer OR fail_watch THEN 'FAIL'
           WHEN NOT watched OR watch_stale OR watch_short THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN undeclared THEN
               'not applicable: no parallel period declared — set SOAK_PARALLEL_FROM to '
               || 'the moment the last KS_WRITE_* flag flipped'
           WHEN fail_breach OR fail_standing OR fail_writer OR fail_watch THEN left(
               concat_ws('; ',
                   CASE WHEN fail_breach THEN
                       format('%s breach(es) in %s h, last %s at %s Kyiv', n_breaches,
                              floor(extract(epoch FROM span) / 3600), last_breach,
                              to_char(last_breach_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN fail_standing THEN format('still paging: %s', standing_keys) END,
                   CASE WHEN fail_writer THEN 'step 13 is on DuckDB: DuckDB derives the warehouse' END,
                   CASE WHEN fail_watch THEN
                       format('the canary''s probe at %s Kyiv found a read served from DuckDB',
                              to_char(last_probe AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END)
               || '; the 14 days start again (OD-17 (a))', 500)
           WHEN NOT watched THEN
               'no watch:read_fallbacks row: nothing durable says reads were watched, so no '
               || 'day of the period can be counted'
           WHEN watch_stale THEN
               format('the canary last read read_fallbacks %s min ago (limit %s): the bot, '
                      || 'or web behind it, is not answering',
                      floor(extract(epoch FROM clock_now - last_probe) / 60),
                      floor(extract(epoch FROM watch_gap) / 60))
           WHEN watch_short THEN
               format('reads watched clean only since %s Kyiv, %s h of the %s; re-run once '
                      || 'the watch covers the day',
                      to_char(clean_since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      round(extract(epoch FROM clock_now - clean_since) / 3600, 1),
                      floor(extract(epoch FROM span) / 3600))
           ELSE
               format('clean since %s Kyiv (%s): %s',
                      to_char(start_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'), start_what,
                      CASE WHEN covered THEN
                          format('the %s days of the parallel period are covered',
                                 floor(extract(epoch FROM period) / 86400))
                      ELSE
                          format('%s d %s h of the %s days',
                                 floor(extract(epoch FROM clock_now - start_at) / 86400),
                                 floor(mod(extract(epoch FROM clock_now - start_at)::numeric, 86400)
                                       / 3600),
                                 floor(extract(epoch FROM period) / 86400))
                      END)
       END AS detail
FROM verdicts;
