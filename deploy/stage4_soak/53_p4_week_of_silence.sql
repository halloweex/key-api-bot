-- P4 — the week of silence: since when nothing has touched DuckDB (OD-17 (a)).
--
-- WHY IT EXISTS
-- Stage 5 may begin only after seven consecutive days with zero hits on all
-- four of OD-17's breaches — web opens DuckDB, the file hash changes, a
-- rollback lever is used, a fallback is served — including a Sunday and a
-- Monday report, any breach restarting the count. The week runs with web's
-- KS_DUCKDB=off; under `on` (today) the check is not applicable.
--
-- THE EVIDENCE
-- - web's opens: the canary pages `duckdb_opened_while_off` (CRITICAL) while
--   web's `/api/health` names any site that reached for the file under `off`
--   (core/duckdb_switch.py), and rewrites one `watch:duckdb_switch` row on
--   every probe that read the block under `off` — its first_fired_at is the
--   moment since which every probe found nothing opened and no web process
--   went unread for over 35 min (the read-fallback watch's rule);
-- - the file: P1's record of the hourly hash, as psql variables — a change,
--   and the last check that found the file missing, which the record keeps
--   after the file comes back;
-- - the host-cron sidecars: the weekly compaction and the nightly off-site
--   start theirs from `.env` (`docker run --env-file`), not from web's
--   environment, and their phase 1 opens the live file read-only — no byte
--   changes, so the hash cannot see it, and only the switch inside the
--   sidecar refuses it. So `.env` must say KS_DUCKDB=off too, read as docker
--   reads it (`duckdb_off_env`), and since when: its mtime
--   (`duckdb_env_changed_at`), because nothing says the sidecars read `off`
--   before the file's last edit;
-- - the levers: P2's journal rows and pages, every one of them, since off;
-- - the fallbacks: F1's pages and watch.
--
-- WHERE THE CLOCK STARTS
-- The latest of the tripwire watch's clean-since (which is no earlier than
-- the first probe under `off`), the file's unchanged-since, the last edit of
-- `.env`, every breach above and the F1 watch's clean-since — so any edit of
-- `.env` restarts the week. 168 h is a full week, so it holds every weekly
-- instant: Sunday 05:00 Kyiv, when the compaction would run — and is refused
-- by the switch before it opens the file, because `.env` says off since
-- before the start — and Monday's reports. A week that holds them by the
-- clock but in which no weekly report was delivered has not shown the Monday
-- path works without DuckDB, so "covered" also needs a
-- `app.weekly_report_sends.sent_at` after the start.
--
-- WHAT EACH VERDICT MEANS
-- FAIL: KS_DUCKDB set to a value web does not understand (web runs `on`, the
-- week is not running); web off while `.env` does not say off, or says it in
-- a way the switch does not understand (the sidecars would run on); a breach
-- inside the day; a tripwire, lever or fallback page still standing; the
-- latest probe of either watch inside the day found something; the file
-- changed, is missing, or was found missing inside the day (it came back, but
-- nothing vouched for it while it was gone); step 13 on DuckDB.
-- UNKNOWN: web down; `.env` unreadable; either watch missing, not written for
-- 35 min, or clean for less than the day; no file record, one over 3 h old, or
-- one that began inside the day. PASS: otherwise, with how many of the 168 h
-- are behind it.
--
-- WHAT IT CANNOT SEE
-- An open in a web process after the canary's last probe of it and before it
-- was replaced (the watch's unread tail, at most 35 min before it restarts);
-- a web started with `on` for less than 35 min between two `off` processes,
-- which the hourly hash is there to catch — web under `on` writes the file at
-- boot; and anything P1 and P2 cannot see.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
limits AS (
    SELECT interval '24 hours' AS span,
           interval '168 hours' AS week,
           interval '35 minutes' AS watch_gap,
           interval '3 hours' AS record_max_age
),
win AS (
    SELECT clock.now - limits.span AS starts, clock.now, limits.span, limits.week,
           limits.watch_gap, limits.record_max_age
    FROM clock CROSS JOIN limits
),
rec AS (
    SELECT NULLIF(:'duckdb_off', '') AS duckdb_off,
           NULLIF(:'duckdb_file_last', '') AS file_last,
           NULLIF(:'duckdb_file_since_reason', '') AS file_since_reason,
           CASE WHEN :'duckdb_file_since' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_file_since')::text::timestamptz END AS file_since,
           CASE WHEN :'duckdb_file_checked_at' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_file_checked_at')::text::timestamptz END AS file_checked_at,
           CASE WHEN :'duckdb_file_missing_at' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_file_missing_at')::text::timestamptz END AS file_missing_at,
           NULLIF(:'duckdb_off_env', '') AS duckdb_off_env,
           CASE WHEN :'duckdb_env_changed_at' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_env_changed_at')::text::timestamptz END AS env_changed_at
),
breach_keys AS (
    SELECT v.condition_key, v.kind
    FROM (VALUES ('duckdb_opened_while_off', 'web opened DuckDB'),
                 ('write_chain_flag_mismatch', 'a lever page'),
                 ('warehouse_hold_stuck', 'a lever page'),
                 ('warehouse_preconditions_unmet', 'a lever page'),
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
    UNION ALL
    SELECT r.file_since, 'the DuckDB file changed' FROM rec r
    WHERE r.file_since_reason = 'change' AND r.file_since IS NOT NULL
    UNION ALL
    SELECT r.file_missing_at, 'the DuckDB file was missing' FROM rec r
    WHERE r.file_missing_at IS NOT NULL
),
standing AS (
    SELECT string_agg(s.condition_key, ', ' ORDER BY s.condition_key) AS keys
    FROM app.alert_series s JOIN breach_keys k USING (condition_key)
    WHERE s.state = 'firing'
),
watches AS (
    SELECT w.condition_key, w.first_fired_at AS clean_since,
           w.last_fired_at AS last_probe, w.fired_count AS clean_probes
    FROM app.alert_series w
    WHERE w.condition_key IN ('watch:duckdb_switch', 'watch:read_fallbacks')
),
report AS (
    SELECT max(r.sent_at) AS sent_at FROM app.weekly_report_sends r, win
    WHERE r.sent_at <= win.now
),
starts AS (
    -- `rank` breaks a tie toward the breach: a file change is also when its
    -- bytes were first seen, and the change is what to name.
    SELECT b.at, b.what, 0 AS rank FROM breaches b, win WHERE b.at <= win.now
    UNION ALL
    SELECT w.clean_since,
           CASE w.condition_key WHEN 'watch:duckdb_switch'
                THEN 'web watched under KS_DUCKDB=off, nothing opened, since'
                ELSE 'reads watched clean since' END,
           1
    FROM watches w
    UNION ALL
    SELECT r.file_since, 'the file''s bytes recorded unchanged since', 1 FROM rec r
    UNION ALL
    SELECT r.env_changed_at, '.env, which the host-cron sidecars read, last edited', 1
    FROM rec r
),
judged AS (
    SELECT win.starts, win.now AS clock_now, win.span, win.week, win.watch_gap,
           win.record_max_age, rec.*,
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
           sw.clean_since AS sw_since, sw.last_probe AS sw_probe, sw.clean_probes AS sw_clean,
           fw.clean_since AS fw_since, fw.last_probe AS fw_probe, fw.clean_probes AS fw_clean,
           report.sent_at AS report_at
    FROM win
    CROSS JOIN rec
    CROSS JOIN standing
    CROSS JOIN report
    LEFT JOIN watches sw ON sw.condition_key = 'watch:duckdb_switch'
    LEFT JOIN watches fw ON fw.condition_key = 'watch:read_fallbacks'
),
verdicts AS (
    SELECT j.*,
           j.duckdb_off = '1' AS applies,
           j.n_breaches > 0 AS fail_breach,
           j.standing_keys IS NOT NULL AS fail_standing,
           COALESCE(j.writer = 'duckdb', false) AS fail_writer,
           COALESCE(j.sw_clean = 0 AND j.sw_probe > j.starts, false)
               OR COALESCE(j.fw_clean = 0 AND j.fw_probe > j.starts, false) AS fail_probe,
           COALESCE(j.file_last IN ('CHANGED', 'MISSING'), false) AS fail_file,
           COALESCE(j.duckdb_off_env IN ('0', 'invalid'), false) AS fail_sidecars,
           j.duckdb_off_env IS NULL OR j.duckdb_off_env NOT IN ('0', '1', 'invalid')
               AS sidecars_unseen,
           j.sw_probe IS NULL OR j.fw_probe IS NULL AS unwatched,
           COALESCE(j.clock_now - j.sw_probe > j.watch_gap
                    OR j.clock_now - j.fw_probe > j.watch_gap, false) AS watch_stale,
           COALESCE(j.sw_since > j.starts OR j.fw_since > j.starts, false) AS watch_short,
           j.file_last IS NULL OR j.file_last NOT IN ('UNCHANGED', 'BASELINE')
               OR j.file_since IS NULL OR j.file_checked_at IS NULL
               OR j.clock_now - j.file_checked_at > j.record_max_age
               OR j.file_since > j.starts OR j.file_last = 'BASELINE' AS file_unseen,
           j.clock_now - j.start_at >= j.week AS week_clean,
           COALESCE(j.report_at > j.start_at, false) AS report_inside
    FROM judged j
)
SELECT 'P4 week of silence'::text AS "check",
       CASE
           WHEN duckdb_off = 'invalid' THEN 'FAIL'
           WHEN duckdb_off IS DISTINCT FROM '1' AND duckdb_off IS DISTINCT FROM '0' THEN 'UNKNOWN'
           WHEN NOT applies THEN 'PASS'
           WHEN fail_breach OR fail_standing OR fail_writer OR fail_probe OR fail_file
                OR fail_sidecars THEN 'FAIL'
           WHEN sidecars_unseen OR unwatched OR watch_stale OR watch_short OR file_unseen
               THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN duckdb_off = 'invalid' THEN
               'KS_DUCKDB is set to a value web does not understand: web runs on and the week '
               || 'of silence is not running (the canary warns duckdb_mode_invalid)'
           WHEN duckdb_off IS DISTINCT FROM '1' AND duckdb_off IS DISTINCT FROM '0' THEN
               'web is not running, so whether KS_DUCKDB is off cannot be read'
           WHEN NOT applies THEN
               'not applicable: KS_DUCKDB is on; the week starts when web runs off'
           WHEN fail_breach OR fail_standing OR fail_writer OR fail_probe OR fail_file
                OR fail_sidecars THEN left(
               concat_ws('; ',
                   CASE WHEN fail_breach THEN
                       format('%s breach(es) in %s h, last %s at %s Kyiv', n_breaches,
                              floor(extract(epoch FROM span) / 3600), last_breach,
                              to_char(last_breach_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN fail_standing THEN format('still paging: %s', standing_keys) END,
                   CASE WHEN fail_writer THEN 'step 13 is on DuckDB: DuckDB derives the warehouse' END,
                   CASE WHEN fail_probe THEN
                       'the canary''s latest probe found an open of DuckDB or a read served from it'
                   END,
                   CASE WHEN fail_file THEN
                       format('the file''s record says %s', file_last)
                   END,
                   CASE WHEN fail_sidecars THEN
                       CASE duckdb_off_env WHEN 'invalid'
                            THEN '.env sets KS_DUCKDB to a value the switch does not understand '
                                 || '(an env file passed to docker keeps its quotes)'
                            ELSE '.env does not set KS_DUCKDB=off' END
                       || ': the weekly compaction and the nightly off-site start their '
                       || 'sidecars from it and would open the file read-only, which neither '
                       || 'the switch nor the hash sees; put KS_DUCKDB=off in .env'
                   END)
               || '; the week starts again (OD-17 (a))', 500)
           WHEN sidecars_unseen THEN
               '.env could not be read: whether the host-cron sidecars (the compaction, the '
               || 'nightly off-site) are refused under off cannot be said'
           WHEN unwatched THEN
               'no ' || concat_ws(' or ',
                   CASE WHEN sw_probe IS NULL THEN 'watch:duckdb_switch' END,
                   CASE WHEN fw_probe IS NULL THEN 'watch:read_fallbacks' END)
               || ' row: nothing durable says the canary read web under KS_DUCKDB=off '
               || '(a bot older than this build, or one without KS_PG_DSN)'
           WHEN watch_stale THEN
               format('a watch was last written over %s min ago: the bot, or web behind it, '
                      || 'is not answering', floor(extract(epoch FROM watch_gap) / 60))
           WHEN watch_short THEN
               format('watched clean only since %s Kyiv; re-run once the watches cover the day',
                      to_char(GREATEST(sw_since, fw_since) AT TIME ZONE 'Europe/Kyiv',
                              'DD.MM HH24:MI'))
           WHEN file_unseen THEN
               'the file''s hash record is missing, stale (over '
               || floor(extract(epoch FROM record_max_age) / 3600)
               || ' h) or younger than the day: see P1'
           ELSE
               format('silent since %s Kyiv (%s): %s',
                      to_char(start_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'), start_what,
                      CASE WHEN week_clean AND report_inside THEN
                          format('the %s h are covered, a weekly report delivered %s Kyiv '
                                 || 'inside them', floor(extract(epoch FROM week) / 3600),
                                 to_char(report_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                      WHEN week_clean THEN
                          format('%s h clean, but no weekly report was delivered inside them: '
                                 || 'not covered until one is', floor(extract(epoch FROM week) / 3600))
                      ELSE
                          format('%s h of the %s', floor(extract(epoch FROM clock_now - start_at) / 3600),
                                 floor(extract(epoch FROM week) / 3600))
                      END)
       END AS detail
FROM verdicts;
