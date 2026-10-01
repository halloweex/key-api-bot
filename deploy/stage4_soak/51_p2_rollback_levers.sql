-- P2 — rollback levers pulled in the last 24 h (OD-17 (a)).
--
-- WHY IT EXISTS
-- The 30-day parallel period and the 7-day week of silence both start again
-- when "any rollback lever is used". This check is the lever half of that,
-- a day at a time; P3 and P4 turn the same evidence into the two clocks.
--
-- WHAT A LEVER IS, AND WHERE IT IS RECORDED
-- - A copy-back (`scripts/chain_copy_back.py`), the only thing that gives a
--   chain back to DuckDB. A release writes one `lever_used` row under
--   `lever:chain_copy_back` in app.alert_events inside its own transaction,
--   and an exit 3 writes one while the owner rows stand
--   (core/lever_journal.py). Before that nothing durable recorded one: the
--   release deletes rows and unlinks a file, and the script's report dies
--   with its --rm container.
-- - A KS_WRITE_* put back while its chain is latched. It moves nothing (OD-19
--   (a)), but somebody reached for a rollback: the canary pages
--   `write_chain_flag_mismatch`, and the Alert Gate journals the page.
-- - Step 13's way back. Completed, it is a record in DuckDB's sync_metadata,
--   `warehouse_writer = {"writer": "duckdb", "since": …}`, which the hourly
--   copy carries to app.sync_metadata; stuck, the canary pages
--   `warehouse_hold_stuck`; and once the parallel period is declared
--   (SOAK_PARALLEL_FROM) or web runs KS_DUCKDB=off, `warehouse_preconditions_unmet`
--   is the way back too — after the flip, any precondition lost is one. Before
--   that it can only mean a first flip held back, which is not a lever.
--
-- WHAT EACH VERDICT MEANS
-- FAIL: a lever recorded in the day, a lever page fired, escalated or resolved
-- in it, one still standing however old, or step 13's way back completed in
-- it. Never UNKNOWN: the journal is written to Postgres directly, not copied
-- out of DuckDB (D9's reason). PASS names the last lever ever recorded.
--
-- WHAT IT CANNOT SEE
-- A way back that finished inside the hour before the hourly copy shipped the
-- record; a marker deleted by hand (the daily comparison's
-- chain_latch_disagrees and the order owner-row checks see that, not this);
-- and a KS_READ_* put back to duckdb outside step 13's list. One inside it —
-- every Silver, Gold and UTM read switch — is a lost precondition, so the
-- way back's page above; one outside it reads DuckDB with nothing failing to
-- count, and only the cutover status page lists it.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
limits AS (
    SELECT interval '24 hours' AS span
),
win AS (
    SELECT clock.now - limits.span AS starts, clock.now, limits.span
    FROM clock CROSS JOIN limits
),
running AS (
    SELECT NULLIF(:'parallel_from', '') IS NOT NULL OR :'duckdb_off' = '1' AS period
),
lever_pages AS (
    SELECT v.condition_key
    FROM (VALUES ('write_chain_flag_mismatch', false),
                 ('warehouse_hold_stuck', false),
                 ('warehouse_preconditions_unmet', true)) AS v (condition_key, in_period_only)
    CROSS JOIN running
    WHERE NOT v.in_period_only OR running.period
),
recorded AS (
    SELECT e.at, e.condition_key,
           concat_ws(' ', substr(e.condition_key, 7), e.context->>'subject',
                     e.context->>'outcome') AS what
    FROM app.alert_events e, win
    WHERE left(e.condition_key, 6) = 'lever:'
      AND e.event_type = 'lever_used'
      AND e.at <= win.now
),
paged AS (
    SELECT e.at, e.condition_key
    FROM app.alert_events e, win
    WHERE e.condition_key IN (SELECT condition_key FROM lever_pages)
      AND e.event_type IN ('fired', 'escalated')
      AND e.at > win.starts
      AND e.at <= win.now
),
series AS (
    SELECT bool_or(s.state = 'firing') AS firing,
           string_agg(s.condition_key, ', ' ORDER BY s.condition_key)
               FILTER (WHERE s.state = 'firing') AS firing_keys,
           max(s.resolved_at) AS resolved_at
    FROM app.alert_series s
    WHERE s.condition_key IN (SELECT condition_key FROM lever_pages)
),
writer AS (
    SELECT substring(m.value FROM '"writer"\s*:\s*"([a-z]+)"') AS writer,
           CASE WHEN m.value ~ '"since"\s*:\s*"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})"'
                THEN substring(m.value FROM '"since"\s*:\s*"([^"]+)"')::timestamptz
           END AS since
    FROM app.sync_metadata m
    WHERE m.key = 'warehouse_writer'
),
judged AS (
    SELECT win.starts, win.now AS clock_now, win.span,
           (SELECT count(*) FROM recorded r WHERE r.at > win.starts) AS n_recorded,
           (SELECT r.what FROM recorded r WHERE r.at > win.starts
             ORDER BY r.at DESC LIMIT 1) AS recorded_today,
           (SELECT max(r.at) FROM recorded r WHERE r.at > win.starts) AS recorded_today_at,
           (SELECT r.what FROM recorded r ORDER BY r.at DESC LIMIT 1) AS last_recorded,
           (SELECT max(r.at) FROM recorded r) AS last_recorded_at,
           (SELECT count(*) FROM paged) AS n_paged,
           (SELECT string_agg(DISTINCT p.condition_key, ', ') FROM paged p) AS paged_keys,
           (SELECT max(p.at) FROM paged p) AS last_paged,
           COALESCE(series.firing, false) AS firing, series.firing_keys,
           series.resolved_at,
           writer.writer, writer.since AS writer_since
    FROM win
    CROSS JOIN series
    LEFT JOIN writer ON true
),
verdicts AS (
    SELECT j.*,
           j.n_recorded > 0 AS fail_recorded,
           j.n_paged > 0 AS fail_paged,
           COALESCE(j.resolved_at > j.starts AND j.resolved_at <= j.clock_now, false)
               AS fail_resolved,
           COALESCE(j.writer = 'duckdb' AND j.writer_since > j.starts
                    AND j.writer_since <= j.clock_now, false) AS fail_way_back
    FROM judged j
)
SELECT 'P2 rollback levers'::text AS "check",
       CASE
           WHEN fail_recorded OR fail_paged OR firing OR fail_resolved OR fail_way_back
               THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN fail_recorded OR fail_paged OR firing OR fail_resolved OR fail_way_back THEN left(
               concat_ws('; ',
                   CASE WHEN fail_recorded THEN
                       format('%s lever(s) recorded in %s h, last %s at %s Kyiv', n_recorded,
                              floor(extract(epoch FROM span) / 3600), recorded_today,
                              to_char(recorded_today_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN fail_paged THEN
                       format('paged %s time(s) in %s h (%s), last %s Kyiv', n_paged,
                              floor(extract(epoch FROM span) / 3600), paged_keys,
                              to_char(last_paged AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN firing THEN format('still paging: %s', firing_keys) END,
                   CASE WHEN fail_resolved AND NOT fail_paged THEN
                       format('a lever page stood until %s Kyiv, inside the window',
                              to_char(resolved_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END,
                   CASE WHEN fail_way_back THEN
                       format('step 13 was given back to DuckDB at %s Kyiv',
                              to_char(writer_since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                   END)
               || '; the parallel period and the week of silence start again (OD-17 (a))', 500)
           ELSE
               format('no rollback lever in %s h; last recorded: %s',
                      floor(extract(epoch FROM span) / 3600),
                      CASE WHEN last_recorded_at IS NULL THEN 'none'
                           ELSE format('%s at %s Kyiv', last_recorded,
                                       to_char(last_recorded_at AT TIME ZONE 'Europe/Kyiv',
                                               'DD.MM HH24:MI')) END)
               || CASE WHEN writer = 'duckdb' THEN
                       format('; step 13 has been back on DuckDB since %s Kyiv',
                              COALESCE(to_char(writer_since AT TIME ZONE 'Europe/Kyiv',
                                               'DD.MM HH24:MI'), 'an unknown time'))
                       ELSE '' END
       END AS detail
FROM verdicts;
