-- P1 — the DuckDB file, as the hourly hash recorded it (OD-17 (a)).
--
-- WHY IT EXISTS
-- The week of silence is seven days in which nothing opened the DuckDB file,
-- and "web opens DuckDB" and "the file hash changes" are two of its four
-- breaches. The tripwire (KS_DUCKDB=off, judged in P4) sees what web opens;
-- this sees the file itself, so it also catches what runs outside web: a
-- one-off container started with KS_DUCKDB=on, the weekly compaction, a
-- script run by hand. `deploy/duckdb_silence_check.sh` hashes the file and
-- its .wal from the host crontab, hourly, and keeps one record under
-- /root/duckdb-silence; deploy/stage4_soak.sh reads that record with
-- `--status` — it hashes nothing and writes nothing — and hands it here as
-- psql variables, so the judgement and its clock live in SQL and are tested
-- against a real server like every other check.
--
-- THE VARIABLES
--   duckdb_off                 1 when web's KS_DUCKDB is `off`, 0 when it is
--                              `on` or unset, invalid, or unknown (web down)
--   duckdb_file_last           the last recorded verdict: UNCHANGED, CHANGED,
--                              BASELINE, MISSING; `none` when there is no
--                              record, `error` when it could not be read
--   duckdb_file_since          when the file's current content was first seen
--   duckdb_file_since_reason   baseline (the first check ever) or change
--   duckdb_file_checked_at     the last recording check
--   duckdb_file_missing_at     the last check that found the file missing,
--                              kept by every later run; empty if none ever did
--
-- WHAT EACH VERDICT MEANS
-- FAIL: the record says the file is MISSING, or a check inside the day found
-- it missing and it has come back since — whatever KS_DUCKDB says, because
-- deleting the file is a DROP and none may happen before the owner's week
-- after full completion (OD-11 (a)), and for the hours it was gone nothing
-- vouched for it, however unchanged its bytes came back — or, under off, the
-- content changed at the last check or inside the day. UNKNOWN: under off, no record, a
-- record not written for over 3 h (the cron runs hourly), or one that began
-- inside the day. PASS: under on, not applicable — web writes the file all
-- day; under off, the same bytes since a moment over a day ago.
--
-- WHAT IT CANNOT SEE
-- A change and its exact reversal between two hourly checks; and anything that
-- opened the file read-only, which changes no byte. The tripwire is the
-- detector for web's opens; this one is for writes from anywhere.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
limits AS (
    SELECT interval '24 hours' AS span,
           interval '3 hours' AS record_max_age
),
rec AS (
    SELECT NULLIF(:'duckdb_off', '') AS duckdb_off,
           NULLIF(:'duckdb_file_last', '') AS last,
           NULLIF(:'duckdb_file_since_reason', '') AS since_reason,
           CASE WHEN :'duckdb_file_since' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_file_since')::text::timestamptz END AS since,
           CASE WHEN :'duckdb_file_checked_at' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_file_checked_at')::text::timestamptz END AS checked_at,
           CASE WHEN :'duckdb_file_missing_at' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
                THEN (:'duckdb_file_missing_at')::text::timestamptz END AS missing_at
),
judged AS (
    SELECT c.now AS clock_now, l.span, l.record_max_age, r.*,
           r.last IN ('UNCHANGED', 'CHANGED', 'BASELINE')
               AND r.since IS NOT NULL AND r.checked_at IS NOT NULL AS readable,
           COALESCE(c.now - r.checked_at > l.record_max_age, false) AS stale,
           COALESCE(r.since > c.now - l.span, false) AS since_in_day,
           COALESCE(r.missing_at > c.now - l.span AND r.missing_at <= c.now, false)
               AS missing_in_day
    FROM clock c CROSS JOIN limits l CROSS JOIN rec r
),
verdicts AS (
    SELECT j.*,
           j.last = 'MISSING' AS fail_missing,
           j.missing_in_day AND j.last IS DISTINCT FROM 'MISSING' AS fail_was_missing,
           j.readable AND NOT j.stale
               AND (j.last = 'CHANGED' OR (j.since_reason = 'change' AND j.since_in_day))
               AS fail_changed
    FROM judged j
)
SELECT 'P1 DuckDB file'::text AS "check",
       CASE
           WHEN fail_missing OR fail_was_missing THEN 'FAIL'
           WHEN duckdb_off = 'unknown' THEN 'UNKNOWN'
           WHEN duckdb_off IS DISTINCT FROM '1' THEN 'PASS'
           WHEN NOT readable OR stale THEN 'UNKNOWN'
           WHEN fail_changed THEN 'FAIL'
           WHEN since_in_day OR last = 'BASELINE' THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN fail_missing THEN
               'the DuckDB file was missing at the last recorded check'
               || CASE WHEN checked_at IS NULL THEN ''
                       ELSE format(' (%s Kyiv)', to_char(checked_at AT TIME ZONE 'Europe/Kyiv',
                                                         'DD.MM HH24:MI')) END
               || ': deleting it is a DROP, and none may happen before the owner''s week '
               || 'after full completion (OD-11 (a)); put it back from the Ark'
           WHEN fail_was_missing THEN
               format('the DuckDB file was missing at a check inside the day (last %s Kyiv) '
                      || 'and has come back: for the hours it was gone nothing vouched for it, '
                      || 'however unchanged its bytes; deleting or moving it is a DROP, and none '
                      || 'may happen before the owner''s week after full completion (OD-11 (a)); '
                      || 'find what moved it',
                      to_char(missing_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           WHEN duckdb_off = 'unknown' THEN
               'web is not running, so whether KS_DUCKDB is off cannot be read'
           WHEN duckdb_off IS DISTINCT FROM '1' THEN
               'not applicable: KS_DUCKDB is not off, and web writes the file all day'
               || CASE WHEN readable THEN format(' (record: %s since %s Kyiv)', last,
                       to_char(since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                       ELSE '' END
           WHEN last IS NULL OR last = 'none' THEN
               'no record of the file''s hash: install deploy/duckdb_silence_check.sh in '
               || 'the host crontab (hourly) at the start of the week of silence'
           WHEN NOT readable THEN
               format('the record could not be read (%s): ask deploy/duckdb_silence_check.sh '
                      || 'for its status on the host', last)
           WHEN stale THEN
               format('the hash was last recorded %s h ago (limit %s): the hourly cron is '
                      || 'not running', floor(extract(epoch FROM clock_now - checked_at) / 3600),
                      floor(extract(epoch FROM record_max_age) / 3600))
           WHEN fail_changed THEN
               format('the DuckDB file changed: first seen at %s Kyiv (checked hourly). Something '
                      || 'wrote it while KS_DUCKDB was off; find what, and the week starts again',
                      to_char(since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           WHEN since_in_day OR last = 'BASELINE' THEN
               format('recorded only since %s Kyiv, %s h of the %s; re-run once the record '
                      || 'covers the day',
                      to_char(since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      round(extract(epoch FROM clock_now - since) / 3600, 1),
                      floor(extract(epoch FROM span) / 3600))
           ELSE
               format('the same bytes since %s Kyiv (%s h), last checked %s Kyiv',
                      to_char(since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      floor(extract(epoch FROM clock_now - since) / 3600),
                      to_char(checked_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
       END AS detail
FROM verdicts;
