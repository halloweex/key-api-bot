-- D1 — owed derivation state.
--
-- WHY IT EXISTS
-- Under KS_PG_DERIVE=own, Postgres rebuilds Silver and Gold when
-- `meta.derivation_signal.requested` moves past `built`, and on a 60-minute
-- heartbeat when nothing is owed (core/pg_derivation.py). Owed state is
-- durable, so a rebuild cannot be lost — but it can stop being *paid*, and
-- the dashboard would then read a Silver that is quietly getting older.
--
-- WHAT A FAIL MEANS
-- - owed > 0 for 15 min: marks are arriving and the derivation is not
--   answering them. The floor is 10 min, so 15 is a tick and a run past it.
--   "For 15 min" is measured from the oldest `bronze.orders` write after the
--   last build — not from `built_at`, as the checklist first had it: after a
--   quiet hour the first new order is owed at once and built within a tick,
--   and `built_at` alone would read that minute as a stall. Where no such
--   write survives (the mark came from the managers or buyers writers, or the
--   order was rewritten since), the latest mark's own time stands in.
-- - nothing built for 62 min: the heartbeat itself stopped — the job is not
--   running, or every run is failing before `complete()` (read D2).
--
-- Both are UNKNOWN, not FAIL, while a job that holds `_heavy_job_lock` is in
-- its window: the derivation queues behind it by design, and only the log can
-- say whether the hold is the job or a stall. The windows are the scheduler's
-- own crons (core/scheduler.py), plus the host's Sunday compaction, which
-- stops the containers and is scheduled in UTC.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
heavy (name, isodows, starts, ends, tz) AS (
    VALUES ('weekly full sync',     '{7}'::int[],   time '02:00', time '03:00', 'Europe/Kyiv'),
           ('model training',       '{1,4}'::int[], time '03:30', time '04:30', 'Europe/Kyiv'),
           ('DuckDB backup',        NULL::int[],    time '04:30', time '05:00', 'Europe/Kyiv'),
           ('order status refresh', NULL::int[],    time '05:15', time '05:45', 'Europe/Kyiv'),
           ('Sunday compaction',    '{7}'::int[],   time '02:00', time '03:30', 'UTC')
),
windows AS (
    SELECT h.name,
           tstzrange((d.kday + h.starts) AT TIME ZONE h.tz,
                     (d.kday + h.ends) AT TIME ZONE h.tz) AS span
    FROM heavy h
    CROSS JOIN (SELECT (clock.now AT TIME ZONE 'Europe/Kyiv')::date - k AS kday
                FROM clock, generate_series(0, 3) AS k) d
    WHERE h.isodows IS NULL OR extract(isodow FROM d.kday)::int = ANY (h.isodows)
),
busy AS (
    SELECT string_agg(DISTINCT w.name, ', ') AS jobs
    FROM windows w, clock
    WHERE w.span @> clock.now
),
sig AS (
    SELECT clock.now, g.layer, g.requested, g.built, g.requested - g.built AS owed,
           g.built_at, g.requested_at,
           (SELECT min(o.mirrored_at) FROM bronze.orders o
             WHERE o.mirrored_at > g.built_at) AS first_write_after_build
    FROM clock
    LEFT JOIN meta.derivation_signal g ON g.layer = 'warehouse'
),
s AS (
    SELECT sig.*,
           floor(extract(epoch FROM now - built_at) / 60)::bigint AS built_min,
           floor(extract(epoch FROM now - requested_at) / 60)::bigint AS mark_min,
           floor(extract(epoch FROM now - COALESCE(first_write_after_build, requested_at))
                 / 60)::bigint AS owed_min
    FROM sig
),
judged AS (
    SELECT s.*, busy.jobs,
           CASE
               WHEN s.layer IS NULL OR s.built_at IS NULL THEN 'broken'
               WHEN (s.owed > 0 AND s.owed_min >= 15) OR s.built_min >= 62 THEN 'late'
               ELSE 'ok'
           END AS state
    FROM s CROSS JOIN busy
)
SELECT 'D1 owed rebuild'::text AS "check",
       CASE
           WHEN state = 'broken' THEN 'FAIL'
           WHEN state = 'late' AND jobs IS NOT NULL THEN 'UNKNOWN'
           WHEN state = 'late' THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN layer IS NULL THEN
               'meta.derivation_signal has no warehouse row (revision 0033 seeds one)'
           WHEN built_at IS NULL THEN
               format('never built: requested %s, built %s', requested, built)
           ELSE
               format('requested %s, built %s, owed %s%s; last build %s min ago, last mark %s min ago%s',
                      requested, built, owed,
                      CASE WHEN owed > 0 THEN format(' for %s min', owed_min) ELSE '' END,
                      built_min, COALESCE(mark_min::text, '?'),
                      CASE WHEN state = 'late' AND jobs IS NOT NULL
                           THEN format(' — inside the %s window; re-run after it', jobs)
                           WHEN state = 'late' AND owed > 0
                           THEN ' — owed and unanswered for 15 min or more'
                           WHEN state = 'late'
                           THEN ' — the 60 min heartbeat has not run'
                           ELSE '' END)
       END AS detail
FROM judged;
