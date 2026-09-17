-- D3 — gaps between derivation runs, last 48 h.
--
-- WHY IT EXISTS
-- D1 sees the state now; this sees a hole that has already closed. The
-- derivation takes `_heavy_job_lock` before it stamps `started_at`, so any job
-- holding that lock for long delays a rebuild — and a hold of ~29 min at a
-- heartbeat instant is where the canary's 90 min limit on the derived tables
-- starts to page. The soak has to show those holds are what they are expected
-- to be, before DuckDB's own derivation stops and nothing else rebuilds.
--
-- WHAT THE VERDICTS MEAN
-- A gap is judged only above 62 min — the heartbeat plus a tick.
-- - PASS: ended by a `first_tick` run within 95 min (a web start, including
--   the Sunday compaction's), or overlapping a named heavy-lock window within
--   90 min (training, the weekly full sync, the backup, the status refresh).
-- - UNKNOWN: an unexplained gap of 95 min or less. A web start whose first run
--   owed marks is journalled as `signal`, not `first_tick`
--   (core/pg_derivation.py, `due`), so a restart can look exactly like a stall
--   here; match the gap against `docker ps` / the deploy log.
-- - FAIL: any gap over 95 min, or over 90 min with no restart behind it. That
--   long, the canary has paged or should have.
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
r AS (
    SELECT d.id, d.trigger, d.started_at,
           lag(d.started_at) OVER (ORDER BY d.id) AS prev_started
    FROM meta.derivation_runs d, clock
    WHERE d.layer = 'warehouse'
      AND d.started_at > clock.now - interval '72 hours'
      AND d.started_at <= clock.now
),
gaps AS (
    SELECT r.*, r.started_at - r.prev_started AS gap,
           (SELECT string_agg(DISTINCT w.name, ', ') FROM windows w
             WHERE w.span && tstzrange(r.prev_started, r.started_at)) AS jobs
    FROM r, clock
    WHERE r.started_at > clock.now - interval '48 hours'
      AND r.prev_started IS NOT NULL
      AND r.started_at - r.prev_started > interval '62 minutes'
),
judged AS (
    SELECT g.*,
           CASE
               WHEN g.gap > interval '95 minutes' THEN 'FAIL'
               WHEN g.trigger = 'first_tick' THEN 'PASS'
               WHEN g.jobs IS NOT NULL AND g.gap <= interval '90 minutes' THEN 'PASS'
               WHEN g.jobs IS NOT NULL THEN 'FAIL'
               ELSE 'UNKNOWN'
           END AS verdict,
           CASE
               WHEN g.trigger = 'first_tick' THEN 'restart'
               WHEN g.jobs IS NOT NULL THEN g.jobs
               ELSE 'unexplained'
           END AS why
    FROM gaps g
),
agg AS (
    SELECT count(*) AS n,
           count(*) FILTER (WHERE verdict = 'FAIL') AS fails,
           count(*) FILTER (WHERE verdict = 'UNKNOWN') AS unknowns,
           string_agg(format('%s min before run %s at %s Kyiv (%s, %s)',
                             floor(extract(epoch FROM gap) / 60), id,
                             to_char(started_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                             trigger, why),
                      '; ' ORDER BY gap DESC) AS listed
    FROM judged
),
runs AS (
    SELECT count(*) AS n FROM r, clock WHERE r.started_at > clock.now - interval '48 hours'
)
SELECT 'D3 derivation gaps'::text AS "check",
       CASE
           WHEN runs.n = 0 THEN 'FAIL'
           WHEN agg.fails > 0 THEN 'FAIL'
           WHEN agg.unknowns > 0 THEN 'UNKNOWN'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN runs.n = 0 THEN 'no derivation run in 48 h'
           WHEN agg.n = 0 THEN format('%s runs in 48 h, no gap over 62 min', runs.n)
           ELSE format('%s runs in 48 h; gaps over 62 min: %s', runs.n, agg.listed)
       END AS detail
FROM agg CROSS JOIN runs;
