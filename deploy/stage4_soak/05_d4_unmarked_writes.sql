-- D4 — a bronze.orders write that raised no mark, last 48 h.
--
-- WHY IT EXISTS
-- The derivation rebuilds only when `requested` moves. A writer that lands
-- orders without raising the mark — a new code path not in `MARK_SITES`, or a
-- process whose mode was never configured — leaves Silver stale until the
-- heartbeat, and under DN-29 nothing else would ever notice. This stands in
-- for DN-14's detector until that lands.
--
-- HOW IT DECIDES
-- Take two consecutive error-free runs. If `requested_seen` did not move
-- between them, no mark was raised in between — so no `bronze.orders` row may
-- have been mirrored in between either. That is exact, not heuristic: every
-- orders writer on the scheduler path holds `_heavy_job_lock`, and the
-- derivation takes that lock before stamping `started_at`
-- (core/scheduler.py, `_run_pg_derivation`), so a write cannot straddle a
-- run's start.
--
-- RESTARTS
-- The boot sync runs before the scheduler configures the mode (web/main.py
-- awaits `init_and_sync` before `start_scheduler`), so its writes are
-- legitimately unmarked, and the first rebuild of the new process covers them.
-- That first run is journalled one of two ways (core/pg_derivation.py, `due`):
-- - `first_tick`, when nothing was owed. Its `requested_seen` equals the last
--   run's, so it IS a quiet pair, and it is excluded here by name.
-- - `signal`, when marks were owed — the usual case after a deploy, measured
--   2026-09-17. It needs no exclusion, and none is added: `due` says `signal`
--   only when `requested > built`, `built` is at least the previous run's
--   `requested_seen` (`complete` never sets it lower), and the run reads
--   `requested` after `due` did — so its `requested_seen` has moved by
--   construction and it can never be a quiet pair. The boot-sync rows in front
--   of it are simply not judged. A time-window exclusion ("rows just before a
--   run that follows a long gap") was considered and refused: every heartbeat
--   follows a 60-minute gap, and heartbeat pairs are nearly all the quiet pairs
--   this check has, so it would blind the check where it looks.
-- DN-05b (configure the modes before the boot sync) makes boot-sync writes
-- marked; after its deploy the `first_tick` exclusion only matters for journal
-- rows older than that deploy.
--
-- WHAT A FAIL MEANS
-- A real unmarked write. After a one-off script (`force_resync`) it is
-- expected and the heartbeat covered it — record it. Otherwise find the writer:
-- the rows' `mirrored_at` says when.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
r AS (
    SELECT d.id, d.trigger, d.started_at, d.requested_seen,
           lag(d.started_at) OVER w AS prev_started,
           lag(d.requested_seen) OVER w AS prev_seen
    FROM meta.derivation_runs d
    WHERE d.layer = 'warehouse' AND d.error IS NULL
    WINDOW w AS (ORDER BY d.id)
),
quiet AS (
    SELECT r.*
    FROM r, clock
    WHERE r.started_at > clock.now - interval '48 hours'
      AND r.started_at <= clock.now
      AND r.trigger <> 'first_tick'
      AND r.requested_seen = r.prev_seen
),
-- One pass over bronze.orders rather than one per run: `mirrored_at` has no
-- index, and the 48 h window can hold a few hundred runs.
recent AS (
    SELECT o.mirrored_at
    FROM bronze.orders o
    WHERE o.mirrored_at >= (SELECT min(prev_started) FROM quiet)
),
hits AS (
    SELECT q.id, q.trigger, q.started_at, q.prev_started, q.requested_seen,
           count(*) AS written, min(o.mirrored_at) AS first_write
    FROM quiet q
    JOIN recent o ON o.mirrored_at >= q.prev_started AND o.mirrored_at < q.started_at
    GROUP BY q.id, q.trigger, q.started_at, q.prev_started, q.requested_seen
),
agg AS (
    SELECT count(*) AS runs, COALESCE(sum(written), 0) AS written,
           string_agg(format('run %s (%s) at %s Kyiv saw requested %s unchanged, yet %s order(s) were mirrored from %s',
                             id, trigger,
                             to_char(started_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                             requested_seen, written,
                             to_char(first_write AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI:SS')),
                      '; ' ORDER BY id DESC) AS listed
    FROM hits
)
SELECT 'D4 unmarked writes'::text AS "check",
       CASE WHEN agg.runs > 0 THEN 'FAIL' ELSE 'PASS' END AS verdict,
       CASE WHEN agg.runs > 0
            THEN left(agg.listed, 600)
            ELSE format('no orders mirrored between runs that saw no mark (%s quiet run pairs in 48 h, first_tick excluded)',
                        (SELECT count(*) FROM quiet))
       END AS detail
FROM agg;
