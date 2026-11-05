-- D2 — the derivation journal, last 24 h.
--
-- WHY IT EXISTS
-- Every Postgres derivation attempt writes one row to `meta.derivation_runs`,
-- with its validation and its error (core/scheduler.py, `_derive_pg_layers`).
-- The alerts for a failed run are fire-and-forget; the journal is the record
-- that does not depend on a message having been delivered.
--
-- WHAT A FAIL MEANS
-- - an error: a rebuild stopped at the named stage and stays owed. Read the
--   web log at that time, then POST /api/warehouse/refresh.
-- - a run that completed and did not pass validation: Silver or Gold in
--   Postgres disagree with their source — `validation` names the check.
-- - a run of 60 s or more: something held PG_LAYER_LOCK, usually a ClickHouse
--   ship; the derivation itself takes 2–5 s.
-- - no run at all: under own this is the derivation not running. A fallback
--   to piggyback on a bad KS_PG_DERIVE shows up exactly like this.
--
-- `first_tick` runs are counted and shown, not judged: one per web start is
-- expected, and only the host knows how many starts there were.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
r AS (
    SELECT d.*
    FROM meta.derivation_runs d, clock
    WHERE d.layer = 'warehouse'
      AND d.started_at > clock.now - interval '24 hours'
      AND d.started_at <= clock.now
),
agg AS (
    SELECT count(*) AS runs,
           count(*) FILTER (WHERE error IS NOT NULL) AS errors,
           count(*) FILTER (WHERE error IS NULL AND validation_passed IS NOT TRUE) AS not_passed,
           max(ended_at - started_at) AS max_dur,
           round(avg(extract(epoch FROM ended_at - started_at))::numeric, 1) AS avg_s
    FROM r
),
by_trigger AS (
    SELECT string_agg(trigger || '=' || n, ' ' ORDER BY trigger) AS triggers
    FROM (SELECT trigger, count(*) AS n FROM r GROUP BY trigger) t
),
worst AS (
    SELECT format('latest bad run %s at %s Kyiv (%s): %s',
                  r.id, to_char(r.started_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                  r.trigger,
                  left(regexp_replace(COALESCE(
                      r.error,
                      'validation failed: ' || (
                          SELECT string_agg(k, ',' ORDER BY k)
                          FROM jsonb_each_text(COALESCE(r.validation, '{}'::jsonb)) AS v(k, val)
                          WHERE k IN ('row_count_match', 'checksum_match',
                                      'cells_match', 'rollup_match')
                            AND val = 'false'),
                      'validation_passed is not true'), '\s+', ' ', 'g'), 200)) AS what
    FROM r
    WHERE r.error IS NOT NULL OR r.validation_passed IS NOT TRUE
    ORDER BY r.id DESC
    LIMIT 1
)
SELECT 'D2 derivation journal'::text AS "check",
       CASE
           WHEN agg.runs = 0 THEN 'FAIL'
           WHEN agg.errors > 0 OR agg.not_passed > 0 THEN 'FAIL'
           WHEN agg.max_dur >= interval '60 seconds' THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN agg.runs = 0 THEN 'no derivation run in 24 h: is KS_PG_DERIVE=own live?'
           ELSE format('%s runs (%s), %s errors, %s not passed, avg %s s, max %s s%s',
                       agg.runs, COALESCE(by_trigger.triggers, ''), agg.errors,
                       agg.not_passed, COALESCE(agg.avg_s::text, '?'),
                       COALESCE(round(extract(epoch FROM agg.max_dur))::text, '?'),
                       COALESCE('; ' || (SELECT what FROM worst), ''))
       END AS detail
FROM agg CROSS JOIN by_trigger;
