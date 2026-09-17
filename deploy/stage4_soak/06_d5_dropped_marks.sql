-- D5 — dropped marks not yet covered by a validated rebuild.
--
-- WHY IT EXISTS
-- A mark gives up after a one-second lock timeout rather than cost the order
-- it rides with (core/pg_derivation.py, `mark`). A dropped mark is recorded
-- against `meta.derivation_signal` in `meta.mirror_state` and is otherwise
-- silent: the rebuild it asked for waits for the next mark or the heartbeat.
--
-- `failures_since_ok` is NOT the criterion. A successful mark writes nothing to
-- `meta.mirror_state` and `_record_failure` only increments, so before DN-05b
-- nothing ever resets it: one lock timeout would read as failing for good.
-- What matters is whether a whole, validated rebuild started after the last
-- dropped mark — a rebuild reads all of bronze, so it covers every write whose
-- mark was lost before it began. The 30 s margin covers the retail-status
-- endpoint, the one writer of a source table that does not hold
-- `_heavy_job_lock`.
--
-- WHAT A FAIL MEANS
-- A mark was dropped and no validated rebuild has started since. The next
-- heartbeat normally clears it within the hour; if it does not, D2 says why.
-- Record every row seen here, healed or not — a mark that keeps dropping is a
-- lock somebody is holding.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
m AS (
    SELECT s.table_name, s.failures_since_ok, s.last_attempted_at,
           left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 200) AS last_error,
           (SELECT max(d.started_at) FROM meta.derivation_runs d
             WHERE d.layer = 'warehouse' AND d.error IS NULL
               AND d.validation_passed IS TRUE
               AND d.started_at <= clock.now) AS last_validated_start
    FROM clock
    LEFT JOIN meta.mirror_state s ON s.table_name = 'meta.derivation_signal'
)
SELECT 'D5 dropped marks'::text AS "check",
       CASE
           WHEN m.table_name IS NULL THEN 'PASS'
           WHEN m.last_attempted_at IS NULL THEN 'PASS'
           WHEN m.last_validated_start IS NOT NULL
                AND m.last_attempted_at < m.last_validated_start - interval '30 seconds' THEN 'PASS'
           ELSE 'FAIL'
       END AS verdict,
       CASE
           WHEN m.table_name IS NULL THEN 'no mark has ever been dropped'
           ELSE format('%s dropped mark(s) on record, the last at %s Kyiv (%s); last validated rebuild started %s%s',
                       m.failures_since_ok,
                       COALESCE(to_char(m.last_attempted_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI:SS'), '?'),
                       m.last_error,
                       COALESCE(to_char(m.last_validated_start AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI:SS') || ' Kyiv', 'never'),
                       CASE WHEN m.last_validated_start IS NOT NULL
                                 AND m.last_attempted_at < m.last_validated_start - interval '30 seconds'
                            THEN ' — covered' ELSE ' — not covered yet' END)
       END AS detail
FROM m;
