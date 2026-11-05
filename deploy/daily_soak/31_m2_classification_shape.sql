-- M2 (chain 5) — the classification keeps its shape, and its sync keeps moving.
--
-- It judged only under chain 5 (KS_WRITE_MANAGERS=postgres latched, or its
-- latch) until step 14's S14-20: every chain writes Postgres since S14-00.
--
-- WHY IT EXISTS
-- Postgres is the only store of a classification an admin makes, and
-- `sales_type` is derived from it on every Postgres rebuild. This proves what
-- the chain itself left: every
-- manager with exactly one open interval, an unbroken history, a current
-- `is_retail` that agrees with it, `set_at` present (the writer's clock),
-- and `last_sync_managers` — the daily sync's own stamp — under 26 hours.
--
-- HOW IT DECIDES
-- The same predicates as the standing watch (`core/pg_chain_invariants.py`,
-- `pg_managers_write.SHAPE_SQL`), read here so a soak day says so even when
-- the 09:00 digest is read late. The stamp is the stored VALUE, which is what
-- `_freshness_check` judges; missing is a FAIL. (A flip time under 30 minutes
-- old, SOAK_MANAGERS_FLIP_AT, excused it until step 14's S14-20.)
--
-- WHAT A FAIL MEANS
-- Each part names its ids. Not exactly one open interval, an overlap or a
-- gap: decide per id — a forward classification through the retail-status
-- route closes an extra open interval; a history is the owner's to correct.
-- A stale or missing stamp: read write_chains.pg_managers_write.sync_step in
-- /api/health, then the web log for 'Manager sync'.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
iv AS (
    SELECT manager_id, valid_from, valid_to,
           LEAD(valid_from) OVER (PARTITION BY manager_id ORDER BY valid_from)
               AS next_from
    FROM app.manager_classifications
),
per AS (
    SELECT m.id, m.is_retail,
           count(c.manager_id) AS intervals,
           count(c.manager_id) FILTER (WHERE c.valid_to IS NULL) AS open_n,
           (array_agg(c.is_retail ORDER BY c.valid_from DESC)
                FILTER (WHERE c.valid_to IS NULL))[1] AS latest_open
    FROM bronze.managers m
    LEFT JOIN app.manager_classifications c ON c.manager_id = m.id
    GROUP BY m.id, m.is_retail
),
shape AS (
    SELECT (SELECT count(*) FROM bronze.managers) AS managers,
           (SELECT array_agg(id ORDER BY id) FROM per WHERE intervals = 0) AS unclassified,
           (SELECT array_agg(id ORDER BY id) FROM per
             WHERE intervals > 0 AND open_n <> 1) AS open_wrong,
           (SELECT array_agg(DISTINCT manager_id ORDER BY manager_id) FROM iv
             WHERE (next_from IS NOT NULL AND valid_to IS DISTINCT FROM next_from)
                OR (valid_to IS NOT NULL AND valid_to <= valid_from)) AS broken,
           (SELECT array_agg(id ORDER BY id) FROM per
             WHERE open_n > 0 AND latest_open IS DISTINCT FROM is_retail) AS disagree,
           (SELECT count(*) FROM app.manager_classifications WHERE set_at IS NULL)
               AS set_at_null
),
mark AS (
    SELECT max(value) AS value, count(*) AS present
    FROM meta.chain_watermarks
    WHERE key = 'last_sync_managers'
),
stamp AS (
    SELECT mark.present > 0 AS present, mark.value,
           CASE WHEN mark.present > 0 AND pg_input_is_valid(mark.value, 'timestamptz')
                THEN floor(extract(epoch FROM clock.now - mark.value::timestamptz) / 3600)::bigint
           END AS age_h
    FROM mark CROSS JOIN clock
),
problems AS (
    SELECT array_remove(ARRAY[
        CASE WHEN shape.managers = 0 THEN 'bronze.managers is empty' END,
        CASE WHEN shape.open_wrong IS NOT NULL THEN
            format('not exactly one open interval: %s', left(shape.open_wrong::text, 80)) END,
        CASE WHEN shape.unclassified IS NOT NULL THEN
            format('no interval at all: %s', left(shape.unclassified::text, 80)) END,
        CASE WHEN shape.broken IS NOT NULL THEN
            format('history overlaps or gaps: %s', left(shape.broken::text, 80)) END,
        CASE WHEN shape.disagree IS NOT NULL THEN
            format('is_retail disagrees with the open interval: %s',
                   left(shape.disagree::text, 80)) END,
        CASE WHEN shape.set_at_null > 0 THEN
            format('%s interval(s) with no set_at', shape.set_at_null) END,
        CASE WHEN NOT stamp.present THEN
            'last_sync_managers is not in meta.chain_watermarks: no manager sync has completed under the chain'
             WHEN stamp.present AND stamp.age_h IS NULL THEN
            format('last_sync_managers holds %s, which is not a timestamp', left(stamp.value, 60))
             WHEN stamp.present AND stamp.age_h >= 26 THEN
            format('last_sync_managers moved %s h ago (limit 26)', stamp.age_h) END
    ], NULL) AS listed
    FROM shape CROSS JOIN stamp
)
SELECT 'M2 classification shape'::text AS "check",
       CASE WHEN cardinality(p.listed) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict,
       CASE WHEN cardinality(p.listed) = 0 THEN
           format('%s manager(s), each with one open interval and an unbroken history; '
                  'last_sync_managers %s',
                  (SELECT managers FROM shape),
                  (SELECT age_h::text || ' h old' FROM stamp))
           ELSE left(array_to_string(p.listed, '; '), 500) END AS detail
FROM problems p;
