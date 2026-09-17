-- S0 — order intake is alive: the Postgres half.
--
-- WHY IT EXISTS
-- Until DN-01, a typo in any KS_WRITE_* line raised inside
-- `get_last_sync_time('orders')`, the first thing `incremental_sync` asks, and
-- a failed job is only logged. Order intake stopped and nothing paged for the
-- eight hours the canary allows the orders mirror. DN-01 narrowed the blast
-- radius; this check is still the fastest way to see the shape again, and it
-- is the first thing to run after any .env edit, deploy or recreate.
--
-- The other half is the log grep in deploy/stage4_soak.sh — a failing job
-- writes a line long before it moves a watermark.
--
-- WHAT A FAIL MEANS
-- `failures_since_ok > 0`: the orders mirror is failing now. Age over 30 min
-- in the day: nothing has shipped, which with customers ordering means the
-- sync is not running. Restore the last .env value first, then investigate.
--
-- The age is judged only 09:00–23:00 Kyiv. `mirror_orders` refuses to move the
-- watermark when there was nothing to ship, and at night there is nothing
-- (bot/canary.py, MIRROR_MAX_AGE_S) — so a night-time age says nothing.
--
-- `soak.now` exists for the tests, which need a wall clock they can set.
-- deploy/stage4_soak.sh never sets it, so this is now().
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
m AS (
    SELECT clock.now,
           (clock.now AT TIME ZONE 'Europe/Kyiv')::time AS kyiv_time,
           s.table_name, s.last_ok_at, s.failures_since_ok,
           regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g') AS last_error,
           floor(extract(epoch FROM clock.now - s.last_ok_at) / 60)::bigint AS age_min
    FROM clock
    LEFT JOIN meta.mirror_state s ON s.table_name = 'bronze.orders'
)
SELECT 'S0 order intake (mirror)'::text AS "check",
       CASE
           WHEN table_name IS NULL THEN 'FAIL'
           WHEN failures_since_ok > 0 THEN 'FAIL'
           WHEN kyiv_time >= time '09:00' AND kyiv_time < time '23:00'
                AND (last_ok_at IS NULL OR age_min >= 30) THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN table_name IS NULL THEN
               'bronze.orders has no row in meta.mirror_state: the orders mirror has never shipped'
           WHEN failures_since_ok > 0 THEN
               format('bronze.orders mirror failing, %s in a row: %s',
                      failures_since_ok, left(last_error, 200))
           WHEN last_ok_at IS NULL THEN
               'bronze.orders mirror has never succeeded'
           WHEN kyiv_time >= time '09:00' AND kyiv_time < time '23:00' THEN
               format('last shipment %s min ago (limit 30 in the day)', age_min)
           ELSE
               format('last shipment %s min ago; not judged at night', age_min)
       END AS detail
FROM m;
