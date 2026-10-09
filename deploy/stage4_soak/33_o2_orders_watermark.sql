-- O2 (chain 3) — the order step's watermark lives in Postgres and keeps being written.
--
-- Only under chain 3; the variables are O1's.
--
-- WHY IT EXISTS
-- Under the chain `last_sync_orders` lives in `meta.chain_watermarks`. The
-- order step reads it before the fetch — its window starts 24 h before it —
-- and writes it after every page it lands; it is the only writer of orders
-- there is. A step that stopped leaves new orders landing nowhere, and a
-- setter that wrote somewhere else leaves DuckDB's frozen stamp standing in
-- for the absent key (`CHAIN_WATERMARK_INHERITS_DUCKDB`): a window that grows
-- by a minute on every tick. O1 only proves the copies stayed away.
--
-- HOW IT DECIDES
-- The row's `updated_at`, not its value. The value is KeyCRM's newest
-- `updated_at` — not a sync time — and stands still overnight by design
-- (`pg_orders_write.CHAIN_WATERMARK_MAX_AGE_MIN` is None for that reason;
-- the 6-hour `freshness_orders` judges it). The row, `now()` and all, is
-- rewritten by every tick that fetched a window, and the window reaches 24 h
-- back from the newest change, so it is never empty while KeyCRM answers:
-- the stamp's age is the time since the order step last completed. 105
-- minutes, the longest the canary goes without paging: it pages
-- `orders_sync_failing` after 15 without a success (`ORDERS_SYNC_STALE_S`),
-- except while the tick waits for the heavy-job lock — the full sync,
-- training, the backup, the 05:15 refresh — whose wait it subtracts first and
-- excuses up to 90 (`ORDERS_SYNC_LOCK_WAIT_MAX_S`). So a step 105 minutes
-- stale behind a 90-minute wait is one the canary calls waiting; a report
-- stricter than the page would FAIL it (chain 3's review: this was 90). Over
-- 105, to the second — a test derives the number from the canary's constants
-- and checks it against the canary itself. Missing is a FAIL unless the
-- handover (the owner rows, else the given flip time) is under 105 minutes old.
--
-- WHAT A FAIL MEANS
-- Missing, not a timestamp, or not rewritten for 105 minutes: read
-- write_chains.pg_orders_write.sync_step in /api/health (the error class and
-- the lock wait), then the web log for 'Incremental sync'.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'orders_on'::text AS state,
           NULLIF(:'orders_flip_at', '')::timestamptz AS flip_at
),
owner AS (
    SELECT min(updated_at) AS at
    FROM meta.chain_watermarks
    WHERE key IN ('owner:bronze.orders', 'owner:bronze.order_products',
                  'owner:bronze.expenses', 'owner:app.order_backfill_misses')
),
mark AS (
    SELECT max(value) AS value, max(updated_at) AS stamped_at, count(*) AS present
    FROM meta.chain_watermarks
    WHERE key = 'last_sync_orders'
),
judged AS (
    SELECT mark.present > 0 AS present, mark.value,
           pg_input_is_valid(mark.value, 'timestamptz') AS readable,
           floor(extract(epoch FROM clock.now - mark.stamped_at) / 60)::bigint AS age_min,
           clock.now - mark.stamped_at > interval '105 minutes' AS stale,
           COALESCE(owner.at, flag.flip_at) IS NOT NULL
               AND clock.now - COALESCE(owner.at, flag.flip_at) < interval '105 minutes'
               AS just_flipped
    FROM mark CROSS JOIN clock CROSS JOIN flag CROSS JOIN owner
)
SELECT 'O2 orders watermark'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN 'pending' THEN 'PASS'
           WHEN '1' THEN CASE
               WHEN NOT j.present THEN CASE WHEN j.just_flipped THEN 'PASS' ELSE 'FAIL' END
               WHEN NOT j.readable OR j.stale THEN 'FAIL'
               ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 3 still writes DuckDB (KS_WRITE_ORDERS is not postgres and no latch marker)'
           WHEN 'pending' THEN 'not applicable: chain 3 has not latched (see O1)'
           WHEN '1' THEN CASE
               WHEN NOT j.present AND j.just_flipped THEN
                   'last_sync_orders not written yet, inside 105 min of the handover'
               WHEN NOT j.present THEN
                   'last_sync_orders is not in meta.chain_watermarks: no order step has completed '
                   'under the chain, and DuckDB''s frozen stamp is where its window starts'
               WHEN NOT j.readable THEN
                   format('last_sync_orders holds %s, which is not a timestamp', left(j.value, 60))
               ELSE format('last_sync_orders rewritten %s min ago (limit 105); KeyCRM''s newest change %s Kyiv',
                           j.age_min,
                           to_char(j.value::timestamptz AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
               END
           ELSE format('orders_on=%s: see O1', flag.state)
       END AS detail
FROM flag CROSS JOIN judged j;
