-- O4 (chain 3) — an order with revenue and no line items is repaired or recorded.
--
-- Only under chain 3; the variables are O1's.
--
-- WHY IT EXISTS
-- The chain writes an order's header and its line items in one transaction
-- from one payload, and the half-written repair (`halfwritten_repair`, every
-- 2 h) re-fetches by id any order carrying revenue and no line items,
-- recording in app.order_backfill_misses each one KeyCRM serves empty again,
-- so that it asks about something else next time. Under the chain all of it
-- is Postgres: the selection (`pg_orders_read.halfwritten_candidates`), the
-- re-write and the record (`pg_orders_write.record_backfill_misses`). A write
-- that created a header without its lines, a line-item rewrite that deleted
-- and inserted nothing, a repair that stopped running and a record that went
-- somewhere else all end the same way: the revenue on every chart, nothing on
-- the product, brand and category ones, and nothing saying so.
--
-- HOW IT DECIDES
-- Orders whose header the chain wrote since the handover (`mirrored_at`, the
-- clock it stamps; the owner rows, else the given flip time), carrying
-- revenue (the repair's own `grand_total > 0`), with no line item, created
-- over 6 hours ago — three of the repair's intervals, so a run held back by
-- the heavy-job lock and a timer restarted by a deploy are not a defect — and
-- with no ledger row checked inside its 30 days plus those 6 hours (an entry
-- expires, and the next run records it again). A header the 05:15 refresh
-- rewrote is in that set: its lines were never touched, so it is either whole
-- or in the ledger already. A created_at nobody stored reads as `mirrored_at`,
-- which only makes the order look younger. With neither an owner row nor a
-- flip time nothing dates the chain's writes: UNKNOWN, pass SOAK_ORDERS_FLIP_AT.
--
-- WHAT A FAIL MEANS
-- The ids are named. Read halfwritten_repair in /api/jobs — when it last ran
-- and what it found — then fetch one id from KeyCRM: line items that arrive
-- mean the chain's write dropped them; none, and no ledger row, mean the
-- record did not land.
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
since AS (
    SELECT COALESCE(owner.at, flag.flip_at) AS at,
           CASE WHEN owner.at IS NOT NULL THEN 'since the handover'
                ELSE 'since the flip' END AS said
    FROM owner CROSS JOIN flag
),
halfwritten AS (
    SELECT o.id, o.grand_total
    FROM bronze.orders o
    CROSS JOIN clock
    CROSS JOIN since
    WHERE o.mirrored_at >= since.at
      AND o.grand_total > 0
      AND COALESCE(o.created_at, o.mirrored_at) < clock.now - interval '6 hours'
      AND NOT EXISTS (SELECT 1 FROM bronze.order_products p WHERE p.order_id = o.id)
      AND NOT EXISTS (
          SELECT 1 FROM app.order_backfill_misses m
          WHERE m.order_id = o.id
            AND m.checked_at > clock.now - interval '30 days' - interval '6 hours')
),
agg AS (
    SELECT count(*) AS n,
           COALESCE(sum(grand_total), 0) AS total,
           (array_agg(id ORDER BY grand_total DESC, id))[1:10] AS sample
    FROM halfwritten
)
SELECT 'O4 half-written orders'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN 'pending' THEN 'PASS'
           WHEN '1' THEN CASE WHEN since.at IS NULL THEN 'UNKNOWN'
                              WHEN agg.n > 0 THEN 'FAIL'
                              ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 3 still writes DuckDB (KS_WRITE_ORDERS is not postgres and no latch marker)'
           WHEN 'pending' THEN 'not applicable: chain 3 has not latched (see O1)'
           WHEN '1' THEN CASE
               WHEN since.at IS NULL THEN
                   'no owner row and no flip time, so nothing dates the chain''s writes: '
                   'pass SOAK_ORDERS_FLIP_AT'
               WHEN agg.n > 0 THEN format(
                   '%s order(s) worth %s written %s with revenue, no line items and no ledger '
                   'row, created over 6 h ago, e.g. %s',
                   agg.n, agg.total, since.said, left(agg.sample::text, 120))
               ELSE 'every order written ' || since.said
                    || ' with revenue has its line items, or a ledger row from the repair' END
           ELSE format('orders_on=%s: see O1', flag.state)
       END AS detail
FROM flag CROSS JOIN agg CROSS JOIN since;
