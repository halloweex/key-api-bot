-- B4 (chain 4) — the buyers the step owes KeyCRM a fetch for stay few.
--
-- It judged only under chain 4 (KS_WRITE_BUYERS=postgres with every reader
-- of the buyers on Postgres, or its latch) until step 14's S14-20: every
-- chain writes Postgres since S14-00.
--
-- WHY IT EXISTS
-- The step selects every buyer an order names that `bronze.buyers` does not
-- hold, or holds without a name (`DuckDBStore.get_missing_buyer_ids`), and
-- fetches up to 500 an hour. If its writes stopped landing where it selects
-- from, the same buyers would be selected and fetched every hour for ever,
-- spending KeyCRM quota and landing nothing. A backlog that stays small is the
-- loop not happening; one that is a day old is a buyer that never arrived.
--
-- HOW IT DECIDES
-- The selection's own predicate over `silver.orders`: FAIL above 50 owed —
-- about three days of new customers at once — or any owed for more than a day
-- (the `buyers_missing_for_orders` threshold).
--
-- WHAT A FAIL MEANS
-- Read buyer_sync in /api/health: `last_selected` against `last_written`, and
-- `last_skipped_bad`. A buyer Postgres refuses is skipped by id every hour and
-- named in the web log ('not written to Postgres').
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
owed AS (
    SELECT o.buyer_id, min(o.ordered_at) AS first_order_at
    FROM silver.orders o
    LEFT JOIN bronze.buyers b ON b.id = o.buyer_id
    WHERE o.buyer_id IS NOT NULL
      AND (b.id IS NULL OR b.full_name IS NULL OR b.full_name = '')
    GROUP BY o.buyer_id
),
facts AS (
    SELECT (SELECT count(*) FROM owed) AS owed,
           (SELECT count(*) FROM owed
            WHERE first_order_at < clock.now - interval '24 hours') AS day_old
    FROM clock
)
SELECT 'B4 buyers selection backlog'::text AS "check",
       CASE WHEN f.owed > 50 OR f.day_old > 0 THEN 'FAIL' ELSE 'PASS' END AS verdict,
       format('%s buyer(s) owed a fetch (limit 50), %s of them for over a day',
              f.owed, f.day_old) AS detail
FROM facts f;
