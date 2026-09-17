-- I3 (chain 1) — stock movements after the flip look like the ones before it.
--
-- Only under KS_WRITE_INVENTORY=postgres; the variables are I1's. The window is
-- the last 14 days, or since the flip if that is later.
--
-- WHY IT EXISTS
-- `stock_movements` is a delta against the previous `offer_stocks`. A Postgres
-- writer that started from an empty or wrong previous state would record every
-- offer as new — one `initial` movement per offer, about the size of the
-- catalogue — and nothing downstream would call that wrong; it is the only
-- record of a quantity ever changing. A movement with no `product_id` is the
-- writer failing to join the catalogue it now owns.
--
-- WHAT A FAIL MEANS
-- - a Kyiv day with `initial` movements numbering half the offer count or more:
--   the writer lost its previous state. Stop and compare with the day before
--   the flip before anything else writes.
-- - movements with no product: the writer's catalogue join is broken.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'inventory_on'::text AS state,
           NULLIF(:'inventory_flip_at', '')::timestamptz AS flip_at
),
win AS (
    SELECT GREATEST(COALESCE(flag.flip_at, clock.now - interval '14 days'),
                    clock.now - interval '14 days') AS since
    FROM clock CROSS JOIN flag
),
offers AS (
    SELECT count(*) AS n FROM bronze.offers
),
days AS (
    SELECT (m.recorded_at AT TIME ZONE 'Europe/Kyiv')::date AS kday,
           count(*) AS movements,
           count(*) FILTER (WHERE m.movement_type = 'initial') AS initial,
           count(*) FILTER (WHERE m.product_id IS NULL) AS no_product
    FROM app.stock_movements m, win, clock
    WHERE m.recorded_at > win.since AND m.recorded_at <= clock.now
    GROUP BY 1
),
agg AS (
    SELECT COALESCE(sum(movements), 0) AS movements,
           COALESCE(sum(no_product), 0) AS no_product,
           COALESCE(max(initial), 0) AS max_initial,
           string_agg(to_char(kday, 'DD.MM') || ' ' || initial, ', ' ORDER BY kday)
               FILTER (WHERE initial * 2 >= (SELECT n FROM offers) AND (SELECT n FROM offers) > 0)
               AS bursts
    FROM days
)
SELECT 'I3 inventory movements'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN '1' THEN CASE WHEN agg.bursts IS NOT NULL OR agg.no_product > 0
                              THEN 'FAIL' ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: KS_WRITE_INVENTORY is not postgres'
           WHEN '1' THEN
               format('%s movement(s) since %s Kyiv, most initial in a day %s (offers %s), %s without a product%s',
                      agg.movements,
                      to_char(win.since AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      agg.max_initial, offers.n, agg.no_product,
                      COALESCE('; initial bursts: ' || agg.bursts, ''))
           ELSE format('inventory_on=%s: see I1', flag.state)
       END AS detail
FROM flag CROSS JOIN win CROSS JOIN offers CROSS JOIN agg;
