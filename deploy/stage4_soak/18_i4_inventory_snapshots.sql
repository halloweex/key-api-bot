-- I4 (chain 1) — the daily snapshots and first_seen_at survive the flip.
--
-- Only under KS_WRITE_INVENTORY=postgres; the variables are I1's.
--
-- WHY IT EXISTS
-- `sku_inventory_status.first_seen_at` is carried forward out of the table's
-- own previous contents, and the 01:00 snapshot writes one
-- `inventory_history` row and one `inventory_sku_history` row per offer per
-- day. A writer that rebuilt the status table from nothing would stamp every
-- SKU as first seen on the flip date — the one column that makes the table
-- irreplaceable — and a snapshot job that stopped or doubled would leave a gap
-- or a duplicate nobody reads until a chart looks odd.
--
-- HOW IT DECIDES
-- Days from three before the flip (or three before today, without a flip
-- time) up to today, judging today only after 01:15 Kyiv:
-- - `inventory_history` holds exactly one row;
-- - `inventory_sku_history` holds within 10% of the status table's row count.
-- With a flip time, also: fewer than half of all SKUs first seen on or after
-- the flip date.
--
-- WHAT A FAIL MEANS
-- A missing or doubled day, a partial per-SKU snapshot, or `first_seen_at`
-- reset. The last is not recoverable from KeyCRM: stop the writer and restore
-- the column from the flip-day baseline the checklist saves.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'inventory_on'::text AS state,
           NULLIF(:'inventory_flip_at', '')::timestamptz AS flip_at
),
today AS (
    SELECT (clock.now AT TIME ZONE 'Europe/Kyiv')::date AS kday,
           (clock.now AT TIME ZONE 'Europe/Kyiv')::time AS kyiv_time,
           (flag.flip_at AT TIME ZONE 'Europe/Kyiv')::date AS flip_day
    FROM clock CROSS JOIN flag
),
status AS (
    SELECT count(*) AS n,
           count(*) FILTER (WHERE first_seen_at >= (SELECT flip_day FROM today)) AS since_flip
    FROM app.sku_inventory_status
),
days AS (
    SELECT today.kday - k AS kday
    FROM today,
         generate_series(0, GREATEST(today.kday - COALESCE(today.flip_day, today.kday) + 3, 3)) AS k
    WHERE today.kday - k >= today.kday - 14
      AND (k > 0 OR today.kyiv_time >= time '01:15')
),
judged AS (
    SELECT d.kday,
           (SELECT count(*) FROM app.inventory_history h WHERE h.date = d.kday) AS totals,
           (SELECT count(*) FROM app.inventory_sku_history h WHERE h.date = d.kday) AS sku_rows
    FROM days d
),
agg AS (
    SELECT count(*) AS n_days,
           string_agg(format('%s totals=%s sku_rows=%s', to_char(j.kday, 'DD.MM'), j.totals, j.sku_rows),
                      ', ' ORDER BY j.kday)
               FILTER (WHERE j.totals <> 1
                          OR j.sku_rows < status.n * 0.9
                          OR j.sku_rows > status.n * 1.1) AS bad_days
    FROM judged j CROSS JOIN status
    GROUP BY status.n
)
SELECT 'I4 inventory snapshots'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN '1' THEN
               CASE WHEN agg.bad_days IS NOT NULL THEN 'FAIL'
                    WHEN flag.flip_at IS NOT NULL AND status.n > 0
                         AND status.since_flip * 2 >= status.n THEN 'FAIL'
                    ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 1 still writes DuckDB (KS_WRITE_INVENTORY is not postgres and no latch marker)'
           WHEN '1' THEN
               format('%s day(s) judged against %s SKUs%s%s',
                      COALESCE(agg.n_days, 0), status.n,
                      CASE WHEN flag.flip_at IS NULL THEN '; no flip time given, first_seen_at not judged'
                           ELSE format('; %s SKU(s) first seen since the flip', status.since_flip) END,
                      COALESCE('; bad days: ' || agg.bad_days, ''))
           ELSE format('inventory_on=%s: see I1', flag.state)
       END AS detail
FROM flag CROSS JOIN status LEFT JOIN agg ON true;
