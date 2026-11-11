-- I4 (chain 1) — the daily snapshots are taken, once a day, whole.
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
-- The three days before today and today, judging today only after 01:15 Kyiv:
-- - `inventory_history` holds exactly one row;
-- - `inventory_sku_history` holds within 10% of the status table's row count.
-- Until step 14's S14-20 it judged only under KS_WRITE_INVENTORY=postgres or
-- chain 1's latch, from three days before a given flip time, and with one it
-- also judged `first_seen_at` against the flip date. Every chain writes
-- Postgres since S14-00 and the flip is weeks behind; a reset `first_seen_at`
-- is the standing watch's `chain_first_seen_reset`, judged four times a day.
--
-- WHAT A FAIL MEANS
-- A missing or doubled day, or a partial per-SKU snapshot.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
today AS (
    SELECT (clock.now AT TIME ZONE 'Europe/Kyiv')::date AS kday,
           (clock.now AT TIME ZONE 'Europe/Kyiv')::time AS kyiv_time
    FROM clock
),
status AS (
    SELECT count(*) AS n FROM app.sku_inventory_status
),
days AS (
    SELECT today.kday - k AS kday
    FROM today, generate_series(0, 3) AS k
    WHERE k > 0 OR today.kyiv_time >= time '01:15'
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
       CASE WHEN agg.bad_days IS NOT NULL THEN 'FAIL' ELSE 'PASS' END AS verdict,
       format('%s day(s) judged against %s SKUs%s',
              COALESCE(agg.n_days, 0), status.n,
              COALESCE('; bad days: ' || agg.bad_days, '')) AS detail
FROM status LEFT JOIN agg ON true;
