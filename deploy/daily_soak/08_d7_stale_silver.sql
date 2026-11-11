-- D7 — stale Silver rows.
--
-- WHY IT EXISTS
-- The derivation's validation compares Silver with bronze by row count and by
-- revenue checksum, both over the whole table. A Silver row that kept an old
-- status while its bronze row moved on passes both whenever the change nets
-- out — and nothing else checks Silver row by row in Postgres before DN-13.
--
-- HOW IT DECIDES
-- A bronze order mirrored before the latest error-free run started must be in
-- that run's Silver, with the same status, total, buyer and manager. One
-- statement, deliberately, and outside REPEATABLE READ: the rebuild is a
-- TRUNCATE plus INSERT, and TRUNCATE is not MVCC-safe — a transaction snapshot
-- taken before it commits reads Silver as empty afterwards. A single
-- READ COMMITTED statement takes its locks before its snapshot, so it waits
-- for the rebuild instead.
--
-- WHAT A FAIL MEANS
-- - `stale_rows > 0`: Silver holds a row the latest rebuild should have
--   replaced. Always a defect.
-- - rows missing from Silver while nothing is owed: an order reached bronze,
--   no mark was raised, and no rebuild has covered it — read D4 and D5.
--   While a rebuild is owed the same count is simply in flight.
WITH last_ok AS (
    SELECT max(started_at) AS started_at
    FROM meta.derivation_runs
    WHERE layer = 'warehouse' AND error IS NULL
),
counts AS (
    SELECT (SELECT count(*) FROM bronze.orders) - (SELECT count(*) FROM silver.orders)
               AS missing_in_silver,
           (SELECT count(*)
              FROM bronze.orders b
              JOIN silver.orders s USING (id)
             WHERE (b.status_id, b.grand_total, b.buyer_id, b.manager_id)
                   IS DISTINCT FROM (s.status_id, s.grand_total, s.buyer_id, s.manager_id)
               AND b.mirrored_at < (SELECT started_at FROM last_ok)) AS stale_rows,
           (SELECT requested - built FROM meta.derivation_signal WHERE layer = 'warehouse')
               AS owed
)
SELECT 'D7 stale Silver rows'::text AS "check",
       CASE
           WHEN stale_rows > 0 THEN 'FAIL'
           WHEN missing_in_silver <> 0 AND COALESCE(owed, 0) = 0 THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       format('stale rows %s; bronze minus Silver %s; owed %s%s',
              stale_rows, missing_in_silver, COALESCE(owed::text, '?'),
              CASE WHEN missing_in_silver <> 0 AND COALESCE(owed, 0) > 0
                   THEN ' — in flight' ELSE '' END) AS detail
FROM counts;
