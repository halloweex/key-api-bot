-- soak:run-as ks_app
--
-- I5 (chain 1) — the stock movement id allocator is above every id.
--
-- Runs as ks_app, the sequence's owner, for E2's reason: `ks_readonly` cannot
-- read the sequence, and `pg_sequences.last_value` is NULL for a sequence
-- nothing has called yet. deploy/daily_soak.sh keeps the session read-only.
-- It judged only under KS_WRITE_INVENTORY=postgres or chain 1's latch until
-- step 14's S14-20: every chain writes Postgres since S14-00.
--
-- WHY IT EXISTS
-- `stock_movements.id` carried DuckDB's sequence for the whole replication
-- period; revision 0030 gave Postgres its own for the flip, and the writer
-- lifts it to MAX(id) inside every write (`_ensure_movement_id_floor`). Rows
-- that arrive any other way — the last hourly append before the flip, a
-- restore — can leave the sequence behind the table.
--
-- WHAT A FAIL MEANS
-- The next id the sequence hands out is not above MAX(id): a write that does
-- not go through the writer collides, and one that does lifts the floor first.
-- Find the path that inserted without it.
WITH seq AS (
    SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END AS next_id
    FROM app.stock_movements_id_seq
),
t AS (
    SELECT COALESCE(max(id), 0) AS max_id, count(*) AS n FROM app.stock_movements
)
SELECT 'I5 inventory allocator'::text AS "check",
       CASE WHEN seq.next_id > t.max_id THEN 'PASS' ELSE 'FAIL' END AS verdict,
       format('next id %s, max id %s over %s row(s)', seq.next_id, t.max_id, t.n) AS detail
FROM seq CROSS JOIN t;
