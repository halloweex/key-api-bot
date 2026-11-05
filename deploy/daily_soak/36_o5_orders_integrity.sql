-- O5 (chain 3) — what is true of the order tables on their own.
--
-- It judged only under chain 3 (KS_WRITE_ORDERS=postgres latched, or its
-- latch) until step 14's S14-20: every chain writes Postgres since S14-00.
--
-- WHY IT EXISTS
-- The daily comparisons against DuckDB stand down for these tables once the
-- chain has them, so what is left is what holds of the Postgres copy alone —
-- `core/pg_chain_invariants.py`'s chain-3 group, asked here every morning of
-- the soak rather than waiting for the integrity run to reach the digest:
--   - no expense older than a day whose order bronze.orders does not hold.
--     The chain writes an order and its costs in one transaction from one
--     payload, so one past a day was written round it — or its order was
--     refused, or dropped for want of `ordered_at` while its costs were kept,
--     as DuckDB always kept them (`chain_expense_orphans`);
--   - no ledger row with a NULL `checked_at`. Postgres has no default there
--     (revision 0008), and the repairs' 30-day test cannot compare a NULL, so
--     that id is asked of KeyCRM again on every run.
-- The predicates are the watch's own statements; the integration test runs
-- both over the same rows and holds the counts equal.
--
-- WHAT A FAIL MEANS
-- A write went round the chain's writer, or an order it refused left its
-- costs behind. Nothing here repairs: find the writer first
-- (chain_expense_orphans and chain_required_column_null in the digest name
-- the ids).
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
orphans AS (
    SELECT e.id
    FROM bronze.expenses e
    CROSS JOIN clock
    WHERE e.mirrored_at < clock.now - interval '1 day'
      AND NOT EXISTS (SELECT 1 FROM bronze.orders o WHERE o.id = e.order_id)
),
facts AS (
    SELECT
        (SELECT count(*) FROM orphans) AS orphan_expenses,
        (SELECT (array_agg(id ORDER BY id))[1:10] FROM orphans) AS orphan_sample,
        (SELECT count(*) FROM app.order_backfill_misses WHERE checked_at IS NULL)
            AS null_checked
)
SELECT 'O5 orders integrity'::text AS "check",
       CASE WHEN f.orphan_expenses + f.null_checked > 0 THEN 'FAIL'
                          ELSE 'PASS' END AS verdict,
       format(
           '%s expense(s) older than a day with no order%s, %s ledger row(s) with no checked_at',
           f.orphan_expenses,
           CASE WHEN f.orphan_expenses > 0
                THEN ' (e.g. ' || left(f.orphan_sample::text, 100) || ')' ELSE '' END,
           f.null_checked) AS detail
FROM facts f;
