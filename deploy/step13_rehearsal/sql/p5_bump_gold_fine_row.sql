-- P5: one per-source Gold row one hryvnia off its roll-up, so that
-- `pg_gold_internal_check` has a `gold_rollup_mismatch` to find. Under the
-- stand-down nothing else in dq_mirror_landing can file that name.
-- The rehearsal's copy only (reh-pg).
WITH target AS (
    SELECT date, sales_type, source_id
      FROM gold.daily_revenue
     WHERE source_id IS NOT NULL AND revenue > 0
     ORDER BY date DESC, sales_type, source_id
     LIMIT 1
)
UPDATE gold.daily_revenue g
   SET revenue = g.revenue + 1
  FROM target t
 WHERE g.date = t.date AND g.sales_type = t.sales_type AND g.source_id = t.source_id
RETURNING g.date::text || '/' || g.sales_type || '/' || g.source_id;
