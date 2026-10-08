-- P4a: one Silver row the integrity twins must name as missing.
-- The rehearsal's copy of production only (reh-pg); never a live database.
-- :grace is minutes: the row's header must have landed before the twins'
-- settle grace, or the check counts it as in flight and files nothing.
-- The largest by grand_total, so it leads the issue's sample.
WITH chosen AS (
    SELECT s.id
      FROM silver.orders s
      JOIN bronze.orders b USING (id)
     WHERE b.mirrored_at < now() - make_interval(mins => :grace)
     ORDER BY s.grand_total DESC NULLS LAST, s.id DESC
     LIMIT 1
)
DELETE FROM silver.orders s USING chosen WHERE s.id = chosen.id
RETURNING s.id;
