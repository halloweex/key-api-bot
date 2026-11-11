-- B3 (chain 4) — every buyer has a verdict, and no human decision was lost.
--
-- The one variable is deploy/daily_soak.sh's:
--   buyers_override_floor  the fewest human overrides there may be
--                          (SOAK_BUYERS_OVERRIDE_FLOOR, the count a previous
--                          report printed), else empty — no floor
-- It judged only under chain 4 (KS_WRITE_BUYERS=postgres with every reader
-- of the buyers on Postgres, or its latch) until step 14's S14-20: every
-- chain writes Postgres since S14-00. The floor was the count on chain 4's
-- flip day until then.
--
-- WHY IT EXISTS
-- A buyer's verdict is written with its name, in the same
-- transaction, and the hourly derivation in Postgres catches what that missed.
-- A buyer with none is in no gendered SMS audience and nothing on the page
-- says so. A human override is the one verdict nothing can re-derive — the
-- `managers.is_retail` lesson — so their number may grow and must not fall.
--
-- HOW IT DECIDES
-- A buyer written more than 90 minutes ago with no verdict at all (one
-- missed hourly derivation); stale rules versions are `buyers_without_verdict`'s
-- to name, since only the application knows the current version. Overrides
-- against the floor when one is given.
--
-- WHAT A FAIL MEANS
-- Missing verdicts: grep the web log for 'gender derivation failed'. Fewer
-- overrides than the floor: something rewrote a human's decision — stop and
-- read before anything else runs.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
floor AS (
    SELECT NULLIF(:'buyers_override_floor', '')::bigint AS n
),
facts AS (
    SELECT (SELECT count(*)
            FROM bronze.buyers b
            WHERE b.mirrored_at < clock.now - interval '90 minutes'
              AND NOT EXISTS (SELECT 1 FROM app.buyer_gender g WHERE g.buyer_id = b.id))
               AS without_verdict,
           (SELECT count(*) FROM app.buyer_gender WHERE override_by_human) AS overrides
    FROM clock
)
SELECT 'B3 buyers verdicts'::text AS "check",
       CASE
           WHEN f.without_verdict > 0 THEN 'FAIL'
           WHEN floor.n IS NOT NULL AND f.overrides < floor.n THEN 'FAIL'
           ELSE 'PASS' END AS verdict,
       format(
           '%s buyer(s) written over 90 min ago with no verdict; %s human override(s)%s',
           f.without_verdict, f.overrides,
           CASE WHEN floor.n IS NULL THEN ' (no floor given)'
                ELSE format(' against a floor of %s', floor.n) END) AS detail
FROM floor CROSS JOIN facts f;
