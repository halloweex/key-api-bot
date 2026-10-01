-- B3 (chain 4) — every buyer has a verdict, and no human decision was lost.
--
-- Only under chain 4; the variables are B1's, plus:
--   buyers_override_floor  how many verdicts a human had set on flip day
--                          (SOAK_BUYERS_OVERRIDE_FLOOR), else empty
--
-- WHY IT EXISTS
-- Under the chain a buyer's verdict is written with its name, in the same
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
-- overrides than on flip day: something rewrote a human's decision — stop and
-- read before anything else runs.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'buyers_on'::text AS state,
           NULLIF(:'buyers_override_floor', '')::bigint AS floor
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
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN 'held' THEN 'PASS'
           WHEN '1' THEN CASE
               WHEN f.without_verdict > 0 THEN 'FAIL'
               WHEN flag.floor IS NOT NULL AND f.overrides < flag.floor THEN 'FAIL'
               ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 4 still writes DuckDB (KS_WRITE_BUYERS is not postgres and no latch marker)'
           WHEN 'held' THEN 'not applicable: chain 4 is held on DuckDB (see B1)'
           WHEN '1' THEN format(
               '%s buyer(s) written over 90 min ago with no verdict; %s human override(s)%s',
               f.without_verdict, f.overrides,
               CASE WHEN flag.floor IS NULL THEN ' (no floor given)'
                    ELSE format(' against %s on flip day', flag.floor) END)
           ELSE format('buyers_on=%s: see B1', flag.state)
       END AS detail
FROM flag CROSS JOIN facts f;
