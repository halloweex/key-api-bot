-- I1 (chain 1) — the hourly copy stays stood down for the six inventory tables.
--
-- Only while chain 1 writes Postgres. SQL cannot read the web container's
-- environment or its latch marker, so deploy/stage4_soak.sh reads both and
-- passes two psql variables:
--   inventory_on       1 (KS_WRITE_INVENTORY=postgres, OR the chain is latched
--                      whatever the variable says — DN-06, which is how
--                      `writes_postgres()` itself resolves it), 0 (duckdb or
--                      unset and not latched), invalid, unknown
--   inventory_flip_at  when the flag was flipped, if the operator gave it
--                      (SOAK_INVENTORY_FLIP_AT), else empty
-- With 0 every other I-check is a "not applicable" PASS — which is why the
-- latch has to be in that 1: latched with the variable put back is precisely
-- the state an operator reaches while believing they have rolled the chain
-- back, and reading the flag alone would report the whole chain-1 half as PASS
-- through it.
--
-- WHY IT EXISTS
-- E1's reason, six tables over: under the Postgres writer, a full replace or an
-- append out of a DuckDB that no longer sees the writes would roll back or
-- duplicate the only record of a stock change. `replicate_operational` stamps
-- `last_ok_at` on every table it writes, so a stamp after the handover means it
-- wrote one.
--
-- HOW IT DECIDES
-- O1's clock (chain 3's review, 2026-10-10). The handover is the chain's own
-- owner rows when they exist (written in the first writing transaction, on
-- Postgres's clock — the clock the copy stamps with), else the operator's flip
-- time, else the last 75 minutes. A `last_ok_at`, or a failure, after it is a
-- copy that did not stand down — except in that last case, which is UNKNOWN:
-- the copy stamps all six tables every hour until the flip, so with neither an
-- owner row nor a flip time a healthy flip reads a stamp inside the window
-- until the chain's first write. Pass SOAK_INVENTORY_FLIP_AT on flip day.
--
-- A failure counts for the day it was stamped, like the rest of the report.
-- Only a successful copy resets `failures_since_ok`, and under the chain the
-- copy never succeeds: it stamps the six tables failing while the flag and the
-- latch disagree, then stands down silently once they agree — so the count
-- would stand for ever and FAIL a cause long gone. A `last_ok_at` after the
-- handover is DuckDB's rows already copied over the chain's, which no later
-- hour undoes, and it stays.
--
-- With inventory_on=0 the owner rows are read too. Rows naming one of chain
-- 1's tables with no marker and the flag at duckdb are the marker lost, not a
-- chain that never moved: the stock step follows the flag back to DuckDB, and
-- nothing carries what it writes to Postgres — the hourly copy stands down on
-- the owner rows, and stamps the six tables failing to say so — so every
-- reader of the inventory in Postgres stops moving, and the daily
-- `chain_latch_disagrees` is the page that names why. Chain 1 is latched in
-- production since 2026-09-30. I2–I5 stay "not applicable"; this one FAILs.
--
-- WHAT A FAIL MEANS
-- - `invalid`: the flag is a value no chain understands; DN-01 stands the chain
--   down, and nothing is being written anywhere by design. Fix the .env line.
-- - owner rows and inventory_on=0: the marker is lost. Restore
--   data/write-chain-owners/pg_inventory_write, else
--   scripts/chain_copy_back.py inventory.
-- - a table written after the handover, or failing after it within the day:
--   the stand-down has lapsed, or the latch and the flag disagree. Read the
--   error — `owned by Postgres since` is the latch with its flag put back, `no
--   local marker` a lost marker; scripts/chain_copy_back.py inventory is the
--   way back from either, never the flag.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'inventory_on'::text AS state,
           NULLIF(:'inventory_flip_at', '')::timestamptz AS flip_at
),
owner AS (
    SELECT min(updated_at) AS at
    FROM meta.chain_watermarks
    WHERE key IN ('owner:bronze.offers', 'owner:bronze.offer_stocks',
                  'owner:app.stock_movements', 'owner:app.sku_inventory_status',
                  'owner:app.inventory_sku_history', 'owner:app.inventory_history')
),
since AS (
    SELECT COALESCE(owner.at, flag.flip_at, clock.now - interval '75 minutes') AS at,
           CASE WHEN owner.at IS NOT NULL THEN 'since the handover'
                WHEN flag.flip_at IS NOT NULL THEN 'since the flip'
                ELSE 'in the last 75 min (no owner row and no flip time: pass SOAK_INVENTORY_FLIP_AT)' END AS said,
           owner.at IS NULL AND flag.flip_at IS NULL AS guessed
    FROM owner CROSS JOIN flag CROSS JOIN clock
),
wanted (table_name) AS (
    VALUES ('bronze.offers'), ('bronze.offer_stocks'), ('app.stock_movements'),
           ('app.sku_inventory_status'), ('app.inventory_sku_history'),
           ('app.inventory_history')
),
judged AS (
    SELECT w.table_name,
           CASE
               WHEN s.failures_since_ok > 0 AND s.last_attempted_at > since.at
                    AND s.last_attempted_at > clock.now - interval '24 hours' THEN
                   format('failing (%s): %s', s.failures_since_ok,
                          left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 120))
               WHEN s.last_ok_at > since.at THEN
                   format('written by the copy at %s Kyiv',
                          to_char(s.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
           END AS problem
    FROM wanted w
    CROSS JOIN since
    CROSS JOIN clock
    LEFT JOIN meta.mirror_state s ON s.table_name = w.table_name
),
agg AS (
    SELECT count(*) FILTER (WHERE problem IS NOT NULL) AS n,
           string_agg(table_name || ' ' || problem, '; ' ORDER BY table_name)
               FILTER (WHERE problem IS NOT NULL) AS listed
    FROM judged
)
SELECT 'I1 inventory copy stood down'::text AS "check",
       CASE flag.state
           WHEN '0' THEN CASE WHEN owner.at IS NULL THEN 'PASS' ELSE 'FAIL' END
           WHEN '1' THEN CASE WHEN agg.n = 0 THEN 'PASS'
                              WHEN since.guessed THEN 'UNKNOWN'
                              ELSE 'FAIL' END
           WHEN 'invalid' THEN 'FAIL'
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN CASE
               WHEN owner.at IS NULL THEN
                   'not applicable: chain 1 still writes DuckDB (KS_WRITE_INVENTORY is not postgres and no latch marker)'
               ELSE format(
                   'chain 1 owns its tables in Postgres since %s Kyiv (owner rows), and '
                   'data/write-chain-owners/pg_inventory_write is gone with KS_WRITE_INVENTORY not '
                   'postgres: the marker is lost, the stock step writes DuckDB again and the '
                   'hourly copy stands down on the owner rows, so the inventory in Postgres stops '
                   'moving (chain_latch_disagrees). Restore the marker, else '
                   'scripts/chain_copy_back.py inventory',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN left(agg.listed || ' ' || since.said, 500)
                              ELSE 'none of the six tables written by the copy ' || since.said END
           WHEN 'invalid' THEN
               'KS_WRITE_INVENTORY is set to a value no chain understands: chain 1 is stood down'
           WHEN 'unknown' THEN 'could not read KS_WRITE_INVENTORY from the web container'
           ELSE format('inventory_on=%s is not one of 0, 1, invalid, unknown', flag.state)
       END AS detail
FROM flag CROSS JOIN agg CROSS JOIN since CROSS JOIN owner;
