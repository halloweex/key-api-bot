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
-- With 0 every I-check is a "not applicable" PASS — which is why the latch has
-- to be in that 1: latched with the variable put back is precisely the state
-- an operator reaches while believing they have rolled the chain back, and
-- reading the flag alone would report the whole chain-1 half as PASS through
-- it.
--
-- WHY IT EXISTS
-- E1's reason, six tables over: under the Postgres writer, a full replace or an
-- append out of a DuckDB that no longer sees the writes would roll back or
-- duplicate the only record of a stock change. `replicate_operational` stamps
-- `last_ok_at` on every table it writes, so a stamp after the flip means it
-- wrote one.
--
-- WHAT A FAIL MEANS
-- - `invalid`: the flag is a value no chain understands; DN-01 stands the chain
--   down, and nothing is being written anywhere by design. Fix the .env line.
-- - a table stamped after the flip — or, without a flip time, in the last
--   75 min — or failing: the stand-down has lapsed. Without a flip time this
--   reads FAIL for the first 75 min after the flip by construction; pass
--   SOAK_INVENTORY_FLIP_AT on flip day.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'inventory_on'::text AS state,
           NULLIF(:'inventory_flip_at', '')::timestamptz AS flip_at
),
wanted (table_name) AS (
    VALUES ('bronze.offers'), ('bronze.offer_stocks'), ('app.stock_movements'),
           ('app.sku_inventory_status'), ('app.inventory_sku_history'),
           ('app.inventory_history')
),
judged AS (
    SELECT w.table_name,
           CASE
               WHEN s.failures_since_ok > 0 THEN
                   format('failing (%s): %s', s.failures_since_ok,
                          left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 120))
               WHEN flag.flip_at IS NOT NULL AND s.last_ok_at > flag.flip_at THEN
                   format('written by the copy at %s Kyiv, after the flip',
                          to_char(s.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
               WHEN flag.flip_at IS NULL AND s.last_ok_at > clock.now - interval '75 minutes' THEN
                   format('written by the copy %s min ago',
                          floor(extract(epoch FROM clock.now - s.last_ok_at) / 60))
           END AS problem
    FROM wanted w
    CROSS JOIN clock
    CROSS JOIN flag
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
           WHEN '0' THEN 'PASS'
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN 'FAIL' ELSE 'PASS' END
           WHEN 'invalid' THEN 'FAIL'
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 1 still writes DuckDB (KS_WRITE_INVENTORY is not postgres and no latch marker)'
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN left(agg.listed, 500)
                              ELSE format('none of the six tables written by the copy %s',
                                          CASE WHEN flag.flip_at IS NULL
                                               THEN 'in the last 75 min (no flip time given)'
                                               ELSE 'since the flip' END) END
           WHEN 'invalid' THEN
               'KS_WRITE_INVENTORY is set to a value no chain understands: chain 1 is stood down'
           WHEN 'unknown' THEN 'could not read KS_WRITE_INVENTORY from the web container'
           ELSE format('inventory_on=%s is not one of 0, 1, invalid, unknown', flag.state)
       END AS detail
FROM flag CROSS JOIN agg;
