-- I2 (chain 1) — the chain's own sync watermarks keep moving.
--
-- Only under KS_WRITE_INVENTORY=postgres; the variables are I1's.
--
-- WHY IT EXISTS
-- Under the Postgres writer, `last_sync_offers` and `last_sync_stocks` live in
-- `meta.chain_watermarks` (revision 0032) rather than DuckDB's sync_metadata.
-- A writer that stopped would leave them standing still and the inventory tab
-- quietly aging, because I1 only proves the copy stayed away.
--
-- WHAT A FAIL MEANS
-- A key is missing, or has not moved for 75 min: the inventory sync is not
-- writing Postgres. Read the web log for the inventory sync job.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'inventory_on'::text AS state
),
wanted (key) AS (
    VALUES ('last_sync_offers'), ('last_sync_stocks')
),
judged AS (
    SELECT w.key, c.key AS present,
           floor(extract(epoch FROM clock.now - c.updated_at) / 60)::bigint AS age_min
    FROM wanted w
    CROSS JOIN clock
    LEFT JOIN meta.chain_watermarks c ON c.key = w.key
),
agg AS (
    SELECT count(*) FILTER (WHERE present IS NULL OR age_min >= 75) AS n,
           string_agg(format('%s %s', key,
                             CASE WHEN present IS NULL THEN 'missing'
                                  ELSE age_min || ' min old' END),
                      '; ' ORDER BY key) AS listed
    FROM judged
)
SELECT 'I2 inventory watermarks'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN 'FAIL' ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: KS_WRITE_INVENTORY is not postgres'
           WHEN '1' THEN agg.listed || ' (limit 75 min)'
           ELSE format('inventory_on=%s: see I1', flag.state)
       END AS detail
FROM flag CROSS JOIN agg;
