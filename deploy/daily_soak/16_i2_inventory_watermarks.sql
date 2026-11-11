-- I2 (chain 1) — the chain's own sync watermarks keep moving.
--
-- WHY IT EXISTS
-- `last_sync_offers` and `last_sync_stocks` live in `meta.chain_watermarks`
-- (revision 0032), written by chain 1, the inventory's only writer. A writer
-- that stopped would leave them standing still and the inventory tab quietly
-- aging, and nothing else on this report would say so.
--
-- It judged only under KS_WRITE_INVENTORY=postgres or chain 1's latch until
-- step 14's S14-20: every chain writes Postgres since S14-00.
--
-- WHAT A FAIL MEANS
-- A key is missing, or has not moved for 75 min: the inventory sync is not
-- writing Postgres. Read the web log for the inventory sync job.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
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
       CASE WHEN agg.n > 0 THEN 'FAIL' ELSE 'PASS' END AS verdict,
       agg.listed || ' (limit 75 min)' AS detail
FROM agg;
