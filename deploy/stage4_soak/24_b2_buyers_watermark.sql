-- B2 (chain 4) — the buyers step's own watermark keeps moving.
--
-- Only under chain 4; the variables are B1's.
--
-- WHY IT EXISTS
-- Under the chain `last_sync_buyers` lives in `meta.chain_watermarks` and the
-- buyers step is the only writer of buyers: a step that stopped leaves new
-- customers landing nowhere, and B1 only proves the copies stayed away.
--
-- HOW IT DECIDES
-- The stored VALUE, which is what the step wrote and what
-- `core/pg_chain_invariants.py` and `_freshness_check` judge — not the row's
-- `updated_at`. 90 minutes: the step falls due hourly and retries ten minutes
-- after a failure, which is also the canary's `buyer_sync_stalled` limit.
--
-- WHAT A FAIL MEANS
-- Missing, unreadable or older than 90 minutes: read buyer_sync in /api/health
-- (the error class and the retry window), then the web log for 'Buyer'.
-- Missing inside 90 minutes of a given flip time is the first step not due yet.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'buyers_on'::text AS state,
           NULLIF(:'buyers_flip_at', '')::timestamptz AS flip_at
),
mark AS (
    SELECT max(value) AS value, count(*) AS present
    FROM meta.chain_watermarks
    WHERE key = 'last_sync_buyers'
),
judged AS (
    SELECT mark.present > 0 AS present, mark.value,
           CASE WHEN mark.present > 0 AND pg_input_is_valid(mark.value, 'timestamptz')
                THEN floor(extract(epoch FROM clock.now - mark.value::timestamptz) / 60)::bigint
           END AS age_min,
           flag.flip_at IS NOT NULL AND clock.now - flag.flip_at < interval '90 minutes' AS just_flipped
    FROM mark CROSS JOIN clock CROSS JOIN flag
)
SELECT 'B2 buyers watermark'::text AS "check",
       CASE flag.state
           WHEN '0' THEN 'PASS'
           WHEN 'held' THEN 'PASS'
           WHEN '1' THEN CASE
               WHEN NOT j.present THEN CASE WHEN j.just_flipped THEN 'PASS' ELSE 'FAIL' END
               WHEN j.age_min IS NULL OR j.age_min >= 90 THEN 'FAIL'
               ELSE 'PASS' END
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN 'not applicable: chain 4 still writes DuckDB (KS_WRITE_BUYERS is not postgres and no latch marker)'
           WHEN 'held' THEN 'not applicable: chain 4 is held on DuckDB (see B1)'
           WHEN '1' THEN CASE
               WHEN NOT j.present AND j.just_flipped THEN
                   'last_sync_buyers not written yet, inside 90 min of the flip'
               WHEN NOT j.present THEN
                   'last_sync_buyers is not in meta.chain_watermarks: no buyers step has completed under the chain'
               WHEN j.age_min IS NULL THEN
                   format('last_sync_buyers holds %s, which is not a timestamp', left(j.value, 60))
               ELSE format('last_sync_buyers moved %s min ago (limit 90)', j.age_min) END
           ELSE format('buyers_on=%s: see B1', flag.state)
       END AS detail
FROM flag CROSS JOIN judged j;
