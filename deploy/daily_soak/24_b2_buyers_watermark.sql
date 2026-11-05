-- B2 (chain 4) — the buyers step's own watermark keeps moving.
--
-- WHY IT EXISTS
-- `last_sync_buyers` lives in `meta.chain_watermarks`, and chain 4's buyers
-- step is the only writer of buyers: a step that stopped leaves new customers
-- landing nowhere, and nothing else on this report would say so.
-- It judged only under chain 4 (KS_WRITE_BUYERS=postgres with every reader
-- of the buyers on Postgres, or its latch) until step 14's S14-20: every
-- chain writes Postgres since S14-00.
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
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
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
           END AS age_min
    FROM mark CROSS JOIN clock
)
SELECT 'B2 buyers watermark'::text AS "check",
       CASE
           WHEN NOT j.present THEN 'FAIL'
           WHEN j.age_min IS NULL OR j.age_min >= 90 THEN 'FAIL'
           ELSE 'PASS' END AS verdict,
       CASE
           WHEN NOT j.present THEN
               'last_sync_buyers is not in meta.chain_watermarks: no buyers step has completed under the chain'
           WHEN j.age_min IS NULL THEN
               format('last_sync_buyers holds %s, which is not a timestamp', left(j.value, 60))
           ELSE format('last_sync_buyers moved %s min ago (limit 90)', j.age_min) END AS detail
FROM judged j;
