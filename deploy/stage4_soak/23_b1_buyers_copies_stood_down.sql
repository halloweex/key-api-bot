-- B1 (chain 4) — nothing ships DuckDB's buyers over the chain's.
--
-- Only while chain 4 writes Postgres. deploy/stage4_soak.sh reads the web
-- container's flags and latch marker and passes:
--   buyers_on        1 (KS_WRITE_BUYERS=postgres with every buyer reader on
--                    postgres, OR the chain is latched whatever the variable
--                    says — DN-06), 0 (duckdb or unset and no latch marker), held
--                    (the flag says postgres and a reader does not, so the
--                    chain is held on DuckDB: `pg_buyers_write.unmet_precondition`),
--                    invalid, unknown
--   buyers_held_by   the first reader that holds it, when held
--   buyers_flip_at   when the flag was flipped, if the operator gave it
--                    (SOAK_BUYERS_FLIP_AT), else empty
--
-- WHY IT EXISTS
-- Two shippers wrote these tables before the chain: the buyers mirror
-- (`core/pg_buyers.py`, both bronze tables, on every sync that fetched buyers)
-- and the hourly `replicate_operational` (`app.buyer_gender`, a full replace).
-- Under the chain both must stand down, or DuckDB's frozen copy goes over what
-- the chain wrote — the verdicts once an hour, the buyers on the next sync.
-- Each stamps `meta.mirror_state.last_ok_at` on every table it writes.
--
-- HOW IT DECIDES
-- The handover is the chain's own owner rows when they exist (written in the
-- first writing transaction, on Postgres's clock — the clock the shippers
-- stamp with), else the operator's flip time, else the last 75 minutes. A
-- stamp, or a failure, after that is a shipper that did not stand down —
-- except in that last case, which is UNKNOWN: the hourly copy stamps
-- app.buyer_gender every run until the flip, so with neither an owner row
-- nor a flip time a healthy flip reads a stamp inside the window until the
-- chain's first write (review of PR-3). Pass SOAK_BUYERS_FLIP_AT.
--
-- A failure counts for the day it was stamped, like the rest of the report
-- (chain 3's review): only a successful copy resets `failures_since_ok`, and
-- under the chain the copy stamps failing while the flag and the latch
-- disagree, then stands down silently once they agree — so the count stood
-- for ever. A `last_ok_at` after the handover is rows already shipped over
-- the chain's, and stays.
--
-- With buyers_on=0 the owner rows are read too: rows naming one of chain 4's
-- tables with no marker and the flag at duckdb are the marker lost. The buyers
-- step follows the flag back to DuckDB, and nothing carries what it writes to
-- Postgres — the buyers mirror and the hourly copy both stand down on the
-- owner rows — so every reader of the buyers in Postgres stops moving, with
-- `chain_latch_disagrees` the only page. B2–B5 stay "not applicable"; this
-- one FAILs.
--
-- WHAT A FAIL MEANS
-- - held: the flip did not move the chain. Set the named reader to postgres,
--   or take the flag back; nothing was written to Postgres by the chain.
-- - invalid: KS_WRITE_BUYERS is a value no chain understands; the chain is
--   stood down and nothing is written anywhere. Fix the .env line.
-- - owner rows and buyers_on=0: the marker is lost. Restore
--   data/write-chain-owners/pg_buyers_write, else
--   scripts/chain_copy_back.py buyers.
-- - a table written or failing after the handover: a shipper is writing
--   DuckDB's copy over the chain's. Read B5, then the web log.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'buyers_on'::text AS state,
           NULLIF(:'buyers_flip_at', '')::timestamptz AS flip_at,
           NULLIF(:'buyers_held_by', '') AS held_by
),
owner AS (
    SELECT min(updated_at) AS at
    FROM meta.chain_watermarks
    WHERE key IN ('owner:bronze.buyers', 'owner:bronze.buyer_contacts',
                  'owner:app.buyer_gender')
),
since AS (
    SELECT COALESCE(owner.at, flag.flip_at, clock.now - interval '75 minutes') AS at,
           CASE WHEN owner.at IS NOT NULL THEN 'since the handover'
                WHEN flag.flip_at IS NOT NULL THEN 'since the flip'
                ELSE 'in the last 75 min (no owner row and no flip time: pass SOAK_BUYERS_FLIP_AT)' END AS said,
           owner.at IS NULL AND flag.flip_at IS NULL AS guessed
    FROM owner CROSS JOIN flag CROSS JOIN clock
),
wanted (table_name) AS (
    VALUES ('bronze.buyers'), ('bronze.buyer_contacts'), ('app.buyer_gender')
),
judged AS (
    SELECT w.table_name,
           CASE
               WHEN s.failures_since_ok > 0 AND s.last_attempted_at > since.at
                    AND s.last_attempted_at > clock.now - interval '24 hours' THEN
                   format('failing (%s): %s', s.failures_since_ok,
                          left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 120))
               WHEN s.last_ok_at > since.at THEN
                   format('written by a copy at %s Kyiv',
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
SELECT 'B1 buyers copies stood down'::text AS "check",
       CASE flag.state
           WHEN '0' THEN CASE WHEN owner.at IS NULL THEN 'PASS' ELSE 'FAIL' END
           WHEN '1' THEN CASE WHEN agg.n = 0 THEN 'PASS'
                              WHEN since.guessed THEN 'UNKNOWN'
                              ELSE 'FAIL' END
           WHEN 'held' THEN 'FAIL'
           WHEN 'invalid' THEN 'FAIL'
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN CASE
               WHEN owner.at IS NULL THEN
                   'not applicable: chain 4 still writes DuckDB (KS_WRITE_BUYERS is not postgres and no latch marker)'
               ELSE format(
                   'chain 4 owns its tables in Postgres since %s Kyiv (owner rows), and '
                   'data/write-chain-owners/pg_buyers_write is gone with KS_WRITE_BUYERS not '
                   'postgres: the marker is lost, the buyers step writes DuckDB again and '
                   'nothing ships it — the mirror and the hourly copy stand down on the owner '
                   'rows — so the buyers in Postgres stop moving (chain_latch_disagrees). '
                   'Restore the marker, else scripts/chain_copy_back.py buyers',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN left(agg.listed || ' ' || since.said, 500)
                              ELSE 'none of the three tables written by a copy ' || since.said END
           WHEN 'held' THEN format(
               'KS_WRITE_BUYERS=postgres, but the chain is held on DuckDB: %s is not postgres, '
               'so nothing moved. Set it, or take the flag back',
               COALESCE(flag.held_by, 'a buyer reader'))
           WHEN 'invalid' THEN
               'KS_WRITE_BUYERS is set to a value no chain understands: chain 4 is stood down'
           WHEN 'unknown' THEN 'could not read KS_WRITE_BUYERS from the web container'
           ELSE format('buyers_on=%s is not one of 0, 1, held, invalid, unknown', flag.state)
       END AS detail
FROM flag CROSS JOIN agg CROSS JOIN since CROSS JOIN owner;
