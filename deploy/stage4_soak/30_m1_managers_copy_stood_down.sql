-- M1 (chain 5) — nothing ships DuckDB's classification over the chain's.
--
-- Only while chain 5 writes Postgres. deploy/stage4_soak.sh reads the web
-- container's flag and latch marker and passes:
--   managers_on        1 (latched: chain 5 has written Postgres, whatever
--                      KS_WRITE_MANAGERS says — DN-06), 0 (duckdb or unset and
--                      no latch marker), pending (the flag says postgres and the
--                      chain has not latched: its preconditions are judged in
--                      web, not here — `pg_managers_write.unmet_precondition`),
--                      invalid, unknown
--   managers_flip_at   when the flag was flipped, if the operator gave it
--                      (SOAK_MANAGERS_FLIP_AT), else empty
--
-- WHY IT EXISTS
-- `core/pg_replication.replicate_managers` full-replaces both tables out of
-- DuckDB — at web's start, on the daily manager sync, after the 03:00 stats
-- job and on every classification — and stamps `meta.mirror_state` for each.
-- Under the chain it must stand down, or DuckDB's frozen classification goes
-- over every decision made in Postgres since the handover: the ₴3.1M mistake
-- of 2026, made again by a copy.
--
-- HOW IT DECIDES
-- The handover is the chain's own owner rows when they exist (written in the
-- first writing transaction, on Postgres's clock — the clock the replica
-- stamps with), else the operator's flip time, else the last 75 minutes. A
-- stamp, or a failure, after that is the replica not standing down — except
-- in that last case, which is UNKNOWN: a healthy flip's last pre-flip copy can
-- sit inside the window until the chain's first write.
--
-- A failure counts for the day it was stamped, like the rest of the report
-- (chain 3's review): only a successful copy resets `failures_since_ok`, and
-- the replica stands down without one, so the count stood for ever. A
-- `last_ok_at` after the handover is rows already replaced, and stays.
--
-- With managers_on=0 the owner rows are read too: rows naming one of chain 5's
-- tables with no marker and the flag at duckdb are the marker lost. A
-- classification follows the flag back to DuckDB, and the replica stands down
-- on the owner rows, so it never reaches Postgres — with
-- `chain_latch_disagrees` the only page. M2 stays "not applicable"; this one
-- FAILs.
--
-- And with managers_on=pending: the flag at postgres and no marker is the
-- marker lost too once owner rows stand, not a flip waiting for its first
-- tick. A precondition web holds unmet keeps the classification on DuckDB and
-- out of Postgres, exactly as above; with every one met, the chain's next
-- write takes the marker again. Either way the lever is the marker, so it
-- FAILs rather than send the operator to the preconditions as UNKNOWN.
--
-- WHAT A FAIL MEANS
-- - invalid: KS_WRITE_MANAGERS is a value no chain understands; the chain is
--   stood down and nothing is written anywhere. Fix the .env line.
-- - owner rows and managers_on=0 or pending: the marker is lost. Restore
--   data/write-chain-owners/pg_managers_write, else
--   scripts/chain_copy_back.py managers. Not the flag: at duckdb it is the
--   same state.
-- - a table written or failing after the handover: something is writing
--   DuckDB's copy over the chain's. Read M2, then the web log for 'replicate:'.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'managers_on'::text AS state,
           NULLIF(:'managers_flip_at', '')::timestamptz AS flip_at
),
owner AS (
    SELECT min(updated_at) AS at
    FROM meta.chain_watermarks
    WHERE key IN ('owner:bronze.managers', 'owner:app.manager_classifications')
),
since AS (
    SELECT COALESCE(owner.at, flag.flip_at, clock.now - interval '75 minutes') AS at,
           CASE WHEN owner.at IS NOT NULL THEN 'since the handover'
                WHEN flag.flip_at IS NOT NULL THEN 'since the flip'
                ELSE 'in the last 75 min (no owner row and no flip time: pass SOAK_MANAGERS_FLIP_AT)' END AS said,
           owner.at IS NULL AND flag.flip_at IS NULL AS guessed
    FROM owner CROSS JOIN flag CROSS JOIN clock
),
wanted (table_name) AS (
    VALUES ('bronze.managers'), ('app.manager_classifications')
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
SELECT 'M1 managers copy stood down'::text AS "check",
       CASE flag.state
           WHEN '0' THEN CASE WHEN owner.at IS NULL THEN 'PASS' ELSE 'FAIL' END
           WHEN '1' THEN CASE WHEN agg.n = 0 THEN 'PASS'
                              WHEN since.guessed THEN 'UNKNOWN'
                              ELSE 'FAIL' END
           WHEN 'pending' THEN CASE WHEN owner.at IS NULL THEN 'UNKNOWN' ELSE 'FAIL' END
           WHEN 'invalid' THEN 'FAIL'
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN CASE
               WHEN owner.at IS NULL THEN
                   'not applicable: chain 5 still writes DuckDB (KS_WRITE_MANAGERS is not postgres and no latch marker)'
               ELSE format(
                   'chain 5 owns its tables in Postgres since %s Kyiv (owner rows), and '
                   'data/write-chain-owners/pg_managers_write is gone with KS_WRITE_MANAGERS not '
                   'postgres: the marker is lost, a classification is written to DuckDB again '
                   'and the replica stands down on the owner rows, so it never reaches Postgres '
                   '(chain_latch_disagrees). Restore the marker, else '
                   'scripts/chain_copy_back.py managers',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN left(agg.listed || ' ' || since.said, 500)
                              ELSE 'neither table written by a copy ' || since.said END
           WHEN 'pending' THEN CASE
               WHEN owner.at IS NULL THEN
                   'KS_WRITE_MANAGERS=postgres and the chain has not latched: its first write '
                   'comes on the first tick, so minutes after the flip this is a chain HELD on '
                   'DuckDB — read /api/health write_chains.pg_managers_write.unmet_precondition'
               ELSE format(
                   'chain 5 owns its tables in Postgres since %s Kyiv (owner rows), and '
                   'data/write-chain-owners/pg_managers_write is gone with KS_WRITE_MANAGERS=postgres: '
                   'the marker is lost. While a precondition is unmet (/api/health '
                   'write_chains.pg_managers_write.unmet_precondition) a classification goes to '
                   'DuckDB, and the replica stands down on the owner rows, so it never reaches '
                   'Postgres (chain_latch_disagrees). Restore the marker, else '
                   'scripts/chain_copy_back.py managers; taking the flag back changes nothing',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END
           WHEN 'invalid' THEN
               'KS_WRITE_MANAGERS is set to a value no chain understands: chain 5 is stood down'
           WHEN 'unknown' THEN 'could not read KS_WRITE_MANAGERS from the web container'
           ELSE format('managers_on=%s is not one of 0, 1, pending, invalid, unknown', flag.state)
       END AS detail
FROM flag CROSS JOIN agg CROSS JOIN since CROSS JOIN owner;
