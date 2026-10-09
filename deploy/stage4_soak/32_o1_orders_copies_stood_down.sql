-- O1 (chain 3) — nothing ships DuckDB's orders, expenses or misses over the chain's.
--
-- Only while chain 3 writes Postgres. deploy/stage4_soak.sh reads the web
-- container's flag and latch marker and passes:
--   orders_on        1 (latched: chain 3 has written Postgres, whatever
--                    KS_WRITE_ORDERS says — DN-06), 0 (duckdb or unset and no
--                    latch marker), pending (the flag says postgres and the chain
--                    has not latched: its preconditions are judged in web, not
--                    here — `pg_orders_write.unmet_precondition`), invalid,
--                    unknown
--   orders_flip_at   when the flag was flipped, if the operator gave it
--                    (SOAK_ORDERS_FLIP_AT), else empty
--   markers_read_at  when the script began reading the latch markers, UTC to
--                    the second (empty: owner rows are judged as if older)
--
-- WHY IT EXISTS
-- Two DuckDB shippers fed these tables every hour before the chain, and both
-- must stand down under it, or DuckDB's frozen copy goes over what the chain
-- wrote:
--   - `replicate_operational` full-replaces app.order_backfill_misses and
--     stamps its `last_ok_at`: the ledger rolled back to DuckDB's, and the gap
--     and half-written repairs asking KeyCRM again for every id the chain
--     recorded since;
--   - the ids-diffs (`hourly_orders_ids_diff`, `hourly_expenses_ids_diff`)
--     ship what Postgres lacks out of DuckDB and stamp `backfilled_at` on the
--     three bronze tables on every complete run — as do the two backfill
--     routes they share a body with.
-- The per-tick mirrors are not read here, because nothing here could tell
-- them apart: the chain's own writer stamps `last_ok_at` on the three bronze
-- tables with the statement the mirrors used. S0 judges that stamp, O2 the
-- order step. A mirror cannot be reached under the chain in any case: DuckDB's
-- writers raise `ChainOwnsOrders` before it.
--
-- HOW IT DECIDES
-- The handover is the chain's own owner rows when they exist (written in the
-- first writing transaction, on Postgres's clock — the clock both shippers
-- stamp with), else the operator's flip time, else the last 75 minutes. After
-- it: a `last_ok_at`, or a failure, on the misses; a `backfilled_at` on any of
-- the bronze three. A failure on those three is the chain's own write failing
-- (S0, O2), not a copy. With neither an owner row nor a flip time a stamp in
-- the window is UNKNOWN rather than FAIL: both hourly jobs stamp until the
-- flip, so a healthy flip reads one inside it until the chain's first write.
--
-- A failure counts for the day it was stamped, like the rest of the report.
-- Only a successful copy resets `failures_since_ok`, and under the chain the
-- copy never succeeds: it stamps failing every hour while the flag and the
-- latch disagree, and stands down silently once they agree again — so the
-- count would stand for ever and FAIL a cause long gone. A stamp is different:
-- a `last_ok_at` or a `backfilled_at` after the handover is DuckDB's rows
-- already shipped over the chain's, which no later hour undoes, and it stays.
--
-- With orders_on=0 the owner rows are read too. Rows naming one of chain 3's
-- tables with no marker and the flag at duckdb are the marker lost, not a
-- chain that never moved: the writes follow the flag back to DuckDB, and the
-- sync's per-tick mirror, which asks only that local answer, ships DuckDB's
-- orders over the chain's on every tick — DN-22a's
-- `order_owner_row_without_marker`, CRITICAL. The four other O-checks stay
-- "not applicable" (no chain writes Postgres for them to judge); this one
-- FAILs.
--
-- And with orders_on=pending: the flag at postgres and no marker is the marker
-- lost too once owner rows stand, not a flip waiting for its first write. A
-- precondition web holds unmet keeps the writes on DuckDB and the mirror
-- shipping, exactly as above; with every one met, the chain's next write
-- takes the marker again. Either way UNKNOWN would send the operator to the
-- preconditions while the lever is the marker, so it FAILs.
--
-- Except owner rows dated after the script read the marker (chain 3's soak
-- review, 2026-10-10). The script reads the marker, then runs the log check
-- and some thirty psql sessions before this file, and a healthy first write
-- takes the marker before it claims the owner rows, which its transaction's
-- `now()` dates — so on flip day, when the chain latches on the first order
-- KeyCRM changes and the checklist runs this report at +2 min, those rows are
-- the latch taken during the report, not a marker lost. UNKNOWN, and run the
-- report again: the next run reads the marker after them, and a marker still
-- missing then is lost. A FAIL here names a rollback tool. Not at 0: the web
-- container whose flag the script read cannot latch, and a recreate during
-- the report makes every flag it passed stale, this one no more than the rest.
--
-- WHAT A FAIL MEANS
-- - invalid: KS_WRITE_ORDERS is a value no chain understands; the chain is
--   stood down and no order is written anywhere. Fix the .env line.
-- - owner rows and orders_on=0, or pending with the rows older than the
--   script's read of the marker: the marker is lost. Restore
--   data/write-chain-owners/pg_orders_write (or redeploy the chain's build),
--   else scripts/chain_copy_back.py orders — DN-22a's lever for the same
--   state. Not the flag: at duckdb it is the same state.
-- - the misses written after the handover: the hourly copy put DuckDB's ledger
--   over the chain's. Failing after it: read the error — `owned by Postgres
--   since` is the latch with its flag put back, `no local marker` a lost
--   data/write-chain-owners/pg_orders_write; scripts/chain_copy_back.py orders
--   is the way back from either, never the flag.
-- - a backfilled_at after the handover: a DuckDB backfill shipped orders or
--   expenses over the chain's. Read the web log for 'Backfill finished' and
--   'Expense backfill'.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
flag AS (
    SELECT :'orders_on'::text AS state,
           NULLIF(:'orders_flip_at', '')::timestamptz AS flip_at,
           NULLIF(:'markers_read_at', '')::timestamptz AS markers_read_at
),
owner AS (
    SELECT min(updated_at) AS at
    FROM meta.chain_watermarks
    WHERE key IN ('owner:bronze.orders', 'owner:bronze.order_products',
                  'owner:bronze.expenses', 'owner:app.order_backfill_misses')
),
since AS (
    SELECT COALESCE(owner.at, flag.flip_at, clock.now - interval '75 minutes') AS at,
           CASE WHEN owner.at IS NOT NULL THEN 'since the handover'
                WHEN flag.flip_at IS NOT NULL THEN 'since the flip'
                ELSE 'in the last 75 min (no owner row and no flip time: pass SOAK_ORDERS_FLIP_AT)' END AS said,
           owner.at IS NULL AND flag.flip_at IS NULL AS guessed
    FROM owner CROSS JOIN flag CROSS JOIN clock
),
-- `copy`: the hourly full replace, judged by its success stamp and its
-- failures. `backfill`: the ids-diffs, judged by the stamp only they write.
wanted (table_name, shipper) AS (
    VALUES ('app.order_backfill_misses', 'copy'),
           ('bronze.orders', 'backfill'),
           ('bronze.order_products', 'backfill'),
           ('bronze.expenses', 'backfill')
),
judged AS (
    SELECT w.table_name,
           CASE
               WHEN w.shipper = 'copy' AND s.failures_since_ok > 0
                    AND s.last_attempted_at > since.at
                    AND s.last_attempted_at > clock.now - interval '24 hours' THEN
                   format('failing (%s): %s', s.failures_since_ok,
                          left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 120))
               WHEN w.shipper = 'copy' AND s.last_ok_at > since.at THEN
                   format('written by the hourly copy at %s Kyiv',
                          to_char(s.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
               WHEN w.shipper = 'backfill' AND s.backfilled_at > since.at THEN
                   format('backfilled out of DuckDB at %s Kyiv',
                          to_char(s.backfilled_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
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
SELECT 'O1 orders copies stood down'::text AS "check",
       CASE flag.state
           WHEN '0' THEN CASE WHEN owner.at IS NULL THEN 'PASS' ELSE 'FAIL' END
           WHEN '1' THEN CASE WHEN agg.n = 0 THEN 'PASS'
                              WHEN since.guessed THEN 'UNKNOWN'
                              ELSE 'FAIL' END
           WHEN 'pending' THEN CASE WHEN owner.at IS NULL THEN 'UNKNOWN'
                                    WHEN owner.at >= flag.markers_read_at THEN 'UNKNOWN'
                                    ELSE 'FAIL' END
           WHEN 'invalid' THEN 'FAIL'
           ELSE 'UNKNOWN'
       END AS verdict,
       CASE flag.state
           WHEN '0' THEN CASE
               WHEN owner.at IS NULL THEN
                   'not applicable: chain 3 still writes DuckDB (KS_WRITE_ORDERS is not postgres and no latch marker)'
               ELSE format(
                   'chain 3 owns its tables in Postgres since %s Kyiv (owner rows), and '
                   'data/write-chain-owners/pg_orders_write is gone with KS_WRITE_ORDERS not '
                   'postgres: the marker is lost, the writes follow the flag back to DuckDB and '
                   'the sync''s mirror ships DuckDB''s orders over the chain''s every tick '
                   '(order_owner_row_without_marker). Restore the marker, else '
                   'scripts/chain_copy_back.py orders',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END
           WHEN '1' THEN CASE WHEN agg.n > 0 THEN left(agg.listed || ' ' || since.said, 500)
                              ELSE 'none of the four tables written by a DuckDB copy ' || since.said END
           WHEN 'pending' THEN CASE
               WHEN owner.at IS NULL THEN
                   'KS_WRITE_ORDERS=postgres and the chain has not latched: it latches on its first '
                   'write, the first order KeyCRM changes after the start, so in the day, minutes '
                   'after the flip, this is a chain HELD on DuckDB — read /api/health '
                   'write_chains.pg_orders_write.unmet_precondition'
               WHEN owner.at >= flag.markers_read_at THEN format(
                   'chain 3 latched while this report ran: its owner rows are dated %s Kyiv, '
                   'after it began reading the latch markers at %s Kyiv and found '
                   'data/write-chain-owners/pg_orders_write missing, and a first write takes the '
                   'marker before it claims them. Not a lost marker: run the report again',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI:SS'),
                   to_char(flag.markers_read_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI:SS'))
               ELSE format(
                   'chain 3 owns its tables in Postgres since %s Kyiv (owner rows), and '
                   'data/write-chain-owners/pg_orders_write is gone with KS_WRITE_ORDERS=postgres: '
                   'the marker is lost. While a precondition is unmet (/api/health '
                   'write_chains.pg_orders_write.unmet_precondition) the writes stay on DuckDB '
                   'and the sync''s mirror ships DuckDB''s orders over the chain''s every tick '
                   '(order_owner_row_without_marker). Restore the marker, else '
                   'scripts/chain_copy_back.py orders; taking the flag back changes nothing',
                   to_char(owner.at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI')) END
           WHEN 'invalid' THEN
               'KS_WRITE_ORDERS is set to a value no chain understands: chain 3 is stood down'
           WHEN 'unknown' THEN 'could not read KS_WRITE_ORDERS from the web container'
           ELSE format('orders_on=%s is not one of 0, 1, pending, invalid, unknown', flag.state)
       END AS detail
FROM flag CROSS JOIN agg CROSS JOIN since CROSS JOIN owner;
