-- E1 (chain 8) — the hourly copy stays stood down for app.manual_expenses.
--
-- WHY IT EXISTS
-- Under KS_WRITE_EXPENSES=postgres, expenses are typed straight into Postgres
-- and `replicate_operational` must leave `app.manual_expenses` alone: its shape
-- for that table is DELETE plus INSERT out of DuckDB, and DuckDB no longer sees
-- the rows. If the stand-down ever lapses the first typed expense is wiped
-- within the hour, looking healthy in between (core/pg_expenses_write.py).
--
-- HOW IT DECIDES
-- The copy stamps `last_ok_at` on every table it writes, every hour. So a
-- `last_ok_at` younger than 75 min means it wrote this table in the last cycle.
-- A `failures_since_ok` above zero means the chain's flag was not understood
-- and the copy says so ("not shipped: ...", DN-01).
--
-- A failure counts for the day it was stamped, and from the handover on once
-- there is one — the chain's owner row, written in its first writing
-- transaction on Postgres's clock (O1's rule, chain 3's review). Only a
-- successful copy resets `failures_since_ok`, and under the chain the copy
-- never succeeds: it stamps failing while the cause lasts, every hour, then
-- stands down silently — so one episode used to FAIL this check for ever.
--
-- WHAT A FAIL MEANS
-- The replace is running again, or the flag is broken. Since DN-06 the stand-
-- down also holds on the latch, so `failures_since_ok` above zero can mean a
-- third thing: "owned by Postgres since ..." — the chain has written Postgres
-- and KS_WRITE_EXPENSES has been put back, which is no longer a rollback and
-- undoes nothing — or a fourth, "no local marker": the owner row stands and
-- data/write-chain-owners/pg_expenses_write is gone. Read E2 before touching
-- anything; the way back is scripts/chain_copy_back.py.
WITH clock AS (
    SELECT COALESCE(NULLIF(current_setting('soak.now', true), '')::timestamptz,
                    now()) AS now
),
owner AS (
    SELECT min(updated_at) AS at
    FROM meta.chain_watermarks
    WHERE key = 'owner:app.manual_expenses'
),
m AS (
    SELECT s.table_name, s.last_ok_at, s.last_attempted_at, s.last_rows,
           s.failures_since_ok,
           left(regexp_replace(COALESCE(s.last_error, ''), '\s+', ' ', 'g'), 200) AS last_error,
           floor(extract(epoch FROM clock.now - s.last_ok_at) / 60)::bigint AS age_min,
           s.failures_since_ok > 0
               AND s.last_attempted_at > clock.now - interval '24 hours'
               AND (owner.at IS NULL OR s.last_attempted_at > owner.at) AS failing
    FROM clock
    CROSS JOIN owner
    LEFT JOIN meta.mirror_state s ON s.table_name = 'app.manual_expenses'
)
SELECT 'E1 expenses copy stood down'::text AS "check",
       CASE
           WHEN m.table_name IS NULL THEN 'PASS'
           WHEN m.failing THEN 'FAIL'
           WHEN m.last_ok_at IS NOT NULL AND m.age_min < 75 THEN 'FAIL'
           ELSE 'PASS'
       END AS verdict,
       CASE
           WHEN m.table_name IS NULL THEN 'the hourly copy has never written app.manual_expenses'
           WHEN m.failing THEN
               format('the copy reports %s failure(s), the last at %s Kyiv: %s',
                      m.failures_since_ok,
                      to_char(m.last_attempted_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'),
                      m.last_error)
           WHEN m.last_ok_at IS NOT NULL AND m.age_min < 75 THEN
               format('the hourly copy wrote app.manual_expenses %s min ago (%s rows): the stand-down has lapsed',
                      m.age_min, m.last_rows)
           ELSE format('last written by the copy %s Kyiv (%s rows), untouched since%s',
                       COALESCE(to_char(m.last_ok_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'), 'never'),
                       COALESCE(m.last_rows::text, '?'),
                       CASE WHEN m.failures_since_ok > 0 THEN
                           format('; its %s failure(s), the last at %s Kyiv, are history '
                                  '(before the day or the handover)', m.failures_since_ok,
                                  to_char(m.last_attempted_at AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI'))
                       ELSE '' END)
       END AS detail
FROM m;
