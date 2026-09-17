-- soak:run-as postgres
--
-- E2 (chain 8) — the id allocator is above every id in the table.
--
-- WHY IT EXISTS
-- Revision 0031 gave `app.manual_expenses.id` a sequence for the day Postgres
-- became the writer, and set it above the table as it stood then. Rows that
-- arrive any other way — the hourly copy before the switch, a restore, a
-- copy-back — carry ids the sequence never handed out. The writer lifts the
-- sequence to MAX(id) inside every insert (`_ensure_expense_id_floor`), so a
-- typed expense heals it; anything that inserts without going through the
-- writer collides on the primary key instead.
--
-- WHY IT RUNS AS postgres
-- `ks_readonly` has no privilege on the sequence, and `pg_sequences.last_value`
-- reads NULL both for a role without one and for a sequence nothing has called
-- yet — which is exactly production's state with zero rows. So this reads the
-- sequence relation itself, which needs the owner or a superuser.
-- deploy/stage4_soak.sh starts every session with
-- `default_transaction_read_only=on`, this one included, so the superuser's
-- transaction is read-only like everybody else's.
--
-- WHAT A FAIL MEANS
-- The next value the sequence will hand out is not above MAX(id): rows arrived
-- by a path that is not the writer, and the floor has not run since. Find that
-- path before anything else. Do not setval by hand — the writer's next insert
-- lifts the sequence in its own transaction.
WITH seq AS (
    SELECT last_value, is_called FROM app.manual_expenses_id_seq
),
t AS (
    SELECT COALESCE(max(id), 0) AS max_id, count(*) AS n FROM app.manual_expenses
)
SELECT 'E2 expenses allocator'::text AS "check",
       CASE
           WHEN (CASE WHEN seq.is_called THEN seq.last_value + 1 ELSE seq.last_value END) > t.max_id
               THEN 'PASS'
           ELSE 'FAIL'
       END AS verdict,
       format('next id %s, max id %s over %s row(s)',
              CASE WHEN seq.is_called THEN seq.last_value + 1 ELSE seq.last_value END,
              t.max_id, t.n) AS detail
FROM seq CROSS JOIN t;
