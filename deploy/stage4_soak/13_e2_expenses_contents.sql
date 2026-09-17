-- E2 (chain 8) — what Postgres alone now holds of the typed expenses.
--
-- WHY IT EXISTS
-- `app.manual_expenses` has no other copy since KS_WRITE_EXPENSES=postgres:
-- DuckDB stopped receiving the rows and the hourly copy stands down (E1). The
-- Postgres writer supplies `created_at` explicitly because the column has no
-- default (core/pg_expenses_write.py, `add_expense`); a writer that forgot it
-- would record every typed expense with no time at all, and nothing would
-- say so.
--
-- WHAT A FAIL MEANS
-- Rows with no `created_at`: some path inserts without the writer, or the
-- writer regressed. The row count and newest ids are in the detail so the
-- "a typed row is still there after two hourly ticks" check can be read off
-- consecutive reports.
SELECT 'E2 expenses contents'::text AS "check",
       CASE WHEN count(*) FILTER (WHERE created_at IS NULL) > 0 THEN 'FAIL' ELSE 'PASS' END
           AS verdict,
       format('%s row(s), max id %s, newest created %s, newest updated %s, %s without created_at',
              count(*), COALESCE(max(id)::text, '-'),
              COALESCE(to_char(max(created_at) AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI') || ' Kyiv', '-'),
              COALESCE(to_char(max(updated_at) AT TIME ZONE 'Europe/Kyiv', 'DD.MM HH24:MI') || ' Kyiv', '-'),
              count(*) FILTER (WHERE created_at IS NULL)) AS detail
FROM app.manual_expenses;
