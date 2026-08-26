"""Silver computed inside Postgres — the first derived table, not a copy.

Verified end to end before this file was written: throwaway
`postgres:17.2-alpine` at revision 0006, the production catalogue of orders
backfilled into `bronze.orders`, the classification replicated, then
`rebuild_silver()`.

    rebuild        46,446 rows in 2.19 s
    against DuckDB 0 only on either side, **0 differing rows** across all
                   fifteen columns
    sales_type     b2b 1189 · exhibition 178 · internal 2288 · retail 42791
                   — identical on both sides

What is pinned here is the wiring that result depended on, because every part
of it is silent when wrong: a column order that shifts values one place left,
a pass rendered for the wrong engine, a rebuild that leaves the two passes in
separate transactions.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

from core.duckdb_store import silver_select_sql
from core.pg_silver import SILVER_COLUMNS, SILVER_TABLE, pass1_sql, pass2_sql, rebuild_silver
from core.sql_dialect import POSTGRES

REPO = Path(__file__).resolve().parents[2]


class TestTheColumnOrderIsTheProjectionsOrder:
    def test_it_matches_the_migration(self):
        """A shift here writes `status_id` into `source_id` and raises nothing:
        both are integers."""
        ddl = (REPO / "migrations" / "versions" / "0006_silver_orders.py").read_text()
        body = ddl[ddl.index("CREATE TABLE silver.orders"):]
        declared = [
            m.group(1) for m in re.finditer(r"^\s{12}([a-z_]+)\s+[A-Z]", body, re.M)
        ]
        assert declared == list(SILVER_COLUMNS)

    def test_the_insert_names_every_column_explicitly(self):
        """Positional INSERT into a table whose column order changed later is
        the same failure with no test to catch it."""
        head = pass1_sql().splitlines()[0]
        for column in SILVER_COLUMNS:
            assert column in head


class TestBothPassesAreRenderedForPostgres:
    def test_pass_one_reads_bronze_and_casts_the_date(self):
        sql = pass1_sql()
        assert f"FROM {POSTGRES.orders} o" in sql
        assert "::date" in sql
        assert "DATE(timezone(" not in sql

    def test_pass_two_targets_the_schema_qualified_table(self):
        assert f"UPDATE {SILVER_TABLE} AS s SET" in pass2_sql()

    def test_pass_two_is_aliased_not_schema_qualified_in_predicates(self):
        """PostgreSQL reads `silver.orders.buyer_id` as a column of a column;
        both dialects address the target through one alias so the text stays
        identical apart from the table name."""
        sql = pass2_sql()
        assert "silver.orders.buyer_id" not in sql
        assert "WHERE s.buyer_id = fo.buyer_id" in sql

    def test_the_projection_is_the_shared_one(self):
        """Not a second Postgres-flavoured copy — rule 1, and #101 is the
        proof it takes under a day to bite."""
        assert silver_select_sql(POSTGRES) in pass1_sql()


class TestTheRebuildIsOneTransaction:
    def test_truncate_and_both_passes_share_a_transaction(self):
        """Between pass 1 and pass 2 every row carries `is_new_customer =
        FALSE`, which is not incomplete but wrong. Nothing may observe it."""
        source = inspect.getsource(rebuild_silver)
        assert "async with conn.transaction():" in source
        for step in ("TRUNCATE", "pass1_sql()", "pass2_sql()"):
            assert step in source
        assert source.index("conn.transaction()") < source.index("TRUNCATE")

    def test_it_moves_the_same_watermark_the_mirror_uses(self):
        """So Reconciliation A can tell 'not rebuilt yet' from 'rebuilt wrong'
        without a second mechanism to learn."""
        source = inspect.getsource(rebuild_silver)
        assert "_WATERMARK_OK" in source

    def test_it_raises_rather_than_reporting_success(self):
        """Unlike the mirror in the sync path: nothing reads this table yet, so
        there is no working system to protect by staying quiet."""
        source = inspect.getsource(rebuild_silver)
        assert "except" not in source
