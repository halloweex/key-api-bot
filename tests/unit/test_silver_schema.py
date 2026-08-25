"""silver.orders — the first table Postgres computes rather than receives.

Everything before it arrived from somewhere else. This one is the output of
`silver_select_sql(POSTGRES)`, so the property that matters is that its columns
are exactly the projection's columns: a table with a column the projection does
not produce, or missing one it does, fails at the first rebuild rather than
here — and the first rebuild is on production data.
"""
from __future__ import annotations

import importlib.util
import inspect
import re
from pathlib import Path

from core import pg
from core.duckdb_store import SILVER_ORDERS_DDL, silver_select_sql
from core.sql_dialect import POSTGRES

REPO = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "_rev0006", REPO / "migrations" / "versions" / "0006_silver_orders.py"
)
_rev = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rev)
MIGRATION = inspect.getsource(_rev.upgrade)
DOWN = inspect.getsource(_rev.downgrade)


def _columns(ddl: str) -> list[str]:
    """Column names from a CREATE TABLE body, in order."""
    body = ddl[ddl.index("(") + 1:]
    names = []
    for line in body.splitlines():
        line = line.split("--")[0].strip()
        match = re.match(r"^([a-z_]+)\s+[A-Z]", line)
        if match:
            names.append(match.group(1))
    return names


class TestItHoldsExactlyWhatTheProjectionProduces:
    def test_the_columns_match_duckdb_silver(self):
        """One projection feeds both tables. A column here that DuckDB's Silver
        does not have could never be filled; one it has and this lacks fails on
        the first INSERT, against production data."""
        assert _columns(MIGRATION) == _columns(SILVER_ORDERS_DDL)

    def test_every_column_is_selected_by_the_postgres_rendering(self):
        """The projection emits `expr AS name` for derived columns and bare
        `o.name` for passthrough ones; both must cover every column here."""
        rendered = silver_select_sql(POSTGRES)
        for column in _columns(MIGRATION):
            assert (
                f"AS {column}" in rendered or f"o.{column}" in rendered
            ), f"{column} is in the table and not in the projection"


class TestTheTypesMirrorDuckDB:
    def test_money_is_numeric_with_the_same_scale(self):
        assert "NUMERIC(12,2)" in MIGRATION
        assert "double precision" not in MIGRATION.lower()

    def test_text_has_no_invented_length(self):
        assert "VARCHAR(" not in MIGRATION

    def test_is_new_customer_defaults_false_on_both_sides(self):
        """Pass 1 writes FALSE and pass 2 recomputes. A different default would
        let a half-applied rebuild invent acquisitions."""
        assert "is_new_customer        BOOLEAN       NOT NULL DEFAULT FALSE" in MIGRATION
        assert "is_new_customer BOOLEAN NOT NULL DEFAULT FALSE" in SILVER_ORDERS_DDL


class TestNothingRejectsARowDuckDBAccepts:
    def test_sales_type_carries_no_check_constraint(self):
        """The legal set is known, and constraining it here would make this a
        filter rather than a copy — the reconciliation would then report a
        difference the schema created."""
        # Matched on the syntax: the DDL's own comment explains why the
        # constraint is absent and therefore contains the word. Fifth time.
        assert "CHECK (" not in MIGRATION

    def test_there_are_no_foreign_keys(self):
        assert "REFERENCES" not in MIGRATION


class TestNoPerRowBookkeeping:
    def test_it_carries_no_mirrored_at(self):
        """Silver is rebuilt whole, so a per-row timestamp would be the same
        value 46,000 times. The table-level fact lives in meta.mirror_state."""
        assert "mirrored_at" not in MIGRATION
        assert "synced_at" not in MIGRATION


class TestTheIndexesTheNextStepsNeed:
    def test_gold_can_group_without_a_scan(self):
        assert "ON silver.orders (order_date, sales_type)" in MIGRATION

    def test_pass_two_can_group_by_buyer(self):
        assert "ON silver.orders (buyer_id)" in MIGRATION


class TestTheChainIsIntact:
    def test_it_follows_the_classification(self):
        assert _rev.down_revision == "0005_manager_classification"
        assert _rev.revision == "0006_silver_orders"

    def test_it_is_what_the_code_requires(self):
        assert pg.REQUIRED_REVISION == "0006_silver_orders"

    def test_it_can_be_undone(self):
        assert "DROP TABLE IF EXISTS silver.orders" in DOWN
