"""The orders tables in Postgres, and the decisions inside them.

Measured against the production backup of 2026-08-24 before the revision was
written: 46,446 orders and 147,508 line items back to 2023-12-02; 0 line items
whose order is missing; 44,231 orders with a NULL `status_group_id`; 11,988
line items with a NULL `product_id`; the longest line-item name 147 characters;
the largest synthetic line-item id 46,489,001.

Each of those numbers is behind a decision below, and every decision is one
somebody could reverse while believing they were tightening the schema.
"""
from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path

from core import pg

REPO = Path(__file__).resolve().parents[2]
_PATH = REPO / "migrations" / "versions" / "0003_landing_orders.py"

# The DDL alone: the docstring explaining "NO FOREIGN KEY" contains the words a
# test looking for a foreign key would find.
_spec = importlib.util.spec_from_file_location("_rev0003", _PATH)
_rev = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rev)
MIGRATION = inspect.getsource(_rev.upgrade)
DOWN = inspect.getsource(_rev.downgrade)


class TestItGoesInBronze:
    def test_both_tables_are_landing_not_application_state(self):
        assert "CREATE TABLE bronze.orders" in MIGRATION
        assert "CREATE TABLE bronze.order_products" in MIGRATION
        assert "CREATE TABLE app." not in MIGRATION

    def test_it_creates_no_schema(self):
        assert "CREATE SCHEMA" not in MIGRATION


class TestTheTypesMirrorDuckDB:
    def test_money_is_numeric_with_the_same_scale(self):
        """DECIMAL(12,2) on the other side. A float would make the daily
        reconciliation report differences that exist only in binary."""
        assert MIGRATION.count("NUMERIC(12,2)") == 2
        assert "double precision" not in MIGRATION.lower()
        assert "REAL" not in MIGRATION

    def test_the_line_item_id_is_bigint_because_duckdb_says_bigint(self):
        """`order_id * 1000 + position`. It fits an INTEGER today and stops
        fitting at order id ~2.1M; matching DuckDB's width means the ceiling
        arrives on both sides at once rather than on one."""
        assert "id          BIGINT        PRIMARY KEY" in MIGRATION

    def test_text_has_no_invented_length(self):
        """DuckDB imposes none, so a VARCHAR(n) here could reject a row the
        other store accepted. The longest line-item name today is 147."""
        assert "VARCHAR(" not in MIGRATION

    def test_the_columns_that_are_null_in_production_are_nullable(self):
        for nullable in ("status_group_id INTEGER,", "product_id  INTEGER,"):
            assert nullable in MIGRATION
        assert "status_group_id INTEGER NOT NULL" not in MIGRATION
        assert "product_id  INTEGER       NOT NULL" not in MIGRATION


class TestNoForeignKey:
    def test_the_line_items_do_not_reference_the_orders(self):
        """0 orphans measured, so it would hold — and it is still not
        declared. A mirror that rejects a row DuckDB accepted is a filter, and
        the backfill walks the two tables in chunks, so a prefix of history is
        an ordinary intermediate state rather than a fault."""
        assert "REFERENCES" not in MIGRATION
        assert "FOREIGN KEY" not in MIGRATION


class TestBookkeepingIsNamedApart:
    def test_it_stamps_mirrored_at_and_never_synced_at(self):
        assert MIGRATION.count("mirrored_at") == 2
        assert "synced_at" not in MIGRATION
        assert "first_seen_at" not in MIGRATION
        assert "update_count" not in MIGRATION

    def test_the_shared_row_carries_neither(self):
        from core.landing_rows import ORDER_COLUMNS, ORDER_PRODUCT_COLUMNS

        for column in ("mirrored_at", "synced_at", "first_seen_at", "update_count"):
            assert column not in ORDER_COLUMNS
            assert column not in ORDER_PRODUCT_COLUMNS


class TestTheIndexesTheWritePathNeeds:
    def test_line_items_are_indexed_by_order(self):
        """The mirror DELETEs by order_id on every single write; without this
        that is a sequential scan of 147,508 rows per synced order."""
        assert "CREATE INDEX idx_order_products_order ON bronze.order_products (order_id)" in MIGRATION

    def test_orders_are_indexed_by_date(self):
        assert "CREATE INDEX idx_orders_ordered_at ON bronze.orders (ordered_at)" in MIGRATION


class TestTheChainIsIntact:
    def test_it_follows_the_catalogue(self):
        assert _rev.down_revision == "0002_landing_catalog"
        assert _rev.revision == "0003_landing_orders"

    def test_it_is_what_the_code_requires(self):
        assert pg.REQUIRED_REVISION == "0003_landing_orders"

    def test_it_can_be_undone_children_first(self):
        assert DOWN.index("bronze.order_products") < DOWN.index("bronze.orders")
