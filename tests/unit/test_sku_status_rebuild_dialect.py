"""The /inventory status rebuild is one body, and this is what makes that true.

Six tables joined into one row per offer. It is the only rule in the inventory
chain that is SQL rather than Python — `plan_stock_movements` is the other half
— and stage 4 is about to give it a second caller in Postgres.

The guarantee this file provides is the one `core/sql_dialect.py` was built
for: render the body for both engines, undo the *known* substitutions, and
assert what is left is the same string. That proves there is no THIRD
divergence, which is the thing a branchy `if postgres:` implementation could
hide indefinitely.
"""
from __future__ import annotations

import re

from core.sql_dialect import (
    DUCKDB,
    DISPLAY_TIMEZONE,
    POSTGRES,
    sku_status_rebuild_select,
)


class TestOneBodyTwoEngines:
    def test_the_renderings_differ_only_in_names_and_the_date(self):
        duck = sku_status_rebuild_select(DUCKDB)
        postgres = sku_status_rebuild_select(POSTGRES)
        assert duck != postgres, "the two engines cannot render identically"

        # Undo the table names.
        for pg_name, dk_name in (
            ("app.sku_inventory_status", "sku_inventory_status"),
            ("bronze.offer_stocks", "offer_stocks"),
            ("bronze.offers", "offers"),
            ("bronze.products", "products"),
            ("bronze.order_products", "order_products"),
            ("bronze.orders", "orders"),
            ("app.stock_movements", "stock_movements"),
        ):
            postgres = postgres.replace(pg_name, dk_name)

        # Undo the one expression that is genuinely two dialects.
        def _flatten(sql: str) -> str:
            sql = re.sub(
                r"DATE\(timezone\('[^']+', ([^)]+)\)\)", r"KYIVDATE(\1)", sql)
            sql = re.sub(
                r"\(timezone\('[^']+', ([^)]+)\)\)::date", r"KYIVDATE(\1)", sql)
            sql = re.sub(
                r"\(now\(\) AT TIME ZONE '[^']+'\)::date", "KYIVTODAY", sql)
            return sql

        assert _flatten(duck) == _flatten(postgres), (
            "a third divergence has appeared between the two renderings — "
            "only table names and the Kyiv date may differ"
        )

    def test_both_name_their_own_tables(self):
        duck = sku_status_rebuild_select(DUCKDB)
        postgres = sku_status_rebuild_select(POSTGRES)
        assert "INSERT INTO sku_inventory_status" in duck
        assert "INSERT INTO app.sku_inventory_status" in postgres
        assert "FROM bronze.offer_stocks" in postgres
        assert "FROM app.stock_movements" in postgres
        # …and neither leaks the other's.
        assert "bronze." not in duck and "app." not in duck


class TestTheRulesInsideItAreNotAccidents:
    """Each of these is a decision that would be easy to "tidy" into a bug."""

    def test_the_first_seen_carry_forward_is_still_there(self):
        """The whole reason this is a DELETE + INSERT…SELECT rather than an
        upsert: `first_seen_at` is read out of the table's own previous
        contents. Lose it and every SKU's first-seen date resets — in BOTH
        stores within the hour, because the hourly replication full-replaces
        from whichever wrote last."""
        for dialect in (DUCKDB, POSTGRES):
            sql = sku_status_rebuild_select(dialect)
            assert "_tmp_first_seen" in sql
            assert "COALESCE(" in sql
            assert sql.index("_tmp_first_seen") < sql.index("first_order_date")

    def test_the_last_sale_status_tuple_is_not_the_return_list(self):
        """`(19, 22, 21, 23)` is NOT `RETURN_STATUS_IDS` — that list is
        (15, 18, 19, 21, 22, 23). This excludes four cancelled states and
        deliberately KEEPS 15 and 18, so "last sold" counts an order that is
        still in a lost/cancel group the revenue side excludes. Asserted so a
        future reader does not unify the two."""
        from bot.config import RETURN_STATUS_IDS

        sql = sku_status_rebuild_select(DUCKDB)
        assert "status_id NOT IN (19, 22, 21, 23)" in sql
        assert set(RETURN_STATUS_IDS) != {19, 22, 21, 23}
        assert {15, 18} <= set(RETURN_STATUS_IDS)

    def test_only_stock_out_feeds_last_stock_out_at(self):
        for dialect in (DUCKDB, POSTGRES):
            assert "movement_type = 'stock_out'" in sku_status_rebuild_select(dialect)

    def test_the_date_default_is_kyiv_and_not_current_date(self):
        """`CURRENT_DATE` reads the session's zone. The two engines do not run
        in the same one, and this project has already paid for that once — a
        window that moved by a day for three hours out of every twenty-four.
        The default comes from `TODAY_IN_KYIV` in both renderings instead."""
        for dialect in (DUCKDB, POSTGRES):
            sql = sku_status_rebuild_select(dialect)
            assert "CURRENT_DATE" not in sql
            assert DISPLAY_TIMEZONE in sql

    def test_the_offer_id_fallback_keeps_a_sku_for_every_row(self):
        """`COALESCE(os.sku, CAST(os.id AS VARCHAR))` — an offer with no SKU
        still gets one, because the /inventory tab keys its rows on it."""
        for dialect in (DUCKDB, POSTGRES):
            assert "COALESCE(os.sku, CAST(os.id AS VARCHAR))" in \
                sku_status_rebuild_select(dialect)


class TestTheRepositoryUsesIt:
    def test_the_duckdb_rebuild_does_not_carry_its_own_copy(self):
        import inspect

        from core.repositories import inventory

        src = inspect.getsource(inventory.InventoryMixin._rebuild_sku_inventory_status)
        assert "sku_status_rebuild_select(DUCKDB)" in src
        assert "INSERT INTO sku_inventory_status" not in src, (
            "the rebuild has grown a second copy of the body"
        )
