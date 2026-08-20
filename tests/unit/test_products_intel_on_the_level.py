"""Basket intelligence, computed from the order-line level.

Six queries wrote out `silver_orders × order_products` (plus the catalog, three
joins deep for category pairs) by hand, and each carried its own copy of the
line predicate — five identical copies, agreeing only because nobody had yet
changed one. Both are gone: the queries read `silver_order_lines` and the
predicate comes from `line_window_where`.

Verified against the production backup before the change landed — all six
returned identical rows over 2024-01-01…2026-08-20. As with the margin page,
this module had **no tests at all**; these are its first.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore

WINDOW = (date.today() - timedelta(days=30), date.today())


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "intel.duckdb")
    await s.connect()
    return s


async def _seed(store):
    """Three baskets across two brands in two different root categories.

    order 1  A×2 + B×1   ₴1,300   two distinct items
    order 2  A×1           ₴500   one
    order 3  A×1 + B×1     ₴800   two distinct items
    """
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO categories (id, name, parent_id) VALUES "
            "(1, 'Care', NULL), (2, 'Serums', 1), (3, 'Makeup', NULL), (4, 'Lips', 3)"
        )
        conn.execute(
            "INSERT INTO products (id, name, category_id, brand, sku, price) VALUES "
            "(100, 'Serum', 2, 'BrandA', 'SKU-A', 500.0), "
            "(200, 'Lipstick', 4, 'BrandB', 'SKU-B', 300.0)"
        )
        for oid, total, days in ((1, 1300.0, 2), (2, 500.0, 3), (3, 800.0, 4)):
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                "buyer_id, manager_id) VALUES (?, 1, 1, ?, ?, 7, NULL)",
                [oid, total, now - timedelta(days=days)],
            )
        conn.execute(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
            "VALUES (1, 1, 100, 'Serum', 2, 500.0), (2, 1, 200, 'Lipstick', 1, 300.0), "
            "(3, 2, 100, 'Serum', 1, 500.0), "
            "(4, 3, 100, 'Serum', 1, 500.0), (5, 3, 200, 'Lipstick', 1, 300.0)"
        )
    await store.refresh_warehouse_layers(trigger="manual")


class TestBasketIntelligence:
    @pytest.mark.asyncio
    async def test_basket_summary_counts_distinct_products_not_lines(self, tmp_path):
        """Two lines of the same product is a one-item basket, not a two."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_basket_summary(*WINDOW)

            assert r["totalOrders"] == 3
            assert r["multiItemOrders"] == 2
            assert r["avgBasketSize"] == 1.7          # (2 + 1 + 2) / 3
            assert r["multiAov"] == 1050              # (1300 + 800) / 2
            assert r["singleAov"] == 500
            assert r["aovUplift"] == 2.1
            assert r["topPair"] == "Serum + Lipstick"
            assert r["topPairCount"] == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_bought_together_reports_the_pair_with_its_lift(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            pairs = await store.get_frequently_bought_together(*WINDOW)

            assert len(pairs) == 1
            pair = pairs[0]
            assert {pair["productA"]["name"], pair["productB"]["name"]} == {"Serum", "Lipstick"}
            assert pair["coOccurrence"] == 2
            assert pair["totalOrders"] == 3
            assert pair["support"] == round(2 / 3, 4)
            # Lipstick appears in 2 orders, and both carry the Serum
            assert pair["confidenceBtoA"] == 1.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_distribution_buckets_orders_by_item_count(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            buckets = {b["bucket"]: b for b in await store.get_basket_distribution(*WINDOW)}

            assert buckets["1 item"]["orders"] == 1
            assert buckets["1 item"]["revenue"] == 500
            assert buckets["2 items"]["orders"] == 2
            assert buckets["2 items"]["aov"] == 1050
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_category_pairs_are_rolled_up_to_the_root(self, tmp_path):
        """Serums and Lips are children; the pair reported is Care × Makeup.

        This is the shape that needed `parent_category_id` on the level — the
        id came from a third catalog join that the level now carries.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            combos = await store.get_category_combinations(*WINDOW)

            assert len(combos) == 1
            assert {combos[0]["categoryA"], combos[0]["categoryB"]} == {"Care", "Makeup"}
            assert combos[0]["coOccurrence"] == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_brand_affinity_needs_two_orders_over_a_long_window(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            affinity = await store.get_brand_affinity(*WINDOW)

            assert len(affinity) == 1
            assert (affinity[0]["brandA"], affinity[0]["brandB"]) == ("BrandA", "BrandB")
            assert affinity[0]["coOccurrence"] == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_sales_type_all_skips_the_filter_rather_than_matching_a_literal(self, tmp_path):
        """`line_window_where` is now the single home of this predicate."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            everything = await store.get_basket_summary(*WINDOW, sales_type="all")
            retail = await store.get_basket_summary(*WINDOW, sales_type="retail")

            assert everything["totalOrders"] == retail["totalOrders"] == 3
        finally:
            await store.close()
