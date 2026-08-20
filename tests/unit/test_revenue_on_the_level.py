"""Revenue's product-filtered paths, computed from the order-line level.

Eleven queries wrote out `silver_orders × order_products × products` by hand,
one of them still reading raw `orders` with its own date conversion, its own
return-status list and its own copy of the sales_type rule. They read
`silver_order_lines` now.

Verified end to end before landing: both versions of the module were run
against a copy of the production database through the real repository methods,
23 calls covering every filter combination. 22 came back byte-identical; the
23rd was identical as a multiset and differed only in the order of rows tied on
quantity — which had no tiebreaker and was therefore whatever the plan
produced. That is now deterministic, and pinned below.

These are the module's first local tests: only the `external` suite, which
needs the live API, touched these methods before.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore

WINDOW = (date.today() - timedelta(days=30), date.today())


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "revenue.duckdb")
    await s.connect()
    return s


async def _seed(store):
    """Two brands in two categories, plus a tie on quantity.

    A (BrandA / Care):    2 × ₴500 + 1 × ₴500  = ₴1,500, 3 units
    B (BrandB / Makeup):  3 × ₴100             =   ₴300, 3 units  ← ties on units
    """
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO categories (id, name, parent_id) VALUES "
            "(1, 'Care', NULL), (2, 'Serums', 1), (3, 'Makeup', NULL)"
        )
        conn.execute(
            "INSERT INTO products (id, name, category_id, brand, sku, price) VALUES "
            "(100, 'Serum', 2, 'BrandA', 'SKU-A', 500.0), "
            "(200, 'Lipstick', 3, 'BrandB', 'SKU-B', 100.0)"
        )
        for oid, total, days, src in ((1, 1200.0, 2, 1), (2, 600.0, 3, 2)):
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                "buyer_id, manager_id) VALUES (?, ?, 1, ?, ?, 7, NULL)",
                [oid, src, total, now - timedelta(days=days)],
            )
        conn.execute(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
            "VALUES (1, 1, 100, 'Serum', 2, 500.0), (2, 1, 200, 'Lipstick', 3, 100.0), "
            "(3, 2, 100, 'Serum', 1, 500.0)"
        )
    await store.refresh_warehouse_layers(trigger="manual")


class TestRevenueReadsTheLevel:
    @pytest.mark.asyncio
    async def test_summary_counts_line_revenue_and_distinct_orders(self, tmp_path):
        """Two orders, five lines: the order count must not follow the lines."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_summary_stats(*WINDOW)

            assert r["totalRevenue"] == 1800.0      # 1000 + 300 + 500
            assert r["totalOrders"] == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_category_filter_narrows_to_that_category_only(self, tmp_path):
        """Care is a parent; the filter has always meant it and its children."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            care = await store.get_summary_stats(*WINDOW, category_id=1)
            makeup = await store.get_summary_stats(*WINDOW, category_id=3)

            assert care["totalRevenue"] == 1500.0
            assert care["totalOrders"] == 2
            assert makeup["totalRevenue"] == 300.0
            assert makeup["totalOrders"] == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_brand_filter_is_case_insensitive(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            exact = await store.get_summary_stats(*WINDOW, brand="BrandB")
            shouty = await store.get_summary_stats(*WINDOW, brand="BRANDB")

            assert exact["totalRevenue"] == shouty["totalRevenue"] == 300.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_by_source_answers_with_two_different_measures(self, tmp_path):
        """Pinned because it is surprising, not because it is right.

        With no product filter this reads Gold, which sums `grand_total` — the
        order's own total, delivery and discounts included. Add a filter and it
        must go to the lines, which sum what the products were sold for. The two
        are different numbers for the same period, and nothing on screen says
        which one you are looking at. Moving to the level did not create this;
        it made it visible, and it is the open `headline_vs_line_items` question
        in another costume.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)

            headline = dict(zip(*(lambda r: (r["labels"], r["revenue"]))(
                await store.get_sales_by_source(*WINDOW))))
            assert headline["Instagram"] == 1200.0   # order 1's grand_total
            assert headline["Telegram"] == 600.0     # order 2's grand_total

            lines = dict(zip(*(lambda r: (r["labels"], r["revenue"]))(
                await store.get_sales_by_source(*WINDOW, brand="BrandA"))))
            assert lines["Instagram"] == 1000.0      # 2 × ₴500 of BrandA
            assert lines["Telegram"] == 500.0        # 1 × ₴500
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_products_tied_on_quantity_come_back_in_a_stable_order(self, tmp_path):
        """Both products sold 3 units. Without a tiebreaker the order was
        whatever the plan produced, and for a top-N cut that decides who is
        shown at all."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            first = await store.get_report_top_products(*WINDOW)
            again = await store.get_report_top_products(*WINDOW)

            names = [p["product_name"] for p in first]
            assert names == [p["product_name"] for p in again]
            assert names == sorted(names), "ties fall back to the product name"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_subcategory_breakdown_no_longer_reads_raw_orders(self, tmp_path):
        """It had its own date conversion, its own return list and its own
        sales_type rule; all three now come from the level."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_subcategory_breakdown(*WINDOW, "Care")

            assert r["parentCategory"] == "Care"
            assert dict(zip(r["labels"], r["revenue"])) == {"Serums": 1500.0}
        finally:
            await store.close()
