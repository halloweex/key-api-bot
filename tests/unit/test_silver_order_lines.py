"""The order-line level: one grain, one definition, and it must equal Gold.

Silver has one grain — the order — so every question about a *product* re-joined
`order_products` where it was asked. That join appears 49 times in this
codebase and 22 of those carry `silver_orders` too, which is the single largest
obstacle to splitting this store in two: the line and its order would land in
different databases.

What is pinned here is the property that makes the level trustworthy — under
Gold's own predicate it reproduces `gold_daily_products` exactly. A level that
disagrees with the aggregate built beside it is a third revenue number, and
this project already has two.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "lines.duckdb")
    await s.connect()
    return s


async def _seed(store, *, with_uncatalogued: bool = True):
    """Two orders, four lines, one of them for a product the catalog lost."""
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.execute("INSERT INTO categories (id, name, parent_id) VALUES (1, 'Care', NULL)")
        conn.execute("INSERT INTO categories (id, name, parent_id) VALUES (2, 'Serums', 1)")
        conn.execute(
            "INSERT INTO products (id, name, category_id, brand, sku, price) "
            "VALUES (100, 'Serum', 2, 'BrandA', 'SKU-1', 500.0)"
        )
        for oid, days in ((1, 3), (2, 5)):
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                "buyer_id, manager_id) VALUES (?, 1, 1, 1000.0, ?, 7, NULL)",
                [oid, now - timedelta(days=days)],
            )
        conn.execute(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
            "VALUES (1, 1, 100, 'Serum', 2, 500.0), (2, 1, 100, 'Serum', 1, 400.0), "
            "(3, 2, 100, 'Serum', 3, 300.0)"
        )
        if with_uncatalogued:
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                "VALUES (4, 2, NULL, 'Gone from the catalog', 1, 250.0)"
            )
    await store.refresh_warehouse_layers(trigger="manual")


class TestTheLevel:
    @pytest.mark.asyncio
    async def test_the_grain_is_the_line(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                lines = conn.execute("SELECT COUNT(*) FROM silver_order_lines").fetchone()[0]
                raw = conn.execute("SELECT COUNT(*) FROM order_products").fetchone()[0]
            assert lines == raw == 4
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_line_whose_product_left_the_catalog_survives(self, tmp_path):
        """31 % of production lines have no category and 8.2 % no product_id.

        A level that inner-joins the catalog is a filter wearing a level's name,
        and it would drop revenue quietly — which is how ₴13–27M of product
        revenue went uncategorised without anyone seeing a number move.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                row = conn.execute(
                    "SELECT product_name, brand, category_id, line_amount "
                    "FROM silver_order_lines WHERE product_id IS NULL"
                ).fetchone()
            assert row is not None, "the uncatalogued line was dropped"
            assert row[1] is None and row[2] is None
            assert float(row[3]) == 250.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_carries_the_order_answer_rather_than_deriving_one(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                mismatches = conn.execute("""
                    SELECT COUNT(*) FROM silver_order_lines l
                    JOIN silver_orders s ON s.id = l.order_id
                    WHERE l.sales_type IS DISTINCT FROM s.sales_type
                       OR l.order_date IS DISTINCT FROM s.order_date
                       OR l.is_return IS DISTINCT FROM s.is_return
                       OR l.is_active_source IS DISTINCT FROM s.is_active_source
                """).fetchone()[0]
            assert mismatches == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_reproduces_gold_to_the_kopeck(self, tmp_path):
        """The load-bearing property. Verified on production too: ₴132,077,453.75
        on both sides, 986 dates on both sides, zero disagreeing."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                gold = conn.execute(
                    "SELECT date, ROUND(SUM(product_revenue), 2) FROM gold_daily_products "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
                level = conn.execute(
                    "SELECT order_date, ROUND(SUM(line_amount), 2) FROM silver_order_lines "
                    "WHERE NOT is_return AND is_active_source GROUP BY 1 ORDER BY 1"
                ).fetchall()
            assert gold == level, "the level and the aggregate beside it disagree"
            assert gold, "the fixture produced no Gold rows, so this proved nothing"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_holds_what_gold_excludes_rather_than_hiding_it(self, tmp_path):
        """Returns and dead sources belong to the caller's predicate, not the level."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                conn.execute("UPDATE orders SET status_id = 19 WHERE id = 2")
            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                total = conn.execute(
                    "SELECT ROUND(SUM(line_amount), 2) FROM silver_order_lines"
                ).fetchone()[0]
                counted = conn.execute(
                    "SELECT ROUND(COALESCE(SUM(line_amount), 0), 2) FROM silver_order_lines "
                    "WHERE NOT is_return AND is_active_source"
                ).fetchone()[0]
                returned = conn.execute(
                    "SELECT ROUND(SUM(line_amount), 2) FROM silver_order_lines WHERE is_return"
                ).fetchone()[0]
            assert float(returned) > 0, "the fixture did not actually produce a return"
            assert float(total) == float(counted) + float(returned)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_still_resolves_after_silver_is_rebuilt_from_scratch(self, tmp_path):
        """`rebuild-silver` drops the table. DuckDB resolves a view by name at
        query time, so it survives — but only as long as the shape does, which
        is why Silver's DDL has exactly one home."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            from core.duckdb_store import SILVER_ORDERS_DDL, silver_select_sql
            async with store.connection() as conn:
                conn.execute("DROP TABLE IF EXISTS silver_orders")
                conn.execute(SILVER_ORDERS_DDL)
                conn.execute(
                    "INSERT INTO silver_orders SELECT " + silver_select_sql() + " FROM orders o"
                )
                assert conn.execute(
                    "SELECT COUNT(*) FROM silver_order_lines"
                ).fetchone()[0] == 4
        finally:
            await store.close()
