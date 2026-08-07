"""A purchase on a retired channel is still a purchase.

The first-order baseline used to be computed over active sources only, so a
buyer whose first order was placed on Opencart counted as brand new the next
time they bought on Instagram: 419 buyers, 422 orders, ₴1,081,979.59 of repeat
business booked as acquisition, overstating new customers by 3.9% across 2025.
"""
from datetime import date

import duckdb
import pytest

from core.duckdb_store import DuckDBStore


async def _store(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "t.duckdb")
    await store.connect()
    return store


def _order(conn, oid, buyer, source, ordered, status=12, total=100.0):
    conn.execute("""
        INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, buyer_id)
        VALUES (?, ?, ?, ?, ?::TIMESTAMPTZ, ?)
    """, [oid, source, status, total, f"{ordered} 12:00:00+03", buyer])


async def _flags(store):
    async with store.connection() as conn:
        return {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT id, is_new_customer, buyer_first_order_date "
                "FROM silver_orders ORDER BY id"
            ).fetchall()
        }


class TestFirstOrderBaseline:
    @pytest.mark.asyncio
    async def test_opencart_first_order_makes_the_next_one_returning(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _order(conn, 1, buyer=50, source=3, ordered="2024-03-01")  # Opencart
                _order(conn, 2, buyer=50, source=1, ordered="2026-01-15")  # Instagram
            await store.refresh_warehouse_layers(trigger="manual")

            flags = await _flags(store)
            assert flags[2][0] is False, "buyer had already bought — not an acquisition"
            assert flags[2][1] == date(2024, 3, 1)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_genuinely_first_order_is_still_new(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _order(conn, 1, buyer=51, source=1, ordered="2026-01-15")
                _order(conn, 2, buyer=51, source=1, ordered="2026-02-20")
            await store.refresh_warehouse_layers(trigger="manual")

            flags = await _flags(store)
            assert flags[1][0] is True
            assert flags[2][0] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_cancelled_first_order_does_not_consume_the_baseline(self, tmp_path):
        """Returns stay excluded — a cancelled order is not a purchase."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _order(conn, 1, buyer=52, source=1, ordered="2025-06-01", status=19)
                _order(conn, 2, buyer=52, source=1, ordered="2026-01-15")
            await store.refresh_warehouse_layers(trigger="manual")

            flags = await _flags(store)
            assert flags[2][0] is True, "their first actual purchase"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_order_on_a_retired_channel_is_never_flagged_new_itself(self, tmp_path):
        """Inactive sources stay out of the metrics; only the baseline uses them."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _order(conn, 1, buyer=53, source=3, ordered="2024-03-01")
            await store.refresh_warehouse_layers(trigger="manual")

            flags = await _flags(store)
            assert flags[1][0] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_same_day_first_orders_are_both_new(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _order(conn, 1, buyer=54, source=1, ordered="2026-01-15")
                _order(conn, 2, buyer=54, source=2, ordered="2026-01-15")
            await store.refresh_warehouse_layers(trigger="manual")

            flags = await _flags(store)
            assert flags[1][0] is True and flags[2][0] is True
        finally:
            await store.close()


class TestDeadTableIsGone:
    @pytest.mark.asyncio
    async def test_daily_stats_is_not_created(self, tmp_path):
        """It was empty, unwritten, and advertised itself as revenue by source."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                names = {r[0] for r in conn.execute(
                    "SELECT table_name FROM duckdb_tables()"
                ).fetchall()}
            assert "daily_stats" not in names
            assert "gold_daily_revenue" in names
        finally:
            await store.close()


class TestHeadlineVsLineItems:
    def test_zero_billed_orders_with_line_items_are_reported(self):
        from core.data_quality import _headline_vs_line_items_check

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE silver_orders (id BIGINT, grand_total DOUBLE,
                                        is_return BOOLEAN, is_active_source BOOLEAN);
            CREATE TABLE order_products (order_id BIGINT, price_sold DOUBLE, quantity INTEGER);
            INSERT INTO silver_orders VALUES (1, 0.0, FALSE, TRUE), (2, 500.0, FALSE, TRUE);
            INSERT INTO order_products VALUES (1, 100.0, 3), (2, 250.0, 2);
        """)
        issues = _headline_vs_line_items_check(conn)

        assert len(issues) == 1
        assert issues[0].count == 1
        assert issues[0].sample_ids == (1,)
        assert "300.00" in issues[0].description
        conn.close()

    def test_clean_data_reports_nothing(self):
        from core.data_quality import _headline_vs_line_items_check

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE silver_orders (id BIGINT, grand_total DOUBLE,
                                        is_return BOOLEAN, is_active_source BOOLEAN);
            CREATE TABLE order_products (order_id BIGINT, price_sold DOUBLE, quantity INTEGER);
            INSERT INTO silver_orders VALUES (1, 300.0, FALSE, TRUE);
            INSERT INTO order_products VALUES (1, 100.0, 3);
        """)
        assert _headline_vs_line_items_check(conn) == []
        conn.close()

    def test_returns_and_inactive_sources_are_out_of_scope(self):
        from core.data_quality import _headline_vs_line_items_check

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE silver_orders (id BIGINT, grand_total DOUBLE,
                                        is_return BOOLEAN, is_active_source BOOLEAN);
            CREATE TABLE order_products (order_id BIGINT, price_sold DOUBLE, quantity INTEGER);
            INSERT INTO silver_orders VALUES (1, 0.0, TRUE, TRUE), (2, 0.0, FALSE, FALSE);
            INSERT INTO order_products VALUES (1, 100.0, 3), (2, 100.0, 3);
        """)
        assert _headline_vs_line_items_check(conn) == []
        conn.close()
