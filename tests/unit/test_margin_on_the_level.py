"""The margin page, computed from the order-line level.

Six queries used to write out `silver_orders × order_products × products ×
offer_stocks` by hand; they now read `silver_order_lines` and join only the
cost table. Verified against the production backup before the change landed —
all six returned byte-identical rows, 170 of them — but a one-off check is not
a guarantee, and this module had **no tests at all**. These are its first.

The numbers below are hand-computed, not captured from a run, so they fail if
the level's arithmetic drifts rather than merely changing shape.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "margin.duckdb")
    await s.connect()
    return s


async def _seed(store):
    """Two brands, one with a known cost, one with none at all.

    A: 2 × ₴500 + 3 × ₴300 = ₴1,900 revenue, cost ₴200/unit → COGS ₴1,000
    B: 1 × ₴250 = ₴250 revenue, no offer_stocks row → uncosted
    """
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.execute("INSERT INTO categories (id, name, parent_id) VALUES (1, 'Care', NULL)")
        conn.execute(
            "INSERT INTO products (id, name, category_id, brand, sku, price) VALUES "
            "(100, 'Serum', 1, 'BrandA', 'SKU-A', 500.0), "
            "(200, 'Cream', 1, 'BrandB', 'SKU-B', 250.0)"
        )
        conn.execute(
            "INSERT INTO offer_stocks (id, sku, price, purchased_price, quantity) "
            "VALUES (1, 'SKU-A', 500.0, 200.0, 10)"
        )
        conn.execute(
            "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
            "buyer_id, manager_id) VALUES (1, 1, 1, 2150.0, ?, 7, NULL)",
            [now - timedelta(days=2)],
        )
        conn.execute(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
            "VALUES (1, 1, 100, 'Serum', 2, 500.0), (2, 1, 100, 'Serum', 3, 300.0), "
            "(3, 1, 200, 'Cream', 1, 250.0)"
        )
    await store.refresh_warehouse_layers(trigger="manual")


WINDOW = (date.today() - timedelta(days=30), date.today())


class TestMarginReadsTheLevel:
    @pytest.mark.asyncio
    async def test_overview_separates_costed_revenue_from_all_revenue(self, tmp_path):
        """Coverage is the point of this page: what share of sales we can price."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_margin_overview(*WINDOW, sales_type="retail")

            assert r["total_revenue"] == 2150.0        # 1000 + 900 + 250
            assert r["costed_revenue"] == 1900.0       # BrandB has no cost row
            assert r["cogs"] == 1000.0                 # 5 units × ₴200
            assert r["profit"] == 900.0
            assert r["margin_pct"] == 47.4             # 900 / 1900
            assert r["coverage_pct"] == 88.4           # 1900 / 2150
            assert r["total_units"] == 6
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_by_brand_keeps_an_uncosted_brand_visible(self, tmp_path):
        """A brand with no purchase price is the finding, not a row to drop."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            rows = {r["brand"]: r for r in await store.get_margin_by_brand(*WINDOW)}

            assert set(rows) == {"BrandA", "BrandB"}
            assert rows["BrandA"]["total_revenue"] == 1900.0
            assert rows["BrandA"]["cogs"] == 1000.0
            assert rows["BrandA"]["costed_units"] == 5
            assert rows["BrandB"]["total_revenue"] == 250.0
            assert rows["BrandB"]["costed_revenue"] == 0.0
            assert rows["BrandB"]["margin_pct"] is None, "no cost is not zero margin"
            assert rows["BrandB"]["coverage_pct"] == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_by_category_shares_sum_to_a_hundred(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            rows = await store.get_margin_by_category(*WINDOW)

            assert [r["category"] for r in rows] == ["Care"]
            assert rows[0]["total_revenue"] == 2150.0
            assert rows[0]["rev_share_pct"] == 100.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_remaining_three_shapes_run_and_agree(self, tmp_path):
        """Trend, brand×category and alerts had no coverage whatsoever."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)

            trend = await store.get_margin_trend(*WINDOW)
            assert sum(r["total_revenue"] for r in trend) == 2150.0

            cross = await store.get_margin_brand_category(*WINDOW, min_revenue=100)
            assert {(r["brand"], r["category"]) for r in cross} == {
                ("BrandA", "Care"), ("BrandB", "Care")
            }

            alerts = await store.get_margin_alerts(
                *WINDOW, margin_floor=60.0, min_revenue=100
            )
            assert [a["brand"] for a in alerts] == ["BrandA"], (
                "47.4 % is below a 60 % floor and should be flagged"
            )
            assert alerts[0]["impact"] == 240.0  # 1900 × 0.60 − 900
        finally:
            await store.close()
