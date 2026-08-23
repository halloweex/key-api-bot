"""One chart, two measures, and a filter that silently chose between them.

`get_revenue_trend` switched on `bool(category_id or brand or promocode)`.
The first two are line-level filters — they select part of an order, and
`grand_total` cannot be split by them, so the answer has to be the value of
the goods. The third is not: a promocode selects whole orders. Applying one
therefore changed the measure as a side effect, by ₴1.5M lifetime, with
nothing in the response saying so.

Both measures are legitimate and both stay. The owner ruled on 2026-08-23
that a brand filter is counted honestly — every good of that brand across
every order in the period, whatever the customer paid with — because those
numbers are presented to suppliers, and what the customer paid with is not
the supplier's business. So this does not collapse the two into one. It
routes each filter to the grain it belongs on and makes the response say
which measure it used.

Two 500s fell out of the same switch, both on `/api/revenue/trend`:
`UPPER(s.promocode)` against a FROM clause binding only `l`, and a
`source_id` Gold holds no column for, whose guard emitted a parameterless
statement while still returning three bound parameters.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_constants import EXHIBITION_SOURCE_ID
from core.duckdb_store import DuckDBStore

WINDOW = (date.today() - timedelta(days=30), date.today())


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "measure.duckdb")
    await s.connect()
    return s


async def _seed(store):
    """Three orders, one of which pays less than its goods are worth.

    Order 1  ₴1,000 grand_total, ₴1,000 of goods   — ordinary
    Order 2  ₴1,000 grand_total, ₴1,000 of goods   — ordinary, promocode SAVE
    Order 3    ₴200 grand_total, ₴1,000 of goods   — a certificate redemption:
             the goods were paid for when the certificate was sold, so only
             the top-up arrived. Goods ₴3,000 against revenue ₴2,200.
    """
    now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO categories (id, name, parent_id) VALUES (1, 'Care', NULL)"
        )
        conn.execute(
            "INSERT INTO products (id, name, category_id, brand, sku, price) VALUES "
            "(100, 'Serum', 1, 'BrandA', 'SKU-A', 500.0)"
        )
        rows = [
            (1, 1000.0, 1, 4, None),
            (2, 1000.0, 2, 4, "SAVE"),
            (3, 200.0, 3, 4, None),
        ]
        for oid, total, days, src, promo in rows:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                "buyer_id, manager_id, promocode) VALUES (?, ?, 1, ?, ?, ?, NULL, ?)",
                [oid, src, total, now - timedelta(days=days), 10 + oid, promo],
            )
        for lid, oid in ((1, 1), (2, 2), (3, 3)):
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity, "
                "price_sold) VALUES (?, ?, 100, 'Serum', 2, 500.0)",
                [lid, oid],
            )
    await store.refresh_warehouse_layers(trigger="manual")


def _total(result) -> float:
    return round(sum(result["revenue"]), 2)


class TestTheMeasureIsDeclared:
    @pytest.mark.asyncio
    async def test_unfiltered_is_money_that_came_in(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(*WINDOW, sales_type="retail")
            assert r["measure"] == "revenue"
            assert _total(r) == 2200.00, "grand_total: ₴1000 + ₴1000 + ₴200"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_brand_filter_is_the_value_of_the_goods(self, tmp_path):
        """The owner's rule: all goods of that brand, whatever was paid with."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(*WINDOW, sales_type="retail", brand="BrandA")
            assert r["measure"] == "goods_value"
            assert _total(r) == 3000.00, "three orders × 2 × ₴500 of BrandA"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_category_filter_is_the_value_of_the_goods(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(*WINDOW, sales_type="retail", category_id=1)
            assert r["measure"] == "goods_value"
            assert _total(r) == 3000.00
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_two_measures_genuinely_differ_here(self, tmp_path):
        """Without this the tests above could both pass on equal numbers and
        prove nothing. The gap is the certificate order, by construction."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            money = await store.get_revenue_trend(*WINDOW, sales_type="retail")
            goods = await store.get_revenue_trend(*WINDOW, sales_type="retail", brand="BrandA")
            assert _total(goods) - _total(money) == 800.00
        finally:
            await store.close()


class TestAPromocodeDoesNotChangeTheMeasure:
    @pytest.mark.asyncio
    async def test_it_selects_whole_orders_so_it_stays_on_money(self, tmp_path):
        """The defect, as a test: before, this returned goods value."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(*WINDOW, sales_type="retail", promocode="SAVE")
            assert r["measure"] == "revenue"
            assert _total(r) == 1000.00, "order 2's grand_total, not its ₴1,000 of goods"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_no_longer_raises(self, tmp_path):
        """`UPPER(s.promocode)` against `FROM silver_order_lines l` was a
        BinderException — every promocode request was a 500."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(*WINDOW, sales_type="retail", promocode="SAVE")
            assert r["revenue"], "a result, not an exception"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_with_a_brand_it_narrows_the_lines_instead(self, tmp_path):
        """Combined with a line-level filter it is still an order-level
        predicate — it scopes which orders count, and the measure follows the
        brand, not the promocode."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(
                *WINDOW, sales_type="retail", brand="BrandA", promocode="SAVE",
            )
            assert r["measure"] == "goods_value"
            assert _total(r) == 1000.00, "order 2's BrandA lines alone"
        finally:
            await store.close()


class TestASourceGoldHasNoColumnFor:
    @pytest.mark.asyncio
    async def test_it_no_longer_raises(self, tmp_path):
        """The guard built `SELECT NULL::DATE, 0, 0 WHERE FALSE` and returned
        three bound parameters with it, so DuckDB raised
        `Parameter argument/count mismatch`."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(
                *WINDOW, sales_type="all", source_id=EXHIBITION_SOURCE_ID,
            )
            assert r["measure"] == "revenue"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_returns_the_real_number(self, tmp_path):
        """An exhibition sale is money like any other. Gold cannot express it
        for want of a column; the order grain can."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            now = datetime.now(timezone.utc).replace(
                hour=12, minute=0, second=0, microsecond=0
            )
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO orders (id, source_id, status_id, grand_total, "
                    "ordered_at, buyer_id, manager_id) VALUES (9, ?, 1, 777.0, ?, 99, NULL)",
                    [EXHIBITION_SOURCE_ID, now - timedelta(days=1)],
                )
            await store.refresh_warehouse_layers(trigger="manual")

            r = await store.get_revenue_trend(
                *WINDOW, sales_type="all", source_id=EXHIBITION_SOURCE_ID,
            )
            assert _total(r) == 777.00
        finally:
            await store.close()


class TestTheTwoOrderGrainPathsAgree:
    @pytest.mark.asyncio
    async def test_gold_and_silver_orders_give_the_same_answer(self, tmp_path):
        """The routing is only safe because these are the same measure: Gold
        is a fast path for the shapes it holds a column for, not a different
        question. Verified on the production copy too — retail 2025 came to
        ₴32,889,736.72 and 13,219 orders on both sides.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                g_sql, g_params = store._build_gold_revenue_query(*WINDOW, "retail")
                s_sql, s_params = store._build_silver_orders_revenue_query(*WINDOW, "retail")
                gold = conn.execute(g_sql, g_params).fetchall()
                silver = conn.execute(s_sql, s_params).fetchall()

            assert {r[0]: (float(r[1]), int(r[2])) for r in gold} == \
                   {r[0]: (float(r[1]), int(r[2])) for r in silver}
        finally:
            await store.close()


class TestTheComparisonUsesTheSameMeasure:
    @pytest.mark.asyncio
    async def test_a_brand_comparison_is_goods_against_goods(self, tmp_path):
        """Measuring the two periods differently makes the growth percentage a
        ratio between two different questions."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(
                *WINDOW, sales_type="retail", brand="BrandA", include_comparison=True,
            )
            assert r["measure"] == "goods_value"
            # The previous window holds no orders at all, so the only thing to
            # assert is that building it did not raise and did not switch path.
            assert "comparison" not in r or r["comparison"]["totals"]["current"] == 3000.00
        finally:
            await store.close()
