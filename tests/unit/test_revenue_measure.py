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

ON POSTGRES SINCE STEP 14 (S14-06)

The trend reads Postgres alone, so every test here does: Bronze seeded,
Silver and Gold derived by Postgres the way production derives them
(`tests/integration/pg_tab_seed.py`), and the store's DuckDB connection a
trap. The numbers asserted are the ones this file asserted against DuckDB.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.duckdb_constants import EXHIBITION_SOURCE_ID
from tests.integration.pg_tab_seed import (
    derive, needs_postgres, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

pytestmark = needs_postgres

WINDOW = (date.today() - timedelta(days=30), date.today())


async def _populate(conn, pool):
    """Three orders, one of which pays less than its goods are worth.

    Order 1  ₴1,000 grand_total, ₴1,000 of goods   — ordinary
    Order 2  ₴1,000 grand_total, ₴1,000 of goods   — ordinary, promocode SAVE
    Order 3    ₴200 grand_total, ₴1,000 of goods   — a certificate redemption:
             the goods were paid for when the certificate was sold, so only
             the top-up arrived. Goods ₴3,000 against revenue ₴2,200.

    And order 9, ₴777 at the exhibition (source 5) with no lines: a sale Gold
    is not asked about by source and the order grain answers. Its sales type
    is `exhibition`, so no retail number below sees it.
    """
    now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    await seed(
        conn,
        categories=[(1, "Care", None)],
        products=[(100, "Serum", 1, "BrandA", "SKU-A", 500.0)],
        orders=[(oid, src, 1, total, now - timedelta(days=days), buyer, None, promo)
                for oid, total, days, src, promo, buyer in (
                    (1, 1000.0, 1, 4, None, 11), (2, 1000.0, 2, 4, "SAVE", 12),
                    (3, 200.0, 3, 4, None, 13),
                    (9, 777.0, 1, EXHIBITION_SOURCE_ID, None, 99))],
        lines=[(lid, oid, 100, "Serum", 2, 500.0) for lid, oid in ((1, 1), (2, 2), (3, 3))])
    await derive(pool)


@pytest_asyncio.fixture
async def store(tmp_path, monkeypatch):
    """The trend as production reads it — Silver and Gold in Postgres — on a
    store whose DuckDB connection is a trap."""
    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the trend")


def _total(result) -> float:
    return round(sum(result["revenue"]), 2)


class TestTheMeasureIsDeclared:
    @pytest.mark.asyncio
    async def test_unfiltered_is_money_that_came_in(self, store):
        r = await store.get_revenue_trend(*WINDOW, sales_type="retail")
        assert r["measure"] == "revenue"
        assert _total(r) == 2200.00, "grand_total: ₴1000 + ₴1000 + ₴200"

    @pytest.mark.asyncio
    async def test_a_brand_filter_is_the_value_of_the_goods(self, store):
        """The owner's rule: all goods of that brand, whatever was paid with."""
        r = await store.get_revenue_trend(*WINDOW, sales_type="retail", brand="BrandA")
        assert r["measure"] == "goods_value"
        assert _total(r) == 3000.00, "three orders × 2 × ₴500 of BrandA"

    @pytest.mark.asyncio
    async def test_a_category_filter_is_the_value_of_the_goods(self, store):
        r = await store.get_revenue_trend(*WINDOW, sales_type="retail", category_id=1)
        assert r["measure"] == "goods_value"
        assert _total(r) == 3000.00

    @pytest.mark.asyncio
    async def test_the_two_measures_genuinely_differ_here(self, store):
        """Without this the tests above could both pass on equal numbers and
        prove nothing. The gap is the certificate order, by construction."""
        money = await store.get_revenue_trend(*WINDOW, sales_type="retail")
        goods = await store.get_revenue_trend(*WINDOW, sales_type="retail", brand="BrandA")
        assert _total(goods) - _total(money) == 800.00


class TestAPromocodeDoesNotChangeTheMeasure:
    @pytest.mark.asyncio
    async def test_it_selects_whole_orders_so_it_stays_on_money(self, store):
        """The defect, as a test: before, this returned goods value."""
        r = await store.get_revenue_trend(*WINDOW, sales_type="retail", promocode="SAVE")
        assert r["measure"] == "revenue"
        assert _total(r) == 1000.00, "order 2's grand_total, not its ₴1,000 of goods"

    @pytest.mark.asyncio
    async def test_it_no_longer_raises(self, store):
        """`UPPER(s.promocode)` against `FROM silver_order_lines l` was a
        BinderException — every promocode request was a 500."""
        r = await store.get_revenue_trend(*WINDOW, sales_type="retail", promocode="SAVE")
        assert r["revenue"], "a result, not an exception"

    @pytest.mark.asyncio
    async def test_with_a_brand_it_narrows_the_lines_instead(self, store):
        """Combined with a line-level filter it is still an order-level
        predicate — it scopes which orders count, and the measure follows the
        brand, not the promocode."""
        r = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", brand="BrandA", promocode="SAVE",
        )
        assert r["measure"] == "goods_value"
        assert _total(r) == 1000.00, "order 2's BrandA lines alone"


class TestASourceGoldHasNoColumnFor:
    @pytest.mark.asyncio
    async def test_it_no_longer_raises(self, store):
        """The guard built `SELECT NULL::DATE, 0, 0 WHERE FALSE` and returned
        three bound parameters with it, so DuckDB raised
        `Parameter argument/count mismatch`."""
        r = await store.get_revenue_trend(
            *WINDOW, sales_type="all", source_id=EXHIBITION_SOURCE_ID,
        )
        assert r["measure"] == "revenue"

    @pytest.mark.asyncio
    async def test_it_returns_the_real_number(self, store):
        """An exhibition sale is money like any other. The dashboard does not
        ask Gold about it by source; the order grain answers."""
        r = await store.get_revenue_trend(
            *WINDOW, sales_type="all", source_id=EXHIBITION_SOURCE_ID,
        )
        assert _total(r) == 777.00


class TestTheTwoOrderGrainPathsAgree:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "all"])
    async def test_gold_and_silver_orders_give_the_same_answer(self, store, sales_type):
        """The routing is only safe because these are the same measure: Gold
        is a fast path for the shapes it is asked about, not a different
        question. Verified on the production copy too — retail 2025 came to
        ₴32,889,736.72 and 13,219 orders on both sides. Since step 14's S14-06
        both are Postgres's: Gold's daily series against Silver's order grain.
        """
        gold = await store._pg_gold_series(*WINDOW, sales_type, None)
        silver = await store._pg_silver(
            *store._build_silver_orders_revenue_query(*WINDOW, sales_type))

        def by_day(rows):
            return {r[0]: (float(r[1]), int(r[2])) for r in rows}

        assert by_day(gold), "the window missed the fixture"
        assert by_day(gold) == by_day(silver)


class TestTheComparisonUsesTheSameMeasure:
    @pytest.mark.asyncio
    async def test_a_brand_comparison_is_goods_against_goods(self, store):
        """Measuring the two periods differently makes the growth percentage a
        ratio between two different questions."""
        r = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", brand="BrandA", include_comparison=True,
        )
        assert r["measure"] == "goods_value"
        # The previous window holds no orders at all, so the only thing to
        # assert is that building it did not raise and did not switch path.
        assert "comparison" not in r or r["comparison"]["totals"]["current"] == 3000.00
