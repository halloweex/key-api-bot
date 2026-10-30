"""The one slice of brand analytics you could see and not click.

`get_brand_analytics` has always drawn a bucket for goods with no brand —
`COALESCE(g.brand, 'Unknown')`, ₴3.5M of retail on the production copy,
fifth by revenue — while `get_brands` listed only
`brand IS NOT NULL AND brand != ''`. So it appeared on the chart and was
absent from the filter, and even typed by hand it would not have worked:
equality never matches NULL.

Fixing it in place would have meant editing ten hand-written copies of
`LOWER(x.brand) = LOWER(?)` across three aliases — the shape that produced
the `UPPER(s.promocode)` binder error in #124. They call one helper now.

Since step 14's S14-05 the filter's list reads Postgres alone, so the goods
are seeded into Bronze and derived by Postgres (`tests/integration/
pg_tab_seed.py`) where they used to be written into DuckDB, and the trend is
read the way production reads it, from Postgres' Silver
(`KS_READ_SILVER=postgres`). Those tests skip without `KS_PG_DSN`.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.duckdb_constants import UNKNOWN_BRAND, brand_where
from tests.integration.pg_tab_seed import (
    derive, needs_postgres, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

WINDOW = (date.today() - timedelta(days=30), date.today())


def _populate(*, with_unbranded=True):
    """Three products: a brand, no brand at all, and a brand set to ''.

    Both spellings of absent have to land in the same bucket — one product
    never had the KeyCRM field, the other has it empty — because from the
    dashboard's side they are the same fact.
    """
    async def populate(conn, pool):
        now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        rows = [(100, 'Serum', 'BrandA')]
        if with_unbranded:
            rows += [(200, 'Cream', None), (300, 'Toner', '')]
        lines = [(1, 1, 100)]
        if with_unbranded:
            lines += [(2, 2, 200), (3, 3, 300)]
        await seed(
            conn,
            categories=[(1, "Care", None)],
            products=[(pid, name, 1, brand, f"SKU-{pid}", 500.0)
                      for pid, name, brand in rows],
            orders=[(oid, 4, 1, 1000.0, now - timedelta(days=oid), 10 + oid, None, None)
                    for oid in (1, 2, 3)],
            lines=[(lid, oid, pid, "x", 2, 500.0) for lid, oid, pid in lines])
        await derive(pool)
    return populate


@pytest_asyncio.fixture
async def goods(tmp_path, monkeypatch):
    """Every product, the unbranded two included, in Postgres; the trend read
    from Postgres' Silver, as in production."""
    monkeypatch.setenv("KS_READ_SILVER", "postgres")
    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate()):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the filter bar")


@pytest_asyncio.fixture
async def branded_only(tmp_path, monkeypatch):
    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch,
                               _populate(with_unbranded=False)):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the filter bar")


def _total(result) -> float:
    return round(sum(result["revenue"]), 2)


class TestThePredicateHasOneHome:
    def test_a_named_brand_binds_a_parameter(self):
        params: list = []
        assert brand_where("NEOGEN", params) == "LOWER(l.brand) = LOWER(?)"
        assert params == ["NEOGEN"]

    def test_the_unknown_bucket_binds_nothing_and_matches_null(self):
        params: list = []
        sql = brand_where(UNKNOWN_BRAND, params)
        assert params == [], "a bucket is not a value to bind"
        assert "IS NULL" in sql and "TRIM" in sql

    @pytest.mark.parametrize("spelling", ["Unknown", "unknown", "UNKNOWN", "  UnKnOwN  "])
    def test_the_bucket_is_recognised_however_it_is_spelled(self, spelling):
        params: list = []
        assert "IS NULL" in brand_where(spelling, params)
        assert params == []

    def test_the_alias_is_honoured(self):
        """Three aliases are in use — `l` for the line, `g` for Gold, `s`
        where the line level stands in for the order. Passing the wrong one
        is exactly how #124's 500 happened."""
        for alias in ("l", "g", "s"):
            assert brand_where("X", [], alias).startswith(f"LOWER({alias}.brand)")
            assert f"{alias}.brand IS NULL" in brand_where(UNKNOWN_BRAND, [], alias)

    def test_no_hand_written_copies_are_left(self):
        """The helper is only worth having if nothing bypasses it."""
        import inspect
        from core.repositories import revenue

        src = inspect.getsource(revenue)
        assert "brand) = LOWER(?)" not in src, (
            "a hand-written brand predicate reappeared; it will drift"
        )


@needs_postgres
class TestTheBucketIsOfferedInTheFilter:
    @pytest.mark.asyncio
    async def test_it_is_listed_when_goods_have_no_brand(self, goods):
        store = goods
        names = [b["name"] for b in await store.get_brands()]
        assert "BrandA" in names
        assert UNKNOWN_BRAND in names

    @pytest.mark.asyncio
    async def test_it_is_not_offered_when_every_product_has_a_brand(self, branded_only):
        """An option that returns nothing is worse than no option."""
        store = branded_only
        names = [b["name"] for b in await store.get_brands()]
        assert names == ["BrandA"]

    @pytest.mark.asyncio
    async def test_a_real_brand_is_never_shadowed_by_the_bucket(self, goods):
        """The bucket borrows a word. If a supplier were ever called
        `Unknown`, selecting it would return the unbranded goods instead —
        this pins that the word is currently free."""
        store = goods
        names = [b["name"] for b in await store.get_brands()]
        assert names.count(UNKNOWN_BRAND) == 1


@needs_postgres
class TestSelectingItReturnsTheGoods:
    @pytest.mark.asyncio
    async def test_the_bucket_holds_both_spellings_of_absent(self, goods):
        store = goods
        r = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", brand=UNKNOWN_BRAND,
        )
        assert r["measure"] == "goods_value"
        assert _total(r) == 2000.00, "the NULL-brand product and the ''-brand one"

    @pytest.mark.asyncio
    async def test_a_named_brand_still_answers_as_before(self, goods):
        """Verified on the production copy too: NEOGEN came to
        ₴13,054,166.02 through the helper and through the hand-written
        predicate it replaced."""
        store = goods
        r = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", brand="BrandA",
        )
        assert _total(r) == 1000.00

    @pytest.mark.asyncio
    async def test_the_bucket_and_the_brands_add_up(self, goods):
        """Whatever is not in a brand is in the bucket, and nothing is in
        both. That is the property the 6% was quietly breaking."""
        store = goods
        named = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", brand="BrandA",
        )
        bucket = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", brand=UNKNOWN_BRAND,
        )
        everything = await store.get_revenue_trend(
            *WINDOW, sales_type="retail", category_id=1,
        )
        assert _total(named) + _total(bucket) == _total(everything)
