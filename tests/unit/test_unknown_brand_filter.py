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
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_constants import UNKNOWN_BRAND, brand_where
from core.duckdb_store import DuckDBStore

WINDOW = (date.today() - timedelta(days=30), date.today())


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "brands.duckdb")
    await s.connect()
    return s


async def _seed(store, *, with_unbranded=True):
    """Three products: a brand, no brand at all, and a brand set to ''.

    Both spellings of absent have to land in the same bucket — one product
    never had the KeyCRM field, the other has it empty — because from the
    dashboard's side they are the same fact.
    """
    now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    async with store.connection() as conn:
        conn.execute("INSERT INTO categories (id, name, parent_id) VALUES (1, 'Care', NULL)")
        rows = [(100, 'Serum', 'BrandA')]
        if with_unbranded:
            rows += [(200, 'Cream', None), (300, 'Toner', '')]
        for pid, name, brand in rows:
            conn.execute(
                "INSERT INTO products (id, name, category_id, brand, sku, price) "
                "VALUES (?, ?, 1, ?, ?, 500.0)",
                [pid, name, brand, f"SKU-{pid}"],
            )
        for oid in (1, 2, 3):
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                "buyer_id, manager_id) VALUES (?, 4, 1, 1000.0, ?, ?, NULL)",
                [oid, now - timedelta(days=oid), 10 + oid],
            )
        lines = [(1, 1, 100)]
        if with_unbranded:
            lines += [(2, 2, 200), (3, 3, 300)]
        for lid, oid, pid in lines:
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity, "
                "price_sold) VALUES (?, ?, ?, 'x', 2, 500.0)",
                [lid, oid, pid],
            )
    await store.refresh_warehouse_layers(trigger="manual")


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


class TestTheBucketIsOfferedInTheFilter:
    @pytest.mark.asyncio
    async def test_it_is_listed_when_goods_have_no_brand(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            names = [b["name"] for b in await store.get_brands()]
            assert "BrandA" in names
            assert UNKNOWN_BRAND in names
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_is_not_offered_when_every_product_has_a_brand(self, tmp_path):
        """An option that returns nothing is worse than no option."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store, with_unbranded=False)
            names = [b["name"] for b in await store.get_brands()]
            assert names == ["BrandA"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_real_brand_is_never_shadowed_by_the_bucket(self, tmp_path):
        """The bucket borrows a word. If a supplier were ever called
        `Unknown`, selecting it would return the unbranded goods instead —
        this pins that the word is currently free."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            names = [b["name"] for b in await store.get_brands()]
            assert names.count(UNKNOWN_BRAND) == 1
        finally:
            await store.close()


class TestSelectingItReturnsTheGoods:
    @pytest.mark.asyncio
    async def test_the_bucket_holds_both_spellings_of_absent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(
                *WINDOW, sales_type="retail", brand=UNKNOWN_BRAND,
            )
            assert r["measure"] == "goods_value"
            assert _total(r) == 2000.00, "the NULL-brand product and the ''-brand one"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_named_brand_still_answers_as_before(self, tmp_path):
        """Verified on the production copy too: NEOGEN came to
        ₴13,054,166.02 through the helper and through the hand-written
        predicate it replaced."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            r = await store.get_revenue_trend(
                *WINDOW, sales_type="retail", brand="BrandA",
            )
            assert _total(r) == 1000.00
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_bucket_and_the_brands_add_up(self, tmp_path):
        """Whatever is not in a brand is in the bucket, and nothing is in
        both. That is the property the 6% was quietly breaking."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
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
        finally:
            await store.close()
