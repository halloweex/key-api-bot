"""Three margin defects the Rust port of /margin found, on a fixture that has them.

The fixture of `test_margin_two_engines.py` has none of the three, which is
why that file's literals did not move when they were fixed:

- **A SKU on two offers.** COGS was joined from `offer_stocks` by SKU, and only
  an offer's `id` is unique there: a second offer of a SKU repeated every line
  of it, doubling its revenue and adding both costs. `_SKU_COST` takes one row
  per SKU, at its highest purchase price. Production held no such SKU (918
  offers, 911 SKUs, all distinct, 2026-10-29); nothing forbids one.
- **A grandchild category.** The root was looked up one level up, so a product
  two levels down filed under its parent. `_ROOT_CAT` walks to the root.
  Production's one grandchild has no products.
- **A window whose lines sum to zero.** The category share divided by the
  window's total, and a day of free b2b or blogger shipments divided by zero,
  which Postgres raises and the page answered 503: five such days in the year
  to 2026-10-29. The share is 0 there.

Skipped without `KS_PG_DSN`, as the other tab tests are.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import (
    derive, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

TWO_OFFERS = date(2026, 9, 13)   # the duplicate SKU and the grandchild sell
ZERO_SUM = date(2026, 9, 5)      # the day whose lines sum to nothing

CATEGORIES = [(1, "Care", None), (2, "Body", 1), (3, "Lotion", 2)]
PRODUCTS = [
    (100, "Deep Lotion", 3, "BrandA", "SKU-A", 500.0),   # two levels below Care
    (200, "Cream", 1, "BrandA", "SKU-B", 100.0),
]
# (offer_id, sku, price, purchased_price)
STOCKS = [
    (1, "SKU-A", 500.0, 200.0),
    (2, "SKU-A", 500.0, 260.0),      # the same SKU on a second offer
    (3, "SKU-B", 100.0, 40.0),
]
ORDERS = [
    (1, 1, 1, 1000.0, datetime(2026, 9, 13, 9, 0, tzinfo=timezone.utc), 1, None, None),
    (2, 1, 1, 0.0, datetime(2026, 9, 5, 9, 0, tzinfo=timezone.utc), 2, None, None),
]
LINES = [
    (1, 1, 100, "Deep Lotion", 2, 500.0),
    (2, 2, 200, "Cream", 1, 100.0),
    (3, 2, 200, "Cream", 1, -100.0),   # given back: the day sums to zero
]


async def _populate(conn, pool):
    await seed(conn, categories=CATEGORIES, products=PRODUCTS, stocks=STOCKS,
               orders=ORDERS, lines=LINES)
    await derive(pool)


@pytest_asyncio.fixture
async def store(tmp_path, monkeypatch):
    store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the margin tab")
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store


@pytest.mark.asyncio
async def test_a_sku_on_two_offers_is_sold_once_at_its_highest_cost(store):
    overview = await store.get_margin_overview(TWO_OFFERS, TWO_OFFERS)
    assert overview["total_revenue"] == 1000.0, "one line, not one per offer"
    assert overview["cogs"] == 2 * 260.0


@pytest.mark.asyncio
async def test_a_grandchild_files_under_its_root(store):
    by_category = await store.get_margin_by_category(TWO_OFFERS, TWO_OFFERS)
    assert [c["category"] for c in by_category] == ["Care"]
    by_brand_category = await store.get_margin_brand_category(
        TWO_OFFERS, TWO_OFFERS, min_revenue=0)
    assert {(r["brand"], r["category"]) for r in by_brand_category} == {("BrandA", "Care")}


@pytest.mark.asyncio
async def test_a_window_that_sums_to_zero_answers_with_no_share(store):
    by_category = await store.get_margin_by_category(ZERO_SUM, ZERO_SUM)
    assert [(c["category"], c["total_revenue"], c["rev_share_pct"]) for c in by_category] \
        == [("Care", 0.0, 0)]
