"""The query behind `/api/stock-margin`, on a Postgres seeded with every case.

`MarginMixin.get_stock_margin_rows` reads the shelf from
`app.sku_inventory_status` and the last 90 days from `silver.order_lines`.
What it must get right, each pinned below on one offer:

* the retail units and revenue count retail lines inside the window, and no
  wholesale line, no return and no line from before the window;
* the units that left the stock count the wholesale line too — a wholesale
  sale empties the same shelf — but still no return;
* an offer with nothing on the shelf is not a row at all;
* a category hangs under its root, as the `/margin` cross-tab files it.

`app.sku_inventory_status` is not among the tables `tab_in_postgres` clears,
so this file empties it for its own rows and puts back what was there.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import (
    derive, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TODAY = date(2026, 11, 26)
SINCE = TODAY - timedelta(days=89)          # the window: 90 Kyiv days, today included
B2B_MANAGER = 15                            # core.duckdb_constants.B2B_MANAGER_ID


def utc(day: date, hour: int = 10) -> datetime:
    return datetime(day.year, day.month, day.day, hour, 0, tzinfo=timezone.utc)


CATEGORIES = [(1, "Face", None), (2, "Creams", 1)]
PRODUCTS = [
    (100, "Cream", 2, "BrandA", "SKU-A", 600.0),
    (200, "Serum", 1, "BrandB", "SKU-B", 1000.0),
]
# (id, source_id, status_id, grand_total, ordered_at, buyer_id, manager_id, promocode)
ORDERS = [
    (1, 4, 1, 2200.0, utc(TODAY - timedelta(days=6)), 1, None, None),   # retail, in the window
    (2, 4, 1, 600.0, utc(SINCE - timedelta(days=1)), 2, None, None),    # retail, the day before it
    (3, 1, 1, 1200.0, utc(TODAY - timedelta(days=16)), 3, B2B_MANAGER, None),  # wholesale
    (4, 4, 19, 600.0, utc(TODAY - timedelta(days=11)), 4, None, None),  # returned
    (5, 4, 1, 550.0, utc(SINCE), 5, None, None),                        # retail, the window's first day
]
# (id, order_id, product_id, name, quantity, price_sold)
LINES = [
    (1, 1, 100, "Cream", 2, 600.0),
    (2, 1, 200, "Serum", 1, 1000.0),
    (3, 2, 100, "Cream", 1, 600.0),
    (4, 3, 100, "Cream", 3, 400.0),
    (5, 4, 100, "Cream", 1, 600.0),
    (6, 5, 100, "Cream", 1, 550.0),
]
STAMP = datetime(2026, 11, 26, 17, 58, tzinfo=timezone.utc)
# (offer_id, product_id, sku, name, brand, category_id, quantity, reserve, price, purchased_price)
SHELF = [
    (11, 100, "SKU-A", "Cream", "BrandA", 2, 10, 1, 600.0, 300.0),
    (22, 200, "SKU-B", "Serum", "BrandB", 1, 0, 0, 1000.0, 400.0),        # sold out: not a row
    (33, 300, "SKU-C", "Gift Certificate", None, None, 5, 0, 1000.0, 40.0),
]


async def _populate(conn, pool):
    await seed(conn, categories=CATEGORIES, products=PRODUCTS, orders=ORDERS, lines=LINES)
    await derive(pool)
    await conn.execute("DELETE FROM app.sku_inventory_status")
    await conn.executemany(
        "INSERT INTO app.sku_inventory_status (offer_id, product_id, sku, name, brand,"
        " category_id, quantity, reserve, price, purchased_price, first_seen_at, updated_at)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        [(*s, date(2026, 1, 27), STAMP) for s in SHELF])


@pytest_asyncio.fixture
async def store(tmp_path, monkeypatch):
    import asyncpg as _asyncpg

    conn = await _asyncpg.connect(DSN)
    try:
        kept = await conn.fetch("SELECT * FROM app.sku_inventory_status")
    finally:
        await conn.close()
    store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the stock margin")
    try:
        async with tab_in_postgres(DSN, monkeypatch, _populate):
            yield store
    finally:
        conn = await _asyncpg.connect(DSN)
        try:
            await conn.execute("DELETE FROM app.sku_inventory_status")
            for r in kept:
                columns = list(r.keys())
                await conn.execute(
                    f"INSERT INTO app.sku_inventory_status ({', '.join(columns)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})", *r.values())
        finally:
            await conn.close()


def by_offer(rows):
    from core.stock_margin import ROW_COLUMNS

    return {r[0]: dict(zip(ROW_COLUMNS, r)) for r in rows}


@pytest.mark.asyncio
async def test_only_offers_with_stock_are_rows(store):
    rows = by_offer(await store.get_stock_margin_rows(SINCE, TODAY))
    assert sorted(rows) == [11, 33]


@pytest.mark.asyncio
async def test_retail_counts_the_window_and_no_wholesale_and_no_return(store):
    cream = by_offer(await store.get_stock_margin_rows(SINCE, TODAY))[11]
    # order 1 (2 × 600) and the window's first day (1 × 550); not the day before
    # it, not the wholesale order, not the return
    assert (float(cream["retail_units"]), float(cream["retail_revenue"])) == (3.0, 1750.0)


@pytest.mark.asyncio
async def test_what_left_the_stock_counts_wholesale_but_not_a_return(store):
    cream = by_offer(await store.get_stock_margin_rows(SINCE, TODAY))[11]
    assert float(cream["moved_units"]) == 3.0 + 3.0


@pytest.mark.asyncio
async def test_a_category_hangs_under_its_root(store):
    cream = by_offer(await store.get_stock_margin_rows(SINCE, TODAY))[11]
    assert (cream["category"], cream["root_category"]) == ("Creams", "Face")


@pytest.mark.asyncio
async def test_the_service_answers_from_these_rows(store, monkeypatch):
    from web.services import margin_service

    async def _store():
        return store

    monkeypatch.setattr(margin_service, "get_store", _store)
    answer = await margin_service.get_stock_margin(today=TODAY)
    items = {i["offer_id"]: i for i in answer["items"]}
    assert set(items) == {11, 33}
    assert items[33]["kind"] == "certificate"
    cream = items[11]
    assert (cream["available"], cream["margin"], cream["profit"]) == (9, 0.5, 2700.0)
    assert cream["realized_margin_90d"] == round((1750 - 300 * 3) / 1750, 6)
    assert cream["days_of_supply"] == round(9 / (6 / 90), 1)
    assert answer["goods"]["positions"] == 1
    assert answer["excluded"]["certificate"] == {"positions": 1, "available": 5, "retail_value": 5000.0}
    assert answer["snapshot_at"] == STAMP.isoformat()
