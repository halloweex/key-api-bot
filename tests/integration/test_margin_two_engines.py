"""`/margin` means the same thing in DuckDB and Postgres.

The cheapest port and the one with the sharpest arithmetic: margin is revenue
minus COGS, COGS comes from `offer_stocks.purchased_price` joined by SKU, and
the interesting cases are all about *absence* — a SKU with no cost row, a cost
of zero, a product whose SKU is NULL. Each of those decides whether a line
counts toward `costed_revenue`, and a difference of one line moves the
percentage the page shows.

`tests/unit/test_pg_margin_read.py` proves the wiring and that every hole
survived its f-string. This proves the two engines agree on the numbers.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

TODAY = date.today()
W = (TODAY - timedelta(days=25), TODAY)

CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Lipstick", 3, "BrandB", "SKU-B", 100.0),
    (300, "Costless", 3, "BrandB", "SKU-C", 250.0),   # no cost row at all
    (400, "ZeroCost", 3, "BrandC", "SKU-D", 300.0),   # cost row, but 0
    (500, "NoSku", 3, "BrandC", None, 150.0),         # NULL sku — joins nothing
]
# (offer_id, sku, price, purchased_price)
STOCKS = [
    (1, "SKU-A", 500.0, 200.0),
    (2, "SKU-B", 100.0, 40.0),
    (4, "SKU-D", 300.0, 0.0),        # zero is "no cost", not "free"
]
ORDERS = [(1, 1, 2000.0, 2), (2, 2, 900.0, 3), (3, 1, 450.0, 4)]
LINES = [
    (1, 1, 100, 2, 500.0),
    (2, 1, 200, 3, 100.0),
    (3, 1, 300, 1, 250.0),     # costless
    (4, 2, 400, 2, 300.0),     # zero-cost
    (5, 2, 500, 2, 150.0),     # NULL sku
    (6, 3, 100, 1, 450.0),     # same SKU, a discounted line
]


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.executemany("INSERT INTO categories (id,name,parent_id) VALUES (?,?,?)",
                         CATEGORIES)
        conn.executemany("INSERT INTO products (id,name,category_id,brand,sku,price)"
                         " VALUES (?,?,?,?,?,?)", PRODUCTS)
        conn.executemany("INSERT INTO offer_stocks (id,sku,price,purchased_price,"
                         "quantity,reserve) VALUES (?,?,?,?,0,0)", STOCKS)
        for oid, src, total, days in ORDERS:
            conn.execute("INSERT INTO orders (id,source_id,status_id,grand_total,"
                         "ordered_at,buyer_id,manager_id) VALUES (?,?,1,?,?,?,NULL)",
                         [oid, src, total, now - timedelta(days=days), oid])
        conn.executemany(
            "INSERT INTO order_products (id,order_id,product_id,name,quantity,"
            "price_sold) SELECT ?,?,?,p.name,?,? FROM products p WHERE p.id = ?",
            [(lid, oid, pid, q, pr, pid) for lid, oid, pid, q, pr in LINES])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store):
    for t in ("silver.orders", "bronze.order_products", "bronze.products",
              "bronze.categories", "bronze.offer_stocks"):
        await conn.execute(f"DELETE FROM {t}")

    async with store.connection() as duck:
        silver = duck.execute(
            "SELECT id, source_id, status_id, grand_total, ordered_at, buyer_id,"
            " manager_id, order_date, is_return, sales_type, is_active_source,"
            " source_name, is_new_customer, buyer_first_order_date, promocode"
            " FROM silver_orders ORDER BY id").fetchall()
        lines = duck.execute(
            "SELECT id, order_id, product_id, name, quantity, price_sold"
            " FROM order_products ORDER BY id").fetchall()

    await conn.executemany(
        "INSERT INTO bronze.categories (id,name,parent_id) VALUES ($1,$2,$3)",
        CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id,name,category_id,brand,sku,price)"
        " VALUES ($1,$2,$3,$4,$5,$6)", PRODUCTS)
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id,sku,price,purchased_price,quantity,"
        "reserve) VALUES ($1,$2,$3,$4,0,0)", STOCKS)
    await conn.executemany(
        "INSERT INTO bronze.order_products (id,order_id,product_id,name,quantity,"
        "price_sold) VALUES ($1,$2,$3,$4,$5,$6)", [tuple(r) for r in lines])
    await conn.executemany(
        "INSERT INTO silver.orders (id,source_id,status_id,grand_total,ordered_at,"
        "buyer_id,manager_id,order_date,is_return,sales_type,is_active_source,"
        "source_name,is_new_customer,buyer_first_order_date,promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "margin.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_duckdb(store)
        async with pool.acquire() as conn:
            await _seed_postgres(conn, store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        async with pool.acquire() as conn:
            for t in ("silver.orders", "bronze.order_products", "bronze.products",
                      "bronze.categories", "bronze.offer_stocks"):
                await conn.execute(f"DELETE FROM {t}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    monkeypatch.delenv("KS_READ_MARGIN", raising=False)
    duck = await getattr(store, name)(*W, **kwargs)
    monkeypatch.setenv("KS_READ_MARGIN", "postgres")
    postgres = await getattr(store, name)(*W, **kwargs)
    monkeypatch.delenv("KS_READ_MARGIN", raising=False)
    return duck, postgres


def _comparable(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, dict):
        return {k: _comparable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_comparable(x) for x in v]
    if hasattr(v, "as_tuple") or isinstance(v, float):
        return round(float(v), 6)
    return v


CALLS = (
    ("get_margin_overview", {}),
    ("get_margin_overview", {"sales_type": "all"}),
    ("get_margin_by_brand", {}),
    ("get_margin_by_brand", {"limit": 2}),
    ("get_margin_by_category", {}),
    ("get_margin_trend", {}),
    ("get_margin_brand_category", {"min_revenue": 0}),
    ("get_margin_brand_category", {"min_revenue": 100000}),   # empty result
    ("get_margin_alerts", {"min_revenue": 0}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,kwargs", CALLS,
    ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
)
async def test_both_engines_return_the_same_answer(
    both_engines, monkeypatch, name, kwargs,
):
    duck, postgres = await _both(both_engines, monkeypatch, name, kwargs)
    assert _comparable(postgres) == _comparable(duck)


@pytest.mark.asyncio
async def test_the_three_kinds_of_missing_cost_all_land(both_engines, monkeypatch):
    """Each is a different reason a line has no COGS, and each has to be
    excluded from `costed_revenue` while still counting toward revenue — that
    difference is what `coverage_pct` measures."""
    overview, pg = await _both(both_engines, monkeypatch, "get_margin_overview", {})
    assert overview == pg

    priced = sum(q * p for _l, _o, pid, q, p in LINES
                 if pid in (100, 200))                    # the two with real cost
    assert overview["costed_revenue"] == priced, overview
    assert overview["total_revenue"] > overview["costed_revenue"], (
        "the costless, zero-cost and NULL-sku lines are not reaching revenue")
    assert 0 < overview["coverage_pct"] < 100
    assert overview["cogs"] > 0 and overview["profit"] > 0


@pytest.mark.asyncio
async def test_an_empty_result_agrees_too(both_engines, monkeypatch):
    """The threshold nobody clears: two engines must return the same nothing."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_margin_brand_category",
        {"min_revenue": 100000})
    assert duck == postgres == []


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(both_engines, monkeypatch):
    by_brand, _ = await _both(both_engines, monkeypatch, "get_margin_by_brand", {})
    assert by_brand, "no brands"
    assert {b["brand"] for b in by_brand} >= {"BrandA", "BrandB"}
    by_cat, _ = await _both(both_engines, monkeypatch, "get_margin_by_category", {})
    assert by_cat, "no categories"
    trend, _ = await _both(both_engines, monkeypatch, "get_margin_trend", {})
    assert trend, "no trend"
