"""`/reports` means the same thing in DuckDB and Postgres.

`tests/unit/test_pg_reports_read.py` proves the bodies are one text and that
nothing goes round the router. Necessary and not sufficient: identical SQL
still means different things where the engines differ — how a correlated
`SUM()` behaves over an order with no line items, what `COUNT(CASE …)` returns
for an empty group, where `NULL`s sort, and which rows a `LIMIT` keeps when the
sort has ties.

So this runs the real thing. The same awkward catalogue goes into a DuckDB
store and a live PostgreSQL, each method is called twice — once with
`KS_READ_REPORTS` unset and once set to `postgres` — and the **whole result**
is compared.

THE FIXTURE IS DELIBERATELY AWKWARD

A report that agrees on easy data proves nothing. It carries an order with
**no line items at all** — 323 of those exist in production and they carry
revenue, which is precisely why `get_report_summary` counts orders at the
order grain and reaches for the lines only through a subquery — a return, an
order from a retired source that must reach no total, a product with no brand,
two products tied on quantity so the top-N cut has a tie to break, and a
category tree two deep so the filter has descendants to find.

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
WINDOW = (TODAY - timedelta(days=30), TODAY)


def _d(days_ago: int) -> date:
    return TODAY - timedelta(days=days_ago)


CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Lipstick", 3, "BrandB", "SKU-B", 100.0),
    (300, "Unbranded", 3, None, "SKU-C", 250.0),
]

# (order_id, source_id, grand_total, days_ago, is_return, sales_type, active)
ORDERS = [
    (1, 1, 1200.0, 2, False, "retail", True),
    (2, 2, 600.0, 3, False, "retail", True),
    (3, 1, 400.0, 4, True, "retail", True),      # a return
    (4, 3, 900.0, 5, False, "retail", False),    # retired source — reaches nothing
    (5, 4, 750.0, 6, False, "retail", True),     # NO LINE ITEMS, and revenue
    (6, 1, 300.0, 7, False, "b2b", True),        # another sales_type
]
# (line_id, order_id, product_id, quantity, price_sold)
LINES = [
    (1, 1, 100, 2, 500.0),
    (2, 1, 200, 3, 100.0),
    (3, 2, 100, 1, 500.0),
    (4, 3, 200, 1, 100.0),
    (5, 4, 300, 4, 250.0),
    (6, 6, 300, 3, 100.0),   # ties with line 2 on quantity
]


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO categories (id, name, parent_id) VALUES (?,?,?)", CATEGORIES)
        conn.executemany(
            "INSERT INTO products (id, name, category_id, brand, sku, price)"
            " VALUES (?,?,?,?,?,?)", PRODUCTS)
        for oid, src, total, days, is_ret, stype, active in ORDERS:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id) VALUES (?,?,?,?,?,7,NULL)",
                [oid, src, 19 if is_ret else 1, total, now - timedelta(days=days)])
        conn.executemany(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity,"
            " price_sold) SELECT ?, ?, ?, p.name, ?, ? FROM products p WHERE p.id = ?",
            [(lid, oid, pid, qty, price, pid) for lid, oid, pid, qty, price in LINES])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store):
    """Postgres gets the Silver the DuckDB rebuild produced.

    Copied rather than recomputed on purpose: this test is about the *report*
    queries agreeing, and seeding Silver twice from two rebuilds would fold in
    whatever the warehouse layer does differently, which is
    `reconcile_silver`'s job and not this one.
    """
    await conn.execute("DELETE FROM silver.orders")
    await conn.execute("DELETE FROM bronze.order_products")
    await conn.execute("DELETE FROM bronze.products")
    await conn.execute("DELETE FROM bronze.categories")

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
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES ($1,$2,$3)",
        CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, category_id, brand, sku, price)"
        " VALUES ($1,$2,$3,$4,$5,$6)", PRODUCTS)
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)",
        [tuple(r) for r in lines])
    await conn.executemany(
        "INSERT INTO silver.orders (id, source_id, status_id, grand_total,"
        " ordered_at, buyer_id, manager_id, order_date, is_return, sales_type,"
        " is_active_source, source_name, is_new_customer,"
        " buyer_first_order_date, promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "reports.duckdb")
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
            await conn.execute("DELETE FROM silver.orders")
            await conn.execute("DELETE FROM bronze.order_products")
            await conn.execute("DELETE FROM bronze.products")
            await conn.execute("DELETE FROM bronze.categories")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    monkeypatch.delenv("KS_READ_REPORTS", raising=False)
    duck = await getattr(store, name)(*WINDOW, **kwargs)
    monkeypatch.setenv("KS_READ_REPORTS", "postgres")
    postgres = await getattr(store, name)(*WINDOW, **kwargs)
    monkeypatch.delenv("KS_READ_REPORTS", raising=False)
    return duck, postgres


def _comparable(value):
    """Money to six places. DuckDB divides a DECIMAL into a DOUBLE and
    Postgres into a NUMERIC, so the two legitimately differ in the last bits —
    far below anything this tab renders, far above the noise."""
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _comparable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_comparable(v) for v in value]
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


CALLS = (
    ("get_report_summary", {}),
    ("get_report_summary", {"sales_type": "all"}),
    ("get_report_summary", {"source_id": 1}),
    ("get_report_summary", {"category_id": 1}),        # a parent, two deep
    ("get_report_summary", {"category_id": 3}),
    ("get_report_summary", {"category_id": 99999}),    # absent — was a 500
    ("get_report_summary", {"brand": "BrandA"}),
    ("get_report_summary", {"brand": "Unknown"}),      # the NULL-brand bucket
    ("get_report_summary", {"category_id": 1, "brand": "BrandA"}),
    ("get_report_top_products", {}),
    ("get_report_top_products", {"limit": 2}),         # the tie meets the cut
    ("get_report_top_products", {"sales_type": "all"}),
    ("get_report_top_products", {"source_id": 1}),
    ("get_report_top_products", {"category_id": 1}),
    ("get_report_top_products", {"brand": "Unknown"}),
    ("get_report_products_by_source", {}),
    ("get_report_products_by_source", {"sales_type": "all"}),
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
async def test_the_fixture_is_not_empty(both_engines, monkeypatch):
    """Two empty answers agree about nothing, and every awkward row has to be
    reachable or the comparison above is decoration."""
    summary, _ = await _both(both_engines, monkeypatch, "get_report_summary", {})
    totals = summary["totals"]
    assert totals["orders_count"] == 3, summary      # 1, 2, 5 — retail, not returned
    assert totals["returns_count"] == 1
    assert totals["revenue"] == 2550.0               # 1200 + 600 + 750

    # The order with no line items is counted and carries its revenue, while
    # contributing nothing to `products_sold`. That asymmetry is the whole
    # reason this method aggregates at the order grain.
    assert totals["products_sold"] == 6              # 2+3 on o1, 1 on o2, none on o5
    assert any(s["source_id"] == 4 and s["revenue"] == 750.0
               for s in summary["sources"]), "the line-less order is missing"

    # The retired source reaches no total at all.
    assert all(s["source_id"] != 3 for s in summary["sources"])

    products, _ = await _both(both_engines, monkeypatch, "get_report_top_products", {})
    assert products, "no products"
    assert any(p["sku"] == "SKU-C" for p in products), "the NULL-brand product"


@pytest.mark.asyncio
async def test_the_tie_is_cut_the_same_way_by_both(both_engines, monkeypatch):
    """Lipstick and Unbranded both sell 3 in the retail window. With `limit=2`
    the cut falls inside the tied group, so an unbroken tie means the two
    engines keep *different products*, not the same ones reordered."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_report_top_products", {"limit": 2})
    assert [p["product_name"] for p in duck] == [p["product_name"] for p in postgres]
    assert len(duck) == 2


@pytest.mark.asyncio
async def test_an_absent_category_is_an_empty_report_on_both(both_engines, monkeypatch):
    """`category_id IN ()` is a parser error on both engines; the guard turns
    it into a filter that matches nothing, which is what it means."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_report_summary", {"category_id": 99999})
    assert duck["totals"]["orders_count"] == 0
    assert postgres == duck


@pytest.mark.asyncio
async def test_the_category_tree_is_the_same_list(both_engines, monkeypatch):
    """It becomes an `IN (...)`, so a different order would be a different
    filter to compare rather than a different answer."""
    store = both_engines
    monkeypatch.delenv("KS_READ_REPORTS", raising=False)
    duck = await store._category_ids(1)
    monkeypatch.setenv("KS_READ_REPORTS", "postgres")
    postgres = await store._category_ids(1)
    monkeypatch.delenv("KS_READ_REPORTS", raising=False)
    assert duck == postgres == [1, 2]
