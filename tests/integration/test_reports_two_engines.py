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

from core.duckdb_constants import B2B_MANAGER_ID
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

# (order_id, source_id, grand_total, days_ago, is_return, manager_id)
#
# `sales_type` is NOT seeded — the warehouse rebuild derives it, and the rule
# reads `manager_id`: NULL is retail, `B2B_MANAGER_ID` is b2b. Writing the word
# into the fixture and leaving the manager NULL is how the first version of
# this file claimed to have a b2b order and had four retail ones.
ORDERS = [
    (1, 1, 1200.0, 2, False, None),
    (2, 2, 600.0, 3, False, None),
    (3, 1, 400.0, 4, True, None),        # a return
    (4, 3, 900.0, 5, False, None),       # source 3 is retired — reaches nothing
    (5, 4, 750.0, 6, False, None),       # NO LINE ITEMS, and revenue
    (6, 1, 300.0, 7, False, B2B_MANAGER_ID),   # genuinely b2b
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
        for oid, src, total, days, is_ret, manager in ORDERS:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id) VALUES (?,?,?,?,?,7,?)",
                [oid, src, 19 if is_ret else 1, total,
                 now - timedelta(days=days), manager])
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
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    These tabs fall back to DuckDB when Postgres raises, so a comparison that
    merely calls the method twice can pass while Postgres never answers: the
    second call falls back and the two "engines" agree because they were the
    same engine. `get_margin_trend` shipped that way — `strftime` is DuckDB's
    alone — and the gate was green.
    """
    monkeypatch.delenv("KS_READ_REPORTS", raising=False)
    duck = await getattr(store, name)(*WINDOW, **kwargs)
    monkeypatch.setenv("KS_READ_REPORTS", "postgres")
    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — Postgres did not answer, so the "
            f"comparison would have compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
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


# Derived from the fixture above rather than written out, because I got the
# arithmetic wrong twice: once by labelling an order `b2b` in a tuple the
# seeding code never read, and once by making that order genuinely b2b and
# thereby removing the only unbranded product from the retail view. Numbers a
# human recomputes on every fixture edit are numbers that will be wrong.
RETIRED_SOURCE = 3
RETAIL_ORDERS = [o for o in ORDERS
                 if not o[4] and o[5] is None and o[1] != RETIRED_SOURCE]
ALL_ORDERS = [o for o in ORDERS if not o[4] and o[1] != RETIRED_SOURCE]
LINES_BY_ORDER = {oid for _l, oid, _p, _q, _pr in LINES}


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(both_engines, monkeypatch):
    """Two empty answers agree about nothing, and every awkward row has to be
    reachable or the comparison above is decoration."""
    summary, _ = await _both(both_engines, monkeypatch, "get_report_summary", {})
    totals = summary["totals"]

    assert totals["orders_count"] == len(RETAIL_ORDERS) == 3, summary
    assert totals["revenue"] == sum(o[2] for o in RETAIL_ORDERS)
    assert totals["returns_count"] == sum(1 for o in ORDERS if o[4])

    # The order with no line items is counted and carries its revenue while
    # contributing nothing to `products_sold`. That asymmetry is the whole
    # reason this method aggregates at the order grain, and production has 323
    # such orders.
    lineless = [o for o in RETAIL_ORDERS if o[0] not in LINES_BY_ORDER]
    assert lineless, "the fixture lost its line-less order"
    assert totals["products_sold"] == sum(
        q for _l, oid, _p, q, _pr in LINES
        if oid in {o[0] for o in RETAIL_ORDERS})
    assert any(s["source_id"] == lineless[0][1]
               and s["revenue"] == lineless[0][2]
               for s in summary["sources"]), "the line-less order is missing"

    # The retired source reaches no total at all.
    assert all(s["source_id"] != RETIRED_SOURCE for s in summary["sources"])

    # …and `all` picks up the b2b order the default excludes, so those two
    # parametrised cases compare different rows rather than the same ones
    # twice.
    every, _ = await _both(
        both_engines, monkeypatch, "get_report_summary", {"sales_type": "all"})
    assert every["totals"]["orders_count"] == len(ALL_ORDERS) == 4
    assert every["totals"]["revenue"] == sum(o[2] for o in ALL_ORDERS)

    assert (await _both(both_engines, monkeypatch,
                        "get_report_top_products", {}))[0], "no products"

    # The NULL-brand product sells only on the b2b order and the retired one,
    # so `all` is where it surfaces — and the `Unknown` bucket has to reach it,
    # since equality never matches NULL and that bucket was displayable but
    # unselectable until `brand_where` named it.
    unbranded = next(p for p in PRODUCTS if p[3] is None)
    every_product, _ = await _both(
        both_engines, monkeypatch, "get_report_top_products", {"sales_type": "all"})
    assert any(p["sku"] == unbranded[4] for p in every_product), every_product


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
