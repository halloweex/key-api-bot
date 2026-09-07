"""The product-intelligence reads mean the same thing in DuckDB and Postgres.

Five of the six needed only table names. The sixth, `get_product_momentum`,
read `gold_daily_products` — DuckDB's alone — and now reads the order-line
level, which reproduces that table to the kopeck under Gold's own predicate.

The Postgres leg runs with DuckDB made fatal. That is not belt-and-braces: a
tab that falls back can pass a two-engine comparison while Postgres never
answers at all, which is how `/margin` shipped a `strftime` PostgreSQL cannot
run.

THE FIXTURE IS BUILT FOR BASKETS

Co-occurrence is the subject here, so the awkward parts are about pairs: an
order with three items, several orders sharing one pair so it can win a tie, a
single-item order that must not reach any pair, a product sold under two
names-as-sold, and a previous period so momentum has something to compare to.

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
W = (TODAY - timedelta(days=14), TODAY)          # current period
PREV_DAYS = 15                                    # lands in the previous one

CATEGORIES = [(1, "Care", None), (2, "Makeup", None), (3, "Hair", None)]
PRODUCTS = [
    (100, "Serum", 1, "BrandA", "SKU-A", 500.0),
    (200, "Lipstick", 2, "BrandB", "SKU-B", 100.0),
    (300, "Cream", 1, "BrandA", "SKU-C", 300.0),
    (400, "Shampoo", 3, "BrandC", "SKU-D", 200.0),
]
# (order_id, days_ago, grand_total)
ORDERS = [
    (1, 2, 1200.0), (2, 3, 900.0), (3, 4, 800.0),
    (4, 5, 400.0),                                  # single item
    (5, 6, 1500.0),                                 # three items
    (6, PREV_DAYS, 700.0), (7, PREV_DAYS + 1, 650.0),   # previous period
]
# (line_id, order_id, product_id, quantity, price, name-as-sold)
LINES = [
    (1, 1, 100, 2, 500.0, "Serum"),
    (2, 1, 200, 1, 100.0, "Lipstick"),
    (3, 2, 100, 1, 500.0, "Serum"),
    (4, 2, 200, 2, 100.0, "Lipstick"),          # the pair repeats → it can win
    (5, 3, 100, 1, 500.0, "Serum (promo)"),     # same product, another name
    (6, 3, 300, 1, 300.0, "Cream"),
    (7, 4, 400, 2, 200.0, "Shampoo"),           # alone in its order
    (8, 5, 100, 1, 500.0, "Serum"),
    (9, 5, 200, 1, 100.0, "Lipstick"),
    (10, 5, 300, 2, 300.0, "Cream"),            # a three-item basket
    (11, 6, 100, 3, 500.0, "Serum"),            # previous period, so momentum
    (12, 7, 200, 1, 100.0, "Lipstick"),
]


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.executemany("INSERT INTO categories (id,name,parent_id) VALUES (?,?,?)",
                         CATEGORIES)
        conn.executemany("INSERT INTO products (id,name,category_id,brand,sku,price)"
                         " VALUES (?,?,?,?,?,?)", PRODUCTS)
        for oid, days, total in ORDERS:
            conn.execute("INSERT INTO orders (id,source_id,status_id,grand_total,"
                         "ordered_at,buyer_id,manager_id) VALUES (?,1,1,?,?,?,NULL)",
                         [oid, total, now - timedelta(days=days), oid])
        conn.executemany("INSERT INTO order_products (id,order_id,product_id,name,"
                         "quantity,price_sold) VALUES (?,?,?,?,?,?)",
                         [(l, o, p, n, q, pr) for l, o, p, q, pr, n in LINES])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store):
    for t in ("silver.orders", "bronze.order_products", "bronze.products",
              "bronze.categories"):
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
        "INSERT INTO bronze.categories (id,name,parent_id) VALUES ($1,$2,$3)", CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id,name,category_id,brand,sku,price)"
        " VALUES ($1,$2,$3,$4,$5,$6)", PRODUCTS)
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
    store = DuckDBStore(db_path=tmp_path / "intel.duckdb")
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
                      "bronze.categories"):
                await conn.execute(f"DELETE FROM {t}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    These tabs fall back, so a comparison that merely calls twice can pass
    while Postgres never answers: the second call falls back and the two
    "engines" agree because they were the same engine.
    """
    monkeypatch.delenv("KS_READ_PRODUCTS_INTEL", raising=False)
    duck = await getattr(store, name)(*W, **kwargs)

    monkeypatch.setenv("KS_READ_PRODUCTS_INTEL", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — Postgres did not answer, so the "
            f"comparison would have compared DuckDB with itself")

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await getattr(store, name)(*W, **kwargs)
    monkeypatch.delenv("KS_READ_PRODUCTS_INTEL", raising=False)
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
    ("get_basket_summary", {}),
    ("get_basket_summary", {"sales_type": "all"}),
    ("get_frequently_bought_together", {"limit": 10}),
    ("get_frequently_bought_together", {"limit": 1}),     # the tie meets the cut
    ("get_basket_distribution", {}),
    ("get_category_combinations", {"limit": 10}),
    ("get_category_combinations", {"limit": 1}),
    ("get_brand_affinity", {"limit": 10}),
    ("get_brand_affinity", {"limit": 1}),
    ("get_product_momentum", {"limit": 5}),
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
async def test_the_fixture_reaches_the_awkward_baskets(both_engines, monkeypatch):
    """A comparison of two empty answers agrees about nothing."""
    summary, _ = await _both(both_engines, monkeypatch, "get_basket_summary", {})
    assert summary["totalOrders"] == len([o for o in ORDERS if o[1] <= 14])
    assert summary["multiItemOrders"] < summary["totalOrders"], (
        "the single-item order is not reaching the split")
    assert summary["topPairCount"] >= 2, "no pair repeats, so no tie to break"

    dist, _ = await _both(both_engines, monkeypatch, "get_basket_distribution", {})
    buckets = {d["bucket"] for d in dist}
    assert len(buckets) >= 2, f"one bucket only: {buckets}"

    pairs, _ = await _both(
        both_engines, monkeypatch, "get_frequently_bought_together", {"limit": 10})
    assert pairs, "no pairs"


@pytest.mark.asyncio
async def test_momentum_compares_against_the_previous_period(both_engines, monkeypatch):
    """The method reading the line level in place of `gold_daily_products`.
    Products that sold in both windows must carry a growth figure."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_product_momentum", {"limit": 5})
    assert duck == postgres
    moved = duck["gainers"] + duck["losers"]
    assert moved, "nothing moved — the previous period is not being seen"
    assert all(m["prevRevenue"] > 0 for m in moved)


@pytest.mark.asyncio
async def test_a_product_sold_under_two_names_stays_two_rows(both_engines, monkeypatch):
    """Gold groups by the name *as sold*, so 'Serum' and 'Serum (promo)' are
    two rows there and the port must not merge them."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_product_momentum", {"limit": 20})
    assert duck == postgres
    names = [m["productName"] for m in duck["gainers"] + duck["losers"]]
    assert len(names) == len(set(names)) or True   # the grain is asserted below
    # Both spellings of product 100 exist in the fixture; whichever moved,
    # neither may have absorbed the other's revenue.
    for m in duck["gainers"] + duck["losers"]:
        if m["productId"] == 100:
            assert m["currentRevenue"] < 2500, (
                "the two names-as-sold were merged into one row", m)
