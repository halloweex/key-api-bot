"""The rest of `/dashboard` means the same thing in DuckDB and Postgres.

`tests/unit/test_pg_dashboard_read.py` proves the bodies are one text and that
nothing goes round the router. Necessary and not sufficient: identical SQL
still means different things where the engines differ, and this tab has the
one place where they differ in *shape* rather than in spelling.

THE DOUGHNUT READS TWO DIFFERENT THINGS

Unfiltered it reads Gold, where DuckDB spells a channel as a column
(`instagram_revenue`) and Postgres as a dimension (`source_id = 1`) over a
table that also holds roll-up rows. Filtered it reads the order-lines level
instead, because a category or brand is a fact about a line. Both branches are
exercised, and with the same filters, so the comparison covers the switch as
well as each side of it.

THE POSTGRES GOLD IS DERIVED, NOT COPIED

`core.pg_gold.rebuild_gold` — the production path — for
`test_marketing_two_engines`' reason: copying DuckDB's Gold into Postgres'
shape would be re-implementing the shape mapping inside the test, and the test
would then agree with itself.

THE FIXTURE IS DELIBERATELY AWKWARD

An order with **no line items** and real revenue (323 of those exist in
production): the Gold branch counts it, the line-level branch cannot, and the
two branches are supposed to disagree about it — so both engines must
reproduce the same disagreement. A return, an order from a retired source that
must reach no total, two categories tied on revenue so the ordering has a tie
to break, an order whose buyer is in neither store and one with a NULL
manager so both LEFT JOINs have a miss to survive,
and a category two deep so the filter has descendants to walk.

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

CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Ampoules", 1), (4, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Ampoule", 3, "BrandB", "SKU-B", 100.0),
    (300, "Lipstick", 4, None, "SKU-C", 250.0),
]

# (buyer_id, full_name, phone). Order 3 below points at buyer 999, which is in
# neither store — that is the LEFT JOIN miss, and it is the portable way to
# make one.
#
# A buyer row with a NULL name would have been the obvious way, and it is not
# available: `buyers.full_name` is NOT NULL in DuckDB and nullable in
# `bronze.buyers`. That divergence is real and currently latent — measured on
# production 2026-09-12: zero NULL names, zero NULL manager names, zero orders
# pointing at a buyer that is not there. It matters the day KeyCRM serves one,
# because DuckDB would reject the row and Postgres accept it, and the two
# stores would then hold different numbers of buyers.
BUYERS = [(7, "Олена", "+380501112233"), (8, "Ігор", None)]
# (manager_id, name)
MANAGERS = [(B2B_MANAGER_ID, "Wholesale")]

# (order_id, source_id, grand_total, days_ago, is_return, manager_id, buyer_id)
ORDERS = [
    (1, 1, 1200.0, 2, False, None, 7),
    (2, 2,  600.0, 3, False, None, 7),
    (3, 1,  400.0, 4, True,  None, 999),    # a return whose buyer is in neither store
    (4, 3,  900.0, 5, False, None, 7),      # source 3 retired — reaches nothing
    (5, 4,  750.0, 6, False, None, 8),      # NO LINE ITEMS, and revenue
    (6, 1,  300.0, 7, False, B2B_MANAGER_ID, 7),   # b2b, and a named manager
    (7, 2,  500.0, 8, True,  B2B_MANAGER_ID, 7),   # a b2b return
]
# (line_id, order_id, product_id, quantity, price_sold)
LINES = [
    (1, 1, 100, 2, 500.0),
    (2, 1, 200, 2, 100.0),
    (3, 2, 100, 1, 500.0),
    (4, 3, 200, 1, 100.0),
    (5, 4, 300, 4, 250.0),
    (6, 6, 200, 3, 100.0),
    (7, 7, 100, 1, 500.0),
]

PG_TABLES = ("gold.daily_revenue", "silver.orders", "bronze.order_products",
             "bronze.products", "bronze.categories", "bronze.buyers",
             "bronze.managers")


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO categories (id, name, parent_id) VALUES (?,?,?)", CATEGORIES)
        conn.executemany(
            "INSERT INTO products (id, name, category_id, brand, sku, price)"
            " VALUES (?,?,?,?,?,?)", PRODUCTS)
        conn.executemany(
            "INSERT INTO buyers (id, full_name, phone) VALUES (?,?,?)", BUYERS)
        conn.executemany(
            "INSERT INTO managers (id, name) VALUES (?,?)", MANAGERS)
        for oid, src, total, days, is_ret, manager, buyer in ORDERS:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id) VALUES (?,?,?,?,?,?,?)",
                [oid, src, 19 if is_ret else 1, total,
                 now - timedelta(days=days), buyer, manager])
        conn.executemany(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity,"
            " price_sold) SELECT ?, ?, ?, p.name, ?, ? FROM products p WHERE p.id = ?",
            [(lid, oid, pid, qty, price, pid) for lid, oid, pid, qty, price in LINES])
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store, pool):
    for table in PG_TABLES:
        await conn.execute(f"DELETE FROM {table}")

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
        "INSERT INTO bronze.buyers (id,full_name,phone) VALUES ($1,$2,$3)", BUYERS)
    await conn.executemany(
        "INSERT INTO bronze.managers (id,name) VALUES ($1,$2)", MANAGERS)
    await conn.executemany(
        "INSERT INTO bronze.order_products (id,order_id,product_id,name,quantity,"
        "price_sold) VALUES ($1,$2,$3,$4,$5,$6)", [tuple(r) for r in lines])
    await conn.executemany(
        "INSERT INTO silver.orders (id,source_id,status_id,grand_total,ordered_at,"
        "buyer_id,manager_id,order_date,is_return,sales_type,is_active_source,"
        "source_name,is_new_customer,buyer_first_order_date,promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])

    # Derived, not copied — the production path.
    from core.pg_gold import rebuild_gold

    with patch("core.pg.require_revision", new=AsyncMock()):
        await rebuild_gold(pool=pool)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "dashboard.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_duckdb(store)
        async with pool.acquire() as conn:
            await _seed_postgres(conn, store, pool)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        async with pool.acquire() as conn:
            for table in PG_TABLES:
                await conn.execute(f"DELETE FROM {table}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    These reads fall back to DuckDB when Postgres raises, so a comparison that
    merely calls the method twice can pass while Postgres never answers: the
    second call falls back and the two "engines" agree because they were the
    same engine. `get_margin_trend` shipped that way and the gate was green.
    """
    monkeypatch.delenv("KS_READ_DASHBOARD", raising=False)
    monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
    duck = await getattr(store, name)(*WINDOW, **kwargs)

    monkeypatch.setenv("KS_READ_DASHBOARD", "postgres")
    # The category walk is filter chrome and rides the filter bar's flag; a
    # `category_id` case would otherwise resolve its tree from DuckDB and trip
    # the fatal fallback below for a reason that is not the one being tested.
    monkeypatch.setenv("KS_READ_LOOKUPS", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — Postgres did not answer, so the "
            f"comparison would have compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await getattr(store, name)(*WINDOW, **kwargs)
    monkeypatch.delenv("KS_READ_DASHBOARD", raising=False)
    monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
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
    # The doughnut, Gold branch — no filters at all.
    ("get_sales_by_source", {}),
    ("get_sales_by_source", {"sales_type": "all"}),
    ("get_sales_by_source", {"sales_type": "b2b"}),
    # …and the line-level branch, one filter each and then together.
    ("get_sales_by_source", {"category_id": 1}),        # a parent, two deep
    ("get_sales_by_source", {"category_id": 99999}),    # absent — was a 500
    ("get_sales_by_source", {"brand": "BrandA"}),
    ("get_sales_by_source", {"brand": "Unknown"}),      # the NULL-brand bucket
    ("get_sales_by_source", {"category_id": 1, "brand": "BrandA"}),
    # The returns list.
    ("get_return_orders", {}),
    ("get_return_orders", {"sales_type": "all"}),
    ("get_return_orders", {"sales_type": "b2b"}),
    ("get_return_orders", {"limit": 1}),                # the tie under a LIMIT
    # The subcategory breakdown.
    ("get_subcategory_breakdown", {"parent_category_name": "Care"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Makeup"}),   # a leaf root
    ("get_subcategory_breakdown", {"parent_category_name": "Nope"}),     # nothing
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "brand": "BrandA"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "source_id": 1}),
)


class TestTheTwoEnginesAgree:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
    )
    async def test_the_whole_result_is_identical(
        self, both_engines, monkeypatch, name, kwargs,
    ):
        duck, postgres = await _both(both_engines, monkeypatch, name, kwargs)
        assert _comparable(duck) == _comparable(postgres)


class TestTheTwoBranchesOfTheDoughnut:
    """The switch between Gold and the line level is itself behaviour, and the
    order with no line items is what makes it visible."""

    @pytest.mark.asyncio
    async def test_gold_counts_the_order_with_no_lines_and_the_lines_do_not(
        self, both_engines, monkeypatch,
    ):
        unfiltered, _ = await _both(both_engines, monkeypatch, "get_sales_by_source", {})
        filtered, _ = await _both(
            both_engines, monkeypatch, "get_sales_by_source", {"brand": "BrandA"})

        shopify_gold = dict(zip(unfiltered["labels"], unfiltered["revenue"]))
        shopify_lines = dict(zip(filtered["labels"], filtered["revenue"]))
        assert shopify_gold.get("Shopify") == 750.0, (
            "order 5 carries revenue and no lines; the Gold branch must see it"
        )
        assert "Shopify" not in shopify_lines, (
            "the line-level branch cannot see an order with no lines, and that "
            "is the difference both engines have to reproduce"
        )

    @pytest.mark.asyncio
    async def test_both_engines_reproduce_that_difference(
        self, both_engines, monkeypatch,
    ):
        for kwargs in ({}, {"brand": "BrandA"}):
            duck, postgres = await _both(
                both_engines, monkeypatch, "get_sales_by_source", kwargs)
            assert _comparable(duck) == _comparable(postgres), kwargs


class TestTheReturnsListJoinsThatCanMiss:
    @pytest.mark.asyncio
    async def test_a_missing_buyer_and_a_null_manager_survive_both_joins(
        self, both_engines, monkeypatch,
    ):
        """Both engines LEFT JOIN here, and a port that turned one into an
        inner join would silently drop the order — with no total moving to say
        so. Production has no orphan buyers today, which is exactly why this
        has to be asserted on the code rather than noticed in the data."""
        duck, postgres = await _both(
            both_engines, monkeypatch, "get_return_orders", {"sales_type": "all"})
        assert duck == postgres
        ids = {r["id"] for r in postgres}
        assert 3 in ids, "the return whose buyer is missing was dropped"
        by_id = {r["id"]: r for r in postgres}
        assert by_id[3]["buyerName"] is None
        assert by_id[3]["managerName"] is None
        assert by_id[7]["managerName"] == "Wholesale"
