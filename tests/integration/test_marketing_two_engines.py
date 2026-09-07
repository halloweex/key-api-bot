"""`/marketing` means the same thing in DuckDB and Postgres.

`tests/unit/test_pg_marketing_read.py` proves the wiring and the shape of the
two renderings. This proves they produce the same numbers, on real engines,
which matters more here than on any tab ported so far — because two of the
three parts are not a table rename:

  * the channel figures come from **columns** in one engine and from a
    `source_id` **dimension** in the other, with the five scalars read from the
    roll-up rows only, since three of them are `COUNT(DISTINCT buyer_id)` and
    distinct counts do not add up;
  * the brand table used to read `gold_daily_products`, which exists only in
    DuckDB, and now reads the order-line level — reproducing that table's grain
    so `SUM(order_count)` keeps meaning what the report has always shown.

The Postgres Gold is built by `core.pg_gold.rebuild_gold`, the production path,
rather than copied from DuckDB's. Copying would compare the report against
itself; deriving is what makes the two shapes independent enough for the
comparison to say anything.

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
WINDOW = (TODAY - timedelta(days=25), TODAY)

CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Lipstick", 3, "BrandB", "SKU-B", 100.0),
    (300, "Unbranded", 3, None, "SKU-C", 250.0),      # the Unknown bucket
]

# (order_id, source_id, grand_total, days_ago, is_return, manager_id, buyer_id)
#
# Two channels for one buyer on one day (orders 1 and 2, buyer 7): that is the
# case where folding the per-source rows would overstate `unique_customers`,
# which is the whole reason the Postgres rendering reads the roll-up row.
ORDERS = [
    (1, 1, 1200.0, 3, False, None, 7),
    (2, 2, 600.0, 3, False, None, 7),
    (3, 4, 900.0, 4, False, None, 8),
    (4, 1, 400.0, 5, True, None, 9),       # a return
    (5, 3, 700.0, 6, False, None, 10),     # retired source
    (6, 1, 300.0, 7, False, B2B_MANAGER_ID, 11),   # b2b
    (7, 4, 850.0, 8, False, None, 12),     # no line items, and revenue
]
LINES = [
    (1, 1, 100, 2, 500.0, "Serum"),
    (2, 1, 200, 3, 100.0, "Lipstick"),
    # the same product twice on one order under a different name-as-sold —
    # the sharpest edge of the grain the brand query has to reproduce
    (3, 1, 100, 1, 500.0, "Serum (promo)"),
    (4, 2, 100, 1, 500.0, "Serum"),
    (5, 3, 300, 4, 250.0, "Unbranded"),
    (6, 4, 200, 1, 100.0, "Lipstick"),
    (7, 5, 300, 2, 250.0, "Unbranded"),
    (8, 6, 300, 3, 100.0, "Unbranded"),
]
GOALS = [("monthly", 5000000.00, True, 4800000.00, 1.10)]


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        conn.executemany("INSERT INTO categories (id,name,parent_id) VALUES (?,?,?)",
                         CATEGORIES)
        conn.executemany(
            "INSERT INTO products (id,name,category_id,brand,sku,price)"
            " VALUES (?,?,?,?,?,?)", PRODUCTS)
        for oid, src, total, days, is_ret, manager, buyer in ORDERS:
            conn.execute(
                "INSERT INTO orders (id,source_id,status_id,grand_total,ordered_at,"
                "buyer_id,manager_id) VALUES (?,?,?,?,?,?,?)",
                [oid, src, 19 if is_ret else 1, total,
                 now - timedelta(days=days), buyer, manager])
        conn.executemany(
            "INSERT INTO order_products (id,order_id,product_id,name,quantity,"
            "price_sold) VALUES (?,?,?,?,?,?)",
            [(lid, oid, pid, name, qty, price)
             for lid, oid, pid, qty, price, name in LINES])
        conn.executemany(
            "INSERT INTO revenue_goals (period_type,goal_amount,is_custom,"
            "calculated_goal,growth_factor) VALUES (?,?,?,?,?)", GOALS)
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store, pool):
    for table in ("gold.daily_revenue", "silver.orders", "bronze.order_products",
                  "bronze.products", "bronze.categories", "app.revenue_goals"):
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
        "INSERT INTO bronze.order_products (id,order_id,product_id,name,quantity,"
        "price_sold) VALUES ($1,$2,$3,$4,$5,$6)", [tuple(r) for r in lines])
    await conn.executemany(
        "INSERT INTO silver.orders (id,source_id,status_id,grand_total,ordered_at,"
        "buyer_id,manager_id,order_date,is_return,sales_type,is_active_source,"
        "source_name,is_new_customer,buyer_first_order_date,promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])
    await conn.executemany(
        "INSERT INTO app.revenue_goals (period_type,goal_amount,is_custom,"
        "calculated_goal,growth_factor,updated_at) VALUES ($1,$2,$3,$4,$5,now())",
        GOALS)

    # Derived, not copied: the production path, and the only way the two Gold
    # shapes stay independent enough for the comparison to mean anything.
    from core.pg_gold import rebuild_gold

    with patch("core.pg.require_revision", new=AsyncMock()):
        await rebuild_gold(pool=pool)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "marketing.duckdb")
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
            for table in ("gold.daily_revenue", "silver.orders",
                          "bronze.order_products", "bronze.products",
                          "bronze.categories", "app.revenue_goals"):
                await conn.execute(f"DELETE FROM {table}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    monkeypatch.delenv("KS_READ_MARKETING", raising=False)
    duck = await getattr(store, name)(*WINDOW, **kwargs)
    monkeypatch.setenv("KS_READ_MARKETING", "postgres")
    postgres = await getattr(store, name)(*WINDOW, **kwargs)
    monkeypatch.delenv("KS_READ_MARKETING", raising=False)
    return duck, postgres


def _comparable(value):
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
    ("get_marketing_report_by_dates", {}),
    ("get_marketing_report_by_dates", {"sales_type": "all"}),
    ("get_marketing_report_by_dates", {"sales_type": "b2b"}),
    ("get_promocode_analytics", {}),
    ("get_promocode_analytics", {"sales_type": "all"}),
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
async def test_the_channel_split_agrees_across_two_shapes(both_engines, monkeypatch):
    """Columns on one side, a `source_id` dimension on the other."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_marketing_report_by_dates", {})
    by_name = {s["source_name"]: s for s in duck["sources"]}
    assert by_name["Instagram"]["revenue"] == 1200.0
    assert by_name["Telegram"]["revenue"] == 600.0
    assert by_name["Сайт"]["revenue"] == 900.0 + 850.0
    assert duck["sources"] == postgres["sources"]


@pytest.mark.asyncio
async def test_a_buyer_on_two_channels_is_counted_once(both_engines, monkeypatch):
    """Buyer 7 bought on Instagram and Telegram the same day. Folding the
    per-source rows would count them twice — the reason the Postgres rendering
    reads `unique_customers` from the roll-up row only. This is the assertion
    that fails if that filter is ever dropped."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_marketing_report_by_dates", {})
    retail_buyers = {o[6] for o in ORDERS
                     if not o[4] and o[5] is None and o[1] != 3}
    assert duck["general_sales"]["current"]["customers"] == len(retail_buyers)
    assert postgres["general_sales"]["current"]["customers"] == len(retail_buyers)
    # …and the naive fold would have been larger.
    assert len(retail_buyers) < len([o for o in ORDERS
                                     if not o[4] and o[5] is None and o[1] != 3])


@pytest.mark.asyncio
async def test_the_brand_table_reproduces_the_gold_grain(both_engines, monkeypatch):
    """One product twice on one order under two names-as-sold is two rows in
    `gold_daily_products`, and the report sums their `order_count`. The
    rewritten query must reproduce that, not improve on it."""
    duck, postgres = await _both(
        both_engines, monkeypatch, "get_marketing_report_by_dates", {})
    brands = {b["brand"]: b for b in duck["brands"]}
    assert set(brands) == {"BrandA", "BrandB", "Unknown"}, brands
    # Order 1 carries Serum twice under two names, so BrandA's summed
    # order_count is 2 for that day plus 1 for order 2 — not the 2 a plain
    # COUNT(DISTINCT order_id) would give.
    assert brands["BrandA"]["orders"] == 3, brands["BrandA"]
    assert duck["brands"] == postgres["brands"]


@pytest.mark.asyncio
async def test_the_goal_comes_from_the_replica(both_engines, monkeypatch):
    """A full calendar month is the only shape that shows it."""
    store = both_engines
    first = TODAY.replace(day=1)
    from calendar import monthrange
    last = TODAY.replace(day=monthrange(TODAY.year, TODAY.month)[1])

    monkeypatch.delenv("KS_READ_MARKETING", raising=False)
    duck = await store.get_marketing_report_by_dates(first, last)
    monkeypatch.setenv("KS_READ_MARKETING", "postgres")
    postgres = await store.get_marketing_report_by_dates(first, last)
    monkeypatch.delenv("KS_READ_MARKETING", raising=False)

    assert duck["general_sales"]["monthly_goal"] == float(GOALS[0][1])
    assert postgres["general_sales"]["monthly_goal"] == duck["general_sales"]["monthly_goal"]


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(both_engines, monkeypatch):
    report, _ = await _both(
        both_engines, monkeypatch, "get_marketing_report_by_dates", {})
    assert report["general_sales"]["current"]["revenue"] > 0
    assert report["brands"], "no brands"
    assert len(report["sources"]) == 3
    assert report["general_sales"]["previous"]["revenue"] == 0, (
        "the previous period must be empty here, or the window is wrong")
