"""The `/inventory` tab means the same thing in DuckDB and Postgres.

`tests/unit/test_inventory_views_dialect.py` proves the eleven views are *one
text*. Necessary and not sufficient: identical SQL still means different things
where the engines' semantics differ — integer against decimal division, what
`ROUND` returns, how `PERCENTILE_CONT` interpolates, where NULLs sort, which
rows a `LIMIT` keeps when the sort has ties.

So this runs the real thing. The same synthetic catalogue goes into a DuckDB
store and a live PostgreSQL, and all eight methods are called twice — once with
`KS_READ_INVENTORY` unset and once with it set to `postgres` — and the **whole
result** of each is compared, not a summary of it. Skipped without
`KS_PG_DSN`, the contract every Postgres-touching test here has;
`deploy/gate_with_stores.sh` supplies one.

THE FIXTURE IS DELIBERATELY AWKWARD

A tab that agrees on easy data proves nothing. It carries a SKU that never
sold, one that sold inside 90 days but not inside 30, one with no cost price
so the portfolio-wide fallback ratio has to be computed, a SKU out of stock, a
SKU whose stock is entirely reserved, an uncategorised SKU, six SKUs in one
category so `v_category_velocity` clears its `HAVING COUNT(*) >= 5`, a return
and an inactive source that must not reach any velocity figure, and a tie in
every column the three `ORDER BY … LIMIT` reads sort on.

AND THE GOLD FIXTURE CARRIES BOTH GRAINS ON PURPOSE

`gold.daily_revenue` in Postgres is grained by source as well, with
`source_id IS NULL` as the roll-up; DuckDB's is not. Seeding only roll-up rows
would let a missing `source_id IS NULL` filter pass this test while doubling
the revenue in production, so the per-source rows are here to make that
failure visible.
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


def _d(days_ago: int) -> date:
    return TODAY - timedelta(days=days_ago)


# `sku_inventory_status.updated_at` is stamped once per rebuild and *carried*
# into Postgres by the replication — the two stores hold the same instant, and
# `get_stock_summary` reports it as `lastSync`. Seeded explicitly here because
# the column defaults to `CURRENT_TIMESTAMP` on both sides, which would have
# the fixture disagree by however long the two seeds took and make a real
# comparison look like a flake.
SNAPSHOT_AT = datetime(2026, 9, 6, 11, 50, 45, tzinfo=timezone.utc)


CATEGORIES = [(10, "Догляд", None), (11, "Очищення", 10), (12, "Порожня", None)]

# (offer_id, product_id, sku, name, brand, category_id, quantity, reserve,
#  price, purchased_price, last_sale_date, first_seen_at)
SKUS = [
    # six in category 10, so v_category_velocity clears HAVING COUNT(*) >= 5
    (1, 101, "S-1", "Тонер", "Cosrx", 10, 40, 0, 500, 250, _d(3), _d(400)),
    (2, 102, "S-2", "Сироватка", "Cosrx", 10, 20, 5, 800, 400, _d(20), _d(300)),
    (3, 103, "S-3", "Крем", "Beauty", 10, 15, 0, 1200, None, _d(60), _d(250)),
    (4, 104, "S-4", "Патчі", "Beauty", 10, 10, 0, 300, 150, _d(120), _d(500)),
    (5, 105, "S-5", "Маска", "Cosrx", 10, 8, 0, 400, 200, _d(200), _d(600)),
    (6, 106, "S-6", "Пінка", "Anua", 10, 5, 0, 600, 300, _d(400), _d(900)),
    # the awkward ones
    (7, 107, "S-7", "Ніколи", "Anua", 11, 12, 0, 900, 450, None, _d(220)),
    (8, 108, "S-8", "Все в резерві", "Anua", 11, 7, 7, 700, 350, _d(10), _d(150)),
    (9, 109, "S-9", "Немає", "Beauty", 11, 0, 0, 1000, 500, _d(5), _d(100)),
    (10, 110, "S-10", "Без категорії", "", None, 9, 0, 550, None, _d(45), _d(80)),
    # a tie in every column the ORDER BY … LIMIT reads sort on
    (11, 111, "S-11", "Близнюк А", "Twin", 11, 3, 0, 500, 250, _d(400), _d(700)),
    (12, 112, "S-12", "Близнюк Б", "Twin", 11, 3, 0, 500, 250, _d(400), _d(700)),
    # Days of supply lands on exactly x.5 (1 unit against 4 sold in 90 days is
    # 22.5), so ROUND's half-way rule is compared rather than assumed. And the
    # tier boundaries are exact integers by construction elsewhere, which is
    # what caught Postgres dividing twice and landing above 180 where DuckDB
    # landed on it.
    (13, 113, "S-13", "Половинка", "Half", 11, 1, 0, 640, 320, _d(4), _d(90)),
]

# (order_id, order_date, sales_type, is_return, is_active_source, source_id)
ORDERS = [
    (1, _d(2), "retail", False, True, 1),
    (2, _d(25), "retail", False, True, 1),
    (3, _d(70), "retail", False, True, 2),
    (4, _d(80), "b2b", False, True, 1),
    (5, _d(5), "retail", True, True, 1),     # a return: invisible to velocity
    (6, _d(6), "retail", False, False, 3),   # retired source: also invisible
    (7, _d(200), "retail", False, True, 1),  # outside both windows
]

# (line_id, order_id, product_id, quantity, price_sold)
LINES = [
    (1, 1, 101, 6, 500), (2, 1, 102, 2, 800),
    (3, 2, 101, 4, 490), (4, 2, 103, 1, 1200),
    (5, 3, 104, 3, 300), (6, 3, 105, 1, 400),
    (7, 4, 101, 10, 450),
    (8, 5, 101, 5, 500),      # the return
    (9, 6, 102, 3, 800),      # the retired source
    (10, 7, 106, 2, 600),     # too old for either window
    (11, 1, 110, 1, 550),
    (12, 1, 113, 4, 640),     # 1 unit left against 4 sold: 22.5 days of supply
    (13, 2, 101, 0, 500),     # a zero-quantity line: it must not divide by it
]

# (date, sales_type, source_id, revenue) — source_id None is the roll-up
GOLD = [
    (_d(2), "retail", None, 4400), (_d(2), "retail", 1, 4400),
    (_d(10), "retail", None, 1500), (_d(10), "retail", 1, 900),
    (_d(10), "retail", 2, 600),
    (_d(25), "retail", None, 3160), (_d(25), "retail", 1, 3160),
    (_d(25), "b2b", None, 4500), (_d(25), "b2b", 1, 4500),
    (_d(70), "retail", None, 1300), (_d(70), "retail", 2, 1300),
]

# (date, total_quantity, total_value, total_reserve, sku_count)
# Two calendar months so the monthly granularity has more than one bucket, and
# a gap, because the snapshot job can miss a day and the chart must not invent
# one. Values chosen so AVG lands on a repeating decimal in both engines.
HISTORY = [
    (_d(n), 1000 + n, 100000 + n * 3, 100 + n, 890 + (n % 3))
    for n in (1, 2, 3, 5, 8, 13, 21, 34, 55, 70)
]

CALLS = (
    ("get_stock_summary", {"limit": 5}),
    ("get_inventory_summary_v2", {}),
    ("get_dead_stock_items_v2", {"limit": 5}),
    ("get_dead_stock_deep", {"limit": 5}),
    ("get_all_skus_deep", {}),
    ("get_brand_rotation", {"min_skus": 1}),
    ("get_recommended_actions", {"limit": 5}),
    ("get_restock_alerts", {"limit": 5}),
    ("get_inventory_turnover", {"days": 30}),
    ("get_abc_skus", {"abc_class": "A", "limit": 5}),
    ("get_abc_skus", {"abc_class": "C", "limit": 5}),
    ("get_average_inventory", {"days": 30}),
    ("get_average_inventory", {"days": 90}),
    ("get_inventory_trend", {"days": 90, "granularity": "daily"}),
    ("get_inventory_trend", {"days": 90, "granularity": "monthly"}),
)


async def _seed_duckdb(store):
    async with store.connection() as conn:
        for cid, name, parent in CATEGORIES:
            conn.execute("INSERT INTO categories (id, name, parent_id)"
                         " VALUES (?,?,?)", [cid, name, parent])
        for pid_row in SKUS:
            (oid, pid, sku, name, brand, cat, qty, res,
             price, cost, last_sale, first_seen) = pid_row
            conn.execute(
                "INSERT INTO products (id, name, brand, sku, category_id)"
                " VALUES (?,?,?,?,?)", [pid, name, brand, sku, cat])
            conn.execute(
                "INSERT INTO offer_stocks (id, sku, price, purchased_price,"
                " quantity, reserve) VALUES (?,?,?,?,?,?)",
                [oid, sku, price, cost, qty, res])
            conn.execute(
                "INSERT INTO sku_inventory_status (offer_id, product_id, sku,"
                " name, brand, category_id, quantity, reserve, price,"
                " purchased_price, last_sale_date, first_seen_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [oid, pid, sku, name, brand, cat, qty, res, price, cost,
                 last_sale, first_seen, SNAPSHOT_AT])
        for oid, day, stype, ret, active, src in ORDERS:
            conn.execute(
                "INSERT INTO silver_orders (id, source_id, status_id,"
                " grand_total, ordered_at, buyer_id, manager_id, order_date,"
                " is_return, sales_type, is_active_source, source_name,"
                " is_new_customer, buyer_first_order_date, promocode)"
                " VALUES (?,?,1,0,?,1,NULL,?,?,?,?,?,FALSE,?,NULL)",
                [oid, src, f"{day} 10:00:00+00", day, ret, stype, active,
                 f"src{src}", day])
        for lid, oid, pid, qty, price in LINES:
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name,"
                " quantity, price_sold) VALUES (?,?,?,?,?,?)",
                [lid, oid, pid, f"line{lid}", qty, price])
        for day, stype, src, revenue in GOLD:
            if src is None:      # DuckDB's Gold has no source grain at all
                conn.execute(
                    "INSERT INTO gold_daily_revenue (date, sales_type, revenue)"
                    " VALUES (?,?,?)", [day, stype, revenue])
        for day, qty, value, reserve, skus in HISTORY:
            conn.execute(
                "INSERT INTO inventory_history (date, total_quantity,"
                " total_value, total_reserve, sku_count) VALUES (?,?,?,?,?)",
                [day, qty, value, reserve, skus])


async def _seed_postgres(conn):
    await conn.execute(
        "TRUNCATE gold.daily_revenue, silver.orders, bronze.order_products,"
        " bronze.products, bronze.categories, bronze.offer_stocks,"
        " app.sku_inventory_status, app.inventory_history")
    await conn.executemany(
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES ($1,$2,$3)",
        CATEGORIES)
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, brand, sku, category_id)"
        " VALUES ($1,$2,$3,$4,$5)",
        [(p, n, b, s, c) for _o, p, s, n, b, c, *_r in SKUS])
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price,"
        " quantity, reserve) VALUES ($1,$2,$3,$4,$5,$6)",
        [(o, s, pr, co, q, r)
         for o, _p, s, _n, _b, _c, q, r, pr, co, _ls, _fs in SKUS])
    await conn.executemany(
        "INSERT INTO app.sku_inventory_status (offer_id, product_id, sku, name,"
        " brand, category_id, quantity, reserve, price, purchased_price,"
        " last_sale_date, first_seen_at, updated_at)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        [(o, p, s, n, b, c, q, r, pr, co, ls, fs, SNAPSHOT_AT)
         for o, p, s, n, b, c, q, r, pr, co, ls, fs in SKUS])
    await conn.executemany(
        "INSERT INTO silver.orders (id, source_id, status_id, grand_total,"
        " ordered_at, buyer_id, manager_id, order_date, is_return, sales_type,"
        " is_active_source, source_name, is_new_customer,"
        " buyer_first_order_date, promocode)"
        " VALUES ($1,$2,1,0,$3,1,NULL,$4,$5,$6,$7,$8,FALSE,$4,NULL)",
        [(o, src, datetime(day.year, day.month, day.day, 10,
                           tzinfo=timezone.utc), day, ret, st, active,
          f"src{src}")
         for o, day, st, ret, active, src in ORDERS])
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name,"
        " quantity, price_sold) VALUES ($1,$2,$3,$4,$5,$6)",
        [(lid, o, p, f"line{lid}", q, pr) for lid, o, p, q, pr in LINES])
    await conn.executemany(
        "INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue,"
        " orders_count, unique_customers, new_customers, returning_customers,"
        " returns_count, returns_revenue, avg_order_value)"
        " VALUES ($1,$2,$3,$4,0,0,0,0,0,0,0)", GOLD)
    await conn.executemany(
        "INSERT INTO app.inventory_history (date, total_quantity, total_value,"
        " total_reserve, sku_count) VALUES ($1,$2,$3,$4,$5)", HISTORY)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    """A DuckDB store and a Postgres holding the identical catalogue.

    The pool is built here and `core.pg.get_pool` patched to hand it over,
    rather than letting the module-level pool open itself: that one is a
    global bound to whichever event loop first asked for it, and closing it
    in a teardown after pytest-asyncio has retired that loop raises.
    `test_sms_segments_two_engines` takes the same route.
    """
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "inventory.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            await _seed_postgres(conn)
        await _seed_duckdb(store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    monkeypatch.delenv("KS_READ_INVENTORY", raising=False)
    duckdb_result = await getattr(store, name)(**kwargs)
    monkeypatch.setenv("KS_READ_INVENTORY", "postgres")
    postgres_result = await getattr(store, name)(**kwargs)
    monkeypatch.delenv("KS_READ_INVENTORY", raising=False)
    return duckdb_result, postgres_result


def _comparable(value):
    """Money and ratios to six places; everything else as it stands.

    DuckDB divides a DECIMAL into a DOUBLE and Postgres into a NUMERIC, so the
    engines legitimately disagree in the last bits of a float. Six places is
    far tighter than any figure this tab renders and far looser than the noise.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _comparable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_comparable(v) for v in value]
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,kwargs", CALLS,
    ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
)
async def test_both_engines_return_the_same_answer(
    both_engines, monkeypatch, name, kwargs,
):
    duckdb_result, postgres_result = await _both(
        both_engines, monkeypatch, name, kwargs,
    )
    assert _comparable(postgres_result) == _comparable(duckdb_result)


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(both_engines, monkeypatch):
    """A comparison of two empty answers agrees about nothing. Every method
    has to return something, and the awkward rows have to be reachable."""
    summary, _ = await _both(both_engines, monkeypatch, "get_inventory_summary_v2", {})
    statuses = {k: v["skuCount"] for k, v in summary["summary"].items()
                if k != "total"}
    assert summary["summary"]["total"]["skuCount"] > 0
    assert statuses["neverSold"] >= 1, "the never-sold SKU is not reaching the view"
    assert statuses["deadStock"] >= 1, "no dead stock: the thresholds are not biting"
    assert summary["categoryThresholds"], "v_category_velocity produced no rows"

    deep, _ = await _both(both_engines, monkeypatch, "get_dead_stock_deep", {"limit": 5})
    assert deep["items"], "no dead-stock items"
    qualities = {i["costQuality"] for i in (await _both(
        both_engines, monkeypatch, "get_all_skus_deep", {}))[0]}
    assert qualities == {"actual", "fallback"}, (
        f"both cost paths must be exercised, saw {qualities}")

    turnover, _ = await _both(
        both_engines, monkeypatch, "get_inventory_turnover", {"days": 30})
    assert turnover["turnover"]["totalRevenue"] > 0
    assert turnover["currentStock"]["valueSale"] > 0

    trend, _ = await _both(
        both_engines, monkeypatch, "get_inventory_trend",
        {"days": 90, "granularity": "monthly"})
    assert len(trend["labels"]) >= 2, (
        "the monthly buckets must span more than one month or the grouping "
        "is not being compared at all")
    daily, _ = await _both(
        both_engines, monkeypatch, "get_inventory_trend",
        {"days": 90, "granularity": "daily"})
    assert len(daily["labels"]) == len(HISTORY)

    average, _ = await _both(
        both_engines, monkeypatch, "get_average_inventory", {"days": 90})
    assert average["dataPoints"] == len(HISTORY)
    assert "message" not in average, "the no-history branch is not what ran"


@pytest.mark.asyncio
async def test_the_summary_exercises_what_it_was_ported_for(both_engines, monkeypatch):
    """The three things `get_stock_summary` gained on the way to Postgres.

    Comparing two engines proves they agree; it does not prove they agree
    about anything interesting. These are the parts of the answer that were
    new or changed, asserted on the DuckDB side and compared on both by the
    parametrised test above.
    """
    summary, _ = await _both(
        both_engines, monkeypatch, "get_stock_summary", {"limit": 5})

    # 1. The out-of-stock SKU reaches the count. It is the row that
    #    `gold.v_sku_status` would have silently dropped — that view is
    #    `WHERE quantity > 0`, which is why the port reads the table. The
    #    count is the only out-of-stock figure the page renders; the list that
    #    used to ride beside it had no reader and is gone.
    assert summary["summary"]["outOfStockCount"] >= 1
    assert "outOfStock" not in summary

    # 2. `name` comes from the row itself now, not from a join to `offers`
    #    that Postgres does not have.
    assert all(i["name"] for i in summary["topByQuantity"])

    # 3. `lastSync` is the snapshot the rows were built in — the same instant
    #    in both stores, because the replication carries the column rather
    #    than restamping it. It used to read a `sync_metadata` key nothing has
    #    ever written, so it was NULL for the life of the feature.
    assert summary["lastSync"] == SNAPSHOT_AT.isoformat()

    # 4. The tie the fixture plants: S-11 and S-12 are both 3 units at 500, so
    #    the low-stock list's ORDER BY has to break it on `offer_id` or the two
    #    engines are free to disagree about which one a LIMIT keeps.
    tied = [i["sku"] for i in summary["lowStock"] if i["sku"] in ("S-11", "S-12")]
    assert tied == ["S-11", "S-12"], tied


@pytest.mark.asyncio
async def test_postgres_answers_in_kyivs_day_whatever_its_session_says(
    both_engines, monkeypatch,
):
    """The bug this test exists for could not be caught by comparing engines.

    Under the gate both run in the same timezone, so both were right together.
    In production the `web` container runs `TZ=Europe/Kyiv` and the Postgres
    server answers in UTC, and between 21:00 and midnight UTC the two name
    different days — moving every `days_since_sale`, every aging bucket and
    every 30/90-day window by one, for three hours a night, on one engine.

    So this asks Postgres the question from a session deliberately set to the
    wrong day and requires the Kyiv answer anyway. `Pacific/Kiritimati` is
    UTC+14 and `Pacific/Midway` is UTC-11: between them, one of the two is
    always on a different calendar day from Kyiv.
    """
    from core import pg

    pool = await pg.get_pool()
    async with pool.acquire() as conn:
        kyiv = await conn.fetchval(
            "SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
        answers = {}
        for zone in ("Pacific/Kiritimati", "Pacific/Midway", "UTC"):
            await conn.execute(f"SET TIME ZONE '{zone}'")
            answers[zone] = (
                await conn.fetchval("SELECT CURRENT_DATE"),
                await conn.fetchval(
                    "SELECT days_since_sale FROM gold.v_sku_analysis"
                    " WHERE offer_id = 1"),
            )
        await conn.execute("SET TIME ZONE 'UTC'")

    # The premise: at least one session really is on another day, or this
    # test proves nothing about timezones at all.
    assert any(day != kyiv for day, _ in answers.values()), (
        f"no session disagreed with Kyiv ({kyiv}) — the test cannot bite: "
        f"{answers}"
    )
    # The claim: the view's answer does not move with the session.
    computed = {value for _day, value in answers.values()}
    assert len(computed) == 1, (
        f"days_since_sale followed the session timezone: {answers}"
    )


@pytest.mark.asyncio
async def test_the_gold_rollup_filter_is_load_bearing(both_engines, monkeypatch):
    """The fixture's Gold carries per-source rows as well as roll-ups, so a
    Postgres reader that forgot `source_id IS NULL` would double the revenue.
    Measured on production over 30 days before this hole existed: 11,107,040.50
    against a true 5,553,520.25."""
    duckdb_result, postgres_result = await _both(
        both_engines, monkeypatch, "get_inventory_turnover", {"days": 30})
    assert (postgres_result["turnover"]["totalRevenue"]
            == duckdb_result["turnover"]["totalRevenue"])
    # And the fine rows really are there to be double-counted.
    from core import pg

    pool = await pg.get_pool()
    async with pool.acquire() as conn:
        both = await conn.fetchval(
            "SELECT SUM(revenue) FROM gold.daily_revenue"
            " WHERE date >= CURRENT_DATE - INTERVAL '30 days'")
        rollup = await conn.fetchval(
            "SELECT SUM(revenue) FROM gold.daily_revenue"
            " WHERE date >= CURRENT_DATE - INTERVAL '30 days'"
            "   AND source_id IS NULL")
    assert both > rollup, "the fixture cannot catch the double count"
