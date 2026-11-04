"""The `/inventory` tab answers in Postgres what DuckDB answered.

`tests/unit/test_inventory_views_dialect.py` proves the eleven views are *one
text*. Necessary and not sufficient: identical SQL still means different things
where the engines' semantics differ — integer against decimal division, what
`ROUND` returns, how `PERCENTILE_CONT` interpolates, where NULLs sort, which
rows a `LIMIT` keeps when the sort has ties.

ONE ENGINE SINCE STEP 14 (S14-07)

`/inventory` reads Postgres and nothing else. The synthetic catalogue goes into
a live PostgreSQL and every method's **whole result** is compared with a
literal (`tests/integration/golden/inventory.json`): DuckDB's answer on this
fixture, recorded while the same run asserted Postgres answered each one
equally (rule 9 of the step-14 plan; all fifteen agreed). The views count from
the server's own Kyiv day, which a test cannot pin, so every date in an answer
is written as days before that day (`_relative`), the unit the fixture is
built in. The monthly trend buckets by the calendar, so its answer moves with
the day the suite runs: it is computed here instead (`_monthly_truth`), from
the history the fixture writes, by the method's own rule. The store's DuckDB
connection is a trap. Skipped without `KS_PG_DSN`, the contract every
Postgres-touching test here has; `deploy/gate_with_stores.sh` supplies one.

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
`source_id IS NULL` as the roll-up. Seeding only roll-up rows would let a
missing `source_id IS NULL` filter pass this test while doubling the revenue
in production, so the per-source rows are here to make that failure visible.
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from tests.integration.pg_tab_seed import call_key, golden

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# Kyiv's day, the one the views count from (`TODAY_IN_KYIV`): the views take
# their "today" from the server's clock, so it cannot be pinned, and the
# answers are recorded relative to it: a date as how many days before today.
# `date.today()` was the runner's day, which is not Kyiv's from 21:00 UTC.
TODAY = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Kyiv")).date()


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
async def in_postgres(tmp_path, monkeypatch):
    """A Postgres holding the catalogue, and a store that is never connected:
    its DuckDB connection is a trap, so an answer below can only be
    Postgres's.

    The pool is built here and `core.pg.get_pool` patched to hand it over,
    rather than letting the module-level pool open itself: that one is a
    global bound to whichever event loop first asked for it, and closing it
    in a teardown after pytest-asyncio has retired that loop raises.
    """
    from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb

    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        async with pool.acquire() as conn:
            await _seed_postgres(conn)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/inventory")
    finally:
        await pool.close()


def _comparable(value):
    """Money and ratios to six places, dates as ISO text — the form the
    literals were recorded in (`tests.integration.pg_tab_seed.comparable`)."""
    from tests.integration.pg_tab_seed import comparable

    return comparable(value)


def _relative(value):
    """Every date in an answer as days before `TODAY`: an ISO date within
    1 000 days, and a daily trend label (`%d %b`) within 300, become `T-n`.
    The literals were written so; everything else stands as it is."""
    dates = {}
    for n in range(1000):
        dates.setdefault((TODAY - timedelta(days=n)).isoformat(), f"T-{n}")
    labels = {}
    for n in range(300):
        labels.setdefault((TODAY - timedelta(days=n)).strftime("%d %b"), f"T-{n}")

    def walk(v):
        if isinstance(v, dict):
            return {k: walk(x) for k, x in v.items()}
        if isinstance(v, list):
            return [walk(x) for x in v]
        if isinstance(v, str):
            if v in dates:
                return dates[v]
            if re.fullmatch(r"\d{2} [A-Z][a-z]{2}", v) and v in labels:
                return labels[v]
        return v

    return walk(value)


def _monthly_truth(days: int) -> dict:
    """`get_inventory_trend(days, "monthly")` as the method's rule computes it
    from `HISTORY`: the snapshots on or after `TODAY - days`, by calendar
    month, each month's average quantity and reserve rounded to a unit and
    its average value to the kopeck, labelled `%b %Y`."""
    months: dict = {}
    for day, qty, value, reserve, _skus in HISTORY:
        if day >= TODAY - timedelta(days=days):
            months.setdefault(date(day.year, day.month, 1), []).append(
                (qty, value, reserve))
    rows = [(month, months[month]) for month in sorted(months)]

    def avg(points, i):
        return Decimal(sum(p[i] for p in points)) / Decimal(len(points))

    return {
        "labels": [month.strftime("%b %Y") for month, _ in rows],
        "quantity": [round(avg(points, 0)) for _, points in rows],
        "value": [round(float(avg(points, 1)), 2) for _, points in rows],
        "reserve": [round(avg(points, 2)) for _, points in rows],
        "granularity": "monthly",
        "periodDays": days,
        "dataPoints": len(rows),
    }


GOLDEN = golden("inventory")
LITERAL_CALLS = tuple(
    (n, k) for n, k in CALLS if not (n == "get_inventory_trend"
                                     and k.get("granularity") == "monthly"))


class TestPostgresAnswersWhatDuckDBAnswered:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", LITERAL_CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in LITERAL_CALLS],
    )
    async def test_the_whole_result_is_the_literal(self, in_postgres, name, kwargs):
        answer = await getattr(in_postgres, name)(**kwargs)
        assert _relative(_comparable(answer)) == GOLDEN[call_key(name, kwargs)], (
            f"{name}{kwargs}: {answer!r}")

    def test_every_literal_is_a_call_and_every_call_has_one(self):
        assert set(GOLDEN) == {call_key(n, k) for n, k in LITERAL_CALLS}

    @pytest.mark.asyncio
    async def test_the_monthly_trend_is_the_rule_on_the_fixture(self, in_postgres):
        """The one call whose answer moves with the calendar: the months the
        history falls in depend on the day the suite runs."""
        answer = await in_postgres.get_inventory_trend(days=90, granularity="monthly")
        assert _comparable(answer) == _comparable(_monthly_truth(90))
        assert answer["dataPoints"] >= 2, (
            "the monthly buckets must span more than one month or the grouping "
            "is not being compared at all")


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(in_postgres):
    """A literal of an empty answer pins nothing. Every method has to return
    something, and the awkward rows have to be reachable."""
    summary = await in_postgres.get_inventory_summary_v2()
    statuses = {k: v["skuCount"] for k, v in summary["summary"].items()
                if k != "total"}
    assert summary["summary"]["total"]["skuCount"] > 0
    assert statuses["neverSold"] >= 1, "the never-sold SKU is not reaching the view"
    assert statuses["deadStock"] >= 1, "no dead stock: the thresholds are not biting"
    assert summary["categoryThresholds"], "v_category_velocity produced no rows"

    deep = await in_postgres.get_dead_stock_deep(limit=5)
    assert deep["items"], "no dead-stock items"
    qualities = {i["costQuality"] for i in (await in_postgres.get_all_skus_deep())}
    assert qualities == {"actual", "fallback"}, (
        f"both cost paths must be exercised, saw {qualities}")

    turnover = await in_postgres.get_inventory_turnover(days=30)
    assert turnover["turnover"]["totalRevenue"] > 0
    assert turnover["currentStock"]["valueSale"] > 0

    trend = await in_postgres.get_inventory_trend(days=90, granularity="monthly")
    assert len(trend["labels"]) >= 2, (
        "the monthly buckets must span more than one month or the grouping "
        "is not being compared at all")
    daily = await in_postgres.get_inventory_trend(days=90, granularity="daily")
    assert len(daily["labels"]) == len(HISTORY)

    average = await in_postgres.get_average_inventory(days=90)
    assert average["dataPoints"] == len(HISTORY)
    assert "message" not in average, "the no-history branch is not what ran"


@pytest.mark.asyncio
async def test_the_summary_exercises_what_it_was_ported_for(in_postgres):
    """The three things `get_stock_summary` gained on the way to Postgres.

    A literal proves the answer is what it was; it does not prove the answer
    says anything interesting. These are the parts of it that were new or
    changed, asserted outright.
    """
    summary = await in_postgres.get_stock_summary(limit=5)

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

    # 3. `lastSync` is the snapshot the rows were built in — the instant the
    #    rebuild stamped, carried rather than restamped. It used to read a `sync_metadata` key nothing has
    #    ever written, so it was NULL for the life of the feature.
    assert summary["lastSync"] == SNAPSHOT_AT.isoformat()

    # 4. The tie the fixture plants: S-11 and S-12 are both 3 units at 500, so
    #    the low-stock list's ORDER BY has to break it on `offer_id` or the
    #    engine is free to choose which one a LIMIT keeps.
    tied = [i["sku"] for i in summary["lowStock"] if i["sku"] in ("S-11", "S-12")]
    assert tied == ["S-11", "S-12"], tied


@pytest.mark.asyncio
async def test_postgres_answers_in_kyivs_day_whatever_its_session_says(in_postgres):
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
async def test_the_gold_rollup_filter_is_load_bearing(in_postgres):
    """The fixture's Gold carries per-source rows as well as roll-ups, so a
    Postgres reader that forgot `source_id IS NULL` would double the revenue.
    Measured on production over 30 days before this hole existed: 11,107,040.50
    against a true 5,553,520.25. The literal is DuckDB's, whose Gold had one
    grain and could not double it."""
    answer = await in_postgres.get_inventory_turnover(days=30)
    recorded = GOLDEN[call_key("get_inventory_turnover", {"days": 30})]
    assert (_comparable(answer["turnover"]["totalRevenue"])
            == recorded["turnover"]["totalRevenue"])
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
