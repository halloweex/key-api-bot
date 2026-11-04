"""`/marketing` answers in Postgres what DuckDB answered.

Two of the three parts of this tab were not a table rename when they moved:

  * the channel figures come from **columns** in DuckDB's Gold and from a
    `source_id` **dimension** in Postgres', with the five scalars read from the
    roll-up rows only, since three of them are `COUNT(DISTINCT buyer_id)` and
    distinct counts do not add up;
  * the brand table used to read `gold_daily_products`, which exists only in
    DuckDB, and now reads the order-line level — reproducing that table's grain
    so `SUM(order_count)` keeps meaning what the report has always shown.

ONE ENGINE SINCE STEP 14 (S14-05)

The tab reads Postgres and nothing else — the goals included, whose read used
to follow chain 7a on a branch of its own. Every call is compared with a
literal (`tests/integration/golden/marketing.json`): DuckDB's answer on this
fixture, from its own Gold of columns, recorded while this file still asserted
Postgres answered the same from its Gold of rows (rule 9 of the step-14 plan).
Postgres derives Silver and Gold from the seeded Bronze — `rebuild_gold`, the
production path (`tests/integration/pg_tab_seed.py`) — and the store's DuckDB
connection is a trap.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.duckdb_constants import B2B_MANAGER_ID
from core.duckdb_store import DuckDBStore
from tests.integration.pg_tab_seed import (
    call_key, comparable, derive, golden, seed, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# Pinned (step 14, S14-05): the answers are literals taken from DuckDB on
# this fixture, so the calendar under them cannot move with the run.
TODAY = date(2026, 9, 15)
NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)   # midday Kyiv
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


async def _populate(conn, pool):
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS,
        orders=[(oid, src, 19 if is_ret else 1, total, NOW - timedelta(days=days),
                 buyer, manager, None)
                for oid, src, total, days, is_ret, manager, buyer in ORDERS],
        lines=[(lid, oid, pid, name, qty, price)
               for lid, oid, pid, qty, price, name in LINES])
    await derive(pool)
    await conn.executemany(
        "INSERT INTO app.revenue_goals (period_type,goal_amount,is_custom,"
        "calculated_goal,growth_factor,updated_at) VALUES ($1,$2,$3,$4,$5,now())",
        GOALS)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The tab's store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    store = DuckDBStore(db_path=tmp_path / "marketing.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the marketing tab reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store


async def _answer(store, name, kwargs):
    return await getattr(store, name)(*WINDOW, **kwargs)


GOLDEN = golden("marketing")


def _the_month():
    from calendar import monthrange

    return TODAY.replace(day=1), TODAY.replace(
        day=monthrange(TODAY.year, TODAY.month)[1])


CALLS = (
    ("get_marketing_report_by_dates", {}),
    ("get_marketing_report_by_dates", {"sales_type": "all"}),
    ("get_marketing_report_by_dates", {"sales_type": "b2b"}),
    ("get_marketing_report_by_dates", {"brand": "BrandA"}),
    ("get_marketing_report_by_dates", {"brand": "Unknown", "sales_type": "all"}),
    ("get_marketing_report_by_dates", {"category_id": 1}),
    ("get_marketing_report_by_dates", {"category_id": 3, "brand": "BrandB"}),
    ("get_promocode_analytics", {}),
    ("get_promocode_analytics", {"sales_type": "all"}),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,kwargs", CALLS,
    ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
)
async def test_postgres_answers_what_duckdb_answered(in_postgres, name, kwargs):
    assert comparable(await _answer(in_postgres, name, kwargs)) == \
        GOLDEN[call_key(name, kwargs)]


def test_every_literal_is_a_call_and_every_call_has_one():
    assert set(GOLDEN) == {call_key(n, k) for n, k in CALLS}


@pytest.mark.asyncio
async def test_the_channel_split_comes_from_the_source_dimension(in_postgres):
    """A `source_id` dimension in Postgres, where DuckDB wrote a column per
    channel: the literal above is DuckDB's answer from the columns."""
    out = await _answer(in_postgres, "get_marketing_report_by_dates", {})
    by_name = {s["source_name"]: s for s in out["sources"]}
    assert by_name["Instagram"]["revenue"] == 1200.0
    assert by_name["Telegram"]["revenue"] == 600.0
    assert by_name["Сайт"]["revenue"] == 900.0 + 850.0


@pytest.mark.asyncio
async def test_a_buyer_on_two_channels_is_counted_once(in_postgres):
    """Buyer 7 bought on Instagram and Telegram the same day. Folding the
    per-source rows would count them twice — the reason the Postgres rendering
    reads `unique_customers` from the roll-up row only. This is the assertion
    that fails if that filter is ever dropped."""
    out = await _answer(in_postgres, "get_marketing_report_by_dates", {})
    retail_buyers = {o[6] for o in ORDERS
                     if not o[4] and o[5] is None and o[1] != 3}
    assert out["general_sales"]["current"]["customers"] == len(retail_buyers)
    # …and the naive fold would have been larger.
    assert len(retail_buyers) < len([o for o in ORDERS
                                     if not o[4] and o[5] is None and o[1] != 3])


@pytest.mark.asyncio
async def test_the_brand_table_reproduces_the_gold_grain(in_postgres):
    """One product twice on one order under two names-as-sold is two rows in
    `gold_daily_products`, and the report sums their `order_count`. The
    rewritten query must reproduce that, not improve on it."""
    out = await _answer(in_postgres, "get_marketing_report_by_dates", {})
    brands = {b["brand"]: b for b in out["brands"]}
    assert set(brands) == {"BrandA", "BrandB", "Unknown"}, brands
    # Order 1 carries Serum twice under two names, so BrandA's summed
    # order_count is 2 for that day plus 1 for order 2 — not the 2 a plain
    # COUNT(DISTINCT order_id) would give.
    assert brands["BrandA"]["orders"] == 3, brands["BrandA"]


@pytest.mark.asyncio
async def test_the_goal_is_read_where_it_is_written(in_postgres):
    """A full calendar month is the only shape that shows it, and the goal is
    `app.revenue_goals` — chain 7a's table, read on the router's one path."""
    first, last = _the_month()
    out = await in_postgres.get_marketing_report_by_dates(first, last)
    assert out["general_sales"]["monthly_goal"] == float(GOALS[0][1])


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(in_postgres):
    report = await _answer(in_postgres, "get_marketing_report_by_dates", {})
    assert report["general_sales"]["current"]["revenue"] > 0
    assert report["brands"], "no brands"
    assert len(report["sources"]) == 3
    assert report["general_sales"]["previous"]["revenue"] == 0, (
        "the previous period must be empty here, or the window is wrong")


@pytest.mark.asyncio
async def test_a_brand_narrows_every_section_to_its_lines(in_postgres):
    """BrandA is Serum: order 1 on Instagram (2×500 + 1×500 under a second
    name-as-sold) and order 2 on Telegram (1×500), both buyer 7 on one day.
    Revenue is the lines, never the baskets' ₴1 800 of grand totals."""
    report = await _answer(in_postgres, "get_marketing_report_by_dates",
                           {"brand": "BrandA"})
    cur = report["general_sales"]["current"]
    assert cur["revenue"] == 2000.0, cur
    assert cur["orders"] == 2
    # One buyer on one day: Gold's grain counts them once.
    assert cur["customers"] == 1
    assert [b["brand"] for b in report["brands"]] == ["BrandA"]
    by_name = {s["source_name"]: s for s in report["sources"]}
    assert by_name["Instagram"]["revenue"] == 1500.0
    assert by_name["Telegram"]["revenue"] == 500.0
    assert by_name["Сайт"]["revenue"] == 0
    assert report["product_filter"] == {"category_id": None, "brand": "BrandA"}


@pytest.mark.asyncio
async def test_a_parent_category_takes_its_children(in_postgres):
    """Serum hangs off Serums (2), a child of Care (1). Choosing Care and
    getting nothing is the empty result nobody debugs."""
    report = await _answer(in_postgres, "get_marketing_report_by_dates",
                           {"category_id": 1})
    assert report["general_sales"]["current"]["revenue"] == 2000.0
    assert [b["brand"] for b in report["brands"]] == ["BrandA"]


@pytest.mark.asyncio
async def test_a_filtered_full_month_hides_the_shop_goal(in_postgres):
    """The goal is the whole shop's; beside one brand it would read as that
    brand's target."""
    first, last = _the_month()
    report = await in_postgres.get_marketing_report_by_dates(first, last, brand="BrandA")
    assert report["general_sales"]["monthly_goal"] is None
    unfiltered = await in_postgres.get_marketing_report_by_dates(first, last)
    assert unfiltered["product_filter"] is None
