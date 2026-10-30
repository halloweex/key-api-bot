"""The product-intelligence reads answer in Postgres what DuckDB answered.

Five of the six needed only table names. The sixth, `get_product_momentum`,
read `gold_daily_products` — DuckDB's alone — and now reads the order-line
level, which reproduces that table to the kopeck under Gold's own predicate.

THE FIXTURE IS BUILT FOR BASKETS

Co-occurrence is the subject here, so the awkward parts are about pairs: an
order with three items, several orders sharing one pair so it can win a tie, a
single-item order that must not reach any pair, a product sold under two
names-as-sold, and a previous period so momentum has something to compare to.

ONE ENGINE SINCE STEP 14 (S14-05)

The reads go to Postgres and nothing else. Every call is compared with a
literal (`tests/integration/golden/products_intel.json`): DuckDB's answer on
this fixture, recorded while this file still asserted Postgres answered the
same with DuckDB made fatal (rule 9 of the step-14 plan). Postgres derives
Silver from the seeded Bronze (`tests/integration/pg_tab_seed.py`), and the
store's DuckDB connection is a trap.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from tests.integration.pg_tab_seed import (
    call_key, comparable, derive, golden, seed, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# The store buckets orders by their **Kyiv** date, so the fixture has to think
# in Kyiv too. It did not, and that cost two red CI runs: `date.today()` on a
# UTC runner plus an `ordered_at` of `now_utc - 15 days` puts the previous
# period's order at 00:11 Kyiv on the window's first day, inside the period it
# exists to sit outside. Both failures were the same order — the current
# period counted six instead of five, and momentum then had an empty previous
# period to compare against.
#
# It is a three-hour flake: the shift only happens when the run starts between
# 21:00 and 23:59 UTC. Both failing runs were at 21:11 and 21:30.
#
# Two changes make it impossible rather than unlikely. The day comes from
# Kyiv, so `W` means what the store means; and every order is stamped at
# **midday**, where no offset this codebase sees can move the calendar day.
KYIV = ZoneInfo("Europe/Kyiv")
# Pinned (step 14, S14-05): the answers are literals taken from DuckDB on
# this fixture, so the calendar under them cannot move with the run.
TODAY = date(2026, 9, 15)
W = (TODAY - timedelta(days=14), TODAY)          # current period
PREV_DAYS = 15                                    # lands in the previous one


def _ordered_at(days_ago: int) -> datetime:
    """Midday Kyiv, `days_ago` days back — a timestamp no timezone can move."""
    return datetime.combine(
        TODAY - timedelta(days=days_ago), time(12, 0), tzinfo=KYIV)

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


async def _populate(conn, pool):
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS,
        orders=[(oid, 1, 1, total, _ordered_at(days), oid, None, None)
                for oid, days, total in ORDERS],
        lines=[(l, o, p, n, q, pr) for l, o, p, q, pr, n in LINES])
    await derive(pool)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The tab's store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    store = DuckDBStore(db_path=tmp_path / "intel.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the product-intelligence read reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store


async def _answer(store, name, kwargs):
    return await getattr(store, name)(*W, **kwargs)


GOLDEN = golden("products_intel")


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
async def test_postgres_answers_what_duckdb_answered(in_postgres, name, kwargs):
    assert comparable(await _answer(in_postgres, name, kwargs)) == \
        GOLDEN[call_key(name, kwargs)]


def test_every_literal_is_a_call_and_every_call_has_one():
    assert set(GOLDEN) == {call_key(n, k) for n, k in CALLS}


@pytest.mark.asyncio
async def test_the_fixture_reaches_the_awkward_baskets(in_postgres):
    """A comparison of two empty answers agrees about nothing."""
    summary = await _answer(in_postgres, "get_basket_summary", {})
    assert summary["totalOrders"] == len([o for o in ORDERS if o[1] <= 14])
    assert summary["multiItemOrders"] < summary["totalOrders"], (
        "the single-item order is not reaching the split")
    assert summary["topPairCount"] >= 2, "no pair repeats, so no tie to break"

    dist = await _answer(in_postgres, "get_basket_distribution", {})
    buckets = {d["bucket"] for d in dist}
    assert len(buckets) >= 2, f"one bucket only: {buckets}"

    pairs = await _answer(in_postgres, "get_frequently_bought_together", {"limit": 10})
    assert pairs, "no pairs"


@pytest.mark.asyncio
async def test_momentum_compares_against_the_previous_period(in_postgres):
    """The method reading the line level in place of `gold_daily_products`.
    Products that sold in both windows must carry a growth figure."""
    out = await _answer(in_postgres, "get_product_momentum", {"limit": 5})
    moved = out["gainers"] + out["losers"]
    assert moved, "nothing moved — the previous period is not being seen"
    assert all(m["prevRevenue"] > 0 for m in moved)


@pytest.mark.asyncio
async def test_a_product_sold_under_two_names_stays_two_rows(in_postgres):
    """Gold groups by the name *as sold*, so 'Serum' and 'Serum (promo)' are
    two rows there and the port must not merge them."""
    out = await _answer(in_postgres, "get_product_momentum", {"limit": 20})
    # Both spellings of product 100 exist in the fixture; whichever moved,
    # neither may have absorbed the other's revenue.
    for m in out["gainers"] + out["losers"]:
        if m["productId"] == 100:
            assert m["currentRevenue"] < 2500, (
                "the two names-as-sold were merged into one row", m)
