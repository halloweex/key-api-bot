"""`/reports` answers in Postgres what DuckDB answered.

`tests/unit/test_pg_reports_read.py` proves the bodies are one text and that
nothing goes round the router. Necessary and not sufficient: identical SQL
still means different things where the engines differ — how a correlated
`SUM()` behaves over an order with no line items, what `COUNT(CASE …)` returns
for an empty group, where `NULL`s sort, and which rows a `LIMIT` keeps when the
sort has ties.

So this runs the real thing, and compares the **whole result**.

ONE ENGINE SINCE STEP 14 (S14-05)

The tab reads Postgres and nothing else, so its answer is compared with a
literal (`tests/integration/golden/reports.json`): DuckDB's, on this fixture,
recorded while this file still asserted Postgres answered the same (rule 9 of
the step-14 plan). Postgres derives Silver from the seeded Bronze — the
production path (`tests/integration/pg_tab_seed.py`), where this file used to
copy DuckDB's Silver across — so a Silver derived differently fails here too.
The store's DuckDB connection is a trap.

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


async def _populate(conn, pool):
    names = {p[0]: p[1] for p in PRODUCTS}
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS,
        orders=[(oid, src, 19 if is_ret else 1, total, NOW - timedelta(days=days),
                 7, manager, None)
                for oid, src, total, days, is_ret, manager in ORDERS],
        lines=[(lid, oid, pid, names[pid], qty, price)
               for lid, oid, pid, qty, price in LINES])
    await derive(pool)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The tab's store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    store = DuckDBStore(db_path=tmp_path / "reports.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the reports tab reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store


async def _answer(store, name, kwargs):
    return await getattr(store, name)(*WINDOW, **kwargs)


GOLDEN = golden("reports")


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
async def test_postgres_answers_what_duckdb_answered(in_postgres, name, kwargs):
    assert comparable(await _answer(in_postgres, name, kwargs)) == \
        GOLDEN[call_key(name, kwargs)]


def test_every_literal_is_a_call_and_every_call_has_one():
    assert set(GOLDEN) == {call_key(n, k) for n, k in CALLS}


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
async def test_the_fixture_is_not_empty(in_postgres):
    """An empty answer agrees with an empty literal about nothing, and every
    awkward row has to be reachable or the comparison above is decoration."""
    summary = await _answer(in_postgres, "get_report_summary", {})
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
    every = await _answer(in_postgres, "get_report_summary", {"sales_type": "all"})
    assert every["totals"]["orders_count"] == len(ALL_ORDERS) == 4
    assert every["totals"]["revenue"] == sum(o[2] for o in ALL_ORDERS)

    assert await _answer(in_postgres, "get_report_top_products", {}), "no products"

    # The NULL-brand product sells only on the b2b order and the retired one,
    # so `all` is where it surfaces — and the `Unknown` bucket has to reach it,
    # since equality never matches NULL and that bucket was displayable but
    # unselectable until `brand_where` named it.
    unbranded = next(p for p in PRODUCTS if p[3] is None)
    every_product = await _answer(
        in_postgres, "get_report_top_products", {"sales_type": "all"})
    assert any(p["sku"] == unbranded[4] for p in every_product), every_product


@pytest.mark.asyncio
async def test_the_tie_is_cut_by_the_key_not_the_plan(in_postgres):
    """Lipstick and Unbranded both sell 3 in the retail window. With `limit=2`
    the cut falls inside the tied group, so an unbroken tie would keep
    whichever product the plan reached first."""
    cut = await _answer(in_postgres, "get_report_top_products", {"limit": 2})
    every = await _answer(in_postgres, "get_report_top_products", {})
    assert len(cut) == 2
    assert [p["product_name"] for p in cut] == [p["product_name"] for p in every][:2]


@pytest.mark.asyncio
async def test_an_absent_category_is_an_empty_report(in_postgres):
    """`category_id IN ()` is a parser error; the guard turns it into a filter
    that matches nothing, which is what it means."""
    out = await _answer(in_postgres, "get_report_summary", {"category_id": 99999})
    assert out["totals"]["orders_count"] == 0


@pytest.mark.asyncio
async def test_the_category_tree_is_ordered_and_complete(in_postgres):
    """It becomes an `IN (...)`, so its order is part of the filter."""
    assert await in_postgres._category_ids(1) == [1, 2]
