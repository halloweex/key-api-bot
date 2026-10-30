"""`/margin` answers in Postgres what DuckDB answered.

The cheapest port and the one with the sharpest arithmetic: margin is revenue
minus COGS, COGS comes from `offer_stocks.purchased_price` joined by SKU, and
the interesting cases are all about *absence* — a SKU with no cost row, a cost
of zero, a product whose SKU is NULL. Each of those decides whether a line
counts toward `costed_revenue`, and a difference of one line moves the
percentage the page shows.

ONE ENGINE SINCE STEP 14 (S14-05)

The tab reads Postgres and nothing else. Every call is compared with a literal
(`tests/integration/golden/margin.json`): DuckDB's answer on this fixture,
recorded while this file still asserted Postgres answered the same (rule 9 of
the step-14 plan). Postgres derives Silver from the seeded Bronze, the
production path (`tests/integration/pg_tab_seed.py`), and the store's DuckDB
connection is a trap.

`tests/unit/test_pg_margin_read.py` proves the statements. This proves the
numbers.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

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

# Pinned (step 14, S14-05): the answers are literals taken from DuckDB on
# this fixture, so the calendar under them cannot move with the run.
TODAY = date(2026, 9, 15)
NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)   # midday Kyiv
W = (TODAY - timedelta(days=25), TODAY)

CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Lipstick", 3, "BrandB", "SKU-B", 100.0),
    (300, "Costless", 3, "BrandB", "SKU-C", 250.0),   # no cost row at all
    (400, "ZeroCost", 3, "BrandC", "SKU-D", 300.0),   # cost row, but 0
    (500, "NoSku", 3, "BrandC", None, 150.0),         # NULL sku — joins nothing
]
# (offer_id, sku, price, purchased_price)
STOCKS = [
    (1, "SKU-A", 500.0, 200.0),
    (2, "SKU-B", 100.0, 40.0),
    (4, "SKU-D", 300.0, 0.0),        # zero is "no cost", not "free"
]
ORDERS = [(1, 1, 2000.0, 2), (2, 2, 900.0, 3), (3, 1, 450.0, 4)]
LINES = [
    (1, 1, 100, 2, 500.0),
    (2, 1, 200, 3, 100.0),
    (3, 1, 300, 1, 250.0),     # costless
    (4, 2, 400, 2, 300.0),     # zero-cost
    (5, 2, 500, 2, 150.0),     # NULL sku
    (6, 3, 100, 1, 450.0),     # same SKU, a discounted line
]


async def _populate(conn, pool):
    names = {p[0]: p[1] for p in PRODUCTS}
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS,
        stocks=STOCKS,
        orders=[(oid, src, 1, total, NOW - timedelta(days=days), oid, None, None)
                for oid, src, total, days in ORDERS],
        lines=[(lid, oid, pid, names[pid], q, pr) for lid, oid, pid, q, pr in LINES])
    await derive(pool)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The tab's store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    store = DuckDBStore(db_path=tmp_path / "margin.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the margin tab reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store


async def _answer(store, name, kwargs):
    return await getattr(store, name)(*W, **kwargs)


GOLDEN = golden("margin")


CALLS = (
    ("get_margin_overview", {}),
    ("get_margin_overview", {"sales_type": "all"}),
    ("get_margin_by_brand", {}),
    ("get_margin_by_brand", {"limit": 2}),
    ("get_margin_by_category", {}),
    ("get_margin_trend", {}),
    ("get_margin_brand_category", {"min_revenue": 0}),
    ("get_margin_brand_category", {"min_revenue": 100000}),   # empty result
    ("get_margin_alerts", {"min_revenue": 0}),
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
async def test_the_three_kinds_of_missing_cost_all_land(in_postgres):
    """Each is a different reason a line has no COGS, and each has to be
    excluded from `costed_revenue` while still counting toward revenue — that
    difference is what `coverage_pct` measures."""
    overview = await _answer(in_postgres, "get_margin_overview", {})

    priced = sum(q * p for _l, _o, pid, q, p in LINES
                 if pid in (100, 200))                    # the two with real cost
    assert overview["costed_revenue"] == priced, overview
    assert overview["total_revenue"] > overview["costed_revenue"], (
        "the costless, zero-cost and NULL-sku lines are not reaching revenue")
    assert 0 < overview["coverage_pct"] < 100
    assert overview["cogs"] > 0 and overview["profit"] > 0


@pytest.mark.asyncio
async def test_an_empty_result_is_an_empty_list(in_postgres):
    """The threshold nobody clears."""
    assert await _answer(in_postgres, "get_margin_brand_category",
                         {"min_revenue": 100000}) == []


@pytest.mark.asyncio
async def test_the_fixture_is_not_empty(in_postgres):
    by_brand = await _answer(in_postgres, "get_margin_by_brand", {})
    assert by_brand, "no brands"
    assert {b["brand"] for b in by_brand} >= {"BrandA", "BrandB"}
    assert await _answer(in_postgres, "get_margin_by_category", {}), "no categories"
    assert await _answer(in_postgres, "get_margin_trend", {}), "no trend"
