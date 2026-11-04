"""`/traffic` answers in Postgres what DuckDB answered.

The port with the sharpest structural claim: DuckDB aggregated this tab out of
`gold_daily_traffic`, and Postgres never had such a table *on purpose* —
DuckDB no longer has one either, since the layer was retired once this port
left it with no reader. Both readers folded that Gold — `SUM(orders_count)`
grouped by a subset of its own primary key — and `silver_order_utm` is keyed
on `order_id`, so every order lands in exactly one cell and the fold is the
same arithmetic as the aggregate over the orders underneath.

That claim is what this file exists to attack, so the fixture is built to break
it if it is wrong: an order with no UTM row at all (falls through the COALESCE
to `organic`/`instagram`), an order whose UTM says paid, one on a source with
no channel of its own, a return and an inactive source that must not count, and
two campaigns tied on revenue to catch a pagination cut that is not
deterministic.

ONE ENGINE SINCE STEP 14 (S14-05)

The tab reads Postgres and nothing else, so the two engines are no longer run
side by side. Every call is compared with a literal instead
(`tests/integration/golden/traffic.json`): DuckDB's answer on this fixture,
recorded while this file still asserted Postgres answered the same (rule 9 of
the step-14 plan) — re-recorded whole, the same way, when the pixel-only
breakdown (#327) added order 10 to the fixture. Postgres derives its Silver
and Gold from the seeded Bronze, the production path
(`tests/integration/pg_tab_seed.py`), and the store's DuckDB connection is a
trap.

`tests/unit/test_pg_traffic_read.py` proves the statements. This proves the
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

# (id, source_id, grand_total, days_ago, status_id)
# status 19 is in KeyCRM's lost/cancel group, so order 6 is a return; source 3
# is Opencart, which `is_active_source` excludes. Neither may reach a chart,
# and both are here because the predicate that drops them lives in the query
# now rather than in the Gold's own WHERE.
ORDERS = [
    (1, 1, 1000.0, 2, 1),     # paid_confirmed / facebook
    (2, 1, 2000.0, 3, 1),     # paid_likely / tiktok
    (3, 2, 300.0, 4, 1),      # no UTM row at all → organic / telegram
    (4, 4, 500.0, 5, 1),      # Shopify, UTM says google
    (5, 1, 700.0, 6, 1),      # tied on revenue with 7, different campaign
    (6, 1, 900.0, 7, 19),     # return — excluded
    (7, 1, 700.0, 8, 1),      # tied on revenue with 5
    (8, 3, 400.0, 9, 1),      # Opencart — inactive source, excluded
    (9, 4, 450.0, 10, 1),     # Shopify, a Klaviyo newsletter → organic / email
    (10, 4, 650.0, 1, 1),     # buyer 9 again, no tags → pixel_only
]

# Every order is its own buyer but one: order 10 is buyer 9 coming back to the
# website with no tags, so the pixel-only breakdown has a first channel to
# find.
BUYER_OF = {10: 9}

# (order_id, utm_source, utm_medium, utm_campaign, traffic_type, platform, fbclid)
UTM = [
    (1, "fbads", "paid", "spring", "paid_confirmed", "facebook", "abc123"),
    (2, "tiktok", "cpc", "summer", "paid_likely", "tiktok", None),
    (4, "google", "cpc", "12345", "paid_confirmed", "google", None),
    (5, "fbads", "paid", "alpha", "paid_confirmed", "facebook", None),
    (6, "fbads", "paid", "spring", "paid_confirmed", "facebook", None),
    (7, "fbads", "paid", "beta", "paid_confirmed", "facebook", None),
    (8, "fbads", "paid", "spring", "paid_confirmed", "facebook", None),
    (9, "klaviyo", "klaviyo", "[07/28/26] Campaign #12: Beauty roulette",
     "organic", "email", None),
    (10, None, None, None, "pixel_only", "unattributed", None),
]

UTM_COLUMNS = (
    "order_id", "utm_source", "utm_medium", "utm_campaign", "utm_content",
    "utm_term", "utm_lang", "fbp", "fbc", "ttp", "fbclid",
    "traffic_type", "platform", "parsed_at",
)

# (id, expense_date_days_ago, category, expense_type, amount, platform)
SPEND = [
    (1, 3, "marketing", "Facebook Ads", 500.0, "facebook"),
    (2, 4, "marketing", "TikTok Ads", 250.0, "tiktok"),
    (3, 5, "salary", "Salary", 9999.0, None),      # not marketing — excluded
    (4, 6, "marketing", "Unattributed", 100.0, None),  # NULL platform — excluded
]


def _utm_row(row):
    oid, src, medium, campaign, ttype, platform, fbclid = row
    return (oid, src, medium, campaign, None, None, None,
            None, None, None, fbclid, ttype, platform, NOW)


async def _populate(conn, pool):
    """Bronze as KeyCRM lands it, Silver and Gold derived from it; then the
    UTM rows and the typed ad spend, the two tables the tab reads beside them.

    The UTM rows go in directly, after Silver, and not through the parser
    reading `manager_comment`, which is not what this file is testing. Gold is
    derived rather than written because ROAS reads it, and the two Golds are
    not the same shape: this one carries `source_id` as a dimension *and* a
    `source_id IS NULL` roll-up row, and only with the fine rows there is the
    double-count the query's predicate prevents armed."""
    await seed(conn, orders=[
        (oid, src, status, total, NOW - timedelta(days=days),
         BUYER_OF.get(oid, oid), None, None)
        for oid, src, total, days, status in ORDERS])
    await derive(pool)
    await conn.executemany(
        f"INSERT INTO silver.order_utm ({', '.join(UTM_COLUMNS)}) VALUES "
        f"({', '.join(f'${i}' for i in range(1, len(UTM_COLUMNS) + 1))})",
        [_utm_row(r) for r in UTM])
    await conn.executemany(
        "INSERT INTO app.manual_expenses (id,expense_date,category,expense_type,"
        "amount,currency,note,created_at,updated_at,platform)"
        " VALUES ($1,$2,$3,$4,$5,'UAH',NULL,$6,$6,$7)",
        [(eid, TODAY - timedelta(days=days), category, etype, amount, NOW, platform)
         for eid, days, category, etype, amount, platform in SPEND])


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The tab's store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    store = DuckDBStore(db_path=tmp_path / "traffic.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the traffic tab reached for DuckDB")

    monkeypatch.setattr(type(store), "connection", _no_duckdb)
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store


async def _answer(store, name, kwargs):
    return await getattr(store, name)(*W, **kwargs)


GOLDEN = golden("traffic")


CALLS = (
    ("get_traffic_analytics", {}),
    ("get_traffic_analytics", {"sales_type": "retail"}),
    ("get_traffic_analytics", {"source_id": 1}),
    ("get_traffic_organic", {}),
    ("get_traffic_organic", {"sales_type": "retail"}),
    ("get_traffic_organic", {"source_id": 2}),
    ("get_traffic_pixel_only", {}),
    ("get_traffic_pixel_only", {"sales_type": "retail"}),
    ("get_traffic_trend", {}),
    ("get_traffic_trend", {"source_id": 2}),
    ("get_traffic_transactions", {}),
    ("get_traffic_transactions", {"limit": 2}),
    ("get_traffic_transactions", {"traffic_type": "organic"}),
    ("get_traffic_transactions", {"platform": "facebook"}),
    ("get_traffic_utm_campaigns", {}),
    ("get_traffic_utm_campaigns", {"limit": 2}),
    ("get_traffic_utm_campaigns", {"limit": 2, "offset": 2}),
    ("get_traffic_utm_campaigns", {"sort_by": "orders", "sort_dir": "asc"}),
    ("get_traffic_roas", {}),
    ("get_traffic_roas", {"sales_type": "retail"}),
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


# `test_postgres_matches_the_gold_duckdb_still_keeps` stood here.
#
# It read `gold_daily_traffic` directly and checked that folding its cells gave
# what Postgres computes from the join — the arithmetic behind the decision not
# to build that Gold in Postgres at all. The layer has been retired in DuckDB
# too, so there is no second computation left to compare against. What is left
# is the literal above: DuckDB's answers on the same fixture, recorded while the
# two engines were still compared.


@pytest.mark.asyncio
async def test_an_order_with_no_utm_row_still_counts(in_postgres):
    """Order 3 has no row in `silver.order_utm` at all, and the LEFT JOIN
    renders it NULL. The COALESCE has to make it `organic`/`telegram` — an
    INNER JOIN anywhere here would drop it silently, and that is the
    commonest order in production."""
    out = await _answer(in_postgres, "get_traffic_analytics", {})
    assert out["by_platform"]["telegram"]["orders"] == 1
    assert out["by_platform"]["telegram"]["revenue"] == 300.0
    # Order 3, and order 9's newsletter, which has a UTM row of its own.
    assert out["summary"]["organic"]["orders"] == 2


# Derived from ORDERS rather than written out, because writing it out is how
# this assertion was wrong the first time: status 19 is in KeyCRM's lost/cancel
# group and source 3 is Opencart, and a reader counting on their fingers misses
# one of the six that remain.
LOST_CANCEL_STATUS = {19}
INACTIVE_SOURCE = {3}
COUNTED = [
    o for o in ORDERS
    if o[4] not in LOST_CANCEL_STATUS and o[1] not in INACTIVE_SOURCE
]


@pytest.mark.asyncio
async def test_the_return_and_the_inactive_source_are_both_absent(in_postgres):
    """Orders 6 and 8 carry ₴1 300 between them and must reach no chart. The
    predicate that drops them used to live in the Gold's own WHERE; it lives
    in the query now, which is exactly the kind of move that loses a filter."""
    out = await _answer(in_postgres, "get_traffic_analytics", {})
    assert out["totals"]["orders"] == len(COUNTED)
    assert out["totals"]["revenue"] == sum(o[2] for o in COUNTED)


@pytest.mark.asyncio
async def test_the_new_buyers_are_not_an_empty_block(in_postgres):
    """Every order here but order 10 has a buyer of its own (`buyer_id =
    id`), so every counted buyer is a new one; order 10 is buyer 9's second.
    That is what keeps the whole-payload
    comparison above from passing on an empty `new_customers` block — and it
    puts the return and Opencart's order, whose buyers are never new, on the
    wrong side of the predicate if it is lost."""
    out = await _answer(in_postgres, "get_traffic_analytics", {})
    paid = {u[0] for u in UTM if u[4] in ("paid_confirmed", "paid_likely")}
    new = out["new_customers"]
    assert new["total"] == len({BUYER_OF.get(o[0], o[0]) for o in COUNTED})
    assert new["summary"]["paid"] == len(paid & {o[0] for o in COUNTED})
    assert new["by_platform"]["telegram"] == 1


@pytest.mark.asyncio
async def test_the_blended_roas_is_not_doubled(in_postgres):
    """The trap the port nearly shipped: Postgres' revenue Gold holds a
    per-source row *and* a roll-up row, so `SUM(revenue)` without the
    predicate counts every order twice — and the ROAS would read exactly
    half, since spend is the divisor."""
    out = await _answer(in_postgres, "get_traffic_roas", {})
    assert out["blended"]["revenue"] == sum(o[2] for o in COUNTED)
    assert out["blended"]["spend"] == 750.0      # marketing, non-NULL platform
    assert out["has_spend_data"] is True


@pytest.mark.asyncio
async def test_two_campaigns_tied_on_revenue_paginate_once_each(in_postgres):
    """Orders 5 and 7 are ₴700 each on different campaigns. Under a `LIMIT`
    with no tiebreaker one could land on both pages or on neither — which is
    not a reordering, it is a campaign missing from the page."""
    first = await _answer(in_postgres, "get_traffic_utm_campaigns", {"limit": 2})
    second = await _answer(in_postgres, "get_traffic_utm_campaigns",
                           {"limit": 2, "offset": 2})
    every = await _answer(in_postgres, "get_traffic_utm_campaigns", {})
    paged = [c["campaign"] for c in first["campaigns"] + second["campaigns"]]
    assert paged == [c["campaign"] for c in every["campaigns"]][:4]
    assert {"alpha", "beta"} <= set(paged)


@pytest.mark.asyncio
async def test_organic_is_split_into_its_channels(in_postgres):
    """Order 3 is organic only through the COALESCE — no UTM row, source 2 —
    and order 9 through its tags. The channel is decided in Python from what
    the engine returns, so what this attacks is the statement: the boolean
    that says "tagged", the COALESCE it is grouped on, and GROUP BY ordinals."""
    out = await _answer(in_postgres, "get_traffic_organic", {})
    channels = {c["channel"]: c for c in out["channels"]}
    assert set(channels) == {"telegram", "email_campaigns"}
    assert channels["telegram"]["revenue"] == 300.0
    assert channels["telegram"]["details"] == [{
        "source": "", "medium": "", "campaign": "", "content": "",
        "orders": 1, "revenue": 300.0,
    }]
    assert channels["email_campaigns"]["details"] == [{
        "source": "klaviyo", "medium": "klaviyo",
        "campaign": "[07/28/26] Campaign #12: Beauty roulette",
        "content": "", "orders": 1, "revenue": 450.0,
    }]
    assert out["totals"]["new_customers"] == 2


@pytest.mark.asyncio
async def test_pixel_only_finds_the_first_channel(in_postgres):
    """Order 10 is buyer 9's second order, untagged; buyer 9 first came
    through a Klaviyo newsletter. The ranking of every buyer's first order —
    a window function over the whole of Silver — is the part a statement
    could get wrong."""
    out = await _answer(in_postgres, "get_traffic_pixel_only", {})
    assert out["first_channels"] == [{
        "group": "organic", "channel": "email_campaigns",
        "orders": 1, "revenue": 650.0, "buyers": 1, "new_buyers": 0,
    }]
    assert out["returning"] == {"orders": 1, "revenue": 650.0, "buyers": 1}
    assert out["new"] == {"orders": 0, "revenue": 0.0, "buyers": 0}
