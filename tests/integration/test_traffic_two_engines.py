"""`/traffic` means the same thing in DuckDB and Postgres.

The port with the sharpest structural claim: DuckDB aggregates this tab out of
`gold_daily_traffic`, and Postgres has no such table *on purpose*. Both readers
folded that Gold — `SUM(orders_count)` grouped by a subset of its own primary
key — and `silver_order_utm` is keyed on `order_id`, so every order lands in
exactly one cell and the fold is the same arithmetic as the aggregate over the
orders underneath. Both engines therefore read the join now.

That claim is what this file exists to attack, so the fixture is built to break
it if it is wrong: an order with no UTM row at all (falls through the COALESCE
to `organic`/`instagram`), an order whose UTM says paid, one on a source with
no channel of its own, a return and an inactive source that must not count, and
two campaigns tied on revenue to catch a pagination cut that is not
deterministic.

`tests/unit/test_pg_traffic_read.py` proves the wiring and that every hole
survived its f-string. This proves the numbers.

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
]

# (order_id, utm_source, utm_medium, utm_campaign, traffic_type, platform, fbclid)
UTM = [
    (1, "fbads", "paid", "spring", "paid_confirmed", "facebook", "abc123"),
    (2, "tiktok", "cpc", "summer", "paid_likely", "tiktok", None),
    (4, "google", "cpc", "12345", "paid_confirmed", "google", None),
    (5, "fbads", "paid", "alpha", "paid_confirmed", "facebook", None),
    (6, "fbads", "paid", "spring", "paid_confirmed", "facebook", None),
    (7, "fbads", "paid", "beta", "paid_confirmed", "facebook", None),
    (8, "fbads", "paid", "spring", "paid_confirmed", "facebook", None),
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
            None, None, None, fbclid, ttype, platform,
            datetime.now(timezone.utc))


async def _seed_duckdb(store):
    now = datetime.now(timezone.utc)
    async with store.connection() as conn:
        for oid, src, total, days, status in ORDERS:
            conn.execute(
                "INSERT INTO orders (id,source_id,status_id,grand_total,"
                "ordered_at,buyer_id,manager_id) VALUES (?,?,?,?,?,?,NULL)",
                [oid, src, status, total, now - timedelta(days=days), oid])
        for row in SPEND:
            eid, days, category, etype, amount, platform = row
            conn.execute(
                "INSERT INTO manual_expenses (id,expense_date,category,"
                "expense_type,amount,currency,note,platform)"
                " VALUES (?,?,?,?,?,'UAH',NULL,?)",
                [eid, TODAY - timedelta(days=days), category, etype,
                 amount, platform])
    await store.refresh_warehouse_layers(trigger="manual")
    # After Silver, because the UTM rows are keyed on orders Silver has to
    # hold — and *not* through `refresh_utm_silver_layer`, which would parse
    # them out of `manager_comment` and is not what this file is testing.
    async with store.connection() as conn:
        conn.execute("DELETE FROM silver_order_utm")
        conn.executemany(
            f"INSERT INTO silver_order_utm ({', '.join(UTM_COLUMNS)}) "
            f"VALUES ({', '.join('?' * len(UTM_COLUMNS))})",
            [_utm_row(r) for r in UTM])
    await store.refresh_traffic_gold_layer()


async def _seed_postgres(conn, store, pool):
    for t in ("silver.order_utm", "silver.orders", "gold.daily_revenue",
              "app.manual_expenses"):
        await conn.execute(f"DELETE FROM {t}")

    async with store.connection() as duck:
        silver = duck.execute(
            "SELECT id, source_id, status_id, grand_total, ordered_at, buyer_id,"
            " manager_id, order_date, is_return, sales_type, is_active_source,"
            " source_name, is_new_customer, buyer_first_order_date, promocode"
            " FROM silver_orders ORDER BY id").fetchall()
        utm = duck.execute(
            f"SELECT {', '.join(UTM_COLUMNS)} FROM silver_order_utm "
            "ORDER BY order_id").fetchall()
        spend = duck.execute(
            "SELECT id, expense_date, category, expense_type, amount, currency,"
            " note, created_at, updated_at, platform FROM manual_expenses"
            " ORDER BY id").fetchall()

    await conn.executemany(
        "INSERT INTO silver.orders (id,source_id,status_id,grand_total,ordered_at,"
        "buyer_id,manager_id,order_date,is_return,sales_type,is_active_source,"
        "source_name,is_new_customer,buyer_first_order_date,promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])
    await conn.executemany(
        f"INSERT INTO silver.order_utm ({', '.join(UTM_COLUMNS)}) VALUES "
        f"({', '.join(f'${i}' for i in range(1, len(UTM_COLUMNS) + 1))})",
        [tuple(r) for r in utm])
    await conn.executemany(
        "INSERT INTO app.manual_expenses (id,expense_date,category,expense_type,"
        "amount,currency,note,created_at,updated_at,platform)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        [tuple(r) for r in spend])

    # ROAS reads the revenue Gold, and the two Golds are not the same shape:
    # this one carries `source_id` as a dimension *and* a `source_id IS NULL`
    # roll-up row. It is *derived* here rather than copied, because writing
    # only the roll-up would hide the double-count the query's predicate
    # exists to prevent — the fine rows have to be there for the trap to be
    # armed.
    from core.pg_gold import rebuild_gold
    await rebuild_gold(pool)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "traffic.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_duckdb(store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            async with pool.acquire() as conn:
                await _seed_postgres(conn, store, pool)
            yield store
    finally:
        async with pool.acquire() as conn:
            for t in ("silver.order_utm", "silver.orders", "gold.daily_revenue",
                      "app.manual_expenses"):
                await conn.execute(f"DELETE FROM {t}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, kwargs):
    """Each engine once — and the Postgres leg with DuckDB made fatal.

    THE FLAW THIS CLOSES, WHICH IS WORSE THAN ANY QUERY BUG

    This tab falls back to DuckDB when Postgres raises. So a comparison that
    merely calls the method twice can pass while Postgres never answers at
    all: the second call falls back, returns the DuckDB answer, and the two
    "engines" agree because they were the same engine.

    That is not hypothetical. `get_margin_trend` used `strftime`, which
    PostgreSQL does not have; the equivalent comparison passed, the gate went
    green, and the method was found on production only by making the fallback
    fatal. So the fallback is fatal here too.
    """
    monkeypatch.delenv("KS_READ_TRAFFIC", raising=False)
    duck = await getattr(store, name)(*W, **kwargs)

    monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — Postgres did not answer, so the "
            f"comparison below would have compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await getattr(store, name)(*W, **kwargs)
    monkeypatch.delenv("KS_READ_TRAFFIC", raising=False)
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
    ("get_traffic_analytics", {}),
    ("get_traffic_analytics", {"sales_type": "retail"}),
    ("get_traffic_analytics", {"source_id": 1}),
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
async def test_both_engines_return_the_same_answer(
    both_engines, monkeypatch, name, kwargs,
):
    duck, postgres = await _both(both_engines, monkeypatch, name, kwargs)
    assert _comparable(postgres) == _comparable(duck)


@pytest.mark.asyncio
async def test_postgres_matches_the_gold_duckdb_still_keeps(
    both_engines, monkeypatch,
):
    """The structural claim, stated as arithmetic.

    DuckDB's `gold_daily_traffic` is still rebuilt, so it can be read directly
    and compared against what Postgres computes from the join. If folding the
    cells were not the same as aggregating the orders — a second UTM row per
    order would be enough — these two would part company here.
    """
    monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("fell back to DuckDB")

    with patch.object(type(both_engines), "connection", _no_duckdb):
        pg = await both_engines.get_traffic_analytics(*W)
    monkeypatch.delenv("KS_READ_TRAFFIC", raising=False)

    async with both_engines.connection() as conn:
        cells = conn.execute(
            "SELECT platform, traffic_type, SUM(orders_count), SUM(revenue)"
            " FROM gold_daily_traffic WHERE date >= ? AND date <= ?"
            " GROUP BY platform, traffic_type", list(W)).fetchall()

    from_gold = {}
    for platform, ttype, orders, revenue in cells:
        if platform == "google":
            platform = ("google_ads" if ttype in ("paid_confirmed", "paid_likely")
                        else "google_organic")
        slot = from_gold.setdefault(platform, {"orders": 0, "revenue": 0.0})
        slot["orders"] += orders
        slot["revenue"] += float(revenue)

    assert {k: {"orders": v["orders"], "revenue": round(v["revenue"], 2)}
            for k, v in from_gold.items()} == pg["by_platform"]


@pytest.mark.asyncio
async def test_an_order_with_no_utm_row_still_counts(both_engines, monkeypatch):
    """Order 3 has no row in `silver_order_utm` at all, and the LEFT JOIN
    renders it NULL. The COALESCE has to make it `organic`/`telegram` in both
    engines — an INNER JOIN anywhere here would drop it silently, and that is
    the commonest order in production."""
    duck, pg = await _both(both_engines, monkeypatch, "get_traffic_analytics", {})
    for out in (duck, pg):
        assert out["by_platform"]["telegram"]["orders"] == 1
        assert out["by_platform"]["telegram"]["revenue"] == 300.0
        assert out["summary"]["organic"]["orders"] == 1


@pytest.mark.asyncio
async def test_the_return_and_the_inactive_source_are_both_absent(
    both_engines, monkeypatch,
):
    """Orders 6 and 8 carry ₴1 300 between them and must reach no chart. The
    predicate that drops them used to live in the Gold's own WHERE; it lives
    in the query now, which is exactly the kind of move that loses a filter."""
    duck, pg = await _both(both_engines, monkeypatch, "get_traffic_analytics", {})
    for out in (duck, pg):
        assert out["totals"]["orders"] == 5
        assert out["totals"]["revenue"] == 4500.0


@pytest.mark.asyncio
async def test_the_blended_roas_is_not_doubled(both_engines, monkeypatch):
    """The trap the port nearly shipped: Postgres' revenue Gold holds a
    per-source row *and* a roll-up row, so `SUM(revenue)` without the
    predicate counts every order twice — and the ROAS would read exactly
    half, since spend is the divisor."""
    duck, pg = await _both(both_engines, monkeypatch, "get_traffic_roas", {})
    assert pg["blended"]["revenue"] == duck["blended"]["revenue"]
    assert pg["blended"]["spend"] == 750.0      # marketing, non-NULL platform
    assert pg["has_spend_data"] is True


@pytest.mark.asyncio
async def test_two_campaigns_tied_on_revenue_paginate_the_same_way(
    both_engines, monkeypatch,
):
    """Orders 5 and 7 are ₴700 each on different campaigns. Under a `LIMIT`
    with no tiebreaker the two engines can put a different one on page 1 —
    which is not a reordering, it is a campaign missing from the page."""
    first = await _both(both_engines, monkeypatch, "get_traffic_utm_campaigns",
                        {"limit": 2})
    second = await _both(both_engines, monkeypatch, "get_traffic_utm_campaigns",
                         {"limit": 2, "offset": 2})
    for duck, pg in (first, second):
        assert [c["campaign"] for c in pg["campaigns"]] == \
               [c["campaign"] for c in duck["campaigns"]]
