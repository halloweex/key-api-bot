"""DN-20b against a real PostgreSQL: `off` refuses a failure, never a working read.

`tests/unit/test_read_fallback_http.py` proves the refusal with a pool that
raises on first touch. What it cannot prove is the other half, and it is the
half that would take the dashboard down: that under `KS_READ_FALLBACK=off` a
Postgres that answers is simply read. A refusal wired one step too early — a
benign "no rows" or a routing answer taken for a failure — would pass every
unit test and turn every tab into a 503 the day `off` is set.

So each router DN-20b covers is asked here, under `off`, of a live Postgres at
the current head: it answers, nothing is refused, nothing falls back, and the
pool is actually reached. The dashboard's summary is seeded differently in
the two stores, so the number itself says which one answered. Then the same
pool is closed — an outage asyncpg reports on its own, not a mock's
exception — and each router refuses, naming its surface; under `duckdb` the
same outage is answered by DuckDB, as before this change.

Cohorts are ClickHouse's and CI has no ClickHouse; their refusal is unit-tested.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import read_fallback

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# A day nobody else's fixture writes, so the seed and its cleanup touch
# nothing but themselves.
DAY = date(2001, 2, 3)
START = DAY - timedelta(days=6)
PG_REVENUE, DUCK_REVENUE = 777.0, 111.0

SWITCHES = {
    "KS_READ_GOLD": "postgres",
    "KS_READ_GOALS": "postgres",
    "KS_READ_TRAFFIC": "postgres",
    "KS_READ_EXPENSES": "postgres",
    "KS_READ_MARGIN": "postgres",
    "KS_READ_PRODUCTS_INTEL": "postgres",
    "KS_READ_INVENTORY": "postgres",
}

# (surface, method, args) — one read per router DN-20b covers.
READS = [
    ("dashboard", "get_summary_stats", (DAY, DAY)),
    ("dashboard", "get_revenue_trend", (START, DAY)),
    ("goals", "get_goals", ()),
    ("traffic", "get_traffic_analytics", (START, DAY)),
    ("expenses", "get_expense_summary", (START, DAY)),
    ("margin", "get_margin_overview", (START, DAY)),
    ("products_intel", "get_basket_summary", (START, DAY)),
    ("inventory", "get_stock_summary", ()),
]


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM gold.daily_revenue WHERE date = $1", DAY)
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A DuckDB and a live Postgres holding different revenue for one day,
    the expense history marked across (so the history gate asks Postgres
    rather than answering "not yet"), every covered switch at `postgres`,
    and a pool handed out through `core.pg.get_pool` that the test can
    count and close."""
    from core import pg_expenses_read
    from core.duckdb_store import DuckDBStore

    for name in [n for n in os.environ if n.startswith("KS_READ_")]:
        monkeypatch.delenv(name, raising=False)
    for name in ("KS_WRITE_GOALS", "KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    for name, value in SWITCHES.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)

    saved = (read_fallback._mode, read_fallback._mode_error,
             list(read_fallback._misconfigured))
    read_fallback.reset_counts()

    store = DuckDBStore(db_path=tmp_path / "off.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO gold_daily_revenue (date, sales_type, revenue, "
            "orders_count) VALUES (?, 'retail', ?, 1)", [DAY, DUCK_REVENUE])

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue,
                orders_count, unique_customers, new_customers,
                returning_customers, returns_count, returns_revenue,
                avg_order_value)
            VALUES ($1, 'retail', NULL, $2, 3, 3, 1, 2, 0, 0, 259)
            """, DAY, PG_REVENUE)
        await conn.execute(
            """
            INSERT INTO meta.mirror_state (table_name, last_ok_at, backfilled_at)
            VALUES ('bronze.expenses', now(), now())
            """)
    get_pool = AsyncMock(return_value=pool)
    try:
        with patch("core.pg.get_pool", new=get_pool):
            yield store, pool, get_pool, monkeypatch
    finally:
        cleanup = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
        await _clean(cleanup)
        await cleanup.close()
        if not pool._closed:
            await pool.close()
        await store.close()
        read_fallback._mode, read_fallback._mode_error = saved[0], saved[1]
        read_fallback._misconfigured = saved[2]
        read_fallback.reset_counts()


def _mode(monkeypatch, value: str) -> None:
    monkeypatch.setenv("KS_READ_FALLBACK", value)
    read_fallback.configure_mode()


class TestOffReadsAWorkingPostgres:
    @pytest.mark.asyncio
    async def test_every_covered_router_answers_and_nothing_is_refused(self, stores):
        store, _pool, get_pool, env = stores
        _mode(env, "off")

        for surface, method, args in READS:
            before = get_pool.await_count
            await getattr(store, method)(*args)
            assert get_pool.await_count > before, (
                f"{method} never asked Postgres — the test would prove nothing")
            assert read_fallback.refusals() == {}, f"{method} refused a working read"
            assert read_fallback.counts() == {}, f"{method} fell back to DuckDB"

    @pytest.mark.asyncio
    async def test_the_number_is_postgres_s(self, stores):
        store, _pool, _get_pool, env = stores
        _mode(env, "off")
        summary = await store.get_summary_stats(DAY, DAY)
        assert summary["totalRevenue"] == PG_REVENUE
        assert summary["totalOrders"] == 3


class TestAnOutage:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("surface,method,args", READS,
                             ids=[m for _s, m, _a in READS])
    async def test_under_off_it_is_refused_naming_the_surface(
        self, stores, surface, method, args,
    ):
        store, pool, _get_pool, env = stores
        _mode(env, "off")
        await pool.close()

        with pytest.raises(read_fallback.ReadUnavailable) as refused:
            await getattr(store, method)(*args)
        assert refused.value.surface == surface
        assert read_fallback.counts() == {}
        assert read_fallback.refusals()[surface]["count"] >= 1

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_outage_is_answered_by_duckdb(self, stores):
        store, pool, _get_pool, env = stores
        _mode(env, "duckdb")
        await pool.close()

        summary = await store.get_summary_stats(DAY, DAY)
        assert summary["totalRevenue"] == DUCK_REVENUE
        assert read_fallback.counts()["dashboard"]["count"] == 1
        assert read_fallback.refusals() == {}
