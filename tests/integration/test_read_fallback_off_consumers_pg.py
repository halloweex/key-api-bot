"""DN-20c against a real PostgreSQL: a consumer under `off` reads a working
Postgres, and answers a real outage itself.

`tests/unit/test_read_fallback_off_consumers.py` shows each answer with a
pool that raises on first touch. What it cannot show is the half that would
cost the business: that under `KS_READ_FALLBACK=off` the Monday traffic
report, the goals job and the assistant simply *read* a Postgres that
answers. An answer wired one step too early — a routing decision or an empty
week taken for a refusal — would pass every unit test and stop both reports
the day `off` is set, with a deferral in the log that looks exactly like the
designed one.

So the three consumers whose reads go through a router that used to fall
back are run here, under `off`, against a live Postgres at the current head:
the pool is reached and nothing is refused. Then the same pool is closed — an
outage asyncpg reports on its own — and each answers by name: the report
defers with no ledger row, the goals job defers with nothing written, the
tool returns `data_unavailable`. The consumers with no fallback at all
(the weekly report, training, the index, the buyers step) meet a real
outage exactly as before DN-20c and are not repeated here.
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import read_fallback

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

SWITCHES = {
    "KS_READ_TRAFFIC": "postgres",
    "KS_READ_GOALS": "postgres",
    "KS_READ_EXPENSES": "postgres",
}


@pytest_asyncio.fixture
async def live(monkeypatch):
    """Every read switch unset but the three consumers' routers, a live
    Postgres behind `core.pg.get_pool` that the test can count and close,
    and `off` read the way web's startup reads it."""
    from core import pg_expenses_read

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
    monkeypatch.setenv("KS_READ_FALLBACK", "off")
    read_fallback.configure_mode()

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    get_pool = AsyncMock(return_value=pool)
    try:
        with patch("core.pg.get_pool", new=get_pool):
            yield pool, get_pool
    finally:
        if not pool._closed:
            await pool.close()
        read_fallback._mode, read_fallback._mode_error = saved[0], saved[1]
        read_fallback._misconfigured = saved[2]
        read_fallback.reset_counts()


def _refused(surface: str) -> dict:
    return {"reason": read_fallback.READ_UNAVAILABLE, "surface": surface}


class TestTheTrafficReport:
    """The readiness gate reads Postgres' Gold since step 14's S14-07: seeded
    there for the working case, and answered for the outage, whose refusal
    under test is the tab's own read (the gate's failure is no refusal)."""

    @pytest_asyncio.fixture(autouse=True)
    async def _gold_left_as_found(self, live):
        pool, _get_pool = live
        await self._empty_gold(pool)
        yield
        if not pool._closed:
            await self._empty_gold(pool)

    @staticmethod
    async def _empty_gold(pool):
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE gold.daily_revenue")

    async def _job(self, monkeypatch, tmp_path, *, gate_answers=False):
        from tests.unit.test_read_fallback_off_consumers import TestTrafficReport, _duck
        from tests.unit.test_traffic_report import _seed_gold

        store = await _duck(tmp_path)
        if gate_answers:
            week_start = TestTrafficReport._the_warehouse_is_ready(monkeypatch)
        else:
            week_start = await _seed_gold(store)
        scheduler, sent = TestTrafficReport()._wire(monkeypatch, store, tmp_path)
        return store, week_start, scheduler, sent

    @pytest.mark.asyncio
    async def test_a_working_postgres_is_read_not_refused(
        self, live, monkeypatch, tmp_path,
    ):
        _pool, get_pool = live
        store, _week_start, scheduler, _sent = await self._job(monkeypatch, tmp_path)
        try:
            result = await scheduler._run_traffic_report()

            assert get_pool.await_count > 0, "the report never asked Postgres"
            assert result.get("reason") != read_fallback.READ_UNAVAILABLE, result
            assert read_fallback.refusals() == {}
            assert read_fallback.counts() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_outage_defers_it_with_no_ledger_row(
        self, live, monkeypatch, tmp_path,
    ):
        from tests.unit.test_read_fallback_off_consumers import _ledger_answers

        pool, _get_pool = live
        store, week_start, scheduler, sent = await self._job(
            monkeypatch, tmp_path, gate_answers=True)
        try:
            # The ledger is chain 11b's, in Postgres since step 14's S14-00,
            # and asked first: answered here, so the refusal under test is the
            # report's own read rather than the ledger's.
            marked = _ledger_answers(monkeypatch)
            await pool.close()

            result = await scheduler._run_traffic_report()

            assert result == {"sent": False, "week": week_start.isoformat(),
                              **_refused("traffic")}
            assert sent == []
            marked.assert_not_awaited()
            assert read_fallback.counts() == {}
        finally:
            await store.close()


async def _twenty_five_march_days(conn, pool):
    """Twenty-five retail days in one month, landed in Bronze, Silver derived
    by Postgres: enough for the seasonality pass to write a row, so "both
    tables are written" is a claim about rows that would otherwise not be
    there."""
    from datetime import datetime, timezone

    from tests.integration.pg_tab_seed import derive, seed

    await seed(conn, orders=[
        (n + 1, 1, 1, 1000.0, datetime(2025, 3, 1 + n, 12, tzinfo=timezone.utc), 9,
         None, None) for n in range(25)])
    await derive(pool)


async def _goal_tables(pool):
    async with pool.acquire() as conn:
        return (await conn.fetchval("SELECT COUNT(*) FROM app.seasonal_indices"),
                await conn.fetchval("SELECT COUNT(*) FROM app.growth_metrics"))


class TestTheGoalsJob:
    """The history is Postgres' Silver and the goal tables are Postgres'
    (chain 7b-3, a constant since S14-00) — production's arrangement, which
    until step 14's S14-07 this held on DuckDB's goal tables."""

    @pytest.mark.asyncio
    async def test_a_working_postgres_is_read_and_both_tables_are_written(
        self, live, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler
        from tests.integration.pg_tab_seed import (
            store_that_never_reaches_duckdb, tab_in_postgres,
        )
        from tests.unit.test_read_fallback_off_consumers import _serve

        _pool, get_pool = live
        async with tab_in_postgres(DSN, monkeypatch, _twenty_five_march_days) as pool:
            # `tab_in_postgres` hands `core.pg.get_pool` its own pool; the
            # count is asked of the live fixture's, so give that one back.
            get_pool.return_value = pool
            with patch("core.pg.get_pool", new=get_pool):
                store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goals job")
                _serve(monkeypatch, store)
                result = await BackgroundScheduler()._run_seasonality_calc()

                assert get_pool.await_count > 0
                assert "skipped" not in result, result
                assert result["retail_months"] == 1, result
                assert await _goal_tables(pool) == (1, 1)
                assert read_fallback.refusals() == {}

    @pytest.mark.asyncio
    async def test_an_outage_defers_it_with_nothing_written(
        self, live, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler
        from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb
        from tests.unit.test_read_fallback_off_consumers import (
            _serve,
            _writer_recorded,
        )

        pool, _get_pool = live
        store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goals job")
        _serve(monkeypatch, store)
        written = _writer_recorded(monkeypatch)
        await pool.close()

        result = await BackgroundScheduler()._run_seasonality_calc()

        assert result == {"skipped": True, **_refused("goals")}
        assert written == []


class TestTheAssistant:
    @pytest.mark.asyncio
    async def test_a_working_postgres_answers_the_tool(self, live, tmp_path, monkeypatch):
        from core.chat_tools import execute_tool
        from tests.unit.test_read_fallback_off_consumers import TestAssistant, _duck

        _pool, get_pool = live
        store = await _duck(tmp_path)
        try:
            TestAssistant._serve_tools(monkeypatch, store)
            result = await execute_tool("list_expenses", {})

            assert get_pool.await_count > 0
            assert "error" not in result, result
            assert read_fallback.refusals() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_outage_is_the_named_result(self, live, tmp_path, monkeypatch):
        from core.chat_tools import DATA_UNAVAILABLE, execute_tool
        from tests.unit.test_read_fallback_off_consumers import TestAssistant, _duck

        pool, _get_pool = live
        store = await _duck(tmp_path)
        try:
            TestAssistant._serve_tools(monkeypatch, store)
            await pool.close()

            result = await execute_tool("list_expenses", {})

            assert result["error"] == DATA_UNAVAILABLE
            assert result["surface"] == "expenses"
            assert read_fallback.counts() == {}
        finally:
            await store.close()
