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
    async def _job(self, monkeypatch, tmp_path):
        from tests.unit.test_read_fallback_off_consumers import TestTrafficReport, _duck
        from tests.unit.test_traffic_report import _seed_gold

        store = await _duck(tmp_path)
        week_start = await _seed_gold(store)
        scheduler, sent = TestTrafficReport()._wire(monkeypatch, store, tmp_path)
        return store, week_start, scheduler, sent

    @staticmethod
    async def _ledger(store, week_start) -> bool:
        from core.traffic_report import already_sent

        async with store.connection() as conn:
            return already_sent(conn, week_start, "retail")

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
        pool, _get_pool = live
        store, week_start, scheduler, sent = await self._job(monkeypatch, tmp_path)
        try:
            await pool.close()

            result = await scheduler._run_traffic_report()

            assert result == {"sent": False, "week": week_start.isoformat(),
                              **_refused("traffic")}
            assert sent == []
            assert not await self._ledger(store, week_start)
            assert read_fallback.counts() == {}
        finally:
            await store.close()


class TestTheGoalsJob:
    @pytest.mark.asyncio
    async def test_a_working_postgres_is_read_and_both_tables_are_written(
        self, live, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler
        from tests.unit.test_read_fallback_off_consumers import (
            _goal_tables,
            _goals_store,
            _serve,
        )

        _pool, get_pool = live
        store = await _goals_store(tmp_path)
        try:
            _serve(monkeypatch, store)
            result = await BackgroundScheduler()._run_seasonality_calc()

            assert get_pool.await_count > 0
            assert "skipped" not in result, result
            assert await _goal_tables(store) == (1, 1)
            assert read_fallback.refusals() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_outage_defers_it_with_nothing_written(
        self, live, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler
        from tests.unit.test_read_fallback_off_consumers import (
            _goal_tables,
            _goals_store,
            _serve,
        )

        pool, _get_pool = live
        store = await _goals_store(tmp_path)
        try:
            _serve(monkeypatch, store)
            await pool.close()

            result = await BackgroundScheduler()._run_seasonality_calc()

            assert result == {"skipped": True, **_refused("goals")}
            assert await _goal_tables(store) == (0, 0)
        finally:
            await store.close()


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
