"""DN-20c: a consumer no HTTP request waits for answers a refusal itself.

A refusal is a 503 for every HTTP route (DN-20b). Everything else that can
receive one — derived, not remembered, by `test_read_fallback_consumers.py`
— gets an answer of its own here, run through the code the scheduler, the
boot and the assistant actually call:

- the weekly traffic report **defers**: no message, no ledger row, the
  reason and the surface in the job's result, and tomorrow's tick asks
  again;
- the Monday goals job defers **whole**: a refusal at any of its reads
  leaves `seasonal_indices` and `growth_metrics` as they were instead of
  half updated;
- the assistant's tools return a **named** `data_unavailable` result, which
  is what the model reads instead of numbers;
- the readers with no fallback — the weekly sales report, the training
  input, the search index, the boot — fail with their own error, as they
  always have, and each consumer contains it the way it did.

Each is shown with a Postgres pool that raises. A refusal reaches these
consumers from the routers that refuse — the traffic tab, `/goals` and its
forecast, `/expenses` — and that is what is held here. The goal tables are
Postgres', as production writes them.

Until step 14's S14-16 every test here also set `KS_READ_FALLBACK=off` and
held that nothing was counted as a read answered from DuckDB; nothing reads
that variable since, and nothing counts such a read. (S14-07 had already
taken the refusals that came from a switch naming Postgres with no
`KS_PG_DSN`.)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import read_fallback
from tests.unit.test_read_fallback_http import stores  # noqa: F401 — fixture
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)


def _refused(surface: str) -> dict:
    return {"reason": read_fallback.READ_UNAVAILABLE, "surface": surface}


async def _duck(tmp_path, name="consumers.duckdb"):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore()
    return store


def _ledger_answers(monkeypatch):
    """The report ledgers as production holds them: in Postgres (chains 11a
    and 11b, a constant since step 14's S14-00), asked before any read of the
    report's own. Here the week is unsent and a write is recorded, so the
    refusal under test is the report's read and "no ledger row" is "never
    marked"."""
    from core import report_ledger

    monkeypatch.setattr(report_ledger, "already_sent", AsyncMock(return_value=False))
    mark = AsyncMock(side_effect=AssertionError("the ledger was written"))
    monkeypatch.setattr(report_ledger, "mark_sent", mark)
    return mark


def _serve(monkeypatch, store) -> None:
    async def _get_store():
        return store

    monkeypatch.setattr("core.duckdb_store.get_store", _get_store)


# ─── The weekly sales report ──────────────────────────────────────────────

class TestWeeklyReport:
    """Its reads have no fallback — a Postgres failure raises, and the daily
    tick is the retry. What `KS_READ_FALLBACK=off` added was the switch with
    no address; it went with the switch (step 14, S14-07)."""

    async def _job(self, monkeypatch, tmp_path):
        """The job wired as the scheduler runs it, with nothing seeded: the
        pool raises at the first read, so a warehouse would never be read."""
        from core.scheduler import SCHEDULER_TIMEZONE
        from core.weekly_report import last_complete_week
        from tests.unit.test_weekly_report import TestSchedulerJob, _store

        harness = TestSchedulerJob()
        store = await _store(tmp_path)
        week_start, _end = last_complete_week(datetime.now(SCHEDULER_TIMEZONE).date())
        sent: list = []
        scheduler = harness._wire(monkeypatch, store, sent, tmp_path)
        return store, week_start, sent, scheduler

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_writes_no_ledger_row(
        self, stores, monkeypatch, tmp_path,
    ):
        """No fallback here to refuse: the pool's own error ends the tick,
        as it always has — at the ledger's gate, which asks Postgres first —
        and nothing is recorded for tomorrow. (Until step 14's S14-10 the
        DuckDB ledger was asked as well, and found empty.)"""
        from core import pg_weekly_ledger_write

        store, _week_start, sent, scheduler = await self._job(monkeypatch, tmp_path)
        marked = AsyncMock()
        monkeypatch.setattr(pg_weekly_ledger_write, "mark_sent", marked)
        monkeypatch.setenv("KS_READ_WEEKLY", "postgres")

        with pytest.raises(OSError, match="connection refused"):
            await scheduler._run_weekly_report()

        assert sent == []
        marked.assert_not_awaited()


# ─── The weekly traffic report ────────────────────────────────────────────

class TestTrafficReport:
    """Reads through the tab's own router, which refuses — so here the
    raising pool itself is what is refused."""

    ADMINS = [111, 222]

    def _wire(self, monkeypatch, store, tmp_path):
        import importlib

        from bot.store_sqlite import SqliteBotStore
        from core.scheduler import BackgroundScheduler

        monkeypatch.delenv("KS_TRAFFIC_REPORT_FIRST_WEEK", raising=False)
        monkeypatch.delenv("KS_TRAFFIC_REPORT_RECIPIENTS", raising=False)
        monkeypatch.setattr("bot.store_sqlite.DB_PATH", tmp_path / "prefs.db")
        bot_store = SqliteBotStore()
        bot_store.initialise()
        monkeypatch.setattr("core.bot_store._store", bot_store)
        monkeypatch.setattr(importlib.import_module("core.config"),
                            "ADMIN_USER_IDS", self.ADMINS)
        _serve(monkeypatch, store)

        sent: list = []

        async def _rich(html, media=None, chat_ids=None, **kwargs):
            sent.append(html)
            return len(chat_ids or [])

        async def _text(body, chat_ids=None, **kwargs):
            sent.append(body)
            return len(chat_ids or [])

        monkeypatch.setattr("core.telegram_alerts.send_rich_message_http", _rich)
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _text)
        return BackgroundScheduler(), sent

    @staticmethod
    def _the_warehouse_is_ready(monkeypatch):
        """The readiness gate answering, past the reported week: it reads
        Postgres' Gold since step 14's S14-07, which the raising pool would
        stop first — and its own failure is no refusal."""
        from core.scheduler import SCHEDULER_TIMEZONE
        from core.weekly_report import last_complete_week

        start, end = last_complete_week(datetime.now(SCHEDULER_TIMEZONE).date())
        monkeypatch.setattr("core.weekly_report.warehouse_max_date",
                            AsyncMock(return_value=end))
        return start

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_defers_with_no_ledger_row(
        self, stores, monkeypatch, tmp_path,
    ):
        store = await _duck(tmp_path)
        week_start = self._the_warehouse_is_ready(monkeypatch)
        scheduler, sent = self._wire(monkeypatch, store, tmp_path)
        marked = _ledger_answers(monkeypatch)
        monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")

        result = await scheduler._run_traffic_report()

        assert result == {"sent": False, "week": week_start.isoformat(),
                          **_refused("traffic")}
        assert sent == []
        marked.assert_not_awaited()
        assert read_fallback.refusals()["traffic"]["count"] >= 1

# ─── The Monday goals job ─────────────────────────────────────────────────

def _writer_recorded(monkeypatch) -> list:
    """Chain 7b-3's store of the goal tables, recorded: production's one
    writer of them (a constant since S14-00)."""
    from core import pg_forecast_write

    written = []

    async def _persist(*args, **kwargs):
        written.append(args)

    monkeypatch.setattr(pg_forecast_write, "persist_goal_tables", _persist)
    return written


class TestSeasonalityJob:
    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_defers_with_nothing_written(
        self, stores, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path, "goals.duckdb")
        _serve(monkeypatch, store)
        written = _writer_recorded(monkeypatch)

        result = await BackgroundScheduler()._run_seasonality_calc()

        assert result == {"skipped": True, **_refused("goals")}
        assert written == [], "a refused job wrote a goal table — half an update"


class TestSeasonalityJobRefusedAtEveryRead:
    """Every history read the job makes goes through the goal router, so a
    refusal can come at any of them — the suggestions, the indices, either
    YoY read, the bounds. Wherever it comes, the job defers whole and every
    table keeps last week's rows: the writer reads everything first (7b-1).
    Injected at each routed read in turn, counted from a clean run rather
    than listed, against a real Postgres holding the history and the goal
    tables (skipped without `KS_PG_DSN`). Mutation M9: persist the indices
    before computing the YoY, and a refusal at the YoY read leaves this
    week's indices beside last week's growth — named here."""

    @staticmethod
    async def _tables(pool):
        async with pool.acquire() as conn:
            out = {}
            for table in ("app.seasonal_indices", "app.growth_metrics",
                          "app.weekly_patterns"):
                rows = await conn.fetch(f"SELECT * FROM {table}")
                out[table] = sorted(repr(tuple(r)) for r in rows)
            return out

    @staticmethod
    async def _scale(pool, factor: float):
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE silver.orders SET grand_total = grand_total * $1 "
                "WHERE order_date >= '2025-06-01'", factor)

    @pytest.mark.asyncio
    async def test_a_refusal_at_any_read_writes_nothing(
        self, fresh_read_fallback, monkeypatch, tmp_path,
    ):
        from core.repositories.goals import GoalsMixin
        from core.scheduler import BackgroundScheduler
        from tests.integration.pg_tab_seed import (
            store_that_never_reaches_duckdb, tab_in_postgres,
        )
        from tests.unit.test_goals_off_duckdb_silver import _seed_history_pg

        dsn = os.getenv("KS_PG_DSN")
        if not dsn:
            pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
        async with tab_in_postgres(dsn, monkeypatch, _seed_history_pg) as pool:
            store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goals job")
            _serve(monkeypatch, store)
            monkeypatch.setattr("core.pg.require_revision", AsyncMock())
            first = await BackgroundScheduler()._run_seasonality_calc()
            assert first["retail_months"] == 12
            # A week later every number would move.
            await self._scale(pool, 1.5)
            before = await self._tables(pool)

            real = GoalsMixin._goals_run
            calls = {"n": 0, "fail_at": None}

            async def counted(self, sql, params=None):
                calls["n"] += 1
                if calls["n"] == calls["fail_at"]:
                    raise read_fallback.refuse("goals", OSError("postgres is gone"))
                return await real(self, sql, params)

            monkeypatch.setattr(GoalsMixin, "_goals_run", counted)
            await BackgroundScheduler()._run_seasonality_calc()
            total = calls["n"]
            assert await self._tables(pool) != before, "a clean run moved nothing"
            assert total >= 6, f"only {total} routed reads — the history is not routed"
            # Back to last week's, then refuse at each read in turn.
            await self._scale(pool, 1 / 1.5)
            await BackgroundScheduler()._run_seasonality_calc()
            await self._scale(pool, 1.5)
            before = await self._tables(pool)

            for fail_at in range(1, total + 1):
                calls.update(n=0, fail_at=fail_at)
                result = await BackgroundScheduler()._run_seasonality_calc()
                assert result == {"skipped": True, **_refused("goals")}, fail_at
                assert await self._tables(pool) == before, (
                    f"refused at routed read {fail_at} of {total}, and a goal "
                    f"table was written — half an update")


# ─── Training ─────────────────────────────────────────────────────────────

@pytest.fixture
def model(tmp_path, monkeypatch):
    """A previous model: a file on disk with a known body and mtime, and an
    object in memory, behind the service the job and the route both get."""
    from core import prediction_service as ps

    path = tmp_path / "revenue_model.joblib"
    path.write_bytes(b"the previous model")
    os.utime(path, (1_000_000_000, 1_000_000_000))
    monkeypatch.setattr(ps, "MODEL_PATH", path)

    service = ps.PredictionService()
    previous = object()
    service._model = previous
    monkeypatch.setattr(ps, "get_prediction_service", lambda: service)

    def unchanged() -> bool:
        stat = path.stat()
        return (path.read_bytes() == b"the previous model"
                and stat.st_mtime == 1_000_000_000
                and service._model is previous)

    return unchanged


class TestTraining:
    """The input gate has no fallback: a Postgres failure is `status: error`
    and the model stands, as before. What `KS_READ_FALLBACK=off` added was
    the switch with no address, where the job skipped by name and the manual
    trigger answered 503; it went with the switch (step 14, S14-07). The
    job's handler naming the refusal stays (S14-16), held by
    `test_read_fallback_consumers.py`'s `KEPT_ANSWERS`."""

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_is_an_error_and_keeps_the_model(
        self, stores, model, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        _serve(monkeypatch, store)

        result = await BackgroundScheduler()._run_revenue_prediction()

        assert result["status"] == "error"
        assert model()
        assert read_fallback.refusals() == {}


# ─── The assistant ────────────────────────────────────────────────────────

class TestAssistant:
    @staticmethod
    def _serve_tools(monkeypatch, store):
        async def _get_store():
            return store

        monkeypatch.setattr("core.chat_tools.get_store", _get_store)

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_is_a_named_tool_result(
        self, stores, monkeypatch, tmp_path,
    ):
        from core.chat_tools import DATA_UNAVAILABLE, execute_tool

        store = await _duck(tmp_path)
        self._serve_tools(monkeypatch, store)
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")

        result = await execute_tool("list_expenses", {})

        assert result["error"] == DATA_UNAVAILABLE
        assert result["surface"] == "expenses"
        assert "Do not estimate" in result["message"]
        assert "connection refused" not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_a_failure_is_the_error_it_always_was(
        self, stores, monkeypatch,
    ):
        """The assistant's own reads have no fallback: a Postgres failure is
        `{"error": <text>}` to the model, as before — and since step 14's
        S14-07, whatever `KS_READ_CHAT` says (and since S14-16 nothing reads
        `KS_READ_FALLBACK`, which this set to `duckdb`). Only a refusal is
        named."""
        from core.chat_tools import execute_tool

        result = await execute_tool("get_revenue_summary", {"period": "week"})

        assert result == {"error": "connection refused"}

    @pytest.mark.asyncio
    async def test_the_model_reads_the_named_result_in_the_conversation(
        self, stores, monkeypatch, tmp_path,
    ):
        """Through `ChatService.chat`, which `POST /api/chat` calls: the tool
        result the model is given is the named one."""
        from core.duckdb_store import DuckDBStore
        from web.services import chat_service as cs

        self._serve_tools(monkeypatch, DuckDBStore())

        class _LLM:
            is_available = True

            def __init__(self):
                self.calls = 0

            async def chat(self, messages, tools, max_tokens):
                self.calls += 1
                if self.calls == 1:
                    return {"content": "", "usage": {}, "tool_calls": [{
                        "id": "t1", "name": "list_expenses", "input": {}}]}
                return {"content": "Those numbers are unavailable.", "usage": {}}

        monkeypatch.setattr(cs, "get_llm_client", lambda: _LLM())
        # A tool whose router refuses: the expenses' (since step 14's S14-07
        # the revenue tools read Postgres with no fallback, and fail as they
        # always did).

        service = cs.ChatService()
        conv_id = service.create_conversation(user_id=1)
        answer = await service.chat(conv_id, "how was the week?", user_id=1)

        assert answer["content"] == "Those numbers are unavailable."
        results = [block for m in service.get_conversation(conv_id).messages
                   if isinstance(m["content"], list)
                   for block in m["content"] if block.get("type") == "tool_result"]
        assert len(results) == 1
        assert json.loads(results[0]["content"])["error"] == "data_unavailable"


# ─── The search index ─────────────────────────────────────────────────────

@pytest.fixture
def meili(monkeypatch):
    client = MagicMock()
    client.health_check = AsyncMock(return_value={"status": "available"})
    for name in ("index_buyers", "index_orders", "index_products"):
        setattr(client, name, AsyncMock(return_value=0))
    monkeypatch.setattr("core.meilisearch_client.get_meili_client", lambda: client)
    monkeypatch.setattr("core.sync_service.get_meili_client", lambda: client)
    return client


def _sync_service(monkeypatch, store):
    from core.sync_service import SyncService

    service = SyncService(store=store)
    monkeypatch.setattr("core.sync_service.get_sync_service",
                        AsyncMock(return_value=service))
    monkeypatch.setattr(store, "set_last_sync_time",
                        AsyncMock(wraps=store.set_last_sync_time))
    return service


class TestSearchIndex:
    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_skips_it_as_before(
        self, stores, meili, monkeypatch, tmp_path,
    ):
        """No fallback to refuse: the index's own handler skips the step,
        watermark held, exactly as it did."""
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        _sync_service(monkeypatch, store)

        result = await BackgroundScheduler()._run_meilisearch_sync()

        assert result == {"buyers": 0, "orders": 0, "products": 0}
        store.set_last_sync_time.assert_not_awaited()
        assert read_fallback.refusals() == {}


    @pytest.mark.asyncio
    async def test_the_boot_contains_it(
        self, stores, meili, monkeypatch, tmp_path, caplog,
    ):
        """Web's boot, which since S14-00 is always `KS_DUCKDB=off`: the
        orders held are asked of where they are written, never DuckDB's
        counts. The index's read fails with the pool, and the index's own
        handler contains it (since step 14's S14-07 no switch can make it a
        refusal)."""
        from core import sync_service as sync_mod

        store = await _duck(tmp_path)
        _serve(monkeypatch, store)
        monkeypatch.setattr(sync_mod, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(store, "orders_held", AsyncMock(return_value=1))
        # Chain 1's rebuild, Postgres's since S14-00; not this test's.
        monkeypatch.setattr(store, "refresh_sku_inventory_status",
                            AsyncMock(return_value=0))
        service = _sync_service(monkeypatch, store)
        service.incremental_sync = AsyncMock(return_value={})
        monkeypatch.setattr(sync_mod, "init_meilisearch", AsyncMock(return_value=True))

        with caplog.at_level(logging.WARNING):
            await sync_mod.init_and_sync()

        store.set_last_sync_time.assert_not_awaited()
        assert "Meilisearch sync error: connection refused" in caplog.text


# ─── The buyers step, inside the sync tick ────────────────────────────────

