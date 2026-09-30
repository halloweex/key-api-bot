"""DN-20c: under `KS_READ_FALLBACK=off`, a consumer no HTTP request waits for
answers a refusal itself.

DN-20b made a refusal a 503 for every HTTP route. Everything else that can
receive one — derived, not remembered, by `test_read_fallback_consumers.py`
— gets an answer of its own here, run through the code the scheduler, the
boot and the assistant actually call:

- the two weekly reports **defer**: no message, no ledger row, the reason
  and the surface in the job's result, and tomorrow's tick asks again;
- the Monday goals job defers **whole**: its one routed read is asked first,
  so a refusal leaves `seasonal_indices` and `growth_metrics` as they were
  instead of half updated;
- training is **skipped** and the previous model file is untouched — and
  `POST /api/revenue/forecast/train` answers 503 like every other route,
  where it used to answer 200 `"status": "error"`;
- the assistant's tools return a **named** `data_unavailable` result, which
  is what the model reads instead of numbers;
- the search index and the buyers step **skip their step for the tick**,
  watermark held, and the sync tick completes its other steps.

Each is shown with a Postgres pool that raises. Several of these consumers
never had a fallback — the weekly report, training input, the buyers step
and the index read Postgres or nothing — so the pool alone produces their own
pre-existing failure, not a refusal; for those, the refusal comes from a
switch naming Postgres with no `KS_PG_DSN` at all, the one route to DuckDB
nothing else marks. Under `duckdb`, the default, each behaves exactly as it
did before this change.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import read_fallback
from tests.unit.test_read_fallback_http import _mode, stores  # noqa: F401 — fixture
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)


def _refused(surface: str) -> dict:
    return {"reason": read_fallback.READ_UNAVAILABLE, "surface": surface}


def _unaddressed(monkeypatch, switch: str) -> None:
    """The switch names Postgres and this process has no address for it."""
    monkeypatch.setenv(switch, "postgres")
    monkeypatch.delenv("KS_PG_DSN", raising=False)


async def _duck(tmp_path, name="consumers.duckdb"):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / name)
    await store.connect()
    return store


def _serve(monkeypatch, store) -> None:
    async def _get_store():
        return store

    monkeypatch.setattr("core.duckdb_store.get_store", _get_store)


# ─── The weekly sales report ──────────────────────────────────────────────

class TestWeeklyReport:
    """Its reads have no fallback — a Postgres failure raises, and the daily
    tick is the retry. What `off` adds is the switch with no address."""

    async def _job(self, monkeypatch, tmp_path):
        from tests.unit.test_weekly_report import TestSchedulerJob, _store

        harness = TestSchedulerJob()
        store = await _store(tmp_path)
        week_start = await harness._seed(store, complete=True)
        sent: list = []
        scheduler = harness._wire(monkeypatch, store, sent, tmp_path)
        return store, week_start, sent, scheduler

    @staticmethod
    async def _ledger(store, week_start) -> bool:
        from core.weekly_report import already_sent

        async with store.connection() as conn:
            return already_sent(conn, week_start, "retail")

    @pytest.mark.asyncio
    async def test_under_off_an_unaddressed_switch_defers_with_no_ledger_row(
        self, stores, monkeypatch, tmp_path,
    ):
        store, week_start, sent, scheduler = await self._job(monkeypatch, tmp_path)
        try:
            _unaddressed(monkeypatch, "KS_READ_WEEKLY")
            _mode(monkeypatch, "off")

            result = await scheduler._run_weekly_report()

            assert result == {"sent": False, "week": week_start.isoformat(),
                              **_refused("weekly_report")}
            assert sent == []
            assert not await self._ledger(store, week_start)
            assert read_fallback.refusals()["weekly_report"]["count"] >= 1
            assert read_fallback.counts() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_writes_no_ledger_row(
        self, stores, monkeypatch, tmp_path,
    ):
        """No fallback here to refuse: the pool's own error ends the tick,
        as it always has, and the ledger is untouched for tomorrow."""
        store, week_start, sent, scheduler = await self._job(monkeypatch, tmp_path)
        try:
            monkeypatch.setenv("KS_READ_WEEKLY", "postgres")
            _mode(monkeypatch, "off")

            with pytest.raises(OSError, match="connection refused"):
                await scheduler._run_weekly_report()

            assert sent == []
            assert not await self._ledger(store, week_start)
            assert read_fallback.counts() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_switch_is_read_from_duckdb_and_sent(
        self, stores, monkeypatch, tmp_path,
    ):
        store, week_start, sent, scheduler = await self._job(monkeypatch, tmp_path)
        try:
            _unaddressed(monkeypatch, "KS_READ_WEEKLY")
            _mode(monkeypatch, "duckdb")

            result = await scheduler._run_weekly_report()

            assert result["sent"] is True and result["revenue"] == 35_000
            assert await self._ledger(store, week_start)
            assert read_fallback.refusals() == {}
        finally:
            await store.close()


# ─── The weekly traffic report ────────────────────────────────────────────

class TestTrafficReport:
    """Reads through the tab's own router, which falls back — so here the
    raising pool itself is what `off` refuses."""

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
    async def _ledger(store, week_start) -> bool:
        from core.traffic_report import already_sent

        async with store.connection() as conn:
            return already_sent(conn, week_start, "retail")

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_defers_with_no_ledger_row(
        self, stores, monkeypatch, tmp_path,
    ):
        from tests.unit.test_traffic_report import _seed_gold

        store = await _duck(tmp_path)
        try:
            week_start = await _seed_gold(store)
            scheduler, sent = self._wire(monkeypatch, store, tmp_path)
            monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
            _mode(monkeypatch, "off")

            result = await scheduler._run_traffic_report()

            assert result == {"sent": False, "week": week_start.isoformat(),
                              **_refused("traffic")}
            assert sent == []
            assert not await self._ledger(store, week_start)
            assert read_fallback.counts() == {}
            assert read_fallback.refusals()["traffic"]["count"] >= 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_off_the_readiness_gate_defers_it_too(
        self, stores, monkeypatch, tmp_path,
    ):
        """`warehouse_max_date` is the sales report's reader, shared — its
        refusal defers this report under the surface it was refused on."""
        from tests.unit.test_traffic_report import _seed_gold

        store = await _duck(tmp_path)
        try:
            week_start = await _seed_gold(store)
            scheduler, sent = self._wire(monkeypatch, store, tmp_path)
            _unaddressed(monkeypatch, "KS_READ_WEEKLY")
            _mode(monkeypatch, "off")

            result = await scheduler._run_traffic_report()

            assert result == {"sent": False, "week": week_start.isoformat(),
                              **_refused("weekly_report")}
            assert sent == []
            assert not await self._ledger(store, week_start)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_pool_falls_back_as_before(
        self, stores, monkeypatch, tmp_path,
    ):
        from tests.unit.test_traffic_report import _seed_gold

        store = await _duck(tmp_path)
        try:
            await _seed_gold(store)
            scheduler, _sent = self._wire(monkeypatch, store, tmp_path)
            monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
            _mode(monkeypatch, "duckdb")

            result = await scheduler._run_traffic_report()

            # DuckDB answered — an empty Silver, so no orders — and the
            # fallback was counted, exactly as before DN-20c.
            assert result["reason"] == "no_orders"
            assert read_fallback.counts()["traffic"]["count"] >= 1
            assert read_fallback.refusals() == {}
        finally:
            await store.close()


# ─── The Monday goals job ─────────────────────────────────────────────────

MARCH = date(2025, 3, 1)


async def _goals_store(tmp_path):
    """Twenty-five retail days in one month: enough for the seasonality pass
    to write a row, so "nothing written" is a claim about a table that would
    otherwise have one."""
    store = await _duck(tmp_path, "goals.duckdb")
    async with store.connection() as conn:
        for n in range(25):
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id) VALUES (?, 1, 1, 1000, ?, 9)",
                [n + 1, datetime(2025, 3, 1 + n, 12, tzinfo=timezone.utc)])
    return store


async def _goal_tables(store):
    async with store.connection() as conn:
        return (conn.execute("SELECT COUNT(*) FROM seasonal_indices").fetchone()[0],
                conn.execute("SELECT COUNT(*) FROM growth_metrics").fetchone()[0])


class TestSeasonalityJob:
    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_defers_with_nothing_written(
        self, stores, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _goals_store(tmp_path)
        try:
            _serve(monkeypatch, store)
            monkeypatch.setenv("KS_READ_GOALS", "postgres")
            _mode(monkeypatch, "off")

            result = await BackgroundScheduler()._run_seasonality_calc()

            assert result == {"skipped": True, **_refused("goals")}
            assert await _goal_tables(store) == (0, 0), (
                "a refused job wrote a goal table — half an update")
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_pool_falls_back_and_writes_both(
        self, stores, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _goals_store(tmp_path)
        try:
            _serve(monkeypatch, store)
            monkeypatch.setenv("KS_READ_GOALS", "postgres")
            _mode(monkeypatch, "duckdb")

            result = await BackgroundScheduler()._run_seasonality_calc()

            assert result["retail_months"] == 1
            assert set(result["retail_goals"]) == {"daily", "weekly", "monthly"}
            indices, growth = await _goal_tables(store)
            assert indices == 1 and growth == 1
            assert read_fallback.counts()["goals"]["count"] >= 1
        finally:
            await store.close()


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
    and the model stands, as before. What `off` adds is the switch with no
    address, and there the job now skips by name."""

    @pytest.mark.asyncio
    async def test_under_off_an_unaddressed_switch_skips_and_keeps_the_model(
        self, stores, model, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        try:
            _serve(monkeypatch, store)
            _unaddressed(monkeypatch, "KS_READ_FORECAST_INPUT")
            _mode(monkeypatch, "off")

            result = await BackgroundScheduler()._run_revenue_prediction()

            assert result == {"status": "skipped", **_refused("forecast")}
            assert model(), "the model file or the model in memory changed"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_is_an_error_and_keeps_the_model(
        self, stores, model, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        try:
            _serve(monkeypatch, store)
            monkeypatch.setenv("KS_READ_FORECAST_INPUT", "postgres")
            _mode(monkeypatch, "off")

            result = await BackgroundScheduler()._run_revenue_prediction()

            assert result["status"] == "error"
            assert model()
            assert read_fallback.refusals() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_switch_reads_duckdb(
        self, stores, model, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        try:
            _serve(monkeypatch, store)
            _unaddressed(monkeypatch, "KS_READ_FORECAST_INPUT")
            _mode(monkeypatch, "duckdb")

            result = await BackgroundScheduler()._run_revenue_prediction()

            # An empty DuckDB answered: too little to train on — not a skip.
            assert result["status"] == "insufficient_data"
            assert model()
            assert read_fallback.refusals() == {}
        finally:
            await store.close()

    def test_the_manual_trigger_is_a_503_like_any_route(
        self, stores, model, admin_client, monkeypatch,
    ):
        """It shares `train` with the job. Answered inside `_train_impl`, the
        refusal came back as 200 `"status": "error"`; let through, the
        route answers it as the dashboard's routes do."""
        _unaddressed(monkeypatch, "KS_READ_FORECAST_INPUT")
        _mode(monkeypatch, "off")

        response = admin_client.post("/api/revenue/forecast/train")

        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "forecast"
        assert model()

    @pytest.mark.asyncio
    async def test_the_boot_training_contains_it(
        self, stores, model, monkeypatch, tmp_path, caplog,
    ):
        import web.main as web_main

        store = await _duck(tmp_path)
        try:
            _serve(monkeypatch, store)
            monkeypatch.setattr("asyncio.sleep", AsyncMock())
            _unaddressed(monkeypatch, "KS_READ_FORECAST_INPUT")
            _mode(monkeypatch, "off")

            with caplog.at_level(logging.WARNING):
                await web_main._train_prediction_model()

            assert model()
            assert "Background prediction training failed" in caplog.text
        finally:
            await store.close()


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
        try:
            self._serve_tools(monkeypatch, store)
            monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
            _mode(monkeypatch, "off")

            result = await execute_tool("list_expenses", {})

            assert result["error"] == DATA_UNAVAILABLE
            assert result["surface"] == "expenses"
            assert "Do not estimate" in result["message"]
            assert "connection refused" not in json.dumps(result)
            assert read_fallback.counts() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_off_an_unaddressed_switch_is_the_same_result(
        self, stores, monkeypatch,
    ):
        from core.chat_tools import DATA_UNAVAILABLE, execute_tool

        _unaddressed(monkeypatch, "KS_READ_CHAT")
        _mode(monkeypatch, "off")

        result = await execute_tool("get_revenue_summary", {"period": "week"})

        assert result["error"] == DATA_UNAVAILABLE
        assert result["surface"] == "assistant"

    @pytest.mark.asyncio
    async def test_under_duckdb_a_failure_is_the_error_it_always_was(
        self, stores, monkeypatch,
    ):
        """The assistant's own reads have no fallback: a Postgres failure is
        `{"error": <text>}` to the model, as before. Only a refusal is
        named."""
        from core.chat_tools import execute_tool

        monkeypatch.setenv("KS_READ_CHAT", "postgres")
        _mode(monkeypatch, "duckdb")

        result = await execute_tool("get_revenue_summary", {"period": "week"})

        assert result == {"error": "connection refused"}

    @pytest.mark.asyncio
    async def test_the_model_reads_the_named_result_in_the_conversation(
        self, stores, monkeypatch,
    ):
        """Through `ChatService.chat`, which `POST /api/chat` calls: the tool
        result the model is given is the named one."""
        from web.services import chat_service as cs

        class _LLM:
            is_available = True

            def __init__(self):
                self.calls = 0

            async def chat(self, messages, tools, max_tokens):
                self.calls += 1
                if self.calls == 1:
                    return {"content": "", "usage": {}, "tool_calls": [{
                        "id": "t1", "name": "get_revenue_summary",
                        "input": {"period": "week"}}]}
                return {"content": "Those numbers are unavailable.", "usage": {}}

        monkeypatch.setattr(cs, "get_llm_client", lambda: _LLM())
        _unaddressed(monkeypatch, "KS_READ_CHAT")
        _mode(monkeypatch, "off")

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
    async def test_under_off_an_unaddressed_switch_skips_the_step(
        self, stores, meili, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        try:
            _sync_service(monkeypatch, store)
            _unaddressed(monkeypatch, "KS_READ_SEARCH_INDEX")
            _mode(monkeypatch, "off")

            result = await BackgroundScheduler()._run_meilisearch_sync()

            assert result == {"skipped": True, **_refused("search_index")}
            store.set_last_sync_time.assert_not_awaited()
            meili.index_buyers.assert_not_awaited()
            meili.index_orders.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_off_a_raising_pool_skips_it_as_before(
        self, stores, meili, monkeypatch, tmp_path,
    ):
        """No fallback to refuse: the index's own handler skips the step,
        watermark held, exactly as it did."""
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        try:
            _sync_service(monkeypatch, store)
            monkeypatch.setenv("KS_READ_SEARCH_INDEX", "postgres")
            _mode(monkeypatch, "off")

            result = await BackgroundScheduler()._run_meilisearch_sync()

            assert result == {"buyers": 0, "orders": 0, "products": 0}
            store.set_last_sync_time.assert_not_awaited()
            assert read_fallback.refusals() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_switch_indexes_from_duckdb(
        self, stores, meili, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler

        store = await _duck(tmp_path)
        try:
            _sync_service(monkeypatch, store)
            _unaddressed(monkeypatch, "KS_READ_SEARCH_INDEX")
            _mode(monkeypatch, "duckdb")

            result = await BackgroundScheduler()._run_meilisearch_sync()

            assert result == {"buyers": 0, "orders": 0, "products": 0}
            # A first sync of an empty DuckDB still stamps its watermark.
            store.set_last_sync_time.assert_awaited()
            assert read_fallback.refusals() == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_boot_contains_it(
        self, stores, meili, monkeypatch, tmp_path, caplog,
    ):
        from core import sync_service as sync_mod

        store = await _duck(tmp_path)
        try:
            _serve(monkeypatch, store)
            monkeypatch.setattr(sync_mod, "get_store", AsyncMock(return_value=store))
            monkeypatch.setattr(store, "get_stats",
                                AsyncMock(return_value={"orders": 1, "products": 0}))
            service = _sync_service(monkeypatch, store)
            service.incremental_sync = AsyncMock(return_value={})
            monkeypatch.setattr(sync_mod, "init_meilisearch", AsyncMock(return_value=True))
            _unaddressed(monkeypatch, "KS_READ_SEARCH_INDEX")
            _mode(monkeypatch, "off")

            with caplog.at_level(logging.WARNING):
                await sync_mod.init_and_sync()

            store.set_last_sync_time.assert_not_awaited()
            assert "Meilisearch initialization failed" in caplog.text
        finally:
            await store.close()


# ─── The buyers step, inside the sync tick ────────────────────────────────

class TestSyncTick:
    """The tick is every two minutes and its other steps must not pay for
    this one. The buyers step already contains every failure of its
    selection — the watermark held, the step skipped for the tick — and a
    refusal is one of them."""

    FRESH, STALE = timedelta(minutes=1), timedelta(hours=2)

    def _tick(self, monkeypatch, store):
        from core import sync_service as sync_mod
        from core.scheduler import BackgroundScheduler

        service = _sync_service(monkeypatch, store)
        service._should_skip_sync = lambda: (False, None)
        service._fetch_orders_with_date_filter = AsyncMock(return_value=[])
        service.sync_offers = AsyncMock(return_value=3)
        service.sync_stocks = AsyncMock(return_value=4)
        monkeypatch.setattr(sync_mod, "get_async_client", AsyncMock())

        now = datetime.now(timezone.utc)

        async def _last(kind):
            stale = kind in ("buyers", "offers", "stocks")
            return now - (self.STALE if stale else self.FRESH)

        monkeypatch.setattr(store, "get_last_sync_time", AsyncMock(side_effect=_last))
        for name in ("refresh_sku_inventory_status", "record_sku_inventory_snapshot",
                     "record_inventory_snapshot"):
            monkeypatch.setattr(store, name, AsyncMock(return_value=0))
        return BackgroundScheduler(), service

    @staticmethod
    def _stamped(store) -> set:
        return {c.args[0] for c in store.set_last_sync_time.await_args_list}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["no_address", "engine_fails"])
    async def test_under_off_the_tick_completes_its_other_steps(
        self, state, stores, monkeypatch, tmp_path,
    ):
        store = await _duck(tmp_path)
        try:
            scheduler, service = self._tick(monkeypatch, store)
            if state == "no_address":
                _unaddressed(monkeypatch, "KS_READ_BUYER_SYNC")
            else:
                monkeypatch.setenv("KS_READ_BUYER_SYNC", "postgres")
            _mode(monkeypatch, "off")

            stats = await scheduler._run_incremental_sync()

            assert stats["buyers"] == 0
            assert stats["offers"] == 3 and stats["stocks"] == 4
            service.sync_offers.assert_awaited_once()
            service.sync_stocks.assert_awaited_once()
            assert "buyers" not in self._stamped(store), (
                "the buyers watermark moved over a selection nobody read")
            assert read_fallback.counts() == {}
            if state == "no_address":
                assert read_fallback.refusals()["buyer_sync"]["count"] == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_under_duckdb_the_same_switch_selects_from_duckdb(
        self, stores, monkeypatch, tmp_path,
    ):
        store = await _duck(tmp_path)
        try:
            scheduler, _service = self._tick(monkeypatch, store)
            _unaddressed(monkeypatch, "KS_READ_BUYER_SYNC")
            _mode(monkeypatch, "duckdb")

            stats = await scheduler._run_incremental_sync()

            # DuckDB answered "nobody missing", a completed step, stamped.
            assert stats["buyers"] == 0 and stats["offers"] == 3
            assert "buyers" in self._stamped(store)
            assert read_fallback.refusals() == {}
        finally:
            await store.close()
