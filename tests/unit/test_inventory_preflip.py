"""Chain 1's pre-flip hardening (DN-24), the parts that need no database.

Chain 1 was flipped on 2026-09-30, and since step 14's S14-00 it writes
Postgres whatever its flag says. S14-11 deleted its preflight — the questions
asked before `KS_WRITE_INVENTORY` was switched on, which answered "already
writes Postgres" since S14-00 — with the flag, and the health block's
`preflight` with it. The chain lock and the Postgres step path stay. The same
properties against a real Postgres are in
`tests/integration/test_inventory_preflip.py`.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core import pg_inventory_write
from core import sync_service as sync_mod
from core.exceptions import KeyCRMConnectionError
from core.sync_service import SyncService

# The tables whose rebuild or daily photograph two callers can race on. The
# stock upsert and the offer upsert are not here: both run only in the stock
# step, under the scheduler's heavy lock, and nothing else calls them.
_REBUILT_OR_PHOTOGRAPHED = (
    "app.sku_inventory_status",
    "app.inventory_sku_history",
    "app.inventory_history",
)


def _module_tree() -> ast.Module:
    return ast.parse(pathlib.Path(inspect.getfile(pg_inventory_write)).read_text(
        encoding="utf-8"))


def _writes_one_of_them(node: ast.AsyncFunctionDef) -> bool:
    """Whether the function writes a rebuilt or photographed table: a literal
    INSERT or DELETE naming it, or the shared rebuild body, which is composed
    elsewhere and never appears here as text."""
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            text = n.value.strip().upper()
            if text.startswith(("INSERT", "DELETE")) and any(
                    t.upper() in text for t in _REBUILT_OR_PHOTOGRAPHED):
                return True
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "sku_status_rebuild_select":
            return True
    return False


def _transactions(node: ast.AsyncFunctionDef):
    """Every `async with <conn>.transaction():` body in the function."""
    for n in ast.walk(node):
        if isinstance(n, ast.AsyncWith) and any(
                isinstance(item.context_expr, ast.Call)
                and getattr(item.context_expr.func, "attr", "") == "transaction"
                for item in n.items):
            yield n


def _first_await(body) -> str:
    first = body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Await):
        call = first.value.value
        if isinstance(call, ast.Call):
            return getattr(call.func, "id", getattr(call.func, "attr", ""))
    return ""


def _rebuilders() -> dict:
    return {node.name: node for node in _module_tree().body
            if isinstance(node, ast.AsyncFunctionDef) and _writes_one_of_them(node)}


class TestEveryRebuildAndSnapshotTakesTheChainLock:
    def test_the_walk_finds_the_three_that_exist(self):
        # Not vacuous: a walk that found nothing would pass the test below.
        assert set(_rebuilders()) == {
            "rebuild_sku_inventory_status",
            "record_sku_inventory_snapshot",
            "record_inventory_snapshot",
        }

    def test_the_lock_is_the_first_statement_of_each_transaction(self):
        """First, before the owner rows and before any read: the second of two
        callers must read what the first committed. Taken after the
        `first_seen_at` carry-forward, the rebuild would still copy a state
        the other one was about to replace."""
        for name, node in _rebuilders().items():
            blocks = list(_transactions(node))
            assert blocks, f"{name} writes outside a transaction"
            for block in blocks:
                assert _first_await(block.body) == "_chain_lock", (
                    f"{name} does not take the chain-1 advisory lock first")

    def test_it_is_a_transaction_lock_on_the_chains_own_key(self):
        """`_xact_`, so COMMIT or ROLLBACK releases it and a writer that dies
        cannot leave it held; and a bigint Postgres accepts."""
        source = inspect.getsource(pg_inventory_write._chain_lock)
        assert "pg_advisory_xact_lock(" in source and "CHAIN_LOCK_KEY" in source
        assert 0 < pg_inventory_write.CHAIN_LOCK_KEY < 2 ** 63
        assert pg_inventory_write.CHAIN_LOCK_KEY & 0xFFFF_FFFF == 1   # chain 1


# ─── /api/health publishes it ────────────────────────────────────────────────

@pytest.fixture
def health():
    """The health module, for its `write_chains` block."""
    from web.routes.api import health as module

    return module, []


class TestHealthPublishesNoPreflight:
    @pytest.mark.asyncio
    async def test_no_entry_carries_one(self, health, monkeypatch):
        """S14-11: the preflight went with the flag. Mutation: put it back
        under chain 1's (or chain 5's) entry."""
        module, _ = health
        monkeypatch.setattr(sync_mod, "get_sync_service",
                            AsyncMock(side_effect=RuntimeError("no store")))
        block = await module._write_chains_block()
        assert not [name for name, entry in block.items() if "preflight" in entry]
        assert not hasattr(pg_inventory_write, "preflight")


# ─── The stock step cannot take the tick down (on the Postgres path) ─────────

STALE, FRESH = timedelta(hours=2), timedelta(minutes=1)


def _tick_service(monkeypatch, *, offers_due=True, stocks_due=True):
    """A SyncService whose tick reaches the inventory steps and nothing else:
    no orders in the window, and every other hourly/daily branch fresh.
    `calls` records the order the store is written in."""
    calls = []
    now = datetime.now(timezone.utc)
    due = {"offers": offers_due, "stocks": stocks_due}

    async def last_sync(kind):
        return now - (STALE if due.get(kind) else FRESH)

    def recorder(name, result=None):
        async def fn(*args, **kwargs):
            calls.append((name, *args))
            return result
        return AsyncMock(side_effect=fn)

    store = SimpleNamespace(
        get_last_sync_time=AsyncMock(side_effect=last_sync),
        set_last_sync_time=recorder("set_last_sync_time"),
        upsert_offers=recorder("upsert_offers", 2),
        upsert_stocks=recorder("upsert_stocks", 3),
        refresh_sku_inventory_status=recorder("refresh_sku_inventory_status", 3),
        record_sku_inventory_snapshot=recorder("record_sku_inventory_snapshot", True),
        record_inventory_snapshot=recorder("record_inventory_snapshot", True),
        mark_warehouse_dirty=AsyncMock(),
    )
    client = SimpleNamespace(
        fetch_all_offers=AsyncMock(return_value=[{"id": 1}, {"id": 2}]),
        fetch_all_stocks=AsyncMock(return_value=[{"id": 1}, {"id": 2}, {"id": 3}]),
    )
    monkeypatch.setattr(sync_mod, "get_async_client", AsyncMock(return_value=client))
    service = SyncService(store=store)
    service._should_skip_sync = lambda: (False, "")
    service._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    return service, store, client, calls


@pytest.fixture
def on_postgres(monkeypatch):
    monkeypatch.setenv("KS_WRITE_INVENTORY", "postgres")
    return monkeypatch


def _stock_watermark_moved(calls) -> bool:
    return ("set_last_sync_time", "stocks") in calls


class TestTheStockStepOnPostgres:
    @pytest.mark.asyncio
    async def test_a_raising_rebuild_is_recorded_and_the_tick_returns(self, on_postgres):
        """The missing-TEMPORARY case: the upsert committed, the rebuild
        raised. Before DN-24 that left `incremental_sync` altogether."""
        service, store, _client, calls = _tick_service(on_postgres)
        store.refresh_sku_inventory_status.side_effect = RuntimeError(
            "the /inventory status rebuild needs one temporary table")

        stats = await service.incremental_sync()

        assert "skipped" not in stats and stats["stocks"] == 3
        assert not _stock_watermark_moved(calls), "last_sync_stocks moved past a failed rebuild"
        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 1 and step["last_failed_step"] == "stocks"
        assert step["last_error"] == "RuntimeError" and step["last_failure_at"]
        assert 0 < step["retry_in_s"] <= sync_mod.INVENTORY_RETRY_AFTER_S
        assert service._last_sync_time is not None, "the tick ended before the backoff"
        store.record_sku_inventory_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_raising_upsert_is_recorded_the_same_way(self, on_postgres):
        service, store, _client, calls = _tick_service(on_postgres)
        store.upsert_stocks.side_effect = OSError("connection refused")

        await service.incremental_sync()

        assert not _stock_watermark_moved(calls)
        assert service.inventory_step_health()["last_error"] == "OSError"
        store.refresh_sku_inventory_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_watermark_moves_last(self, on_postgres):
        service, _store, _client, calls = _tick_service(on_postgres)

        stats = await service.incremental_sync()

        assert [c[0] for c in calls] == [
            "upsert_offers", "set_last_sync_time",                    # offers, as before
            "upsert_stocks", "refresh_sku_inventory_status",
            "record_sku_inventory_snapshot", "record_inventory_snapshot",
            "set_last_sync_time"]
        assert calls[1] == ("set_last_sync_time", "offers")
        assert calls[-1] == ("set_last_sync_time", "stocks")
        assert (stats["offers"], stats["stocks"]) == (2, 3)
        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 0 and step["last_ok_at"] and step["retry_in_s"] is None

    @pytest.mark.asyncio
    async def test_a_success_clears_the_count(self, on_postgres):
        service, store, _client, _calls = _tick_service(on_postgres)
        store.record_inventory_snapshot.side_effect = [OSError("gone"), True]
        await service.incremental_sync()
        assert service.inventory_step_health()["failures_since_ok"] == 1

        service._inventory_retry_at = 0.0                         # the hold has passed
        await service.incremental_sync()
        assert service.inventory_step_health()["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_the_next_attempt_waits_for_the_hold(self, on_postgres):
        """With the watermark held the step is due again on the next tick; it
        must not fetch every stock from KeyCRM once a minute while it fails."""
        service, store, client, _calls = _tick_service(on_postgres)
        store.refresh_sku_inventory_status.side_effect = RuntimeError("no TEMPORARY")
        await service.incremental_sync()
        await service.incremental_sync()
        assert client.fetch_all_stocks.await_count == 1
        asked = [c.args for c in store.get_last_sync_time.await_args_list]
        assert asked.count(("stocks",)) == 1, "the held step read its watermark again"

        service._inventory_retry_at = 0.0
        await service.incremental_sync()
        assert client.fetch_all_stocks.await_count == 2

    @pytest.mark.asyncio
    async def test_an_unreadable_watermark_is_recorded_too(self, on_postgres):
        """Under this flag the watermarks live in `meta.chain_watermarks`, so
        a Postgres that is down fails the read before any write."""
        service, store, client, _calls = _tick_service(on_postgres)
        real = store.get_last_sync_time.side_effect

        async def last_sync(kind):
            if kind == "offers":
                raise OSError("connection refused")
            return await real(kind)

        store.get_last_sync_time.side_effect = last_sync
        await service.incremental_sync()
        assert service.inventory_step_health()["last_failed_step"] == "offers"
        client.fetch_all_stocks.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_offers_step_skips_the_stocks_step(self, on_postgres):
        """Stocks read the offer→product map: written ahead of their offers,
        the new SKUs' movements would be recorded with no product."""
        service, store, client, calls = _tick_service(on_postgres)
        store.upsert_offers.side_effect = OSError("connection refused")

        await service.incremental_sync()

        assert service.inventory_step_health()["last_failed_step"] == "offers"
        client.fetch_all_stocks.assert_not_awaited()
        assert calls == []

    @pytest.mark.asyncio
    async def test_a_keycrm_failure_rebuilds_but_holds_the_watermark(self, on_postgres):
        """As on the DuckDB path: nothing fetched is nothing to stamp, and the
        rebuild and the snapshots still run on the stock already held."""
        service, store, client, calls = _tick_service(on_postgres, offers_due=False)
        client.fetch_all_stocks.side_effect = KeyCRMConnectionError("timeout")

        stats = await service.incremental_sync()

        assert stats["stocks"] == 0 and not _stock_watermark_moved(calls)
        store.record_inventory_snapshot.assert_awaited_once()
        # A fetch that never arrived is not a success (L1-14): recorded under
        # the stocks step, and with no hold — the fetch is retried next tick.
        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 1 and step["last_ok_at"] is None
        assert step["last_failed_step"] == "stocks"
        assert step["last_error"] == "KeyCRMConnectionError"
        assert step["retry_in_s"] is None

    @pytest.mark.asyncio
    async def test_nothing_due_is_not_a_success(self, on_postgres):
        service, _store, _client, calls = _tick_service(
            on_postgres, offers_due=False, stocks_due=False)
        await service.incremental_sync()
        assert calls == [] and service.inventory_step_health()["last_ok_at"] is None


class TestHealthPublishesTheSyncStep:
    @pytest.mark.asyncio
    async def test_it_sits_under_chain_1s_entry(self, health, monkeypatch):
        module, _ = health
        state = sync_mod.InventoryStepState()
        state.failed("stocks", RuntimeError("the /inventory status rebuild needs one temporary table"))
        service = SimpleNamespace(inventory_step_health=lambda: state.published(599))
        monkeypatch.setattr(sync_mod, "get_sync_service", AsyncMock(return_value=service))

        step = (await module._write_chains_block())["pg_inventory_write"]["sync_step"]

        assert step["failures_since_ok"] == 1 and step["last_failed_step"] == "stocks"
        assert step["last_error"] == "RuntimeError" and step["retry_in_s"] == 599
        assert "temporary" not in str(step)                   # the class, never the text

    @pytest.mark.asyncio
    async def test_no_sync_service_is_null_not_empty(self, health, monkeypatch):
        module, _ = health
        monkeypatch.setattr(sync_mod, "get_sync_service",
                            AsyncMock(side_effect=RuntimeError("no store")))
        assert (await module._write_chains_block())["pg_inventory_write"]["sync_step"] is None
