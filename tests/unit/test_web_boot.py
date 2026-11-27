"""web's start and its boot sync, when what they depend on is down.

web is the only process that syncs orders, so its start must not end on a
boot sync that failed: a KeyCRM or a Postgres down at boot is a crash loop
otherwise, and the scheduler — which retries the order step — never starts.
And the boot sync must pull the whole history into a store that holds none.

Both were held by the KS_DUCKDB switch's tests (`test_duckdb_switch.py`'s
`TestTheStartup`, `TestTheBootSync`) and the off sweep's boot case until step
14's S14-18 deleted them with the switch; nothing about them needed DuckDB.
Each guard names the mutation it catches.
"""
from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from tests.integration.pg_sms_seed import needs_postgres


def _start(monkeypatch, init_and_sync):
    """web's real `startup_event`, around the boot sync it is given; what it
    reaches beyond the sync recorded rather than run. Returns (main,
    restore)."""
    import web.main as main
    from core import store as store_module

    monkeypatch.setattr(main, "validate_config", MagicMock())
    monkeypatch.setattr(main, "init_database", MagicMock())
    monkeypatch.setattr(main, "start_scheduler", AsyncMock())
    monkeypatch.setattr(main, "_register_event_handlers", MagicMock())
    monkeypatch.setattr("core.prediction_service.get_prediction_service",
                        MagicMock(return_value=MagicMock(is_ready=True)))
    restore = AsyncMock(return_value=False)
    monkeypatch.setattr(store_module.AnalyticsStore, "backfill_sms_campaign_record",
                        restore)
    monkeypatch.setattr(main, "init_and_sync", init_and_sync)
    # web's logger does not propagate (core.observability), so it is read
    # where it is called.
    monkeypatch.setattr(main, "logger", MagicMock())

    async def run():
        try:
            await main.startup_event()
        finally:
            await store_module.close_store()

    asyncio.run(run())
    return main, restore


def _said(mock) -> str:
    return " ".join(str(c.args[0]) for c in mock.call_args_list)


class TestTheStartup:
    @pytest.mark.parametrize("failure", [OSError("KeyCRM is down"),
                                         RuntimeError("Postgres is down")],
                             ids=["keycrm", "postgres"])
    def test_a_failed_boot_sync_does_not_end_the_startup(self, monkeypatch, failure):
        """Mutation: let the boot sync's `except` raise, or skip the rest of
        the start after it."""
        boot_sync = AsyncMock(side_effect=failure)
        main, restore = _start(monkeypatch, boot_sync)
        boot_sync.assert_awaited_once_with(full_sync_days=730)
        main.start_scheduler.assert_awaited_once()
        main._register_event_handlers.assert_called_once()
        restore.assert_awaited_once()
        assert "Serving with the boot sync failed" in _said(main.logger.warning)
        assert "Boot sync failed on startup" in _said(main.logger.error)
        assert "Dashboard ready" in _said(main.logger.info)

    def test_a_boot_sync_that_succeeded_starts_as_ever(self, monkeypatch):
        main, restore = _start(monkeypatch, AsyncMock())
        main.start_scheduler.assert_awaited_once()
        restore.assert_awaited_once()
        assert "Boot sync done" in _said(main.logger.info)
        assert "Serving with the boot sync failed" not in _said(main.logger.warning)


KYIV = ZoneInfo("Europe/Kyiv")


@asynccontextmanager
async def _keycrm(monkeypatch):
    """KeyCRM faked at the HTTP transport under the real client: every list
    is empty, and orders are refused, which ends the sync once it has asked
    for them. Yields (path, query) of every request, in order."""
    import httpx

    from core import keycrm
    from core.resilience import CircuitBreaker

    asked = []

    def handler(request):
        asked.append((request.url.path, dict(request.url.params)))
        if request.url.path.rstrip("/").endswith("/order"):
            return httpx.Response(400, json={"message": "the test stops here"})
        return httpx.Response(200, json={"data": []})

    client = keycrm.KeyCRMClient(api_key="test-only")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(keycrm, "_client_instance", client)
    monkeypatch.setattr(keycrm, "_circuit_breaker",
                        CircuitBreaker(config=keycrm.CIRCUIT_BREAKER_CONFIG))
    try:
        yield asked
    finally:
        await client._client.aclose()


@asynccontextmanager
async def _orders_held(monkeypatch, watermark):
    """The test Postgres holding no orders (`watermark` None), or one order
    and the order watermark at `watermark`. The watermarks the sync stamps
    are put back as they were afterwards."""
    from tests.integration.pg_sms_seed import on_postgres

    async with on_postgres(monkeypatch, ("bronze.orders",), what="the boot sync") as pool:
        async with pool.acquire() as conn:
            kept = [tuple(r) for r in await conn.fetch(
                "SELECT key, value, updated_at FROM meta.chain_watermarks")]
            if watermark is not None:
                await conn.execute(
                    "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                    "ordered_at, created_at, updated_at) VALUES (1, 4, 1, 100, $1, $1, $1)",
                    watermark)
                await conn.execute(
                    "INSERT INTO meta.chain_watermarks (key, value) "
                    "VALUES ('last_sync_orders', $1) "
                    "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    watermark.isoformat())
        try:
            yield
        finally:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM meta.chain_watermarks")
                await conn.executemany(
                    "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                    "VALUES ($1, $2, $3)", kept)


def _first_order_window(asked):
    """The (from, to) of the first order request."""
    for path, query in asked:
        if path.rstrip("/").endswith("/order"):
            start, end = query["filter[created_between]"].split(", ")
            return start, end
    raise AssertionError(f"no order was asked for: {[path for path, _ in asked]}")


@needs_postgres
class TestTheBootSync:
    """A store that holds no orders is filled with the whole history: the
    boot asks KeyCRM for orders from `full_sync_days` back. A store that
    holds some asks only from its order watermark on.

    Over the test Postgres, with KeyCRM faked at its HTTP transport; what is
    asserted is the window the first order request asked for. The search
    index, the one other service the boot reaches, answers "not available"."""

    @pytest.fixture(autouse=True)
    def _no_search_index(self, monkeypatch):
        monkeypatch.setattr("core.sync_service.init_meilisearch",
                            AsyncMock(return_value=False))

    @pytest.mark.asyncio
    async def test_no_history_is_a_full_sync(self, monkeypatch):
        """Mutation: never take the full-sync branch, or not with the days given."""
        from core.exceptions import KeyCRMError
        from core.sync_service import init_and_sync

        async with _orders_held(monkeypatch, None), _keycrm(monkeypatch) as asked:
            days_back = {(datetime.now(KYIV) - timedelta(days=730)).date().isoformat()}
            with contextlib.suppress(KeyCRMError):
                await init_and_sync(full_sync_days=730)
            days_back.add((datetime.now(KYIV) - timedelta(days=730)).date().isoformat())

        start, _ = _first_order_window(asked)
        assert start in days_back

    @pytest.mark.asyncio
    async def test_history_held_is_an_incremental_sync(self, monkeypatch):
        """Mutation: always take the full-sync branch."""
        from core.exceptions import KeyCRMError
        from core.sync_service import init_and_sync

        watermark = (datetime.now(timezone.utc) - timedelta(days=2)).replace(microsecond=0)
        async with _orders_held(monkeypatch, watermark), _keycrm(monkeypatch) as asked:
            with contextlib.suppress(KeyCRMError):
                await init_and_sync(full_sync_days=730)

        start, _ = _first_order_window(asked)
        start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KYIV)
        assert watermark - timedelta(days=2) <= start <= watermark
