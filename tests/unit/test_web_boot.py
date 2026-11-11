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
from unittest.mock import AsyncMock, MagicMock

import pytest


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


class TestTheBootSync:
    """`init_and_sync` asks the store the orders are written to whether it
    holds any; none is the 730-day full sync, any is the incremental one."""

    def _boot(self, monkeypatch, held):
        from core import sync_service as mod

        store = MagicMock()
        store.orders_held = AsyncMock(return_value=held)
        store.refresh_sku_inventory_status = AsyncMock(return_value=0)
        service = MagicMock()
        service.full_sync = AsyncMock()
        service.incremental_sync = AsyncMock()
        monkeypatch.setattr(mod, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(mod, "get_sync_service", AsyncMock(return_value=service))
        monkeypatch.setattr(mod, "init_meilisearch", AsyncMock(return_value=False))
        asyncio.run(mod.init_and_sync(full_sync_days=730))
        store.orders_held.assert_awaited_once()
        return service

    def test_no_history_is_a_full_sync(self, monkeypatch):
        """Mutation: never take the full-sync branch (`if held == 0`)."""
        service = self._boot(monkeypatch, held=0)
        service.full_sync.assert_awaited_once_with(days_back=730)
        service.incremental_sync.assert_not_awaited()

    def test_history_held_is_an_incremental_sync(self, monkeypatch):
        """Mutation: always take the full-sync branch."""
        service = self._boot(monkeypatch, held=46_000)
        service.incremental_sync.assert_awaited_once()
        service.full_sync.assert_not_awaited()
