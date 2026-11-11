"""A KeyCRM fetch that failed is not a success of the inventory step (L1-14).

`sync_offers` answered 0 when KeyCRM failed, so the Postgres step went on to
the stocks: written ahead of their offers, a new SKU's movement was recorded
with no product, and the step then published `last_ok_at` with
`failures_since_ok` 0 over an offers fetch that never arrived. Now the offers
answer None on a KeyCRM failure, the stocks are not asked, and the step
records the failure under the step KeyCRM failed — with no hold, since the
fetch is what is retried on the next tick, as it always was.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import core.sync_service as sync_mod
from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError
from core.sync_service import SyncService


def _service(monkeypatch, client):
    stale = datetime.now(timezone.utc) - timedelta(hours=2)
    store = SimpleNamespace(
        get_last_sync_time=AsyncMock(return_value=stale),
        set_last_sync_time=AsyncMock(),
        upsert_offers=AsyncMock(return_value=2),
        upsert_stocks=AsyncMock(return_value=3),
        refresh_sku_inventory_status=AsyncMock(),
        record_sku_inventory_snapshot=AsyncMock(),
        record_inventory_snapshot=AsyncMock(),
    )
    monkeypatch.setattr(sync_mod, "get_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(sync_mod.events, "emit", AsyncMock())
    return SyncService(store=store), store  # type: ignore[arg-type]


class TestAFailedOffersFetch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", [
        KeyCRMConnectionError("API returned 503"), KeyCRMAPIError("refused")])
    async def test_the_stocks_wait_and_the_step_says_it_failed(self, monkeypatch, error):
        """Mutation: answer 0 from `sync_offers` again — the stocks run and
        the step publishes a success."""
        client = SimpleNamespace(
            fetch_all_offers=AsyncMock(side_effect=error),
            fetch_all_stocks=AsyncMock(return_value=[{"id": 1, "quantity": 4}]),
        )
        service, store = _service(monkeypatch, client)
        stats = {"offers": 0, "stocks": 0}

        await service._inventory_step_postgres(stats)

        client.fetch_all_stocks.assert_not_awaited()
        store.upsert_stocks.assert_not_awaited()
        store.set_last_sync_time.assert_not_awaited()
        assert stats == {"offers": 0, "stocks": 0}
        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 1 and step["last_ok_at"] is None
        assert step["last_failed_step"] == "offers"
        assert step["last_error"] == type(error).__name__
        assert step["retry_in_s"] is None, "a KeyCRM failure holds nothing"

    @pytest.mark.asyncio
    async def test_a_later_good_tick_clears_it(self, monkeypatch):
        client = SimpleNamespace(
            fetch_all_offers=AsyncMock(side_effect=[KeyCRMConnectionError("503"), [{"id": 1}]]),
            fetch_all_stocks=AsyncMock(return_value=[{"id": 1, "quantity": 4}]),
        )
        service, store = _service(monkeypatch, client)
        await service._inventory_step_postgres({"offers": 0, "stocks": 0})
        await service._inventory_step_postgres({"offers": 0, "stocks": 0})

        step = service.inventory_step_health()
        assert step["failures_since_ok"] == 0 and step["last_ok_at"]
        store.upsert_stocks.assert_awaited_once()


class TestTheWeeklyFullSync:
    @pytest.mark.asyncio
    async def test_sync_offers_answers_none_not_zero(self, monkeypatch):
        client = SimpleNamespace(fetch_all_offers=AsyncMock(
            side_effect=KeyCRMConnectionError("503")))
        service, store = _service(monkeypatch, client)
        assert await service.sync_offers() is None
        store.set_last_sync_time.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_good_fetch_still_answers_its_count(self, monkeypatch):
        client = SimpleNamespace(fetch_all_offers=AsyncMock(return_value=[{"id": 1}]))
        service, store = _service(monkeypatch, client)
        assert await service.sync_offers() == 2
        store.set_last_sync_time.assert_awaited_once_with("offers")
