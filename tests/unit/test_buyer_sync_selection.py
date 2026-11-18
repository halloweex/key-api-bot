"""A failed buyer selection holds the watermark and costs nothing else."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


class TestAFailedSelectionIsContained:
    @pytest.mark.asyncio
    async def test_it_returns_without_moving_the_watermark(self):
        from core.sync_service import SyncService

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(side_effect=OSError("pool closed"))
        store.set_last_sync_time = AsyncMock()

        assert await SyncService(store=store).sync_missing_buyers() == 0
        store.set_last_sync_time.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_empty_selection_still_moves_it(self):
        """The control: "nothing missing" is a completed sync and is stamped."""
        from core.sync_service import SyncService

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(return_value=[])
        store.set_last_sync_time = AsyncMock()

        assert await SyncService(store=store).sync_missing_buyers() == 0
        store.set_last_sync_time.assert_awaited_once_with("buyers")


class TestASkippedBuyerIsCounted:
    @pytest.mark.asyncio
    async def test_the_step_publishes_how_many_postgres_refused(self, monkeypatch):
        from core import sync_service as mod
        from core.sync_service import SyncService

        class _B:
            def __init__(self, i):
                self.id, self.birthday = i, None

        client = MagicMock()
        client.fetch_buyers_by_ids = AsyncMock(return_value=[_B(1), _B(2)])
        monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))

        async def upsert(buyers, *, skipped_out=None):
            skipped_out.append(2)
            return 1

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(return_value=[1, 2])
        store.set_last_sync_time = AsyncMock()
        store.upsert_buyers = upsert
        svc = SyncService(store=store)

        assert await svc.sync_missing_buyers() == 1
        block = svc.buyer_sync_state.published(retry_in_s=None)
        assert block["last_skipped_bad"] == 1 and block["last_written"] == 1
        store.set_last_sync_time.assert_awaited_once_with("buyers")


class TestABatchPostgresRefusedWholeIsNotASuccess:
    """Chain 4 skips a buyer Postgres would refuse and lands the rest. A batch
    where it refused every one landed nothing, and with the chain there is no
    other writer: the step must not stamp its watermark or refresh the clock
    the canary pages by (review of PR-3). A data error, so it is not counted
    toward consecutive failures; the age pages."""

    @pytest.mark.asyncio
    async def test_the_watermark_holds_and_the_clock_does_not_move(self, monkeypatch):
        from core import sync_service as mod
        from core.sync_service import SyncService

        class _B:
            def __init__(self, i):
                self.id, self.birthday = i, None

        client = MagicMock()
        client.fetch_buyers_by_ids = AsyncMock(return_value=[_B(1), _B(2)])
        monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))

        async def upsert(buyers, *, skipped_out=None):
            skipped_out.extend([1, 2])
            return 0

        store = MagicMock()
        store.get_missing_buyer_ids = AsyncMock(return_value=[1, 2])
        store.set_last_sync_time = AsyncMock()
        store.upsert_buyers = upsert
        svc = SyncService(store=store)

        assert await svc.sync_missing_buyers() == 0
        store.set_last_sync_time.assert_not_awaited()
        state = svc.buyer_sync_state
        assert state.last_ok_at is None
        assert state.last_error_class == "BuyersRefused"
        assert state.consecutive_failures == 0
