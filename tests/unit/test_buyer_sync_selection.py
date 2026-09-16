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

    def test_an_unknown_engine_raises(self, monkeypatch):
        from core import pg_buyer_sync_read
        monkeypatch.setenv("KS_READ_BUYER_SYNC", "postgre")
        with pytest.raises(ValueError):
            pg_buyer_sync_read.enabled()
