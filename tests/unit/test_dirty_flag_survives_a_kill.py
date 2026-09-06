"""The dirty flag outlives the tick that read it.

The refresh job used to consume the flag first and work second, so a kill or
a shutdown cancellation mid-refresh lost the ids for good. Now it peeks, works,
and clears only the row it read — keyed on the stamp every marker bumps.
"""
import inspect
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore


async def _store(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "dirty.duckdb")
    await store.connect()
    await store.consume_warehouse_dirty()   # whatever connect left behind
    return store


class TestPeekAndClear:
    @pytest.mark.asyncio
    async def test_peek_does_not_clear(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await store.mark_warehouse_dirty([1, 2])
            dirty, ids, stamp = await store.peek_warehouse_dirty()
            assert (dirty, sorted(ids)) == (True, [1, 2]) and stamp is not None
            dirty_again, _, _ = await store.peek_warehouse_dirty()
            assert dirty_again is True
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_clear_removes_exactly_the_row_that_was_read(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await store.mark_warehouse_dirty([1])
            _, _, stamp = await store.peek_warehouse_dirty()
            assert await store.clear_warehouse_dirty(stamp) is True
            assert (await store.peek_warehouse_dirty())[0] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_marker_that_landed_in_between_survives_the_clear(self, tmp_path):
        """The ids merged during the refresh are the next tick's business."""
        store = await _store(tmp_path)
        try:
            await store.mark_warehouse_dirty([1])
            _, _, stamp = await store.peek_warehouse_dirty()
            await store.mark_warehouse_dirty([2])          # bumps the stamp
            assert await store.clear_warehouse_dirty(stamp) is False
            dirty, ids, _ = await store.peek_warehouse_dirty()
            assert dirty is True and sorted(ids) == [1, 2]
        finally:
            await store.close()


class TestTheJob:
    async def _job(self, tmp_path, monkeypatch, refresh):
        from core.scheduler import BackgroundScheduler

        store = await _store(tmp_path)
        await store.mark_warehouse_dirty([7])
        monkeypatch.setattr(store, "refresh_warehouse_layers", refresh)

        async def _get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        scheduler = BackgroundScheduler()
        monkeypatch.setattr(scheduler, "_rebuild_postgres_layers", AsyncMock())
        return store, scheduler

    @pytest.mark.asyncio
    async def test_a_successful_refresh_clears_the_flag(self, tmp_path, monkeypatch):
        store, scheduler = await self._job(
            tmp_path, monkeypatch, AsyncMock(return_value={"status": "success"}),
        )
        try:
            await scheduler._run_warehouse_refresh()
            assert (await store.peek_warehouse_dirty())[0] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_refresh_that_dies_leaves_the_flag_for_the_next_tick(self, tmp_path, monkeypatch):
        """The kill case: consume-then-do lost these ids for good."""
        store, scheduler = await self._job(
            tmp_path, monkeypatch, AsyncMock(side_effect=RuntimeError("killed")),
        )
        try:
            with pytest.raises(RuntimeError):
                await scheduler._run_warehouse_refresh()
            dirty, ids, _ = await store.peek_warehouse_dirty()
            assert dirty is True and ids == [7]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_deferred_postgres_rebuild_runs_on_a_quiet_tick(self, tmp_path, monkeypatch):
        from core.scheduler import BackgroundScheduler

        store = await _store(tmp_path)

        async def _get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        scheduler = BackgroundScheduler()
        rebuild = AsyncMock()
        monkeypatch.setattr(scheduler, "_rebuild_postgres_layers", rebuild)
        monkeypatch.setattr(BackgroundScheduler, "_pg_layers_pending", True)
        try:
            result = await scheduler._run_warehouse_refresh()
            assert result["pg_layers"] == "deferred"
            rebuild.assert_awaited_once()
        finally:
            monkeypatch.setattr(BackgroundScheduler, "_pg_layers_pending", False)
            await store.close()


class TestAdjacentMarks:
    def test_the_sync_marks_dirty_right_after_writing_orders(self):
        """A 429 on the hourly products page used to skip the mark at the end
        of the tick, leaving the orders just written out of Silver/Gold."""
        from core.sync_service import SyncService

        src = inspect.getsource(SyncService.incremental_sync)
        assert src.index("mark_warehouse_dirty(changed_ids)") < src.index("_get_max_updated_at(")

    def test_the_manual_silver_rebuild_asks_for_validation(self):
        from web.routes.api import admin

        assert "mark_warehouse_dirty(None)" in inspect.getsource(admin.rebuild_silver_from_scratch)
