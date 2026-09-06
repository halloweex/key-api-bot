"""`force_resync` rewrites the window in place; it no longer empties the store.

The DELETE-then-refill shape meant a 429 storm halfway through a year-long
walk over the API — its ordinary failure — left months of orders missing, with
nothing to restore them but the hourly gap crawl at 200 ids an hour. Rewriting
in place with `force_update=True` reaches the same end state and can simply be
run again.
"""
import pytest

from core import sync_service as sync_module


class _RecordingSyncService:
    def __init__(self):
        self.calls = []
        self._use_ordered_between = "stale"

    async def full_sync(self, days_back=730, force_update=False):
        self.calls.append({"days_back": days_back, "force_update": force_update})
        return {"orders": 1}


class _StoreThatMustNotBeTouched:
    def connection(self):
        raise AssertionError("force_resync must not open the store — it used to DELETE from it")


@pytest.mark.asyncio
async def test_it_forces_a_full_sync_and_deletes_nothing(monkeypatch):
    service = _RecordingSyncService()

    async def _service():
        return service

    async def _store():
        return _StoreThatMustNotBeTouched()

    monkeypatch.setattr(sync_module, "get_sync_service", _service)
    monkeypatch.setattr(sync_module, "get_store", _store)

    stats = await sync_module.force_resync(days_back=90)

    assert stats == {"orders": 1}
    assert service.calls == [{"days_back": 90, "force_update": True}]
    assert service._use_ordered_between is None, "the filter detection is re-run"


def test_full_sync_threads_the_flag_to_the_order_writer():
    """Structural: the kwarg exists only to reach `_upsert_orders_with_expenses`."""
    import inspect

    source = inspect.getsource(sync_module.SyncService.full_sync)
    assert "force_update=force_update" in source
