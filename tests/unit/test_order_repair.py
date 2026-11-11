"""Repairing orders that only an id can reach.

Full-history reconciliation found 1 616 orders KeyCRM has and we do not, and
552 orders whose header landed while their line items never did — ₴1,422,610.30
of revenue with nothing to attribute it to. Neither is reachable by a delta sync
keyed on updated_at: the first we have never seen, and the second looks
complete.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.sync_service import SyncService


def _service():
    service = SyncService.__new__(SyncService)
    service.store = MagicMock()
    service.store.mark_warehouse_dirty = AsyncMock()
    service._upsert_orders_with_expenses = AsyncMock(return_value=(0, 0))
    return service


def _client(orders_by_id, *, fails=()):
    client = MagicMock()

    async def get_order(order_id, include=None):
        if order_id in fails:
            raise RuntimeError("boom")
        return orders_by_id.get(order_id)

    client.get_order = AsyncMock(side_effect=get_order)
    return client


class TestRepairOrders:
    @pytest.mark.asyncio
    async def test_fetches_each_id_and_upserts_them_together(self):
        service = _service()
        service._upsert_orders_with_expenses = AsyncMock(return_value=(2, 0))
        client = _client({1: {"id": 1}, 2: {"id": 2}})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders([1, 2])

        assert result["repaired"] == 2
        assert result["failed"] == 0
        assert client.get_order.await_count == 2
        upserted = service._upsert_orders_with_expenses.await_args
        assert [o["id"] for o in upserted.args[0]] == [1, 2]
        assert upserted.kwargs["force_update"] is True

    @pytest.mark.asyncio
    async def test_marks_no_duckdb_warehouse(self):
        """It marked DuckDB's warehouse dirty for the repaired ids until step
        14's S14-15 — a no-op since step 13. Chain 3's writer raises the
        Postgres derivation's signal in the order's own transaction."""
        service = _service()
        service._upsert_orders_with_expenses = AsyncMock(return_value=(1, 0))
        client = _client({7: {"id": 7}})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            await service.repair_orders([7])

        service.store.mark_warehouse_dirty.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_one_failing_order_does_not_lose_the_others(self):
        service = _service()
        service._upsert_orders_with_expenses = AsyncMock(return_value=(1, 0))
        client = _client({1: {"id": 1}, 2: {"id": 2}}, fails={1})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders([1, 2])

        assert result["repaired"] == 1
        assert result["failed"] == 1
        assert "RuntimeError" in result["failures"][1]

    @pytest.mark.asyncio
    async def test_an_order_keycrm_does_not_have_is_a_failure_not_a_crash(self):
        service = _service()
        client = _client({1: None})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders([1])

        assert result["failed"] == 1
        assert "not found" in result["failures"][1]
        service._upsert_orders_with_expenses.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_run_is_bounded_and_the_rest_is_carried_over(self):
        service = _service()
        service._upsert_orders_with_expenses = AsyncMock(return_value=(3, 0))
        client = _client({i: {"id": i} for i in range(10)})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders(range(10), limit=3)

        assert result["requested"] == 10
        assert result["attempted"] == 3
        assert result["remaining"] == 7
        assert client.get_order.await_count == 3

    @pytest.mark.asyncio
    async def test_duplicate_ids_are_fetched_once(self):
        service = _service()
        service._upsert_orders_with_expenses = AsyncMock(return_value=(1, 0))
        client = _client({5: {"id": 5}})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders([5, 5, 5])

        assert result["requested"] == 1
        assert client.get_order.await_count == 1

    @pytest.mark.asyncio
    async def test_nothing_to_repair_touches_no_api(self):
        service = _service()
        client = _client({})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders([])

        assert result == {"requested": 0, "attempted": 0, "repaired": 0,
                          "failed": 0, "remaining": 0, "failures": {}}
        client.get_order.assert_not_awaited()


class TestIntegrityScanSurvivesAMissingTable:
    """`/warehouse/rebuild-silver` DROPped silver_orders while the DuckDB scan
    could land inside that window, and the handler written to tolerate it
    referenced a logger the module never imported. The scan reads no DuckDB
    table since step 14's S14-14; the name it needed stays named."""

    def test_the_module_has_a_logger(self):
        """The bug was a name that did not exist, so name it in a test."""
        import core.data_quality as dq

        assert hasattr(dq, "logger")


class TestTheWriteRunsUnderTheHeavyLock:
    """The lock covers the upsert — never the per-id fetches, which would hold
    the sync up for a batch of HTTP calls. (It covered DuckDB's dirty mark
    too, until step 14's S14-15 deleted the mark.)"""

    @pytest.mark.asyncio
    async def test_fetch_outside_and_write_inside(self):
        import asyncio

        lock = asyncio.Lock()
        service = _service()
        seen = {}

        async def _get_order(order_id, include=None):
            seen["fetch"] = lock.locked()
            return {"id": order_id}

        async def _upsert(orders, **kw):
            seen["upsert"] = lock.locked()
            return (len(orders), 0)

        client = MagicMock()
        client.get_order = AsyncMock(side_effect=_get_order)
        service._upsert_orders_with_expenses = AsyncMock(side_effect=_upsert)

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders([5], lock=lock)

        assert result["repaired"] == 1
        assert seen == {"fetch": False, "upsert": True}
        assert not lock.locked()
