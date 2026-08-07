"""Repairing orders that only an id can reach.

Full-history reconciliation found 1 616 orders KeyCRM has and we do not, and
552 orders whose header landed while their line items never did — ₴1,422,610.30
of revenue with nothing to attribute it to. Neither is reachable by a delta sync
keyed on updated_at: the first we have never seen, and the second looks
complete.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from core.data_quality import Severity, _orders_without_line_items_check
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
    async def test_marks_the_warehouse_dirty_for_the_repaired_ids(self):
        service = _service()
        service._upsert_orders_with_expenses = AsyncMock(return_value=(1, 0))
        client = _client({7: {"id": 7}})

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            await service.repair_orders([7])

        service.store.mark_warehouse_dirty.assert_awaited_once_with([7])

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


class TestOrdersWithoutLineItemsCheck:
    @pytest.fixture
    def conn(self):
        c = duckdb.connect(":memory:")
        c.execute("""
            CREATE TABLE orders (id BIGINT, grand_total DECIMAL(12,2));
            CREATE TABLE order_products (order_id BIGINT, quantity INTEGER);
        """)
        yield c
        c.close()

    def test_reports_orders_with_revenue_and_no_products(self, conn):
        conn.execute("""
            INSERT INTO orders VALUES (1, 500.00), (2, 300.00), (3, 100.00);
            INSERT INTO order_products VALUES (3, 1);
        """)
        issues = _orders_without_line_items_check(conn)

        assert len(issues) == 1
        assert issues[0].count == 2
        assert issues[0].check_name == "orders_without_line_items"
        assert issues[0].severity == Severity.WARN
        assert issues[0].sample_ids == (1, 2)
        assert "800.00" in issues[0].description

    def test_zero_value_orders_are_not_flagged(self, conn):
        """An order billed at nothing having sold nothing is coherent."""
        conn.execute("INSERT INTO orders VALUES (1, 0.00)")

        assert _orders_without_line_items_check(conn) == []

    def test_clean_data_reports_nothing(self, conn):
        conn.execute("""
            INSERT INTO orders VALUES (1, 500.00);
            INSERT INTO order_products VALUES (1, 2);
        """)

        assert _orders_without_line_items_check(conn) == []

    def test_an_order_with_several_lines_counts_once(self, conn):
        conn.execute("""
            INSERT INTO orders VALUES (1, 500.00), (2, 200.00);
            INSERT INTO order_products VALUES (1, 1), (1, 2), (1, 3);
        """)
        issues = _orders_without_line_items_check(conn)

        assert issues[0].count == 1
        assert issues[0].sample_ids == (2,)
