"""The gap backfill must finish, not circle.

Observed in production on the first overnight drain: 1 610 orders recovered,
then the tail stopped moving. Each hourly run found ~37 holes, failed on all of
them, and recorded only 5 as permanently absent — so the same ids came back an
hour later, forever.

Two causes, both here.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.exceptions import KeyCRMAPIError, KeyCRMError
from core.sync_service import SyncService
from tests.helpers.orders_sync import answered, keycrm_serving_orders, order_payload


def _service():
    service = SyncService.__new__(SyncService)
    service.store = MagicMock()
    service.store.mark_warehouse_dirty = AsyncMock()
    service._upsert_orders_with_expenses = AsyncMock(return_value=(0, 0))
    return service


class TestAllFailuresReachTheCaller:
    @pytest.mark.asyncio
    async def test_more_than_ten_failures_are_all_returned(self):
        """The caller decides what is permanent; a sample cannot."""
        service = _service()
        client = MagicMock()
        client.get_order = AsyncMock(side_effect=KeyCRMAPIError(
            "API returned 404: not found", status_code=404,
        ))

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders(range(37))

        assert result["failed"] == 37
        assert len(result["failures"]) == 37, (
            "truncating here is what made the backfill loop"
        )

    @pytest.mark.asyncio
    async def test_every_absent_id_can_be_recorded_in_one_pass(self):
        service = _service()
        client = MagicMock()
        client.get_order = AsyncMock(side_effect=KeyCRMAPIError(
            "API returned 404: Requested entity not found", status_code=404,
        ))

        with patch("core.sync_service.get_async_client", AsyncMock(return_value=client)):
            result = await service.repair_orders(range(20))

        permanent = {
            oid for oid, reason in result["failures"].items()
            if "not found" in str(reason).lower()
        }
        assert permanent == set(range(20))


class TestTheBreakerOpensOnlyWhenKeyCRMIsDown:
    """A 4xx is KeyCRM answering clearly, not KeyCRM being down. Counted as
    breaker failures, a handful of 404s (the gap backfill walking ids KeyCRM
    no longer has) opened the circuit and refused every other call for a
    minute, the incremental sync included."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status, opens", [
        (400, False), (401, False), (403, False), (404, False), (422, False),
        (429, True), (503, True)])
    async def test_a_run_of_one_answer_then_keycrm_recovers(self, monkeypatch, status, opens):
        """As many calls answered `status` as it takes to open the breaker,
        then KeyCRM answers again: after a client error the next call goes
        through; after a rate limit or an outage it is refused before it
        reaches KeyCRM. The real client and breaker, a fresh breaker per test,
        KeyCRM faked at the transport."""
        from core import keycrm
        from core.resilience import CircuitOpenError

        down = True
        served = keycrm_serving_orders(
            monkeypatch, [order_payload(1, created=datetime(2026, 11, 2, 12, tzinfo=timezone.utc))],
            answer=lambda _request: answered(status) if down else None)
        client = await keycrm.get_async_client()

        for _ in range(keycrm.CIRCUIT_BREAKER_CONFIG.failure_threshold):
            with pytest.raises(KeyCRMError):
                await client.get_order(1)
        down = False
        asked = len(served.requests)

        if opens:
            with pytest.raises(CircuitOpenError):
                await client.get_order(1)
            assert len(served.requests) == asked, "the refused call reached KeyCRM"
        else:
            assert (await client.get_order(1))["id"] == 1
