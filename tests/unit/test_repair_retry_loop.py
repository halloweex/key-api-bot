"""The gap backfill must finish, not circle.

Observed in production on the first overnight drain: 1 610 orders recovered,
then the tail stopped moving. Each hourly run found ~37 holes, failed on all of
them, and recorded only 5 as permanently absent — so the same ids came back an
hour later, forever.

Two causes, both here.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError
from core.sync_service import SyncService


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


class TestCircuitBreakerIgnoresClientErrors:
    """A 404 is the server answering, not the server being down."""

    def _client(self):
        from core.keycrm import KeyCRMClient

        client = KeyCRMClient()
        client._client = MagicMock()
        return client

    def _response(self, status_code):
        r = MagicMock()
        r.status_code = status_code
        r.headers = {}
        r.text = "{}"
        r.content = b"{}"
        r.json = MagicMock(return_value={})
        return r

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    async def test_client_errors_do_not_trip_the_breaker(self, status, monkeypatch):
        import core.keycrm as keycrm

        client = self._client()

        async def request(**_kwargs):
            return self._response(status)

        client._client.request = request
        breaker = MagicMock()
        breaker.can_execute = AsyncMock(return_value=True)
        breaker.record_success = AsyncMock()
        breaker.record_failure = AsyncMock()
        monkeypatch.setattr(keycrm, "_circuit_breaker", breaker)

        with pytest.raises(KeyCRMAPIError):
            await client._request("GET", "order/1")

        breaker.record_failure.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_server_errors_still_trip_the_breaker(self, monkeypatch):
        import core.keycrm as keycrm

        client = self._client()

        async def request(**_kwargs):
            return self._response(503)

        client._client.request = request
        breaker = MagicMock()
        breaker.can_execute = AsyncMock(return_value=True)
        breaker.record_success = AsyncMock()
        breaker.record_failure = AsyncMock()
        monkeypatch.setattr(keycrm, "_circuit_breaker", breaker)
        monkeypatch.setattr(keycrm, "RETRY_CONFIG",
                            keycrm.RetryConfig(max_attempts=1, base_delay=0))

        with pytest.raises(KeyCRMConnectionError):
            await client._request("GET", "order/1")

        breaker.record_failure.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_run_of_404s_does_not_block_the_next_call(self, monkeypatch):
        """The failure mode itself: consecutive 404s starving everything else."""
        import core.keycrm as keycrm
        from core.resilience import CircuitState

        keycrm._circuit_breaker.state = CircuitState.CLOSED
        keycrm._circuit_breaker.failure_count = 0

        client = self._client()
        statuses = [404] * 10 + [200]

        async def request(**_kwargs):
            return self._response(statuses.pop(0))

        client._client.request = request

        for _ in range(10):
            with pytest.raises(KeyCRMAPIError):
                await client._request("GET", "order/x")

        assert await client._request("GET", "order/ok") == {}
