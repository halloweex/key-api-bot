"""KeyCRM's failures are KeyCRM's, whatever shape they arrive in (L1-02).

The order step tells a KeyCRM failure (the tick ends, nothing is recorded in
Postgres) from chain 3's own (recorded in `meta.mirror_state`, the canary
pages `mirror_failing:bronze.orders`) by the `KeyCRMError` family. Two shapes
of KeyCRM failing were outside it: an open circuit breaker (`CircuitOpenError`
subclassed `Exception`), and a 200 whose body is not JSON — a proxy's or a
maintenance page — which raised a bare `JSONDecodeError`. Both were filed as
Postgres failures and paged as one.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import core.sync_service as sync_mod
from core.exceptions import KeyCRMConnectionError, KeyCRMDataError, KeyCRMError
from core.keycrm import KeyCRMClient
from core.resilience import CircuitOpenError
from core.sync_service import SyncService


def _html_client() -> KeyCRMClient:
    def handler(request):
        return httpx.Response(200, text="<html><body>Service temporarily unavailable</body></html>",
                              headers={"content-type": "text/html"})

    client = KeyCRMClient(api_key="test-only")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                       base_url="https://example.invalid")
    return client


class TestTheFamilies:
    def test_an_open_breaker_is_a_connection_error(self):
        """Mutation: `class CircuitOpenError(Exception)` again."""
        exc = CircuitOpenError("Circuit breaker is open, request to order rejected")
        assert isinstance(exc, KeyCRMConnectionError) and isinstance(exc, KeyCRMError)

    @pytest.mark.asyncio
    async def test_a_body_that_is_not_json_is_a_data_error(self):
        """Mutation: `return response.json()` bare again — JSONDecodeError."""
        client = _html_client()
        with pytest.raises(KeyCRMDataError) as err:
            async for _ in client.paginate("order", params={}):
                pass
        assert "200" in str(err.value)
        await client._client.aclose()


def _service(monkeypatch, client):
    store = SimpleNamespace(
        get_last_sync_time=AsyncMock(
            return_value=datetime.now(timezone.utc) - timedelta(minutes=5)),
    )
    monkeypatch.setattr(sync_mod, "get_async_client", AsyncMock(return_value=client))
    monkeypatch.setattr(sync_mod, "emit_sync_started", AsyncMock())
    monkeypatch.setattr(sync_mod, "emit_sync_completed", AsyncMock())
    failed = AsyncMock()
    monkeypatch.setattr(sync_mod, "emit_sync_failed", failed)
    svc = SyncService(store=store)  # type: ignore[arg-type]
    svc._should_skip_sync = lambda: (False, "")
    svc._record_orders_failure = AsyncMock()
    return svc, failed


class _Refusing:
    def __init__(self, exc):
        self.exc = exc

    async def paginate(self, *args, **kwargs):
        raise self.exc
        yield  # pragma: no cover — an async generator


class TestTheOrderStep:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("client", [
        _Refusing(CircuitOpenError("Circuit breaker is open, request to order rejected")),
        "html",
    ], ids=["circuit-open", "html-200"])
    async def test_it_is_keycrm_s_and_stamps_nothing_in_postgres(self, monkeypatch, client):
        if client == "html":
            client = _html_client()
        svc, failed = _service(monkeypatch, client)

        await svc.incremental_sync()                         # does not escape

        step = svc.orders_step.published()
        assert step["consecutive_failures"] == 0 and step["last_error_class"] is None
        svc._record_orders_failure.assert_not_awaited()     # no meta.mirror_state stamp
        failed.assert_awaited_once()                        # the tick says it failed
