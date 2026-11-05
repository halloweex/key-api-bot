"""A KeyCRM failure in the hourly products step costs that step, not the tick (L1-02).

The products step paginated KeyCRM outside every handler, so an open breaker
or a page that would not decode ended the incremental tick there: managers,
buyers and inventory never ran that minute. It is contained now like the other
steps — recorded under the step, retried after its hold — and stamps nothing
in `meta.mirror_state`, because nothing in Postgres failed.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import core.sync_service as sync_mod
from core.exceptions import KeyCRMDataError
from core.resilience import CircuitOpenError
from core.sync_service import SyncService


class _Client:
    def __init__(self, exc):
        self.exc, self.asked = exc, []

    async def paginate(self, endpoint, *args, **kwargs):
        self.asked.append(endpoint)
        if endpoint == "order":
            return
        raise self.exc
        yield  # pragma: no cover — an async generator


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [CircuitOpenError("open"),
                                 KeyCRMDataError("API returned 200 with a body that is not JSON")],
                         ids=["circuit-open", "not-json"])
async def test_the_tick_goes_on_to_managers_buyers_and_inventory(monkeypatch, exc):
    """Mutation: take the paginate out of its `try` again — the tick ends at
    products and none of the three steps after it runs."""
    stale = datetime.now(timezone.utc) - timedelta(hours=2)
    store = SimpleNamespace(get_last_sync_time=AsyncMock(return_value=stale),
                            upsert_products=AsyncMock())
    client = _Client(exc)
    monkeypatch.setattr(sync_mod, "get_async_client", AsyncMock(return_value=client))
    for name in ("emit_sync_started", "emit_sync_completed", "emit_sync_failed"):
        monkeypatch.setattr(sync_mod, name, AsyncMock())
    svc = SyncService(store=store)  # type: ignore[arg-type]
    svc._should_skip_sync = lambda: (False, "")
    svc._clear_orders_failure = AsyncMock()
    ran = []

    async def managers():
        ran.append("managers")
        return 0

    async def inventory(stats):
        ran.append("inventory")

    svc._managers_step_postgres = managers
    svc._inventory_step_postgres = inventory
    with patch("core.pg_landing._record_failure", new=AsyncMock()) as stamp, \
         patch.object(SyncService, "sync_missing_buyers",
                      new=AsyncMock(side_effect=lambda *a, **k: ran.append("buyers") or 0)):
        await svc.incremental_sync()

    assert "products" in client.asked
    assert ran[0] == "managers" and "inventory" in ran
    store.upsert_products.assert_not_awaited()
    stamp.assert_not_awaited()
    step = svc.catalogue_step.published(svc._catalogue_retry_in())
    assert step["last_error"] == type(exc).__name__
    assert step["retry_in_s"] and step["retry_in_s"] > 0
