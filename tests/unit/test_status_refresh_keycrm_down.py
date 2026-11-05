"""A 05:15 status refresh KeyCRM did not answer is a failed run, not 0 orders.

`refresh_order_statuses` caught `KeyCRMConnectionError`, logged a warning and
returned its stats, so a morning KeyCRM was unreachable read in /api/jobs as
a successful run that refreshed nothing. An open circuit breaker raised past
it until `CircuitOpenError` became a `KeyCRMConnectionError` (A10, L1-02);
from then on it was swallowed too, and the open breaker turned into a
success. Both now raise after the warning, the way `full_sync` does: the job
is recorded FAILED, and the manual route's inline run answers an error status
instead of "success". `KeyCRMAPIError` and the rest of `KeyCRMError` are still
logged and swallowed, as before.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from core.exceptions import KeyCRMConnectionError
from core.resilience import CircuitOpenError

DOWN = [
    pytest.param(lambda: CircuitOpenError(
        "Circuit breaker is open, request to order rejected"), id="breaker_open"),
    pytest.param(lambda: KeyCRMConnectionError("Request timeout"), id="connection_error"),
]


@pytest.fixture
def keycrm_down(monkeypatch):
    """KeyCRM's client raising `make()` on the first page; the store is never
    reached, so a bare mock stands in for it."""
    from core import sync_service as sync_module

    asked = []

    def install(make):
        class Client:
            async def paginate(self, endpoint, params=None, page_size=50):
                asked.append(endpoint)
                raise make()
                yield  # an async generator, as the real one is

        async def get_client():
            return Client()

        service = sync_module.SyncService(MagicMock())

        async def get_service():
            return service

        monkeypatch.setattr(sync_module, "get_async_client", get_client)
        monkeypatch.setattr(sync_module, "get_sync_service", get_service)
        return service

    install.asked = asked
    return install


@pytest.mark.parametrize("make", DOWN)
@pytest.mark.asyncio
async def test_the_refresh_raises_when_keycrm_is_down(keycrm_down, make):
    """Mutation: swallow `KeyCRMConnectionError` again — 0 orders, no raise."""
    service = keycrm_down(make)
    with pytest.raises(KeyCRMConnectionError):
        await service.refresh_order_statuses(days_back=30)
    assert keycrm_down.asked == ["order"]


@pytest.mark.parametrize("make", DOWN)
@pytest.mark.asyncio
async def test_the_job_is_recorded_failed(keycrm_down, make):
    """The scheduler's own run of `order_status_refresh`, through
    APScheduler and the listeners `start()` wires: /api/jobs reads this."""
    from core.scheduler import SCHEDULER_TIMEZONE, BackgroundScheduler

    keycrm_down(make)
    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    s._scheduler.add_listener(s._on_job_executed, EVENT_JOB_EXECUTED)
    s._scheduler.add_listener(s._on_job_error, EVENT_JOB_ERROR)
    s._add_job(
        job_id="order_status_refresh", name="Order Status Refresh",
        description="test", func=s._run_order_status_refresh,
        trigger=DateTrigger(run_date=datetime.now(SCHEDULER_TIMEZONE)),
    )
    s._scheduler.start()
    try:
        deadline = time.monotonic() + 5
        while not s.get_job_history("order_status_refresh") and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
    finally:
        s._scheduler.shutdown(wait=False)
    history = s.get_job_history("order_status_refresh")
    assert [run["status"] for run in history] == ["failed"], history
    assert s._job_info["order_status_refresh"].last_status.value == "failed"
    assert not s._heavy_job_lock.locked()


def _admin_client():
    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    cookie = session_serializer.dumps(create_session_data({
        "id": str(sorted(ADMIN_USER_IDS)[0]), "first_name": "T", "last_name": "U",
        "username": "t", "auth_date": str(int(time.time()))}, role="admin"))
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, cookie)
    return client


@pytest.fixture
def fresh_limits():
    from web.routes.api._deps import limiter

    limiter.reset()
    yield
    limiter.reset()


@pytest.mark.parametrize("make", DOWN)
def test_the_inline_route_answers_an_error(keycrm_down, make, fresh_limits):
    """`POST /api/sync/refresh-statuses?background=false` said "success" for
    a morning that refreshed nothing. It answers its own error now: a status
    and a sentence, never a traceback."""
    keycrm_down(make)
    r = _admin_client().post("/api/sync/refresh-statuses",
                             params={"background": "false"})
    assert r.status_code == 500, r.text
    assert r.json()["detail"].startswith("Status refresh failed: ")
    assert "Traceback" not in r.text
