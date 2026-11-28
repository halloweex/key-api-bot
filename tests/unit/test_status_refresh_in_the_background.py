"""A status refresh started in the background runs to its end, and a
cancelled one leaves no error behind.

`POST /api/sync/refresh-statuses` with `background=true` (its default)
starts the refresh as a task and answers at once. That task was the one piece
of detached work in `web/routes/api/admin.py` with no strong reference: the
loop holds tasks weakly, so it could be collected mid-flight. A collection at
that moment cannot be made to happen on demand, so the first test shows the
background run carried to its end, through a `gc.collect()`. Its
done-callback also called `t.exception()`, which raises on a cancelled task,
so every cancelled refresh ended in "Exception in callback" on the loop: the
second test.

The app runs on the test's own loop (`httpx.ASGITransport`), where the task
it starts lives; the sync service and the scheduler's heavy-job lock are
stand-ins.
"""
from __future__ import annotations

import asyncio
import gc
import time

import httpx
import pytest


def _admin_cookie() -> tuple[str, str]:
    from core.permissions import ADMIN_USER_IDS
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    return SESSION_COOKIE, session_serializer.dumps(create_session_data({
        "id": str(sorted(ADMIN_USER_IDS)[0]), "first_name": "T", "last_name": "U",
        "username": "t", "auth_date": str(int(time.time()))}, role="admin"))


class _Refresh:
    """The sync service: holds until released, then ends as told."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.ended = asyncio.Event()
        self.outcome: BaseException | None = None
        self.finished: list[int] = []

    async def refresh_order_statuses(self, days_back: int) -> dict:
        self.started.set()
        try:
            await self.release.wait()
            if self.outcome is not None:
                raise self.outcome
            self.finished.append(days_back)
            return {"updated": 0}
        finally:
            self.ended.set()


@pytest.fixture
def refresh(monkeypatch):
    from core import scheduler as scheduler_module
    from core import sync_service as sync_module
    from web.routes.api._deps import limiter

    service = _Refresh()

    class _Scheduler:
        _heavy_job_lock = asyncio.Lock()

    async def get_sync_service():
        return service

    monkeypatch.setattr(sync_module, "get_sync_service", get_sync_service)
    monkeypatch.setattr(scheduler_module, "get_scheduler", lambda: _Scheduler())
    limiter.reset()
    yield service
    limiter.reset()


async def _start(days: int) -> httpx.Response:
    from web.main import app

    name, value = _admin_cookie()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test", cookies={name: value}) as client:
        return await client.post("/api/sync/refresh-statuses", params={"days": days})


async def _callbacks_run() -> None:
    """A finished task's done-callbacks run on the loop's next turns."""
    for _ in range(3):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_a_background_refresh_is_carried_to_its_end(refresh):
    response = await _start(days=7)
    assert response.status_code == 200
    assert response.json()["status"] == "started"

    await asyncio.wait_for(refresh.started.wait(), 5)
    gc.collect()
    refresh.release.set()
    await asyncio.wait_for(refresh.ended.wait(), 5)

    assert refresh.finished == [7]


@pytest.mark.asyncio
async def test_a_cancelled_refresh_leaves_no_error_on_the_loop(refresh):
    loop = asyncio.get_running_loop()
    reported: list[dict] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(context))
    try:
        refresh.outcome = asyncio.CancelledError()
        response = await _start(days=7)
        assert response.status_code == 200

        await asyncio.wait_for(refresh.started.wait(), 5)
        refresh.release.set()
        await asyncio.wait_for(refresh.ended.wait(), 5)
        await _callbacks_run()
    finally:
        loop.set_exception_handler(previous)

    assert [c.get("message") for c in reported] == []
