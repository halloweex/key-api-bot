"""HTTP contract for `POST /api/jobs/{job_id}/trigger`.

`job_id` is a free-form string naming a scheduler job, so the questions are
what an id nobody registered does, and what happens when the scheduler is not
there to ask. Both were answered by the route in writing and by neither branch
in fact: `get_scheduler()` constructs the singleton rather than returning None,
and `trigger_job` *raises* on an unknown id instead of returning None — so the
503 and the 404 were unreachable and an unknown job answered 500.

Nothing was ever started by mistake; the defect is a control that reports an
internal error where it means "no such job". Pinned here because the next
person reading the route would take those two branches for the behaviour.

The scheduler under test is the **real** `BackgroundScheduler` — a stopped
APScheduler holds its jobs pending, so `trigger_job` runs its real lookup and
moves a real `next_run_time`, and nothing executes. A fake would have pinned
the route against a contract of the test's own invention, which is exactly the
mismatch that produced the bug.
"""
import time
from datetime import datetime, timedelta

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    SESSION_COOKIE,
    create_session_data,
    require_admin,
    session_serializer,
)
from core.permissions import ADMIN_USER_IDS
from core.scheduler import BackgroundScheduler, JobInfo, SCHEDULER_TIMEZONE

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

TRIGGER_ROUTE = "/api/jobs/{job_id}/trigger"
KNOWN_JOB = "warehouse_refresh"


def _make_cookie(user_id: int, role: str = "admin") -> str:
    data = create_session_data(
        {
            "id": str(user_id),
            "first_name": "Test",
            "last_name": "User",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role=role,
    )
    return session_serializer.dumps(data)


async def _never_runs():
    raise AssertionError("a stopped scheduler must not execute anything")


def _running_scheduler() -> BackgroundScheduler:
    """A scheduler that knows one job and will never run it.

    `_started` with a stopped APScheduler is the state the route cares about:
    `is_running` is true, `get_job` answers from the pending list, and
    `job.modify(next_run_time=...)` writes a date no runner will ever read.
    The job is given a run time up front because a pending job has none, and
    "was it brought forward" needs something to compare against.
    """
    scheduler = BackgroundScheduler()
    scheduler._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    job = scheduler._scheduler.add_job(
        _never_runs, "interval", hours=1, id=KNOWN_JOB, name="Warehouse refresh",
    )
    job.modify(next_run_time=datetime.now(SCHEDULER_TIMEZONE) + timedelta(days=1))
    scheduler._job_info[KNOWN_JOB] = JobInfo(
        id=KNOWN_JOB, name="Warehouse refresh", description="", trigger="interval",
    )
    scheduler._started = True
    return scheduler


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from web.routes.api._deps import limiter
    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def scheduler(monkeypatch):
    """Install a scheduler for the route without touching the process singleton."""
    fake = _running_scheduler()
    monkeypatch.setattr("core.scheduler.get_scheduler", lambda: fake)
    return fake


def _next_run(scheduler: BackgroundScheduler, job_id: str):
    return scheduler._scheduler.get_job(job_id).next_run_time


class TestAuthorization:
    def test_triggering_requires_a_session(self, client):
        assert client.post(f"/api/jobs/{KNOWN_JOB}/trigger").status_code == 401

    def test_admin_dependency_is_present(self):
        from tests.routes_helper import find_route, route_dependencies

        assert find_route(app, TRIGGER_ROUTE, "POST") is not None
        assert require_admin in set(route_dependencies(app, TRIGGER_ROUTE, "POST"))

    def test_viewer_cannot_trigger(self, client, monkeypatch, scheduler):
        viewer_id = 555_000_444
        assert viewer_id not in ADMIN_USER_IDS
        before = _next_run(scheduler, KNOWN_JOB)

        async def _fake_resolve(session):
            return {"user_id": viewer_id, "role": "viewer"}

        monkeypatch.setattr("web.routes.auth._resolve_session", _fake_resolve)
        client.cookies.set(SESSION_COOKIE, _make_cookie(viewer_id, role="viewer"))

        assert client.post(f"/api/jobs/{KNOWN_JOB}/trigger").status_code == 403
        assert _next_run(scheduler, KNOWN_JOB) == before


class TestBehaviour:
    @pytest.fixture(autouse=True)
    def _as_admin(self, client):
        client.cookies.set(SESSION_COOKIE, _make_cookie(ADMIN_ID))

    def test_known_job_is_triggered(self, client, scheduler):
        before = _next_run(scheduler, KNOWN_JOB)

        res = client.post(f"/api/jobs/{KNOWN_JOB}/trigger")

        assert res.status_code == 200
        assert res.json()["job_id"] == KNOWN_JOB
        assert _next_run(scheduler, KNOWN_JOB) < before, "the run was not brought forward"

    def test_unknown_job_is_404(self, client, scheduler):
        """The scheduler's own registry is the whitelist: an id it does not
        hold reaches no job, and must say so rather than report an internal
        error."""
        before = _next_run(scheduler, KNOWN_JOB)

        res = client.post("/api/jobs/no_such_job/trigger")

        assert res.status_code == 404
        assert _next_run(scheduler, KNOWN_JOB) == before

    def test_scheduler_not_running_is_503(self, client, monkeypatch):
        """A scheduler that never started knows no jobs at all — reporting that
        as "job not found" would send someone looking for a typo."""
        monkeypatch.setattr(
            "core.scheduler.get_scheduler", lambda: BackgroundScheduler(),
        )

        res = client.post(f"/api/jobs/{KNOWN_JOB}/trigger")

        assert res.status_code == 503
