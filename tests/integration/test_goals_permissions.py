"""Authorization and identifier handling for the goal-writing endpoints.

`DELETE /api/goals/{period_type}` takes a *type*, not a row id — the table is
`revenue_goals`, keyed on `period_type`, three rows at most and shared by
everyone. So the question an IDOR pass asks of it is not "whose goal is this"
but "what does an unexpected segment reach": the answer must be one named row
and nothing wider, and a value outside the three must reach no write at all.

Despite the verb, it deletes nothing. It recomputes the suggestion and upserts
that one row, which is why the assertions below are about which row the store
was asked for rather than about rows disappearing.

The three writers here are `require_admin`, and that gate was unpinned:
downgrading this one to `require_user` left the whole suite green.
"""
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    SESSION_COOKIE,
    create_session_data,
    require_admin,
    session_serializer,
)
from core.permissions import ADMIN_USER_IDS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

RESET_ROUTE = "/api/goals/{period_type}"
PERIOD_TYPES = ["daily", "weekly", "monthly"]

# Every route in this module that writes a goal, with a request that would
# succeed if it were allowed to run.
WRITERS = [
    ("DELETE", "/api/goals/daily"),
    ("POST", "/api/goals?period_type=daily&amount=1000"),
    ("POST", "/api/goals/recalculate"),
    # A GET, but `recalculate=true` rewrites the shared seasonality tables the
    # way the POST does, so it is gated the same way.
    ("GET", "/api/goals/forecast?year=2026&month=9&recalculate=true"),
]


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


class _FakeStore:
    """Records what the goal writers were asked to do, and nothing else."""

    def __init__(self):
        self.reset_calls = []
        self.set_calls = []
        self.recalculated = []

    async def reset_goal_to_auto(self, period_type, sales_type="retail"):
        self.reset_calls.append((period_type, sales_type))
        return {"periodType": period_type, "amount": 1000.0, "isCustom": False}

    async def set_goal(self, period_type, amount, is_custom=True, growth_factor=1.10):
        self.set_calls.append((period_type, amount, is_custom, growth_factor))
        return {"periodType": period_type, "amount": amount, "isCustom": is_custom}

    async def calculate_seasonality_indices(self, sales_type="retail"):
        self.recalculated.append(("seasonality", sales_type))
        return {}

    async def calculate_yoy_growth(self, sales_type="retail"):
        self.recalculated.append(("growth", sales_type))
        return {"overall_yoy": 0.0, "yearly_data": []}

    async def calculate_weekly_patterns(self, sales_type="retail"):
        self.recalculated.append(("weekly", sales_type))
        return {}


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
def store(monkeypatch):
    fake = _FakeStore()

    async def _fake_get_store():
        return fake

    monkeypatch.setattr("web.routes.api.goals.get_store", _fake_get_store)
    return fake


def _wrote_nothing(store: _FakeStore) -> bool:
    return not (store.reset_calls or store.set_calls or store.recalculated)


class TestAuthorization:
    @pytest.mark.parametrize("method,path", WRITERS)
    def test_writing_requires_a_session(self, client, method, path):
        assert client.request(method, path).status_code == 401

    @pytest.mark.parametrize("method,path", [
        ("DELETE", RESET_ROUTE),
        ("POST", "/api/goals"),
        ("POST", "/api/goals/recalculate"),
    ])
    def test_admin_dependency_is_present(self, method, path):
        from tests.routes_helper import find_route, route_dependencies

        assert find_route(app, path, method) is not None, \
            f"{method} {path} is not registered"
        assert require_admin in set(route_dependencies(app, path, method)), \
            f"{method} {path} dropped require_admin — admin→user downgrade regression"

    @pytest.mark.parametrize("method,path", WRITERS)
    def test_viewer_cannot_write_a_goal(self, client, monkeypatch, store, method, path):
        viewer_id = 555_000_555
        assert viewer_id not in ADMIN_USER_IDS

        async def _fake_resolve(session):
            return {"user_id": viewer_id, "role": "viewer"}

        monkeypatch.setattr("web.routes.auth._resolve_session", _fake_resolve)
        client.cookies.set(SESSION_COOKIE, _make_cookie(viewer_id, role="viewer"))

        assert client.request(method, path).status_code == 403
        assert _wrote_nothing(store)


class TestThePathSegmentIsAType:
    @pytest.fixture(autouse=True)
    def _as_admin(self, client):
        client.cookies.set(SESSION_COOKIE, _make_cookie(ADMIN_ID))

    @pytest.mark.parametrize("period_type", PERIOD_TYPES)
    def test_a_known_type_resets_that_type_alone(self, client, store, period_type):
        res = client.delete(f"/api/goals/{period_type}")

        assert res.status_code == 200
        assert store.reset_calls == [(period_type, "retail")]

    @pytest.mark.parametrize(
        "period_type", ["quarterly", "DAILY", "daily%20", "*", "1"],
    )
    def test_an_unknown_type_reaches_no_write(self, client, store, period_type):
        """The whitelist is on the route, ahead of the store — so a value the
        goal system has no meaning for cannot become a row, and cannot pick up
        one that already exists under a different name."""
        res = client.delete(f"/api/goals/{period_type}")

        assert res.status_code == 400
        assert _wrote_nothing(store)
