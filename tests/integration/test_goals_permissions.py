"""Authorization and identifier handling for the goal-writing endpoints.

`DELETE /api/goals/{period_type}` takes a *type*, not a row id — the table is
`revenue_goals`, keyed on `period_type`, three rows at most and shared by
everyone. So the question an IDOR pass asks of it is not "whose goal is this"
but "what does an unexpected segment reach": the answer must be one named row
and nothing wider, and a value outside the three must reach no write at all.

Despite the verb, it deletes nothing. It recomputes the suggestion and upserts
that one row, which is why the test on a real database reads the three goals
back after it.

The three writers here are `require_admin`, and that gate was unpinned:
downgrading this one to `require_user` left the whole suite green.
"""
import os
import time
from decimal import Decimal

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
from tests.integration.pg_tab_seed import needs_postgres

KS_PG_DSN = os.getenv("KS_PG_DSN")

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

RESET_ROUTE = "/api/goals/{period_type}"
PERIOD_TYPES = ["daily", "weekly", "monthly"]

# Every route in this module that writes a goal, with a request that would
# succeed if it were allowed to run.
WRITERS = [
    ("DELETE", "/api/goals/daily"),
    ("POST", "/api/goals?period_type=daily&amount=1000"),
    ("POST", "/api/goals/recalculate"),
    # `GET /api/goals/forecast?recalculate=true` stood here while it rewrote
    # the shared seasonality tables the way the POST does. A GET never writes
    # now (OD-14 (i)): it is refused for everybody, an admin included —
    # `tests/unit/test_goals_read_paths_do_not_write.py`.
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

    async def recalculate_goal_tables(self, *, include_weekly):
        self.recalculated.append(("goal_tables", include_weekly))
        return {"seasonal": {}, "yoy": {"overall_yoy": 0.0, "yearly_data": []}}


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


async def _goal_rows(conn):
    rows = await conn.fetch(
        "SELECT period_type, goal_amount, is_custom FROM app.revenue_goals")
    return {r["period_type"]: (r["goal_amount"], r["is_custom"]) for r in rows}


async def _clear_goals(conn):
    await conn.execute("DELETE FROM app.revenue_goals")
    await conn.execute("DELETE FROM app.revenue_goal_history")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1",
                       "capture.app.revenue_goal_history")
    await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


class TestAResetTouchesOneRow:
    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("period_type", PERIOD_TYPES)
    async def test_a_known_type_resets_that_type_alone(self, period_type):
        """Three typed goals; resetting one makes that row automatic and
        leaves the other two as typed."""
        import asyncpg

        from tests.helpers.app_client import signed_in

        conn = await asyncpg.connect(KS_PG_DSN)
        await _clear_goals(conn)
        try:
            async with signed_in("admin") as admin:
                for kind in PERIOD_TYPES:
                    r = await admin.post(f"/api/goals?period_type={kind}&amount=5000")
                    assert r.status_code == 200, r.text
                assert all(custom for _, custom in (await _goal_rows(conn)).values())

                res = await admin.delete(f"/api/goals/{period_type}")
                assert res.status_code == 200, res.text

            rows = await _goal_rows(conn)
            assert set(rows) == set(PERIOD_TYPES)
            for kind in PERIOD_TYPES:
                amount, custom = rows[kind]
                if kind == period_type:
                    assert custom is False
                else:
                    assert (amount, custom) == (Decimal("5000.00"), True)
        finally:
            await _clear_goals(conn)
            await conn.close()
