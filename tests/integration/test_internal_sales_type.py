"""`sales_type=internal` is staff activity, and it is admin-only.

Retail is a fixed list of retail managers; b2b is the wholesale manager;
nobody else may be mixed into either. What is left used to be called `other`
and appeared on no page at all — a junk drawer holding two unrelated things:
one manager's own sales (₴1.96M) and another's shipments to bloggers (₴1.31M).
It now has a name, a filter, and a gate.
"""
import time

import pytest
from fastapi.testclient import TestClient

from core.duckdb_constants import KNOWN_SALES_TYPES
from core.validators import validate_sales_type
from web.main import app
from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer
from core.permissions import ADMIN_USER_IDS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]
VIEWER_ID = 555_000_777


def _make_cookie(user_id: int, role: str = "admin") -> str:
    return session_serializer.dumps(create_session_data(
        {
            "id": str(user_id),
            "first_name": "Test",
            "last_name": "User",
            "username": "tester",
            "auth_date": str(int(time.time())),
        },
        role=role,
    ))


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
def as_viewer(client, monkeypatch):
    assert VIEWER_ID not in ADMIN_USER_IDS

    async def _resolve(session):
        return {"user_id": VIEWER_ID, "role": "viewer"}

    monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)
    client.cookies.set(SESSION_COOKIE, _make_cookie(VIEWER_ID, role="viewer"))
    return client


@pytest.fixture
def as_admin(client, monkeypatch):
    async def _resolve(session):
        return {"user_id": ADMIN_ID, "role": "admin"}

    monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)
    client.cookies.set(SESSION_COOKIE, _make_cookie(ADMIN_ID))
    return client


class TestTheCategoryExists:
    def test_internal_is_part_of_the_partition(self):
        assert "internal" in KNOWN_SALES_TYPES
        assert "other" not in KNOWN_SALES_TYPES

    def test_the_validator_accepts_it(self):
        assert validate_sales_type("internal") == "internal"

    def test_the_validator_still_rejects_nonsense(self):
        from core.exceptions import ValidationError
        with pytest.raises(ValidationError):
            validate_sales_type("wholesale")

    def test_silver_emits_internal_for_unlisted_managers(self):
        """The value comes from one CASE, and this is its last branch."""
        import inspect
        from core import duckdb_store

        source = inspect.getsource(duckdb_store.DuckDBStore.refresh_warehouse_layers)
        assert "ELSE 'internal'" in source
        assert "ELSE 'other'" not in source


class TestTheGate:
    def test_a_viewer_is_refused(self, as_viewer):
        res = as_viewer.get("/api/summary?period=today&sales_type=internal")
        assert res.status_code == 403
        assert "admin" in res.json()["detail"].lower()

    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    def test_a_viewer_keeps_the_other_categories(self, as_viewer, sales_type):
        res = as_viewer.get(f"/api/summary?period=today&sales_type={sales_type}")
        assert res.status_code != 403

    def test_an_admin_passes(self, as_admin):
        res = as_admin.get("/api/summary?period=today&sales_type=internal")
        assert res.status_code != 403

    # Two tests here covered /api/dashboard/batch, the one route that carried
    # sales_type in a request body where api_gate — which reads the query
    # string — could not see it. That route is gone, and it was the only
    # body-carrying one, so the gap it guarded has no shape to test. Restore
    # both from git history alongside any new POST that takes sales_type in a
    # body; the gate still cannot see one.

    def test_an_anonymous_request_is_still_401_not_403(self, client):
        """Authentication comes first; the category rule is not a login page."""
        res = client.get("/api/summary?period=today&sales_type=internal")
        assert res.status_code == 401
