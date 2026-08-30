"""
Authorization tests for the margin analysis GET endpoints.

Broken-access-control regression guard. The six margin *read* endpoints return
cost, margin, GMROI and low-margin alerts — the same class of sensitive data as
expenses. The frontend gates the whole ``/margin`` page behind ``<AdminGuard>``
(web/frontend/src/App.tsx), so the intent is admin-only; but the server carried
only the inherited ``api_gate`` (a valid session), so any approved viewer could
read margin through the API. These tests pin the server to the frontend's
intent: ``require_admin`` on every one of the six.

Sessions are forged the same way as tests/integration/test_security_hardening.py
and tests/integration/test_expenses_permissions.py, and the TestClient is built
WITHOUT the context-manager form so no startup event (DuckDB sync, scheduler)
runs. The data layer is stubbed so a 200 proves the handler ran (authz bypassed)
and a 403 proves the gate fired before it.
"""
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    session_serializer,
    create_session_data,
    require_admin,
    SESSION_COOKIE,
)
from core.permissions import ADMIN_USER_IDS, invalidate_permissions_cache
from web.services import margin_service

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

# The six read endpoints under test.
MARGIN_GETS = [
    "/api/margin/overview",
    "/api/margin/by-brand",
    "/api/margin/by-category",
    "/api/margin/trend",
    "/api/margin/brand-category",
    "/api/margin/alerts",
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_cookie(user_id: int, role: str) -> str:
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


def _cookie_header(value: str) -> dict:
    return {"Cookie": f"{SESSION_COOKIE}={value}"}


class _FakeStore:
    """Enough of the store to resolve a non-admin session's role."""

    def __init__(self, role: str):
        self._role = role

    async def get_user(self, uid):
        return {"status": "approved", "role": self._role}


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _stub_data_layer(monkeypatch):
    """Make every margin handler trivially return 200 when reached, so a 200
    proves the handler ran and authz was bypassed, and a 403 proves the gate
    fired before it."""

    def _parse_period(*a, **k):
        return ("2026-08-01", "2026-08-28")

    async def _overview(*a, **k):
        return {}

    async def _list(*a, **k):
        return []

    monkeypatch.setattr(margin_service, "parse_period", _parse_period)
    monkeypatch.setattr(margin_service, "get_margin_overview", _overview)
    monkeypatch.setattr(margin_service, "get_margin_by_brand", _list)
    monkeypatch.setattr(margin_service, "get_margin_by_category", _list)
    monkeypatch.setattr(margin_service, "get_margin_trend", _list)
    monkeypatch.setattr(margin_service, "get_margin_brand_category", _list)
    monkeypatch.setattr(margin_service, "get_margin_alerts", _list)
    # Fresh permission resolution every test (module-level cache is global).
    invalidate_permissions_cache()


def _login(monkeypatch, role: str) -> dict:
    """Install a fake store for `role` on every get_store() call site and
    return the cookie header for a matching signed session."""
    store = _FakeStore(role)

    async def _fake_get_store():
        return store

    # _resolve_session imports get_store from core.duckdb_store at call time.
    monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)
    # Non-hardcoded id so the role actually comes from the fake store.
    uid = 555_000_333
    assert uid not in ADMIN_USER_IDS
    return _cookie_header(_make_cookie(uid, role=role))


# ─── The bypass: non-admin roles must be refused ───────────────────────────────

class TestMarginReadRequiresAdmin:
    """Margin is admin-only (AdminGuard on the frontend). Every read endpoint
    must answer 403 for a non-admin session, not 200."""

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_viewer_is_forbidden(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "viewer")
        assert client.get(path, headers=headers).status_code == 403

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_marketer_is_forbidden(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "marketer")
        assert client.get(path, headers=headers).status_code == 403

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_editor_is_forbidden(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "editor")
        assert client.get(path, headers=headers).status_code == 403


# ─── The allowed side: admins still get data ───────────────────────────────────

class TestMarginReadAllowedForAdmin:
    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_db_role_admin_is_allowed(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "admin")
        assert client.get(path, headers=headers).status_code == 200

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_hardcoded_admin_is_allowed(self, client, monkeypatch, path):
        # Hardcoded admin short-circuits session resolution; no store needed,
        # but install one so any DB call path is harmless.
        store = _FakeStore("admin")

        async def _fake_get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)
        headers = _cookie_header(_make_cookie(ADMIN_ID, role="admin"))
        assert client.get(path, headers=headers).status_code == 200


# ─── No-session still 401 (api_gate), unaffected by the admin gate ─────────────

class TestMarginReadStillRequiresSession:
    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_anonymous_is_401(self, client, path):
        assert client.get(path).status_code == 401


# ─── Structural: require_admin is present on every margin route ────────────────

class TestMarginRouteStructure:
    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_require_admin_in_dependency_tree(self, path):
        from tests.routes_helper import route_dependencies

        assert require_admin in set(route_dependencies(app, path, "GET")), \
            f"{path} is missing require_admin — margin is admin-only"
