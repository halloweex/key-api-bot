"""
Authorization tests for the margin analysis GET endpoints.

Broken-access-control regression guard. The six margin *read* endpoints return
cost, margin, GMROI and low-margin alerts — the same class of sensitive data as
expenses. They carried only the inherited ``api_gate`` (a valid session) once,
so any approved viewer could read margin through the API; the fix was
``require_admin`` on all six.

Since 2026-09-07 the gate is the ``margin`` **permission** instead, held by the
admin role alone. What the tests below pin is unchanged in substance — a
viewer, an editor and a marketer are all still refused — and one case is new:
a viewer whose personal tab set names ``margin`` is allowed, which is the whole
point of the change and the thing that would silently stop working if somebody
put ``require_admin`` back.

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
    """Enough of the store to resolve a non-admin session's role and tabs."""

    def __init__(self, role: str, allowed_features=None):
        self._role = role
        self._features = allowed_features

    async def get_user(self, uid):
        return {
            "status": "approved",
            "role": self._role,
            "allowed_features": self._features,
        }


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


def _login(monkeypatch, role: str, allowed_features=None) -> dict:
    """Install a fake store for `role` on every get_store() call site and
    return the cookie header for a matching signed session."""
    store = _FakeStore(role, allowed_features)

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

class TestMarginReadAllowedByTheTabItself:
    """The reason margin stopped being `require_admin`: it can be granted to
    one person without handing them user management and the warehouse."""

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_a_viewer_with_the_margin_tab_is_allowed(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "viewer", allowed_features=["margin"])
        assert client.get(path, headers=headers).status_code == 200

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_a_viewer_with_other_tabs_is_still_refused(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "viewer", allowed_features=["traffic"])
        assert client.get(path, headers=headers).status_code == 403


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
    def test_the_margin_permission_is_in_the_dependency_tree(self, path):
        """`require_permission` returns a fresh closure per call, so this
        cannot compare identities the way the `require_admin` version did —
        it reads the feature out of the closure instead."""
        from tests.routes_helper import route_dependencies

        features = set()
        for dep in route_dependencies(app, path, "GET"):
            if getattr(dep, "__name__", "") != "check_permission":
                continue
            for cell in dep.__closure__ or ():
                if isinstance(cell.cell_contents, str):
                    features.add(cell.cell_contents)

        assert "margin" in features, (
            f"{path} is not gated on the margin permission — every approved "
            "session could read cost and profit through it"
        )

    @pytest.mark.parametrize("path", MARGIN_GETS)
    def test_no_admin_gate_remains(self, path):
        """Two gates on one route, one saying "admin" and one saying "whoever
        holds this tab", makes the tab checkbox silently ineffective."""
        from tests.routes_helper import route_dependencies

        assert require_admin not in set(route_dependencies(app, path, "GET"))
