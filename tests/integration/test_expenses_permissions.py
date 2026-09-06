"""
Authorization tests for the expense GET endpoints.

Broken-access-control regression guard. The four expense *read* endpoints
return cost, margin and profit — data the permissions matrix withholds from
`viewer` and `marketer` (both carry ``EXPENSES = set()``). They must require
``expenses:view`` server-side, not merely a valid session, otherwise the UI's
``ProtectedSection feature="expenses"`` hides data the API still hands over.

The mutation endpoints (POST/DELETE) carry ``expenses:edit`` /
``expenses:delete``; those gates went unpinned until an IDOR pass removed both
and watched the whole suite stay green, so they are pinned below too. The
delete gate is the one that separates ``editor`` from ``admin`` — an editor may
book an expense and may not unbook one — which no read test can express.

Sessions are forged the same way as tests/integration/test_security_hardening.py
and the TestClient is built WITHOUT the context-manager form so no startup
event (DuckDB sync, scheduler) runs. The data layer is stubbed so a 200 proves
the handler ran (authz bypassed) and a 403 proves the gate fired before it.
"""
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    session_serializer,
    create_session_data,
    SESSION_COOKIE,
)
from core.permissions import ADMIN_USER_IDS, invalidate_permissions_cache
from web.services import dashboard_service

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

# The four read endpoints under test.
EXPENSE_GETS = [
    "/api/expense-types",
    "/api/expenses/summary",
    "/api/expenses/profit",
    "/api/expenses",
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
    """Enough of the store to (a) resolve a non-admin session's role and
    (b) let the expense handlers return cleanly if they are reached.

    It intentionally lacks ``seed_default_permissions`` / ``get_role_permissions``
    so ``get_permissions_for_role_async`` falls back to the hardcoded
    ``ROLE_PERMISSIONS`` matrix — the source of truth this test pins against.
    """

    def __init__(self, role: str, deletes: bool = True):
        self._role = role
        self._deletes = deletes
        self.added = []
        self.deleted = []

    async def get_user(self, uid):
        return {"status": "approved", "role": self._role}

    async def list_expenses(self, *a, **k):
        return []

    async def get_expenses_summary(self, *a, **k):
        return {}

    async def add_expense(self, **kwargs):
        self.added.append(kwargs)
        return {"id": 1, **{k: str(v) for k, v in kwargs.items()}}

    async def delete_expense(self, expense_id):
        self.deleted.append(expense_id)
        return self._deletes


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _stub_data_layer(monkeypatch):
    """Make the four handlers trivially return 200 when reached, and route
    session/permission lookups to a fake store. A distinct fake store per role
    is installed by ``_login``.
    """
    # Downstream data functions: async no-DB stubs.
    async def _types():
        return [{"id": 1, "name": "Facebook Ads"}]

    async def _summary(*a, **k):
        return {"by_type": [], "daily": []}

    async def _profit(*a, **k):
        return {"revenue": 0, "expenses": 0, "profit": 0}

    monkeypatch.setattr(dashboard_service, "get_expense_types", _types)
    monkeypatch.setattr(dashboard_service, "get_expense_summary", _summary)
    monkeypatch.setattr(dashboard_service, "get_profit_analysis", _profit)
    # parse_period is pure but depends on the calendar; pin it.
    monkeypatch.setattr(
        dashboard_service, "parse_period",
        lambda *a, **k: ("2026-08-01", "2026-08-28"),
    )
    # Fresh permission resolution every test (module-level cache is global).
    invalidate_permissions_cache()


def _login(monkeypatch, role: str) -> dict:
    """Install a fake store for `role` on every get_store() call site and
    return the cookie header for a matching signed session."""
    return _login_with_store(monkeypatch, role)[0]


def _login_with_store(monkeypatch, role: str, deletes: bool = True):
    """As `_login`, and hands back the store so a refusal can be shown to have
    reached no write at all — a 403 raised after the row was already gone
    would look identical from outside."""
    store = _FakeStore(role, deletes=deletes)

    async def _fake_get_store():
        return store

    # _resolve_session and get_permissions_for_role_async import get_store from
    # core.duckdb_store at call time; the /api/expenses handler holds a
    # reference bound in web.routes.api.expenses at import time.
    monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)
    monkeypatch.setattr("web.routes.api.expenses.get_store", _fake_get_store)
    # Non-hardcoded id so the role actually comes from the fake store.
    uid = 555_000_222
    assert uid not in ADMIN_USER_IDS
    return _cookie_header(_make_cookie(uid, role=role)), store


# ─── The bypass: roles without expenses:view must be refused ───────────────────

class TestExpenseReadRequiresViewPermission:
    """viewer and marketer have EXPENSES = set() — no view. Every read
    endpoint must answer 403, not 200."""

    @pytest.mark.parametrize("path", EXPENSE_GETS)
    def test_viewer_is_forbidden(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "viewer")
        assert client.get(path, headers=headers).status_code == 403

    @pytest.mark.parametrize("path", EXPENSE_GETS)
    def test_marketer_is_forbidden(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "marketer")
        assert client.get(path, headers=headers).status_code == 403


# ─── The allowed side: roles with expenses:view still get data ─────────────────

class TestExpenseReadAllowedForPrivilegedRoles:
    @pytest.mark.parametrize("path", EXPENSE_GETS)
    def test_editor_is_allowed(self, client, monkeypatch, path):
        headers = _login(monkeypatch, "editor")
        assert client.get(path, headers=headers).status_code == 200

    @pytest.mark.parametrize("path", EXPENSE_GETS)
    def test_hardcoded_admin_is_allowed(self, client, monkeypatch, path):
        # Hardcoded admin short-circuits both session resolution and the
        # permission check; still install a store for the handler's DB calls.
        store = _FakeStore("admin")

        async def _fake_get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)
        monkeypatch.setattr("web.routes.api.expenses.get_store", _fake_get_store)
        headers = _cookie_header(_make_cookie(ADMIN_ID, role="admin"))
        assert client.get(path, headers=headers).status_code == 200


# ─── No-session still 401 (api_gate), unaffected by the view gate ──────────────

class TestExpenseReadStillRequiresSession:
    @pytest.mark.parametrize("path", EXPENSE_GETS)
    def test_anonymous_is_401(self, client, path):
        assert client.get(path).status_code == 401


# ─── Writing and unwriting an expense ─────────────────────────────────────────

VALID_EXPENSE = {
    "expense_date": "2026-08-28",
    "category": "marketing",
    "expense_type": "Facebook Ads",
    "amount": 1000.0,
}

# `manual_expenses` has no owner column and never had one: an expense belongs
# to the company, not to whoever typed it in. So there is nothing here for one
# user to reach that is another's — the permission *is* the whole model, and
# these tests pin it as such rather than looking for an ownership check that
# was never designed.
DELETE_PATH = "/api/expenses/7"


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """POST and DELETE are 30/minute each; the module's own tests are well
    inside that, but the limiter is app-wide and shared with every other suite
    in the process."""
    from web.routes.api._deps import limiter
    limiter.reset()
    yield
    limiter.reset()


class TestExpenseWriteRequiresEditPermission:
    @pytest.mark.parametrize("role", ["viewer", "marketer"])
    def test_role_without_edit_cannot_create(self, client, monkeypatch, role):
        headers, store = _login_with_store(monkeypatch, role)

        res = client.post("/api/expenses", json=VALID_EXPENSE, headers=headers)

        assert res.status_code == 403
        assert store.added == []

    def test_editor_can_create(self, client, monkeypatch):
        headers, store = _login_with_store(monkeypatch, "editor")

        res = client.post("/api/expenses", json=VALID_EXPENSE, headers=headers)

        assert res.status_code == 200
        assert len(store.added) == 1


class TestExpenseDeleteRequiresDeletePermission:
    @pytest.mark.parametrize("role", ["viewer", "marketer", "editor"])
    def test_role_without_delete_is_refused(self, client, monkeypatch, role):
        """`editor` holds EXPENSES {view, edit} and no delete — booking a cost
        and erasing one are different powers, and only this endpoint tells
        them apart."""
        headers, store = _login_with_store(monkeypatch, role)

        res = client.delete(DELETE_PATH, headers=headers)

        assert res.status_code == 403
        assert store.deleted == []

    def test_hardcoded_admin_can_delete(self, client, monkeypatch):
        _, store = _login_with_store(monkeypatch, "admin")
        headers = _cookie_header(_make_cookie(ADMIN_ID, role="admin"))

        res = client.delete(DELETE_PATH, headers=headers)

        assert res.status_code == 200
        assert store.deleted == [7]

    def test_deleting_an_id_that_does_not_exist_is_404(self, client, monkeypatch):
        """The store reports what it deleted, and the route must not turn an
        empty DELETE into `{"success": true}` — a confirmation for a row that
        was never there is how a caller learns the wrong thing about the
        ledger."""
        _, store = _login_with_store(monkeypatch, "admin", deletes=False)
        headers = _cookie_header(_make_cookie(ADMIN_ID, role="admin"))

        res = client.delete("/api/expenses/999999", headers=headers)

        assert res.status_code == 404
        assert store.deleted == [999999]

    def test_anonymous_is_401(self, client):
        assert client.delete(DELETE_PATH).status_code == 401
