"""Who may read `/api/stock-margin`: the admin role, and nobody else.

The block names the purchase price of every offer in stock, so the owner asked
for it to be shown to admins only (2026-11-26). The case this file exists for
is the second class: `/margin` is a *tab* an admin can tick for a person who
is not an admin, and ticking it must not hand that person the purchase price
of everything on the shelf. If somebody gates the route on the `margin`
permission instead of the role, those tests fail.

Sessions are forged as in tests/integration/test_margin_permissions.py, the
TestClient is built without its context manager so no startup job runs, and
the service is stubbed: a 200 proves the handler ran, a 403 that the gate
fired before it.
"""
import time

import pytest
from fastapi.testclient import TestClient

from core.permissions import ADMIN_USER_IDS, invalidate_permissions_cache
from web.main import app
from web.routes.auth import (
    SESSION_COOKIE,
    create_session_data,
    require_admin,
    session_serializer,
)
from web.services import margin_service

PATH = "/api/stock-margin"
ADMIN_ID = sorted(ADMIN_USER_IDS)[0]


def _cookie(user_id: int, role: str) -> dict:
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
    return {"Cookie": f"{SESSION_COOKIE}={session_serializer.dumps(data)}"}


class _FakeStore:
    """Enough of the store to resolve a non-admin session's role and tabs."""

    def __init__(self, role: str, allowed_features=None):
        self._role = role
        self._features = allowed_features

    async def get_user(self, uid):
        return {"status": "approved", "role": self._role,
                "allowed_features": self._features}


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _stub_service(monkeypatch):
    async def _answer(*a, **k):
        return {"items": []}

    monkeypatch.setattr(margin_service, "get_stock_margin", _answer)
    invalidate_permissions_cache()


def _login(monkeypatch, role: str, allowed_features=None) -> dict:
    store = _FakeStore(role, allowed_features)

    async def _fake_get_store():
        return store

    monkeypatch.setattr("core.store.get_store", _fake_get_store)
    uid = 555_000_444
    assert uid not in ADMIN_USER_IDS
    return _cookie(uid, role)


class TestOnlyTheAdminRoleMayRead:
    @pytest.mark.parametrize("role", ["viewer", "editor"])
    def test_a_role_below_admin_is_refused(self, client, monkeypatch, role):
        assert client.get(PATH, headers=_login(monkeypatch, role)).status_code == 403

    @pytest.mark.parametrize("role", ["viewer", "editor"])
    def test_the_margin_tab_ticked_for_a_non_admin_does_not_open_it(self, client, monkeypatch, role):
        headers = _login(monkeypatch, role, allowed_features=["margin"])
        assert client.get(PATH, headers=headers).status_code == 403

    def test_every_tab_ticked_still_does_not_open_it(self, client, monkeypatch):
        from core.permissions import TAB_FEATURES

        headers = _login(monkeypatch, "editor", allowed_features=list(TAB_FEATURES))
        assert client.get(PATH, headers=headers).status_code == 403

    def test_an_admin_by_role_reads_it(self, client, monkeypatch):
        response = client.get(PATH, headers=_login(monkeypatch, "admin"))
        assert response.status_code == 200
        assert response.json() == {"items": []}

    def test_a_hardcoded_admin_reads_it(self, client, monkeypatch):
        store = _FakeStore("admin")

        async def _fake_get_store():
            return store

        monkeypatch.setattr("core.store.get_store", _fake_get_store)
        assert client.get(PATH, headers=_cookie(ADMIN_ID, "admin")).status_code == 200

    def test_no_session_is_401(self, client):
        assert client.get(PATH).status_code == 401


class TestTheGateIsTheRole:
    def test_require_admin_is_in_the_dependency_tree(self):
        from tests.routes_helper import route_dependencies

        assert require_admin in set(route_dependencies(app, PATH, "GET"))

    def test_the_margin_permission_is_not_what_opens_it(self):
        """One gate, the role: a `margin` permission beside it would read as
        the rule and be the weaker one."""
        from tests.routes_helper import route_dependencies

        names = {getattr(dep, "__name__", "") for dep in route_dependencies(app, PATH, "GET")}
        assert "check_permission" not in names
