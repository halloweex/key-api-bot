"""HTTP contract for the manager classification endpoints.

`managers.is_retail` decides `sales_type`, and every dashboard endpoint
defaults to `Query("retail")` — so a manager nobody has classified sells into
`other` and appears on no page. KeyCRM does not carry that fact, which makes
these two endpoints the only way it can ever be set. They must be admin-only,
and setting it must mark the warehouse dirty: `sales_type` is materialised
into Silver, so a classification that never triggers a rebuild changes nothing
a human can see.
"""
from datetime import date
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

LIST_PATH = "/api/managers"
SET_PATH = "/api/managers/34/retail-status"
# The router registers the template, not the concrete id.
SET_ROUTE = "/api/managers/{manager_id}/retail-status"


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


def _all_dep_calls(dependant) -> set:
    calls = set()
    for d in dependant.dependencies:
        if d.call is not None:
            calls.add(d.call)
        calls |= _all_dep_calls(d)
    return calls


def _route(path: str, method: str = "GET"):
    for r in app.routes:
        if getattr(r, "path", None) == path and method in getattr(r, "methods", set()):
            return r
    return None


class _FakeConn:
    """Silver grouped by (manager_id, sales_type), as the endpoint reads it."""

    def execute(self, sql, params=None):
        self.sql = sql
        return self

    def fetchall(self):
        return [
            (34, "other", 230_300.0),
            (22, "retail", 8_215_717.77),
            (15, "b2b", 15_549_070.0),
            (None, "retail", 1_000_000.0),
        ]


class _FakeStore:
    def __init__(self):
        self.set_calls = []
        self.dirty_calls = []

    async def get_all_managers(self):
        return [
            {"id": 22, "name": "Retail One", "is_retail": True, "order_count": 900},
            {"id": 34, "name": "Unclassified", "is_retail": False, "order_count": 120},
            {"id": 15, "name": "Wholesale", "is_retail": False, "order_count": 1160},
            {"id": 99, "name": "Never Sold", "is_retail": False, "order_count": 0},
        ]

    async def set_manager_retail_status(
        self, manager_id, is_retail, effective_from=None, set_by=None, note=None
    ):
        self.set_calls.append((manager_id, is_retail, effective_from, set_by, note))
        # Mirrors the real method, which marks the warehouse dirty itself so
        # that every caller triggers the rebuild — not only the route. A fake
        # that omits it would let this test pass while production regressed.
        await self.mark_warehouse_dirty(None)

    async def mark_warehouse_dirty(self, changed_order_ids=None):
        self.dirty_calls.append(changed_order_ids)

    def connection(self):
        store = self

        class _Ctx:
            async def __aenter__(self):
                return _FakeConn()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


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

    monkeypatch.setattr("web.routes.api.admin.get_store", _fake_get_store)
    return fake


class TestAuthorization:
    def test_listing_requires_a_session(self, client):
        assert client.get(LIST_PATH).status_code == 401

    def test_setting_requires_a_session(self, client):
        assert client.post(f"{SET_PATH}?is_retail=true").status_code == 401

    @pytest.mark.parametrize("path,method", [(LIST_PATH, "GET"), (SET_ROUTE, "POST")])
    def test_admin_dependency_is_present(self, path, method):
        route = _route(path, method)
        assert route is not None, f"{method} {path} is not registered"
        assert require_admin in _all_dep_calls(route.dependant)

    def test_viewer_cannot_reclassify(self, client, monkeypatch, store):
        viewer_id = 555_000_333
        assert viewer_id not in ADMIN_USER_IDS

        async def _fake_resolve(session):
            return {"user_id": viewer_id, "role": "viewer"}

        monkeypatch.setattr("web.routes.auth._resolve_session", _fake_resolve)
        client.cookies.set(SESSION_COOKIE, _make_cookie(viewer_id, role="viewer"))

        assert client.post(f"{SET_PATH}?is_retail=true").status_code == 403
        assert store.set_calls == []


class TestBehaviour:
    @pytest.fixture(autouse=True)
    def _as_admin(self, client):
        client.cookies.set(SESSION_COOKIE, _make_cookie(ADMIN_ID))

    def test_listing_shows_what_an_unclassified_manager_sells(self, client, store):
        body = client.get(LIST_PATH).json()
        assert body["status"] == "ok"

        by_id = {m["id"]: m for m in body["managers"]}
        assert by_id[34]["sales_type"] == "other"
        assert by_id[34]["revenue_365d"] == 230_300.0
        assert by_id[22]["sales_type"] == "retail"

    def test_the_b2b_manager_is_not_reported_as_unclassified(self, client, store):
        """is_retail is FALSE for wholesale too — which is not the same thing.
        Deriving the label from it called ₴15.5M of B2B `other`."""
        by_id = {m["id"]: m for m in client.get(LIST_PATH).json()["managers"]}

        assert by_id[15]["sales_type"] == "b2b"
        assert by_id[15]["revenue_365d"] == 15_549_070.0

    def test_a_manager_who_has_sold_nothing_has_no_sales_type(self, client, store):
        """Silver says nothing about them, so neither does this."""
        by_id = {m["id"]: m for m in client.get(LIST_PATH).json()["managers"]}

        assert by_id[99]["sales_type"] is None
        assert by_id[99]["revenue_365d"] == 0.0

    def test_setting_marks_the_warehouse_dirty(self, client, store):
        """sales_type lives in Silver; without a rebuild nothing changes on screen."""
        res = client.post(f"{SET_PATH}?is_retail=true")

        assert res.status_code == 200
        assert len(store.set_calls) == 1
        manager_id, is_retail, effective_from, _set_by, _note = store.set_calls[0]
        assert (manager_id, is_retail) == (34, True)
        assert effective_from is None, "no date given means today, decided in the store"
        assert store.dirty_calls == [None], "must request a FULL rebuild"

    def test_classification_is_forward_dated_unless_backdated(self, client, store):
        """A reclassification must not restate last year's reports by default.

        The route carries the date; the store turns it into an interval. What
        this pins is that a caller who says nothing gets *today*, and only an
        explicit date reaches back — that asymmetry is the owner's 2026-08-20
        decision, and it used to be impossible to express at all.
        """
        res = client.post(f"{SET_PATH}?is_retail=true&effective_from=2026-01-01")

        assert res.status_code == 200
        assert store.set_calls[0][2] == date(2026, 1, 1)
        assert res.json()["effective_from"] == "2026-01-01"

    def test_unknown_manager_is_rejected(self, client, store):
        res = client.post("/api/managers/99999/retail-status?is_retail=true")

        assert res.status_code == 404
        assert store.set_calls == []
        assert store.dirty_calls == []
