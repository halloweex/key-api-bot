"""HTTP contract for the manager classification endpoints.

A manager's classification decides `sales_type`, and every dashboard endpoint
defaults to `Query("retail")` — so a manager nobody has classified sells into
`internal` and appears on no page but the admin's. KeyCRM does not carry that
fact, which makes these two endpoints the only way it can ever be set. They
must be admin-only. The list is read over the test Postgres
(`TestTheListOverPostgres`); what a classification writes — the interval from
today in Kyiv when no date is given, the derivation signal in the same
transaction — is `test_managers_chain_writer.py`'s.
"""
from datetime import date
import os
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer
from core.permissions import ADMIN_USER_IDS
from tests.helpers import managers_fixture
from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import needs_postgres, tab_in_postgres

DSN = os.getenv("KS_PG_DSN")

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

LIST_PATH = "/api/managers"
SET_PATH = "/api/managers/34/retail-status"


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
    def __init__(self):
        self.set_calls = []

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
        # The real method raises the Postgres derivation signal in the same
        # transaction (proved in tests/integration/test_managers_chain_writer.py,
        # `test_the_route_writes_postgres_marks_the_derivation_and_lists_it`);
        # this fake records the call and nothing else. It also marked a DuckDB
        # dirty flag here, until step 14's S14-15 deleted the flag.
        self.set_calls.append((manager_id, is_retail, effective_from, set_by, note))


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

    def test_a_viewer_can_neither_list_nor_reclassify(self, client, monkeypatch, store):
        viewer_id = 555_000_333
        assert viewer_id not in ADMIN_USER_IDS

        async def _fake_resolve(session):
            return {"user_id": viewer_id, "role": "viewer"}

        monkeypatch.setattr("web.routes.auth._resolve_session", _fake_resolve)
        client.cookies.set(SESSION_COOKIE, _make_cookie(viewer_id, role="viewer"))

        assert client.get(LIST_PATH).status_code == 403
        assert client.post(f"{SET_PATH}?is_retail=true").status_code == 403
        assert store.set_calls == []


class TestBehaviour:
    @pytest.fixture(autouse=True)
    def _as_admin(self, client):
        client.cookies.set(SESSION_COOKIE, _make_cookie(ADMIN_ID))

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


@needs_postgres
class TestTheListOverPostgres:
    @pytest.mark.asyncio
    async def test_each_manager_shows_the_sales_type_silver_gave_and_a_year_of_revenue(
            self, monkeypatch):
        """The label is Silver's, not derived from `is_retail`: that is FALSE
        for the wholesale manager too, and deriving it called ₴15.5M of b2b
        `other`. Staff sell `internal`; a manager who sold nothing has no
        label and no revenue; a return, an order from before the year and an
        order with no manager count for nobody."""
        async with tab_in_postgres(DSN, monkeypatch, managers_fixture.populate):
            async with signed_in("admin") as admin:
                res = await admin.get(LIST_PATH)

        assert res.status_code == 200, res.text
        assert res.json()["status"] == "ok"
        listed = [(m["id"], m["is_retail"], m["sales_type"], m["revenue_365d"])
                  for m in res.json()["managers"]]
        assert listed == managers_fixture.EXPECTED_LIST
