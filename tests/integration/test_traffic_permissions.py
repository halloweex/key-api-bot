"""Authorization tests for the traffic router's write endpoint.

``POST /api/traffic/backfill-utm`` carried no gate beyond the inherited
``api_gate``, which made it the only endpoint under ``/api`` that changes
*shared* state and is reachable by any approved viewer. Walking the live route
table, the only other two mutating routes without ``require_admin`` or a
permission gate are ``PATCH /api/me/preferences``, which writes the caller's
own row, and the TurboSMS webhook, which authenticates itself with a signature.

Its two neighbours in the same module — ``/api/traffic/refresh`` and
``/api/traffic/reclassify`` — both carry ``Depends(require_admin)``, and no
frontend code calls any of the three (``web/frontend/src/api/client.ts`` knows
only the four traffic *reads*), so the missing gate is an oversight rather than
a decision, and closing it locks nobody out of a page.

Why it is not cosmetic: the handler launches a background job that walks up to
1000 days of KeyCRM in 30-day chunks through the process-wide circuit breaker,
and each chunk takes the single DuckDB write lock to run its UPDATEs and a
CHECKPOINT on the event loop of a one-worker, ``cpus: 1.0`` container. Nothing
can stop it once started.

The traffic *reads* are deliberately left open: ``/traffic`` is not behind
``AdminGuard`` in web/frontend/src/App.tsx and its sidebar link is
unconditional, so the transactions table is a viewer-level page by design. The
last class pins that, so hardening the write does not creep across the file.

Sessions are forged the same way as tests/integration/test_security_hardening.py
and tests/integration/test_margin_permissions.py, and the TestClient is built
WITHOUT the context-manager form so no startup event (DuckDB sync, scheduler)
runs. ``_run_backfill`` is replaced by a recorder, so a launch is observed
rather than performed.
"""
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.api import traffic
from web.routes.auth import (
    session_serializer,
    create_session_data,
    require_admin,
    SESSION_COOKIE,
)
from core.permissions import ADMIN_USER_IDS, invalidate_permissions_cache

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]

BACKFILL_PATH = "/api/traffic/backfill-utm"
BACKFILL_STATUS_PATH = "/api/traffic/backfill-utm/status"

# The two neighbours whose gate this file borrows, and the read that must stay
# reachable without one.
ADMIN_TRAFFIC_POSTS = ["/api/traffic/refresh", "/api/traffic/reclassify"]
TRANSACTIONS_PATH = "/api/traffic/transactions"


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
    """Resolves a non-admin session's role, and answers the one read."""

    def __init__(self, role: str):
        self._role = role

    async def get_user(self, uid):
        return {"status": "approved", "role": self._role}

    async def get_traffic_transactions(self, **kwargs):
        return {"transactions": [], "total": 0, "limit": 50, "offset": 0}


def _login(monkeypatch, role: str) -> dict:
    """Install a store for `role` on both call sites and return its cookie.

    Two patch targets: `_resolve_session` imports get_store from
    core.duckdb_store at call time, but web/routes/api/traffic.py bound it into
    its own namespace at import — patching only the first leaves the handler
    bodies reaching the real DuckDB file.
    """
    store = _FakeStore(role)

    async def _fake_get_store():
        return store

    monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)
    monkeypatch.setattr("web.routes.api.traffic.get_store", _fake_get_store)

    uid = ADMIN_ID if role == "hardcoded_admin" else 555_000_444
    assert role == "hardcoded_admin" or uid not in ADMIN_USER_IDS
    return _cookie_header(_make_cookie(uid, role="admin" if role == "hardcoded_admin" else role))


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from web.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture(autouse=True)
def launched(monkeypatch):
    """Record backfill launches instead of running one.

    The module-level status dict is reset around each test because a launch
    latches `running=True`, and a latched flag turns the next POST into an
    "already_running" 200 that proves nothing about the gate.
    """
    calls = []

    async def _record(days: int):
        calls.append(days)

    monkeypatch.setattr(traffic, "_run_backfill", _record)
    traffic._backfill_status.update(running=False, result=None)
    invalidate_permissions_cache()
    yield calls
    traffic._backfill_status.update(running=False, result=None)
    invalidate_permissions_cache()


# ─── The bypass: a viewer must not be able to start the job ───────────────────

class TestBackfillUtmRequiresAdmin:
    """Every non-admin role is refused, and refused *before* the launch."""

    @pytest.mark.parametrize("role", ["viewer", "editor", "marketer"])
    def test_non_admin_is_forbidden(self, client, monkeypatch, launched, role):
        headers = _login(monkeypatch, role)
        r = client.post(BACKFILL_PATH, headers=headers)
        assert r.status_code == 403, \
            f"a {role} started a warehouse-wide backfill: {r.status_code} {r.text}"
        assert launched == [], \
            f"forbidden, but the job was launched anyway: {launched}"

    def test_forbidden_caller_cannot_choose_the_window(self, client, monkeypatch, launched):
        """`days` is caller-supplied up to 1000 — 34 chunks of KeyCRM and 34
        checkpoints on the single writer."""
        headers = _login(monkeypatch, "viewer")
        assert client.post(f"{BACKFILL_PATH}?days=1000", headers=headers).status_code == 403
        assert launched == []


class TestBackfillUtmAllowedForAdmin:
    def test_db_role_admin_may_start_it(self, client, monkeypatch, launched):
        headers = _login(monkeypatch, "admin")
        assert client.post(BACKFILL_PATH, headers=headers).status_code == 200
        assert launched == [730], "the default window is no longer what reaches the job"

    def test_hardcoded_admin_may_start_it(self, client, monkeypatch, launched):
        headers = _login(monkeypatch, "hardcoded_admin")
        assert client.post(BACKFILL_PATH, headers=headers).status_code == 200
        assert launched == [730]


# ─── The status half of the same operation ────────────────────────────────────

class TestBackfillUtmStatusRequiresAdmin:
    """The read half discloses how far an admin-only job has got and how many
    orders are missing attribution. It has no frontend caller either, so it
    follows the mutation rather than standing open beside it."""

    @pytest.mark.parametrize("role", ["viewer", "editor", "marketer"])
    def test_non_admin_is_forbidden(self, client, monkeypatch, role):
        headers = _login(monkeypatch, role)
        assert client.get(BACKFILL_STATUS_PATH, headers=headers).status_code == 403

    def test_admin_is_allowed(self, client, monkeypatch):
        headers = _login(monkeypatch, "admin")
        r = client.get(BACKFILL_STATUS_PATH, headers=headers)
        assert r.status_code == 200
        assert r.json() == {"running": False, "result": None}


# ─── No session is still 401, unaffected by the admin gate ────────────────────

class TestBackfillUtmStillRequiresSession:
    def test_anonymous_post_is_401(self, client):
        assert client.post(BACKFILL_PATH).status_code == 401

    def test_anonymous_status_is_401(self, client):
        assert client.get(BACKFILL_STATUS_PATH).status_code == 401


# ─── Structural: the gate is on the route, not only in the answer ─────────────

class TestTrafficRouteStructure:
    @pytest.mark.parametrize("method,path", [
        ("POST", BACKFILL_PATH),
        ("GET", BACKFILL_STATUS_PATH),
    ])
    def test_backfill_routes_require_admin(self, method, path):
        from tests.routes_helper import find_route, route_dependencies

        assert find_route(app, path, method) is not None, \
            f"{method} {path} is no longer registered — if it moved, its new " \
            f"home must be admin-only and this list must name it"
        assert require_admin in set(route_dependencies(app, path, method)), \
            f"{method} {path} lets any approved viewer drive the warehouse"

    @pytest.mark.parametrize("path", ADMIN_TRAFFIC_POSTS)
    def test_neighbours_keep_their_gate(self, path):
        """The evidence the backfill's gate was chosen from. If these two ever
        lose `require_admin`, the argument for the one above changes too."""
        from tests.routes_helper import route_dependencies

        assert require_admin in set(route_dependencies(app, path, "POST"))


# ─── The other direction: the reads must stay open ────────────────────────────

class TestTrafficReadsStayOpenToViewers:
    """`/traffic` sits in `AppShell` with no `AdminGuard` and an unconditional
    sidebar link, and the transactions table is its main panel — so a viewer
    reaching it is the design, not a leak. The rows carry an order id, a date,
    an amount and the ad identifiers (`_fbc`, `_fbp`, `ttp`, `fbclid`) the page
    renders as evidence pills; no name, phone, email or buyer id, which are the
    fields this codebase actually gates. A `customers`/`view` gate here would
    also deny nobody — all four roles hold it.
    """

    @pytest.mark.parametrize("role", ["viewer", "editor", "marketer"])
    def test_transactions_answers_a_non_admin(self, client, monkeypatch, role):
        headers = _login(monkeypatch, role)
        r = client.get(f"{TRANSACTIONS_PATH}?period=month", headers=headers)
        assert r.status_code == 200, \
            f"the traffic page broke for {role}: {r.status_code} {r.text}"

    def test_transactions_still_requires_a_session(self, client):
        assert client.get(TRANSACTIONS_PATH).status_code == 401
