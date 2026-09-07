"""Per-user tab access, enforced on the server.

The point of the whole change is that "this person sees only /traffic" is a
*permission*, not a hidden link. Until 2026-09-07 only `expenses` and `sms`
were gated at all; every other page's endpoints were reachable by any approved
session, so hiding a tab in the sidebar would have been decoration — a URL
typed by hand, or a bookmark, still returned the data.

So these tests are the half that matters: an account granted one tab gets 403
on the others, and the tab it was granted still works.

Sessions are forged the way tests/integration/test_margin_permissions.py forges
them, and the TestClient is built WITHOUT the context-manager form so no
startup event runs.
"""
import time

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    SESSION_COOKIE,
    create_session_data,
    session_serializer,
)
from core.permissions import (
    ACCESS_PRESETS,
    ADMIN_USER_IDS,
    TAB_FEATURE_KEYS,
    invalidate_permissions_cache,
)
from web.services import dashboard_service

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]
PLAIN_ID = 555_000_444
assert PLAIN_ID not in ADMIN_USER_IDS

# One read per tab, chosen because it is what the page loads first.
TAB_ENDPOINTS = {
    "dashboard": "/api/revenue/trend",
    "products": "/api/products/intel/summary",
    "traffic": "/api/traffic/analytics",
    "inventory": "/api/stocks/summary",
    "reports": "/api/reports/summary",
    "marketing": "/api/reports/marketing-summary",
    "margin": "/api/margin/overview",
    "expenses": "/api/expenses/summary",
    "sms": "/api/customers/sms-segments",
}


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
    """Enough of the store for `_resolve_session` and one traffic read."""

    def __init__(self, role: str, allowed_features):
        self._role = role
        self._features = allowed_features

    async def get_user(self, uid):
        return {
            "status": "approved",
            "role": self._role,
            "allowed_features": self._features,
        }

    async def get_traffic_analytics(self, **kwargs):
        return {"summary": {}}

    async def set_user_features(self, user_id, features, changed_by):
        self.written = (user_id, features, changed_by)
        return user_id != 404


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _fresh_permissions():
    # The role matrix is cached process-wide; a stale copy would make these
    # pass or fail depending on test order.
    invalidate_permissions_cache()
    yield
    invalidate_permissions_cache()


# Each route module does `from ._deps import get_store` at import time, so it
# holds its own reference: patching `_deps` alone leaves every handler talking
# to the real store. Named here rather than discovered per test, because the
# failure mode is a green test that opened a database.
_STORE_HOLDERS = (
    "core.duckdb_store.get_store",
    "web.routes.api._deps.get_store",
    "web.routes.api.traffic.get_store",
    "web.routes.api.users.get_store",
    "web.routes.api.analytics.get_store",
)


def _install_store(monkeypatch, store) -> None:
    async def _fake_get_store():
        return store

    for target in _STORE_HOLDERS:
        monkeypatch.setattr(target, _fake_get_store)


def _login(monkeypatch, role="viewer", allowed_features=None) -> dict:
    _install_store(monkeypatch, _FakeStore(role, allowed_features))
    monkeypatch.setattr(
        dashboard_service, "parse_period",
        lambda *a, **k: ("2026-08-01", "2026-08-31"),
    )
    return _cookie(PLAIN_ID, role)


# ─── the traffic-only account, which is the case that was asked for ──────────


class TestOneTabOnly:
    @pytest.mark.parametrize(
        "path",
        [p for key, p in TAB_ENDPOINTS.items() if key != "traffic"],
    )
    def test_every_other_tab_is_refused(self, client, monkeypatch, path):
        headers = _login(monkeypatch, allowed_features=["traffic"])
        assert client.get(path, headers=headers).status_code == 403

    def test_the_granted_tab_answers(self, client, monkeypatch):
        headers = _login(monkeypatch, allowed_features=["traffic"])
        assert client.get(TAB_ENDPOINTS["traffic"], headers=headers).status_code == 200

    def test_the_shared_totals_still_answer(self, client, monkeypatch):
        """`/api/summary` is the dashboard's cards *and* the ROAS block on
        /traffic and the ROI calculator on /marketing. Gating it on `dashboard`
        alone would leave a traffic-only account looking at empty tiles."""
        headers = _login(monkeypatch, allowed_features=["traffic"])

        async def _summary(*a, **k):
            return {"total_revenue": 0}

        monkeypatch.setattr(dashboard_service, "get_summary_stats", _summary)
        assert client.get("/api/summary", headers=headers).status_code == 200

    def test_the_filter_lookups_still_answer(self, client, monkeypatch):
        """The header's filter dropdowns are on every page. Gating them per
        tab would break the chrome of the one page the account can open."""
        headers = _login(monkeypatch, allowed_features=["traffic"])

        async def _empty():
            return []

        store = _FakeStore("viewer", ["traffic"])
        store.get_categories = _empty
        store.get_brands = _empty
        _install_store(monkeypatch, store)

        assert client.get("/api/categories", headers=headers).status_code == 200
        assert client.get("/api/brands", headers=headers).status_code == 200


class TestNoOverrideChangesNothing:
    """The 24 accounts that existed when this shipped carry `None`, and must
    see exactly what a viewer saw the day before."""

    @pytest.mark.parametrize("key", ACCESS_PRESETS["standard"])
    def test_a_plain_viewer_still_reaches_every_standard_tab(
        self, client, monkeypatch, key,
    ):
        headers = _login(monkeypatch, allowed_features=None)
        assert client.get(TAB_ENDPOINTS[key], headers=headers).status_code != 403

    @pytest.mark.parametrize("key", ["margin", "sms", "expenses"])
    def test_and_still_reaches_none_of_the_narrower_ones(
        self, client, monkeypatch, key,
    ):
        headers = _login(monkeypatch, allowed_features=None)
        assert client.get(TAB_ENDPOINTS[key], headers=headers).status_code == 403


class TestAnEmptySetIsNotNoSet:
    @pytest.mark.parametrize("path", sorted(TAB_ENDPOINTS.values()))
    def test_nothing_ticked_means_nothing_at_all(self, client, monkeypatch, path):
        headers = _login(monkeypatch, allowed_features=[])
        assert client.get(path, headers=headers).status_code == 403


# ─── what the page is told ───────────────────────────────────────────────────


class TestWhatMeReports:
    def test_it_reports_the_narrowed_matrix(self, client, monkeypatch):
        headers = _login(monkeypatch, allowed_features=["traffic"])
        body = client.get("/api/me", headers=headers).json()

        assert body["permissions"]["traffic"]["view"] is True
        assert body["permissions"]["dashboard"]["view"] is False
        assert body["allowed_features"] == ["traffic"]

    def test_no_override_reports_null(self, client, monkeypatch):
        headers = _login(monkeypatch, allowed_features=None)
        body = client.get("/api/me", headers=headers).json()
        assert body["allowed_features"] is None
        assert body["permissions"]["dashboard"]["view"] is True

    def test_a_hardcoded_admin_is_told_it_has_everything(self, client, monkeypatch):
        """Those ids short-circuit every gate on the server. If the page were
        told less, the sidebar would hide tabs the API happily serves — which
        reads as a broken dashboard, not as a permission."""
        _install_store(monkeypatch, _FakeStore("viewer", ["traffic"]))
        body = client.get("/api/me", headers=_cookie(ADMIN_ID, "admin")).json()
        assert all(body["permissions"][key]["view"] for key in TAB_FEATURE_KEYS)
        assert body["allowed_features"] is None


# ─── setting them from the admin page ────────────────────────────────────────


class TestTheAdminEndpoint:
    def _admin(self, monkeypatch):
        store = _FakeStore("admin", None)
        _install_store(monkeypatch, store)
        return store, _cookie(ADMIN_ID, "admin")

    def test_a_list_is_stored_in_tab_order(self, client, monkeypatch):
        store, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/77/features",
            headers=headers, json={"features": ["sms", "traffic"]},
        )
        assert response.status_code == 200
        assert response.json()["allowed_features"] == ["traffic", "sms"]
        assert store.written[1] == ["traffic", "sms"]

    def test_a_preset_is_expanded_here_not_on_the_page(self, client, monkeypatch):
        store, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/77/features",
            headers=headers, json={"preset": "traffic_only"},
        )
        assert response.status_code == 200
        assert response.json()["allowed_features"] == ["traffic"]

    def test_null_clears_the_override(self, client, monkeypatch):
        store, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/77/features", headers=headers, json={"features": None},
        )
        assert response.status_code == 200
        assert response.json()["allowed_features"] is None
        assert store.written[1] is None

    def test_an_empty_list_is_accepted_as_a_decision(self, client, monkeypatch):
        store, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/77/features", headers=headers, json={"features": []},
        )
        assert response.status_code == 200
        assert response.json()["allowed_features"] == []
        assert store.written[1] == []

    def test_an_unknown_tab_is_refused_rather_than_dropped(self, client, monkeypatch):
        """Dropping it would answer 200 having stored something else, and the
        admin would read the checklist as saved."""
        _, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/77/features",
            headers=headers, json={"features": ["traffic", "user_management"]},
        )
        assert response.status_code == 400

    def test_an_unknown_preset_is_refused(self, client, monkeypatch):
        _, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/77/features", headers=headers, json={"preset": "nope"},
        )
        assert response.status_code == 400

    def test_a_missing_user_is_404(self, client, monkeypatch):
        _, headers = self._admin(monkeypatch)
        response = client.patch(
            "/api/admin/users/404/features",
            headers=headers, json={"features": ["traffic"]},
        )
        assert response.status_code == 404

    def test_a_hardcoded_admin_cannot_be_given_a_tab_set(self, client, monkeypatch):
        """Those ids open every tab whatever is stored, so storing one would
        report a change that does not happen — `_refuse_if_ineffective`'s
        lesson, applied to the column it did not exist for."""
        _, headers = self._admin(monkeypatch)
        response = client.patch(
            f"/api/admin/users/{ADMIN_ID}/features",
            headers=headers, json={"features": ["traffic"]},
        )
        assert response.status_code == 409

    def test_but_it_can_be_put_back_to_as_the_role(self, client, monkeypatch):
        store, headers = self._admin(monkeypatch)
        response = client.patch(
            f"/api/admin/users/{ADMIN_ID}/features",
            headers=headers, json={"features": None},
        )
        assert response.status_code == 200
        assert store.written[1] is None

    def test_a_viewer_cannot_set_anybody_s_tabs(self, client, monkeypatch):
        headers = _login(monkeypatch, allowed_features=None)
        response = client.patch(
            "/api/admin/users/77/features",
            headers=headers, json={"features": ["traffic"]},
        )
        assert response.status_code == 403


# ─── the cost of asking twice ────────────────────────────────────────────────


class _CountingStore(_FakeStore):
    def __init__(self, role="viewer", allowed_features=None):
        super().__init__(role, allowed_features)
        self.reads = 0
        self.status = "approved"

    async def get_user(self, uid):
        self.reads += 1
        return {
            "status": self.status,
            "role": self._role,
            "allowed_features": self._features,
        }

    async def get_role_permissions(self, role):
        return {}

    async def seed_default_permissions(self):
        return None

    async def get_stats(self):
        return {}


class TestTheSessionIsResolvedOncePerRequest:
    """Two dependencies ask who this is on every gated endpoint — `api_gate`
    at the include, and the permission gate on the route. Each ask used to be
    a full resolution: a signature check and a read of `app.dashboard_users`.

    That read carries `require_revision()`, which takes a pool connection of
    its own before the query takes a second, so a doubled resolution is four
    round trips per request against a pool of five. It was invisible while only
    `expenses` and `sms` were gated; per-user tabs put a gate on roughly 120 of
    140 endpoints.
    """

    @pytest.mark.parametrize("path", [
        "/api/traffic/analytics",   # a tab gate
        "/api/summary",             # a shared gate
        "/api/me",                  # require_user on top of api_gate
        "/api/categories",          # no gate at all — the control
    ])
    def test_one_store_read(self, client, monkeypatch, path):
        store = _CountingStore(allowed_features=["traffic"])

        async def _empty():
            return []

        store.get_categories = _empty
        _install_store(monkeypatch, store)
        monkeypatch.setattr(
            dashboard_service, "parse_period",
            lambda *a, **k: ("2026-08-01", "2026-08-31"),
        )

        async def _summary(*a, **k):
            return {"total_revenue": 0}

        monkeypatch.setattr(dashboard_service, "get_summary_stats", _summary)

        assert client.get(path, headers=_cookie(PLAIN_ID, "viewer")).status_code == 200
        assert store.reads == 1

    def test_an_admin_route_asks_once_too(self, client, monkeypatch):
        """`require_admin` stacks on `api_gate` the same way a tab gate does,
        so it paid the same double read."""
        store = _CountingStore(role="admin")

        async def _list_users(**kwargs):
            return []

        store.list_users = _list_users
        _install_store(monkeypatch, store)
        assert client.get(
            "/api/admin/users", headers=_cookie(PLAIN_ID, "admin"),
        ).status_code == 200
        assert store.reads == 1

    def test_the_cache_dies_with_the_request(self, client, monkeypatch):
        """It has to, or revocation would stop working — the session cookie is
        a stateless signed token, and the only thing that withdraws access is
        the *next* request finding the account no longer approved."""
        store = _CountingStore(allowed_features=["traffic"])
        _install_store(monkeypatch, store)
        headers = _cookie(PLAIN_ID, "viewer")

        assert client.get("/api/traffic/analytics", headers=headers).status_code == 200
        store.status = "denied"
        assert client.get("/api/traffic/analytics", headers=headers).status_code == 401
        assert store.reads == 2   # one per request, not one per process


# ─── structural: a tab nobody enforces is decoration ─────────────────────────


class TestEveryTabIsEnforcedSomewhere:
    @pytest.mark.parametrize("feature", sorted(TAB_FEATURE_KEYS))
    def test_at_least_one_route_is_gated_on_it(self, feature):
        """Adding a tab to `TAB_FEATURES` and to the sidebar, and forgetting
        the server, gives an admin a checkbox that hides a link and protects
        nothing. This fails in that case."""
        from tests.routes_helper import route_dependencies, iter_endpoints

        for endpoint in iter_endpoints(app):
            for dep in endpoint.dependencies:
                if getattr(dep, "__name__", "") not in ("check_permission", "check_any"):
                    continue
                for cell in dep.__closure__ or ():
                    value = cell.cell_contents
                    if value == feature or (
                        isinstance(value, (list, tuple)) and feature in value
                    ):
                        return
        raise AssertionError(f"no route is gated on the {feature!r} tab")
