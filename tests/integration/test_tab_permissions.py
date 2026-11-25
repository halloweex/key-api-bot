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

import pytest
from fastapi.testclient import TestClient

from web.main import app
from core.permissions import ACCESS_PRESETS, ADMIN_USER_IDS, TAB_FEATURE_KEYS
from web.services import dashboard_service
from tests.helpers.access_lists import (
    access_lists_empty, bot_store_on_postgres, dashboard_row, dashboard_user,
    execute, pending_request, rows,
)
from tests.helpers.app_client import signed_in
from tests.helpers.tab_permissions import (  # noqa: F401
    PLAIN_ID, _FakeStore, _cookie, _fresh_permissions, _install_store,
)
from tests.integration.pg_sms_seed import needs_postgres

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]
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
    "tgbot": "/api/tg-bot/activity",
    "expenses": "/api/expenses/summary",
    "sms": "/api/customers/sms-segments",
}


@pytest.fixture
def client():
    return TestClient(app)


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
        # The gate is the subject, not the data: /marketing's target line is
        # chain 7a's table, read from Postgres, and a Postgres that cannot
        # answer behind this client is a 503 — not the 403 asserted against.
        # (Until step 14's S14-11 the chain was routed back to DuckDB here.)
        assert client.get(TAB_ENDPOINTS[key], headers=headers).status_code != 403

    @pytest.mark.parametrize("key", ["margin", "sms", "expenses"])
    def test_and_still_reaches_none_of_the_narrower_ones(
        self, client, monkeypatch, key,
    ):
        headers = _login(monkeypatch, allowed_features=None)
        assert client.get(TAB_ENDPOINTS[key], headers=headers).status_code == 403


class TestAnEmptySetIsNotNoSet:
    """Every tab is enforced on the server: nothing ticked is a 403 on each
    tab's endpoint, and ticking that tab alone opens it. A tab added to
    `TAB_FEATURES` and the sidebar but gated nowhere would be a checkbox that
    hides a link and protects nothing, and fails here."""

    def test_every_tab_has_an_endpoint_here(self):
        assert set(TAB_ENDPOINTS) == set(TAB_FEATURE_KEYS)

    @pytest.mark.parametrize("path", sorted(TAB_ENDPOINTS.values()))
    def test_nothing_ticked_means_nothing_at_all(self, client, monkeypatch, path):
        headers = _login(monkeypatch, allowed_features=[])
        assert client.get(path, headers=headers).status_code == 403

    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", sorted(TAB_ENDPOINTS))
    async def test_ticking_one_tab_opens_its_endpoint(self, monkeypatch, key):
        # A real row holding that one tab, over the test Postgres. The gate is
        # the subject, not the data: over an empty database an endpoint may
        # answer zeros or a 503, never a refused session or a 403.
        async with access_lists_empty(monkeypatch):
            async with signed_in("viewer", tabs=key,
                                 raise_server_exceptions=False) as viewer:
                r = await viewer.get(TAB_ENDPOINTS[key])
        assert r.status_code not in (401, 403), \
            f"the {key!r} tab does not open {TAB_ENDPOINTS[key]}: {r.status_code}"


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


@needs_postgres
class TestTheAdminEndpoint:
    """`PATCH /api/admin/users/{id}/features` as the hardcoded admin, over the
    test Postgres. The column has three states and they must not collapse:
    NULL is "as the role", '' is "nothing ticked", and a list is exactly
    those tabs, in the sidebar's order."""

    PERSON = 7_000_000_501

    @staticmethod
    async def _stored(user_id):
        row = await dashboard_row(user_id)
        return row and row["allowed_features"]

    async def _patch(self, monkeypatch, user_id, body, *, seeded="unset"):
        """Seed the person's row (unless `seeded` is None: no row), PATCH as
        the hardcoded admin, and return (response, the column after)."""
        async with access_lists_empty(monkeypatch):
            if seeded is not None:
                await dashboard_user(user_id, allowed_features=(
                    None if seeded == "unset" else seeded))
            async with signed_in("admin") as admin:
                r = await admin.patch(f"/api/admin/users/{user_id}/features", json=body)
            exists = await dashboard_row(user_id) is not None
            return r, (await self._stored(user_id) if exists else "no row")

    @pytest.mark.asyncio
    async def test_a_list_is_stored_in_tab_order(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, self.PERSON, {"features": ["sms", "traffic"]})
        assert r.status_code == 200
        assert r.json()["allowed_features"] == ["traffic", "sms"]
        assert stored == "traffic,sms"

    @pytest.mark.asyncio
    async def test_a_preset_is_expanded_here_not_on_the_page(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, self.PERSON, {"preset": "traffic_only"})
        assert r.status_code == 200
        assert r.json()["allowed_features"] == ["traffic"]
        assert stored == "traffic"

    @pytest.mark.asyncio
    async def test_null_clears_the_override(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, self.PERSON, {"features": None},
                                      seeded="traffic")
        assert r.status_code == 200
        assert r.json()["allowed_features"] is None
        assert stored is None

    @pytest.mark.asyncio
    async def test_an_empty_list_is_accepted_as_a_decision(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, self.PERSON, {"features": []})
        assert r.status_code == 200
        assert r.json()["allowed_features"] == []
        assert stored == ""

    @pytest.mark.asyncio
    async def test_an_unknown_tab_is_refused_rather_than_dropped(self, monkeypatch):
        """Dropping it would answer 200 having stored something else, and the
        admin would read the checklist as saved."""
        r, stored = await self._patch(
            monkeypatch, self.PERSON, {"features": ["traffic", "user_management"]},
            seeded="marketing")
        assert r.status_code == 400
        assert stored == "marketing"

    @pytest.mark.asyncio
    async def test_an_unknown_preset_is_refused(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, self.PERSON, {"preset": "nope"},
                                      seeded="marketing")
        assert r.status_code == 400
        assert stored == "marketing"

    @pytest.mark.asyncio
    async def test_a_missing_user_is_404(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, self.PERSON, {"features": ["traffic"]},
                                      seeded=None)
        assert r.status_code == 404
        assert stored == "no row"

    @pytest.mark.asyncio
    async def test_a_hardcoded_admin_cannot_be_given_a_tab_set(self, monkeypatch):
        """Those ids open every tab whatever is stored, so storing one would
        report a change that does not happen — `_refuse_if_ineffective`'s
        lesson, applied to the column it did not exist for."""
        r, stored = await self._patch(monkeypatch, ADMIN_ID, {"features": ["traffic"]})
        assert r.status_code == 409
        assert stored is None

    @pytest.mark.asyncio
    async def test_but_it_can_be_put_back_to_as_the_role(self, monkeypatch):
        r, stored = await self._patch(monkeypatch, ADMIN_ID, {"features": None},
                                      seeded="traffic")
        assert r.status_code == 200
        assert stored is None

    @pytest.mark.asyncio
    async def test_a_viewer_cannot_set_anybody_s_tabs(self, monkeypatch):
        async with access_lists_empty(monkeypatch):
            await dashboard_user(self.PERSON, allowed_features="marketing")
            async with signed_in("viewer") as viewer:
                r = await viewer.patch(f"/api/admin/users/{self.PERSON}/features",
                                       json={"features": ["traffic"]})
            assert r.status_code == 403
            assert await self._stored(self.PERSON) == "marketing"


# ─── what happens when the store cannot be read ──────────────────────────────


class TestAnErrorIsNotAnAbsence:
    """The two used to share an exit, and the shared exit granted a `viewer`
    with no tab override — every tab a viewer's role opens.

    For an account with **no row** that is right: there is no tab set to
    contradict. For an account whose row could not be **read** it is the
    opposite of right, because the row may say `traffic` and handing back the
    full set silently undoes an admin's decision at the one moment nothing can
    be verified.
    """

    def _login_with(self, monkeypatch, store) -> dict:
        _install_store(monkeypatch, store)
        monkeypatch.setattr(
            dashboard_service, "parse_period",
            lambda *a, **k: ("2026-08-01", "2026-08-31"),
        )

        async def _trend(*a, **k):
            return {}

        monkeypatch.setattr(dashboard_service, "get_revenue_trend", _trend)
        # The bot's list, which the no-row path consults, says approved.
        monkeypatch.setattr(
            "web.routes.auth.check_user_access",
            lambda uid: {"authorized": True, "status": "approved"},
        )
        return _cookie(PLAIN_ID, "viewer")

    def test_a_read_that_raises_refuses_the_request(self, client, monkeypatch):
        """`SchemaVersionError` is the realistic one: `web` deployed ahead of
        `migrate`, where this read raises while the bot's store — which checks
        the revision only in `initialise()` — keeps answering. Every narrowed
        account used to get its tabs back, at WARNING, until somebody noticed."""

        class Broken(_FakeStore):
            async def get_user(self, uid):
                raise RuntimeError("schema is at 0020, this code requires 0021")

        headers = self._login_with(monkeypatch, Broken("viewer", ["traffic"]))
        assert client.get("/api/revenue/trend", headers=headers).status_code == 401
        assert client.get("/api/traffic/analytics", headers=headers).status_code == 401

    def test_no_row_still_falls_back_to_the_bots_list(self, client, monkeypatch):
        """The migration-era path stays: an approved account the dashboard's
        list has never seen gets a viewer's tabs, which is what it had before
        per-user access existed."""

        class Empty(_FakeStore):
            async def get_user(self, uid):
                return None

        headers = self._login_with(monkeypatch, Empty("viewer", None))
        assert client.get("/api/revenue/trend", headers=headers).status_code == 200

    def test_the_narrowing_holds_when_the_store_answers(self, client, monkeypatch):
        headers = self._login_with(monkeypatch, _FakeStore("viewer", ["traffic"]))
        assert client.get("/api/revenue/trend", headers=headers).status_code == 403


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


# ─── the request itself, in the UI ───────────────────────────────────────────


@needs_postgres
class TestTheAccessRequestQueue:
    """A request lands in the *bot's* list and the dashboard's list gains a row
    only when somebody approves — so an incoming request was invisible on the
    admin page, and the decision could only be taken from a phone.

    These endpoints are the missing door. They do not merge the two lists: they
    read the bot's queue and write exactly the two rows the bot's own Approve
    button writes.

    Over the test Postgres with the bot's store there (`KS_BOT_STORE=postgres`):
    the queue is `app.authorized_users`, the grant `app.dashboard_users`. The
    message to the person is caught at the HTTP transport the route sends it
    through, and what is checked is to whom it went.
    """

    ASKER = 7_000_000_401

    @pytest.fixture
    def told(self, monkeypatch):
        """Who the route wrote to on Telegram, one entry per message."""
        sent = []

        async def _send(text, *a, chat_ids=None, **k):
            sent.append(chat_ids)
            return 1

        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _send)
        return sent

    async def _bot_row(self):
        found = await rows("SELECT status, reviewed_by, denial_count "
                           "FROM app.authorized_users WHERE user_id = $1", self.ASKER)
        return found[0] if found else None

    @pytest.mark.asyncio
    async def test_the_queue_is_listed(self, monkeypatch, tmp_path):
        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await pending_request(self.ASKER, username="asker")
            await execute("INSERT INTO app.authorized_users (user_id, username, status) "
                          "VALUES ($1, 'decided', 'approved')", self.ASKER + 1)
            async with signed_in("admin") as admin:
                r = await admin.get("/api/admin/access-requests")

        assert r.status_code == 200
        assert r.json()["count"] == 1
        assert [(q["user_id"], q["username"]) for q in r.json()["requests"]] == [
            (self.ASKER, "asker")]

    def test_a_viewer_cannot_see_the_queue(self, client, monkeypatch):
        headers = _login(monkeypatch, allowed_features=None)
        assert client.get(
            "/api/admin/access-requests", headers=headers).status_code == 403

    @pytest.mark.asyncio
    async def test_approving_writes_both_rows_and_says_so(self, monkeypatch, tmp_path, told):
        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await pending_request(self.ASKER)
            async with signed_in("admin") as admin:
                r = await admin.post(f"/api/admin/access-requests/{self.ASKER}/approve",
                                     json={"preset": "traffic_only"})

            assert r.status_code == 200
            assert r.json()["allowed_features"] == ["traffic"]
            # the bot's list
            assert await self._bot_row() == {
                "status": "approved", "reviewed_by": ADMIN_ID, "denial_count": 0}
            # the dashboard's list, with the tabs chosen here
            assert await dashboard_row(self.ASKER) == {
                "user_id": self.ASKER, "role": "viewer", "status": "approved",
                "allowed_features": "traffic", "denial_count": 0,
                "reviewed_by": ADMIN_ID}
        # and the person is told, and nobody else
        assert told == [[self.ASKER]]

    @pytest.mark.asyncio
    async def test_omitting_the_body_grants_the_default(self, monkeypatch, tmp_path, told):
        from core.permissions import ACCESS_PRESETS, DEFAULT_PRESET, parse_features

        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await pending_request(self.ASKER)
            async with signed_in("admin") as admin:
                r = await admin.post(f"/api/admin/access-requests/{self.ASKER}/approve",
                                     json={})

            assert r.status_code == 200
            stored = (await dashboard_row(self.ASKER))["allowed_features"]
        assert parse_features(stored) == list(ACCESS_PRESETS[DEFAULT_PRESET])

    @pytest.mark.asyncio
    async def test_a_request_already_decided_is_refused(self, monkeypatch, tmp_path, told):
        """Two admins answering the same request: the second is refused by the
        conditional write on the bot's list, and nothing it asked is stored."""
        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await pending_request(self.ASKER)
            async with signed_in("admin") as admin:
                first = await admin.post(f"/api/admin/access-requests/{self.ASKER}/approve",
                                         json={"preset": "traffic_only"})
                second = await admin.post(f"/api/admin/access-requests/{self.ASKER}/approve",
                                          json={"preset": "standard"})
                deny = await admin.post(f"/api/admin/access-requests/{self.ASKER}/deny")

            assert (first.status_code, second.status_code, deny.status_code) == (200, 409, 409)
            assert (await self._bot_row())["status"] == "approved"
            assert (await dashboard_row(self.ASKER))["allowed_features"] == "traffic"
        assert told == [[self.ASKER]]

    @pytest.mark.asyncio
    async def test_denying_writes_no_dashboard_row(self, monkeypatch, tmp_path, told):
        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await pending_request(self.ASKER)
            async with signed_in("admin") as admin:
                r = await admin.post(f"/api/admin/access-requests/{self.ASKER}/deny")

            assert r.status_code == 200
            assert r.json()["frozen"] is False
            assert await self._bot_row() == {
                "status": "denied", "reviewed_by": ADMIN_ID, "denial_count": 1}
            assert await dashboard_row(self.ASKER) is None, \
                "a refusal is not a decision about a row"
        assert told == [[self.ASKER]]

    @pytest.mark.asyncio
    async def test_an_unknown_tab_is_refused(self, monkeypatch, tmp_path, told):
        async with bot_store_on_postgres(monkeypatch, tmp_path):
            await pending_request(self.ASKER)
            async with signed_in("admin") as admin:
                r = await admin.post(f"/api/admin/access-requests/{self.ASKER}/approve",
                                     json={"features": ["user_management"]})

            assert r.status_code == 400
            assert (await self._bot_row())["status"] == "pending"
            assert await dashboard_row(self.ASKER) is None
        assert told == []


class TestThePageIsToldWhatInheritingMeans:
    """The admin page cannot derive "the tabs this level opens" any more.

    It used to read the permissions matrix and take every feature with `view`.
    That was right while the matrix carried areas; the day it became uniform
    depth, it meant *all nine tabs for every level*. An admin opening an
    inheriting row saw nine lit chips instead of six — and one click would have
    written an explicit eight-tab set, granting margin, expenses and the SMS
    roster by touching an unrelated tab. Reported from the page, with a
    screenshot of nine.
    """

    def test_the_matrix_endpoint_sends_the_level_defaults(self, client, monkeypatch):
        from core.permissions import DEFAULT_TABS

        store = _FakeStore("admin", None)

        async def _seed():
            return None

        async def _all():
            return {}

        store.seed_default_permissions = _seed
        store.get_all_permissions = _all
        _install_store(monkeypatch, store)

        body = client.get(
            "/api/admin/permissions", headers=_cookie(ADMIN_ID, "admin"),
        ).json()

        assert "default_tabs" in body, "the page has to be told, not left to derive"
        assert set(body["default_tabs"]) == set(DEFAULT_TABS)
        assert body["default_tabs"]["viewer"] == list(DEFAULT_TABS["viewer"])
        # `null` is "not narrowed at all", which is what admin means.
        assert body["default_tabs"]["admin"] is None

    def test_a_viewer_default_is_not_every_tab(self):
        """The assertion the bug would have failed."""
        from core.permissions import DEFAULT_TABS, TAB_FEATURE_KEYS

        assert set(DEFAULT_TABS["viewer"]) < set(TAB_FEATURE_KEYS)
        for closed in ("margin", "expenses", "sms"):
            assert closed not in DEFAULT_TABS["viewer"]
