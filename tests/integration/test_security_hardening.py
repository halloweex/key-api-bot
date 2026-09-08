"""
Integration tests for the pre-launch security hardening.

Covers the audit findings fixed on the `security-hardening` branch:
- #1/#2  The /api/* surface requires an authenticated session
- #3      WebSocket endpoints require a session
- #4      A single shared rate-limiter instance is used everywhere
- #10     The admin/ops router is admin-only; /api/health stays public
- #12     Telegram HMAC verification is constant-time and rejects tampering
- #13     The SPA catch-all blocks path traversal

These tests intentionally construct the TestClient WITHOUT the context-manager
form, so FastAPI startup events (DuckDB sync, scheduler) do not run. Sessions
are forged for a hardcoded admin id, whose auth path does not touch the DB.
"""
import hashlib
import hmac
import time

import pytest
from starlette.requests import Request
from starlette.websockets import WebSocketDisconnect
from fastapi import HTTPException
from fastapi.testclient import TestClient

from web.main import app
from web.routes.auth import (
    session_serializer,
    create_session_data,
    api_gate,
    require_user,
    require_admin,
    get_current_user_ws,
    PUBLIC_API_PATHS,
    SESSION_COOKIE,
)
from core.permissions import ADMIN_USER_IDS

ADMIN_ID = sorted(ADMIN_USER_IDS)[0]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_cookie(user_id: int, role: str = "admin") -> str:
    """Build a validly-signed session cookie value."""
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


def _all_dep_calls(dependant) -> set:
    """Flatten every callable in a route's resolved dependency tree."""
    calls = set()
    for d in dependant.dependencies:
        if d.call is not None:
            calls.add(d.call)
        calls |= _all_dep_calls(d)
    return calls


def _route(path: str, method: str = "GET"):
    # `app.routes` is no longer flat — see tests/routes_helper.
    from tests.routes_helper import find_route

    return find_route(app, path, method)


def _deps(path: str, method: str = "GET") -> set:
    """Every dependency callable applying to an endpoint.

    Takes the path rather than a route object: on the fastapi production runs,
    `require_admin` is attached at `include_router(...)` and lives in the
    include context, not on the route's own `dependant`. Reading only the
    dependant would report every admin endpoint as unprotected.
    """
    from tests.routes_helper import route_dependencies

    return set(route_dependencies(app, path, method))


@pytest.fixture
def client():
    """TestClient without startup events (no DuckDB / scheduler)."""
    return TestClient(app)


# ─── #1 / #2  API surface requires authentication ─────────────────────────────

class TestApiRequiresAuth:
    """Every data/mutation endpoint must reject unauthenticated callers."""

    PROTECTED_GETS = [
        "/api/summary",
        "/api/categories",
        "/api/customers/insights",
        "/api/expenses/summary",
        "/api/margin/overview",
        "/api/stocks/summary",
        "/api/products/intel/summary",
        "/api/reports/marketing-summary",
        "/api/health/detailed",
        "/api/duckdb/stats",
        # The health router's only public path is /api/health itself; the four
        # operational reads beside it must each still refuse an anonymous
        # caller. What they disclose to an *approved* one is a separate
        # question, answered in tests/integration/test_health_router_disclosure.py.
        "/api/health/data-quality",
        "/api/metrics",
        "/ws/stats",
    ]

    PROTECTED_POSTS = [
        "/api/revenue/forecast/train",
        "/api/revenue/forecast/tune",
        "/api/expenses",
        "/api/stocks/snapshot/refresh",
        "/api/duckdb/sync-buyers",
    ]

    @pytest.mark.parametrize("path", PROTECTED_GETS)
    def test_protected_get_returns_401_without_session(self, client, path):
        assert client.get(path).status_code == 401

    @pytest.mark.parametrize("path", PROTECTED_POSTS)
    def test_protected_post_returns_401_without_session(self, client, path):
        assert client.post(path, json={}).status_code == 401

    def test_malformed_session_cookie_rejected(self, client):
        r = client.get("/api/summary", headers=_cookie_header("not-a-valid-token"))
        assert r.status_code == 401

    def test_tampered_session_cookie_rejected(self, client):
        good = _make_cookie(ADMIN_ID)
        tampered = good[:-3] + ("aaa" if not good.endswith("aaa") else "bbb")
        r = client.get("/api/summary", headers=_cookie_header(tampered))
        assert r.status_code == 401


# ─── #10  Admin router is admin-only; /api/health stays public ────────────────

class TestAuthorizationStructure:
    """Structural checks on the resolved dependency tree (DB-free, deterministic)."""

    def test_health_is_listed_public(self):
        # The single source of truth for what's reachable without a session.
        #
        # /api/health          — polled by Docker, nginx and uptime monitors.
        # /api/webhooks/turbosms — called by the SMS gateway, which cannot hold
        #   a session; it authenticates with a SHA1 signature over a shared
        #   secret and fails closed when the secret is unset. Signature
        #   behaviour is pinned in tests/integration/test_turbosms_webhook.py.
        assert PUBLIC_API_PATHS == {"/api/health", "/api/webhooks/turbosms"}, \
            "PUBLIC_API_PATHS drifted — every entry must be a deliberate, audited exception"

    def test_every_api_route_is_under_api_gate(self):
        """Iterate ALL registered /api/* routes and assert each carries api_gate.

        Previously this was a hand-curated path list of 6 spot-checks; a new
        endpoint added on a router that escapes the include-level gate (the
        chat router was such a case) would not be caught. Now we walk the
        live route table — every /api/* route must have api_gate in its
        resolved dependency tree, period.
        """
        from tests.routes_helper import iter_endpoints

        leaked = []
        for endpoint in iter_endpoints(app):
            if not endpoint.path.startswith("/api/"):
                continue
            # `api_gate` is applied at the `/api` include, so it is inherited
            # rather than present on the route's own dependant.
            if api_gate not in endpoint.dependencies:
                methods = ",".join(sorted(endpoint.methods or {"?"}))
                leaked.append(f"{methods} {endpoint.path}")
        assert not leaked, \
            "audit-invariant drift — /api/* routes outside api_gate:\n  " + "\n  ".join(leaked)

    @pytest.mark.parametrize("path", ["/api/jobs", "/api/sync/stats", "/api/warehouse/status", "/api/bronze/stats"])
    def test_admin_ops_endpoints_require_admin(self, path):
        route = _route(path)
        assert route is not None
        assert require_admin in _deps(path), \
            f"{path} dropped require_admin — admin→user downgrade regression"

    def test_admin_endpoint_403_for_non_admin(self, client, monkeypatch):
        """A logged-in viewer (not a hardcoded admin) is forbidden from /api/jobs."""
        viewer_id = 555_000_111
        assert viewer_id not in ADMIN_USER_IDS

        class _Store:
            async def get_user(self, uid):
                return {"status": "approved", "role": "viewer"}

        async def _fake_get_store():
            return _Store()

        # _resolve_session imports get_store from core.duckdb_store at call time
        monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)

        r = client.get("/api/jobs", headers=_cookie_header(_make_cookie(viewer_id, role="viewer")))
        assert r.status_code == 403


# ─── Customer PII is admin-only ───────────────────────────────────────────────

# Every path that answers with a named customer — full_name, phone, email, city,
# manager, and that person's order history. The ids are small sequential
# integers, so one of these losing its admin dependency does not leak a record,
# it leaks the customer base by counting up.
CUSTOMER_PII_PATHS = [
    "/api/buyers/{buyer_id}",
    "/api/orders/{order_id}",
    "/api/search",
    "/api/search/buyers",
    "/api/search/orders",
]

# The two accessors that reach buyer and order rows: the search service reads
# them straight out of DuckDB, the chat service through its tools. Naming the
# readers rather than the routes is what lets the scan below survive an endpoint
# moving to another module — it follows the data, not the URL.
#
# Deliberately conservative: the product endpoints hold no PII but share the
# accessor, so they are swept in too. Opening product search to viewers is a
# real decision, and it should have to be made here rather than by adding a
# route nobody re-reads this list over.
PII_SERVICE_ACCESSORS = {"get_search_service", "get_chat_service"}


def _handler_reaches(fn, names: set) -> bool:
    """Does this endpoint's own body call one of `names`?

    Unwrapped first because a `@limiter.limit` endpoint is a slowapi wrapper
    whose co_names are the limiter's; without this, adding a rate limit to a
    PII route would silently drop it out of the scan.
    """
    import inspect

    code = getattr(inspect.unwrap(fn), "__code__", None)
    return code is not None and bool(names & set(code.co_names))


class TestCustomerPiiIsAdminOnly:
    """The entire defence for these endpoints is one `dependencies=[...]`
    argument on the chat include in web/main.py. Nothing in web/routes/chat.py
    states or enforces it, so a route split onto its own router, moved to
    another module, or added beside the existing ones inherits whatever gate
    happens to be there — and any approved viewer is then reading names and
    phone numbers. There is no per-object ownership to check here (every admin
    legitimately sees every customer), so the admin dependency is the whole
    authorization model and it needs pinning where it can be seen.
    """

    @pytest.mark.parametrize("path", CUSTOMER_PII_PATHS)
    def test_pii_path_requires_admin(self, path):
        assert _route(path) is not None, \
            f"{path} is no longer registered — if it moved, its new home must " \
            f"be admin-only and this list must name it"
        assert require_admin in _deps(path), \
            f"{path} serves customer PII to any approved viewer — admin→viewer downgrade"

    def test_every_pii_reader_endpoint_requires_admin(self):
        """Any endpoint reaching the buyer/order readers, wherever it lives.

        The parametrised list above catches a path that survives a move with a
        weaker gate; this catches the other direction — a *new* endpoint on any
        router that reads the same rows under a new URL the list has never
        heard of.
        """
        from tests.routes_helper import iter_endpoints

        leaked = []
        for endpoint in iter_endpoints(app):
            handler = getattr(endpoint.route, "endpoint", None)
            if handler is None or not _handler_reaches(handler, PII_SERVICE_ACCESSORS):
                continue
            if require_admin not in endpoint.dependencies:
                methods = ",".join(sorted(endpoint.methods or {"?"}))
                leaked.append(f"{methods} {endpoint.path}")
        assert not leaked, \
            "customer PII readable without require_admin:\n  " + "\n  ".join(leaked)

    def test_approved_viewer_is_forbidden_from_buyer_details(self, client, monkeypatch):
        """The structural checks read the route table; this reads the answer.

        A dependency present in the tree but never reached — a handler that
        resolves it lazily, an exception swallowed on the way — would satisfy
        introspection and still serve the row.
        """
        viewer_id = 555_000_111
        assert viewer_id not in ADMIN_USER_IDS

        class _Store:
            async def get_user(self, uid):
                return {"status": "approved", "role": "viewer"}

        async def _fake_get_store():
            return _Store()

        monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)

        headers = _cookie_header(_make_cookie(viewer_id, role="viewer"))
        for path in ("/api/buyers/1", "/api/orders/1", "/api/search?q=ab"):
            r = client.get(path, headers=headers)
            assert r.status_code == 403, f"{path} answered a viewer with {r.status_code}"


# ─── Admin user management: the endpoints that hand out access ────────────────

# Every route that reads or rewrites who may use this dashboard. `user_id` is a
# Telegram id in the path, so a missing gate here is not one leaked record — it
# is the roster of all 24 users, and two of these hand out roles.
ADMIN_USER_ROUTES = [
    ("GET", "/api/admin/users"),
    ("GET", "/api/admin/users/{user_id}"),
    ("PATCH", "/api/admin/users/{user_id}/role"),
    ("PATCH", "/api/admin/users/{user_id}/status"),
    ("GET", "/api/admin/permissions"),
    ("PATCH", "/api/admin/permissions"),
]


class _RecordingStore:
    """A store that answers session resolution and records every write."""

    def __init__(self, status: str = "approved", role: str = "viewer"):
        self._status, self._role = status, role
        self.writes = []

    async def get_user(self, uid):
        return {"user_id": uid, "status": self._status, "role": self._role}

    async def list_users(self, **kwargs):
        return []

    async def update_user_role(self, user_id, role, changed_by):
        self.writes.append(("role", user_id, role))
        return True

    async def update_user_status(self, user_id, status, reviewed_by):
        self.writes.append(("status", user_id, status))
        return True


def _install_store(monkeypatch, store):
    """Route both the session check and the handler bodies to `store`.

    Two patch targets, not one: `_resolve_session` imports `get_store` from
    `core.duckdb_store` at call time, but web/routes/api/users.py bound it into
    its own namespace at import. Patching only the first leaves every handler
    body reaching the real DuckDB file.
    """
    async def _fake():
        return store

    monkeypatch.setattr("core.duckdb_store.get_store", _fake)
    monkeypatch.setattr("web.routes.api.users.get_store", _fake)
    return store


class TestAdminUserManagementIsAdminOnly:
    """Unlike the PII routes above, these six carry `require_admin` in their own
    signatures rather than on the include — so a router reshuffle that moved
    them under a plain include would not visibly drop anything. Pinned here
    because losing the gate promotes any approved viewer to user administration.
    """

    @pytest.mark.parametrize("method,path", ADMIN_USER_ROUTES)
    def test_admin_user_route_requires_admin(self, method, path):
        assert _route(path, method) is not None, \
            f"{method} {path} is no longer registered — if it moved, its new " \
            f"home must be admin-only and this list must name it"
        assert require_admin in _deps(path, method), \
            f"{method} {path} manages access for any approved viewer — " \
            f"admin→viewer downgrade"

    def test_approved_viewer_is_forbidden_from_user_management(self, client, monkeypatch):
        """The structural check reads the route table; this reads the answer."""
        viewer_id = 555_000_111
        assert viewer_id not in ADMIN_USER_IDS

        store = _install_store(monkeypatch, _RecordingStore())
        headers = _cookie_header(_make_cookie(viewer_id, role="viewer"))
        target = sorted(ADMIN_USER_IDS)[0]

        urls = [
            ("GET", "/api/admin/users"),
            ("GET", f"/api/admin/users/{target}"),
            ("PATCH", f"/api/admin/users/{target}/role?role=admin"),
            ("PATCH", f"/api/admin/users/{target}/status?status=denied"),
            ("GET", "/api/admin/permissions"),
            ("PATCH", "/api/admin/permissions?role=viewer&feature=sms"
                      "&can_view=true&can_edit=true&can_delete=false"),
        ]
        for method, url in urls:
            r = client.request(method, url, headers=headers)
            assert r.status_code == 403, f"{method} {url} answered a viewer with {r.status_code}"
        assert store.writes == [], \
            f"a forbidden request still wrote: {store.writes}"


class TestHardcodedAdminAccessIsNotSilentlyRevocable:
    """`_resolve_session` short-circuits for the ids in `ADMIN_USER_IDS` before
    it looks at status or role, so for exactly those two accounts the stored
    values are decoration. Denying one used to answer 200 and change nothing —
    the admin page then showed "denied" beside an account with full access.

    The endpoints refuse instead. Enforcing the deny is the wrong fix: the
    hardcoded set is the recovery path when the DuckDB row or the whole
    permissions system is unusable, and honouring a stored status there would
    make it possible to lock every admin out with one click, recoverable only
    by editing source and redeploying.
    """

    def _admin_headers(self):
        return _cookie_header(_make_cookie(ADMIN_ID, role="admin"))

    @pytest.mark.parametrize("status", ["denied", "frozen", "pending"])
    def test_revoking_a_hardcoded_admin_is_refused(self, client, monkeypatch, status):
        store = _install_store(monkeypatch, _RecordingStore(role="admin"))
        r = client.patch(
            f"/api/admin/users/{ADMIN_ID}/status?status={status}",
            headers=self._admin_headers(),
        )
        assert r.status_code == 409, \
            f"status={status} on a hardcoded admin answered {r.status_code}; " \
            f"it does not remove access, so it must not report success"
        assert store.writes == [], \
            "refused, but the row was written anyway — the page would show a " \
            f"status the session ignores: {store.writes}"

    @pytest.mark.parametrize("role", ["viewer", "editor"])
    def test_demoting_a_hardcoded_admin_is_refused(self, client, monkeypatch, role):
        store = _install_store(monkeypatch, _RecordingStore(role="admin"))
        r = client.patch(
            f"/api/admin/users/{ADMIN_ID}/role?role={role}",
            headers=self._admin_headers(),
        )
        assert r.status_code == 409, \
            f"role={role} on a hardcoded admin answered {r.status_code}; " \
            f"require_admin ignores the stored role for these ids"
        assert store.writes == []

    def test_the_truthful_values_are_still_writable(self, client, monkeypatch):
        """Refusing everything would strand a row this bug already corrupted.

        `approved`/`admin` are what the code enforces for these ids, so writing
        them makes the admin page agree with reality rather than diverge from it.
        """
        store = _install_store(monkeypatch, _RecordingStore(role="admin"))
        headers = self._admin_headers()

        assert client.patch(
            f"/api/admin/users/{ADMIN_ID}/status?status=approved", headers=headers
        ).status_code == 200
        assert client.patch(
            f"/api/admin/users/{ADMIN_ID}/role?role=admin", headers=headers
        ).status_code == 200
        assert store.writes == [
            ("status", ADMIN_ID, "approved"),
            ("role", ADMIN_ID, "admin"),
        ]

    def test_ordinary_users_are_still_revocable(self, client, monkeypatch):
        """The guard keys on the hardcoded set, not on 'is an admin'."""
        ordinary = 555_000_222
        assert ordinary not in ADMIN_USER_IDS

        store = _install_store(monkeypatch, _RecordingStore(role="admin"))
        headers = self._admin_headers()

        assert client.patch(
            f"/api/admin/users/{ordinary}/status?status=denied", headers=headers
        ).status_code == 200
        assert client.patch(
            f"/api/admin/users/{ordinary}/role?role=viewer", headers=headers
        ).status_code == 200
        assert store.writes == [
            ("status", ordinary, "denied"),
            ("role", ordinary, "viewer"),
        ]

    @pytest.mark.asyncio
    async def test_a_denied_hardcoded_admin_still_resolves_as_admin(self, monkeypatch):
        """The premise the refusal rests on, asserted rather than assumed.

        If `_resolve_session` is ever changed to honour the stored status for
        these ids, this fails — and the refusal above becomes the wrong answer
        and should be revisited with it.
        """
        from web.routes.auth import _resolve_session

        _install_store(monkeypatch, _RecordingStore(status="denied", role="viewer"))
        session = await _resolve_session(_make_cookie(ADMIN_ID, role="admin"))
        assert session is not None, "a hardcoded admin lost access to a stored status"
        assert session["role"] == "admin"


# ─── #3  WebSocket endpoints require a session ────────────────────────────────

class TestWebSocketAuth:
    def test_ws_dashboard_rejects_anonymous(self, client):
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/dashboard"):
                pass

    def test_ws_admin_rejects_anonymous(self, client):
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/admin"):
                pass

    @pytest.mark.asyncio
    async def test_get_current_user_ws_none_without_cookie(self):
        class _WS:
            cookies = {}

        assert await get_current_user_ws(_WS()) is None


# ─── #4  Single shared rate-limiter instance ──────────────────────────────────

class TestRateLimiterShared:
    def test_one_limiter_instance_everywhere(self):
        from web.ratelimit import limiter as shared
        from web.main import limiter as main_limiter
        from web.routes.api._deps import limiter as deps_limiter
        from web.routes.chat import limiter as chat_limiter

        assert main_limiter is shared
        assert deps_limiter is shared
        assert chat_limiter is shared

    def test_app_state_limiter_is_shared(self):
        from web.ratelimit import limiter as shared
        assert app.state.limiter is shared


# ─── #12  Telegram HMAC verification ──────────────────────────────────────────

class TestTelegramHmac:
    """verify_telegram_auth must accept valid data and reject any tampering."""

    def _signed_auth_data(self, bot_token: str) -> dict:
        data = {
            "id": "12345",
            "first_name": "Valid",
            "username": "validuser",
            "auth_date": str(int(time.time())),
        }
        secret = hashlib.sha256(bot_token.encode()).digest()
        check = "\n".join(f"{k}={data[k]}" for k in sorted(data))
        data["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        return data

    def test_valid_auth_data_accepted(self):
        from bot.config import BOT_TOKEN
        if not BOT_TOKEN:
            pytest.skip("BOT_TOKEN not configured")
        from web.services.auth_service import verify_telegram_auth
        assert verify_telegram_auth(self._signed_auth_data(BOT_TOKEN)) is True

    def test_tampered_field_rejected(self):
        from bot.config import BOT_TOKEN
        if not BOT_TOKEN:
            pytest.skip("BOT_TOKEN not configured")
        from web.services.auth_service import verify_telegram_auth
        data = self._signed_auth_data(BOT_TOKEN)
        data["id"] = "99999"  # tamper after signing
        assert verify_telegram_auth(data) is False

    def test_missing_hash_rejected(self):
        from web.services.auth_service import verify_telegram_auth
        assert verify_telegram_auth({"id": "1", "auth_date": str(int(time.time()))}) is False

    def test_uses_constant_time_compare(self):
        """Guard against removing hmac.compare_digest from auth_service.

        The behavioral test (test_tampered_field_rejected) cannot catch a
        regression from compare_digest back to `!=`: both correctly reject
        tampered hashes — the only difference is timing, which a unit test
        cannot observe. The previous `"!= received_hash" not in src` check
        was a brittle spelling-specific guard; the positive assertion below
        is the meaningful invariant.
        """
        import inspect
        from web.services import auth_service
        assert "hmac.compare_digest" in inspect.getsource(auth_service)


# ─── WebApp auth: HttpOnly cookie set server-side, no token leaks in body ────

class TestWebappAuthHardening:
    def test_login_page_has_no_inline_scripts(self, client):
        """CSP drops 'unsafe-inline' from script-src — login.html must be clean.

        Regression guard: a previous commit broke /login by leaving an inline
        <script> block that the new CSP refuses to execute. Telegram WebApp
        auto-auth ended up hung. Externalising the script fixed it; this test
        pins the invariant so it doesn't drift back.
        """
        import re
        r = client.get("/login")
        assert r.status_code == 200
        tags = re.findall(r"<script\b[^>]*>", r.text)
        inline = [t for t in tags if "src=" not in t]
        assert not inline, f"inline <script> in /login breaks CSP: {inline}"

    def test_webapp_auth_error_body_does_not_leak_session(self, client):
        """/auth/webapp must NEVER include a session-token field in its body.

        Previously the success path returned {"success": True, "session": "<token>"}
        and the client set it via document.cookie — defeating HttpOnly. The
        endpoint now sets the cookie server-side and returns only {success}.
        Error paths must also not leak any tokenish field.
        """
        # Missing initData
        r = client.post("/auth/webapp", json={})
        assert r.status_code == 400
        assert "session" not in r.json()
        # Garbage initData (fails HMAC verify)
        r = client.post("/auth/webapp", json={"initData": "garbage"})
        assert r.status_code == 401
        assert "session" not in r.json()


# ─── #13  SPA catch-all blocks path traversal ─────────────────────────────────

class TestPathTraversal:
    def test_static_asset_outside_root_is_blocked(self, client, tmp_path, monkeypatch):
        """A traversal path resolving outside STATIC_V2_DIR must not be served."""
        import web.routes.pages as pages

        # A "static root" with one legit asset, and a secret sibling outside it.
        static_root = tmp_path / "static-v2"
        static_root.mkdir()
        (static_root / "app.js").write_text("// legit bundle")
        secret = tmp_path / "secret.js"
        secret.write_text("TOP SECRET")

        monkeypatch.setattr(pages, "STATIC_V2_DIR", static_root)

        admin = _cookie_header(_make_cookie(ADMIN_ID))

        # Legit asset inside the root is served.
        ok = client.get("/app.js", headers=admin)
        assert ok.status_code == 200

        # Traversal to the sibling secret must be refused (404, never its body).
        evil = client.get("/%2e%2e/secret.js", headers=admin)
        assert evil.status_code == 404
        assert "TOP SECRET" not in evil.text


# ─── require_user unit behaviour ──────────────────────────────────────────────

class TestRequireUserDependency:
    @pytest.mark.asyncio
    async def test_require_user_raises_401_without_cookie(self):
        req = Request({"type": "http", "headers": [], "query_string": b""})
        with pytest.raises(HTTPException) as exc:
            await require_user(req)
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_require_user_accepts_valid_admin_session(self):
        cookie = _make_cookie(ADMIN_ID)
        req = Request({
            "type": "http",
            "query_string": b"",
            "headers": [(b"cookie", f"{SESSION_COOKIE}={cookie}".encode())],
        })
        user = await require_user(req)
        assert user["user_id"] == ADMIN_ID
        assert user["role"] == "admin"
