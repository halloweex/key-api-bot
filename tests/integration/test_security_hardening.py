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
    for r in app.routes:
        if getattr(r, "path", None) == path and method in getattr(r, "methods", set()):
            return r
    return None


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
        "/api/metrics",
        "/ws/stats",
    ]

    PROTECTED_POSTS = [
        "/api/revenue/forecast/train",
        "/api/revenue/forecast/tune",
        "/api/expenses",
        "/api/stocks/snapshot/refresh",
        "/api/duckdb/sync-buyers",
        "/api/dashboard/batch",
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
        assert PUBLIC_API_PATHS == {"/api/health"}, \
            "PUBLIC_API_PATHS drifted — every entry must be a deliberate, audited exception"

    def test_every_api_route_is_under_api_gate(self):
        """Iterate ALL registered /api/* routes and assert each carries api_gate.

        Previously this was a hand-curated path list of 6 spot-checks; a new
        endpoint added on a router that escapes the include-level gate (the
        chat router was such a case) would not be caught. Now we walk the
        live route table — every /api/* route must have api_gate in its
        resolved dependency tree, period.
        """
        from starlette.routing import Route as _Route
        leaked = []
        for r in app.routes:
            if not isinstance(r, _Route):
                continue
            path = getattr(r, "path", "")
            if not path.startswith("/api/"):
                continue
            if api_gate not in _all_dep_calls(r.dependant):
                methods = ",".join(sorted(getattr(r, "methods", set()) or {"?"}))
                leaked.append(f"{methods} {path}")
        assert not leaked, \
            "audit-invariant drift — /api/* routes outside api_gate:\n  " + "\n  ".join(leaked)

    @pytest.mark.parametrize("path", ["/api/jobs", "/api/sync/stats", "/api/warehouse/status", "/api/bronze/stats"])
    def test_admin_ops_endpoints_require_admin(self, path):
        route = _route(path)
        assert route is not None
        assert require_admin in _all_dep_calls(route.dependant), \
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
        from web.routes.batch import limiter as batch_limiter
        from web.routes.chat import limiter as chat_limiter

        assert main_limiter is shared
        assert deps_limiter is shared
        assert batch_limiter is shared
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
