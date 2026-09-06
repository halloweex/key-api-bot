"""Everything reachable over HTTP/WS from *outside* the ``api_gate`` umbrella.

``web/main.py`` claims ``PUBLIC_API_PATHS`` is "one set to audit". That is true
of ``/api``. It says nothing about the three surfaces that never pass through
the gate: the WebSocket endpoints, the SPA page routes, and the two
``StaticFiles`` mounts. This module is that audit, written down.

**The defect: the WebSocket handshake accepted any ``Origin``.** The module
docstring in ``web/routes/websocket.py`` credited the session cookie with
preventing Cross-Site WebSocket Hijacking. It is the other way round — cookie
authentication is the *precondition* for CSWSH: WebSockets are exempt from
CORS, so a page on any origin can open ``wss://<dashboard>/ws/admin`` and the
browser attaches whatever cookies its own policy allows. Measured before the
fix: a handshake carrying ``Origin: https://attacker.example`` and a valid
session cookie was accepted into both the ``dashboard`` and the ``admin`` room.

What actually stood in the way was ``samesite="lax"`` on the session cookie —
and both halves of that protection live outside this codebase. ``Lax`` is set
because the Telegram login redirect needs it, not because a socket depends on
it; and it separates the attacker from us only while ``duckdns.org`` is a
public suffix, which was measured (public_suffix_list.dat, PRIVATE DOMAINS
section) rather than assumed — without that entry any free ``*.duckdns.org``
subdomain would be same-site with the dashboard and ``Lax`` would send the
cookie. So the attack was not live in a current browser, and nothing in the
application was refusing it. The check below is what refuses it.

**Hostname, not origin string.** Vite's dev proxy sets ``changeOrigin: true``,
which rewrites ``Host`` to the backend (``localhost:8080``) while forwarding
the browser's ``Origin: http://localhost:5173`` untouched. A port- or
scheme-exact comparison would refuse every local development handshake. nginx
passes ``Host $host`` and ``Origin`` through unchanged, so in production the
two hostnames are equal.

**A handshake with no ``Origin`` header at all is allowed.** Browsers always
send one on a WebSocket handshake; something that does not is not a browser,
and therefore not the confused deputy this check exists for.

The remaining classes are characterisation, not fixes — each records a
judgement so that changing it is somebody's decision rather than a drift.
Sessions are forged the way tests/integration/test_security_hardening.py and
tests/integration/test_health_router_disclosure.py forge them, and the
TestClient is built WITHOUT the context-manager form so no startup event
(DuckDB sync, scheduler) runs. Every identifier here is invented; the
repository is public.
"""
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from core.permissions import ADMIN_USER_IDS
from core.websocket_manager import manager
from web.config import STATIC_DIR, STATIC_V2_DIR
from web.main import app
from web.routes.auth import (
    SESSION_COOKIE,
    create_session_data,
    session_serializer,
)

# Invented, and asserted not to be one of the real hardcoded admins.
VIEWER_ID = 555_000_888

FOREIGN_ORIGIN = "https://attacker.example"


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


def _cookie_header(user_id: int = VIEWER_ID, role: str = "viewer") -> dict:
    return {"Cookie": f"{SESSION_COOKIE}={_make_cookie(user_id, role)}"}


class _FakeStore:
    """Resolves a session's role and status without touching the DuckDB file."""

    def __init__(self, role: str = "viewer", status: str = "approved"):
        self.role = role
        self.status = status

    async def get_user(self, uid):
        return {"status": self.status, "role": self.role}


@pytest.fixture
def store(monkeypatch):
    """Every session in this module resolves through this store."""
    fake = _FakeStore()

    async def _fake_get_store():
        return fake

    monkeypatch.setattr("core.duckdb_store.get_store", _fake_get_store)
    return fake


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(scope="module", autouse=True)
def _a_frontend_to_serve():
    """Give the module a built frontend when the checkout has none.

    `web/static-v2/` is Vite's output and is gitignored, so a fresh checkout
    — CI, or a laptop that has not run `npm run build` — holds no `index.html`
    and no bundle. Three tests here compare what the app serves against those
    bytes, and the traversal class needs one real asset to prove the guard has
    not cost the branch its purpose. A placeholder shell and a single asset
    satisfy every assertion in this module. Where the real build exists,
    nothing is written; on teardown only what was planted is removed.
    """
    planted: list[Path] = []
    made_dirs: list[Path] = []

    def plant(path: Path, content: bytes) -> None:
        if path.exists():
            return
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
            made_dirs.append(path.parent)
        path.write_bytes(content)
        planted.append(path)

    plant(
        STATIC_V2_DIR / "index.html",
        b"<!doctype html><title>placeholder shell (test suite)</title>\n",
    )
    if not any((STATIC_V2_DIR / "assets").glob("*.js")):
        plant(
            STATIC_V2_DIR / "assets" / "placeholder.js",
            b"// planted by tests/integration/test_unauthenticated_surface.py\n",
        )
    yield
    for path in planted:
        path.unlink(missing_ok=True)
    for directory in reversed(made_dirs):
        if directory != STATIC_V2_DIR and not any(directory.iterdir()):
            directory.rmdir()


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from web.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


def _raw_get(path: str, headers: dict) -> tuple[int, bytes]:
    """Drive the ASGI app with a path the HTTP client would have normalised.

    httpx collapses ``..`` out of a URL before it ever reaches the app, so a
    traversal attempt sent through TestClient tests the client, not the route.
    The guard in ``_serve_spa_route`` only sees a ``..`` if something speaks
    ASGI directly — which is what this does.
    """
    import anyio

    async def _drive():
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.1"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "root_path": "",
            "query_string": b"",
            "headers": [(b"host", b"testserver")]
            + [(k.lower().encode(), v.encode()) for k, v in headers.items()],
            "client": ("test", 1),
            "server": ("testserver", 80),
        }
        messages = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            messages.append(message)

        await app(scope, receive, send)
        status = next(
            m["status"] for m in messages if m["type"] == "http.response.start"
        )
        body = b"".join(
            m.get("body", b"") for m in messages if m["type"] == "http.response.body"
        )
        return status, body

    return anyio.run(_drive)


# ─── The defect: no Origin check on the handshake ─────────────────────────────

class TestWebSocketRefusesForeignOrigins:
    """WebSockets are exempt from CORS. The handshake is the only place to look."""

    def test_dashboard_refuses_a_foreign_origin(self, client, store):
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/dashboard",
                headers={**_cookie_header(), "Origin": FOREIGN_ORIGIN},
            ):
                pass
        assert exc.value.code == 4002

    def test_admin_refuses_a_foreign_origin(self, client, store):
        store.role = "admin"
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/admin",
                headers={**_cookie_header(), "Origin": FOREIGN_ORIGIN},
            ):
                pass
        assert exc.value.code == 4002

    def test_a_hostname_that_merely_contains_ours_is_refused(self, client, store):
        """`testserver.attacker.example` is the attacker's domain, not ours."""
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/dashboard",
                headers={
                    **_cookie_header(),
                    "Origin": "https://testserver.attacker.example",
                },
            ):
                pass
        assert exc.value.code == 4002

    def test_a_hostname_ending_in_ours_is_refused(self, client, store):
        """The comparison must be equality, never `endswith`.

        Production answers to a name under `duckdns.org`, a provider that hands
        out any free subdomain — so a suffix test would accept
        `evil<ourname>.duckdns.org`, which is one registration away. This case
        exists because writing the check as `endswith` passed every other test
        in this class.
        """
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/dashboard",
                headers={**_cookie_header(), "Origin": "https://eviltestserver"},
            ):
                pass
        assert exc.value.code == 4002

    def test_a_null_origin_is_refused(self, client, store):
        """A sandboxed iframe sends the literal string `null`."""
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/dashboard",
                headers={**_cookie_header(), "Origin": "null"},
            ):
                pass
        assert exc.value.code == 4002

    def test_the_origin_is_judged_before_the_cookie(self, client, store):
        """A foreign page must not learn whether the victim is even logged in."""
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/dashboard", headers={"Origin": FOREIGN_ORIGIN}
            ):
                pass
        assert exc.value.code == 4002, "the origin check ran after authentication"


class TestWebSocketAcceptsItsOwnOrigins:
    """The check must cost the real client nothing."""

    def test_same_host_origin_connects(self, client, store):
        with client.websocket_connect(
            "/ws/dashboard",
            headers={**_cookie_header(), "Origin": "http://testserver"},
        ) as ws:
            assert "connected" in ws.receive_text()

    def test_a_handshake_without_an_origin_connects(self, client, store):
        """Browsers always send one; something that does not is not a browser."""
        with client.websocket_connect(
            "/ws/dashboard", headers=_cookie_header()
        ) as ws:
            assert "connected" in ws.receive_text()

    def test_the_vite_dev_proxy_still_connects(self, client, store):
        """`changeOrigin: true` rewrites Host and leaves Origin alone, so the
        two differ by port in every local `npm run dev` session."""
        with client.websocket_connect(
            "/ws/dashboard",
            headers={
                **_cookie_header(),
                "Host": "localhost:8080",
                "Origin": "http://localhost:5173",
            },
        ) as ws:
            assert "connected" in ws.receive_text()

    def test_the_configured_dashboard_url_connects(self, client, store, monkeypatch):
        """Behind a proxy that rewrites Host, DASHBOARD_URL is the other name
        this deployment answers to."""
        monkeypatch.setenv("DASHBOARD_URL", "https://dashboard.invalid")
        with client.websocket_connect(
            "/ws/dashboard",
            headers={**_cookie_header(), "Origin": "https://dashboard.invalid"},
        ) as ws:
            assert "connected" in ws.receive_text()


# ─── What the handshake already did right ─────────────────────────────────────

class TestWebSocketAuthentication:

    def test_dashboard_refuses_anonymous(self, client, store):
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect("/ws/dashboard"):
                pass
        assert exc.value.code == 4001

    def test_admin_refuses_a_viewer(self, client, store):
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect("/ws/admin", headers=_cookie_header()):
                pass
        assert exc.value.code == 4003

    def test_admin_refuses_a_stale_admin_cookie(self, client, store):
        """The cookie says admin, the store says viewer. The store wins."""
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                "/ws/admin", headers=_cookie_header(role="admin")
            ):
                pass
        assert exc.value.code == 4003

    def test_a_hardcoded_admin_reaches_the_admin_room(self, client, store):
        """`/ws/admin` compares `role != "admin"` itself instead of reusing
        `require_admin`, so it does not consult `is_hardcoded_admin`. It works
        only because `_resolve_session` stamps `role='admin'` on those ids
        first. Pinned because the duplication is invisible until that stops."""
        admin_id = sorted(ADMIN_USER_IDS)[0]
        with client.websocket_connect(
            "/ws/admin", headers=_cookie_header(admin_id, role="viewer")
        ) as ws:
            assert "connected" in ws.receive_text()


class TestRoomsAreFixedAtHandshake:
    """`handle_message` parses `{"action": "subscribe"}` and does nothing with
    it — a literal `pass` under a "Future: handle room switching" comment.
    Completing that TODO without re-reading this endpoint would hand every
    dashboard viewer the admin room, because the room is the only thing
    separating them and it is chosen from the URL after the role check."""

    def test_subscribe_cannot_move_a_connection_to_the_admin_room(
        self, client, store
    ):
        with client.websocket_connect(
            "/ws/dashboard", headers=_cookie_header()
        ) as ws:
            ws.receive_text()
            ws.send_text('{"action": "subscribe", "room": "admin"}')
            ws.send_text("ping")
            assert "pong" in ws.receive_text()
            assert manager.connection_count("admin") == 0


class TestASocketOutlivesTheSessionThatOpenedIt:
    """ACCEPTED, and recorded rather than fixed.

    HTTP re-reads role and status from the store on every request, so a denied
    user is cut off by their next call. A socket authenticates once at the
    handshake and nothing re-checks it — not on a timer, and not on a message.
    `cleanup_stale_connections` exists but has no caller outside the tests, so
    the window is not five minutes: it is until the client closes.

    Judged low because of what the rooms carry. Every broadcast is a
    cache-invalidation signal — `orders_synced {count}`, `products_synced
    {count}`, `inventory_updated {stocks_count}`, `expenses_updated {action,
    id}` — and the frontend answers each one with an HTTP fetch that *is*
    re-authorised. The admin room carries `sync_failed {sync_type, error}`,
    operational text and no business data. A revoked user learns that a sync
    happened and how many rows moved, and can read none of it.

    If a payload ever carries the data instead of the signal, this stops being
    acceptable and the endpoint needs periodic re-authorisation.
    """

    def test_a_denied_user_keeps_the_socket_but_loses_http(self, client, store):
        with client.websocket_connect(
            "/ws/dashboard",
            headers={**_cookie_header(), "Origin": "http://testserver"},
        ) as ws:
            ws.receive_text()

            store.status = "denied"

            assert client.get(
                "/api/summary", headers=_cookie_header()
            ).status_code == 401

            ws.send_text("ping")
            assert "pong" in ws.receive_text()
            assert manager.connection_count("dashboard") == 1


class TestWsStatsStaysViewerReadable:
    """`/ws/stats` hands an approved viewer the room names, a connection count
    per room and the oldest connection's timestamp — including for `admin`.
    Left at `require_user` on the same reasoning that left `/api/metrics`
    there: on a 24-person internal dashboard "somebody is connected to the
    admin room" is not a fact worth a control, and `/openapi.json` already
    names the endpoint. Recorded so restricting it is a decision."""

    def test_a_viewer_may_read_it(self, client, store):
        assert client.get("/ws/stats", headers=_cookie_header()).status_code == 200

    def test_it_still_requires_a_session(self, client, store):
        assert client.get("/ws/stats").status_code == 401


# ─── The SPA routes ───────────────────────────────────────────────────────────

class TestPageRoutesRequireASession:

    @pytest.mark.parametrize("path", ["/", "/v2", "/v2/reports", "/admin/users", "/whatever"])
    def test_anonymous_is_redirected_to_login(self, client, store, path):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 302
        assert response.headers["location"] == "/login"

    def test_the_catch_all_does_not_shadow_the_api(self, client, store):
        """The pages router is included last and owns `/{path:path}`. An /api
        path must still reach `api_gate` and be refused as JSON, not fall
        through to the SPA shell with a 200."""
        response = client.get("/api/buyers/1")
        assert response.status_code == 401
        assert response.json()["detail"] == "Authentication required"

    def test_the_admin_shell_is_the_same_html_as_every_other_route(
        self, client, store
    ):
        """ACCEPTED: `/admin/{path}` serves an approved viewer the SPA shell,
        which is the same bytes every route serves — React decides what to
        render and every admin screen's data is gated server-side. The shell
        discloses nothing the bundle at `/static-v2` does not already."""
        headers = _cookie_header()
        assert (
            client.get("/admin/users", headers=headers).content
            == client.get("/", headers=headers).content
        )


class TestTheCatchAllCannotReadOutsideTheBuild:
    """`_serve_spa_route` is the one place in the app that maps a URL segment
    onto a filesystem path. It takes the file branch only for a known asset
    extension, and only after `require_auth`."""

    ESCAPES = [
        "/../frontend/package.json",
        "/../../data/dow_corrections.json",
        "/../../../../../../etc/passwd.json",
        "/..%2f..%2fpackage.json",
        "//app/data/config.json",
        "/assets/../../../main.py.json",
    ]

    @pytest.mark.parametrize("path", ESCAPES)
    def test_traversal_is_refused(self, client, store, path):
        status, body = _raw_get(path, _cookie_header())
        assert status == 404, f"{path} was served: {body[:200]!r}"

    def test_a_real_asset_is_still_served(self, client, store):
        """The guard must not cost the branch its purpose."""
        asset = next((STATIC_V2_DIR / "assets").glob("*.js"))
        response = client.get(f"/assets/{asset.name}", headers=_cookie_header())
        assert response.status_code == 200
        assert response.content == asset.read_bytes()

    @pytest.mark.parametrize(
        "path", ["/../../../../etc/hosts", "/../../requirements.lock"]
    )
    def test_a_non_asset_path_is_never_a_file_read(self, client, store, path):
        """Paths without an asset extension never reach the file branch at all
        — they go to React Router — so a traversal attempt there comes back as
        the shell rather than as anything read off disk."""
        status, body = _raw_get(path, _cookie_header())
        assert status == 200
        assert body == (STATIC_V2_DIR / "index.html").read_bytes()


# ─── The static mounts ────────────────────────────────────────────────────────

class TestStaticMountsServeOnlyTheBuiltApp:
    """`/static` and `/static-v2` are mounted with no dependency at all, so
    everything under them is world-readable. ACCEPTED for what is there — the
    Vite bundle, three icons and the login widget's script — on the same
    footing as `/openapi.json`, which already names every route anonymously.
    The tests below are what keeps "what is there" true."""

    def test_the_spa_shell_and_bundle_are_public(self, client):
        assert client.get("/static-v2/index.html").status_code == 200
        assert client.get("/static/favicon.png").status_code == 200

    def test_no_source_maps_are_published(self):
        """Vite emits none by default (`build.sourcemap` is unset). Turning it
        on would publish the frontend's source to anonymous callers."""
        maps = [
            str(p.relative_to(STATIC_V2_DIR))
            for p in STATIC_V2_DIR.rglob("*.map")
        ]
        assert maps == [], f"source maps are being served: {maps}"

    def test_no_dotfiles_or_configuration_are_published(self):
        for root in (STATIC_DIR, STATIC_V2_DIR):
            leaked = [
                str(p.relative_to(root))
                for p in root.rglob("*")
                if p.is_file()
                and (p.name.startswith(".") or p.suffix in {".env", ".pem", ".key"})
            ]
            assert leaked == [], f"{root.name} publishes {leaked}"

    @pytest.mark.parametrize(
        "path",
        [
            "/static/../main.py",
            "/static/../../requirements.lock",
            "/static-v2/../../main.py",
            "/static/..%2fmain.py",
        ],
    )
    def test_starlette_refuses_traversal_out_of_a_mount(self, client, path):
        """Starlette's own defence, verified rather than assumed — measured on
        starlette 0.50.0 here; production and CI run a later one."""
        status, _body = _raw_get(path, {})
        assert status == 404
