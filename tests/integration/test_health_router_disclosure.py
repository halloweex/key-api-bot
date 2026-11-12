"""What the health router hands to whoever can reach it.

Two findings, both about disclosure rather than about a missing gate.

**``GET /api/metrics`` was a directory of the identifiers other people had
used.** ``RequestLoggingMiddleware`` keyed its counters and timers on
``f"{method} {request.url.path}"`` — the path *as asked*, ids included — and
``/api/metrics`` returns ``metrics.get_stats()`` verbatim to any approved
viewer. Driving the app and reading the endpoint back produced keys like
``GET /api/buyers/<id>`` and ``PATCH /api/admin/users/<telegram id>/role``:
every route that takes an id is admin-only or permission-gated *for the id it
is given*, so this converted "you must know a valid id" into "here is the list
of ids known to be valid". Refused requests were recorded too, so a 403 still
published the id it refused.

The same key is also the growth: one entry per distinct id for the life of the
process, each with its own list of up to 100 latency samples, and nginx proxies
``/static/`` to the app so every hashed asset filename ever built added one
more. Nothing evicted any of it.

The fix templates the key in the middleware instead of redacting the response,
because that keeps every operational property the metric had — per-route counts
and latencies — while there is no id to redact, and it bounds the dictionary by
the route table rather than by traffic.

**Templated, and since 2026-11-11 gated too.** The key was templated rather
than the response redacted, and that still holds: it bounds the dictionary by
the route table, and an admin reading the metric should see route names, not
ids. The endpoint was then left viewer-readable on purpose — ``/openapi.json``
names every route template to anonymous callers, so what was left was route
names already public plus counts, latencies and exception class names — and a
class below pinned that, so restricting it had to be a decision. The owner took
it (O8 (a), 2026-11-11): ``/api/metrics``, ``/api/health/detailed`` and
``/api/health/data-quality`` are behind ``require_admin``, and that class now
pins the new answer for all three.

**``/api/health`` published DuckDB exception text to anonymous callers.** The
handler scrubs the store's error on purpose ("don't leak DuckDB exception text
(paths, PIDs, schema fragments) to anonymous probes") and then, eleven lines
below, put ``str(e)`` from the very same failure into ``migrations.error``.
That endpoint faces the open internet — Docker, nginx and an external uptime
monitor poll it, so it is in ``PUBLIC_API_PATHS`` — and the string carries the
container's database path, a pid and the user it runs as. Since step 14's
S14-17 the endpoint does not ask the DuckDB store at all — the ``duckdb``
block and the ``migrations`` ledger went — so a store that cannot be opened
neither leaks into it nor degrades it.

Sessions are forged the same way as tests/integration/test_security_hardening.py
and tests/integration/test_traffic_permissions.py, and the TestClient is built
WITHOUT the context-manager form so no startup event (DuckDB sync, scheduler)
runs. Every identifier below is invented; the repository is public.
"""
import json
import time

import pytest
from fastapi.testclient import TestClient

from core.observability import metrics
from core.permissions import ADMIN_USER_IDS
from web.main import app
from web.middleware import request_metric_key
from web.routes.auth import (
    session_serializer,
    create_session_data,
    SESSION_COOKIE,
)

METRICS_PATH = "/api/metrics"

# Invented ids, chosen long enough that they cannot collide with a latency
# reading or a counter in the payload the assertions search. The buyer and order
# cards these first probed (`/api/buyers/<id>`, `/api/orders/<id>`) were retired
# by OD-10 on 2026-09-30; an admin read of one user and an admin write to one
# manager carry the same shape of identifier.
USER_ID = "4242424"
MANAGER_ID = "9090909"
TELEGRAM_ID = "770001112"
CAMPAIGN = "not-a-real-campaign"
PRESET = "not-a-real-preset"

# One request per shape of identifier the audit cares about. Each is refused —
# they are admin-only or permission-gated — which is the point: the metric
# recorded them anyway.
ID_BEARING_REQUESTS = [
    ("GET", f"/api/admin/users/{USER_ID}", "/api/admin/users/{user_id}"),
    (
        "POST",
        f"/api/managers/{MANAGER_ID}/retail-status",
        "/api/managers/{manager_id}/retail-status",
    ),
    ("PATCH", f"/api/admin/users/{TELEGRAM_ID}/role", "/api/admin/users/{user_id}/role"),
    (
        "PUT",
        f"/api/customers/sms-audience-presets/{PRESET}",
        "/api/customers/sms-audience-presets/{name}",
    ),
    (
        "GET",
        f"/api/customers/sms-campaigns/{CAMPAIGN}/results",
        "/api/customers/sms-campaigns/{campaign}/results",
    ),
]

SECRET_FRAGMENTS = [USER_ID, MANAGER_ID, TELEGRAM_ID, CAMPAIGN, PRESET]


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
    """Resolves a session's role without touching the DuckDB file."""

    def __init__(self, role: str):
        self._role = role

    async def get_user(self, uid):
        return {"status": "approved", "role": self._role}


def _login(monkeypatch, role: str) -> dict:
    store = _FakeStore(role)

    async def _fake_get_store():
        return store

    monkeypatch.setattr("core.store.get_store", _fake_get_store)

    uid = 555_000_888
    assert uid not in ADMIN_USER_IDS
    return _cookie_header(_make_cookie(uid, role=role))


def _read_metrics(client, monkeypatch) -> dict:
    """What an admin reads back from /api/metrics (admin-only since O8 (a))."""
    return client.get(METRICS_PATH, headers=_login(monkeypatch, "admin")).json()


def _metric_keys(payload: dict) -> set:
    """Every key a reader of /api/metrics can see."""
    return set(payload.get("requests", {})) | set(payload.get("timing", {}))


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _isolate_metrics():
    """The collector is a process-wide singleton shared with every other test."""
    metrics.reset()
    yield
    metrics.reset()


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from web.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


# ─── The leak ─────────────────────────────────────────────────────────────────

class TestMetricsDoesNotEchoIdentifiers:

    def test_viewer_cannot_read_back_the_ids_others_used(self, client, monkeypatch):
        headers = _login(monkeypatch, "viewer")

        for method, path, _template in ID_BEARING_REQUESTS:
            client.request(method, path, headers=headers)

        body = _read_metrics(client, monkeypatch)
        keys = _metric_keys(body)

        leaked = sorted(
            key for key in keys
            if any(fragment in key for fragment in SECRET_FRAGMENTS)
        )
        assert not leaked, (
            "the metric read back the identifiers other callers used:\n  "
            + "\n  ".join(leaked)
        )

    def test_the_route_is_still_named(self, client, monkeypatch):
        """Templating must not cost the metric its per-route breakdown."""
        headers = _login(monkeypatch, "viewer")

        for method, path, _template in ID_BEARING_REQUESTS:
            client.request(method, path, headers=headers)

        keys = _metric_keys(_read_metrics(client, monkeypatch))
        for method, _path, template in ID_BEARING_REQUESTS:
            assert f"{method} {template}" in keys, \
                f"{method} {template} vanished from the metric instead of being templated"

    def test_a_refused_request_is_recorded_without_its_id(self, client, monkeypatch):
        """The 403 path is the one that matters: an attacker probing ids they
        may not read still writes a key, so the key must carry no id."""
        headers = _login(monkeypatch, "viewer")

        assert client.get(f"/api/admin/users/{USER_ID}", headers=headers).status_code == 403

        body = _read_metrics(client, monkeypatch)
        assert body["requests"].get("GET /api/admin/users/{user_id}") == 1
        assert USER_ID not in json.dumps(body)


class TestMetricKeysAreBoundedByTheRouteTable:
    """One key per route, not one per id ever requested."""

    def test_many_ids_collapse_to_one_key(self, client, monkeypatch):
        headers = _login(monkeypatch, "viewer")

        for user_id in range(9_100_000, 9_100_050):
            client.get(f"/api/admin/users/{user_id}", headers=headers)

        body = _read_metrics(client, monkeypatch)
        user_keys = [k for k in body["requests"] if "/api/admin/users/" in k]
        assert user_keys == ["GET /api/admin/users/{user_id}"], \
            f"50 ids produced {len(user_keys)} keys — the dictionary still grows with traffic"
        assert body["requests"]["GET /api/admin/users/{user_id}"] == 50

    def test_static_assets_share_one_key(self, client, monkeypatch):
        """nginx proxies /static/ here, so every hashed filename ever built used
        to add a key and a list of 100 latency samples that nothing evicted."""
        for name in ("index-aaa111.js", "index-bbb222.js", "style-ccc333.css"):
            client.get(f"/static/{name}")

        keys = _metric_keys(_read_metrics(client, monkeypatch))
        static_keys = sorted(k for k in keys if "static" in k)
        assert static_keys == ["GET /static/*"], \
            f"static filenames are still metric keys: {static_keys}"


# ─── The middleware must survive whatever arrives ─────────────────────────────

class TestUnroutableRequestsAreHandled:
    """This runs on every request Docker and nginx make. A shape it cannot key
    must produce a sentinel, never an exception and never the raw path."""

    def test_unmatched_path_key(self):
        assert request_metric_key("GET", "/nothing/here/12345", {}) == "GET <unmatched>"

    def test_mounted_app_with_no_route_is_keyed_by_its_mount(self):
        scope = {"root_path": "/static", "path_params": {}}
        assert request_metric_key("GET", "/static/index-aaa111.js", scope) == "GET /static/*"

    def test_a_matched_route_without_parameters_keeps_its_path(self):
        scope = {"route": object(), "path_params": {}}
        assert request_metric_key("GET", "/api/summary", scope) == "GET /api/summary"

    def test_a_multi_segment_parameter_is_templated_whole(self):
        """The SPA catch-all is `/{path:path}` — its value spans segments."""
        scope = {"route": object(), "path_params": {"path": "api/no/such/thing/42"}}
        assert request_metric_key("GET", "/api/no/such/thing/42", scope) == "GET /{path}"

    def test_an_empty_parameter_value_is_not_substituted(self):
        """`"".join` semantics would splice the name between every character."""
        scope = {"route": object(), "path_params": {"path": ""}}
        assert request_metric_key("GET", "/", scope) == "GET /"

    def test_an_unusable_scope_fails_closed(self):
        """Anything unexpected must cost the metric, never the request."""
        assert request_metric_key("GET", "/api/buyers/1", None) == "GET <unmatched>"

    def test_health_check_still_answers_through_the_middleware(self, client):
        """Docker's healthcheck and nginx poll this unauthenticated."""
        assert client.get("/api/health").status_code == 200

    def test_a_404_does_not_break_the_request(self, client):
        assert client.get("/static/definitely-absent.js").status_code == 404


# ─── The decision to gate ─────────────────────────────────────────────────────

# The health router's operational readouts. Viewer-readable until the owner's
# decision O8 (a) (2026-11-11) put each behind `require_admin`.
ADMIN_ONLY_READOUTS = (METRICS_PATH, "/api/health/detailed", "/api/health/data-quality")


class TestTheOperationalReadoutsAreAdminOnly:
    """Recorded as a test, as the earlier answer was (any approved session
    may read them), so opening them again is a decision somebody makes on
    purpose. Mutation: drop `require_admin` from any of the three."""

    @pytest.mark.parametrize("path", ADMIN_ONLY_READOUTS)
    @pytest.mark.parametrize("role", ["viewer", "editor"])
    def test_a_non_admin_session_is_refused(self, client, monkeypatch, path, role):
        assert client.get(path, headers=_login(monkeypatch, role)).status_code == 403

    @pytest.mark.parametrize("path", ADMIN_ONLY_READOUTS)
    def test_an_admin_may_read_it(self, client, monkeypatch, path):
        assert client.get(path, headers=_login(monkeypatch, "admin")).status_code == 200

    @pytest.mark.parametrize("path", ADMIN_ONLY_READOUTS)
    def test_it_still_requires_a_session(self, client, path):
        assert client.get(path).status_code == 401

    def test_the_public_health_stays_open(self, client):
        """The canary, Docker, nginx and the uptime monitor read it with no
        session; O8 (a) left it as it was."""
        assert client.get("/api/health").status_code == 200


# ─── The public endpoint ──────────────────────────────────────────────────────

class TestPublicHealthCarriesNoExceptionText:

    @pytest.fixture
    def broken_store(self, monkeypatch):
        """A store that cannot be opened, reporting the way DuckDB reports it."""
        detail = (
            'IO Error: Could not set lock on file "/app/data/analytics.duckdb": '
            "Conflicting lock is held in /usr/local/bin/python (PID 41) by user root."
        )
        asked = []

        async def _raise():
            asked.append(True)
            raise RuntimeError(detail)

        monkeypatch.setattr("core.store.get_store", _raise)
        monkeypatch.setattr("web.routes.api._deps.get_store", _raise)
        yield asked

    def test_anonymous_caller_gets_no_database_path(self, client, broken_store):
        response = client.get("/api/health")
        assert response.status_code == 200

        body = response.text
        for fragment in ("analytics.duckdb", "/app/data", "PID", "user root", "IO Error"):
            assert fragment not in body, \
                f"the public health endpoint published {fragment!r}: {body}"

    def test_the_store_is_not_asked_and_degrades_nothing(self, client, broken_store):
        """Step 14's S14-17: the `duckdb` block, the ledger and their three
        causes went, so `status` and `degraded_by` — which bot/canary.py,
        Docker and the deploy's gate read — are `healthy` and `[]` whatever
        the DuckDB store would say, and nothing asks it. Mutation: put either
        block back — the store is asked, and fails."""
        body = client.get("/api/health").json()
        assert body["status"] == "healthy" and body["degraded_by"] == []
        assert "duckdb" not in body and "migrations" not in body
        assert broken_store == []
