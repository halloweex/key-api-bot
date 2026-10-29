"""The Rust pilot decides who may ask the way the Python app does.

One Postgres, two services: Python is `web.main.app` called in-process, Rust
is the pilot at `KS_RUST_PILOT_URL`. Every request below goes to both.

WHAT IS COMPARED

`GET /api/revenue/trend`, the six `GET /api/margin/*` routes (wave 1's first
tab, read from Postgres as production reads them) and the SPA catch-all's
JSON 404 for an `/api` path are the endpoints the pilot answers, and they
are compared whole: status,
content type and body — byte for byte unless the answer is a 200, which is
compared as parsed JSON, `0` and `0.0` apart.

Every other route the pilot answers 501 once its gates let a request
through, so for those the gates' decision is what is compared. Python's
endpoint for such a route is replaced — in this process, for this module —
by one that keeps the route's gates (the dependencies FastAPI runs before the
first that is not a gate) and answers 299 when they pass. A refusal must be
the same on both sides to the byte: the frontend redirects to sign-in on the
401 and draws its guard from the 403. A 299 must be Rust's 501 naming the
route. Neither service has a way to be asked what a gate would decide: the
stub is the test's, and the pilot's image carries no such door.

THE MATRIX

- cookie states × routes with every kind of gate: none, garbage, another key,
  another salt, a tampered signature or payload, expired, from the future,
  about to expire, compressed or not, junk in the signature the base64 skips,
  quoted, two of them, two headers — and payloads only the key can sign: not
  base64, not zlib, not a dict, no user, `user_id` 0, text, a float, `true`,
  a list, NaN, too big, a timestamp past year 9999;
- users × every route × `sales_type` absent, `internal` and ` InTeRnAl `:
  a hardcoded admin; approved viewers, editors and admins with no tab set, an
  empty one, a blank one, one, two and an untidy one; levels the code does
  not know; pending, denied, frozen, a status in capitals; nobody in the
  dashboard's list but in the bot's, approved, pending or without a status;
  nobody anywhere;
- the role matrix empty with Rust asked first, as Python finds it before its
  first seed; a customised matrix; the schema at another revision; each of
  the three tables unreadable;
- routing before the gates: wrong methods, unknown paths, `%2F`, Python's `$`
  before a final newline, a slash redirect;
- the trend's 30-a-minute limit: the 31st request, a key that strips to
  nothing, a key and its padded twin, refusals and 422s that cost nothing,
  400s that count, and a window that ends.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_PILOT_ACCESS_REPORT` set, the counts are
written there.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import re
import secrets
import time
from collections import Counter
from itertools import count
from typing import Dict, List, Optional, Sequence, Tuple
from unittest.mock import AsyncMock, patch
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT = os.getenv("KS_RUST_PILOT_ACCESS_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
    pytest.mark.asyncio,
]

TREND = "/api/revenue/trend"
# The routes the pilot answers past the gates, by method and template.
MARGIN = tuple(f"/api/margin/{name}" for name in (
    "overview", "by-brand", "by-category", "trend", "brand-category", "alerts"))
PORTED = {("GET", TREND), ("GET", "/{path:path}")} | {("GET", path) for path in MARGIN}
PASSED = 299
# Explicit dates: both services read the real clock, and a range does not move.
TREND_QUERY = "start_date=2026-01-05&end_date=2026-01-11"
SALES_TYPES = ("", "sales_type=internal", "sales_type=%20InTeRnAl%20")
BASE_ID = 9_100_000_000

NO_ROW = "no row"
# Each person: their row in app.dashboard_users as (role, status,
# allowed_features), or None; and their row's status in app.authorized_users.
SUBJECTS: Dict[str, tuple] = {
    "viewer": (("viewer", "approved", None), NO_ROW),
    "viewer, no tabs": (("viewer", "approved", ""), NO_ROW),
    "viewer, blank tabs": (("viewer", "approved", "   "), NO_ROW),
    "viewer, traffic": (("viewer", "approved", "traffic"), NO_ROW),
    "viewer, dashboard and sms": (("viewer", "approved", "dashboard,sms"), NO_ROW),
    "viewer, untidy tabs": (("viewer", "approved", " Dashboard , TRAFFIC,unknown,,margin "), NO_ROW),
    "editor": (("editor", "approved", None), NO_ROW),
    "editor, traffic and expenses": (("editor", "approved", "traffic,expenses"), NO_ROW),
    "admin": (("admin", "approved", None), NO_ROW),
    "admin, no tabs": (("admin", "approved", ""), NO_ROW),
    "admin, margin": (("admin", "approved", "margin"), NO_ROW),
    "marketer": (("marketer", "approved", None), NO_ROW),
    "marketer, dashboard and traffic": (("marketer", "approved", "dashboard,traffic"), NO_ROW),
    "Admin in capitals": (("Admin", "approved", None), NO_ROW),
    "no role, dashboard": (("", "approved", "dashboard"), NO_ROW),
    "pending": (("viewer", "pending", None), NO_ROW),
    "denied admin": (("admin", "denied", None), NO_ROW),
    "frozen": (("viewer", "frozen", None), NO_ROW),
    "APPROVED in capitals": (("viewer", "APPROVED", None), NO_ROW),
    "denied, approved by the bot": (("viewer", "denied", None), "approved"),
    "bot: approved": (None, "approved"),
    "bot: pending": (None, "pending"),
    "bot: no status": (None, None),
    "nobody": (None, NO_ROW),
}
SUBJECT_IDS = {name: BASE_ID + i for i, name in enumerate(SUBJECTS, 1)}

_REPORT: Dict[str, object] = {"phases": {}}
# A client key per request, and per run: the pilot's limiter outlives a test
# process, so a rerun within the minute must not reuse a key.
_RUN = secrets.token_hex(3)
_CLIENTS = count(1)


# ─── cookies ─────────────────────────────────────────────────────────────────

def _mint(payload, *, age: int = 0, ts: Optional[int] = None,
          secret: Optional[str] = None, salt: bytes = b"itsdangerous") -> str:
    """What the app's serializer writes, signed when we say."""
    from itsdangerous import TimestampSigner, URLSafeTimedSerializer
    from web.routes.auth import SECRET_KEY

    stamp = ts if ts is not None else int(time.time()) - age

    class At(TimestampSigner):
        def get_timestamp(self) -> int:
            return stamp

    return URLSafeTimedSerializer(secret or SECRET_KEY, salt=salt, signer=At).dumps(payload)


def _signed_raw(payload: bytes) -> str:
    """A payload the serializer would never write, signed with the key."""
    from itsdangerous import TimestampSigner
    from web.routes.auth import SECRET_KEY

    return TimestampSigner(SECRET_KEY, salt=b"itsdangerous").sign(payload).decode()


def _session(user_id) -> dict:
    """A session as `create_session_data` builds it — long enough to compress,
    and claiming admin, which no row-backed session is believed about."""
    now = int(time.time())
    return {"user_id": user_id, "first_name": "Олена", "last_name": "Тестова",
            "username": "pilot_viewer", "photo_url": "https://t.me/i/userpic/320/" + "ab" * 20 + ".jpg",
            "auth_date": now, "logged_in_at": now, "role": "admin"}


def _cookie(value: str) -> List[Tuple[str, str]]:
    return [("Cookie", f"dashboard_session={value}")]


def _admin_id() -> int:
    from core.permissions import ADMIN_USER_IDS

    return min(ADMIN_USER_IDS)


def _cookie_states() -> Dict[str, List[Tuple[str, str]]]:
    from web.routes.auth import SESSION_MAX_AGE

    viewer = SUBJECT_IDS["viewer"]
    compressed = _mint(_session(viewer))
    small = _mint({"user_id": viewer})
    value, signature = small.rsplit(".", 1)
    payload, stamp = value.rsplit(".", 1)
    flipped = "A" if payload[3] != "A" else "B"
    assert compressed.startswith("."), "the long session should be written compressed"
    return {
        "no cookie": [],
        "empty": _cookie(""),
        "garbage": _cookie("garbage"),
        "broken base64": _cookie("%%%.@@@.***"),
        "another key": _cookie(_mint(_session(viewer), secret="another-key-entirely")),
        "another salt": _cookie(_mint(_session(viewer), salt=b"cookie-session")),
        "tampered signature": _cookie(f"{value}.{signature[:-1]}{'A' if signature[-1] != 'A' else 'B'}"),
        "tampered payload": _cookie(f"{payload[:3]}{flipped}{payload[4:]}.{stamp}.{signature}"),
        "expired": _cookie(_mint(_session(viewer), age=SESSION_MAX_AGE + 120)),
        "from the future": _cookie(_mint(_session(viewer), age=-600)),
        "about to expire": _cookie(_mint(_session(viewer), age=SESSION_MAX_AGE - 120)),
        "compressed": _cookie(compressed),
        "uncompressed": _cookie(small),
        "junk in the signature": _cookie(f"{value}.{signature[:5]}!!{signature[5:]}"),
        "quoted": [("Cookie", f'dashboard_session="{small}"')],
        "two, the last valid": [("Cookie", f"dashboard_session=garbage; dashboard_session={small}")],
        "two, the first valid": [("Cookie", f"dashboard_session={small}; dashboard_session=garbage")],
        "two headers, the second valid": [("Cookie", "dashboard_session=garbage"),
                                          ("Cookie", f"dashboard_session={small}")],
        "another cookie only": [("Cookie", "theme=dark")],
        "hardcoded admin": _cookie(_mint({"user_id": _admin_id()})),
        # Only the key signs these: the app never writes them.
        "signed: not base64": _cookie(_signed_raw(b"!!!")),
        "signed: not zlib": _cookie(_signed_raw(b".bm90IHpsaWI")),
        "signed: a list": _cookie(_mint([1, 2])),
        "signed: no user": _cookie(_mint({"role": "admin"})),
        "signed: user 0": _cookie(_mint({"user_id": 0})),
        "signed: user as text": _cookie(_mint({"user_id": str(viewer)})),
        "signed: user as a float": _cookie(_mint({"user_id": viewer + 0.7})),
        "signed: admin as a float": _cookie(_mint({"user_id": float(_admin_id())})),
        "signed: user true": _cookie(_mint({"user_id": True})),
        "signed: user a list": _cookie(_mint({"user_id": [viewer]})),
        "signed: user too big": _cookie(_mint({"user_id": 2 ** 70})),
        "signed: user NaN": _cookie(_mint({"user_id": float("nan")})),
        "signed: past year 9999": _cookie(_mint({"user_id": viewer}, ts=2 ** 40)),
    }


def _subject_cookie(name: str) -> List[Tuple[str, str]]:
    if name == "hardcoded admin":
        return _cookie(_mint(_session(_admin_id())))
    return _cookie(_mint(_session(SUBJECT_IDS[name])))


# ─── the routes ──────────────────────────────────────────────────────────────

def _contract_routes() -> List[Tuple[str, str, str]]:
    """(method, a concrete path, the template) for every route a dashboard
    session can ask, read from the exported contract."""
    from tests.rust_access_contract import CONTRACT_PATH

    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    out = []
    for route in contract["routes"]:
        if route["kind"] != "api" or not route["path"].startswith(("/api/", "/ws/")):
            continue
        concrete = re.sub(r"\{[^}]+\}", "1", route["path"])
        for method in route["methods"]:
            out.append((method, concrete, route["path"]))
    return out


def _url(path: str, *queries: str) -> str:
    query = "&".join(q for q in queries if q)
    return f"{path}?{query}" if query else path


def _ported(method: str, template: str) -> bool:
    return (method, template) in PORTED


# ─── comparing ───────────────────────────────────────────────────────────────

def _first_difference(a, b, path="$") -> Optional[str]:
    if type(a) is not type(b):
        return f"{path}: {a!r} against {b!r}"
    if isinstance(a, dict):
        for key in sorted(set(a) | set(b)):
            if key not in a or key not in b:
                return f"{path}.{key}: only on one side"
            found = _first_difference(a[key], b[key], f"{path}.{key}")
            if found:
                return found
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path}: {len(a)} items against {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            found = _first_difference(x, y, f"{path}[{i}]")
            if found:
                return found
        return None
    return None if repr(a) == repr(b) else f"{path}: {a!r} against {b!r}"


def _differs(py, rs, ported: bool) -> Optional[str]:
    if py.status_code == PASSED:
        if ported:
            return "the stub answered a route the pilot ports"
        try:
            branch = rs.json().get("branch")
        except ValueError:
            branch = None
        if rs.status_code == 501 and branch == "route":
            return None
        return f"Python's gates passed; Rust answered {rs.status_code}: {rs.text[:160]}"
    if py.status_code != rs.status_code:
        return f"status {py.status_code} against {rs.status_code}: {py.text[:120]!r} | {rs.text[:120]!r}"
    if py.headers.get("content-type") != rs.headers.get("content-type"):
        return f"content-type {py.headers.get('content-type')} against {rs.headers.get('content-type')}"
    if py.status_code == 307:
        where = [urlsplit(r.headers.get("location", "")) for r in (py, rs)]
        if (where[0].path, where[0].query) != (where[1].path, where[1].query):
            return f"redirected to {where[0]} against {where[1]}"
    if py.status_code == 200:
        a, b = json.loads(py.content), json.loads(rs.content)
        if json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True):
            return None
        return _first_difference(a, b) or "the same values, differently serialised"
    if py.content != rs.content:
        return f"bodies differ: {py.content[:160]!r} | {rs.content[:160]!r}"
    return None


class Batch:
    """One phase's requests, sent to both, compared, counted."""

    def __init__(self, phase: str):
        self.phase = phase
        self.statuses: Counter = Counter()
        self.kinds: Counter = Counter()
        self.differences: List[str] = []
        self.requests = 0

    def record(self, label: str, method: str, template: str, py, rs) -> None:
        self.requests += 1
        self.statuses[str(py.status_code)] += 1
        ported = _ported(method, template)
        self.kinds["ported" if ported else "gate only"] += 1
        if py.status_code == PASSED:
            self.kinds["passed the gates"] += 1
        elif py.status_code in (401, 403):
            self.kinds["refused by a gate"] += 1
        if py.status_code not in (200, PASSED) and py.content == rs.content:
            self.kinds["byte-identical refusal or error"] += 1
        found = _differs(py, rs, ported)
        if found:
            self.differences.append(f"{label} | {method} {template}: {found}")

    def close(self) -> None:
        _REPORT["phases"][self.phase] = {
            "requests": self.requests,
            "differences": len(self.differences),
            "statuses": dict(sorted(self.statuses.items())),
            "kinds": dict(self.kinds),
            "first_differences": self.differences[:20],
        }
        assert not self.differences, (
            f"{self.phase}: {len(self.differences)} of {self.requests} requests answer "
            f"differently:\n" + "\n".join(self.differences[:20])
        )


async def _ask(client, method: str, url: str, headers: Sequence[Tuple[str, object]]):
    with_ip = list(headers)
    if not any(name.lower() == "x-real-ip" for name, _ in with_ip):
        with_ip.append(("X-Real-IP", f"pilot-{_RUN}-{next(_CLIENTS)}"))
    return await client.request(method, url, headers=with_ip)


async def _both(services, method, url, headers, *, rust_first=False):
    python, rust, _ = services
    if rust_first:
        rs = await _ask(rust, method, url, headers)
        py = await _ask(python, method, url, headers)
    else:
        py = await _ask(python, method, url, headers)
        rs = await _ask(rust, method, url, headers)
    return py, rs


# ─── the world both services see ─────────────────────────────────────────────

async def _seed(pool) -> List[tuple]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            for table in ("app.dashboard_users", "app.authorized_users"):
                await conn.execute(f"DELETE FROM {table} WHERE user_id BETWEEN $1 AND $2",
                                   BASE_ID, BASE_ID + 999)
            for name, (row, bot) in SUBJECTS.items():
                uid = SUBJECT_IDS[name]
                if row is not None:
                    await conn.execute(
                        "INSERT INTO app.dashboard_users (user_id, role, status, allowed_features) "
                        "VALUES ($1, $2, $3, $4)", uid, *row)
                if bot is not NO_ROW:
                    await conn.execute(
                        "INSERT INTO app.authorized_users (user_id, status) VALUES ($1, $2)", uid, bot)
        matrix = [tuple(r) for r in await conn.fetch(
            "SELECT role, feature, can_view, can_edit, can_delete FROM app.role_permissions")]
        await conn.execute("DELETE FROM app.role_permissions")
    return matrix


async def _unseed(pool, matrix: List[tuple]) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            for table in ("app.dashboard_users", "app.authorized_users"):
                await conn.execute(f"DELETE FROM {table} WHERE user_id BETWEEN $1 AND $2",
                                   BASE_ID, BASE_ID + 999)
            await conn.execute("DELETE FROM app.role_permissions")
            if matrix:
                await conn.executemany(
                    "INSERT INTO app.role_permissions (role, feature, can_view, can_edit, can_delete) "
                    "VALUES ($1, $2, $3, $4, $5)", matrix)


def _stub_unported(app) -> List[tuple]:
    """Every route the pilot does not port keeps its gates and answers 299."""
    from fastapi import routing as fastapi_routing
    from fastapi.responses import Response

    from tests.rust_access_contract import _dependency

    async def passed(**_):
        return Response(status_code=PASSED)

    saved = []
    for entry in app.router.routes:
        if not isinstance(entry, fastapi_routing._IncludedRouter):
            continue
        for ctx in entry.effective_route_contexts():
            if not isinstance(ctx.original_route, fastapi_routing.APIRoute):
                continue
            if any(_ported(method, ctx.path) for method in ctx.methods):
                continue
            gates = []
            for sub in ctx.dependant.dependencies:
                if "gate" not in _dependency(sub.call):
                    break
                gates.append(sub)
            saved.append((ctx, ctx.dependant, ctx.body_field, ctx._embed_body_fields))
            ctx.dependant = dataclasses.replace(
                ctx.dependant, call=passed, dependencies=gates, path_params=[],
                query_params=[], header_params=[], cookie_params=[], body_params=[],
                request_param_name=None, websocket_param_name=None,
                http_connection_param_name=None, response_param_name=None,
                background_tasks_param_name=None, security_scopes_param_name=None,
            )
            ctx.body_field = None
            ctx._embed_body_fields = False
    return saved


@pytest.fixture(scope="module")
def world():
    """The people in both lists, the matrix emptied, unported routes stubbed:
    once for the module, and all of it put back afterwards."""
    from web.main import app

    async def _with_pool(work, *args):
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            return await work(pool, *args)
        finally:
            await pool.close()

    matrix = asyncio.run(_with_pool(_seed))
    saved = _stub_unported(app)
    _REPORT["routes_stubbed"] = len(saved)
    try:
        yield {"run": secrets.token_hex(4)}
    finally:
        for ctx, dependant, body_field, embed in saved:
            ctx.dependant, ctx.body_field, ctx._embed_body_fields = dependant, body_field, embed
        asyncio.run(_with_pool(_unseed, matrix))
        if REPORT:
            totals = Counter()
            for phase in _REPORT["phases"].values():
                totals["requests"] += phase["requests"]
                totals["differences"] += phase["differences"]
            _REPORT["totals"] = dict(totals)
            with open(REPORT, "w", encoding="utf-8") as out:
                json.dump(_REPORT, out, indent=2, ensure_ascii=False)


@pytest_asyncio.fixture
async def services(world, duckdb_off, monkeypatch):
    from core import bot_store, read_fallback
    from core.permissions import invalidate_permissions_cache
    from web.main import app
    from web.ratelimit import limiter

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_GOLD", "postgres")
    monkeypatch.setenv("KS_READ_MARGIN", "postgres")
    monkeypatch.setenv("KS_USER_STORE", "postgres")
    monkeypatch.setenv("KS_BOT_STORE", "postgres")
    monkeypatch.setattr(limiter, "enabled", True)
    limiter.reset()
    invalidate_permissions_cache()
    read_fallback.reset_counts()
    previous = bot_store.use_bot_store(None)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=5)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            async with httpx.AsyncClient(transport=transport, base_url="http://python") as python, \
                    httpx.AsyncClient(base_url=PILOT, timeout=60) as rust:
                yield python, rust, pool
    finally:
        current = bot_store.use_bot_store(previous)
        if current is not None and hasattr(current, "close"):
            current.close()
        invalidate_permissions_cache()
        await pool.close()


# ─── the tests ───────────────────────────────────────────────────────────────

PERMISSION_ROUTES = (
    ("GET", f"{TREND}?{TREND_QUERY}", TREND),
    ("GET", "/api/summary", "/api/summary"),
    ("GET", "/api/traffic/analytics", "/api/traffic/analytics"),
    ("GET", "/api/margin/overview", "/api/margin/overview"),
    ("GET", "/api/expenses", "/api/expenses"),
    ("POST", "/api/expenses", "/api/expenses"),
    ("GET", "/api/customers/sms-campaigns", "/api/customers/sms-campaigns"),
    ("GET", "/api/admin/users", "/api/admin/users"),
)


async def test_the_pilot_holds_the_apps_session_key(services):
    """Every other test rests on this: a cookie the app signs, the pilot reads."""
    _, rust, _ = services
    response = await _ask(rust, "GET", "/api/me", _cookie(_mint({"user_id": _admin_id()})))
    assert response.status_code == 501, (
        f"the pilot answered {response.status_code} to the hardcoded admin's cookie: it "
        f"does not hold the key the app signs with, or not the admin ids ({response.text[:200]})"
    )


async def test_an_unseeded_matrix_answers_as_the_seed_would(services):
    """Rust is asked first, so every one of its answers is computed against the
    empty table Python finds before its first seed; Python then seeds."""
    python, rust, pool = services
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM app.role_permissions") == 0
    batch = Batch("matrix unseeded, Rust first")
    asked = []
    for name in SUBJECTS:
        for method, url, template in PERMISSION_ROUTES:
            cookie = _subject_cookie(name)
            asked.append((name, method, url, template, cookie, await _ask(rust, method, url, cookie)))
    for name, method, url, template, cookie, rs in asked:
        py = await _ask(python, method, url, cookie)
        batch.record(name, method, template, py, rs)
    async with pool.acquire() as conn:
        seeded = await conn.fetchval("SELECT count(*) FROM app.role_permissions")
    _REPORT["matrix_rows_after_python_seeded"] = seeded
    assert seeded > 0, "Python never seeded: no permission gate was reached"
    batch.close()


async def test_every_cookie_state(services):
    batch = Batch("cookie states")
    routes = (("GET", f"{TREND}?{TREND_QUERY}", TREND), ("GET", "/api/me", "/api/me"),
              ("GET", "/api/admin/users", "/api/admin/users"),
              ("GET", "/api/traffic/analytics", "/api/traffic/analytics"),
              ("GET", "/api/summary", "/api/summary"), ("GET", "/api/categories", "/api/categories"),
              ("GET", "/api/health", "/api/health"), ("POST", "/api/expenses", "/api/expenses"),
              ("GET", "/ws/stats", "/ws/stats"))
    for state, headers in _cookie_states().items():
        for method, url, template in routes:
            py, rs = await _both(services, method, url, headers)
            batch.record(state, method, template, py, rs)
    batch.close()


async def test_every_user_over_every_route(services):
    batch = Batch("users × routes × sales_type")
    routes = _contract_routes()
    _REPORT["routes_swept"] = len(routes)
    for name in ["hardcoded admin", *SUBJECTS]:
        cookie = _subject_cookie(name)
        for method, path, template in routes:
            for sales_type in SALES_TYPES:
                query = TREND_QUERY if template == TREND else ""
                py, rs = await _both(services, method, _url(path, query, sales_type), cookie)
                batch.record(name, method, template, py, rs)
    batch.close()


async def test_routing_before_any_gate(services):
    batch = Batch("routing")
    probes = (
        ("POST", TREND, TREND), ("DELETE", TREND, TREND), ("PUT", "/api/me", "/api/me"),
        ("OPTIONS", "/api/summary", "/api/summary"), ("PATCH", "/api/health", "/api/health"),
        ("GET", "/api/nothing-here", "/{path:path}"), ("POST", "/api/nothing-here", "-"),
        ("GET", "/api", "/{path:path}"), ("GET", "/api/", "/{path:path}"),
        ("GET", f"/api/revenue%2Ftrend?{TREND_QUERY}", TREND),
        ("GET", f"{TREND}/?{TREND_QUERY}", "/{path:path}"),
        ("GET", "/api/health%0A", "/api/health"),
        ("GET", f"/api/revenue/trend%0A?{TREND_QUERY}", TREND),
        ("GET", "/api/categories%3F", "/{path:path}"), ("GET", "/api/health/", "/{path:path}"),
        ("GET", "/api/jobs/1/trigger", "-"), ("POST", "/api/jobs/a%0Ab/trigger/", "-"),
        ("GET", "/api/me%23x", "/{path:path}"), ("GET", "/api/admin/users%0A", "/api/admin/users"),
    )
    for state in ("no cookie", "viewer", "hardcoded admin"):
        cookie = [] if state == "no cookie" else _subject_cookie(state)
        for method, url, template in probes:
            py, rs = await _both(services, method, url, cookie)
            batch.record(state, method, template, py, rs)
    batch.close()


async def test_a_customised_matrix(services):
    """Rows turned off, one deleted (Python's next seed puts it back, which is
    why Rust is asked first), and a level the code does not know given two."""
    from core.permissions import invalidate_permissions_cache

    python, _, pool = services
    # The table as the seed leaves it, whatever ran before.
    await _ask(python, "GET", "/api/summary", _subject_cookie("viewer"))
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE app.role_permissions SET can_view = false WHERE role = 'viewer' AND feature = 'dashboard'")
        await conn.execute(
            "UPDATE app.role_permissions SET can_view = false WHERE role = 'admin' AND feature = 'margin'")
        await conn.execute(
            "UPDATE app.role_permissions SET can_edit = false WHERE role = 'editor' AND feature = 'expenses'")
        await conn.execute("DELETE FROM app.role_permissions WHERE role = 'editor' AND feature = 'traffic'")
        await conn.execute(
            "INSERT INTO app.role_permissions (role, feature, can_view, can_edit) VALUES "
            "('marketer', 'dashboard', true, false), ('marketer', 'margin', true, true)")
    invalidate_permissions_cache()
    batch = Batch("matrix customised, Rust first")
    for name in SUBJECTS:
        for method, url, template in PERMISSION_ROUTES:
            py, rs = await _both(services, method, url, _subject_cookie(name), rust_first=True)
            batch.record(name, method, template, py, rs)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.role_permissions")
    invalidate_permissions_cache()
    batch.close()


@pytest.mark.parametrize("failure", ["revision", "app.dashboard_users", "app.authorized_users",
                                     "app.role_permissions"])
async def test_when_a_store_cannot_answer(services, failure):
    """Another schema revision, and each table the session reads made
    unreadable: an error in the user list is a 401, in the bot's list the
    app's 500, in the matrix the code's own matrix."""
    from core.pg import REQUIRED_REVISION
    from core.permissions import invalidate_permissions_cache

    _, _, pool = services
    aside = failure.replace("app.", "") + "_pilot_aside"
    async with pool.acquire() as conn:
        if failure == "revision":
            await conn.execute("UPDATE meta.alembic_version SET version_num = 'pilot-elsewhere'")
        else:
            await conn.execute(f"ALTER TABLE {failure} RENAME TO {aside}")
    invalidate_permissions_cache()
    batch = Batch(f"unreadable: {failure}")
    try:
        for name in ["hardcoded admin", *SUBJECTS]:
            for method, url, template in PERMISSION_ROUTES[:4]:
                py, rs = await _both(services, method, url, _subject_cookie(name))
                batch.record(name, method, template, py, rs)
    finally:
        async with pool.acquire() as conn:
            if failure == "revision":
                await conn.execute("UPDATE meta.alembic_version SET version_num = $1", REQUIRED_REVISION)
            else:
                await conn.execute(f"ALTER TABLE app.{aside} RENAME TO {failure.split('.')[1]}")
        invalidate_permissions_cache()
    batch.close()


async def test_the_trends_rate_limit(services, world):
    """slowapi's 30 a minute, keyed on X-Real-IP, checked after the gates and
    after the query parses — so refusals and 422s cost nothing and 400s do."""
    batch = Batch("rate limit")
    run = world["run"]
    admin = _subject_cookie("hardcoded admin")
    url = f"{TREND}?{TREND_QUERY}"

    async def burst(label, n, key, headers=admin, target=url):
        seen = []
        for _ in range(n):
            py, rs = await _both(services, "GET", target, [*headers, ("X-Real-IP", key)])
            batch.record(label, "GET", TREND, py, rs)
            seen.append(py.status_code)
        return seen

    first_hit = time.time()
    assert await burst("thirty-two", 32, f"rl-{run}-a") == [200] * 30 + [429] * 2
    assert set(await burst("a key of nothing but NBSP", 35, b"\xa0")) == {200}
    await burst("padded key", 15, b"\xa0rl-" + run.encode() + b"-b\xa0")
    assert await burst("its bare twin", 16, f"rl-{run}-b") == [200] * 15 + [429]
    assert set(await burst("no session", 40, f"rl-{run}-c", headers=[])) == {401}
    assert await burst("then signed", 31, f"rl-{run}-c") == [200] * 30 + [429]
    assert set(await burst("a 422", 40, f"rl-{run}-d", target=f"{url}&source_id=abc")) == {422}
    assert await burst("then valid", 31, f"rl-{run}-d") == [200] * 30 + [429]
    assert set(await burst("a 400", 30, f"rl-{run}-e", target=f"{url}&period=xyz")) == {400}
    assert await burst("then valid", 1, f"rl-{run}-e") == [429]
    # The window opened at the first hit and lasts a minute on both sides.
    await asyncio.sleep(max(0.0, first_hit + 61 - time.time()))
    assert await burst("after the window", 1, f"rl-{run}-a") == [200]
    batch.close()
