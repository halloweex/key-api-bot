"""The stage 5 sweep's wider doors (PR-9): what a request, a socket, an event
and the assistant are made of, so `duckdb_off_sweep_run.py` can call every one
of them — never collected on its own, the name does not match `test_*.py`.

The GET sweep reads a route's parameters off the app and sends the required
ones (`test_read_fallback_http._request_for`). The doors this module adds are
the rest of what a week of silence exercises, read off the app the same way:

- every route under `/api/` that is not a GET — the admin levers, the forms,
  the SMS flows, the access requests, the webhook;
- every route outside `/api/` — the pages, the login, the two Telegram
  sign-ins, `/ws/stats`, the icons;
- every WebSocket route, as an admin, as a viewer and as nobody, with the
  messages a client sends;
- every event the bus carries, with a socket open in each room, so the
  handlers web registers at its start broadcast to somebody;
- every tool the assistant offers, through `execute_tool` and through both
  `/api/chat` routes, under a model that asks for every tool;
- every GET route again under a real viewer's session, which nothing patches.

A request a route refuses as malformed — FastAPI's 422 — swept nothing past
its validation, so the outer test fails on one rather than counting it.

Everything here runs on the sweep's own event loop: the requests through an
in-process ASGI transport and the sockets by hand over ASGI, because a test
client runs each request on a loop of its own, and the live flavour's
Postgres pool belongs to the loop that opened it.
"""
from __future__ import annotations

import ast
import asyncio
import hashlib
import hmac
import inspect
import json
import textwrap
import time
import types
import typing
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

# Accounts nobody hardcodes as an admin: a viewer whose session is real —
# `_resolve_session` reads its row, nothing patches it — and two people
# waiting in the bot's queue, one for each access-request door: a request
# the first door approved is no longer the second door's to deny.
VIEWER_ID = 900_000_001
PENDING_ID = 900_000_002
DENIED_ID = 900_000_003

# The TurboSMS callback is signed with a shared secret; the sweep signs its
# own with this one, so the webhook is swept past its signature check.
WEBHOOK_SECRET = "sweep-webhook-secret-not-a-real-one"

# Credentials the doors would otherwise read from a developer's `.env`. The
# network is refused at the socket in any case; these make sure a real token
# is not even in hand.
FAKE_ENV = {
    "TURBOSMS_API_TOKEN": "sweep-turbosms-token-not-a-real-one",
    "TURBOSMS_TOKEN": "",
    "TURBOSMS_SENDER": "Sweep",
    "TURBOSMS_VIBER_SENDER": "Sweep",
    "TURBOSMS_WEBHOOK_SECRET": WEBHOOK_SECRET,
    "TURBOSMS_WEBHOOK_DEBUG": "",
    "ANTHROPIC_API_KEY": "sweep-anthropic-key-not-a-real-one",
}

DAY = date.today() - timedelta(days=3)


# The steps the doors sweep records besides one per door, in either flavour:
# web's start (as `setup`, so it is not the process sweep's `boot`), the
# sockets held open while the events are emitted, and web's stop. Where
# Postgres answers, `setup:derive` too, between the start and the doors.
DOORS_FRAME = ("setup", "setup:init_and_sync", "setup:scheduler_start",
               "events:open", "events:close", "shutdown")


# ─── Which doors ─────────────────────────────────────────────────────────────

def _endpoints(app) -> list:
    from tests.routes_helper import iter_endpoints

    return list(iter_endpoints(app))


def is_socket(endpoint) -> bool:
    """A WebSocket route: no HTTP methods, a WebSocket route's class."""
    return not endpoint.methods and "WebSocket" in type(endpoint.route).__name__


# The order a method is called in at one path: what a later door finds was
# made by an earlier one — a preset saved, then deleted.
METHOD_ORDER = ("POST", "PUT", "PATCH", "DELETE")


def write_routes(app) -> List[Tuple[str, Any]]:
    """`(method, endpoint)` for every route under `/api/` that is not a GET,
    read off the app, in path order and then `METHOD_ORDER`."""
    found = []
    for endpoint in _endpoints(app):
        if not endpoint.path.startswith("/api/") or is_socket(endpoint):
            continue
        for method in sorted(endpoint.methods - {"GET", "HEAD"}, key=METHOD_ORDER.index):
            found.append((method, endpoint))
    return sorted(found, key=lambda pair: (pair[1].path, METHOD_ORDER.index(pair[0])))


def outside_routes(app) -> List[Tuple[str, Any]]:
    """`(method, endpoint)` for every HTTP route outside `/api/` — the pages,
    the sign-ins, `/ws/stats`. Static mounts and the generated docs carry no
    dependant and are not routes the application wrote."""
    found = []
    for endpoint in _endpoints(app):
        if endpoint.path.startswith("/api/") or is_socket(endpoint):
            continue
        for method in sorted(endpoint.methods - {"HEAD"}):
            found.append((method, endpoint))
    return sorted(found, key=lambda pair: (pair[1].path, pair[0]))


def socket_routes(app) -> List[Any]:
    return sorted((e for e in _endpoints(app) if is_socket(e)), key=lambda e: e.path)


def get_api_routes(app) -> List[Any]:
    """Every GET route under `/api/` — the viewer's sweep. Unlike the admin
    one, `/api/chat/stream` is in it: it is admin-only, and a viewer is
    refused before any model is asked."""
    return sorted((e for e in _endpoints(app)
                   if "GET" in e.methods and e.path.startswith("/api/")),
                  key=lambda e: e.path)


# ─── What a request carries ──────────────────────────────────────────────────

# Values by parameter name, where a value of the parameter's type alone would
# end the request at the route's own validation (a 400) before it reached
# anything. Values, not subjects: a name missing here still gets one by type.
EXAMPLE_VALUES: Dict[str, Any] = {
    "period_type": "monthly", "abc_class": "A", "type": "summary",
    "role": "viewer", "status": "approved", "feature": "dashboard",
    "is_retail": True, "buyer_id": 1, "phone": "+380501234567",
    "text": "Sweep", "campaign": "sweep-campaign", "name": "sweep-preset",
    "path": "dashboard", "manager_id": 1, "expense_id": 1, "parent_id": 1,
    "sales_type": "retail", "channel": "sms", "days": 1,
    "chunk_size": 100, "limit": 10,
}

# The JSON bodies of the routes that declare one or read one themselves
# (`request.json()`), by `METHOD path`. `reads_a_body` finds both kinds, and
# the outer test requires every one of them to be here.
EXAMPLE_BODIES: Dict[str, Any] = {
    "POST /api/chat": {"message": "How are sales this week?"},
    "POST /api/expenses": {
        "expense_date": DAY.isoformat(), "category": "marketing",
        "expense_type": "Facebook Ads", "amount": 100.0, "currency": "UAH",
        "note": "stage 5 sweep", "platform": "facebook"},
    "PATCH /api/admin/users/{user_id}/features": {"preset": "standard"},
    # The tabs picked on the approval card, before the grant.
    "POST /api/admin/access-requests/{user_id}/approve": {"preset": "standard"},
    "PATCH /api/me/preferences": {"language": "uk", "notifications_enabled": True},
    "PUT /api/customers/sms-audience-presets/{name}": {
        "grouping": "single", "recency_max": 270},
    # Filled in by `webhook_body`: the signature depends on the event id.
    "POST /api/webhooks/turbosms": None,
    # Filled in by `webapp_body`: the hash depends on the moment.
    "POST /auth/webapp": None,
}


# Optional query parameters a door needs to get past its own refusal, by
# `METHOD path`: a campaign frozen under the placeholder name `default` is
# refused before its roster is read.
EXAMPLE_QUERIES: Dict[str, Dict[str, Any]] = {
    "POST /api/customers/sms-campaigns": {"campaign": "sweep-campaign"},
}


def door_key(method: str, endpoint) -> str:
    return f"{method} {endpoint.path}"


def made_by(key: str, response) -> Dict[str, Any]:
    """What a door made that a later door at its path acts on — the expense
    `POST /api/expenses` created is the one `DELETE` removes."""
    if response is None or response.status_code != 200:
        return {}
    try:
        body = response.json()
    except ValueError:
        return {}
    if key == "POST /api/expenses" and isinstance(body, dict) and body.get("id"):
        return {"expense_id": body["id"]}
    return {}


def path_value(name: str, path: str) -> Any:
    """A path parameter's value: a waiting person for each access-request
    door, the viewer for every other `{user_id}`."""
    if name == "user_id":
        if "/access-requests/" in path:
            return DENIED_ID if path.endswith("/deny") else PENDING_ID
        return VIEWER_ID
    return EXAMPLE_VALUES.get(name, "ab1")


def _by_type(param) -> Any:
    info = param.field_info
    annotation = info.annotation
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        annotation = args[0] if args else str
    if annotation is bool:
        return True
    if annotation is int:
        floor = [m.ge for m in getattr(info, "metadata", ()) if hasattr(m, "ge")]
        return floor[0] if floor else 1
    if annotation is float:
        return 1.0
    if annotation is date:
        return DAY.isoformat()
    return "ab1"


def query_value(param) -> Any:
    if param.name in EXAMPLE_VALUES:
        return EXAMPLE_VALUES[param.name]
    return _by_type(param)


def _required_query(dependant) -> Dict[str, Any]:
    from tests.routes_helper import is_required

    found: Dict[str, Any] = {}

    def walk(node) -> None:
        for param in getattr(node, "query_params", ()) or ():
            if is_required(param) and param.name not in found:
                found[param.name] = query_value(param)
        for sub in getattr(node, "dependencies", ()) or ():
            walk(sub)

    walk(dependant)
    return found


def reads_a_body(endpoint) -> bool:
    """The route declares a body, or reads one itself with `request.json()`
    — read from its source, so a route that starts reading one tomorrow is
    asked for an example the day it does."""
    if endpoint.route.dependant.body_params:
        return True
    try:
        source = textwrap.dedent(inspect.getsource(inspect.unwrap(endpoint.route.endpoint)))
    except (OSError, TypeError):
        return False
    for node in ast.walk(ast.parse(source)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "json" and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "request"):
            return True
    return False


def example_request(method: str, endpoint) -> Tuple[str, Dict[str, Any], Any]:
    """`(path, query, json)` for one door: every path parameter filled, every
    required query parameter, and the body when the route takes one."""
    path = endpoint.path
    for param in endpoint.route.dependant.path_params:
        path = path.replace("{" + param.name + "}",
                            str(path_value(param.name, endpoint.path)))
    query = {**_required_query(endpoint.route.dependant),
             **EXAMPLE_QUERIES.get(door_key(method, endpoint), {})}
    body = EXAMPLE_BODIES.get(door_key(method, endpoint))
    return path, query, body


def webhook_body(message_id: str = "sweep-message-1") -> Dict[str, Any]:
    """A delivery report signed the way the gateway signs it."""
    event_id = f"sweep-event-{message_id}"
    signature = hashlib.sha1(f"{WEBHOOK_SECRET}{event_id}".encode()).hexdigest()
    return {"id": event_id, "signature": signature, "type": "message_status",
            "try": 1, "data": {"message_id": message_id, "status": "DELIVERED"}}


def _bot_token() -> str:
    from bot.config import BOT_TOKEN

    return BOT_TOKEN


def telegram_login_query(user_id: int) -> Dict[str, str]:
    """The Login Widget's callback for `user_id`, signed as Telegram signs it
    (`web.services.auth_service.verify_telegram_auth`)."""
    data = {"id": str(user_id), "first_name": "Sweep", "username": "sweep",
            "auth_date": str(int(time.time()))}
    check = "\n".join(f"{k}={data[k]}" for k in sorted(data))
    secret = hashlib.sha256(_bot_token().encode()).digest()
    data["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return data


def webapp_body(user_id: int) -> Dict[str, str]:
    """A Mini App's `initData` for `user_id`, signed as Telegram signs it
    (`verify_webapp_auth`)."""
    fields = {"auth_date": str(int(time.time())),
              "user": json.dumps({"id": user_id, "first_name": "Sweep",
                                  "username": "sweep"})}
    check = "\n".join(f"{k}={fields[k]}" for k in sorted(fields))
    secret = hmac.new(b"WebAppData", _bot_token().encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return {"initData": urlencode(fields)}


def session_cookie(user_id: int, role: str) -> str:
    """A signed dashboard session, as `/auth/telegram/callback` sets it — a
    real one: `_resolve_session` reads it as it reads any visitor's."""
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    data = create_session_data({"id": str(user_id), "first_name": "Sweep",
                                "last_name": "", "username": "sweep",
                                "auth_date": str(int(time.time()))}, role=role)
    return f"{SESSION_COOKIE}={session_serializer.dumps(data)}"


def hardcoded_admin() -> int:
    from core.permissions import ADMIN_USER_IDS

    return sorted(ADMIN_USER_IDS)[0]


# ─── Requests, on the sweep's loop ───────────────────────────────────────────

class Http:
    """The app called in-process over ASGI, on the loop that runs the sweep —
    as uvicorn calls it, one loop for every request, so a pool one request
    opened serves the next and a task a request leaves behind runs on."""

    def __init__(self, app) -> None:
        import httpx

        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://testserver", follow_redirects=False, timeout=None)

    @staticmethod
    def fresh() -> None:
        """What one request must not inherit from the one before: a rate
        limit's count, a refusal's count, the health route's cached stats."""
        from core import read_fallback
        from web.ratelimit import limiter
        from web.routes.api import health as health_routes

        limiter.reset()
        read_fallback.reset_counts()
        health_routes._stats_cache.update(data=None, expires_at=0)

    async def request(self, method: str, path: str, *, cookie: Optional[str] = None,
                      params: Optional[dict] = None, body: Any = None):
        self.fresh()
        headers = {"cookie": cookie} if cookie else {}
        kwargs: Dict[str, Any] = {"params": params or None, "headers": headers}
        if body is not None:
            kwargs["json"] = body
        return await self.client.request(method, path, **kwargs)

    async def close(self) -> None:
        await self.client.aclose()


class Socket:
    """One WebSocket to the app, driven by hand over ASGI: the handshake, the
    client's messages, the close — and what the app sent back, kept."""

    def __init__(self, app, path: str, cookie: Optional[str] = None) -> None:
        self.app, self.path = app, path
        headers = [(b"host", b"testserver")]
        if cookie:
            headers.append((b"cookie", cookie.encode()))
        self.scope = {
            "type": "websocket", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "scheme": "ws", "path": path, "raw_path": path.encode(),
            "root_path": "", "query_string": b"", "headers": headers,
            "client": ("127.0.0.1", 50000), "server": ("testserver", 80),
            "subprotocols": [], "state": {}, "extensions": {},
        }
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.sent: List[dict] = []
        self._answered = asyncio.Event()
        self.task: Optional[asyncio.Task] = None

    async def _send(self, message: dict) -> None:
        self.sent.append(message)
        if message["type"] in ("websocket.accept", "websocket.close"):
            self._answered.set()

    @property
    def accepted(self) -> bool:
        return any(m["type"] == "websocket.accept" for m in self.sent)

    @property
    def close_code(self) -> Optional[int]:
        for message in self.sent:
            if message["type"] == "websocket.close":
                return message.get("code", 1000)
        return None

    def texts(self) -> List[str]:
        return [m.get("text") or "" for m in self.sent if m["type"] == "websocket.send"]

    def events(self) -> List[str]:
        """The `event` of every JSON frame the app sent
        (`ConnectionManager._send_to_connection`)."""
        found = []
        for text in self.texts():
            try:
                found.append(json.loads(text).get("event"))
            except (ValueError, AttributeError):
                continue
        return found

    async def open(self, timeout: float = 10.0) -> bool:
        await self.inbox.put({"type": "websocket.connect"})
        self.task = asyncio.ensure_future(self.app(self.scope, self.inbox.get, self._send))
        answered = asyncio.ensure_future(self._answered.wait())
        await asyncio.wait({self.task, answered}, timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED)
        answered.cancel()
        return self.accepted

    async def say(self, text: str) -> None:
        """Send one client frame and give the app a moment to answer it."""
        before = len(self.sent)
        await self.inbox.put({"type": "websocket.receive", "text": text})
        for _ in range(20):
            if len(self.sent) > before or (self.task and self.task.done()):
                return
            await asyncio.sleep(0.01)

    async def close(self, timeout: float = 10.0) -> None:
        if self.task is None:
            return
        await self.inbox.put({"type": "websocket.disconnect", "code": 1000})
        try:
            await asyncio.wait_for(self.task, timeout)
        except Exception:  # noqa: BLE001 — the app's own ending, recorded elsewhere
            pass


# What a client sends on a socket: the keep-alive, and the subscribe frame
# the frontend's hook sends.
CLIENT_FRAMES = ("ping", json.dumps({"action": "subscribe", "room": "dashboard"}),
                 json.dumps({"action": "ping"}), "not json")


# ─── Events ──────────────────────────────────────────────────────────────────

# One payload for every event: each handler reads the keys it needs, and the
# counters are non-zero so the broadcasts behind `count > 0` are sent.
EVENT_PAYLOAD = {"count": 1, "duration_ms": 1.0, "sync_type": "orders",
                 "error": "stage 5 sweep", "period": "monthly",
                 "revenue": 1.0, "target": 2.0}


def events() -> list:
    from core.events import SyncEvent

    return list(SyncEvent)


# ─── The assistant ───────────────────────────────────────────────────────────

def _schema_value(name: str, schema: dict) -> Any:
    if "enum" in schema:
        return schema["enum"][0]
    kind = schema.get("type")
    if kind == "string":
        if "date" in name or name.endswith(("_start", "_end")):
            return DAY.isoformat()
        return "sweep"
    if kind == "integer":
        return 1
    if kind == "number":
        return 100.0
    if kind == "boolean":
        return True
    if kind == "array":
        return [_schema_value(name, schema.get("items") or {})]
    if kind == "object":
        return {key: _schema_value(key, sub)
                for key, sub in (schema.get("properties") or {}).items()}
    return "sweep"


def tool_inputs(tool: dict) -> List[Dict[str, Any]]:
    """The inputs one tool is called with: every property filled — an enum
    at its first value — and then once more per other value of each enum,
    since a sales type or a sort key picks the body's branch."""
    properties = (tool.get("input_schema") or {}).get("properties") or {}
    base = {name: _schema_value(name, schema) for name, schema in properties.items()}
    found = [base]
    for name, schema in properties.items():
        for value in (schema.get("enum") or [])[1:]:
            found.append({**base, name: value})
    return found


def tool_key(name: str, data: dict, base: dict) -> str:
    """`tool:get_top_products?by=quantity` — the step a variant runs as."""
    changed = {k: v for k, v in data.items() if base.get(k) != v}
    if not changed:
        return f"tool:{name}"
    return f"tool:{name}?" + "&".join(f"{k}={v}" for k, v in sorted(changed.items()))


class FakeModel:
    """A model that asks for every tool the assistant offers, once each, and
    then answers — so `/api/chat` and `/api/chat/stream` each run every
    tool through the route's own path. No network: it is a model in name."""

    is_available = True

    def __init__(self, tools: Iterable[dict]) -> None:
        self.calls = [{"id": f"sweep-{i}", "name": tool["name"],
                       "input": tool_inputs(tool)[0]}
                      for i, tool in enumerate(tools)]
        # Per answer the model was asked for: the tools whose results it was
        # handed — which is how a route proves it ran every one of them.
        self.results_seen: List[List[str]] = []

    def _answering(self, messages: list) -> bool:
        last = messages[-1] if messages else {}
        content = last.get("content")
        if not isinstance(content, list):
            return False
        names = {call["id"]: call["name"] for call in self.calls}
        self.results_seen.append(sorted(
            names.get(block.get("tool_use_id"), "?") for block in content
            if isinstance(block, dict) and block.get("type") == "tool_result"))
        return True

    async def chat(self, messages, tools=None, max_tokens=1000, **_kwargs):
        usage = {"input_tokens": 1, "output_tokens": 1}
        if self._answering(messages):
            return {"content": "done", "tool_calls": [], "usage": usage}
        return {"content": "", "tool_calls": list(self.calls), "usage": usage}

    async def chat_stream(self, messages, tools=None, max_tokens=1000, **_kwargs):
        if self._answering(messages):
            yield {"type": "text", "text": "done"}
        else:
            for call in self.calls:
                yield {"type": "tool_use", "tool": dict(call)}
        yield {"type": "end", "usage": {"input_tokens": 1, "output_tokens": 1}}


def stream_events(text: str) -> List[str]:
    """The `event:` names of a Server-Sent Events body, in order."""
    return [line.split(":", 1)[1].strip() for line in text.splitlines()
            if line.startswith("event:")]
