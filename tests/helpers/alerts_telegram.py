"""Telegram at the transport, for the alert tests.

The real senders run — `bot.main.send_admin_message` and the HTTP transport
in `core.telegram_alerts` — and only the network under them is replaced:
every httpx request a test makes is answered here and recorded, so what
reached which admin is read off the requests, as Telegram would see them.

`tests/conftest.py` stubs `send_admin_message_http` for every test (it sends
nothing and answers 0). `telegram_at_the_transport` puts the real one back,
bound below when this module is imported — at collection, before any test
has run — so import it at the top of a test module.
"""
from __future__ import annotations

import importlib
import json
from typing import Awaitable, Callable, List, Optional, Tuple

import httpx

from core import telegram_alerts

_REAL_SEND = telegram_alerts.send_admin_message_http
if _REAL_SEND.__module__ != "core.telegram_alerts":  # pragma: no cover
    raise RuntimeError(
        "tests.helpers.alerts_telegram was first imported inside a test, after "
        "the transport was stubbed; import it at the top of the test module")

# Two admins, with ids no real account has.
ADMINS: Tuple[int, ...] = (900_000_001, 900_000_002)


class Telegram:
    """The Bot API as the tests see it. `status` is what every sendMessage is
    answered with (403: the admin blocked the bot, so nobody is reached);
    `on_send`, when set, is awaited with each sendMessage payload before it is
    answered — the moment a message leaves."""

    def __init__(self) -> None:
        self.status = 200
        self.on_send: Optional[Callable[[dict], Awaitable[None]]] = None
        # (Bot API method, payload, status answered), in order.
        self.calls: List[Tuple[str, dict, int]] = []

    @property
    def attempts(self) -> List[dict]:
        """Every sendMessage, answered or refused."""
        return [p for method, p, _ in self.calls if method == "sendMessage"]

    @property
    def messages(self) -> List[dict]:
        """The sendMessage payloads Telegram accepted."""
        return [p for method, p, status in self.calls
                if method == "sendMessage" and status < 400]

    def chats(self) -> List[int]:
        """Who was reached, once per message, sorted."""
        return sorted(int(p["chat_id"]) for p in self.messages)

    def _record(self, request: httpx.Request) -> Tuple[str, dict]:
        method = request.url.path.rsplit("/", 1)[-1]
        try:
            payload = json.loads(request.content or b"{}")
        except ValueError:
            payload = {}
        return method, payload

    def _answer(self, method: str, payload: dict) -> httpx.Response:
        status = self.status if method == "sendMessage" else 404
        self.calls.append((method, payload, status))
        if status >= 400:
            return httpx.Response(status, json={
                "ok": False, "error_code": status,
                "description": "Forbidden: bot was blocked by the user"})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(self.calls)}})

    async def handle(self, request: httpx.Request) -> httpx.Response:
        method, payload = self._record(request)
        if method == "sendMessage" and self.on_send is not None:
            await self.on_send(payload)
        return self._answer(method, payload)

    def handle_sync(self, request: httpx.Request) -> httpx.Response:
        method, payload = self._record(request)
        return self._answer(method, payload)


def telegram_at_the_transport(monkeypatch, *, admins=ADMINS) -> Telegram:
    """Answer every httpx request of the test from a `Telegram`, with the
    real HTTP sender restored and `admins` as the admin list."""
    fake = Telegram()
    mock = httpx.MockTransport(fake.handle)

    async def handle_async_request(_transport, request):
        return await mock.handle_async_request(request)

    def handle_request(_transport, request):
        request.read()
        return fake.handle_sync(request)

    # Under the real clients: whichever way a sender builds its client, its
    # requests end in one of these two.
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request",
                        handle_async_request)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", handle_request)
    monkeypatch.setattr(telegram_alerts, "send_admin_message_http", _REAL_SEND)
    monkeypatch.setattr(importlib.import_module("core.config"), "ADMIN_USER_IDS",
                        set(admins))
    # The web container's case: no bot Application, so the HTTP fallback sends.
    monkeypatch.setattr(importlib.import_module("bot.main"), "_application", None)
    return fake
