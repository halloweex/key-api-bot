"""Telegram-bot activity, asked of the bot itself.

The customer bot — ks-tg-bot, a separate project on this host — keeps its users
and its event log in its own SQLite. Nothing here reads that file. The bot
computes the readout and serves it on ``GET /internal/activity`` over the
docker network both projects already share (``key-api-bot_default``, the one
nginx reaches the bot's ``/rivo/`` webhook through). What crosses is counts
only: no phone, no name, no chat id. Who counts as active, which day a moment
falls on (Kyiv) and why the admins are left out are the bot's decisions, made
once in its ``core/usecases/analytics.py::activity_report`` and printed by its
own ``/stats`` from the same function.

**A small in-process cache, and it exists for the bot, not for latency.** The
standing rule here is no server-side cache without evidence (CLAUDE.md,
"Caching"). This one is not about speed: it keeps the last good answer per
window so the page still shows numbers — marked stale — while the bot restarts
on a deploy, and it bounds how often the dashboard can make the bot scan its
log. Bounded by construction: one entry per window in ``WINDOWS``.

Unset ``KS_TG_BOT_TOKEN`` is a supported state: the route answers 503 "not
configured" and nothing is asked of the network.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import httpx

#: The windows the page offers. A closed set rather than a range, because each
#: is a cache entry and the cache declares its bound here.
WINDOWS = (30, 90, 180)
DEFAULT_WINDOW = 90

#: What the bot's readout calls version 1. A different number means a field
#: changed meaning, and the page would draw it wrongly rather than fail.
CONTRACT_VERSION = 1

#: Matches the frontend's refetch cadence for this page.
TTL_S = 120
TIMEOUT_S = 3.0
PATH = "/internal/activity"


class BotUnavailable(Exception):
    """The bot could not be asked, and there is no earlier answer to show."""


def _url() -> str:
    return os.getenv("KS_TG_BOT_URL", "http://ks-tg-bot:8081").rstrip("/")


def _token() -> str:
    return os.getenv("KS_TG_BOT_TOKEN", "").strip()


# days → {"payload": dict, "fetched_at": iso str, "expires_at": monotonic}
_cache: Dict[int, Dict[str, Any]] = {}
_lock = asyncio.Lock()


def _reset_cache() -> None:
    """For tests."""
    _cache.clear()


async def _ask(days: int, client: Optional[httpx.AsyncClient]) -> dict:
    token = _token()
    if not token:
        raise BotUnavailable("not configured: KS_TG_BOT_TOKEN is not set")
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{_url()}{PATH}"
    try:
        if client is not None:
            response = await client.get(url, params={"days": days},
                                        headers=headers, timeout=TIMEOUT_S)
        else:
            async with httpx.AsyncClient(timeout=TIMEOUT_S) as own:
                response = await own.get(url, params={"days": days},
                                         headers=headers)
    except httpx.HTTPError as exc:
        raise BotUnavailable(f"bot unreachable: {type(exc).__name__}") from exc
    if response.status_code != 200:
        # The body is not echoed: the bot keeps its reasons in its own log.
        raise BotUnavailable(f"bot answered {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise BotUnavailable("bot answered something that is not JSON") from exc
    if not isinstance(payload, dict) or payload.get("version") != CONTRACT_VERSION:
        raise BotUnavailable(
            f"bot answered contract version {payload.get('version') if isinstance(payload, dict) else '?'}"
            f", this page reads {CONTRACT_VERSION}")
    return payload


async def get_activity(
    days: int = DEFAULT_WINDOW,
    *,
    client: Optional[httpx.AsyncClient] = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict:
    """The bot's activity readout for `days`, with `stale` and `fetched_at`.

    Fresh within TTL_S; otherwise asked again. If asking fails and an earlier
    answer exists, that answer comes back with ``stale: true``. Only with no
    earlier answer at all does it raise ``BotUnavailable``.
    """
    if days not in WINDOWS:
        raise ValueError(f"days must be one of {WINDOWS}")
    async with _lock:
        cached = _cache.get(days)
        if cached is not None and clock() < cached["expires_at"]:
            return {**cached["payload"], "stale": False,
                    "fetched_at": cached["fetched_at"]}
        try:
            payload = await _ask(days, client)
        except BotUnavailable:
            if cached is not None:
                return {**cached["payload"], "stale": True,
                        "fetched_at": cached["fetched_at"]}
            raise
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _cache[days] = {"payload": payload, "fetched_at": fetched_at,
                        "expires_at": clock() + TTL_S}
        return {**payload, "stale": False, "fetched_at": fetched_at}
