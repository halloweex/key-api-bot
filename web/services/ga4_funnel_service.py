"""The site's purchase funnel for /traffic, asked of GA4 (``core/ga4.py``).

Three windows are asked together: the period, the one of equal length just
before it, and the same dates a year earlier. Each window is two funnels, by
channel group (whose total row is the whole site) and by device, so a period
costs six requests run at once.

**A small in-process cache, and it exists for GA4's quota.** The standing rule
is no server-side cache without evidence (CLAUDE.md, "Caching"). GA4 meters
every request against a per-property hourly quota, and a day's numbers go on
settling for a day or two, so an hour of reuse costs nothing in truth and
bounds what a busy tab can spend. While GA4 cannot be asked, the last good
answer for the period comes back marked stale. Bounded by construction: at
most ``MAX_ENTRIES`` periods, the oldest dropped first.

**The traffic type is decided here, at read time,** from GA4's channel group,
so the split can change without anything to rewrite. Cold is advertising shown
to people who do not know the brand yet; warm is people who know it or look
for the product themselves; other is what GA4 could not place, links from
other sites and AI assistants.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
import time

import httpx

from core import ga4

TTL_S = 3600
MAX_ENTRIES = 32
CHANNEL = "sessionDefaultChannelGroup"
DEVICE = "deviceCategory"
#: GA4 keeps event data this many months, and funnels are built from events.
RETENTION_MONTHS = 14

COLD = frozenset({
    "Paid Social", "Cross-network", "Paid Shopping", "Paid Other", "Paid Video", "Display",
})
WARM = frozenset({
    "Direct", "Email", "Organic Social", "Organic Search", "Paid Search",
    "Organic Shopping", "SMS", "Mobile Push Notifications",
})
TYPES = ("cold", "warm", "other")

# (start, end) → {"payload": dict, "fetched_at": iso str, "expires_at": monotonic}
_cache: "OrderedDict[Tuple[date, date], Dict[str, Any]]" = OrderedDict()
_lock = asyncio.Lock()


def _reset_cache() -> None:
    """For tests."""
    _cache.clear()


def channel_type(channel: str) -> str:
    if channel in COLD:
        return "cold"
    if channel in WARM:
        return "warm"
    return "other"


def previous_window(start: date, end: date) -> Tuple[date, date]:
    """The period of equal length ending the day before ``start``."""
    days = (end - start).days + 1
    prev_end = start - timedelta(days=1)
    return prev_end - timedelta(days=days - 1), prev_end


def year_ago(day: date) -> date:
    """The same date a year earlier; 29 February becomes the 28th."""
    try:
        return day.replace(year=day.year - 1)
    except ValueError:
        return day.replace(year=day.year - 1, day=28)


def earliest_day(today: date) -> date:
    """The first day GA4 still holds funnel data for, by its retention."""
    months = today.year * 12 + (today.month - 1) - RETENTION_MONTHS
    return date(months // 12, months % 12 + 1, 1)


def _empty() -> Dict[str, int]:
    return {c: 0 for c in ga4.COLUMNS}


def _add(into: Dict[str, int], steps: Dict[str, int]) -> None:
    for c in ga4.COLUMNS:
        into[c] += int(steps.get(c, 0))


def assemble(
    windows: Dict[str, Tuple[date, date]],
    by_channel: Dict[str, Dict[str, Dict[str, int]]],
    by_device: Dict[str, Dict[str, Dict[str, int]]],
    *,
    today: date,
) -> Dict[str, Any]:
    """The route's answer from parsed funnels, one entry per window name."""
    names = list(windows)
    total = {w: by_channel[w].get(ga4.TOTAL, _empty()) for w in names}

    channels: Dict[str, Dict[str, Dict[str, int]]] = {}
    for w in names:
        for key, steps in by_channel[w].items():
            if key != ga4.TOTAL:
                channels.setdefault(key, {n: _empty() for n in names})[w] = steps
    types: Dict[str, Dict[str, Dict[str, int]]] = {t: {n: _empty() for n in names} for t in TYPES}
    for key, per_window in channels.items():
        for w in names:
            _add(types[channel_type(key)][w], per_window[w])

    devices: Dict[str, Dict[str, Dict[str, int]]] = {}
    for w in names:
        for key, steps in by_device[w].items():
            if key != ga4.TOTAL:
                devices.setdefault(key, {n: _empty() for n in names})[w] = steps

    def rows(groups: Dict[str, Dict[str, Dict[str, int]]], **extra: Callable[[str], Any]) -> List[Dict[str, Any]]:
        out = [
            {"key": key, **{k: f(key) for k, f in extra.items()}, **per_window}
            for key, per_window in groups.items()
        ]
        return sorted(out, key=lambda r: (-r["current"]["entered"], r["key"]))

    return {
        "periods": {w: {"start": s.isoformat(), "end": e.isoformat()} for w, (s, e) in windows.items()},
        "earliest": earliest_day(today).isoformat(),
        "steps": list(ga4.COLUMNS),
        "total": total,
        "by_type": [{"key": t, **types[t]} for t in TYPES],
        "by_channel": rows(channels, type=channel_type),
        "by_device": rows(devices),
    }


async def _ask(start: date, end: date, client: httpx.AsyncClient, today: date) -> Dict[str, Any]:
    prev = previous_window(start, end)
    windows = {"current": (start, end), "previous": prev, "year_ago": (year_ago(start), year_ago(end))}
    token = await ga4.access_token(client)
    names = list(windows)
    asks = [
        ga4.run_funnel(client, token, s, e, breakdown)
        for w in names
        for (s, e) in [windows[w]]
        for breakdown in (CHANNEL, DEVICE)
    ]
    answers = await asyncio.gather(*asks)
    by_channel = {w: answers[2 * i] for i, w in enumerate(names)}
    by_device = {w: answers[2 * i + 1] for i, w in enumerate(names)}
    return assemble(windows, by_channel, by_device, today=today)


async def get_funnel(
    start: date,
    end: date,
    *,
    client: Optional[httpx.AsyncClient] = None,
    clock: Callable[[], float] = time.monotonic,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """The funnel for ``start..end`` with ``stale`` and ``fetched_at``.

    Fresh within ``TTL_S``; otherwise asked again. If asking fails and an
    earlier answer for the period exists, it comes back with ``stale: true``.
    Only with no earlier answer does it raise ``ga4.GA4Unavailable``.
    """
    key = (start, end)
    day = today or datetime.now(timezone.utc).date()
    async with _lock:
        cached = _cache.get(key)
        if cached is not None and clock() < cached["expires_at"]:
            _cache.move_to_end(key)
            return {**cached["payload"], "stale": False, "fetched_at": cached["fetched_at"]}
        try:
            if client is not None:
                payload = await _ask(start, end, client, day)
            else:
                async with httpx.AsyncClient() as own:
                    payload = await _ask(start, end, own, day)
        except ga4.GA4Unavailable:
            if cached is not None:
                return {**cached["payload"], "stale": True, "fetched_at": cached["fetched_at"]}
            raise
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _cache[key] = {"payload": payload, "fetched_at": fetched_at, "expires_at": clock() + TTL_S}
        _cache.move_to_end(key)
        while len(_cache) > MAX_ENTRIES:
            _cache.popitem(last=False)
        return {**payload, "stale": False, "fetched_at": fetched_at}
