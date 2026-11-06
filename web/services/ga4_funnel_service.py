"""The site's purchase funnel for /traffic, asked of GA4 (``core/ga4.py``).

Three windows are asked together: the period, the one of equal length just
before it, and the same dates a year earlier. Each window is two funnels, by
channel group (whose total row is the whole site) and by device, so a period
costs six requests run at once. Across periods at most ``GA4_CONCURRENCY``
are in flight, under the Data API's ten concurrent requests per property.

**A small in-process cache, and it exists for GA4's quota.** The standing rule
is no server-side cache without evidence (CLAUDE.md, "Caching"). GA4 meters
every request against a per-property hourly quota, and a day's numbers go on
settling for a day or two, so an hour of reuse costs nothing in truth and
bounds what a busy tab can spend. While GA4 cannot be asked, the last good
answer for the period comes back marked stale. Bounded by construction: at
most ``MAX_ENTRIES`` periods, the oldest dropped first.

**Only the days GA4 holds are asked.** A funnel exists from ``earliest_day``
to today, Kyiv time, so a window is cut to those days before it is asked, and
a window with none of them is zeros with no request. The counts are the same
either way. What it buys is that GA4 is never sent what it refuses. The
dashboard's date window admits a backwards range and days before 2015-08-14,
GA4's first day. Asked for either, GA4 answered 400 (measured 2026-11-04),
and the route answered "GA4 is unavailable".

**The traffic type is decided here, at read time,** from GA4's channel group,
so the split can change without anything to rewrite. Cold is advertising shown
to people who do not know the brand yet; warm is people who know it or look
for the product themselves; other is what GA4 could not place, links from
other sites and AI assistants.
"""
from __future__ import annotations

import asyncio
import calendar
import contextlib
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple
import time
from zoneinfo import ZoneInfo

import httpx

from core import ga4

#: The property counts days in Kyiv time, so its "today" is Kyiv's.
_KYIV_TZ = ZoneInfo("Europe/Kyiv")
TTL_S = 3600
MAX_ENTRIES = 32
#: GA4 requests in flight at once, across every period. The Data API admits
#: ten concurrent requests per standard property; one period's six still go
#: side by side under it.
GA4_CONCURRENCY = 6
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
# One lock per period, so two readers of one period ask GA4 once, and a fresh
# answer for one period never waits behind another period's fetch (L3-05: a
# cached hit waited 2.97 s behind an unrelated period's six requests, under
# one module-wide lock). A lock lives while somebody holds it or waits on it,
# and goes with its last user whatever the fetch did: pruned only after a
# fetch that succeeded, a period GA4 failed for left its lock behind for as
# long as GA4 kept failing.
_locks: "Dict[Tuple[date, date], asyncio.Lock]" = {}
_lock_users: "Dict[Tuple[date, date], int]" = {}
# The cap across periods: per-period locks let three uncached periods put
# eighteen requests in flight at once. Made on the running loop the first time
# it is asked for there, since an asyncio primitive belongs to one loop.
_slots: "Optional[Tuple[asyncio.AbstractEventLoop, asyncio.Semaphore]]" = None


def _reset_cache() -> None:
    """For tests."""
    global _slots
    _cache.clear()
    _locks.clear()
    _lock_users.clear()
    _slots = None


@contextlib.asynccontextmanager
async def _period_lock(key: Tuple[date, date]) -> AsyncIterator[None]:
    """Hold `key`'s lock. The count is taken before the wait and given back
    after the release, synchronously both times, so a lock is never dropped
    while a reader waits on it: a reader that came later would make a second
    one and fetch beside the first."""
    lock = _locks.get(key)
    if lock is None:
        lock = _locks[key] = asyncio.Lock()
    _lock_users[key] = _lock_users.get(key, 0) + 1
    try:
        async with lock:
            yield
    finally:
        left = _lock_users[key] - 1
        if left:
            _lock_users[key] = left
        else:
            del _lock_users[key]
            del _locks[key]


def _ga4_slots() -> asyncio.Semaphore:
    global _slots
    loop = asyncio.get_running_loop()
    if _slots is None or _slots[0] is not loop:
        _slots = (loop, asyncio.Semaphore(GA4_CONCURRENCY))
    return _slots[1]


def _fresh(key: Tuple[date, date], clock: Callable[[], float]) -> Optional[Dict[str, Any]]:
    """The cached answer for `key` while it is fresh, else None. Synchronous,
    so it needs no lock: nothing can interleave with it on the event loop."""
    cached = _cache.get(key)
    if cached is not None and clock() < cached["expires_at"]:
        _cache.move_to_end(key)
        return {**cached["payload"], "stale": False, "fetched_at": cached["fetched_at"]}
    return None


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
    """The first day GA4 still holds funnel data for: the same date 14 months back.

    Measured on 2026-11-04: one funnel per day came back from 2025-09-04 on, and
    empty for the 1st to the 3rd. The retention rolls by the day, not the month.
    """
    months = today.year * 12 + (today.month - 1) - RETENTION_MONTHS
    year, month = months // 12, months % 12 + 1
    return date(year, month, min(today.day, calendar.monthrange(year, month)[1]))


def asked_window(start: date, end: date, today: date) -> Optional[Tuple[date, date]]:
    """The days of ``start..end`` GA4 holds a funnel for, or None if there are none."""
    first, last = max(start, earliest_day(today)), min(end, today)
    return (first, last) if first <= last else None


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
    names = list(windows)
    plan: List[Tuple[str, date, date, str]] = []
    for w in names:
        asked = asked_window(*windows[w], today)
        if asked is not None:
            plan += [(w, asked[0], asked[1], breakdown) for breakdown in (CHANNEL, DEVICE)]
    answers: List[Dict[str, Dict[str, int]]] = []
    if plan:
        token = await ga4.access_token(client)
        slots = _ga4_slots()

        async def one(s: date, e: date, breakdown: str) -> Dict[str, Dict[str, int]]:
            async with slots:
                return await ga4.run_funnel(client, token, s, e, breakdown)

        answers = list(await asyncio.gather(*(
            one(s, e, breakdown) for _, s, e, breakdown in plan
        )))
    by_channel: Dict[str, Dict[str, Dict[str, int]]] = {w: {} for w in names}
    by_device: Dict[str, Dict[str, Dict[str, int]]] = {w: {} for w in names}
    for (w, _, _, breakdown), answer in zip(plan, answers):
        (by_channel if breakdown == CHANNEL else by_device)[w] = answer
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
    day = today or datetime.now(_KYIV_TZ).date()
    hit = _fresh(key, clock)
    if hit is not None:
        return hit
    async with _period_lock(key):
        # Asked again under the period's lock: a reader that waited here for
        # another's fetch of the same period takes its answer.
        hit = _fresh(key, clock)
        if hit is not None:
            return hit
        cached = _cache.get(key)
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
