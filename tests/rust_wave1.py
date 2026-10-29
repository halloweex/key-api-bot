"""What every wave-1 harness does alike: two services, one question.

Wave 1 ports the dashboard's read routes a tab at a time, and proves each by
contract. The requests the tab's page can send — `tests/frontend_contract.py`
reads them out of the TypeScript — go to `web.main.app` in this process and
to the Rust service over HTTP, against one Postgres, and must answer the same
bytes: the status, the headers the page reads, the body. Anything else is
hostile input and only its status is compared; where the statuses differ,
the difference must be in the tab's list (`Divergence`) with its reason, and
a listed difference that has gone away fails as well, so the list stays the
list.

Each tab's harness (`tests/integration/test_rust_wave1_<tab>.py`) brings its
contract, its seed and its list. This module brings the clock, the pair of
clients, the comparisons and the report.
"""
from __future__ import annotations

import json
import secrets
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from itertools import count
from typing import (Any, AsyncIterator, Dict, Iterator, List, Mapping, Optional,
                    Sequence, Tuple)
from unittest.mock import AsyncMock, patch

import pytest

# A Wednesday inside the seeded history (`tests/rust_pilot_fixture.py`):
# the matrix does not move with the day it runs on.
NOW = "2026-10-21T09:00:00+00:00"

# One second either side of the Kyiv midnights a period shortcut turns on.
KYIV_INSTANTS = (
    "2026-03-28T21:59:59+00:00", "2026-03-28T22:00:00+00:00",   # into the 23-hour day
    "2026-03-29T20:59:59+00:00", "2026-03-29T21:00:00+00:00",   # out of it
    "2026-10-24T20:59:59+00:00", "2026-10-24T21:00:00+00:00",   # into the 25-hour day
    "2026-10-25T21:59:59+00:00", "2026-10-25T22:00:00+00:00",   # out of it
    "2026-10-20T21:30:00+00:00",                                 # UTC's Tuesday, Kyiv's Wednesday
    "2026-10-18T20:59:59+00:00", "2026-10-18T21:00:00+00:00",   # Sunday into Monday
    "2026-10-31T21:59:59+00:00", "2026-10-31T22:00:00+00:00",   # a month turns
    "2026-12-31T21:59:59+00:00", "2026-12-31T22:00:00+00:00",   # a year turns
    "2024-02-29T10:00:00+00:00", "2026-03-31T10:00:00+00:00",   # leap day; the 31st
)


# What the date picker can give: two dates, the second never before the
# first (`DateRangePicker` swaps a backward pick), with no lower bound — the
# last two are the 400s a page can get (a range past 3 660 days, a date
# before 2000).
PICKER_RANGES = (
    ("2026-10-21", "2026-10-21"),        # today, picked
    ("2026-10-01", "2026-10-21"),        # the month so far
    ("2025-10-26", "2025-10-26"),        # the 25-hour day
    ("2026-03-29", "2026-03-29"),        # the 23-hour day
    ("2024-02-29", "2024-03-31"),        # a leap day to a month's end
    ("2025-12-25", "2026-01-07"),        # across a year's end
    ("2025-01-01", "2025-12-31"),        # a year
    ("2016-10-26", "2026-10-21"),        # the whole history, inside the cap
    ("2016-01-01", "2026-10-21"),        # 3 947 days
    ("1999-12-31", "2000-01-02"),        # before 2000
)


async def header_filters(python) -> Dict[str, list]:
    """What the header's dropdowns offer: `SourceFilter`'s constant, and the
    app's own answers for the other three — a root's children too, which
    `CategoryFilter` offers once a root is picked."""
    from tests import frontend_contract

    async def names(path: str) -> list:
        answer = await python.get(path)
        assert answer.status_code == 200, (path, answer.status_code, answer.text[:200])
        return answer.json()

    roots = await names("/api/categories")
    children = await names(f"/api/categories/{roots[0]['id']}/children") if roots else []
    found = {
        "source_id": list(frontend_contract.sources()),
        "category_id": [c["id"] for c in roots + children],
        "brand": [b["name"] for b in await names("/api/brands")],
        "promocode": [p["name"] for p in await names("/api/promocodes")],
    }
    # Two values at least per filter: a seed that offers one shrinks the
    # contract without a word.
    assert all(len(values) >= 2 for values in found.values()), found
    return found


class Clock(datetime):
    """`datetime` with `now()` pinned: what `core.filters` calls for a
    period shortcut. The pilot is told the same instant by `X-Pilot-Now`."""

    pinned: Optional[datetime] = None

    @classmethod
    def now(cls, tz=None):
        if cls.pinned is None:
            return datetime.now(tz)
        return cls.pinned.astimezone(tz) if tz is not None else cls.pinned


# Python answers in this process, beside the test, on what a shared runner
# has to spare, and `RequestTimeoutMiddleware` turns a slow answer into a
# 504. Its limit says how long Python may take, not what it answers, so a
# comparison lifts it, and a request that took longer than the limit is put
# in its phase's report instead of failing as a 504 against the pilot's 200.
# On 2026-10-29 the products summary over the picker's longest range passed
# 30 s on a CI runner.
PYTHON_LIMIT_LIFTED = 900.0
OVER_LIMIT: List[Dict[str, Any]] = []


def _python_limit(path: str) -> float:
    """The limit `RequestTimeoutMiddleware` gives `path`."""
    from web import middleware

    return (middleware.SLOW_ENDPOINT_TIMEOUT if path in middleware.SLOW_ENDPOINTS
            else middleware.DEFAULT_REQUEST_TIMEOUT)


@dataclass
class Pair:
    """The two services, signed in alike."""

    python: Any
    rust: Any
    pool: Any
    # The pilot's limiter outlives a test, and a rerun within the minute must
    # not reuse a key: a key per request, per run.
    run: str = field(default_factory=lambda: secrets.token_hex(3))
    _clients: Iterator[int] = field(default_factory=lambda: count(1))

    async def ask(self, path: str, query: str = "", *, now: Optional[str] = NOW,
                  key: Optional[str] = None):
        """Both answers to one GET, each service told the same `now` (None:
        their own clocks). A client key of its own unless `key` names one,
        so the route's limit is not what a comparison measures — nor
        Python's time limit, lifted for the request (`PYTHON_LIMIT_LIFTED`)."""
        Clock.pinned = datetime.fromisoformat(now) if now else None
        client = {"X-Real-IP": key or f"wave1-{self.run}-{next(self._clients)}"}
        url = f"{path}?{query}" if query else path
        started = time.monotonic()
        with patch("web.middleware.DEFAULT_REQUEST_TIMEOUT", PYTHON_LIMIT_LIFTED), \
                patch("web.middleware.SLOW_ENDPOINT_TIMEOUT", PYTHON_LIMIT_LIFTED):
            py = await self.python.get(url, headers=client)
        took = time.monotonic() - started
        if took > _python_limit(path):
            OVER_LIMIT.append({"request": url, "seconds": round(took, 1),
                               "limit": _python_limit(path)})
        rs = await self.rust.get(url, headers={**client, **({"X-Pilot-Now": now} if now else {})})
        return py, rs


@asynccontextmanager
async def services(dsn: str, pilot: str, env: Mapping[str, str]) -> AsyncIterator[Pair]:
    """`web.main.app` in this process and the pilot at `pilot`, both as a
    hardcoded admin — `_resolve_session` answers from the signature alone,
    so the session reads no user list, and `internal` is allowed — with the
    route limits on and Postgres the engine: `env` sets the tab's read
    switches, and the caller's `duckdb_off` refuses the file."""
    import asyncpg
    import httpx
    from core import read_fallback
    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.auth import SESSION_COOKIE, session_serializer

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("KS_PG_DSN", dsn)
        for name, value in env.items():
            mp.setenv(name, value)
        mp.setattr(limiter, "enabled", True)
        limiter.reset()
        mp.setattr("core.filters.datetime", Clock)
        read_fallback.reset_counts()
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        cookie = session_serializer.dumps({"user_id": min(ADMIN_USER_IDS), "role": "admin"})
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        try:
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
                async with httpx.AsyncClient(transport=transport, base_url="http://python",
                                             cookies={SESSION_COOKIE: cookie}) as python, \
                        httpx.AsyncClient(base_url=pilot, timeout=60,
                                          cookies={SESSION_COOKIE: cookie}) as rust:
                    yield Pair(python, rust, pool)
        finally:
            Clock.pinned = None
            await pool.close()


def difference(py, rs, headers: Sequence[str] = ("content-type",)) -> Optional[str]:
    """Where two answers part — the status, a header the page reads, the
    body's bytes — or None. The contract's comparison."""
    if py.status_code != rs.status_code:
        return f"status {py.status_code} against {rs.status_code}: {rs.text[:160]!r}"
    for name in headers:
        if py.headers.get(name) != rs.headers.get(name):
            return f"{name} {py.headers.get(name)!r} against {rs.headers.get(name)!r}"
    a, b = py.content, rs.content
    if a == b:
        return None
    i = next((n for n, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
    return f"bodies part at byte {i}: {a[max(0, i - 60):i + 60]!r} against {b[max(0, i - 60):i + 60]!r}"


@dataclass(frozen=True)
class Divergence:
    """A class of hostile input the two services answer differently, and
    accepted: Python's status, Rust's, and why Rust is not made to agree."""

    python: int
    rust: int
    why: str


def judge_hostile(answers: Sequence[Tuple[str, str, int, int]],
                  listed: Mapping[str, Divergence]) -> Tuple[List[str], List[str]]:
    """`answers` are (class, request, Python's status, Rust's). Returns the
    differences no entry lists, and the entries that no longer hold: a class
    whose requests all agree now, or that is never asked."""
    unlisted, differed = [], set()
    for kind, request, py, rs in answers:
        if py == rs:
            continue
        entry = listed.get(kind)
        if entry and (entry.python, entry.rust) == (py, rs):
            differed.add(kind)
        else:
            unlisted.append(f"{kind} ({request}): Python {py}, Rust {rs}"
                            + (f", listed as {entry.python}/{entry.rust}" if entry else ""))
    stale = sorted(set(listed) - differed)
    return unlisted, stale


class Report:
    """Counts per phase, written as JSON where the workflow asks for it."""

    def __init__(self) -> None:
        self.data: Dict[str, Any] = {"phases": {}}

    def phase(self, name: str, rows: Sequence[Tuple[str, int, Optional[str]]]) -> List[str]:
        """`rows` are (request, Python's status, the difference or None).
        Records the phase, with the requests asked since the phase before
        that took Python past its own limit, and returns its differences."""
        differences = [f"{request}: {found}" for request, _, found in rows if found]
        self.data["phases"][name] = {
            "requests": len(rows),
            "identical": len(rows) - len(differences),
            "statuses": {str(k): v for k, v in sorted(Counter(s for _, s, _ in rows).items())},
            "first_differences": differences[:20],
        }
        if OVER_LIMIT:
            self.data["phases"][name]["python_over_limit"] = OVER_LIMIT[:]
            OVER_LIMIT.clear()
        return differences

    def write(self, path: Optional[str]) -> None:
        if path:
            with open(path, "w", encoding="utf-8") as out:
                json.dump(self.data, out, indent=2, ensure_ascii=False, default=str)
