"""The Rust pilot answers `GET /api/revenue/trend` the way the Python app does.

One Postgres, two services. Python is the real application — `web.main.app`
with its middleware, its exception handlers and a signed admin session,
called in-process — and Rust is `rust/crates/ks-api` at `KS_RUST_PILOT_URL`.
Every request below goes to both. The answers must have the same status, the
same content type and the same JSON once keys are sorted: `json.loads` then
`json.dumps`, so `0` and `0.0` stay different numbers and a float must agree
to the last bit. That compares what a client parses, not what it means.

WHAT IS ASKED

Every `period` with every spelling of every `sales_type`, every source Gold
has a channel for and every `compare_type`; explicit ranges across both
clock changes, a leap day, month ends, a year end, before and after the
history, reversed; dates `strptime` forgives and dates it refuses; the
validators' 400s and FastAPI's 422s; and the period shortcuts at instants one
second either side of the Kyiv midnights that matter — `X-Pilot-Now` on the
Rust side, `core.filters.datetime` pinned on the Python side.

WHAT IS NOT

The branches the pilot answers 501: a line-level filter, a promocode, source
3 and the forecast overlay. They are asked too, to prove the pilot refuses
them rather than answering them wrongly; Python's answer is recorded, not
compared.

Python cannot have answered from DuckDB: `duckdb_off` refuses the file, and
the test asks the switch and the fallback counters afterwards.

Skipped without the pilot or without a Postgres; the pilot's own workflow
(`.github/workflows/rust-pilot.yml`) runs it with both. With
`KS_RUST_PILOT_REPORT` set, the counts are written there.
"""
from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT = os.getenv("KS_RUST_PILOT_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
ROUTE = "/api/revenue/trend"
# A Wednesday inside the seeded history: the matrix does not move with the
# day it runs on.
NOW = "2026-10-21T09:00:00+00:00"

PERIODS = (None, "today", "yesterday", "week", "last_week", "month", "last_month",
           "last_7_days", "last_28_days")
SALES_TYPES = (None, "retail", "b2b", "internal", "exhibition", "all", " B2B ", "", "ALL")
SOURCES = (None, "1", "2", "4")
COMPARES = (None, "previous_period", "year_ago", "month_ago", "bogus", "YEAR_AGO")
VALID_COMPARES = ("previous_period", "year_ago", "month_ago")

RANGES = (
    ("2026-10-21", "2026-10-21"),            # one day
    ("2026-03-01", "2026-03-31"),            # the spring clock change inside
    ("2025-10-01", "2025-10-31"),            # the autumn one
    ("2026-03-29", "2026-03-29"),            # the 23-hour day
    ("2025-10-26", "2025-10-26"),            # the 25-hour day
    ("2026-10-24", "2026-10-26"),            # the boundary orders around it
    ("2024-02-29", "2024-02-29"),            # leap day: a year ago is 28 February
    ("2024-02-28", "2024-03-01"),
    ("2026-03-31", "2026-03-31"),            # a month ago clamps to 28 February
    ("2025-12-25", "2026-01-07"),            # across a year end
    ("2026-12-31", "2027-01-01"),            # the last midnight's orders
    ("2023-11-01", "2023-11-30"),            # before the history: every day int 0
    ("2030-01-01", "2030-01-31"),            # after it
    ("2026-01-10", "2026-01-05"),            # reversed: no days, a forward window
    ("2025-01-01", "2025-12-31"),            # a year
    ("2023-12-01", "2027-01-31"),            # everything
    ("2016-01-01", "2026-01-07"),            # 3 660 days, the longest range allowed
    ("2000-01-01", "2000-01-31"), ("2099-12-01", "2099-12-31"),   # the window's ends
    ("2028-02-29", "2028-02-29"),            # a leap day ahead answers (#314)
    ("2026-1-5", "2026-01- 9"),              # padding strptime forgives
    ("２０２６-01-01", "2026-01-1٥"),           # Unicode digits where `\\d` reads them
)
# 400s since #308 and #314 — a date that does not parse, one outside
# 2000-01-01..2099-12-31, a range over 3 660 days; the first two were 500s
# before — and the fallbacks to "today" for a missing date.
UNPARSEABLE = (
    ("2026-02-30", "2026-03-01"), ("2026-13-01", "2026-12-01"), ("26-01-01", "2026-01-02"),
    ("2026-01-01T00:00", "2026-01-02"), ("٢٠٢٦-٠١-٠٥", "2026-01-06"),
    ("0001-01-01", "0001-01-05"),            # year 1: outside the window
    ("9999-12-30", "9999-12-31"),            # year 9999: outside it too
    ("9999-12-31", "bogus"),                 # the start is named first
    ("1999-12-31", "2000-01-01"), ("2099-12-31", "2100-01-01"),   # one day out
    ("1999-1-5", "2000-01-01"),              # a forgiven form, named as written
    ("2016-01-01", "2026-01-08"),            # 3 661 days, one over the cap
    ("2026-01-01", ""), ("", "2026-01-01"),
)
RAW = (
    "period=TODAY", "period=Week&start_date=2026-01-01&end_date=2026-01-03", "period=%20month%20",
    "period=thisweek", "period=xyz", "period=it's", "period=a%22b%27c",
    "period=%D0%94%D0%95%D0%9D%D0%AC", "period=x%09y%5Cz", "period=%C2%A0bad",
    "period=%1Ftoday%1C", "period=today&period=nope", "period=nope&period=today",
    "sales_type=wholesale", "sales_type=%D1%80%D0%BE%D0%B7%D0%BD%D0%B8%D1%86%D0%B0",
    "sales_type=%E2%80%A8", "source_id=5", "source_id=0", "source_id=-1", "source_id=abc",
    "source_id=", "source_id=%201", "source_id=1_0", "source_id=4.000", "source_id=1.",
    "source_id=99999999999999999999", "source_id=%2B2", "source_id=02",
    "source_id=" + "9" * 4301,
    "category_id=0", "category_id=-5", "category_id=x", "brand=%20%20", "brand=" + "x" * 300,
    # Past `bronze.categories.id`'s int32, and past 64 bits: the validator's
    # 400 (#335); a NUL anywhere in the query: the middleware's 400.
    "category_id=2147483648", "category_id=99999999999999999999",
    "category_id=-99999999999999999999", "period=%00", "brand=a%00b", "x=%00&period=week",
    "brand=bad%3Cchar", "promocode=" + "y" * 101, "promocode=%20%20&period=week",
    "include_forecast=maybe", "include_forecast=%20true", "include_forecast=false&period=month",
    "include_forecast=0&period=week", "include_forecast=true&period=last_month",
    "include_forecast=1&source_id=1&period=month",
    "include_forecast=yes&start_date=2025-01-01&end_date=2025-01-31",
    "source_id=x&category_id=y&include_forecast=maybe", "unknown=1&period=week",
    "start_date=2026-10-01&end_date=2026-10-31&compare_type=",
)
# What the pilot leaves to Python, and the branch it names.
OUT_OF_PILOT = (
    ("source_id=3&period=month", "source_without_gold_channel"),
    ("brand=BrandA&period=month", "line_level_filter"),
    ("category_id=1&period=month", "line_level_filter"),
    ("promocode=SALE&period=month", "promocode_filter"),
    ("period=month&include_forecast=true", "forecast_overlay"),
    ("period=week&include_forecast=true&sales_type=b2b", "forecast_overlay"),
    ("start_date=2026-10-01&end_date=2026-11-30&include_forecast=true", "forecast_overlay"),
)
# One second either side of the midnights a shortcut's answer turns on.
INSTANTS = (
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


def _q(**params: Optional[str]) -> str:
    return urlencode({k: v for k, v in params.items() if v is not None})


def _cases() -> List[Tuple[str, str]]:
    """(query string, the instant both services take for now)."""
    cases = []
    for period in PERIODS:
        for sales_type in SALES_TYPES:
            for source in SOURCES:
                for compare in COMPARES:
                    cases.append((_q(period=period, sales_type=sales_type, source_id=source,
                                     compare_type=compare), NOW))
    for start, end in RANGES:
        for sales_type in (None, "all", "b2b"):
            for source in (None, "4"):
                for compare in VALID_COMPARES:
                    cases.append((_q(start_date=start, end_date=end, sales_type=sales_type,
                                     source_id=source, compare_type=compare), NOW))
    for start, end in UNPARSEABLE:
        for compare in VALID_COMPARES:
            cases.append((_q(start_date=start, end_date=end, compare_type=compare), NOW))
    cases += [(raw, NOW) for raw in RAW]
    for instant in INSTANTS:
        for period in PERIODS:
            for compare in VALID_COMPARES:
                for sales_type in (None, "all"):
                    cases.append((_q(period=period, compare_type=compare,
                                     sales_type=sales_type), instant))
    return cases


class _Clock(datetime):
    """`datetime` with `now()` pinned: what `core.filters` and the route call."""

    pinned: Optional[datetime] = None

    @classmethod
    def now(cls, tz=None):
        if cls.pinned is None:
            return datetime.now(tz)
        return cls.pinned.astimezone(tz) if tz is not None else cls.pinned


_REPORT: Dict[str, object] = {}


@pytest.fixture(scope="module")
def seeded():
    """Forty thousand orders through production's derivations, once."""
    from tests.rust_pilot_fixture import boundary_days, clear, seed

    async def _seed():
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            return await seed(pool), await boundary_days(pool)
        finally:
            await pool.close()

    async def _clear():
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
        try:
            await clear(pool)
        finally:
            await pool.close()

    summary, days = asyncio.run(_seed())
    _REPORT["seed"] = {"orders": summary["orders"], "gold_rows": summary["gold"]["rows"]}
    yield days
    asyncio.run(_clear())
    if REPORT:
        with open(REPORT, "w", encoding="utf-8") as out:
            json.dump(_REPORT, out, indent=2, ensure_ascii=False, default=str)


@pytest_asyncio.fixture
async def services(seeded, duckdb_off, monkeypatch):
    from core import read_fallback
    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.auth import SESSION_COOKIE, session_serializer

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_GOLD", "postgres")
    monkeypatch.setattr(limiter, "enabled", False)
    monkeypatch.setattr("core.filters.datetime", _Clock)
    monkeypatch.setattr("web.routes.api.analytics._datetime", _Clock)
    read_fallback.reset_counts()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=5)
    # A hardcoded admin: `_resolve_session` answers from the signature alone,
    # so the session reads no user list — and `internal` is allowed.
    cookie = session_serializer.dumps({"user_id": min(ADMIN_USER_IDS), "role": "admin"})
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            async with httpx.AsyncClient(transport=transport, base_url="http://python",
                                         cookies={SESSION_COOKIE: cookie}) as python, \
                    httpx.AsyncClient(base_url=PILOT, timeout=60) as rust:
                yield python, rust
    finally:
        _Clock.pinned = None
        await pool.close()


async def _ask(python, rust, query: str, now: Optional[str]):
    _Clock.pinned = datetime.fromisoformat(now) if now else None
    headers = {"X-Pilot-Now": now} if now else {}
    py = await python.get(f"{ROUTE}?{query}")
    rs = await rust.get(f"{ROUTE}?{query}", headers=headers)
    return py, rs


def _first_difference(a, b, path="$") -> Optional[str]:
    """Where two parsed bodies part, typed: `0` is not `0.0`, nor `-0.0`."""
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


def _differs(py, rs) -> Optional[str]:
    if py.status_code != rs.status_code:
        return f"status {py.status_code} against {rs.status_code}: {rs.text[:200]}"
    if py.headers.get("content-type") != rs.headers.get("content-type"):
        return f"content-type {py.headers.get('content-type')} against {rs.headers.get('content-type')}"
    try:
        a, b = json.loads(py.content), json.loads(rs.content)
    except ValueError:
        return "a body that is not JSON"
    canonical = [json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                 for x in (a, b)]
    if canonical[0] == canonical[1]:
        return None
    return _first_difference(a, b) or "the same values, differently serialised"


def test_the_seed_puts_each_midnight_order_on_its_kyiv_day(seeded):
    """The harness exercises the boundaries only if production's Silver put
    the orders where they belong: otherwise both services would agree about
    a day that holds the wrong orders."""
    from tests.rust_pilot_fixture import BOUNDARY_ID_BASE, BOUNDARY_ORDERS

    expected = {BOUNDARY_ID_BASE + i: day for i, (_, day) in enumerate(BOUNDARY_ORDERS)}
    assert seeded == expected


@pytest.mark.asyncio
async def test_a_nul_is_refused_before_routing_byte_for_byte(services):
    """`RejectNulMiddleware`: any `/api/` path, served or not, with a NUL in
    the path or the query string, is the same 400 before any route runs."""
    python, rust = services
    for url in ("/api/revenue%00/trend", "/api/nowhere%00", "/api/revenue/trend?period=%00",
                "/api/nowhere?x=1&y=%00", "/%61pi/revenue/trend%00"):
        py, rs = await python.get(url), await rust.get(url)
        assert py.status_code == 400, (url, py.status_code)
        assert _differs(py, rs) is None, (url, _differs(py, rs))
        assert py.content == rs.content, (url, py.content, rs.content)


@pytest.mark.asyncio
async def test_every_answer_is_the_same(services):
    from core import duckdb_switch, read_fallback

    python, rust = services
    cases = _cases()
    statuses: Counter = Counter()
    mismatches, byte_identical, nonzero = [], 0, 0
    for query, now in cases:
        py, rs = await _ask(python, rust, query, now)
        statuses[py.status_code] += 1
        found = _differs(py, rs)
        if found:
            mismatches.append({"query": query, "now": now, "difference": found})
            continue
        byte_identical += py.content == rs.content
        if py.status_code == 200 and json.loads(py.content)["comparison"]["totals"]["current"]:
            nonzero += 1

    _REPORT["matrix"] = {
        "requests": len(cases), "identical": len(cases) - len(mismatches),
        "byte_identical": byte_identical, "statuses": dict(sorted(statuses.items())),
        "with_revenue": nonzero, "mismatches": mismatches[:50],
    }
    assert not mismatches, (
        f"{len(mismatches)} of {len(cases)} requests answered differently; first: "
        + json.dumps(mismatches[:5], ensure_ascii=False, indent=1))
    # The matrix reached every kind of answer it claims to, and real numbers.
    # No 500 is required any more: since #308 and #314 every date the matrix
    # once broke the route with is a 400.
    assert {200, 400, 422} <= set(statuses), statuses
    assert nonzero > 1000, nonzero
    # And Python answered from Postgres: no DuckDB open, nothing fell back.
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


@pytest.mark.asyncio
async def test_what_the_pilot_leaves_to_python_it_refuses(services):
    python, rust = services
    recorded = {}
    for query, branch in OUT_OF_PILOT:
        py, rs = await _ask(python, rust, query, NOW)
        assert rs.status_code == 501, (query, rs.status_code, rs.text[:200])
        assert rs.json()["branch"] == branch, query
        recorded[query] = {"python": py.status_code, "rust": rs.status_code, "branch": branch}
    _REPORT["out_of_pilot"] = recorded


@pytest.mark.asyncio
async def test_today_on_the_real_clock(services):
    """Nothing pinned: each service reads its own clock. Asked again if a
    Kyiv midnight fell between the two requests."""
    python, rust = services
    for _ in range(3):
        before = datetime.now(KYIV).date()
        py, rs = await _ask(python, rust, "period=today&sales_type=all", None)
        if datetime.now(KYIV).date() == before:
            break
    assert _differs(py, rs) is None, _differs(py, rs)
    assert py.status_code == 200
