"""Python against Rust on `/api/margin/*`, by contract: wave 1's first tab.

`web.main.app` in this process under `KS_READ_MARGIN=postgres`, as
production runs it, and the pilot over HTTP, on one Postgres seeded through
production's derivations (`tests/rust_wave1_fixture.py`). The machinery —
the clock, the pair, the comparisons — is `tests/rust_wave1.py`'s.

THE CONTRACT: status, Content-Type and body, byte for byte

What the /margin page can send, read out of its TypeScript
(`tests/frontend_contract.margin()`): one of the eight periods, or the two
dates the picker gives in order; the five sales types; and the header's four
filters, which these routes ignore — so they are sent as well, with values
out of the app's own lookups, and must be ignored alike. Then the shortcuts
one second either side of the Kyiv midnights, the 400 a range past 3 660
days gets, the 503 naming `margin` when Postgres cannot answer, and the
route's limit of thirty a minute.

THE REST: the status alone

`limit`, `min_revenue` and `margin_floor` the page never sends, and other
spellings of what it does send. Where the two statuses differ, the class is
in DIVERGENCES with its reason, and an entry that no longer holds fails.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_MARGIN_REPORT` set, the counts go there.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests import frontend_contract, rust_wave1
from tests.rust_wave1 import NOW, Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_MARGIN_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
CONTRACT = frontend_contract.margin()
ENV = {"KS_READ_MARGIN": "postgres", "KS_READ_LOOKUPS": "postgres"}
OVERVIEW, BY_BRAND, BY_CATEGORY, TREND, BRAND_CATEGORY, ALERTS = CONTRACT.endpoints

# What the picker can give: two dates, the second never before the first
# (`DateRangePicker` swaps a backward pick), with no lower bound.
RANGES = (
    ("2026-10-21", "2026-10-21"),        # today, picked
    ("2026-10-01", "2026-10-21"),        # the month so far
    ("2025-10-26", "2025-10-26"),        # the 25-hour day
    ("2026-03-29", "2026-03-29"),        # the 23-hour day
    ("2024-02-29", "2024-03-31"),        # a leap day to a month's end
    ("2025-12-25", "2026-01-07"),        # across a year's end
    ("2025-01-01", "2025-12-31"),        # a year: twelve months on the trend
    ("2016-10-26", "2026-10-21"),        # the whole history, inside the 3 660-day cap
    ("2016-01-01", "2026-10-21"),        # 3 947 days: the 400 the page can get
    ("1999-12-31", "2000-01-02"),        # before 2000: a 400 as well
)

# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "limit out of bounds": [(BY_BRAND, "limit=0"), (BY_BRAND, "limit=51"),
                            (BY_BRAND, "limit=-1"), (BY_BRAND, "limit=99999999999999999999")],
    "limit not an integer": [(BY_BRAND, "limit=abc"), (BY_BRAND, "limit="),
                             (BY_BRAND, "limit=5.5")],
    "limit as an integral float": [(BY_BRAND, "limit=5.0")],
    "limit padded": [(BY_BRAND, "limit=%205"), (BY_BRAND, "limit=5%20")],
    "limit with an underscore": [(BY_BRAND, "limit=1_0")],
    "limit signed or zero-led": [(BY_BRAND, "limit=%2B5"), (BY_BRAND, "limit=05")],
    "limit repeated": [(BY_BRAND, "limit=0&limit=5"), (BY_BRAND, "limit=5&limit=0")],
    "min_revenue out of bounds": [(BRAND_CATEGORY, "min_revenue=-1"),
                                  (BRAND_CATEGORY, "min_revenue=-0.01")],
    "min_revenue not a number": [(BRAND_CATEGORY, "min_revenue=abc"),
                                 (BRAND_CATEGORY, "min_revenue=")],
    "min_revenue not finite": [(BRAND_CATEGORY, "min_revenue=nan"),
                               (BRAND_CATEGORY, "min_revenue=inf"),
                               (BRAND_CATEGORY, "min_revenue=-inf")],
    "min_revenue past 28 digits": [(BRAND_CATEGORY, "min_revenue=1e300")],
    "min_revenue padded": [(BRAND_CATEGORY, "min_revenue=%20500")],
    "min_revenue with an underscore": [(BRAND_CATEGORY, "min_revenue=1_000")],
    "min_revenue as written": [(BRAND_CATEGORY, "min_revenue=0"),
                               (BRAND_CATEGORY, "min_revenue=500.5"),
                               (BRAND_CATEGORY, "min_revenue=1e3"),
                               (BRAND_CATEGORY, "min_revenue=-0")],
    "margin_floor out of bounds": [(ALERTS, "margin_floor=101"), (ALERTS, "margin_floor=-1"),
                                   (ALERTS, "margin_floor=100.0001")],
    "margin_floor not finite": [(ALERTS, "margin_floor=nan"), (ALERTS, "margin_floor=inf")],
    "margin_floor padded": [(ALERTS, "margin_floor=%2030")],
    "margin_floor as written": [(ALERTS, "margin_floor=0"), (ALERTS, "margin_floor=100"),
                                (ALERTS, "margin_floor=1e1"), (ALERTS, "margin_floor=55.5")],
    "another route's parameter": [(OVERVIEW, "limit=x&min_revenue=y&margin_floor=z"),
                                  (TREND, "limit=0")],
    "period unknown": [(OVERVIEW, "period=xyz"), (OVERVIEW, "period=custom"),
                       (OVERVIEW, "period=")],
    "period respelled": [(OVERVIEW, "period=TODAY"), (TREND, "period=%20month%20"),
                         (BY_CATEGORY, "period=Last_Week")],
    "period repeated": [(TREND, "period=xyz&period=month"), (TREND, "period=month&period=xyz")],
    "no window": [(OVERVIEW, "sales_type=retail"), (OVERVIEW, "")],
    "sales type unknown": [(OVERVIEW, "sales_type=bogus"), (OVERVIEW, "sales_type=")],
    "sales type respelled": [(TREND, "sales_type=%20all%20"), (BY_CATEGORY, "sales_type=B2B")],
    "dates malformed": [(OVERVIEW, "start_date=2026-02-30&end_date=2026-03-01"),
                        (OVERVIEW, "start_date=26-01-01&end_date=2026-01-02")],
    "dates strptime forgives": [(OVERVIEW, "start_date=2026-1-5&end_date=2026-01-09")],
    "dates reversed": [(TREND, "start_date=2026-03-01&end_date=2026-02-01")],
    "one date alone": [(TREND, "start_date=2026-03-01"), (TREND, "end_date=2026-03-01")],
    "period and dates together": [(OVERVIEW, "period=week&start_date=2026-01-01&end_date=2026-01-31")],
    "header filters malformed": [(ALERTS, "source_id=abc&category_id=-1&brand=%00&promocode="
                                  + "x" * 300)],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree. None of it can come from the page: its six hooks pass
# `useQueryParams()` and nothing else, so `limit`, `min_revenue` and
# `margin_floor` are never sent at all. What Python accepts here is
# pydantic's lax reading of a query string; Rust reads the number as written
# and finite, and refuses the rest with a 422 naming the field.
_LAX = "pydantic's lax {kind} reads {what}; Rust reads a number as written"
DIVERGENCES: Dict[str, Divergence] = {
    "limit as an integral float": Divergence(200, 422, _LAX.format(
        kind="int", what="'5.0' as 5")),
    "limit padded": Divergence(200, 422, _LAX.format(
        kind="int", what="' 5' and '5 ' as 5, stripping the whitespace")),
    "limit with an underscore": Divergence(200, 422, _LAX.format(
        kind="int", what="'1_0' as 10, as int() does")),
    "min_revenue padded": Divergence(200, 422, _LAX.format(
        kind="float", what="' 500' as 500.0")),
    "min_revenue with an underscore": Divergence(200, 422, _LAX.format(
        kind="float", what="'1_000' as 1000.0")),
    "margin_floor padded": Divergence(200, 422, _LAX.format(
        kind="float", what="' 30' as 30.0")),
    # 'nan' and '-inf' fail `ge=0` on both sides; 'inf' passes it in Python.
    "min_revenue not finite": Divergence(200, 422, (
        "Python binds float('inf') as numeric Infinity and answers an empty "
        "list; Rust refuses a number that is not finite")),
    "min_revenue past 28 digits": Divergence(200, 422, (
        "Python binds 1e300 as numeric; Rust's decimal has 28 digits and "
        "refuses a bound it cannot bind rather than round it")),
}

REPORT = rust_wave1.Report()


@pytest.fixture(scope="module")
def seeded():
    """The pilot's orders with a catalogue, line items and costs, once."""
    from tests import rust_wave1_fixture

    async def _with_pool(work):
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            return await work(pool)
        finally:
            await pool.close()

    REPORT.data["seed"] = asyncio.run(_with_pool(rust_wave1_fixture.seed))
    yield
    asyncio.run(_with_pool(rust_wave1_fixture.clear))
    REPORT.write(REPORT_PATH)


@pytest_asyncio.fixture
async def pair(seeded, duckdb_off):
    async with rust_wave1.services(DSN, PILOT, ENV) as both:
        yield both


async def _lookups(python) -> Dict[str, list]:
    """What the header's dropdowns offer: `SourceFilter`'s constant, and
    the app's own answers for the other three — a root's children too,
    which `CategoryFilter` offers once a root is picked."""
    async def names(path: str) -> list:
        answer = await python.get(path)
        assert answer.status_code == 200, (path, answer.status_code, answer.text[:200])
        return answer.json()

    roots = await names("/api/categories")
    children = await names(f"/api/categories/{roots[0]['id']}/children") if roots else []
    return {
        "source_id": list(frontend_contract.sources()),
        "category_id": [c["id"] for c in roots + children],
        "brand": [b["name"] for b in await names("/api/brands")],
        "promocode": [p["name"] for p in await names("/api/promocodes")],
    }


def _contract(lookups: Dict[str, list]) -> List[Tuple[str, str]]:
    """Every endpoint, window and sales type with no filter set; then each
    filter value the header offers, and all four at once, on the periods."""
    windows = [{"period": p} for p in CONTRACT.periods]
    windows += [{"period": "custom", "start_date": s, "end_date": e} for s, e in RANGES]
    filters: List[Dict[str, object]] = [
        {name: value} for name, values in lookups.items() for value in values]
    filters.append({name: values[-1] for name, values in lookups.items() if values})
    cases = []
    for path in CONTRACT.endpoints:
        for window in windows:
            for sales_type in CONTRACT.sales_types:
                cases.append((path, frontend_contract.query(CONTRACT, sales_type=sales_type,
                                                            **window)))
        for period in CONTRACT.periods:
            for chosen in filters:
                cases.append((path, frontend_contract.query(
                    CONTRACT, period=period, sales_type="retail", **chosen)))
        for chosen in filters:
            cases.append((path, frontend_contract.query(
                CONTRACT, period="month", sales_type="all", **chosen)))
    return cases


def _has_rows(path: str, body: bytes) -> bool:
    answer = json.loads(body)
    return bool(answer["total_revenue"]) if path == OVERVIEW else bool(answer)


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    lookups = await _lookups(pair.python)
    REPORT.data["lookups"] = lookups
    assert all(lookups.values()), lookups
    rows, with_rows = [], 0
    for path, query in _contract(lookups):
        py, rs = await pair.ask(path, query)
        found = rust_wave1.difference(py, rs)
        rows.append((f"{path}?{query}", py.status_code, found))
        with_rows += not found and py.status_code == 200 and _has_rows(path, py.content)
    differences = REPORT.phase("contract", rows)
    REPORT.data["phases"]["contract"]["with_rows"] = with_rows
    assert not differences, "\n".join(differences[:20])
    assert {200, 400} <= {status for _, status, _ in rows}
    assert with_rows > len(rows) // 2, (with_rows, len(rows))
    # Python answered from Postgres: no DuckDB open, nothing fell back or was refused.
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


@pytest.mark.asyncio
async def test_the_shortcuts_turn_at_kyivs_midnights(pair):
    rows = []
    for now in rust_wave1.KYIV_INSTANTS:
        for path in CONTRACT.endpoints:
            for period in CONTRACT.periods:
                query = frontend_contract.query(CONTRACT, period=period, sales_type="retail")
                py, rs = await pair.ask(path, query, now=now)
                rows.append((f"{now} {path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("midnights", rows)
    assert not differences, "\n".join(differences[:20])


@pytest.mark.asyncio
async def test_today_on_the_real_clock(pair):
    """Nothing pinned: each service reads its own clock. Asked again if a
    Kyiv midnight fell between the two requests."""
    for _ in range(3):
        before = datetime.now(KYIV).date()
        py, rs = await pair.ask(OVERVIEW, "period=today&sales_type=all", now=None)
        if datetime.now(KYIV).date() == before:
            break
    assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    assert py.status_code == 200


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_503(pair):
    """A schema this build does not know, then a table that cannot be read:
    both services refuse with the 503 the page reads as "try again"."""
    query = frontend_contract.query(CONTRACT, period="month", sales_type="retail")
    rows = []
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    breaks = (
        ("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
         f"UPDATE meta.alembic_version SET version_num = '{revision}'"),
        ("table", "ALTER TABLE bronze.offer_stocks RENAME TO offer_stocks_away",
         "ALTER TABLE bronze.offer_stocks_away RENAME TO offer_stocks"),
    )
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        try:
            for path in CONTRACT.endpoints:
                py, rs = await pair.ask(path, query)
                rows.append((f"{name} {path}", py.status_code, rust_wave1.difference(py, rs)))
        finally:
            async with pair.pool.acquire() as conn:
                await conn.execute(mended)
    differences = REPORT.phase("unreadable", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {503}
    # And once mended, both answer again.
    py, rs = await pair.ask(OVERVIEW, query)
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_the_limit_counts_what_it_counts_on_both(pair):
    """Thirty a minute per route and client. A request FastAPI refuses (422)
    never reaches the limit; one the route's validators refuse (400) is
    counted. The 429 is something the page can get, so its bytes compare."""
    key = f"margin-limit-{pair.run}"
    asks = (["limit=0"] * 3 + ["period=xyz&sales_type=retail"] * 5
            + ["period=month&sales_type=retail"] * 26)
    py_statuses, rs_statuses = [], []
    for query in asks:
        py, rs = await pair.ask(BY_BRAND, query, key=key)
        py_statuses.append(py.status_code)
        rs_statuses.append(rs.status_code)
    REPORT.phase("limit", [(f"{BY_BRAND}?{q}", p, None if p == r else f"{p} against {r}")
                           for q, p, r in zip(asks, py_statuses, rs_statuses)])
    assert py_statuses == [422] * 3 + [400] * 5 + [200] * 25 + [429], py_statuses
    assert rs_statuses == py_statuses
    assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)


@pytest.mark.asyncio
async def test_hostile_input_differs_only_where_listed(pair):
    answers = []
    for kind, requests in HOSTILE.items():
        for path, query in requests:
            py, rs = await pair.ask(path, query)
            answers.append((kind, f"{path}?{query}", py.status_code, rs.status_code))
    REPORT.phase("hostile", [(request, py, None if py == rs else f"{py} against {rs}")
                             for _, request, py, rs in answers])
    REPORT.data["divergences"] = {kind: {"python": d.python, "rust": d.rust, "why": d.why}
                                  for kind, d in DIVERGENCES.items()}
    unlisted, stale = rust_wave1.judge_hostile(answers, DIVERGENCES)
    assert not unlisted, "differences no entry lists:\n" + "\n".join(unlisted)
    assert not stale, f"entries that no longer hold: {stale}"
