"""Python against Rust on `/api/reports/*`, by contract: wave 1's /reports
and /marketing, which share one router.

`web.main.app` in this process under `KS_READ_REPORTS=postgres` and
`KS_READ_MARKETING=postgres`, as production runs it, and the pilot over
HTTP, on one Postgres seeded through production's derivations
(`tests/rust_wave1_fixture.py`) plus a monthly goal; the machinery is
`tests/rust_wave1.py`'s.

THE CONTRACT: status, Content-Type and body, byte for byte — and for the CSV
downloads `Content-Disposition` too, which names the file the browser saves.

What the two pages can send, read out of the TypeScript: /reports' three
hooks (`frontend_contract.reports()`) and its CSV export
(`download_csv_query`: the summary, and the top products with their source
chip and a limit, 5000 for "all"); /marketing's report
(`frontend_contract.marketing_page()`) and its CSV link
(`marketing_csv_query`, built by hand: the sales type first, no source).
Over one of the eight periods or the two dates the picker gives, the five
sales types and the header's four filters, which the routes read — the
source, the category with its branch, the brand — or ignore alike (the
promocode). Then the shortcuts at the Kyiv midnights, a few edges beyond the
contract the bytes still settle, the 503s naming `reports`, `marketing` and
`lookups`, and the two limits, 30 a minute and 10 for a CSV.

THE REST: the status alone, against DIVERGENCES.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_REPORTS_REPORT` set, the counts go there.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests import frontend_contract as fc
from tests import rust_wave1
from tests.rust_wave1 import PICKER_RANGES, Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_REPORTS_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
REPORTS = fc.reports()
MARKETING = fc.marketing_page()
ENV = {"KS_READ_REPORTS": "postgres", "KS_READ_MARKETING": "postgres",
       "KS_READ_LOOKUPS": "postgres"}
SUMMARY, TOP, ALL_PRODUCTS = REPORTS.endpoints
(MONTHLY,) = MARKETING.endpoints
EXPORT = fc.download_csv_path()
MARKETING_CSV = fc.marketing_csv_path()
CSVS = (EXPORT, MARKETING_CSV)

# The top products tab: its source chips (None is "all sources", which sends
# nothing) and its limits; "all" (0) asks /all-products beside a top ten, and
# exports 5000.
CHIPS: Tuple[Optional[int], ...] = (None, *fc.report_sources())
TOP_LIMITS = tuple(n for n in fc.report_limits() if n)
EXPORT_LIMITS = (*TOP_LIMITS, 5000)
DEFAULT_LIMIT = TOP_LIMITS[0]

# ₴4.2 млн: `_fmt_goal`'s fractional branch, shown on a full month only.
GOAL = Decimal("4200000.00")


def _headers(path: str) -> Tuple[str, ...]:
    return ("content-type", "content-disposition") if path in CSVS else ("content-type",)


# Hostile input, by class: (endpoint, query). The status alone is compared.
_WEEK = "period=week&sales_type=retail"
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "limit out of bounds": [(TOP, "limit=0"), (TOP, "limit=51"), (TOP, "limit=-1"),
                            (EXPORT, "type=top_products&limit=5001"),
                            (EXPORT, "type=top_products&limit=0")],
    "limit not an integer": [(TOP, "limit=abc"), (TOP, "limit="), (TOP, "limit=5.5"),
                             (EXPORT, "type=summary&limit=abc")],
    "limit as pydantic reads it": [(TOP, "limit=5.0"), (TOP, "limit=%205"), (TOP, "limit=1_0"),
                                   (TOP, "limit=%2B5"), (EXPORT, "type=top_products&limit=05")],
    "limit past 64 bits": [(TOP, "limit=99999999999999999999"),
                           (EXPORT, "type=top_products&limit=99999999999999999999")],
    "limit where it is not validated": [(EXPORT, "type=summary&limit=0"),
                                        (EXPORT, "type=summary&limit=-5")],
    "source_id out of range": [(SUMMARY, "source_id=0"), (TOP, "source_id=5"),
                               (EXPORT, "type=summary&source_id=-1")],
    "source_id not an integer": [(SUMMARY, "source_id=abc"), (ALL_PRODUCTS, "source_id="),
                                 (TOP, "source_id=1.5")],
    "source_id as pydantic reads it": [(SUMMARY, "source_id=1.0"), (TOP, "source_id=%204"),
                                       (ALL_PRODUCTS, "source_id=0_2")],
    "source_id past 64 bits": [(SUMMARY, "source_id=99999999999999999999")],
    "category_id not positive": [(SUMMARY, "category_id=0"), (MONTHLY, "category_id=-1"),
                                 (MARKETING_CSV, "category_id=0")],
    "category_id not an integer": [(TOP, "category_id=abc"), (MONTHLY, "category_id=1.5"),
                                   (MARKETING_CSV, "category_id=")],
    "category_id as pydantic reads it": [(SUMMARY, "category_id=1.0"),
                                         (MONTHLY, "category_id=%201")],
    "category_id past int32": [(SUMMARY, "category_id=3000000000"),
                               (MONTHLY, "category_id=3000000000")],
    "category_id past 64 bits": [(SUMMARY, "category_id=99999999999999999999"),
                                 (MONTHLY, "category_id=99999999999999999999")],
    "brand malformed": [(SUMMARY, "brand=%00"), (TOP, "brand=%3Cscript%3E"),
                        (MONTHLY, "brand=%20%20"), (MARKETING_CSV, "brand=" + "a" * 256)],
    "brand with a combining mark": [(SUMMARY, "brand=%E0%A4%95%E0%A4%BE"),
                                    (MONTHLY, "brand=%E2%92%B6")],
    "type unknown": [(EXPORT, "type=bogus"), (EXPORT, "type="), (EXPORT, "type=SUMMARY")],
    "type missing": [(EXPORT, ""), (EXPORT, "period=week&limit=abc")],
    "type checked first": [(EXPORT, "type=bogus&period=xyz"),
                           (EXPORT, "type=bogus&source_id=9")],
    "another route's parameter": [(SUMMARY, "limit=x&type=y"), (ALL_PRODUCTS, "limit=abc"),
                                  (MONTHLY, "source_id=abc&limit=x&type=y"),
                                  (MARKETING_CSV, "source_id=x&promocode=%00")],
    "period unknown": [(SUMMARY, "period=xyz"), (TOP, "period=custom"), (MONTHLY, "period=xyz"),
                       (EXPORT, "type=summary&period=xyz")],
    "period empty": [(SUMMARY, "period="), (MONTHLY, "period="), (MARKETING_CSV, "period=")],
    "period respelled": [(SUMMARY, "period=TODAY"), (MONTHLY, "period=%20month%20")],
    "period repeated": [(TOP, "period=xyz&period=month"), (MONTHLY, "period=month&period=xyz")],
    "no window": [(SUMMARY, ""), (MONTHLY, ""), (EXPORT, "type=summary")],
    "sales type unknown": [(SUMMARY, "sales_type=bogus"), (MONTHLY, "sales_type=wholesale"),
                           (MARKETING_CSV, "sales_type=x")],
    "sales type respelled": [(TOP, "sales_type=%20all%20"), (MONTHLY, "sales_type=B2B"),
                             (ALL_PRODUCTS, "sales_type=")],
    "dates malformed": [(SUMMARY, "start_date=2026-02-30&end_date=2026-03-01"),
                        (MONTHLY, "start_date=x&end_date=2026-03-01")],
    "dates strptime forgives": [(TOP, "start_date=2026-1-5&end_date=2026-01-29"),
                                (MONTHLY, "start_date=2026-1-5&end_date=2026-01-29")],
    "dates reversed": [(SUMMARY, "start_date=2026-03-01&end_date=2026-02-01"),
                       (MONTHLY, "start_date=2026-03-01&end_date=2026-02-01"),
                       (MARKETING_CSV, "start_date=2026-03-01&end_date=2026-02-01")],
    "one date alone": [(ALL_PRODUCTS, "start_date=2026-03-01"), (MONTHLY, "end_date=2026-03-01")],
    "validators in their order": [(SUMMARY, "period=xyz&source_id=9"),
                                  (SUMMARY, "source_id=9&category_id=-1"),
                                  (TOP, "category_id=-1&limit=0"),
                                  (TOP, f"{_WEEK}&brand=%00&limit=0"),
                                  (MONTHLY, "sales_type=x&period=xyz"),
                                  (MONTHLY, "period=xyz&category_id=-1"),
                                  (MARKETING_CSV, "category_id=-1&brand=%00")],
    "a parameter repeated": [(SUMMARY, "source_id=9&source_id=1"),
                             (EXPORT, "type=bogus&type=summary"),
                             (TOP, "limit=0&limit=5")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree. The pages send `source_id`, `category_id` and `limit` as decimal
# digits of a value they hold — a chip, a dropdown's id, a tab's option.
# Rust reads them as pydantic does as far as 64 bits reach: past that,
# Python's int has no width, Rust's i64 does not parse.
_WIDTH = "Python's int has no width{}; Rust's i64 does not parse it"
DIVERGENCES: Dict[str, Divergence] = {
    "limit past 64 bits": Divergence(400, 422, _WIDTH.format(
        ", so validate_limit refuses it as over the cap")),
    "source_id past 64 bits": Divergence(400, 422, _WIDTH.format(
        ", so validate_source_id refuses it as no source")),
    # Not listed since #335: a category_id past int32, or past 64 bits, is
    # validate_category_id's 400 on both sides; BAD_INPUT compares it byte
    # for byte.
    "brand with a combining mark": Divergence(400, 200, (
        "validate_brand_name's \\w is str.isalnum(); Rust's char::is_alphanumeric "
        "also takes Unicode's Other_Alphabetic marks — a Devanagari vowel sign, "
        "a circled letter")),
}

REPORT = rust_wave1.Report()


@pytest.fixture(scope="module")
def seeded():
    from tests import rust_wave1_fixture

    async def _seed(pool):
        seed = await rust_wave1_fixture.seed(pool)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.revenue_goals")
            await conn.execute(
                "INSERT INTO app.revenue_goals (period_type, goal_amount, is_custom, updated_at) "
                "VALUES ('monthly', $1, TRUE, now())", GOAL)
        return {**seed, "monthly_goal": str(GOAL)}

    async def _clear(pool):
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.revenue_goals")
        await rust_wave1_fixture.clear(pool)

    async def _with_pool(work):
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            return await work(pool)
        finally:
            await pool.close()

    REPORT.data["seed"] = asyncio.run(_with_pool(_seed))
    yield
    asyncio.run(_with_pool(_clear))
    REPORT.write(REPORT_PATH)


@pytest_asyncio.fixture
async def pair(seeded, duckdb_off):
    async with rust_wave1.services(DSN, PILOT, ENV) as both:
        yield both


def _windows() -> List[Dict[str, str]]:
    windows = [{"period": p} for p in REPORTS.periods]
    windows += [{"period": "custom", "start_date": s, "end_date": e} for s, e in PICKER_RANGES]
    return windows


def _requests(state: Dict[str, object]) -> List[Tuple[str, str]]:
    """Every request the two pages make from one state of the header, the
    top products tab at its defaults."""
    marketing_link = {k: v for k, v in state.items()
                      if k in ("period", "start_date", "end_date", "sales_type",
                               "category_id", "brand")}
    return [
        (SUMMARY, fc.query(REPORTS, **state)),
        (TOP, fc.query(REPORTS, endpoint=TOP, extra={"limit": DEFAULT_LIMIT}, **state)),
        (ALL_PRODUCTS, fc.query(REPORTS, endpoint=ALL_PRODUCTS, extra={}, **state)),
        (EXPORT, fc.download_csv_query(REPORTS, "summary", **state)),
        (EXPORT, fc.download_csv_query(REPORTS, "top_products", limit=DEFAULT_LIMIT, **state)),
        (MONTHLY, fc.query(MARKETING, **state)),
        (MARKETING_CSV, fc.marketing_csv_query(**marketing_link)),
    ]


def _contract(filters: Dict[str, list]) -> List[Tuple[str, str]]:
    windows = _windows()
    chosen: List[Dict[str, object]] = [
        {name: value} for name, values in filters.items() for value in values]
    chosen.append({name: values[-1] for name, values in filters.items()})
    cases: List[Tuple[str, str]] = []
    for window in windows:
        for sales_type in REPORTS.sales_types:
            cases += _requests({"sales_type": sales_type, **window})
    for period in REPORTS.periods:
        for header in chosen:
            cases += _requests({"period": period, "sales_type": "retail", **header})
    # The tab's own controls, over every window: a chip, a limit, "all".
    for window in windows:
        for sales_type in ("retail", "all"):
            state = {"sales_type": sales_type, **window}
            for chip in CHIPS:
                for limit in TOP_LIMITS:
                    cases.append((TOP, fc.query(REPORTS, endpoint=TOP, **state,
                                                extra={"source_id": chip, "limit": limit})))
                cases.append((ALL_PRODUCTS, fc.query(REPORTS, endpoint=ALL_PRODUCTS, **state,
                                                     extra={"source_id": chip})))
                for limit in EXPORT_LIMITS:
                    cases.append((EXPORT, fc.download_csv_query(
                        REPORTS, "top_products", chip=chip, limit=limit, **state)))
    # A source picked in the header and a chip over it, on the CSV too.
    for chip in CHIPS:
        state = {"period": "last_month", "sales_type": "retail", "source_id": 4}
        cases.append((TOP, fc.query(REPORTS, endpoint=TOP, **state,
                                    extra={"source_id": chip, "limit": 25})))
        cases.append((EXPORT, fc.download_csv_query(
            REPORTS, "top_products", chip=chip, limit=5000, **state)))
    return list(dict.fromkeys(cases))


def _csv_rows(body: bytes) -> List[List[str]]:
    assert body.startswith(b"\xef\xbb\xbf"), body[:20]
    return list(csv.reader(io.StringIO(body[3:].decode("utf-8"), newline="")))


def _has_rows(path: str, query: str, body: bytes) -> bool:
    if path == EXPORT:
        rows = _csv_rows(body)
        if "type=summary" in query:
            return any(row and row[0].startswith("Source: ") for row in rows)
        return len(rows) > 1
    if path == MARKETING_CSV:
        return any(row and row[0] == "Виручка (грн)" and row[1] != "₴0"
                   for row in _csv_rows(body))
    answer = json.loads(body)
    if path == SUMMARY:
        return bool(answer["totals"]["orders_count"])
    if path == MONTHLY:
        return bool(answer["general_sales"]["current"]["orders"])
    return bool(answer)


def _shows_the_goal(path: str, body: bytes) -> bool:
    if path == MONTHLY:
        return json.loads(body)["general_sales"]["monthly_goal"] is not None
    if path == MARKETING_CSV:
        return any("Ціль місяця" in row for row in _csv_rows(body))
    return False


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    filters = await rust_wave1.header_filters(pair.python)
    REPORT.data["lookups"] = filters
    rows, with_rows, goals, narrowed = [], 0, 0, 0
    for path, query in _contract(filters):
        py, rs = await pair.ask(path, query)
        found = rust_wave1.difference(py, rs, _headers(path))
        rows.append((f"{path}?{query}", py.status_code, found))
        if found or py.status_code != 200:
            continue
        with_rows += _has_rows(path, query, py.content)
        goals += _shows_the_goal(path, py.content)
        narrowed += path == MONTHLY and json.loads(py.content)["product_filter"] is not None
    differences = REPORT.phase("contract", rows)
    REPORT.data["phases"]["contract"].update(
        with_rows=with_rows, with_the_goal=goals, narrowed_by_a_product=narrowed)
    assert not differences, "\n".join(differences[:20])
    assert {200, 400} <= {status for _, status, _ in rows}
    assert with_rows > len(rows) // 2, (with_rows, len(rows))
    # The goal's branch and the product filter's: neither left to a seed
    # that happened to miss it.
    assert goals and narrowed, (goals, narrowed)
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


@pytest.mark.asyncio
async def test_the_shortcuts_turn_at_kyivs_midnights(pair):
    rows = []
    for now in rust_wave1.KYIV_INSTANTS:
        for period in REPORTS.periods:
            for path, query in _requests({"period": period, "sales_type": "retail"}):
                py, rs = await pair.ask(path, query, now=now)
                rows.append((f"{now} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs, _headers(path))))
    differences = REPORT.phase("midnights", rows)
    assert not differences, "\n".join(differences[:20])


@pytest.mark.asyncio
async def test_today_on_the_real_clock(pair):
    for _ in range(3):
        before = datetime.now(KYIV).date()
        py, rs = await pair.ask(MONTHLY, "period=today&sales_type=all", now=None)
        if datetime.now(KYIV).date() == before:
            break
    assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    assert py.status_code == 200


# Beyond the contract, and still settled by the bytes: what a value the
# pages never send asks of the arithmetic and the SQL.
EDGES: Tuple[Tuple[str, str], ...] = (
    (SUMMARY, f"{_WEEK}&brand=Unknown"),                  # the unnamed bucket
    (TOP, f"{_WEEK}&brand=%20unKnown%20&limit=50"),       # stripped, any case
    (MONTHLY, "period=last_month&sales_type=all&brand=Unknown"),
    (SUMMARY, f"{_WEEK}&category_id=999999"),             # a category nobody has
    (TOP, "period=last_month&sales_type=all&category_id=77&limit=50"),  # carried, not listed
    (MONTHLY, "period=last_month&sales_type=retail&category_id=99"),    # an orphan
    (SUMMARY, "period=last_month&sales_type=all&source_id=3"),          # retired
    (TOP, "start_date=2026-03-01&end_date=2026-02-01&sales_type=retail&limit=1"),
    (MONTHLY, "start_date=2026-03-01&end_date=2026-02-01&sales_type=retail"),
    (MARKETING_CSV, "start_date=2026-03-01&end_date=2026-02-01&sales_type=retail"),
    (EXPORT, "type=summary&period=last_month&sales_type=all&limit=0"),
    (EXPORT, "type=top_products&period=last_month&sales_type=all&limit=1"),
    (EXPORT, "type=summary&start_date=2026-03-01&end_date=2026-02-01&sales_type=all"),
    (MONTHLY, "period=&start_date=2024-02-01&end_date=2024-02-29&sales_type=retail"),
    (MARKETING_CSV, "start_date=2024-02-01&end_date=2024-02-29&sales_type=all"),
    (MONTHLY, "start_date=2025-02-28&end_date=2025-02-28&sales_type=retail"),
)


@pytest.mark.asyncio
async def test_the_edges_answer_byte_for_byte(pair):
    rows = []
    for path, query in EDGES:
        py, rs = await pair.ask(path, query)
        rows.append((f"{path}?{query}", py.status_code,
                     rust_wave1.difference(py, rs, _headers(path))))
    differences = REPORT.phase("edges", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {200}


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_503(pair):
    """A schema this build does not know, then the line view gone: each
    surface the routes name, the category tree's among them."""
    month = "period=month&sales_type=retail"
    asks = (
        (SUMMARY, month), (SUMMARY, f"{month}&category_id=1"), (SUMMARY, f"{month}&brand=COSRX"),
        (TOP, f"{month}&limit=10"), (ALL_PRODUCTS, month),
        (EXPORT, f"type=summary&{month}"), (EXPORT, f"type=top_products&{month}&limit=10"),
        (MONTHLY, month), (MONTHLY, f"{month}&category_id=1"),
        (MARKETING_CSV, month),
    )
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    breaks = (
        ("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
         f"UPDATE meta.alembic_version SET version_num = '{revision}'"),
        ("view", "ALTER VIEW silver.order_lines RENAME TO order_lines_away",
         "ALTER VIEW silver.order_lines_away RENAME TO order_lines"),
    )
    rows, statuses, surfaces = [], {}, set()
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        try:
            for path, query in asks:
                py, rs = await pair.ask(path, query)
                rows.append((f"{name} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs, _headers(path))))
                statuses[(name, path, query)] = py.status_code
                if py.status_code == 503:
                    surfaces.add(py.json()["surface"])
        finally:
            async with pair.pool.acquire() as conn:
                await conn.execute(mended)
    differences = REPORT.phase("unreadable", rows)
    assert not differences, "\n".join(differences)
    # Every read asks the revision first; only the unfiltered summary reads
    # no line view.
    assert {s for (name, _, _), s in statuses.items() if name == "revision"} == {503}
    assert {(p, q) for (name, p, q), s in statuses.items()
            if name == "view" and s != 503} == {(SUMMARY, month)}
    assert surfaces == {"reports", "marketing", "lookups"}, surfaces
    py, rs = await pair.ask(MONTHLY, month)
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_each_limit_counts_what_it_counts_on_both(pair):
    """30 a minute on a report, 10 on a CSV: a 422 is never counted, a 400
    the route raises always is."""
    plans = (
        (TOP, 30, ["limit=abc"] * 3 + ["period=xyz&sales_type=retail"] * 5
         + ["period=month&sales_type=retail"] * 26),
        (EXPORT, 10, ["type=summary&limit=abc"] * 3 + ["type=bogus&period=month"] * 3
         + ["type=summary&period=month&sales_type=retail"] * 8),
        (MARKETING_CSV, 10, ["category_id=abc"] * 3 + ["sales_type=bogus"] * 3
         + ["sales_type=retail&period=month"] * 8),
    )
    rows = []
    for plan, (path, per_minute, asks) in enumerate(plans):
        key = f"reports-limit-{pair.run}-{plan}"
        py_statuses, rs_statuses = [], []
        for query in asks:
            py, rs = await pair.ask(path, query, key=key)
            py_statuses.append(py.status_code)
            rs_statuses.append(rs.status_code)
            rows.append((f"{path}?{query}", py.status_code,
                         None if py.status_code == rs.status_code
                         else f"{py.status_code} against {rs.status_code}"))
        refused = sum(1 for s in py_statuses if s == 400)
        assert py_statuses == ([422] * 3 + [400] * refused
                               + [200] * (per_minute - refused) + [429]), (path, py_statuses)
        assert rs_statuses == py_statuses, path
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    REPORT.phase("limit", rows)


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


# #335's 400s, byte for byte: a category past `bronze.categories.id`'s int32
# and past 64 bits, on the reports' order of checks and on the marketing
# routes' own; and a NUL anywhere in the query, which the middleware refuses
# before routing.
BAD_INPUT = ((SUMMARY, "category_id=2147483648"), (SUMMARY, "category_id=99999999999999999999"),
             (TOP, "category_id=-99999999999999999999"), (ALL_PRODUCTS, "category_id=3000000000"),
             (MONTHLY, "category_id=2147483648"),
             (MONTHLY, "sales_type=retail&category_id=99999999999999999999"),
             (SUMMARY, "brand=a%00b"), (TOP, "period=%00"), (MONTHLY, "x=%00"))


@pytest.mark.asyncio
async def test_bad_input_is_the_same_400_byte_for_byte(pair):
    rows = []
    for path, query in BAD_INPUT:
        py, rs = await pair.ask(path, query)
        rows.append((f"{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("bad_input", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {400}
