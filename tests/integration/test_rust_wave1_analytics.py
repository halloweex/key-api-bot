"""Python against Rust on the analytics router's reads, by contract: the
dashboard's cards and charts, /marketing's promocodes and the filter bar.

`web.main.app` in this process with `KS_READ_GOLD`, `KS_READ_SILVER`,
`KS_READ_DASHBOARD`, `KS_READ_LOOKUPS` and `KS_READ_MARKETING` at `postgres`,
as production runs it, and the pilot over HTTP, on one Postgres seeded through
production's derivations (`tests/rust_wave1_fixture.py`) plus what the tab
reads on top: buyers and managers by name for the returns list, product names
the top-products chart has to wrap, and one day whose lines were given away at
a quantity of nought.

THE CONTRACT: status, Content-Type and body, byte for byte.

What the dashboard and /marketing send (`frontend_contract.dashboard()`,
`.marketing_analytics()`): one of the eight periods or the two dates the
picker gives, the five sales types, and the header's four filters, whose
values the app's own lookups offer. The category drill-down, with every name
the category chart can be clicked on. The filter bar's four lookups, a root's
children among them. Then the shortcuts at the Kyiv midnights, edges, the
catalogue emptied, the ways Postgres cannot answer, the two limits, and #335's
400s byte for byte.

THE REST: the status alone, against DIVERGENCES.

Not here: `/api/revenue/trend` (`test_rust_pilot_trend.py`) and the forecast,
which the pilot does not answer.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_ANALYTICS_REPORT` set, the counts go there.
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

from tests import frontend_contract as fc
from tests import rust_wave1
from tests.rust_wave1 import PICKER_RANGES, Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_ANALYTICS_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
CONTRACT = fc.dashboard()
MARKETING = fc.marketing_analytics()
ENV = {"KS_READ_GOLD": "postgres", "KS_READ_SILVER": "postgres",
       "KS_READ_DASHBOARD": "postgres", "KS_READ_LOOKUPS": "postgres",
       "KS_READ_MARKETING": "postgres"}
SUMMARY, RETURNS, BY_SOURCE, TOP, PERFORMANCE, BRAND_CHART = CONTRACT.endpoints
PROMOS = MARKETING.endpoints[1]
BREAKDOWN = "/api/categories/breakdown"
CATEGORIES, CHILDREN, BRANDS, PROMOCODES = fc.lookup_paths()
# The routes that take the page's query.
WINDOWED = (*CONTRACT.endpoints, PROMOS)
# Ids `CATEGORIES` in `tests/rust_wave1_fixture.py` names, a parent that does
# not exist, and ones nothing has.
CATEGORY_IDS = (1, 2, 3, 11, 12, 13, 31, 111, 99, 98, 77, 2147483647)

# The orders name buyers 1..4 000 and 4 001 on: a quarter have no row.
BUYERS = 3000
# The wholesale manager and the first retail one by name; the internal one
# (7) has no row, so its returns show no manager name.
MANAGERS = ((15, "Оптовий відділ"), (4, "Retail desk"))
# Names the top-products chart wraps, given to the products that sell most
# so the chart shows them: long, many words, a no-break space, one word past
# the width, whitespace alone, an empty catalogue name.
LABELS = ("Крем для обличчя з гіалуроновою кислотою і SPF 50",
          "Toner with a no-break space and a long tail",
          "x" * 40, " " * 30, "", "aaaaaaaaaaaa bbbbbbbbbbbb c")


async def _seed(pool) -> Dict[str, object]:
    from tests import rust_wave1_fixture

    seed = await rust_wave1_fixture.seed(pool)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.buyers WHERE id <= $1", BUYERS)
        await conn.copy_records_to_table(
            "buyers", schema_name="bronze", columns=("id", "full_name", "phone"),
            records=[(i, None if i % 17 == 0 else f"Покупець {i}",
                      None if i % 5 == 0 else f"+000000{i:04d}") for i in range(1, BUYERS + 1)])
        await conn.execute("DELETE FROM bronze.managers WHERE id = ANY($1::int[])",
                           [m for m, _ in MANAGERS])
        await conn.copy_records_to_table("managers", schema_name="bronze",
                                         columns=("id", "name"), records=list(MANAGERS))
        top = [r["product_id"] for r in await conn.fetch(
            "SELECT product_id FROM silver.order_lines WHERE product_id IS NOT NULL "
            "GROUP BY product_id ORDER BY SUM(quantity) DESC, product_id LIMIT $1", len(LABELS))]
        for product, name in zip(top, LABELS):
            await conn.execute("UPDATE bronze.products SET name = $2 WHERE id = $1", product, name)
        zero_day = await conn.fetchval(
            "SELECT MIN(order_date) FROM silver.orders "
            "WHERE order_date >= '2025-06-10' AND NOT is_return AND is_active_source")
        zeroed = await conn.execute(
            "UPDATE bronze.order_products SET quantity = 0 "
            "WHERE order_id IN (SELECT id FROM silver.orders WHERE order_date = $1)", zero_day)
    return {**seed, "buyers": BUYERS, "managers": len(MANAGERS), "relabelled": top,
            "zero_day": zero_day.isoformat(), "zeroed_lines": int(zeroed.split()[-1])}


async def _clear(pool) -> None:
    from tests import rust_wave1_fixture

    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.buyers WHERE id <= $1", BUYERS)
        await conn.execute("DELETE FROM bronze.managers WHERE id = ANY($1::int[])",
                           [m for m, _ in MANAGERS])
    await rust_wave1_fixture.clear(pool)


REPORT = rust_wave1.Report()


@pytest.fixture(scope="module")
def seeded():
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


def q(sales_type: str = "retail", path: str = SUMMARY, **state) -> str:
    """What the page sends `path` for this state of the header."""
    if path == BREAKDOWN:
        parent = state.pop("parent")
        return fc.category_breakdown_query(CONTRACT, parent, sales_type=sales_type, **state)
    return fc.query(MARKETING if path == PROMOS else CONTRACT, sales_type=sales_type, **state)


def _children(category: int) -> str:
    return CHILDREN.replace("{parent_id}", str(category))


async def _parents(python) -> List[str]:
    """The names the category chart can be clicked on: what
    `/products/performance` answers as its category split, over the whole
    history and every sales type."""
    names: List[str] = []
    for sales_type in CONTRACT.sales_types:
        answer = await python.get(f"{PERFORMANCE}?" + q(
            sales_type, PERFORMANCE, period="custom",
            start_date="2023-12-01", end_date="2027-01-31"))
        assert answer.status_code == 200, answer.text[:200]
        for name in answer.json()["categoryBreakdown"]["labels"]:
            if name not in names:
                names.append(name)
    assert "Other" in names and len(names) >= 4, names
    return names


def _contract(filters: Dict[str, list], parents: List[str]) -> List[Tuple[str, str]]:
    windows = [{"period": p} for p in CONTRACT.periods]
    windows += [{"period": "custom", "start_date": s, "end_date": e} for s, e in PICKER_RANGES]
    chosen: List[Dict[str, object]] = [
        {name: value} for name, values in filters.items() for value in values]
    chosen.append({name: values[-1] for name, values in filters.items()})
    cases = []
    for path in WINDOWED:
        for window in windows:
            for sales_type in CONTRACT.sales_types:
                cases.append((path, q(sales_type, path, **window)))
        for period in CONTRACT.periods:
            for header in chosen:
                cases.append((path, q("retail", path, period=period, **header)))
    for parent in parents:
        for window in windows:
            cases.append((BREAKDOWN, q("retail", BREAKDOWN, parent=parent, **window)))
        for header in chosen:
            cases.append((BREAKDOWN, q("all", BREAKDOWN, parent=parent, period="last_month",
                                       **header)))
    cases += [(CATEGORIES, ""), (BRANDS, ""), (PROMOCODES, "")]
    cases += [(_children(c), "") for c in filters["category_id"]]
    return cases


def _has_rows(path: str, body: bytes) -> bool:
    answer = json.loads(body)
    if path == SUMMARY:
        return bool(answer["totalOrders"])
    if path == RETURNS:
        return bool(answer["count"])
    if path in (PERFORMANCE, BRAND_CHART):
        return bool(answer["topByRevenue"]["labels"])
    if path == PROMOS:
        return bool(answer["table"])
    if isinstance(answer, dict):
        return bool(answer.get("labels") or answer.get("data"))
    return bool(answer)


async def _compare(pair, asks, prefix: str = "") -> List[Tuple[str, int, object]]:
    rows = []
    for path, query in asks:
        py, rs = await pair.ask(path, query)
        rows.append((f"{prefix}{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    return rows


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    filters = await rust_wave1.header_filters(pair.python)
    parents = await _parents(pair.python)
    REPORT.data["lookups"] = {**filters, "parent_category": parents}
    rows, with_rows = [], 0
    for path, query in _contract(filters, parents):
        py, rs = await pair.ask(path, query)
        found = rust_wave1.difference(py, rs)
        rows.append((f"{path}?{query}", py.status_code, found))
        with_rows += not found and py.status_code == 200 and _has_rows(path, py.content)
    differences = REPORT.phase("contract", rows)
    REPORT.data["phases"]["contract"]["with_rows"] = with_rows
    assert not differences, "\n".join(differences[:20])
    # A brand of spaces alone is in the dropdown and refused by the route.
    assert {200, 400} <= {status for _, status, _ in rows}
    assert with_rows > len(rows) // 2, (with_rows, len(rows))
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


@pytest.mark.asyncio
async def test_the_shortcuts_turn_at_kyivs_midnights(pair):
    rows = []
    for now in rust_wave1.KYIV_INSTANTS:
        for path in WINDOWED:
            for period in CONTRACT.periods:
                query = q("retail", path, period=period)
                py, rs = await pair.ask(path, query, now=now)
                rows.append((f"{now} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
    differences = REPORT.phase("midnights", rows)
    assert not differences, "\n".join(differences[:20])


@pytest.mark.asyncio
async def test_today_on_the_real_clock(pair):
    for _ in range(3):
        before = datetime.now(KYIV).date()
        py, rs = await pair.ask(SUMMARY, "period=today&sales_type=all", now=None)
        if datetime.now(KYIV).date() == before:
            break
    assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    assert py.status_code == 200


def _edges(zero_day: str) -> List[Tuple[str, str]]:
    month, history = "period=month&sales_type=", "start_date=2023-12-01&end_date=2027-01-31"
    nothing = "start_date=2030-01-01&end_date=2030-01-31&sales_type=retail"
    edges = [
        (TOP, month + "retail&limit=1"), (TOP, month + "all&limit=50"),
        (TOP, f"{history}&sales_type=all&limit=50"),
        # Every line of the day given away at nought: `sum(data)` is 0 and
        # Python divides by it.
        (TOP, f"start_date={zero_day}&end_date={zero_day}&sales_type=all"),
        (RETURNS, month + "all&limit=1"), (RETURNS, month + "all&limit=100"),
        (RETURNS, f"{history}&sales_type=all&limit=100"),
        (RETURNS, f"{history}&sales_type=b2b&limit=100"),
        # Opencart: no column in Gold, the four zeros; with a brand, the lines.
        (SUMMARY, month + "retail&source_id=3"),
        (SUMMARY, month + "retail&source_id=3&brand=COSRX"),
        (SUMMARY, month + "all&brand=Unknown"), (SUMMARY, month + "all&brand=%20unknown%20"),
        (SUMMARY, month + "retail&brand=%20COSRX%20"),
        (SUMMARY, month + "retail&promocode=%20vip%20"),
        (SUMMARY, month + "retail&promocode=sale10"),
        # A promocode of spaces alone is no promocode: the Gold branch.
        (SUMMARY, month + "retail&promocode=%20%20"),
        (SUMMARY, month + "retail&category_id=99999"), (SUMMARY, month + "retail&category_id=98"),
        (SUMMARY, month + "all&category_id=1&brand=COSRX&promocode=vip&source_id=4"),
        (SUMMARY, f"{history}&sales_type=all&category_id=11"),
        (BY_SOURCE, month + "all&category_id=1"), (BY_SOURCE, month + "retail&source_id=2"),
        (BY_SOURCE, f"{history}&sales_type=all&brand=Unknown"), (BY_SOURCE, nothing),
        (BY_SOURCE, nothing + "&brand=COSRX"),
        (PERFORMANCE, f"{history}&sales_type=all"), (PERFORMANCE, nothing),
        (PERFORMANCE, month + "all&brand=Unknown&promocode=SALE10"),
        (BRAND_CHART, f"{history}&sales_type=all"), (BRAND_CHART, nothing),
        (PROMOS, f"{history}&sales_type=all"), (PROMOS, nothing),
        (TOP, nothing), (SUMMARY, nothing), (RETURNS, nothing),
        (BREAKDOWN, month + "all&parent_category=Other"),
        (BREAKDOWN, month + "all&parent_category="),
        (BREAKDOWN, month + "all&parent_category=%D0%9A%D1%80%D0%B5%D0%BC%D0%B8"),
        (BREAKDOWN, month + "all&parent_category=Orphan"),
        (BREAKDOWN, f"{history}&sales_type=all&parent_category=Makeup&brand=Unknown&source_id=1"),
        (BREAKDOWN, nothing + "&parent_category=Makeup"),
    ]
    edges += [(_children(c), "") for c in CATEGORY_IDS]
    return edges


@pytest.mark.asyncio
async def test_the_edges_answer_byte_for_byte(pair):
    rows = await _compare(pair, _edges(REPORT.data["seed"]["zero_day"]))
    differences = REPORT.phase("edges", rows)
    assert not differences, "\n".join(differences)
    statuses = {status for _, status, _ in rows}
    assert {200, 500} <= statuses, statuses


@pytest.mark.asyncio
async def test_without_a_catalogue_the_bytes_still_agree(pair):
    """No product and no category: every line falls back to the name it was
    sold under, every brand is the unknown one, every category `Other`."""
    tables = ("bronze.products", "bronze.categories")
    async with pair.pool.acquire() as conn:
        saved = {t: [tuple(r) for r in await conn.fetch(f"SELECT * FROM {t}")] for t in tables}
        columns = {t: [r["attname"] for r in await conn.fetch(
            "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 "
            "AND NOT attisdropped ORDER BY attnum", t)] for t in tables}
        for table in tables:
            await conn.execute(f"DELETE FROM {table}")
    try:
        month = "period=last_month&sales_type=all"
        asks = [(path, month) for path in WINDOWED]
        asks += [(path, month + "&brand=Unknown") for path in (SUMMARY, TOP, PERFORMANCE)]
        asks += [(BREAKDOWN, month + "&parent_category=Other"),
                 (CATEGORIES, ""), (BRANDS, ""), (PROMOCODES, ""), (_children(1), ""),
                 (SUMMARY, month + "&category_id=1")]
        rows = await _compare(pair, asks)
    finally:
        async with pair.pool.acquire() as conn:
            for table in tables:
                schema, name = table.split(".")
                await conn.copy_records_to_table(name, schema_name=schema,
                                                 columns=columns[table], records=saved[table])
    differences = REPORT.phase("no_catalogue", rows)
    assert not differences, "\n".join(differences)


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_failure(pair):
    """A schema this build does not know, then each relation the reads name.
    The summary's Gold asks no revision; the returns list reads chain 5's
    managers with no fallback, so it fails as the app's 500."""
    month = "period=month&sales_type=retail"
    asks = [(path, month) for path in WINDOWED]
    asks += [(SUMMARY, month + "&brand=COSRX"), (SUMMARY, month + "&category_id=1"),
             (BY_SOURCE, month + "&promocode=vip"), (TOP, month + "&category_id=2"),
             (BREAKDOWN, month + "&parent_category=Makeup"),
             (CATEGORIES, ""), (_children(1), ""), (BRANDS, ""), (PROMOCODES, "")]
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    renamed = (("VIEW", "silver.order_lines"), ("TABLE", "gold.daily_revenue"),
               ("TABLE", "silver.orders"), ("TABLE", "bronze.categories"),
               ("TABLE", "bronze.products"), ("TABLE", "bronze.managers"))
    breaks = [("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
               f"UPDATE meta.alembic_version SET version_num = '{revision}'")]
    breaks += [(name, f"ALTER {kind} {name} RENAME TO {name.split('.')[1]}_away",
                f"ALTER {kind} {name}_away RENAME TO {name.split('.')[1]}")
               for kind, name in renamed]
    rows, statuses = [], {}
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        try:
            found = await _compare(pair, asks, prefix=f"{name} ")
        finally:
            async with pair.pool.acquire() as conn:
                await conn.execute(mended)
        rows += found
        statuses[name] = [status for _, status, _ in found]
    REPORT.data["unreadable_statuses"] = statuses
    differences = REPORT.phase("unreadable", rows)
    assert not differences, "\n".join(differences)
    assert {200, 500, 503} <= {status for _, status, _ in rows}
    py, rs = await pair.ask(SUMMARY, month)
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_each_limit_counts_what_it_counts_on_both(pair):
    """30 a minute on the charts, 60 on the lookups: a 422 is never counted,
    a 400 the route raises always is."""
    plans = (
        ([(TOP, "limit=abc")] * 3 + [(TOP, "limit=0&period=month")] * 4
         + [(TOP, "period=month&sales_type=retail")] * 27, 30),
        ([(CATEGORIES, "")] * 61, 60),
        ([("/api/categories/abc/children", "")] * 3 + [("/api/categories/0/children", "")] * 4
         + [(_children(1), "")] * 57, 60),
    )
    rows = []
    for plan, (asks, limit) in enumerate(plans):
        key = f"analytics-limit-{pair.run}-{plan}"
        py_statuses, rs_statuses = [], []
        for path, query in asks:
            py, rs = await pair.ask(path, query, key=key)
            py_statuses.append(py.status_code)
            rs_statuses.append(rs.status_code)
            rows.append((f"{path}?{query}", py.status_code,
                         None if py.status_code == rs.status_code
                         else f"{py.status_code} against {rs.status_code}"))
        unprocessable = sum(1 for s in py_statuses if s == 422)
        refused = sum(1 for s in py_statuses if s == 400)
        assert py_statuses == ([422] * unprocessable + [400] * refused
                               + [200] * (limit - refused) + [429]), (plan, py_statuses)
        assert rs_statuses == py_statuses, plan
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    REPORT.phase("limit", rows)


# #335's 400s and the routes' own, byte for byte: an integer past its column's
# width or past 64 bits, a limit past either bound, and a NUL anywhere.
BAD_INPUT = ((SUMMARY, "category_id=2147483648"), (TOP, "category_id=99999999999999999999"),
             (BY_SOURCE, "category_id=-99999999999999999999"),
             (SUMMARY, "source_id=99999999999999999999"),
             (PERFORMANCE, "source_id=-99999999999999999999"),
             (TOP, "limit=99999999999999999999"), (TOP, "limit=-99999999999999999999"),
             (TOP, "limit=51"), (TOP, "limit=0"),
             ("/api/categories/2147483648/children", ""),
             ("/api/categories/99999999999999999999/children", ""),
             ("/api/categories/0/children", ""), ("/api/categories/-1/children", ""),
             (SUMMARY, "period=%00"), (PROMOS, "sales_type=re%00tail"),
             (BREAKDOWN, "parent_category=a%00b"), ("/api/categories%00", ""))


@pytest.mark.asyncio
async def test_bad_input_is_the_same_400_byte_for_byte(pair):
    rows = await _compare(pair, BAD_INPUT)
    differences = REPORT.phase("bad_input", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {400}


# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "source_id malformed": [(SUMMARY, "source_id=x"), (TOP, "source_id="),
                            (PERFORMANCE, "source_id=1.5"),
                            (BREAKDOWN, "parent_category=a&source_id=5"),
                            (BY_SOURCE, "source_id=0"), (SUMMARY, "source_id=-1")],
    "source_id as pydantic reads it": [(SUMMARY, "source_id=%201"), (TOP, "source_id=4.0"),
                                       (PERFORMANCE, "source_id=0_4")],
    "category_id malformed": [(SUMMARY, "category_id=x"), (TOP, "category_id=-1"),
                              (BY_SOURCE, "category_id=0"), (SUMMARY, "category_id=")],
    "category_id as pydantic reads it": [(SUMMARY, "category_id=%201"),
                                         (TOP, "category_id=1.0")],
    "top limit malformed": [(TOP, "limit=x"), (TOP, "limit=-1"), (TOP, "limit=5.5"),
                            (TOP, "limit=")],
    "top limit as pydantic reads it": [(TOP, "limit=5.0"), (TOP, "limit=%205"),
                                       (TOP, "limit=1_0")],
    "returns limit out of bounds": [(RETURNS, "limit=0"), (RETURNS, "limit=101"),
                                    (RETURNS, "limit=x"),
                                    (RETURNS, "limit=99999999999999999999"),
                                    (RETURNS, "limit=-99999999999999999999")],
    "returns limit as pydantic reads it": [(RETURNS, "limit=5.0"), (RETURNS, "limit=%2010")],
    "brand malformed": [(SUMMARY, "brand=%3Cscript%3E"), (TOP, "brand=%20%20"),
                        (PERFORMANCE, "brand=" + "a" * 256)],
    "brand with a combining mark": [(SUMMARY, "brand=%E0%A4%95%E0%A4%BE"),
                                    (BREAKDOWN, "parent_category=a&brand=%E2%92%B6")],
    "promocode too long": [(SUMMARY, "promocode=" + "a" * 101)],
    "period malformed": [(SUMMARY, "period=decade"), (PROMOS, "period=MONTH"),
                         (BRAND_CHART, "period=%20month%20"), (TOP, "period=custom")],
    "dates malformed": [(SUMMARY, "start_date=2026-13-01&end_date=2026-13-02"),
                        (RETURNS, "start_date=2026-03-01&end_date=2026-02-01"),
                        (PERFORMANCE, "start_date=1999-01-01&end_date=1999-01-02"),
                        (BY_SOURCE, "start_date=2016-01-01&end_date=2026-10-21"),
                        (PROMOS, "start_date=2026-03-01"),
                        (BREAKDOWN,
                         "parent_category=Makeup&start_date=2026-1-5&end_date=2026-01-29")],
    "sales type unknown": [(SUMMARY, "sales_type=wholesale"), (PROMOS, "sales_type="),
                           (BRAND_CHART, "sales_type=%20ALL%20")],
    "the breakdown without its parent": [(BREAKDOWN, "period=month"),
                                         (BREAKDOWN, "source_id=x")],
    "a parent of no integer": [("/api/categories/abc/children", ""),
                               ("/api/categories/1.0/children", ""),
                               ("/api/categories/%201/children", ""),
                               ("/api/categories/1_1/children", "")],
    "another route's parameter": [(BRAND_CHART, "source_id=x&category_id=y&limit=0"),
                                  (RETURNS, "source_id=x&brand=%3C"),
                                  (PERFORMANCE, "category_id=x"), (PROMOS, "brand=%3C"),
                                  (CATEGORIES, "period=x&limit=0")],
    "a parameter repeated": [(TOP, "limit=0&limit=5"), (SUMMARY, "source_id=x&source_id=1"),
                             (BREAKDOWN, "parent_category=a&parent_category=Makeup")],
    "a NUL": [(SUMMARY, "brand=%00"), (CATEGORIES, "x=%00")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree.
DIVERGENCES: Dict[str, Divergence] = {
    "brand with a combining mark": Divergence(400, 200, (
        "validate_brand_name's \\w is str.isalnum(); Rust's char::is_alphanumeric "
        "also takes Unicode's Other_Alphabetic marks — a Devanagari vowel sign, "
        "a circled letter")),
}


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
