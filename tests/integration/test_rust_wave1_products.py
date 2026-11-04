"""Python against Rust on `/api/products/intel/*`, by contract: wave 1's
second tab.

`web.main.app` in this process under `KS_READ_PRODUCTS_INTEL=postgres`, as
production runs it, and the pilot over HTTP, on one Postgres seeded through
production's derivations (`tests/rust_wave1_fixture.py`); the machinery is
`tests/rust_wave1.py`'s.

THE CONTRACT: status, Content-Type and body, byte for byte

What the /products page's six hooks can send, read out of the TypeScript
(`tests/frontend_contract.products()`): one of the eight periods or the two
dates the picker gives; the five sales types; the header's four filters,
which these routes ignore and must ignore alike; and, on `/pairs` alone, the
`product_id` of a product a pair named — taken here from Python's own
`/pairs` answers, as the widget takes it from what it shows. Then the
shortcuts at the Kyiv midnights, the 400s a range can get, the 503 naming
`products_intel`, and the route's limit.

THE REST: the status alone, against DIVERGENCES.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_PRODUCTS_REPORT` set, the counts go there.
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
from tests.rust_wave1 import PICKER_RANGES, Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_PRODUCTS_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
CONTRACT = frontend_contract.products()
ENV = {"KS_READ_PRODUCTS_INTEL": "postgres", "KS_READ_LOOKUPS": "postgres"}
SUMMARY, PAIRS, DISTRIBUTION, COMBOS, BRANDS, MOMENTUM = CONTRACT.endpoints

# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "limit out of bounds": [(PAIRS, "limit=0"), (PAIRS, "limit=51"), (COMBOS, "limit=31"),
                            (BRANDS, "limit=-1"), (MOMENTUM, "limit=21")],
    "limit past 64 bits": [(PAIRS, "limit=99999999999999999999")],
    "limit not an integer": [(PAIRS, "limit=abc"), (MOMENTUM, "limit="), (COMBOS, "limit=5.5")],
    "limit as an integral float": [(PAIRS, "limit=5.0")],
    "limit padded": [(BRANDS, "limit=%205"), (MOMENTUM, "limit=5%20")],
    "limit with an underscore": [(COMBOS, "limit=1_0")],
    "limit signed or zero-led": [(PAIRS, "limit=%2B5"), (MOMENTUM, "limit=05")],
    "limit repeated": [(PAIRS, "limit=0&limit=5"), (PAIRS, "limit=5&limit=0")],
    "product_id not an integer": [(PAIRS, "product_id=abc"), (PAIRS, "product_id="),
                                  (PAIRS, "product_id=1.5")],
    "product_id as an integral float": [(PAIRS, "product_id=5.0")],
    "product_id padded": [(PAIRS, "product_id=%205")],
    "product_id with an underscore": [(PAIRS, "product_id=1_0")],
    "product_id signed or zero": [(PAIRS, "product_id=%2B5"), (PAIRS, "product_id=-1"),
                                  (PAIRS, "product_id=0")],
    "product_id past 64 bits": [(PAIRS, "product_id=99999999999999999999")],
    "another route's parameter": [(SUMMARY, "limit=x&product_id=y"),
                                  (DISTRIBUTION, "limit=0"), (MOMENTUM, "product_id=x")],
    "period unknown": [(SUMMARY, "period=xyz"), (PAIRS, "period=custom"), (MOMENTUM, "period=")],
    "period respelled": [(SUMMARY, "period=TODAY"), (COMBOS, "period=%20month%20")],
    "period repeated": [(BRANDS, "period=xyz&period=month"), (BRANDS, "period=month&period=xyz")],
    "no window": [(SUMMARY, "sales_type=retail"), (MOMENTUM, "")],
    "sales type unknown": [(SUMMARY, "sales_type=bogus"), (PAIRS, "sales_type=")],
    "sales type respelled": [(DISTRIBUTION, "sales_type=%20all%20"), (MOMENTUM, "sales_type=B2B")],
    "dates malformed": [(SUMMARY, "start_date=2026-02-30&end_date=2026-03-01")],
    "dates strptime forgives": [(PAIRS, "start_date=2026-1-5&end_date=2026-01-29")],
    "dates reversed": [(MOMENTUM, "start_date=2026-03-01&end_date=2026-02-01"),
                       (PAIRS, "start_date=2026-03-01&end_date=2026-02-01")],
    "one date alone": [(COMBOS, "start_date=2026-03-01")],
    "validators in their order": [(PAIRS, "period=xyz&limit=0"), (PAIRS, "sales_type=x&limit=0"),
                                  (PAIRS, "limit=0&start_date=2026-02-30&end_date=2026-03-01")],
    "header filters malformed": [(BRANDS, "source_id=abc&category_id=-1&brand=%00")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree. The page sends `product_id` as `String(productId)` of an id a pair
# returned — decimal digits — and never sends `limit`. What Python accepts
# beyond that is pydantic's lax reading of a query string.
_LAX = "pydantic's lax int reads {what}; Rust reads an integer as written"
DIVERGENCES: Dict[str, Divergence] = {
    "limit as an integral float": Divergence(200, 422, _LAX.format(what="'5.0' as 5")),
    "limit padded": Divergence(200, 422, _LAX.format(
        what="' 5' and '5 ' as 5, stripping the whitespace")),
    "limit with an underscore": Divergence(200, 422, _LAX.format(what="'1_0' as 10")),
    "limit past 64 bits": Divergence(400, 422, (
        "Python's int has no width, so validate_limit refuses it as over the "
        "cap; Rust's i64 does not parse it")),
    "product_id as an integral float": Divergence(200, 422, _LAX.format(what="'5.0' as 5")),
    "product_id padded": Divergence(200, 422, _LAX.format(what="' 5' as 5")),
    "product_id with an underscore": Divergence(200, 422, _LAX.format(what="'1_0' as 10")),
}
# Not listed since #335: a product_id past 64 bits is validate_db_int's 400
# on both sides, and BAD_INPUT compares it byte for byte.

REPORT = rust_wave1.Report()


@pytest.fixture(scope="module")
def seeded():
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


async def _pair_products(python) -> List[int]:
    """The product ids the pairs widget can be clicked on: what `/pairs`
    answers, over a short window (a threshold of one, where a line with no
    product pairs too) and a long one."""
    ids: List[int] = []
    for query in ("period=week&sales_type=retail", "period=last_28_days&sales_type=all",
                  "start_date=2025-01-01&end_date=2025-12-31&sales_type=retail",
                  *(f"start_date={day}&end_date={day}&sales_type=all"
                    for day in ("2026-10-05", "2026-10-12", "2026-10-19"))):
        answer = await python.get(f"{PAIRS}?{query}&limit=50")
        assert answer.status_code == 200, answer.text[:200]
        for row in answer.json():
            for side in ("productA", "productB"):
                if row[side]["id"] and row[side]["id"] not in ids:
                    ids.append(row[side]["id"])
    # A line with no product pairs as its line id (`COALESCE(product_id,
    # line_id)`), and the widget sends that id like any other.
    line_ids = [i for i in ids if i >= 1000]
    assert len(ids) >= 4 and line_ids, ids
    return ids[:3] + line_ids[:1]


def _contract(filters: Dict[str, list], products: List[int]) -> List[Tuple[str, str]]:
    windows = [{"period": p} for p in CONTRACT.periods]
    windows += [{"period": "custom", "start_date": s, "end_date": e} for s, e in PICKER_RANGES]
    chosen: List[Dict[str, object]] = [
        {name: value} for name, values in filters.items() for value in values]
    chosen.append({name: values[-1] for name, values in filters.items()})
    cases = []
    for path in CONTRACT.endpoints:
        for window in windows:
            for sales_type in CONTRACT.sales_types:
                cases.append((path, frontend_contract.query(CONTRACT, sales_type=sales_type,
                                                            **window)))
        for period in CONTRACT.periods:
            for header in chosen:
                cases.append((path, frontend_contract.query(
                    CONTRACT, period=period, sales_type="retail", **header)))
    for window in windows:
        for sales_type in CONTRACT.sales_types:
            for product in products:
                cases.append((PAIRS, frontend_contract.query(
                    CONTRACT, sales_type=sales_type, endpoint=PAIRS,
                    extra={"product_id": product}, **window)))
    return cases


def _has_rows(path: str, body: bytes) -> bool:
    answer = json.loads(body)
    if path == SUMMARY:
        return bool(answer["totalOrders"])
    if path == MOMENTUM:
        return bool(answer["gainers"] or answer["losers"])
    return bool(answer)


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    filters = await rust_wave1.header_filters(pair.python)
    products = await _pair_products(pair.python)
    REPORT.data["lookups"] = {**filters, "product_id": products}
    rows, with_rows = [], 0
    for path, query in _contract(filters, products):
        py, rs = await pair.ask(path, query)
        found = rust_wave1.difference(py, rs)
        rows.append((f"{path}?{query}", py.status_code, found))
        with_rows += not found and py.status_code == 200 and _has_rows(path, py.content)
    differences = REPORT.phase("contract", rows)
    REPORT.data["phases"]["contract"]["with_rows"] = with_rows
    assert not differences, "\n".join(differences[:20])
    assert {200, 400} <= {status for _, status, _ in rows}
    assert with_rows > len(rows) // 2, (with_rows, len(rows))
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
                rows.append((f"{now} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
    differences = REPORT.phase("midnights", rows)
    assert not differences, "\n".join(differences[:20])


@pytest.mark.asyncio
async def test_today_on_the_real_clock(pair):
    for _ in range(3):
        before = datetime.now(KYIV).date()
        py, rs = await pair.ask(MOMENTUM, "period=today&sales_type=all", now=None)
        if datetime.now(KYIV).date() == before:
            break
    assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    assert py.status_code == 200


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_503(pair):
    """A schema this build does not know, then the line view gone."""
    query = frontend_contract.query(CONTRACT, period="month", sales_type="retail")
    rows = []
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    breaks = (
        ("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
         f"UPDATE meta.alembic_version SET version_num = '{revision}'"),
        ("view", "ALTER VIEW silver.order_lines RENAME TO order_lines_away",
         "ALTER VIEW silver.order_lines_away RENAME TO order_lines"),
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
    differences = REPORT.phase("unreadable", rows, may_hand_over=True)
    assert not differences, "\n".join(differences)
    # Another revision is a migration window: Rust hands every request to
    # Python, whose image is the schema's. A table neither can read is the
    # same answer from both.
    assert rust_wave1.handed_over(rows) == [r for r, _, _ in rows if r.startswith("revision ")]
    assert {status for _, status, _ in rows} == {503}
    py, rs = await pair.ask(SUMMARY, query)
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_the_limit_counts_what_it_counts_on_both(pair):
    key = f"products-limit-{pair.run}"
    asks = (["limit=abc"] * 3 + ["period=xyz&sales_type=retail"] * 5
            + ["period=month&sales_type=retail"] * 26)
    py_statuses, rs_statuses = [], []
    for query in asks:
        py, rs = await pair.ask(PAIRS, query, key=key)
        py_statuses.append(py.status_code)
        rs_statuses.append(rs.status_code)
    REPORT.phase("limit", [(f"{PAIRS}?{q}", p, None if p == r else f"{p} against {r}")
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


# #335's 400s, byte for byte: an integer past its column's width, and a NUL
# anywhere in the query, which the middleware refuses before routing.
BAD_INPUT = ((PAIRS, "product_id=99999999999999999999"),
             (PAIRS, "product_id=-99999999999999999999"),
             (PAIRS, "product_id=%2B99999999999999999999"), (PAIRS, "product_id=%00"),
             (SUMMARY, "sales_type=re%00tail"), (MOMENTUM, "period=%00"))


@pytest.mark.asyncio
async def test_bad_input_is_the_same_400_byte_for_byte(pair):
    rows = []
    for path, query in BAD_INPUT:
        py, rs = await pair.ask(path, query)
        rows.append((f"{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("bad_input", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {400}
