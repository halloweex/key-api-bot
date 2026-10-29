"""Python against Rust on the traffic routes' seven GETs, by contract.

`web.main.app` in this process under `KS_READ_TRAFFIC=postgres`, as production
runs it, and the pilot over HTTP, on one Postgres seeded through production's
derivations (`tests/rust_wave1_fixture.py`) plus what the tab reads on top:
`silver.order_utm` for most orders — every verdict and platform, tags in
mixed case, padded, Cyrillic, a `Campaign #N` with a dotted capital I, the
pixels' cookies — and marketing spend in `app.manual_expenses` for the ROAS.

THE CONTRACT: status, Content-Type and body, byte for byte.

What the page sends (`frontend_contract.traffic()`): the header's period or
dates, sales type and source to all seven; the two tables' filters, sort and
pages of 50 on top. Then edges, the tab's tables emptied, the ways Postgres
cannot answer, the limit, and #335's 400s byte for byte.

THE REST: the status alone, against DIVERGENCES.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_TRAFFIC_REPORT` set, the counts go there.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Dict, List, Tuple

import pytest
import pytest_asyncio

from tests import frontend_contract as fc
from tests import rust_wave1
from tests.rust_wave1 import Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_TRAFFIC_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

CONTRACT = fc.traffic()
OPTIONS = fc.traffic_table_options()
ENV = {"KS_READ_TRAFFIC": "postgres", "KS_READ_GOLD": "postgres"}
(ANALYTICS, ORGANIC, PIXEL, TREND, ROAS, TRANSACTIONS, CAMPAIGNS) = CONTRACT.endpoints
BARE = (ANALYTICS, ORGANIC, PIXEL, TREND, ROAS)
TABLES = ("silver.order_utm", "app.manual_expenses")
# Tags as the parser leaves them: (traffic_type, platform, utm_source,
# utm_medium, utm_campaign, utm_content, fbp, fbc, ttp, fbclid).
TAGS = (
    ("paid_confirmed", "facebook", "facebook", "cpc", "Spring Sale", "ad_1",
     "fb.1.a", "fb.1.c", None, "abc"),
    ("paid_likely", "google", "google", "cpc", "12345", None, None, None, None, None),
    ("organic", "google", "Google", " Organic ", None, None, None, None, None, None),
    ("organic", "instagram", " IG ", "Stories_blogger", "Блогер Аня", "code1",
     None, None, None, None),
    ("organic", "instagram", "ig", "social", "profile", None, "fb.1.p", None, None, None),
    ("organic", "email", "klaviyo", "Klaviyo", "Welcome", None, None, None, None, None),
    ("organic", "email", "newsletter", "email", "Campaign #42", None, None, None, None, None),
    ("organic", "email", "rivo", "email", "loyalty flow", None, None, None, None, None),
    ("organic", "email", "shopify", "email", "CAMPAİGN #7", None, None, None, None, None),
    ("organic", "telegram", "telegram", "channel", "post 5", None, None, None, None, None),
    ("organic", "ai", "chatgpt.com", "referral", None, None, None, None, None, None),
    ("organic", "facebook", "facebook", "social", None, None, None, None, None, None),
    ("organic", "tiktok", "TikTok", "bio", "", None, None, None, "ttp0", None),
    ("organic", "other", "qr", "print", "Flyer", "", None, None, None, None),
    ("manager", "manager", None, None, None, None, None, None, None, None),
    ("pixel_only", "unattributed", None, None, None, None, "fb.1.only", None, "ttp1", None),
    ("pixel_only", "unattributed", None, None, None, None, None, None, "ttp2", None),
    ("unknown", "other", "novaposhta", "sms", None, None, None, None, None, None),
    ("paid_confirmed", "tiktok", "tiktok", "paid", "TT Launch", None, None, None, "ttp3", None),
    ("paid_likely", "instagram", "instagram", "paid", "Reels", None, None, None, None, None),
)
# Marketing spend by platform, a row in another category, one with no
# platform: (date, category, amount, platform).
SPEND = (
    ("2026-10-02", "marketing", "12000.50", "facebook"),
    ("2026-10-09", "marketing", "8300.00", "facebook"),
    ("2026-10-05", "marketing", "4100.25", "google"),
    ("2026-09-15", "marketing", "9999.99", "tiktok"),
    ("2026-10-06", "logistics", "500.00", "facebook"),
    ("2026-10-07", "marketing", "700.00", None),
    ("2025-10-20", "marketing", "31000.00", "facebook"),
)


async def _seed(pool) -> Dict[str, object]:
    from tests import rust_wave1_fixture

    seed = await rust_wave1_fixture.seed(pool)
    stamp = datetime(2026, 10, 1, tzinfo=timezone.utc)
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
        ids = [r["id"] for r in await conn.fetch("SELECT id FROM silver.orders ORDER BY id")]
        # One order in eleven has no row at all: the readers' COALESCE
        # places it by its source.
        rows = [(oid, *TAGS[oid % len(TAGS)][2:6], *TAGS[oid % len(TAGS)][6:10],
                 TAGS[oid % len(TAGS)][0], TAGS[oid % len(TAGS)][1], stamp, stamp)
                for oid in ids if oid % 11]
        await conn.copy_records_to_table(
            "order_utm", schema_name="silver",
            columns=("order_id", "utm_source", "utm_medium", "utm_campaign", "utm_content",
                     "fbp", "fbc", "ttp", "fbclid", "traffic_type", "platform",
                     "parsed_at", "mirrored_at"), records=rows)
        spend = [(i, date.fromisoformat(d), category, "ads", Decimal(amount), "UAH", None,
                  stamp, stamp, platform, stamp)
                 for i, (d, category, amount, platform) in enumerate(SPEND, 1)]
        await conn.copy_records_to_table(
            "manual_expenses", schema_name="app",
            columns=("id", "expense_date", "category", "expense_type", "amount", "currency",
                     "note", "created_at", "updated_at", "platform", "mirrored_at"),
            records=spend)
    return {**seed, "utm_rows": len(rows), "spend_rows": len(spend)}


async def _clear(pool) -> None:
    from tests import rust_wave1_fixture

    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
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


def q(sales_type: str = "retail", endpoint=None, extra=None, **window) -> str:
    return fc.query(CONTRACT, sales_type=sales_type, endpoint=endpoint, extra=extra, **window)


def _table(endpoint: str, **chosen) -> str:
    extra = {"limit": OPTIONS["limit"][0], "offset": chosen.pop("offset", 0), **chosen}
    if endpoint == CAMPAIGNS:
        extra.setdefault("sort_by", "revenue")
        extra.setdefault("sort_dir", "desc")
    return q(endpoint=endpoint, extra=extra, period="month")


def _contract() -> List[Tuple[str, str]]:
    windows = [{"period": p} for p in CONTRACT.periods]
    windows += [{"period": "custom", "start_date": s, "end_date": e}
                for s, e in rust_wave1.PICKER_RANGES[:7]]
    cases = []
    for path in BARE:
        for window in windows:
            for sales_type in CONTRACT.sales_types:
                cases.append((path, q(sales_type, **window)))
        for source in fc.sources():
            cases.append((path, q("retail", source_id=source, period="month")))
            cases.append((path, q("all", source_id=source, period="last_month")))
    for path in (TRANSACTIONS, CAMPAIGNS):
        for offset in (0, 50, 100):
            cases.append((path, _table(path, offset=offset)))
        for traffic_type in OPTIONS["traffic_type"]:
            cases.append((path, _table(path, traffic_type=traffic_type)))
        for platform in OPTIONS["platform"]:
            cases.append((path, _table(path, platform=platform)))
        cases.append((path, _table(path, traffic_type="organic", platform="instagram")))
    for sort_by in OPTIONS["sort_by"]:
        for sort_dir in OPTIONS["sort_dir"]:
            cases.append((CAMPAIGNS, _table(CAMPAIGNS, sort_by=sort_by, sort_dir=sort_dir)))
    return cases


async def _compare(pair, asks, prefix: str = "") -> List[Tuple[str, int, object]]:
    rows = []
    for path, query in asks:
        py, rs = await pair.ask(path, query)
        rows.append((f"{prefix}{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    return rows


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    rows = await _compare(pair, _contract())
    differences = REPORT.phase("contract", rows)
    assert not differences, "\n".join(differences[:20])
    assert {status for _, status, _ in rows} == {200}
    # The seed reaches what the arithmetic branches on.
    py, _ = await pair.ask(ORGANIC, q("retail", period="custom",
                                      start_date="2025-01-01", end_date="2026-10-21"))
    channels = sorted(c["channel"] for c in json.loads(py.content)["channels"])
    py, _ = await pair.ask(PIXEL, q("all", period="custom",
                                    start_date="2024-01-01", end_date="2026-10-21"))
    firsts = sorted({c["group"] for c in json.loads(py.content)["first_channels"]})
    REPORT.data["phases"]["contract"].update(channels=channels, first_groups=firsts)
    assert len(channels) >= 10 and {"organic", "paid"} <= set(firsts), (channels, firsts)
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


EDGES: Tuple[Tuple[str, str], ...] = (
    *((path, q("all", period="custom", start_date="2030-01-01", end_date="2030-01-31"))
      for path in BARE),
    *((path, q("b2b", period="custom", start_date="2026-03-29", end_date="2026-03-29"))
      for path in BARE),
    *((path, "") for path in (*BARE, TRANSACTIONS, CAMPAIGNS)),
    (TRANSACTIONS, "limit=1&offset=0&period=year"), (TRANSACTIONS, "limit=200&offset=999999"),
    (CAMPAIGNS, "limit=200&sort_by=campaign&sort_dir=asc&period=year"),
    (CAMPAIGNS, "traffic_type=&platform=&sort_by=orders&sort_dir=asc"),
    (ORGANIC, q("retail", period="custom", start_date="2024-02-29", end_date="2024-02-29")),
)


@pytest.mark.asyncio
async def test_the_edges_answer_byte_for_byte(pair):
    rows = await _compare(pair, EDGES)
    differences = REPORT.phase("edges", rows)
    assert not differences, "\n".join(differences[:20])


EVERY_ROUTE: Tuple[Tuple[str, str], ...] = (
    *((path, q("retail", period="custom", start_date="2026-01-01", end_date="2026-10-21"))
      for path in BARE),
    (TRANSACTIONS, _table(TRANSACTIONS)), (CAMPAIGNS, _table(CAMPAIGNS)),
)


@pytest.mark.asyncio
async def test_without_tags_or_spend_the_bytes_still_agree(pair):
    """No order classified — every one placed by the COALESCE on its source —
    and no spend: the zeros and the fallbacks."""
    async with pair.pool.acquire() as conn:
        saved = {t: [tuple(r) for r in await conn.fetch(f"SELECT * FROM {t}")] for t in TABLES}
        columns = {t: [r["attname"] for r in await conn.fetch(
            "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 "
            "AND NOT attisdropped ORDER BY attnum", t)] for t in TABLES}
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
    try:
        rows = await _compare(pair, EVERY_ROUTE, "no tags ")
    finally:
        async with pair.pool.acquire() as conn:
            for table in TABLES:
                schema, name = table.split(".")
                await conn.copy_records_to_table(name, schema_name=schema,
                                                 columns=columns[table], records=saved[table])
    differences = REPORT.phase("empty", rows)
    assert not differences, "\n".join(differences[:20])
    assert {status for _, status, _ in rows} == {200}


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_failure(pair):
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    breaks = [("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
               f"UPDATE meta.alembic_version SET version_num = '{revision}'")]
    for name in ("silver.order_utm", "app.manual_expenses", "gold.daily_revenue"):
        schema, short = name.split(".")
        breaks.append((name, f"ALTER TABLE {name} RENAME TO {short}_away",
                       f"ALTER TABLE {schema}.{short}_away RENAME TO {short}"))
    rows, statuses = [], {}
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        try:
            for path, query in EVERY_ROUTE:
                py, rs = await pair.ask(path, query)
                rows.append((f"{name} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
                statuses.setdefault(name, []).append(py.status_code)
        finally:
            async with pair.pool.acquire() as conn:
                await conn.execute(mended)
    differences = REPORT.phase("unreadable", rows)
    REPORT.data["unreadable_statuses"] = statuses
    assert not differences, "\n".join(differences)
    assert set(statuses["revision"]) == {503}
    assert statuses["app.manual_expenses"].count(503) == 1


@pytest.mark.asyncio
async def test_each_limit_counts_what_it_counts_on_both(pair):
    """30 a minute: a 422 is never counted, a 400 the route raises always is."""
    plans = (
        (ANALYTICS, ["source_id=x"] * 3 + ["period=month"] * 31),
        (TRANSACTIONS, ["limit=0"] * 3 + ["traffic_type=bogus"] * 4 + ["period=week"] * 27),
    )
    rows = []
    for plan, (path, asks) in enumerate(plans):
        key = f"traffic-limit-{pair.run}-{plan}"
        py_statuses, rs_statuses = [], []
        for query in asks:
            py, rs = await pair.ask(path, query, key=key)
            py_statuses.append(py.status_code)
            rs_statuses.append(rs.status_code)
            rows.append((f"{path}?{query}", py.status_code,
                         None if py.status_code == rs.status_code
                         else f"{py.status_code} against {rs.status_code}"))
        refused = sum(1 for s in py_statuses if s == 400)
        assert py_statuses == ([422] * 3 + [400] * refused + [200] * (30 - refused) + [429]), (
            path, py_statuses)
        assert rs_statuses == py_statuses, path
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    REPORT.phase("limit", rows)


# #335's 400s and the routes' own, byte for byte: an offset and a source past
# 64 bits, the tables' filters and sort refused by name, and a NUL anywhere.
BAD_INPUT = ((TRANSACTIONS, "offset=99999999999999999999"),
             (CAMPAIGNS, "offset=%2B99999999999999999999&sort_by=x"),
             (ANALYTICS, "source_id=99999999999999999999"),
             (ORGANIC, "source_id=-99999999999999999999"),
             (TRANSACTIONS, "traffic_type=paid"), (TRANSACTIONS, "platform=Google"),
             (CAMPAIGNS, "sort_by=Revenue"), (CAMPAIGNS, "sort_dir=ASC"),
             (CAMPAIGNS, "traffic_type=x&sort_by=y"), (PIXEL, "period=%00"),
             (ROAS, "sales_type=re%00tail"), (TREND, "x=%00"))


@pytest.mark.asyncio
async def test_bad_input_is_the_same_400_byte_for_byte(pair):
    rows = await _compare(pair, BAD_INPUT)
    differences = REPORT.phase("bad_input", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {400}


# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "source_id malformed": [(ANALYTICS, "source_id=x"), (TREND, "source_id="),
                            (ORGANIC, "source_id=1.5"), (PIXEL, "source_id=3"),
                            (TRANSACTIONS, "source_id=0"), (CAMPAIGNS, "source_id=-1")],
    "source_id as pydantic reads it": [(ANALYTICS, "source_id=%201"), (TREND, "source_id=4.0"),
                                       (ORGANIC, "source_id=0_4")],
    "source_id on the ROAS, which takes none": [(ROAS, "source_id=x"), (ROAS, "source_id=9")],
    "period malformed": [(ANALYTICS, "period=decade"), (ROAS, "period=MONTH"),
                         (TREND, "start_date=2026-13-01&end_date=2026-13-02"),
                         (ORGANIC, "start_date=1999-01-01&end_date=1999-01-02"),
                         (PIXEL, "start_date=2026-10-21&end_date=2026-10-01")],
    "sales type unknown": [(ANALYTICS, "sales_type=wholesale"), (ROAS, "sales_type=")],
    "limit and offset out of bounds": [(TRANSACTIONS, "limit=0"), (TRANSACTIONS, "limit=201"),
                                       (CAMPAIGNS, "offset=-1"), (CAMPAIGNS, "limit=abc"),
                                       (TRANSACTIONS, "limit=99999999999999999999"),
                                       (TRANSACTIONS, "offset=-99999999999999999999")],
    "limit and offset as pydantic reads them": [(TRANSACTIONS, "limit=5.0"),
                                                (CAMPAIGNS, "offset=%2010"),
                                                (TRANSACTIONS, "limit=1_0")],
    "the tables' filters unknown": [(TRANSACTIONS, "traffic_type=Organic"),
                                    (CAMPAIGNS, "platform=x"), (CAMPAIGNS, "sort_by="),
                                    (CAMPAIGNS, "sort_dir=up")],
    "another route's parameter": [(ANALYTICS, "traffic_type=x&sort_by=y&limit=0"),
                                  (ROAS, "offset=-1"), (TRANSACTIONS, "sort_by=x")],
    "a parameter repeated": [(TRANSACTIONS, "limit=0&limit=5"),
                             (CAMPAIGNS, "sort_dir=x&sort_dir=asc")],
    "a NUL": [(ANALYTICS, "period=%00"), (CAMPAIGNS, "sort_by=re%00venue")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree. Nothing so far: the integers are read as pydantic reads them and held
# to their columns as #335 holds them, and the texts are checked by name.
DIVERGENCES: Dict[str, Divergence] = {}


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
