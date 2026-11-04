"""Python against Rust on the inventory routes' ten GETs, by contract.

`web.main.app` in this process under `KS_READ_INVENTORY=postgres`, as
production runs it, and the pilot over HTTP, on one Postgres seeded through
production's derivations (`tests/rust_wave1_fixture.py`) plus the stock side:
prices, reserves and an oversold offer on `bronze.offer_stocks`, the offers
that tie them to products, a few stock-outs, `app.sku_inventory_status`
rebuilt by production's own statement, and four hundred days of
`app.inventory_history` with gaps and NULLs. Every "today" here is the
database's, so no clock is pinned.

THE CONTRACT: status, Content-Type and body, byte for byte.

What the inventory page sends (`frontend_contract.inventory_*`): the stock
cards' `limit`, the trend chart's periods and granularities, the turnover
card's five settings across the ranges its inputs allow, the three ABC
lists, and the five GETs that take no query — the restock alerts' among
them, although no component calls its hook. Then the edges the bytes still
settle — `/stocks/average`, which has no caller, among them — the tables
empty or nearly so, the ways Postgres cannot answer, and the limit.

THE REST: the status alone, against DIVERGENCES.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_INVENTORY_REPORT` set, the counts go there.
"""
from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timedelta, timezone
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
REPORT_PATH = os.getenv("KS_RUST_INVENTORY_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

ENV = {"KS_READ_INVENTORY": "postgres"}
S = "/api/stocks"
SUMMARY, AVERAGE, TREND = f"{S}/summary", f"{S}/average", f"{S}/trend"
ANALYSIS, TURNOVER, SKUS = f"{S}/analysis", f"{S}/turnover", f"{S}/skus"
BRANDS, ACTIONS, ALERTS = f"{S}/brand-rotation", f"{S}/actions", f"{S}/alerts"
STOCK_TABLES = ("bronze.offers", "app.stock_movements", "app.sku_inventory_status",
                "app.inventory_history")
# The turnover card's settings: lead time, multiplier, buffer, max days.
# The defaults, each input's ends, and lead × (multiplier − 1) on a half
# (2.5, 1.5, 3.5, 0.5), which `round` takes to the even side.
SETTINGS = ((14, 1.5, 5, 60), (5, 1.5, 0, 30), (3, 1.5, 30, 365), (7, 1.5, 5, 60),
            (1, 1.5, 5, 60), (14, 1.1, 5, 60), (14, 2.3, 10, 120), (21, 2.95, 1, 200),
            (90, 1.05, 0, 365), (10, 1.25, 7, 45), (1, 1.0, 0, 30), (90, 3.0, 30, 365))


def _offer_updates(offers, rng: random.Random) -> List[tuple]:
    """Prices, quantities and reserves for the fixture's offers: out of stock,
    low (1 to 5), plenty; a reserve on some; one offer oversold (1 in stock,
    3 reserved), one with a reserve and nothing in stock, one with no price."""
    rows = []
    for i, offer in enumerate(offers):
        price = None if i == 7 else Decimal(rng.randint(5_000, 300_000)) / 100
        quantity = (0, 0, 1, 3, 5, 6, 12, 25, 40, 80, 2, 140)[i % 12]
        reserve = (0, 0, 0, 1, 0, 2, 0)[i % 7]
        if i == 4:
            quantity, reserve = 1, 3
        if i == 9:
            quantity, reserve = 0, 1
        rows.append((offer["id"], price, quantity, reserve))
    return rows


def _history(today, stamp) -> List[tuple]:
    """Four hundred days back to today, a day missing in every seventeen, a
    NULL reserve or SKU count now and then."""
    rows = []
    for back in range(400, -1, -1):
        if back % 17 == 5:
            continue
        quantity = 2_000 + (back * 37) % 900 + back % 2
        value = Decimal(quantity * 615 + (back * 7919) % 100_000) / 100
        rows.append((today - timedelta(days=back), quantity, value,
                     None if back % 23 == 0 else (back * 13) % 70,
                     None if back % 29 == 0 else 300 + back % 40, stamp))
    return rows


async def _seed(pool) -> Dict[str, object]:
    from core.sql_dialect import POSTGRES, sku_status_rebuild_select
    from tests import rust_wave1_fixture

    seed = await rust_wave1_fixture.seed(pool)
    rng = random.Random(20261029)
    stamp = datetime(2026, 10, 1, tzinfo=timezone.utc)
    async with pool.acquire() as conn:
        for table in STOCK_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        offers = await conn.fetch("SELECT id, sku FROM bronze.offer_stocks ORDER BY id")
        await conn.executemany(
            "UPDATE bronze.offer_stocks SET price = $2, quantity = $3, reserve = $4 WHERE id = $1",
            _offer_updates(offers, rng))
        # The last product with a SKU wins it, so the shared SKU maps one way;
        # one offer with a SKU is left without an offers row (product 0).
        product_of = {r["sku"]: r["id"] for r in await conn.fetch(
            "SELECT id, sku FROM bronze.products WHERE sku IS NOT NULL ORDER BY id DESC")}
        links = [(o["id"], product_of[o["sku"]], o["sku"], stamp) for o in offers
                 if o["sku"] in product_of and o["id"] != 3]
        await conn.copy_records_to_table(
            "offers", schema_name="bronze", columns=("id", "product_id", "sku", "synced_at"),
            records=links)
        outs = [(o["id"], product_of.get(o["sku"]), "stock_out", 2, 0, -2, 0, 0,
                 stamp - timedelta(days=11 * n), "sync") for n, o in enumerate(offers[::9])]
        await conn.copy_records_to_table(
            "stock_movements", schema_name="app",
            columns=("offer_id", "product_id", "movement_type", "quantity_before",
                     "quantity_after", "delta", "reserve_before", "reserve_after",
                     "recorded_at", "source"), records=outs)
        async with conn.transaction():
            await conn.execute("CREATE TEMP TABLE _tmp_first_seen ON COMMIT DROP AS "
                               "SELECT offer_id, first_seen_at FROM app.sku_inventory_status")
            await conn.execute(sku_status_rebuild_select(POSTGRES))
        today = await conn.fetchval("SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
        history = _history(today, stamp)
        await conn.copy_records_to_table(
            "inventory_history", schema_name="app",
            columns=("date", "total_quantity", "total_value", "total_reserve", "sku_count",
                     "recorded_at"), records=history)
        statuses = await conn.fetchval("SELECT COUNT(*) FROM app.sku_inventory_status")
    return {**seed, "sku_statuses": statuses, "history_days": len(history),
            "offer_links": len(links), "stock_outs": len(outs)}


async def _clear(pool) -> None:
    from tests import rust_wave1_fixture

    async with pool.acquire() as conn:
        for table in STOCK_TABLES:
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


def _contract() -> List[Tuple[str, str]]:
    cases = [(SUMMARY, f"limit={fc.inventory_stock_summary_limit()}")]
    days, granularities = fc.inventory_trend_options()
    cases += [(TREND, f"days={d}&granularity={g}") for d in days for g in granularities]
    options = fc.inventory_turnover_options()
    for d in options["days"]:
        for lead, safety, buffer, max_days in SETTINGS:
            for name, value in (("lead_time", lead), ("safety_multiplier", safety),
                                ("buffer_days", buffer), ("max_acceptable_days", max_days)):
                lo, hi = options[name]
                assert lo <= value <= hi, (name, value, "outside what the page sends")
            cases.append((TURNOVER, fc.inventory_turnover_query(
                days=d, lead_time=lead, safety_multiplier=safety, buffer_days=buffer,
                max_acceptable_days=max_days)))
    cases += [(f"{S}/abc/{c}", "") for c in fc.inventory_abc_classes()]
    cases += [(path, "") for path in fc.inventory_bare_paths()]
    return cases


async def _compare(pair, asks, prefix: str = "") -> List[Tuple[str, int, object]]:
    rows = []
    for path, query in asks:
        py, rs = await pair.ask(path, query, now=None)
        rows.append((f"{prefix}{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    return rows


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    import json

    from core import duckdb_switch, read_fallback

    rows = await _compare(pair, _contract())
    differences = REPORT.phase("contract", rows)
    assert not differences, "\n".join(differences[:20])
    assert {status for _, status, _ in rows} == {200}
    # The seed reaches what the arithmetic branches on.
    py, _ = await pair.ask(SKUS, "", now=None)
    items = json.loads(py.content)
    decisions = sorted({item["decision"] for item in items})
    tiers = sorted({item["velocityTier"] for item in items})
    REPORT.data["phases"]["contract"].update(decisions=decisions, tiers=tiers, skus=len(items))
    assert len(items) > 20 and len(decisions) >= 2, (decisions, tiers)
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


EDGES: Tuple[Tuple[str, str], ...] = (
    (SUMMARY, ""), (SUMMARY, "limit=5"), (SUMMARY, "limit=50"),
    (AVERAGE, ""), *((AVERAGE, f"days={d}") for d in (7, 30, 31, 90, 180, 365)),
    (TREND, ""), *((TREND, f"days={d}&granularity={g}") for d in (7, 365)
                   for g in ("daily", "monthly")),
    (TREND, "granularity=weekly"), (TREND, "granularity="), (TREND, "granularity=MONTHLY"),
    (TURNOVER, ""), (TURNOVER, "days=7"), (TURNOVER, "days=90&safety_multiplier=1"),
    *((BRANDS, f"min_skus={n}") for n in (1, 2, 3, 5, 20)),
    (f"{S}/abc/a", ""), (f"{S}/abc/b", "limit=1"), (f"{S}/abc/c", "limit=200"),
    (f"{S}/abc/%41", "limit=7"),
    *((path, f"carrying_rate={cr}&liquidation_discount={ld}") for path in (ANALYSIS, SKUS)
      for cr, ld in (("0.05", "0.2"), ("0.5", "0.8"), ("0.333", "0.47"), ("2.5e-1", "5E-1"),
                     ("%200.3%20", "0.6"))),
)


@pytest.mark.asyncio
async def test_the_edges_answer_byte_for_byte(pair):
    rows = await _compare(pair, EDGES)
    differences = REPORT.phase("edges", rows)
    assert not differences, "\n".join(differences[:20])
    assert {status for _, status, _ in rows} == {200}


EVERY_ROUTE: Tuple[Tuple[str, str], ...] = (
    (SUMMARY, "limit=15"), (AVERAGE, ""), (TREND, "days=30&granularity=daily"),
    (TREND, "days=365&granularity=monthly"), (ANALYSIS, ""), (TURNOVER, ""), (SKUS, ""),
    (BRANDS, ""), (f"{S}/abc/A", ""), (f"{S}/abc/C", ""), (ACTIONS, ""), (ALERTS, ""),
)


async def _swap(conn, table: str, rows: List[tuple]) -> List[tuple]:
    """Replace `table`'s rows, returning what was there."""
    columns = [r["attname"] for r in await conn.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 "
        "AND NOT attisdropped ORDER BY attnum", table)]
    before = [tuple(r) for r in await conn.fetch(f"SELECT {', '.join(columns)} FROM {table}")]
    await conn.execute(f"DELETE FROM {table}")
    if rows:
        schema, name = table.split(".")
        await conn.copy_records_to_table(name, schema_name=schema,
                                         columns=columns[:len(rows[0])], records=rows)
    return before


@pytest.mark.asyncio
async def test_empty_and_thin_tables_answer_byte_for_byte(pair):
    """No stock at all, then the history alone thinned: empty, one day, a
    first day of nothing, and months whose averages fall on a half."""
    async with pair.pool.acquire() as conn:
        today = await conn.fetchval("SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
    stamp = datetime(2026, 10, 1, tzinfo=timezone.utc)
    halves = [(today - timedelta(days=back), quantity, Decimal(value), reserve, None, stamp)
              for back, quantity, value, reserve in (
                  (70, 3, "0.01", 1), (69, 4, "0.02", 2), (40, 2, "100.01", None),
                  (39, 3, "100.00", None), (20, 7, "100.01", 5), (0, 9, "100.02", 6))]
    thin = {
        "history empty": [],
        "one day": [(today, 5, Decimal("100.50"), 1, 2, stamp)],
        "first day nothing": [(today - timedelta(days=3), 0, Decimal("0.00"), 0, 0, stamp),
                              (today, 4, Decimal("80.00"), 0, 4, stamp)],
        "halves": halves,
    }
    rows = []
    async with pair.pool.acquire() as conn:
        stock = {t: await _swap(conn, t, []) for t in ("app.sku_inventory_status",
                                                       "bronze.offer_stocks",
                                                       "app.inventory_history")}
    try:
        rows += await _compare(pair, EVERY_ROUTE, "no stock ")
    finally:
        async with pair.pool.acquire() as conn:
            for table, before in stock.items():
                await _swap(conn, table, before)
    for name, history in thin.items():
        async with pair.pool.acquire() as conn:
            before = await _swap(conn, "app.inventory_history", history)
        try:
            rows += await _compare(pair, ((SUMMARY, "limit=15"), (AVERAGE, ""),
                                          (AVERAGE, "days=365"),
                                          (TREND, "days=90&granularity=daily"),
                                          (TREND, "days=90&granularity=monthly")),
                                   f"{name} ")
        finally:
            async with pair.pool.acquire() as conn:
                await _swap(conn, "app.inventory_history", before)
    differences = REPORT.phase("empty_and_thin", rows)
    assert not differences, "\n".join(differences[:20])
    assert {status for _, status, _ in rows} == {200}


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_failure(pair):
    """A schema this build does not know, then each object a route reads by
    name gone: a refusal naming `inventory` where a read falls back, and
    nothing where the route does not read it."""
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    breaks = [("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
               f"UPDATE meta.alembic_version SET version_num = '{revision}'")]
    for kind, name in (("TABLE", "app.inventory_history"), ("TABLE", "app.sku_inventory_status"),
                       ("TABLE", "bronze.offer_stocks"), ("VIEW", "gold.v_sku_dead_stock_v2"),
                       ("VIEW", "gold.v_restock_alerts")):
        schema, short = name.split(".")
        breaks.append((name, f"ALTER {kind} {name} RENAME TO {short}_away",
                       f"ALTER {kind} {schema}.{short}_away RENAME TO {short}"))
    rows, statuses = [], {}
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        try:
            for path, query in EVERY_ROUTE:
                py, rs = await pair.ask(path, query, now=None)
                rows.append((f"{name} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
                statuses.setdefault(name, []).append(py.status_code)
        finally:
            async with pair.pool.acquire() as conn:
                await conn.execute(mended)
    differences = REPORT.phase("unreadable", rows, may_hand_over=True)
    REPORT.data["unreadable_statuses"] = statuses
    assert not differences, "\n".join(differences)
    # Another revision is a migration window: Rust hands every request to
    # Python, whose image is the schema's. An object neither can read is the
    # same answer from both.
    assert rust_wave1.handed_over(rows) == [r for r, _, _ in rows if r.startswith("revision ")]
    assert set(statuses["revision"]) == {503}
    assert {200, 503} <= set(statuses["gold.v_restock_alerts"])
    py, rs = await pair.ask(SUMMARY, "limit=15", now=None)
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_each_limit_counts_what_it_counts_on_both(pair):
    """30 a minute on every route: a 422 is never counted, a 400 the route
    raises always is."""
    plans = (
        (SUMMARY, ["limit=x"] * 3 + ["limit=15"] * 31),
        (f"{S}/abc/D", ["limit=0"] * 3 + [""] * 31),
        (TURNOVER, ["safety_multiplier=x"] * 3 + [""] * 31),
    )
    rows = []
    for plan, (path, asks) in enumerate(plans):
        key = f"inventory-limit-{pair.run}-{plan}"
        py_statuses, rs_statuses = [], []
        for query in asks:
            py, rs = await pair.ask(path, query, now=None, key=key)
            py_statuses.append(py.status_code)
            rs_statuses.append(rs.status_code)
            rows.append((f"{path}?{query}", py.status_code,
                         None if py.status_code == rs.status_code
                         else f"{py.status_code} against {rs.status_code}"))
        answered = 400 if path.endswith("/D") else 200
        assert py_statuses == [422] * 3 + [answered] * 30 + [429], (path, py_statuses)
        assert rs_statuses == py_statuses, path
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    REPORT.phase("limit", rows)


# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "limit not an integer": [(SUMMARY, "limit=abc"), (SUMMARY, "limit=4.5"),
                             (SUMMARY, "limit="), (f"{S}/abc/A", "limit=x")],
    "limit out of bounds": [(SUMMARY, "limit=4"), (SUMMARY, "limit=51"),
                            (SUMMARY, "limit=-1"), (f"{S}/abc/A", "limit=0"),
                            (f"{S}/abc/A", "limit=201")],
    "limit as pydantic reads it": [(SUMMARY, "limit=15.0"), (SUMMARY, "limit=%2015"),
                                   (SUMMARY, "limit=1_5")],
    "days out of bounds": [(AVERAGE, "days=6"), (AVERAGE, "days=366"), (TREND, "days=0"),
                           (TURNOVER, "days=91"), (TURNOVER, "days=6")],
    "days past 64 bits": [(AVERAGE, "days=99999999999999999999"),
                          (TURNOVER, "days=-99999999999999999999")],
    "granularity anything": [(TREND, "granularity=%00"), (TREND, "granularity=yearly"),
                             (TREND, "granularity=monthly%20")],
    "a float unparsed": [(ANALYSIS, "carrying_rate=abc"), (ANALYSIS, "carrying_rate=0,25"),
                         (ANALYSIS, "carrying_rate=0x1p-2"), (ANALYSIS, "carrying_rate="),
                         (SKUS, "liquidation_discount=1e"), (SKUS, "liquidation_discount=..5")],
    "a float out of bounds": [(ANALYSIS, "carrying_rate=0.04"), (ANALYSIS, "carrying_rate=0.51"),
                              (ANALYSIS, "carrying_rate=nan"), (SKUS, "carrying_rate=inf"),
                              (SKUS, "carrying_rate=-inf"), (SKUS, "carrying_rate=1e400"),
                              (ANALYSIS, "liquidation_discount=0.19"),
                              (SKUS, "liquidation_discount=0.81")],
    "a float as pydantic reads it": [(ANALYSIS, "carrying_rate=0.2_5"),
                                     (ANALYSIS, "carrying_rate=_0.25"),
                                     (ANALYSIS, "carrying_rate=0.25_"),
                                     (SKUS, "carrying_rate=%2B0.25"), (SKUS, "carrying_rate=.25"),
                                     (SKUS, "carrying_rate=25e-2"),
                                     (SKUS, "liquidation_discount=0.500000000000000000001"),
                                     (SKUS, "liquidation_discount=%090.5%0A")],
    "safety multiplier": [(TURNOVER, "safety_multiplier=0.99"),
                          (TURNOVER, "safety_multiplier=3.01"),
                          (TURNOVER, "safety_multiplier=nan"), (TURNOVER, "safety_multiplier=x"),
                          (TURNOVER, "safety_multiplier=1_5"), (TURNOVER, "safety_multiplier=2_0")],
    "turnover integers": [(TURNOVER, "lead_time=0"), (TURNOVER, "lead_time=91"),
                          (TURNOVER, "lead_time=x"), (TURNOVER, "buffer_days=-1"),
                          (TURNOVER, "buffer_days=31"), (TURNOVER, "max_acceptable_days=29"),
                          (TURNOVER, "max_acceptable_days=366"),
                          (TURNOVER, "lead_time=0&max_acceptable_days=x&safety_multiplier=9")],
    "min_skus": [(BRANDS, "min_skus=0"), (BRANDS, "min_skus=21"), (BRANDS, "min_skus=x"),
                 (BRANDS, "min_skus=1.0")],
    "abc class unknown": [(f"{S}/abc/D", ""), (f"{S}/abc/ab", ""), (f"{S}/abc/%C3%9F", ""),
                          (f"{S}/abc/%EF%BC%A1", ""), (f"{S}/abc/a%00", ""),
                          (f"{S}/abc/%20A", ""), (f"{S}/abc/D", "limit=0")],
    "another route's parameter": [(ACTIONS, "limit=x"), (ALERTS, "days=0"),
                                  (SUMMARY, "granularity=x&carrying_rate=9"),
                                  (ANALYSIS, "limit=0&days=1")],
    "a parameter repeated": [(SUMMARY, "limit=4&limit=15"),
                             (ANALYSIS, "carrying_rate=9&carrying_rate=0.3"),
                             (TURNOVER, "days=30&days=900")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree. Nothing so far: every parameter here is bounded, a float is read by
# the parser pydantic-core itself uses, and no value reaches SQL unchecked.
DIVERGENCES: Dict[str, Divergence] = {}


@pytest.mark.asyncio
async def test_hostile_input_differs_only_where_listed(pair):
    answers = []
    for kind, requests in HOSTILE.items():
        for path, query in requests:
            py, rs = await pair.ask(path, query, now=None)
            answers.append((kind, f"{path}?{query}", py.status_code, rs.status_code))
    REPORT.phase("hostile", [(request, py, None if py == rs else f"{py} against {rs}")
                             for _, request, py, rs in answers])
    REPORT.data["divergences"] = {kind: {"python": d.python, "rust": d.rust, "why": d.why}
                                  for kind, d in DIVERGENCES.items()}
    unlisted, stale = rust_wave1.judge_hostile(answers, DIVERGENCES)
    assert not unlisted, "differences no entry lists:\n" + "\n".join(unlisted)
    assert not stale, f"entries that no longer hold: {stale}"


def test_the_pilot_times_out_where_python_does():
    """`SLOW_ENDPOINTS` — `/api/stocks/analysis` among them — wait 300 s
    before the 504, everything else 30: the pilot's list is Python's."""
    import re
    from pathlib import Path

    from web.middleware import DEFAULT_REQUEST_TIMEOUT, SLOW_ENDPOINT_TIMEOUT, SLOW_ENDPOINTS

    source = (Path(__file__).resolve().parents[2]
              / "rust" / "crates" / "ks-api" / "src" / "http.rs").read_text(encoding="utf-8")
    block = re.search(r"const SLOW_ENDPOINTS: \[&str; \d+\] = \[(.*?)\];", source, re.S)
    assert block, "http.rs: no SLOW_ENDPOINTS"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == SLOW_ENDPOINTS
    assert "/api/stocks/analysis" in SLOW_ENDPOINTS
    assert f"Request exceeded {SLOW_ENDPOINT_TIMEOUT}s timeout" in source
    assert f"Request exceeded {DEFAULT_REQUEST_TIMEOUT}s timeout" in source
    assert re.search(rf"SLOW_REQUEST_TIMEOUT: Duration = Duration::from_secs\("
                     rf"{int(SLOW_ENDPOINT_TIMEOUT)}\)", source)
