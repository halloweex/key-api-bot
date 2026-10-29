"""Python against Rust on the goal routes' seven GETs, by contract.

`web.main.app` in this process under `KS_READ_GOALS=postgres`, as production
runs it, and the pilot over HTTP, on one Postgres seeded through
production's derivations (`tests/rust_wave1_fixture.py`) plus what the goals
read on top: stored goals, a forecast, and the three shared goal tables.
The machinery is `tests/rust_wave1.py`'s; the goals module's own clock
(`core.repositories.goals.datetime`) is pinned beside `core.filters`'.

THE CONTRACT: status, Content-Type and body, byte for byte.

What `api/client.ts` sends (`frontend_contract.goal_queries()`): the sales
type to four routes; the history's period type, four weeks and the sales
type; a smart goal for this month or a given one; a forecast for a given
month with `recalculate=false` — and with `true`, a 400 for everybody.
Then the Kyiv midnights, edges the bytes still settle, `KS_GOALS_HISTORY`
under values that would once have refused, the ways Postgres cannot answer,
and the three limits.

THE REST: the status alone, against DIVERGENCES.

Skipped without the pilot or a Postgres; `.github/workflows/rust-pilot.yml`
runs it with both. With `KS_RUST_GOALS_REPORT` set, the counts go there.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests import frontend_contract as fc
from tests import rust_wave1
from tests.rust_wave1 import Divergence

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

PILOT = os.getenv("KS_RUST_PILOT_URL", "").rstrip("/")
DSN = os.getenv("KS_PG_DSN")
REPORT_PATH = os.getenv("KS_RUST_GOALS_REPORT")

pytestmark = [
    pytest.mark.skipif(not PILOT, reason="needs the Rust pilot service at KS_RUST_PILOT_URL"),
    pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN"),
]

KYIV = ZoneInfo("Europe/Kyiv")
QUERIES = fc.goal_queries()
SALES_TYPES = fc.sales_types()
GOALS, HISTORY, SEASONALITY = "/api/goals", "/api/goals/history", "/api/goals/seasonality"
GROWTH, WEEKLY = "/api/goals/growth", "/api/goals/weekly-patterns"
SMART, FORECAST = "/api/goals/smart", "/api/goals/forecast"
ENV = {"KS_READ_GOALS": "postgres", "KS_READ_LOOKUPS": "postgres"}
# Months either side of the pinned Wednesday, 2026-10-21: the forecast is
# seeded for October and November, last year's months are in the history.
MONTHS = tuple((y, m) for y in (2025, 2026) for m in range(1, 13))
GOAL_TABLES = ("app.revenue_goals", "app.revenue_predictions", "app.seasonal_indices",
               "app.growth_metrics", "app.weekly_patterns")


async def _seed(pool) -> Dict[str, object]:
    """Goals a person set — monthly by hand, weekly not — a forecast for two
    months and three sales types, and the shared tables as a recalculation
    leaves them: ten months indexed (every confidence, a NULL YoY, an index
    of 0), the overall YoY, and weekly weights for eight months, some weeks
    missing."""
    from tests import rust_wave1_fixture

    seed = await rust_wave1_fixture.seed(pool)
    stamp = datetime(2026, 10, 1, tzinfo=timezone.utc)
    async with pool.acquire() as conn:
        for table in GOAL_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.executemany(
            "INSERT INTO app.revenue_goals (period_type, goal_amount, is_custom, updated_at) "
            "VALUES ($1, $2, $3, $4)",
            [("monthly", Decimal("4200000.00"), True, stamp),
             ("weekly", Decimal("910000.00"), False, stamp),
             ("yearly", Decimal("50000000.00"), True, stamp)])
        predictions = []
        day = date(2026, 10, 1)
        while day <= date(2026, 11, 30):
            for i, sales_type in enumerate(("retail", "b2b", "all")):
                amount = Decimal(120_000 + 3_517 * (day.day % 9) + 41_000 * i) + Decimal("0.37")
                predictions.append((day, sales_type, amount, Decimal("15000.00"),
                                    Decimal("21.50"), Decimal("27.66"), stamp))
            day += timedelta(days=1)
        await conn.copy_records_to_table(
            "revenue_predictions", schema_name="app",
            columns=("prediction_date", "sales_type", "predicted_revenue", "model_mae",
                     "model_mape", "model_wape", "created_at"), records=predictions)
        seasonal = []
        for month in range(1, 11):
            index = Decimal("0") if month == 4 else Decimal("0.8123") + Decimal(month) / 37
            yoy = None if month in (3, 6) else Decimal("-0.0500") + Decimal(month) / 53
            confidence = ("high", "medium", "low", "", "very high")[month % 5]
            seasonal.append((month, index.quantize(Decimal("0.0001")), 2 + month % 3,
                             Decimal(2_400_000 + 117_000 * month) + Decimal("0.45"),
                             Decimal("1900000.00"), Decimal("3100000.00"),
                             yoy.quantize(Decimal("0.0001")) if yoy is not None else None,
                             confidence, stamp))
        await conn.copy_records_to_table(
            "seasonal_indices", schema_name="app",
            columns=("month", "seasonality_index", "sample_size", "avg_revenue", "min_revenue",
                     "max_revenue", "yoy_growth", "confidence", "updated_at"), records=seasonal)
        await conn.execute(
            "INSERT INTO app.growth_metrics (metric_type, value, period_start, period_end, "
            "sample_size, updated_at) VALUES ('yoy_overall', 0.1234, $1, $2, 2, $3)",
            date(2023, 1, 1), date(2026, 10, 20), stamp)
        weekly = [(month, week, (Decimal("0.2111") + Decimal(week * month % 7) / 100)
                   .quantize(Decimal("0.0001")), 3, stamp)
                  for month in range(1, 9) for week in range(1, 6)
                  if not (month % 3 == 0 and week == 5)]
        await conn.copy_records_to_table(
            "weekly_patterns", schema_name="app",
            columns=("month", "week_of_month", "weight", "sample_size", "updated_at"),
            records=weekly)
    return {**seed, "predictions": len(predictions), "seasonal_months": len(seasonal),
            "weekly_rows": len(weekly)}


async def _clear(pool) -> None:
    from tests import rust_wave1_fixture

    async with pool.acquire() as conn:
        for table in GOAL_TABLES:
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
    # The calculators and the smart goal read `datetime.now(DEFAULT_TZ)`
    # through the goals module's own import.
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("core.repositories.goals.datetime", rust_wave1.Clock)
        async with rust_wave1.services(DSN, PILOT, ENV) as both:
            yield both


def q(path: str, **values) -> str:
    return fc.goal_query(path, **values)


def _contract() -> List[Tuple[str, str]]:
    cases: List[Tuple[str, str]] = []
    for sales_type in SALES_TYPES:
        for path in (GOALS, SEASONALITY, GROWTH, WEEKLY):
            cases.append((path, q(path, sales_type=sales_type)))
        for period_type in ("daily", "weekly", "monthly"):
            cases.append((HISTORY, q(HISTORY, period_type=period_type, weeks_back=4,
                                     sales_type=sales_type)))
        cases.append((SMART, q(SMART, sales_type=sales_type, year=None, month=None)))
        for year, month in MONTHS:
            cases.append((SMART, q(SMART, sales_type=sales_type, year=year, month=month)))
            cases.append((FORECAST, q(FORECAST, year=year, month=month, sales_type=sales_type,
                                      recalculate=False)))
        # What the client sends when asked to recalculate: refused for all.
        cases.append((FORECAST, q(FORECAST, year=2026, month=10, sales_type=sales_type,
                                  recalculate=True)))
    return cases


def _has_rows(path: str, body: bytes) -> bool:
    answer = json.loads(body)
    if path in (SMART, FORECAST):
        return answer["monthly"]["calculationMethod"] != "fallback"
    if path == HISTORY:
        return bool(answer["periodCount"])
    if path == GROWTH:
        return bool(answer["yearly_data"])
    return bool(answer)


@pytest.mark.asyncio
async def test_the_contract_answers_byte_for_byte(pair):
    from core import duckdb_switch, read_fallback

    rows, with_rows, methods = [], 0, set()
    for path, query in _contract():
        py, rs = await pair.ask(path, query)
        found = rust_wave1.difference(py, rs)
        rows.append((f"{path}?{query}", py.status_code, found))
        if found or py.status_code != 200:
            continue
        with_rows += _has_rows(path, py.content)
        if path in (SMART, FORECAST):
            methods.add(json.loads(py.content)["monthly"]["calculationMethod"])
    differences = REPORT.phase("contract", rows)
    REPORT.data["phases"]["contract"].update(with_rows=with_rows, methods=sorted(methods))
    assert not differences, "\n".join(differences[:20])
    assert {200, 400} <= {status for _, status, _ in rows}
    assert with_rows > len(rows) // 2, (with_rows, len(rows))
    # The blend's branches, so none is left to a seed that missed it.
    assert {"yoy_growth", "ml_forecast"} <= methods, methods
    assert duckdb_switch.opened() == {}
    assert read_fallback.counts() == {} and read_fallback.refusals() == {}


@pytest.mark.asyncio
async def test_the_clock_turns_at_kyivs_midnights(pair):
    asks = [(SMART, q(SMART, sales_type="retail", year=None, month=None)),
            (GROWTH, q(GROWTH, sales_type="retail")),
            (FORECAST, q(FORECAST, year=2026, month=10, sales_type="retail",
                         recalculate=False))]
    rows = []
    for now in rust_wave1.KYIV_INSTANTS:
        for path, query in asks:
            py, rs = await pair.ask(path, query, now=now)
            rows.append((f"{now} {path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("midnights", rows)
    assert not differences, "\n".join(differences[:20])


@pytest.mark.asyncio
async def test_on_the_real_clock(pair):
    """The routes whose answers carry no instant of their own: the smart
    goal's `calculatedAt` is the request's microsecond, and is compared
    under the pinned clock."""
    for path, query in ((GROWTH, "sales_type=all"), (GOALS, "sales_type=retail")):
        for _ in range(3):
            before = datetime.now(KYIV).date()
            py, rs = await pair.ask(path, query, now=None)
            if datetime.now(KYIV).date() == before:
                break
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
        assert py.status_code == 200


EDGES: Tuple[Tuple[str, str], ...] = (
    *((HISTORY, f"period_type={p}&weeks_back={w}&sales_type=retail")
      for p in ("daily", "weekly", "monthly") for w in range(1, 13)),
    (SMART, "sales_type=retail&year=2020&month=1"), (SMART, "sales_type=all&year=2030&month=12"),
    (SMART, "sales_type=b2b&month=2"), (SMART, "sales_type=internal&year=2024"),
    (FORECAST, "year=2020&month=1&sales_type=exhibition"),
    (FORECAST, "year=2024&month=2&sales_type=all"),
    (FORECAST, "year=2026&month=10"),                        # no sales type, no recalculate
)


@pytest.mark.asyncio
async def test_the_edges_answer_byte_for_byte(pair):
    rows = []
    for path, query in EDGES:
        py, rs = await pair.ask(path, query)
        rows.append((f"{path}?{query}", py.status_code, rust_wave1.difference(py, rs)))
    differences = REPORT.phase("edges", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {200}


@pytest.mark.asyncio
async def test_a_retired_goals_history_refuses_nothing(pair):
    """`KS_GOALS_HISTORY` is read for nothing since step 14's S14-00:
    `pg_goals_read.history_mode()` answers `silver` whatever it says, so a
    value that once refused every history read — `bridge`, a typo — is the
    same 200 as `silver`, and the pilot, which never reads it, agrees."""
    asks = [(path, q(path, sales_type="retail")) for path in (SEASONALITY, GROWTH, WEEKLY)]
    asks += [(SMART, q(SMART, sales_type="retail", year=2026, month=9)),
             (FORECAST, q(FORECAST, year=2026, month=11, sales_type="all", recalculate=False))]
    rows = []
    for value in ("bridge", "brigde", "SILVER ", ""):
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("KS_GOALS_HISTORY", value)
            for path, query in asks:
                py, rs = await pair.ask(path, query)
                rows.append((f"KS_GOALS_HISTORY={value!r} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
    differences = REPORT.phase("retired_history", rows)
    assert not differences, "\n".join(differences)
    assert {status for _, status, _ in rows} == {200}


@pytest.mark.asyncio
async def test_a_postgres_that_cannot_answer_is_the_same_failure(pair):
    """A schema this build does not know, then each table a goal reads apart
    from the history, gone: a refusal naming `goals` where `_goals_run` or a
    chain's read refuses, the app's 500 where the stored goals are read raw."""
    asks = ((GOALS, "sales_type=retail"),
            (HISTORY, "period_type=weekly&weeks_back=4&sales_type=retail"),
            (SEASONALITY, "sales_type=retail"), (GROWTH, "sales_type=retail"),
            (WEEKLY, "sales_type=retail"), (SMART, "sales_type=retail"),
            (FORECAST, "year=2026&month=10&sales_type=retail&recalculate=false"))
    async with pair.pool.acquire() as conn:
        revision = await conn.fetchval("SELECT version_num FROM meta.alembic_version")
    breaks = [("revision", "UPDATE meta.alembic_version SET version_num = 'wave1_moved'",
               f"UPDATE meta.alembic_version SET version_num = '{revision}'")]
    breaks += [(table, f"ALTER TABLE app.{table} RENAME TO {table}_away",
                f"ALTER TABLE app.{table}_away RENAME TO {table}")
               for table in ("revenue_goals", "seasonal_indices", "revenue_predictions")]
    rows, statuses = [], {}
    for name, broken, mended in breaks:
        async with pair.pool.acquire() as conn:
            await conn.execute(broken)
        try:
            for path, query in asks:
                py, rs = await pair.ask(path, query)
                rows.append((f"{name} {path}?{query}", py.status_code,
                             rust_wave1.difference(py, rs)))
                statuses[(name, path)] = py.status_code
        finally:
            async with pair.pool.acquire() as conn:
                await conn.execute(mended)
    differences = REPORT.phase("unreadable", rows)
    assert not differences, "\n".join(differences)
    order = [path for path, _ in asks]
    REPORT.data["unreadable_statuses"] = {name: [statuses[(name, p)] for p in order]
                                          for name, _, _ in breaks}
    assert [statuses[("revision", p)] for p in order] == [500, 503, 503, 503, 503, 503, 503]
    assert [statuses[("revenue_goals", p)] for p in order] == [500, 200, 200, 200, 200, 500, 200]
    assert [statuses[("seasonal_indices", p)] for p in order] == [200] * 5 + [503, 503]
    assert [statuses[("revenue_predictions", p)] for p in order] == [200] * 5 + [503, 503]
    py, rs = await pair.ask(SMART, "sales_type=retail")
    assert py.status_code == 200 and rust_wave1.difference(py, rs) is None


@pytest.mark.asyncio
async def test_each_limit_counts_what_it_counts_on_both(pair):
    """60 a minute on the goals, 30 on the others: a 422 is never counted,
    a 400 the route raises always is."""
    plans = (
        (GOALS, 60, ["sales_type=bogus"] * 3 + ["sales_type=retail"] * 58),
        (SMART, 30, ["year=abc"] * 3 + ["sales_type=bogus"] * 5 + ["sales_type=retail"] * 26),
        (FORECAST, 30, ["year=2026"] * 3 + ["year=2026&month=10&recalculate=true"] * 5
         + ["year=2026&month=10&sales_type=retail&recalculate=false"] * 26),
    )
    rows = []
    for plan, (path, per_minute, asks) in enumerate(plans):
        key = f"goals-limit-{pair.run}-{plan}"
        py_statuses, rs_statuses = [], []
        for query in asks:
            py, rs = await pair.ask(path, query, key=key)
            py_statuses.append(py.status_code)
            rs_statuses.append(rs.status_code)
            rows.append((f"{path}?{query}", py.status_code,
                         None if py.status_code == rs.status_code
                         else f"{py.status_code} against {rs.status_code}"))
        refused = sum(1 for s in py_statuses if s == 400)
        unparsed = sum(1 for s in py_statuses if s == 422)
        assert py_statuses == ([422] * unparsed + [400] * refused
                               + [200] * (per_minute - refused) + [429]), (path, py_statuses)
        assert rs_statuses == py_statuses, path
        assert rust_wave1.difference(py, rs) is None, rust_wave1.difference(py, rs)
    REPORT.phase("limit", rows)


# Hostile input, by class: (endpoint, query). The status alone is compared.
HOSTILE: Dict[str, List[Tuple[str, str]]] = {
    "sales type unknown": [(GOALS, "sales_type=bogus"), (SMART, "sales_type=wholesale"),
                           (FORECAST, "year=2026&month=10&sales_type=x"),
                           (HISTORY, "period_type=daily&sales_type=x")],
    "sales type respelled": [(GROWTH, "sales_type=%20All%20"), (WEEKLY, "sales_type=B2B"),
                             (SEASONALITY, "sales_type="), (GOALS, "sales_type=r%00")],
    "period_type unknown": [(HISTORY, "period_type=yearly"), (HISTORY, "period_type=DAILY"),
                            (HISTORY, "period_type="),
                            (HISTORY, "period_type=yearly&sales_type=x")],
    "period_type missing": [(HISTORY, ""), (HISTORY, "weeks_back=4&sales_type=retail")],
    "weeks_back out of bounds": [(HISTORY, "period_type=daily&weeks_back=0"),
                                 (HISTORY, "period_type=daily&weeks_back=13"),
                                 (HISTORY, "period_type=weekly&weeks_back=-1")],
    "weeks_back not an integer": [(HISTORY, "period_type=daily&weeks_back=abc"),
                                  (HISTORY, "period_type=daily&weeks_back=4.5"),
                                  (HISTORY, "period_type=daily&weeks_back=")],
    "weeks_back as pydantic reads it": [(HISTORY, "period_type=daily&weeks_back=4.0"),
                                        (HISTORY, "period_type=weekly&weeks_back=%204"),
                                        (HISTORY, "period_type=monthly&weeks_back=1_2")],
    "weeks_back past 64 bits": [(HISTORY, "period_type=daily&weeks_back=99999999999999999999")],
    "year out of bounds": [(SMART, "year=2019"), (SMART, "year=2031"),
                           (FORECAST, "year=2019&month=1"), (FORECAST, "year=-2026&month=1")],
    "year as pydantic reads it": [(SMART, "year=2026.0&month=9"), (SMART, "year=%202026"),
                                  (FORECAST, "year=2_026&month=9")],
    "year past 64 bits": [(SMART, "year=99999999999999999999"),
                          (FORECAST, "year=99999999999999999999&month=1")],
    "month out of bounds": [(SMART, "month=0"), (SMART, "month=13"),
                            (FORECAST, "year=2026&month=13")],
    "month not an integer": [(SMART, "month=abc"), (FORECAST, "year=2026&month=9.5")],
    "forecast without its month": [(FORECAST, ""), (FORECAST, "year=2026"),
                                   (FORECAST, "month=10"), (FORECAST, "sales_type=retail")],
    "recalculate truthy": [(FORECAST, "year=2026&month=10&recalculate=1"),
                           (FORECAST, "year=2026&month=10&recalculate=YES"),
                           (FORECAST, "year=2026&month=10&recalculate=on")],
    "recalculate falsy": [(FORECAST, "year=2026&month=10&recalculate=0"),
                          (FORECAST, "year=2026&month=10&recalculate=Off")],
    "recalculate not a boolean": [(FORECAST, "year=2026&month=10&recalculate=maybe"),
                                  (FORECAST, "year=2026&month=10&recalculate=")],
    "recalculate after the sales type": [
        (FORECAST, "year=2026&month=10&sales_type=x&recalculate=true")],
    "another route's parameter": [(GOALS, "year=x&month=y&weeks_back=z"),
                                  (SEASONALITY, "period_type=x&recalculate=maybe"),
                                  (SMART, "recalculate=maybe&weeks_back=x")],
    "a parameter repeated": [(SMART, "month=13&month=9"),
                             (FORECAST, "year=2026&month=10&recalculate=true&recalculate=false")],
}

# Where the two statuses differ on hostile input, and why Rust is not made to
# agree: nowhere yet. Every integer here is bounded, so pydantic refuses a
# value past 64 bits by its bound where Rust refuses it by its width — 422
# both — and the text the routes take is validated before it reaches SQL.
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
