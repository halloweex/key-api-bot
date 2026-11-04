"""The goal calculators' Silver history answers in Postgres what DuckDB did.

The unit tests prove the Silver bodies compute what the rows imply, written
out in Python (`tests/unit/test_goals_off_duckdb_silver.py`). This is the
three-year history in a real Postgres — Bronze seeded, Silver and Gold
derived, the production path — and two things are proved there:

  * **T11, the literal** — seasonality, YoY, weekly patterns, the twelve
    growth caps, last year's month, the recent months, the bounds, the smart
    goal and the stored tables, equal to DuckDB Silver's answer on this
    history (`tests/integration/golden/goals_history.json`, rule 9 of the
    step-14 plan), recorded while the same run asserted Postgres answered
    alike to 1e-6 relative. The places the two engines could part were
    `EXTRACT` (BIGINT against NUMERIC), the division of a day number by
    seven, `STDDEV` and a DECIMAL divided into a DOUBLE or a NUMERIC.
  * **met by construction** — the store is never connected and its DuckDB
    connection is a trap: every goal is computed, the Monday job included,
    with nothing to read but Postgres.

ONE ENGINE SINCE STEP 14 (S14-07). Every goal read is Postgres'
(`GoalsMixin._goals_run`); `KS_READ_GOALS` and `KS_GOALS_HISTORY` are not
read. The compaction shape this file also held — DuckDB's Silver emptied,
nothing moves — went with the DuckDB side: there is no DuckDB Silver to
empty.

Skipped without `KS_PG_DSN`; CI and `deploy/gate_with_stores.sh` supply one.
Run with `TZ=UTC` like every Postgres test here.
"""
from __future__ import annotations

import asyncio
import os

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import (
    call_key, comparable, golden, store_that_never_reaches_duckdb, tab_in_postgres,
)
from tests.unit.test_goals_off_duckdb_silver import (
    TIMEOUT_S,
    _seed_history_pg,
    _without_clock,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)


@pytest_asyncio.fixture
async def history(tmp_path, monkeypatch):
    """The history in Postgres, and a store that never reaches DuckDB."""
    async with tab_in_postgres(DSN, monkeypatch, _seed_history_pg) as pool:
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT COUNT(*) FROM silver.orders") > 1500, (
                "the history did not derive into Silver")
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goal history")


def _close(value):
    """Floats to a 1e-6 relative grain: a DuckDB DOUBLE against a Postgres
    NUMERIC legitimately differ in the last bits; integers and text exact.
    The literal was recorded through `comparable` and then this."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, dict):
        return {k: _close(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_close(v) for v in value]
    if isinstance(value, float) or hasattr(value, "as_tuple"):
        return float(f"{float(value):.6g}")
    return value


def _as_recorded(value):
    return _close(comparable(value))


async def _history_answers(store, sales_type):
    """Everything the history decides, read only."""
    return {
        "seasonality": await store.calculate_seasonality_indices(sales_type),
        "yoy": await store.calculate_yoy_growth(sales_type),
        "weekly": await store.calculate_weekly_patterns(sales_type),
        "caps": [await store._dynamic_growth_cap(m, sales_type) for m in range(1, 13)],
        "last_year": await store._last_year_month_revenue(2026, 10, sales_type),
        "recent": await store._recent_three_month_average(sales_type),
        "bounds": await store._history_bounds(),
    }


GOLDEN = golden("goals_history")


class TestTheLiteral:
    """T11. Mutation M14: divide an INTEGER day by seven in the weekly body
    (`CAST(EXTRACT(DAY …) AS INTEGER) / 7.0` is fine; `CAST(… AS INTEGER) / 7`
    truncates in Postgres and did not in DuckDB) — the week keys part, here."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_history_reads(self, history, sales_type):
        answers = await _history_answers(history, sales_type)
        assert len(answers["seasonality"]) == 12
        assert answers["yoy"]["sample_size"] >= 1
        assert _as_recorded(answers) == GOLDEN[call_key("_history_answers", [sales_type])]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b"])
    async def test_the_smart_goal_and_the_stored_tables(self, history, sales_type):
        await history.recalculate_goal_tables(include_weekly=True)
        smart = _without_clock(await asyncio.wait_for(
            history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S))
        tables = await history._compute_goal_tables(include_weekly=True)

        assert smart["monthly"]["lastYearRevenue"] > 0
        assert _as_recorded(smart) == GOLDEN[
            call_key("generate_smart_goals", [2026, 10, sales_type])]
        assert _as_recorded(tables) == GOLDEN[call_key("_compute_goal_tables", [sales_type])]

    def test_every_literal_is_asked(self):
        assert set(GOLDEN) == (
            {call_key("_history_answers", [s]) for s in ("retail", "b2b", "all")}
            | {call_key("generate_smart_goals", [2026, 10, s]) for s in ("retail", "b2b")}
            | {call_key("_compute_goal_tables", [s]) for s in ("retail", "b2b")})


class TestMetByConstruction:
    """Every goal computation, the Monday job included, on a store whose
    DuckDB connection is a trap."""

    @pytest.mark.asyncio
    async def test_nothing_reads_duckdb(self, history, monkeypatch):
        from core.scheduler import BackgroundScheduler

        async def _get_store():
            return history

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        for sales_type in ("retail", "b2b", "all"):
            await _history_answers(history, sales_type)
            await asyncio.wait_for(
                history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S)
        result = await BackgroundScheduler()._run_seasonality_calc()
        assert result["retail_months"] == 12
