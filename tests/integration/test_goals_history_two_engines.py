"""The goal calculators' Silver history means the same thing in both engines.

The unit tests prove the Silver bodies select the orders written out for them,
on DuckDB's Silver (`tests/unit/test_goals_history_silver.py`). This is about
Postgres: under `KS_READ_GOALS=postgres` every history read is answered by
`silver.orders` — the only history since chain 7b-4 deleted the DN-12 bridge
and retired `KS_GOALS_HISTORY` (production sets it to `silver`, which reads
the same) — and three things are proved against a real server:

  * **T11, two engines** — the three-year history in DuckDB and its Silver
    copied into `silver.orders`: seasonality, YoY, weekly patterns, the
    twelve growth caps, last year's month, the recent months, the bounds and
    the smart goal, equal to DuckDB Silver's answer to 1e-6 relative (it was
    the bridge's answer until 7b-4, and the two were equal). The places the
    two engines could part are `EXTRACT` (BIGINT against NUMERIC), the
    division of a day number by seven, `STDDEV` and a DECIMAL divided into a
    DOUBLE or a NUMERIC.
  * **met by construction, on Postgres** — DuckDB's `orders`, `managers`,
    `manager_classifications`, `silver_orders` and Gold are refused at the
    statement, and every goal is still computed.
  * **T14, compaction shape** — DuckDB's Silver emptied, as the first Sunday
    compaction after step 13 leaves it: nothing moves.

Skipped without `KS_PG_DSN`; CI and `deploy/gate_with_stores.sh` supply one.
Run with `TZ=UTC` like every two-engine test.
"""
from __future__ import annotations

import asyncio
import os
import re
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from tests.unit.test_goals_off_duckdb_silver import (
    TIMEOUT_S,
    _seed_history,
    _without_clock,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

SILVER_COLUMNS = ("id, source_id, status_id, grand_total, ordered_at, buyer_id, "
                  "manager_id, order_date, is_return, sales_type, is_active_source, "
                  "source_name, is_new_customer, buyer_first_order_date, promocode")
PG_TABLES = ("silver.orders", "gold.daily_revenue", "app.revenue_predictions",
             "app.revenue_goals")

# Every DuckDB table the goal history and the ML signal read, and the two the
# deleted bridge did. Under postgres none of them may be asked.
DUCKDB_HISTORY = re.compile(
    r"(?<![\w.])(orders|managers|manager_classifications|silver_orders|"
    r"gold_daily_revenue|revenue_predictions|revenue_goals)(?![\w])",
    re.IGNORECASE)


async def _copy_silver(pool, store) -> int:
    async with store.connection() as duck:
        rows = duck.execute(
            f"SELECT {SILVER_COLUMNS} FROM silver_orders ORDER BY id").fetchall()
    async with pool.acquire() as conn:
        for table in PG_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.copy_records_to_table(
            "orders", schema_name="silver", records=[tuple(r) for r in rows],
            columns=[c.strip() for c in SILVER_COLUMNS.split(",")])
    return len(rows)


@pytest_asyncio.fixture
async def history(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv("KS_READ_GOALS", raising=False)
    monkeypatch.delenv("KS_READ_FALLBACK", raising=False)
    store = DuckDBStore(db_path=tmp_path / "goals-history.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_history(store)
        copied = await _copy_silver(pool, store)
        assert copied > 1500, "the history did not reach Postgres"
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        async with pool.acquire() as conn:
            for table in PG_TABLES:
                await conn.execute(f"DELETE FROM {table}")
        await pool.close()
        await store.close()


def _close(value):
    """Floats to a 1e-6 relative grain: a DuckDB DOUBLE against a Postgres
    NUMERIC legitimately differ in the last bits; integers and text exact."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, dict):
        return {k: _close(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_close(v) for v in value]
    if isinstance(value, float) or hasattr(value, "as_tuple"):
        return float(f"{float(value):.6g}")
    return value


async def _history_answers(store, sales_type):
    """Everything the history switch decides, read only."""
    return {
        "seasonality": await store.calculate_seasonality_indices(sales_type),
        "yoy": await store.calculate_yoy_growth(sales_type),
        "weekly": await store.calculate_weekly_patterns(sales_type),
        "caps": [await store._dynamic_growth_cap(m, sales_type) for m in range(1, 13)],
        "last_year": await store._last_year_month_revenue(2026, 10, sales_type),
        "recent": await store._recent_three_month_average(sales_type),
        "bounds": await store._history_bounds(),
    }


def _duckdb_refused(store):
    """The store's connection, refusing every statement that names a DuckDB
    table of the goal history — so a read DuckDB answered fails rather than
    passing as a comparison of DuckDB with itself."""
    real = type(store).connection

    class _Refusing:
        def __init__(self, conn):
            self._conn = conn

        def _check(self, sql):
            found = DUCKDB_HISTORY.search(sql)
            if found:
                raise AssertionError(
                    f"DuckDB {found.group(1)!r} was read under "
                    f"KS_READ_GOALS=postgres: {' '.join(sql.split())[:160]}")

        def execute(self, sql, *a, **k):
            self._check(sql)
            return self._conn.execute(sql, *a, **k)

        def executemany(self, sql, *a, **k):
            self._check(sql)
            return self._conn.executemany(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    @asynccontextmanager
    async def guarded(self):
        async with real(self) as conn:
            yield _Refusing(conn)

    return patch.object(type(store), "connection", guarded)


class TestTwoEngines:
    """T11. Mutation M14: divide an INTEGER day by seven in the weekly body
    (`CAST(EXTRACT(DAY …) AS INTEGER) / 7.0` is fine; `CAST(… AS INTEGER) / 7`
    truncates in Postgres and not in DuckDB) — the week keys part, here."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_history_reads(self, history, monkeypatch, sales_type):
        monkeypatch.setenv("KS_GOALS_HISTORY", "silver")   # production's line
        duckdb_silver = await _history_answers(history, sales_type)

        monkeypatch.setenv("KS_READ_GOALS", "postgres")

        def _no_duckdb(*_a, **_k):
            raise AssertionError("a history read reached DuckDB under KS_READ_GOALS=postgres")

        with patch.object(type(history), "connection", _no_duckdb):
            postgres = await _history_answers(history, sales_type)

        assert len(duckdb_silver["seasonality"]) == 12
        assert duckdb_silver["yoy"]["sample_size"] >= 1
        assert _close(postgres) == _close(duckdb_silver)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b"])
    async def test_the_smart_goal_and_the_stored_tables(
        self, history, monkeypatch, sales_type,
    ):
        await history.recalculate_goal_tables(include_weekly=True)
        duckdb_silver = _without_clock(await asyncio.wait_for(
            history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S))
        duckdb_tables = await history._compute_goal_tables(include_weekly=True)

        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        postgres_tables = await history._compute_goal_tables(include_weekly=True)
        await history.recalculate_goal_tables(include_weekly=True)
        with _duckdb_refused(history):
            postgres = _without_clock(await asyncio.wait_for(
                history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S))

        assert _close(postgres) == _close(duckdb_silver)
        assert duckdb_silver["monthly"]["lastYearRevenue"] > 0
        assert postgres_tables["history_bounds"] == duckdb_tables["history_bounds"]
        for key in ("seasonal", "yoy", "weekly_rows"):
            assert _close(postgres_tables[key]) == _close(duckdb_tables[key]), key


class TestMetByConstructionOnPostgres:
    """Every goal computation, the Monday job included, with DuckDB's
    orders, classification, Silver and Gold refused at the statement."""

    @pytest.mark.asyncio
    async def test_nothing_reads_the_duckdb_history(self, history, monkeypatch):
        from core.scheduler import BackgroundScheduler

        monkeypatch.setenv("KS_GOALS_HISTORY", "silver")
        monkeypatch.setenv("KS_READ_GOALS", "postgres")

        async def _get_store():
            return history

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        with _duckdb_refused(history):
            for sales_type in ("retail", "b2b", "all"):
                await _history_answers(history, sales_type)
                await asyncio.wait_for(
                    history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S)
            result = await BackgroundScheduler()._run_seasonality_calc()
        assert result["retail_months"] == 12


class TestCompactionShape:
    """T14. DuckDB's Silver emptied — what a compaction leaves after step 13 —
    and nothing moves, because Postgres answers. Mutation M17: send one body
    to DuckDB explicitly (the growth cap on the store's own connection with
    a DuckDB rendering, as the deleted bridge read), and the caps fall to
    0.35 here."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_nothing_moves(self, history, monkeypatch, sales_type):
        monkeypatch.setenv("KS_GOALS_HISTORY", "silver")
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        before = await _history_answers(history, sales_type)
        await history.recalculate_goal_tables(include_weekly=True)
        smart_before = _without_clock(await asyncio.wait_for(
            history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S))

        async with history.connection() as conn:
            conn.execute("DELETE FROM silver_orders")

        after = await _history_answers(history, sales_type)
        await history.recalculate_goal_tables(include_weekly=True)
        smart_after = _without_clock(await asyncio.wait_for(
            history.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S))
        assert _close(after) == _close(before)
        assert _close(smart_after) == _close(smart_before)
        assert len(after["seasonality"]) == 12
        assert after["yoy"]["sample_size"] >= 1
