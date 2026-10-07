"""Chain 7b-3: the goal and forecast tables, written to Postgres, against a
real server.

`KS_WRITE_FORECAST=postgres` moves the writes of `app.seasonal_indices`,
`app.growth_metrics`, `app.weekly_patterns` and `app.revenue_predictions`
from DuckDB (and the hourly full replace behind it) to Postgres, once the
calculators' inputs are Postgres too. What that has to survive, and what has
to stay true around it:

- the Monday job's store and the POST's go to Postgres and not DuckDB, in one
  transaction that stores exactly the rows the DuckDB branch stores — the
  index upsert never touching `yoy_growth`, the placeholder never written over
  a measured rate — and a YoY update that matches no row rolls the whole set
  back;
- the forecast replaces its range and only its range, stamped with the
  caller's clock, and two trainings at once do not collide;
- the first write latches the chain and claims all four owner rows in the
  writing transaction;
- every read of the four tables follows the chain whatever `KS_READ_GOALS`
  says, and a Postgres failure there raises rather than showing DuckDB's
  frozen copy;
- the hourly replace stands down for the four tables (a control proves it
  would otherwise wipe them), and stays down with the flag put back;
- the copy-back carries the four tables into DuckDB, compares them at zero and
  releases the latch, and `--handover` before a flip names what the flip
  would strand;
- the standing watch reads what it judges.

The inputs are a three-year history seeded in DuckDB and its Silver copied
into `silver.orders`, so the calculators read Postgres Silver as they will in
production (`KS_GOALS_HISTORY=silver`, `KS_READ_GOALS=postgres`).

Skipped without `KS_PG_DSN`. Run with `TZ=UTC` like every two-engine test.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import chain_latch, pg_forecast_write, read_fallback
from core.chain_transfer import CopyBackRefused, copy_back, handover_check

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

CHAIN = pg_forecast_write.CHAIN
TABLES = pg_forecast_write.CHAIN_TABLES
SEASONAL, GROWTH, WEEKLY, PREDICTIONS = (
    "app.seasonal_indices", "app.growth_metrics", "app.weekly_patterns",
    "app.revenue_predictions")
# What the three goal tables and the forecast hold, stamps left out: those are
# each store's clock by construction (the copy-back forgives them too).
COLUMNS = {
    SEASONAL: ("month", "seasonality_index", "sample_size", "avg_revenue",
               "min_revenue", "max_revenue", "yoy_growth", "confidence"),
    GROWTH: ("metric_type", "value", "period_start", "period_end", "sample_size"),
    WEEKLY: ("month", "week_of_month", "weight", "sample_size"),
    PREDICTIONS: ("prediction_date", "sales_type", "predicted_revenue",
                  "model_mae", "model_mape", "model_wape"),
}
STAMP = {SEASONAL: "updated_at", GROWTH: "updated_at", WEEKLY: "updated_at",
         PREDICTIONS: "created_at"}
SILVER_LEFT = ("silver.orders", "gold.daily_revenue", "app.revenue_goals")


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in TABLES + SILVER_LEFT:
            await conn.execute(f"DELETE FROM {table}")
        # Left behind, an owner row reads in the next module as a chain owning
        # a table with no local marker — a real CRITICAL a fixture made.
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(TABLES))


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB holding the history, its Silver in Postgres, and every
    precondition of the chain met — the flag itself left unset."""
    from core.duckdb_store import DuckDBStore
    from tests.integration.test_goals_history_two_engines import _copy_silver
    from tests.unit.test_goals_off_duckdb_silver import _seed_history

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_GOALS_HISTORY", "silver")
    monkeypatch.setenv("KS_READ_GOALS", "postgres")
    monkeypatch.setenv("KS_READ_FORECAST_INPUT", "postgres")
    monkeypatch.setenv("KS_READ_FALLBACK", "off")
    monkeypatch.setattr(read_fallback, "_mode", read_fallback.OFF)
    monkeypatch.setattr(read_fallback, "_mode_error", None)
    store = DuckDBStore(db_path=tmp_path / "forecast.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    await _seed_history(store)
    assert await _copy_silver(pool, store) > 1500
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
            patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


def _flag(env):
    env.setenv(pg_forecast_write.WRITE_ENV, "postgres")
    assert pg_forecast_write.writes_postgres(), pg_forecast_write.unmet_precondition()


async def _pg(pool, table, stamps=False):
    cols = COLUMNS[table] + ((STAMP[table],) if stamps else ())
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT {', '.join(cols)} FROM {table} ORDER BY {cols[0]}, {cols[1]}")
    return [tuple(r) for r in rows]


async def _duck(store, table, stamps=False):
    cols = COLUMNS[table] + ((STAMP[table],) if stamps else ())
    name = table.split(".", 1)[1]
    async with store.connection() as conn:
        rows = conn.execute(
            f"SELECT {', '.join(cols)} FROM {name} ORDER BY {cols[0]}, {cols[1]}"
        ).fetchall()
    return [tuple(r) for r in rows]


async def _all_pg(pool, stamps=False):
    return {t: await _pg(pool, t, stamps) for t in TABLES}


async def _all_duck(store, stamps=False):
    return {t: await _duck(store, t, stamps) for t in TABLES}


FORECAST = [{"date": (date(2026, 10, 7) + timedelta(days=n)).isoformat(),
             "predicted_revenue": 1000.0 + n} for n in range(5)]
METRICS = {"mae": 120.5, "mape": 31.2, "wape": 27.66}


# ─── Under the flag ─────────────────────────────────────────────────────────


class TestUnderTheFlag:
    @pytest.mark.asyncio
    async def test_the_post_stores_three_tables_in_postgres_and_none_in_duckdb(
            self, stores):
        store, pool, env = stores
        _flag(env)
        before = await _all_duck(store)

        tables = await store.recalculate_goal_tables(include_weekly=True)

        assert len(tables["seasonal"]) == 12
        pg = await _all_pg(pool, stamps=True)
        assert len(pg[SEASONAL]) == 12 and len(pg[WEEKLY]) == 60
        assert [r[0] for r in pg[GROWTH]] == ["yoy_overall"]
        assert all(r[6] is not None for r in pg[SEASONAL]), "a month lost its YoY"
        # One stamp for the whole set: the writer's clock, one transaction.
        stamps = {r[-1] for t in (SEASONAL, GROWTH, WEEKLY) for r in pg[t]}
        assert len(stamps) == 1 and None not in stamps
        assert await _all_duck(store) == before, "the write reached DuckDB"
        assert chain_latch.latched(CHAIN)

    @pytest.mark.asyncio
    async def test_the_monday_job_stores_two_tables_and_no_weekly_weights(self, stores):
        store, pool, env = stores
        _flag(env)

        await store.recalculate_goal_tables(include_weekly=False)

        assert len(await _pg(pool, SEASONAL)) == 12
        assert len(await _pg(pool, GROWTH)) == 1
        assert await _pg(pool, WEEKLY) == [], "the Monday job never stored them (OQ-2)"

    @pytest.mark.asyncio
    async def test_the_forecast_goes_to_postgres_and_not_duckdb(self, stores):
        store, pool, env = stores
        _flag(env)

        assert await store.store_predictions(FORECAST, "retail", METRICS) == 5

        rows = await _pg(pool, PREDICTIONS, stamps=True)
        assert [r[0] for r in rows] == [date(2026, 10, 7) + timedelta(days=n)
                                       for n in range(5)]
        assert rows[0][2:6] == (Decimal("1000.00"), Decimal("120.50"),
                                Decimal("31.20"), Decimal("27.66"))
        assert len({r[-1] for r in rows}) == 1 and rows[0][-1] is not None
        assert await _duck(store, PREDICTIONS) == []

    @pytest.mark.asyncio
    async def test_both_branches_store_the_same_rows(self, stores):
        """The same history through the DuckDB branch and then the Postgres
        one: every value equal, the stamps aside. A SET list that differs
        from DuckDB's — `yoy_growth` in the index upsert, say — or a growth
        row built from other columns, shows here."""
        store, pool, env = stores
        await store.recalculate_goal_tables(include_weekly=True)     # DuckDB
        await store.store_predictions(FORECAST, "retail", METRICS)
        duck = await _all_duck(store)
        _flag(env)
        await store.recalculate_goal_tables(include_weekly=True)     # Postgres
        await store.store_predictions(FORECAST, "retail", METRICS)

        assert await _all_pg(pool) == duck
        assert len(duck[SEASONAL]) == 12 and len(duck[WEEKLY]) == 60

    @pytest.mark.asyncio
    async def test_the_owner_rows_for_all_four_share_the_writes_transaction(self, stores):
        store, pool, env = stores
        _flag(env)

        await store.recalculate_goal_tables(include_weekly=False)

        stamp = chain_latch.latched_at(CHAIN)
        assert await chain_latch.read_owners(pool) == {t: stamp for t in TABLES}
        async with pool.acquire() as conn:
            owners = {r["x"] for r in await conn.fetch(
                "SELECT xmin::text AS x FROM meta.chain_watermarks "
                "WHERE key LIKE 'owner:%'")}
            row = await conn.fetchval(
                f"SELECT xmin::text FROM {SEASONAL} WHERE month = 1")
        assert owners == {row}

    @pytest.mark.asyncio
    async def test_an_unknown_value_refuses_both_writers_in_both_stores(self, stores):
        store, pool, env = stores
        env.setenv(pg_forecast_write.WRITE_ENV, "postgrse")
        before = await _all_duck(store)

        with pytest.raises(RuntimeError, match=pg_forecast_write.WRITE_ENV):
            await store.recalculate_goal_tables(include_weekly=True)
        with pytest.raises(RuntimeError, match=pg_forecast_write.WRITE_ENV):
            await store.store_predictions(FORECAST, "retail", METRICS)

        assert await _all_pg(pool) == {t: [] for t in TABLES}
        assert await _all_duck(store) == before
        assert not chain_latch.latched(CHAIN)


# ─── The goal tables' transaction ───────────────────────────────────────────


NOW = datetime(2026, 10, 5, 1, 0, tzinfo=timezone.utc)
ROW = (3, 0.95, 3, 1000.0, 800.0, 1200.0, "high")


async def _seed_set(pool):
    """A stored set in Postgres: month 3 with a YoY, a measured overall rate
    and one weekly weight."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {SEASONAL} VALUES (3, 0.9, 1, 900, 800, 1000, 0.42, 'low', $1)",
            NOW - timedelta(days=7))
        await conn.execute(
            f"INSERT INTO {GROWTH} VALUES ('yoy_overall', 0.2345, '2023-12-02', "
            f"'2025-12-31', 3, $1)", NOW - timedelta(days=7))
        await conn.execute(
            f"INSERT INTO {WEEKLY} VALUES (3, 1, 0.2, 1, $1)", NOW - timedelta(days=7))


class TestTheGoalTablesTransaction:
    @pytest.mark.asyncio
    async def test_a_month_without_a_pair_keeps_its_yoy(self, stores):
        """The index upsert never touches `yoy_growth`: a month with no pair
        of years keeps the rate it had, as in DuckDB."""
        _store, pool, _env = stores
        await _seed_set(pool)

        await pg_forecast_write.persist_goal_tables(
            [ROW], (0.5, date(2023, 12, 2), date(2025, 12, 31), 1), {}, [], NOW)

        (row,) = await _pg(pool, SEASONAL)
        assert row[1] == Decimal("0.9500"), "the index was not rewritten"
        assert row[6] == Decimal("0.4200"), "the stored YoY was lost"

    @pytest.mark.asyncio
    async def test_the_placeholder_never_overwrites_a_measured_rate(self, stores):
        """With no pair of full years the overall rate is the 0.10 guess, and
        a stored measurement is kept — through the repository, over an emptied
        Postgres Silver, the compaction shape the bridge once failed in."""
        store, pool, env = stores
        await _seed_set(pool)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM silver.orders")
        _flag(env)

        tables = await store.recalculate_goal_tables(include_weekly=False)

        assert tables["yoy"]["sample_size"] == 0
        assert await _pg(pool, GROWTH) == [
            ("yoy_overall", Decimal("0.2345"), date(2023, 12, 2),
             date(2025, 12, 31), 3)]

    @pytest.mark.asyncio
    async def test_a_first_run_writes_the_placeholder(self, stores):
        _store, pool, _env = stores
        await pg_forecast_write.persist_goal_tables(
            [], (0.10, None, None, 0), {}, [], NOW)
        assert await _pg(pool, GROWTH) == [("yoy_overall", Decimal("0.1000"),
                                            None, None, 0)]

    @pytest.mark.asyncio
    async def test_on_an_empty_table_every_month_with_a_pair_gets_its_yoy(self, stores):
        """The UPDATE runs after the INSERT, in one transaction: on an empty
        table the reverse order would match nothing and leave every rate
        NULL — and the guard would refuse it."""
        store, pool, env = stores
        _flag(env)

        tables = await store.recalculate_goal_tables(include_weekly=False)

        stored = {r[0]: r[6] for r in await _pg(pool, SEASONAL)}
        assert set(tables["yoy"]["monthly_yoy"]) == set(range(1, 13))
        for month, rate in tables["yoy"]["monthly_yoy"].items():
            assert stored[month] == Decimal(str(rate)).quantize(Decimal("0.0001"))

    @pytest.mark.asyncio
    async def test_a_yoy_update_that_matches_no_row_rolls_the_whole_set_back(
            self, stores):
        _store, pool, _env = stores
        await _seed_set(pool)
        before = await _all_pg(pool, stamps=True)

        with pytest.raises(RuntimeError, match="UPDATE 1"):
            await pg_forecast_write.persist_goal_tables(
                [ROW], (0.5, None, None, 1), {3: 0.3, 5: 0.25},
                [(3, 1, 0.3, 2)], NOW)

        assert await _all_pg(pool, stamps=True) == before

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fails_on", [
        "pg_advisory_xact_lock", "INSERT INTO meta.chain_watermarks",
        "INSERT INTO app.seasonal_indices", "SELECT 1 FROM app.growth_metrics",
        "INSERT INTO app.growth_metrics", "UPDATE app.seasonal_indices",
        "INSERT INTO app.weekly_patterns",
    ])
    async def test_a_failure_at_any_statement_leaves_every_table_as_it_was(
            self, stores, fails_on):
        _store, pool, _env = stores
        await _seed_set(pool)
        before = await _all_pg(pool, stamps=True)
        failing = _FailingPool(pool, fails_on)

        with patch.object(pg_forecast_write, "_pool", AsyncMock(return_value=failing)):
            with pytest.raises(_Injected):
                await pg_forecast_write.persist_goal_tables(
                    [ROW, (4, 1.1, 3, 1100.0, 900.0, 1300.0, "high")],
                    (0.5, date(2023, 12, 2), date(2025, 12, 31), 1),
                    {3: 0.3, 4: 0.31}, [(3, 1, 0.3, 2), (4, 1, 0.25, 2)], NOW)

        assert failing.hit, f"{fails_on!r} was never executed"
        assert await _all_pg(pool, stamps=True) == before
        assert await chain_latch.read_owners(pool) == {}


class _Injected(Exception):
    pass


class _FailingConn:
    """The writer's connection, raising on the first statement that carries
    `needle` — inside the writer's own transaction."""

    def __init__(self, conn, needle, pool):
        self._conn, self._needle, self._pool = conn, needle, pool

    def _check(self, sql):
        if self._needle in sql:
            self._pool.hit = True
            raise _Injected(self._needle)

    async def execute(self, sql, *a, **k):
        self._check(sql)
        return await self._conn.execute(sql, *a, **k)

    async def executemany(self, sql, *a, **k):
        self._check(sql)
        return await self._conn.executemany(sql, *a, **k)

    async def fetchval(self, sql, *a, **k):
        self._check(sql)
        return await self._conn.fetchval(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _FailingPool:
    def __init__(self, pool, needle):
        self._pool, self._needle, self.hit = pool, needle, False

    def acquire(self):
        outer = self

        class _Ctx:
            async def __aenter__(self):
                self._ctx = outer._pool.acquire()
                conn = await self._ctx.__aenter__()
                return _FailingConn(conn, outer._needle, outer)

            async def __aexit__(self, *exc):
                return await self._ctx.__aexit__(*exc)
        return _Ctx()


# ─── The forecast ───────────────────────────────────────────────────────────


class TestTheForecast:
    @pytest.mark.asyncio
    async def test_it_replaces_its_range_and_only_its_range(self, stores):
        _store, pool, _env = stores
        old = datetime(2026, 9, 1, 0, 30, tzinfo=timezone.utc)
        await pg_forecast_write.store_predictions(
            [{"date": date(2026, 10, d), "predicted_revenue": 500.0}
             for d in range(1, 12)], "retail", METRICS, old)
        await pg_forecast_write.store_predictions(
            [{"date": date(2026, 10, d), "predicted_revenue": 500.0}
             for d in range(1, 3)], "b2b", METRICS, old)

        new = datetime(2026, 10, 8, 0, 30, tzinfo=timezone.utc)
        await pg_forecast_write.store_predictions(FORECAST, "retail", METRICS, new)

        rows = {(r[0], r[1]): r for r in await _pg(pool, PREDICTIONS, stamps=True)}
        # Before the range, kept as it was; inside it, replaced; another
        # sales type, untouched.
        for d in range(1, 7):
            assert rows[(date(2026, 10, d), "retail")][2] == Decimal("500.00")
            assert rows[(date(2026, 10, d), "retail")][-1] == old
        for n in range(5):
            day = date(2026, 10, 7) + timedelta(days=n)
            assert rows[(day, "retail")][2] == Decimal(f"{1000 + n}.00")
            assert rows[(day, "retail")][-1] == new, "the stamp is the caller's"
        assert (date(2026, 10, 1), "b2b") in rows
        assert len(rows) == 6 + 5 + 2

    @pytest.mark.asyncio
    async def test_two_trainings_at_once_do_not_collide(self, stores):
        """DuckDB's store lock admitted one; here the advisory lock does. Two
        DELETE+INSERTs over one range without it: the second dies on the
        primary key."""
        _store, pool, _env = stores
        stamp = datetime(2026, 10, 8, 0, 30, tzinfo=timezone.utc)
        a = [{"date": p["date"], "predicted_revenue": 1.0} for p in FORECAST]
        b = [{"date": p["date"], "predicted_revenue": 2.0} for p in FORECAST]

        for _ in range(3):
            results = await asyncio.gather(
                pg_forecast_write.store_predictions(a, "retail", METRICS, stamp),
                pg_forecast_write.store_predictions(b, "retail", METRICS, stamp),
                return_exceptions=True)
            assert results == [5, 5], results
            values = {r[2] for r in await _pg(pool, PREDICTIONS)}
            assert values in ({Decimal("1.00")}, {Decimal("2.00")}), values

    @pytest.mark.asyncio
    async def test_the_largest_values_the_columns_hold_are_stored(self, stores):
        _store, pool, _env = stores
        await pg_forecast_write.store_predictions(
            [{"date": "2026-10-07", "predicted_revenue": 9_999_999_999.99}],
            "retail", {"mae": 99_999_999.99, "mape": 9999.99, "wape": 9999.994},
            datetime(2026, 10, 8, tzinfo=timezone.utc))
        (row,) = await _pg(pool, PREDICTIONS)
        assert row[2:] == (Decimal("9999999999.99"), Decimal("99999999.99"),
                           Decimal("9999.99"), Decimal("9999.99"))

    @pytest.mark.asyncio
    async def test_nan_is_refused_here_though_postgres_would_store_it(self, stores):
        _store, pool, _env = stores
        async with pool.acquire() as conn:      # the divergence, measured
            assert str(await conn.fetchval("SELECT 'NaN'::numeric(6,2)")) == "NaN"
        with pytest.raises(ValueError, match="model_mape"):
            await pg_forecast_write.store_predictions(
                FORECAST, "retail", {"mae": 1, "mape": float("nan"), "wape": 1},
                datetime(2026, 10, 8, tzinfo=timezone.utc))
        assert not chain_latch.latched(CHAIN)
        assert await _pg(pool, PREDICTIONS) == []


# ─── The reads follow the chain ─────────────────────────────────────────────


class TestTheReadsFollowTheChain:
    """DuckDB and Postgres are made to hold different numbers; the answer
    says which was read."""

    async def _two_versions(self, store, pool):
        from core.pg_operational import replicate_operational

        await store.recalculate_goal_tables(include_weekly=True)        # DuckDB
        await store.store_predictions(FORECAST, "retail", METRICS)
        shipped = await replicate_operational(store)
        assert shipped["replaced"][SEASONAL] == 12, shipped
        async with pool.acquire() as conn:
            await conn.execute(f"UPDATE {SEASONAL} SET seasonality_index = 1.2345 "
                               "WHERE month = 10")
            await conn.execute(f"UPDATE {GROWTH} SET value = 0.0777")
            await conn.execute(f"UPDATE {PREDICTIONS} SET predicted_revenue = 7")

    @pytest.mark.asyncio
    async def test_latched_every_read_is_postgres_with_the_flag_at_duckdb(self, stores):
        store, pool, env = stores
        await self._two_versions(store, pool)
        chain_latch.latch(CHAIN, pg_forecast_write.WRITE_ENV)
        env.setenv("KS_READ_GOALS", "duckdb")                    # a read rollback

        smart = await store.generate_smart_goals(2026, 10, "retail")
        assert smart["monthly"]["seasonalityIndex"] == pytest.approx(1.2345)
        preds = await store.get_predictions(date(2026, 10, 7), date(2026, 10, 11))
        assert {p["predicted_revenue"] for p in preds} == {7.0}

    @pytest.mark.asyncio
    async def test_flagged_but_held_every_read_is_where_it_was(self, stores):
        store, pool, env = stores
        await self._two_versions(store, pool)
        env.setenv(pg_forecast_write.WRITE_ENV, "postgres")
        env.setenv("KS_READ_GOALS", "duckdb")          # unmet: the chain is held
        assert not pg_forecast_write.writes_postgres()

        smart = await store.generate_smart_goals(2026, 10, "retail")
        assert smart["monthly"]["seasonalityIndex"] != pytest.approx(1.2345)
        preds = await store.get_predictions(date(2026, 10, 7), date(2026, 10, 11))
        assert 7.0 not in {p["predicted_revenue"] for p in preds}

    @pytest.mark.asyncio
    async def test_off_the_goal_tables_stay_on_duckdb_whatever_the_read_flag(
            self, stores):
        """Production today: `KS_READ_GOALS=postgres`, chain off. The goal
        tables are read where they are written — the replica could be an hour
        behind a POST — and the forecast by the flag, as since #183."""
        store, pool, _env = stores
        await self._two_versions(store, pool)

        smart = await store.generate_smart_goals(2026, 10, "retail")
        assert smart["monthly"]["seasonalityIndex"] != pytest.approx(1.2345)
        preds = await store.get_predictions(date(2026, 10, 7), date(2026, 10, 11))
        assert {p["predicted_revenue"] for p in preds} == {7.0}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", [SEASONAL, PREDICTIONS])
    async def test_a_postgres_failure_raises_rather_than_reading_duckdb(
            self, stores, table):
        from core import pg_goals_read

        store, pool, env = stores
        await self._two_versions(store, pool)
        chain_latch.latch(CHAIN, pg_forecast_write.WRITE_ENV)
        read_fallback.reset_counts()
        real = pg_goals_read.fetch

        async def down(sql, params=()):
            if table in sql:
                raise ConnectionError("pg down")
            return await real(sql, params)

        with patch.object(pg_goals_read, "fetch", new=down):
            with pytest.raises(ConnectionError, match="pg down"):
                if table == SEASONAL:
                    await store.generate_smart_goals(2026, 10, "retail")
                else:
                    await store.get_predictions(date(2026, 10, 7), date(2026, 10, 11))
        assert "goals" not in read_fallback.counts(), "it fell back to DuckDB"


# ─── The hourly replace ─────────────────────────────────────────────────────


class TestTheHourlyReplaceCannotRollItBack:
    @pytest.mark.asyncio
    async def test_under_the_flag_the_four_tables_survive(self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        _flag(env)
        await store.recalculate_goal_tables(include_weekly=True)
        await store.store_predictions(FORECAST, "retail", METRICS)
        written = await _all_pg(pool, stamps=True)

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert set(TABLES) <= set(result.get("stood_down", [])), result
        assert not set(TABLES) & set(result["replaced"]), result
        assert await _all_pg(pool, stamps=True) == written

    @pytest.mark.asyncio
    async def test_control_released_and_unflagged_the_same_replace_wipes_them(
            self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        _flag(env)
        await store.recalculate_goal_tables(include_weekly=True)
        await store.store_predictions(FORECAST, "retail", METRICS)
        env.delenv(pg_forecast_write.WRITE_ENV)
        chain_latch.release(CHAIN)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert {t: result["replaced"][t] for t in TABLES} == {t: 0 for t in TABLES}
        assert await _all_pg(pool) == {t: [] for t in TABLES}

    @pytest.mark.asyncio
    async def test_the_flag_put_back_moves_nothing(self, stores):
        """OD-19 (a): latched, `KS_WRITE_FORECAST=duckdb` is a disagreement,
        not a rollback."""
        from core.pg_operational import replicate_operational
        from web.routes.api.health import _write_chains

        store, pool, env = stores
        _flag(env)
        await store.store_predictions(FORECAST, "retail", METRICS)
        env.setenv(pg_forecast_write.WRITE_ENV, "duckdb")
        before = await _all_duck(store)

        await store.recalculate_goal_tables(include_weekly=True)

        assert len(await _pg(pool, SEASONAL)) == 12, "the flag moved the write back"
        assert await _all_duck(store) == before, "a second writer started in DuckDB"
        result = await replicate_operational(store)
        assert set(TABLES) <= set(result["stood_down"])
        assert list(result["chain_flag_mismatch"]) == [CHAIN]
        block = _write_chains()[CHAIN]
        assert block["mode"] == "postgres" and block["latched"] is True
        assert block["mismatch"] is True


# ─── The copy-back and the handover ─────────────────────────────────────────


class TestTheCopyBack:
    @pytest.mark.asyncio
    async def test_the_four_tables_arrive_compare_at_zero_and_the_copy_resumes(
            self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        _flag(env)
        await store.recalculate_goal_tables(include_weekly=True)
        await store.store_predictions(FORECAST, "retail", METRICS)
        written = await _all_pg(pool, stamps=True)

        result = await copy_back(store, pg_forecast_write, dry_run=False)

        assert result["findings"] == [], result["findings"]
        assert result["released"] is True
        assert result["rows"] == {SEASONAL: 12, GROWTH: 1, WEEKLY: 60, PREDICTIONS: 5}
        assert result["sequences"] == {} and result["sync_keys"] == {}
        assert await _all_duck(store, stamps=True) == written
        assert not chain_latch.latched(CHAIN)
        assert await chain_latch.read_owners(pool) == {}

        env.setenv(pg_forecast_write.WRITE_ENV, "duckdb")
        shipped = await replicate_operational(store)
        assert "error" not in shipped, shipped
        assert {t: shipped["replaced"][t] for t in TABLES} == result["rows"]
        assert await _all_pg(pool, stamps=True) == written

        # And the next recalculation is DuckDB's again — the cheaper way back
        # for the content, now that the writer is.
        await store.recalculate_goal_tables(include_weekly=False)
        assert not chain_latch.latched(CHAIN)
        assert await _pg(pool, SEASONAL, stamps=True) == written[SEASONAL]

    @pytest.mark.asyncio
    async def test_a_dry_run_writes_nothing_and_releases_nothing(self, stores):
        store, _pool, env = stores
        _flag(env)
        await store.store_predictions(FORECAST, "retail", METRICS)

        result = await copy_back(store, pg_forecast_write)

        assert result["executed"] is False
        assert result["rows"][PREDICTIONS] == 5
        assert await _duck(store, PREDICTIONS) == []
        assert chain_latch.latched(CHAIN)

    @pytest.mark.asyncio
    async def test_an_unlatched_chain_is_refused(self, stores):
        store, _pool, env = stores
        _flag(env)
        with pytest.raises(CopyBackRefused, match="not latched"):
            await copy_back(store, pg_forecast_write, dry_run=False)


class TestTheHandoverBeforeAFlip:
    @pytest.mark.asyncio
    async def test_an_unshipped_recalculation_fails_it(self, stores):
        store, _pool, _env = stores
        await store.recalculate_goal_tables(include_weekly=True)     # DuckDB

        issues = await handover_check(store, pg_forecast_write)

        assert {(i.check_name, i.severity.value, i.table_name) for i in issues} == {
            ("handover_rows_missing", "CRITICAL", SEASONAL),
            ("handover_rows_missing", "CRITICAL", GROWTH),
            ("handover_rows_missing", "CRITICAL", WEEKLY)}

    @pytest.mark.asyncio
    async def test_a_value_changed_after_the_shipment_fails_it(self, stores):
        from core.pg_operational import replicate_operational

        store, _pool, _env = stores
        await store.recalculate_goal_tables(include_weekly=True)
        await replicate_operational(store)
        assert await handover_check(store, pg_forecast_write) == []
        async with store.connection() as conn:
            conn.execute("UPDATE seasonal_indices SET seasonality_index = 1.1111 "
                         "WHERE month = 1")

        issues = await handover_check(store, pg_forecast_write)

        assert [(i.check_name, i.severity.value, i.table_name) for i in issues] == [
            ("handover_rows_differ", "CRITICAL", SEASONAL)]
        await replicate_operational(store)
        assert await handover_check(store, pg_forecast_write) == []

    @pytest.mark.asyncio
    async def test_a_new_stamp_alone_is_not_a_difference(self, stores):
        """The stamps are forgiven: the same history recomputed an hour later
        differs only in `updated_at`, and that must not hold a flip back."""
        from core.pg_operational import replicate_operational

        store, _pool, _env = stores
        await store.recalculate_goal_tables(include_weekly=True)
        await replicate_operational(store)
        await store.recalculate_goal_tables(include_weekly=True)
        assert await handover_check(store, pg_forecast_write) == []


# ─── The standing watch reads what it judges ────────────────────────────────


class TestTheStandingWatchReadsItsFacts:
    @pytest.mark.asyncio
    async def test_read_forecast_on_seeded_tables(self, stores):
        from core import pg_chain_invariants as inv

        _store, pool, _env = stores
        stamp = datetime(2026, 10, 5, 1, 1, tzinfo=timezone.utc)
        async with pool.acquire() as conn:
            await conn.executemany(
                f"INSERT INTO {SEASONAL} VALUES ($1, 1.0, 3, 1, 1, 1, $2, 'high', $3)",
                [(m, None if m == 7 else Decimal("0.1"), None if m == 8 else stamp)
                 for m in range(1, 12)])
            await conn.execute(
                f"INSERT INTO {GROWTH} VALUES ('yoy_overall', 0.1, '2023-12-02', "
                f"'2025-12-31', 0, $1)", stamp)
            await conn.executemany(
                f"INSERT INTO {PREDICTIONS} VALUES ($1, 'retail', 1, 1, 1, 1, $2)",
                [(date(2026, 10, 1) + timedelta(days=n),
                  None if n == 9 else stamp) for n in range(10)])
            facts = await inv._read_forecast(conn, None, date(2026, 10, 7))

        assert facts == inv.Forecast(
            latched_at=None, months=11, yoy_null=1, seasonal_at=stamp,
            seasonal_unstamped=1, yoy_rows=1, yoy_overall=0.1, yoy_sample=0,
            yoy_from=date(2023, 12, 2), forecast_at=stamp,
            horizon=date(2026, 10, 10), forecast_unstamped=1)
        issues = inv.check_chain_invariants(inv.Facts(
            watched=(CHAIN,), now=datetime(2026, 10, 8, 10, 0, tzinfo=timezone.utc),
            forecast=facts))
        assert {i.check_name for i in issues} == {
            inv.GOAL_TABLES_INCOMPLETE, inv.FORECAST_STALE}

    @pytest.mark.asyncio
    async def test_read_facts_after_a_real_write_is_clean(self, stores):
        from core import pg_chain_invariants as inv

        store, _pool, env = stores
        _flag(env)
        await store.recalculate_goal_tables(include_weekly=True)
        await store.store_predictions(
            [{"date": date.today() + timedelta(days=n), "predicted_revenue": 1.0}
             for n in range(60)], "retail", METRICS)

        facts = await inv.read_facts()

        assert facts.watched == (CHAIN,) and facts.whole is None
        assert isinstance(facts.forecast, inv.Forecast)
        assert facts.forecast.months == 12 and facts.forecast.yoy_null == 0
        assert facts.forecast.latched_at is not None
        assert inv.check_chain_invariants(facts) == []
