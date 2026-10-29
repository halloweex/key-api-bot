"""A GET never writes the goal tables, and the one writer writes them whole.

`seasonal_indices`, `growth_metrics` and `weekly_patterns` have no sales_type
in their keys, so whoever stored last decided what every user's retail goal
was built from. Four things used to store, and only one of them meant to:

  * three GETs (`/goals/seasonality`, `/growth`, `/weekly-patterns`), open to
    any viewer, until they were given `persist=False`;
  * `GET /goals/smart` and `GET /goals/forecast`, through
    `generate_smart_goals`, which recomputed and stored all three tables
    whenever `seasonal_indices` held fewer than twelve rows — for any viewer,
    with the viewer's own sales_type — and on `recalculate=true`;
  * the Monday job's b2b pass, which overwrote the retail rows every week;
  * `POST /goals/recalculate`, with the caller's sales_type.

Chain 7b-1 (OD-14 (i)): the calculators compute and nothing else, and
`recalculate_goal_tables` — the Monday job and the retail-only POST — reads
everything first and then stores it in one transaction. What is proved here:

  * **the sweep** — every GET route under `/api`, read off the app, with
    `seasonal_indices` emptied (the old trigger) and every optional boolean
    switched on, leaves all five goal tables byte-identical;
  * `recalculate=true` is a 400 naming the POST, for an admin too;
  * the POST refuses any sales_type but retail, and writes the three tables;
  * a failure at any write statement leaves every table as it was;
  * the YoY recency weighting takes floats, and never the current year — the
    `TypeError` that would have failed the Monday job from 2026-11-02;
  * what is stored is what the calculators answer, and a month without a
    pair of years keeps the `yoy_growth` it had;
  * the Monday job stores retail and nothing else.

Each guard has a named mutation in the test that bites it.

ONE ENGINE SINCE STEP 14 (S14-07). Every goal read is Postgres', and the goal
tables are stored in Postgres (chain 7b-3, a constant since S14-00), so the
history is seeded in Postgres' Bronze — Postgres derives Silver from it — and
the stored tables are read back from `app.*`. The sweep runs in production's
state: the chain's Postgres writers recorded, every Postgres read answering.
The one-transaction tests hold DuckDB's writer branch, which stays until its
chain's DuckDB branch goes (S14-11), and so keep the goal tables in a DuckDB
store under the test (`route_chains_to_duckdb`). Skipped without `KS_PG_DSN`,
which CI supplies.
"""
from __future__ import annotations

import ast
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore
from core.repositories import goals as goals_module
from tests.integration.pg_tab_seed import (
    derive, needs_postgres, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)
from tests.unit.test_goals_off_duckdb_silver import _seed_history_pg
from tests.unit.test_read_fallback_http import (  # noqa: F401, F811 — fixtures
    _request_for,
    no_network,
    sweep_client,
    swept_routes,
)
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)


REPO = Path(__file__).resolve().parents[2]
KYIV = ZoneInfo("Europe/Kyiv")
DSN = os.getenv("KS_PG_DSN")

# Every table a goal computation stores, or a goal read reads back.
GOAL_TABLES = ("app.seasonal_indices", "app.growth_metrics", "app.weekly_patterns",
               "app.revenue_predictions", "app.revenue_goals")


class _ConnectPerAcquire:
    """`core.pg.get_pool`'s answer for an app served by `TestClient`: every
    acquire opens a connection of its own in whichever loop asks, so the
    client's portal loop and the test's never share one, and nothing is left
    bound to a loop that has closed."""

    def __init__(self, dsn):
        self._dsn = dsn

    @asynccontextmanager
    async def acquire(self, *_a, **_k):
        import asyncpg

        conn = await asyncpg.connect(self._dsn)
        try:
            yield conn
        finally:
            await conn.close()


async def _fingerprint(pool) -> dict:
    async with pool.acquire() as conn:
        out = {}
        for table in GOAL_TABLES:
            rows = await conn.fetch(f"SELECT * FROM {table}")
            out[table] = sorted(repr(tuple(r)) for r in rows)
        return out


async def _seed_goal_tables(conn, pool=None) -> None:
    """Rows in every goal table but `seasonal_indices`, which stays empty —
    the state that used to make any viewer's page load recompute and store
    all three."""
    now = datetime(2026, 1, 5, 4, 0, tzinfo=KYIV)
    for table in ("app.seasonal_indices", "app.weekly_patterns", "app.growth_metrics",
                  "app.revenue_goals", "app.revenue_predictions"):
        await conn.execute(f"DELETE FROM {table}")
    await conn.executemany(
        "INSERT INTO app.weekly_patterns (month, week_of_month, weight, "
        "sample_size, updated_at) VALUES ($1, $2, 0.2, 1, $3)",
        [(m, w, now) for m in range(1, 13) for w in range(1, 6)])
    await conn.execute(
        "INSERT INTO app.growth_metrics (metric_type, value, period_start, "
        "period_end, sample_size, updated_at) "
        "VALUES ('yoy_overall', 0.2345, '2023-12-02', '2025-12-31', 3, $1)", now)
    await conn.execute(
        "INSERT INTO app.revenue_goals (period_type, goal_amount, is_custom, "
        "calculated_goal, growth_factor, updated_at) "
        "VALUES ('monthly', 4000000, TRUE, 3900000, 1.1, $1)", now)
    await conn.executemany(
        "INSERT INTO app.revenue_predictions (prediction_date, sales_type, "
        "predicted_revenue, model_mae, model_mape, model_wape) "
        "VALUES ($1, 'retail', 1000, 1, 1, 1)",
        [(date.today() + timedelta(days=n),) for n in range(10)])


async def _history_and_goal_tables(conn, pool) -> None:
    await _seed_history_pg(conn, pool)
    await _seed_goal_tables(conn)


@pytest_asyncio.fixture
async def served(tmp_path, monkeypatch):
    """The history and the goal tables in Postgres, served to an app under
    `TestClient` (`_ConnectPerAcquire`): yields the pool to read them with."""
    async with tab_in_postgres(DSN, monkeypatch, _history_and_goal_tables) as pool:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_ConnectPerAcquire(DSN))), \
                patch("core.pg.require_revision", new=AsyncMock()):
            yield pool


@pytest_asyncio.fixture
async def history(tmp_path, monkeypatch):
    """The history in Postgres and a store that never reaches DuckDB:
    `(store, pool)`."""
    async with tab_in_postgres(DSN, monkeypatch, _seed_history_pg) as pool:
        with patch("core.pg.require_revision", new=AsyncMock()):
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goal tables"), pool


def _switched_on(endpoint) -> dict:
    """Every optional boolean query parameter set to true — the shape of a
    request most likely to write, `recalculate=true` among them."""
    from typing import Optional

    from tests.routes_helper import is_required

    return {q.name: "true" for q in endpoint.route.dependant.query_params
            if not is_required(q)
            and q.field_info.annotation in (bool, Optional[bool])}


# ─── The sweep ────────────────────────────────────────────────────────────

def _writers_recorded(monkeypatch) -> list:
    """Chain 7b-3's two Postgres writers, and chain 7a's, replaced by
    recorders that raise: a GET that reached one is named."""
    from core import pg_forecast_write, pg_goals_write

    reached = []

    def recorder(name):
        async def _recorded(*a, **kw):
            reached.append(name)
            raise AssertionError(f"a GET reached {name}")
        return _recorded

    for module, name in ((pg_forecast_write, "persist_goal_tables"),
                         (pg_forecast_write, "store_predictions"),
                         (pg_goals_write, "set_goal")):
        monkeypatch.setattr(module, name, recorder(f"{module.__name__}.{name}"))
    return reached


class TestNoGetWrites:
    """Every GET route under /api, read off the app — never a list, so a GET
    added tomorrow is held to it the day it is registered — in production's
    state: the goal tables' writers are Postgres', and replaced by recorders
    that raise; every Postgres goal read answers, with nothing stored in
    `seasonal_indices` (the old trigger). Until S14-07 a sibling of this
    swept the same routes against the goal tables in a DuckDB store; that
    store is written by nothing in production.

    Mutations: M4 — restore `generate_smart_goals`' `indices_exist < 12`
    branch, and `/api/goals/smart` reaches `persist_goal_tables`, named here;
    M5 — let `/api/goals/forecast` honour `recalculate`, and its switched-on
    request does too."""

    def test_no_get_reaches_a_postgres_writer(
        self, sweep_client, no_network, fresh_read_fallback, monkeypatch,
    ):
        from web.main import app
        from web.ratelimit import limiter

        reached = _writers_recorded(monkeypatch)
        asked = []

        async def _nothing(sql, params=()):
            asked.append(sql)
            return []

        monkeypatch.setattr("core.pg_goals_read.fetch", _nothing)

        routes = swept_routes(app)
        for endpoint in routes:
            path, params = _request_for(endpoint)
            for extra in ({}, _switched_on(endpoint)):
                limiter.reset()
                sweep_client.get(path, params={**params, **extra})
        assert reached == []
        assert any("{" not in sql and "app.seasonal_indices" in sql for sql in asked), (
            "no GET read the goal tables — the sweep proved nothing")
        assert len(routes) >= 90, len(routes)
        assert "/api/goals/smart" in {e.path for e in routes}
        assert "/api/goals/forecast" in {e.path for e in routes}

    def test_the_sweep_switches_recalculate_on(self):
        """The second request per route is not vacuous for the one GET that
        used to take a write switch."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        endpoint = find_endpoint(app, "/api/goals/forecast", "GET")
        assert _switched_on(endpoint) == {"recalculate": "true"}


@needs_postgres
class TestTheForecastGetRefusesToRecalculate:
    """OD-14 (i). Mutation M6: accept `recalculate=true` for an admin."""

    @pytest.mark.asyncio
    async def test_recalculate_true_is_a_400_naming_the_post(
        self, served, admin_client, fresh_read_fallback,
    ):
        before = await _fingerprint(served)

        response = admin_client.get(
            "/api/goals/forecast",
            params={"year": 2026, "month": 10, "recalculate": "true"})

        assert response.status_code == 400, response.text
        assert "POST /api/goals/recalculate" in response.json()["detail"]
        assert await _fingerprint(served) == before

    @pytest.mark.asyncio
    async def test_recalculate_false_reads_and_answers(
        self, served, admin_client, fresh_read_fallback,
    ):
        before = await _fingerprint(served)

        response = admin_client.get(
            "/api/goals/forecast",
            params={"year": 2026, "month": 10, "recalculate": "false"})

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["targetMonth"] == 10 and body["monthly"]["lastYearRevenue"] > 0
        # Nothing stored in `seasonal_indices`: the month is read as missing.
        assert body["monthly"]["seasonalityIndex"] == 1.0
        assert body["monthly"]["confidence"] == "low"
        assert await _fingerprint(served) == before


@needs_postgres
class TestThePostIsRetailOnly:
    """The three tables mean retail. Mutation M7: accept b2b."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["b2b", "all", "internal"])
    async def test_any_other_sales_type_is_refused_with_nothing_written(
        self, served, admin_client, fresh_read_fallback, sales_type,
    ):
        before = await _fingerprint(served)

        response = admin_client.post(
            "/api/goals/recalculate", params={"sales_type": sales_type})

        assert response.status_code == 400, response.text
        assert "retail" in response.json()["detail"]
        assert await _fingerprint(served) == before

    @pytest.mark.asyncio
    async def test_retail_writes_the_three_tables(
        self, served, admin_client, fresh_read_fallback,
    ):
        response = admin_client.post(
            "/api/goals/recalculate", params={"sales_type": "retail"})

        assert response.status_code == 200, response.text
        assert response.json()["summary"]["monthsCalculated"] == 12
        async with served.acquire() as conn:
            assert await conn.fetchval(
                "SELECT COUNT(*) FROM app.seasonal_indices WHERE yoy_growth IS NOT NULL") == 12
            assert await conn.fetchval(
                "SELECT sample_size FROM app.growth_metrics "
                "WHERE metric_type = 'yoy_overall'") >= 1
            assert await conn.fetchval(
                "SELECT COUNT(*) FROM app.weekly_patterns WHERE weight <> 0.2") == 60


# ─── One transaction ──────────────────────────────────────────────────────

def _write_statements() -> dict:
    """Every module-level statement in goals.py that writes one of the three
    shared tables — parsed, so a statement added to the writer is held to
    the transaction the day it is written."""
    tree = ast.parse((REPO / "core" / "repositories" / "goals.py").read_text())
    out = {}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            text = node.value.value.upper()
            if any(f"{verb} {t}".upper() in " ".join(text.split())
                   for verb in ("INSERT INTO", "UPDATE")
                   for t in ("seasonal_indices", "weekly_patterns", "growth_metrics")):
                out[node.targets[0].id] = node.value.value
    return out


class _FailsAt:
    """The store's connection, raising at one statement."""

    def __init__(self, conn, statement: str):
        self._conn, self._statement = conn, statement

    def _check(self, sql):
        if " ".join(sql.split()) == " ".join(self._statement.split()):
            raise RuntimeError("injected failure mid-transaction")

    def execute(self, sql, *args, **kwargs):
        self._check(sql)
        return self._conn.execute(sql, *args, **kwargs)

    def executemany(self, sql, *args, **kwargs):
        self._check(sql)
        return self._conn.executemany(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _duck_fingerprint(conn) -> dict:
    return {t: sorted(repr(r) for r in conn.execute(f"SELECT * FROM {t}").fetchall())
            for t in ("seasonal_indices", "growth_metrics", "weekly_patterns")}


async def _store_fingerprint(store) -> dict:
    async with store.connection() as conn:
        return _duck_fingerprint(conn)


@pytest_asyncio.fixture
async def duck(tmp_path):
    """A DuckDB store for the writer branch that still writes there."""
    s = DuckDBStore(db_path=tmp_path / "goal-tables.duckdb")
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


class TestTheWriterIsOneTransaction:
    """DuckDB's writer branch, which stays until chain 7b-3's DuckDB branch
    goes (S14-11): the goal tables in a DuckDB store under the test, the
    history they are computed from in Postgres. Postgres' writer is held to
    its own transaction in `tests/integration/test_forecast_writer.py`.

    Mutation M8: drop the BEGIN/COMMIT, and the statements before the
    failing one stay — this week's indices beside last week's growth."""

    def test_every_write_statement_is_found(self):
        found = _write_statements()
        assert set(found) == {"_SEASONAL_UPSERT_SQL", "_YOY_OVERALL_UPSERT_SQL",
                              "_MONTHLY_YOY_UPDATE_SQL", "_WEEKLY_UPSERT_SQL"}, found

    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("statement", sorted(_write_statements()))
    async def test_a_failure_anywhere_leaves_every_table_as_it_was(
        self, duck, statement, monkeypatch,
    ):
        from core import pg_forecast_write
        from tests.conftest import route_chains_to_duckdb

        route_chains_to_duckdb(monkeypatch, pg_forecast_write)
        store = duck
        async with tab_in_postgres(DSN, monkeypatch, _seed_history_pg) as pool:
            await self._fails_whole(store, pool, statement)

    async def _fails_whole(self, store, pool, statement):
        await store.recalculate_goal_tables(include_weekly=True)
        # A week later, more orders: every stored number would move.
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE bronze.orders SET grand_total = grand_total * 1.5 "
                "WHERE ordered_at >= '2025-06-01'")
        await derive(pool)
        before = await _store_fingerprint(store)

        real = type(store).connection
        text = getattr(goals_module, statement)

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def failing(self):
            async with real(self) as conn:
                yield _FailsAt(conn, text)

        with patch.object(type(store), "connection", failing):
            with pytest.raises(RuntimeError, match="injected"):
                await store.recalculate_goal_tables(include_weekly=True)

        assert await _store_fingerprint(store) == before
        # And the connection is usable afterwards: nothing left open.
        tables = await store.recalculate_goal_tables(include_weekly=True)
        assert await _store_fingerprint(store) != before
        assert len(tables["seasonal"]) == 12


# ─── What is stored is what is computed ───────────────────────────────────

@needs_postgres
class TestWhatIsStored:
    @pytest.mark.asyncio
    async def test_the_rows_are_the_calculators_answers(self, history):
        """T8: number-neutral against the calculators the tables used to be
        stored by, read back column by column — from Postgres, where chain
        7b-3 stores them."""
        store, pool = history
        seasonal = await store.calculate_seasonality_indices("retail")
        yoy = await store.calculate_yoy_growth("retail")
        weekly = await store.calculate_weekly_patterns("retail")

        await store.recalculate_goal_tables(include_weekly=True)

        async with pool.acquire() as conn:
            rows = {r[0]: tuple(r)[1:] for r in await conn.fetch(
                "SELECT month, seasonality_index, sample_size, avg_revenue, "
                "min_revenue, max_revenue, confidence, yoy_growth "
                "FROM app.seasonal_indices")}
            growth = await conn.fetchrow(
                "SELECT value, period_start, period_end, sample_size "
                "FROM app.growth_metrics WHERE metric_type = 'yoy_overall'")
            stored_weekly = {(r[0], r[1]): float(r[2]) for r in await conn.fetch(
                "SELECT month, week_of_month, weight FROM app.weekly_patterns")}
            bounds = await conn.fetchrow(
                "SELECT MIN((timezone('Europe/Kyiv', ordered_at))::date), "
                "MAX((timezone('Europe/Kyiv', ordered_at))::date) FROM bronze.orders")

        assert set(rows) == set(seasonal) == set(range(1, 13))
        for month, data in seasonal.items():
            index, n, avg, low, high, confidence, month_yoy = rows[month]
            assert float(index) == pytest.approx(data["seasonality_index"], abs=1e-4)
            assert n == data["sample_size"] and confidence == data["confidence"]
            assert (float(avg), float(low), float(high)) == pytest.approx(
                (data["avg_revenue"], data["min_revenue"], data["max_revenue"]), abs=0.01)
            assert float(month_yoy) == pytest.approx(yoy["monthly_yoy"][month], abs=1e-4)
        assert float(growth[0]) == pytest.approx(yoy["overall_yoy"], abs=1e-4)
        assert (growth[1], growth[2]) == (bounds[0], bounds[1])
        assert growth[3] == yoy["sample_size"] >= 1
        for (month, week), weight in stored_weekly.items():
            assert weight == pytest.approx(weekly[month][week], abs=1e-4)

    @pytest.mark.asyncio
    async def test_a_month_without_a_pair_keeps_its_yoy(self, tmp_path, monkeypatch):
        """Only months with a pair of years have their `yoy_growth` set; the
        upsert of the indices never touches the column. Mutation M10: let the
        merged write null it."""
        async def populate(conn, pool):
            await seed(conn, orders=[
                (n + 1, 1, 1, 1000.0, datetime(2025, 3, 1 + n, 12, tzinfo=KYIV), 9, None, None)
                for n in range(25)])
            await derive(pool)
            await conn.execute(
                "INSERT INTO app.seasonal_indices (month, seasonality_index, "
                "sample_size, yoy_growth, confidence) VALUES (3, 0.9, 1, 0.42, 'low')")

        async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
            store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goal tables")
            with patch("core.pg.require_revision", new=AsyncMock()):
                tables = await store.recalculate_goal_tables(include_weekly=False)

            assert tables["yoy"]["monthly_yoy"] == {}, "the fixture has no pair"
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT seasonality_index, yoy_growth "
                                          "FROM app.seasonal_indices WHERE month = 3")
        assert float(row[0]) == pytest.approx(1.0), "the index was rewritten"
        assert float(row[1]) == pytest.approx(0.42), "the stored YoY was lost"


# ─── The YoY weighting ────────────────────────────────────────────────────

class _Clock(datetime):
    """`datetime` whose `now()` is `_Clock.at`, in Kyiv."""

    at: datetime = datetime(2026, 11, 2, 12, 0, tzinfo=KYIV)

    @classmethod
    def now(cls, tz=None):
        return cls.at.astimezone(tz) if tz else cls.at.replace(tzinfo=None)


def _monthly_orders(until: date) -> list:
    """One retail order a month from January 2024 to `until`, a different
    amount each year so the two rates differ; the last on the 1st."""
    rows, oid = [], 1
    year, month = 2024, 1
    while date(year, month, 1) <= until:
        day = 1 if (year, month) == (until.year, until.month) else 15
        total = Decimal({2024: "1000.10", 2025: "1500.20", 2026: "2400.30"}[year])
        rows.append((oid, total, datetime.combine(date(year, month, day),
                                                  time(12, 0), tzinfo=KYIV)))
        oid += 1
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return rows


@asynccontextmanager
async def _monthly(tmp_path, monkeypatch, until: date):
    """The monthly orders in Postgres' Bronze, Silver derived, and a store
    that never reaches DuckDB."""
    async def populate(conn, pool):
        await seed(conn, orders=[(oid, 1, 1, total, when, 9, None, None)
                                 for oid, total, when in _monthly_orders(until)])
        await derive(pool)

    async with tab_in_postgres(DSN, monkeypatch, populate):
        with patch("core.pg.require_revision", new=AsyncMock()):
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the YoY")


@needs_postgres
class TestTheYoyWeighting:
    @pytest.mark.asyncio
    async def test_from_the_first_of_november_the_running_year_stays_out(
        self, tmp_path, monkeypatch,
    ):
        """T1. On 2026-11-02 the running year has eleven months and used to
        count as full: a second pair appeared and the DECIMAL × float
        weighting raised. Mutation M2: drop the year bound."""
        async with _monthly(tmp_path, monkeypatch, date(2026, 11, 1)) as store:
            await self._t1(store, monkeypatch)

    async def _t1(self, store, monkeypatch):
        monkeypatch.setattr(_Clock, "at", datetime(2026, 11, 2, 12, 0, tzinfo=KYIV))
        with patch("core.repositories.goals.datetime", _Clock):
            yoy = await store.calculate_yoy_growth("retail")
            tables = await store.recalculate_goal_tables(include_weekly=False)

        assert [y["year"] for y in yoy["yearly_data"]] == [2024, 2025]
        assert yoy["sample_size"] == 1
        expected = (12 * 1500.20 - 12 * 1000.10) / (12 * 1000.10)
        assert yoy["overall_yoy"] == pytest.approx(round(expected, 4), abs=1e-9)
        assert tables["yoy_overall"] == pytest.approx(expected, rel=1e-12)

    @pytest.mark.asyncio
    async def test_two_pairs_are_weighted_one_and_two(self, tmp_path, monkeypatch):
        """T2. In 2027 there are two pairs, and the newer weighs twice the
        older. Mutation M1: drop the float conversion — TypeError; M3:
        reverse the weights."""
        async with _monthly(tmp_path, monkeypatch, date(2026, 12, 15)) as store:
            await self._t2(store, monkeypatch)

    async def _t2(self, store, monkeypatch):
        monkeypatch.setattr(_Clock, "at", datetime(2027, 1, 5, 12, 0, tzinfo=KYIV))
        with patch("core.repositories.goals.datetime", _Clock):
            yoy = await store.calculate_yoy_growth("retail")

        assert [y["year"] for y in yoy["yearly_data"]] == [2024, 2025, 2026]
        older = (1500.20 - 1000.10) / 1000.10
        newer = (2400.30 - 1500.20) / 1500.20
        assert older != pytest.approx(newer)
        assert yoy["overall_yoy"] == pytest.approx(
            round((older * 1.0 + newer * 2.0) / 3.0, 4), abs=1e-9)
        assert yoy["sample_size"] == 2


# ─── The Monday job ───────────────────────────────────────────────────────

@needs_postgres
class TestTheMondayJobStoresRetail:
    """It used to follow the retail pass with a b2b one, over the same rows."""

    @pytest.mark.asyncio
    async def test_the_stored_rows_are_retails(self, tmp_path, monkeypatch):
        from core.scheduler import BackgroundScheduler

        async with tab_in_postgres(DSN, monkeypatch, _history_and_goal_tables) as pool:
            store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the Monday job")
            with patch("core.pg.require_revision", new=AsyncMock()):
                retail = await store.calculate_seasonality_indices("retail")
                b2b = await store.calculate_seasonality_indices("b2b")
                assert retail[1]["avg_revenue"] != b2b[1]["avg_revenue"], (
                    "the fixture cannot tell retail from b2b")
                weekly_before = (await _fingerprint(pool))["app.weekly_patterns"]

                async def _get_store():
                    return store

                monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
                result = await BackgroundScheduler()._run_seasonality_calc()

            assert result["retail_months"] == 12
            async with pool.acquire() as conn:
                stored = {r[0]: float(r[1]) for r in await conn.fetch(
                    "SELECT month, avg_revenue FROM app.seasonal_indices")}
            assert stored == pytest.approx(
                {m: d["avg_revenue"] for m, d in retail.items()}, abs=0.01)
            # The job never stored the weekly weights (OQ-2), and still does not.
            assert (await _fingerprint(pool))["app.weekly_patterns"] == weekly_before


# ─── Unchanged neighbours, kept pinned ────────────────────────────────────

class TestPredictionsAndModelFilesLandWhole:
    def test_store_predictions_is_one_transaction(self):
        from core.repositories.goals import GoalsMixin
        import inspect

        src = inspect.getsource(GoalsMixin.store_predictions)
        assert "BEGIN TRANSACTION" in src and "ROLLBACK" in src

    def test_json_files_are_replaced_not_truncated(self, tmp_path):
        from core.prediction_service import _write_json_atomically

        target = tmp_path / "params.json"
        target.write_text("{\"old\": true}")
        _write_json_atomically(target, {"new": 1})
        assert target.read_text().strip().startswith("{")
        assert "new" in target.read_text()
        assert [p.name for p in tmp_path.iterdir()] == ["params.json"], "no temp file left behind"

    def test_the_model_writer_uses_replace(self):
        from core.prediction_service import PredictionService
        import inspect

        src = inspect.getsource(PredictionService._save_model)
        assert "os.replace(tmp, MODEL_PATH)" in src
        assert "_write_json_atomically" in src
