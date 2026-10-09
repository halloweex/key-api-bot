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
"""
from __future__ import annotations

import ast
import asyncio
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from core.duckdb_constants import B2B_MANAGER_ID
from core.duckdb_store import DuckDBStore
from core.repositories import goals as goals_module
from tests.unit.test_goals_off_duckdb_silver import (  # noqa: F401 — fixture
    _seed_history,
    store,
)
from tests.unit.test_read_fallback_http import (  # noqa: F401 — fixtures
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

# Every table a goal computation stores, or a goal read reads back.
GOAL_TABLES = ("seasonal_indices", "growth_metrics", "weekly_patterns",
               "revenue_predictions", "revenue_goals")

# The two values of the retired `KS_GOALS_HISTORY` that read — unset and
# `silver`, both Silver through the goal router since chain 7b-4 deleted the
# DN-12 bridge. Any other value refuses every history read
# (`tests/unit/test_goals_history_silver.py`).
HISTORIES = (None, "silver")


def _history(monkeypatch, mode) -> None:
    if mode is None:
        monkeypatch.delenv("KS_GOALS_HISTORY", raising=False)
    else:
        monkeypatch.setenv("KS_GOALS_HISTORY", mode)


def _fingerprint(conn) -> dict:
    return {t: sorted(repr(r) for r in conn.execute(f"SELECT * FROM {t}").fetchall())
            for t in GOAL_TABLES}


async def _store_fingerprint(store) -> dict:
    async with store.connection() as conn:
        return _fingerprint(conn)


async def _seed_goal_tables(store) -> None:
    """Rows in every goal table but `seasonal_indices`, which stays empty —
    the state that used to make any viewer's page load recompute and store
    all three."""
    now = datetime(2026, 1, 5, 4, 0, tzinfo=KYIV)
    async with store.connection() as conn:
        conn.execute("DELETE FROM seasonal_indices")
        conn.execute("DELETE FROM weekly_patterns")
        conn.execute("DELETE FROM growth_metrics")
        conn.executemany(
            "INSERT INTO weekly_patterns (month, week_of_month, weight, "
            "sample_size, updated_at) VALUES (?, ?, 0.2, 1, ?)",
            [(m, w, now) for m in range(1, 13) for w in range(1, 6)])
        conn.execute(
            "INSERT INTO growth_metrics (metric_type, value, period_start, "
            "period_end, sample_size, updated_at) "
            "VALUES ('yoy_overall', 0.2345, '2023-12-02', '2025-12-31', 3, ?)",
            [now])
        conn.execute("DELETE FROM revenue_goals")
        conn.execute(
            "INSERT INTO revenue_goals (period_type, goal_amount, is_custom, "
            "calculated_goal, growth_factor, updated_at) "
            "VALUES ('monthly', 4000000, TRUE, 3900000, 1.1, ?)", [now])
        conn.execute("DELETE FROM revenue_predictions")
        conn.executemany(
            "INSERT INTO revenue_predictions (prediction_date, sales_type, "
            "predicted_revenue, model_mae, model_mape, model_wape) "
            "VALUES (?, 'retail', 1000, 1, 1, 1)",
            [(date.today() + timedelta(days=n),) for n in range(10)])


def _seed_app_store(*steps) -> None:
    """Run `steps` against the per-test DuckDB file the app is about to open."""

    async def run():
        s = DuckDBStore()
        await s.connect()
        try:
            for step in steps:
                await step(s)
        finally:
            await s.close()

    asyncio.run(run())


def _app_connection(client):
    """A cursor on the database the app holds. One request first, so the
    app's store exists."""
    from core import duckdb_store

    client.get("/api/goals")
    return duckdb_store._store_instance._connection.cursor()


def _switched_on(endpoint) -> dict:
    """Every optional boolean query parameter set to true — the shape of a
    request most likely to write, `recalculate=true` among them."""
    from typing import Optional

    from tests.routes_helper import is_required

    return {q.name: "true" for q in endpoint.route.dependant.query_params
            if not is_required(q)
            and q.field_info.annotation in (bool, Optional[bool])}


# ─── The sweep ────────────────────────────────────────────────────────────

class TestNoGetWrites:
    """Every GET route under /api, read off the app — never a list, so a GET
    added tomorrow is held to it the day it is registered.

    Mutations: M4 — restore `generate_smart_goals`' `indices_exist < 12`
    branch, and `/api/goals/smart` fails here, named; M5 — let
    `/api/goals/forecast` honour `recalculate`, and its switched-on request
    fails here, named."""

    @pytest.mark.parametrize("history", HISTORIES)
    def test_every_get_leaves_every_goal_table_as_it_was(
        self, history, sweep_client, no_network, fresh_read_fallback, monkeypatch,
    ):
        from web.main import app
        from web.ratelimit import limiter

        _history(monkeypatch, history)
        _seed_app_store(_seed_history, _seed_goal_tables)
        conn = _app_connection(sweep_client)
        before = _fingerprint(conn)
        assert before["seasonal_indices"] == [], "the trigger is not set"
        assert all(before[t] for t in GOAL_TABLES if t != "seasonal_indices"), (
            "an empty table cannot show a write")

        routes = swept_routes(app)
        changed, asked = [], 0
        for endpoint in routes:
            path, params = _request_for(endpoint)
            for extra in ({}, _switched_on(endpoint)):
                limiter.reset()
                sweep_client.get(path, params={**params, **extra})
                asked += 1
                after = _fingerprint(conn)
                if after != before:
                    moved = sorted(t for t in GOAL_TABLES if after[t] != before[t])
                    changed.append(f"GET {path} {extra or ''} wrote {moved}")
                    before = after

        assert not changed, "a GET wrote a goal table: " + "; ".join(changed)
        assert len(routes) >= 90, len(routes)
        assert "/api/goals/smart" in {e.path for e in routes}
        assert "/api/goals/forecast" in {e.path for e in routes}

    def test_under_chain_7b3_no_get_reaches_its_postgres_writers(
        self, sweep_client, no_network, fresh_read_fallback, monkeypatch,
    ):
        """The same sweep with chain 7b-3 latched: the goal tables' writes
        then go to Postgres, so a GET that stored again would not show in
        DuckDB at all. Both of the chain's writers are replaced by recorders
        that raise, and every Postgres read answers nothing.

        Mutation: give `generate_smart_goals` back a store and the smart
        goal's GET reaches `persist_goal_tables`, named here."""
        from core import chain_latch, pg_forecast_write
        from web.main import app
        from web.ratelimit import limiter

        reached = []

        async def _recorder(*a, **kw):
            reached.append(a)
            raise AssertionError("a GET reached chain 7b-3's writer")

        async def _nothing(sql, params=()):
            return []

        monkeypatch.setattr(pg_forecast_write, "persist_goal_tables", _recorder)
        monkeypatch.setattr(pg_forecast_write, "store_predictions", _recorder)
        monkeypatch.setattr("core.pg_goals_read.fetch", _nothing)
        _history(monkeypatch, None)
        chain_latch.latch(pg_forecast_write.CHAIN, pg_forecast_write.WRITE_ENV)
        assert pg_forecast_write.writes_postgres()

        _seed_app_store(_seed_history, _seed_goal_tables)
        conn = _app_connection(sweep_client)
        before = _fingerprint(conn)
        routes = swept_routes(app)
        for endpoint in routes:
            path, params = _request_for(endpoint)
            for extra in ({}, _switched_on(endpoint)):
                limiter.reset()
                sweep_client.get(path, params={**params, **extra})
        assert reached == []
        assert _fingerprint(conn) == before
        assert "/api/goals/smart" in {e.path for e in routes}

    def test_the_sweep_switches_recalculate_on(self):
        """The second request per route is not vacuous for the one GET that
        used to take a write switch."""
        from tests.routes_helper import find_endpoint
        from web.main import app

        endpoint = find_endpoint(app, "/api/goals/forecast", "GET")
        assert _switched_on(endpoint) == {"recalculate": "true"}


class TestTheForecastGetRefusesToRecalculate:
    """OD-14 (i). Mutation M6: accept `recalculate=true` for an admin."""

    def test_recalculate_true_is_a_400_naming_the_post(
        self, admin_client, fresh_read_fallback,
    ):
        _seed_app_store(_seed_history, _seed_goal_tables)
        conn = _app_connection(admin_client)
        before = _fingerprint(conn)

        response = admin_client.get(
            "/api/goals/forecast",
            params={"year": 2026, "month": 10, "recalculate": "true"})

        assert response.status_code == 400, response.text
        assert "POST /api/goals/recalculate" in response.json()["detail"]
        assert _fingerprint(conn) == before

    def test_recalculate_false_reads_and_answers(
        self, admin_client, fresh_read_fallback,
    ):
        _seed_app_store(_seed_history, _seed_goal_tables)
        conn = _app_connection(admin_client)
        before = _fingerprint(conn)

        response = admin_client.get(
            "/api/goals/forecast",
            params={"year": 2026, "month": 10, "recalculate": "false"})

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["targetMonth"] == 10 and body["monthly"]["lastYearRevenue"] > 0
        # Nothing stored in `seasonal_indices`: the month is read as missing.
        assert body["monthly"]["seasonalityIndex"] == 1.0
        assert body["monthly"]["confidence"] == "low"
        assert _fingerprint(conn) == before


class TestThePostIsRetailOnly:
    """The three tables mean retail. Mutation M7: accept b2b."""

    @pytest.mark.parametrize("sales_type", ["b2b", "all", "internal"])
    def test_any_other_sales_type_is_refused_with_nothing_written(
        self, admin_client, fresh_read_fallback, sales_type,
    ):
        _seed_app_store(_seed_history, _seed_goal_tables)
        conn = _app_connection(admin_client)
        before = _fingerprint(conn)

        response = admin_client.post(
            "/api/goals/recalculate", params={"sales_type": sales_type})

        assert response.status_code == 400, response.text
        assert "retail" in response.json()["detail"]
        assert _fingerprint(conn) == before

    def test_retail_writes_the_three_tables(self, admin_client, fresh_read_fallback):
        _seed_app_store(_seed_history, _seed_goal_tables)
        conn = _app_connection(admin_client)

        response = admin_client.post(
            "/api/goals/recalculate", params={"sales_type": "retail"})

        assert response.status_code == 200, response.text
        assert response.json()["summary"]["monthsCalculated"] == 12
        assert conn.execute(
            "SELECT COUNT(*) FROM seasonal_indices WHERE yoy_growth IS NOT NULL"
        ).fetchone()[0] == 12
        assert conn.execute("SELECT sample_size FROM growth_metrics "
                            "WHERE metric_type = 'yoy_overall'").fetchone()[0] >= 1
        assert conn.execute("SELECT COUNT(*) FROM weekly_patterns "
                            "WHERE weight <> 0.2").fetchone()[0] == 60


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


class TestTheWriterIsOneTransaction:
    """Mutation M8: drop the BEGIN/COMMIT, and the statements before the
    failing one stay — this week's indices beside last week's growth."""

    def test_every_write_statement_is_found(self):
        found = _write_statements()
        assert set(found) == {"_SEASONAL_UPSERT_SQL", "_YOY_OVERALL_UPSERT_SQL",
                              "_MONTHLY_YOY_UPDATE_SQL", "_WEEKLY_UPSERT_SQL"}, found

    @pytest.mark.asyncio
    @pytest.mark.parametrize("statement", sorted(_write_statements()))
    async def test_a_failure_anywhere_leaves_every_table_as_it_was(
        self, store, statement, monkeypatch,
    ):
        _history(monkeypatch, None)
        await _seed_history(store)
        await store.recalculate_goal_tables(include_weekly=True)
        # A week later, more orders: every stored number would move.
        async with store.connection() as conn:
            conn.execute(
                "UPDATE orders SET grand_total = grand_total * 1.5 "
                "WHERE ordered_at >= '2025-06-01'")
        await store.refresh_warehouse_layers(trigger="manual")
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

class TestWhatIsStored:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("history", HISTORIES)
    async def test_the_rows_are_the_calculators_answers(self, store, monkeypatch, history):
        """T8: number-neutral against the calculators the tables used to be
        stored by, read back column by column."""
        _history(monkeypatch, history)
        await _seed_history(store)
        seasonal = await store.calculate_seasonality_indices("retail")
        yoy = await store.calculate_yoy_growth("retail")
        weekly = await store.calculate_weekly_patterns("retail")

        await store.recalculate_goal_tables(include_weekly=True)

        async with store.connection() as conn:
            rows = {r[0]: r[1:] for r in conn.execute(
                "SELECT month, seasonality_index, sample_size, avg_revenue, "
                "min_revenue, max_revenue, confidence, yoy_growth "
                "FROM seasonal_indices").fetchall()}
            growth = conn.execute(
                "SELECT value, period_start, period_end, sample_size "
                "FROM growth_metrics WHERE metric_type = 'yoy_overall'").fetchone()
            stored_weekly = {(r[0], r[1]): float(r[2]) for r in conn.execute(
                "SELECT month, week_of_month, weight FROM weekly_patterns").fetchall()}
            bounds = conn.execute(
                "SELECT MIN(DATE(timezone('Europe/Kyiv', ordered_at))), "
                "MAX(DATE(timezone('Europe/Kyiv', ordered_at))) FROM orders").fetchone()

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
    @pytest.mark.parametrize("history", HISTORIES)
    async def test_a_month_without_a_pair_keeps_its_yoy(self, store, monkeypatch, history):
        """Only months with a pair of years have their `yoy_growth` set; the
        upsert of the indices never touches the column. Mutation M10: let the
        merged write null it."""
        _history(monkeypatch, history)
        async with store.connection() as conn:
            conn.executemany(
                "INSERT INTO orders (id, source_id, status_id, grand_total, "
                "ordered_at, buyer_id) VALUES (?, 1, 1, 1000, ?, 9)",
                [(n + 1, datetime(2025, 3, 1 + n, 12, tzinfo=KYIV)) for n in range(25)])
            conn.execute(
                "INSERT INTO seasonal_indices (month, seasonality_index, "
                "sample_size, yoy_growth, confidence) VALUES (3, 0.9, 1, 0.42, 'low')")
        await store.refresh_warehouse_layers(trigger="manual")

        tables = await store.recalculate_goal_tables(include_weekly=False)

        assert tables["yoy"]["monthly_yoy"] == {}, "the fixture has no pair"
        async with store.connection() as conn:
            row = conn.execute("SELECT seasonality_index, yoy_growth "
                               "FROM seasonal_indices WHERE month = 3").fetchone()
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


async def _seed_monthly(store, until: date) -> None:
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, "
            "ordered_at, buyer_id) VALUES (?, 1, 1, ?, ?, 9)", _monthly_orders(until))
    await store.refresh_warehouse_layers(trigger="manual")


class TestTheYoyWeighting:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("history", HISTORIES)
    async def test_from_the_first_of_november_the_running_year_stays_out(
        self, store, monkeypatch, history,
    ):
        """T1. On 2026-11-02 the running year has eleven months and used to
        count as full: a second pair appeared and the DECIMAL × float
        weighting raised. Mutation M2: drop the year bound."""
        _history(monkeypatch, history)
        await _seed_monthly(store, date(2026, 11, 1))
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
    @pytest.mark.parametrize("history", HISTORIES)
    async def test_two_pairs_are_weighted_one_and_two(self, store, monkeypatch, history):
        """T2. In 2027 there are two pairs, and the newer weighs twice the
        older. Mutation M1: drop the float conversion — TypeError; M3:
        reverse the weights."""
        _history(monkeypatch, history)
        await _seed_monthly(store, date(2026, 12, 15))
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

class TestTheMondayJobStoresRetail:
    """It used to follow the retail pass with a b2b one, over the same rows."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("history", HISTORIES)
    async def test_the_stored_rows_are_retails(self, store, monkeypatch, history):
        from core.scheduler import BackgroundScheduler

        _history(monkeypatch, history)
        await _seed_history(store)
        await _seed_goal_tables(store)
        retail = await store.calculate_seasonality_indices("retail")
        b2b = await store.calculate_seasonality_indices("b2b")
        assert retail[1]["avg_revenue"] != b2b[1]["avg_revenue"], (
            "the fixture cannot tell retail from b2b")
        weekly_before = (await _store_fingerprint(store))["weekly_patterns"]

        async def _get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        result = await BackgroundScheduler()._run_seasonality_calc()

        assert result["retail_months"] == 12
        async with store.connection() as conn:
            stored = {r[0]: float(r[1]) for r in conn.execute(
                "SELECT month, avg_revenue FROM seasonal_indices").fetchall()}
        assert stored == pytest.approx({m: d["avg_revenue"] for m, d in retail.items()},
                                       abs=0.01)
        # The job never stored the weekly weights (OQ-2), and still does not.
        assert (await _store_fingerprint(store))["weekly_patterns"] == weekly_before


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
