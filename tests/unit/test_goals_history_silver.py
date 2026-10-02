"""The goal calculators' history from Silver (chain 7b-2, `KS_GOALS_HISTORY`).

Since DN-12 the calculators read DuckDB `orders` through a bridge that renders
`silver_sales_type_case` over DuckDB's `managers` and
`manager_classifications`. Step 13 freezes all three. `KS_GOALS_HISTORY=silver`
reads the same history from `{silver_orders}`, through the goal router, so the
engine is `KS_READ_GOALS`' and DN-20's counting and refusal cover it.

What is proved here, without a database server (the two-engine half is in
`tests/integration/test_goals_history_two_engines.py`):

  * **equivalence** (T9) — on every branch of the sales-type CASE, Silver
    selects the orders the bridge selects, for every sales type, with one
    accepted difference named row by row (the return rule: KeyCRM's status
    group first, measured at 0 orders in production) and a source-3 order —
    the 2024 website — counted by both;
  * **met by construction** (T10) — under `silver` no statement any calculator,
    the smart goal or the Monday job runs names DuckDB `orders`, `managers` or
    `manager_classifications`;
  * **the window** (T12) — no Silver body bounds a window with the server's
    clock, none filters `is_active_source`, none names a bridge table — read
    off the module, never listed;
  * **the switch** (T13) — `bridge` by default; an unknown value raises at
    every history read and at none of the imports; and under `silver` the
    engine flag's typo stops them too, because they now ask it;
  * **the answers** — every calculator, cap and smart goal equal under both
    histories, and the history really is read from Silver.

Each guard names the mutation that bites it.
"""
from __future__ import annotations

import ast
import asyncio
import os
import re
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from core import pg_goals_read
from core.duckdb_constants import KNOWN_SALES_TYPES
from core.models import OrderStatus
from core.repositories import goals as goals_module
from tests.unit.test_goals_off_duckdb_silver import (  # noqa: F401 — fixture
    EXPECTED,
    INTERVALS,
    MANAGERS,
    ORDERS,
    TIMEOUT_S,
    _calculators,
    _seed_history,
    _settled,
    _smart,
    store,
)

REPO = Path(__file__).resolve().parents[2]
GOALS = REPO / "core" / "repositories" / "goals.py"

# The three tables the bridge reads out of DuckDB, as SQL names.
BRIDGE_TABLES = ("orders", "managers", "manager_classifications")
_BRIDGE_TABLE = re.compile(
    r"(?<![\w.])(" + "|".join(BRIDGE_TABLES) + r")(?![\w])", re.IGNORECASE)


def _history(monkeypatch, mode):
    if mode is None:
        monkeypatch.delenv(pg_goals_read.HISTORY_ENV, raising=False)
    else:
        monkeypatch.setenv(pg_goals_read.HISTORY_ENV, mode)


@pytest.fixture(autouse=True)
def _duckdb_engine(monkeypatch):
    """The engine flag at its default: Silver here is DuckDB's, so this file
    needs no server. The engine half is the integration test's."""
    monkeypatch.delenv("KS_READ_GOALS", raising=False)
    monkeypatch.delenv("KS_PG_DSN", raising=False)


# ─── T9: Silver selects what the bridge selects ────────────────────────────

SOURCE_3_RETAIL = 101    # the 2024 website on Opencart: no manager, retail
GROUP_6_UNLISTED = 102   # lost/cancel by KeyCRM's group, status not on the list
LISTED_NOT_GROUP_6 = 103  # on the status list, KeyCRM's group says it is not lost

# (id, source_id, status_id, status_group_id, manager_id, ordered_at)
EXTRA_ORDERS = [
    (SOURCE_3_RETAIL, 3, 1, None, None, ORDERS[0][4]),
    (GROUP_6_UNLISTED, 1, 1, 6, None, ORDERS[0][4]),
    (LISTED_NOT_GROUP_6, 1, 19, 4, None, ORDERS[0][4]),
]
assert 19 in {int(s) for s in OrderStatus.return_statuses()}
assert 1 not in {int(s) for s in OrderStatus.return_statuses()}

# The one accepted difference, by row: the return is decided by
# `status_group_id = 6` before the status list. Measured on the 2026-08-31
# production copy: 0 orders disagree.
RETURN_RULE = {GROUP_6_UNLISTED, LISTED_NOT_GROUP_6}


async def _seed_equivalence_and_more(store, scenario: str) -> None:
    async with store.connection() as conn:
        conn.execute("DELETE FROM manager_classifications")
        conn.execute("DELETE FROM managers")
        if scenario != "nothing_synced":
            conn.executemany(
                "INSERT INTO managers (id, name, is_retail) VALUES (?, 'm', ?)",
                MANAGERS)
        if scenario == "classified":
            conn.executemany(
                "INSERT INTO manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to) VALUES (?, ?, ?, ?)",
                INTERVALS)
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, "
            "ordered_at, buyer_id, manager_id) VALUES (?, ?, ?, 1000.0, ?, 1, ?)",
            [(oid, src, st, when, mgr) for oid, src, st, mgr, when in ORDERS])
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id, manager_id) "
            "VALUES (?, ?, ?, ?, 1000.0, ?, 1, ?)",
            [(oid, src, st, grp, when, mgr)
             for oid, src, st, grp, mgr, when in EXTRA_ORDERS])
    await store.refresh_warehouse_layers(trigger="manual")


def _bridge_ids(conn, sales_type):
    statuses = tuple(int(s) for s in OrderStatus.return_statuses())
    clause, params = goals_module._orders_sales_type_predicate(sales_type)
    return {r[0] for r in conn.execute(
        f"SELECT o.id FROM orders o WHERE o.status_id NOT IN {statuses} "
        f"AND {clause}", params).fetchall()}


def _silver_ids(conn, sales_type):
    clause, params = goals_module._silver_history_where(sales_type)
    return {r[0] for r in conn.execute(
        f"SELECT s.id FROM silver_orders s WHERE {clause}", params).fetchall()}


class TestSilverSelectsWhatTheBridgeSelects:
    """Mutations: M11 — add `is_active_source` to `_silver_history_where`, and
    the 2024 website's order is lost; M12 — decide returns by the status
    list instead of `is_return`, and the accepted difference disappears,
    which means the rule Silver and every tab use was not the one read."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scenario", sorted(EXPECTED))
    async def test_every_sales_type_on_every_branch(self, store, scenario):
        await _seed_equivalence_and_more(store, scenario)
        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0] \
                == len(ORDERS) + len(EXTRA_ORDERS), "Silver was not built"
            for sales_type in (*KNOWN_SALES_TYPES, "all"):
                bridge = _bridge_ids(conn, sales_type)
                silver = _silver_ids(conn, sales_type)
                assert silver ^ bridge <= RETURN_RULE, (scenario, sales_type,
                                                        sorted(silver ^ bridge))
                if sales_type in ("retail", "all"):
                    assert SOURCE_3_RETAIL in silver and SOURCE_3_RETAIL in bridge, (
                        "the 2024 website's order is not counted (OQ-1)")
                    assert silver ^ bridge == RETURN_RULE, (scenario, sales_type)
                    assert GROUP_6_UNLISTED in bridge and GROUP_6_UNLISTED not in silver
                    assert LISTED_NOT_GROUP_6 in silver and LISTED_NOT_GROUP_6 not in bridge

    def test_the_where_carries_no_source_filter(self):
        for sales_type in (*KNOWN_SALES_TYPES, "all"):
            clause, _params = goals_module._silver_history_where(sales_type)
            assert "source" not in clause.lower(), clause
            assert "is_return" in clause

    def test_an_unknown_sales_type_refuses(self):
        with pytest.raises(ValueError, match="wholsale"):
            goals_module._silver_history_where("wholsale")


# ─── The answers, both ways ───────────────────────────────────────────────

class TestTheAnswersAreTheSame:
    """On the three-year history: every calculator, every month's cap and the
    smart goal, under `bridge` and under `silver`, equal to twelve places."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", [*KNOWN_SALES_TYPES, "all"])
    async def test_the_calculators(self, store, monkeypatch, sales_type):
        await _seed_history(store)
        _history(monkeypatch, "bridge")
        bridge = await _calculators(store, sales_type)
        _history(monkeypatch, "silver")
        silver = await _calculators(store, sales_type)
        assert silver == bridge

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_smart_goal(self, store, monkeypatch, sales_type):
        await _seed_history(store)
        _history(monkeypatch, "bridge")
        bridge = await _smart(store, sales_type)
        _history(monkeypatch, "silver")
        silver = await _smart(store, sales_type)
        assert silver == bridge
        assert bridge["monthly"]["lastYearRevenue"] > 0
        assert bridge["monthly"]["recent3MonthAvg"] > 0

    @pytest.mark.asyncio
    async def test_the_stored_tables(self, store, monkeypatch):
        """What the Monday job and the POST store, column by column, bounds
        included — the bounds read every order, as the bridge's do."""
        await _seed_history(store)
        _history(monkeypatch, "bridge")
        bridge = await store._compute_goal_tables(include_weekly=True)
        _history(monkeypatch, "silver")
        silver = await store._compute_goal_tables(include_weekly=True)
        assert silver["history_bounds"] == bridge["history_bounds"]
        assert silver["yoy_overall"] == pytest.approx(bridge["yoy_overall"], rel=1e-12)
        for key in ("seasonal", "yoy"):
            assert silver[key] == bridge[key], key
        assert _settled(silver["weekly_rows"]) == _settled(bridge["weekly_rows"])

    @pytest.mark.asyncio
    async def test_the_bounds_read_every_order_every_status(self, store, monkeypatch):
        """`growth_metrics.period_start`/`period_end` are the first and last
        order of the whole history — a return, a b2b order and a source that
        is not a revenue source included. The history's own extremes are
        retail and not returns, so they cannot tell; these two outlie it.

        Mutation (review MXb2): `_SILVER_BOUNDS_SQL` narrowed to
        `WHERE NOT s.is_return AND s.sales_type = 'retail'` — the first bound
        becomes the history's own first day, and this fails under `silver`.
        """
        from tests.unit.test_goals_off_duckdb_silver import (
            B2B_MANAGER_ID,
            HISTORY_END,
            HISTORY_START,
            _kyiv_noon,
        )

        first = HISTORY_START - timedelta(days=40)
        last = HISTORY_END + timedelta(days=40)
        await _seed_history(store)
        async with store.connection() as conn:
            conn.executemany(
                "INSERT INTO orders (id, source_id, status_id, grand_total, "
                "ordered_at, buyer_id, manager_id) VALUES (?, ?, ?, ?, ?, 1, ?)",
                [
                    # A retail return: status 19 is on the list.
                    (990_001, 1, 19, 400.0, _kyiv_noon(first), None),
                    # A b2b order on Opencart, which is not a revenue source.
                    (990_002, 3, 1, 600.0, _kyiv_noon(last), B2B_MANAGER_ID),
                ])
        await store.refresh_warehouse_layers(trigger="manual")
        async with store.connection() as conn:
            row = conn.execute(
                "SELECT is_return, sales_type, is_active_source FROM silver_orders "
                "WHERE id IN (990001, 990002) ORDER BY id").fetchall()
        assert row == [(True, "retail", True), (False, "b2b", False)], row

        for mode in ("bridge", "silver"):
            _history(monkeypatch, mode)
            assert await store._history_bounds() == (first, last), mode
            tables = await store._compute_goal_tables(include_weekly=False)
            assert tables["history_bounds"] == (first, last), mode


class TestSilverIsWhatIsRead:
    """Not vacuous: under `silver` the history really comes out of Silver.
    Emptied, DuckDB's Silver answers nothing — which is why step 13 needs
    `KS_READ_GOALS=postgres` beside this switch, and the readiness asks for
    both. Under `bridge` the same emptying moves nothing (DN-12's
    `TestCompactionShape`)."""

    @pytest.mark.asyncio
    async def test_an_emptied_duckdb_silver_empties_the_answer(self, store, monkeypatch):
        await _seed_history(store)
        _history(monkeypatch, "silver")
        seasonality, yoy, _weekly, _caps = await _calculators(store, "retail")
        assert len(seasonality) == 12 and yoy["sample_size"] >= 1
        async with store.connection() as conn:
            conn.execute("DELETE FROM silver_orders")
        seasonality, yoy, _weekly, caps = await _calculators(store, "retail")
        assert seasonality == {} and yoy["sample_size"] == 0
        assert caps == [0.35] * 12


# ─── T10: met by construction ──────────────────────────────────────────────

class _NoBridgeTables:
    """The store's connection, refusing any statement that names a table
    the bridge reads — and counting the Silver ones, so a run that read
    nothing cannot pass."""

    def __init__(self, conn, seen):
        self._conn, self._seen = conn, seen

    def _check(self, sql):
        found = _BRIDGE_TABLE.search(sql)
        if found:
            raise AssertionError(
                f"a goal read named DuckDB {found.group(1)!r} under "
                f"KS_GOALS_HISTORY=silver: {' '.join(sql.split())[:200]}")
        if "silver_orders" in sql:
            self._seen.append(sql)

    def execute(self, sql, *args, **kwargs):
        self._check(sql)
        return self._conn.execute(sql, *args, **kwargs)

    def executemany(self, sql, *args, **kwargs):
        self._check(sql)
        return self._conn.executemany(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


@asynccontextmanager
async def _bridge_tables_refused(store):
    """Every statement on the store's connection from here on is checked;
    yields the Silver statements seen."""
    real = type(store).connection
    seen: list = []

    @asynccontextmanager
    async def guarded(self):
        async with real(self) as conn:
            yield _NoBridgeTables(conn, seen)

    with patch.object(type(store), "connection", guarded):
        yield seen


class TestNoGoalReadNamesABridgeTable:
    """Under `silver`, everything that computes a goal runs with DuckDB's
    `orders`, `managers` and `manager_classifications` refused at the
    statement. Mutation M13: leave the last-year read (or any one history
    read) on `orders o`, and it raises here, named."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_calculators_and_the_smart_goal(
        self, store, monkeypatch, sales_type,
    ):
        await _seed_history(store)
        _history(monkeypatch, "silver")
        async with _bridge_tables_refused(store) as seen:
            await store.calculate_seasonality_indices(sales_type)
            await store.calculate_yoy_growth(sales_type)
            await store.calculate_weekly_patterns(sales_type)
            for month in range(1, 13):
                await store._dynamic_growth_cap(month, sales_type)
            await store.recalculate_goal_tables(include_weekly=True)
            await asyncio.wait_for(
                store.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S)
            await store.get_smart_goals(sales_type, year=2026, month=10)
            await store.get_goals(sales_type)
        assert len(seen) >= 20, "the guard saw no Silver read — nothing was proved"

    @pytest.mark.asyncio
    async def test_the_monday_job(self, store, monkeypatch):
        from core.scheduler import BackgroundScheduler

        await _seed_history(store)
        _history(monkeypatch, "silver")

        async def _get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        async with _bridge_tables_refused(store) as seen:
            result = await BackgroundScheduler()._run_seasonality_calc()
        assert result["retail_months"] == 12
        assert seen, "the job read no Silver"

    @pytest.mark.asyncio
    async def test_the_guard_bites_the_bridge(self, store, monkeypatch):
        """Not vacuous: the same run under `bridge` is refused."""
        await _seed_history(store)
        _history(monkeypatch, "bridge")
        async with _bridge_tables_refused(store):
            with pytest.raises(AssertionError, match="named DuckDB 'orders'"):
                await store.calculate_seasonality_indices("retail")


# ─── T12: the window and the row set, read off the module ─────────────────

def _silver_bodies() -> dict:
    """Every module-level statement in goals.py that reads `{silver_orders}`
    — parsed, so a body added tomorrow is held to these rules the day it is
    written."""
    tree = ast.parse(GOALS.read_text(encoding="utf-8"))
    return {node.targets[0].id: node.value.value
            for node in tree.body
            if isinstance(node, ast.Assign) and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            and "{silver_orders}" in node.value.value}


SERVER_CLOCKS = re.compile(
    r"CURRENT_DATE|CURRENT_TIMESTAMP|LOCALTIMESTAMP|\bnow\s*\(|\btoday\s*\(",
    re.IGNORECASE)


class TestTheBodies:
    """Mutation M15: write `DATE_TRUNC('month', CURRENT_DATE)` back into the
    recent-months body — the engines disagree about that day for three hours
    out of twenty-four — and it fails here, named."""

    def test_the_walk_finds_the_history_bodies(self):
        found = set(_silver_bodies())
        history = {name for name in found if name.startswith("_SILVER_")}
        assert {"_SILVER_SEASONALITY_SQL", "_SILVER_YEARLY_SQL",
                "_SILVER_MONTHLY_YOY_SQL", "_SILVER_BOUNDS_SQL",
                "_SILVER_WEEKLY_SQL", "_SILVER_GROWTH_CAP_SQL",
                "_SILVER_LAST_YEAR_SQL", "_SILVER_RECENT_MONTHS_SQL"} <= history

    @pytest.mark.parametrize("name", sorted(_silver_bodies()))
    def test_no_body_reads_the_servers_clock(self, name):
        body = _silver_bodies()[name]
        # `TODAY_IN_KYIV` is spliced in at the read, never written here.
        assert not SERVER_CLOCKS.search(body), (name, SERVER_CLOCKS.search(body))

    @pytest.mark.parametrize("name", sorted(
        n for n in _silver_bodies() if n.startswith("_SILVER_")))
    def test_no_history_body_filters_the_source_or_names_the_bridge(self, name):
        body = _silver_bodies()[name]
        assert "is_active_source" not in body, (
            f"{name} drops the 2024 website from the growth baseline (OQ-1)")
        assert not _BRIDGE_TABLE.search(body), name


# ─── T13: the switch ───────────────────────────────────────────────────────

class TestTheSwitch:
    @pytest.mark.parametrize("raw", [None, "", "  ", "bridge", "BRIDGE"])
    def test_the_default_is_the_bridge(self, monkeypatch, raw):
        _history(monkeypatch, raw)
        assert pg_goals_read.history_mode() == "bridge"
        assert pg_goals_read.history_from_silver() is False

    @pytest.mark.parametrize("raw", ["silver", " Silver "])
    def test_silver(self, monkeypatch, raw):
        _history(monkeypatch, raw)
        assert pg_goals_read.history_mode() == "silver"

    def test_a_typo_raises_naming_the_variable(self, monkeypatch):
        _history(monkeypatch, "silvr")
        with pytest.raises(ValueError, match="KS_GOALS_HISTORY='silvr'"):
            pg_goals_read.history_mode()

    def test_it_is_not_an_engine_switch(self):
        """The step-13 readiness reads every `KS_READ_*` name as an engine
        that must equal `postgres`; this one's values are not engines."""
        assert not pg_goals_read.HISTORY_ENV.startswith("KS_READ_")
        assert pg_goals_read.HISTORY_ENV != pg_goals_read.ENV

    def test_nothing_raises_at_import(self):
        """Web is the only syncer: a typo must stop goal reads, never the
        process. Imported fresh, with the typo set."""
        env = {**os.environ, "KS_GOALS_HISTORY": "silvr"}
        done = subprocess.run(
            [sys.executable, "-c",
             "import core.pg_goals_read, core.repositories.goals, "
             "core.warehouse_cutover, core.scheduler, web.main"],
            cwd=REPO, env=env, capture_output=True, text=True, timeout=120)
        assert done.returncode == 0, done.stderr[-2000:]


# Every read the history switch decides, and the Monday job's writer.
HISTORY_CALLS = (
    ("calculate_seasonality_indices", ("retail",), {}),
    ("calculate_yoy_growth", ("retail",), {}),
    ("calculate_weekly_patterns", ("retail",), {}),
    ("_dynamic_growth_cap", (10, "retail"), {}),
    ("_last_year_month_revenue", (2026, 10, "retail"), {}),
    ("_recent_three_month_average", ("retail",), {}),
    ("_history_bounds", (), {}),
    ("generate_smart_goals", (2026, 10), {}),
    ("get_smart_goals", (), {}),
    ("recalculate_goal_tables", (), {"include_weekly": True}),
)


async def _tables(store):
    async with store.connection() as conn:
        return {t: sorted(repr(r) for r in conn.execute(f"SELECT * FROM {t}").fetchall())
                for t in ("seasonal_indices", "growth_metrics", "weekly_patterns")}


class TestATypoStopsEveryHistoryRead:
    """Mutation M16: run an unknown value as `bridge`, and every one of these
    answers instead of refusing."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,args,kwargs", HISTORY_CALLS,
                             ids=[c[0] for c in HISTORY_CALLS])
    async def test_kS_GOALS_HISTORY(self, store, monkeypatch, name, args, kwargs):
        await _seed_history(store)
        await store.recalculate_goal_tables(include_weekly=True)
        before = await _tables(store)
        _history(monkeypatch, "silvr")
        with pytest.raises(ValueError, match="KS_GOALS_HISTORY"):
            await asyncio.wait_for(getattr(store, name)(*args, **kwargs), TIMEOUT_S)
        assert await _tables(store) == before, "a refused computation wrote"

    @pytest.mark.asyncio
    async def test_the_monday_job_fails_with_nothing_written(self, store, monkeypatch):
        from core.scheduler import BackgroundScheduler

        await _seed_history(store)
        await store.recalculate_goal_tables(include_weekly=False)
        before = await _tables(store)
        _history(monkeypatch, "silvr")

        async def _get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        with pytest.raises(ValueError, match="KS_GOALS_HISTORY"):
            await BackgroundScheduler()._run_seasonality_calc()
        assert await _tables(store) == before

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,args,kwargs", HISTORY_CALLS,
                             ids=[c[0] for c in HISTORY_CALLS])
    async def test_under_silver_the_engine_flags_typo_too(
        self, tmp_path, monkeypatch, name, args, kwargs,
    ):
        """They ask `KS_READ_GOALS` now, through `_goals_run`, so its typo
        stops them as it stops every other goal read
        (`tests/unit/test_pg_goals_read.py::TestATypoStopsEveryRead`)."""
        from core.duckdb_store import DuckDBStore

        _history(monkeypatch, "silver")
        monkeypatch.setenv("KS_READ_GOALS", "postgress")
        store = DuckDBStore(db_path=tmp_path / f"typo-{name}.duckdb")
        await store.connect()
        try:
            with pytest.raises(ValueError, match="KS_READ_GOALS"):
                await asyncio.wait_for(
                    getattr(store, name)(*args, **kwargs), TIMEOUT_S)
        finally:
            await store.close()


class TestATypoIsSeen:
    """The raise is where it belongs — at the read — but on its own it reached
    nobody: a 500 on the goal widget and on `/goals/*`, and a Monday job error
    in a log (review of 7b). `/api/health` publishes it and the canary pages
    it, the way `KS_READ_FALLBACK`, `KS_UTM_PARSE` and `KS_WRITE_WAREHOUSE`
    are seen.

    Mutations: drop `goals_history` from `HealthResponse` (the public
    endpoint loses it); `check_goals_history_mode` returning [] (the canary
    is quiet); the wiring left at `warn` (no page).
    """

    TYPO = {"mode": None,
            "error": "KS_GOALS_HISTORY='silvr' — unknown history. Expected one "
                     "of ('bridge', 'silver')"}

    @pytest.mark.parametrize("raw,mode", [(None, "bridge"), ("silver", "silver")])
    def test_health_publishes_the_mode(self, monkeypatch, raw, mode):
        from web.routes.api.health import _goals_history

        _history(monkeypatch, raw)
        assert _goals_history() == {"mode": mode, "error": None}

    def test_health_publishes_the_error_a_read_would_raise(self, monkeypatch):
        from web.routes.api.health import _goals_history

        _history(monkeypatch, "silvr")
        block = _goals_history()
        assert block["mode"] is None
        with pytest.raises(ValueError) as raised:
            pg_goals_read.history_mode()
        assert block["error"] == str(raised.value)
        assert "KS_GOALS_HISTORY='silvr'" in block["error"]

    def test_the_public_endpoint_carries_it_through_its_response_model(
        self, monkeypatch,
    ):
        """`/api/health` declares a response model, and a key the model does
        not name is dropped on the way out without a word."""
        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter

        _history(monkeypatch, "silvr")
        limiter.reset()
        try:
            body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
        block = body["goals_history"]
        assert block["mode"] is None
        assert "KS_GOALS_HISTORY='silvr'" in block["error"]

    def test_the_canary_pages_an_error_and_is_quiet_otherwise(self):
        from bot.canary import check_goals_history_mode

        assert [k for k, _ in check_goals_history_mode(
            {"goals_history": self.TYPO})] == ["goals_history_mode_invalid"]
        assert check_goals_history_mode({}) == []
        assert check_goals_history_mode(
            {"goals_history": {"mode": "silver", "error": None}}) == []

    @pytest.mark.asyncio
    async def test_the_wiring_pages_and_names_its_lever(self):
        from datetime import datetime, timedelta, timezone

        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["goals_history"] = self.TYPO

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "critical"
        assert result.failure_keys == ["goals_history_mode_invalid"]
        assert "KS_GOALS_HISTORY" in canary._what_to_do(result)

    def test_it_is_a_registered_condition(self):
        from core.alerting import Kind, spec_for

        assert spec_for("goals_history_mode_invalid").kind is Kind.CONDITION
