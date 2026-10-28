"""Step 14's S14-06: the dashboard asks Postgres and nothing else.

`_dashboard_run` (the doughnut, the returns list, the subcategories, the
three that left `gold_daily_products`, the customer block), the two Gold
primitives (`_pg_gold_summary`, `_pg_gold_series`), the Silver escapes of
`/api/summary` and `/api/revenue/trend` (`_pg_silver`) and the managers
screen's year (`get_manager_sales_365d`) each used to choose an engine by a
`KS_READ_*` switch (`KS_READ_DASHBOARD`, `KS_READ_GOLD`, `KS_READ_SILVER`),
ask `read_fallback.no_address` about it, try Postgres, and on a failure call
`read_fallback.fall_back` — which under `KS_READ_FALLBACK=off`, production's
mode since 2026-10-09 and since S14-00 the only one, refused the read: a 503
naming the surface, counted under `read_fallback_mode.refused`. The DuckDB
tails below that call were reachable by nothing in production.

So each keeps exactly the path production took: the Postgres read, and on any
exception `raise read_fallback.refuse(surface, exc)`. What is held here,
without a database:

- the switch is not read — unset, `duckdb`, a typo or junk, the read goes to
  Postgres and nothing reaches for the DuckDB store (step 13's readiness on
  `/api/warehouse/status` still reads the three variables, as `reader:*`
  keys, until S14-15 — `tests/unit/test_warehouse_cutover.py`);
- a failure is the refusal `fall_back` made under `off`: the surface, the
  count, the log line, the cause, and no fallback counted;
- without `KS_PG_DSN` the pool's own refusal to guess a database is that
  failure, so the answer is the same 503 — `no_address` named the switch in
  the log, the cause names the DSN now;
- junk in any of the three variables starts web and imports every module
  (rule 3 of the plan);
- the shape of what the card row and the chart receive: the summary's seven
  keys, and the trend's labels, series, datasets, comparison and measure.

The numbers are `tests/integration/test_dashboard_two_engines.py`'s, against a
real Postgres and literals DuckDB answered before its side went; every route
through the real app is `tests/unit/test_read_fallback_http.py`'s; that no
engine call sits outside its `try` is `tests/unit/test_read_fallback_sites.py`'s
rule 5.
"""
from __future__ import annotations

import importlib
import logging
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core import read_fallback

ROOT = Path(__file__).resolve().parents[2]
D1, D2 = date(2026, 8, 1), date(2026, 8, 3)

MODULES = ("pg_dashboard_read", "pg_gold_read", "pg_silver_read")
SWITCHES = ("KS_READ_DASHBOARD", "KS_READ_GOLD", "KS_READ_SILVER")
SWITCH_VALUES = [None, "", "duckdb", "postgres", "postgress", "junk-s14-06"]


def _dashboard(store):
    return store._dashboard_run(
        "SELECT 1 FROM {silver_orders} s LEFT JOIN {managers} m ON s.manager_id = m.id"
        " WHERE s.id = ?", [7])


def _silver(store):
    return store._pg_silver("SELECT 1 FROM {order_lines} l WHERE l.order_id = ?", [7])


def _gold_summary(store):
    return store._pg_gold_summary(D1, D2, "retail", None)


def _gold_series(store):
    return store._pg_gold_series(D1, D2, "b2b", 4)


def _managers(store):
    return store.get_manager_sales_365d()


# (reader, surface, module, function the reader calls, the switch it no longer
# reads, how to call it, what the call must hand the engine: a table the
# rendered statement names, or the arguments)
READERS = [
    ("_dashboard_run", "dashboard", "pg_dashboard_read", "fetch", "KS_READ_DASHBOARD",
     _dashboard, "bronze.managers"),
    ("_pg_silver", "dashboard", "pg_silver_read", "fetch", "KS_READ_SILVER",
     _silver, "silver.order_lines"),
    ("_pg_gold_summary", "dashboard", "pg_gold_read", "fetch_summary", "KS_READ_GOLD",
     _gold_summary, (D1, D2, "retail", None)),
    ("_pg_gold_series", "dashboard", "pg_gold_read", "fetch_series", "KS_READ_GOLD",
     _gold_series, (D1, D2, "b2b", 4)),
    ("get_manager_sales_365d", "managers", "pg_silver_read", "fetch", "KS_READ_SILVER",
     _managers, "silver.orders"),
]
IDS = [r[0] for r in READERS]


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A store that is never connected, whose connection is a trap: nothing
    here may reach for DuckDB, under any value of any switch."""
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "never.duckdb")

    def trap(*_a, **_k):
        raise AssertionError("a Postgres-only dashboard read reached for DuckDB")

    for name in ("connection", "_fetch_one", "_fetch_all"):
        monkeypatch.setattr(type(store), name, trap)
    read_fallback.reset_counts()
    yield store
    read_fallback.reset_counts()
    assert not (tmp_path / "never.duckdb").exists()


def _set(monkeypatch, switch, value):
    if value is None:
        monkeypatch.delenv(switch, raising=False)
    else:
        monkeypatch.setenv(switch, value)


class TestTheSwitchIsNotRead:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", SWITCH_VALUES,
                             ids=[repr(v) for v in SWITCH_VALUES])
    @pytest.mark.parametrize(
        "reader,surface,module,function,switch,call,handed", READERS, ids=IDS)
    async def test_postgres_answers_whatever_it_says(
        self, store, monkeypatch, reader, surface, module, function, switch,
        call, handed, value,
    ):
        _set(monkeypatch, switch, value)
        answer = [(1, "x", 2.0)]
        engine = AsyncMock(return_value=answer)
        monkeypatch.setattr(f"core.{module}.{function}", engine)

        assert await call(store) == answer

        args = engine.await_args.args
        if isinstance(handed, tuple):
            assert args == handed
        else:
            rendered, params = args
            assert handed in rendered and "{" not in rendered
            assert list(params) in ([7], [])
        assert read_fallback.refusals() == {} and read_fallback.counts() == {}

    @pytest.mark.parametrize("module", MODULES)
    def test_the_module_carries_no_switch(self, module):
        """Mutation: put `ENV`/`enabled`/`available` back — the sites walk
        would then take the module for a switch, and a reader could start
        asking it again."""
        m = importlib.import_module(f"core.{module}")
        assert not any(hasattr(m, name) for name in ("ENV", "enabled", "available"))

    def test_the_walk_finds_none_of_them_a_switch(self):
        from tests.unit.test_read_fallback_sites import read_switch_modules

        assert not read_switch_modules() & set(MODULES)

    def test_no_dashboard_reader_asks_the_managers_chain(self):
        """`_dashboard_run` asked `pg_managers_write.reads_the_chain` for the
        returns list's `{managers}` join until S14-06; with one path, the
        question decides nothing, and no router in the repository asks it."""
        import ast

        for path in (ROOT / "core").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                     and isinstance(n.func, ast.Attribute)
                     and n.func.attr == "reads_the_chain"
                     and isinstance(n.func.value, ast.Name)
                     and n.func.value.id == "pg_managers_write"]
            assert not calls, path


class TestAFailureIsTheRefusalOffMade:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reader,surface,module,function,switch,call,handed", READERS, ids=IDS)
    async def test_a_503_naming_the_surface_counted_as_a_refusal(
        self, store, monkeypatch, caplog, reader, surface, module, function, switch,
        call, handed,
    ):
        down = OSError("connection refused")
        monkeypatch.setattr(f"core.{module}.{function}", AsyncMock(side_effect=down))

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            with pytest.raises(read_fallback.ReadUnavailable) as raised:
                await call(store)

        assert raised.value.surface == surface
        assert raised.value.__cause__ is down
        assert read_fallback.refusals()[surface]["count"] == 1
        assert read_fallback.counts() == {}
        # `fall_back`'s line under off, word for word: `read_refused`'s lever
        # greps "read refused", the soak a fallback by the phrase below.
        assert (f"read refused: {surface} — its engine failed, and "
                f"KS_READ_FALLBACK=off forbids answering from DuckDB "
                f"(1 refused since start): connection refused") in caplog.text
        assert "falling back to DuckDB" not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reader,surface,module,function,switch,call,handed", READERS, ids=IDS)
    async def test_no_dsn_is_the_same_refusal(
        self, store, monkeypatch, reader, surface, module, function, switch,
        call, handed,
    ):
        """No pool patched and no DSN: `core.pg.dsn()` refuses to guess a
        database, and that is the failure refused. It was `no_address`'s
        before, naming the switch; the HTTP answer is the same 503."""
        from core import pg

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setattr(pg, "_pool", None)
        monkeypatch.setenv(switch, "postgres")

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await call(store)

        assert raised.value.surface == surface
        assert isinstance(raised.value.__cause__, RuntimeError)
        assert "KS_PG_DSN is not set" in str(raised.value.__cause__)
        assert read_fallback.refusals()[surface]["count"] == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,kwargs,module,function", [
        ("get_summary_stats", {}, "pg_gold_read", "fetch_summary"),
        ("get_summary_stats", {"brand": "BrandA"}, "pg_silver_read", "fetch"),
        ("get_revenue_trend", {}, "pg_gold_read", "fetch_series"),
        ("get_revenue_trend", {"promocode": "X"}, "pg_silver_read", "fetch"),
        ("get_return_orders", {}, "pg_dashboard_read", "fetch"),
        ("get_customer_insights", {}, "pg_dashboard_read", "fetch"),
    ], ids=["summary_gold", "summary_silver", "trend_gold", "trend_silver",
            "returns", "insights"])
    async def test_the_dashboard_s_methods_refuse_through_them(
        self, store, monkeypatch, method, kwargs, module, function,
    ):
        monkeypatch.setattr(f"core.{module}.{function}",
                            AsyncMock(side_effect=OSError("down")))
        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await getattr(store, method)(D1, D2, **kwargs)
        assert raised.value.surface == "dashboard"


SUMMARY_KEYS = {"totalOrders", "totalRevenue", "avgCheck", "totalReturns",
                "returnsRevenue", "startDate", "endDate"}
DATASET_KEYS = {"label", "data", "borderColor", "backgroundColor", "fill",
                "tension", "borderWidth"}


class TestTheShape:
    """What the card row and the chart receive, held apart from the numbers:
    one key gone or renamed and the frontend draws a blank, with no number
    in any literal changing to say so."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kwargs", [{}, {"source_id": 1}, {"source_id": 3},
                                        {"brand": "BrandA"}, {"category_id": 1}],
                             ids=["gold", "gold_source", "zeros", "brand", "category"])
    async def test_the_summary_has_its_seven_keys(self, store, monkeypatch, kwargs):
        monkeypatch.setattr("core.pg_gold_read.fetch_summary",
                            AsyncMock(return_value=(4, 1000.0, 1, 50.0)))
        monkeypatch.setattr("core.pg_silver_read.fetch",
                            AsyncMock(return_value=[(4, 1000.0)]))
        monkeypatch.setattr("core.pg_lookups_read.fetch",
                            AsyncMock(return_value=[(1,)]))
        out = await store.get_summary_stats(D1, D2, **kwargs)
        assert set(out) == SUMMARY_KEYS
        assert (out["startDate"], out["endDate"]) == ("2026-08-01", "2026-08-03")
        assert all(isinstance(out[k], int) for k in ("totalOrders", "totalReturns"))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kwargs,measure", [
        ({}, "revenue"), ({"source_id": 5}, "revenue"), ({"promocode": "X"}, "revenue"),
        ({"brand": "BrandA"}, "goods_value"), ({"category_id": 1}, "goods_value"),
    ], ids=["gold", "order_grain_source", "order_grain_promocode", "brand", "category"])
    async def test_the_trend_with_its_comparison(self, store, monkeypatch, kwargs, measure):
        rows = [(D1, 100.0, 1), (D2, 50.0, 2)]
        monkeypatch.setattr("core.pg_gold_read.fetch_series", AsyncMock(return_value=rows))
        monkeypatch.setattr("core.pg_silver_read.fetch", AsyncMock(return_value=rows))
        monkeypatch.setattr("core.pg_lookups_read.fetch", AsyncMock(return_value=[(1,)]))

        out = await store.get_revenue_trend(D1, D2, **kwargs)

        assert set(out) == {"labels", "revenue", "orders", "datasets", "measure",
                            "comparison"}
        assert out["measure"] == measure
        assert out["labels"] == ["01.08", "02.08", "03.08"]
        assert out["revenue"] == [100.0, 0, 50.0] and out["orders"] == [1, 0, 2]
        this, previous = out["datasets"]
        assert set(this) == DATASET_KEYS and this["label"] == "This Period"
        assert set(previous) == DATASET_KEYS | {"borderDash"}
        assert previous["label"] == "Previous Period"
        comparison = out["comparison"]
        assert set(comparison) == {"labels", "revenue", "orders", "period", "totals"}
        assert comparison["period"] == {"start": "2026-07-29", "end": "2026-07-31",
                                        "type": "previous_period"}
        assert set(comparison["totals"]) == {"current", "previous", "growth_percent"}

    @pytest.mark.asyncio
    async def test_the_trend_without_its_comparison_asks_one_period(
        self, store, monkeypatch,
    ):
        series = AsyncMock(return_value=[])
        monkeypatch.setattr("core.pg_gold_read.fetch_series", series)
        out = await store.get_revenue_trend(D1, D2, include_comparison=False)
        assert set(out) == {"labels", "revenue", "orders", "datasets", "measure"}
        assert len(out["datasets"]) == 1
        assert series.await_count == 1


class TestJunkStartsWeb:
    """Rule 3 of the plan: a variable the code stopped reading cannot stop
    web, whatever it holds — neither its import nor its start. The start
    that reads these variables is `configure_modes(web=True)`, which
    `web/main.py`'s startup calls first: `read_fallback.configure_mode` scans
    every `KS_READ_*` for a switch with no address. So both tests run it with
    junk in all three."""

    def test_a_fresh_interpreter_imports_and_starts_with_junk_in_every_one(self, tmp_path):
        """The child's environment is exactly what is written here, with the
        suite's test signing secrets (`tests/conftest.py`), from `tmp_path`,
        so no `.env` above the cwd is loaded — the child asserts it found
        none."""
        code = (
            "import importlib, sys\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from dotenv import find_dotenv\n"
            "found = find_dotenv(usecwd=True)\n"
            "assert not found, 'a .env above the cwd would be loaded: ' + found\n"
            "import web.main\n"
            "for m in %r:\n"
            "    importlib.import_module('core.' + m)\n"
            "importlib.import_module('core.repositories.revenue')\n"
            "from core.runtime_modes import configure_modes\n"
            "configure_modes(web=True)\n"
            "configure_modes(web=True)\n"   # the scheduler's call, after the startup's
            "print('ok')\n" % list(MODULES)
        )
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
               "TZ": "UTC",
               "DASHBOARD_SECRET_KEY": os.environ["DASHBOARD_SECRET_KEY"],
               "BOT_TOKEN": os.environ["BOT_TOKEN"],
               **{s: "junk-s14-06" for s in SWITCHES}}
        run = subprocess.run([sys.executable, "-I", "-c", code], cwd=tmp_path, env=env,
                             capture_output=True, text=True, timeout=180)
        assert run.returncode == 0, run.stderr[-3000:]
        assert run.stdout.strip().endswith("ok")

    def test_health_answers_and_publishes_none_of_them(self, monkeypatch):
        from fastapi.testclient import TestClient

        from core.runtime_modes import configure_modes
        from web.main import app

        # What `configure_modes` caches here, restored after the test (the
        # duckdb switch and the warehouse writer are conftest's to reset).
        for name in ("_misconfigured", "_unreported", "_configured"):
            monkeypatch.setattr(read_fallback, name, getattr(read_fallback, name))
        for switch in SWITCHES:
            monkeypatch.setenv(switch, "junk-s14-06")
        configure_modes(web=True)
        response = TestClient(app).get("/api/health")
        assert response.status_code == 200, response.text
        block = response.json()["read_fallback_mode"]
        assert not any(s in line for line in block["misconfigured"] for s in SWITCHES)
