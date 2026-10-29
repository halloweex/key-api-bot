"""Step 14's S14-07: the third set of readers asks one engine and nothing else.

Four routers used to choose an engine by a `KS_READ_*` switch, ask
`read_fallback.no_address` about it, try that engine, and on a failure call
`read_fallback.fall_back` — which under `KS_READ_FALLBACK=off`, production's
mode since 2026-10-09 and since S14-00 the only one, refused the read: a 503
naming the surface, counted under `read_fallback_mode.refused`:

- `_expenses_run` (`/expenses`, `KS_READ_EXPENSES`), with the history gate:
  a statement reading `{expenses}` while `bronze.expenses` has no backfilled
  history had no engine that could answer it;
- `_inventory_batch` and `_inventory_rows` (`/inventory`, `KS_READ_INVENTORY`);
- `_goals_run` and `_goal_tables_run` (`/goals`, the smart goal, the
  forecast; `KS_READ_GOALS` and the chains' `reads_the_chain`);
- `_analytics_rows` (the cohorts, `KS_READ_COHORTS`), whose one engine is
  ClickHouse: under `off` a switch not naming it, or no `KS_CH_URL`, was
  refused by `no_engine`/`no_address`.

Each keeps exactly the path production took: the engine read, and on any
exception `raise read_fallback.refuse(surface, exc)`. Five readers never had
a fallback — the training frame (`KS_READ_FORECAST_INPUT`), the weekly report
(`KS_READ_WEEKLY`), the assistant's tools (`KS_READ_CHAT`), the buyer-sync
selection (`KS_READ_BUYER_SYNC`) and the search index
(`KS_READ_SEARCH_INDEX`) — and read Postgres alone now, a failure their own
error as before. What is held here, without a database:

- no switch is read — unset, `duckdb`, a typo or junk, Postgres (ClickHouse
  for the cohorts) is asked, and nothing reaches for the DuckDB store;
- a router's failure is the refusal `fall_back` made under `off`: the
  surface, the count, the log line, the cause, and no fallback counted; so is
  no `KS_PG_DSN` (no `KS_CH_URL` for the cohorts), and the expenses' history
  not across; chain 7a's `app.revenue_goals`, read raw before S14-07, is
  refused the same way, and through the app that is a `read_refused` page
  (an accepted change in failure mode);
- a reader with no fallback lets its own error through, uncounted, as it
  did;
- no reader module carries `ENV`, `enabled` or `available`, and
  `ch_cohorts` no `KS_READ_COHORTS`;
- junk in every one of those variables, and in `KS_GOALS_HISTORY`, starts
  web and imports every module (rule 3 of the plan).

The numbers are the `tests/integration/*_two_engines.py` files', against a
real Postgres and literals DuckDB answered before its side went; every route
through the real app is `tests/unit/test_read_fallback_http.py`'s.
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
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

ROOT = Path(__file__).resolve().parents[2]

SWITCHES = ("KS_READ_EXPENSES", "KS_READ_INVENTORY", "KS_READ_GOALS",
            "KS_READ_COHORTS", "KS_READ_FORECAST_INPUT", "KS_READ_WEEKLY",
            "KS_READ_CHAT", "KS_READ_BUYER_SYNC", "KS_READ_SEARCH_INDEX")
MODULES = ("pg_expenses_read", "pg_inventory_read", "pg_goals_read",
           "pg_forecast_read", "pg_weekly_read", "pg_chat_read",
           "pg_buyer_sync_read", "pg_search_index_read", "ch_cohorts")
SWITCH_VALUES = [None, "", "duckdb", "postgres", "clickhouse", "junk-s14-07"]


def _expenses(store):
    return store._expenses_run(
        "SELECT 1 FROM {expenses} e JOIN {silver_orders} s ON s.id = e.order_id"
        " WHERE s.id = ?", [7])


def _manual(store):
    return store._expenses_run("SELECT 1 FROM {manual_expenses} WHERE id = ?", [7])


def _inventory(store):
    return store._inventory_rows("SELECT 1 FROM {sku_inventory_status} WHERE offer_id = ?",
                                 [7])


def _goals(store):
    return store._goals_run("SELECT 1 FROM {silver_orders} s WHERE s.id = ?", [7])


def _stored_goals(store):
    """Chain 7a's table, which `_goals_run` read raw — uncounted, a 500 on a
    failure — until S14-07 (`pg_goals_write.reads_the_chain`)."""
    return store._goals_run(
        "SELECT 1 FROM {revenue_goals} WHERE period_type = ?", [7])


def _goal_tables(store):
    return store._goal_tables_run("SELECT 1 FROM {seasonal_indices} WHERE month = ?", [7])


def _cohorts(store):
    return store._analytics_rows(
        lambda d: f"SELECT 1 FROM {d.silver_orders} o WHERE o.id = ?", [7], ("int",))


# (router, surface, module, the function it hands the statement, how to call
# it, the name the rendered statement must carry)
ROUTERS = [
    ("_expenses_run", "expenses", "pg_expenses_read", "fetch", _expenses,
     "bronze.expenses"),
    ("_expenses_run (manual)", "expenses", "pg_expenses_read", "fetch", _manual,
     "app.manual_expenses"),
    ("_inventory_rows", "inventory", "pg_inventory_read", "fetch_many", _inventory,
     "app.sku_inventory_status"),
    ("_goals_run", "goals", "pg_goals_read", "fetch", _goals, "silver.orders"),
    ("_goals_run (chain 7a)", "goals", "pg_goals_read", "fetch", _stored_goals,
     "app.revenue_goals"),
    ("_goal_tables_run", "goals", "pg_goals_read", "fetch", _goal_tables,
     "app.seasonal_indices"),
    ("_analytics_rows", "cohorts", "ch_cohorts", "fetch", _cohorts, "silver.orders"),
]
IDS = [r[0] for r in ROUTERS]


def _rendered(fn_name, call):
    """The statement a mocked engine call was handed."""
    if fn_name == "fetch_many":
        return call.args[0][0][0], list(call.args[0][0][1])
    return call.args[0], list(call.args[1])


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A store that is never connected, whose connection is a trap: nothing
    here may reach for DuckDB, under any value of any switch. The expenses'
    history is across unless a test says otherwise."""
    from core import pg_expenses_read
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "never.duckdb")

    def trap(*_a, **_k):
        raise AssertionError("a one-engine reader reached for DuckDB")

    for name in ("connection", "_fetch_one", "_fetch_all"):
        monkeypatch.setattr(type(store), name, trap)
    monkeypatch.setattr(pg_expenses_read, "backfilled", AsyncMock(return_value=True))
    read_fallback.reset_counts()
    yield store
    read_fallback.reset_counts()
    assert not (tmp_path / "never.duckdb").exists()


def _set(monkeypatch, value):
    for switch in SWITCHES:
        if value is None:
            monkeypatch.delenv(switch, raising=False)
        else:
            monkeypatch.setenv(switch, value)


class TestTheSwitchIsNotRead:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", SWITCH_VALUES, ids=[repr(v) for v in SWITCH_VALUES])
    @pytest.mark.parametrize("router,surface,module,fn,call,table", ROUTERS, ids=IDS)
    async def test_the_engine_answers_whatever_it_says(
        self, store, monkeypatch, router, surface, module, fn, call, table, value,
    ):
        _set(monkeypatch, value)
        rows = [(1,)]
        engine = AsyncMock(return_value=[rows] if fn == "fetch_many" else rows)
        monkeypatch.setattr(f"core.{module}.{fn}", engine)

        answer = await call(store)

        assert answer == rows
        rendered, params = _rendered(fn, engine.await_args)
        assert table in rendered and "{" not in rendered
        assert params == [7]
        assert read_fallback.refusals() == {} and read_fallback.counts() == {}

    @pytest.mark.parametrize("module", MODULES)
    def test_the_module_carries_no_switch(self, module):
        """Mutation: put `ENV`/`enabled`/`available` back — the sites walk
        would then take the module for a switch, and a reader could start
        asking it again."""
        m = importlib.import_module(f"core.{module}")
        assert not any(hasattr(m, name) for name in ("ENV", "enabled", "available",
                                                     "_VALID", "HISTORY_ENV"))

    def test_the_walk_finds_none_of_them_a_switch(self):
        from tests.unit.test_read_fallback_sites import read_switch_modules

        assert not read_switch_modules() & set(MODULES)

    def test_nothing_names_the_cohorts_an_engine_only_surface(self):
        """`ENGINE_ONLY` held the cohorts' switch for `no_engine`; it routes
        nothing now, so it is empty and `/api/health`'s `no_engine` too."""
        assert read_fallback.ENGINE_ONLY == {}
        assert read_fallback.no_engine_routes() == []


class TestAFailureIsTheRefusalOffMade:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("router,surface,module,fn,call,table", ROUTERS, ids=IDS)
    async def test_a_503_naming_the_surface_counted_as_a_refusal(
        self, store, monkeypatch, caplog, router, surface, module, fn, call, table,
    ):
        down = OSError("connection refused")
        monkeypatch.setattr(f"core.{module}.{fn}", AsyncMock(side_effect=down))

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            with pytest.raises(read_fallback.ReadUnavailable) as raised:
                await call(store)

        assert raised.value.surface == surface
        assert raised.value.__cause__ is down
        assert read_fallback.refusals()[surface]["count"] == 1
        assert read_fallback.counts() == {}
        assert (f"read refused: {surface} — its engine failed, and "
                f"KS_READ_FALLBACK=off forbids answering from DuckDB "
                f"(1 refused since start): connection refused") in caplog.text
        assert "falling back to DuckDB" not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "router,surface,module,fn,call,table",
        [r for r in ROUTERS if r[2] != "ch_cohorts"],
        ids=[r[0] for r in ROUTERS if r[2] != "ch_cohorts"])
    async def test_no_dsn_is_the_same_refusal(
        self, store, monkeypatch, router, surface, module, fn, call, table,
    ):
        """No pool patched and no DSN: `core.pg.dsn()` refuses to guess a
        database, and that is the failure refused — where `no_address`
        named the switch."""
        from core import pg

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setattr(pg, "_pool", None)
        _set(monkeypatch, "postgres")

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await call(store)

        assert raised.value.surface == surface
        assert isinstance(raised.value.__cause__, RuntimeError)
        assert "KS_PG_DSN is not set" in str(raised.value.__cause__)
        assert read_fallback.refusals()[surface]["count"] == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "clickhouse", "duckdb"])
    async def test_no_clickhouse_address_is_the_cohorts_refusal(
        self, store, monkeypatch, value,
    ):
        """The cohorts' one engine, unconfigured: what `no_address` (the
        switch on ClickHouse) and `no_engine` (the switch anywhere else)
        refused under `off`. Mutation: answer the cohorts from DuckDB again."""
        from core import ch_common

        monkeypatch.delenv(ch_common.URL_ENV, raising=False)
        _set(monkeypatch, value)

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await _cohorts(store)

        assert raised.value.surface == "cohorts"
        assert "KS_CH_URL is not set" in str(raised.value.__cause__)
        assert read_fallback.refusals()["cohorts"]["count"] == 1

    @pytest.mark.asyncio
    async def test_the_expenses_history_not_across_is_refused(self, store, monkeypatch):
        """A NULL `backfilled_at`: an answer, not a failure — and under `off`
        a refusal, since no engine could answer it. Mutation: let the read
        through to an empty `bronze.expenses`."""
        from core import pg_expenses_read

        monkeypatch.setattr(pg_expenses_read, "backfilled", AsyncMock(return_value=False))
        engine = AsyncMock(return_value=[(1,)])
        monkeypatch.setattr(pg_expenses_read, "fetch", engine)

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await _expenses(store)

        assert raised.value.surface == "expenses"
        assert "backfilled" in str(raised.value.__cause__)
        engine.assert_not_awaited()
        # The type dictionary and the typed expenses have no history to miss.
        # That the gate names `bronze.expenses` rather than importing it from
        # the backfill module is `tests/unit/test_pg_expenses_read.py`'s.
        assert await _manual(store) == [(1,)]


class TestChain7aAFailedReadPages:
    """An accepted change in failure mode (the review of S14-07, decided):
    a failed read of chain 7a's `app.revenue_goals` — `GET /api/goals`, the
    smart goal's goals block — was let through raw before S14-07, an
    uncounted 500 that paged nothing; it is the counted 503 naming `goals`
    now, published under `read_fallback_mode.refused`, and the canary pages
    `read_refused` (WARN) when it is the only read that fails. The same
    change S14-05 made for /marketing's goal line and S14-06 for the returns
    list. Mutation: read the chain's table raw again."""

    def test_only_the_stored_goals_failing_is_a_counted_503_and_a_page(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        from bot import canary
        from core import pg_goals_read

        async def fetch(sql, params):
            if "app.revenue_goals" in sql:
                raise OSError('relation "app.revenue_goals" does not exist')
            return []

        monkeypatch.setattr(pg_goals_read, "fetch", fetch)

        response = admin_client.get("/api/goals")
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "goals"

        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"]["refused"]["goals"]["count"] == 1
        assert health["read_fallbacks"] == {}
        [(key, message)] = canary.check_read_refusals(health)
        assert key == "read_refused"
        assert message.startswith(canary.READ_REFUSED_LINE + "goals ×1")


# (reader, module, function it calls, how to call it)
async def _frame(store):
    from core.prediction_service import PredictionService

    return await PredictionService()._query_daily_revenue(store, "retail", 30)


async def _weekly(store):
    from core.weekly_report import warehouse_max_date

    return await warehouse_max_date(store)


async def _chat(store):
    from core.chat_tools import _chat_run

    return await _chat_run("SELECT 1 FROM {gold_daily_revenue} WHERE ?", [True])


async def _buyers(store):
    return await store.get_missing_buyer_ids(5)


READERS = [
    ("training frame", "pg_forecast_read", "fetch_frame", _frame),
    ("weekly report", "pg_weekly_read", "fetch", _weekly),
    ("assistant", "pg_chat_read", "fetch", _chat),
    ("buyer sync", "pg_buyer_sync_read", "fetch", _buyers),
]


class TestTheReadersWithNoFallback:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "duckdb", "junk-s14-07"])
    @pytest.mark.parametrize("reader,module,fn,call", READERS, ids=[r[0] for r in READERS])
    async def test_postgres_answers_whatever_the_switch_says(
        self, store, monkeypatch, reader, module, fn, call, value,
    ):
        _set(monkeypatch, value)
        engine = AsyncMock(return_value=[(date(2026, 9, 1),)])
        monkeypatch.setattr(f"core.{module}.{fn}", engine)
        await call(store)
        assert engine.await_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reader,module,fn,call", READERS, ids=[r[0] for r in READERS])
    async def test_a_failure_is_their_own_error_and_counts_nothing(
        self, store, monkeypatch, reader, module, fn, call,
    ):
        """There never was a fallback here: the callers contain the error
        (the trend drops the forecast, training keeps the model, the weekly
        tick defers, the tool says it failed, the buyers step holds its
        watermark). Mutation: turn it into a refusal, or into DuckDB."""
        down = OSError("connection refused")
        monkeypatch.setattr(f"core.{module}.{fn}", AsyncMock(side_effect=down))
        with pytest.raises(OSError):
            await call(store)
        assert read_fallback.refusals() == {} and read_fallback.counts() == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reader,module,fn,call", READERS, ids=[r[0] for r in READERS])
    async def test_a_switch_published_without_its_dsn_is_not_refused(
        self, store, fresh_read_fallback, monkeypatch, caplog, reader, module, fn, call,
    ):
        """What `/api/health` says of `read_fallback_mode.misconfigured`
        (`web/schemas.py`), what the module's docstring says and what the
        start's line says, since the second review of S14-07: the switch is
        still listed, by prefix, but no `no_address` stands behind it any
        more, so its reader meets `core.pg`'s own error, counted nowhere —
        `misconfigured` is no longer exactly what is refused. No pool
        patched, no DSN. Mutations: ask `no_address` again (a refusal is
        counted), or have the line say every read behind the switch is
        refused, or offer unsetting the flag as the cure."""
        from core import pg

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setattr(pg, "_pool", None)
        _set(monkeypatch, "postgres")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.configure_mode()

        published = {line.split("=", 1)[0] for line in fresh_read_fallback.misconfigured()}
        assert published == set(SWITCHES)
        assert "every read behind it is refused" not in caplog.text
        assert "unset the flag" not in caplog.text
        assert ("no read of that engine can succeed, whatever the flag says: "
                "a tab's is refused (KS_READ_FALLBACK=off)") in caplog.text

        with pytest.raises(RuntimeError, match="KS_PG_DSN is not set"):
            await call(store)
        assert fresh_read_fallback.refusals() == {}
        assert fresh_read_fallback.counts() == {}

    def test_the_search_index_keeps_one_watermark(self):
        """The DuckDB-built index's `meilisearch` cursor went with its frames;
        the index reads Postgres under `meilisearch_pg` alone."""
        import inspect

        from core import pg_search_index_read
        from core.sync_service import SyncService

        assert pg_search_index_read.WATERMARK_KEY == "meilisearch_pg"
        source = inspect.getsource(SyncService)
        assert '"meilisearch"' not in source and "'meilisearch'" not in source
        assert not any(hasattr(SyncService, name) for name in (
            "_meili_high_watermark", "_meili_buyers_frame", "_meili_orders_frame",
            "_meili_products_frame"))


class TestJunkStartsWeb:
    """Rule 3: a variable the code stopped reading cannot stop web, whatever
    it holds — its import or its start (`configure_modes(web=True)`, the
    startup's and the scheduler's call)."""

    def test_a_fresh_interpreter_imports_and_starts_with_junk_in_every_one(self, tmp_path):
        """The child's environment is exactly what is written here, with the
        suite's test signing secrets, run from `tmp_path` so `load_dotenv()`
        finds no `.env` above it (asserted)."""
        code = (
            "import importlib, sys\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from dotenv import find_dotenv\n"
            "found = find_dotenv(usecwd=True)\n"
            "assert not found, 'a .env above the cwd would be loaded: ' + found\n"
            "import web.main\n"
            "for m in %r:\n"
            "    importlib.import_module('core.' + m)\n"
            "import core.repositories.goals, core.repositories.expenses\n"
            "import core.repositories.inventory, core.chat_tools, core.weekly_report\n"
            "import core.prediction_service, core.sync_service\n"
            "from core.runtime_modes import configure_modes\n"
            "configure_modes(web=True)\n"
            "configure_modes(web=True)\n"
            "print('ok')\n" % (list(MODULES),)
        )
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
               "TZ": "UTC",
               "DASHBOARD_SECRET_KEY": os.environ["DASHBOARD_SECRET_KEY"],
               "BOT_TOKEN": os.environ["BOT_TOKEN"],
               "KS_GOALS_HISTORY": "junk-s14-07",
               **{s: "junk-s14-07" for s in SWITCHES}}
        run = subprocess.run([sys.executable, "-I", "-c", code], cwd=tmp_path, env=env,
                             capture_output=True, text=True, timeout=180)
        assert run.returncode == 0, run.stderr[-3000:]
        assert run.stdout.strip().endswith("ok")

    def test_health_answers_and_publishes_none_of_them(self, monkeypatch):
        from fastapi.testclient import TestClient

        from core.runtime_modes import configure_modes
        from web.main import app

        for name in ("_misconfigured", "_unreported", "_configured"):
            monkeypatch.setattr(read_fallback, name, getattr(read_fallback, name))
        monkeypatch.setenv("KS_GOALS_HISTORY", "junk-s14-07")
        for switch in SWITCHES:
            monkeypatch.setenv(switch, "junk-s14-07")
        configure_modes(web=True)
        response = TestClient(app).get("/api/health")
        assert response.status_code == 200, response.text
        body = response.json()
        assert "goals_history" not in body
        block = body["read_fallback_mode"]
        assert not any(s in line for line in block["misconfigured"] for s in SWITCHES)
        assert block["no_engine"] == []
