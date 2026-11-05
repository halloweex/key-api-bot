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
- nothing in the routers' files reads an engine round them — S14-05's walk,
  which the merge of S14-06 brought: only a router, a reader rule 5 holds
  clean, or a function that writes, by its writes alone (never a writer
  module's reader); the cohorts' ClickHouse through `_analytics_rows` alone;
- no reader module carries `ENV`, `enabled` or `available` — these nine
  and every other `core/pg_*_read*.py` — and `ch_cohorts` no
  `KS_READ_COHORTS`;
- junk in every one of those variables, and in `KS_GOALS_HISTORY`, starts
  web and imports every module (rule 3 of the plan).

The numbers are the `tests/integration/*_two_engines.py` files', against a
real Postgres and literals DuckDB answered before its side went; every route
through the real app is `tests/unit/test_read_fallback_http.py`'s.
"""
from __future__ import annotations

import ast
import functools
import importlib
import logging
import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional
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

    store = DuckDBStore()

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
        assert read_fallback.refusals() == {}

    @pytest.mark.parametrize("module", MODULES)
    def test_the_module_carries_no_switch(self, module):
        """Mutation: put `ENV`/`enabled`/`available` back — the sites walk
        would then take the module for a switch, and a reader could start
        asking it again."""
        m = importlib.import_module(f"core.{module}")
        assert not any(hasattr(m, name) for name in ("ENV", "enabled", "available",
                                                     "_VALID", "HISTORY_ENV"))

    def test_no_reader_module_carries_one(self):
        """The same of every reader module, read off `core/` rather than
        named: S14-05's and S14-06's, a reader that comes later, and what the
        merge of main brought in (#327's pixel-only breakdown reads through
        `pg_traffic_read`). Mutation: `ENV = "KS_READ_TRAFFIC"` back in
        `pg_traffic_read`."""
        readers = sorted({p.stem for p in (ROOT / "core").glob("pg_*_read*.py")}
                         | {"ch_cohorts"})
        assert set(MODULES) <= set(readers) and len(readers) >= 20, readers
        carrying = {name: [a for a in ("ENV", "enabled", "available", "_VALID",
                                       "HISTORY_ENV")
                           if hasattr(importlib.import_module(f"core.{name}"), a)]
                    for name in readers}
        assert {name: found for name, found in carrying.items() if found} == {}

    def test_the_walk_finds_none_of_them_a_switch(self):
        from tests.unit.test_read_fallback_sites import read_switch_modules

        assert not read_switch_modules() & set(MODULES)


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
        assert (f"read refused: {surface} — its engine failed, and no other "
                f"store may answer it "
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
        assert "read_fallbacks" not in health
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
        assert read_fallback.refusals() == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reader,module,fn,call", READERS, ids=[r[0] for r in READERS])
    async def test_a_reader_with_no_dsn_meets_its_own_error(
        self, store, fresh_read_fallback, monkeypatch, reader, module, fn, call,
    ):
        """A reader with no fallback, its switch naming Postgres and no DSN:
        it meets `core.pg`'s own error, counted nowhere — no `no_address`
        stands behind the switch since S14-07. No pool patched, no DSN.
        Mutation: refuse it (a refusal is counted). (Until step 14's S14-16
        this also held what `/api/health` published of such a switch under
        `read_fallback_mode.misconfigured`, and the start's line, both gone
        with it.)"""
        from core import pg

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setattr(pg, "_pool", None)
        _set(monkeypatch, "postgres")

        with pytest.raises(RuntimeError, match="KS_PG_DSN is not set"):
            await call(store)
        assert fresh_read_fallback.refusals() == {}

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


# The files of the routers above, with the `_*_run` routers in each and the
# readers there that refuse for themselves (rule 5 holds both).
THIRD_SET = [
    ("core/repositories/expenses.py", {"_expenses_run"}, set()),
    ("core/repositories/inventory.py", set(), {"_inventory_batch"}),
    ("core/repositories/goals.py", {"_goals_run", "_goal_tables_run"}, set()),
]
COHORTS_FILE = "core/repositories/customers.py"
# A chain's writer, which the files above call to write: a write is not a
# read round the router, and its failure is the writer's own (S14-11 takes
# the chains' mode away). Every other engine module is a read's. A module
# matching this is no licence by itself: it carries readers too
# (`pg_inventory_write.read_snapshot_calendar`, the chains' `preflight`), so
# what the files may call in one is decided by the function (`_writer_reaches`).
# `core.pg_operational` matched too, until step 14's S14-13 deleted it.
WRITE_MODULE = re.compile(r"core\.pg_[a-z0-9_]+_write")
# The writers' mode, which every write asks first; constant since S14-00.
MODE_PREDICATE = "writes_postgres"


def _engine_bindings(tree: ast.AST) -> Dict[str, str]:
    """Every name the file binds to an engine module or to a function of
    one, and the module: `from core import pg_x as y`, `from core.pg_x
    import f`, `import core.pg_x as y`."""
    from tests.unit.test_read_fallback_sites import ENGINE_PREFIXES, _is_engine_module

    out: Dict[str, str] = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name == "pg" or a.name.startswith(ENGINE_PREFIXES):
                    out[a.asname or a.name] = f"core.{a.name}"
        elif isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module):
            for a in n.names:
                out[a.asname or a.name] = n.module
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.asname and _is_engine_module(a.name):
                    out[a.asname] = a.name
    return out


def _imported_names(tree: ast.AST) -> Dict[str, str]:
    """`{alias: name}` for every function the file imports out of an engine
    module by its name (`from core.pg_x import f as g`), so a reach through
    `g` is judged as `f`."""
    from tests.unit.test_read_fallback_sites import _is_engine_module

    return {a.asname or a.name: a.name
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module)
            for a in n.names}


@functools.lru_cache(maxsize=None)
def _chain_writers(module: str) -> FrozenSet[str]:
    """The functions of a writer module that write: the latch walk's writers
    (`test_chain_latch._writers` — a public async function reaching a
    connection or a writing statement, bar the readers it names), then every
    function of the module calling one of them by name, to a fixed point
    (`pg_operational.replicate_after_manual_expense`, the hourly copy called
    sooner, was one until step 14's S14-13). A reader of the module — `read_snapshot_calendar`, `preflight`,
    `reading`, `reads_the_chain` — is none of them."""
    from tests.unit.test_chain_latch import _writers

    mod = importlib.import_module(module)
    found = set(_writers(mod))
    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    functions = [n for n in tree.body
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    grew = True
    while grew:
        grew = False
        for fn in functions:
            if fn.name in found:
                continue
            called = {getattr(c.func, "id", getattr(c.func, "attr", ""))
                      for c in ast.walk(fn) if isinstance(c, ast.Call)}
            if called & found:
                found.add(fn.name)
                grew = True
    return frozenset(found)


def _called_name(reach: str, names: Dict[str, str]) -> Optional[str]:
    """The function one engine reach calls or holds, by its name in its
    module; None for an import, which names no function."""
    what = reach[len("engine: "):]
    if what.startswith("imports "):
        return None
    name = (what[:-2] if what.endswith("()") else what).rsplit(".", 1)[-1]
    return names.get(name, name)


def _writer_reaches(seen: List[str], bindings: Dict[str, str],
                    names: Dict[str, str]) -> List[str]:
    """Which of one function's reaches its writing allows: none unless the
    function writes — calls a function `_chain_writers` counts in a writer
    module — and then its writes, its imports of writer modules and their
    mode predicate. A writer module's reader is never allowed, from a writer
    or a reader alike."""
    def allowed(reach: str, writes_only: bool) -> bool:
        module = _module_of(reach, bindings)
        if module is None or not WRITE_MODULE.fullmatch(module):
            return False
        name = _called_name(reach, names)
        if name is not None and name in _chain_writers(module):
            return True
        return not writes_only and (name is None or name == MODE_PREDICATE)

    if not any(allowed(r, writes_only=True) for r in seen):
        return []
    return [r for r in seen if allowed(r, writes_only=False)]


def _module_of(reach: str, bindings: Dict[str, str]) -> Optional[str]:
    """The engine module one reach of S14-05's walk names, or None for a
    reach that is not an engine's (the old shape, the environment)."""
    if not reach.startswith("engine: "):
        return None
    what = reach[len("engine: "):]
    if what.startswith("imports "):
        return what[len("imports "):]
    name = what[:-2] if what.endswith("()") else what
    if name.startswith("core."):
        return name.rsplit(".", 1)[0]
    head = name.split(".", 1)[0]
    if head in bindings:
        return bindings[head]
    return "core.pg" if name.rsplit(".", 1)[-1] == "get_pool" else None


def round_the_routers(source: str, relpath: str) -> List[str]:
    """What in `source` reads an engine round its routers: S14-06's
    `round_a_refusal` — S14-05's walk of the tab modules, allowing a router
    nothing and a reader rule 5 holds clean only the engine calls rule 5
    judges — less what a function that writes reaches to write
    (`_writer_reaches`). Judged per function: a module allowance let any
    reader call a writer module's own reader (`read_snapshot_calendar` in
    `get_stock_summary`) and pass."""
    from collections import Counter

    from tests.unit.test_dashboard_routers_postgres_only import round_a_refusal

    tree = ast.parse(source)
    bindings, names = _engine_bindings(tree), _imported_names(tree)
    by_function: Dict[str, List[str]] = {}
    for line in round_a_refusal(source, relpath):
        function, reach = line.split(": ", 1)
        by_function.setdefault(function, []).append(reach)
    out = []
    for function, seen in by_function.items():
        left = Counter(seen) - Counter(_writer_reaches(seen, bindings, names))
        out += [f"{function}: {r}" for r in sorted(left.elements())]
    return out


class TestNothingGoesRoundTheRouter:
    """The merge of S14-06 (S14-05's merge of main, #327's pixel-only
    breakdown) brought the walk that holds /traffic's, /margin's and product
    intelligence's readers to their router, and S14-06 held the dashboard to
    it: a reader calling its engine itself answers a failure with an
    uncounted 500, which rule 5 (it judges routers and what refuses) and the
    HTTP sweep's violations (they judge refusals) never see. Here the same
    walk holds this PR's: every function of `/expenses`', `/inventory`'s and
    `/goals`' files reaches an engine only as a router, as a reader rule 5
    holds clean, or as a function that writes — and that one only by its
    writes, by importing the writer's module and by asking its
    `writes_postgres`, never by a writer module's reader; the cohorts' file —
    half of it the SMS tab's, which keeps its own router until S14-08 — reads
    ClickHouse through `_analytics_rows` alone, and the cohort readers reach
    no engine at all. Mutations: `await pg_inventory_read.fetch_many(...)` in
    `get_stock_summary`, `await pg_goals_read.fetch(...)` in `set_goal`,
    `await ch_cohorts.fetch(...)` in `get_cohort_retention`, and
    `await pg_inventory_write.read_snapshot_calendar(date.today())` in
    `get_stock_summary` (X4 of the merge's review: a writer module's reader,
    which the module allowance let through)."""

    @pytest.mark.parametrize("path,routers,refusing", THIRD_SET,
                             ids=[p for p, _r, _f in THIRD_SET])
    def test_nothing_but_the_router_reads_an_engine(self, path, routers, refusing):
        from tests.unit.test_read_fallback_sites import read_switch_modules, walk_source
        from tests.unit.test_read_routers_postgres_only import (
            _what_reaches_round_the_router,
        )

        source = (ROOT / path).read_text(encoding="utf-8")
        found, reaches = _what_reaches_round_the_router(source)
        assert found == routers, found
        clean = {s.function for s in walk_source(source, path, read_switch_modules())
                 if s.rule == "refuse" and s.ok}
        assert refusing <= clean, clean
        assert round_the_routers(source, path) == []
        # Non-vacuity: the walk read the tab's readers, and saw the writers
        # beside them reach their chain's writer, which is all it allowed —
        # every one of them by a write, not by the module alone.
        tree = ast.parse(source)
        readers = [n.name for n in ast.walk(tree)
                   if isinstance(n, ast.AsyncFunctionDef) and n.name.startswith("get_")]
        assert len(readers) >= 4, readers
        bindings, names = _engine_bindings(tree), _imported_names(tree)
        writers = {fn for fn, seen in reaches.items() if fn not in refusing
                   and sorted(_writer_reaches(seen, bindings, names)) == sorted(seen)}
        assert len(writers) >= 3, sorted(reaches)
        assert not writers & set(readers), sorted(writers & set(readers))

    def test_only_the_cohort_router_reads_clickhouse(self):
        from tests.unit.test_read_fallback_sites import read_switch_modules, walk_source
        from tests.unit.test_read_routers_postgres_only import (
            _what_reaches_round_the_router,
        )

        source = (ROOT / COHORTS_FILE).read_text(encoding="utf-8")
        tree = ast.parse(source)
        bindings = _engine_bindings(tree)
        _routers, reaches = _what_reaches_round_the_router(source)
        clickhouse = {fn for fn, seen in reaches.items()
                      if any((_module_of(r, bindings) or "").startswith("core.ch_")
                             for r in seen)}
        assert clickhouse == {"_analytics_rows"}, clickhouse
        assert any(s.function == "_analytics_rows" and s.rule == "refuse" and s.ok
                   for s in walk_source(source, COHORTS_FILE, read_switch_modules()))
        cohort_readers = {
            fn.name for fn in ast.walk(tree)
            if isinstance(fn, ast.AsyncFunctionDef) and fn.name != "_analytics_rows"
            and any(isinstance(n, ast.Attribute) and n.attr == "_analytics_rows"
                    for n in ast.walk(fn))}
        assert len(cohort_readers) >= 4, cohort_readers
        assert not cohort_readers & set(reaches), {
            fn: reaches[fn] for fn in cohort_readers & set(reaches)}

    ROUTER = ("from core import read_fallback\n"
              "class T:\n"
              "    async def _t_run(self, sql):\n"
              "        from core import pg_goals_read\n"
              "        try:\n"
              "            return await pg_goals_read.fetch(sql, [])\n"
              "        except Exception as exc:\n"
              "            raise read_fallback.refuse('goals', exc)\n\n")
    WRITER = ("    async def set_x(self):\n"
              "        from core import pg_goals_write\n"
              "        if pg_goals_write.writes_postgres():\n"
              "            return await pg_goals_write.set_goal(1)\n")

    @pytest.mark.parametrize("writer", [
        WRITER,
        # Imported by its name and renamed: judged as `set_goal`.
        ("    async def set_x(self):\n"
         "        from core.pg_goals_write import set_goal as put, writes_postgres\n"
         "        if writes_postgres():\n"
         "            return await put(1)\n"),
    ], ids=["by_its_module", "by_another_name"])
    def test_a_writer_reaching_its_chain_passes(self, writer):
        assert round_the_routers(self.ROUTER + writer, "core/repositories/x.py") == []

    @pytest.mark.parametrize("method", [
        ("    async def get_x(self):\n"
         "        from core import pg_goals_read\n"
         "        return await pg_goals_read.fetch('q', [])\n"),
        ("    async def get_x(self):\n"
         "        from core import ch_cohorts\n"
         "        return await ch_cohorts.fetch('q', [], ())\n"),
        ("    async def get_x(self):\n"
         "        from core.pg import get_pool\n"
         "        return await get_pool()\n"),
        WRITER + "        return await pg_goals_read.fetch('q', [])\n",
        WRITER.replace("pg_goals_write.writes_postgres()",
                       "os.getenv('KS_READ_GOALS')"),
        # X4: a reader calling a writer module's reader.
        ("    async def get_x(self):\n"
         "        from core import pg_inventory_write\n"
         "        return await pg_inventory_write.read_snapshot_calendar(1)\n"),
        # A writer reaching the same reader by another name.
        WRITER + ("        from core.pg_inventory_write import read_snapshot_calendar as cal\n"
                  "        return await cal(1)\n"),
        # A writer reading through its own module's reader.
        WRITER + "        return await pg_inventory_write.read_snapshot_calendar(1)\n",
        # A function that writes nothing may not even ask the writers' mode.
        ("    async def get_x(self):\n"
         "        from core import pg_goals_write\n"
         "        return pg_goals_write.writes_postgres()\n"),
    ], ids=["bare_reader", "bare_clickhouse_reader", "bare_pool",
            "writer_reads_round_the_router", "writer_reads_a_switch",
            "reader_reads_a_writer_modules_reader",
            "writer_reads_it_by_another_name",
            "writer_reads_a_writer_modules_reader",
            "reader_asks_the_writers_mode"])
    def test_the_walk_bites(self, method):
        source = ("from core import pg_goals_read\nfrom core import pg_inventory_write\n"
                  + self.ROUTER + method)
        assert round_the_routers(source, "core/repositories/x.py") != []

    def test_a_writer_module_holds_readers_the_allowance_does_not_cover(self):
        """Non-vacuity of the per-function rule: the modules the files call
        carry readers, and `_chain_writers` counts none of them."""
        inventory = _chain_writers("core.pg_inventory_write")
        assert {"upsert_offers", "upsert_stocks", "record_inventory_snapshot"} <= inventory
        assert not {"read_snapshot_calendar", "preflight", MODE_PREDICATE} & inventory
        assert not {"reads_postgres", "reads_the_chain"} & _chain_writers("core.pg_goals_write")


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

        monkeypatch.setenv("KS_GOALS_HISTORY", "junk-s14-07")
        for switch in SWITCHES:
            monkeypatch.setenv(switch, "junk-s14-07")
        configure_modes(web=True)
        response = TestClient(app).get("/api/health")
        assert response.status_code == 200, response.text
        body = response.json()
        assert "goals_history" not in body
        block = body["read_fallback_mode"]
        # The refusals alone since step 14's S14-16: no `misconfigured`
        # left to name a switch in.
        assert set(block) == {"refused"}
