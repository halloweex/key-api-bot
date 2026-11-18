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
  `/api/warehouse/status` read the three variables, as `reader:*` keys,
  until S14-15 deleted it);
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
rule 5, and that nothing in `revenue.py` but a router or a refusing helper
reaches an engine at all is `TestNothingGoesRoundARefusal`'s, below.
"""
from __future__ import annotations

import ast
import logging
from datetime import date
from pathlib import Path
from typing import List, Set
from unittest.mock import AsyncMock

import pytest

from core import read_fallback

ROOT = Path(__file__).resolve().parents[2]
D1, D2 = date(2026, 8, 1), date(2026, 8, 3)


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
    from core.store import AnalyticsStore

    store = AnalyticsStore()

    def trap(*_a, **_k):
        raise AssertionError("a Postgres-only dashboard read reached for DuckDB")

    # `_fetch_one`/`_fetch_all` went with the store's DuckDB half in step
    # 14's S14-18 and `connection()` is a refusal since; trapped all the same,
    # so a reader that reaches for any of them fails here by name.
    for name in ("connection", "_fetch_one", "_fetch_all"):
        monkeypatch.setattr(type(store), name, trap, raising=False)
    read_fallback.reset_counts()
    yield store
    read_fallback.reset_counts()
    assert not (tmp_path / "never.duckdb").exists()


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
        # `refuse`'s line, word for word: `read_refused`'s lever greps "read
        # refused", and a fallback was grepped by the phrase below. (Until
        # step 14's S14-16 it named KS_READ_FALLBACK=off as what refused.)
        assert (f"read refused: {surface} — its engine failed, and no other "
                f"store may answer it "
                f"(1 refused since start): connection refused") in caplog.text
        assert "falling back to DuckDB" not in caplog.text

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


# `core/repositories/revenue.py`: the four `_*_run` routers it holds (S14-05's
# three and the dashboard's), and the dashboard's four helpers that refuse for
# themselves.
REVENUE = "core/repositories/revenue.py"
REVENUE_ROUTERS = {"_dashboard_run", "_reports_run", "_marketing_run", "_lookups_run"}
REFUSING_HELPERS = {"_pg_gold_summary", "_pg_gold_series", "_pg_silver",
                    "get_manager_sales_365d"}


def _judged_by_rule_5(fn: ast.AST, aliases: Set[str], names: Set[str]) -> List[str]:
    """The engine reaches of `fn`, in the words of S14-05's walk, that rule 5
    of `test_read_fallback_sites.py` judges: every call its `_engine_calls`
    returns, and every import in `fn` binding what those calls are read by —
    a module by its alias (`from core import pg_x`, `import core.pg_x as y`),
    spelled out (`import core.pg_x`), or a function by its name (`from
    core.pg_x import y`) — and the same of a `core.ch_*` module, which rule 5
    reads since step 14's S14-07. `fn`'s own nodes only, as rule 5 reads
    it."""
    from tests.unit.test_read_fallback_sites import (
        ENGINE_PREFIXES, _engine_calls, _is_engine_module, _own_nodes,
    )
    from tests.unit.test_read_routers_postgres_only import _dotted

    judged = [f"engine: {_dotted(c.func)}()" for c in _engine_calls(fn, aliases, names)]
    for n in _own_nodes(fn):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            judged += [f"engine: imports core.{a.name}" for a in n.names
                       if a.name.startswith(ENGINE_PREFIXES)]
        elif isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module):
            judged.append(f"engine: imports {n.module}")
        elif isinstance(n, ast.Import):
            judged += [f"engine: imports {a.name}" for a in n.names
                       if _is_engine_module(a.name)]
    return judged


def _engine_held_as_a_value(fn: ast.AST, aliases: Set[str], names: Set[str]) -> List[str]:
    """Where `fn` holds an engine module or function other than as the
    callee of a call rule 5 judges. `fetch = pg_gold_read.fetch_series`, then
    `await fetch(...)` outside the `try`, is a call neither walk sees."""
    from tests.unit.test_read_fallback_sites import (
        _engine_calls, _names_an_engine_module, _own_nodes,
    )
    from tests.unit.test_read_routers_postgres_only import _dotted

    callees = set()
    for call in _engine_calls(fn, aliases, names):
        node = call.func
        while isinstance(node, ast.Attribute):
            callees.add(id(node))
            node = node.value
        callees.add(id(node))
    held = []
    for n in _own_nodes(fn):
        if id(n) in callees:
            continue
        if (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                and (n.id in aliases or n.id in names or n.id == "get_pool")):
            held.append(f"engine: {n.id} held as a value")
        elif isinstance(n, ast.Attribute) and _names_an_engine_module(n):
            held.append(f"engine: {_dotted(n)} held as a value")
    return held


def round_a_refusal(source: str, relpath: str) -> List[str]:
    """What in `source` reaches what only a router or a refusing helper may,
    and is neither. S14-05's walk of the tab modules
    (`test_read_routers_postgres_only._what_reaches_round_the_router`: an
    engine, the old shape's machinery, the environment), with one allowance:
    a function rule 5 of `test_read_fallback_sites.py` holds clean — it
    refuses, and every engine call it makes sits inside the `try` whose
    handler is the refusal — may reach an engine, but only by what rule 5
    judges. Every other engine reach of it is still reported: S14-05's walk
    sees calls rule 5 does not (a function imported by its name was one,
    until the review of this PR's merge widened rule 5), and an engine held
    as a value is called by a name no walk reads. A name defined twice in
    the file gets no allowance, since the walk keys its reaches by name."""
    from collections import Counter

    from tests.unit.test_read_fallback_sites import (
        _engine_aliases, _engine_names, _functions, read_switch_modules, walk_source,
    )
    from tests.unit.test_read_routers_postgres_only import (
        _what_reaches_round_the_router,
    )

    tree = ast.parse(source)
    aliases, names = _engine_aliases(tree), _engine_names(tree)
    _routers, reaches = _what_reaches_round_the_router(source)
    defined = Counter(getattr(fn, "name", "") for fn in _functions(tree))
    clean = {s.function for s in walk_source(source, relpath, read_switch_modules())
             if s.rule == "refuse" and s.ok and defined[s.function] == 1}
    out = []
    for function, found in sorted(reaches.items()):
        left = Counter(found)
        if function in clean:
            fn = next(f for f in _functions(tree) if getattr(f, "name", "") == function)
            left -= Counter(_judged_by_rule_5(fn, aliases, names))
            left += Counter(_engine_held_as_a_value(fn, aliases, names))
        out += [f"{function}: {f}" for f in sorted(left.elements())]
    return out


class TestNothingGoesRoundARefusal:
    """The merge of S14-05's main (#327, the pixel-only breakdown) brought
    `test_nothing_but_the_router_reaches_an_engine`: in the three tab modules
    whose every reader goes through one router, nothing but that router may
    reach an engine, because a reader calling Postgres itself answers an
    engine failure with an uncounted 500 — rule 5 judges only routers and
    functions that refuse, and the HTTP sweep only refusals. It left
    `revenue.py` to this PR. Here the dashboard's file meets the same walk,
    with the four helpers that refuse for themselves allowed an engine.
    Mutations: a bare `await pg_dashboard_read.fetch(...)` in
    `get_sales_by_source`, which no other test sees; and the merge review's
    MD2, `from core.pg_gold_read import fetch_series` and an `await
    fetch_series(...)` before `_pg_gold_summary`'s `try` — a 500 on
    `/api/summary?source_id=2` that the first form of the allowance dropped,
    and that this and rule 5 each find now."""

    def test_the_dashboard_file(self):
        source = (ROOT / REVENUE).read_text(encoding="utf-8")
        from tests.unit.test_read_routers_postgres_only import (
            _what_reaches_round_the_router,
        )

        routers, reaches = _what_reaches_round_the_router(source)
        assert routers == REVENUE_ROUTERS, routers
        # Non-vacuity: the walk read the helpers, and found them clean.
        assert set(reaches) == REFUSING_HELPERS, sorted(reaches)
        assert round_a_refusal(source, REVENUE) == []

    ROUTER = ("from core import read_fallback\n"
              "class T:\n"
              "    async def _t_run(self, sql):\n"
              "        from core import pg_dashboard_read\n"
              "        try:\n"
              "            return await pg_dashboard_read.fetch(sql, [])\n"
              "        except Exception as exc:\n"
              "            raise read_fallback.refuse('dashboard', exc)\n\n")
    HELPER = ("    async def _pg_x(self):\n"
              "        from core import pg_gold_read\n"
              "        try:\n"
              "            return await pg_gold_read.fetch_summary(1)\n"
              "        except Exception as exc:\n"
              "            raise read_fallback.refuse('dashboard', exc)\n")

    # The allowance is what rule 5 judges, in each spelling it reads: with
    # the call inside the `try`, nothing is left to report.
    @pytest.mark.parametrize("helper", [
        HELPER,
        HELPER.replace("        try:\n",
                       "        from core.pg_gold_read import fetch_series\n"
                       "        try:\n"
                       "            await fetch_series(1)\n"),
        HELPER.replace("        try:\n",
                       "        import core.pg_gold_read\n"
                       "        try:\n"
                       "            await core.pg_gold_read.fetch_series(1)\n"),
        # ClickHouse's modules, which rule 5 reads since step 14's S14-07:
        # by the module, and by a name imported from it.
        HELPER.replace("        try:\n",
                       "        from core import ch_cohorts\n"
                       "        try:\n"
                       "            await ch_cohorts.fetch('q', [], ())\n"),
        HELPER.replace("        try:\n",
                       "        from core.ch_cohorts import fetch\n"
                       "        try:\n"
                       "            await fetch('q', [], ())\n"),
    ], ids=["module", "imported_name", "spelled_out", "clickhouse_module",
            "clickhouse_imported_name"])
    def test_a_clean_helper_passes(self, helper):
        assert round_a_refusal(self.ROUTER + helper, REVENUE) == []

    @pytest.mark.parametrize("method", [
        ("    async def get_x(self):\n"
         "        from core import pg_dashboard_read\n"
         "        return await pg_dashboard_read.fetch('q', [])\n"),
        HELPER.replace("        try:\n",
                       "        rows = await pg_gold_read.fetch_series(1)\n"
                       "        try:\n"),
        # The merge review's MD2: rule 5 did not read a call by a name
        # imported from `core.pg_*`, so it held the helper clean and the
        # allowance dropped what S14-05's walk had found.
        HELPER.replace("        try:\n",
                       "        from core.pg_gold_read import fetch_series\n"
                       "        rows = await fetch_series(1)\n"
                       "        try:\n"),
        # No walk reads a call by a name the engine was handed to.
        HELPER.replace("        try:\n",
                       "        fetch = pg_gold_read.fetch_series\n"
                       "        rows = await fetch(1)\n"
                       "        try:\n"),
        HELPER.replace("        try:\n",
                       "        rows = await (lambda: pg_gold_read.fetch_series(1))()\n"
                       "        try:\n"),
        HELPER.replace("            raise read_fallback.refuse('dashboard', exc)\n",
                       "            return None\n"),
        ("    async def _pg_x(self):\n"
         "        import os\n"
         "        from core import pg_gold_read\n"
         "        try:\n"
         "            return await pg_gold_read.fetch_summary(os.getenv('KS_READ_GOLD'))\n"
         "        except Exception as exc:\n"
         "            raise read_fallback.refuse('dashboard', exc)\n"),
        # Two functions of one name: the walk keys its reaches by name, so
        # neither is allowed anything.
        HELPER + "class U:\n" + HELPER,
    ], ids=["bare_reader", "helper_bare_second_call",
            "helper_second_call_by_imported_name", "helper_holds_the_engine_as_a_value",
            "helper_calls_it_in_a_lambda", "helper_silent_handler",
            "helper_reads_its_switch", "helper_name_defined_twice"])
    def test_the_walk_bites(self, method):
        assert round_a_refusal(self.ROUTER + method, REVENUE) != []


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
