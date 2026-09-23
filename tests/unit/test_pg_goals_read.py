"""Which engine answers `/goals`, and — mostly — what this flag may not touch.

`/goals` is the tab where the interesting assertions are about the *boundary*
rather than the routing, because only four of its thirteen methods can move:

  * two read `revenue_predictions`, `seasonal_indices`, `weekly_patterns` and
    `growth_metrics`, **none of which exists in Postgres**. Routed, they would
    fall back for ever while looking switched.
  * seven **write**, and `app.revenue_goals` is an hourly read replica: a
    write sent to Postgres would land in a copy and be overwritten within the
    hour.

    The boundary there is narrower than it first looks, and the first version
    of this file got it wrong. A writer *may* read through the router —
    `set_goal` computes a suggestion from the revenue history before storing
    it, and reading that history from Postgres is the point of the change.
    What must not happen is a write *statement* going through it, and that is
    what is asserted: every body handed to the router reads, and every writer
    still opens DuckDB for its own write.

The rest is this file's usual: an unknown value stops the read, a Postgres read
does not take DuckDB's lock, a fault still answers, and every routed method
returns rather than hanging.

AND THE ONE THAT WOULD HAVE BEEN INVISIBLE

The windows used to be bound with `CURRENT_DATE`, which is Kyiv's day in the
`web` container and UTC's on the Postgres server — different days for three
hours out of twenty-four. A differential test cannot find that: under the gate
both engines sit in the same timezone, so both are wrong together. It is
asserted here as a property of the *text* instead.
"""
from __future__ import annotations

import ast
import asyncio
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_goals_read
from core.duckdb_store import DuckDBStore

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "goals.py"
TIMEOUT_S = 20

ROUTED = ("get_goals", "get_smart_goals", "get_historical_revenue",
          "get_daily_revenue_for_dates", "get_predictions")

# `generate_smart_goals` is not a read, and finding that out is why it is
# alone here now.
#
# The audit filed it beside `get_predictions` because it has no INSERT of its
# own — too shallow a test. It counts `seasonal_indices`, and when the count
# is short (or `recalculate=True`, which is an argument callers pass) it runs
# the three calculators, which write those tables **in DuckDB**, and then
# reads them back. Route the read and the recompute writes to one store while
# the read comes from another, an hour behind.
#
# It belongs to the writes, with the calculators it drives.
#
# Since DN-12 it does reach the router along exactly one path: its ML signal,
# `_get_ml_forecast_total`, which reads Gold and `revenue_predictions` — both
# of which Postgres has — before the method takes its own connection. That
# path is excluded from the walk below by name, and what it routes is checked
# separately, so the compute-then-read loop over the three seasonality tables
# stays whole on DuckDB.
NOT_ROUTED = ("generate_smart_goals",)
ROUTED_ONLY_VIA = {"generate_smart_goals": ("_get_ml_forecast_total",)}
ABSENT_FROM_POSTGRES_READS = ("seasonal_indices", "weekly_patterns", "growth_metrics")

# The writes. These *may* reach the router — `set_goal` computes a suggestion
# from the revenue history before storing it, and reading that history from
# Postgres is exactly what this change is for. What they must not do is send a
# *write* through it, and must still open DuckDB for the write itself.
WRITERS = ("set_goal", "calculate_seasonality_indices", "calculate_yoy_growth",
           "calculate_weekly_patterns", "store_predictions")

BODIES = ("_GOALS_PERIOD_REVENUE_SQL", "_GOALS_WEEKLY_TREND_SQL",
          "_GOALS_DAILY_FOR_DATES_SQL", "_STORED_GOALS_SQL",
          "_PREDICTIONS_SQL")

CALLS = (
    ("get_historical_revenue", ("daily",), {}),
    ("get_historical_revenue", ("weekly",), {}),
    ("get_historical_revenue", ("monthly",), {}),
    ("get_historical_revenue", ("weekly",), {"sales_type": "all"}),
    ("get_daily_revenue_for_dates", ([date.today(), date.today() - timedelta(days=1)],), {}),
    ("get_daily_revenue_for_dates", ([],), {}),          # the early return
    ("get_predictions", (date.today() - timedelta(days=7), date.today()), {}),
)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_GOALS", raising=False)
        assert pg_goals_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        assert pg_goals_read.enabled() is True

    def test_a_typo_raises_rather_than_serving_the_other_store(self, monkeypatch):
        monkeypatch.setenv("KS_READ_GOALS", "postgress")
        with pytest.raises(ValueError, match="KS_READ_GOALS"):
            pg_goals_read.enabled()

    def test_without_a_dsn_there_is_nothing_to_ask(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert pg_goals_read.available() is False


class TestTheBoundary:
    """What must *not* be routed, and why — the load-bearing part here."""

    def _method(self, name):
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        return next(n for n in ast.walk(tree)
                    if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef))
                    and n.name == name)

    def _reaches_router(self, name, excluding=()):
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        bodies, calls = {}, {}
        for n in ast.walk(tree):
            if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef)):
                bodies[n.name] = n
                calls[n.name] = {c.func.attr for c in ast.walk(n)
                                 if isinstance(c, ast.Call)
                                 and isinstance(c.func, ast.Attribute)}

        def walk(fn, seen):
            if fn in seen:
                return False
            seen.add(fn)
            if "_goals_run" in calls.get(fn, ()):
                return True
            return any(walk(c, seen) for c in calls.get(fn, ()) if c in bodies)

        return walk(name, set(excluding))

    @pytest.mark.parametrize("name", ROUTED)
    def test_the_four_that_can_move_go_through_the_router(self, name):
        assert self._reaches_router(name), f"{name} never reaches _goals_run"

    @pytest.mark.parametrize("name", NOT_ROUTED)
    def test_the_two_reading_absent_tables_do_not(self, name):
        assert not self._reaches_router(
            name, excluding=ROUTED_ONLY_VIA.get(name, ())), (
            f"{name} reads a table Postgres does not have "
            f"(revenue_predictions / seasonal_indices / weekly_patterns / "
            f"growth_metrics) — routed, it would fall back for ever while "
            f"looking switched."
        )

    @pytest.mark.parametrize("name", sorted(
        {via for vias in ROUTED_ONLY_VIA.values() for via in vias}))
    def test_the_one_permitted_path_reads_only_what_postgres_has(self, name):
        """The exclusion above is only honest if what that path routes names
        none of the tables Postgres lacks. Walked, not listed: every body any
        function under it hands to `_goals_run`."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        module = {n.targets[0].id: n.value.value
                  for n in ast.walk(tree)
                  if isinstance(n, ast.Assign) and len(n.targets) == 1
                  and isinstance(n.targets[0], ast.Name)
                  and isinstance(n.value, ast.Constant)
                  and isinstance(n.value.value, str)}
        bodies = {n.name: n for n in ast.walk(tree)
                  if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef))}

        routed, todo, seen = [], [name], set()
        while todo:
            fn = todo.pop()
            if fn in seen or fn not in bodies:
                continue
            seen.add(fn)
            for call in ast.walk(bodies[fn]):
                if not (isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Attribute)):
                    continue
                if call.func.attr == "_goals_run":
                    routed += [n.id for n in ast.walk(call.args[0])
                               if isinstance(n, ast.Name) and n.id in module]
                else:
                    todo.append(call.func.attr)

        assert routed, f"{name} no longer reaches the router — drop the exclusion"
        for body in routed:
            for table in ABSENT_FROM_POSTGRES_READS:
                assert table not in module[body], (
                    f"{body}, routed from {name}, reads {table}: Postgres has "
                    f"only an hourly replica of it, behind the recompute this "
                    f"path follows")

    @pytest.mark.parametrize("name", WRITERS)
    def test_a_writer_still_writes_to_duckdb(self, name):
        """The first version of this asserted that no writer reaches the
        router at all, and it was wrong: `set_goal` computes a suggestion from
        the revenue history before storing, and reading that from Postgres is
        the point. The boundary is narrower — a writer may *read* through the
        router, and must still open DuckDB to write."""
        node = self._method(name)
        calls = {c.func.attr for c in ast.walk(node)
                 if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)}
        assert "connection" in calls, (
            f"{name} no longer opens DuckDB. `app.revenue_goals` is an hourly "
            f"read replica and DuckDB is still the writer, so a write that "
            f"went to Postgres would be overwritten within the hour."
        )

    def test_no_write_statement_ever_reaches_the_router(self):
        """The invariant the parametrised tests above approximate: whatever is
        handed to `_goals_run` reads."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        module = {n.targets[0].id: n.value.value
                  for n in ast.walk(tree)
                  if isinstance(n, ast.Assign) and len(n.targets) == 1
                  and isinstance(n.targets[0], ast.Name)
                  and isinstance(n.value, ast.Constant)
                  and isinstance(n.value.value, str)}
        seen = 0
        for call in ast.walk(tree):
            if not (isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "_goals_run"):
                continue
            seen += 1
            names = [n.id for n in ast.walk(call.args[0])
                     if isinstance(n, ast.Name) and n.id in module]
            assert names, f"line {call.lineno}: the body is not a module constant"
            for name in names:
                upper = module[name].upper()
                for verb in ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE", "CREATE "):
                    assert verb not in upper, f"{name} writes, and it is routed"
        assert seen >= 4, f"expected at least four routed calls, found {seen}"

    def test_the_forecast_table_now_has_a_postgres_name(self):
        """This test used to assert the opposite — that none of the four had a
        home — and said that when one appeared it should be deleted and the
        read moved. Revision 0025 brought them, so it was."""
        from core.sql_dialect import DUCKDB, POSTGRES

        assert POSTGRES.revenue_predictions == "app.revenue_predictions"
        assert DUCKDB.revenue_predictions == "revenue_predictions"

    def test_the_other_three_are_still_only_a_replication_target(self):
        """They have a Postgres home too, and no dialect entry — deliberately.
        Nothing reads them through a routed body: `generate_smart_goals` writes
        them before reading, so it stays whole on DuckDB until the writes
        move."""
        from core.sql_dialect import POSTGRES

        for name in ("seasonal_indices", "weekly_patterns", "growth_metrics"):
            assert not hasattr(POSTGRES, name), (
                f"{name} gained a dialect entry — if something now reads it "
                f"through the router, `generate_smart_goals`' compute-then-read "
                f"loop has to be resolved first"
            )


class TestItDoesNotTakeTheDuckDbLock:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,args,kwargs", CALLS,
        ids=[f"{n}{a[0] if a and not isinstance(a[0], list) else ''}" for n, a, k in CALLS],
    )
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch, name, args, kwargs,
    ):
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / f"g-{name}-{len(args)}.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError(f"{name} reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_goals_read.fetch", new=AsyncMock(return_value=[])):
                await getattr(store, name)(*args, **kwargs)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KS_READ_GOALS", raising=False)
        store = DuckDBStore(db_path=tmp_path / "g-mirror.duckdb")
        await store.connect()
        try:
            with patch("core.pg_goals_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_historical_revenue("weekly")
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "g-fallback.duckdb")
        await store.connect()
        try:
            with patch("core.pg_goals_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                out = await store.get_historical_revenue("weekly")
            assert out["periodType"] == "weekly"    # answered, not raised
        finally:
            await store.close()


class TestEveryRoutedMethodReturns:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,args,kwargs", CALLS,
        ids=[f"{n}{a[0] if a and not isinstance(a[0], list) else ''}" for n, a, k in CALLS],
    )
    async def test_it_returns_rather_than_hanging(
        self, tmp_path, monkeypatch, name, args, kwargs,
    ):
        monkeypatch.delenv("KS_READ_GOALS", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"g-ret-{name}-{len(args)}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(
                getattr(store, name)(*args, **kwargs), timeout=TIMEOUT_S)
        finally:
            await store.close()


class TestTheRoutedBodies:
    def test_both_renderings_leave_no_hole(self):
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables
        from core.repositories import goals

        for name in BODIES:
            sql = getattr(goals, name)
            sql = sql.replace("{where}", "TRUE").replace("{grain}", "s.order_date")
            for dialect in (DUCKDB, POSTGRES):
                rendered = render_tables(sql, dialect)
                assert "{" not in rendered and "}" not in rendered, name

    def test_the_two_renderings_differ(self):
        from core.sql_dialect import DUCKDB, POSTGRES, render_tables
        from core.repositories import goals

        for name in BODIES:
            sql = getattr(goals, name)
            sql = sql.replace("{where}", "TRUE").replace("{grain}", "s.order_date")
            assert render_tables(sql, DUCKDB) != render_tables(sql, POSTGRES), name

    def test_no_body_computes_the_kyiv_date(self):
        """`silver.orders.order_date` is already it."""
        from core.repositories import goals

        for name in BODIES:
            assert "timezone(" not in getattr(goals, name), name

    def test_no_body_carries_its_own_return_or_source_rule(self):
        """Silver has `is_return` — which prefers KeyCRM's status group — and
        `is_active_source`. A second list of status ids here is a second
        definition, and one of these bodies had a channel filter the other
        lacked."""
        from core.repositories import goals

        for name in BODIES:
            body = getattr(goals, name)
            assert "status_id" not in body, name
            assert "source_id IN" not in body, name

    def test_the_window_is_not_bound_with_current_date(self):
        """The two engines disagree about what day `CURRENT_DATE` is for three
        hours out of twenty-four, and under the gate they sit in one timezone
        — so this cannot be a differential test.

        Asserted on the routed method's own tree, with its docstring taken out:
        the docstring explains the trap and contains the words."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        node = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.AsyncFunctionDef)
                    and n.name == "get_historical_revenue")
        doc = ast.get_docstring(node, clean=False)
        strings = [n.value for n in ast.walk(node)
                   if isinstance(n, ast.Constant) and isinstance(n.value, str)
                   and n.value != doc]
        for text in strings:
            assert "CURRENT_DATE" not in text.upper(), (
                "the window is bound with CURRENT_DATE again — use "
                "core.sql_dialect.TODAY_IN_KYIV"
            )
        assert any("TODAY_IN_KYIV" in n.id
                   for n in ast.walk(node) if isinstance(n, ast.Name)), (
            "TODAY_IN_KYIV is no longer used to bound the window"
        )
