"""Which engine answers `/goals`, and — mostly — what this flag may not touch.

`/goals` is the tab where the interesting assertions are about the *boundary*
rather than the routing, because not every one of its thirteen methods can
move:

  * `generate_smart_goals` reads `seasonal_indices`, `weekly_patterns` and
    `growth_metrics`, which DuckDB writes. Postgres has had those three since
    revision 0025, but only as an hourly replica, so a routed read would
    return them up to an hour behind `POST /goals/recalculate`. They are read
    where they are written until their writer moves (chain 7b-3). Its ML
    signal is routed, and reads only Gold and `revenue_predictions` (which
    moved with `get_predictions`, #183).
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

# `generate_smart_goals` was not a read, and finding that out is why it is
# alone here.
#
# The audit filed it beside `get_predictions` because it has no INSERT of its
# own — too shallow a test. It counted `seasonal_indices`, and when the count
# was short (or `recalculate=True`) it ran the three calculators, which wrote
# those tables **in DuckDB**, and then read them back. Chain 7b-1 (OD-14 (i))
# took the write branch out — it reads only now — but the three tables it
# reads are still written in DuckDB, by `recalculate_goal_tables`, and
# Postgres holds an hourly replica of them. So they are still read where they
# are written.
#
# It reaches the router along four paths, and each is excluded from the walk
# below by name, with what it routes checked separately: its ML signal,
# `_get_ml_forecast_total`, which reads Gold and `revenue_predictions` (since
# DN-12); and, under `KS_GOALS_HISTORY=silver` (chain 7b-2), the three history
# reads it asks before taking its own connection — the growth cap, last
# year's month and the recent months — which read `{silver_orders}`. None of
# the four names a table Postgres holds only as a replica, so the three
# seasonality tables stay read where they are written.
#
# Since chain 7b-3 "where they are written" has two answers, and
# `_goal_tables_run` asks that chain for it — DuckDB while it writes DuckDB,
# Postgres once it writes there — and never `KS_READ_GOALS`, which is what this
# walk forbids: it does not reach `_goals_run`.
# `tests/unit/test_goals_reads_follow_chain.py` holds the other half.
NOT_ROUTED = ("generate_smart_goals",)
ROUTED_ONLY_VIA = {"generate_smart_goals": (
    "_get_ml_forecast_total", "_dynamic_growth_cap", "_last_year_month_revenue",
    "_recent_three_month_average")}
ABSENT_FROM_POSTGRES_READS = ("seasonal_indices", "weekly_patterns", "growth_metrics")

# The writes. These *may* reach the router — `set_goal` computes a suggestion
# from the revenue history before storing it, and reading that history from
# Postgres is exactly what this change is for. What they must not do is send a
# *write* through it, and must still open DuckDB for the write itself.
#
# The three calculators are not writers since chain 7b-1: they compute, and
# `_persist_goal_tables` stores what they computed, in one transaction.
WRITERS = ("set_goal", "_persist_goal_tables", "store_predictions")

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


# Every read that asks the flag. `get_daily_revenue_for_dates([])` is not here:
# it returns before asking any engine, so there is nothing for a typo to stop.
TYPO_CALLS = (
    ("get_goals", (), {}),
    ("get_smart_goals", (), {}),
    ("get_historical_revenue", ("weekly",), {}),
    ("get_daily_revenue_for_dates", ([date.today()],), {}),
    ("get_predictions", (date.today() - timedelta(days=7), date.today()), {}),
    # The smart goal's ML signal, and the method `/api/goals/forecast` returns
    # as it stands — the pair the DN-12 review found swallowing the refusal.
    ("_get_ml_forecast_total", (2026, 10), {}),
    ("generate_smart_goals", (2026, 10), {}),
)


class TestATypoStopsEveryRead:
    """`enabled()` raising is a contract only while no caller swallows it.

    `_get_ml_forecast_total` did: it read through `_goals_run` inside an
    `except Exception`, so the ValueError became a missing ML signal, logged at
    DEBUG, and `generate_smart_goals` answered with a different goal — the
    blend re-weighted over what was left, the method switched from
    `ml_forecast` to `yoy_growth` — and a 200."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,args,kwargs", TYPO_CALLS,
                             ids=[c[0] for c in TYPO_CALLS])
    async def test_it_raises(self, tmp_path, monkeypatch, name, args, kwargs):
        monkeypatch.setenv("KS_READ_GOALS", "postgress")
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"typo-{name}.duckdb")
        await store.connect()
        try:
            with pytest.raises(ValueError, match="KS_READ_GOALS"):
                await asyncio.wait_for(
                    getattr(store, name)(*args, **kwargs), TIMEOUT_S)
        finally:
            await store.close()


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
    def test_the_seasonality_tables_are_not_routed(self, name):
        assert not self._reaches_router(
            name, excluding=ROUTED_ONLY_VIA.get(name, ())), (
            f"{name} reaches the router outside its ML signal and its history "
            f"reads. It reads seasonal_indices / weekly_patterns / "
            f"growth_metrics, which DuckDB writes; Postgres holds only an "
            f"hourly replica of those three, so a routed read would answer up "
            f"to an hour behind POST /goals/recalculate."
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
            f"read replica and DuckDB is the writer by default, so a write "
            f"that went to Postgres outside a write chain — for `set_goal`, "
            f"chain 7a behind KS_WRITE_GOALS — would be overwritten within the "
            f"hour."
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

    def test_the_other_three_follow_their_writer_not_this_flag(self):
        """This used to assert the three had no dialect entry: nothing could
        read them through a routed body while `generate_smart_goals` wrote
        them before reading. Chain 7b-1 took that write out, and chain 7b-3
        moves their writer, so they have entries now — and the one body that
        carries them goes to `_goal_tables_run`, which asks the chain, never
        to `_goals_run`, which asks `KS_READ_GOALS` and would read the hourly
        replica while DuckDB is still the writer."""
        from core.sql_dialect import DUCKDB, POSTGRES

        for name in ("seasonal_indices", "weekly_patterns", "growth_metrics"):
            assert getattr(POSTGRES, name) == f"app.{name}"
            assert getattr(DUCKDB, name) == name
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        routers = {c.func.attr for c in ast.walk(tree)
                   if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                   and c.args and isinstance(c.args[0], ast.Name)
                   and c.args[0].id == "_SHARED_GOAL_TABLES_SQL"}
        assert routers == {"_goal_tables_run"}


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
