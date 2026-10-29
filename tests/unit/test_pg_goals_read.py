"""The `/goals` statements, and — mostly — what the router may not touch.

ONE ENGINE SINCE STEP 14 (S14-07). `_goals_run` and `_goal_tables_run` read
Postgres and refuse any failure (`tests/unit/test_tab_routers_postgres_only.py`);
`KS_READ_GOALS` is not read. Until then this file held that flag — its
default, a typo stopping every read — the DuckDB path, every routed method on
an empty DuckDB, and the boundary that kept the three shared goal tables off
`_goals_run` while Postgres held only an hourly replica of them (chain 7b-3
wrote DuckDB). Those went with the flag and the replica. What stays is about
the statements, and the paragraphs below are the history of the boundary.

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
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore


REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "goals.py"

ROUTED = ("get_goals", "get_smart_goals", "get_historical_revenue",
          "get_daily_revenue_for_dates", "get_predictions")

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

class TestTheRoutedBodies:
    def test_the_rendering_leaves_no_hole(self):
        from core.sql_dialect import POSTGRES, render_tables
        from core.repositories import goals

        for name in BODIES:
            sql = getattr(goals, name)
            sql = sql.replace("{where}", "TRUE").replace("{grain}", "s.order_date")
            rendered = render_tables(sql, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, name

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
