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
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "goals.py"

BODIES = ("_GOALS_PERIOD_REVENUE_SQL", "_GOALS_WEEKLY_TREND_SQL",
          "_GOALS_DAILY_FOR_DATES_SQL", "_STORED_GOALS_SQL",
          "_PREDICTIONS_SQL")


class TestTheBoundary:
    """What must *not* be routed, and why — the load-bearing part here."""

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
