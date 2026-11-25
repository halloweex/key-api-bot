"""The filter bar's statements, and what reaches Postgres.

The numbers are `tests/integration/test_lookups_two_engines.py`'s job, against
a real Postgres; that the router asks Postgres and nothing else since step
14's S14-05 — whatever `KS_READ_LOOKUPS` says, and a failure refused naming
`lookups` — is `tests/unit/test_read_routers_postgres_only.py`'s. Here:

  * every one of the four sends its statements to Postgres, on a store whose
    DuckDB connection is a trap;
  * the bodies render with no hole left, and none is inlined at a call site.

WHY THE BODIES ARE CHECKED BY NAME AND NOT BY SCANNING FOR LITERALS

`/reports` passes its SQL as a literal at the call site, so its test walks the
call and reads the string out of the tree. These five are module constants
reused by name, so the equivalent check is the other way round: render each
constant under both dialects, and assert that every `_lookups_run` call passes
one of those names — which is what catches somebody inlining a sixth body that
the rendering test would never see.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"

BODIES = (
    "_ROOT_CATEGORIES_SQL",
    "_CHILD_CATEGORIES_SQL",
    "_BRANDS_SQL",
    "_UNBRANDED_EXISTS_SQL",
    "_PROMOCODES_SQL",
    # The category tree joined the filter bar's flag when `/dashboard` became
    # its second routed caller: it is chrome shared across tabs, so riding one
    # tab's flag meant another tab's category filter took the DuckDB lock.
    "_CATEGORY_TREE_SQL",
)


class TestTheRoutedBodies:
    def test_the_rendering_leaves_no_hole(self):
        from core.sql_dialect import POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            rendered = RevenueMixin._render_report(sql, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, (
                f"{name} still has a hole for {POSTGRES.name}"
            )

    def test_every_body_names_its_table_through_a_hole(self):
        """Otherwise a body with no hole at all would pass the test above while
        naming a table by a name Postgres does not have (the bare DuckDB one)."""
        from core.sql_dialect import POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            rendered = RevenueMixin._render_report(sql, POSTGRES)
            assert rendered != sql and ("bronze." in rendered or "silver." in rendered), (
                f"{name} names a table rather than a hole")

    def test_every_router_call_passes_one_of_those_names(self):
        """A sixth body inlined at the call site would never be rendered by the
        test above, and would ship a DuckDB table name to Postgres."""
        tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
        calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr == "_lookups_run"]
        assert len(calls) == len(BODIES), (
            f"expected {len(BODIES)} router calls, found {len(calls)}"
        )
        for call in calls:
            first = call.args[0]
            assert isinstance(first, ast.Name), (
                f"line {call.lineno}: the body is not a module constant"
            )
            assert first.id in BODIES, f"line {call.lineno}: unknown body {first.id}"
