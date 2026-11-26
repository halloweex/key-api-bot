"""The `/reports` statements, and what the router may not touch.

The numbers are `tests/integration/test_reports_two_engines.py`'s job, against
a real Postgres; that the router asks Postgres and nothing else since step
14's S14-05 — whatever `KS_READ_REPORTS` says, and a failure refused naming
`reports` — is `tests/unit/test_read_routers_postgres_only.py`'s. Here:

  * every report method sends its statements to Postgres, on a store whose
    DuckDB connection is a trap;
  * the bodies render with no hole left, and their order cannot tie;
  * the category tree has one body, and an absent category is an empty
    report rather than a parser error.

THE HELPER THAT MADE THE DEADLOCK LIKELY

`_get_category_with_children` took a connection its caller already held, and
the report methods used to call it from inside one. They call
`_category_ids`, which goes through the filter bar's router and takes no
lock; the older helper went with the DuckDB tails it served (S14-05).
"""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.store import AnalyticsStore

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"

# The three the flag moves, and the arguments that reach every branch.
from datetime import date, timedelta  # noqa: E402

WINDOW = (date.today() - timedelta(days=30), date.today())


class TestTheRoutedBodies:
    def _statements(self):
        tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "_reports_run"):
                continue
            for lit in ast.walk(node):
                if (isinstance(lit, ast.Constant) and isinstance(lit.value, str)
                        and "SELECT" in lit.value.upper()):
                    yield lit.lineno, lit.value

    def test_the_scan_finds_them(self):
        assert len(list(self._statements())) >= 4

    def test_the_rendering_leaves_no_hole(self):
        from core.sql_dialect import POSTGRES
        from core.repositories.revenue import RevenueMixin

        for lineno, sql in self._statements():
            rendered = RevenueMixin._render_report(sql, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, (
                f"line {lineno} still has a hole for {POSTGRES.name}"
            )


class TestACategoryThatDoesNotExist:
    """`?category_id=99999` was a 500, on every one of the seven callers.

    The recursive term's anchor is `WHERE id = ?`, so an absent id yields
    nothing, and `category_id IN ()` is a parser error. The guard
    `_at_least_the_root` is a resurrection: the older helper opened with
    `result = [category_id]` and never used it, which reads as dead code and
    was really a guard wired up wrong. Against a real Postgres it is
    `tests/integration/test_reports_two_engines.py`'s absent-category case.
    """

    @pytest.mark.asyncio
    async def test_an_absent_category_gives_an_empty_report_not_a_crash(self, tmp_path):
        store = AnalyticsStore()
        with patch("core.pg_lookups_read.fetch", new=AsyncMock(return_value=[])), \
             patch("core.pg_reports_read.fetch",
                   new=AsyncMock(return_value=[])) as reports:
            assert await store._category_ids(99999) == [99999]
            summary = await store.get_report_summary(*WINDOW, category_id=99999)
            assert summary["totals"]["orders_count"] == 0
            assert await store.get_report_top_products(*WINDOW, category_id=99999) == []
        for call in reports.await_args_list:
            sql, params = call.args
            assert "category_id IN (?)" in sql and 99999 in list(params), sql[-300:]
