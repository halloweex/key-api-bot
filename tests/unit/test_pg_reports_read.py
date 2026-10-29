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
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"

# The three the flag moves, and the arguments that reach every branch.
from datetime import date, timedelta  # noqa: E402

WINDOW = (date.today() - timedelta(days=30), date.today())

CALLS = (
    ("get_report_summary", {}),
    ("get_report_summary", {"category_id": 1}),     # the product-filter branch
    ("get_report_summary", {"brand": "BrandA"}),
    ("get_report_top_products", {"limit": 5}),
    ("get_report_top_products", {"category_id": 1}),
    ("get_report_products_by_source", {}),
)


class TestEveryReportReadsPostgres:
    """On a store whose DuckDB connection is a trap, whatever the switch that
    used to choose says."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
    )
    async def test_each_sends_its_statements_to_postgres(
        self, tmp_path, monkeypatch, name, kwargs,
    ):
        monkeypatch.setenv("KS_READ_REPORTS", "duckdb")   # not read any more
        store = DuckDBStore(db_path=tmp_path / "never.duckdb")

        def boom(*_a, **_k):
            raise AssertionError("the report read reached for DuckDB")

        with patch.object(type(store), "connection", boom), \
             patch("core.pg_reports_read.fetch",
                   new=AsyncMock(return_value=[])) as reports, \
             patch("core.pg_lookups_read.fetch",
                   new=AsyncMock(return_value=[])) as lookups:
            await getattr(store, name)(*WINDOW, **kwargs)
        assert reports.await_count >= 1
        assert "silver." in reports.await_args.args[0]
        # The category tree is the filter bar's, and only a category asks it.
        assert lookups.await_count == (1 if "category_id" in kwargs else 0)


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

    def test_no_report_method_opens_the_store_itself(self):
        """A method left on `self.connection()` would read DuckDB beside the
        router."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in ("get_report_summary", "get_report_top_products",
                     "get_report_products_by_source"):
            node = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            body = ast.get_source_segment(source, node) or ""
            assert "self.connection()" not in body, f"{name} still opens DuckDB"

    def test_the_router_is_never_called_under_the_lock(self):
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)

        def opens_the_store(node):
            return any(
                isinstance(i.context_expr, ast.Call)
                and isinstance(i.context_expr.func, ast.Attribute)
                and i.context_expr.func.attr == "connection"
                for i in getattr(node, "items", [])
            )

        offenders = [
            inner.lineno
            for node in ast.walk(tree)
            if isinstance(node, (ast.With, ast.AsyncWith)) and opens_the_store(node)
            for inner in ast.walk(node)
            if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
            and inner.func.attr in ("_reports_run", "_lookups_run", "_category_ids")
        ]
        assert not offenders, (
            f"routing helper called inside `self.connection()` at {offenders} — "
            f"the store lock is not reentrant and the deadlock will not raise"
        )

    def test_the_rendering_leaves_no_hole(self):
        from core.sql_dialect import POSTGRES
        from core.repositories.revenue import RevenueMixin

        for lineno, sql in self._statements():
            rendered = RevenueMixin._render_report(sql, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, (
                f"line {lineno} still has a hole for {POSTGRES.name}"
            )

    def test_every_ordered_result_ends_on_a_key_that_cannot_tie(self):
        """Two engines break a tie differently, and two of these carry a LIMIT
        — where a tie means different *rows*, not a different order."""
        unique = {"s.source_id", "source_id", "sku", "product_name"}
        for lineno, sql in self._statements():
            upper = sql.upper()
            if "ORDER BY" not in upper:
                continue
            tail = re.split(r"LIMIT", sql[upper.index("ORDER BY") + 8:], flags=re.I)[0]
            tail = "\n".join(l.split("--")[0] for l in tail.splitlines())
            last = [c for c in tail.split(",") if c.strip()][-1].strip().split()[0]
            assert last in unique, f"line {lineno} can tie on {last!r}"


class TestTheCategoryTreeHasOneBody:
    def test_the_tree_is_one_text(self):
        from core.repositories import revenue

        source = REPOSITORY.read_text(encoding="utf-8")
        assert source.count("WITH RECURSIVE category_tree") == 1, (
            "the recursive category query has been copied"
        )
        assert "{categories}" in revenue._CATEGORY_TREE_SQL

    def test_the_tree_is_ordered(self):
        """The result becomes an `IN (...)` list that the differential test
        compares; unordered, the two engines would build the same filter from a
        different list and the comparison would report a difference that is
        not one."""
        from core.repositories import revenue

        assert "ORDER BY id" in revenue._CATEGORY_TREE_SQL


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
        store = DuckDBStore(db_path=tmp_path / "never.duckdb")
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
