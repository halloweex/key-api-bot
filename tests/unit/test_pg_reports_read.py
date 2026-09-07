"""Which engine answers `/reports`, and what the flag may not touch.

Three things need proving here, and none of them is about numbers — the numbers
are `tests/integration/test_reports_two_engines.py`'s job:

  * an unknown value stops the read instead of quietly serving the other store;
  * a Postgres read never touches DuckDB's lock (§34), tested by making the
    lock fatal, with the mirror test so it cannot pass by the routing being
    broken in both directions;
  * each of the three methods returns rather than hanging. The store lock is
    not reentrant and a nested acquisition does not raise — it simply never
    comes back, which is how the cohort port shipped a deadlock past a green
    suite. Two of these three had no test that called them at all before this
    file.

AND THE HELPER THAT MADE THE DEADLOCK LIKELY

`_get_category_with_children` takes a connection its caller already holds, and
the report methods used to call it from inside one. Routed, they hold nothing —
so they call `_category_ids`, the twin that goes through the router. Both run
`_CATEGORY_TREE_SQL`; six unported methods still use the older one, which is
why there are two executors and one body rather than two bodies.
"""
from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_reports_read
from core.duckdb_store import DuckDBStore

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"
TIMEOUT_S = 20

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


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_REPORTS", raising=False)
        assert pg_reports_read.enabled() is False

    def test_an_empty_value_is_the_default_too(self, monkeypatch):
        monkeypatch.setenv("KS_READ_REPORTS", "  ")
        assert pg_reports_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_REPORTS", "postgres")
        assert pg_reports_read.enabled() is True

    def test_a_typo_raises_rather_than_serving_the_other_store(self, monkeypatch):
        monkeypatch.setenv("KS_READ_REPORTS", "postgres ")   # trailing space is fine
        assert pg_reports_read.enabled() is True
        monkeypatch.setenv("KS_READ_REPORTS", "postgress")
        with pytest.raises(ValueError, match="KS_READ_REPORTS"):
            pg_reports_read.enabled()

    def test_without_a_dsn_there_is_nothing_to_ask(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert pg_reports_read.available() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        assert pg_reports_read.available() is True


class TestItDoesNotTakeTheDuckDbLock:
    """The invariant, tested by making the lock fatal."""

    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("KS_READ_REPORTS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "r.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError("the report read reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_reports_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_report_top_products(*WINDOW)
            assert fetch.await_count == 1
            sql = fetch.await_args.args[0]
            assert "silver.order_lines" in sql, sql[:120]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        """The mirror: without it the test above could pass by the routing
        being broken in both directions."""
        monkeypatch.delenv("KS_READ_REPORTS", raising=False)
        store = DuckDBStore(db_path=tmp_path / "r.duckdb")
        await store.connect()
        try:
            with patch("core.pg_reports_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_report_top_products(*WINDOW)
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        """Nothing diverges — the tab writes nothing and both engines read the
        same Silver — so a fault must cost the engine, not the page."""
        monkeypatch.setenv("KS_READ_REPORTS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "r.duckdb")
        await store.connect()
        try:
            with patch("core.pg_reports_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                rows = await store.get_report_top_products(*WINDOW)
            assert rows == []          # answered from an empty DuckDB, not raised
        finally:
            await store.close()


class TestEveryMethodReturns:
    """A nested acquisition of the store lock does not raise; it hangs. So the
    assertion is a timeout, and every routed method is walked."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
    )
    async def test_it_returns_rather_than_hanging(self, tmp_path, monkeypatch, name, kwargs):
        monkeypatch.delenv("KS_READ_REPORTS", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"r-{name}-{len(kwargs)}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(
                getattr(store, name)(*WINDOW, **kwargs), timeout=TIMEOUT_S,
            )
        finally:
            await store.close()


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
        """A method left on `self.connection()` would keep working on DuckDB
        and silently ignore the flag."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in ("get_report_summary", "get_report_top_products",
                     "get_report_products_by_source"):
            node = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            body = ast.get_source_segment(source, node) or ""
            assert "self.connection()" not in body, f"{name} still opens DuckDB"
            assert "_get_category_with_children" not in body, (
                f"{name} calls the connection-taking helper — that is the "
                f"deadlock, and it does not raise"
            )

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
            and inner.func.attr in ("_reports_run", "_category_ids")
        ]
        assert not offenders, (
            f"routing helper called inside `self.connection()` at {offenders} — "
            f"the store lock is not reentrant and the deadlock will not raise"
        )

    def test_both_renderings_leave_no_hole(self):
        from core.sql_dialect import DUCKDB, POSTGRES
        from core.repositories.revenue import RevenueMixin

        for lineno, sql in self._statements():
            for dialect in (DUCKDB, POSTGRES):
                rendered = RevenueMixin._render_report(sql, dialect)
                assert "{" not in rendered and "}" not in rendered, (
                    f"line {lineno} still has a hole for {dialect.name}"
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
    def test_both_helpers_run_the_same_text(self):
        from core.repositories import revenue

        source = REPOSITORY.read_text(encoding="utf-8")
        assert source.count("WITH RECURSIVE category_tree") == 1, (
            "the recursive category query has been copied — six methods take a "
            "connection and three go through the router, but the text is one"
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
    nothing, and `category_id IN ()` is a parser error on both engines. The
    guard `_at_least_the_root` is a resurrection: the older helper opened with
    `result = [category_id]` and never used it, which reads as dead code and
    was really a guard wired up wrong.

    Found by the routing tests above, whose fixture has no categories at all —
    which is the only reason a defect this old surfaced now.
    """

    @pytest.mark.asyncio
    async def test_an_absent_category_gives_an_empty_report_not_a_crash(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "cat.duckdb")
        await store.connect()
        try:
            assert await store._category_ids(99999) == [99999]
            summary = await store.get_report_summary(*WINDOW, category_id=99999)
            assert summary["totals"]["orders_count"] == 0
            assert await store.get_report_top_products(*WINDOW, category_id=99999) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_connection_taking_helper_is_guarded_too(self, tmp_path):
        """Six methods still call that one, and they had the same crash."""
        store = DuckDBStore(db_path=tmp_path / "cat2.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                assert await store._get_category_with_children(conn, 99999) == [99999]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_existing_category_is_unaffected(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "cat3.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                conn.execute("INSERT INTO categories (id, name, parent_id) VALUES "
                             "(1,'Care',NULL),(2,'Serums',1),(3,'Makeup',NULL)")
            assert await store._category_ids(1) == [1, 2]
            assert await store._category_ids(3) == [3]
        finally:
            await store.close()
