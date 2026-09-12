"""Which engine answers the filter bar, and what the flag may not touch.

The numbers are `tests/integration/test_lookups_two_engines.py`'s job. Here:

  * an unknown value stops the read instead of quietly serving the other store;
  * a Postgres read never touches DuckDB's lock (§34), tested by making the
    lock fatal, with the mirror test so it cannot pass by the routing being
    broken in both directions;
  * a Postgres fault still answers — these four are drawn on every page, so a
    fault that took them down would take down nine tabs, not one;
  * each of the four returns rather than hanging. The store lock is not
    reentrant and a nested acquisition does not raise — it simply never comes
    back, which is how the cohort port shipped a deadlock past a green suite.

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
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_lookups_read
from core.duckdb_store import DuckDBStore

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"
TIMEOUT_S = 20

METHODS = ("get_categories", "get_child_categories", "get_brands", "get_promocodes")

CALLS = (
    ("get_categories", ()),
    ("get_child_categories", (1,)),
    ("get_brands", ()),          # two statements through the router, not one
    ("get_promocodes", ()),
)

BODIES = (
    "_ROOT_CATEGORIES_SQL",
    "_CHILD_CATEGORIES_SQL",
    "_BRANDS_SQL",
    "_UNBRANDED_EXISTS_SQL",
    "_PROMOCODES_SQL",
)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
        assert pg_lookups_read.enabled() is False

    def test_an_empty_value_is_the_default_too(self, monkeypatch):
        monkeypatch.setenv("KS_READ_LOOKUPS", "  ")
        assert pg_lookups_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_LOOKUPS", "postgres")
        assert pg_lookups_read.enabled() is True

    def test_a_typo_raises_rather_than_serving_the_other_store(self, monkeypatch):
        monkeypatch.setenv("KS_READ_LOOKUPS", "postgres ")   # trailing space is fine
        assert pg_lookups_read.enabled() is True
        monkeypatch.setenv("KS_READ_LOOKUPS", "postgress")
        with pytest.raises(ValueError, match="KS_READ_LOOKUPS"):
            pg_lookups_read.enabled()

    def test_without_a_dsn_there_is_nothing_to_ask(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert pg_lookups_read.available() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        assert pg_lookups_read.available() is True


class TestItDoesNotTakeTheDuckDbLock:
    """The invariant, tested by making the lock fatal."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,args", CALLS, ids=[c[0] for c in CALLS])
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch, name, args,
    ):
        monkeypatch.setenv("KS_READ_LOOKUPS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / f"l-{name}.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError(f"{name} reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_lookups_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await getattr(store, name)(*args)
            assert fetch.await_count >= 1
            sql = fetch.await_args.args[0]
            assert "bronze." in sql or "silver." in sql, sql[:160]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        """The mirror: without it the test above could pass by the routing
        being broken in both directions."""
        monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
        store = DuckDBStore(db_path=tmp_path / "l-mirror.duckdb")
        await store.connect()
        try:
            with patch("core.pg_lookups_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_categories()
                await store.get_brands()
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        """A dark dropdown is not one tab going down, it is the filter bar on
        all nine — so a fault must cost the engine, not the page."""
        monkeypatch.setenv("KS_READ_LOOKUPS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "l-fallback.duckdb")
        await store.connect()
        try:
            with patch("core.pg_lookups_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                assert await store.get_categories() == []
                assert await store.get_brands() == []
                assert await store.get_promocodes() == []
        finally:
            await store.close()


class TestEveryMethodReturns:
    """A nested acquisition of the store lock does not raise; it hangs. So the
    assertion is a timeout, and every routed method is walked."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,args", CALLS, ids=[c[0] for c in CALLS])
    async def test_it_returns_rather_than_hanging(self, tmp_path, monkeypatch, name, args):
        monkeypatch.delenv("KS_READ_LOOKUPS", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"l-ret-{name}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(
                getattr(store, name)(*args), timeout=TIMEOUT_S,
            )
        finally:
            await store.close()


class TestTheRoutedBodies:
    def test_both_renderings_leave_no_hole(self):
        from core.sql_dialect import DUCKDB, POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            for dialect in (DUCKDB, POSTGRES):
                rendered = RevenueMixin._render_report(sql, dialect)
                assert "{" not in rendered and "}" not in rendered, (
                    f"{name} still has a hole for {dialect.name}"
                )

    def test_the_two_renderings_differ(self):
        """Otherwise a body with no hole at all would pass the test above while
        naming a DuckDB table Postgres does not have."""
        from core.sql_dialect import DUCKDB, POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            assert (RevenueMixin._render_report(sql, DUCKDB)
                    != RevenueMixin._render_report(sql, POSTGRES)), (
                f"{name} renders identically for both engines — it names a "
                f"table rather than a hole"
            )

    def test_every_router_call_passes_one_of_those_names(self):
        """A sixth body inlined at the call site would never be rendered by the
        test above, and would ship a DuckDB table name to Postgres."""
        tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
        calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr == "_lookups_run"]
        assert len(calls) == 5, f"expected five router calls, found {len(calls)}"
        for call in calls:
            first = call.args[0]
            assert isinstance(first, ast.Name), (
                f"line {call.lineno}: the body is not a module constant"
            )
            assert first.id in BODIES, f"line {call.lineno}: unknown body {first.id}"

    def test_no_lookup_method_opens_the_store_itself(self):
        """A method left on `self.connection()` would keep working on DuckDB
        and silently ignore the flag."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in METHODS:
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
            and inner.func.attr == "_lookups_run"
        ]
        assert not offenders, (
            f"`_lookups_run` called inside `self.connection()` at {offenders} — "
            f"the store lock is not reentrant and the deadlock will not raise"
        )
