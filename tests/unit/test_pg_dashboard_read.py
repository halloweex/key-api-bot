"""Which engine answers the rest of `/dashboard`, and what the flag may not touch.

The numbers are `tests/integration/test_dashboard_two_engines.py`'s job. Here:

  * an unknown value stops the read instead of quietly serving the other store;
  * a Postgres read never touches DuckDB's lock (§34), with the mirror test so
    it cannot pass by the routing being broken in both directions;
  * a Postgres fault still answers;
  * each routed method returns rather than hanging — the store lock is not
    reentrant and a nested acquisition does not raise, it never comes back;
  * **the doughnut's filtered branch does not call the connection-taking
    category helper.** That is the specific deadlock `/reports` shipped: the
    method used to hold a connection and pass it to
    `_get_category_with_children`; routed, it holds nothing, so it must use
    `_category_ids`, the twin that goes through the router.

AND THE HOLE THAT IS NOT A NAME

`{channel_measures}` renders the one place the two Golds differ in *shape* —
DuckDB spells a channel as a column, Postgres as a dimension. The order of the
six items is the contract, because the caller unpacks positionally, so it is
asserted here rather than left to the differential test to discover as a
number that is merely wrong.
"""
from __future__ import annotations

import ast
import asyncio
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_dashboard_read
from core.duckdb_store import DuckDBStore

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"
TIMEOUT_S = 20

TODAY = date.today()
WINDOW = (TODAY - timedelta(days=30), TODAY)

METHODS = ("get_return_orders", "get_sales_by_source", "get_subcategory_breakdown")

CALLS = (
    ("get_return_orders", {}),
    ("get_return_orders", {"sales_type": "all"}),
    ("get_sales_by_source", {}),                       # the Gold branch
    ("get_sales_by_source", {"brand": "BrandA"}),      # the line-level branch
    ("get_sales_by_source", {"category_id": 1}),       # …and the category walk
    ("get_subcategory_breakdown", {"parent_category_name": "Care"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "brand": "BrandA"}),
)

BODIES = (
    "_RETURN_ORDERS_SQL",
    "_SALES_BY_SOURCE_FILTERED_SQL",
    "_SALES_BY_SOURCE_GOLD_SQL",
    "_SUBCATEGORY_SQL",
)

# The holes each body fills in itself before the dialect renderer sees it.
CALLER_HOLES = ("{where}", "{brand_filter}", "{promocode_filter}")


def _call(store, name, kwargs):
    if name == "get_subcategory_breakdown":
        return getattr(store, name)(*WINDOW, **kwargs)
    return getattr(store, name)(*WINDOW, **kwargs)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_DASHBOARD", raising=False)
        assert pg_dashboard_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_DASHBOARD", "postgres")
        assert pg_dashboard_read.enabled() is True

    def test_a_typo_raises_rather_than_serving_the_other_store(self, monkeypatch):
        monkeypatch.setenv("KS_READ_DASHBOARD", "postgress")
        with pytest.raises(ValueError, match="KS_READ_DASHBOARD"):
            pg_dashboard_read.enabled()

    def test_it_is_not_the_gold_flag(self, monkeypatch):
        """`KS_READ_GOLD` is already `postgres` in production. If these reads
        rode on it they would have switched at deploy rather than at a
        decision, which is the whole reason for a flag per surface."""
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.delenv("KS_READ_DASHBOARD", raising=False)
        assert pg_dashboard_read.enabled() is False

    def test_without_a_dsn_there_is_nothing_to_ask(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert pg_dashboard_read.available() is False


class TestItDoesNotTakeTheDuckDbLock:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
    )
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch, name, kwargs,
    ):
        monkeypatch.setenv("KS_READ_DASHBOARD", "postgres")
        # The category walk needs `KS_READ_LOOKUPS` too, and that is the
        # point rather than a nuisance: the tree is filter chrome, so it
        # belongs to the filter bar's flag and not to whichever tab happens
        # to be asking. It rode `KS_READ_REPORTS` until this test caught it
        # reaching for DuckDB with this tab's flag on.
        monkeypatch.setenv("KS_READ_LOOKUPS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / f"d-{name}-{len(kwargs)}.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError(f"{name} reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_lookups_read.fetch",
                       new=AsyncMock(return_value=[])), \
                 patch("core.pg_dashboard_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await _call(store, name, kwargs)
            assert fetch.await_count >= 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KS_READ_DASHBOARD", raising=False)
        store = DuckDBStore(db_path=tmp_path / "d-mirror.duckdb")
        await store.connect()
        try:
            with patch("core.pg_dashboard_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_return_orders(*WINDOW)
                await store.get_sales_by_source(*WINDOW)
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_DASHBOARD", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "d-fallback.duckdb")
        await store.connect()
        try:
            with patch("core.pg_dashboard_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                assert await store.get_return_orders(*WINDOW) == []
                doughnut = await store.get_sales_by_source(*WINDOW)
            assert doughnut["labels"] == []      # answered from an empty DuckDB
        finally:
            await store.close()


class TestEveryMethodReturns:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
    )
    async def test_it_returns_rather_than_hanging(self, tmp_path, monkeypatch, name, kwargs):
        monkeypatch.delenv("KS_READ_DASHBOARD", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"d-ret-{name}-{len(kwargs)}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(_call(store, name, kwargs), timeout=TIMEOUT_S)
        finally:
            await store.close()


class TestTheRoutedBodies:
    def test_both_renderings_leave_no_hole(self):
        from core.sql_dialect import DUCKDB, POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            for hole in CALLER_HOLES:
                sql = sql.replace(hole, "TRUE" if hole == "{where}" else "")
            for dialect in (DUCKDB, POSTGRES):
                rendered = RevenueMixin._render_report(sql, dialect)
                assert "{" not in rendered and "}" not in rendered, (
                    f"{name} still has a hole for {dialect.name}: {rendered}"
                )

    def test_the_two_renderings_differ(self):
        from core.sql_dialect import DUCKDB, POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            for hole in CALLER_HOLES:
                sql = sql.replace(hole, "TRUE" if hole == "{where}" else "")
            assert (RevenueMixin._render_report(sql, DUCKDB)
                    != RevenueMixin._render_report(sql, POSTGRES)), (
                f"{name} renders identically for both engines — it names a "
                f"table rather than a hole"
            )

    def test_no_dashboard_method_opens_the_store_itself(self):
        """Walked as calls in the tree, never as a substring of the source:
        the comment beside the fix names the helper it replaced, and a grep
        finds the comment. That mistake has been made three times here."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in METHODS:
            node = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            calls = {c.func.attr for c in ast.walk(node)
                     if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)}
            assert "connection" not in calls, f"{name} still opens DuckDB"
            assert "_get_category_with_children" not in calls, (
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
            and inner.func.attr in ("_dashboard_run", "_category_ids")
        ]
        assert not offenders, (
            f"routing helper called inside `self.connection()` at {offenders}"
        )

    def test_every_ordered_body_ends_on_a_key_that_cannot_tie(self):
        """Two engines break a tie differently. `_RETURN_ORDERS_SQL` carries a
        LIMIT, where a tie means different *rows* and not a different order."""
        from core.repositories import revenue

        unique = {"l.source_id", "l.category_name", "s.id"}
        for name in BODIES:
            sql = getattr(revenue, name)
            upper = sql.upper()
            if "ORDER BY" not in upper:
                continue
            tail = re.split(r"LIMIT", sql[upper.index("ORDER BY") + 8:], flags=re.I)[0]
            tail = "\n".join(l.split("--")[0] for l in tail.splitlines())
            last = [c for c in tail.split(",") if c.strip()][-1].strip().split()[0]
            assert last in unique, f"{name} can tie on {last!r}"


class TestTheChannelShapeHasOneHome:
    """`{channel_measures}` is the only hole here that is not a table name."""

    def test_the_six_items_are_orders_then_revenue_per_channel(self):
        """The caller unpacks positionally: ig_orders, ig_rev, tg_orders,
        tg_rev, sh_orders, sh_rev. Getting the pairs the wrong way round
        produces numbers that are merely wrong, on a chart nobody would
        immediately disbelieve."""
        from core.sql_dialect import DUCKDB, POSTGRES, channel_measures

        duck = [i.strip() for i in channel_measures(DUCKDB).split(",\n")]
        assert len(duck) == 6
        for i, name in enumerate(("instagram", "telegram", "shopify")):
            assert f"{name}_orders" in duck[i * 2], duck[i * 2]
            assert f"{name}_revenue" in duck[i * 2 + 1], duck[i * 2 + 1]

        pg = [i.strip() for i in channel_measures(POSTGRES).split(",\n")]
        assert len(pg) == 6
        for i, sid in enumerate((1, 2, 4)):
            assert "orders_count" in pg[i * 2] and f"source_id = {sid}" in pg[i * 2]
            assert "SUM(revenue)" in pg[i * 2 + 1] and f"source_id = {sid}" in pg[i * 2 + 1]

    def test_the_postgres_rendering_never_reads_the_rollup_row(self):
        """Postgres' Gold holds both grains. A channel measure that did not
        filter on `source_id` would add the roll-up to the fine rows and
        double every channel."""
        from core.sql_dialect import POSTGRES, channel_measures

        for item in channel_measures(POSTGRES).split(",\n"):
            assert "FILTER (WHERE source_id =" in item, item

    def test_the_marketing_measures_render_the_channels_from_the_same_place(self):
        """Two callers, one home. A fourth channel column must be added once."""
        from core.sql_dialect import DUCKDB, POSTGRES, marketing_period_measures
        from core.sql_dialect import _channel_items

        for dialect in (DUCKDB, POSTGRES):
            eleven = marketing_period_measures(dialect)
            for item in _channel_items(dialect):
                assert item in eleven, (
                    f"{dialect.name}: the eleven-item rendering no longer uses "
                    f"the shared channel items"
                )
