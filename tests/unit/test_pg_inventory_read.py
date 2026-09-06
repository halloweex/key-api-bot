"""Which engine answers `/inventory`, and what the flag may not touch.

Three things need proving here and none of them is about numbers — the numbers
are `tests/integration/test_inventory_two_engines.py`'s job:

  * an unknown value stops the read instead of quietly serving the other store;
  * a Postgres read never touches DuckDB's lock (§34), tested by making the
    lock fatal, with the mirror test so it cannot pass by the routing being
    broken in both directions;
  * every one of the eight methods returns rather than hanging, because the
    store lock is not reentrant and a nested acquisition does not raise — it
    simply never comes back. That is how the cohort port shipped a deadlock
    past a green suite.
"""
from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_inventory_read
from tests.sql_helper import strip_comments_and_literals
from core.duckdb_store import DuckDBStore

TIMEOUT_S = 20

# (method, kwargs) — every consumer of the eleven views, plus the three that
# read `inventory_history` or `sku_inventory_status` directly. All ten of the
# tab's read routes: `/stocks/summary` joined the rest on 2026-09-06.
CALLS = (
    ("get_stock_summary", {"limit": 10}),
    ("get_inventory_summary_v2", {}),
    ("get_dead_stock_items_v2", {"limit": 10}),
    ("get_dead_stock_deep", {"limit": 10}),
    ("get_brand_rotation", {"min_skus": 1}),
    ("get_recommended_actions", {"limit": 10}),
    ("get_restock_alerts", {"limit": 10}),
    ("get_inventory_turnover", {"days": 30}),
    ("get_abc_skus", {"abc_class": "A", "limit": 10}),
    ("get_average_inventory", {"days": 30}),
    ("get_inventory_trend", {"days": 90, "granularity": "daily"}),
    ("get_inventory_trend", {"days": 90, "granularity": "monthly"}),
)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv(pg_inventory_read.ENV, raising=False)
        assert pg_inventory_read.enabled() is False

    def test_an_empty_value_is_the_default_too(self, monkeypatch):
        monkeypatch.setenv(pg_inventory_read.ENV, "   ")
        assert pg_inventory_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv(pg_inventory_read.ENV, "postgres")
        assert pg_inventory_read.enabled() is True

    def test_a_typo_raises_rather_than_serving_the_other_store(self, monkeypatch):
        # `KS_BOT_STORE`'s rule. A misspelling in the variable that decides
        # which engine answers must stop the read loudly.
        monkeypatch.setenv(pg_inventory_read.ENV, "postgre")
        with pytest.raises(ValueError, match="unknown engine"):
            pg_inventory_read.enabled()

    def test_without_a_dsn_there_is_nothing_to_ask(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert pg_inventory_read.available() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        assert pg_inventory_read.available() is True


class TestItDoesNotTakeTheDuckDbLock:
    """§34's invariant: the engine is chosen before any connection is taken,
    so a Postgres read never queues behind DuckDB's single writer."""

    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv(pg_inventory_read.ENV, "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://unused/unused")
        store = DuckDBStore(db_path=tmp_path / "locked.duckdb")
        await store.connect()

        def _explode(*_a, **_k):
            raise AssertionError("the Postgres read reached for DuckDB's connection")

        # One row of eleven columns — the shape v_restock_alerts' consumer
        # unpacks, which is enough to prove the Postgres path ran.
        rows = [[(1, "sku", "name", "brand", 0, 5, "CRITICAL")]]
        try:
            monkeypatch.setattr(type(store), "connection", _explode)
            with patch("core.pg_inventory_read.fetch_many",
                       new=AsyncMock(return_value=rows)) as fetch:
                result = await store.get_restock_alerts(limit=10)
            assert fetch.await_count == 1
            assert result == [{
                "offerId": 1, "sku": "sku", "name": "name", "brand": "brand",
                "unitsLeft": 0, "daysSinceSale": 5, "alertLevel": "CRITICAL",
            }]
        finally:
            monkeypatch.undo()
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        """The mirror, so the test above cannot pass by the routing being
        broken in both directions."""
        monkeypatch.delenv(pg_inventory_read.ENV, raising=False)
        store = DuckDBStore(db_path=tmp_path / "used.duckdb")
        await store.connect()
        try:
            with patch("core.pg_inventory_read.fetch_many", new=AsyncMock()) as fetch:
                result = await store.get_restock_alerts(limit=10)
            assert fetch.await_count == 0
            assert result == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        """Nothing diverges between these two — the tab writes nothing — so a
        fault costs the engine, not the tab. The opposite of `/sms`, where a
        silent fallback would answer from a store missing every opt-out
        recorded since the switch."""
        monkeypatch.setenv(pg_inventory_read.ENV, "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://unused/unused")
        store = DuckDBStore(db_path=tmp_path / "fallback.duckdb")
        await store.connect()
        try:
            with patch("core.pg_inventory_read.fetch_many",
                       new=AsyncMock(side_effect=RuntimeError("pool is gone"))):
                result = await store.get_restock_alerts(limit=10)
            assert result == []
        finally:
            await store.close()


class TestNoResultOrderIsLeftToThePlanner:
    """Every routed query that produces an ordered result ends its ORDER BY on
    a key that cannot tie.

    Two engines break a tie differently, and where a LIMIT falls inside the
    tied group they return *different rows*, not merely a different order. The
    synthetic fixture cannot be relied on to produce every tie: comparing the
    two engines over the real 891-SKU catalogue found one that thirteen SKUs
    had missed, in `get_dead_stock_items_v2`. This is the check that does not
    depend on the data.
    """

    # Columns that are unique within their result, so nothing can tie on them.
    UNIQUE_KEYS = frozenset({
        "offer_id", "date", "bucket", "period", "category_id",
        "1",            # a positional key: the GROUP BY column itself
    })

    @staticmethod
    def _trailing_order_by(sql: str) -> str | None:
        """The result's own ORDER BY, or None.

        Window frames (`ROW_NUMBER() OVER (ORDER BY …)`) and ordered-set
        aggregates (`WITHIN GROUP (ORDER BY …)`) also spell `ORDER BY`, and
        neither orders the result. Both live inside parentheses, so the one
        that matters is the one at depth zero.
        """
        body = strip_comments_and_literals(sql)
        depth = 0
        for i, char in enumerate(body):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and body[i:i + 8].upper() == "ORDER BY":
                tail = body[i + 8:]
                tail = re.split(r"LIMIT", tail, flags=re.I)[0]
                return " ".join(tail.split())
        return None

    def _routed_queries(self):
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("_inventory_rows", "_inventory_batch")):
                continue
            for literal in ast.walk(node):
                if (isinstance(literal, ast.Constant)
                        and isinstance(literal.value, str)
                        and "SELECT" in literal.value.upper()):
                    yield literal.lineno, literal.value

    def test_the_routed_queries_are_found_at_all(self):
        # A guard on the guard: an AST walk that matched nothing would let
        # every assertion below pass in silence.
        assert len(list(self._routed_queries())) >= 15

    def test_every_ordered_result_ends_on_a_key_that_cannot_tie(self):
        offenders = []
        for lineno, sql in self._routed_queries():
            order = self._trailing_order_by(sql)
            if order is None:
                continue
            last = order.split(",")[-1].strip().split()[0]
            if last not in self.UNIQUE_KEYS:
                offenders.append((lineno, order))
        assert not offenders, (
            f"ORDER BY can tie, so the two engines may disagree about which "
            f"rows a LIMIT keeps: {offenders}"
        )

    def test_an_unordered_result_is_never_read_as_a_list(self):
        """The queries with no ORDER BY at all are the ones whose caller keys
        the rows rather than rendering them in order — `v_inventory_summary`
        and `v_abc_summary` become dicts. Anything else without one would be a
        list in planner order."""
        unordered = [
            " ".join(sql.split())
            for _lineno, sql in self._routed_queries()
            if self._trailing_order_by(sql) is None
        ]
        assert unordered, "no unordered queries found — the scan is broken"
        for sql in unordered:
            assert (
                "v_inventory_summary" in sql
                or "v_abc_summary" in sql
                or "COUNT(*)" in sql.upper()      # single-row aggregates
                or "COALESCE(SUM" in sql.upper()
            ), f"unordered query that is read as a list: {sql[:120]}"


class TestEveryMethodReturns:
    """A nested `self.connection()` does not raise — it hangs. Five methods
    were covered by two tests in the cohort port and neither called any of
    them, which is how that deadlock shipped."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,kwargs", CALLS, ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS])
    async def test_it_returns_rather_than_hanging(
        self, tmp_path, monkeypatch, name, kwargs,
    ):
        monkeypatch.delenv(pg_inventory_read.ENV, raising=False)
        store = DuckDBStore(db_path=tmp_path / f"{name}.duckdb")
        await store.connect()
        try:
            try:
                await asyncio.wait_for(getattr(store, name)(**kwargs), TIMEOUT_S)
            except asyncio.TimeoutError:
                pytest.fail(
                    f"{name} did not return within {TIMEOUT_S}s — the store "
                    f"lock is not reentrant, so this is almost certainly a "
                    f"routing helper called from inside `self.connection()`"
                )
        finally:
            await store.close()


class TestTheHelperIsNeverCalledUnderTheLock:
    """The structural half of the test above: the timeout catches a deadlock
    on a path a test walks, this catches the shape anywhere."""

    def test_no_routing_call_sits_inside_a_connection_block(self):
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        def opens_the_store(node: ast.AST) -> bool:
            for item in getattr(node, "items", []):
                call = item.context_expr
                if (isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "connection"):
                    return True
            return False

        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.With, ast.AsyncWith)) or not opens_the_store(node):
                continue
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr in ("_inventory_rows", "_inventory_batch")):
                    offenders.append(inner.lineno)
        assert not offenders, (
            f"routing helper called inside `self.connection()` at line(s) "
            f"{offenders} — the store lock is not reentrant and the deadlock "
            f"will not raise"
        )

    def test_every_view_reader_goes_through_the_helper(self):
        """A method left on `conn.execute` against a view would keep working
        on DuckDB and silently ignore the flag — the half-switched state
        `refuse_while_unported` exists to prevent on the `/sms` side."""
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        stragglers = []
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("execute", "executemany")):
                continue
            for arg in node.args[:1]:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    if "v_" in arg.value and "FROM" in arg.value.upper():
                        stragglers.append((node.lineno, arg.value.strip()[:60]))
        assert not stragglers, f"view read outside the routing helper: {stragglers}"

    def test_no_reader_names_an_unrouted_table(self):
        """The boundary this test used to guard is gone, and this is what
        replaced it.

        Until 2026-09-06 `/api/stocks/summary` was the one read route left on
        DuckDB, and the recorded reason was that it joined `offers` and
        `sync_metadata`, neither of which is in Postgres. Both halves turned
        out to be avoidable rather than blocking — the join to `offers` existed
        only to fetch `p.name`, which `sku_inventory_status` already carries
        from the identical joins, and the `sync_metadata` row was a key nothing
        in the repository has ever written, so `lastSync` had been NULL since
        the feature shipped. The port needed no replication at all.

        So the check is now the general one: no read method may name a table
        that exists only in DuckDB. A writer may — it writes there.
        """
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        # Tables DuckDB has and Postgres does not. `offers` and `sync_metadata`
        # are landing/control state nobody replicated; adding one here is how
        # the next port learns it has a dependency to resolve first.
        DUCKDB_ONLY = ("FROM offers", "JOIN offers", "FROM sync_metadata")

        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            if not node.name.startswith("get_"):
                continue          # writers and refreshers keep their own store
            body = ast.get_source_segment(source, node) or ""
            for name in DUCKDB_ONLY:
                if name in body:
                    offenders.append((node.name, name))
        assert not offenders, (
            f"read method naming a table Postgres does not have: {offenders} — "
            f"the flag cannot switch it, and it will answer from DuckDB while "
            f"the rest of the tab answers from Postgres"
        )

    def test_the_summary_reads_history_through_the_helper(self):
        """`inventory_history` has two readers and only one may hold a
        connection: the writer that records the snapshot."""
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        readers = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            body = ast.get_source_segment(source, node) or ""
            if "{inventory_history}" in body or "FROM inventory_history" in body:
                readers.add(node.name)
        assert "get_stock_summary" in readers
        assert "get_average_inventory" in readers
        # The writer is the only one allowed to name it unrendered.
        unrendered = {
            n.name for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef)
            and "FROM inventory_history" in (ast.get_source_segment(source, n) or "")
        }
        assert unrendered == {"record_inventory_snapshot"}, unrendered
