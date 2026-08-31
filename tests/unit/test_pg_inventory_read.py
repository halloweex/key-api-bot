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
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_inventory_read
from core.duckdb_store import DuckDBStore

TIMEOUT_S = 20

# (method, kwargs) — every consumer of the eleven views.
CALLS = (
    ("get_inventory_summary_v2", {}),
    ("get_dead_stock_items_v2", {"limit": 10}),
    ("get_dead_stock_deep", {"limit": 10}),
    ("get_brand_rotation", {"min_skus": 1}),
    ("get_recommended_actions", {"limit": 10}),
    ("get_restock_alerts", {"limit": 10}),
    ("get_inventory_turnover", {"days": 30}),
    ("get_abc_skus", {"abc_class": "A", "limit": 10}),
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


class TestEveryMethodReturns:
    """A nested `self.connection()` does not raise — it hangs. Five methods
    were covered by two tests in the cohort port and neither called any of
    them, which is how that deadlock shipped."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,kwargs", CALLS, ids=[c[0] for c in CALLS])
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
