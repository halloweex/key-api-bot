"""A run in which a check could not look does not announce its recovery.

`_resolve_dq_layer` resolves every delivered page condition this run did not
find again. A check that raised — or said it is not watching — did not find its
conditions because it did not look, and "✅ Resolved" for a live
`silver_missing_rows` page after `silver_arc` crashed is a recovery nobody
verified. Those conditions are held as still firing; everything the run did
verify resolves as before. Found by the chain-2 review; the gap predates #208,
which only made the crash visible.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import pathlib
import textwrap
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import data_quality as dq
from core.data_quality import IntegrityIssue, Severity
from core.duckdb_store import DuckDBStore


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "blind.duckdb")
    await s.connect()
    yield s
    await s.close()


def _issue(name, severity=Severity.WARN):
    return IntegrityIssue(check_name=name, table_name="t", severity=severity, count=1)


class TestWhatTheScanCouldNotSee:
    @pytest.mark.asyncio
    async def test_a_raising_check_is_reported_by_its_guard_name(self, store):
        raised = []
        async with store.connection() as conn:
            conn.execute("DROP TABLE silver_order_utm")
            dq.check_internal_integrity(conn, raised_out=raised)
        assert raised == ["attribution_coverage"]

    def test_its_conditions_are_unverified(self):
        assert dq.unverified_conditions(["silver_arc"], []) == [
            "silver_missing_rows", "silver_orphan_rows", "silver_row_values"]

    def test_an_unwatched_inventory_calendar_leaves_the_gaps_unverified(self):
        assert dq.unverified_conditions([], [_issue("inventory_continuity_unwatched")]) == [
            "inventory_snapshot_gaps"]

    def test_unwatched_watermarks_leave_the_moved_freshness_unverified(self, monkeypatch):
        monkeypatch.setenv("KS_WRITE_INVENTORY", "postgres")
        monkeypatch.delenv("KS_WRITE_EXPENSES", raising=False)
        assert dq.unverified_conditions([], [_issue("sync_watermarks_unwatched")]) == [
            "freshness_offers", "freshness_stocks"]

    def test_a_clean_run_has_nothing_unverified(self):
        assert dq.unverified_conditions([], [_issue("headline_vs_line_items")]) == []


class TestTheResolution:
    def _still_firing(self, issues, unverified=()):
        from core.scheduler import BackgroundScheduler

        resolve = AsyncMock(return_value=0)
        with patch("core.alerting.resolve_group", resolve):
            asyncio.run(BackgroundScheduler()._resolve_dq_layer(
                "integrity", issues, None, unverified=unverified))
        return set(resolve.await_args.kwargs["still_firing"])

    def test_unverified_conditions_are_held_firing(self):
        still = self._still_firing([_issue("integrity_check_raised")],
                                   unverified=["silver_missing_rows"])
        assert still == {"silver_missing_rows"}

    def test_verified_criticals_still_hold_and_the_rest_resolve(self):
        still = self._still_firing([_issue("fk_orphan_order_products_order_id", Severity.CRITICAL)])
        assert still == {"fk_orphan_order_products_order_id"}


class TestTheMapCannotDrift:
    def test_every_guard_name_is_mapped_and_nothing_else(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(dq.check_internal_integrity)))
        guards = {n.args[0].value for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                  and n.func.id == "guarded" and n.args and isinstance(n.args[0], ast.Constant)}
        assert guards and guards == set(dq.GUARDED_CHECK_CONDITIONS)

    @pytest.mark.parametrize("guard", sorted(dq.GUARDED_CHECK_CONDITIONS))
    def test_each_tuple_is_what_the_check_emits(self, guard):
        """Derived from the check itself, minus its own `*_unwatched` finding,
        which is a blindness report rather than a condition it verifies."""
        tree = ast.parse(textwrap.dedent(inspect.getsource(dq.check_internal_integrity)))
        call = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                    and n.func.id == "guarded" and n.args[0].value == guard)
        check_name = next(n.func.id for n in ast.walk(call.args[1])
                          if isinstance(n, ast.Call) and isinstance(n.func, ast.Name))
        fn_tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(dq, check_name))))
        emitted = set()
        for node in ast.walk(fn_tree):
            if isinstance(node, ast.keyword) and node.arg == "check_name" \
                    and isinstance(node.value, ast.Constant):
                emitted.add(node.value.value)
        emitted = {e for e in emitted if not e.endswith("_unwatched")}
        assert emitted and emitted == set(dq.GUARDED_CHECK_CONDITIONS[guard])

    def test_the_job_hands_what_it_could_not_see_to_the_resolution(self):
        from core.scheduler import BackgroundScheduler

        tree = ast.parse(textwrap.dedent(inspect.getsource(BackgroundScheduler._run_dq_integrity)))
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        scan = [c for c in calls if isinstance(c.func, ast.Name) and c.func.id == "check_internal_integrity"]
        assert scan and all(any(k.arg == "raised_out" for k in c.keywords) for c in scan)
        resolve = [c for c in calls if isinstance(c.func, ast.Attribute) and c.func.attr == "_resolve_dq_layer"]
        assert resolve and all(any(k.arg == "unverified" for k in c.keywords) for c in resolve)
