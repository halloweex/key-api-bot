"""An integrity check that raises is named in the run, not swallowed at DEBUG.

Chain 2, step 1. Seven checks read tables that may not exist on a fresh
database, and each caught everything at DEBUG — so a dropped or renamed
Silver/Gold/UTM table made its check disappear while the run persisted as a
clean success. Stage 4 moves and stands down tables; this is what makes a check
that lost its table visible.
"""
from __future__ import annotations

import ast
import inspect
import textwrap

import pytest
import pytest_asyncio

from core.data_quality import Severity, check_internal_integrity
from core.duckdb_store import DuckDBStore


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "integrity.duckdb")
    await s.connect()
    yield s
    await s.close()


def _raised(issues):
    return [i for i in issues if i.check_name == "integrity_check_raised"]


class TestARaisingCheckIsNamed:
    @pytest.mark.asyncio
    async def test_a_clean_database_raises_nothing(self, store):
        async with store.connection() as conn:
            assert _raised(check_internal_integrity(conn)) == []

    @pytest.mark.asyncio
    async def test_a_lost_table_is_reported_by_the_check_that_needed_it(self, store):
        async with store.connection() as conn:
            conn.execute("DROP TABLE silver_order_utm")
            issues = check_internal_integrity(conn)
        found = _raised(issues)
        assert len(found) == 1
        assert found[0].severity == Severity.WARN
        assert found[0].count == 1
        assert "attribution_coverage" in found[0].description

    @pytest.mark.asyncio
    async def test_every_raising_check_is_counted_and_the_rest_still_run(self, store):
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at,"
                " created_at, updated_at) VALUES (1, 99, 12, 100,"
                " '2026-09-01 10:00:00+03', '2026-09-01 10:00:00+03',"
                " '2026-09-01 10:00:00+03')")
            conn.execute("DROP TABLE silver_order_utm")
            conn.execute("DROP TABLE gold_daily_revenue")
            issues = check_internal_integrity(conn)
        found = _raised(issues)
        assert len(found) == 1 and found[0].count == 2
        assert "attribution_coverage" in found[0].description
        assert "gold_cell_values" in found[0].description
        # The checks that did not raise are still in the run.
        assert "value_domain_orders_source_id" in {i.check_name for i in issues}


class TestTheGuardHasOneHome:
    def test_the_scan_has_one_except_and_it_feeds_the_collector(self):
        """Walks the function, not a list of checks: the next check somebody
        wraps in its own try/except is the one a list would not name."""
        tree = ast.parse(textwrap.dedent(inspect.getsource(check_internal_integrity)))
        handlers = [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]
        assert len(handlers) == 1, "a second try/except in the scan swallows on its own"
        appends = [n for n in ast.walk(handlers[0])
                   if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                   and n.func.attr == "append"
                   and isinstance(n.func.value, ast.Name) and n.func.value.id == "raised"]
        assert appends, "the one handler does not record the check it caught"

    def test_nothing_in_the_scan_logs_at_debug(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(check_internal_integrity)))
        debug = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Attribute) and n.attr == "debug"]
        assert debug == []
