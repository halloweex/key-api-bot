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

from core.data_quality import check_internal_integrity
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
