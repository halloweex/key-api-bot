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


from core import data_quality
from core.data_quality import check_internal_integrity


def _raised(issues):
    return [i for i in issues if i.check_name == "integrity_check_raised"]


class TestARaisingCheckIsNamed:
    def test_clean_facts_raise_nothing(self):
        """Since step 14's S14-14 the scan reads no store: it judges the
        facts it is handed. (Until then: a clean, empty DuckDB.)"""
        assert _raised(check_internal_integrity(
            inventory_calendar=(None, frozenset()), chain_watermarks={},
            chain_stamps={}, frozen_sync_rows={})) == []

    def test_a_check_that_raises_is_named_and_its_conditions_held(self, monkeypatch):
        def boom(*_a, **_k):
            raise KeyError("inventory_sku_history")

        monkeypatch.setattr(data_quality, "_inventory_snapshot_continuity_check", boom)
        raised: list = []
        issues = check_internal_integrity(
            inventory_calendar=(None, frozenset()), chain_watermarks={},
            chain_stamps={}, frozen_sync_rows={}, raised_out=raised)
        (finding,) = _raised(issues)
        assert "inventory_snapshot_continuity (KeyError)" in finding.description
        assert raised == ["inventory_snapshot_continuity"]
        assert data_quality.unverified_conditions(raised, issues) == ["inventory_snapshot_gaps"]


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
