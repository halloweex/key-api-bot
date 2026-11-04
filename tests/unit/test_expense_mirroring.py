"""The expense mirror and its backfill (the daily comparison that watched them,
`reconcile_expenses`, went in step 14's S14-12).

`tests/unit/test_pg_expenses_read.py` covers which engine answers and the
shared parse. This covers the machinery that keeps the two stores equal: that
the sync ships what it writes, that history can be carried across, and that
the daily comparison actually catches the ways this can go wrong.
"""
from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


REPO = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)


class TestTheSyncShipsWhatItWrites:
    """Every path that writes these tables in DuckDB must mirror them, or
    Postgres silently falls behind and only the daily comparison notices."""

    SYNC = REPO / "core" / "sync_service.py"

    def _pairs(self):
        src = self.SYNC.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            calls = [
                node.func.attr for node in ast.walk(fn)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            ] + [
                node.func.id for node in ast.walk(fn)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            ]
            yield fn.name, calls

    # `test_every_expense_upsert_is_mirrored` stood here: every sync path that
    # wrote expenses to DuckDB (`upsert_expenses_batch`) had to ship them.
    # Step 14's S14-09 deleted that writer — chain 3 writes the expenses with
    # their orders, in Postgres — so there is nothing left for it to hold.

    def test_the_type_dictionary_is_not_mirrored_from_the_payload(self):
        """It rides `replicate_operational` instead, and the difference is not
        academic: KeyCRM serves this dictionary only to the **weekly** full
        sync, so a payload-fed mirror left Postgres empty for up to a week —
        measured on production the hour the flag first went on, where it
        collapsed every expense type into "Other" and emptied the filter.

        `bronze.offer_stocks` sits in that family for the same reason. Two
        writers for one table is how they drift, so the mirror is gone rather
        than kept beside the replica."""
        for name, calls in self._pairs():
            assert "mirror_expense_types" not in calls, (
                f"{name} mirrors a dictionary that only the weekly sync carries"
            )

    def test_it_is_in_the_replicated_family(self):
        from core.pg_operational import EXPENSE_TYPES_TABLE, _FULL_REPLACE

        assert EXPENSE_TYPES_TABLE in {spec[0] for spec in _FULL_REPLACE}


class TestTheMirrorNeverStopsASync:
    """`_mirror` swallows, so a Postgres fault costs freshness and not the
    sync. The backfill and the comparison are what recover from it."""

    @pytest.mark.asyncio
    async def test_it_stands_down_when_the_mirror_is_off(self):
        from core.pg_landing import mirror_expenses

        with patch("core.pg_landing.enabled", return_value=False):
            out = await mirror_expenses([{"id": 1, "expenses": [{"id": 9}]}])
        assert out.skipped


class TestTheBackfill:
    """No cursor, and it stamps only when it finished."""

    def _store(self, duck_ids):
        store = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(i,) for i in duck_ids]
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        store.connection.return_value = ctx
        return store

    def test_it_never_awaits_the_network_holding_the_store(self):
        """Every reader waits behind that lock; the first draft of this module
        read Postgres inside the block."""
        src = (REPO / "core" / "pg_expense_backfill.py").read_text()
        for node in ast.walk(ast.parse(src)):
            if not (isinstance(node, ast.AsyncWith)
                    and "connection()" in ast.unparse(node.items[0].context_expr)):
                continue
            for inner in ast.walk(node):
                assert not isinstance(inner, ast.Await), (
                    f"line {node.lineno} awaits while holding the store lock"
                )

    def test_it_keeps_no_cursor(self):
        """Interrupt it anywhere and the next run resumes by recomputing;
        there is no stored position to be wrong."""
        src = (REPO / "core" / "pg_expense_backfill.py").read_text()
        assert "cursor" not in src.lower().replace("no cursor", "")


