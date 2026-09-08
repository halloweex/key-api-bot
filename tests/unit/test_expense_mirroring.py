"""The expense mirror, its backfill, and the comparison that watches them.

`tests/unit/test_pg_expenses_read.py` covers which engine answers and the
shared parse. This covers the machinery that keeps the two stores equal: that
the sync ships what it writes, that history can be carried across, and that
the daily comparison actually catches the ways this can go wrong.
"""
from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.landing_rows import EXPENSE_COLUMNS
from core.mirror_reconciliation import EXPENSES_TABLE, compare_table

REPO = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)


def _row(eid, order_id=1, type_id=1, amount=100.0, description="delivery"):
    return (eid, order_id, type_id, amount, description, "paid", None,
            NOW - timedelta(days=3))


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

    def test_every_expense_upsert_is_mirrored(self):
        for name, calls in self._pairs():
            if "upsert_expenses_batch" in calls:
                assert "mirror_expenses" in calls, (
                    f"{name} writes expenses to DuckDB and never ships them"
                )

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
    async def test_a_failure_is_reported_not_raised(self):
        from core.pg_landing import mirror_expenses

        with patch("core.pg_landing.enabled", return_value=True), \
             patch("core.pg_landing._write",
                   new=AsyncMock(side_effect=RuntimeError("pg is down"))), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            out = await mirror_expenses([{"id": 1, "expenses": [{"id": 9}]}])
        assert out.ok is False and "pg is down" in (out.error or "")

    @pytest.mark.asyncio
    async def test_it_stands_down_when_the_mirror_is_off(self):
        from core.pg_landing import mirror_expense_types

        with patch("core.pg_landing.enabled", return_value=False):
            out = await mirror_expense_types([{"id": 1, "name": "x"}])
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

    @pytest.mark.asyncio
    async def test_nothing_missing_stamps_and_ships_nothing(self):
        from core import pg_expense_backfill as bf

        with patch.object(bf, "_postgres_ids", new=AsyncMock(return_value={1, 2})), \
             patch.object(bf, "_mark_backfilled", new=AsyncMock()) as mark, \
             patch("core.pg.get_pool", new=AsyncMock(return_value=MagicMock())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.pg_landing._WATERMARK_OK", "SELECT 1"):
            out = await bf.backfill_expenses(self._store({1, 2}))
        assert out["missing_at_start"] == 0 and out["complete"] is True
        mark.assert_awaited()

    @pytest.mark.asyncio
    async def test_it_does_not_stamp_while_rows_are_still_missing(self):
        """A partial backfill claiming completion would open the row-level
        comparison over rows it had not yet shipped."""
        from core import pg_expense_backfill as bf

        with patch.object(bf, "_postgres_ids", new=AsyncMock(return_value={1})), \
             patch.object(bf, "_mark_backfilled", new=AsyncMock()) as mark, \
             patch.object(bf, "_read_chunk", return_value=[]), \
             patch("core.pg.get_pool", new=AsyncMock(return_value=MagicMock())), \
             patch("core.pg.require_revision", new=AsyncMock()):
            out = await bf.backfill_expenses(self._store({1, 2, 3}))
        assert out["still_missing"] == 2 and out["complete"] is False
        mark.assert_not_awaited()

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


class TestTheComparisonCatchesWhatItMustCatch:
    WATERMARK = {"last_ok_at": NOW - timedelta(minutes=2),
                 "backfilled_at": NOW - timedelta(days=1),
                 "failures_since_ok": 0, "last_error": None}

    def _run(self, dk, pg):
        synced = {k: NOW - timedelta(days=3) for k in dk}
        return compare_table(
            EXPENSES_TABLE, dk, synced, pg, self.WATERMARK,
            now=NOW, grace_minutes=15,
        )

    def _sides(self):
        dk = {i: _row(i) for i in (10, 11, 12)}
        return dk, dict(dk)

    def test_an_honest_copy_reports_nothing(self):
        dk, pg = self._sides()
        assert self._run(dk, pg) == []

    def test_a_changed_amount_is_caught(self):
        dk, pg = self._sides()
        pg[11] = _row(11, amount=999.0)
        assert self._run(dk, pg)

    def test_a_reworded_description_of_equal_length_is_caught(self):
        """The reason this table is read whole rather than fingerprinted: a
        fingerprint sums numbers and text *lengths*."""
        dk, pg = self._sides()
        pg[12] = _row(12, description="delivered")
        assert len("delivery") != len("delivered")   # not the equal-length case
        assert self._run(dk, pg)
        pg[12] = _row(12, description="deliverY")
        assert len("delivery") == len("deliverY")    # the one a print misses
        assert self._run(dk, pg)

    def test_an_expense_only_postgres_has_is_caught(self):
        dk, pg = self._sides()
        pg[99] = _row(99)
        assert self._run(dk, pg)

    def test_the_clock_is_this_stores_bookkeeping(self):
        """Not KeyCRM's `created_at`, which would read a newly synced old
        expense as lost, and would be both the clock and a compared value."""
        assert EXPENSES_TABLE.synced_column == "synced_at"
        assert "synced_at" not in EXPENSES_TABLE.columns
        assert set(EXPENSES_TABLE.columns) == set(EXPENSE_COLUMNS)

    def test_it_is_not_a_full_replace(self):
        """The mirror upserts a delta, so 'in DuckDB and not in Postgres' has
        the retired reading available to it."""
        assert EXPENSES_TABLE.full_replace is False
