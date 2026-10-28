"""Thirteen of the fourteen Gold columns had nothing checking them.

Validation asserts three scalars, and since the cell guard, that the *set* of
(date, sales_type) cells matches. It says nothing about what is inside a cell
apart from revenue: order counts, unique/new/returning customers, per-source
revenue and orders, returns, average order value. PR #41 was a bug in exactly
one of those — the new-customer baseline — and a person found it, not a check.

Report-only, and structurally so: an integrity finding cannot reach
`validation_passed`, so this cannot start the rebuild loop the design debate
vetoed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.data_quality import check_internal_integrity
from core.duckdb_store import GOLD_REVENUE_SELECT_SQL, DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(conn, oid, buyer_id, when, total="1000.00"):
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, 4, 12, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """,
        [oid, total, when, when, when, buyer_id],
    )


async def _seed(store, n=4):
    """Orders on two dates: order 1 on `when`, the rest three days earlier.

    An incremental refresh scoped to order 1 rebuilds only its own date, so
    the older cell is the one a corruption can survive in — which is exactly
    the situation the audit exists for.

    Anchored to midday UTC on purpose. Silver derives order_date as the *Kyiv*
    calendar date (`duckdb_store.py:2206`), while the tests below address rows
    by `.date()` of the UTC instant seeded here. Those two agree at every hour
    except 21:00-24:00 UTC, when Kyiv has already turned over — and in that
    window the corrupting UPDATE matched no row, the audit correctly found
    nothing, and the test failed. It was broken three hours out of every
    twenty-four for as long as it existed, which nothing noticed because
    nothing ran the suite. Midday is far enough from both edges that no
    offset Kyiv has ever used can move the date.
    """
    when = datetime.now(timezone.utc).replace(
        hour=12, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    older = when - timedelta(days=3)
    async with store.connection() as conn:
        _insert_order(conn, 1, 11, when)
        for oid in range(2, n + 1):
            _insert_order(conn, oid, 10 + oid, older)
    await store.refresh_warehouse_layers(trigger="manual")
    return when, older


def _findings(conn):
    return {i.check_name: i for i in check_internal_integrity(conn)}


class TestOneDefinitionOfACell:
    def test_the_rebuild_and_the_audit_read_the_same_sql(self):
        """An audit with its own copy of the projection asks a different
        question: whether two hand-written queries agree."""
        import inspect
        from core import data_quality, duckdb_store

        assert "{date_filter}" in GOLD_REVENUE_SELECT_SQL
        assert "GOLD_REVENUE_SELECT_SQL" in inspect.getsource(
            duckdb_store.DuckDBStore.refresh_warehouse_layers
        )
        assert "GOLD_REVENUE_SELECT_SQL" in inspect.getsource(
            data_quality._gold_cell_values_check
        )


class TestTheAudit:
    @pytest.mark.asyncio
    async def test_a_freshly_built_warehouse_is_silent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store)
            async with store.connection() as conn:
                assert "gold_cell_values" not in _findings(conn)
        finally:
            await store.close()


class TestItCannotDriveARebuild:
    @pytest.mark.asyncio
    async def test_the_finding_never_reaches_validation_passed(self, tmp_path):
        """The architect's veto, as a test: fourteen columns across ~2 000
        cells failing every two minutes would rebuild the warehouse forever."""
        store = await _make_store(tmp_path)
        try:
            _, older = await _seed(store)
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE gold_daily_revenue SET orders_count = 4242 WHERE date = ?",
                    [older.date()],
                )

            for _ in range(2):
                res = await store.refresh_warehouse_layers(
                    trigger="dirty_flag", changed_order_ids=[1],
                )
                assert res["validation_passed"] is True

            is_dirty, _ = await store.consume_warehouse_dirty()
            assert is_dirty is False, "a report-only finding must not mark dirty"
        finally:
            await store.close()
