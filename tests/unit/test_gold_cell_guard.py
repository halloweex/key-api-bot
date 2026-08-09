"""Gold holds one row per (date, sales_type). The scalars cannot see a missing one.

Across the three backups taken during the August incident: 100 → 90 → 84
mismatched cells and **zero** value mismatches. Every single one was a cell
Gold was missing while the revenue sums still agreed — which is precisely the
shape the checksums are blind to, and precisely what a set comparison catches
for about seven milliseconds.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(conn, oid, buyer_id, when, total="1000.00", manager_id=None):
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, 4, 12, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
        [oid, total, when, when, when, buyer_id, manager_id],
    )


class TestTheGuard:
    @pytest.mark.asyncio
    async def test_a_healthy_warehouse_passes(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            async with store.connection() as conn:
                _insert_order(conn, 1, 10, when)
                _insert_order(conn, 2, 20, when - timedelta(days=3))

            res = await store.refresh_warehouse_layers(trigger="manual")
            assert res["validation_passed"] is True
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_missing_cell_fails_validation_while_the_sums_agree(self, tmp_path):
        """The incident's exact shape: a cell gone, every scalar still balanced."""
        store = await _make_store(tmp_path)
        try:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            async with store.connection() as conn:
                _insert_order(conn, 1, 10, when, total="1000.00")
                # A returned order: its cell carries no revenue at all, so
                # deleting the row leaves every revenue checksum untouched.
                conn.execute(
                    """
                    INSERT INTO orders (
                        id, source_id, status_id, grand_total, ordered_at,
                        created_at, updated_at, buyer_id, manager_id,
                        manager_comment, promocode
                    ) VALUES (2, 4, 19, '5000.00', ?, ?, ?, 20, NULL, NULL, NULL)
                    """,
                    [when - timedelta(days=3)] * 3,
                )
            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                before = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(revenue), 0) FROM gold_daily_revenue"
                ).fetchone()
                conn.execute(
                    "DELETE FROM gold_daily_revenue WHERE revenue = 0"
                )
                after = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(revenue), 0) FROM gold_daily_revenue"
                ).fetchone()

            assert after[0] < before[0], "a cell was removed"
            assert float(after[1]) == float(before[1]), (
                "…and the revenue sum did not move, which is why the "
                "checksums were blind to it"
            )

            # An incremental refresh that touches neither date leaves the hole.
            res = await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )
            assert res["checksum_match"] is True, "the scalar still agrees"
            assert res["validation_passed"] is False, "the guard does not"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_orphaned_cell_is_caught_too(self, tmp_path):
        """A Gold row no Silver row supports — the other direction."""
        store = await _make_store(tmp_path)
        try:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            async with store.connection() as conn:
                # Enough orders that touching one stays under the 50% cascade
                # guardrail — otherwise Silver goes full, Gold follows, and the
                # orphan is swept away before the guard ever sees it.
                for oid in range(1, 6):
                    _insert_order(conn, oid, 10 + oid, when)
            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                conn.execute("""
                    INSERT INTO gold_daily_revenue
                    (date, sales_type, revenue, orders_count, unique_customers,
                     new_customers, returning_customers, returns_count,
                     returns_revenue, avg_order_value)
                    VALUES (DATE '2019-01-01', 'retail', 0, 0, 0, 0, 0, 0, 0, 0)
                """)

            res = await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )
            assert res["validation_passed"] is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_full_rebuild_heals_it(self, tmp_path):
        """The guard feeds validation_passed precisely because a rebuild fixes it."""
        store = await _make_store(tmp_path)
        try:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            async with store.connection() as conn:
                _insert_order(conn, 1, 10, when)
                _insert_order(conn, 2, 20, when - timedelta(days=3))
            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                conn.execute("DELETE FROM gold_daily_revenue WHERE date = ?",
                             [(when - timedelta(days=3)).date()])

            res = await store.refresh_warehouse_layers(trigger="manual")
            assert res["validation_passed"] is True
        finally:
            await store.close()
