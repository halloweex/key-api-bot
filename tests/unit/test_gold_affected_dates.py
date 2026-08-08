"""Gold is rebuilt per date, so the dates a row *leaves* matter as much as the
dates it arrives at.

`affected_dates` was read from Silver **after** the rebuild, which only ever
sees where rows ended up. Move an order from the 1st to the 5th and only the
5th is recomputed: the 1st keeps the revenue too, and the money is counted
twice, in a table whose totals still balance against Silver's — because Silver
holds the order once and Gold's error is a spare row on a date Silver no longer
mentions.
"""
from datetime import date
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(conn, *, oid, buyer_id, ordered_at, grand_total="1000.00"):
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, 4, 1, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """,
        [oid, grand_total, ordered_at, ordered_at, ordered_at, buyer_id],
    )


async def _gold_by_date(store) -> dict:
    async with store.connection() as conn:
        rows = conn.execute(
            "SELECT date, SUM(revenue) FROM gold_daily_revenue GROUP BY date ORDER BY date"
        ).fetchall()
    return {r[0]: float(r[1]) for r in rows}


class TestOrderMovedToAnotherDate:
    @pytest.mark.asyncio
    async def test_revenue_is_not_left_behind_on_the_old_date(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, oid=1, buyer_id=10,
                              ordered_at="2026-08-01T10:00:00+03:00")
                _insert_order(conn, oid=2, buyer_id=20,
                              ordered_at="2026-08-09T10:00:00+03:00")
            await store.refresh_warehouse_layers(trigger="manual")
            assert await _gold_by_date(store) == {
                date(2026, 8, 1): 1000.0, date(2026, 8, 9): 1000.0,
            }

            # KeyCRM corrects the order's date — a backdated order arriving late.
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE orders SET ordered_at = ? WHERE id = 1",
                    ["2026-08-05T10:00:00+03:00"],
                )
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )

            gold = await _gold_by_date(store)
            assert date(2026, 8, 1) not in gold, "revenue left behind on the old date"
            assert gold[date(2026, 8, 5)] == 1000.0
            assert sum(gold.values()) == 2000.0, "the order was counted twice"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_silver_and_gold_still_agree_afterwards(self, tmp_path):
        """The duplicate would balance the checksum, so assert the shape too."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, oid=1, buyer_id=10,
                              ordered_at="2026-08-01T10:00:00+03:00")
            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                conn.execute(
                    "UPDATE orders SET ordered_at = ? WHERE id = 1",
                    ["2026-08-05T10:00:00+03:00"],
                )
            res = await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )

            assert res["validation_passed"] is True
            async with store.connection() as conn:
                dates_in_silver = {
                    r[0] for r in conn.execute(
                        "SELECT DISTINCT order_date FROM silver_orders"
                    ).fetchall()
                }
                dates_in_gold = {
                    r[0] for r in conn.execute(
                        "SELECT DISTINCT date FROM gold_daily_revenue"
                    ).fetchall()
                }
            assert dates_in_gold == dates_in_silver
        finally:
            await store.close()


class TestOrderRemovedUpstream:
    @pytest.mark.asyncio
    async def test_a_deleted_order_leaves_its_date(self, tmp_path):
        """The cascade keeps the run incremental, so the empty date must still
        be recomputed — after the DELETE nothing points at it any more."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # One buyer, two dates: the cascade covers the survivor, which
                # is what keeps affected_dates non-empty and the run incremental.
                _insert_order(conn, oid=1, buyer_id=10,
                              ordered_at="2026-08-01T10:00:00+03:00",
                              grand_total="500.00")
                _insert_order(conn, oid=2, buyer_id=10,
                              ordered_at="2026-08-03T10:00:00+03:00",
                              grand_total="700.00")
                # Padding so deleting one order does not trip the 50% guardrail.
                for oid in range(3, 13):
                    _insert_order(conn, oid=oid, buyer_id=oid,
                                  ordered_at="2026-08-09T10:00:00+03:00",
                                  grand_total="100.00")
            await store.refresh_warehouse_layers(trigger="manual")
            assert (await _gold_by_date(store))[date(2026, 8, 1)] == 500.0

            async with store.connection() as conn:
                conn.execute("DELETE FROM orders WHERE id = 1")
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )

            gold = await _gold_by_date(store)
            assert date(2026, 8, 1) not in gold, "Gold kept a deleted order's date"
            assert gold[date(2026, 8, 3)] == 700.0
        finally:
            await store.close()


class TestGoldFollowsSilver:
    @pytest.mark.asyncio
    async def test_guardrail_full_silver_forces_full_gold(self, tmp_path):
        """A full Silver rebuild with a partial Gold one is how Gold ends up
        holding rows no Silver row supports."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # All one buyer → touching any order cascades to 100% > 50%.
                for oid in range(1, 5):
                    _insert_order(conn, oid=oid, buyer_id=100,
                                  ordered_at=f"2026-08-0{oid}T10:00:00+03:00")
            await store.refresh_warehouse_layers(trigger="manual")

            # A stale Gold row on a date no changed order touches. Only a full
            # Gold rebuild removes it.
            async with store.connection() as conn:
                conn.execute("""
                    INSERT INTO gold_daily_revenue
                    (date, sales_type, revenue, orders_count, unique_customers,
                     new_customers, returning_customers, returns_count,
                     returns_revenue, avg_order_value)
                    VALUES (DATE '2025-01-01', 'retail', 99999, 1, 1, 0, 1, 0, 0, 99999)
                """)

            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )

            gold = await _gold_by_date(store)
            assert date(2025, 1, 1) not in gold, (
                "Silver was rebuilt in full; Gold was not"
            )
        finally:
            await store.close()


class TestTheSilentHalf:
    """Not every stale row disturbs the checksum.

    Validation compares revenue sums. A *returned* order contributes nothing to
    revenue on either date, so moving one leaves a duplicated returns row on the
    old date with every checksum still balancing — nothing fails, nothing
    retries, nothing is said.
    """

    @pytest.mark.asyncio
    async def test_a_moved_return_does_not_leave_a_ghost(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO orders (
                        id, source_id, status_id, grand_total, ordered_at,
                        created_at, updated_at, buyer_id, manager_id,
                        manager_comment, promocode
                    ) VALUES (1, 4, 19, '800.00', ?, ?, ?, 10, NULL, NULL, NULL)
                    """,
                    ["2026-08-01T10:00:00+03:00"] * 3,
                )
                _insert_order(conn, oid=2, buyer_id=20,
                              ordered_at="2026-08-09T10:00:00+03:00")
            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                conn.execute(
                    "UPDATE orders SET ordered_at = ? WHERE id = 1",
                    ["2026-08-05T10:00:00+03:00"],
                )
            res = await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1],
            )

            # The checksums are blind here — they balanced before the fix too.
            assert res["validation_passed"] is True

            async with store.connection() as conn:
                returns = conn.execute("""
                    SELECT date, returns_count, returns_revenue
                    FROM gold_daily_revenue
                    WHERE returns_count > 0 ORDER BY date
                """).fetchall()

            assert [r[0] for r in returns] == [date(2026, 8, 5)], (
                "the return is counted on both the old date and the new one"
            )
            assert float(returns[0][2]) == 800.0
        finally:
            await store.close()
