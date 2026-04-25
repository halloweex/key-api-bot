"""Tests for refresh_warehouse_layers Silver path: bootstrap, cascade, orphans, guardrail.

These tests construct a small DuckDB instance, populate `orders` (and sometimes
`silver_orders`) directly via SQL to control state precisely, then drive
`refresh_warehouse_layers` and assert silver state.
"""
from datetime import date
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(
    conn,
    *,
    oid: int,
    buyer_id: int | None = None,
    ordered_at: str = "2026-01-15T10:00:00+03:00",
    status_id: int = 1,
    grand_total: str = "100.00",
    source_id: int = 4,  # Shopify (active source)
    manager_id: int | None = None,  # NULL → retail
) -> None:
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
        """,
        [
            oid, source_id, status_id, grand_total, ordered_at,
            ordered_at, ordered_at, buyer_id, manager_id,
        ],
    )


def _insert_silver(
    conn,
    *,
    oid: int,
    buyer_id: int | None = None,
    order_date: str = "2026-01-15",
    status_id: int = 1,
    grand_total: str = "100.00",
    source_id: int = 4,
    manager_id: int | None = None,
    is_return: bool = False,
    is_new_customer: bool = False,
    buyer_first_order_date: str | None = None,
) -> None:
    """Direct insert into silver_orders for setting up "stale" state."""
    conn.execute(
        """
        INSERT INTO silver_orders (
            id, source_id, status_id, grand_total, ordered_at,
            buyer_id, manager_id, order_date, is_return, sales_type,
            is_active_source, source_name, is_new_customer,
            buyer_first_order_date, promocode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'retail', TRUE,
                  'Shopify', ?, ?, NULL)
        """,
        [
            oid, source_id, status_id, grand_total,
            order_date + "T10:00:00+03:00",
            buyer_id, manager_id, order_date, is_return,
            is_new_customer, buyer_first_order_date,
        ],
    )


async def _silver_state(store: DuckDBStore) -> dict:
    """Snapshot silver_orders as {id: row dict} for assertions."""
    async with store.connection() as conn:
        rows = conn.execute(
            "SELECT id, buyer_id, is_return, is_new_customer, "
            "buyer_first_order_date, order_date FROM silver_orders ORDER BY id"
        ).fetchall()
    return {
        r[0]: {
            "buyer_id": r[1],
            "is_return": r[2],
            "is_new_customer": r[3],
            "buyer_first_order_date": r[4],
            "order_date": r[5],
        }
        for r in rows
    }


class TestSilverFullRebuildPaths:
    """Cases where Silver should fall back to full rebuild."""

    @pytest.mark.asyncio
    async def test_full_when_changed_ids_none(self, tmp_path):
        """Manual trigger / startup / drift retry — no changed_ids → full."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, oid=1, buyer_id=10)
                _insert_order(conn, oid=2, buyer_id=20)

            await store.refresh_warehouse_layers(trigger="manual", changed_order_ids=None)

            state = await _silver_state(store)
            assert set(state.keys()) == {1, 2}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_full_after_compact_when_silver_empty(self, tmp_path):
        """Post-compact: orders has rows, silver is empty → bootstrap to full.

        Reproduces the bug from commit 94ab9c6: incremental path with empty
        silver would have left silver at len(changed_ids), not orders_count.
        """
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                for i in range(1, 11):
                    _insert_order(conn, oid=i, buyer_id=100 + i)
                # silver_orders is empty (post-compact state)

            # Caller passes only one id, but silver is empty → must full-rebuild
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            assert len(state) == 10, "Silver must contain all 10 orders, not just changed_ids"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_full_when_silver_below_95pct(self, tmp_path):
        """Silver < 95% of orders → bootstrap heuristic triggers full."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                for i in range(1, 21):  # 20 orders
                    _insert_order(conn, oid=i, buyer_id=100 + i)
                # Silver has only 18 of 20 = 90% < 95% threshold
                for i in range(1, 19):
                    _insert_silver(conn, oid=i, buyer_id=100 + i)

            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            assert len(state) == 20  # all orders now in silver
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_full_when_scope_exceeds_guardrail(self, tmp_path):
        """Cascade scope > 50% of orders → guardrail kicks, runs full."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # 10 orders all share buyer_id=100. Changing any one cascades to all 10 → 100%.
                for i in range(1, 11):
                    _insert_order(conn, oid=i, buyer_id=100)
                    _insert_silver(conn, oid=i, buyer_id=100)

            # Touch one order. Cascade scope = all 10 = 100% > 50% guardrail.
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            assert len(state) == 10
        finally:
            await store.close()


class TestSilverIncrementalPath:
    """Cases where incremental should run and produce correct state."""

    @pytest.mark.asyncio
    async def test_incremental_basic_only_touches_scope(self, tmp_path):
        """Untouched orders' silver rows stay untouched (no DELETE+INSERT)."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # 10 orders, distinct buyers, populated in silver
                for i in range(1, 11):
                    _insert_order(conn, oid=i, buyer_id=100 + i)
                    _insert_silver(conn, oid=i, buyer_id=100 + i)

            # Touch order 5
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[5]
            )

            state = await _silver_state(store)
            assert len(state) == 10  # nothing dropped or added

        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_cascade_first_order_returned(self, tmp_path):
        """Buyer's first order returns → second order should become is_new_customer.

        This is the core cascade semantics that motivated cascade scope.
        """
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # Buyer 100: O1 (Jan 1), O2 (Jan 5). Both initially good.
                _insert_order(
                    conn, oid=1, buyer_id=100,
                    ordered_at="2026-01-01T10:00:00+03:00",
                )
                _insert_order(
                    conn, oid=2, buyer_id=100,
                    ordered_at="2026-01-05T10:00:00+03:00",
                )

            # Initial full rebuild — sets up correct baseline
            await store.refresh_warehouse_layers(trigger="manual", changed_order_ids=None)
            baseline = await _silver_state(store)
            assert baseline[1]["is_new_customer"] is True
            assert baseline[2]["is_new_customer"] is False
            assert baseline[1]["buyer_first_order_date"] == date(2026, 1, 1)
            assert baseline[2]["buyer_first_order_date"] == date(2026, 1, 1)

            # Now O1 is returned (status_id=19)
            async with store.connection() as conn:
                conn.execute("UPDATE orders SET status_id = 19 WHERE id = 1")

            # Sync would mark dirty with [1], not [1, 2] — cascade must catch O2
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            assert state[1]["is_return"] is True
            assert state[1]["is_new_customer"] is False
            # Cascade: O2 should now be the buyer's first non-return order
            assert state[2]["is_new_customer"] is True, (
                "Cascade missed: O2 should become first after O1 returned"
            )
            assert state[2]["buyer_first_order_date"] == date(2026, 1, 5)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_cascade_buyer_reassignment(self, tmp_path):
        """Order moves from B1 to B2 → both buyers' rows must be refreshed.

        Validates that affected_buyers UNIONs orders and silver to catch the
        old buyer (still in silver) and the new buyer (in orders).
        """
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # B1: O1 (its only order). B2: O2 (its only order).
                _insert_order(
                    conn, oid=1, buyer_id=1,
                    ordered_at="2026-01-01T10:00:00+03:00",
                )
                _insert_order(
                    conn, oid=2, buyer_id=2,
                    ordered_at="2026-01-05T10:00:00+03:00",
                )

            await store.refresh_warehouse_layers(trigger="manual", changed_order_ids=None)
            baseline = await _silver_state(store)
            assert baseline[1]["is_new_customer"] is True  # B1's first
            assert baseline[2]["is_new_customer"] is True  # B2's first

            # Reassign O1: B1 → B2 (B1 now has no orders; B2 now has both)
            async with store.connection() as conn:
                conn.execute("UPDATE orders SET buyer_id = 2 WHERE id = 1")

            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            # O1 now belongs to B2; B2's MIN(order_date) = Jan 1 (O1's date)
            assert state[1]["buyer_id"] == 2
            assert state[1]["is_new_customer"] is True
            assert state[1]["buyer_first_order_date"] == date(2026, 1, 1)
            # O2 was previously B2's first (Jan 5), now O1 (Jan 1) is first → O2 not new
            assert state[2]["is_new_customer"] is False, (
                "Old buyer's silver row not recomputed — affected_buyers UNION missed silver side"
            )
            assert state[2]["buyer_first_order_date"] == date(2026, 1, 1)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_null_buyer_no_cascade(self, tmp_path):
        """Order with NULL buyer_id: scope is just changed_id, no cascade."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, oid=1, buyer_id=None)
                _insert_order(conn, oid=2, buyer_id=200)
                _insert_silver(conn, oid=1, buyer_id=None)
                _insert_silver(conn, oid=2, buyer_id=200)

            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            # Both rows still present; null-buyer order has is_new_customer=False
            # (CASE requires buyer_id IS NOT NULL for TRUE)
            assert state[1]["is_new_customer"] is False
            assert state[1]["buyer_first_order_date"] is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_orphan_silver_row_cleaned_via_cascade(self, tmp_path):
        """Silver has row for an order no longer in `orders` (deleted),
        and that order's buyer is in cascade scope → orphan must be DELETE'd.

        Validates the fix in commit 8eba56f (UNION on silver-side scope).
        """
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                # Buyer 100 has O1, O2 in orders. O3 is an orphan in silver only.
                _insert_order(
                    conn, oid=1, buyer_id=100,
                    ordered_at="2026-01-01T10:00:00+03:00",
                )
                _insert_order(
                    conn, oid=2, buyer_id=100,
                    ordered_at="2026-01-05T10:00:00+03:00",
                )
                _insert_silver(
                    conn, oid=1, buyer_id=100, order_date="2026-01-01",
                )
                _insert_silver(
                    conn, oid=2, buyer_id=100, order_date="2026-01-05",
                )
                _insert_silver(
                    conn, oid=3, buyer_id=100, order_date="2026-01-10",
                )  # orphan — not in orders

            # Touch O1. Cascade includes B100 → all of B100's silver rows.
            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[1]
            )

            state = await _silver_state(store)
            assert 3 not in state, "Orphan silver row not cleaned by cascade scope"
            assert set(state.keys()) == {1, 2}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_orphan_via_changed_id_cleaned(self, tmp_path):
        """Order in changed_ids that doesn't exist in `orders` → silver row deleted."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, oid=1, buyer_id=100)
                _insert_silver(conn, oid=1, buyer_id=100)
                # O2 was deleted from orders but is still in silver
                _insert_silver(conn, oid=2, buyer_id=200)

            await store.refresh_warehouse_layers(
                trigger="dirty_flag", changed_order_ids=[2]
            )

            state = await _silver_state(store)
            assert 2 not in state, "Orphan order id passed via changed_ids must be DELETE'd"
            assert 1 in state
        finally:
            await store.close()
