"""End-to-end tests for upsert_orders skip-if-unchanged.

The decider is unit-tested separately. This file exercises the full
pipeline against a real DuckDB: insert orders, run upsert again with the
same payload, and verify the second call performs zero writes.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


UTC = timezone.utc


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _order_payload(
    oid: int,
    *,
    updated_at: str = "2026-04-01T10:00:00+00:00",
    status_id: int = 12,
    grand_total: str = "100.00",
    qty: int = 1,
):
    """Build a KeyCRM-shape order dict accepted by Order.from_api."""
    return {
        "id": oid,
        "source_id": 1,
        "status_id": status_id,
        "grand_total": grand_total,
        "ordered_at": "2026-04-01T10:00:00+00:00",
        "created_at": "2026-04-01T09:00:00+00:00",
        "updated_at": updated_at,
        "buyer": None,
        "manager": None,
        "manager_comment": None,
        "promocode": None,
        "products": [
            {
                "id": oid * 1000 + 1,
                "product_id": 1,
                "name": "test product",
                "quantity": qty,
                "price_sold": "50.00",
            }
        ],
    }


async def _count(store, table: str) -> int:
    async with store.connection() as conn:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


async def _max_synced_at(store, order_id: int):
    async with store.connection() as conn:
        return conn.execute(
            "SELECT synced_at FROM orders WHERE id = ?", [order_id]
        ).fetchone()[0]


class TestSkipIfUnchanged:
    @pytest.mark.asyncio
    async def test_initial_insert_writes_orders_and_products(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            orders = [_order_payload(1), _order_payload(2)]
            n = await store.upsert_orders(orders)
            assert n == 2
            assert await _count(store, "orders") == 2
            assert await _count(store, "order_products") == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_resyncing_same_payload_does_not_rewrite_orders(self, tmp_path):
        """The headline behaviour: second call with identical payload
        leaves synced_at untouched on existing rows."""
        store = await _make_store(tmp_path)
        try:
            orders = [_order_payload(1), _order_payload(2)]
            await store.upsert_orders(orders)
            first_sync_at_1 = await _max_synced_at(store, 1)

            # Wait a beat so synced_at would clearly change if rewritten
            import asyncio
            await asyncio.sleep(0.05)

            n = await store.upsert_orders(orders)
            assert n == 2  # both still "successful" (in desired state)
            second_sync_at_1 = await _max_synced_at(store, 1)
            assert first_sync_at_1 == second_sync_at_1, (
                "synced_at must not change when the row was skipped — "
                "otherwise we're still writing identity UPDATEs"
            )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_resyncing_same_payload_does_not_rewrite_products(self, tmp_path):
        """The bigger bytes-on-disk win: order_products are NOT
        deleted+reinserted on identity sync."""
        store = await _make_store(tmp_path)
        try:
            orders = [_order_payload(1, qty=3)]
            await store.upsert_orders(orders)

            # Snapshot the line item's id — DELETE+INSERT would re-issue
            # the same id (we generate from order_id * 1000) but we can
            # detect by inserting a marker product directly:
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [999_999, 1, 999, "marker", 7, 7.00],
                )

            # Re-sync. If skip works, marker survives.
            await store.upsert_orders(orders)

            async with store.connection() as conn:
                marker = conn.execute(
                    "SELECT COUNT(*) FROM order_products WHERE id = 999999"
                ).fetchone()[0]
            assert marker == 1, (
                "skipped order's order_products were rebuilt — "
                "the DELETE+INSERT path is still running"
            )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_newer_updated_at_rewrites_row(self, tmp_path):
        """When KeyCRM bumps updated_at, we MUST write."""
        store = await _make_store(tmp_path)
        try:
            orders = [_order_payload(1, updated_at="2026-04-01T10:00:00+00:00",
                                     grand_total="100.00")]
            await store.upsert_orders(orders)

            orders_v2 = [_order_payload(1, updated_at="2026-04-01T11:00:00+00:00",
                                        grand_total="500.00")]
            await store.upsert_orders(orders_v2)

            async with store.connection() as conn:
                gt = conn.execute(
                    "SELECT grand_total FROM orders WHERE id = 1"
                ).fetchone()[0]
            assert float(gt) == 500.00
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_force_update_overrides_skip(self, tmp_path):
        """status_refresh path: same updated_at, but force=True writes anyway."""
        store = await _make_store(tmp_path)
        try:
            orders = [_order_payload(1, status_id=12,
                                     updated_at="2026-04-01T10:00:00+00:00")]
            await store.upsert_orders(orders)
            first_sync = await _max_synced_at(store, 1)

            import asyncio
            await asyncio.sleep(0.05)

            # Same updated_at, different status — must write
            orders_v2 = [_order_payload(1, status_id=19,  # RETURNED
                                        updated_at="2026-04-01T10:00:00+00:00")]
            await store.upsert_orders(orders_v2, force_update=True)

            async with store.connection() as conn:
                st = conn.execute(
                    "SELECT status_id FROM orders WHERE id = 1"
                ).fetchone()[0]
            assert st == 19
            second_sync = await _max_synced_at(store, 1)
            assert second_sync > first_sync
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_mixed_batch_writes_only_changed(self, tmp_path):
        """Real-world case: 3 orders, 1 changed, 1 unchanged, 1 new.
        Verify only the changed + new ones cause writes."""
        store = await _make_store(tmp_path)
        try:
            # Bootstrap: orders 1 and 2 exist at t0
            t0 = "2026-04-01T10:00:00+00:00"
            await store.upsert_orders([
                _order_payload(1, updated_at=t0),
                _order_payload(2, updated_at=t0),
            ])
            sync_1_first = await _max_synced_at(store, 1)
            sync_2_first = await _max_synced_at(store, 2)

            import asyncio
            await asyncio.sleep(0.05)

            # Mixed batch: order 1 unchanged, order 2 has newer updated_at,
            # order 3 is brand new
            await store.upsert_orders([
                _order_payload(1, updated_at=t0),                      # skip
                _order_payload(2, updated_at="2026-04-01T11:00:00+00:00"),  # write
                _order_payload(3, updated_at=t0),                      # insert
            ])

            sync_1_second = await _max_synced_at(store, 1)
            sync_2_second = await _max_synced_at(store, 2)
            sync_3 = await _max_synced_at(store, 3)

            assert sync_1_first == sync_1_second, "order 1 should have been skipped"
            assert sync_2_first != sync_2_second, "order 2 should have been updated"
            assert sync_3 is not None, "order 3 should have been inserted"
        finally:
            await store.close()


class TestRegression:
    """Guards against re-introducing the 1440× amplification bug."""

    @pytest.mark.asyncio
    async def test_278_orders_resynced_60_times_zero_writes(self, tmp_path):
        """Simulate one hour of incremental_sync on the production hot set
        (278 orders × 60 syncs). The legacy path issued 16,680 UPDATEs.
        The new path must issue exactly 0 after the bootstrap insert."""
        store = await _make_store(tmp_path)
        try:
            orders = [_order_payload(i) for i in range(1, 279)]
            await store.upsert_orders(orders)  # bootstrap

            # Snapshot synced_at for all rows
            async with store.connection() as conn:
                before = {
                    int(r[0]): r[1]
                    for r in conn.execute(
                        "SELECT id, synced_at FROM orders"
                    ).fetchall()
                }

            # 60 resyncs with identical payload
            for _ in range(60):
                await store.upsert_orders(orders)

            async with store.connection() as conn:
                after = {
                    int(r[0]): r[1]
                    for r in conn.execute(
                        "SELECT id, synced_at FROM orders"
                    ).fetchall()
                }

            changed = [oid for oid in before if before[oid] != after[oid]]
            assert changed == [], (
                f"{len(changed)} synced_at values changed in 60 identity "
                f"resyncs — write amplification not eliminated"
            )
        finally:
            await store.close()
