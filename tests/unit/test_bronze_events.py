"""Tests for H3 bronze_order_events: schema + append + stats + backfill + promotion + prune + replay."""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _sample_order(oid: int, **overrides) -> dict:
    """Build a minimal order dict suitable for both upsert and bronze append."""
    base = {
        "id": oid,
        "source_id": 1,
        "status_id": 3,
        "grand_total": "100.00",
        "ordered_at": "2026-01-15T10:00:00+04:00",
        "created_at": "2026-01-15T09:00:00+04:00",
        "updated_at": "2026-01-15T11:00:00+04:00",
        "buyer": {"id": 42},
        "manager": {"id": 22},
        "manager_comment": None,
        "promocode": None,
        "products": [],
    }
    base.update(overrides)
    return base


class TestBronzeEvents:
    @pytest.mark.asyncio
    async def test_table_exists_with_expected_columns(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                cols = conn.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = 'bronze_order_events' ORDER BY ordinal_position"
                ).fetchall()
        finally:
            await store.close()

        names = [c[0] for c in cols]
        assert names == [
            "id", "order_id", "payload", "source", "event_ts", "processed_at",
        ]
        types = {c[0]: c[1] for c in cols}
        assert "BIGINT" in types["id"].upper()
        assert "INTEGER" in types["order_id"].upper()
        assert "JSON" in types["payload"].upper()

    @pytest.mark.asyncio
    async def test_append_round_trip(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            orders = [
                {"id": 101, "status_id": 1, "grand_total": "123.45", "promocode": "FOO"},
                {"id": 102, "status_id": 2, "grand_total": "67.89"},
                {"id": None},           # must be skipped
                {"status_id": 3},       # no id -> skipped
                {"id": "not-a-number"}, # unparseable -> skipped
            ]
            n = await store.append_bronze_events(orders, source="sync_delta")
            assert n == 2

            async with store.connection() as conn:
                rows = conn.execute(
                    "SELECT order_id, source, payload::VARCHAR, processed_at "
                    "FROM bronze_order_events ORDER BY order_id"
                ).fetchall()

            assert len(rows) == 2
            assert rows[0][0] == 101
            assert rows[0][1] == "sync_delta"
            assert rows[0][3] is None  # unprocessed
            payload = json.loads(rows[0][2])
            assert payload["status_id"] == 1
            assert payload["promocode"] == "FOO"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_append_empty_is_noop(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            assert await store.append_bronze_events([], source="sync_delta") == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_multiple_events_per_order(self, tmp_path):
        """One order_id can have many events — audit log, not a buffer."""
        store = await _make_store(tmp_path)
        try:
            for i in range(5):
                await store.append_bronze_events(
                    [{"id": 999, "status_id": i, "grand_total": str(100 + i)}],
                    source="sync_delta",
                )
            async with store.connection() as conn:
                (count,) = conn.execute(
                    "SELECT COUNT(*) FROM bronze_order_events WHERE order_id = 999"
                ).fetchone()
                latest_payload = conn.execute(
                    "SELECT payload::VARCHAR FROM bronze_order_events "
                    "WHERE order_id = 999 ORDER BY event_ts DESC LIMIT 1"
                ).fetchone()[0]
            assert count == 5
            assert json.loads(latest_payload)["status_id"] == 4
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_stats_empty(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            stats = await store.get_bronze_stats()
            assert stats["total"] == 0
            assert stats["unprocessed"] == 0
            assert stats["oldest_unprocessed_age_s"] is None
            assert stats["latest_event_ts"] is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_stats_after_append(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.append_bronze_events(
                [{"id": i} for i in range(10)], source="sync_delta",
            )
            stats = await store.get_bronze_stats()
            assert stats["total"] == 10
            assert stats["unprocessed"] == 10
            assert stats["oldest_unprocessed_age_s"] is not None
            assert stats["oldest_unprocessed_age_s"] >= 0
            assert stats["latest_event_ts"] is not None

            # Mark half as processed, stats should reflect it.
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE bronze_order_events SET processed_at = CURRENT_TIMESTAMP "
                    "WHERE order_id < 5"
                )
            stats2 = await store.get_bronze_stats()
            assert stats2["total"] == 10
            assert stats2["unprocessed"] == 5
        finally:
            await store.close()


class TestBackfillBronze:
    """H3 Phase 3: backfill orders → bronze_order_events."""

    @pytest.mark.asyncio
    async def test_backfill_populates_bronze_from_orders(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            # Insert orders directly into orders table
            async with store.connection() as conn:
                for oid in (1, 2, 3):
                    conn.execute(
                        "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at) "
                        "VALUES (?, 1, 3, 100.0, '2026-01-15')", [oid]
                    )

            result = await store.backfill_bronze_from_orders()
            assert result["total_orders"] == 3
            assert result["inserted"] == 3
            assert result["skipped_existing"] == 0

            stats = await store.get_bronze_stats()
            assert stats["total"] == 3
            assert stats["unprocessed"] == 3
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_backfill_idempotent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at) "
                    "VALUES (1, 1, 3, 100.0, '2026-01-15')"
                )

            r1 = await store.backfill_bronze_from_orders()
            assert r1["inserted"] == 1

            r2 = await store.backfill_bronze_from_orders()
            assert r2["inserted"] == 0
            assert r2["skipped_existing"] == 1

            stats = await store.get_bronze_stats()
            assert stats["total"] == 1
        finally:
            await store.close()


class TestPromoteBronzeToOrders:
    """H3 Phase 3: promote bronze events → orders table."""

    @pytest.mark.asyncio
    async def test_promote_inserts_into_orders(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            orders = [_sample_order(101), _sample_order(102)]
            await store.append_bronze_events(orders, source="sync_delta")

            result = await store.promote_bronze_to_orders()
            assert result["promoted"] == 2
            assert result["skipped"] == 0

            async with store.connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            assert count == 2

            stats = await store.get_bronze_stats()
            assert stats["unprocessed"] == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_promote_updates_existing_order(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            # First version
            await store.append_bronze_events(
                [_sample_order(101, grand_total="100.00")], source="sync_delta"
            )
            await store.promote_bronze_to_orders()

            # Updated version
            await store.append_bronze_events(
                [_sample_order(101, grand_total="200.00")], source="sync_delta"
            )
            result = await store.promote_bronze_to_orders()
            assert result["promoted"] == 1

            async with store.connection() as conn:
                total = conn.execute(
                    "SELECT grand_total FROM orders WHERE id = 101"
                ).fetchone()[0]
            assert float(total) == 200.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_promote_noop_when_empty(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            result = await store.promote_bronze_to_orders()
            assert result["promoted"] == 0
        finally:
            await store.close()


class TestPruneBronzeEvents:
    """H3 Phase 5: prune old processed events."""

    @pytest.mark.asyncio
    async def test_prune_deletes_old_processed(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.append_bronze_events(
                [_sample_order(i) for i in range(5)], source="sync_delta"
            )

            # Mark all as processed and backdate event_ts
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE bronze_order_events "
                    "SET processed_at = CURRENT_TIMESTAMP, "
                    "    event_ts = CURRENT_TIMESTAMP - INTERVAL '10 days'"
                )

            deleted = await store.prune_bronze_events(retention_days=7)
            assert deleted == 5

            stats = await store.get_bronze_stats()
            assert stats["total"] == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_prune_keeps_unprocessed(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.append_bronze_events(
                [_sample_order(i) for i in range(3)], source="sync_delta"
            )

            # Backdate but don't mark as processed
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE bronze_order_events "
                    "SET event_ts = CURRENT_TIMESTAMP - INTERVAL '10 days'"
                )

            deleted = await store.prune_bronze_events(retention_days=7)
            assert deleted == 0

            stats = await store.get_bronze_stats()
            assert stats["total"] == 3
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_prune_keeps_recent_processed(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.append_bronze_events(
                [_sample_order(i) for i in range(3)], source="sync_delta"
            )

            # Mark as processed but keep recent
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE bronze_order_events SET processed_at = CURRENT_TIMESTAMP"
                )

            deleted = await store.prune_bronze_events(retention_days=7)
            assert deleted == 0
        finally:
            await store.close()


class TestReplayBronzeEvents:
    """H3 Phase 5: replay bronze events by resetting processed_at."""

    @pytest.mark.asyncio
    async def test_replay_resets_processed_at(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.append_bronze_events(
                [_sample_order(i) for i in range(5)], source="sync_delta"
            )

            # Mark all as processed
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE bronze_order_events SET processed_at = CURRENT_TIMESTAMP"
                )

            stats_before = await store.get_bronze_stats()
            assert stats_before["unprocessed"] == 0

            since = datetime.now(timezone.utc) - timedelta(hours=1)
            replayed = await store.replay_bronze_events(since=since)
            assert replayed == 5

            stats_after = await store.get_bronze_stats()
            assert stats_after["unprocessed"] == 5
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_replay_filters_by_source(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.append_bronze_events(
                [_sample_order(1)], source="sync_delta"
            )
            await store.append_bronze_events(
                [_sample_order(2)], source="reconciliation"
            )

            # Mark all as processed
            async with store.connection() as conn:
                conn.execute(
                    "UPDATE bronze_order_events SET processed_at = CURRENT_TIMESTAMP"
                )

            since = datetime.now(timezone.utc) - timedelta(hours=1)
            replayed = await store.replay_bronze_events(since=since, source="reconciliation")
            assert replayed == 1

            stats = await store.get_bronze_stats()
            assert stats["unprocessed"] == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_replay_requires_since(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            with pytest.raises(ValueError, match="since"):
                await store.replay_bronze_events(since=None)
        finally:
            await store.close()
