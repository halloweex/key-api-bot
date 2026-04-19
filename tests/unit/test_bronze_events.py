"""Tests for H3 bronze_order_events: schema + append + stats."""
import asyncio
import json
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


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
