"""Finding missing orders without asking the API.

KeyCRM issues order ids as a dense sequence, so every hole between our lowest
and highest id is an order we never stored. 1 660 holes existed as of 2026-08,
and a full-history comparison confirmed 1 616 of them were real orders — the
other 44 are gone upstream and must be remembered, or the job asks for them
again every hour forever.
"""
import pytest

from core.duckdb_store import DuckDBStore


async def _store(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "t.duckdb")
    await store.connect()
    return store


def _insert(conn, *ids):
    for oid in ids:
        conn.execute("""
            INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at)
            VALUES (?, 1, 12, 100.0, '2026-06-10 12:00:00+03')
        """, [oid])


class TestFindOrderIdGaps:
    @pytest.mark.asyncio
    async def test_finds_the_holes_between_our_lowest_and_highest_id(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert(conn, 1, 4, 9, 10)

            assert await store.find_order_id_gaps() == [2, 3, 5, 6, 7, 8]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_dense_sequence_has_nothing_to_do(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert(conn, 1, 2, 3)

            assert await store.find_order_id_gaps() == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_empty_table_does_not_explode(self, tmp_path):
        store = await _store(tmp_path)
        try:
            assert await store.find_order_id_gaps() == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_batch_is_bounded(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert(conn, 1, 1000)

            gaps = await store.find_order_id_gaps(limit=5)
            assert gaps == [2, 3, 4, 5, 6]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_ids_absent_upstream_are_not_offered_again(self, tmp_path):
        """Otherwise the job asks KeyCRM for the same 404s every hour."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert(conn, 1, 5)
            assert await store.find_order_id_gaps() == [2, 3, 4]

            await store.record_backfill_misses({3: "not found in KeyCRM"})

            assert await store.find_order_id_gaps() == [2, 4]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_list_drains_to_empty(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert(conn, 1, 5)
                _insert(conn, 2, 3, 4)  # repaired
            assert await store.find_order_id_gaps() == []
        finally:
            await store.close()


class TestRecordBackfillMisses:
    @pytest.mark.asyncio
    async def test_records_and_is_idempotent(self, tmp_path):
        store = await _store(tmp_path)
        try:
            assert await store.record_backfill_misses({7: "not found"}) == 1
            assert await store.record_backfill_misses({7: "not found again"}) == 1

            async with store.connection() as conn:
                rows = conn.execute(
                    "SELECT order_id, reason FROM order_backfill_misses"
                ).fetchall()
            assert rows == [(7, "not found again")]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_nothing_to_record_is_a_no_op(self, tmp_path):
        store = await _store(tmp_path)
        try:
            assert await store.record_backfill_misses({}) == 0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_long_reason_is_truncated_not_rejected(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await store.record_backfill_misses({1: "x" * 500})

            async with store.connection() as conn:
                reason = conn.execute(
                    "SELECT reason FROM order_backfill_misses WHERE order_id = 1"
                ).fetchone()[0]
            assert len(reason) == 200
        finally:
            await store.close()
