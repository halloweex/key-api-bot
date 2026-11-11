"""Finding missing orders without asking the API.

KeyCRM issues order ids as a dense sequence, so every hole between our lowest
and highest id is an order we never stored. 1 660 holes existed as of 2026-08,
and a full-history comparison confirmed 1 616 of them were real orders — the
other 44 are gone upstream and must be remembered, or the job asks for them
again every hour forever.

Proved on Postgres since step 14's S14-09, which deleted the DuckDB bodies
these tests used to pin: `find_order_id_gaps` is `pg_orders_read.order_id_gaps`
and `record_backfill_misses` is chain 3's writer of `app.order_backfill_misses`
(`tests/pg_orders_seed.py`).
"""
import pytest

from tests.pg_orders_seed import insert_orders, needs_pg, pg_orders  # noqa: F401 — the fixture

pytestmark = needs_pg


async def _misses(pool):
    async with pool.acquire() as conn:
        return [(r["order_id"], r["reason"]) for r in await conn.fetch(
            "SELECT order_id, reason FROM app.order_backfill_misses ORDER BY order_id")]


class TestFindOrderIdGaps:
    @pytest.mark.asyncio
    async def test_finds_the_holes_between_our_lowest_and_highest_id(self, pg_orders):
        store, pool = pg_orders
        await insert_orders(pool, 1, 4, 9, 10)
        assert await store.find_order_id_gaps() == [2, 3, 5, 6, 7, 8]

    @pytest.mark.asyncio
    async def test_a_dense_sequence_has_nothing_to_do(self, pg_orders):
        store, pool = pg_orders
        await insert_orders(pool, 1, 2, 3)
        assert await store.find_order_id_gaps() == []

    @pytest.mark.asyncio
    async def test_an_empty_table_does_not_explode(self, pg_orders):
        store, _pool = pg_orders
        assert await store.find_order_id_gaps() == []

    @pytest.mark.asyncio
    async def test_the_batch_is_bounded(self, pg_orders):
        store, pool = pg_orders
        await insert_orders(pool, 1, 1000)
        assert await store.find_order_id_gaps(limit=5) == [2, 3, 4, 5, 6]

    @pytest.mark.asyncio
    async def test_ids_absent_upstream_are_not_offered_again(self, pg_orders):
        """Otherwise the job asks KeyCRM for the same 404s every hour."""
        store, pool = pg_orders
        await insert_orders(pool, 1, 5)
        assert await store.find_order_id_gaps() == [2, 3, 4]

        await store.record_backfill_misses({3: "not found in KeyCRM"})

        assert await store.find_order_id_gaps() == [2, 4]

    @pytest.mark.asyncio
    async def test_the_list_drains_to_empty(self, pg_orders):
        store, pool = pg_orders
        await insert_orders(pool, 1, 5)
        await insert_orders(pool, 2, 3, 4)  # repaired
        assert await store.find_order_id_gaps() == []


class TestRecordBackfillMisses:
    @pytest.mark.asyncio
    async def test_records_and_is_idempotent(self, pg_orders):
        store, pool = pg_orders
        assert await store.record_backfill_misses({7: "not found"}) == 1
        assert await store.record_backfill_misses({7: "not found again"}) == 1
        assert await _misses(pool) == [(7, "not found again")]

    @pytest.mark.asyncio
    async def test_nothing_to_record_is_a_no_op(self, pg_orders):
        store, pool = pg_orders
        assert await store.record_backfill_misses({}) == 0
        assert await _misses(pool) == []

    @pytest.mark.asyncio
    async def test_a_long_reason_is_truncated_not_rejected(self, pg_orders):
        store, pool = pg_orders
        await store.record_backfill_misses({1: "x" * 500})
        ((_oid, reason),) = await _misses(pool)
        assert len(reason) == 200
