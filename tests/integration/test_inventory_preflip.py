"""Chain 1's pre-flip hardening (DN-24) against a real PostgreSQL.

KS_WRITE_INVENTORY stays off in production; these are the three gaps that
would bite the day it is switched on:

* the 01:00 snapshot job takes no heavy lock, so two Postgres rebuilds can
  interleave — the second one's INSERT then collides with the first one's rows
  and the whole rebuild raises;
* a missing TEMPORARY grant makes every status rebuild raise, and nothing said
  so before the flip;
* the preflight has to read the replication and the journal as they really are.

The writers are called directly here, not through the repository: the property
under test belongs to the Postgres transaction, and the route to it is pinned
by `tests/integration/test_chain_latch.py`.

Skipped without `KS_PG_DSN`, like every store test; the TEMPORARY case also
needs `KS_PG_READONLY_DSN`, which CI provisions.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import pg_inventory_write

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# A day nothing else in the suite photographs, so the rows are ours to delete.
DAY = date(2031, 1, 7)


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.sku_inventory_status")
        await conn.execute("DELETE FROM bronze.offer_stocks")
        await conn.execute("DELETE FROM bronze.offers")
        await conn.execute("DELETE FROM app.inventory_sku_history WHERE date = $1", DAY)
        await conn.execute("DELETE FROM app.inventory_history WHERE date = $1", DAY)
        await conn.execute("DELETE FROM meta.chain_watermarks")


@pytest_asyncio.fixture
async def pool():
    """A pool wide enough for two writers and a lock holder, standing in for
    `core.pg.get_pool` so the writers use it."""
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(p)
    async with p.acquire() as conn:
        await conn.executemany(
            "INSERT INTO bronze.offers (id, product_id, sku, synced_at) "
            "VALUES ($1, $2, $3, now())",
            [(9_910_001, 9_910_101, "DN24-1"), (9_910_002, 9_910_102, "DN24-2"),
             (9_910_003, 9_910_103, "DN24-3")])
        await conn.executemany(
            "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price, "
            "quantity, reserve, mirrored_at) VALUES ($1, $2, 100, 50, $3, 0, now())",
            [(9_910_001, "DN24-1", 5), (9_910_002, "DN24-2", 0), (9_910_003, "DN24-3", 9)])
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield p
    await _clean(p)
    await p.close()


class TestTheChainLock:
    @pytest.mark.asyncio
    async def test_two_concurrent_rebuilds_both_commit(self, pool, monkeypatch):
        """The stock step and the 01:00 job arriving together. A pause between
        the DELETE and the INSERT makes the overlap certain rather than lucky:
        without the lock the second rebuild's DELETE sees none of the first's
        new rows, and its INSERT then dies on `offer_id`."""
        body = pg_inventory_write.sku_status_rebuild_select
        monkeypatch.setattr(
            pg_inventory_write, "sku_status_rebuild_select",
            lambda dialect: "SELECT pg_sleep(0.4); " + body(dialect))

        first, second = await asyncio.gather(
            pg_inventory_write.rebuild_sku_inventory_status(),
            pg_inventory_write.rebuild_sku_inventory_status(),
        )

        assert first == second == 3
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT COUNT(*) FROM app.sku_inventory_status") == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call", [
        pytest.param(lambda: pg_inventory_write.rebuild_sku_inventory_status(),
                     id="rebuild_sku_inventory_status"),
        pytest.param(lambda: pg_inventory_write.record_sku_inventory_snapshot(DAY),
                     id="record_sku_inventory_snapshot"),
        pytest.param(lambda: pg_inventory_write.record_inventory_snapshot(today=DAY),
                     id="record_inventory_snapshot"),
    ])
    async def test_each_one_waits_for_the_lock_and_then_commits(self, pool, call):
        """Deterministic, one writer at a time: while another transaction holds
        the chain-1 key the writer cannot finish, and it finishes the moment
        that transaction does."""
        holder = await pool.acquire()
        try:
            tx = holder.transaction()
            await tx.start()
            await holder.execute("SELECT pg_advisory_xact_lock($1)",
                                 pg_inventory_write.CHAIN_LOCK_KEY)
            task = asyncio.ensure_future(call())
            done, _ = await asyncio.wait({task}, timeout=0.5)
            assert not done, "the writer finished while another held the chain lock"
            await tx.commit()
            assert await asyncio.wait_for(task, timeout=10)
        finally:
            await pool.release(holder)

    @pytest.mark.asyncio
    async def test_the_second_snapshot_of_the_day_waits_and_finds_it_taken(self, pool):
        await pg_inventory_write.rebuild_sku_inventory_status()   # three SKUs to photograph
        first, second = await asyncio.gather(
            pg_inventory_write.record_sku_inventory_snapshot(DAY),
            pg_inventory_write.record_sku_inventory_snapshot(DAY),
        )
        assert sorted([first, second]) == [False, True]
