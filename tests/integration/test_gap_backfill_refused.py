"""An order Postgres refuses is ledgered, so the gap scan moves on (L1-10).

The gap backfill fetched each hole by id and ledgered only "not found". A
hole KeyCRM served with a value Postgres refuses (a NUL in its comment) was
refused by the writer, counted nowhere (`failed` 0) and never ledgered, so
the scan offered the same ids on every run and asked KeyCRM for them again.
`repair_orders` reports the writer's refusals as failures now ("Postgres
refused: …"), and the job ledgers them like an id KeyCRM does not have.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from tests.integration.repair_fixture import DSN, misses, payload, pool_fixture

pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

IDS = [990_961, 990_962]
pool = pool_fixture(IDS)


async def _run():
    import core.sync_service as ss
    from core.scheduler import BackgroundScheduler
    from core.store import get_store

    stamp = datetime.now(timezone.utc) - timedelta(days=400)

    async def get_order(order_id, include=None):
        p = payload(order_id, stamp)
        p["manager_comment"] = "utm_source=ig\x00"
        return p

    client = type("Client", (), {"get_order": staticmethod(get_order)})()
    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    scheduler._heavy_job_lock = asyncio.Lock()
    store = await get_store()
    with patch.object(ss, "get_async_client", new=AsyncMock(return_value=client)), \
         patch.object(type(store), "find_order_id_gaps", new=AsyncMock(return_value=list(IDS))):
        return await scheduler._run_order_gap_backfill()


@pytest.mark.asyncio
async def test_a_refused_hole_is_counted_and_ledgered(pool):
    """Mutation: ledger "not found" alone again — `absent_upstream` 0 and no
    miss, so the next scan offers the same ids."""
    out = await _run()
    assert out["repaired"] == 0 and out["failed"] == 2
    assert out["absent_upstream"] == 2
    ledgered = await misses(pool, IDS)
    assert sorted(ledgered) == IDS
    assert all(why.startswith("Postgres refused: ") for why in ledgered.values())


@pytest.mark.asyncio
async def test_the_scan_does_not_offer_them_again(pool):
    from core import pg_orders_read

    await _run()
    async with pool.acquire() as conn:
        # A neighbour each side, so both ids are holes the scan could offer.
        await conn.execute(
            "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, ordered_at) "
            "VALUES ($1, 1, 1, 1, now()), ($2, 1, 1, 1, now())", IDS[0] - 1, IDS[1] + 1)
    try:
        offered = await pg_orders_read.order_id_gaps(limit=100_000)
        assert not set(IDS) & set(offered)
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])",
                               [IDS[0] - 1, IDS[1] + 1])
