"""A repair never writes a payload older than what Postgres holds (L1-12).

`repair_orders` fetches by id and writes under force. When another writer —
the 05:15 status refresh — stored the order after the fetch began, the stale
payload (same `updated_at`, KeyCRM does not bump it on a status change) was
written over the newer status. It reads Postgres' clock before fetching now,
and under the heavy lock leaves alone any order stored after it.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from tests.integration.repair_fixture import DSN, payload, pool_fixture

pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

IDS = [990_971, 990_972]
pool = pool_fixture(IDS)


async def _status(pool, oid):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT status_id FROM bronze.orders WHERE id = $1", oid)


@pytest.mark.asyncio
async def test_an_order_rewritten_during_the_fetch_is_left_as_written(pool):
    """Mutation: drop the `rewritten_since` check — status goes back to 12."""
    import core.sync_service as ss
    from core import pg_orders_write as chain

    stamp = datetime.now(timezone.utc) - timedelta(days=40)
    await chain.upsert_orders_with_expenses([payload(oid, stamp) for oid in IDS])

    async def get_order(order_id, include=None):
        if order_id == IDS[0]:
            # The 05:15 refresh lands while this fetch is in flight.
            await chain.upsert_orders_with_expenses(
                [payload(order_id, stamp, status=19)], force_update=True)
        return payload(order_id, stamp, status=12 if order_id == IDS[0] else 13)

    client = type("Client", (), {"get_order": staticmethod(get_order)})()
    svc = ss.SyncService.__new__(ss.SyncService)
    with patch.object(ss, "get_async_client", new=AsyncMock(return_value=client)):
        result = await svc.repair_orders(IDS, lock=asyncio.Lock())

    assert result["rewritten_since_fetch"] == [IDS[0]]
    assert await _status(pool, IDS[0]) == 19            # the refresh's, kept
    assert await _status(pool, IDS[1]) == 13            # repaired as fetched
    assert result["failed"] == 0
