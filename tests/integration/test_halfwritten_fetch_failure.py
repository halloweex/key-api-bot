"""A half-written order KeyCRM did not serve is not ledgered as empty (L1-07).

`_run_halfwritten_repair` re-fetches orders with revenue and no line items and
ledgers those still empty afterwards as "KeyCRM served no line items", which
hides them from the job for 30 days. It ledgered every candidate still empty,
including the ones whose fetch had failed: a 503 on both gave
`{'failed': 2, 'still_empty': 2}`, both ledgered, and the next selection was
empty — a KeyCRM outage cost a month of repairs. Only the orders KeyCRM
actually served are ledgered now: as served empty, or, when Postgres refused
what KeyCRM served, under the refusal — left out, such an order was offered
again every two hours for ever.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.exceptions import KeyCRMConnectionError
from tests.integration.repair_fixture import DSN, misses, payload, pool_fixture

pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

IDS = [990_951, 990_952]
pool = pool_fixture(IDS)


async def _half_written(pool):
    from core import pg_orders_write as chain

    stamp = datetime.now(timezone.utc) - timedelta(days=2)
    await chain.upsert_orders_with_expenses(
        [payload(oid, stamp, products=[]) for oid in IDS])


async def _run(get_order):
    import core.sync_service as ss
    from core.scheduler import BackgroundScheduler
    from core.store import get_store

    client = type("Client", (), {"get_order": staticmethod(get_order)})()
    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    import asyncio
    scheduler._heavy_job_lock = asyncio.Lock()
    store = await get_store()
    with patch.object(ss, "get_async_client", new=AsyncMock(return_value=client)), \
         patch.object(type(store), "find_halfwritten_orders",
                      new=AsyncMock(return_value=list(IDS))):
        return await scheduler._run_halfwritten_repair()


class TestTheLedger:
    @pytest.mark.asyncio
    async def test_a_fetch_that_failed_is_not_ledgered(self, pool):
        """Mutation: ledger `halfwritten_among(candidates)` whole again."""
        await _half_written(pool)

        async def get_order(order_id, include=None):
            raise KeyCRMConnectionError("API returned 503")

        out = await _run(get_order)
        assert out["failed"] == 2 and out["still_empty"] == 0
        assert await misses(pool, IDS) == {}

    @pytest.mark.asyncio
    async def test_an_order_kecrm_serves_empty_still_is(self, pool):
        await _half_written(pool)
        stamp = datetime.now(timezone.utc) - timedelta(days=2)

        async def get_order(order_id, include=None):
            if order_id == IDS[0]:
                raise KeyCRMConnectionError("API returned 503")
            return payload(order_id, stamp, products=[])

        out = await _run(get_order)
        assert out["failed"] == 1 and out["still_empty"] == 1
        assert list(await misses(pool, IDS)) == [IDS[1]]

    @pytest.mark.asyncio
    async def test_an_order_postgres_refuses_is_ledgered_as_refused(self, pool):
        """KeyCRM served the order, with a line Postgres refuses (a NUL in its
        name). Asked again it is refused again, so it is ledgered under the
        refusal — once — and the selection moves on. Mutation: exclude the
        refused ids from the ledger with the fetch failures — no miss, and
        the order is offered (one KeyCRM call, one ERROR) every two hours."""
        from core import pg_orders_read

        await _half_written(pool)
        stamp = datetime.now(timezone.utc) - timedelta(days=2)
        asked = []

        async def get_order(order_id, include=None):
            asked.append(order_id)
            p = payload(order_id, stamp)
            p["products"][0]["name"] = "Товар\x00"
            return p

        out = await _run(get_order)
        ledgered = await misses(pool, IDS)
        assert sorted(ledgered) == IDS
        assert all(why.startswith("Postgres refused: ") for why in ledgered.values())
        assert out["failed"] == 2 and out["still_empty"] == 0
        assert out["refused"] == 2

        # Not offered again: the real selection, not the patched one.
        offered = await pg_orders_read.halfwritten_candidates(100_000)
        assert not set(IDS) & set(offered)
        assert sorted(asked) == IDS, "asked KeyCRM more than once per order"

    @pytest.mark.asyncio
    async def test_an_order_rewritten_since_the_fetch_is_not_ledgered(self, pool):
        """Fetched, and not written: Postgres held a newer write of it. That
        is not KeyCRM saying "no line items", and ledgered as one it would hide
        the order for 30 days. Mutation: drop `rewritten_since_fetch` from the
        ids the ledger skips."""
        await _half_written(pool)
        stamp = datetime.now(timezone.utc) - timedelta(days=2)

        async def get_order(order_id, include=None):
            return payload(order_id, stamp, products=[])

        with patch("core.pg_orders_read.rewritten_since",
                   new=AsyncMock(return_value=[IDS[0]])):
            out = await _run(get_order)
        assert out["failed"] == 0 and out["still_empty"] == 1
        assert list(await misses(pool, IDS)) == [IDS[1]]
