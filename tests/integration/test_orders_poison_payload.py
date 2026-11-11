"""One malformed KeyCRM order costs that order, never the order intake (L1-01).

`Order.from_api` calls `float()` and `int()` on the payload with no guard, so
one order KeyCRM served with a line item's `price_sold` null raised a
`TypeError` out of the parse, before any refusal ran. The order step failed
on every tick, `last_sync_orders` stayed where it was, and the window
(watermark minus 24 h) fetched the same page again a minute later: every
order behind the bad one waited for somebody to read the log.

Now each payload is parsed on its own (`core.landing_rows.landed_orders`),
an unreadable one is refused by id with its expenses, counted in `failed` and
listed in `UpsertResult.refused`, and the rest of the page lands. Driven here
through `SyncService.incremental_sync`, the tick production runs, against a
real Postgres.

A window in which the parse could read nothing is the other case (AR-02): no
order written, none unchanged, and the parse refused some. That is a shape
KeyCRM changed for every order, or a parser defect, not one bad order, and
reported as a success it moved the watermark past every order and kept
`orders_sync_failing` quiet. The writer still raises nothing — the repairs
ledger what it refused — and the order step fails on it (`OrdersRefused`).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 990_700
GOOD, NO_PRICE, BAD_TOTAL, NO_QTY = BASE, BASE + 1, BASE + 2, BASE + 3
IDS = [GOOD, NO_PRICE, BAD_TOTAL, NO_QTY]


def _payload(oid: int, updated: datetime) -> dict:
    return {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": updated.isoformat(),
        "created_at": updated.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
        "expenses": [{"id": oid * 10, "expense_type_id": 1, "amount": 10,
                      "status": "paid"}],
    }


def _page(now: datetime) -> list:
    good = _payload(GOOD, now - timedelta(minutes=4))
    no_price = _payload(NO_PRICE, now - timedelta(minutes=3))
    no_price["products"][0]["price_sold"] = None
    bad_total = _payload(BAD_TOTAL, now - timedelta(minutes=2))
    bad_total["grand_total"] = "abc"
    no_qty = _payload(NO_QTY, now - timedelta(minutes=1))
    no_qty["products"][0]["quantity"] = None
    return [good, no_price, bad_total, no_qty]


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.expenses WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)


@pytest_asyncio.fixture
async def pool():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        saved = await conn.fetch(
            "SELECT * FROM meta.chain_watermarks WHERE key = 'last_sync_orders'")
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key = 'last_sync_orders'")
        for row in saved:
            cols = list(row.keys())
            await conn.execute(
                f"INSERT INTO meta.chain_watermarks ({', '.join(cols)}) VALUES "
                f"({', '.join(f'${i + 1}' for i in range(len(cols)))})", *row.values())
    await pool.close()


async def _held(pool) -> list:
    async with pool.acquire() as conn:
        return sorted(r["id"] for r in await conn.fetch(
            "SELECT id FROM bronze.orders WHERE id = ANY($1::int[])", IDS))


async def _expense_orders(pool) -> list:
    async with pool.acquire() as conn:
        return sorted(r["order_id"] for r in await conn.fetch(
            "SELECT order_id FROM bronze.expenses WHERE order_id = ANY($1::int[])", IDS))


class TestTheWriterRefusesOnlyTheBadOrders:
    @pytest.mark.asyncio
    async def test_the_good_order_lands_and_the_bad_ones_are_counted(self, pool):
        result, expense_count = await chain.upsert_orders_with_expenses(
            _page(datetime.now(timezone.utc)))
        assert result.changed_ids == [GOOD]
        assert result.failed == 3
        assert sorted(oid for oid, _ in result.refused) == [NO_PRICE, BAD_TOTAL, NO_QTY]
        assert all(why.startswith("unparseable: ") for _, why in result.refused)
        assert await _held(pool) == [GOOD]
        # A refused order's expenses go with it.
        assert expense_count == 1
        assert await _expense_orders(pool) == [GOOD]

    @pytest.mark.asyncio
    async def test_a_page_of_nothing_but_bad_orders_raises_nothing(self, pool):
        """The writer's answer, which the repairs ledger by id; the order step
        is what fails on it (`TestAWindowNothingCouldRead`)."""
        result, expense_count = await chain.upsert_orders_with_expenses(
            _page(datetime.now(timezone.utc))[1:])
        assert (result.count, result.changed_ids, result.failed, expense_count) == (
            0, [], 3, 0)
        assert result.unparseable == 3
        assert await _held(pool) == []


class TestTheOrderStepGoesOn:
    @pytest.mark.asyncio
    async def test_three_ticks_land_the_good_order_and_move_the_watermark(self, pool):
        import core.sync_service as ss
        from core.keycrm import KeyCRMClient
        from core.store import get_store

        now = datetime.now(timezone.utc)
        page = _page(now)

        class Fake(KeyCRMClient):
            async def _request(self, method, endpoint, params=None, json=None):
                params = params or {}
                if endpoint == "order" and params.get("page") == 1:
                    return {"data": [dict(p) for p in page]}
                return {"data": []}

        client = Fake(api_key="test-only")
        store = await get_store()
        before = now - timedelta(minutes=30)
        await store.set_last_sync_time("orders", before)
        svc = ss.SyncService(store)
        with patch.object(ss, "get_async_client", new=AsyncMock(return_value=client)), \
             patch.object(ss.SyncService, "_catalogue_step_postgres", new=AsyncMock()), \
             patch.object(ss.SyncService, "_managers_step_postgres",
                          new=AsyncMock(return_value=0)), \
             patch.object(ss.SyncService, "_buyers_retry_in", return_value=60), \
             patch.object(ss.SyncService, "_inventory_step_postgres", new=AsyncMock()):
            for _ in range(3):
                svc._last_sync_time = None       # no adaptive skip
                await svc.incremental_sync()
                step = svc.orders_step.published()
                assert step["consecutive_failures"] == 0, step
                assert step["last_error_class"] is None
                assert step["last_refused"] == 3
        assert await _held(pool) == [GOOD]
        moved = await store.get_last_sync_time("orders")
        assert moved is not None and moved > before


class TestAWindowNothingCouldRead:
    @pytest.mark.asyncio
    async def test_three_ticks_fail_hold_the_watermark_and_page(self, pool):
        """The reviewer's reproduction (AR-02): every order's `price_sold`
        null. Mutation: drop the `OrdersRefused` raise — no failure, the
        watermark moves, and the canary pages nothing."""
        import core.sync_service as ss
        from bot.canary import check_orders_sync_chain
        from core.keycrm import KeyCRMClient
        from core.store import get_store

        now = datetime.now(timezone.utc)
        page = _page(now)[1:]
        for p in page:
            p["products"][0]["price_sold"] = None

        class Fake(KeyCRMClient):
            async def _request(self, method, endpoint, params=None, json=None):
                params = params or {}
                if endpoint == "order" and params.get("page") == 1:
                    return {"data": [dict(p, products=[dict(x) for x in p["products"]])
                                     for p in page]}
                return {"data": []}

        client = Fake(api_key="test-only")
        store = await get_store()
        before = (now - timedelta(minutes=30)).replace(microsecond=0)
        await store.set_last_sync_time("orders", before)
        svc = ss.SyncService(store)
        recorded = AsyncMock()
        with patch.object(ss, "get_async_client", new=AsyncMock(return_value=client)), \
             patch.object(ss.SyncService, "_catalogue_step_postgres", new=AsyncMock()), \
             patch.object(ss.SyncService, "_managers_step_postgres",
                          new=AsyncMock(return_value=0)), \
             patch.object(ss.SyncService, "_buyers_retry_in", return_value=60), \
             patch.object(ss.SyncService, "_inventory_step_postgres", new=AsyncMock()), \
             patch.object(ss.SyncService, "_record_orders_failure", new=recorded):
            for _ in range(3):
                svc._last_sync_time = None       # no adaptive skip
                await svc.incremental_sync()
        step = svc.orders_step.published()
        assert (step["consecutive_failures"], step["last_error_class"]) == (3, "OrdersRefused")
        assert recorded.await_count == 3
        assert await _held(pool) == []
        assert await store.get_last_sync_time("orders") == before
        pages = check_orders_sync_chain(
            {"write_chains": {"pg_orders_write": {"sync_step": step}}})
        assert [key for key, _ in pages] == ["orders_sync_failing"]
