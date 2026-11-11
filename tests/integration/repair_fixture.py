"""Shared set-up for the repair jobs' tests (A15): a pool standing in for
`core.pg.get_pool`, an order payload, and cleanup of the ids a test owns."""
from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")


def payload(oid: int, stamp: datetime, *, status: int = 12, products=None) -> dict:
    return {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": stamp.isoformat(),
        "created_at": stamp.isoformat(), "updated_at": stamp.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}] if products is None else products,
    }


async def clean(pool, ids):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.expenses WHERE order_id = ANY($1::int[])", ids)
        await conn.execute("DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", ids)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", ids)
        await conn.execute("DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", ids)
        await conn.execute("DELETE FROM app.order_backfill_misses WHERE order_id = ANY($1::bigint[])", ids)


def pool_fixture(ids):
    @pytest_asyncio.fixture
    async def pool():
        p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
        await clean(p, ids)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield p
        await clean(p, ids)
        await p.close()
    return pool


async def misses(pool, ids) -> dict:
    async with pool.acquire() as conn:
        return {int(r["order_id"]): r["reason"] for r in await conn.fetch(
            "SELECT order_id, reason FROM app.order_backfill_misses "
            "WHERE order_id = ANY($1::bigint[])", ids)}
