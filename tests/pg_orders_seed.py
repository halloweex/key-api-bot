"""A live Postgres holding chain 3's tables, for the unit tests whose subject
is a selection or a repair over the orders — never collected on its own.

Until step 14's S14-09 these tests pinned the selections' semantics on
DuckDB's implementation (the gap scan, the half-written repair's selection,
the backdated orders, the misses ledger), with the chain routed back to
DuckDB for the test. S14-09 deleted those bodies: the store's methods are
`core.pg_orders_read`'s and `core.pg_orders_write`'s, so the same semantics
are proved here on Postgres.

`pg_orders` hands a test the store as web holds it — `KS_DUCKDB=off`, never
connected, its DuckDB connection a trap — and the pool every chain-3 read and
write is served. The four chain-3 tables, the archive and the owner rows a
write claims are emptied before and after: the gap scan reads MIN and MAX
over the whole of `bronze.orders`, so a row another test left would be a
hole of its own.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")
needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLES = ("bronze.order_products", "bronze.expenses", "bronze.orders",
          "app.order_backfill_misses", "app.order_versions")
WHEN = datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc)


async def clean(pool) -> None:
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            ["bronze.orders", "bronze.order_products", "bronze.expenses"])


async def insert_orders(pool, *ids, total=100.0, ordered_at=WHEN, created_at=None,
                        lines=0) -> None:
    """Headers as a test needs them — `lines` line items each — written as
    SQL, not through the chain: the subject is what reads them."""
    async with pool.acquire() as conn:
        for oid in ids:
            await conn.execute(
                "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                "ordered_at, created_at, updated_at) VALUES ($1, 1, 12, $2, $3, $4, $3)",
                oid, total, ordered_at, created_at or ordered_at)
            for i in range(lines):
                await conn.execute(
                    "INSERT INTO bronze.order_products (id, order_id, product_id, name, "
                    "quantity, price_sold) VALUES ($1, $2, 700, 'Line', 1, 50)",
                    oid * 1000 + i, oid)


@pytest_asyncio.fixture
async def pg_orders(monkeypatch, tmp_path):
    """`(store, pool)`: the store web holds, and chain 3's tables, empty."""
    import asyncpg

    from core.store import AnalyticsStore

    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = AnalyticsStore()
    monkeypatch.setattr(store, "connection", MagicMock(
        side_effect=AssertionError("chain 3's selections read DuckDB")), raising=False)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await clean(pool)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
                patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool
    finally:
        await clean(pool)
        await pool.close()
