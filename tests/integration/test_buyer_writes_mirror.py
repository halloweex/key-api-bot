"""A buyer changed by `sync-all-buyers` reaches Postgres (chain 4, PR-1, step 3).

`sync-all-buyers` used to write DuckDB alone. Every buyer it changed then
differed between the stores for good: the hourly ids-diff ships only ids
Postgres lacks, and the comparison filed `mirror_row_values` CRITICAL for it
every morning with nothing that could repair it.

The route writes through `DuckDBStore.upsert_buyers`, which now mirrors each
portion itself, so this drives exactly that call against a real DuckDB file and
a real PostgreSQL, then runs the real `reconcile_buyers` past its 15-minute
grace — inside the grace, any difference is forgiven and the check proves
nothing. The control is the old path, the DuckDB-only portion write: it must
still produce the CRITICAL, or this test is not measuring the mirror.

Since step 14's S14-00 chain 4 writes Postgres whatever its flag says, so
`upsert_buyers` reaches Postgres through the chain, the comparison stands
down for its tables, and the control — a DuckDB write the comparison catches
— went with the DuckDB path. The comparison itself (`reconcile_buyers`) went
in step 14's S14-12; what stays is the write it watched.
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.store import AnalyticsStore
from core.models import Buyer

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

STATES = ("bronze.buyers", "bronze.buyer_contacts")


def _buyer(name, phones):
    return Buyer.from_api({"id": 9_500_001, "full_name": name, "phone": phones,
                           "created_at": "2026-09-01 10:00:00+00:00"})


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = AnalyticsStore()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        before = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(STATES))
        # The comparison reads whole tables; the precedent in this suite
        # (test_buyer_sync_two_engines) empties them around the test.
        await conn.execute("DELETE FROM bronze.buyer_contacts")
        await conn.execute("DELETE FROM bronze.buyers")
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.buyer_contacts")
            await conn.execute("DELETE FROM bronze.buyers")
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                list(STATES))
            for row in before:
                cols = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(cols) + 1))})",
                    *row.values())
        await pool.close()


@pytest.mark.asyncio
async def test_a_changed_buyer_reaches_postgres(stores):
    store, pool = stores
    await store.upsert_buyers([_buyer("Олена Стара", ["+380500000001", "+380500000002"])])
    await store.upsert_buyers([_buyer("Олена Нова", ["+380500000003"])])   # sync-all's call

    async with pool.acquire() as conn:
        name = await conn.fetchval("SELECT full_name FROM bronze.buyers WHERE id = 9500001")
        phones = [r["value"] for r in await conn.fetch(
            "SELECT value FROM bronze.buyer_contacts WHERE buyer_id = 9500001")]
    assert name == "Олена Нова"
    assert phones == ["+380500000003"], "a shrunk contact list kept an old number"


