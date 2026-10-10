"""Stage 5 against a real Postgres: what `KS_DUCKDB=off` reads from the frozen
copy of DuckDB's `sync_metadata` instead of the file.

The unit tests (`tests/unit/test_warehouse_writer_off.py`) hand the readers a
connection that answers what the test says. These ask a Postgres at head the
same statements, on the tables the hourly copy writes — so a statement that
names a column the schema does not have, or a type the driver hands back
differently, fails here rather than on the morning the week starts.
"""
from __future__ import annotations

import json
import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

RESOLVED = {"writer": "postgres", "since": "2026-10-01T06:00:00+00:00",
            "resolved": True, "resolved_at": "2026-10-01T06:00:01+00:00"}


@pytest_asyncio.fixture
async def pool():
    """A pool on the throwaway Postgres, with `app.sync_metadata`'s
    `warehouse_writer` row put back as it was found."""
    from core import warehouse_cutover as wc

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        held = await conn.fetchrow(
            "SELECT value, updated_at FROM app.sync_metadata WHERE key = $1",
            wc.WRITER_KEY)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                               wc.WRITER_KEY)
            if held is not None:
                await conn.execute(
                    "INSERT INTO app.sync_metadata (key, value, updated_at) "
                    "VALUES ($1, $2, $3)", wc.WRITER_KEY, held["value"],
                    held["updated_at"])
        await pool.close()


async def _seed_writer(pool, value):
    from core import warehouse_cutover as wc

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO app.sync_metadata (key, value, updated_at) "
            "VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            wc.WRITER_KEY, value)
        return await conn.fetchrow(
            "SELECT value, updated_at FROM app.sync_metadata WHERE key = $1",
            wc.WRITER_KEY)


class TestTheWriterRecordFromTheFrozenCopy:
    @pytest.mark.asyncio
    async def test_the_copy_reads_as_the_record(self, pool):
        from core import warehouse_cutover as wc

        await _seed_writer(pool, json.dumps(RESOLVED))
        assert await wc.read_frozen_writer() == RESOLVED
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                               wc.WRITER_KEY)
        assert await wc.read_frozen_writer() is None

    @pytest.mark.asyncio
    async def test_a_settle_under_off_reads_it_and_writes_nothing(
        self, pool, duckdb_off, monkeypatch,
    ):
        """Through the application's own reader — the revision check, the
        pool, the statement — and the row left exactly as it was."""
        from core import duckdb_switch, warehouse_cutover as wc

        before = await _seed_writer(pool, json.dumps(RESOLVED))
        monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
        resolve = AsyncMock(return_value=0)
        with patch("core.alerting.resolve_group", resolve):
            await wc.settle_writer(None)
        async with pool.acquire() as conn:
            after = await conn.fetchrow(
                "SELECT value, updated_at FROM app.sync_metadata WHERE key = $1",
                wc.WRITER_KEY)
        assert after == before
        assert wc._settled and wc.status()["writer"] == RESOLVED
        resolve.assert_not_awaited()
        assert duckdb_switch.opened() == {} and not duckdb_off.exists()
