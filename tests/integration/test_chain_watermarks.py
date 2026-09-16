"""Chain 1's watermarks against a real Postgres (revision 0032).

Through the store's own `get_last_sync_time`/`set_last_sync_time` — the calls
the sync makes — on a DuckDBStore whose `connection` raises where a key must not
reach DuckDB. Then the case that decided the design: the hourly replace of
`app.sync_metadata` runs for real, and the watermark is still there.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM meta.chain_watermarks")


async def _stored(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM meta.chain_watermarks")
    return {r["key"]: r["value"] for r in rows}


@pytest_asyncio.fixture
async def pg_only(monkeypatch):
    from core.duckdb_store import DuckDBStore

    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
        monkeypatch.delenv(env, raising=False)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    await _clean(pool)
    store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("reached DuckDB")

    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()), \
         patch.object(DuckDBStore, "connection", _no_duckdb):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()


class TestTheStoreRoutesThroughTheRegistry:
    @pytest.mark.asyncio
    async def test_under_the_flag_what_is_set_is_what_is_read(self, pg_only):
        store, pool, env = pg_only
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        stamp = datetime(2026, 9, 16, 21, 5, 7, 123456, tzinfo=timezone.utc)

        await store.set_last_sync_time("offers", stamp)

        assert await store.get_last_sync_time("offers") == stamp
        assert set(await _stored(pool)) == {"last_sync_offers"}

    @pytest.mark.asyncio
    async def test_a_key_postgres_never_received_is_due(self, pg_only):
        """None is what makes the first tick after a switch sync at once —
        never DuckDB's frozen value standing in for it."""
        store, _pool, env = pg_only
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        assert await store.get_last_sync_time("stocks") is None

    @pytest.mark.asyncio
    async def test_a_second_set_moves_it(self, pg_only):
        store, _pool, env = pg_only
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        first = datetime(2026, 9, 16, 10, 0, tzinfo=timezone.utc)
        await store.set_last_sync_time("stocks", first)
        await store.set_last_sync_time("stocks", first + timedelta(hours=1))
        assert await store.get_last_sync_time("stocks") == first + timedelta(hours=1)

    @pytest.mark.asyncio
    async def test_under_the_flag_a_key_the_chain_does_not_own_stays_on_duckdb(
        self, pg_only,
    ):
        store, pool, env = pg_only
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        with pytest.raises(AssertionError, match="reached DuckDB"):
            await store.set_last_sync_time("orders", datetime.now(timezone.utc))
        with pytest.raises(AssertionError, match="reached DuckDB"):
            await store.get_last_sync_time("orders")
        assert await _stored(pool) == {}

    @pytest.mark.asyncio
    async def test_without_the_flag_the_chain_keys_stay_on_duckdb(self, pg_only):
        store, pool, _env = pg_only
        with pytest.raises(AssertionError, match="reached DuckDB"):
            await store.set_last_sync_time("offers", datetime.now(timezone.utc))
        with pytest.raises(AssertionError, match="reached DuckDB"):
            await store.get_last_sync_time("offers")
        assert await _stored(pool) == {}


class TestTheHourlyReplaceCannotRollItBack:
    """`app.sync_metadata` is full-replaced out of DuckDB every hour. The first
    port of chain 1 kept its watermarks there, so the replace put DuckDB's
    frozen stamp back each hour. The run must have no `error`, and the frozen
    stamp must be seen landing in `app.sync_metadata` — that is what shows the
    replace really ran, so the survival is the design working and not a run
    that quietly did nothing."""

    @pytest_asyncio.fixture
    async def both_stores(self, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore

        for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setenv("KS_PG_DSN", DSN)
        store = DuckDBStore(db_path=tmp_path / "frozen.duckdb")
        await store.connect()
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
        await _clean(pool)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool, monkeypatch
        await _clean(pool)
        await pool.close()
        await store.close()

    @pytest.mark.asyncio
    async def test_the_watermark_survives_and_the_freshness_check_is_quiet(
        self, both_stores,
    ):
        from core.data_quality import _freshness_check
        from core.pg_chain_watermarks import read_values
        from core.pg_operational import replicate_operational
        from core.write_chains import stood_down_sync_keys

        store, pool, env = both_stores
        now = datetime.now(timezone.utc)
        frozen = now - timedelta(hours=60)
        fresh = now - timedelta(minutes=10)

        # Before the switch: DuckDB owns both keys, and they stop here.
        await store.set_last_sync_time("offers", frozen)
        await store.set_last_sync_time("stocks", frozen)

        env.setenv("KS_WRITE_INVENTORY", "postgres")
        await store.set_last_sync_time("offers", fresh)
        await store.set_last_sync_time("stocks", fresh)

        result = await replicate_operational(store)

        assert "error" not in result, result
        async with pool.acquire() as conn:
            copied = await conn.fetchval(
                "SELECT value FROM app.sync_metadata WHERE key = 'last_sync_offers'")
        assert copied == frozen.isoformat(), "the replace did not run"
        assert await store.get_last_sync_time("offers") == fresh
        assert await store.get_last_sync_time("stocks") == fresh

        watermarks = await read_values(stood_down_sync_keys())
        async with store.connection() as conn:
            issues = _freshness_check(conn, now, chain_watermarks=watermarks)
        assert not [i for i in issues
                    if i.check_name in ("freshness_offers", "freshness_stocks")], issues
