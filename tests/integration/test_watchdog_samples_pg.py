"""Chain 10 against a real Postgres: the samples written there, shadowed in DuckDB.

- one tick puts the same rows in both stores, column for column, and prunes
  both at the same instant;
- the differencing reads are Postgres's own, in the tick's transaction — a
  history only Postgres holds is found, which is the chain map's warning met;
- the daily comparison is silent on an honest shadow, names a failed one, and
  reads a prune DuckDB has not caught up with as INFO;
- the standing watch reads the spans with its real SQL;
- the way back is a near no-op copy that refuses nothing for a lagging prune.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_watchdog_write as chain, shadow_writes, watchdog_samples

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLES = chain.CHAIN_TABLES
MEM = {"working_set": 600 * 1024 * 1024, "page_cache": 100 * 1024 * 1024,
       "limit": 1024 * 1024 * 1024, "oom_kills": 0}


async def _clean(pool):
    async with pool.acquire() as conn:
        for t in TABLES:
            await conn.execute(f"DELETE FROM {t}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core import write_chains
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    shadow_writes.reset()
    store = DuckDBStore(db_path=tmp_path / "samples.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()
    shadow_writes.reset()


def _sample(at):
    return {"sampled_at": at, "db_size_mb": 2_000.25, "disk_pct_used": 41.5,
            "disk_free_gb": 99.75, "disk_used_bytes": 1}


def _utc(row):
    return tuple(v.astimezone(timezone.utc) if isinstance(v, datetime) else v
                 for v in row)


async def _rows(store, pool, table, cols):
    async with pool.acquire() as conn:
        pg = sorted(_utc(tuple(r)) for r in await conn.fetch(f"SELECT {cols} FROM {table}"))
    async with store.connection() as conn:
        dk = sorted(_utc(r) for r in conn.execute(
            f"SELECT {cols} FROM {table.split('.', 1)[1]}").fetchall())
    return pg, dk


COLS = {
    "app.disk_samples": "sampled_at, db_size_mb, disk_pct_used, disk_free_gb",
    "app.data_dir_samples": "sampled_at, path_group, bytes",
    "app.memory_samples": "sampled_at, working_set_mb, page_cache_mb, limit_mb, oom_kills",
}


class TestOneTickTwoStores:
    @pytest.mark.asyncio
    async def test_the_same_rows_land_in_both(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        now = datetime.now(timezone.utc)
        await watchdog_samples.disk_tick(store, _sample(now), {"duckdb": 10, "unattributed": 20})
        await watchdog_samples.memory_tick(store, MEM)
        for table, cols in COLS.items():
            pg, dk = await _rows(store, pool, table, cols)
            assert pg and pg == dk, table
        assert shadow_writes.failures_of(chain.CHAIN)["count"] == 0
        assert chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_the_history_is_postgres_own(self, stores):
        """A 24 h-old sample and a 168 h-old directory set that only Postgres
        holds — the history copied there before the flip — are what the tick
        reads. Mutation: read DuckDB's — the first ticks after a flip would
        report no growth at all."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        now = datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.disk_samples VALUES ($1, 1500, 40, 100)",
                now - timedelta(hours=24))
            await conn.executemany(
                "INSERT INTO app.data_dir_samples VALUES ($1, $2, $3)",
                [(now - timedelta(hours=168), "duckdb", 5),
                 (now - timedelta(hours=168), "unattributed", 7)])
            await conn.execute(
                "INSERT INTO app.memory_samples VALUES ($1, 500, 100, 1024, 2)",
                now - timedelta(minutes=30))
        disk = await watchdog_samples.disk_tick(store, _sample(now), {"duckdb": 10, "unattributed": 9})
        mem = await watchdog_samples.memory_tick(store, MEM)
        assert disk["history"]["db_size_mb"] == 1500
        assert disk["dir_week_ago"] == {"duckdb": 5, "unattributed": 7}
        assert [b for _, b in disk["remainder"]] == [7, 9]
        assert mem["last"]["oom_kills"] == 2
        assert mem["peak_24h"] == pytest.approx(600.0)

    @pytest.mark.asyncio
    async def test_both_stores_prune_at_one_instant(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=15)
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO app.disk_samples VALUES ($1, 1, 1, 1)", old)
        async with store.connection() as conn:
            conn.execute("INSERT INTO disk_samples VALUES (?, 1, 1, 1)", [old])
        reads = await watchdog_samples.disk_tick(store, _sample(now), {})
        assert reads["deleted"] == 1
        pg, dk = await _rows(store, pool, "app.disk_samples", COLS["app.disk_samples"])
        assert pg == dk and len(pg) == 1


class TestTheShadowIsCompared:
    @pytest.mark.asyncio
    async def test_honest_failed_and_lagging(self, stores):
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        old = datetime.now(timezone.utc) - timedelta(hours=5)
        await watchdog_samples.disk_tick(store, _sample(old), {"duckdb": 1})
        honest = [i for i in await reconcile_operational(store) if i.table_name in TABLES]
        assert honest == [], honest

        # A shadow that failed: the sample is in Postgres only.
        async with store.connection() as conn:
            conn.execute("DELETE FROM disk_samples")
        found = {(i.check_name, i.table_name) for i in await reconcile_operational(store)
                 if i.table_name in TABLES}
        assert found == {("shadow_missing_in_duckdb", "app.disk_samples")}

        # A DuckDB prune that lagged: DuckDB holds a sample older than
        # anything Postgres still holds. Retention, not a defect.
        async with store.connection() as conn:
            conn.execute("INSERT INTO disk_samples VALUES (?, 2000.25, 41.5, 99.75)", [old])
            conn.execute("INSERT INTO disk_samples VALUES (?, 1, 1, 1)",
                         [old - timedelta(days=15)])
        found = {(i.check_name, i.severity.value) for i in await reconcile_operational(store)
                 if i.table_name in TABLES}
        assert found == {("shadow_pruned_rows", "INFO")}


class TestTheStandingWatch:
    @pytest.mark.asyncio
    async def test_the_spans_are_read(self, stores):
        from core import pg_chain_invariants as inv

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        now = datetime.now(timezone.utc)
        await watchdog_samples.memory_tick(store, MEM)
        facts = await inv.read_facts(pool=pool)
        spans = {t: (n, o) for t, n, o in facts.watchdogs.spans}
        assert spans["app.disk_samples"] == (None, None)
        assert abs((spans["app.memory_samples"][0] - now).total_seconds()) < 60
        names = {(i.check_name, i.table_name) for i in inv.check_chain_invariants(facts)}
        assert (inv.SAMPLES_STALE, "app.disk_samples") in names
        assert (inv.SAMPLES_STALE, "app.memory_samples") not in names


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_a_lagging_prune_does_not_refuse_and_the_copy_releases(self, stores):
        from core.chain_transfer import copy_back, handover_check

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        now = datetime.now(timezone.utc)
        await watchdog_samples.disk_tick(store, _sample(now - timedelta(hours=3)), {"duckdb": 1})
        await watchdog_samples.memory_tick(store, MEM)
        async with store.connection() as conn:
            conn.execute("INSERT INTO disk_samples VALUES (?, 1, 1, 1)",
                         [now - timedelta(days=16)])
        issues = await handover_check(store, chain)
        assert {(i.check_name, i.severity.value) for i in issues} == {
            ("handover_rows_pruned", "INFO")}, issues
        result = await copy_back(store, chain, dry_run=False)
        assert result["findings"] == [] and result["released"] is True, result
        for table, cols in COLS.items():
            pg, dk = await _rows(store, pool, table, cols)
            assert pg == dk, table
