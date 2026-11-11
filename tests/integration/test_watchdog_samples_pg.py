"""Chain 10 against a real Postgres: the samples written and read there.

- one tick puts the rows Postgres is handed in its tables, column for column
  as DuckDB's shadow held them (step 14's rule 9: the literals were taken
  from DuckDB's rows on the base `e3112a7d`, the last tree with a shadow);
- the differencing reads are Postgres's own, in the tick's transaction — a
  history only Postgres holds is found, which is the chain map's warning met;
- every prune runs in the tick, at the cutoff the door computed;
- the statements' own semantics — the sample nearest an age inside its
  slack, one directory set never mixed with another run's, the peak over a
  window, retention — proved on Postgres, the one engine that runs them.
  Until S14-10 these were proved on DuckDB alone (`tests/unit/
  test_disk_monitor.py`, `test_memory_monitor.py`), through the DuckDB
  writers and readers S14-10 deleted; the texts are the same, and the cases
  are those tests' cases;
- the standing watch reads the spans with its real SQL.

The way back — a near no-op copy that refused nothing for a lagging prune —
went with the copy-back in step 14's S14-04; the shadow and its daily
comparison with S14-10.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_watchdog_write as chain, watchdog_samples

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLES = chain.CHAIN_TABLES
MB = 1024 * 1024
MEM = {"working_set": 600 * MB, "page_cache": 100 * MB, "limit": 1024 * MB,
       "oom_kills": 0}


async def _clean(pool):
    async with pool.acquire() as conn:
        for t in TABLES:
            await conn.execute(f"DELETE FROM {t}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def pool(monkeypatch):
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


def _sample(at, db_size_mb=2_000.25, pct=41.5, free=99.75):
    return {"sampled_at": at, "db_size_mb": db_size_mb, "disk_pct_used": pct,
            "disk_free_gb": free, "disk_used_bytes": 1}


def _mem(working_mb, oom=0):
    return {"working_set": int(working_mb * MB), "page_cache": 100 * MB,
            "limit": 1024 * MB, "oom_kills": oom}


COLS = {
    "app.disk_samples": "sampled_at, db_size_mb, disk_pct_used, disk_free_gb",
    "app.data_dir_samples": "sampled_at, path_group, bytes",
    "app.memory_samples": "sampled_at, working_set_mb, page_cache_mb, limit_mb, oom_kills",
}

# DuckDB's rows after the same two ticks on the base `e3112a7d` (the shadow
# under `KS_WRITE_WATCHDOGS=postgres`), each timestamp read as "now" (the
# sample's own `sampled_at`) or "tick" (the instant the door took), sorted.
DUCKDB_ROWS_ONE_TICK = {
    "app.disk_samples": [["now", 2000.25, 41.5, 99.75]],
    "app.data_dir_samples": [["tick", "duckdb", 10], ["tick", "unattributed", 20]],
    "app.memory_samples": [["tick", 600.0, 100.0, 1024.0, 0]],
}
# And after a tick that found one row past retention in each table.
DUCKDB_ROWS_AFTER_PRUNE = {
    "app.disk_samples": [["now", 2000.25, 41.5, 99.75]],
    "app.data_dir_samples": [["tick", "duckdb", 3]],
    "app.memory_samples": [["tick", 600.0, 100.0, 1024.0, 0]],
}


async def _rows(pool, now, before, after):
    out = {}
    async with pool.acquire() as conn:
        for table, cols in COLS.items():
            got = sorted(tuple(r) for r in await conn.fetch(f"SELECT {cols} FROM {table}"))
            out[table] = [[("now" if v == now else "tick" if before <= v <= after else v)
                           if isinstance(v, datetime) else v for v in r] for r in got]
    return out


class TestOneTick:
    @pytest.mark.asyncio
    async def test_the_rows_land_as_duckdbs_did(self, pool):
        now = datetime.now(timezone.utc)
        before = datetime.now(timezone.utc)
        await watchdog_samples.disk_tick(_sample(now), {"duckdb": 10, "unattributed": 20})
        await watchdog_samples.memory_tick(MEM)
        after = datetime.now(timezone.utc)
        assert await _rows(pool, now, before, after) == DUCKDB_ROWS_ONE_TICK
        assert chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_the_history_is_postgres_own(self, pool):
        """A 24 h-old sample and a 168 h-old directory set that only Postgres
        holds — the history copied there before the flip — are what the tick
        reads. Mutation: read anywhere else — the watchdog would report no
        growth at all."""
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
        disk = await watchdog_samples.disk_tick(_sample(now), {"duckdb": 10, "unattributed": 9})
        mem = await watchdog_samples.memory_tick(MEM)
        assert disk["history"]["db_size_mb"] == 1500
        assert disk["dir_week_ago"] == {"duckdb": 5, "unattributed": 7}
        assert [b for _, b in disk["remainder"]] == [7, 9]
        assert mem["last"]["oom_kills"] == 2
        assert mem["peak_24h"] == pytest.approx(600.0)

    @pytest.mark.asyncio
    async def test_every_table_prunes_in_the_tick(self, pool):
        """Every table: a row past its retention is gone, and what remains is
        what DuckDB's shadow held. Mutation: drop any one of the three prunes
        from the Postgres tick — the memory prune's absence survived every
        other test here, since nothing else seeds an old memory sample."""
        now = datetime.now(timezone.utc)
        old = {"app.disk_samples": now - timedelta(days=15),
               "app.data_dir_samples": now - timedelta(days=22),
               "app.memory_samples": now - timedelta(days=15)}
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO app.disk_samples VALUES ($1, 1, 1, 1)",
                               old["app.disk_samples"])
            await conn.execute("INSERT INTO app.data_dir_samples VALUES ($1, 'duckdb', 1)",
                               old["app.data_dir_samples"])
            await conn.execute("INSERT INTO app.memory_samples VALUES ($1, 1, 1, 1, 0)",
                               old["app.memory_samples"])
        before = datetime.now(timezone.utc)
        reads = await watchdog_samples.disk_tick(_sample(now), {"duckdb": 3})
        await watchdog_samples.memory_tick(MEM)
        after = datetime.now(timezone.utc)
        assert reads["deleted"] == 1
        assert await _rows(pool, now, before, after) == DUCKDB_ROWS_AFTER_PRUNE


async def _seed_disk(pool, *rows):
    async with pool.acquire() as conn:
        for at, size in rows:
            await conn.execute(
                "INSERT INTO app.disk_samples VALUES ($1, $2, 25.0, 55.0)", at, size)


async def _seed_dir(pool, at, totals):
    async with pool.acquire() as conn:
        for group, size in totals.items():
            await conn.execute(
                "INSERT INTO app.data_dir_samples VALUES ($1, $2, $3)", at, group, size)


async def _seed_memory(pool, *rows):
    async with pool.acquire() as conn:
        for at, working_mb, oom in rows:
            await conn.execute(
                "INSERT INTO app.memory_samples VALUES ($1, $2, 100, 1024, $3)",
                at, working_mb, oom)


class TestTheDiskStatements:
    """The sample nearest 24 h ago, inside ±2 h, and the prune at 14 days —
    `test_disk_monitor.py`'s `TestPersistence` cases, on Postgres."""

    @pytest.mark.asyncio
    async def test_the_sample_from_24h_ago_is_found(self, pool):
        now = datetime.now(timezone.utc)
        await _seed_disk(pool, (now - timedelta(hours=24), 7_000))
        reads = await watchdog_samples.disk_tick(_sample(now), {})
        assert reads["history"]["db_size_mb"] == 7_000

    @pytest.mark.asyncio
    async def test_no_sample_in_the_window_is_no_history(self, pool):
        now = datetime.now(timezone.utc)
        await _seed_disk(pool, (now - timedelta(hours=1), 7_000))
        assert (await watchdog_samples.disk_tick(_sample(now), {}))["history"] is None

    @pytest.mark.asyncio
    async def test_a_sample_inside_the_slack_counts(self, pool):
        """A sample taken 22 h ago still counts for "24 h ago"."""
        now = datetime.now(timezone.utc)
        await _seed_disk(pool, (now - timedelta(hours=22), 6_500))
        reads = await watchdog_samples.disk_tick(_sample(now), {})
        assert reads["history"]["db_size_mb"] == 6_500

    @pytest.mark.asyncio
    async def test_the_closest_of_two_in_the_window_wins(self, pool):
        """Mutation: order by anything but the distance to the target."""
        now = datetime.now(timezone.utc)
        await _seed_disk(pool, (now - timedelta(hours=22, minutes=30), 1),
                         (now - timedelta(hours=24, minutes=10), 2))
        reads = await watchdog_samples.disk_tick(_sample(now), {})
        assert reads["history"]["db_size_mb"] == 2

    @pytest.mark.asyncio
    async def test_the_prune_keeps_fourteen_days(self, pool):
        now = datetime.now(timezone.utc)
        await _seed_disk(pool, (now - timedelta(days=20), 1), (now - timedelta(days=5), 2))
        reads = await watchdog_samples.disk_tick(_sample(now), {})
        assert reads["deleted"] == 1
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT COUNT(*) FROM app.disk_samples") == 2


class TestTheDirectoryStatements:
    """`test_disk_monitor.py`'s `TestDirSampling` cases, on Postgres."""

    @pytest.mark.asyncio
    async def test_a_sample_set_round_trips(self, pool):
        now = datetime.now(timezone.utc)
        totals = {"live_db": 2_791_728_742, "backups": 5_000_000_000, "other": 7}
        await _seed_dir(pool, now - timedelta(hours=168), totals)
        reads = await watchdog_samples.disk_tick(_sample(now), None)
        assert reads["dir_week_ago"] == totals

    @pytest.mark.asyncio
    async def test_groups_never_come_from_two_different_runs(self, pool):
        """Differencing a mix of timestamps would invent growth out of
        nothing: the read pins one `sampled_at` and returns only that set."""
        now = datetime.now(timezone.utc)
        await _seed_dir(pool, now - timedelta(hours=170), {"live_db": 1, "backups": 2})
        await _seed_dir(pool, now - timedelta(hours=166), {"live_db": 999})
        fetched = (await watchdog_samples.disk_tick(_sample(now), None))["dir_week_ago"]
        assert fetched in ({"live_db": 1, "backups": 2}, {"live_db": 999})
        assert not (fetched.get("live_db") == 999 and "backups" in fetched)

    @pytest.mark.asyncio
    async def test_retention_outlives_the_comparison_window(self, pool):
        """Pruning at 14 days would delete the baseline a 168 h lag needs on a
        scheduler that missed a few runs; 21 days leaves slack. The prune runs
        only on a tick with a directory set to store."""
        now = datetime.now(timezone.utc)
        await _seed_dir(pool, now - timedelta(days=25), {"live_db": 1})
        await _seed_dir(pool, now - timedelta(days=8), {"live_db": 2})
        await watchdog_samples.disk_tick(_sample(now), {"live_db": 3})
        async with pool.acquire() as conn:
            kept = [r[0] for r in await conn.fetch(
                "SELECT bytes FROM app.data_dir_samples ORDER BY sampled_at")]
        assert kept == [2, 3]


class TestTheMemoryStatements:
    """`test_memory_monitor.py`'s `TestPersistence` cases, on Postgres: the
    kernel forgets on recreate, and this table is what remembers."""

    @pytest.mark.asyncio
    async def test_the_last_sample_is_the_newest_and_an_empty_table_has_none(self, pool):
        assert (await watchdog_samples.memory_tick(_mem(650)))["last"] is None
        last = (await watchdog_samples.memory_tick(_mem(700, oom=1)))["last"]
        assert last["working_set_mb"] == pytest.approx(650, abs=1)
        assert last["oom_kills"] == 0

    @pytest.mark.asyncio
    async def test_an_oom_kill_survives_the_restart_that_reset_the_counter(self, pool):
        """A kill recorded, the container recreated and the kernel counter
        back at zero: the stored value is what the next sample is compared
        against."""
        from core.memory_monitor import evaluate_memory

        await watchdog_samples.memory_tick(_mem(500, oom=0))
        previous = (await watchdog_samples.memory_tick(_mem(500, oom=1)))["last"]["oom_kills"]
        alert = evaluate_memory(working_set_bytes=500 * MB, page_cache_bytes=0,
                                limit_bytes=1024 * MB, oom_kills=1,
                                previous_oom_kills=previous)
        assert alert is not None and alert.oom_kills_delta == 1

    @pytest.mark.asyncio
    async def test_the_peak_is_the_max_over_the_window(self, pool):
        now = datetime.now(timezone.utc)
        await _seed_memory(pool, (now - timedelta(hours=20), 500, 0),
                           (now - timedelta(hours=10), 5_300, 0))
        assert (await watchdog_samples.memory_tick(_mem(650)))["peak_24h"] == \
            pytest.approx(5_300, abs=1)

    @pytest.mark.asyncio
    async def test_the_peak_ignores_samples_outside_the_window(self, pool):
        now = datetime.now(timezone.utc)
        await _seed_memory(pool, (now - timedelta(days=3), 6_000, 0))
        assert (await watchdog_samples.memory_tick(_mem(650)))["peak_24h"] == \
            pytest.approx(650, abs=1)

    @pytest.mark.asyncio
    async def test_the_prune_keeps_fourteen_days(self, pool):
        now = datetime.now(timezone.utc)
        await _seed_memory(pool, (now - timedelta(days=20), 500, 0),
                           (now - timedelta(days=5), 650, 0))
        await watchdog_samples.memory_tick(_mem(700))
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT COUNT(*) FROM app.memory_samples") == 2


class TestTheStandingWatch:
    @pytest.mark.asyncio
    async def test_the_spans_are_read(self, pool):
        from core import pg_chain_invariants as inv

        now = datetime.now(timezone.utc)
        await watchdog_samples.memory_tick(MEM)
        facts = await inv.read_facts(pool=pool)
        spans = {t: (n, o) for t, n, o in facts.watchdogs.spans}
        assert spans["app.disk_samples"] == (None, None)
        assert abs((spans["app.memory_samples"][0] - now).total_seconds()) < 60
        names = {(i.check_name, i.table_name) for i in inv.check_chain_invariants(facts)}
        assert (inv.SAMPLES_STALE, "app.disk_samples") in names
        assert (inv.SAMPLES_STALE, "app.memory_samples") not in names
