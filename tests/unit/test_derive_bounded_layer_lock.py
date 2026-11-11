"""The derivation waits for the layer lock a bounded time (L2-11).

The derivation runs under the scheduler's `_heavy_job_lock` and waited for
`PG_LAYER_LOCK` with no bound. That lock's other holders — the hourly
ClickHouse ship and the daily read-back — hold it across HTTP round trips of
up to 30 s each, so a slow ClickHouse held the heavy lock too, and the minute
sync, which waits for the heavy lock, stopped taking orders. The wait is
`DERIVE_LOCK_WAIT_S` now: past it the tick returns, nothing is journalled,
the signal stays owed, and the heavy lock is free.
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

import core.scheduler as sched_mod
from core.pg_silver import PG_LAYER_LOCK
from core.scheduler import BackgroundScheduler, _acquire_within


def _scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    scheduler._heavy_job_lock = asyncio.Lock()
    return scheduler


@pytest.fixture
def held_elsewhere():
    """`PG_LAYER_LOCK` held by another task — the ClickHouse ship."""
    async def hold(release: asyncio.Event):
        async with PG_LAYER_LOCK:
            await release.wait()
    return hold


class TestTheTick:
    @pytest.mark.asyncio
    async def test_it_returns_within_the_bound_owed_and_the_heavy_lock_free(
            self, monkeypatch, held_elsewhere):
        """Mutation: `async with PG_LAYER_LOCK` again — the tick waits until
        the holder lets go and the heavy lock is held all that time."""
        monkeypatch.setattr(sched_mod, "DERIVE_LOCK_WAIT_S", 0.2)
        release = asyncio.Event()
        holder = asyncio.create_task(held_elsewhere(release))
        await asyncio.sleep(0)
        scheduler = _scheduler()
        rebuild = AsyncMock()
        parse = AsyncMock()
        with patch("core.pg_silver.rebuild_silver", new=rebuild), \
             patch.object(BackgroundScheduler, "_parse_order_utm_in_postgres", new=parse):
            started = time.monotonic()
            async with scheduler._heavy_job_lock:
                result = await scheduler._derive_pg_layers(object(), "test")
            elapsed = time.monotonic() - started

        assert result["skipped"] and result["reason"] == "layer_lock_busy"
        assert elapsed < 1.0
        assert not scheduler._heavy_job_lock.locked()
        rebuild.assert_not_awaited()          # nothing read: the signal stays owed
        parse.assert_not_awaited()            # the parse would wait for the same lock
        assert PG_LAYER_LOCK.locked()         # still the holder's
        release.set()
        await holder
        assert not PG_LAYER_LOCK.locked()


class TestTheAcquire:
    @pytest.mark.asyncio
    async def test_a_free_lock_is_taken(self):
        lock = asyncio.Lock()
        assert await _acquire_within(lock, 0.1) and lock.locked()
        lock.release()

    @pytest.mark.asyncio
    async def test_a_timeout_never_leaves_the_lock_held(self):
        """The waiter that gave up must not win the lock later and keep it."""
        lock = asyncio.Lock()
        await lock.acquire()
        assert not await _acquire_within(lock, 0.05)
        lock.release()
        await asyncio.sleep(0.01)
        assert not lock.locked()

    @pytest.mark.asyncio
    async def test_a_cancelled_waiter_never_leaves_it_held(self):
        lock = asyncio.Lock()
        await lock.acquire()
        waiter = asyncio.create_task(_acquire_within(lock, 5))
        await asyncio.sleep(0.01)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        lock.release()
        await asyncio.sleep(0.01)
        assert not lock.locked()
