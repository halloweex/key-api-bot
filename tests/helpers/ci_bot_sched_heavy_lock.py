"""The scheduler's heavy-job lock held by a test while an admin's lever or a
repair job runs beside it, all on the test's own event loop.

An `asyncio.Lock` serves one loop, so the app is called through
`httpx.ASGITransport` in this loop (`TestClient` would run it on another),
and the scheduler whose lock everything takes is a fresh one, installed as
`core.scheduler.get_scheduler()`'s for the test. Its lock is a real lock that
also says when somebody has to wait for it, so a test knows the moment the
work is parked behind it instead of guessing with a sleep:

    scheduler = fresh_scheduler(monkeypatch)
    lock = scheduler._heavy_job_lock
    await lock.acquire()                      # another heavy job is running
    work = asyncio.ensure_future(admin.post("/api/sync/resync"))
    await parked_behind(lock, work)           # it waits; nothing done yet
    lock.release()
    response = await asyncio.wait_for(work, 60)
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Sequence
from unittest.mock import AsyncMock, patch

import httpx

# Bound at import: `tests.helpers.orders_sync.keycrm_serving_orders` replaces
# `httpx.AsyncClient` for the whole module, and the admin's client must keep
# the app's transport.
_AsyncClient = httpx.AsyncClient


class WatchedLock(asyncio.Lock):
    """A real lock that records when somebody has to wait for it."""

    def __init__(self) -> None:
        super().__init__()
        self.queued = asyncio.Event()

    async def acquire(self):
        if self.locked():
            self.queued.set()
        return await super().acquire()


def fresh_scheduler(monkeypatch):
    """A new, unstarted scheduler as the process's, its heavy lock watched."""
    from core import scheduler as scheduler_module
    from core.scheduler import BackgroundScheduler

    scheduler = BackgroundScheduler()
    scheduler._heavy_job_lock = WatchedLock()
    monkeypatch.setattr(scheduler_module, "_scheduler", scheduler)
    return scheduler


async def parked_behind(lock: WatchedLock, work: Optional[asyncio.Future] = None,
                        *, timeout_s: float = 15.0) -> None:
    """Return once somebody waits for `lock`, which the test holds. Fails if
    `work` finishes first, or nothing queues within `timeout_s`."""
    queued = asyncio.ensure_future(lock.queued.wait())
    waiting = {queued} | ({work} if work is not None else set())
    done, _ = await asyncio.wait(waiting, timeout=timeout_s,
                                 return_when=asyncio.FIRST_COMPLETED)
    if queued in done:
        return
    queued.cancel()
    finished = work is not None and work.done()
    raise AssertionError(
        "finished without waiting for the heavy-job lock" if finished
        else f"nothing waited for the heavy-job lock within {timeout_s:g} s")


@asynccontextmanager
async def admin_in_this_loop() -> AsyncIterator[httpx.AsyncClient]:
    """A hardcoded admin's client, the app served in this test's loop. The
    rate limiter starts and ends empty."""
    from tests.sessions import hardcoded_admin, session_cookie
    from web.main import app
    from web.ratelimit import limiter

    name, value = session_cookie(hardcoded_admin(), "admin").split("=", 1)
    limiter.reset()
    try:
        async with _AsyncClient(transport=httpx.ASGITransport(app=app),
                                base_url="http://testserver",
                                cookies={name: value}) as client:
            yield client
    finally:
        limiter.reset()


@asynccontextmanager
async def postgres_by_connection(dsn: str) -> AsyncIterator[None]:
    """`core.pg.get_pool` answering a fresh connection per acquire, on the
    test Postgres (`tests.helpers.app_client.ConnectionPerAcquire`)."""
    from tests.helpers.app_client import ConnectionPerAcquire

    with patch("core.pg.get_pool", new=AsyncMock(return_value=ConnectionPerAcquire(dsn))):
        yield


@asynccontextmanager
async def derivation_stand(monkeypatch, dsn: str, orders: Sequence[Sequence]):
    """Bronze holding `orders` and nothing derived from them, `core.pg.get_pool`
    on the test Postgres. The derivation's signal, its journal and every
    watermark are put back afterwards, and so is the process-wide state the
    scheduler keeps about it. (After `held_at_the_floor` in
    `tests/integration/test_admin_levers_reach_pg_derivation.py`.)"""
    import asyncpg

    from core import pg_derivation
    from core.scheduler import BackgroundScheduler
    from tests.integration.pg_tab_seed import seed, tab_in_postgres

    monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)
    monkeypatch.setattr(BackgroundScheduler, "_pg_derive_ran", True)
    monkeypatch.setattr(BackgroundScheduler, "_pg_derive_started", None)
    monkeypatch.setattr(pg_derivation, "_signal_failures", 0)

    conn = await asyncpg.connect(dsn)
    try:
        signal = tuple(await conn.fetchrow(
            "SELECT requested, built, requested_at, built_at FROM meta.derivation_signal"
            " WHERE layer = 'warehouse'"))
        last_run = await conn.fetchval("SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")
        stamps = await conn.fetch("SELECT * FROM meta.mirror_state")
    finally:
        await conn.close()

    async def populate(conn, _pool):
        await seed(conn, orders=orders)

    try:
        async with tab_in_postgres(dsn, monkeypatch, populate) as pool:
            yield pool
    finally:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                "UPDATE meta.derivation_signal SET requested = $1, built = $2,"
                " requested_at = $3, built_at = $4 WHERE layer = 'warehouse'", *signal)
            await conn.execute("DELETE FROM meta.derivation_runs WHERE id > $1", last_run)
            await conn.execute("DELETE FROM meta.mirror_state")
            for row in stamps:
                columns = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                    *row.values())
        finally:
            await conn.close()


__all__ = [
    "WatchedLock", "admin_in_this_loop", "derivation_stand", "fresh_scheduler",
    "parked_behind", "postgres_by_connection",
]
