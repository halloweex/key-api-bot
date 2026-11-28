"""The alert ledger — `app.alert_series` and `app.alert_events`, revision
0012 — on the test Postgres, for the alert tests that read back what the
archive wrote. Both tables are emptied before and after; `core.pg.get_pool`
answers the ledger's pool meanwhile, and KS_PG_DSN names the test database
(the archive stands down without it).

The archive writes a fired event in a task of its own (`record_fired`), so a
test awaits `settle()` before it reads.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, List, Tuple
from unittest.mock import AsyncMock, patch

DSN = os.getenv("KS_PG_DSN")


async def settle() -> None:
    """Wait for every other task of the running loop: the archive's
    fire-and-forget writes."""
    current = asyncio.current_task()
    pending = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
    if pending:
        await asyncio.wait(pending, timeout=10)


class Ledger:
    def __init__(self, pool) -> None:
        self.pool = pool

    async def events(self) -> List[Tuple[str, str, object]]:
        """(condition_key, event_type, delivered_to), in the order written."""
        rows = await self.pool.fetch(
            "SELECT condition_key, event_type, delivered_to FROM app.alert_events ORDER BY id")
        return [tuple(r) for r in rows]

    async def series(self) -> List[Tuple[str, str, int]]:
        """(condition_key, state, fired_count), by key."""
        rows = await self.pool.fetch(
            "SELECT condition_key, state, fired_count FROM app.alert_series ORDER BY condition_key")
        return [tuple(r) for r in rows]

    async def count(self, event_type: str) -> int:
        return await self.pool.fetchval(
            "SELECT count(*) FROM app.alert_events WHERE event_type = $1", event_type)

    async def firing(self, key: str, *, since: timedelta) -> None:
        """A condition firing, unacknowledged, since `since` ago: its series
        row and the fired event that opened the cycle."""
        first = datetime.now(timezone.utc) - since
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at,"
                " last_fired_at, fired_count, instance) VALUES ($1, 'condition', 'firing',"
                " $2, $2, 1, 'test')", key, first)
            await conn.execute(
                "INSERT INTO app.alert_events (condition_key, event_type, instance, at)"
                " VALUES ($1, 'fired', 'test', $2)", key, first)


async def _empty(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.alert_events")
        await conn.execute("DELETE FROM app.alert_series")


@asynccontextmanager
async def alert_ledger(monkeypatch) -> AsyncIterator[Ledger]:
    """The ledger, empty, for the block; empty again after it."""
    import asyncpg

    if not DSN:
        raise RuntimeError("alert_ledger needs a live PostgreSQL at KS_PG_DSN")
    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _empty(pool)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            try:
                yield Ledger(pool)
            finally:
                await settle()
    finally:
        await _empty(pool)
        await pool.close()
