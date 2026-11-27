"""`GET /api/health` publishes the derivation's dropped marks, over a real Postgres.

A mark that could not be raised leaves its rows landed and nothing owed; the
loss shows only in `meta.mirror_state`'s row for `meta.derivation_signal`,
which a validated rebuild heals (DN-05b). The canary, in the other container,
judges `marks_dropped_unhealed` with `last_mark_drop_age_s`. Beside them,
`signal_unreadable_ticks` is this process's count of ticks that could not read
the signal (DN-05a), published even when Postgres cannot be read, because that
is when it is not zero.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from tests.helpers.app_client import ConnectionPerAcquire
from tests.integration.pg_tab_seed import DSN, needs_postgres

asyncpg = pytest.importorskip("asyncpg")

pytestmark = needs_postgres

SIGNAL = "meta.derivation_signal"
# Nothing listens on port 1: every connection is refused.
UNREADABLE = "postgresql://ks_app:x@127.0.0.1:1/ks"

# `(failures_since_ok, seconds since last_attempted_at, seconds since last_ok_at)`
DROPPED = (3, 80 * 60, None)
HEALED = (0, 5 * 3600, 5 * 3600)


@pytest.fixture
def fresh(monkeypatch):
    """Every cached block of /api/health empty, the rate limiter reset, and
    the process's signal-read count put back after the test."""
    from core import pg_derivation
    from web.ratelimit import limiter
    from web.routes.api import health

    for name in ("_mirror_cache", "_journal_ages_cache", "_marks_cache"):
        monkeypatch.setattr(health, name, {"data": None, "expires_at": 0})
    monkeypatch.setattr(health, "_buyer_watermark_cache", {"age": None, "expires_at": 0.0})
    for name in ("_mirror_cache_lock", "_journal_ages_lock", "_marks_cache_lock"):
        monkeypatch.setattr(health, name, asyncio.Lock())
    monkeypatch.setattr(pg_derivation, "_signal_failures", 0)
    limiter.reset()
    yield
    limiter.reset()


@pytest_asyncio.fixture
async def signal_row():
    """Writes the signal's row in meta.mirror_state, or removes it; whatever
    stood there before is put back."""
    conn = await asyncpg.connect(DSN)
    try:
        kept = await conn.fetchrow("SELECT * FROM meta.mirror_state WHERE table_name = $1", SIGNAL)
    finally:
        await conn.close()

    async def write(row: Optional[Tuple[int, int, Optional[int]]]) -> None:
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", SIGNAL)
            if row is not None:
                failures, attempted_ago, ok_ago = row
                now = datetime.now(timezone.utc)
                await conn.execute(
                    "INSERT INTO meta.mirror_state"
                    " (table_name, last_attempted_at, last_ok_at, failures_since_ok, last_error)"
                    " VALUES ($1, $2, $3, $4, $5)",
                    SIGNAL, now - timedelta(seconds=attempted_ago),
                    None if ok_ago is None else now - timedelta(seconds=ok_ago), failures,
                    "LockNotAvailableError: simulated" if failures else None)
        finally:
            await conn.close()

    yield write
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", SIGNAL)
        if kept is not None:
            columns = list(kept.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                *kept.values())
    finally:
        await conn.close()


class TestTheDerivationBlock:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("row, dsn, count, age", [
        pytest.param(DROPPED, DSN, 3, 80 * 60, id="three drops, the latest 80 minutes ago"),
        # A heal moves `last_attempted_at` too; read as a drop, it would age
        # into a page an hour after every heal.
        pytest.param(HEALED, DSN, 0, None, id="healed five hours ago: no age"),
        pytest.param(None, DSN, 0, None, id="never dropped a mark: no row, zero"),
        pytest.param(DROPPED, UNREADABLE, None, None, id="unreadable: null, never zero"),
    ])
    async def test_it_is_read_from_the_signals_watermark(
            self, fresh, signal_row, row, dsn, count, age):
        from fastapi.testclient import TestClient

        from core import pg_derivation
        from web.main import app

        await signal_row(row)
        pg_derivation.note_signal_unreadable()
        pg_derivation.note_signal_unreadable()

        with patch("core.pg.get_pool", new=AsyncMock(return_value=ConnectionPerAcquire(dsn))):
            response = await asyncio.to_thread(TestClient(app).get, "/api/health")

        assert response.status_code == 200
        block = response.json()["derivation"]
        assert block["marks_dropped_unhealed"] == count
        if age is None:
            assert block["last_mark_drop_age_s"] is None
        else:
            assert age <= block["last_mark_drop_age_s"] <= age + 60
        assert block["signal_unreadable_ticks"] == 2
