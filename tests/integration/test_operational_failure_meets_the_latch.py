"""A failed hourly copy while a chain is held down by its owner rows alone.

DN-06 stands the shipper down on `meta.chain_watermarks` owner rows even when
the local marker is gone, and stamps the chain's tables with what to do about
it. The serialised copy stamps a run's own failure on every table it was
copying. Put together, a run that fails after it has read the owner rows must
leave the owned table's watermark saying "no local marker", not a duplicate
key it never had anything to do with — and must not touch the table.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

from core import chain_latch

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

EXPENSES = "app.manual_expenses"
FIRST_ID = 970_001


def _shipped():
    from core.pg_operational import _APPEND_ABOVE, _FULL_REPLACE

    return (*(t for t, _d, _c, _o in _FULL_REPLACE), *(s.pg_table for s in _APPEND_ABOVE))


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.manual_expenses")
        await conn.execute("DELETE FROM meta.chain_watermarks")
        await conn.execute("DELETE FROM app.reconciliation_log WHERE id >= $1", FIRST_ID)
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(_shipped()))


async def _states(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT table_name, last_ok_at, failures_since_ok, last_error "
            "FROM meta.mirror_state WHERE table_name = ANY($1::text[])", list(_shipped()))
    return {r["table_name"]: dict(r) for r in rows}


def _log_row(conn, row_id):
    conn.execute(
        "INSERT INTO reconciliation_log (id, check_date, api_count, db_count, "
        "discrepancy, discrepancy_pct, status) "
        "VALUES (?, DATE '2026-09-20', 14, 14, 0, 0, 'ok')", [row_id])


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    import core.pg_operational as pg_operational
    from core.duckdb_store import DuckDBStore
    from core.write_chains import WRITE_CHAINS

    for chain in WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setattr(pg_operational, "OPERATIONAL_REPLICATION_LOCK", asyncio.Lock())
    store = DuckDBStore(db_path=tmp_path / "ops.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    async with pool.acquire() as conn:
        pg_max = await conn.fetchval("SELECT MAX(id) FROM app.reconciliation_log")
    assert pg_max is None or pg_max < FIRST_ID
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


@pytest.mark.asyncio
async def test_a_failure_after_the_owner_rows_leaves_the_owned_watermark_alone(stores):
    from core.pg_operational import replicate_operational

    store, pool, env = stores

    # Chain 8 takes ownership with a real write, then loses its marker and the
    # operator flips the flag back: the state DN-06's owner-row read exists for.
    env.setenv("KS_WRITE_EXPENSES", "postgres")
    typed = await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
    chain_latch.release("pg_expenses_write")
    env.setenv("KS_WRITE_EXPENSES", "duckdb")

    async with store.connection() as conn:
        _log_row(conn, FIRST_ID)
    healthy = await replicate_operational(store)
    assert "error" not in healthy, healthy
    assert EXPENSES in healthy["stood_down"]
    before = (await _states(pool))[EXPENSES]
    assert "no local marker" in before["last_error"]

    # Now a run that fails inside the write transaction, after the owner rows.
    async with store.connection() as conn:
        _log_row(conn, FIRST_ID + 1)
        _log_row(conn, FIRST_ID + 1)
    failed = await replicate_operational(store)
    assert "UniqueViolation" in failed["error"], failed

    states = await _states(pool)
    for table in _shipped():
        if table == EXPENSES:
            continue
        assert "duplicate key" in (states[table]["last_error"] or ""), table
    # The owned table: untouched, and its watermark still says what to do.
    assert states[EXPENSES] == before
    async with pool.acquire() as conn:
        ids = [r["id"] for r in await conn.fetch("SELECT id FROM app.manual_expenses")]
    assert ids == [typed["id"]]
