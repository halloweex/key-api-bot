"""The DQ journal's Postgres fixture: a pool on the test database and a run's
arguments.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest_asyncio

from core import pg_dq_journal_write as chain
from core.data_quality import Discrepancy, DiscrepancyClass, IntegrityIssue, Severity


DSN = os.getenv("KS_PG_DSN")


TABLES = chain.CHAIN_TABLES


# Fixed, so the literals below are DuckDB's answers on this very fixture.
NOW = datetime(2026, 10, 1, 7, 30, tzinfo=timezone.utc)


DAY = NOW.date()


async def _clean(pool):
    """The journal, the chain's owner rows and the beat in both places it
    is read from — the throwaway database's, put back as the suite found it."""
    async with pool.acquire() as conn:
        for t in reversed(TABLES):
            await conn.execute(f"DELETE FROM {t}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
                           "OR key = $1", chain.DIGEST_MARKER_KEY)
        await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                           chain.DIGEST_MARKER_KEY)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


def _kwargs(layer="integrity", started=None, issues=None, diffs=None):
    started = started or NOW - timedelta(minutes=5)
    return dict(
        started_at=started, ended_at=started + timedelta(seconds=4), as_of=started,
        window_start=DAY, window_end=DAY, layer=layer,
        issues=issues if issues is not None else [
            IntegrityIssue(check_name="pk_uniqueness_orders", table_name="orders",
                           severity=Severity.WARN, count=2, sample_ids=(5, 6),
                           description="двa дублі")],
        discrepancies=diffs if diffs is not None else [
            Discrepancy(month="2026-09", source_id=4,
                        diff_class=DiscrepancyClass.MISSING_IN_DK, field="orders",
                        dk_value=1.25, kc_value=3.5, severity=Severity.CRITICAL,
                        order_ids=(77,))],
    )
