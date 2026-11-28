"""The integrity job judges sync freshness from what Postgres holds.

`_run_dq_integrity` reads the sync watermarks a chain took to Postgres
(`meta.chain_watermarks`) and DuckDB's frozen `app.sync_metadata` rows, hands
both to the freshness check and journals the findings. Dropped from the job,
a stalled sync pages nobody (or the check says it is "unwatched" instead of
judging it). Run through the real job over the test Postgres; the journalled
run is what is read back.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import pg_dq_journal_write
from core.scheduler import BackgroundScheduler
from core.write_chains import sync_keys
from tests.helpers.dq_journal_pg import pool  # noqa: F401  (the journal's fixture)
from tests.integration.pg_tab_seed import needs_postgres

pytestmark = needs_postgres

STALE = timedelta(hours=60)    # past every threshold (orders 6 h, the rest 48 h+)
FRESH = timedelta(minutes=1)
LIVE = "meta.chain_watermarks"
FROZEN = "app.sync_metadata"


@pytest_asyncio.fixture
async def watermarks(pool):  # noqa: F811
    """`last_sync_*` rows of both tables emptied for the test, put back after."""
    kept = {}
    async with pool.acquire() as conn:
        for table in (LIVE, FROZEN):
            kept[table] = await conn.fetch(
                f"SELECT key, value, updated_at FROM {table} WHERE key LIKE 'last_sync_%'")
            await conn.execute(f"DELETE FROM {table} WHERE key LIKE 'last_sync_%'")
    try:
        yield pool
    finally:
        async with pool.acquire() as conn:
            for table, rows in kept.items():
                await conn.execute(f"DELETE FROM {table} WHERE key LIKE 'last_sync_%'")
                for r in rows:
                    await conn.execute(
                        f"INSERT INTO {table} (key, value, updated_at) VALUES ($1, $2, $3)",
                        r["key"], r["value"], r["updated_at"])


async def _seed(pool, *, frozen_age, live_age=None):
    """Every moved sync `FRESH` in `meta.chain_watermarks` (`live_age` names
    exceptions) except `orders`, absent there and held only by the frozen
    copy, `frozen_age` old."""
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        for key in sorted(sync_keys()):
            if key == "last_sync_orders":
                continue
            then = now - (live_age or {}).get(key, FRESH)
            await conn.execute(
                f"INSERT INTO {LIVE} (key, value, updated_at) VALUES ($1, $2, $3)",
                key, then.isoformat(), then)
        then = now - frozen_age
        await conn.execute(
            f"INSERT INTO {FROZEN} (key, value, updated_at) VALUES ($1, $2, $3)",
            "last_sync_orders", then.isoformat(), then)


async def _findings(monkeypatch) -> set:
    """The check names the job journalled for one run."""
    scheduler = BackgroundScheduler()
    monkeypatch.setattr(scheduler, "_send_dq_alert_throttled", AsyncMock(return_value=True))
    with patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
        result = await scheduler._run_dq_integrity()
    async with pg_dq_journal_write.reading() as conn:
        return {r["check_name"] for r in
                await pg_dq_journal_write.run_issues(conn, result["run_id"])}


class TestTheJobJudgesWhatPostgresHolds:
    @pytest.mark.asyncio
    async def test_a_stalled_moved_sync_is_a_finding(self, watermarks, monkeypatch):
        """Chain 1's offers stalled in `meta.chain_watermarks`: the run files
        `freshness_offers` and nothing for the syncs that are fresh."""
        await _seed(watermarks, frozen_age=FRESH, live_age={"last_sync_offers": STALE})
        found = await _findings(monkeypatch)
        assert "freshness_offers" in found
        assert "sync_watermarks_unwatched" not in found
        assert not {"freshness_products", "freshness_stocks", "freshness_orders"} & found

    @pytest.mark.asyncio
    async def test_fresh_watermarks_file_no_freshness_finding(self, watermarks, monkeypatch):
        await _seed(watermarks, frozen_age=FRESH)
        found = await _findings(monkeypatch)
        assert not {c for c in found if c.startswith("freshness_")}
        assert "sync_watermarks_unwatched" not in found

    @pytest.mark.asyncio
    async def test_orders_absent_from_postgres_are_judged_by_the_frozen_copy(
            self, watermarks, monkeypatch):
        """`last_sync_orders` only in `app.sync_metadata`, 60 h old: the job
        hands the copy to the check, so the order sync is judged stale rather
        than reported unwatched."""
        await _seed(watermarks, frozen_age=STALE)
        found = await _findings(monkeypatch)
        assert "freshness_orders" in found
        assert "sync_watermarks_unwatched" not in found
