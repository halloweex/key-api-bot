"""The soak no longer excuses a stall inside a job that no longer runs (L4-10).

D1 and D3 read a derivation stall as UNKNOWN, not FAIL, while a job holding
`_heavy_job_lock` is in its window. Two of those windows had stopped
existing: the DuckDB backup (04:30–05:00 Kyiv, `db_backup` not registered
since step 14's S14-00) and the host's Sunday compaction (02:00–03:30 UTC,
its cron line retired on 2026-10-16, its script deleted in S14-18). A stall
inside either read UNKNOWN every day and every Sunday. The UTM gap check
excused its Sunday window the same way. All three judge it now.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from tests.integration.test_daily_soak_sql import (  # noqa: F401 — `pool` is a fixture
    KYIV, needs_pg, no_runs, pool, run, scenario, signal, verdict,
)

D1 = "02_d1_owed_rebuild.sql"
D3 = "04_d3_derivation_gaps.sql"

# A Tuesday, so no training (Mon/Thu); 04:40 is the retired backup's window.
TUESDAY_0440 = datetime(2030, 11, 5, 4, 40, tzinfo=KYIV)
# A Sunday in winter: 02:15 UTC is 04:15 Kyiv, past the weekly full sync and
# before the status refresh — the retired compaction's window alone.
SUNDAY_0215_UTC = datetime(2030, 11, 3, 4, 15, tzinfo=KYIV)


@needs_pg
class TestD1:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("now", [TUESDAY_0440, SUNDAY_0215_UTC],
                             ids=["backup-window", "compaction-window"])
    async def test_owed_for_twenty_minutes_is_a_fail(self, pool, now):  # noqa: F811
        """Mutation: put the two VALUES rows back — UNKNOWN naming them."""
        async with scenario(pool) as conn:
            await signal(conn, requested=5, built=3,
                         requested_at=now - timedelta(minutes=20),
                         built_at=now - timedelta(minutes=30))
            v, detail = await verdict(conn, D1, now=now)
        assert v == "FAIL", detail

    @pytest.mark.asyncio
    async def test_a_live_window_still_excuses_it(self, pool):  # noqa: F811
        refresh = datetime(2030, 11, 5, 5, 25, tzinfo=KYIV)
        async with scenario(pool) as conn:
            await signal(conn, requested=5, built=3,
                         requested_at=refresh - timedelta(minutes=20),
                         built_at=refresh - timedelta(minutes=30))
            v, detail = await verdict(conn, D1, now=refresh)
        assert v == "UNKNOWN" and "order status refresh" in detail, detail


@needs_pg
class TestD3:
    @pytest.mark.asyncio
    async def test_a_gap_over_the_retired_backup_is_not_explained_by_it(self, pool):  # noqa: F811
        """Runs at 04:05 and 05:10 Kyiv on a Tuesday: 65 min, overlapping only
        the retired backup's window. It explained the gap (PASS) until L4-10."""
        later = datetime(2030, 11, 5, 9, 0, tzinfo=KYIV)
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=datetime(2030, 11, 5, 4, 5, tzinfo=KYIV))
            await run(conn, trigger="heartbeat", started_at=datetime(2030, 11, 5, 5, 10, tzinfo=KYIV))
            v, detail = await verdict(conn, D3, now=later)
        assert v == "UNKNOWN" and "DuckDB backup" not in detail, detail


@needs_pg
class TestTheUtmGap:
    @pytest.mark.asyncio
    async def test_a_missing_verdict_on_sunday_night_is_a_fail(self, pool):  # noqa: F811
        """Mutation: put the compaction CTE back — UNKNOWN, "re-run after 03:30"."""
        oid = 990_000_051
        async with scenario(pool) as conn:
            await conn.execute("DELETE FROM silver.order_utm WHERE order_id = $1", oid)
            await conn.execute(
                "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                "manager_comment, mirrored_at) VALUES ($1, 4, 1, 100, 'utm_source=ig', $2)",
                oid, SUNDAY_0215_UTC - timedelta(hours=1))
            v, detail = await verdict(conn, "11_utm_gap.sql", now=SUNDAY_0215_UTC)
        assert v == "FAIL" and "compaction" not in detail, detail
