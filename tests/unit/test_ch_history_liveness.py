"""An archive copy that stopped receiving is a finding, not a PASS (L2-02).

`reconcile_ch_history` compares bucket counts below ClickHouse's own MAX(id).
A copy that stopped receiving agrees with Postgres perfectly below that
ceiling: stopped at 47 000 while Postgres held 52 448, it filed nothing and
held nothing. It now files `ch_history_stalled` from Postgres' side of the
ship — versions above the copy's ceiling captured more than 3 h ago, or the
archive's state row failing or without a success for 3 h — WARN, CRITICAL
past a day. And a Gold derivation that fails in ClickHouse is recorded
against Gold's state row, not Silver's.

Postgres is the test database: the versions in `app.order_versions`, the
ships' state rows in `meta.mirror_state`. ClickHouse is met at its HTTP
transport (`tests/helpers/recon_clickhouse.py`).
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from core import ch_history, ch_silver
from tests.helpers.recon_clickhouse import (
    CLICKHOUSE_DOWN,
    FakeClickHouse,
    archive_in_postgres,
    clickhouse_at_the_transport,
    kept_mirror_state,
    stamp,
)
from tests.integration.pg_tab_seed import DSN, needs_postgres, tab_in_postgres

# Postgres holds twelve versions; a copy that stopped holds the first eight.
HELD, STOPPED_AT = 12, 8
MIN, H = timedelta(minutes=1), timedelta(hours=1)


def _found(issues):
    return sorted((i.check_name, i.severity.value, i.count) for i in issues)


@needs_postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("copy_top, waiting_for, shipped, expected", [
    # The reproduction: four versions waiting a day and more, the ship
    # failing since. Mutation: drop the `_stall` call — no finding at all.
    pytest.param(STOPPED_AT, 30 * H, (30 * H, 25),
                 [("ch_history_stalled", "CRITICAL", 4)], id="stopped-over-a-day-ago"),
    pytest.param(STOPPED_AT, 4 * H, (30 * MIN, 0),
                 [("ch_history_stalled", "WARN", 4)], id="versions-waiting-four-hours"),
    # Versions captured since the last hourly ship wait for the next one.
    pytest.param(STOPPED_AT, 40 * MIN, (30 * MIN, 0), [],
                 id="versions-since-the-last-hourly-ship"),
    pytest.param(HELD, None, (50 * MIN, 1),
                 [("ch_history_stalled", "WARN", 1)], id="a-ship-failing-since-it-succeeded"),
    pytest.param(HELD, None, (4 * H, 0),
                 [("ch_history_stalled", "WARN", 1)], id="no-success-for-four-hours"),
    pytest.param(HELD, None, (25 * H, 0),
                 [("ch_history_stalled", "CRITICAL", 1)], id="no-success-for-a-day"),
    pytest.param(HELD, None, None,
                 [("ch_history_stalled", "WARN", 1)], id="no-ship-ever-recorded"),
    pytest.param(HELD, None, (30 * MIN, 0), [], id="a-quiet-archive-shipped-fresh"),
])
async def test_a_copy_that_stopped_receiving_is_a_finding(
        monkeypatch, copy_top, waiting_for, shipped, expected):
    """Twelve versions in Postgres, the copy holding the first `copy_top`;
    the ship's state row its last success `shipped[0]` ago with
    `shipped[1]` failures since, or absent."""
    now = datetime.now(timezone.utc)
    async with archive_in_postgres(monkeypatch) as archive:
        await archive.versions(range(1, copy_top + 1), captured_at=now - timedelta(days=3))
        if waiting_for is not None:
            await archive.versions(range(copy_top + 1, HELD + 1), captured_at=now - waiting_for)
        if shipped is not None:
            ago, failures = shipped
            await archive.shipped(last_ok_at=now - ago, failures=failures)
        clickhouse_at_the_transport(monkeypatch, FakeClickHouse(archive=range(1, copy_top + 1)))
        found = await ch_history.reconcile_ch_history()
    assert _found(found) == expected


def test_it_is_held_like_the_buckets():
    assert "ch_history_stalled" in ch_history.HISTORY_WATCH_CONDITIONS


# ─── Whose state row a ClickHouse failure is ────────────────────────────────

STATES = (ch_silver.SILVER_STATE, ch_silver.GOLD_STATE)


async def _nothing(conn, pool):
    """Silver empty: the ship sends no rows."""


@asynccontextmanager
async def _both_shipped(monkeypatch, at):
    """Silver and Gold last shipped at `at`, Silver empty in Postgres; both
    state rows put back as they were after the block."""
    async with kept_mirror_state(*STATES), tab_in_postgres(DSN, monkeypatch, _nothing) as pool:
        for table in STATES:
            await stamp(pool, table, last_ok_at=at)
        yield pool


async def _states(pool):
    rows = await pool.fetch(
        "SELECT table_name, last_ok_at, failures_since_ok FROM meta.mirror_state"
        " WHERE table_name = ANY($1::text[])", list(STATES))
    return {r["table_name"]: (r["last_ok_at"], r["failures_since_ok"]) for r in rows}


@needs_postgres
class TestAGoldThatFailsToDerive:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("entry", ["ch_silver_sync", "reconcile_clickhouse"])
    async def test_its_failure_is_gold_s_row(self, monkeypatch, entry):
        """The ship lands and ClickHouse refuses Gold's derivation: Silver
        reads fresh, Gold failing since its last success (A17). Mutation:
        `_mark_failed(SILVER_STATE, …)` again — Gold reads fresh."""
        before = datetime.now(timezone.utc) - 2 * H
        async with _both_shipped(monkeypatch, before) as pool:
            clickhouse_at_the_transport(monkeypatch, FakeClickHouse(refuse=["gold"]))
            await getattr(ch_silver, entry)()
            states = await _states(pool)
        silver_ok, silver_failures = states[ch_silver.SILVER_STATE]
        assert silver_failures == 0 and silver_ok > before
        assert states[ch_silver.GOLD_STATE] == (before, 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("entry", ["ch_silver_sync", "reconcile_clickhouse"])
    async def test_a_silver_ship_failure_stays_silver_s(self, monkeypatch, entry):
        """ClickHouse down: the ship fails, and Gold, never reached, keeps
        the state it had."""
        before = datetime.now(timezone.utc) - 2 * H
        async with _both_shipped(monkeypatch, before) as pool:
            monkeypatch.setenv("KS_CH_URL", CLICKHOUSE_DOWN)
            await getattr(ch_silver, entry)()
            states = await _states(pool)
        assert states == {ch_silver.SILVER_STATE: (before, 1),
                          ch_silver.GOLD_STATE: (before, 0)}
