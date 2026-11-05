"""An archive copy that stopped receiving is a finding, not a PASS (L2-02).

`reconcile_ch_history` compares bucket counts below ClickHouse's own MAX(id).
A copy that stopped receiving agrees with Postgres perfectly below that
ceiling: stopped at 47 000 while Postgres held 52 448, it filed nothing and
held nothing. It now files `ch_history_stalled` from Postgres' side of the
ship — versions above the copy's ceiling captured more than 3 h ago, or the
archive's state row failing or without a success for 3 h — WARN, CRITICAL
past a day. And a Gold derivation that fails in ClickHouse is recorded
against Gold's state row, not Silver's.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from core import ch_history, ch_silver

NOW = datetime.now(timezone.utc)
PG_MAX, CH_MAX = 52_448, 47_000


def _buckets(top: int) -> dict:
    out: dict = {}
    for i in range(1, top + 1):
        out[i // 10000] = out.get(i // 10000, 0) + 1
    return out


class _Conn:
    def __init__(self, pg_max, oldest_unshipped, state):
        self.pg_max, self.oldest, self.state = pg_max, oldest_unshipped, state

    async def fetch(self, sql, ceiling):
        return list(_buckets(min(self.pg_max, ceiling)).items())

    async def fetchrow(self, sql, arg):
        if "order_versions" in sql:
            waiting = max(0, self.pg_max - arg)
            return {"n": waiting, "oldest": self.oldest if waiting else None}
        return self.state


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class Acq:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *a):
                return False
        return Acq()


async def _run(monkeypatch, *, ch_max, oldest, state):
    async def execute(sql, body=None):
        return "".join(f"{b}\t{n}\n" for b, n in _buckets(ch_max).items())

    monkeypatch.setattr(ch_history, "configured", lambda: True)
    monkeypatch.setattr(ch_history, "_ch_max_id", AsyncMock(return_value=ch_max))
    monkeypatch.setattr(ch_history, "_execute", execute)
    monkeypatch.setattr(ch_history, "last_heal", {})
    pool = _Pool(_Conn(PG_MAX, oldest, state))
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=pool))
    return await ch_history.reconcile_ch_history()


def _by_name(issues):
    return {i.check_name: i for i in issues}


HEALTHY = {"last_ok_at": NOW - timedelta(minutes=30), "failures_since_ok": 0}


class TestAStoppedCopy:
    @pytest.mark.asyncio
    async def test_the_reproduction_is_a_finding(self, monkeypatch):
        """Mutation: drop the `_stall` call — findings [] as before."""
        issues = await _run(monkeypatch, ch_max=CH_MAX, oldest=NOW - timedelta(hours=30),
                            state={"last_ok_at": NOW - timedelta(hours=30),
                                   "failures_since_ok": 25})
        stalled = _by_name(issues)["ch_history_stalled"]
        assert stalled.severity.value == "CRITICAL"
        assert stalled.count == PG_MAX - CH_MAX
        assert "ch_history_buckets" not in _by_name(issues)    # below the ceiling, equal

    @pytest.mark.asyncio
    async def test_a_few_hours_is_a_warn(self, monkeypatch):
        issues = await _run(monkeypatch, ch_max=CH_MAX, oldest=NOW - timedelta(hours=4),
                            state=HEALTHY)
        assert _by_name(issues)["ch_history_stalled"].severity.value == "WARN"

    @pytest.mark.asyncio
    async def test_a_failing_ship_is_a_warn(self, monkeypatch):
        issues = await _run(monkeypatch, ch_max=PG_MAX, oldest=None,
                            state={"last_ok_at": NOW - timedelta(minutes=50),
                                   "failures_since_ok": 1})
        assert _by_name(issues)["ch_history_stalled"].severity.value == "WARN"

    @pytest.mark.asyncio
    async def test_a_ship_never_recorded_is_a_warn(self, monkeypatch):
        issues = await _run(monkeypatch, ch_max=PG_MAX, oldest=None, state=None)
        assert "ch_history_stalled" in _by_name(issues)


class TestAHealthyCopy:
    @pytest.mark.asyncio
    async def test_versions_inside_the_hour_are_not_a_stall(self, monkeypatch):
        """Versions captured since the last hourly ship wait for the next."""
        issues = await _run(monkeypatch, ch_max=CH_MAX, oldest=NOW - timedelta(minutes=40),
                            state=HEALTHY)
        assert "ch_history_stalled" not in _by_name(issues)

    @pytest.mark.asyncio
    async def test_a_quiet_archive_with_fresh_ships_is_clean(self, monkeypatch):
        assert await _run(monkeypatch, ch_max=PG_MAX, oldest=None, state=HEALTHY) == []

    def test_it_is_held_like_the_buckets(self):
        assert "ch_history_stalled" in ch_history.HISTORY_WATCH_CONDITIONS


class TestAGoldThatFailsToDerive:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("entry", ["sync", "reconcile"])
    async def test_its_failure_is_gold_s_row(self, monkeypatch, entry):
        """Mutation: `_mark_failed(SILVER_STATE, …)` again — Gold reads fresh."""
        failed, ok = [], []
        monkeypatch.setattr(ch_silver, "configured", lambda: True)
        monkeypatch.setattr(ch_silver, "_fetch_pg_silver", AsyncMock(return_value=[]))
        monkeypatch.setattr(ch_silver, "_ship_silver_rows", AsyncMock())
        monkeypatch.setattr(ch_silver, "_derive_gold",
                            AsyncMock(side_effect=RuntimeError("code 241")))

        async def mark_ok(row, n):
            ok.append(row)

        async def mark_failed(row, detail):
            failed.append(row)

        monkeypatch.setattr(ch_silver, "_mark_ok", mark_ok)
        monkeypatch.setattr(ch_silver, "_mark_failed", mark_failed)
        if entry == "sync":
            assert "error" in await ch_silver.ch_silver_sync()
        else:
            await ch_silver.reconcile_clickhouse()
        assert ok == [ch_silver.SILVER_STATE]
        assert failed == [ch_silver.GOLD_STATE]

    @pytest.mark.asyncio
    async def test_a_silver_ship_failure_stays_silver_s(self, monkeypatch):
        failed = []
        monkeypatch.setattr(ch_silver, "configured", lambda: True)
        monkeypatch.setattr(ch_silver, "_fetch_pg_silver", AsyncMock(return_value=[]))
        monkeypatch.setattr(ch_silver, "_ship_silver_rows",
                            AsyncMock(side_effect=RuntimeError("down")))

        async def mark_failed(row, detail):
            failed.append(row)

        monkeypatch.setattr(ch_silver, "_mark_failed", mark_failed)
        await ch_silver.ch_silver_sync()
        assert failed == [ch_silver.SILVER_STATE]
