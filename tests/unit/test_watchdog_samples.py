"""Chain 10: the watchdogs' samples, written to and read from Postgres alone.

The parts that need no database; the real-Postgres half is
`tests/integration/test_watchdog_samples_pg.py`. Until step 14's S14-10 the
samples had a DuckDB shadow and a `duckdb` branch behind `KS_WRITE_WATCHDOGS`;
their tests went with them. Every guard names, in its docstring, the mutation
it exists to catch.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_watchdog_write as chain, watchdog_samples


@pytest.fixture
def flag(monkeypatch):
    """No chain's flag. The flag routes nothing since S14-00; it is set only
    to show that."""
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)

    def set_(value):
        if value is None:
            monkeypatch.delenv(chain.WRITE_ENV, raising=False)
        else:
            monkeypatch.setenv(chain.WRITE_ENV, value)
    yield set_


@pytest.fixture
def no_duckdb(monkeypatch):
    """Any reach for DuckDB's store fails the test."""
    from core import store as core_store

    def connection(self):
        raise AssertionError("the samples opened a DuckDB connection")

    monkeypatch.setattr(core_store.AnalyticsStore, "connection", connection, raising=False)


def _sample(at=None, pct=40.0, free=100.0):
    return {"sampled_at": at or datetime.now(timezone.utc), "db_size_mb": 2_000.0,
            "disk_pct_used": pct, "disk_free_gb": free, "disk_used_bytes": 1}


MEM = {"working_set": 600 * 1024 * 1024, "page_cache": 100 * 1024 * 1024,
       "limit": 1024 * 1024 * 1024, "oom_kills": 0}


# ─── The tick: one Postgres transaction, nothing after it ─────────────────────


class TestTheTick:

    @pytest.mark.asyncio
    async def test_a_refusing_postgres_returns_no_history(self, flag, no_duckdb,
                                                          monkeypatch):
        """Mutation: let the failure propagate — the disk job would raise
        before judging capacity, on the day the disk is full."""
        monkeypatch.setattr(chain, "disk_tick", AsyncMock(side_effect=OSError("no space")))
        monkeypatch.setattr(chain, "memory_tick", AsyncMock(side_effect=OSError("no space")))
        assert await watchdog_samples.disk_tick(_sample(), {"duckdb": 1}) == \
            watchdog_samples.EMPTY_DISK
        assert await watchdog_samples.memory_tick(MEM) == watchdog_samples.EMPTY_MEMORY

    @pytest.mark.asyncio
    async def test_the_capacity_page_goes_out_with_postgres_down(
            self, flag, no_duckdb, monkeypatch, tmp_path):
        """The job end to end: a full disk and a Postgres that refuses. The
        alert is the one that must still be sent. The store is the job's only
        for its file's path (the sample's `db_size_mb`)."""
        from core.scheduler import BackgroundScheduler

        monkeypatch.setattr(chain, "disk_tick", AsyncMock(side_effect=OSError("no space")))
        store = SimpleNamespace(db_path=tmp_path / "analytics.duckdb")
        with patch("core.disk_monitor.sample_disk_state",
                   return_value=_sample(pct=97.0, free=1.5)), \
             patch("core.store.get_store", AsyncMock(return_value=store)), \
             patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)):
            result = await BackgroundScheduler()._run_disk_watchdog()
        assert result["alert_fired"] is True, result
        assert result["db_24h_ago_mb"] is None


# ─── The standing watch ──────────────────────────────────────────────────────


class TestTheStandingWatch:
    NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)

    def _judge(self, spans):
        from core import pg_chain_invariants as inv

        return inv.check_chain_invariants(inv.Facts(
            watched=(chain.CHAIN,), now=self.NOW, watchdogs=inv.Watchdogs(spans=spans)))

    def _healthy(self):
        return (("app.disk_samples", self.NOW - timedelta(hours=3), self.NOW - timedelta(days=13)),
                ("app.data_dir_samples", self.NOW - timedelta(hours=3), self.NOW - timedelta(days=20)),
                ("app.memory_samples", self.NOW - timedelta(minutes=20), self.NOW - timedelta(days=13)))

    def test_a_healthy_set_is_quiet(self):
        assert self._judge(self._healthy()) == []

    def test_a_watchdog_that_stopped_storing_is_stale(self):
        """Mutation: drop the staleness rule — with the copy and its
        comparison stood down, nothing else notices a watchdog that stopped."""
        from core import pg_chain_invariants as inv

        spans = list(self._healthy())
        spans[2] = ("app.memory_samples", self.NOW - timedelta(hours=2), spans[2][2])
        (issue,) = self._judge(tuple(spans))
        assert issue.check_name == inv.SAMPLES_STALE
        assert issue.table_name == "app.memory_samples"

    def test_an_empty_table_is_stale(self):
        spans = list(self._healthy())
        spans[0] = ("app.disk_samples", None, None)
        (issue,) = self._judge(tuple(spans))
        assert "holds no sample" in issue.description

    def test_a_prune_that_stopped_is_unbounded(self):
        """Mutation: drop the retention rule."""
        from core import pg_chain_invariants as inv

        spans = list(self._healthy())
        spans[1] = ("app.data_dir_samples", spans[1][1], self.NOW - timedelta(days=23))
        (issue,) = self._judge(tuple(spans))
        assert issue.check_name == inv.RETENTION_UNBOUNDED

    def test_the_limits_are_the_jobs_own(self):
        from core.disk_monitor import DIR_RETENTION_DAYS, DISK_RETENTION_DAYS
        from core.memory_monitor import MEMORY_RETENTION_DAYS

        assert chain.retention_days() == {
            "app.disk_samples": DISK_RETENTION_DAYS,
            "app.data_dir_samples": DIR_RETENTION_DAYS,
            "app.memory_samples": MEMORY_RETENTION_DAYS}
        assert set(chain.STALE_AFTER) == set(chain.CHAIN_TABLES)
