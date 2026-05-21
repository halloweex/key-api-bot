"""Tests for evaluate_disk_growth.

Pure function — the I/O wrapper (sample_disk_state) is intentionally
not covered here; it's tested by the scheduler integration on prod.
"""
from __future__ import annotations

import pytest

from core.data_quality import Severity
from core.disk_monitor import (
    CRITICAL_DISK_PCT,
    CRITICAL_GROWTH_PCT_24H,
    DiskAlert,
    MIN_DB_SIZE_FOR_GROWTH_CHECK_MB,
    WARN_DISK_PCT,
    WARN_GROWTH_PCT_24H,
    evaluate_disk_growth,
)


# ─── No-alert (healthy) ───────────────────────────────────────────────────────


class TestHealthy:
    def test_low_disk_no_growth_no_alert(self):
        result = evaluate_disk_growth(
            disk_pct_used=25.0,
            disk_free_gb=55.0,
            db_size_mb=7_000,
            db_size_24h_ago_mb=6_900,  # +1.4% — well below warn
        )
        assert result is None

    def test_below_warn_threshold_no_alert(self):
        result = evaluate_disk_growth(
            disk_pct_used=74.9,
            disk_free_gb=19.0,
            db_size_mb=10_000,
            db_size_24h_ago_mb=10_000,
        )
        assert result is None

    def test_negative_growth_after_compact_no_alert(self):
        """Post-compact: DB shrank from 43 GB to 7 GB → -84% growth.
        Must NOT alert (this is the success state)."""
        result = evaluate_disk_growth(
            disk_pct_used=25.0,
            disk_free_gb=55.0,
            db_size_mb=7_200,
            db_size_24h_ago_mb=43_000,
        )
        assert result is None


# ─── Capacity tier ────────────────────────────────────────────────────────────


class TestCapacityAlerts:
    def test_warn_at_75_pct(self):
        result = evaluate_disk_growth(
            disk_pct_used=75.0,
            disk_free_gb=18.75,
            db_size_mb=7_000,
            db_size_24h_ago_mb=7_000,
        )
        assert result is not None
        assert result.severity == Severity.WARN
        assert "75.0%" in result.reason
        assert "18.7" in result.reason or "18.8" in result.reason

    def test_critical_at_90_pct(self):
        result = evaluate_disk_growth(
            disk_pct_used=92.0,
            disk_free_gb=6.0,
            db_size_mb=40_000,
            db_size_24h_ago_mb=40_000,
        )
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert "92" in result.reason

    def test_below_warn_threshold_boundary(self):
        """Just below the floor — must not alert."""
        result = evaluate_disk_growth(
            disk_pct_used=WARN_DISK_PCT - 0.01,
            disk_free_gb=19.0,
            db_size_mb=7_000,
            db_size_24h_ago_mb=7_000,
        )
        assert result is None


# ─── Growth tier ──────────────────────────────────────────────────────────────


class TestGrowthAlerts:
    def test_warn_at_10_pct_growth(self):
        """The May 2026 bronze regression hit this rate before we noticed."""
        result = evaluate_disk_growth(
            disk_pct_used=30.0,
            disk_free_gb=50.0,
            db_size_mb=11_000,
            db_size_24h_ago_mb=10_000,  # +10%
        )
        assert result is not None
        assert result.severity == Severity.WARN
        assert "10.0%" in result.reason

    def test_critical_at_25_pct_growth(self):
        """Runaway growth — disk would fill in days."""
        result = evaluate_disk_growth(
            disk_pct_used=30.0,
            disk_free_gb=50.0,
            db_size_mb=13_000,
            db_size_24h_ago_mb=10_000,  # +30%
        )
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert "30.0%" in result.reason

    def test_below_growth_threshold_no_alert(self):
        result = evaluate_disk_growth(
            disk_pct_used=30.0,
            disk_free_gb=50.0,
            db_size_mb=10_900,
            db_size_24h_ago_mb=10_000,  # +9% — below warn
        )
        assert result is None


# ─── Bootstrap (no history) ───────────────────────────────────────────────────


class TestBootstrap:
    def test_no_24h_sample_skips_growth_check(self):
        """First run after deploy — no 24h-ago sample. Capacity check still
        fires; growth check stays silent."""
        result = evaluate_disk_growth(
            disk_pct_used=30.0,
            disk_free_gb=50.0,
            db_size_mb=7_000,
            db_size_24h_ago_mb=None,
        )
        assert result is None

    def test_no_24h_sample_capacity_still_fires(self):
        result = evaluate_disk_growth(
            disk_pct_used=92.0,
            disk_free_gb=6.0,
            db_size_mb=7_000,
            db_size_24h_ago_mb=None,
        )
        assert result is not None
        assert result.severity == Severity.CRITICAL

    def test_tiny_db_skips_growth_check(self):
        """DB < 100 MB — percentage growth is dominated by noise. Skip."""
        result = evaluate_disk_growth(
            disk_pct_used=30.0,
            disk_free_gb=50.0,
            db_size_mb=80,
            db_size_24h_ago_mb=50,  # +60%, but tiny absolute
        )
        assert result is None


# ─── Worst-of-N: both tiers fire ──────────────────────────────────────────────


class TestCombinedAlerts:
    def test_both_warn_picks_warn(self):
        result = evaluate_disk_growth(
            disk_pct_used=78.0,    # WARN
            disk_free_gb=16.5,
            db_size_mb=11_200,
            db_size_24h_ago_mb=10_000,  # +12% WARN
        )
        assert result is not None
        assert result.severity == Severity.WARN
        # Both reasons combined
        assert "78" in result.reason
        assert "12" in result.reason

    def test_critical_dominates_warn(self):
        """Disk at WARN + growth at CRITICAL → overall CRITICAL."""
        result = evaluate_disk_growth(
            disk_pct_used=78.0,  # WARN
            disk_free_gb=16.5,
            db_size_mb=14_000,
            db_size_24h_ago_mb=10_000,  # +40% CRITICAL
        )
        assert result is not None
        assert result.severity == Severity.CRITICAL


# ─── Regression markers ───────────────────────────────────────────────────────


class TestProductionScenarios:
    def test_current_state_is_quiet(self):
        """As of 2026-05-21: 25% disk, 7.2 GB DB, ~0% daily growth.
        Must not alert."""
        result = evaluate_disk_growth(
            disk_pct_used=25.0,
            disk_free_gb=55.0,
            db_size_mb=7_200,
            db_size_24h_ago_mb=7_180,  # +0.3%/day post-skip-fix
        )
        assert result is None

    def test_pre_compact_bloat_would_have_alerted(self):
        """If this watchdog had existed in April, it would have fired
        on the slow approach to 73% disk."""
        result = evaluate_disk_growth(
            disk_pct_used=73.0,
            disk_free_gb=20.0,
            db_size_mb=43_000,
            db_size_24h_ago_mb=42_000,  # +2.4%/day
        )
        # 73% < 75% WARN — quiet (the cumulative was the problem, not 24h delta)
        assert result is None

    def test_bronze_regression_would_have_fired(self):
        """If something like the May 2026 bronze accumulation kicked off
        today, the 24h growth rate would trigger WARN before the diskpct
        hit 75%."""
        result = evaluate_disk_growth(
            disk_pct_used=30.0,
            disk_free_gb=50.0,
            db_size_mb=8_000,   # +14% in one day = catastrophe rate
            db_size_24h_ago_mb=7_000,
        )
        assert result is not None
        assert result.severity == Severity.WARN
        assert "14" in result.reason


# ─── Persistence: insert / fetch_at_age / prune ───────────────────────────────


from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.disk_monitor import (
    fetch_sample_at_age,
    insert_sample,
    prune_old_samples,
)
from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


class TestPersistence:
    @pytest.mark.asyncio
    async def test_insert_then_fetch_latest(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                # 24h-ago sample
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=24),
                    "db_size_mb": 7_000, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                })
                sample = fetch_sample_at_age(conn, hours=24)
            assert sample is not None
            assert sample["db_size_mb"] == 7_000
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_no_sample_in_window_returns_none(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                # Only have a recent sample, nothing from 24h ago
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=1),
                    "db_size_mb": 7_000, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                })
                sample = fetch_sample_at_age(conn, hours=24, slack_hours=2)
            assert sample is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_fetch_at_age_within_slack(self, tmp_path):
        """A sample taken 22h or 26h ago still counts for "24h ago"."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=22),
                    "db_size_mb": 6_500, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                })
                sample = fetch_sample_at_age(conn, hours=24, slack_hours=2)
            assert sample is not None
            assert sample["db_size_mb"] == 6_500
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_picks_closest_when_multiple_in_window(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                # Both within ±2h of -24h. Closer one should win.
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=22, minutes=30),
                    "db_size_mb": 1, "disk_pct_used": 25.0, "disk_free_gb": 55.0,
                })
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=24, minutes=10),
                    "db_size_mb": 2, "disk_pct_used": 25.0, "disk_free_gb": 55.0,
                })
                sample = fetch_sample_at_age(conn, hours=24, slack_hours=2)
            assert sample["db_size_mb"] == 2  # closer to -24h

        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_prune_old_samples(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_sample(conn, {
                    "sampled_at": now - timedelta(days=20),  # old
                    "db_size_mb": 1, "disk_pct_used": 0, "disk_free_gb": 0,
                })
                insert_sample(conn, {
                    "sampled_at": now - timedelta(days=5),   # keep
                    "db_size_mb": 2, "disk_pct_used": 0, "disk_free_gb": 0,
                })
                deleted = prune_old_samples(conn, retention_days=14)
                count = conn.execute(
                    "SELECT COUNT(*) FROM disk_samples"
                ).fetchone()[0]
            assert deleted == 1
            assert count == 1
        finally:
            await store.close()


# ─── Scheduler integration ────────────────────────────────────────────────────


from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class TestSchedulerJob:
    @pytest.mark.asyncio
    async def test_bootstrap_run_persists_sample_no_alert(self, tmp_path):
        """First run: no history → growth check skipped, sample inserted."""
        from core.scheduler import BackgroundScheduler
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        try:
            BackgroundScheduler._disk_alert_last_sent = 0.0
            scheduler = BackgroundScheduler()

            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 7_200, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
                result = await scheduler._run_disk_growth_check()

            assert result["alert_fired"] is False
            assert result["db_24h_ago_mb"] is None
            send.assert_not_called()

            async with store.connection() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM disk_samples"
                ).fetchone()[0]
            assert count == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_critical_growth_fires_alert(self, tmp_path):
        from core.scheduler import BackgroundScheduler
        from core.duckdb_store import DuckDBStore
        from core.disk_monitor import insert_sample

        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        try:
            BackgroundScheduler._disk_alert_last_sent = 0.0

            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                # 24h-ago sample showing much smaller DB
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=24),
                    "db_size_mb": 7_000, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                })

            scheduler = BackgroundScheduler()
            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": now,
                    "db_size_mb": 10_000, "disk_pct_used": 35.0,
                    "disk_free_gb": 48.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
                result = await scheduler._run_disk_growth_check()

            assert result["alert_fired"] is True
            send.assert_called_once()
            msg = send.call_args[0][0]
            assert "Disk watchdog: CRITICAL" in msg
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_repeated_breach_throttled(self, tmp_path):
        """A persistent breach must not page admins every 6h."""
        from core.scheduler import BackgroundScheduler
        from core.duckdb_store import DuckDBStore
        from core.disk_monitor import insert_sample
        import time as _time

        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        try:
            # Pretend we alerted 1 minute ago
            BackgroundScheduler._disk_alert_last_sent = _time.time() - 60

            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=24),
                    "db_size_mb": 7_000, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                })

            scheduler = BackgroundScheduler()
            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": now,
                    "db_size_mb": 12_000, "disk_pct_used": 35.0,
                    "disk_free_gb": 48.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
                result = await scheduler._run_disk_growth_check()

            assert result["alert_fired"] is False
            send.assert_not_called()  # throttled
        finally:
            await store.close()
