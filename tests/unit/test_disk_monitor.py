"""Tests for evaluate_disk_capacity and the disk_samples persistence helpers.

Fixtures here are production readings, dated, with their source named.

That rule exists because of how this file failed. It used to assert that
"as of 2026-05-21" production sat at 25% disk with a 7.2 GB database and
~0% daily growth. That stopped being true on 2026-05-31, when the weekly
compact began cutting the database to ~70 MB every Sunday. The suite
stayed green for weeks while the thresholds it was guarding drifted
into firing CRITICAL every week by construction — and a green suite
asserting a world that has ended is worse than no suite, because it is
what convinces the next reader the calibration is still good.

So: when the production regime changes, these fixtures change with it, and
every docstring says when its numbers were read.
"""
from __future__ import annotations

import pytest

from core.data_quality import Severity
from core.disk_monitor import (
    WARN_DISK_PCT,
    evaluate_disk_capacity,
)


# Ten consecutive samples read out of production `disk_samples` on
# 2026-08-12, covering 2026-08-10 07:00 → 2026-08-12 19:00 Kyiv.
# Measured growth across them: 607-682 MB/24h. A flat line.
# The percentage rule this module used to carry reported the same flat line
# as +456% decaying to +33.7% — a deceleration manufactured entirely by a
# denominator that the Sunday compact resets to ~80 MB.
SAWTOOTH_WEEK_DB_MB = [
    831.76, 1025.26, 1235.01, 1398.26, 1463.76,
    1654.76, 1855.76, 2014.26, 2082.00, 2480.00,
]

# Whole-disk usage over that same window, both endpoints as recorded.
SAWTOOTH_WEEK_DISK_PCT = (54.71, 61.26)


# ─── No-alert (healthy) ───────────────────────────────────────────────────────


class TestHealthy:
    def test_low_disk_no_alert(self):
        result = evaluate_disk_capacity(
            disk_pct_used=25.0,
            disk_free_gb=55.0,
            db_size_mb=7_000,
        )
        assert result is None

    def test_below_warn_threshold_no_alert(self):
        result = evaluate_disk_capacity(
            disk_pct_used=74.9,
            disk_free_gb=19.0,
            db_size_mb=10_000,
        )
        assert result is None

    def test_huge_db_on_a_roomy_disk_is_not_an_alert(self):
        """Size alone is never the trigger. A 43 GB database on a disk at
        25% is exactly what the pre-compact regime looked like, and it was
        fine — the compact was keeping up."""
        result = evaluate_disk_capacity(
            disk_pct_used=25.0,
            disk_free_gb=55.0,
            db_size_mb=43_000,
        )
        assert result is None


# ─── Capacity tier ────────────────────────────────────────────────────────────


class TestCapacityAlerts:
    def test_warn_at_75_pct(self):
        result = evaluate_disk_capacity(
            disk_pct_used=75.0,
            disk_free_gb=18.75,
            db_size_mb=7_000,
        )
        assert result is not None
        assert result.severity == Severity.WARN
        assert "75.0%" in result.reason
        assert "18.7" in result.reason or "18.8" in result.reason

    def test_critical_at_90_pct(self):
        result = evaluate_disk_capacity(
            disk_pct_used=92.0,
            disk_free_gb=6.0,
            db_size_mb=40_000,
        )
        assert result is not None
        assert result.severity == Severity.CRITICAL
        assert "92" in result.reason

    def test_below_warn_threshold_boundary(self):
        """Just below the floor — must not alert."""
        result = evaluate_disk_capacity(
            disk_pct_used=WARN_DISK_PCT - 0.01,
            disk_free_gb=19.0,
            db_size_mb=7_000,
        )
        assert result is None

    def test_db_size_is_carried_but_not_judged(self):
        result = evaluate_disk_capacity(
            disk_pct_used=92.0,
            disk_free_gb=6.0,
            db_size_mb=2_480,
        )
        assert result is not None
        assert result.db_size_mb == 2_480
        # The DB is reported for context; the reason names the disk.
        assert "disk" in result.reason


# ─── Regression: the sawtooth that could not be quiet ─────────────────────────


class TestSawtoothRegression:
    """The defect this module was changed to fix.

    weekly_compact.sh rebuilds the DB every Sunday 02:00 UTC and leaves it
    near 80 MB, so any percentage measured against a 24h-old baseline is
    measured against a denominator that resets weekly. To read below the
    old 10% WARN line the baseline had to exceed 6300 MB; the weekly peak
    is 4.4 GB. A healthy week produced 15 CRITICAL and 9 WARN evaluations
    and zero quiet ones. It must now be silent end to end.
    """

    @pytest.mark.parametrize("db_size_mb", SAWTOOTH_WEEK_DB_MB)
    @pytest.mark.parametrize("disk_pct_used", SAWTOOTH_WEEK_DISK_PCT)
    def test_healthy_sawtooth_week_never_alerts(self, db_size_mb, disk_pct_used):
        result = evaluate_disk_capacity(
            disk_pct_used=disk_pct_used,
            disk_free_gb=25.0,
            db_size_mb=db_size_mb,
        )
        assert result is None

    def test_the_alert_that_started_this_is_now_silent(self):
        """2026-08-12 07:00 Kyiv paged CRITICAL: "DB grew +42.2% in 24h
        (1464 -> 2082 MB)". Same reading, same disk, nothing wrong."""
        result = evaluate_disk_capacity(
            disk_pct_used=61.8,
            disk_free_gb=25.4,
            db_size_mb=2_082.0,
        )
        assert result is None


# ─── Regression markers ───────────────────────────────────────────────────────


class TestProductionScenarios:
    def test_current_state_is_quiet(self):
        """Read from the host on 2026-08-12 19:00: 62.4% disk, 25.0 GB
        free, 2.5 GB database mid-week. Must not alert."""
        result = evaluate_disk_capacity(
            disk_pct_used=62.4,
            disk_free_gb=25.0,
            db_size_mb=2_480,
        )
        assert result is None

    def test_the_2026_08_05_regression_is_below_capacity_reach(self):
        """A change once moved the post-compact disk floor by tens of points
        in a single week, by adding files that were not the database.

        Capacity does not catch that, and this test says so on purpose: 58%
        is well under WARN. The detector that would catch it — absolute
        growth of the whole data directory, with per-path attribution so
        the alert can name what grew — is not written yet. Nothing here
        should be read as claiming that gap is covered.
        """
        result = evaluate_disk_capacity(
            disk_pct_used=58.0,
            disk_free_gb=31.0,
            db_size_mb=81,
        )
        assert result is None

    def test_the_same_trend_continuing_does_fire(self):
        """What capacity is for: one more regression of that size, and the
        disk crosses 75%."""
        result = evaluate_disk_capacity(
            disk_pct_used=76.0,
            disk_free_gb=18.0,
            db_size_mb=2_480,
        )
        assert result is not None
        assert result.severity == Severity.WARN


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


from unittest.mock import AsyncMock, patch


class TestSchedulerJob:
    @pytest.mark.asyncio
    async def test_bootstrap_run_persists_sample_no_alert(self, tmp_path):
        """First run: no history → no 24h delta to report, sample inserted."""
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
                result = await scheduler._run_disk_watchdog()

            assert result["alert_fired"] is False
            assert result["db_24h_ago_mb"] is None
            assert result["db_growth_mb_24h"] is None
            send.assert_not_called()

            async with store.connection() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM disk_samples"
                ).fetchone()[0]
            assert count == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_growth_is_measured_and_reported_but_never_paged(self, tmp_path):
        """A mid-week sawtooth reading: +618 MB in 24h, the exact delta that
        paged CRITICAL on 2026-08-12. The number must reach the job result
        and the log. It must not reach anyone's phone."""
        from core.scheduler import BackgroundScheduler
        from core.duckdb_store import DuckDBStore
        from core.disk_monitor import insert_sample

        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        try:
            BackgroundScheduler._disk_alert_last_sent = 0.0

            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_sample(conn, {
                    "sampled_at": now - timedelta(hours=24),
                    "db_size_mb": 1_463.76, "disk_pct_used": 61.2,
                    "disk_free_gb": 25.8,
                })

            scheduler = BackgroundScheduler()
            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": now,
                    "db_size_mb": 2_082.0, "disk_pct_used": 61.8,
                    "disk_free_gb": 25.4,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
                result = await scheduler._run_disk_watchdog()

            assert result["alert_fired"] is False
            assert result["db_growth_mb_24h"] == pytest.approx(618.24)
            send.assert_not_called()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_capacity_breach_fires_alert(self, tmp_path):
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
                    "db_size_mb": 2_480, "disk_pct_used": 92.0,
                    "disk_free_gb": 6.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
                result = await scheduler._run_disk_watchdog()

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
        import time as _time

        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        try:
            # Pretend we alerted 1 minute ago
            BackgroundScheduler._disk_alert_last_sent = _time.time() - 60

            scheduler = BackgroundScheduler()
            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 2_480, "disk_pct_used": 92.0,
                    "disk_free_gb": 6.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
                result = await scheduler._run_disk_watchdog()

            assert result["alert_fired"] is False
            send.assert_not_called()  # throttled
        finally:
            await store.close()
