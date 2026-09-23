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
            scheduler = BackgroundScheduler()

            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 7_200, "disk_pct_used": 25.0,
                    "disk_free_gb": 55.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)) as send:
                result = await scheduler._run_disk_watchdog()

            assert result["alert_fired"] is False
            assert result["db_24h_ago_mb"] is None
            assert result["db_growth_mb_24h"] is None
            send.assert_not_called()

            # The heartbeat host cron reads. Without it, a watchdog that has
            # stopped sampling is indistinguishable from one with nothing to
            # say — which is how this one went blind for eleven weeks.
            beat = tmp_path / "health" / "watchdog_last_sample"
            assert beat.exists(), "the run must leave evidence outside the process"
            assert beat.read_text().startswith(str(datetime.now(timezone.utc).year))

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
               patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)) as send:
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

            scheduler = BackgroundScheduler()
            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 2_480, "disk_pct_used": 92.0,
                    "disk_free_gb": 6.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)) as send:
                result = await scheduler._run_disk_watchdog()

            assert result["alert_fired"] is True
            send.assert_called_once()
            msg = send.call_args[0][0]
            assert "Disk" in msg and "92" in msg
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
            # A delivery 60s ago, recorded where the policy now lives: the Gate.
            # Both halves of what `raise_alert` records on a delivery: the
            # bucket's cooldown and the condition's delivered-notice. Seeding
            # only the first describes a condition that has since been
            # announced resolved — and a resolved condition coming back is
            # news, which is the Gate working, not a throttle failing.
            from core.alerting import _gate
            for bucket in ("disk:WARN", "disk:CRITICAL"):
                _gate.decide(bucket, has_condition=True, conditions=[bucket])
                _gate.record_delivery(bucket)
                _gate.note_delivered_conditions([bucket], "disk")

            scheduler = BackgroundScheduler()
            with patch(
                "core.disk_monitor.sample_disk_state",
                return_value={
                    "sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 2_480, "disk_pct_used": 92.0,
                    "disk_free_gb": 6.0,
                },
            ), patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
               patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)) as send:
                result = await scheduler._run_disk_watchdog()

            assert result["alert_fired"] is False
            send.assert_not_called()  # throttled
        finally:
            await store.close()


# ─── Growth across the whole data directory ───────────────────────────────────


from core.disk_monitor import (  # noqa: E402
    CRITICAL_GROWTH_GB_168H,
    WARN_GROWTH_GB_168H,
    evaluate_dir_growth,
    fetch_dir_sample_at_age,
    insert_dir_samples,
    prune_old_dir_samples,
    sample_data_dir,
)

_GB = 1024 ** 3

# The directory as it stood on 2026-08-14, after the clean-up: two local
# backups, one pre-compact copy, the live database and its WAL.
HEALTHY_DIR = {
    "live_db": int(2.6 * _GB),
    "wal": int(0.04 * _GB),
    "old": int(4.6 * _GB),
    "backups": int(5.1 * _GB),
    "export_parquet": int(0.008 * _GB),
    "other": int(0.001 * _GB),
}


class TestDirGrowth:
    def test_a_steady_week_is_silent(self):
        """Week-over-week drift on a healthy system is business growth. The
        sawtooth itself cancels because the lag is the compact's own period."""
        after = dict(HEALTHY_DIR, live_db=HEALTHY_DIR["live_db"] + int(0.2 * _GB))
        assert evaluate_dir_growth(current=after, baseline=HEALTHY_DIR) is None

    def test_no_baseline_is_not_an_alert(self):
        assert evaluate_dir_growth(current=HEALTHY_DIR, baseline=None) is None

    def test_shrinking_is_never_an_alert(self):
        """The compact and the prune both make this number fall. That is them
        working, and it is the reading the old percentage rule inverted."""
        after = dict(HEALTHY_DIR, backups=int(0.5 * _GB))
        assert evaluate_dir_growth(current=after, baseline=HEALTHY_DIR) is None

    def test_warn_threshold(self):
        after = dict(HEALTHY_DIR,
                     backups=HEALTHY_DIR["backups"] + int(WARN_GROWTH_GB_168H * _GB))
        alert = evaluate_dir_growth(current=after, baseline=HEALTHY_DIR)
        assert alert is not None
        assert alert.severity == Severity.WARN

    def test_critical_threshold(self):
        after = dict(HEALTHY_DIR,
                     backups=HEALTHY_DIR["backups"] + int(CRITICAL_GROWTH_GB_168H * _GB))
        alert = evaluate_dir_growth(current=after, baseline=HEALTHY_DIR)
        assert alert is not None
        assert alert.severity == Severity.CRITICAL

    def test_the_2026_08_05_event_is_caught_and_named(self):
        """The regression this detector exists for: 8.93 GB of hand-made copies
        appeared beside the database in one afternoon. The old check sampled the
        database file alone, saw nothing, and blamed the database anyway.

        Twelve times the CRITICAL line, and the alert has to say 'other'."""
        after = dict(HEALTHY_DIR, other=HEALTHY_DIR["other"] + int(8.93 * _GB))
        alert = evaluate_dir_growth(current=after, baseline=HEALTHY_DIR)
        assert alert is not None
        assert alert.severity == Severity.CRITICAL
        assert alert.top_group == "other"
        assert alert.total_delta_gb == pytest.approx(8.93, abs=0.02)
        assert "other" in alert.reason

    def test_a_group_that_is_new_since_the_baseline_still_counts(self):
        """A newcomer has no baseline entry. Treating a missing key as 'no
        change' is how a whole new directory hides."""
        baseline = {k: v for k, v in HEALTHY_DIR.items() if k != "backups"}
        alert = evaluate_dir_growth(current=HEALTHY_DIR, baseline=baseline)
        assert alert is not None
        assert alert.top_group == "backups"

    def test_growth_spread_thinly_still_totals(self):
        """No single group crosses the line; together they do. The threshold is
        on the total for exactly this case."""
        after = {k: v + int(0.4 * _GB) for k, v in HEALTHY_DIR.items()}
        alert = evaluate_dir_growth(current=after, baseline=HEALTHY_DIR)
        assert alert is not None
        assert alert.severity == Severity.CRITICAL

    def test_the_bootstrap_window_reuses_the_same_evaluator(self):
        """Before a week of history exists, a step change is still a step
        change — same function, tighter window, its own thresholds."""
        after = dict(HEALTHY_DIR, backups=HEALTHY_DIR["backups"] + int(1.5 * _GB))
        alert = evaluate_dir_growth(
            current=after, baseline=HEALTHY_DIR,
            window_hours=6, warn_gb=1.0, critical_gb=2.0,
        )
        assert alert is not None
        assert alert.severity == Severity.WARN
        assert "6h" in alert.reason


class TestDirSampling:
    def test_paths_are_grouped_the_way_the_alert_reports_them(self, tmp_path):
        (tmp_path / "analytics.duckdb").write_bytes(b"x" * 100)
        (tmp_path / "analytics.duckdb.wal").write_bytes(b"x" * 10)
        (tmp_path / "analytics.duckdb.old").write_bytes(b"x" * 50)
        (tmp_path / "backups").mkdir()
        (tmp_path / "backups" / "a.duckdb").write_bytes(b"x" * 200)
        (tmp_path / "backups" / "b.duckdb").write_bytes(b"x" * 300)
        (tmp_path / "stray.bak").write_bytes(b"x" * 7)

        totals = sample_data_dir(str(tmp_path))
        assert totals["live_db"] == 100
        assert totals["wal"] == 10
        assert totals["old"] == 50
        assert totals["backups"] == 500      # summed across the directory
        assert totals["other"] == 7          # the catch-all keeps the sum honest

    def test_a_missing_directory_is_empty_not_an_exception(self, tmp_path):
        assert sample_data_dir(str(tmp_path / "nope")) == {}

    @pytest.mark.asyncio
    async def test_a_sample_set_round_trips(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_dir_samples(conn, HEALTHY_DIR, sampled_at=now - timedelta(hours=168))
                fetched = fetch_dir_sample_at_age(conn, hours=168, slack_hours=12)
            assert fetched == HEALTHY_DIR
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_groups_never_come_from_two_different_runs(self, tmp_path):
        """Differencing a mix of timestamps would invent growth out of nothing.
        The fetch pins one sampled_at and returns only that set."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_dir_samples(conn, {"live_db": 1, "backups": 2},
                                   sampled_at=now - timedelta(hours=170))
                insert_dir_samples(conn, {"live_db": 999},
                                   sampled_at=now - timedelta(hours=166))
                fetched = fetch_dir_sample_at_age(conn, hours=168, slack_hours=12)
            assert fetched in ({"live_db": 1, "backups": 2}, {"live_db": 999})
            assert not (fetched.get("live_db") == 999 and "backups" in fetched)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_retention_outlives_the_comparison_window(self, tmp_path):
        """Pruning at 14 days would delete the baseline a 168h lag needs on a
        scheduler that missed a few runs. 21 days leaves slack."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                insert_dir_samples(conn, {"live_db": 1}, sampled_at=now - timedelta(days=25))
                insert_dir_samples(conn, {"live_db": 2}, sampled_at=now - timedelta(days=8))
                deleted = prune_old_dir_samples(conn, retention_days=21)
                kept = conn.execute("SELECT COUNT(*) FROM data_dir_samples").fetchone()[0]
            assert deleted == 1
            assert kept == 1
        finally:
            await store.close()


from core.disk_monitor import (  # noqa: E402
    FS_CRITICAL_GROWTH_GB_168H,
    FS_WARN_GROWTH_GB_168H,
    UNATTRIBUTED,
    evaluate_growth,
    sample_disk_state,
)


class TestUnattributedRemainder:
    """The disk the container cannot walk.

    Only ./data and ./logs are mounted, so Docker images, journald and
    backups/ are unreachable from here. On 2026-08-30 all three grew and every
    group this module can name held still.
    """

    def test_remainder_is_the_shortfall_against_the_filesystem(self, tmp_path):
        (tmp_path / "analytics.duckdb").write_bytes(b"x" * 1000)
        totals = sample_data_dir(str(tmp_path), disk_used_bytes=5000)
        assert totals["live_db"] == 1000
        assert totals[UNATTRIBUTED] == 4000

    def test_absent_when_not_asked_for(self, tmp_path):
        """Still usable as a plain directory sizer."""
        (tmp_path / "analytics.duckdb").write_bytes(b"x" * 1000)
        assert UNATTRIBUTED not in sample_data_dir(str(tmp_path))

    def test_parts_exceeding_the_whole_clamp_to_zero(self, tmp_path):
        """`du` sums apparent sizes, statvfs counts allocated blocks. A sparse
        file makes the parts bigger than the whole; that is an artefact of two
        measuring conventions, and must not read as a negative group."""
        (tmp_path / "analytics.duckdb").write_bytes(b"x" * 10_000)
        assert sample_data_dir(str(tmp_path), disk_used_bytes=5000)[UNATTRIBUTED] == 0

    def test_unreadable_directory_reports_nothing_at_all(self, tmp_path):
        """Not even a remainder: with no groups to subtract, the whole
        filesystem would be booked as `unattributed` and read as a step."""
        assert sample_data_dir(str(tmp_path / "nope"), disk_used_bytes=5000) == {}

    def test_sample_disk_state_carries_used_bytes(self, tmp_path):
        db = tmp_path / "analytics.duckdb"
        db.write_bytes(b"x" * 1000)
        sample = sample_disk_state(str(db), mount_path=str(tmp_path))
        assert sample["disk_used_bytes"] > 0


class TestSplitGrowth:
    """Two comparisons, not one sum — the remainder moves with deploy churn."""

    def _flat_dir(self):
        return dict(HEALTHY_DIR)

    def test_the_2026_08_30_week_is_caught(self):
        """~11 GB arrived outside data/ while every named group held still.
        This is the case the watchdog was blind to; the weekly floor check was
        the only thing that saw it."""
        before = dict(self._flat_dir(), **{UNATTRIBUTED: int(19.5 * _GB)})
        after = dict(self._flat_dir(), **{UNATTRIBUTED: int(30.75 * _GB)})
        alert = evaluate_growth(current=after, baseline=before)
        assert alert is not None
        assert alert.severity.value == "CRITICAL"
        assert alert.top_group == UNATTRIBUTED
        assert "disk outside data/" in alert.reason
        assert "data dir grew" not in alert.reason

    def test_deploy_churn_below_the_filesystem_line_stays_quiet(self):
        """An ordinary week of image pulls is larger than the data directory's
        own WARN line. Folding the two into one sum would either cry wolf here
        or blind the directory; separate thresholds do neither."""
        drift = int(1.2 * _GB)
        assert drift / _GB > WARN_GROWTH_GB_168H
        assert drift / _GB < FS_WARN_GROWTH_GB_168H
        before = dict(self._flat_dir(), **{UNATTRIBUTED: int(19.5 * _GB)})
        after = dict(self._flat_dir(), **{UNATTRIBUTED: int(19.5 * _GB) + drift})
        assert evaluate_growth(current=after, baseline=before) is None

    def test_the_directory_keeps_its_own_tighter_line(self):
        """Adding the remainder must not raise the limits the data directory
        was calibrated against."""
        bump = int((WARN_GROWTH_GB_168H + 0.1) * _GB)
        before = dict(self._flat_dir(), **{UNATTRIBUTED: int(19.5 * _GB)})
        after = dict(before, live_db=before["live_db"] + bump)
        alert = evaluate_growth(current=after, baseline=before)
        assert alert is not None
        assert "data dir grew" in alert.reason
        assert alert.severity.value == "WARN"

    def test_both_halves_breaching_names_both(self):
        """Which of the two is the cause is what the reader has to decide.
        Dropping the quieter one is how the wrong component gets named."""
        before = dict(self._flat_dir(), **{UNATTRIBUTED: int(19.5 * _GB)})
        after = dict(
            before,
            live_db=before["live_db"] + int(3 * _GB),
            **{UNATTRIBUTED: int(19.5 * _GB) + int(6 * _GB)},
        )
        alert = evaluate_growth(current=after, baseline=before)
        assert alert is not None
        assert "data dir grew" in alert.reason
        assert "disk outside data/" in alert.reason
        assert alert.severity.value == "CRITICAL"

    def test_a_baseline_from_before_this_existed_reports_nothing_new(self):
        """The first sample after the deploy that added the remainder has a
        baseline without it. Reading its absence as zero would file the entire
        disk as one week's growth."""
        before = self._flat_dir()
        after = dict(self._flat_dir(), **{UNATTRIBUTED: int(30 * _GB)})
        assert evaluate_growth(current=after, baseline=before) is None

    def test_the_filesystem_line_is_the_looser_of_the_two(self):
        """Deploy churn lives out there; the data directory's own drift is
        ≈0.22 GB/week. Equal limits would make one of the two useless."""
        assert FS_WARN_GROWTH_GB_168H < FS_CRITICAL_GROWTH_GB_168H
        assert FS_WARN_GROWTH_GB_168H > WARN_GROWTH_GB_168H
        assert FS_CRITICAL_GROWTH_GB_168H > CRITICAL_GROWTH_GB_168H


class TestEvidenceForGrowth:
    """The group table the three-line message has no room for.

    Every disk alert reached the ledger with `context IS NULL` until
    2026-09-09, so the diagnostician was summoned eight times over the volume
    leak and reasoned from `du` alone each time.
    """

    def _sample(self):
        return {"disk_pct_used": 47.0, "disk_free_gb": 37.0}

    def test_it_shows_the_mover_and_that_the_rest_held_still(self):
        from core.disk_monitor import UNATTRIBUTED, evidence_for_growth
        before = {UNATTRIBUTED: int(25.6 * _GB), "live_db": int(0.19 * _GB)}
        after = {UNATTRIBUTED: int(31.2 * _GB), "live_db": int(0.19 * _GB)}
        ev = evidence_for_growth(current=after, baseline=before,
                                 sample=self._sample())
        assert ev["window_hours"] == 168
        assert ev["disk_free_gb"] == 37.0
        top = ev["groups"][0]
        assert top["group"] == UNATTRIBUTED
        assert top["delta_gb"] == 5.6
        assert all(g["delta_gb"] == 0.0 for g in ev["groups"][1:])

    def test_worst_mover_leads(self):
        """Sorted by delta, so the reader's eye lands on the cause."""
        from core.disk_monitor import evidence_for_growth
        before = {"a": 0, "b": 0, "c": 0}
        after = {"a": int(1 * _GB), "b": int(4 * _GB), "c": int(2 * _GB)}
        ev = evidence_for_growth(current=after, baseline=before,
                                 sample=self._sample())
        assert [g["group"] for g in ev["groups"]] == ["b", "c", "a"]

    def test_a_group_that_appeared_since_the_baseline_counts_whole(self):
        from core.disk_monitor import evidence_for_growth
        ev = evidence_for_growth(current={"new": int(3 * _GB)}, baseline={},
                                 sample=self._sample())
        assert ev["groups"][0] == {"group": "new", "gb": 3.0, "delta_gb": 3.0}

    def test_no_baseline_says_so_instead_of_implying_nothing_moved(self):
        """Absent deltas and zero deltas are different answers."""
        from core.disk_monitor import evidence_for_growth
        ev = evidence_for_growth(current={"live_db": int(0.19 * _GB)},
                                 baseline=None, sample=self._sample())
        assert "baseline" in ev
        assert "delta_gb" not in ev["groups"][0]

    def test_it_never_guesses_what_is_inside_the_remainder(self):
        """This process cannot see /var. Naming a cause it cannot observe is
        exactly how the 09-07 diagnosis went wrong."""
        import json
        from core.disk_monitor import UNATTRIBUTED, evidence_for_growth
        ev = evidence_for_growth(current={UNATTRIBUTED: int(31.2 * _GB)},
                                 baseline={UNATTRIBUTED: int(25.6 * _GB)},
                                 sample=self._sample())
        blob = json.dumps(ev).lower()
        for invented in ("docker", "volume", "/var", "image", "cause"):
            assert invented not in blob, f"evidence invented {invented!r}"

    def test_the_payload_stays_small(self):
        """Seven groups, not seven thousand — but pin it, because the ledger
        write is fire-and-forget with a one-second budget."""
        import json
        from core.disk_monitor import evidence_for_growth
        big = {f"g{i}": i * _GB for i in range(50)}
        ev = evidence_for_growth(current=big, baseline={}, sample=self._sample())
        assert len(json.dumps(ev).encode()) < 8192


# ── The remainder is judged by persistence ───────────────────────────────────

# 67 real `unattributed` samples from production, 2026-08-31 to 09-17, as
# (hours since the first sample, GB). Embedded rather than synthesised because
# the property being guarded is *statistical* — a 168h delta with a spread of
# 4.14 GB around a mean of -0.93 — and no hand-written fixture reproduces that
# honestly.
PROD_SERIES = [
    (0.0, 26.8653), (6.0, 27.2410), (12.0, 27.2033), (18.0, 27.3972),
    (24.0, 27.8487), (30.0, 27.9717), (36.0, 28.1342), (42.0, 28.4068),
    (48.0, 28.8449), (54.0, 29.0346), (60.0, 29.2250), (66.0, 28.4691),
    (72.0, 28.8234), (78.0, 25.9014), (84.0, 26.0005), (90.0, 24.3050),
    (96.0, 24.5666), (102.0, 24.8016), (108.0, 25.0384), (114.0, 24.1406),
    (120.0, 24.4557), (126.0, 24.7307), (132.0, 24.8459), (138.0, 23.4035),
    (144.0, 26.9070), (150.0, 27.6173), (156.0, 28.0519), (162.0, 27.9450),
    (168.0, 30.0857), (174.0, 31.9079), (180.0, 31.9358), (186.0, 32.1162),
    (192.0, 33.0647), (198.0, 33.5464), (204.0, 33.7147), (210.0, 23.7389),
    (216.0, 24.1147), (222.0, 24.3162), (228.0, 24.4758), (234.0, 23.7023),
    (240.0, 24.0808), (246.0, 24.2846), (252.0, 24.4481), (258.0, 23.6803),
    (264.0, 24.0392), (270.0, 24.1783), (276.0, 24.2071), (282.0, 23.5242),
    (288.0, 23.8631), (294.0, 26.2829), (300.0, 26.2016), (306.0, 25.4799),
    (312.0, 26.2281), (318.0, 29.8283), (324.0, 28.3939), (330.0, 25.1370),
    (336.0, 25.4544), (342.0, 25.7885), (348.0, 25.8272), (354.0, 25.2412),
    (360.0, 25.5273), (366.0, 25.9074), (372.0, 25.9368), (378.0, 25.3501),
    (384.0, 26.1040), (390.0, 27.0287), (396.0, 27.1078), (402.0, 28.3873),
    (408.0, 27.9506), (414.0, 34.4562), (420.0, 28.5533),
]


def _prod_series(step_after_h=None, step_gb=0.0):
    """The production series as (datetime, bytes), optionally with a step."""
    from datetime import datetime, timedelta, timezone
    # The real first sample. Not midnight: the watchdog runs at
    # 01/07/13/19 Kyiv, so its samples land on 22/04/10/16 UTC, and a
    # fixture anchored to midnight shifts every verdict by four hours.
    base = datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc)
    out = []
    for hours, gb in PROD_SERIES:
        v = gb + (step_gb if step_after_h is not None and hours >= step_after_h
                  else 0.0)
        out.append((base + timedelta(hours=hours), int(v * (1024 ** 3))))
    return out


class TestTheRemainderIsJudgedByPersistence:
    """The remainder is deploy churn, and differencing two points of it is
    close to a coin toss: over every window the watchdog could have judged, 8
    crossed WARN and 3 crossed CRITICAL on a series whose mean 168h delta is
    **negative**. Those are the eight disk:WARN fires between 2026-09-07 and
    09-17. Growth now has to hold to count.
    """

    def test_the_real_series_fires_only_on_growth_that_held(self):
        """The whole point, replayed on every sample this host has taken.

        Four windows survive, in two clusters, and both are real:

        * 2026-09-08/09 — the 173 anonymous volumes leaked by container
          removal without its volume flag. This alert is what found them.
        * 2026-09-17/18 — the build cache climbing 1.6 to 3.8 GB in four days
          as the gate began running far more often, plus another project's
          images. Confirmed against the host, not inferred.

        Every other window is silent, including the 22:00 transient below.
        """
        from core.disk_monitor import Severity, evaluate_remainder_growth

        series = _prod_series()
        fired = [
            (now, alert)
            for now, _ in series
            if (alert := evaluate_remainder_growth(series=series, now=now))
        ]

        assert [t.strftime("%m-%d %H") for t, _ in fired] == [
            "09-08 22", "09-09 04", "09-17 22", "09-18 04",
        ], repr([(t.isoformat(), a.severity.value) for t, a in fired])
        assert all(a.severity is Severity.WARN for _, a in fired)
        # All four are the held-growth path; none is a cliff.
        assert all("stayed there" in a.reason for _, a in fired)

    def test_the_series_it_is_judging_has_no_upward_trend(self):
        """Guards the premise, not the code. If this fixture is ever replaced
        by one that genuinely grows, the test above stops meaning what it
        claims and should fail loudly rather than pass for the wrong reason."""
        import statistics

        deltas = []
        for hours, gb in PROD_SERIES:
            base = [g for hh, g in PROD_SERIES if abs(hh - (hours - 168)) <= 12]
            if base:
                deltas.append(gb - base[len(base) // 2])
        assert len(deltas) > 20
        assert statistics.mean(deltas) < 0.5, (
            "the fixture now trends upward; the false-positive claim it "
            "supports is no longer about noise"
        )

    def test_a_spike_in_either_window_cannot_carry_the_verdict(self):
        """Taking the minimum of the recent window and the maximum of the
        baseline is what this is for. One tall sample now, or one low sample a
        week back, is exactly how the old comparison manufactured growth."""
        from datetime import datetime, timedelta, timezone

        from core.disk_monitor import evaluate_remainder_growth

        gb = 1024 ** 3
        base = datetime(2026, 9, 1, tzinfo=timezone.utc)
        flat = [(base + timedelta(hours=6 * i), 20 * gb) for i in range(40)]
        now = flat[-1][0]
        assert evaluate_remainder_growth(series=flat, now=now) is None

        spiked = list(flat)
        spiked[-2] = (spiked[-2][0], 30 * gb)
        assert evaluate_remainder_growth(series=spiked, now=now) is None

        dipped = list(flat)
        for i, (t, _b) in enumerate(dipped):
            if now - timedelta(hours=180) <= t <= now - timedelta(hours=156):
                dipped[i] = (t, 10 * gb)
                break
        assert evaluate_remainder_growth(series=dipped, now=now) is None

    def test_growth_that_holds_is_reported(self):
        """The other half of the bargain: persistence must not mean deaf."""
        from datetime import datetime, timedelta, timezone

        from core.disk_monitor import Severity, evaluate_remainder_growth

        gb = 1024 ** 3
        base = datetime(2026, 9, 1, tzinfo=timezone.utc)
        series = [
            (base + timedelta(hours=6 * i), (20 + (6 if i >= 20 else 0)) * gb)
            for i in range(40)
        ]
        alert = evaluate_remainder_growth(series=series, now=series[-1][0])
        assert alert is not None
        assert alert.severity is Severity.CRITICAL
        assert alert.total_delta_gb == 6.0
        assert "stayed there" in alert.reason

    def test_a_cliff_must_still_be_there_a_sample_later(self):
        """Persistence needs about a day to confirm even a large step, and the
        2026-08-05 event put +8.93 GB on the disk in an afternoon. So the cliff
        fires on one sample of confirmation rather than twenty-four hours of
        it — but it does need that one."""
        from datetime import datetime, timedelta, timezone

        from core.disk_monitor import Severity, evaluate_remainder_growth

        gb = 1024 ** 3
        base = datetime(2026, 9, 1, tzinfo=timezone.utc)
        series = [(base + timedelta(hours=6 * i), 20 * gb) for i in range(40)]
        # The jump, then a reading that shows it stayed.
        series.append((series[-1][0] + timedelta(hours=6), 29 * gb))
        series.append((series[-1][0] + timedelta(hours=6), 29 * gb))

        alert = evaluate_remainder_growth(series=series, now=series[-1][0])
        assert alert is not None
        assert alert.severity is Severity.CRITICAL
        assert "jumped" in alert.reason and "still there" in alert.reason
        assert alert.window_hours == 0

    def test_the_jump_alone_is_not_yet_a_cliff(self):
        """The sample that first sees the jump cannot know whether it is a
        build in flight. Six hours of patience is what separates the two."""
        from datetime import datetime, timedelta, timezone

        from core.disk_monitor import evaluate_remainder_growth

        gb = 1024 ** 3
        base = datetime(2026, 9, 1, tzinfo=timezone.utc)
        series = [(base + timedelta(hours=6 * i), 20 * gb) for i in range(40)]
        series.append((series[-1][0] + timedelta(hours=6), 29 * gb))

        assert evaluate_remainder_growth(
            series=series, now=series[-1][0]) is None

    def test_a_spike_that_returns_is_not_a_cliff(self):
        """2026-09-17 22:00, reproduced from what actually happened: the sample
        caught a gate build in flight at 34.46 GB, the next reading was 28.55,
        and the unconfirmed form had already paged and escalated over bytes
        that no longer existed."""
        from core.disk_monitor import evaluate_remainder_growth

        series = _prod_series()
        # The transient is the second-to-last real sample.
        assert round(series[-2][1] / (1024 ** 3), 2) == 34.46
        assert round(series[-1][1] / (1024 ** 3), 2) == 28.55

        alert = evaluate_remainder_growth(series=series, now=series[-1][0])
        assert alert is None or "jumped" not in alert.reason

    def test_no_cliff_fires_anywhere_in_the_real_series(self):
        """Every sample this host has ever taken, judged as it arrived. The
        unconfirmed form fired once — the transient above. This form fires on
        none of them, and still catches a jump that stays."""
        from core.disk_monitor import evaluate_remainder_growth

        series = _prod_series()
        cliffs = [
            now for now, _ in series
            if (a := evaluate_remainder_growth(series=series, now=now))
            and "jumped" in a.reason
        ]
        assert cliffs == [], (
            "a cliff fired on real data: "
            + repr([t.isoformat() for t in cliffs])
        )

    def test_the_cliff_is_calibrated_on_held_rises_not_raw_ones(self):
        """Why the cliff needs a confirming sample, in two numbers.

        The threshold was first set from raw sample-to-sample rises, whose
        largest was +3.60 GB across 66 samples — comfortably under 5.0. On
        2026-09-17 a gate build in flight produced a raw rise of +6.51 GB that
        was gone six hours later, and the unconfirmed form paged and escalated
        on it. So raw rises are not the quantity to calibrate against: a
        *held* rise is, and the largest this host has produced is well under
        the line.
        """
        from core.disk_monitor import FS_CLIFF_STEP_GB

        raw = [
            PROD_SERIES[i + 1][1] - PROD_SERIES[i][1]
            for i in range(len(PROD_SERIES) - 1)
        ]
        held = [
            min(PROD_SERIES[i + 1][1], PROD_SERIES[i][1]) - PROD_SERIES[i - 1][1]
            for i in range(1, len(PROD_SERIES) - 1)
        ]

        assert max(raw) > FS_CLIFF_STEP_GB, (
            "no raw rise exceeds the cliff any more, so the transient that "
            "motivated confirmation has been dropped from the fixture"
        )
        assert max(held) < FS_CLIFF_STEP_GB, (
            "a held rise now exceeds the cliff: "
            + repr(round(max(held), 2))
        )

    def test_thin_windows_say_nothing(self):
        """Day one after deploy. A window without a spread cannot tell a held
        rise from a spike, and inventing a verdict there is how a new series
        reads as a week of growth against a baseline that does not exist."""
        from datetime import datetime, timedelta, timezone

        from core.disk_monitor import evaluate_remainder_growth

        gb = 1024 ** 3
        base = datetime(2026, 9, 1, tzinfo=timezone.utc)
        young = [(base + timedelta(hours=6 * i), (20 + i) * gb) for i in range(4)]
        assert evaluate_remainder_growth(series=young, now=young[-1][0]) is None
        assert evaluate_remainder_growth(series=[], now=None) is None

    def test_shrinkage_is_never_an_alert(self):
        from datetime import datetime, timedelta, timezone

        from core.disk_monitor import evaluate_remainder_growth

        gb = 1024 ** 3
        base = datetime(2026, 9, 1, tzinfo=timezone.utc)
        series = [
            (base + timedelta(hours=6 * i), (40 - i) * gb) for i in range(40)
        ]
        assert evaluate_remainder_growth(series=series, now=series[-1][0]) is None
