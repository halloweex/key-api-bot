"""Tests for evaluate_bronze_invariant and the scheduler job that runs it.

This is the watchdog that would have caught the 2026-05-18 incident in 6h
instead of 30 days. The invariant: bronze row count must match what the
current (mode, shadow_enabled) combination implies.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import (
    BRONZE_INVARIANT_THRESHOLDS,
    evaluate_bronze_invariant,
)


# ─── Pure function: evaluate_bronze_invariant ────────────────────────────────


class TestEvaluateBronzeInvariant:
    def test_legacy_no_shadow_zero_rows_is_healthy(self):
        """The expected steady state after Commit 2 lands and prune runs."""
        ok, reason = evaluate_bronze_invariant(
            {"total": 0, "unprocessed": 0}, mode="legacy", shadow_enabled=False
        )
        assert ok and reason is None

    def test_legacy_no_shadow_within_slop_is_healthy(self):
        """Allow ~one prune-cycle's worth of rows before alerting."""
        ok, _ = evaluate_bronze_invariant(
            {"total": 5_000, "unprocessed": 5_000},
            mode="legacy", shadow_enabled=False,
        )
        assert ok

    def test_legacy_no_shadow_million_rows_is_violation(self):
        """The 2026-05-18 production state. Must fire."""
        ok, reason = evaluate_bronze_invariant(
            {"total": 4_354_169, "unprocessed": 4_354_169},
            mode="legacy", shadow_enabled=False,
        )
        assert not ok
        assert "4,354,169" in reason
        assert "legacy" in reason

    def test_legacy_with_shadow_million_rows_is_healthy(self):
        """Opt-in shadow log: a million rows is normal, prune keeps it bounded."""
        ok, _ = evaluate_bronze_invariant(
            {"total": 500_000, "unprocessed": 0},
            mode="legacy", shadow_enabled=True,
        )
        assert ok

    def test_legacy_with_shadow_breach_when_prune_broken(self):
        """If prune is broken even shadow mode can blow past threshold."""
        ok, reason = evaluate_bronze_invariant(
            {"total": 2_000_000, "unprocessed": 2_000_000},
            mode="legacy", shadow_enabled=True,
        )
        assert not ok
        assert "2,000,000" in reason

    def test_staging_zero_rows_is_healthy(self):
        ok, _ = evaluate_bronze_invariant(
            {"total": 0, "unprocessed": 0}, mode="staging", shadow_enabled=False
        )
        assert ok

    def test_staging_promotion_backlog_alerts(self):
        """Promotion falling behind → unprocessed grows past threshold."""
        ok, reason = evaluate_bronze_invariant(
            {"total": 500_000, "unprocessed": 200_000},
            mode="staging", shadow_enabled=False,
        )
        assert not ok
        assert "unprocessed" in reason

    def test_unknown_mode_returns_unhealthy(self):
        """Defensive: typo'd SYNC_MODE shouldn't silently pass."""
        ok, reason = evaluate_bronze_invariant(
            {"total": 0, "unprocessed": 0}, mode="experimental", shadow_enabled=False
        )
        assert not ok
        assert "unknown" in reason

    def test_threshold_table_covers_all_four_combinations(self):
        """The matrix is the source of truth — keep it exhaustive."""
        assert set(BRONZE_INVARIANT_THRESHOLDS.keys()) == {
            ("legacy", False),
            ("legacy", True),
            ("staging", False),
            ("staging", True),
        }


# ─── Scheduler integration: _run_bronze_invariant_check ──────────────────────


class TestSchedulerInvariantJob:
    @pytest.mark.asyncio
    async def test_job_sends_alert_when_invariant_breached(self):
        """End-to-end: bad bronze stats + legacy mode → Telegram alert."""
        from core.scheduler import BackgroundScheduler

        scheduler = BackgroundScheduler()
        # Reset throttle.
        BackgroundScheduler._bronze_invariant_last_alert = 0.0

        fake_store = AsyncMock()
        fake_store.get_bronze_stats = AsyncMock(
            return_value={
                "total": 4_354_169,
                "unprocessed": 4_354_169,
                "oldest_unprocessed_age_s": 2_000_000,
                "latest_event_ts": None,
            }
        )
        fake_config = SimpleNamespace(
            sync=SimpleNamespace(
                mode="legacy", legacy_bronze_shadow=False,
                is_staging=False,
            )
        )

        with patch("core.duckdb_store.get_store", AsyncMock(return_value=fake_store)), \
             patch("core.config.config", fake_config), \
             patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
            result = await scheduler._run_bronze_invariant_check()

        assert result["healthy"] is False
        assert result["total"] == 4_354_169
        send.assert_called_once()
        msg = send.call_args[0][0]
        assert "Bronze invariant violated" in msg
        assert "4,354,169" in msg

    @pytest.mark.asyncio
    async def test_job_silent_when_healthy(self):
        from core.scheduler import BackgroundScheduler

        scheduler = BackgroundScheduler()
        BackgroundScheduler._bronze_invariant_last_alert = 0.0

        fake_store = AsyncMock()
        fake_store.get_bronze_stats = AsyncMock(
            return_value={
                "total": 0, "unprocessed": 0,
                "oldest_unprocessed_age_s": None, "latest_event_ts": None,
            }
        )
        fake_config = SimpleNamespace(
            sync=SimpleNamespace(
                mode="legacy", legacy_bronze_shadow=False,
                is_staging=False,
            )
        )

        with patch("core.duckdb_store.get_store", AsyncMock(return_value=fake_store)), \
             patch("core.config.config", fake_config), \
             patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
            result = await scheduler._run_bronze_invariant_check()

        assert result["healthy"] is True
        send.assert_not_called()

    @pytest.mark.asyncio
    async def test_job_throttles_repeated_breaches(self):
        """A persistent breach should not page admins every 6 hours forever."""
        from core.scheduler import BackgroundScheduler

        scheduler = BackgroundScheduler()
        # Pretend we alerted 1 minute ago.
        import time as _time
        BackgroundScheduler._bronze_invariant_last_alert = _time.time() - 60

        fake_store = AsyncMock()
        fake_store.get_bronze_stats = AsyncMock(
            return_value={
                "total": 5_000_000, "unprocessed": 5_000_000,
                "oldest_unprocessed_age_s": 9999, "latest_event_ts": None,
            }
        )
        fake_config = SimpleNamespace(
            sync=SimpleNamespace(
                mode="legacy", legacy_bronze_shadow=False,
                is_staging=False,
            )
        )

        with patch("core.duckdb_store.get_store", AsyncMock(return_value=fake_store)), \
             patch("core.config.config", fake_config), \
             patch("bot.main.send_admin_message", new_callable=AsyncMock) as send:
            result = await scheduler._run_bronze_invariant_check()

        assert result["healthy"] is False
        send.assert_not_called()  # Throttled.
