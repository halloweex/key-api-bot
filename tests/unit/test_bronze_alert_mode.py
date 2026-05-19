"""Tests for _send_bronze_alert mode-gating.

Bronze backlog alerts only carry signal in staging mode (where promotion
flips processed_at). In legacy mode the unprocessed count is mechanical —
it grows by ~150K/day with no incident behind it. The alert must short-
circuit so an operator who later wires the alert into a broader path
doesn't get spammed.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from core.scheduler import BackgroundScheduler


class TestBronzeAlertModeGate:
    @pytest.mark.asyncio
    async def test_alert_skipped_in_legacy_mode(self):
        """Defence-in-depth: even if a caller forgets the staging guard,
        the function must not page admins in legacy mode."""
        scheduler = BackgroundScheduler()

        fake_config = SimpleNamespace(
            sync=SimpleNamespace(mode="legacy", is_staging=False)
        )
        stats = {
            "unprocessed": 4_354_169,  # production value at incident
            "oldest_unprocessed_age_s": 2_500_000,
        }

        with patch("core.config.config", fake_config), \
             patch("bot.main.send_admin_message", new_callable=AsyncMock) as mock_send:
            await scheduler._send_bronze_alert(stats)

        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_alert_sent_in_staging_mode(self):
        scheduler = BackgroundScheduler()

        fake_config = SimpleNamespace(
            sync=SimpleNamespace(mode="staging", is_staging=True)
        )
        stats = {"unprocessed": 1500, "oldest_unprocessed_age_s": 600}

        with patch("core.config.config", fake_config), \
             patch("bot.main.send_admin_message", new_callable=AsyncMock) as mock_send:
            await scheduler._send_bronze_alert(stats)

        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "Bronze Backlog Alert" in msg
        assert "1500" in msg

    @pytest.mark.asyncio
    async def test_alert_handles_send_failure_silently(self):
        """Telegram outage must not crash the scheduler job."""
        scheduler = BackgroundScheduler()

        fake_config = SimpleNamespace(
            sync=SimpleNamespace(mode="staging", is_staging=True)
        )
        stats = {"unprocessed": 2000, "oldest_unprocessed_age_s": 300}

        with patch("core.config.config", fake_config), \
             patch(
                 "bot.main.send_admin_message",
                 new_callable=AsyncMock,
                 side_effect=RuntimeError("telegram down"),
             ):
            # Must not raise.
            await scheduler._send_bronze_alert(stats)
