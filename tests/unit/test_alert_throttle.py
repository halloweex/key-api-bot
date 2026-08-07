"""An alert that repeats every two minutes stops being an alert.

Restoring delivery (PR #36) uncovered this immediately: the warehouse validator
re-raises the same CRITICAL on every refresh, so both admins got 30 identical
messages an hour for a condition that needs one human action.
"""
from unittest.mock import AsyncMock, patch

import pytest

from core.telegram_alerts import AlertThrottle, reset_throttle, throttle_check

bot_main = __import__("importlib").import_module("bot.main")


class TestAlertThrottle:
    def test_first_alert_passes(self):
        throttle = AlertThrottle(cooldown_seconds=60)
        assert throttle.check("boom", now=0.0) == (True, 0)

    def test_repeat_within_cooldown_is_suppressed_and_counted(self):
        throttle = AlertThrottle(cooldown_seconds=60)
        throttle.check("boom", now=0.0)

        assert throttle.check("boom", now=10.0) == (False, 1)
        assert throttle.check("boom", now=20.0) == (False, 2)
        assert throttle.check("boom", now=30.0) == (False, 3)

    def test_alert_passes_again_after_cooldown_reporting_what_it_stood_for(self):
        throttle = AlertThrottle(cooldown_seconds=60)
        throttle.check("boom", now=0.0)
        for t in (10.0, 20.0, 30.0):
            throttle.check("boom", now=t)

        assert throttle.check("boom", now=61.0) == (True, 3)

    def test_counter_resets_after_a_send(self):
        throttle = AlertThrottle(cooldown_seconds=60)
        throttle.check("boom", now=0.0)
        throttle.check("boom", now=10.0)
        throttle.check("boom", now=61.0)      # sends, clears the tally

        assert throttle.check("boom", now=200.0) == (True, 0)

    def test_distinct_alerts_do_not_mask_each_other(self):
        throttle = AlertThrottle(cooldown_seconds=60)
        throttle.check("warehouse broken", now=0.0)

        assert throttle.check("disk nearly full", now=1.0) == (True, 0)

    def test_a_changed_metric_in_the_body_still_gets_through(self):
        """Keyed on exact text, so a moving number is a new alert."""
        throttle = AlertThrottle(cooldown_seconds=3600)
        throttle.check("gap=1000", now=0.0)

        assert throttle.check("gap=2000", now=1.0)[0] is True


class TestThrottleCheckDecoration:
    def setup_method(self):
        reset_throttle()

    def teardown_method(self):
        reset_throttle()

    def test_passes_text_through_unchanged_when_nothing_was_muted(self):
        assert throttle_check("hello") == (True, "hello")

    def test_suppressed_repeats_are_reported_on_the_next_send(self, monkeypatch):
        import core.telegram_alerts as alerts

        monkeypatch.setattr(alerts._throttle, "cooldown_seconds", 0.0)
        alerts._throttle._last_sent["x"] = 0.0
        alerts._throttle._suppressed["x"] = 7

        should_send, text = throttle_check("x")
        assert should_send is True
        assert "repeated 7×" in text
        assert text.startswith("x")


class TestSendAdminMessageIsThrottled:
    def setup_method(self):
        reset_throttle()

    def teardown_method(self):
        reset_throttle()

    @pytest.mark.asyncio
    async def test_identical_alerts_hit_the_transport_once(self, monkeypatch):
        monkeypatch.setattr(bot_main, "_application", None)
        sender = AsyncMock(return_value=2)

        with patch("core.telegram_alerts.send_admin_message_http", sender):
            for _ in range(15):
                await bot_main.send_admin_message("🚨 CRITICAL: validation failed")

        assert sender.await_count == 1

    @pytest.mark.asyncio
    async def test_different_alerts_all_get_through(self, monkeypatch):
        monkeypatch.setattr(bot_main, "_application", None)
        sender = AsyncMock(return_value=2)

        with patch("core.telegram_alerts.send_admin_message_http", sender):
            await bot_main.send_admin_message("warehouse broken")
            await bot_main.send_admin_message("disk nearly full")
            await bot_main.send_admin_message("sync stalled")

        assert sender.await_count == 3
