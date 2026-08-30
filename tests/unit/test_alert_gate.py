"""The Gate: one door, and a cooldown that grows with the condition's age.

The number is measured, not chosen: production history holds three multi-week
episodes of the warehouse validator failing ~545 ticks a day — a standing
CRITICAL against a flat 30-minute cooldown is 48 messages a day for weeks
(one June condition delivered 404 copies). A condition standing an hour is no
longer news; it becomes one daily reminder that says how long and how many
repeats it stands for.
"""
from unittest.mock import AsyncMock, patch

import pytest

from core.alerting import AlertGate, raise_alert, reset_gate


class TestTheEscalatingCooldown:
    def gate(self):
        return AlertGate()

    def test_a_new_incident_fires_immediately(self):
        ok, suffix = self.gate().decide("b", has_condition=True, now=1000.0)
        assert ok and suffix == ""

    def test_loud_phase_repeats_every_30_minutes(self):
        g = self.gate()
        g.decide("b", has_condition=True, now=0.0)
        g.record_delivery("b", now=0.0)
        assert g.decide("b", has_condition=True, now=120.0)[0] is False
        ok, _ = g.decide("b", has_condition=True, now=1900.0)
        assert ok  # 31 min in, still inside the loud hour

    def test_a_standing_condition_drops_to_one_reminder_a_day(self):
        """The anti-48-messages-a-day rule, replayed at the validator's real
        cadence: one raise every two minutes for 25 hours straight."""
        g = self.gate()
        fired = []
        for t in range(0, 25 * 3600, 120):
            ok, suffix = g.decide("b", has_condition=True, now=float(t))
            if ok:
                fired.append((t, suffix))
                g.record_delivery("b", now=float(t))
        # Loud hour: the first raise plus one per 30 min. After that: exactly
        # one daily reminder — not the 48 a day the flat cooldown delivered.
        loud = [t for t, _ in fired if t < 3600]
        after = [(t, sfx) for t, sfx in fired if t >= 3600]
        assert loud == [0, 1800]
        assert len(after) == 1
        t, suffix = after[0]
        assert t >= 1800 + 86400
        assert "standing 24h" in suffix
        assert "repeats ×" in suffix
        assert "next reminder in 24h" in suffix

    def test_events_never_escalate_their_cooldown(self):
        """An event repeat is a new fact, not a standing state."""
        g = self.gate()
        g.decide("b", has_condition=False, now=0.0)
        g.record_delivery("b", now=0.0)
        ok, _ = g.decide("b", has_condition=False, now=7200.0)
        assert ok  # 2h beats the 30-min base; no daily escalation applies

    def test_an_hour_of_quiet_makes_the_next_raise_a_new_incident(self):
        """A condition that cleared and returned is news again, immediately —
        not an heir to last week's cooldown."""
        g = self.gate()
        g.decide("b", has_condition=True, now=0.0)
        g.record_delivery("b", now=0.0)
        g.decide("b", has_condition=True, now=100.0)  # suppressed repeat
        ok, suffix = g.decide("b", has_condition=True, now=100.0 + 3700.0)
        assert ok and suffix == ""

    def test_the_cooldown_is_paid_only_on_delivery(self):
        """No record_delivery — the attempt failed — so the next raise tries
        again instead of inheriting silence."""
        g = self.gate()
        g.decide("b", has_condition=True, now=0.0)  # decided, never delivered
        ok, _ = g.decide("b", has_condition=True, now=60.0)
        assert ok

    def test_buckets_do_not_mask_each_other(self):
        g = self.gate()
        g.decide("disk:WARN", has_condition=True, now=0.0)
        g.record_delivery("disk:WARN", now=0.0)
        ok, _ = g.decide("disk:CRITICAL", has_condition=True, now=60.0)
        assert ok  # the escalation the old shared float used to mute


class TestRaiseAlert:
    def setup_method(self):
        reset_gate()

    def teardown_method(self):
        reset_gate()

    @pytest.mark.asyncio
    async def test_delivers_through_the_shared_channel_pre_throttled(self):
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)) as send:
            delivered = await raise_alert(
                "боль", conditions=["disk:WARN"], bucket="disk:WARN",
            )
        assert delivered == 2
        assert send.await_args.kwargs["pre_throttled"] is True

    @pytest.mark.asyncio
    async def test_no_bucket_means_no_dedup(self):
        """The OOM contract: the caller already decided this must go."""
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=1)) as send:
            for _ in range(3):
                await raise_alert("💥", conditions=[], bucket=None)
        assert send.await_count == 3

    @pytest.mark.asyncio
    async def test_a_suppressed_raise_reports_zero(self):
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)):
            assert await raise_alert(
                "x", conditions=["disk:WARN"], bucket="disk:WARN") == 2
            assert await raise_alert(
                "x", conditions=["disk:WARN"], bucket="disk:WARN") == 0

    @pytest.mark.asyncio
    async def test_an_unregistered_key_is_tolerated_and_logged(self, caplog):
        import logging

        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=1)):
            with caplog.at_level(logging.WARNING, logger="core.alerting"):
                delivered = await raise_alert(
                    "x", conditions=["nobody_declared_this"],
                    bucket="nobody_declared_this",
                )
        assert delivered == 1
        assert "unregistered condition key" in caplog.text

    @pytest.mark.asyncio
    async def test_failed_delivery_does_not_buy_silence(self):
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=0)):
            assert await raise_alert(
                "x", conditions=["disk:WARN"], bucket="disk:WARN") == 0
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)):
            assert await raise_alert(
                "x", conditions=["disk:WARN"], bucket="disk:WARN") == 2


class TestPreThrottledChannel:
    @pytest.mark.asyncio
    async def test_pre_throttled_skips_the_text_throttle_not_the_kill_switch(
        self, monkeypatch,
    ):
        from core import telegram_alerts

        bot_main = __import__("importlib").import_module("bot.main")
        monkeypatch.setenv(telegram_alerts.DISABLE_ENV, "1")
        assert await bot_main.send_admin_message(
            "x", pre_throttled=True) == 0  # policy upstream, channel gated

    @pytest.mark.asyncio
    async def test_pre_throttled_identical_texts_all_pass(self, monkeypatch):
        from core import telegram_alerts

        bot_main = __import__("importlib").import_module("bot.main")
        telegram_alerts.reset_throttle()
        monkeypatch.setattr(bot_main, "_application", None)
        with patch("core.telegram_alerts.send_admin_message_http",
                   new=AsyncMock(return_value=1)) as http:
            await bot_main.send_admin_message("same", pre_throttled=True)
            await bot_main.send_admin_message("same", pre_throttled=True)
        assert http.await_count == 2
