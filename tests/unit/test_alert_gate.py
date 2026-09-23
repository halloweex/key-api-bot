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


class TestOneSendPerBucketAtATime:
    """`decide` commits nothing and `record_delivery` runs after the transport
    returns, so two raises for one bucket inside a single Telegram round trip
    both used to look fresh: the two-minute refresh and a manual one failing
    validation together produced two messages and two agent diagnoses."""

    def test_a_bucket_in_flight_is_not_claimed_twice(self):
        gate = AlertGate()
        assert gate.decide("b", has_condition=True, now=1000.0) == (True, "")
        assert gate.claim("b") is True
        # No delivery recorded yet — `decide` still says yes, and did before.
        assert gate.decide("b", has_condition=True, now=1001.0)[0] is True
        assert gate.claim("b") is False
        assert gate._state["b"].suppressed == 1, "it rides the repeat counter"

    def test_release_reopens_the_bucket(self):
        gate = AlertGate()
        gate.decide("b", has_condition=True, now=1000.0)
        gate.claim("b")
        gate.release("b")
        assert gate.claim("b") is True

    def test_other_buckets_are_unaffected(self):
        gate = AlertGate()
        gate.claim("b")
        assert gate.claim("c") is True

    @pytest.mark.asyncio
    async def test_two_concurrent_raises_send_once(self):
        import asyncio

        reset_gate()
        try:
            async def _slow_send(*args, **kwargs):
                await asyncio.sleep(0.05)
                return 2

            with patch("bot.main.send_admin_message", new=AsyncMock(side_effect=_slow_send)) as send:
                results = await asyncio.gather(
                    raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN"),
                    raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN"),
                )
            assert sorted(results) == [0, 2]
            assert send.await_count == 1
        finally:
            reset_gate()

    @pytest.mark.asyncio
    async def test_a_transport_that_raises_does_not_leave_the_bucket_stuck(self):
        reset_gate()
        try:
            with patch("bot.main.send_admin_message",
                       new=AsyncMock(side_effect=RuntimeError("telegram down"))):
                with pytest.raises(RuntimeError):
                    await raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN")
            with patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)):
                assert await raise_alert(
                    "x", conditions=["disk:WARN"], bucket="disk:WARN") == 2
        finally:
            reset_gate()


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


class TestTheEmitterSetsTheRhythm:
    """"An hour of quiet = a new incident" holds only for emitters that run
    more often than hourly. The disk watchdog and the integrity layer run every
    six hours, the reconciliation layers daily, so between two of their passes
    there is always more than an hour of quiet — and every pass of theirs was a
    new incident: loud, and a fresh summons for the diagnostic agent.

    Measured 2026-09-23: fourteen disk deliveries, fourteen agent runs, standing
    WARNs every 6.0 h, and the agent's API credit spent to nothing on 09-22.
    """

    SIX_H = 6 * 3600.0
    DAY = 86400.0

    @staticmethod
    def replay(gate, times, cadence_s, bucket="disk:WARN"):
        """Raise at each time; deliver whatever the Gate admits."""
        sent = []
        for t in times:
            ok, suffix = gate.decide(bucket, has_condition=True, now=t,
                                     cadence_s=cadence_s)
            if ok:
                sent.append((t, suffix))
                gate.record_delivery(bucket, now=t)
        return sent

    def test_a_six_hour_emitter_reminds_once_a_day_not_every_pass(self):
        """The 2026-09-18..23 WARN, replayed: one raise every six hours for
        five days. One loud first message, then one reminder a day."""
        times = [i * self.SIX_H for i in range(21)]  # 0h .. 120h
        sent = self.replay(AlertGate(), times, self.SIX_H)

        assert [t / 3600 for t, _ in sent] == [0, 24, 48, 72, 96, 120]
        # Exactly one fresh incident — the only raise that summons the agent.
        assert [sfx for _, sfx in sent].count("") == 1
        assert all("standing" in sfx for _, sfx in sent[1:])

    def test_without_a_cadence_the_old_rule_still_applies(self):
        """What every slow emitter got until now, pinned so the difference is
        visible: every six-hour pass a new incident, every one loud. Callers
        outside the scheduler — a webhook, a startup path — still get this,
        which is right for them: they have no rhythm to measure quiet by."""
        times = [i * self.SIX_H for i in range(5)]
        sent = self.replay(AlertGate(), times, None)

        assert len(sent) == 5
        assert all(sfx == "" for _, sfx in sent)

    def test_a_return_after_a_clear_is_loud_again(self):
        """The case the reset cannot be allowed to swallow. The condition
        holds for two passes, one pass finds it clear (the emitter resolves
        it and a "✅ Resolved" goes out), and the next pass finds it back. The
        reader was just told it cleared; silence now would be a lie.

        This is why the reset is one period, not two: the earliest a
        condition can come back after being seen clear is two periods after
        its last raise, and one period plus the hour keeps that loud."""
        g = AlertGate()
        self.replay(g, [0.0, self.SIX_H], self.SIX_H)
        # 12h: a pass that saw the condition clear raises nothing.
        ok, suffix = g.decide("disk:WARN", has_condition=True,
                              now=3 * self.SIX_H, cadence_s=self.SIX_H)
        assert ok and suffix == ""

    def test_cron_jitter_does_not_push_the_reminder_a_period_late(self):
        """The first delivery lands a second after its slot and the pass a
        day later lands exactly on it, so the gap is 24h minus a second.
        Without slack that reminder waits for the next pass: 30h for this
        emitter, 48h for a daily one, depending on which side of the second
        the scheduler woke."""
        g = AlertGate()
        g.decide("disk:WARN", has_condition=True, now=1.0, cadence_s=self.SIX_H)
        g.record_delivery("disk:WARN", now=1.0)
        for i in (1, 2, 3):
            assert g.decide("disk:WARN", has_condition=True, now=i * self.SIX_H,
                            cadence_s=self.SIX_H)[0] is False
        ok, suffix = g.decide("disk:WARN", has_condition=True, now=self.DAY,
                              cadence_s=self.SIX_H)
        assert ok and "standing 23h" in suffix

    def test_a_daily_emitter_reminds_daily_and_summons_the_agent_once(self):
        times = [i * self.DAY for i in range(5)]
        sent = self.replay(AlertGate(), times, self.DAY,
                           bucket="dq:reconciliation:CRITICAL:x")

        assert len(sent) == 5
        assert [sfx for _, sfx in sent].count("") == 1
        assert all("standing" in sfx for _, sfx in sent[1:])

    def test_a_fast_emitter_is_unchanged(self):
        """The validator's two-minute rhythm, replayed exactly as the original
        daily-reminder test does. A cadence this short moves the reset by two
        minutes and the reminder by one — nothing a reader could notice."""
        sent = self.replay(AlertGate(), [float(t) for t in range(0, 25 * 3600, 120)],
                           120.0, bucket="b")
        assert [t for t, _ in sent if t < 3600] == [0, 1800]
        assert len([t for t, _ in sent if t >= 3600]) == 1


class TestRaiseAlertHearsTheRhythm:
    def teardown_method(self):
        reset_gate()

    @pytest.mark.asyncio
    async def test_the_cadence_in_context_reaches_the_gate(self):
        from core.alerting import ALERT_CADENCE_S, _gate

        token = ALERT_CADENCE_S.set(21600.0)
        try:
            with patch.object(_gate, "decide",
                              wraps=_gate.decide) as decide, \
                 patch("bot.main.send_admin_message",
                       new=AsyncMock(return_value=1)):
                await raise_alert("x", conditions=["disk:WARN"],
                                  bucket="disk:WARN")
        finally:
            ALERT_CADENCE_S.reset(token)
        assert decide.call_args.kwargs["cadence_s"] == 21600.0

    @pytest.mark.asyncio
    async def test_outside_a_job_there_is_no_cadence(self):
        from core.alerting import _gate

        with patch.object(_gate, "decide", wraps=_gate.decide) as decide, \
             patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=1)):
            await raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN")
        assert decide.call_args.kwargs["cadence_s"] is None
