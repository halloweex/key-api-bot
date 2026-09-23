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


class TestAResolveEndsTheIncident:
    """Timing alone cannot tell a standing condition from one that cleared
    and came back: a clean pass off the schedule — the recheck button, a
    manual trigger, a catch-up — resolves between two slots, and the next slot
    lands well inside any quiet window. Found by review, reproduced before the
    fix: resolved at 02:00, back at 07:00, silent until 01:00 the next day.
    """

    H = 3600.0

    @staticmethod
    def raise_(g, bucket, conds, t, cadence, group="g", requested=False):
        ok, sfx = g.decide(bucket, has_condition=True, now=t, cadence_s=cadence,
                           conditions=conds, requested=requested)
        if ok:
            g.record_delivery(bucket, now=t)
            g.note_delivered_conditions(conds, group, now=t)
        return ok, sfx

    def test_a_clean_pass_off_the_schedule_makes_the_return_news(self):
        g = AlertGate()
        self.raise_(g, "disk:CRITICAL", ["disk:CRITICAL"], 0.0, 6 * self.H, "disk")
        assert g.take_resolved("disk", [], now=1 * self.H)  # "✅ Resolved"
        ok, sfx = self.raise_(g, "disk:CRITICAL", ["disk:CRITICAL"],
                              6 * self.H, 6 * self.H, "disk")
        assert ok and sfx == ""

    def test_a_check_that_dropped_below_critical_and_returned_is_news(self):
        """The integrity fingerprint names every issue, WARN included, so a
        check can leave the CRITICAL set, be announced resolved, and come back
        under the very same bucket."""
        g = AlertGate()
        b = "dq:integrity:CRITICAL:fk_orphan_order_products_order_id,pg_silver_missing_rows"
        self.raise_(g, b, ["fk_orphan_order_products_order_id", "pg_silver_missing_rows"], 0.0, 6 * self.H)
        self.raise_(g, b, ["fk_orphan_order_products_order_id"], 6 * self.H, 6 * self.H)
        g.take_resolved("g", ["fk_orphan_order_products_order_id"], now=6 * self.H)
        ok, sfx = self.raise_(g, b, ["fk_orphan_order_products_order_id", "pg_silver_missing_rows"],
                              12 * self.H, 6 * self.H)
        assert ok and sfx == ""

    def test_a_check_newly_critical_in_a_standing_bucket_is_news(self):
        g = AlertGate()
        b = "dq:integrity:CRITICAL:fk_orphan_order_products_order_id,freshness_orders"
        self.raise_(g, b, ["fk_orphan_order_products_order_id"], 0.0, 6 * self.H)
        ok, sfx = self.raise_(g, b, ["fk_orphan_order_products_order_id", "freshness_orders"], 6 * self.H, 6 * self.H)
        assert ok and sfx == ""

    def test_a_standing_condition_nobody_resolved_stays_standing(self):
        g = AlertGate()
        self.raise_(g, "disk:WARN", ["disk:WARN"], 0.0, 6 * self.H, "disk")
        ok, _ = self.raise_(g, "disk:WARN", ["disk:WARN"], 6 * self.H,
                            6 * self.H, "disk")
        assert ok is False

    def test_events_are_untouched_by_the_resolved_check(self):
        """Events never enter `_delivered`, by kind. Reading their absence as
        "resolved" would make every repeat of an event a new incident."""
        g = AlertGate()
        ev = "warehouse:backup_failed"
        ok, _ = g.decide(ev, has_condition=False, now=0.0, conditions=[ev])
        g.record_delivery(ev, now=0.0)
        g.note_delivered_conditions([ev], "warehouse", now=0.0)
        ok, _ = g.decide(ev, has_condition=False, now=600.0, conditions=[ev])
        assert ok is False  # still inside the 30-minute base cooldown


class TestARequestedRecheckDeliversItsVerdict:
    """The button promises "результат придёт отдельным сообщением". A recheck
    that still fails, inside a standing cooldown, used to be held — leaving the
    executor's green tick as the only reply, which reads as fixed."""

    H = 3600.0

    def test_the_verdict_goes_out_inside_the_cooldown(self):
        g = AlertGate()
        g.decide("b", has_condition=True, now=0.0, cadence_s=6 * self.H,
                 conditions=["fk_orphan_order_products_order_id"])
        g.record_delivery("b", now=0.0)
        g.note_delivered_conditions(["fk_orphan_order_products_order_id"], "g", now=0.0)
        ok, sfx = g.decide("b", has_condition=True, now=2 * self.H,
                           cadence_s=6 * self.H, conditions=["fk_orphan_order_products_order_id"],
                           requested=True)
        assert ok
        # Same incident: a non-empty suffix never summons the agent again.
        assert sfx != "" and "recheck on request" in sfx

    def test_an_unrequested_pass_at_the_same_moment_is_held(self):
        g = AlertGate()
        g.decide("b", has_condition=True, now=0.0, cadence_s=6 * self.H,
                 conditions=["fk_orphan_order_products_order_id"])
        g.record_delivery("b", now=0.0)
        g.note_delivered_conditions(["fk_orphan_order_products_order_id"], "g", now=0.0)
        ok, _ = g.decide("b", has_condition=True, now=2 * self.H,
                         cadence_s=6 * self.H, conditions=["fk_orphan_order_products_order_id"])
        assert ok is False


class TestTheBucketKeepsItsOwnRhythm:
    H = 3600.0

    def test_a_fast_bucket_raised_from_a_slow_job_keeps_the_fast_window(self):
        """The warehouse validator raises from the two-minute tick and from
        inside the daily status refresh. Its quiet is judged by the tick."""
        g = AlertGate()
        g.decide("wh", has_condition=True, now=0.0, cadence_s=120.0)
        g.record_delivery("wh", now=0.0)
        ok, sfx = g.decide("wh", has_condition=True, now=3 * self.H,
                           cadence_s=86400.0)
        assert ok and sfx == ""  # 3h > 1h + 2min: new, not a 25h window

    def test_the_reset_is_exactly_one_hour_past_one_period(self):
        """Pins the hour of slack on top of the period — a mutant that drops
        it survived review."""
        six = 6 * self.H
        g = AlertGate()
        for b in ("inside", "outside"):
            g.decide(b, has_condition=True, now=0.0, cadence_s=six)
            g.record_delivery(b, now=0.0)
        inside = g.decide("inside", has_condition=True,
                          now=six + self.H - 1, cadence_s=six)
        outside = g.decide("outside", has_condition=True,
                           now=six + self.H + 1, cadence_s=six)
        assert inside[0] is False
        assert outside == (True, "")

    def test_the_slack_is_for_standing_reminders_only(self):
        """The loud-phase 30 minutes is not shortened by a cadence — another
        mutant that survived review."""
        g = AlertGate()
        g.decide("b", has_condition=True, now=0.0, cadence_s=600.0)
        g.record_delivery("b", now=0.0)
        ok, _ = g.decide("b", has_condition=True, now=26 * 60.0, cadence_s=600.0)
        assert ok is False
