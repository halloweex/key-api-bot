"""Jobs that never run are worse than jobs that run late.

Measured on production: the 6-hourly integrity scan executed 6 times in 79 days
against ~308 scheduled, `disk_samples` holds one row ever, and the logs show 108
`Job bronze_promotion missed` in a single five-hour window — dropped for being
1.8 seconds late.

Two causes, both here. APScheduler's `misfire_grace_time` defaults to ONE SECOND
and was never overridden. And `IntervalTrigger` computes its next fire from
registration, while `_add_job` re-registers with `replace_existing=True` on every
scheduler start — so every deploy pushed a six-hour job six hours further out.
`CronTrigger` computes from the wall clock and is immune, which is why the daily
cron reconciliation managed 68/79 days while the interval jobs managed 6.
"""
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from core.scheduler import (
    DEFAULT_MISFIRE_GRACE_SECONDS,
    INVARIANT_CHECK_HOURS,
    BackgroundScheduler,
)


class TestMisfireGrace:
    def test_grace_is_generous_enough_to_outlast_a_heavy_job(self):
        """Reconciliation runs 119s median / 210s max and holds the heavy lock.
        A grace shorter than that drops whatever is due while it runs."""
        assert DEFAULT_MISFIRE_GRACE_SECONDS >= 3600

    def test_every_job_is_registered_with_an_explicit_grace(self, monkeypatch):
        added = []

        class FakeScheduler:
            def add_job(self, func, **kwargs):
                added.append(kwargs)

            def get_job(self, job_id):
                return None

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler()
        scheduler._add_job(
            job_id="x", name="X", description="d",
            func=lambda: None, trigger=IntervalTrigger(minutes=2),
        )

        assert added[0]["misfire_grace_time"] == DEFAULT_MISFIRE_GRACE_SECONDS

    def test_grace_can_be_overridden_per_job(self):
        added = []

        class FakeScheduler:
            def add_job(self, func, **kwargs):
                added.append(kwargs)

            def get_job(self, job_id):
                return None

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler()
        scheduler._add_job(
            job_id="x", name="X", description="d", func=lambda: None,
            trigger=IntervalTrigger(minutes=2), misfire_grace_time=30,
        )

        assert added[0]["misfire_grace_time"] == 30


class TestInvariantJobsUseCron:
    def test_hours_avoid_the_dst_window(self):
        """Europe/Kyiv shifts at 03:00 and 04:00. A cron job at either hour is
        skipped or doubled twice a year."""
        hours = {int(h) for h in INVARIANT_CHECK_HOURS.split(",")}

        assert not (hours & {3, 4})
        assert len(hours) == 4

    def test_no_six_hourly_interval_triggers_survive(self):
        """The trigger type is the bug, not the interval length."""
        import inspect

        import core.scheduler as mod

        source = inspect.getsource(mod.BackgroundScheduler._register_jobs)
        assert "IntervalTrigger(hours=6)" not in source

    def test_cron_next_fire_does_not_move_when_re_registered(self):
        """The property that makes Cron immune to the deploy reset: its next
        fire depends on the wall clock, not on when the job was added."""
        from datetime import datetime, timedelta

        from zoneinfo import ZoneInfo

        kyiv = ZoneInfo("Europe/Kyiv")
        trigger = CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=kyiv)
        now = datetime(2026, 8, 8, 8, 0, tzinfo=kyiv)

        first = trigger.get_next_fire_time(None, now)
        # Re-registration an hour later, as a deploy would do.
        later = trigger.get_next_fire_time(None, now + timedelta(hours=1))

        assert first == later

    def test_an_interval_trigger_does_move_when_re_registered(self):
        """Contrast, so the reason for the change is pinned by a test.

        _register_jobs constructs a FRESH IntervalTrigger on every scheduler
        start, and its start_date defaults to construction time. So each deploy
        pushed the next fire another six hours out — which is the whole reason
        six-hourly jobs ran 6 times in 79 days.
        """
        from datetime import datetime, timedelta

        from zoneinfo import ZoneInfo

        kyiv = ZoneInfo("Europe/Kyiv")
        boot = datetime(2026, 8, 8, 8, 0, tzinfo=kyiv)
        redeploy = boot + timedelta(hours=1)

        at_boot = IntervalTrigger(hours=6, timezone=kyiv, start_date=boot)
        at_redeploy = IntervalTrigger(hours=6, timezone=kyiv, start_date=redeploy)

        first = at_boot.get_next_fire_time(None, boot)
        after = at_redeploy.get_next_fire_time(None, redeploy)

        assert after > first, "this is why six-hourly jobs stopped running"

        cron = CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=kyiv)
        assert (cron.get_next_fire_time(None, redeploy)
                == cron.get_next_fire_time(None, redeploy)), "cron is immune"
