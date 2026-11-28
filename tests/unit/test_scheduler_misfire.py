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
    def test_every_day_of_the_year_gets_the_same_number_of_checks(self):
        """Europe/Kyiv moves its clocks at 03:00 and 04:00 twice a year, and a
        cron hour inside the change is skipped or doubled that day. Production
        runs in Kyiv's zone; there, every day of 2026, the two the clocks moved
        on included, gets as many checks as any other."""
        from datetime import date, datetime, timedelta, timezone

        from core.scheduler import SCHEDULER_TIMEZONE as kyiv

        trigger = CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=kyiv)

        def midnight(day):
            return datetime(day.year, day.month, day.day, tzinfo=kyiv)

        def runs_on(day):
            end, runs = midnight(day + timedelta(days=1)), 0
            at = trigger.get_next_fire_time(None, midnight(day))
            for _ in range(48):
                if at is None or at >= end:
                    break
                runs += 1
                # A second on in absolute time: wall-clock arithmetic steps
                # back into the hour the clocks repeat.
                after = (at.astimezone(timezone.utc) + timedelta(seconds=1)).astimezone(kyiv)
                at = trigger.get_next_fire_time(at, after)
            return runs

        days = [date(2026, 1, 1) + timedelta(days=n) for n in range(365)]
        moved = [d for d in days
                 if midnight(d).utcoffset() != midnight(d + timedelta(days=1)).utcoffset()]
        assert len(moved) == 2, f"no clock change in 2026 to test against: {moved}"
        runs = {day: runs_on(day) for day in days}
        usual = runs[days[0]]
        assert usual > 0
        assert {day: n for day, n in runs.items() if n != usual} == {}

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


class TestNextRunReporting:
    """`next_run: null` on a healthy daily job is a lie the dashboard tells.

    `_add_job` samples `next_run_time` while registering — before
    `scheduler.start()`, when APScheduler has not computed one yet — and only
    the post-execution listener refreshes it. Every cron job therefore read
    "null" until its first run in the process, which is indistinguishable from
    "this job will never fire".
    """

    def _scheduler_with_job(self, next_run_time):
        from core.scheduler import JobInfo

        class FakeJob:
            trigger = "cron[hour='9', minute='0']"

            def __init__(self, nrt):
                self.next_run_time = nrt

        class FakeScheduler:
            def __init__(self, job):
                self._job = job

            def get_job(self, job_id):
                return self._job

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler(FakeJob(next_run_time))
        scheduler._job_info["dq_digest"] = JobInfo(
            id="dq_digest", name="DQ: Daily Digest", description="d",
        )
        return scheduler

    def test_next_run_comes_from_the_live_scheduler(self):
        from datetime import datetime, timezone

        when = datetime(2026, 8, 9, 9, 0, tzinfo=timezone.utc)
        jobs = self._scheduler_with_job(when).get_jobs()

        assert jobs[0]["next_run"] == when.isoformat()

    def test_falls_back_to_the_cached_value(self):
        from datetime import datetime, timezone

        scheduler = self._scheduler_with_job(None)
        cached = datetime(2026, 8, 9, 5, 0, tzinfo=timezone.utc)
        scheduler._job_info["dq_digest"].next_run = cached

        assert scheduler.get_jobs()[0]["next_run"] == cached.isoformat()
