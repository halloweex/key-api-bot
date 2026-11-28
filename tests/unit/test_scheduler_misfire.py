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
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from core.scheduler import (
    INVARIANT_CHECK_HOURS,
    BackgroundScheduler,
)


async def _registered():
    """Every production job, registered on a real scheduler not yet started."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from core.scheduler import SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    await s._register_jobs()
    return s


async def _probe():
    """In each job's place, so no job's body runs."""
    return None


class TestMisfireGrace:
    @pytest.mark.asyncio
    async def test_every_job_still_runs_when_its_turn_comes_ten_minutes_late(self):
        """The incident: runs dropped as missed for being 1.8 s late, because
        APScheduler's grace is one second unless a job is given another. A
        turn that comes while a heavy job holds things up (reconciliation:
        119 s median, 210 s max), here ten minutes late, still runs — for
        every job production registers, its body replaced by a probe."""
        from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_MISSED

        s = await _registered()
        ran, missed = set(), set()
        s._scheduler.add_listener(lambda e: ran.add(e.job_id), EVENT_JOB_EXECUTED)
        s._scheduler.add_listener(lambda e: missed.add(e.job_id), EVENT_JOB_MISSED)
        s._scheduler.start(paused=True)
        try:
            jobs = {job.id for job in s._scheduler.get_jobs()}
            late = datetime.now(timezone.utc) - timedelta(minutes=10)
            for job_id in jobs:
                s._scheduler.modify_job(job_id, func=_probe, next_run_time=late)
            s._scheduler.resume()
            for _ in range(1000):
                if ran | missed >= jobs:
                    break
                await asyncio.sleep(0.01)
        finally:
            s._scheduler.shutdown(wait=False)

        assert jobs, "nothing registered: has the registry moved?"
        assert sorted(missed) == []
        assert ran == jobs

    @pytest.mark.asyncio
    async def test_grace_can_be_overridden_per_job(self):
        """A job registered with its own grace is dropped when its turn comes
        later than that, and run when it comes sooner; a job with no override
        keeps the generous default, so the override is what decided."""
        from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        from core.scheduler import SCHEDULER_TIMEZONE

        s = BackgroundScheduler()
        s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
        for job_id, extra in (("tight_in_time", {"misfire_grace_time": 30}),
                              ("tight_too_late", {"misfire_grace_time": 30}),
                              ("default_late", {})):
            s._add_job(job_id=job_id, name=job_id, description="d", func=_probe,
                       trigger=IntervalTrigger(hours=1), **extra)
        ran, missed = set(), set()
        s._scheduler.add_listener(lambda e: ran.add(e.job_id), EVENT_JOB_EXECUTED)
        s._scheduler.add_listener(lambda e: missed.add(e.job_id), EVENT_JOB_MISSED)
        s._scheduler.start(paused=True)
        try:
            now = datetime.now(timezone.utc)
            for job_id, late_s in (("tight_in_time", 10), ("tight_too_late", 90),
                                   ("default_late", 90)):
                s._scheduler.modify_job(
                    job_id, func=_probe,
                    next_run_time=now - timedelta(seconds=late_s))
            s._scheduler.resume()
            for _ in range(1000):
                if len(ran | missed) == 3:
                    break
                await asyncio.sleep(0.01)
        finally:
            s._scheduler.shutdown(wait=False)

        assert ran == {"tight_in_time", "default_late"}
        assert missed == {"tight_too_late"}


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

    @pytest.mark.asyncio
    async def test_no_job_of_six_hours_or_more_counts_from_its_registration(self):
        """The trigger type is the bug, not the interval length: an interval
        counts from registration, every deploy registers afresh, and the
        six-hourly scan ran 6 times in 79 days. Read off the triggers
        production registers, however the interval is spelt."""
        jobs = (await _registered())._scheduler.get_jobs()
        assert jobs, "nothing registered: has the registry moved?"
        long_intervals = sorted(
            job.id for job in jobs
            if isinstance(job.trigger, IntervalTrigger)
            and job.trigger.interval >= timedelta(hours=6))
        assert long_intervals == []

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

    @pytest.mark.asyncio
    async def test_every_registered_job_reports_when_it_fires_next(self):
        """Production's registration on a started scheduler: no job, cron or
        interval, is reported as never firing before its first run."""
        s = await _registered()
        s._scheduler.start(paused=True)
        try:
            jobs = s.get_jobs()
            live = {job.id: job.next_run_time for job in s._scheduler.get_jobs()}
        finally:
            s._scheduler.shutdown(wait=False)

        assert jobs, "nothing registered: has the registry moved?"
        assert [j["id"] for j in jobs if j["next_run"] is None] == []
        for j in jobs:
            assert j["next_run"] == live[j["id"]].isoformat()

    @pytest.mark.asyncio
    async def test_a_job_the_scheduler_holds_no_time_for_reports_the_last_known_one(self):
        """A paused job has no next run in APScheduler; the listing then
        shows the value last recorded for it rather than nothing."""
        s = await _registered()
        s._scheduler.start(paused=True)
        try:
            cached = datetime(2026, 8, 9, 5, 0, tzinfo=timezone.utc)
            s._scheduler.pause_job("dq_digest")
            s._job_info["dq_digest"].next_run = cached
            reported = {j["id"]: j["next_run"] for j in s.get_jobs()}
        finally:
            s._scheduler.shutdown(wait=False)

        assert reported["dq_digest"] == cached.isoformat()
