"""Every scheduled job raises its alerts at its own rhythm.

The alert Gate decides what is a new incident by how long an emitter has been
quiet, and "quiet" only means something against how often that emitter runs.
The scheduler supplies it: each job is wrapped at registration so that
`core.alerting.ALERT_CADENCE_S` holds that job's cadence, derived from its own
trigger, for as long as the job runs.

Derived and walked, not listed. Four slow emitters were missed by the rule
this replaces; a fifth must not be missable.
"""
import asyncio
import inspect
from datetime import datetime, timedelta

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


def _registered():
    """A scheduler with every production job registered and none started."""
    from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    return s


class TestEveryJobCarriesItsCadence:
    def test_every_registered_job_runs_through_the_wrapper(self):
        """Walks whatever `_register_jobs` registers today. A job added
        tomorrow is in this list the moment it exists."""
        jobs = _registered()._scheduler.get_jobs()
        assert len(jobs) > 20, "the registry looks empty — has it moved?"
        unwrapped = [j.id for j in jobs if not hasattr(j.func, "__wrapped__")]
        assert unwrapped == [], (
            f"{unwrapped} bypass the cadence wrapper; their alerts would reach "
            f"the Gate with no rhythm and every slow pass would be news again"
        )

    def test_every_job_is_a_coroutine_function(self):
        """The wrapper awaits what it wraps. A synchronous job would return a
        value where a coroutine is expected and fail on its first run."""
        jobs = _registered()._scheduler.get_jobs()
        sync = [j.id for j in jobs
                if not inspect.iscoroutinefunction(j.func.__wrapped__)]
        assert sync == [], f"synchronous jobs need their own path: {sync}"

    def test_the_slow_emitters_get_the_rhythm_their_triggers_give(self):
        """The four whose alerts arrived every pass as news. Named here only
        as the record of why this exists; the two tests above are what keeps
        the next one covered."""
        s = _registered()
        cadence = {j.id: s._trigger_cadence_s(j.trigger)
                   for j in s._scheduler.get_jobs()}
        assert cadence["disk_watchdog"] == 6 * 3600
        assert cadence["dq_integrity_check"] == 6 * 3600
        assert cadence["dq_reconciliation"] == 24 * 3600
        assert cadence["dq_mirror_landing"] == 24 * 3600


class TestTheWrapper:
    @staticmethod
    def _probe(trigger):
        """Register a job that records the cadence it ran under."""
        from core.alerting import ALERT_CADENCE_S
        from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

        seen = []

        async def probe():
            seen.append(ALERT_CADENCE_S.get())

        s = BackgroundScheduler()
        s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
        s._add_job(job_id="probe", name="probe", description="probe",
                   func=probe, trigger=trigger)
        return s, s._scheduler.get_job("probe"), seen

    def test_the_job_runs_under_its_triggers_cadence(self):
        from core.scheduler import INVARIANT_CHECK_HOURS, SCHEDULER_TIMEZONE

        _, job, seen = self._probe(
            CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=SCHEDULER_TIMEZONE))
        asyncio.run(job.func())
        assert seen == [6 * 3600]

    def test_the_cadence_does_not_leak_past_the_job(self):
        from core.alerting import ALERT_CADENCE_S

        _, job, _ = self._probe(IntervalTrigger(minutes=2))

        async def run_then_look():
            await job.func()
            return ALERT_CADENCE_S.get()

        assert asyncio.run(run_then_look()) is None

    def test_a_catch_up_carries_the_scheduled_jobs_cadence(self):
        """The catch-up re-adds `job.func` under a one-shot DateTrigger, which
        has no rhythm of its own. The cadence was captured at registration, so
        a caught-up integrity run still raises at six hours."""
        from core.scheduler import INVARIANT_CHECK_HOURS, SCHEDULER_TIMEZONE

        s, job, seen = self._probe(
            CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=SCHEDULER_TIMEZONE))
        s._scheduler.add_job(
            job.func, trigger=DateTrigger(
                run_date=datetime.now(SCHEDULER_TIMEZONE) + timedelta(hours=1)),
            id="probe_catchup")
        asyncio.run(s._scheduler.get_job("probe_catchup").func())
        assert seen == [6 * 3600]

    def test_a_one_shot_trigger_has_no_cadence(self):
        from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

        assert BackgroundScheduler._trigger_cadence_s(
            DateTrigger(run_date=datetime.now(SCHEDULER_TIMEZONE))) is None
