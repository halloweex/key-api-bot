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

# The stage 5 sweep's target state, for the one class below that asks what is
# registered in it.
from tests.unit.test_duckdb_off_sweep import target_state  # noqa: F401 — fixture
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: F401 — fixture

# The instants every cadence below is read at, never the wall clock.
# `_trigger_cadence_s` takes the longest gap among a trigger's next twelve
# firings, so read today a six-hourly Kyiv cron says seven hours for the three
# days before the night the clocks go back, and a daily one 25 h for the
# twelve: the suite went red every October (25200.0, measured 2026-10-22).
# Kyiv leaves summer time at 04:00 on 2026-10-25 and enters it at 03:00 on
# 2026-03-29.
QUIET = (2026, 7, 15, 12)    # no clock change in the next twelve days
AUTUMN = (2026, 10, 23, 12)  # 01:00 → 07:00 on 25 Oct is seven hours
SPRING = (2026, 3, 26, 12)   # 01:00 → 07:00 on 29 Mar is five; six still wins


def _kyiv(at):
    from core.scheduler import SCHEDULER_TIMEZONE

    return datetime(*at, tzinfo=SCHEDULER_TIMEZONE)


@pytest.fixture
def cadence_read_at(monkeypatch):
    """Fix the instant `_add_job` reads a trigger's cadence at. It asks with
    no `now`, so without this the cadence the wrapper captures is today's."""
    from core.scheduler import BackgroundScheduler

    real = BackgroundScheduler._trigger_cadence_s

    def at(when):
        monkeypatch.setattr(
            BackgroundScheduler, "_trigger_cadence_s",
            staticmethod(lambda trigger, now=None: real(trigger, now or when)))

    return at


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
        the next one covered.

        Read at a quiet instant. These triggers carry the process's zone —
        UTC in CI, Europe/Kyiv in production — so on the wall clock this
        failed only under a zone with a clock change: a laptop in Kyiv in
        October."""
        s = _registered()
        cadence = {j.id: s._trigger_cadence_s(j.trigger, _kyiv(QUIET))
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

    @pytest.mark.parametrize("at, hours", [(QUIET, 6), (AUTUMN, 7)],
                             ids=["quiet", "clocks_go_back"])
    def test_the_job_runs_under_its_triggers_cadence(
            self, cadence_read_at, at, hours):
        """Both directions: six hours on a quiet week, seven across the night
        the clocks go back — what the trigger gives at registration, not a
        constant."""
        from core.scheduler import INVARIANT_CHECK_HOURS, SCHEDULER_TIMEZONE

        cadence_read_at(_kyiv(at))
        _, job, seen = self._probe(
            CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=SCHEDULER_TIMEZONE))
        asyncio.run(job.func())
        assert seen == [hours * 3600]

    def test_the_cadence_does_not_leak_past_the_job(self):
        from core.alerting import ALERT_CADENCE_S

        _, job, _ = self._probe(IntervalTrigger(minutes=2))

        async def run_then_look():
            await job.func()
            return ALERT_CADENCE_S.get()

        assert asyncio.run(run_then_look()) is None

    @pytest.mark.parametrize("at, hours", [(QUIET, 6), (AUTUMN, 7)],
                             ids=["quiet", "clocks_go_back"])
    def test_a_catch_up_carries_the_scheduled_jobs_cadence(
            self, cadence_read_at, at, hours):
        """The catch-up re-adds `job.func` under a one-shot DateTrigger, which
        has no rhythm of its own. The cadence was captured at registration, so
        a caught-up integrity run still raises at the scheduled run's six
        hours — seven when registered across the night the clocks go back."""
        from core.scheduler import INVARIANT_CHECK_HOURS, SCHEDULER_TIMEZONE

        cadence_read_at(_kyiv(at))
        s, job, seen = self._probe(
            CronTrigger(hour=INVARIANT_CHECK_HOURS, timezone=SCHEDULER_TIMEZONE))
        s._scheduler.add_job(
            job.func, trigger=DateTrigger(
                run_date=datetime.now(SCHEDULER_TIMEZONE) + timedelta(hours=1)),
            id="probe_catchup")
        asyncio.run(s._scheduler.get_job("probe_catchup").func())
        assert seen == [hours * 3600]

    def test_a_one_shot_trigger_has_no_cadence(self):
        from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

        assert BackgroundScheduler._trigger_cadence_s(
            DateTrigger(run_date=datetime.now(SCHEDULER_TIMEZONE))) is None


class TestTheCadenceIsElapsedTime:
    def test_an_interval_trigger_is_its_interval(self):
        from core.scheduler import BackgroundScheduler

        assert BackgroundScheduler._trigger_cadence_s(
            IntervalTrigger(minutes=7)) == 420.0

    def test_the_night_the_clocks_go_back_is_seven_hours(self):
        """Europe/Kyiv leaves summer time at 04:00 on 2026-10-25. The 01:00
        and 07:00 slots that night are seven hours apart; subtracting two
        aware datetimes in one zone gives wall time and reads six."""
        from core.scheduler import (
            BackgroundScheduler, INVARIANT_CHECK_HOURS, SCHEDULER_TIMEZONE,
        )

        trigger = CronTrigger(hour=INVARIANT_CHECK_HOURS,
                              timezone=SCHEDULER_TIMEZONE)
        before = datetime(2026, 10, 23, 12, 0, tzinfo=SCHEDULER_TIMEZONE)
        assert BackgroundScheduler._trigger_cadence_s(trigger, before) == 7 * 3600

    def test_the_night_the_clocks_go_forward_is_still_six_hours(self):
        """The longest gap, not the one across the change: on 2026-03-29 the
        01:00 and 07:00 slots are five hours apart, and the six beside them
        is what the Gate is told. Why only October ever turned this red."""
        from core.scheduler import (
            BackgroundScheduler, INVARIANT_CHECK_HOURS, SCHEDULER_TIMEZONE,
        )

        trigger = CronTrigger(hour=INVARIANT_CHECK_HOURS,
                              timezone=SCHEDULER_TIMEZONE)
        assert BackgroundScheduler._trigger_cadence_s(
            trigger, _kyiv(SPRING)) == 6 * 3600


class TestARequestedRunIsMarked:
    def test_the_run_a_human_asked_for_is_marked_and_only_that_one(self):
        from core.alerting import ALERT_REQUESTED
        from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

        seen = []

        async def probe():
            seen.append(ALERT_REQUESTED.get())

        s = BackgroundScheduler()
        s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
        s._add_job(job_id="probe", name="probe", description="probe",
                   func=probe, trigger=IntervalTrigger(hours=6))
        job = s._scheduler.get_job("probe")

        s._requested_runs.add("probe")   # what run_job_now records
        asyncio.run(job.func())
        asyncio.run(job.func())          # the next scheduled run
        assert seen == [True, False]

    def test_run_job_now_records_the_request(self):
        import inspect
        from core.scheduler import BackgroundScheduler

        src = inspect.getsource(BackgroundScheduler.run_job_now)
        assert "_requested_runs.add(job_id)" in src


class TestWhatIsNotRegisteredUnderDuckdbOff:
    """Stage 5, PR-7: under `KS_DUCKDB=off` the two jobs whose subject is the
    DuckDB file are not registered — a CHECKPOINT of a file nothing writes,
    and a daily copy of one frozen at the start of the week — and neither is
    DuckDB's warehouse tick, which step 13 already leaves out."""

    DUCKDB_JOBS = {"duckdb_checkpoint", "db_backup"}

    def test_under_on_both_are_registered(self):
        """The control. Mutation: drop either from `_register_jobs`."""
        ids = {j.id for j in _registered()._scheduler.get_jobs()}
        assert self.DUCKDB_JOBS <= ids

    def test_under_off_in_the_target_state_none_of_the_three_is(self, target_state):
        """Mutation: register either under off — the sweep's `job:db_backup`
        row comes back, and a CHECKPOINT runs against an unconnected store
        every hour for nothing."""
        ids = {j.id for j in _registered()._scheduler.get_jobs()}
        assert not ids & (self.DUCKDB_JOBS | {"warehouse_refresh"}), ids
        assert {"dq_integrity_check", "replicate_operational",
                "dq_mirror_landing"} <= ids
        from core import duckdb_switch

        assert duckdb_switch.opened() == {}
