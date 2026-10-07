"""Chain 7b-3 without a database: the goal and forecast tables' writer.

`core/pg_forecast_write.py` moves the writes of `app.seasonal_indices`,
`app.growth_metrics`, `app.weekly_patterns` and `app.revenue_predictions` to
Postgres behind `KS_WRITE_FORECAST`. What is proved here needs no server: the
flag and its four preconditions, the refusals taken before the latch, the
repository's routing, the one-statement read of the three goal tables, and the
scheduler slots the standing watch judges staleness by. The SQL itself is
proved against a real Postgres in `tests/integration/test_forecast_writer.py`.
"""
from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler


def _registered():
    """A scheduler with every production job registered and none started."""
    from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    return s


def _fields(trigger) -> dict:
    return {f.name: str(f) for f in trigger.fields}


class TestTheSlotsAreTheSchedulers:
    """The standing watch judges the four tables stale against the last slot
    each writer was due at. Those slots are read from the constants the
    scheduler registers its jobs with, so the watch cannot judge by a time the
    scheduler no longer fires at."""

    def test_training_fires_on_the_exported_schedule(self):
        from apscheduler.triggers.cron import CronTrigger
        from core.scheduler import REVENUE_TRAIN_SCHEDULE, SCHEDULER_TIMEZONE

        job = _registered()._scheduler.get_job("revenue_prediction_train")
        assert _fields(job.trigger) == _fields(
            CronTrigger(**REVENUE_TRAIN_SCHEDULE, timezone=SCHEDULER_TIMEZONE))

    def test_the_monday_job_fires_on_the_exported_schedule(self):
        from apscheduler.triggers.cron import CronTrigger
        from core.scheduler import SCHEDULER_TIMEZONE, SEASONALITY_SCHEDULE

        job = _registered()._scheduler.get_job("seasonality_calc")
        assert _fields(job.trigger) == _fields(
            CronTrigger(**SEASONALITY_SCHEDULE, timezone=SCHEDULER_TIMEZONE))

    def test_training_is_twice_a_week_not_daily(self):
        """CLAUDE.md said "retrains daily at 3:30" for months. A watch built on
        that sentence would page every Tuesday, Wednesday, Friday, Saturday
        and Sunday."""
        from core.scheduler import REVENUE_TRAIN_SCHEDULE

        assert REVENUE_TRAIN_SCHEDULE["day_of_week"] == "mon,thu"
        assert (REVENUE_TRAIN_SCHEDULE["hour"],
                REVENUE_TRAIN_SCHEDULE["minute"]) == (3, 30)
