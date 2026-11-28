"""A restart must not cost an hourly job its hour.

`IntervalTrigger(hours=1)` counts the first firing from the moment the
scheduler starts, and every deploy restarts the scheduler. Measured on
production 2026-09-08: five deploys in an afternoon, a gap of **138 minutes**
between two runs of the operational replication against a grace window of 90
sized for the interval, and a rollout held up three separate times waiting for
a copy that takes 116 ms.
"""
from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


@asynccontextmanager
async def _started(register):
    """A real scheduler after a start: `register(s)` adds the jobs, then
    APScheduler starts paused, so every first run is computed and none
    fires."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from core.scheduler import SCHEDULER_TIMEZONE, BackgroundScheduler

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    await register(s)
    s._scheduler.start(paused=True)
    try:
        yield {job.id: job.next_run_time for job in s._scheduler.get_jobs()}
    finally:
        s._scheduler.shutdown(wait=False)


class TestTheFirstRunCanBeBroughtForward:
    @pytest.mark.asyncio
    async def test_without_a_delay_an_hourly_job_waits_its_hour(self):
        """Not a default: a job that fires on every restart runs five times on
        a deploy-heavy afternoon, which is right for a cheap idempotent copy
        and wrong for anything that costs."""
        from apscheduler.triggers.interval import IntervalTrigger

        async def probe():
            return None

        async def register(s):
            s._add_job(job_id="hourly", name="n", description="d", func=probe,
                       trigger=IntervalTrigger(hours=1))

        started = datetime.now(timezone.utc)
        async with _started(register) as first_runs:
            when = first_runs["hourly"]
        assert started + timedelta(minutes=59) <= when <= started + timedelta(minutes=61)

    def test_timezone_is_imported_where_the_code_runs(self):
        """It was not, on the first cut: `timezone.utc` at module scope with
        only `datetime, timedelta` imported. That parses and raises the first
        time a job is registered — the f-string brace's failure mode."""
        src = (REPO / "core" / "scheduler.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        names = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "datetime"
            and node.col_offset == 0
            for alias in node.names
        }
        assert "timezone" in names


class TestTheReplicationUsesIt:
    @pytest.mark.asyncio
    async def test_the_operational_family_does_not_wait_an_hour(self):
        """Production's registration, as a restart leaves it: the hourly
        gender derivation (`replicate_operational`) first runs minutes after
        the start, not an hour, so a buyer added while web was down gets a
        verdict soon after it is back. Compared as instants: a naive time is
        read in the scheduler's zone (Kyiv) and, on a UTC host, lands hours
        early."""
        started = datetime.now(timezone.utc)
        async with _started(lambda s: s._register_jobs()) as first_runs:
            when = first_runs["replicate_operational"]
        assert started < when <= started + timedelta(minutes=5)

    def test_it_is_used_sparingly(self):
        """One job today. If this grows, the reason for each has to grow with
        it — the cost of firing on every restart is paid per job."""
        src = (REPO / "core" / "scheduler.py").read_text(encoding="utf-8")
        uses = src.count("first_run_delay_s=")
        assert uses <= 3, f"{uses} jobs now fire on every restart — is each cheap?"
