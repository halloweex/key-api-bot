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
import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO = Path(__file__).resolve().parents[2]


def _scheduler():
    from core.scheduler import BackgroundScheduler

    s = BackgroundScheduler.__new__(BackgroundScheduler)
    s._scheduler = MagicMock()
    s._job_info = {}
    s._job_history = {}
    return s


class TestTheFirstRunCanBeBroughtForward:
    def test_a_delay_sets_an_aware_time_in_the_future(self):
        from apscheduler.triggers.interval import IntervalTrigger

        s = _scheduler()
        before = datetime.now(timezone.utc)
        s._add_job(
            job_id="j", name="n", description="d", func=lambda: None,
            trigger=IntervalTrigger(hours=1), first_run_delay_s=90,
        )
        when = s._scheduler.add_job.call_args.kwargs["next_run_time"]
        assert when.tzinfo is not None, "naive datetimes mean the wrong instant"
        assert before + timedelta(seconds=80) < when < before + timedelta(seconds=100)

    def test_without_one_the_trigger_is_left_alone(self):
        """Not a default: a job that fires on every restart runs five times on
        a deploy-heavy afternoon, which is right for a cheap idempotent copy
        and wrong for anything that costs."""
        from apscheduler.triggers.interval import IntervalTrigger

        s = _scheduler()
        s._add_job(
            job_id="j", name="n", description="d", func=lambda: None,
            trigger=IntervalTrigger(hours=1),
        )
        assert "next_run_time" not in s._scheduler.add_job.call_args.kwargs

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
    def test_the_operational_family_does_not_wait_an_hour(self):
        """It is the job that carries `bronze.expense_types`, whose absence
        collapses the `/expenses` breakdown into a single "Other"."""
        from core.scheduler import BackgroundScheduler

        src = inspect.getsource(BackgroundScheduler._register_jobs) \
            if hasattr(BackgroundScheduler, "_register_jobs") \
            else (REPO / "core" / "scheduler.py").read_text(encoding="utf-8")
        block = src.split('job_id="replicate_operational"', 1)[1].split(")\n\n", 1)[0]
        assert "first_run_delay_s" in block

    def test_it_is_used_sparingly(self):
        """One job today. If this grows, the reason for each has to grow with
        it — the cost of firing on every restart is paid per job."""
        src = (REPO / "core" / "scheduler.py").read_text(encoding="utf-8")
        uses = src.count("first_run_delay_s=")
        assert uses <= 3, f"{uses} jobs now fire on every restart — is each cheap?"
