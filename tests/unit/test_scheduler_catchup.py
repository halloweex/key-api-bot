"""A job that was due while the process was down is not late — it is absent.

`CronTrigger` computes its next fire from the moment of registration. A
scheduler that comes up at 02:00:51 for a job due at 02:00:00 sets the next run
a full day out, and `misfire_grace_time` has nothing to forgive: there was no
live scheduler to dispatch it late.

Measured 2026-08-09: the host cron `0 2 * * 0 weekly_compact.sh` stopped both
containers at 02:00:07 UTC, swapped the DB, started them at 02:00:38, and the
scheduler logged "Background scheduler started" at 02:00:51 — while
`dq_reconciliation` was due at 02:00:00 UTC (05:00 Kyiv). No row for that date.
Every Sunday, and any deploy that lands on a cron instant.
"""
from datetime import datetime, timedelta, timezone

import pytest
from apscheduler.triggers.cron import CronTrigger

from core.scheduler import CATCHUP_CHECKS, BackgroundScheduler


class _FakeJob:
    def __init__(self, job_id):
        self.id = job_id
        self.name = job_id
        self.func = lambda: None


class _FakeScheduler:
    def __init__(self, known=()):
        self.known = {j: _FakeJob(j) for j in known}
        self.added = {}

    def get_job(self, job_id):
        return self.known.get(job_id)

    def add_job(self, func, **kwargs):
        self.added[kwargs.get("id")] = kwargs


def _scheduler_with(monkeypatch, ages, known=("dq_reconciliation", "dq_integrity_check")):
    scheduler = BackgroundScheduler()
    scheduler._scheduler = _FakeScheduler(known)

    class _Conn:
        pass

    class _Ctx:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *exc):
            return False

    class _Store:
        def connection(self):
            return _Ctx()

    async def _get_store():
        return _Store()

    monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
    monkeypatch.setattr(
        "core.data_quality.fetch_last_success_ages", lambda conn, *a, **k: ages,
    )
    return scheduler


def _ages(recon_s, integrity_s):
    def entry(age):
        return {"last_success_at": None if age is None else "2026-08-08T05:00:00+03:00",
                "age_seconds": age}
    return {"reconciliation": entry(recon_s), "integrity": entry(integrity_s)}


class TestCatchUp:
    @pytest.mark.asyncio
    async def test_a_missed_reconciliation_is_queued(self, monkeypatch):
        scheduler = _scheduler_with(monkeypatch, _ages(30 * 3600, 3600))

        await scheduler._schedule_catchup_runs()

        assert "dq_reconciliation_catchup" in scheduler._scheduler.added
        assert "dq_integrity_check_catchup" not in scheduler._scheduler.added

    @pytest.mark.asyncio
    async def test_a_fresh_verdict_queues_nothing(self, monkeypatch):
        scheduler = _scheduler_with(monkeypatch, _ages(3 * 3600, 3600))

        await scheduler._schedule_catchup_runs()

        assert scheduler._scheduler.added == {}

    @pytest.mark.asyncio
    async def test_never_succeeded_counts_as_overdue(self, monkeypatch):
        """A null age is the case most worth catching up, not an unknown."""
        scheduler = _scheduler_with(monkeypatch, _ages(None, None))

        await scheduler._schedule_catchup_runs()

        assert set(scheduler._scheduler.added) == {
            "dq_reconciliation_catchup", "dq_integrity_check_catchup",
        }

    @pytest.mark.asyncio
    async def test_the_run_is_delayed_out_of_the_startup_rush(self, monkeypatch):
        scheduler = _scheduler_with(monkeypatch, _ages(30 * 3600, 30 * 3600))
        before = datetime.now(timezone.utc)

        await scheduler._schedule_catchup_runs()

        recon = scheduler._scheduler.added["dq_reconciliation_catchup"]
        integrity = scheduler._scheduler.added["dq_integrity_check_catchup"]
        recon_at = recon["trigger"].run_date
        integrity_at = integrity["trigger"].run_date

        assert recon_at > before + timedelta(seconds=240)
        assert integrity_at < recon_at, "the heavy one should not go first"

    @pytest.mark.asyncio
    async def test_a_broken_store_does_not_stop_the_scheduler(self, monkeypatch):
        """A watchdog that can prevent startup is worse than no watchdog."""
        scheduler = BackgroundScheduler()
        scheduler._scheduler = _FakeScheduler(("dq_reconciliation",))

        async def _boom():
            raise RuntimeError("db is down")

        monkeypatch.setattr("core.duckdb_store.get_store", _boom)

        await scheduler._schedule_catchup_runs()  # must not raise

        assert scheduler._scheduler.added == {}

    @pytest.mark.asyncio
    async def test_an_unregistered_job_is_skipped(self, monkeypatch):
        scheduler = _scheduler_with(monkeypatch, _ages(30 * 3600, 30 * 3600), known=())

        await scheduler._schedule_catchup_runs()

        assert scheduler._scheduler.added == {}


class TestReconciliationIsOffTheCompactWindow:
    """05:00 Kyiv is 02:00 UTC, which is when the host cron stops the containers."""

    def test_reconciliation_does_not_fire_at_02_00_utc(self, monkeypatch):
        added = {}

        class FakeScheduler:
            def add_job(self, func, **kwargs):
                added[kwargs.get("id")] = kwargs

            def get_job(self, job_id):
                return None

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler()
        import asyncio
        asyncio.run(scheduler._register_jobs())

        trigger = added["dq_reconciliation"]["trigger"]
        assert isinstance(trigger, CronTrigger)

        fields = {f.name: str(f) for f in trigger.fields}
        assert fields["hour"] == "5"
        assert fields["minute"] != "0", (
            "05:00 Kyiv == 02:00 UTC == weekly_compact stopping the containers"
        )

    def test_every_catchup_entry_names_a_registered_job(self):
        added = {}

        class FakeScheduler:
            def add_job(self, func, **kwargs):
                added[kwargs.get("id")] = kwargs

            def get_job(self, job_id):
                return None

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler()
        import asyncio
        asyncio.run(scheduler._register_jobs())

        assert set(CATCHUP_CHECKS) <= set(added)
