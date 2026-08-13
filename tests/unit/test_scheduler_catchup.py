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

from core.scheduler import (
    CATCHUP_CHECKS,
    INVENTORY_CATCHUP_DELAY_S,
    BackgroundScheduler,
)


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


_NO_TABLE = object()


def _scheduler_with(
    monkeypatch, ages, known=("dq_reconciliation", "dq_integrity_check"),
    inventory_today=_NO_TABLE,
):
    scheduler = BackgroundScheduler()
    scheduler._scheduler = _FakeScheduler(known)

    class _Conn:
        def execute(self, sql, *args):
            # The only raw SQL _schedule_catchup_runs issues is the probe for
            # today's inventory snapshot. The default is a schema without that
            # table, which must degrade quietly rather than disable the
            # data-quality catch-ups above it.
            if inventory_today is _NO_TABLE:
                raise RuntimeError("Catalog Error: inventory_sku_history does not exist")

            class _Result:
                def fetchone(_self):
                    return (1,) if inventory_today else None

            return _Result()

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


class TestReconciliationWindowIsAParameter:
    """The daily job covers 90 days; months older than that are checked by
    nobody, and the months this job spent dying on 429s were never checked at
    all. `POST /api/reconcile?days=N` is how you go back and look."""

    def test_the_default_is_still_ninety_days(self):
        import inspect
        from core.scheduler import BackgroundScheduler

        sig = inspect.signature(BackgroundScheduler._run_dq_reconciliation)
        assert sig.parameters["window_days"].default == 90

    def test_the_endpoint_is_admin_only(self):
        from web.main import app
        from web.routes.auth import require_admin

        route = next(
            r for r in app.routes
            if getattr(r, "path", None) == "/api/reconcile" and "POST" in getattr(r, "methods", set())
        )
        calls = set()
        stack = [route.dependant]
        while stack:
            d = stack.pop()
            for dep in d.dependencies:
                if dep.call is not None:
                    calls.add(dep.call)
                stack.append(dep)
        assert require_admin in calls


class TestReconcileEndpointDoesNotHoldTheRequestOpen:
    """Run inline, a 365-day window 504s after two minutes while the work
    carries on for another eight — harmless, but it reads like a failure."""

    def _client(self, monkeypatch, recorder):
        import time as _time
        from fastapi.testclient import TestClient
        from web.main import app
        from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer
        from core.permissions import ADMIN_USER_IDS
        from web.routes.api._deps import limiter

        limiter.reset()
        admin_id = sorted(ADMIN_USER_IDS)[0]

        class _FakeScheduler:
            async def _run_dq_reconciliation(self, window_days=90):
                recorder.append(window_days)
                return {"run_id": 1, "discrepancies_count": 0}

        monkeypatch.setattr("core.scheduler.get_scheduler", lambda: _FakeScheduler())

        async def _resolve(session):
            return {"user_id": admin_id, "role": "admin"}

        monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)
        client = TestClient(app)
        client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
            {"id": str(admin_id), "first_name": "T", "last_name": "U",
             "username": "t", "auth_date": str(int(_time.time()))}, role="admin",
        )))
        return client

    def test_it_returns_at_once_and_says_where_the_result_lands(self, monkeypatch):
        seen = []
        res = self._client(monkeypatch, seen).post("/api/reconcile?days=365")

        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "started"
        assert body["window_days"] == 365
        assert "/api/health/data-quality" in body["note"]

    def test_inline_is_still_available_for_a_short_window(self, monkeypatch):
        seen = []
        res = self._client(monkeypatch, seen).post("/api/reconcile?days=7&background=false")

        assert res.status_code == 200
        assert res.json()["discrepancies_count"] == 0
        assert seen == [7], "the window must reach the job"


class TestInventorySnapshotCatchUp:
    """The snapshot is the one missed run that cannot be repaid.

    It photographs current per-SKU stock and KeyCRM serves current stock only,
    so a day the job did not run is gone with the day. Twenty-five are already
    missing from 2026, twenty-one of them consecutive, and nothing said so.

    The catch-up cannot recover a past day — nothing can. It turns "the
    container was down at 01:00, so today is lost too" into "today was recorded
    late", which is the only part still available to fix.
    """

    @pytest.mark.asyncio
    async def test_no_snapshot_today_queues_a_catchup(self, monkeypatch):
        scheduler = _scheduler_with(
            monkeypatch, _ages(3600, 3600),
            known=("dq_reconciliation", "dq_integrity_check", "inventory_snapshot"),
            inventory_today=False,
        )

        await scheduler._schedule_catchup_runs()

        assert "inventory_snapshot_catchup" in scheduler._scheduler.added

    @pytest.mark.asyncio
    async def test_a_snapshot_already_taken_queues_nothing(self, monkeypatch):
        scheduler = _scheduler_with(
            monkeypatch, _ages(3600, 3600),
            known=("dq_reconciliation", "dq_integrity_check", "inventory_snapshot"),
            inventory_today=True,
        )

        await scheduler._schedule_catchup_runs()

        assert scheduler._scheduler.added == {}

    @pytest.mark.asyncio
    async def test_the_run_is_staggered_after_the_dq_catchups(self, monkeypatch):
        """Startup already carries the initial sync, model training and the
        first warehouse refresh. This waits its turn."""
        scheduler = _scheduler_with(
            monkeypatch, _ages(3600, 3600),
            known=("dq_reconciliation", "dq_integrity_check", "inventory_snapshot"),
            inventory_today=False,
        )
        before = datetime.now(timezone.utc)

        await scheduler._schedule_catchup_runs()

        run_at = scheduler._scheduler.added["inventory_snapshot_catchup"]["trigger"].run_date
        delay = (run_at - before).total_seconds()
        assert delay > max(d for _, _, d in CATCHUP_CHECKS.values())
        assert delay <= INVENTORY_CATCHUP_DELAY_S + 5

    @pytest.mark.asyncio
    async def test_a_schema_without_the_table_does_not_break_the_others(self, monkeypatch):
        """The probe is nested inside its own try for this reason: an older
        schema must not cost us the data-quality catch-ups."""
        scheduler = _scheduler_with(
            monkeypatch, _ages(30 * 3600, 30 * 3600),
            known=("dq_reconciliation", "dq_integrity_check", "inventory_snapshot"),
        )  # inventory probe raises

        await scheduler._schedule_catchup_runs()

        assert set(scheduler._scheduler.added) == {
            "dq_reconciliation_catchup", "dq_integrity_check_catchup",
        }

    @pytest.mark.asyncio
    async def test_an_unregistered_snapshot_job_is_skipped(self, monkeypatch):
        scheduler = _scheduler_with(
            monkeypatch, _ages(3600, 3600), inventory_today=False,
        )  # inventory_snapshot not in known jobs

        await scheduler._schedule_catchup_runs()

        assert scheduler._scheduler.added == {}
