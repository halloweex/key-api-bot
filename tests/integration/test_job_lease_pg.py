"""K0 over a real Postgres: who executes a scheduler job, and its lease.

Two `JobLeases` are two processes (two sessions); a plain asyncpg session
stands in for the Rust worker, doing what the contract in `core/job_lease.py`
tells it to. Each test leases a job id no real job and no other test uses,
and removes the switch rows it wrote.
"""
from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import ANY

import asyncpg
import pytest

from core import job_lease

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

ROOT = Path(__file__).resolve().parents[2]


def _probe_job() -> str:
    return f"k0_probe_{uuid.uuid4().hex[:12]}"


@asynccontextmanager
async def _session(application_name: str = "k0-test"):
    conn = await asyncpg.connect(DSN, server_settings={"application_name": application_name})
    try:
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def _leases(dsn=None, **kwargs):
    leases = job_lease.JobLeases(dsn or DSN, **kwargs)
    try:
        yield leases
    finally:
        leases.terminate()


async def _write_switch(job_id: str, value: str) -> None:
    async with _session() as conn:
        await conn.execute(job_lease.WRITE_SWITCH_SQL, job_lease.switch_key(job_id), value)


async def _clear_switch(job_id: str) -> None:
    async with _session() as conn:
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key = $1",
                           job_lease.switch_key(job_id))


async def _try_lock(conn, job_id: str) -> bool:
    return await conn.fetchval(job_lease.TRY_LOCK_SQL, job_lease.lease_key(job_id))


class TestOneProcessAtATime:
    @pytest.mark.asyncio
    async def test_a_second_process_does_not_run_a_job_the_first_is_running(self):
        job = _probe_job()
        async with _leases(application_name="k0-first") as first, \
                _leases(application_name="k0-second") as second:
            lease = await first.claim(job)
            assert isinstance(lease, job_lease.Lease) and lease.guarded

            refused = await second.claim(job)
            assert isinstance(refused, job_lease.NotRunHere)
            assert refused.as_result() == {
                "skipped": True, "reason": "lease_held",
                "lease_key": str(job_lease.lease_key(job)),
                "holder_pid": ANY, "holder_application": "k0-first"}

            await lease.release()
            taken = await second.claim(job)
            assert isinstance(taken, job_lease.Lease) and taken.guarded
            await taken.release()

    @pytest.mark.asyncio
    async def test_two_runs_inside_one_process_both_hold_it_as_before(self):
        """A catch-up beside its scheduled twin, or a lever beside the minute
        tick, ran side by side before the leases; they still do. The lease
        is free to another process only when both have finished."""
        job = _probe_job()
        async with _leases() as mine, _session() as other:
            a = await mine.claim(job)
            b = await mine.claim(job)
            assert isinstance(a, job_lease.Lease) and isinstance(b, job_lease.Lease)
            assert a.guarded and b.guarded
            await a.release()
            assert await _try_lock(other, job) is False
            await b.release()
            assert await _try_lock(other, job) is True

    @pytest.mark.asyncio
    async def test_a_lease_whose_session_ended_is_reported_lost(self, caplog):
        job = _probe_job()
        async with _leases(application_name="k0-doomed") as mine, \
                _leases(application_name="k0-heir") as heir, _session() as admin:
            lease = await mine.claim(job)
            assert isinstance(lease, job_lease.Lease) and lease.guarded
            assert await admin.fetchval(
                "SELECT bool_and(pg_terminate_backend(pid)) FROM pg_stat_activity "
                "WHERE application_name = 'k0-doomed'")

            # The lock goes when the backend has exited, a moment later.
            deadline = time.monotonic() + 5
            taken = await heir.claim(job)
            while isinstance(taken, job_lease.NotRunHere) and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
                taken = await heir.claim(job)
            assert isinstance(taken, job_lease.Lease) and taken.guarded

            with caplog.at_level(logging.ERROR, logger="core.job_lease"):
                await lease.release()
            assert [r for r in caplog.records if r.name == "core.job_lease"
                    and r.levelno == logging.ERROR
                    and str(job_lease.lease_key(job)) in r.getMessage()]

            # The process that lost it opens a fresh session and finds the
            # job running elsewhere, instead of running it unguarded.
            again = await mine.claim(job)
            assert isinstance(again, job_lease.NotRunHere)
            assert again.reason == job_lease.LEASE_HELD
            await taken.release()


class TestTheSwitch:
    @pytest.mark.asyncio
    async def test_rust_gets_the_job_and_its_lease_is_left_for_rust(self):
        job = _probe_job()
        await _write_switch(job, "rust")
        try:
            async with _leases() as python, _session("ks-rust-worker") as rust:
                decision = await python.claim(job)
                assert isinstance(decision, job_lease.NotRunHere)
                assert decision.as_result() == {"skipped": True, "reason": "executed_by_rust",
                                                "executor": "rust", "remembered": False}
                assert await _try_lock(rust, job) is True
        finally:
            await _clear_switch(job)

    @pytest.mark.parametrize("value", [None, "python", "Rust", " rust", ""])
    @pytest.mark.asyncio
    async def test_anything_but_rust_runs_it_here(self, value, caplog):
        job = _probe_job()
        if value is not None:
            await _write_switch(job, value)
        try:
            async with _leases() as python:
                with caplog.at_level(logging.WARNING, logger="core.job_lease"):
                    for _ in range(2):
                        lease = await python.claim(job)
                        assert isinstance(lease, job_lease.Lease) and lease.guarded
                        await lease.release()
        finally:
            await _clear_switch(job)
        odd = value not in (None, "python")
        named = [r for r in caplog.records if r.name == "core.job_lease"
                 and r.levelno == logging.WARNING
                 and job_lease.switch_key(job) in r.getMessage()]
        assert len(named) == (1 if odd else 0)   # once a process, not once a tick

    @pytest.mark.asyncio
    async def test_postgres_computes_every_jobs_key_and_shows_it_as_documented(self):
        from core.scheduler import registered_job_ids

        jobs = await registered_job_ids()
        async with _session() as conn:
            for job in jobs:
                assert await conn.fetchval(f"SELECT {job_lease.LEASE_KEY_SQL}", job) \
                    == job_lease.lease_key(job), job

        probe = _probe_job()
        key = job_lease.lease_key(probe)
        async with _leases(application_name="k0-shown") as mine, _session() as conn:
            lease = await mine.claim(probe)
            row = await conn.fetchrow(
                "SELECT l.classid::bigint AS hi, l.objid::bigint AS lo FROM pg_locks l "
                "JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory' "
                "AND l.objsubid = 1 AND a.application_name = 'k0-shown'")
            assert (row["hi"], row["lo"]) == (key >> 32, key & 0xFFFFFFFF)
            await lease.release()


class TestWhenPostgresCannotBeAsked:
    @pytest.mark.asyncio
    async def test_the_job_runs_as_before_and_it_is_said_once(self, caplog):
        """Port 9 refuses: no job that ran before the leases is lost to an
        outage, and the log carries one line, not one a tick."""
        job = _probe_job()
        with caplog.at_level(logging.WARNING, logger="core.job_lease"):
            async with _leases("postgresql://ks_app:x@127.0.0.1:9/ks",
                               connect_timeout_s=2) as cut_off:
                first = await cut_off.claim(job)
                second = await cut_off.claim(job)
        assert isinstance(first, job_lease.Lease) and not first.guarded
        assert isinstance(second, job_lease.Lease) and not second.guarded
        assert len([r for r in caplog.records if r.name == "core.job_lease"
                    and r.levelno == logging.WARNING]) == 1

    @pytest.mark.asyncio
    async def test_a_job_last_seen_switched_to_rust_stays_skipped(self, monkeypatch):
        job, never_switched = _probe_job(), _probe_job()
        await _write_switch(job, "rust")
        try:
            async with _leases() as python:
                first = await python.claim(job)
                assert isinstance(first, job_lease.NotRunHere)
                assert first.reason == job_lease.EXECUTED_BY_RUST

                python.terminate()                    # the session goes...

                async def refused(*_a, **_k):
                    raise OSError("connection refused")

                monkeypatch.setattr(asyncpg, "connect", refused)   # ...and Postgres
                decision = await python.claim(job)
                assert isinstance(decision, job_lease.NotRunHere)
                assert decision.reason == job_lease.EXECUTED_BY_RUST
                assert decision.detail["remembered"] is True
                other = await python.claim(never_switched)
                assert isinstance(other, job_lease.Lease) and not other.guarded
        finally:
            monkeypatch.undo()
            await _clear_switch(job)


# ─── Through the scheduler ────────────────────────────────────────────────────

def _scheduler(leases):
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from core.scheduler import SCHEDULER_TIMEZONE, BackgroundScheduler

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    s._scheduler.add_listener(s._on_job_executed, EVENT_JOB_EXECUTED)
    s._scheduler.add_listener(s._on_job_error, EVENT_JOB_ERROR)
    s._leases = leases
    return s


async def _run_once(s, job_id: str, body):
    """The job registered as `_register_jobs` registers it, fired once by
    APScheduler, with the listeners `start()` wires: what /api/jobs reads."""
    from apscheduler.triggers.date import DateTrigger

    from core.scheduler import SCHEDULER_TIMEZONE

    s._add_job(job_id=job_id, name=job_id, description="probe", func=body,
               trigger=DateTrigger(run_date=datetime.now(SCHEDULER_TIMEZONE)))
    s._scheduler.start()
    try:
        deadline = time.monotonic() + 5
        while not s.get_job_history(job_id) and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
    finally:
        s._scheduler.shutdown(wait=False)
    return s.get_job_history(job_id)


def _admin_client(monkeypatch, scheduler):
    from fastapi.testclient import TestClient

    from tests.sessions import hardcoded_admin, session_cookie
    from web.main import app
    from web.routes.api._deps import limiter

    limiter.reset()
    monkeypatch.setattr("core.scheduler.get_scheduler", lambda: scheduler)
    client = TestClient(app)
    name, _, value = session_cookie(hardcoded_admin(), "admin").partition("=")
    client.cookies.set(name, value)
    return client


def _jobs_api(monkeypatch, scheduler):
    r = _admin_client(monkeypatch, scheduler).get("/api/jobs")
    assert r.status_code == 200, r.text
    return r.json()


class TestThroughTheScheduler:
    def test_a_run_given_to_rust_is_not_made_and_reads_skipped(self, monkeypatch):
        job = _probe_job()
        ran = []

        async def body():
            ran.append(job)
            return {"done": True}

        async def scenario():
            await _write_switch(job, "rust")
            try:
                async with _leases() as leases:
                    s = _scheduler(leases)
                    return s, await _run_once(s, job, body)
            finally:
                await _clear_switch(job)

        s, history = asyncio.run(scenario())
        assert ran == []
        assert [h["status"] for h in history] == ["skipped"]
        assert history[0]["result"]["reason"] == "executed_by_rust"

        api = _jobs_api(monkeypatch, s)
        [entry] = [h for h in api["history"] if h["job_id"] == job]
        assert (entry["status"], entry["error"]) == ("skipped", None)
        assert entry["result"]["reason"] == "executed_by_rust"
        [info] = [j for j in api["jobs"] if j["id"] == job]
        assert info["last_status"] == "skipped"

    def test_a_run_another_process_is_making_is_not_made_here(self, caplog):
        job = _probe_job()
        ran = []

        async def body():
            ran.append(job)

        async def scenario():
            async with _session("ks-rust-worker") as holder:
                assert await _try_lock(holder, job)
                async with _leases() as leases:
                    with caplog.at_level(logging.INFO, logger="core.job_lease"):
                        return await _run_once(_scheduler(leases), job, body)

        history = asyncio.run(scenario())
        assert ran == []
        assert history[0]["status"] == "skipped"
        assert history[0]["result"]["reason"] == "lease_held"
        assert history[0]["result"]["holder_application"] == "ks-rust-worker"
        said = [r for r in caplog.records
                if r.name == "core.job_lease" and job in r.getMessage()]
        assert [r.levelno for r in said] == [logging.WARNING]   # one line

    def test_with_no_switch_the_job_runs_and_its_history_is_as_before(self, monkeypatch):
        job = _probe_job()
        ran = []

        async def body():
            ran.append(job)
            return {"rows": 3}

        async def scenario():
            async with _leases() as leases, _session() as other:
                s = _scheduler(leases)
                history = await _run_once(s, job, body)
                return s, history, await _try_lock(other, job)

        s, history, free_after = asyncio.run(scenario())
        assert ran == [job] and free_after is True
        # The entry's shape as before the leases: no `result`.
        assert history == [{"started_at": ANY, "finished_at": ANY, "status": "success",
                            "duration_ms": ANY, "error": None}]
        [entry] = [h for h in _jobs_api(monkeypatch, s)["history"] if h["job_id"] == job]
        assert (entry["status"], entry["result"]) == ("success", None)

    def test_a_run_that_fails_still_gives_the_lease_back(self):
        job = _probe_job()

        async def body():
            raise RuntimeError("the job's own failure")

        async def scenario():
            async with _leases() as leases, _session() as other:
                history = await _run_once(_scheduler(leases), job, body)
                return history, await _try_lock(other, job)

        history, free_after = asyncio.run(scenario())
        assert [h["status"] for h in history] == ["failed"]
        assert free_after is True

    @pytest.mark.asyncio
    async def test_a_started_scheduler_leases_and_frees_its_leases_at_shutdown(
            self, monkeypatch):
        from core import pg
        from core.scheduler import BackgroundScheduler

        monkeypatch.delenv(job_lease.LEASES_ENV, raising=False)
        monkeypatch.setattr(pg, "_pool", None)   # its own pool, closed below
        s = BackgroundScheduler()
        await s.start()
        try:
            lease = await s.claim_job_run("ch_sync")
            assert lease.guarded
        finally:
            s.shutdown(wait=False)
            await pg.close_pool()
        async with _session() as other:
            deadline = time.monotonic() + 5
            while not await _try_lock(other, "ch_sync") and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            assert await other.fetchval(job_lease.UNLOCK_SQL, job_lease.lease_key("ch_sync"))


# ─── The lease alone, for code that writes what a job writes ──────────────────

class TestTheLeaseAlone:
    @pytest.mark.asyncio
    async def test_it_is_taken_whoever_the_switch_names(self):
        job = _probe_job()
        await _write_switch(job, "rust")
        try:
            async with _leases() as leases, _session() as other:
                lease = await _scheduler(leases).claim_job_lease(job)
                assert lease.guarded
                assert await _try_lock(other, job) is False
                await lease.release()
                assert await _try_lock(other, job) is True
        finally:
            await _clear_switch(job)

    @pytest.mark.asyncio
    async def test_another_process_holding_it_is_refused_by_name(self):
        job = _probe_job()
        async with _session("ks-rust-worker") as holder, _leases() as leases:
            assert await _try_lock(holder, job)
            with pytest.raises(job_lease.RunsElsewhere) as refused:
                await _scheduler(leases).claim_job_lease(job)
        assert refused.value.decision.reason == job_lease.LEASE_HELD
        assert refused.value.decision.detail["holder_application"] == "ks-rust-worker"


class TestTheClickHouseComparison:
    """The daily comparison ships Silver and derives Gold into `ch_sync`'s
    tables, so it holds `ch_sync`'s lease: the real job, its checks stubbed
    (`tests/helpers/mirror_landing.py`), its leases real."""

    @pytest.mark.asyncio
    async def test_while_another_process_runs_ch_sync_nothing_is_shipped(
            self, monkeypatch):
        from core import scheduler as scheduler_module
        from tests.helpers.mirror_landing import _run

        monkeypatch.setattr(scheduler_module, "CH_SYNC_LEASE_RETRY_S", 0)
        mocks: dict = {}
        async with _session("ks-rust-worker") as holder, _leases() as leases:
            assert await _try_lock(holder, "ch_sync")
            monkeypatch.setattr(scheduler_module.BackgroundScheduler, "_leases", leases)
            persisted, _, _ = await _run({}, mocks=mocks)
        assert mocks["reconcile_clickhouse"].await_count == 0
        [unwatched] = [i for i in persisted["issues"]
                       if i.check_name == "gold_values_unwatched"]
        assert "ks-rust-worker" in unwatched.description
        assert persisted["error_message"] is None   # a verdict, not a raise

    @pytest.mark.asyncio
    async def test_otherwise_it_ships_holding_the_lease_and_gives_it_back(
            self, monkeypatch):
        from core import scheduler as scheduler_module
        from tests.helpers.mirror_landing import _run

        taken_by_another_meanwhile = []

        async def ship_and_compare():
            async with _session() as other:
                taken_by_another_meanwhile.append(await _try_lock(other, "ch_sync"))
            return []

        mocks: dict = {}

        def look_from_another_process():
            mocks["reconcile_clickhouse"].side_effect = ship_and_compare

        async with _leases() as leases, _session() as after:
            monkeypatch.setattr(scheduler_module.BackgroundScheduler, "_leases", leases)
            await _run({}, mocks=mocks, hook=look_from_another_process)
            assert taken_by_another_meanwhile == [False]
            assert await _try_lock(after, "ch_sync") is True
            assert await after.fetchval(job_lease.UNLOCK_SQL, job_lease.lease_key("ch_sync"))


# ─── The admin levers that run a job's body themselves ────────────────────────

LEVERS = [
    ("/api/warehouse/refresh", "pg_warehouse_derive", "_run_pg_derivation"),
    ("/api/warehouse/rebuild-silver", "pg_warehouse_derive", "_run_pg_derivation"),
    ("/api/reconcile?background=false", "dq_reconciliation", "_run_dq_reconciliation"),
]


def _press(monkeypatch, path: str, body_name: str):
    from core.scheduler import BackgroundScheduler

    ran = []

    async def body(**kwargs):
        ran.append(kwargs)
        return {"status": "success"}

    s = BackgroundScheduler()
    s._leases = job_lease.JobLeases(DSN)
    monkeypatch.setattr(s, body_name, body)
    try:
        r = _admin_client(monkeypatch, s).post(path)
    finally:
        s._leases.terminate()
    return r, ran


class TestTheLevers:
    @pytest.mark.parametrize("path, job_id, body_name", LEVERS)
    def test_a_job_given_to_rust_is_not_run_by_its_lever(self, monkeypatch, path, job_id,
                                                        body_name):
        asyncio.run(_write_switch(job_id, "rust"))
        try:
            r, ran = _press(monkeypatch, path, body_name)
        finally:
            asyncio.run(_clear_switch(job_id))
        assert (r.status_code, ran) == (409, [])
        assert job_lease.switch_key(job_id) in r.json()["detail"]

    @pytest.mark.parametrize("path, job_id, body_name", LEVERS)
    def test_a_job_another_process_is_running_is_not_run_beside_it(self, monkeypatch, path,
                                                                  job_id, body_name):
        async def hold_and_press():
            async with _session("ks-rust-worker") as holder:
                assert await _try_lock(holder, job_id)
                return await asyncio.to_thread(_press, monkeypatch, path, body_name)

        r, ran = asyncio.run(hold_and_press())
        assert (r.status_code, ran) == (409, [])
        assert "ks-rust-worker" in r.json()["detail"]

    @pytest.mark.parametrize("path, job_id, body_name", LEVERS)
    def test_otherwise_the_lever_runs_it_and_gives_the_lease_back(self, monkeypatch, path,
                                                                 job_id, body_name):
        r, ran = _press(monkeypatch, path, body_name)
        assert r.status_code == 200, r.text
        assert len(ran) == 1

        async def free():
            async with _session() as other:
                taken = await _try_lock(other, job_id)
                await other.fetchval(job_lease.UNLOCK_SQL, job_lease.lease_key(job_id))
                return taken

        assert asyncio.run(free()) is True


# ─── The switch's one writer ──────────────────────────────────────────────────

def _cli():
    spec = importlib.util.spec_from_file_location(
        "_job_executor_cli", ROOT / "scripts" / "job_executor.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestTheCli:
    @pytest.mark.asyncio
    async def test_it_switches_a_job_to_rust_and_back(self, capsys):
        cli = _cli()
        try:
            assert await cli.run(["set", "ch_sync", "rust"]) == 0
            async with _leases() as python:
                decision = await python.claim("ch_sync")
                assert isinstance(decision, job_lease.NotRunHere)
                assert decision.reason == job_lease.EXECUTED_BY_RUST

            capsys.readouterr()
            assert await cli.run(["show"]) == 0
            [line] = [ln for ln in capsys.readouterr().out.splitlines()
                      if ln.startswith("ch_sync ")]
            assert line.split()[1:3] == ["rust", "rust"]

            assert await cli.run(["set", "ch_sync", "python"]) == 0
            async with _leases() as python:
                lease = await python.claim("ch_sync")
                assert isinstance(lease, job_lease.Lease) and lease.guarded
                await lease.release()
        finally:
            await _clear_switch("ch_sync")

    @pytest.mark.parametrize("argv", [["set", "ch-sync", "rust"], ["set", "ch_sync", "Rust"]])
    @pytest.mark.asyncio
    async def test_it_refuses_a_job_nobody_runs_and_an_executor_nobody_is(self, argv):
        cli = _cli()
        assert await cli.run(argv) == 2
        async with _session() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM meta.chain_watermarks WHERE key = $1",
                job_lease.switch_key(argv[1])) == 0
