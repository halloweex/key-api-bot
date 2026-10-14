"""The stage 5 sweep's inner half, run by `tests/unit/test_duckdb_off_sweep.py`
in a fresh interpreter — never collected on its own: the name does not match
`test_*.py`, and pytest collects a file outside that pattern only when it is
named on the command line, which is what the outer test does.

Why a process of its own: the sweep boots web, starts the scheduler and runs
every job it registers, and those leave module state behind in a dozen places
— the sync service and scheduler singletons, a KeyCRM client whose circuit
breaker has opened, a Postgres pool on a loop that has closed, the prediction
service, the transport-failure counters /api/health publishes. Every later
test in the session would inherit them. A fresh interpreter inherits nothing
and leaves nothing, and still runs under `tests/conftest.py`'s protections
(no production database, no real latch marker, no Telegram).

What it writes, into the directory `KS_SWEEP_OUT` names, is one JSON file per
sweep (`process.json`, `routes.json`): per step, its outcome and the sites
that were refused under `KS_DUCKDB=off` while it ran. Judging that against
`KNOWN_RESIDUAL` is the outer test's business; this file only observes.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import duckdb_switch
from tests.unit.test_duckdb_off_sweep import (
    FLAVOR_ENV, LIVE, NOT_SCHEDULED, OUT_ENV, STEP_TIMEOUT_S, FrozenFile,
    apply_target_state, name_the_key, optional_variants, variant_key,
)
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client, fresh_read_fallback,
)

FLAVOR = os.environ.get(FLAVOR_ENV, "")
OUT = os.environ.get(OUT_ENV, "")

pytestmark = pytest.mark.skipif(
    not OUT, reason="run by tests/unit/test_duckdb_off_sweep.py, never on its own")


# ─── Observation ─────────────────────────────────────────────────────────────

class Recorder:
    """Which sites were refused during which step. A site is counted where it
    was raised (`duckdb_switch.guard`), so swallowing callers cannot hide it;
    between steps the counts are emptied and the mode kept."""

    def __init__(self, sweep: str) -> None:
        self.sweep = sweep
        self.steps: Dict[str, Dict[str, object]] = {}

    def collect(self, step: str, status: Optional[str] = None,
                skipped: Optional[str] = None) -> None:
        entry = self.steps.setdefault(step, {"status": None, "sites": []})
        sites = set(entry["sites"]) | set(duckdb_switch.opened())
        entry["sites"] = sorted(sites)
        if status is not None:
            entry["status"] = status
        if skipped is not None:
            entry["skipped"] = skipped
        duckdb_switch.reset_counts()

    def write(self, **extra) -> None:
        path = os.path.join(OUT, f"{self.sweep}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"flavor": FLAVOR, "steps": self.steps, **extra}, handle,
                      indent=1, sort_keys=True)


def skipped_by(result) -> Optional[str]:
    """Why a job's run said it skipped, when its result says so — the sync's
    `{"skipped": True, "reason": ...}`, a shipper's `{"skipped": "..."}` —
    and None when it ran or returned nothing of the kind."""
    if isinstance(result, dict) and result.get("skipped"):
        reason = result.get("reason") or result["skipped"]
        return str(reason)
    return None


async def run_step(coro, timeout: float = STEP_TIMEOUT_S, *, reraise: bool = False) -> str:
    """Await one step, then whatever tasks it left running — a site refused
    in a task the step started belongs to the step, not to the next one.
    Returns the outcome: "ok", "timeout", or the exception's class; with
    `reraise`, the step's own exception leaves after its tasks are settled,
    for a caller whose handling of it is part of what is swept."""
    before = asyncio.all_tasks()
    failure = None
    try:
        await asyncio.wait_for(coro, timeout)
        status = "ok"
    except asyncio.TimeoutError:
        status = "timeout"
    except Exception as exc:  # noqa: BLE001 — recorded, the sweep goes on
        status, failure = type(exc).__name__, exc
    current = asyncio.current_task()
    left = {t for t in asyncio.all_tasks() - before if t is not current}
    if left:
        _done, pending = await asyncio.wait(left, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=5)
    if reraise and failure is not None:
        raise failure
    return status


# ─── The world outside the process ───────────────────────────────────────────

def refused(message: str):
    """A side effect raising a new exception on every call. One instance
    raised again and again grows its traceback by the frames of every raise,
    and every log line that formats it grows with it: the routes sweep spent
    ~0.2 s a request on that by its four hundredth."""
    def _raise(*_args, **_kwargs):
        raise OSError(message)

    return _raise


@pytest.fixture
def sweep_network(monkeypatch):
    """Nothing leaves the process but, in the live flavor, Postgres itself:
    KeyCRM, ClickHouse, Meilisearch and Telegram are refused at the socket,
    at once, rather than waited on."""
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_getaddrinfo = socket.getaddrinfo
    # (host, port) as the DSN names it, and every address it resolves to —
    # what a socket is finally connected to is the address.
    allowed, addresses = set(), set()
    if FLAVOR == LIVE:
        from urllib.parse import urlsplit

        parts = urlsplit(os.environ["KS_PG_DSN"])
        allowed = {(parts.hostname, parts.port or 5432)}
        addresses = {(info[4][0], parts.port or 5432)
                     for info in real_getaddrinfo(parts.hostname, parts.port or 5432)}

    def _ok(address) -> bool:
        return (isinstance(address, tuple) and len(address) >= 2
                and (address[0], address[1]) in addresses)

    def connect(self, address):
        if _ok(address):
            return real_connect(self, address)
        raise OSError("the network is off in this sweep")

    def connect_ex(self, address):
        if _ok(address):
            return real_connect_ex(self, address)
        raise OSError("the network is off in this sweep")

    def getaddrinfo(host, port, *args, **kwargs):
        if any(host == h and int(port or 0) == p for h, p in allowed | addresses):
            return real_getaddrinfo(host, port, *args, **kwargs)
        raise OSError("the network is off in this sweep")

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    if FLAVOR != LIVE:
        # The pool refuses at once rather than through a DSN nothing answers.
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=refused("connection refused")))


@pytest.fixture
def production_host(monkeypatch, tmp_path):
    """What a production container reads that a laptop and a CI runner read
    differently: a cgroup v2 memory reading (a laptop has none, so the
    memory job would stop before its sample), and the prediction model's
    directory."""
    import core.memory_monitor as memory_monitor
    import core.prediction_service as prediction_service

    reading = {"working_set": 1 << 30, "page_cache": 1 << 28,
               "current": (1 << 30) + (1 << 28), "limit": 7 << 30, "oom_kills": 0}
    monkeypatch.setattr(memory_monitor, "read_cgroup_memory", lambda *a, **k: dict(reading))
    monkeypatch.setattr(prediction_service, "MODEL_DIR", tmp_path / "model")


# ─── The live flavor's Postgres: canonical on the way in, restored on the way out

def _tables_sql() -> str:
    return """
        SELECT n.nspname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_%'
          AND has_table_privilege(c.oid, 'SELECT')
          AND has_table_privilege(c.oid, 'INSERT')
          AND has_table_privilege(c.oid, 'TRUNCATE')
        ORDER BY 1, 2
    """


# The one table the canonical state keeps: the schema's own revision.
KEEP = {("meta", "alembic_version")}


async def _snapshot(conn) -> Dict[tuple, dict]:
    tables = {}
    for schema, name in await conn.fetch(_tables_sql()):
        if (schema, name) in KEEP:
            continue
        qualified = f'"{schema}"."{name}"'
        columns = [r["attname"] for r in await conn.fetch(
            "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass "
            "AND attnum > 0 AND NOT attisdropped AND attgenerated = '' "
            "ORDER BY attnum", qualified)]
        names = ", ".join('"' + c.replace('"', '""') + '"' for c in columns)
        rows = await conn.fetch(f"SELECT {names} FROM {qualified}")
        tables[(schema, name)] = {"columns": columns, "rows": rows,
                                  "hash": await _hash(conn, qualified)}
    return tables


async def _hash(conn, qualified: str) -> str:
    return await conn.fetchval(
        f"SELECT md5(coalesce(string_agg(t::text, E'\\n' ORDER BY t::text), '')) "
        f"FROM {qualified} t")


async def _parents_first(conn, tables) -> List[tuple]:
    """The tables in an order where every foreign key's target comes first."""
    edges = await conn.fetch("""
        SELECT cn.nspname AS cs, cc.relname AS ct, pn.nspname AS ps, pc.relname AS pt
        FROM pg_constraint k
        JOIN pg_class cc ON cc.oid = k.conrelid JOIN pg_namespace cn ON cn.oid = cc.relnamespace
        JOIN pg_class pc ON pc.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE k.contype = 'f'""")
    needs = {t: set() for t in tables}
    for e in edges:
        child, parent = (e["cs"], e["ct"]), (e["ps"], e["pt"])
        if child in needs and parent in needs and child != parent:
            needs[child].add(parent)
    ordered, placed = [], set()
    while needs:
        ready = sorted(t for t, deps in needs.items() if deps <= placed)
        if not ready:
            raise RuntimeError(f"a foreign-key cycle among {sorted(needs)}")
        for t in ready:
            ordered.append(t)
            placed.add(t)
            del needs[t]
    return ordered


def _truncate_sql(tables) -> str:
    return "TRUNCATE " + ", ".join(f'"{s}"."{n}"' for s, n in sorted(tables))


@pytest.fixture
def canonical_postgres():
    """The live flavor's database, as the sweep needs it and as CI must get it
    back. Every table the application's role can write is read whole, then
    emptied — `meta.alembic_version` kept — so the sweep meets one state on
    every machine whatever ran before it; afterwards every table is put back
    row for row and checked by hash, and a table that does not match fails
    here rather than in whichever test runs next."""
    if FLAVOR != LIVE:
        yield None
        return
    import asyncpg

    dsn = os.environ["KS_PG_DSN"]

    async def take():
        conn = await asyncpg.connect(dsn)
        try:
            snapshot = await _snapshot(conn)
            await conn.execute(_truncate_sql(snapshot))
            return snapshot
        finally:
            await conn.close()

    async def put_back(snapshot):
        conn = await asyncpg.connect(dsn)
        try:
            async with conn.transaction():
                await conn.execute(_truncate_sql(snapshot))
                for schema, name in await _parents_first(conn, snapshot):
                    held = snapshot[(schema, name)]
                    if held["rows"]:
                        await conn.copy_records_to_table(
                            name, schema_name=schema, columns=held["columns"],
                            records=[tuple(r) for r in held["rows"]])
            differ = [f"{s}.{n}" for (s, n), held in sorted(snapshot.items())
                      if await _hash(conn, f'"{s}"."{n}"') != held["hash"]]
        finally:
            await conn.close()
        assert not differ, f"not restored after the sweep: {differ}"

    snapshot = asyncio.run(take())
    try:
        yield snapshot
    finally:
        asyncio.run(put_back(snapshot))


async def _seed_live_facts(dsn: str) -> None:
    """What production's Postgres holds that the state reads: an owner row
    for every table of every latched chain — the audit half of the latch, the
    half anything holding a pool stands down on — `bronze.expenses` marked
    backfilled, step 13's `expenses_backfilled`, and one order."""
    import asyncpg

    from core import chain_latch
    from core.write_chains import WRITE_CHAINS, chain_name

    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            for chain in WRITE_CHAINS:
                stamp = chain_latch.latched_at(chain_name(chain))
                await chain_latch.claim(conn, tuple(chain.CHAIN_TABLES), stamp)
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_ok_at, backfilled_at) "
                "VALUES ('bronze.expenses', now(), now())")
            # History: the boot asks the store the orders are written to
            # whether there is any, and with none it pulls 730 days from
            # KeyCRM instead of taking the path every production boot takes.
            await conn.execute(
                "INSERT INTO bronze.orders (id, source_id, status_id, grand_total) "
                "VALUES (1, 4, 1, 100)")
    finally:
        await conn.close()


@pytest.fixture
def state(canonical_postgres, monkeypatch, tmp_path, fresh_read_fallback):  # noqa: F811
    """The target state, in the flavour this process was started for, and
    the frozen file beside it. Returns the file."""
    from core import chain_latch
    from core.write_chains import WRITE_CHAINS, chain_name

    if FLAVOR == LIVE:
        # The local half of the latch first: the owner rows carry its stamp.
        for chain in WRITE_CHAINS:
            chain_latch.latch(chain_name(chain), chain.WRITE_ENV)
        asyncio.run(_seed_live_facts(os.environ["KS_PG_DSN"]))
    apply_target_state(monkeypatch, tmp_path, live=FLAVOR == LIVE)
    # A refusal under `get_last_sync_time` is recorded with its key.
    name_the_key(monkeypatch)
    import core.duckdb_store as store_module

    return FrozenFile(store_module.DB_PATH)


# ─── The two sweeps ──────────────────────────────────────────────────────────

def test_process(state, sweep_network, production_host, monkeypatch):
    """Web's real startup, then every job the scheduler registered, one at a
    time and in the order it registered them."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    import core.scheduler as scheduler_module
    import core.sync_service as sync_module
    import web.main as main
    from core.duckdb_store import get_store

    record = Recorder("process")
    monkeypatch.setattr(main, "validate_config", MagicMock())
    # The bot's store — Postgres, or SQLite — never DuckDB.
    monkeypatch.setattr(main, "init_database", MagicMock())
    # Training is swept as its own job; the boot's ten-second delayed copy of
    # it would land inside whichever job happened to be running by then.
    monkeypatch.setattr(main, "_train_prediction_model", AsyncMock())
    # Started paused: the sweep runs each job itself, and a trigger firing in
    # the background would put its sites under somebody else's step.
    real_start = AsyncIOScheduler.start
    monkeypatch.setattr(AsyncIOScheduler, "start",
                        lambda self, paused=False: real_start(self, paused=True))
    monkeypatch.setattr(scheduler_module, "_scheduler", None)

    real_sync, real_start_scheduler = main.init_and_sync, main.start_scheduler

    async def init_and_sync(**kwargs):
        # The boot sync's own exception goes back to `startup_event`, whose
        # handling of it — the refusal's branch, or the generic one — is swept.
        record.collect("boot")
        try:
            await run_step(real_sync(**kwargs), reraise=True)
        except Exception as exc:
            record.collect("boot:init_and_sync", type(exc).__name__)
            raise
        record.collect("boot:init_and_sync", "ok")

    async def start_scheduler():
        record.collect("boot")
        status = await run_step(real_start_scheduler())
        record.collect("boot:scheduler_start", status)
        return scheduler_module.get_scheduler()

    monkeypatch.setattr(main, "init_and_sync", init_and_sync)
    monkeypatch.setattr(main, "start_scheduler", start_scheduler)

    async def sweep():
        record.collect("before")
        record.collect("boot", await run_step(main.startup_event()))
        scheduler = scheduler_module.get_scheduler()
        jobs = list(scheduler._job_info)
        for job_id in jobs:
            job = scheduler._scheduler.get_job(job_id)
            if job is None:  # registered, then taken off the scheduler
                record.collect(f"job:{job_id}", NOT_SCHEDULED)
                continue
            # Each job runs as the tick that comes once every window an
            # earlier step armed has passed — what production runs from a few
            # minutes after the boot on. The boot's own sync tick arms the
            # sync service's adaptive backoff (120 s at least, 300 s off-hours)
            # and its steps' retry windows, and a job run seconds later was
            # skipped by them: `job:incremental_sync` read nothing at all, and
            # what every later tick reaches was missing from the list (review
            # of PR-1). So the sync service is built fresh for each job, with
            # no window armed. What else it holds is the last tick's step
            # states, which /api/health reads, and a cached KeyCRM capability.
            sync_module._sync_service = None
            ran = {}

            async def run(job=job):
                ran["result"] = await job.func(*job.args, **job.kwargs)

            status = await run_step(run())
            record.collect(f"job:{job_id}", status, skipped_by(ran.get("result")))
        # The control: the tripwire still stands in this state.
        async def control():
            async with (await get_store()).connection():
                pass
        record.collect("control", await run_step(control()))
        if scheduler._scheduler.running:
            scheduler._scheduler.shutdown(wait=False)
        from core.pg import close_pool
        await close_pool()
        return jobs

    jobs = asyncio.run(sweep())
    record.write(jobs=jobs, file=state.unchanged())


def test_routes(state, sweep_network, production_host, admin_client,  # noqa: F811
                monkeypatch):
    """Every GET route under /api/, read off the app."""
    from fastapi.testclient import TestClient

    from core import ch_cohorts, read_fallback
    from tests.unit.test_read_fallback_http import _request_for, swept_routes
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.api import health as health_routes

    assert FLAVOR != LIVE, "the routes are swept with Postgres refusing"
    monkeypatch.setattr(ch_cohorts, "fetch", AsyncMock(side_effect=refused("down")))
    client = TestClient(app, raise_server_exceptions=False)
    client.cookies.update(admin_client.cookies)

    record = Recorder("routes")
    record.collect("before")
    statuses, variants, surfaces = {}, {}, {}

    def call(key: str, path: str, params: dict) -> int:
        limiter.reset()
        read_fallback.reset_counts()
        # A count cached by an earlier request is not a reach for the file.
        health_routes._stats_cache.update(data=None, expires_at=0)
        response = client.get(path, params=params)
        try:
            surfaces[key] = response.json().get("surface")
        except Exception:  # noqa: BLE001 — a CSV, an HTML page, a list
            surfaces[key] = None
        record.collect(f"GET {key}", str(response.status_code))
        return response.status_code

    for endpoint in swept_routes(app):
        path, params = _request_for(endpoint)
        statuses[endpoint.path] = call(endpoint.path, path, params)
        # Again with the optional values that can pick a branch the base
        # request never takes (`optional_variants`).
        for variant in optional_variants(endpoint):
            key = variant_key(endpoint.path, variant)
            variants[key] = call(key, path, {**params, **variant})
    record.write(routes=statuses, variants=variants, surfaces=surfaces,
                 file=state.unchanged())
