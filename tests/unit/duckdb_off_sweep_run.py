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
(no production database, no real latch marker, no Telegram). One per sweep,
too, since PR-9: the doors sweep boots web again, and a boot that met the
process sweep's opened circuit breaker failed its sync on it.

Three sweeps, each one JSON file in the directory `KS_SWEEP_OUT` names:
`process.json` (the boot and every job), `routes.json` (every GET route under
`/api/` as an admin) and `doors.json` (everything else a week exercises —
`tests/unit/duckdb_off_sweep_doors.py`). Per step, its outcome, the sites
that were refused under `KS_DUCKDB=off` while it ran, what it asked of the
world outside and what that served or refused
(`tests/unit/duckdb_off_sweep_world.py`) — and, where Postgres answers, what
it wrote there. Judging that against `KNOWN_RESIDUAL` is the outer test's
business; this file only observes.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import duckdb_switch
from tests.unit.test_duckdb_off_sweep import (
    FLAVOR_ENV, LIVES, NOT_SCHEDULED, OFFLINES, OUT_ENV, STEP_TIMEOUT_S, FrozenFile,
    apply_target_state, name_the_key, optional_variants, variant_key,
)
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: F401 — fixture

FLAVOR = os.environ.get(FLAVOR_ENV, "")
OUT = os.environ.get(OUT_ENV, "")
# The flavour's two halves: whether Postgres answers, and whether KeyCRM and
# TurboSMS do.
PG_LIVE = FLAVOR in LIVES
WORLD_ANSWERS = FLAVOR not in OFFLINES

pytestmark = pytest.mark.skipif(
    not OUT, reason="run by tests/unit/test_duckdb_off_sweep.py, never on its own")


# ─── Observation ─────────────────────────────────────────────────────────────

class Recorder:
    """Which sites were refused during which step. A site is counted where it
    was raised (`duckdb_switch.guard`), so swallowing callers cannot hide it;
    between steps the counts are emptied and the mode kept. What the step
    asked of the world outside is filed beside them, the same way."""

    def __init__(self, sweep: str, world=None) -> None:
        self.sweep, self.world = sweep, world
        self.steps: Dict[str, Dict[str, object]] = {}
        self._last = time.monotonic()

    def collect(self, step: str, status: Optional[str] = None,
                skipped: Optional[str] = None) -> None:
        entry = self.steps.setdefault(step, {"status": None, "sites": [], "world": {}})
        sites = set(entry["sites"]) | set(duckdb_switch.opened())
        entry["sites"] = sorted(sites)
        for what, counts in (self.world.drain() if self.world else {}).items():
            held = entry["world"].setdefault(what, dict.fromkeys(counts, 0))
            for name, n in counts.items():
                held[name] += n
        # Since the last collect: what a slow sweep is slow on.
        now = time.monotonic()
        entry["seconds"] = round(float(entry.get("seconds", 0)) + now - self._last, 2)
        self._last = now
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
    left = {t for t in asyncio.all_tasks() - before
            if t is not current and not lives_with_the_server(t)}
    if left:
        _done, pending = await asyncio.wait(left, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=5)
    if reraise and failure is not None:
        raise failure
    return status


# The packages whose tasks are the server's, not a step's.
SERVER_PACKAGES = ("sse_starlette",)


def lives_with_the_server(task) -> bool:
    """A task a step started that belongs to the server rather than to the
    step: it lives until the process stops, so waiting for it would wait out
    the step's whole timeout and then cancel what production keeps. The one
    there is: sse_starlette's shutdown watcher, which the first event stream
    on a loop starts (PR-9's `/api/chat/stream`, 60 s a run until this)."""
    frame = getattr(task.get_coro(), "cr_frame", None)
    module = frame.f_globals.get("__name__", "") if frame is not None else ""
    return module.split(".")[0] in SERVER_PACKAGES


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
    """Nothing leaves the process but, where Postgres answers, Postgres
    itself: ClickHouse, Meilisearch, Telegram — and anything else — are
    refused at the socket, at once, rather than waited on. KeyCRM and
    TurboSMS never reach a socket: they are the world's (`sweep_world`)."""
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_getaddrinfo = socket.getaddrinfo
    # (host, port) as the DSN names it, and every address it resolves to —
    # what a socket is finally connected to is the address.
    allowed, addresses = set(), set()
    if PG_LIVE:
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
    if not PG_LIVE:
        # The pool refuses at once rather than through a DSN nothing answers.
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=refused("connection refused")))


@pytest.fixture
def sweep_world(monkeypatch):
    """KeyCRM and TurboSMS for the whole process, answering or refusing as
    the flavour says (`duckdb_off_sweep_world.py`), and recording either."""
    from tests.unit.duckdb_off_sweep_world import World, install

    world = World(answers=WORLD_ANSWERS)
    install(monkeypatch, world)
    return world


class Landed:
    """What Postgres wrote while a step ran, where it answers: per table, the
    row versions written by a transaction that began after the step did. A
    row's `xmin` is the transaction that wrote its current version, inserted
    or updated, so an order a fetch served again and the sync rewrote counts
    as much as a new one — "the write half ran", which is the question.

    Read on a connection of its own, one statement over every table the
    application's role can write (`_tables_sql`). Transaction ids are compared
    within one epoch, which a throwaway database never leaves."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.conn = None
        self._sql = ""

    async def _connection(self):
        if self.conn is None:
            import asyncpg

            self.conn = await asyncpg.connect(self.dsn)
            parts = []
            for schema, name in await self.conn.fetch(_tables_sql()):
                label = f"{schema}.{name}".replace("'", "''")
                parts.append(
                    f"SELECT '{label}' AS t, count(*) AS n FROM "
                    f"{_quoted(schema)}.{_quoted(name)} WHERE xmin::text::bigint > $1")
            self._sql = " UNION ALL ".join(parts)
        return self.conn

    async def mark(self) -> int:
        conn = await self._connection()
        xid = await conn.fetchval("SELECT pg_current_xact_id()::text::bigint")
        return int(xid) % (1 << 32)

    async def since(self, mark: int) -> Dict[str, int]:
        conn = await self._connection()
        return {r["t"]: r["n"] for r in await conn.fetch(self._sql, mark) if r["n"]}

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def landed_where_postgres_answers() -> Optional[Landed]:
    return Landed(os.environ["KS_PG_DSN"]) if PG_LIVE else None


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
    if not PG_LIVE:
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
    backfilled, step 13's `expenses_backfilled`, one order, and off's two
    facts (PR-2): step 13's record resolved in the frozen copy of
    `sync_metadata`, and that copy shipped after the owner rows."""
    import asyncpg

    from core import chain_latch, warehouse_cutover as wc
    from core.pg_operational import SYNC_METADATA_TABLE
    from core.write_chains import WRITE_CHAINS, chain_name
    from tests.unit.test_duckdb_off_sweep import WRITER_RESOLVED

    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            for chain in WRITE_CHAINS:
                stamp = chain_latch.latched_at(chain_name(chain))
                await chain_latch.claim(conn, tuple(chain.CHAIN_TABLES), stamp)
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_ok_at, backfilled_at) "
                "VALUES ('bronze.expenses', now(), now())")
            await conn.execute(
                "INSERT INTO app.sync_metadata (key, value, updated_at) "
                "VALUES ($1, $2, now())", wc.WRITER_KEY, json.dumps(WRITER_RESOLVED))
            # Later than every owner row, which carry this transaction's now().
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_ok_at) "
                "VALUES ($1, now() + interval '1 second')", SYNC_METADATA_TABLE)
            # History: the boot asks the store the orders are written to
            # whether there is any, and with none it pulls 730 days from
            # KeyCRM instead of taking the path every production boot takes.
            # Dated, as every order KeyCRM serves is: Silver's `order_date` is
            # NOT NULL, and an undated row failed the derivation (PR-9).
            await conn.execute(
                "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                "ordered_at, created_at, updated_at) VALUES (1, 4, 1, 100, "
                "now() - interval '400 days', now() - interval '400 days', "
                "now() - interval '400 days')")
            # Two people (PR-9's doors): a viewer whose session is real — the
            # dashboard's list approves them, so `_resolve_session` reads a
            # row and the viewer's sweep gets past it — and somebody waiting
            # in the bot's queue for the access-request doors to decide.
            from tests.unit.duckdb_off_sweep_doors import DENIED_ID, PENDING_ID, VIEWER_ID

            await conn.execute(
                "INSERT INTO app.dashboard_users (user_id, username, role, status) "
                "VALUES ($1, 'sweep-viewer', 'viewer', 'approved')", VIEWER_ID)
            await conn.execute(
                "INSERT INTO app.authorized_users (user_id, username, status, "
                "requested_at, denial_count) VALUES "
                "($1, 'sweep-viewer', 'approved', now(), 0), "
                "($2, 'sweep-pending', 'pending', now(), 0), "
                "($3, 'sweep-denied', 'pending', now(), 0)", VIEWER_ID, PENDING_ID, DENIED_ID)
            # The derivation's signal row, as revision 0033 seeds it and as
            # production holds it. Emptying the database took it too, and
            # without it Postgres' derivation is "signal unreadable" on every
            # tick: until PR-9 the live sweep's `pg_warehouse_derive` ran and
            # derived nothing.
            await conn.execute(
                "INSERT INTO meta.derivation_signal (layer, requested, built, "
                "requested_at) VALUES ('warehouse', 1, 0, now())")
            await _seed_live_history(conn)
    finally:
        await conn.close()


async def _seed_live_history(conn) -> None:
    """A small history in Bronze (PR-9): the catalogue, three customers who
    come back, a manager, and orders across two months and every active
    source — one with a promocode, one carrying UTM tags. Derived into
    Silver, Gold and the customer profile by Postgres itself where a sweep
    asks for it (`derive_in_postgres`), so a route reads rows and not an
    empty table: a loop over rows, a CSV with lines in it, a campaign with a
    roster are paths an empty database never takes. Ids clear of the one
    order above."""
    await conn.execute(
        "INSERT INTO bronze.categories (id, name, parent_id) VALUES "
        "(1, 'Care', NULL), (2, 'Serums', 1)")
    await conn.execute(
        "INSERT INTO bronze.products (id, name, category_id, brand, sku, price) VALUES "
        "(1, 'Serum A', 2, 'ab1', 'SKU-1', 500), (2, 'Cream B', 2, 'Brand B', 'SKU-2', 300), "
        "(3, 'Toner C', 1, 'ab1', 'SKU-3', 200)")
    await conn.execute(
        "INSERT INTO bronze.managers (id, name, status, is_retail) VALUES "
        "(1, 'Sweep Manager', 'active', true)")
    for buyer in (1, 2, 3):
        phone = f"+38050111111{buyer}"
        await conn.execute(
            "INSERT INTO bronze.buyers (id, full_name, phone, city, created_at, updated_at) "
            "VALUES ($1, $2, $3, 'Kyiv', now() - interval '90 days', now())",
            buyer, f"Sweep Buyer {buyer}", phone)
        await conn.execute(
            "INSERT INTO bronze.buyer_contacts (buyer_id, contact_type, value, is_primary) "
            "VALUES ($1, 'phone', $2, true)", buyer, phone)
    comment = "utm_source=facebook&utm_medium=cpc&utm_campaign=sweep"
    for i, days in enumerate((1, 2, 3, 5, 8, 12, 20, 35, 60)):
        order_id, source = 100 + i, (1, 2, 4)[i % 3]
        await conn.execute(
            "INSERT INTO bronze.orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, created_at, updated_at, buyer_id, manager_id, "
            "manager_comment, promocode) VALUES ($1, $2, 1, 1, $3, now() - make_interval("
            "days => $4), now() - make_interval(days => $4), now(), $5, $6, $7, $8)",
            order_id, source, 700 + 10 * i, days, 1 + i % 3, None if i % 4 else 1,
            comment if source == 4 else None, "ab1" if i == 2 else None)
        await conn.execute(
            "INSERT INTO bronze.order_products (id, order_id, product_id, name, quantity, "
            "price_sold) VALUES ($1, $2, 1, 'Serum A', 1, 500), ($3, $2, $4, 'Line', 1, $5)",
            order_id * 1000 + 1, order_id, order_id * 1000 + 2, 2 + i % 2, 200 + 10 * i)


async def derive_in_postgres(record: "Recorder", step: str) -> None:
    """Postgres derives Silver, Gold and the customer profile from the seeded
    Bronze — the scheduler's own derivation job, forced — recorded as a step
    of its own, so what it reaches is judged like any other. Live only."""
    import core.scheduler as scheduler_module

    ran = {}

    async def run():
        ran["result"] = await scheduler_module.get_scheduler()._run_pg_derivation(
            trigger="stage 5 sweep", force=True)

    status = await run_step(run())
    record.collect(step, status, skipped_by(ran.get("result")))
    record.steps[step]["result"] = json.loads(json.dumps(ran.get("result"), default=str))


@pytest.fixture
def state(canonical_postgres, monkeypatch, tmp_path, fresh_read_fallback):  # noqa: F811
    """The target state, in the flavour this process was started for, and
    the frozen file beside it. Returns the file."""
    from core import chain_latch
    from core.write_chains import WRITE_CHAINS, chain_name

    if PG_LIVE:
        # The local half of the latch first: the owner rows carry its stamp.
        for chain in WRITE_CHAINS:
            chain_latch.latch(chain_name(chain), chain.WRITE_ENV)
        asyncio.run(_seed_live_facts(os.environ["KS_PG_DSN"]))
    apply_target_state(monkeypatch, tmp_path, live=PG_LIVE)
    # A refusal under `get_last_sync_time` is recorded with its key.
    name_the_key(monkeypatch)
    import core.duckdb_store as store_module

    return FrozenFile(store_module.DB_PATH)


# ─── Web's start, as the sweeps run it ───────────────────────────────────────

def boot_patches(monkeypatch, record: "Recorder", prefix: str = "boot",
                 landed: Optional[Landed] = None) -> None:
    """Web's `startup_event` as production runs it, instrumented: the boot's
    sync and the scheduler's start each recorded as a step of their own
    (`<prefix>:init_and_sync`, `<prefix>:scheduler_start`), and what is left
    of the start as `<prefix>`. The scheduler starts paused — a sweep runs
    what it registered itself, and a trigger firing in the background would
    put its sites under somebody else's step. With `landed`, what the boot's
    sync wrote to Postgres is kept on its step.

    The bot's store is web's too: `init_database`, the access requests and
    the sign-ins reach it (PR-9). It runs for real where Postgres answers,
    with its local cache in the sweep's directory; with Postgres refusing it
    would end the start at the bot store's own revision check — fail closed,
    by design and before anything this sweep asks — so there it is skipped."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    import bot.store_sqlite as bot_sqlite
    import core.bot_store as bot_store
    import core.scheduler as scheduler_module
    import web.main as main

    monkeypatch.setattr(main, "validate_config", MagicMock())
    monkeypatch.setattr(bot_store, "_store", None)
    monkeypatch.setattr(bot_sqlite, "DB_PATH", Path(OUT) / f"bot-{record.sweep}.db")
    if not PG_LIVE:
        monkeypatch.setattr(main, "init_database", MagicMock())
    # Training is swept as its own job; the boot's ten-second delayed copy of
    # it would land inside whichever step happened to be running by then.
    monkeypatch.setattr(main, "_train_prediction_model", AsyncMock())
    real_start = AsyncIOScheduler.start
    monkeypatch.setattr(AsyncIOScheduler, "start",
                        lambda self, paused=False: real_start(self, paused=True))
    monkeypatch.setattr(scheduler_module, "_scheduler", None)

    real_sync, real_start_scheduler = main.init_and_sync, main.start_scheduler

    async def init_and_sync(**kwargs):
        # The boot sync's own exception goes back to `startup_event`, whose
        # handling of it — the refusal's branch, or the generic one — is swept.
        record.collect(prefix)
        step = f"{prefix}:init_and_sync"
        mark = await landed.mark() if landed else None
        try:
            await run_step(real_sync(**kwargs), reraise=True)
        except Exception as exc:
            record.collect(step, type(exc).__name__)
            raise
        finally:
            if landed:
                record.steps.setdefault(step, {"status": None, "sites": [], "world": {}})
                record.steps[step]["landed"] = await landed.since(mark)
        record.collect(step, "ok")

    async def start_scheduler():
        record.collect(prefix)
        status = await run_step(real_start_scheduler())
        record.collect(f"{prefix}:scheduler_start", status)
        return scheduler_module.get_scheduler()

    monkeypatch.setattr(main, "init_and_sync", init_and_sync)
    monkeypatch.setattr(main, "start_scheduler", start_scheduler)


async def control(record: "Recorder") -> None:
    """The tripwire still stands in this state: one reach, on purpose."""
    from core.duckdb_store import get_store

    async def reach():
        async with (await get_store()).connection():
            pass
    record.collect("control", await run_step(reach()))


# ─── The three sweeps ────────────────────────────────────────────────────────

def test_process(state, sweep_network, sweep_world, production_host, monkeypatch):
    """Web's real startup, then every job the scheduler registered, one at a
    time and in the order it registered them."""
    import core.scheduler as scheduler_module
    import core.sync_service as sync_module
    import web.main as main

    record = Recorder("process", sweep_world)
    landed = landed_where_postgres_answers()
    boot_patches(monkeypatch, record, landed=landed)

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

            mark = await landed.mark() if landed else None
            status = await run_step(run())
            record.collect(f"job:{job_id}", status, skipped_by(ran.get("result")))
            if landed:
                record.steps[f"job:{job_id}"]["landed"] = await landed.since(mark)
        await control(record)
        if scheduler._scheduler.running:
            scheduler._scheduler.shutdown(wait=False)
        if landed:
            await landed.close()
        from core.pg import close_pool
        await close_pool()
        return jobs

    jobs = asyncio.run(sweep())
    from core import shadow_writes

    # Which shadow chains reached their DuckDB half and stood it down (PR-4),
    # and which counted a failure there instead.
    record.write(jobs=jobs, file=state.unchanged(),
                 shadow_stood_down=sorted(shadow_writes.stood_down()),
                 shadow_failures=sorted(shadow_writes.failures()))


def answer_the_cohorts(monkeypatch) -> None:
    """ClickHouse is the cohorts' only engine under off, and no sweep has
    one: it refuses with Postgres, and answers an empty result beside a live
    one, so the cohort routes are swept past their engine where they can be."""
    from core import ch_cohorts

    fetch = (AsyncMock(return_value=[]) if PG_LIVE
             else AsyncMock(side_effect=refused("down")))
    monkeypatch.setattr(ch_cohorts, "fetch", fetch)


class Calls:
    """The HTTP half of a sweep: each request one step, on the sweep's loop
    (`duckdb_off_sweep_doors.Http`), with what it answered kept — the status
    per step, the surface a 503 names, and the bodies the outer test reads.
    With `landed`, a call asked to `measure` keeps what it wrote to Postgres
    on its step."""

    def __init__(self, record: "Recorder", app, landed: Optional[Landed] = None) -> None:
        from tests.unit.duckdb_off_sweep_doors import Http

        self.record, self.http, self.landed = record, Http(app), landed
        self.statuses: Dict[str, int] = {}
        self.surfaces: Dict[str, Optional[str]] = {}
        self.bodies: Dict[str, object] = {}
        # What a refusal said, cut short — the reason a door stopped where it
        # did, for whoever reads the run's JSON.
        self.details: Dict[str, str] = {}

    async def call(self, step: str, method: str, path: str, *, cookie=None,
                   params=None, body=None, keep_body: bool = False,
                   measure: bool = False):
        box = {}
        landed = self.landed if measure else None

        async def go():
            box["response"] = await self.http.request(
                method, path, cookie=cookie, params=params, body=body)

        mark = await landed.mark() if landed else None
        status = await run_step(go())
        response = box.get("response")
        if response is None:  # the request itself raised or timed out
            self.statuses[step] = 0
            self.record.collect(step, status)
        else:
            self.statuses[step] = response.status_code
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001 — a CSV, an HTML page, an event stream
                payload = None
            self.surfaces[step] = (payload.get("surface") if isinstance(payload, dict)
                                   else None)
            if keep_body:
                self.bodies[step] = payload if payload is not None else response.text
            if response.status_code >= 400:
                self.details[step] = response.text[:300]
            self.record.collect(step, str(response.status_code))
        if landed:
            self.record.steps[step]["landed"] = await landed.since(mark)
        return response


def test_routes(state, sweep_network, sweep_world, production_host, monkeypatch):
    """Every GET route under /api/, read off the app, as an admin — with its
    required parameters, then once per value of each optional parameter that
    can pick a branch, and with the header's filters together. In both
    flavours since PR-9: with Postgres refusing, and with it answering — what
    a route reaches only once Postgres has answered is in the second."""
    from tests.unit.duckdb_off_sweep_doors import hardcoded_admin, session_cookie
    from tests.unit.test_read_fallback_http import _request_for, swept_routes
    from web.main import app

    answer_the_cohorts(monkeypatch)
    admin = session_cookie(hardcoded_admin(), "admin")
    record = Recorder("routes", sweep_world)
    record.collect("before")
    routes, variants, health = {}, {}, {}

    async def sweep():
        if PG_LIVE:
            await derive_in_postgres(record, "routes:derive")
        calls = Calls(record, app)
        for endpoint in swept_routes(app):
            path, params = _request_for(endpoint)
            keep = endpoint.path in PAID_BY_PR8 or endpoint.path == "/api/health"
            await calls.call(f"GET {endpoint.path}", "GET", path, cookie=admin,
                             params=params, keep_body=keep)
            routes[endpoint.path] = calls.statuses[f"GET {endpoint.path}"]
            # Again with the optional values that can pick a branch the base
            # request never takes (`optional_variants`).
            for variant in optional_variants(endpoint):
                key = variant_key(endpoint.path, variant)
                await calls.call(f"GET {key}", "GET", path, cookie=admin,
                                 params={**params, **variant},
                                 keep_body=key in PAID_BY_PR8)
                variants[key] = calls.statuses[f"GET {key}"]
        # The probes whose answers the outer test reads, beside the sweep.
        for path, params in PROBES:
            key = variant_key(path, params)
            await calls.call(f"GET {key}", "GET", path, cookie=admin, params=params,
                             keep_body=True)
        await calls.http.close()
        from core.pg import close_pool
        await close_pool()
        return calls

    calls = asyncio.run(sweep())
    body = calls.bodies.get("GET /api/health")
    if isinstance(body, dict):
        # What the canary judges the week's probe by (PR-3).
        health.update(status=body.get("status"), degraded_by=body.get("degraded_by"),
                      duckdb=(body.get("duckdb") or {}).get("status"),
                      migrations=body.get("migrations"))
    record.write(routes=routes, variants=variants,
                 surfaces={step[4:]: s for step, s in calls.surfaces.items()},
                 health=health,
                 bodies={key: calls.bodies.get(f"GET {key}") for key in (
                     *PAID_BY_PR8, *(variant_key(p, q) for p, q in PROBES))},
                 file=state.unchanged())


# The routes stage 5's PR-8 decoupled, whose answers the sweep keeps.
PAID_BY_PR8 = ("/api/warehouse/status", "/api/summary?source_id=3")
# Requests beside the sweep whose answers it keeps: the month's summary,
# which holds the seeded orders when Postgres answers (PR-9).
PROBES = (("/api/summary", {"period": "month"}),)


def test_doors(state, sweep_network, sweep_world, production_host, monkeypatch):
    """PR-9: what a week of silence exercises beyond the GET routes and the
    jobs — every other door, after web's real start and before its stop.
    `tests/unit/duckdb_off_sweep_doors.py` says what each door is made of."""
    import web.main as main
    from core.chat_tools import TOOLS, execute_tool
    from core.events import events as bus
    from tests.unit import duckdb_off_sweep_doors as doors
    from tests.unit.test_read_fallback_http import _request_for
    from web.main import app
    from web.services import chat_service

    for name, value in doors.FAKE_ENV.items():
        monkeypatch.setenv(name, value)
    answer_the_cohorts(monkeypatch)
    model = doors.FakeModel(TOOLS)
    monkeypatch.setattr(chat_service, "get_llm_client", lambda: model)
    monkeypatch.setattr(chat_service, "_chat_service", None)
    monkeypatch.setattr(chat_service.ChatService, "_conversations", {})
    # The bus is a process singleton: the handlers swept are the ones this
    # start registers, not the process sweep's as well.
    monkeypatch.setattr(bus, "_handlers", {})
    monkeypatch.setattr(bus, "_wildcard_handlers", [])

    record = Recorder("doors", sweep_world)
    landed = landed_where_postgres_answers()
    boot_patches(monkeypatch, record, prefix="setup", landed=landed)
    admin = doors.session_cookie(doors.hardcoded_admin(), "admin")
    viewer = doors.session_cookie(doors.VIEWER_ID, "viewer")
    who = (("nobody", None), ("viewer", viewer), ("admin", admin))
    sockets, events, tools, handlers = {}, {}, {}, {}

    async def open_and_talk(path: str, cookie, step: str) -> None:
        socket = doors.Socket(app, path, cookie)

        async def go():
            if await socket.open():
                for frame in doors.CLIENT_FRAMES:
                    await socket.say(frame)
            await socket.close()

        status = await run_step(go())
        record.collect(step, status)
        sockets[step] = {"accepted": socket.accepted, "close_code": socket.close_code,
                         "events": socket.events()}

    async def sweep():
        import core.scheduler as scheduler_module

        record.collect("before")
        record.collect("setup", await run_step(main.startup_event()))
        if PG_LIVE:
            await derive_in_postgres(record, "setup:derive")
        calls = Calls(record, app, landed)
        # The trigger door is given a job the scheduler registered; any one,
        # since a paused scheduler only notes the request.
        jobs = list(scheduler_module.get_scheduler()._job_info)
        if jobs:
            monkeypatch.setitem(doors.EXAMPLE_VALUES, "job_id", jobs[0])

        # A real viewer's session, unpatched, on every GET route under /api/.
        for endpoint in doors.get_api_routes(app):
            path, params = _request_for(endpoint)
            await calls.call(f"viewer GET {endpoint.path}", "GET", path,
                             cookie=viewer, params=params)

        # Every route under /api/ that is not a GET, as an admin, with what
        # each wrote to Postgres kept on its step.
        for method, endpoint in doors.write_routes(app):
            key = doors.door_key(method, endpoint)
            path, params, body = doors.example_request(method, endpoint)
            if key == "POST /api/webhooks/turbosms":
                # A report on a message the send recorded, where one was sent.
                body = doors.webhook_body(*sweep_world.message_ids[:1])
            response = await calls.call(key, method, path, cookie=admin, params=params,
                                        body=body, keep_body=key == "POST /api/chat",
                                        measure=True)
            # Again with the optional values that can pick a branch — the
            # inline run behind `background=false`, a dry run, a sales type.
            for variant in optional_variants(endpoint):
                await calls.call(f"{method} {variant_key(endpoint.path, variant)}",
                                 method, path, cookie=admin,
                                 params={**params, **variant}, body=body, measure=True)
            # What a door made, the next door at its path is given.
            made = doors.made_by(key, response)
            for name, value in made.items():
                monkeypatch.setitem(doors.EXAMPLE_VALUES, name, value)

        # The GET routes the admin's sweep leaves out for the network they
        # would reach (`NETWORK_ROUTES`) — the assistant's stream, where the
        # model is the sweep's own — each as an admin.
        for endpoint in doors.network_routes(app):
            path, params, _body = doors.example_request("GET", endpoint)
            await calls.call(f"admin GET {endpoint.path}", "GET", path, cookie=admin,
                             params=params, keep_body=True)

        # Every route outside /api/, as nobody, a viewer and an admin; the two
        # Telegram sign-ins signed as Telegram signs them, for the viewer.
        for method, endpoint in doors.outside_routes(app):
            path, params, body = doors.example_request(method, endpoint)
            if endpoint.path == "/auth/telegram/callback":
                params = doors.telegram_login_query(doors.VIEWER_ID)
            if endpoint.path == "/auth/webapp":
                body = doors.webapp_body(doors.VIEWER_ID)
            for name, cookie in who:
                await calls.call(f"{method} {endpoint.path} as {name}", method, path,
                                 cookie=cookie, params=params, body=body)

        # Every WebSocket route: the handshake, the client's frames, the close.
        for endpoint in doors.socket_routes(app):
            for name, cookie in who:
                await open_and_talk(endpoint.path, cookie,
                                    f"WS {endpoint.path} as {name}")

        # Every event the bus carries, with a socket open in each room, so
        # the handlers web registered at its start broadcast to somebody.
        # The viewer listens too, wherever it is let in: a broadcast scoped to
        # a tab reaches only the sockets whose viewer may open it.
        listeners = {f"{e.path} as {name}": doors.Socket(app, e.path, cookie)
                     for e in doors.socket_routes(app)
                     for name, cookie in (("admin", admin), ("viewer", viewer))}
        for listener in listeners.values():
            await listener.open()
        record.collect("events:open", "ok")
        for event in doors.events():
            handlers[event.value] = len(bus._handlers.get(event, [])) + len(
                bus._wildcard_handlers)
            step = f"event:{event.value}"
            record.collect(step, await run_step(bus.emit(event, dict(doors.EVENT_PAYLOAD))))
        for listener in listeners.values():
            await listener.close()
        record.collect("events:close", "ok")
        events.update({label: listener.events() for label, listener in listeners.items()})

        # Every tool, straight through `execute_tool`, with every value of
        # each enum that picks a branch. A tool that acts on something by id
        # is given one an earlier tool made (`doors.made_by_tool`), a tool of
        # its own, in the order they come — the delete one, the update the
        # next — so it acts on a row rather than answering "not found".
        made: Dict[str, List[int]] = {}
        taken: Dict[str, int] = {}
        for tool in TOOLS:
            inputs = doors.tool_inputs(tool)
            for name in set(inputs[0]) & set(made):
                if taken.get(name, 0) < len(made[name]):
                    inputs = [{**data, name: made[name][taken.get(name, 0)]}
                              for data in inputs]
                    taken[name] = taken.get(name, 0) + 1
            for data in inputs:
                step = doors.tool_key(tool["name"], data, inputs[0])
                box = {}

                async def go(name=tool["name"], data=data):
                    box["result"] = await execute_tool(name, data)

                status = await run_step(go())
                record.collect(step, status)
                result = box.get("result")
                tools[step] = (result.get("error") if isinstance(result, dict) else None)
                for name, ids in doors.made_by_tool(result).items():
                    made.setdefault(name, []).extend(ids)

        # Web's stop: the scheduler, the prediction service, KeyCRM, the store.
        record.collect("shutdown", await run_step(main.shutdown_event()))
        await control(record)
        await calls.http.close()
        if landed:
            await landed.close()
        from core.pg import close_pool
        await close_pool()
        return calls

    calls = asyncio.run(sweep())
    chat = calls.bodies.get("POST /api/chat")
    stream = calls.bodies.get("admin GET /api/chat/stream")
    record.write(
        statuses=calls.statuses, surfaces=calls.surfaces, details=calls.details,
        sockets=sockets,
        events=events, handlers=handlers, tools=tools,
        writes=[doors.door_key(m, e) for m, e in doors.write_routes(app)],
        outside=[doors.door_key(m, e) for m, e in doors.outside_routes(app)],
        socket_routes=[e.path for e in doors.socket_routes(app)],
        network=[e.path for e in doors.network_routes(app)],
        message_ids=sweep_world.message_ids,
        tool_names=[t["name"] for t in TOOLS],
        chat={"answer": chat if isinstance(chat, dict) else None,
              "stream": doors.stream_events(stream) if isinstance(stream, str) else None,
              "results_seen": model.results_seen},
        file=state.unchanged())
