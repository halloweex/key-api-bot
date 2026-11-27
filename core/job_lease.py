"""Who executes a scheduler job, and the lease that keeps a job in one place.

K0 of the Rust plan (edition 4, `.planning/RUST_MIGRATION_PLAN.md`): any job
`core/scheduler.py` registers can be handed to a Rust process one at a time,
without touching the others, and one job never runs in two places at once —
neither Python beside Rust, nor two Python processes. Two things do it, and
both live in Postgres, because Postgres is the one thing both processes see.

THE SWITCH — WHO EXECUTES

A row of `meta.chain_watermarks`, the store's key-value bookkeeping (revision
0032), keyed `executor:<job_id>`. Its value is exactly `rust` — compared byte
for byte, no trimming, no case folding — and the job is Rust's. Anything else,
no row included, and the job is Python's: `python` is the spelling the CLI
writes, and any other text is read as Python's with a warning. One rule both
sides can compute the same way, so there is no value both think is theirs,
and none neither does. Python and Rust read it before every run; neither
writes it. A person does, with `scripts/job_executor.py`, so switching and
switching back need neither a deploy nor a restart: the next tick reads it.

No row exists until somebody switches a job, so until then every job runs in
Python exactly as before this module.

THE LEASE — ONE RUN AT A TIME

`pg_try_advisory_lock(key)`, session-level, taken by whoever the switch names
just before a run and released right after it. Not granted means the job is
running somewhere else: the run is skipped, the scheduler records `skipped`,
and the next tick asks again. Never `pg_advisory_lock`, which would wait: a
tick that waits for a lease is a tick that piles up behind the run it is
meant to give way to.

The key is the 64-bit integer whose hex is `6b734a4c` ('ks', 'JL' for "job
lease", the `0x6b73…` convention of this repository's other advisory keys)
followed by the first eight hex digits of sha256 of the job id in UTF-8:
`lease_key`, and `LEASE_KEY_SQL` for the same number computed by Postgres.
Every key in that prefix is a job's; nothing else may take one. The other
advisory locks here are transaction-scoped and sit under `0x6b730000…` or
below, so no existing key falls in it.

Python holds every lease of its process on one session of its own, opened
for the purpose (`JobLeases`), never a pooled connection: a lease lives as
long as its session, and the web pool has five connections for every
request. A session's advisory locks are re-entrant, so two runs of one job
inside this process — a catch-up beside its scheduled twin, an admin lever
beside the minute tick — both hold it, exactly as they both ran before this
module; inside one process the scheduler's own locks (`_heavy_job_lock`,
`PG_LAYER_LOCK`, APScheduler's `max_instances`) keep their order, untouched.
The lease is for the other processes.

WHEN POSTGRES CANNOT BE ASKED

A job whose switch cannot be read runs in Python, without a lease — the
behaviour from before this module, so a Postgres outage costs no job that
ran through one before (the memory and disk watchdogs matter most then) —
except a job this process last saw switched to Rust, which stays skipped.
The residual case is named, not hidden: a process that starts while it
cannot reach Postgres, for a job switched to Rust, runs it unguarded until
it can. A lease whose session ended during its run is reported at its
release (`lease lost`), because the lock went with the session and another
process could have taken it meanwhile.

`KS_JOB_LEASES=off` turns all of it off — no switch is read, no lease taken,
every job runs in Python as before this module — and must never be used
while a job is switched to Rust.

THE CONTRACT FOR THE OTHER SIDE

- Read `SELECT value FROM meta.chain_watermarks WHERE key = 'executor:<job>'`
  before every run, in the database Python uses (advisory locks are per
  database). Run only when it is exactly `rust`.
- Then `SELECT pg_try_advisory_lock(<key>)` on a session that stays open for
  the whole run; false means skip. Release with `pg_advisory_unlock(<key>)`
  right after the run, and hold nothing between runs.
- Never take a key of this prefix for anything but the job it names, or a
  name with a `:` in it, which no scheduler job carries: a shadow run takes
  `<job_id>:shadow`'s key by the same formula, never the job's own.
- Name the session in `application_name`: Python's skip names the holder.
- Read access to `meta.chain_watermarks` is all it needs; an advisory lock
  needs no grant.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)

# ─── The switch ───────────────────────────────────────────────────────────────

SWITCH_TABLE = "meta.chain_watermarks"
SWITCH_PREFIX = "executor:"
PYTHON = "python"
RUST = "rust"
EXECUTORS = (PYTHON, RUST)

READ_SWITCH_SQL = f"SELECT value FROM {SWITCH_TABLE} WHERE key = $1"

# The only statement that writes a switch; `scripts/job_executor.py` runs it,
# and nothing in the scheduler does. A row is never deleted: `python` is the
# way back, and `updated_at` then says when it was taken.
WRITE_SWITCH_SQL = (
    f"INSERT INTO {SWITCH_TABLE} (key, value, updated_at) VALUES ($1, $2, now()) "
    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "
    "updated_at = EXCLUDED.updated_at"
)
READ_SWITCH_FOR_UPDATE_SQL = f"SELECT value FROM {SWITCH_TABLE} WHERE key = $1 FOR UPDATE"
READ_ALL_SWITCHES_SQL = (
    f"SELECT key, value, updated_at FROM {SWITCH_TABLE} WHERE starts_with(key, $1) "
    "ORDER BY key"
)


def switch_key(job_id: str) -> str:
    """`executor:<job_id>`, the switch's row."""
    return SWITCH_PREFIX + job_id


def executor_of(value: Optional[str]) -> str:
    """Who executes, from the switch's raw value: Rust for exactly `rust`,
    Python for everything else, a missing row included."""
    return RUST if value == RUST else PYTHON


# ─── The lease ────────────────────────────────────────────────────────────────

LEASE_PREFIX_HEX = "6b734a4c"
LEASE_PREFIX = int(LEASE_PREFIX_HEX, 16)

# The same number Postgres computes from the job id, for an operator reading
# `pg_locks` and for the Rust side to check its own against.
LEASE_KEY_SQL = (
    f"('x' || '{LEASE_PREFIX_HEX}' || "
    "left(encode(sha256(convert_to($1::text, 'UTF8')), 'hex'), 8))::bit(64)::bigint"
)

TRY_LOCK_SQL = "SELECT pg_try_advisory_lock($1::bigint)"
UNLOCK_SQL = "SELECT pg_advisory_unlock($1::bigint)"

# Who holds a key, best effort, for the skip's one line. A bigint key shows in
# `pg_locks` as its high and low halves, `objsubid` 1, in this database.
HOLDER_SQL = (
    "SELECT l.pid, a.application_name FROM pg_locks l "
    "LEFT JOIN pg_stat_activity a ON a.pid = l.pid "
    "WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 "
    "AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
    "AND l.classid = $1::bigint::oid AND l.objid = $2::bigint::oid "
    "ORDER BY l.pid LIMIT 1"
)


def lease_key(job_id: str) -> int:
    """The job's advisory key: hex `6b734a4c` and the first eight hex digits
    of sha256(job_id). Positive, so it is the same number as a bigint."""
    digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()
    return int(LEASE_PREFIX_HEX + digest[:8], 16)


# ─── The kill switch ──────────────────────────────────────────────────────────

LEASES_ENV = "KS_JOB_LEASES"
_OFF = frozenset({"off", "0", "false"})
_ON = frozenset({"", "on", "1", "true"})
_warned_env_values: Set[str] = set()


def leases_on() -> bool:
    """True unless `KS_JOB_LEASES` says off/0/false. An unrecognised value
    keeps them on and says so once: the leases are what keep a switched job
    out of Python, and a typo must not let it back in."""
    raw = os.getenv(LEASES_ENV, "")
    value = raw.strip().lower()
    if value in _OFF:
        return False
    if value not in _ON and value not in _warned_env_values:
        _warned_env_values.add(value)
        logger.warning("%s=%r is not on/off/1/0/true/false: job leases stay on",
                       LEASES_ENV, raw)
    return True


# ─── What a claim answers ─────────────────────────────────────────────────────

EXECUTED_BY_RUST = "executed_by_rust"
LEASE_HELD = "lease_held"


@dataclass(frozen=True)
class NotRunHere:
    """A run this process does not make, and why. What the scheduler returns
    in place of the job's result, so its history says `skipped`."""

    job_id: str
    reason: str
    detail: Mapping[str, Any] = field(default_factory=dict)

    def as_result(self) -> Dict[str, Any]:
        return {"skipped": True, "reason": self.reason, **dict(self.detail)}

    def sentence(self) -> str:
        if self.reason == EXECUTED_BY_RUST:
            said = (f"{SWITCH_TABLE} '{switch_key(self.job_id)}' says {RUST}")
            if self.detail.get("remembered"):
                said += (" (as last read; the switch could not be read now: "
                         f"{self.detail.get('error')})")
            return f"Job {self.job_id} not run here: {said}"
        holder = self.detail.get("holder_pid")
        who = (f"pid {holder} ({self.detail.get('holder_application') or 'no application_name'})"
               if holder is not None else "another session")
        return (f"Job {self.job_id} not run here: its lease "
                f"{self.detail.get('lease_key')} is held by {who}; the next tick asks again")


class RunsElsewhere(Exception):
    """Raised for a caller outside the schedule that must not run the job
    here — an admin lever — carrying the decision."""

    def __init__(self, decision: NotRunHere):
        super().__init__(decision.sentence())
        self.decision = decision


def log_not_run(decision: NotRunHere) -> None:
    """The skip's one line. Rust's turn is the steady state once a job is
    switched, so INFO; a lease held elsewhere means something else is running
    this job, so WARNING."""
    level = logging.INFO if decision.reason == EXECUTED_BY_RUST else logging.WARNING
    logger.log(level, decision.sentence())


class Lease:
    """A run this process makes. `release()` is idempotent and never raises;
    an unguarded lease (no store, or none reachable) releases nothing."""

    __slots__ = ("job_id", "key", "generation", "_owner", "_released")

    def __init__(self, job_id: str, key: int, generation: int,
                 owner: Optional["JobLeases"]):
        self.job_id = job_id
        self.key = key
        self.generation = generation
        self._owner = owner
        self._released = False

    @property
    def guarded(self) -> bool:
        return self._owner is not None

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        if self._owner is not None:
            await self._owner._release(self)


def unguarded(job_id: str) -> Lease:
    return Lease(job_id, lease_key(job_id), 0, None)


Claim = Union[Lease, NotRunHere]


class LeaseStoreUnavailable(Exception):
    """The switch or the lease could not be asked. `session_lost`: a session
    that had been working broke, which one fresh session may cure at once."""

    def __init__(self, said: str, *, session_lost: bool = False):
        super().__init__(said)
        self.session_lost = session_lost


class _Lost(Exception):
    """The session a lease was taken on is gone, and the lock with it."""


# ─── The session ──────────────────────────────────────────────────────────────

APPLICATION_NAME = "ks-web-jobs"
CONNECT_TIMEOUT_S = 5      # whole seconds: asyncpg's `connect(timeout=)` is an int
STATEMENT_TIMEOUT_S = 5.0
# After a failed connect, the ticks inside this window answer "unreachable" at
# once rather than each waiting out the connect timeout again.
RETRY_AFTER_S = 30.0


def _said(exc: BaseException) -> str:
    text = " ".join(str(exc).split())
    return f"{type(exc).__name__}: {text}"[:200] if text else type(exc).__name__


class JobLeases:
    """This process's leases, on one Postgres session of their own.

    Every statement runs under one asyncio lock (a session runs one statement
    at a time) and is bounded: a hung server costs a tick `STATEMENT_TIMEOUT_S`
    and then the job runs as it did before leases. The session is opened on
    first use and again after it is lost; an unlock that could not be sent is
    retried on the next use of the same session, so a lease is never left
    held by a session that goes on living.
    """

    def __init__(self, dsn: str, *,
                 application_name: str = APPLICATION_NAME,
                 connect_timeout_s: int = CONNECT_TIMEOUT_S,
                 statement_timeout_s: float = STATEMENT_TIMEOUT_S,
                 retry_after_s: float = RETRY_AFTER_S):
        self._dsn = dsn
        self._application_name = application_name
        self._connect_timeout_s = connect_timeout_s
        self._statement_timeout_s = statement_timeout_s
        self._retry_after_s = retry_after_s
        self._conn: Any = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._io_lock: Optional[asyncio.Lock] = None
        self._generation = 0
        self._retry_at = 0.0
        self._last_error: Optional[str] = None
        self._down = False
        # job_id -> the switch's raw value as last read; what a job switched
        # to Rust is still known by while the switch cannot be read.
        self._last_known: Dict[str, Optional[str]] = {}
        self._warned_values: Set[Tuple[str, str]] = set()
        # (key, generation) of unlocks that could not be sent.
        self._owed_unlocks: List[Tuple[int, int]] = []

    # The claim ───────────────────────────────────────────────────────────────

    async def claim(self, job_id: str) -> Claim:
        """Run here (a `Lease`) or not (`NotRunHere`): the switch, then the
        lease. Never raises but a cancellation: a fault here must not stop a
        job that ran before."""
        return await self._claim(job_id, honour_switch=True)

    async def claim_lease(self, job_id: str) -> Claim:
        """The job's lease alone, whoever the switch names: for code that
        writes what the job writes without being the job, so that it never
        runs beside the job in another process. A `Lease`, or `NotRunHere`
        with `lease_held`; unguarded when the store cannot be asked."""
        return await self._claim(job_id, honour_switch=False)

    async def _claim(self, job_id: str, *, honour_switch: bool) -> Claim:
        key = lease_key(job_id)
        for attempt in range(2):
            try:
                decision = await self._decide(job_id, key, honour_switch=honour_switch)
            except LeaseStoreUnavailable as exc:
                # A session that died since its last use (Postgres restarted,
                # the connection reset) is replaced once, at once: running
                # this tick unguarded would be the worse answer.
                if exc.session_lost and attempt == 0:
                    continue
                self._note_down(exc)
                if honour_switch and executor_of(self._last_known.get(job_id)) == RUST:
                    return NotRunHere(job_id, EXECUTED_BY_RUST, {
                        "executor": RUST, "remembered": True, "error": str(exc)})
                return unguarded(job_id)
            except Exception as exc:  # noqa: BLE001 — never the job's failure
                logger.error("Job %s: its lease could not be claimed (%s); it "
                             "runs without one", job_id, _said(exc), exc_info=True)
                return unguarded(job_id)
            self._note_up()
            return decision
        return unguarded(job_id)   # not reached: the second attempt returns

    async def _decide(self, job_id: str, key: int, *, honour_switch: bool) -> Claim:
        async with self._session() as conn:
            if honour_switch:
                raw = await conn.fetchval(READ_SWITCH_SQL, switch_key(job_id))
                self._last_known[job_id] = raw
                if executor_of(raw) == RUST:
                    return NotRunHere(job_id, EXECUTED_BY_RUST,
                                      {"executor": RUST, "remembered": False})
                self._note_odd_value(job_id, raw)
            if await conn.fetchval(TRY_LOCK_SQL, key):
                return Lease(job_id, key, self._generation, self)
            # The key as text: a JSON reader in doubles (jq, a browser) would
            # round a 64-bit number.
            return NotRunHere(job_id, LEASE_HELD, {
                "lease_key": str(key), **await self._holder(conn, key)})

    async def _holder(self, conn, key: int) -> Dict[str, Any]:
        try:
            row = await conn.fetchrow(HOLDER_SQL, key >> 32, key & 0xFFFFFFFF)
        except Exception:  # noqa: BLE001 — who holds it is a courtesy
            return {"holder_pid": None, "holder_application": None}
        if row is None:
            return {"holder_pid": None, "holder_application": None}
        return {"holder_pid": row["pid"], "holder_application": row["application_name"]}

    async def _release(self, lease: Lease) -> None:
        try:
            async with self._session(generation=lease.generation) as conn:
                released = await conn.fetchval(UNLOCK_SQL, lease.key)
        except _Lost:
            self._report_lost(lease)
            return
        except LeaseStoreUnavailable as exc:
            if self._conn is None or self._generation != lease.generation:
                self._report_lost(lease)    # the unlock found the session gone
                return
            self._owed_unlocks.append((lease.key, lease.generation))
            logger.warning("Job %s: its lease %s could not be released now "
                           "(%s); the next use of the session releases it",
                           lease.job_id, lease.key, exc)
            return
        except Exception as exc:  # noqa: BLE001 — a release never fails its job
            self._owed_unlocks.append((lease.key, lease.generation))
            logger.error("Job %s: releasing its lease failed (%s)",
                         lease.job_id, _said(exc), exc_info=True)
            return
        if not released:
            logger.error("Job %s: its lease %s was not held at its release",
                         lease.job_id, lease.key)

    @staticmethod
    def _report_lost(lease: Lease) -> None:
        logger.error(
            "Job %s: its lease %s was lost during the run (its session ended), "
            "so another process could have run it meanwhile",
            lease.job_id, lease.key)

    # The session ─────────────────────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _session(self, *, generation: Optional[int] = None) -> AsyncIterator[Any]:
        """The session, under the lock. With `generation`, only that session:
        a release never opens a new one, which would hold nothing."""
        self._bind_loop()
        assert self._io_lock is not None
        async with self._io_lock:
            conn = self._conn
            if generation is not None and (
                    generation != self._generation or conn is None or conn.is_closed()):
                raise _Lost()
            if conn is None or conn.is_closed():
                conn = await self._connect()
            try:
                await self._pay_owed_unlocks(conn)
                yield conn
            except (_Lost, LeaseStoreUnavailable):
                raise
            except Exception as exc:
                import asyncpg

                # A server error or a timeout on a session still open leaves it
                # sound, with every lease on it; dropping it would release
                # them. Anything else, or a session that closed, is gone.
                lost = conn.is_closed() or not isinstance(
                    exc, (asyncpg.PostgresError, asyncio.TimeoutError))
                if lost:
                    self._drop(conn)
                raise LeaseStoreUnavailable(_said(exc), session_lost=lost) from exc

    async def _pay_owed_unlocks(self, conn) -> None:
        owed, self._owed_unlocks = self._owed_unlocks, []
        for key, generation in owed:
            if generation == self._generation:
                await conn.fetchval(UNLOCK_SQL, key)

    async def _connect(self):
        if time.monotonic() < self._retry_at:
            raise LeaseStoreUnavailable(self._last_error or "Postgres was unreachable")
        import asyncpg

        try:
            conn = await asyncpg.connect(
                self._dsn, timeout=self._connect_timeout_s,
                command_timeout=self._statement_timeout_s,
                server_settings={"application_name": self._application_name})
        except Exception as exc:
            self._retry_at = time.monotonic() + self._retry_after_s
            self._last_error = _said(exc)
            raise LeaseStoreUnavailable(self._last_error) from exc
        self._conn = conn
        self._generation += 1
        self._owed_unlocks = []
        return conn

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is loop:
            return
        # A session made on a loop that has gone cannot be used from this one.
        if self._conn is not None:
            self._drop(self._conn)
        self._loop = loop
        self._io_lock = asyncio.Lock()

    def _drop(self, conn) -> None:
        with contextlib.suppress(Exception):
            conn.terminate()
        if self._conn is conn:
            self._conn = None

    def terminate(self) -> None:
        """Close the session at shutdown; Postgres releases what it held."""
        if self._conn is not None:
            self._drop(self._conn)

    # What the log says ───────────────────────────────────────────────────────

    def _note_odd_value(self, job_id: str, raw: Optional[str]) -> None:
        if raw is None or raw in EXECUTORS or (job_id, raw) in self._warned_values:
            return
        self._warned_values.add((job_id, raw))
        logger.warning("%s '%s' = %r is neither %r nor %r: the job stays in Python",
                       SWITCH_TABLE, switch_key(job_id), raw, PYTHON, RUST)

    def _note_down(self, exc: BaseException) -> None:
        if self._down:
            return
        self._down = True
        logger.warning("Job leases unavailable (%s): jobs run without a lease, "
                       "and a job last seen switched to Rust stays skipped, "
                       "until Postgres answers again", exc)

    def _note_up(self) -> None:
        if self._down:
            self._down = False
            logger.info("Job leases available again")


def from_environment() -> Optional[JobLeases]:
    """The process's leases, or None: off by `KS_JOB_LEASES`, or no Postgres
    configured (a developer's machine), where every job runs as before."""
    if not leases_on():
        logger.warning("%s=off: no job's switch is read and no lease is taken; "
                       "never while a job is switched to Rust", LEASES_ENV)
        return None
    from core.pg import DSN_ENV

    dsn = os.getenv(DSN_ENV, "").strip()
    if not dsn:
        logger.info("Job leases off: %s is not set", DSN_ENV)
        return None
    return JobLeases(dsn)


# ─── The switch's one writer ──────────────────────────────────────────────────

async def set_executor(conn, job_id: str, executor: str) -> Optional[str]:
    """Write the switch, in one transaction, and return what it said before.
    `scripts/job_executor.py` is the only caller."""
    if executor not in EXECUTORS:
        raise ValueError(f"executor must be one of {EXECUTORS}, not {executor!r}")
    async with conn.transaction():
        before = await conn.fetchval(READ_SWITCH_FOR_UPDATE_SQL, switch_key(job_id))
        await conn.execute(WRITE_SWITCH_SQL, switch_key(job_id), executor)
    return before
