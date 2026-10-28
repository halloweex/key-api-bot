"""`KS_DUCKDB`: whether this process may open the DuckDB file at all.

WHY A SWITCH BEFORE THE CODE IS READY FOR IT

Stage 5 ends DuckDB, and it may start only after a week in which nothing
opened the file (the week of silence, OD-17 (a)). "Nothing opened it" cannot
be shown by reading the code — 78 write paths were found by walking it, not by
remembering them — so it has to be shown by running: web with DuckDB switched
off, and every path that still reaches for the file caught in the act. This is
that switch and that catch. It is built before stage 5's decoupling on
purpose: the tripwire and its page have to be proven before the week they
judge, not during it.

`on`, the default, is today: the file is opened exactly as before, through the
one opener below. `off` means **web must not open the file**: any path that
would is refused before `duckdb.connect` runs — so the file is not even
created — with `DuckDBOpenedWhileOff`, counted, logged at CRITICAL, published
on `/api/health` as `duckdb_switch.opened_while_off` (sites and counts, never
an exception's text: the endpoint is public), and paged CRITICAL by the canary
as `duckdb_opened_while_off`.

**What `off` does is stage 5's code decoupling**
(`.planning/DUCKDB_EXIT_STAGE5_DECOUPLING.md`). Since its PR-1, `get_store()`
under `off` hands out the store unconnected, so a caller that only hosts a
Postgres router on it works, and the tripwire stands at `connection()`,
where the file is reached. What still reaches it is the stage 5 sweep's
`KNOWN_RESIDUAL` (`tests/unit/test_duckdb_off_sweep.py`); what used to be
inherited from DuckDB's `sync_metadata` — the order step's window among it —
is read from its Postgres copy since PR-5 (`core/pg_frozen_metadata.py`),
and the list is empty since PR-0 to PR-8 together. PR-9 widened the sweep
to every door a week exercises — every route that is not a GET and every
one outside `/api/`, the sockets, the events, the assistant's tools and
routes, a real viewer's session, web's stop — with Postgres refusing and
with it answering on a derived history, each with KeyCRM and TurboSMS
answering and refusing, and walked the code for any way in
the sweep does not open (`tests/unit/test_duckdb_off_entry_points.py`): the
list stayed empty. Web's startup contains a refusal rather than dying of it,
so `/api/health` answers and the page can be seen. That is the code's half
of the week's licence, not the licence: the host tools (PR-10), the second
Ark (PR-11), the rehearsal on the production copies and the owner's own yes
come before anybody sets it in production.

COUNTED WHERE IT IS RAISED

The count is taken at the raise, not where the exception lands. Dozens of
callers wrap `get_store()` in `except Exception`, and a refusal one of them
swallows is exactly the open the week of silence must not miss.

ONE OPENER

`open_file` is the only reach for a driver function in `core/`, `web/` and
`bot/` — `duckdb.connect`, and every function that runs on the driver's
default connection, which `ATTACH` points at any file — however the driver is
spelled: imported, re-exported by another module, imported by name.
`tests/unit/test_duckdb_switch.py` walks those trees (and `scripts/` and
`deploy/`, whose host-side tools are exempt by name and checked to still
exist) so a second opener fails the suite rather than slipping past the
switch. The weekly compaction's phase 1 — the one scheduled process outside
web that opens the live file, read-only, which the file's hash cannot see —
opens through it too.

AN UNKNOWN VALUE DOES NOT STOP A SCRIPT

`KS_READ_FALLBACK`'s rule and OD-09's: a typo runs as `on`, logs at ERROR and
is published as `duckdb_switch.error`. In web it no longer can (below): web
reads the variable for nothing since S14-00, so the canary's
`duckdb_mode_invalid` has nothing left to fire on and goes with the block.

Read once, in `core.runtime_modes.configure_modes()`, before the boot sync —
and, unlike the other cached modes, lazily on the first open if no entry point
configured it: a script that forgot `configure_modes()` must not be the one
process that opens the file under `off`.

WEB IS ALWAYS OFF (step 14's S14-00)

Stage 5's PR-2 made `off` a claim checked at web's start: one precondition
unmet and web ran `on`. That verdict (`core/duckdb_off.py`) is gone. Since
S14-00 **web runs `off` whatever `KS_DUCKDB` says**: `configure_modes(web=
True)` configures this switch as web's, the value read is published as
`value` and one WARNING says when it is not `off`, and `held_back` is
published as `[]`. Every path web takes is a Postgres path (the step-14
constants, `.planning/DUCKDB_EXIT_STEP14_PLAN.md` S14-00), and the hourly
host hash of the file must keep reading UNCHANGED until the file is deleted.
The tripwire — `guard`, `open_file`, `opened_while_off` and the canary's
`duckdb_opened_while_off` — stays until step 14's S14-18.

A script and a host tool are not web: they read `KS_DUCKDB` as before, and
the archive tools in the pinned image are run with `-e KS_DUCKDB=on`.
`HELD_BACK_NOTE` stays for the compaction's sidecar, which quotes it.

History, before S14-00: in web, `off` held only when nothing web did still
needed the file — step 13 in force, every write chain on Postgres, the user
list there, step 13's record and the chains' frozen watermarks in Postgres'
copy, and no page open that a check `off` stands down would announce
resolved unseen. One unmet and web ran `on` (OD-S5-1 (a)), published the
keys as `held_back`, and the canary paged `duckdb_off_held_back`.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

logger = logging.getLogger(__name__)

ENV = "KS_DUCKDB"
ON = "on"
OFF = "off"
_VALID = (ON, OFF)

# The frames skipped when naming who asked for the file: this module and the
# store, whose `connect()` and `get_store()` are on every path and would name
# nothing, and the standard library's machinery between a caller and them
# (`async with store.connection()` passes through contextlib). The first frame
# outside them is the site — `module:function`.
_PLUMBING = frozenset({__name__, "core.duckdb_store"})
_PLUMBING_PACKAGES = ("contextlib", "asyncio", "functools", "concurrent")

# The application's own packages. A frame outside them, reached after the walk
# has left the running task's coroutine, is what turns the event loop rather
# than a caller (`_site`).
APPLICATION = ("core", "web", "bot", "scripts", "deploy")

# Bounded, because it is published: a site is a code location, and there are
# only so many, but a counter keyed on anything must say where it stops. Past
# the bound every new site is counted under `OTHER`.
MAX_SITES = 20
OTHER = "other"

_mode: Optional[str] = None
_value: Optional[str] = None
_mode_error: Optional[str] = None
# Whether this process is web: `configure_modes(web=True)` says so, and from
# then on the switch is `off` whatever the variable says (S14-00). Kept for
# the life of the process.
_web = False
# The value the WARNING below was last logged for, so web's startup and its
# scheduler, which both configure, say it once.
_warned: Optional[str] = None

_lock = threading.Lock()
_opened: Dict[str, Dict[str, object]] = {}

# What a host sidecar refused under `off` says about the week, after the
# sentence saying that a web running `off` has started it: the sidecar reads
# `KS_DUCKDB` from `.env` and reaches no verdict, so it is refused whether web
# runs `off` or was held back on `on` — and held back, web goes on writing the
# file the refusal leaves uncompacted and unsnapshotted (review of PR-2). One
# sentence for every such refusal, short because weekly_compact.sh quotes 300
# characters of the line it ends; `/api/health` publishes `held_back` under
# `off` alone (`health_block`). Since S14-00 only an image from before step 14
# can be held back — a step-14 web publishes `[]` — and `on` is a lever on
# that image alone (CLAUDE.md, "Starting the week of silence").
HELD_BACK_NOTE = (f"If a pre-step-14 web is held back (/api/health "
                  f"duckdb_switch.held_back), it has not: meet those keys or set {ENV}={ON}")


class DuckDBOpenedWhileOff(RuntimeError):
    """A path opened, or was about to open, the DuckDB file under
    `KS_DUCKDB=off`. Raised by `open_file` before `duckdb.connect`, so the
    file is neither opened nor created. Counted before it is raised."""

    def __init__(self, site: str, count: int):
        super().__init__(
            f"{ENV}=off: {site} tried to open the DuckDB file "
            f"({count} refused since start)")
        self.site = site
        self.count = count


def configure_mode(*, web: bool = False) -> str:
    """Read `KS_DUCKDB` once. Never raises. Called by `configure_modes()`.

    **In web it is `off`, whatever the variable says** (S14-00): `web=True`
    marks this process as web for its life, the value is read only to be
    published, and one WARNING says when it is not `off`. Anywhere else —
    a script, a host tool — the variable decides as it always has: `on`
    unless it says `off`, and a value nobody understands runs as `on` and is
    published as the error."""
    global _mode, _value, _mode_error, _web, _warned
    previous = _mode
    raw = os.getenv(ENV)
    value = (raw or "").strip().lower()
    _value = value or None
    _web = _web or web
    if _web:
        mode, error = OFF, None
        if value != OFF and value != _warned:
            logger.warning(
                "%s is %s, and web runs off whatever it says since step 14's "
                "S14-00: it never opens the DuckDB file. Scripts and host tools "
                "still read the variable; leave it off in .env", ENV,
                "unset" if raw is None else repr(raw))
        _warned = value
    elif not value or value in _VALID:
        mode, error = (value or ON), None
    else:
        mode = ON
        error = f"{ENV}={value!r} is not one of {_VALID}; running as {ON!r}"
    # Once per cause, as `read_fallback.configure_mode` does: web's startup and
    # the scheduler both configure.
    if error and error != _mode_error:
        logger.error(error)
    _mode, _mode_error = mode, error
    if mode == OFF and previous != OFF:
        logger.critical(
            "%s=off: this process must not open the DuckDB file. Every path "
            "that tries is refused, counted and published under "
            "duckdb_switch.opened_while_off on /api/health", ENV)
    return mode


def mode() -> str:
    """The configured mode — configured now if nothing has yet."""
    if _mode is None:
        return configure_mode()
    return _mode


def value() -> Optional[str]:
    """`KS_DUCKDB` as read, trimmed and lower-cased; None when unset."""
    mode()
    return _value


def mode_error() -> Optional[str]:
    mode()
    return _mode_error


def is_off() -> bool:
    return mode() == OFF


def held_back() -> Optional[Tuple[str, ...]]:
    """`()` in web, which runs `off` with nothing held back since S14-00;
    None in any other process, which reaches no verdict. Kept for the
    `/api/health` block's shape until the block goes (S14-17)."""
    mode()
    return () if _web else None


def _plumbing(module: str) -> bool:
    return module in _PLUMBING or module.split(".")[0] in _PLUMBING_PACKAGES


def _task_root() -> Optional[Any]:
    """The coroutine the running task was created with, or None outside a
    task (no running loop in this thread, or a coroutine that is not one)."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    if task is None:
        return None
    coro = task.get_coro()
    return coro if getattr(coro, "cr_frame", None) is not None else None


def _site() -> str:
    """`module:function` of the first frame outside the plumbing.

    Unless the walk has already passed the running task's own coroutine and
    the frame it reaches is outside the application (`APPLICATION`): that
    frame is not a caller but whatever turns the event loop — uvicorn in
    production, a test function on a laptop — and the caller is the task
    itself. A read handed to `asyncio.ensure_future(...)`, `shield` or
    `create_task` starts a task whose coroutine is the store's own method,
    which is plumbing, so the walk went straight through it: the boot sync's
    order watermark was named after the test that ran the loop
    (`tests…:test_target_off_pg`), and in production would have been named
    after uvicorn. Such a site is the task's coroutine, marked `(task)`."""
    root = _task_root()
    root_frame = root.cr_frame if root is not None else None
    left_the_task = False
    frame = sys._getframe(1)
    while frame is not None:
        module = frame.f_globals.get("__name__", "?")
        if not _plumbing(module):
            if left_the_task and module.split(".")[0] not in APPLICATION:
                break
            return f"{module}:{frame.f_code.co_name}"
        if frame is root_frame:
            left_the_task = True
        frame = frame.f_back
    if left_the_task:
        module = root_frame.f_globals.get("__name__", "?")
        return f"{module}:{root.cr_code.co_name} (task)"
    return "unknown"


def _tally(site: str) -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with _lock:
        if site not in _opened and len(_opened) >= MAX_SITES:
            site = OTHER
        entry = _opened.setdefault(site, {"count": 0, "last_at": None})
        entry["count"] = int(entry["count"]) + 1
        entry["last_at"] = now
        return int(entry["count"])


def guard() -> None:
    """Refuse, count and raise when this process may not open the file.

    Does nothing under `on`. Under `off` the count is taken before the raise,
    so a caller that swallows the exception cannot hide the attempt."""
    if not is_off():
        return
    site = _site()
    count = _tally(site)
    logger.critical(
        "DuckDB opened while %s=off: %s tried to open the file and was refused "
        "(%d since start). This breaks the week of silence (OD-17 (a)).",
        ENV, site, count)
    raise DuckDBOpenedWhileOff(site, count)


def open_file(path: Union[str, Path], *, read_only: bool = False,
              config: Optional[Mapping[str, str]] = None) -> Any:
    """The one way the application opens a DuckDB database, and the one
    read-write `duckdb.connect` of an analytics file. Two guards, in order.

    **The switch.** Under `off`, `DuckDBOpenedWhileOff`, before the driver is
    touched — the file is neither opened nor created. Under `on`, the open.

    **The kill guard: a read-write open CHECKPOINTs before anything else
    runs.** DuckDB 1.5.5 loses index entries across a kill. Rows a killed
    writer left in the WAL are replayed into every `CREATE INDEX` index (not
    the PK/UNIQUE ones) as entries that index has not bound yet, and DuckDB's
    *own* checkpoint — the one `close()` runs, or `wal_autocheckpoint` —
    writes those indexes without them unless something bound the index first.
    From then on `WHERE col = ?` misses the rows a scan still sees, and a
    write that must take such a row out of the index is a FatalException
    ("Failed to delete all rows from index"). An explicit CHECKPOINT here, on
    the freshly replayed instance, keeps every entry: measured through
    `DuckDBStore.connect()`, all 57 indexes whole instead of 45 of 45
    single-column ones short.

    **First, not after the caller's SETs**: a SET that fails leaves the
    instance valid, and closing a valid instance is the lossy checkpoint. With
    nothing before it, the only failure left is the CHECKPOINT's own. One that
    fails inside DuckDB is a FatalException in 1.5.5: the instance writes
    nothing on close, the WAL stays, and the next open replays it again. One
    that is *interrupted* is not — `InterruptException`, or Ctrl-C in a CLI
    that opens through the store, leaves the instance valid with the WAL
    unapplied (measured), and closing that is the lossy checkpoint. The PRAGMA
    below is what keeps that close from writing, so no exit from here takes
    the lossy path. And every exit closes: an instance left open while its
    exception's traceback lives holds the file's lock against every other
    process — the compaction, a CLI — until something drops the traceback.

    Free on a clean start — a graceful close leaves no WAL. Behind a kill it
    is the checkpoint the restart would have taken at close, moved to the
    open: 1.5 s / 3.6 s / 8.9 s for 30 / 300 / 900 MB of WAL on one CPU, peak
    memory set by the replay, not by this. A read-only open replays into
    memory and answers correctly; it can neither lose entries nor take this,
    so it returns the connection as opened.

    **`config` is the instance's from its first moment** — what DuckDB
    takes at the open, before the replay and before the CHECKPOINT here, so
    a memory limit given there bounds both, where a SET after the open bounds
    neither (the store's limit, batch-E review). A value DuckDB refuses
    raises before any instance exists: nothing is replayed, nothing to close.
    Every in-process open of one file must pass the same `config`, or DuckDB
    refuses the second ("a different configuration"); only the store opens
    the live file in web.

    ONE driver call, for both modes: `tests/unit/test_duckdb_switch.py` holds
    the application to exactly one reach for the driver, here, and
    `tests/unit/test_duckdb_open_guard.py` holds every read-write open in the
    repository to this function.
    """
    guard()
    import duckdb

    wal = Path(f"{path}.wal")
    try:
        replayed = 0 if read_only else wal.stat().st_size
    except OSError:
        replayed = 0
    con = duckdb.connect(str(path), read_only=read_only, config=dict(config or {}))
    if read_only:
        return con
    started = time.monotonic()
    try:
        con.execute("CHECKPOINT")
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("PRAGMA disable_checkpoint_on_shutdown")
        with contextlib.suppress(Exception):
            con.close()
        raise
    if replayed:
        # A clean close leaves no WAL, so one here means the last read-write
        # instance did not close cleanly: an OOM kill, a stop that ran out of
        # grace, or this process reopening after a FATAL invalidated it (an
        # invalidated instance writes nothing on close).
        logger.warning(
            "DuckDB replayed %.1f MB of WAL that a writer left without a clean "
            "close (killed, or invalidated by a FATAL) and checkpointed it in "
            "%.1f s before anything else ran: %s",
            replayed / 1e6, time.monotonic() - started, path,
        )
    return con


class RefusedConnection:
    """A DuckDB connection in name only, for code that takes one and must not
    reach the file under `off`: every statement is refused through `guard()`
    — counted, logged, raised — and, this module being plumbing to `_site()`,
    the refusal names the function that issued the statement, not this class.

    The integrity scan is handed one under `off` (stage 5, PR-7): every check
    over the file stands down there, and one that does not is caught in the
    act rather than failing on a None in a way the switch would never count.
    Under `on` nothing hands one out; a statement then raises all the same,
    since there is no file behind it."""

    def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        guard()
        raise RuntimeError("RefusedConnection: there is no DuckDB connection here")

    sql = execute


@contextlib.asynccontextmanager
async def refused_connection():
    """`async with` a `RefusedConnection` — where `store.connection()` stood."""
    yield RefusedConnection()


def opened() -> Dict[str, Dict[str, object]]:
    """`{site: {count, last_at}}` — opens refused under `off` since this
    process started. A copy."""
    with _lock:
        return {site: dict(entry) for site, entry in _opened.items()}


def health_block() -> Dict[str, Any]:
    """What `/api/health` publishes as `duckdb_switch`. Local state, no I/O,
    no exception text."""
    block: Dict[str, Any] = {
        "mode": mode(),
        "value": value(),
        "error": mode_error(),
        "opened_while_off": opened(),
    }
    # Under `off` only, so `on` publishes what it always did. Since S14-00 web
    # is always `off` and nothing is ever held back, so web publishes `[]`
    # here — the shape the canary and the soak read, until S14-17 removes it.
    if mode() == OFF:
        held = held_back()
        block["held_back"] = None if held is None else list(held)
    return block


def reset_counts() -> None:
    """Forget every count and keep the mode. Tests only: a sweep that asks,
    step by step, which paths still reach for the file under `off` empties
    the counts between steps without reading the switch again — `reset()`
    would leave it unconfigured, and the next open would configure it from
    whatever the environment says by then."""
    with _lock:
        _opened.clear()


def reset() -> None:
    """Forget the mode and every count. Tests only; a process never forgets
    its own."""
    global _mode, _value, _mode_error, _web, _warned
    with _lock:
        _opened.clear()
    _mode = _value = _mode_error = _warned = None
    _web = False
