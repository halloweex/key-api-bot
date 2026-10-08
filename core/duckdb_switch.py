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

**`off` does not make web work without DuckDB.** That is stage 5's code
decoupling (the boot sync's zero-orders gate, `get_stats` behind
`/api/health`, the every-boot migrations, the inventory views...). Under `off`
today almost everything web does reaches `get_store()` and is refused, so
order intake stops and the dashboard answers errors. Web's startup contains
the refusal rather than dying of it only so that `/api/health` answers and the
page can be seen. Never set it in production before that decoupling.

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

AN UNKNOWN VALUE DOES NOT STOP WEB

`KS_READ_FALLBACK`'s rule and OD-09's: web is the only process that syncs
orders, so a typo runs as `on`, today's behaviour, logs at ERROR, is published
as `duckdb_switch.error`, and the canary warns `duckdb_mode_invalid`. Whoever
typed `of` believes the week of silence is running; it is not, and the soak's
week-of-silence check says so too.

Read once, in `core.runtime_modes.configure_modes()`, before the boot sync —
and, unlike the other cached modes, lazily on the first open if no entry point
configured it: a script that forgot `configure_modes()` must not be the one
process that opens the file under `off`.
"""
from __future__ import annotations

import contextlib
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Union

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

# Bounded, because it is published: a site is a code location, and there are
# only so many, but a counter keyed on anything must say where it stops. Past
# the bound every new site is counted under `OTHER`.
MAX_SITES = 20
OTHER = "other"

_mode: Optional[str] = None
_value: Optional[str] = None
_mode_error: Optional[str] = None

_lock = threading.Lock()
_opened: Dict[str, Dict[str, object]] = {}


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


def configure_mode() -> str:
    """Read `KS_DUCKDB` once. Never raises. Called by `configure_modes()`."""
    global _mode, _value, _mode_error
    previous = _mode
    raw = os.getenv(ENV)
    value = (raw or "").strip().lower()
    _value = value or None
    if not value or value in _VALID:
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


def _site() -> str:
    """`module:function` of the first frame outside the plumbing."""
    frame = sys._getframe(1)
    while frame is not None:
        module = frame.f_globals.get("__name__", "?")
        if (module not in _PLUMBING
                and module.split(".")[0] not in _PLUMBING_PACKAGES):
            return f"{module}:{frame.f_code.co_name}"
        frame = frame.f_back
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


def opened() -> Dict[str, Dict[str, object]]:
    """`{site: {count, last_at}}` — opens refused under `off` since this
    process started. A copy."""
    with _lock:
        return {site: dict(entry) for site, entry in _opened.items()}


def health_block() -> Dict[str, Any]:
    """What `/api/health` publishes as `duckdb_switch`. Local state, no I/O,
    no exception text."""
    return {
        "mode": mode(),
        "value": value(),
        "error": mode_error(),
        "opened_while_off": opened(),
    }


def reset() -> None:
    """Forget the mode and every count. Tests only; a process never forgets
    its own."""
    global _mode, _value, _mode_error
    with _lock:
        _opened.clear()
    _mode = _value = _mode_error = None
