"""Every read that falls back to DuckDB, counted in one place (DN-20a).

Fourteen routers answer from DuckDB when Postgres (or, for cohorts,
ClickHouse) fails, and each used to say so in its own words: "falling back to
DuckDB" in most of them, "cannot read the backfill watermark" in
`pg_expenses_read.backfilled()`, which served DuckDB without ever using the
phrase. A fallback was therefore visible only to somebody grepping the web
log for every spelling at once, inside a window that log rotation shortens
without saying so. After step 13 each of those paths serves frozen DuckDB
numbers — and after the first Sunday compaction, empty ones — behind a page
that looks as if it works.

So every such site calls `fall_back(surface, exc)`: one uniform ERROR line,
one in-process counter per surface, and `/api/health` publishes the counters
under `read_fallbacks`. `tests/unit/test_read_fallback_sites.py` walks `core/`
and `web/` for the shapes a fallback takes rather than trusting a list.

NOTHING HERE CHANGES WHAT A READ RETURNS

`fall_back` counts and logs, and the caller then reads DuckDB exactly as it
did before. `KS_READ_FALLBACK` is parsed now — `duckdb`, the default, or
`off` — so that the value an operator sets is validated and published from
the first deploy, but under `off` this module still falls back. Refusing is
DN-20b (HTTP routes, a 503 naming the surface) and DN-20c (the weekly
reports, the assistant, training and the sync), and both raise
`ReadUnavailable` from here, which is why the class exists already: a
handler wrapping a router has to be able to let it through, and the walk
checks that it does.

AN UNKNOWN VALUE DOES NOT STOP WEB

`KS_BOT_STORE`'s rule — raise on a typo — is wrong for this variable. Web is
the only process that syncs orders, so a raise at startup is a crash loop
that stops order intake over a setting about how a read degrades. It runs as
`duckdb`, today's behaviour, logs at ERROR, publishes the error in
`/api/health` under `read_fallback_mode`, and the canary files
`read_fallback_mode_invalid`. `core/pg_derivation.py` answers `KS_PG_DERIVE`
the same way, for the same reason; OD-09 recommends it for every flag of
this family.

Read once, in `core.runtime_modes.configure_modes()`, which web's startup
calls before the boot sync: a mode cached at scheduler start misses
everything the boot does (DN-05b).

THE OTHER SILENT FALLBACK: A FLAG WITH NOTHING TO ASK

A router's gate is `enabled() and available()`, and `available()` is only
"is a DSN set". `KS_READ_GOLD=postgres` with no `KS_PG_DSN` therefore serves
DuckDB on every request without a single exception to count. That is a
configuration, not a failure, so it is found once, at the same moment the
mode is read, and published beside it (`misconfigured`) — every
`KS_READ_*=postgres` without `KS_PG_DSN`, and every `KS_READ_*=clickhouse`
without `KS_CH_URL`. Nothing raises there either.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

ENV = "KS_READ_FALLBACK"
DUCKDB = "duckdb"
OFF = "off"
_VALID = (DUCKDB, OFF)

# The switches whose value names another engine, and what that engine needs.
# Found by prefix in the environment, never listed: a read flag added later is
# covered the day it is set, which is the point of looking at startup at all.
READ_FLAG_PREFIX = "KS_READ_"
_NEEDS = {
    "postgres": "KS_PG_DSN",
    "clickhouse": "KS_CH_URL",
}

_mode: Optional[str] = None
_mode_error: Optional[str] = None
_misconfigured: List[str] = []

_lock = threading.Lock()
_counts: Dict[str, Dict[str, object]] = {}


class ReadUnavailable(Exception):
    """A read that would have fallen back to DuckDB, refused instead.

    Nothing raises it in DN-20a. DN-20b and DN-20c make `fall_back` raise it
    under `KS_READ_FALLBACK=off`; it is defined now so that a handler around a
    router — `GoalsMixin._get_ml_forecast_total` is the one there is — can
    already let it through rather than turn a refusal into a quiet answer.
    """

    def __init__(self, surface: str):
        super().__init__(f"{surface}: the read cannot be answered without DuckDB")
        self.surface = surface


def _misconfigured_reads() -> List[str]:
    """Every read switch naming an engine this process has no address for."""
    found = []
    for name in sorted(os.environ):
        if not name.startswith(READ_FLAG_PREFIX) or name == ENV:
            continue
        value = os.environ[name].strip().lower()
        needed = _NEEDS.get(value)
        if needed and not os.getenv(needed, "").strip():
            found.append(f"{name}={value} without {needed}")
    return found


def configure_mode() -> str:
    """Read `KS_READ_FALLBACK` once, and look for read switches that cannot
    reach their engine. Never raises. Called by `configure_modes()`."""
    global _mode, _mode_error, _misconfigured
    previous = _mode
    value = os.getenv(ENV, DUCKDB).strip().lower() or DUCKDB
    if value in _VALID:
        error = None
        _mode = value
    else:
        _mode = DUCKDB
        error = (f"{ENV}={value!r} is not one of {_VALID}; "
                 f"running as {DUCKDB!r}")
    # Logged when it changes, not on every call: web's startup and the
    # scheduler both configure, and one ERROR per cause is what a reader of
    # the log can use.
    if error and error != _mode_error:
        logger.error(error)
    _mode_error = error
    if _mode == OFF and previous != OFF:
        logger.warning(
            "%s=off is read but not enforced yet: every fallback is still "
            "served from DuckDB and counted under read_fallbacks", ENV)

    misconfigured = _misconfigured_reads()
    for line in misconfigured:
        if line not in _misconfigured:
            logger.error(
                "%s — every read behind it is served from DuckDB. Set the "
                "address or unset the flag.", line)
    _misconfigured = misconfigured
    return _mode


def mode() -> str:
    """The configured mode; `duckdb` until `configure_mode` has run."""
    return _mode or DUCKDB


def mode_error() -> Optional[str]:
    return _mode_error


def misconfigured() -> List[str]:
    return list(_misconfigured)


def fall_back(surface: str, exc: Optional[BaseException] = None) -> None:
    """A read on `surface` is about to be answered by DuckDB: count it, say so.

    Called from the handler that caught the other engine's failure, before
    the DuckDB read — so that DN-20b can make this the one place that refuses
    instead. `surface` is the tab or consumer a reader would name
    (`dashboard`, `goals`, `cohorts`…), never the flag: several flags serve
    one tab, and the question the count answers is which page was not
    reading the store it claims to.
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with _lock:
        entry = _counts.setdefault(surface, {"count": 0, "last_at": None})
        entry["count"] = int(entry["count"]) + 1
        entry["last_at"] = now
        count = entry["count"]
    logger.error(
        "read fallback: %s read failed, falling back to DuckDB (%d since "
        "start): %s", surface, count, exc,
        exc_info=exc if isinstance(exc, BaseException) else None,
    )


def counts() -> Dict[str, Dict[str, object]]:
    """`{surface: {count, last_at}}` since this process started. A copy."""
    with _lock:
        return {surface: dict(entry) for surface, entry in _counts.items()}


def reset_counts() -> None:
    """Forget every count. Tests only; a process never forgets its own."""
    with _lock:
        _counts.clear()
