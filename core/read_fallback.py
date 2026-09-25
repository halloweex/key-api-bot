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

UNDER `off`, A FALLBACK IS REFUSED (DN-20b)

`KS_READ_FALLBACK` is `duckdb`, the default, or `off`. Under `duckdb`,
`fall_back` counts and logs, and the caller then reads DuckDB exactly as it
did before this module existed. Under `off` it raises `ReadUnavailable`
instead, so the DuckDB read below the call is never reached, and
`web/main.py` maps that exception, from any route, to one 503 naming the
surface. After step 13 a fallback serves frozen Silver and Gold, and after
the first Sunday compaction empty ones, behind a page that looks as if it
works; a 503 is an outage somebody can see. The frontend already reads a
503 as "temporarily unavailable" and retries it (`ApiErrorState`).

A refusal is not a fallback, so it is counted apart (`refusals()`), logged
without the phrase the soak greps for, and published only beside the mode
that produces it (`read_fallback_mode.refused` on `/api/health`, under `off`
alone). `read_fallbacks` keeps meaning "answered from DuckDB", and under `off`
it stays empty.

One place raises, so every consumer of a router refuses at once — and a
gate whose switch names an engine with no address refuses through
`no_address`, below, for the same reason. The HTTP routes answer 503 here.
Cohorts go one step further, because they have no Postgres body: under
`off` they are answered by a live ClickHouse or not at all (`no_engine`),
whatever `KS_READ_COHORTS` says.

WHAT NO HTTP REQUEST WAITS FOR ANSWERS ITSELF (DN-20c)

Everything else that can receive a refusal gives it an answer of its own,
and never a DuckDB number: the two weekly reports defer with no ledger row;
the Monday goals job asks its one routed read before it writes anything, so
it defers whole; training is skipped and the previous model stands; the
search index and the buyers step skip their step for the tick with the
watermark held, and the tick goes on; the boot contains each of those; and
the assistant's tools return a named `data_unavailable` result. A job says
so in its result through `answered`, below, so `/api/jobs` shows a refusal
rather than a quiet run. The consumers are derived, not remembered —
`tests/unit/test_read_fallback_consumers.py` walks up from every refusal to
the entry points nothing in the repository calls (`NON_HTTP_CONSUMERS`), then
back down through the `try` each call sits in, and pins where every
consumer's refusal stops (`ANSWERS`): a refusal that could leave one as an
exception, or a new handler on the way, fails it. A handler that answers
rather than raises may live only in a function no HTTP route reaches but
the assistant's two (`POST /api/chat`, `GET /api/chat/stream`), which answer
inside the conversation; anywhere else it would take that route's 503 away.

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
without `KS_CH_URL`. Under `duckdb` nothing raises there.

Under `off` it is refused, per request, like a failure (`no_address`): a lost
DSN line is the most ordinary way for a whole dashboard to go back to DuckDB,
and after step 13 it would serve frozen numbers behind pages that all look as
if they work — exactly what `off` exists to prevent. Every gate asks it
before it lets DuckDB answer, with the switch it just consulted, and the walk
in `tests/unit/test_read_fallback_sites.py` requires that of every
`enabled() and available()` gate it finds. The rule is the one
`misconfigured` publishes, read from the same table (`_NEEDS`), so a switch
that is published as misconfigured is exactly a switch that is refused.
Chosen over having the canary page `misconfigured` under `off`: a page would
still have served DuckDB until somebody read it.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, NoReturn, Optional

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
# Refusals under `off`, apart from the fallbacks: a refusal served nothing
# from DuckDB, and `read_fallbacks` is read as exactly that.
_refused: Dict[str, Dict[str, object]] = {}


class ReadUnavailable(Exception):
    """A read that would have been answered by DuckDB, refused instead.

    Raised by `fall_back`, `no_address` and `no_engine` under
    `KS_READ_FALLBACK=off`, and by nothing else. `web/main.py` maps it to a 503 carrying `surface`, so a
    handler between a route and a router must let it through — the walk in
    `tests/unit/test_read_fallback_sites.py` requires every handler around a
    router to say so, and `GoalsMixin._get_ml_forecast_total` is the one
    that had to.
    """

    def __init__(self, surface: str):
        super().__init__(f"{surface}: the read cannot be answered without DuckDB")
        self.surface = surface


def _unaddressed(name: str) -> Optional[str]:
    """`"NAME=engine without ADDRESS"` when the read switch `name` names an
    engine this process has no address for, else None. The one rule, shared
    by what is published (`misconfigured`) and what is refused
    (`no_address`)."""
    value = os.getenv(name, "").strip().lower()
    needed = _NEEDS.get(value)
    if needed and not os.getenv(needed, "").strip():
        return f"{name}={value} without {needed}"
    return None


def _misconfigured_reads() -> List[str]:
    """Every read switch naming an engine this process has no address for."""
    found = []
    for name in sorted(os.environ):
        if not name.startswith(READ_FLAG_PREFIX) or name == ENV:
            continue
        line = _unaddressed(name)
        if line:
            found.append(line)
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
            "%s=off: a read whose engine fails is refused rather than "
            "answered from DuckDB — an HTTP route answers 503 naming the "
            "surface. Refusals are published under read_fallback_mode.refused",
            ENV)

    misconfigured = _misconfigured_reads()
    for line in misconfigured:
        if line not in _misconfigured:
            logger.error(
                "%s — every read behind it is %s. Set the address or unset "
                "the flag.", line,
                "refused (KS_READ_FALLBACK=off)" if _mode == OFF
                else "served from DuckDB")
    _misconfigured = misconfigured
    return _mode


def mode() -> str:
    """The configured mode; `duckdb` until `configure_mode` has run."""
    return _mode or DUCKDB


def mode_error() -> Optional[str]:
    return _mode_error


def misconfigured() -> List[str]:
    return list(_misconfigured)


def refusing() -> bool:
    """Whether a read that would be answered by DuckDB is refused instead."""
    return mode() == OFF


def _tally(table: Dict[str, Dict[str, object]], surface: str) -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with _lock:
        entry = table.setdefault(surface, {"count": 0, "last_at": None})
        entry["count"] = int(entry["count"]) + 1
        entry["last_at"] = now
        return int(entry["count"])


def _refuse(surface: str, why: str, exc: Optional[BaseException]) -> NoReturn:
    """Count one refusal on `surface`, say why, and raise `ReadUnavailable`.

    The line deliberately does not say "falling back to DuckDB": nothing
    fell back, and that phrase is what the soak before the flip greps for.
    """
    count = _tally(_refused, surface)
    logger.error(
        "read refused: %s — %s, and %s=off forbids answering from DuckDB "
        "(%d refused since start)%s", surface, why, ENV, count,
        f": {exc}" if exc is not None else "",
        exc_info=exc if isinstance(exc, BaseException) else None,
    )
    raise ReadUnavailable(surface) from exc


def fall_back(surface: str, exc: Optional[BaseException] = None) -> None:
    """A read on `surface` is about to be answered by DuckDB: count it, say so
    — or, under `KS_READ_FALLBACK=off`, refuse it.

    Called from the handler that caught the other engine's failure, before
    the DuckDB read, which is what makes this the one place that refuses:
    under `off` it raises `ReadUnavailable` and the DuckDB read below the
    call is never reached. `surface` is the tab or consumer a reader would
    name (`dashboard`, `goals`, `cohorts`…), never the flag: several flags
    serve one tab, and the question the count answers is which page was not
    reading the store it claims to.
    """
    if refusing():
        _refuse(surface, "its engine failed", exc)
    count = _tally(_counts, surface)
    logger.error(
        "read fallback: %s read failed, falling back to DuckDB (%d since "
        "start): %s", surface, count, exc,
        exc_info=exc if isinstance(exc, BaseException) else None,
    )


def no_engine(surface: str, why: str) -> None:
    """`surface` is about to be answered by DuckDB because no other engine
    is live for it — not because one failed.

    Under `duckdb` that is routing, not a failure: nothing is counted and
    nothing is said, per request. Under `off` it is refused like a fallback.
    One caller today, the cohorts: they have no Postgres body, so under
    `off` a live ClickHouse is the only thing that may answer them (DN-20b).
    """
    if refusing():
        _refuse(surface, why, None)


def no_address(surface: str, switch: Any) -> None:
    """Under `off`, refuse `surface` when `switch` names an engine this
    process has no address for — `KS_READ_TRAFFIC=postgres` without
    `KS_PG_DSN` — because the gate beside the call is about to let DuckDB
    answer instead (DN-20b).

    `switch` is the read-switch module the gate consults, and its `ENV` is
    what is read, per request, as the gate reads it. Under `duckdb` it does
    nothing at all, not even read the environment: that state is a
    configuration, published once at start under `misconfigured`, and the
    page keeps answering from DuckDB exactly as it always has. A switch that
    names DuckDB, or names nothing, is a routing decision under either mode
    and is never refused here.
    """
    if not refusing():
        return
    line = _unaddressed(switch.ENV)
    if line:
        _refuse(surface, line, None)


READ_UNAVAILABLE = "read_unavailable"


def answered(consumer: str, exc: ReadUnavailable, answer: str) -> Dict[str, str]:
    """`consumer` received a refusal the 503 handler will not answer — no
    HTTP request is waiting for it, or (the assistant) the request is a
    conversation and the answer goes inside it — and answered it itself
    (DN-20c). Returns the fields its result carries.

    `consumer` names the caller as a reader of the job list would —
    `weekly_report`, `revenue_prediction` — and `answer` says in words what
    it did instead of reading DuckDB: deferred, kept the previous model,
    skipped the step. The refusal itself was logged at ERROR and counted
    where it was raised (`read_fallback_mode.refused`), so this line says
    only what became of it, at WARNING, and never the phrase the soak greps
    for. The returned `{"reason", "surface"}` goes into the consumer's own
    result, so `/api/jobs` history shows a refusal rather than a quiet run.
    """
    logger.warning("refused read answered by %s (%s): %s",
                   consumer, exc.surface, answer)
    return {"reason": READ_UNAVAILABLE, "surface": exc.surface}


def counts() -> Dict[str, Dict[str, object]]:
    """`{surface: {count, last_at}}` — reads answered from DuckDB since this
    process started. A copy."""
    with _lock:
        return {surface: dict(entry) for surface, entry in _counts.items()}


def refusals() -> Dict[str, Dict[str, object]]:
    """`{surface: {count, last_at}}` — reads refused under `off` since this
    process started. A copy."""
    with _lock:
        return {surface: dict(entry) for surface, entry in _refused.items()}


def reset_counts() -> None:
    """Forget every count. Tests only; a process never forgets its own."""
    with _lock:
        _counts.clear()
        _refused.clear()
