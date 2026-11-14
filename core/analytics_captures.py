"""The analysts' captures: the histories Postgres overwrites, kept from revision 0036 on.

WHAT A CAPTURE IS

Seven things this system writes are overwritten in place, and nothing records
what they were before: an offer's purchase price, a product's brand and
category, the stored forecast, a goal, an order's channel verdict, an expense
KeyCRM stopped listing, and the GA4 funnel past its fourteen months. Revision
0036 gives each a table in `app`, and each table has ONE writer — the module
that already writes the thing it remembers (`CAPTURES`, held to it by a walk
of the repository in `tests/unit/test_write_chains.py`). This module is what
every one of them shares: the switch, the two ways of running a capture, the
stamps, and the watch over them. The design is the analysts' warehouse,
revision 3.1, §3.3 and §9; the owner approved it on 2026-11-13.

A CAPTURE NEVER FAILS THE WRITER IT RIDES ON

Four captures write inside another writer's transaction (`IN_TRANSACTION`: the
01:00 inventory snapshot, the forecast's replacement, a goal's write, chain
3's expense delete). `capture_in_transaction` runs the capture's statement
under a SAVEPOINT of its own: if it raises, only the savepoint rolls back, the
failure is stamped in a savepoint of its own, and the writer's write commits
as if the capture had not been there. It never raises into the writer, a
cancellation apart. Revision 2 of the design had no savepoint, and a NULL
`created_at` in `app.revenue_predictions` aborted the whole forecast write
(T8, `chdwh/rev3/out/t07_pg.out`).

The two content-diff captures (the catalogue, the verdicts) run in a
transaction of their own after their writer's (`run_own_transaction`): a
missed run only coarsens when a change became known, because the next run
diffs against the last version kept. A self-check or a mass-delete guard that
refuses raises `CaptureRefused`, writes nothing, and is stamped as such.

THE SWITCH

`KS_ANALYTICS_CAPTURES=off` (or `0`, `false`) stops every capture, the
in-transaction ones included: their statement is skipped and nothing is
stamped. Unset, or anything else, is on. An unrecognised value does not raise
— it is not a store selector, and a typo must not take a writer down — but is
logged once per value. While it is off each day is a gap in every history, and
the watch says the captures are off rather than stalled: a WARN that is news in
every digest it stands in, holding every stamp's finding as it was.

WHERE A FAILURE GOES — WARN, NEVER A PAGE (owner decision D8 (b))

Not through `core.alerting.raise_alert`: that is a Telegram message to the
admins, a page, and a condition it archives as firing is escalated by the bot
after six hours. A capture's failure is a gap in a history, not a wrong
number on any screen, so it is counted and digested:

- the capture stamps its row in `meta.mirror_state`, `capture.<table>`
  (`state_row`), with the mirror's own statements — `WATERMARK_OK_SQL` on
  success, in the same savepoint or transaction as the capture's rows, so the
  stamp commits exactly when they do; `RECORD_FAILURE_SQL` on failure, which
  counts `failures_since_ok` and keeps the reason in `last_error`. Both run on
  the connection the capture already holds: inside a writer's transaction a
  second connection could wait on a full pool;
- the integrity job (01, 07, 13, 19 Kyiv) reads those rows (`read_facts`) and
  files WARN findings (`judge`): `capture_failed:<table>`,
  `capture_self_check_failed:<table>` and `capture_stalled:<table>`, which the
  09:00 digest carries, and two INFO ones that ride along without summoning
  it (`capture_unstarted:<table>`, `ga4_backfill_incomplete`). Nothing here
  is CRITICAL, so the integrity job pages on none of it, and it builds any
  page another finding owes without them (`is_capture_finding`): riding a
  page, a capture that stamped between two runs would change its Gate bucket
  and deliver a standing CRITICAL again.

What that does not keep: a failure that the capture's next run clears before
the integrity job reads the row is in the web log only (ERROR, with the
table and the reason). For the content-diff captures that costs nothing — the
next run catches up. For an in-transaction capture it is that run's rows.

Why a row in `meta.mirror_state` and not a table of its own: it already has
the shape (attempted, ok, consecutive failures, the error), `ch_common`
stamps ClickHouse's tables there the same way, and nothing that reads it
iterates every row — each reader asks for its own names — so new rows judge
nothing they were not written for.

THE WATCH

A capture is stalled when its last ok stamp is older than its writer's
cadence plus a grace (`CaptureSpec.max_age`). A capture whose writer runs
only when a human or an event moves it (a goal, an expense delete) has no
cadence and is judged on its failures only.

A capture that has never stamped is given the same grace, from the first
integrity run that found it so (owner, 2026-11-14: a WARN for a writer that
had not yet had its turn was the digest's noise the morning after every
deploy that adds a capture). Until then it is `capture_unstarted:<table>`,
INFO; past it, `capture_stalled:<table>`, WARN, a name of its own, so the
morning it turns is news (`data_quality.build_digest` compares a finding's
count with the same name's the morning before, and "unchanged" is not news).
"Found so" is read from the journal, never from the process: the streak of
the newest successful integrity runs, unbroken, that each filed the
capture's `capture_unstarted` or `capture_stalled` (`missing_since`), so a
restart or a deploy moves nothing. A run that could not read the stamps
neither extends a streak nor breaks it; a run with the switch off breaks
every one, since its writers could not stamp. A journal that cannot be read
is no grace: the capture is filed stalled, WARN, as before. A capture that
stamped once and then stopped is judged by its stamp alone.

The GA4 capture is also judged on what it has not kept yet (the design's §9):
closed periods inside GA4's horizon missing from `app.ga4_funnel`, read in
the same transaction as the stamps where GA4 is configured, and judged by
their slack (`ga4_capture.nights_to_spare`) — the nights the capture could
still fail and keep every one of them. With `GA4_MARGIN_NIGHTS` or more to
spare it is the backfill's progress, `ga4_backfill_incomplete`, INFO; below,
`ga4_backlog_at_risk`, WARN, again a name of its own.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING, Any, Awaitable, Callable, Collection, Dict, List, Mapping,
    Optional, Sequence, Tuple,
)

if TYPE_CHECKING:
    from core.ga4_capture import Backlog

logger = logging.getLogger(__name__)


# ─── The switch ───────────────────────────────────────────────────────────────

SWITCH_ENV = "KS_ANALYTICS_CAPTURES"
_OFF = frozenset({"off", "0", "false"})
_ON = frozenset({"", "on", "1", "true"})
_warned_values: set = set()


def captures_on() -> bool:
    """True unless `KS_ANALYTICS_CAPTURES` says off/0/false. Read on every
    call, so a test or an operator's change of the environment is seen at the
    next capture. Never raises."""
    raw = os.getenv(SWITCH_ENV, "")
    value = raw.strip().lower()
    if value in _OFF:
        return False
    if value not in _ON and value not in _warned_values:
        _warned_values.add(value)
        logger.warning(
            "%s=%r is not on/off/1/0/true/false: the captures stay on",
            SWITCH_ENV, raw)
    return True


# ─── What is captured, by whom, how often ─────────────────────────────────────

IN_TRANSACTION = "in_transaction"     # inside the writer's own transaction
OWN_TRANSACTION = "own_transaction"   # a content diff after the writer's
JOB = "job"                           # a scheduled job of its own


@dataclass(frozen=True)
class CaptureSpec:
    writer: str                       # the one module that writes the table
    kind: str                         # IN_TRANSACTION | OWN_TRANSACTION | JOB
    # How often the writer runs the capture, or None when it runs only when
    # something moves it — such a capture is judged on its failures alone.
    cadence: Optional[timedelta]
    grace: timedelta = timedelta(0)
    # A content diff with a self-check or a mass-delete guard of its own,
    # which refuses by raising `CaptureRefused`.
    self_checked: bool = False
    # The history in words, for the digest and any message that names a
    # finding (`human_names`).
    history: str = ""

    @property
    def max_age(self) -> Optional[timedelta]:
        return None if self.cadence is None else self.cadence + self.grace


# The seven captures of revision 0036. The four key tables beside them have
# no writer in this build (`write_chains.KEY_TABLES`).
CAPTURES: Dict[str, CaptureSpec] = {
    # Chain 1's 01:00 snapshot, daily. A failed night is not retried until
    # the next, so 26 h says "last night did not stamp" by the 07:00 run.
    "app.offer_cost_daily": CaptureSpec(
        "core.pg_inventory_write", IN_TRANSACTION,
        cadence=timedelta(days=1), grace=timedelta(hours=2),
        history="cost history"),
    # Chain 6's hourly products step writes the whole catalogue every hour.
    "app.catalogue_versions": CaptureSpec(
        "core.pg_catalogue_write", OWN_TRANSACTION,
        cadence=timedelta(hours=1), grace=timedelta(hours=2), self_checked=True,
        history="catalogue history"),
    # Chain 7b-3's training, Mon and Thu 03:30: Thu to Mon is the longest
    # gap, and three hours covers a training and the 07:00 run after it.
    "app.revenue_prediction_vintages": CaptureSpec(
        "core.pg_forecast_write", IN_TRANSACTION,
        cadence=timedelta(days=4), grace=timedelta(hours=3),
        history="forecast history"),
    # Chain 7a: a goal is written when a human sets or resets one.
    "app.revenue_goal_history": CaptureSpec(
        "core.pg_goals_write", IN_TRANSACTION, cadence=None,
        history="goal history"),
    # The parse at the end of every derivation, at least hourly (heartbeat).
    "app.order_channel_verdicts": CaptureSpec(
        "core.pg_utm_parse", OWN_TRANSACTION,
        cadence=timedelta(hours=1), grace=timedelta(hours=2), self_checked=True,
        history="channel verdict log"),
    # Chain 3's delete of expenses KeyCRM stopped listing (O2 (a)): it runs
    # only when an order is re-fetched without one.
    "app.expense_deletions": CaptureSpec(
        "core.pg_orders_write", IN_TRANSACTION, cadence=None,
        history="deleted-expense log"),
    # A daily job of its own; six hours of grace for GA4's quota.
    "app.ga4_funnel": CaptureSpec(
        "core.ga4_capture", JOB,
        cadence=timedelta(days=1), grace=timedelta(hours=6),
        history="GA4 funnel history"),
}


def state_row(table: str) -> str:
    """The capture's row in `meta.mirror_state`: `capture.app.offer_cost_daily`."""
    return f"capture.{table}"


# ─── Conditions (WARN at most, D8 (b)) ────────────────────────────────────────

FAILED = "capture_failed"
STALLED = "capture_stalled"
# Never stamped, and inside its grace (INFO); past it, it is STALLED.
UNSTARTED = "capture_unstarted"
SELF_CHECK_FAILED = "capture_self_check_failed"
UNWATCHED = "captures_unwatched"
SWITCHED_OFF = "captures_switched_off"
# The GA4 capture's own measure beside its stamp: closed periods inside GA4's
# horizon that are still not kept (the design's §9; `ga4_capture.backlog`).
# INFO while it has `GA4_MARGIN_NIGHTS` to spare, else the WARN beside it.
GA4_BACKFILL = "ga4_backfill_incomplete"
GA4_AT_RISK = "ga4_backlog_at_risk"
GA4_TABLE = "app.ga4_funnel"
# The nights the GA4 backlog must have to spare to be INFO. A week: a night
# that fails or does not run is a WARN that morning already (`capture_failed`,
# `capture_stalled` at 30 h), so this is the backstop for runs that succeed
# and still do not keep up, and it is said a week before the first period
# would be lost — the digest's own beat for a standing WARN
# (`data_quality.DIGEST_RESTATE_AFTER`), a weekend and a deploy inside it,
# and room to catch up by hand at 24 periods an hour.
GA4_MARGIN_NIGHTS = 7

# What a refused self-check writes at the head of `last_error`, so the watch
# can tell it from a failure.
SELF_CHECK_MARK = "self_check: "
# Where the whole-capture findings are filed (`app.data_quality_issues` keys
# on (run, check, table)).
WHOLE_PART = "(captures)"


def condition(family: str, table: str) -> str:
    return f"{family}:{table}"


def conditions() -> Tuple[str, ...]:
    """Every name `judge` can file — the REGISTRY family over `CAPTURES`."""
    out = [UNWATCHED, SWITCHED_OFF]
    for table, spec in CAPTURES.items():
        out.append(condition(FAILED, table))
        if spec.max_age is not None:
            out.append(condition(STALLED, table))
            out.append(condition(UNSTARTED, table))
        if spec.self_checked:
            out.append(condition(SELF_CHECK_FAILED, table))
    out += [GA4_BACKFILL, GA4_AT_RISK]
    return tuple(out)


_GA4_CONDITIONS = (GA4_BACKFILL, GA4_AT_RISK)


def _stamp_conditions() -> Tuple[str, ...]:
    """The names judged from the stamps alone — what a run that did not judge
    them (the switch off) holds rather than clears."""
    return tuple(n for n in conditions()
                 if n not in (UNWATCHED, SWITCHED_OFF) + _GA4_CONDITIONS)


_FAMILY_WORDS = {
    FAILED: "capture failed",
    STALLED: "capture stalled",
    UNSTARTED: "capture not started yet",
    SELF_CHECK_FAILED: "capture refused its self-check",
}


def human_names() -> Dict[str, str]:
    """Every name in `conditions()` in words, for `data_quality.HUMAN_CHECK_NAMES`:
    the digest prints the label beside the key."""
    out = {UNWATCHED: "capture stamps not examined",
           SWITCHED_OFF: "the analysts' captures switched off",
           GA4_BACKFILL: "GA4 periods not kept yet",
           GA4_AT_RISK: "GA4 periods at risk of aging out unkept"}
    for table, spec in CAPTURES.items():
        for family, words in _FAMILY_WORDS.items():
            out[condition(family, table)] = f"{spec.history}: {words}"
    return {name: out[name] for name in conditions()}


def is_capture_finding(issue: Any) -> bool:
    """Whether `issue` is one `judge` files. The integrity job journals these
    and leaves them out of what it pages with (D8 (b)): a capture moving
    between two runs must not make a standing CRITICAL a new incident."""
    return (getattr(issue, "table_name", None) == WHOLE_PART
            or getattr(issue, "check_name", None) in conditions())


# ─── Running a capture ────────────────────────────────────────────────────────


class CaptureRefused(Exception):
    """A content-diff capture's self-check or mass-delete guard refused. Raise
    it from the capture: nothing it did is kept, and the stamp says
    `capture_self_check_failed` rather than `capture_failed`.

    `facts` are the numbers and sample ids the message is written from, so
    a reader that needs them does not parse the sentence."""

    def __init__(self, message: str, **facts: Any) -> None:
        super().__init__(message)
        self.facts: Dict[str, Any] = facts


def _rows(status: Any) -> int:
    """The row count out of an asyncpg status ("INSERT 0 5" → 5), else 0."""
    try:
        return int(str(status).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


def _reason(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return SELF_CHECK_MARK + text if isinstance(exc, CaptureRefused) else text


# How long one capture statement may take, the stamps included: asyncpg's
# per-call timeout, which cancels the statement and leaves the session as it
# was. Inside a writer's transaction a capture waiting on a lock — a CREATE
# INDEX or VACUUM FULL on its table, its stamp row held — would otherwise hold
# the writer open for KS_PG_TIMEOUT (30 s), every lock it took with it, and
# for its stamp row twice that. Not `SET LOCAL`: under a savepoint it
# survives RELEASE and would bound the rest of the writer's transaction too.
# The slowest capture inside a transaction copies ~920 rows (the cost).
CAPTURE_TIMEOUT_S = 5.0


async def _stamp_failed(conn, table: str, reason: str) -> None:
    """`RECORD_FAILURE_SQL` on `conn`, in a savepoint when `conn` is inside a
    transaction, so a stamp that fails costs the stamp and nothing else."""
    from core.pg_landing import RECORD_FAILURE_SQL

    try:
        async with conn.transaction():
            await conn.execute(RECORD_FAILURE_SQL, state_row(table), reason[:2000],
                               timeout=CAPTURE_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — a double fault, logged
        logger.warning("capture %s: the failure could not be stamped either: %s",
                       table, exc)


async def capture_in_transaction(conn, table: str, sql: str, *args: Any) -> bool:
    """Run one capture statement inside the writer's open transaction on
    `conn`, under a SAVEPOINT of its own. Returns whether it was kept.

    On success the capture's rows and its ok stamp are in the writer's
    transaction and commit with it. On failure both roll back alone, the
    failure is stamped and logged, and the writer carries on. Each statement
    may wait `CAPTURE_TIMEOUT_S`, so a blocked capture holds its writer
    that long at most (twice, when its stamp row is the thing held). With the
    switch off nothing is executed. Raises only a cancellation.

    The statement is the writer module's own constant, so the repository walk
    finds the writer and not this helper (`tests/unit/test_write_chains.py`).
    """
    if not captures_on():
        return False
    if table not in CAPTURES:
        logger.error("capture %s: not in CAPTURES, nothing captured", table)
        return False
    from core.pg_landing import WATERMARK_OK_SQL

    try:
        # Inside the try: a connection that cannot answer is a failure of
        # the capture, never an exception in the writer.
        if not conn.is_in_transaction():
            reason = "not inside the writer's transaction: nothing captured"
            logger.error("capture failed: %s: %s", table, reason)
            await _stamp_failed(conn, table, reason)
            return False
        async with conn.transaction():
            status = await conn.execute(sql, *args, timeout=CAPTURE_TIMEOUT_S)
            await conn.execute(WATERMARK_OK_SQL, state_row(table), _rows(status),
                               timeout=CAPTURE_TIMEOUT_S)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — rolled back alone, stamped, logged
        reason = _reason(exc)
        logger.error("capture failed: %s rolled back alone, the writer's write "
                     "carries on: %s", table, reason)
        await _stamp_failed(conn, table, reason)
        return False


Capture = Callable[[Any], Awaitable[Optional[int]]]


async def run_own_transaction(table: str, capture: Capture, *, conn=None,
                              acquire_timeout: float = 20.0) -> bool:
    """Run `capture(conn)` in a transaction of its own and stamp it. Returns
    whether it was kept.

    `capture` writes the capture's rows and returns how many (None is 0).
    `conn`, when given, must not be inside a transaction — this one would
    become a savepoint of the caller's, and that is `capture_in_transaction`.
    Without it a connection is taken from the pool. A capture that raises
    `CaptureRefused` is stamped as a refused self-check, anything else as a
    failure; nothing it wrote is kept either way. With the switch off nothing
    runs. Raises only a cancellation.
    """
    if not captures_on():
        return False
    if table not in CAPTURES:
        logger.error("capture %s: not in CAPTURES, nothing captured", table)
        return False
    if conn is not None:
        return await _own_transaction(conn, table, capture)
    try:
        from core.pg import get_pool, require_revision

        pool = await get_pool()
        async with pool.acquire(timeout=acquire_timeout) as acquired:
            await require_revision(conn=acquired)
            return await _own_transaction(acquired, table, capture)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — no connection: logged, not stamped
        logger.error("capture failed: %s could not reach Postgres: %s: %s",
                     table, type(exc).__name__, exc)
        return False


async def _own_transaction(conn, table: str, capture: Capture) -> bool:
    from core.pg_landing import WATERMARK_OK_SQL

    try:
        if conn.is_in_transaction():
            reason = "run_own_transaction inside a transaction: nothing captured"
            logger.error("capture failed: %s: %s", table, reason)
            await _stamp_failed(conn, table, reason)
            return False
        async with conn.transaction():
            rows = await capture(conn)
            await conn.execute(WATERMARK_OK_SQL, state_row(table), int(rows or 0))
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — rolled back, stamped, logged
        reason = _reason(exc)
        logger.error("capture failed: %s: %s", table, reason)
        await _stamp_failed(conn, table, reason)
        return False


# ─── The watch (the integrity job) ────────────────────────────────────────────

STATEMENT_TIMEOUT = "15s"
ACQUIRE_TIMEOUT_S = 20

_STAMPS_SQL = """
SELECT table_name, last_ok_at, failures_since_ok, last_error
FROM meta.mirror_state
WHERE table_name = ANY($1::text[])
"""


@dataclass(frozen=True)
class Stamp:
    last_ok_at: Optional[datetime]
    failures_since_ok: int
    last_error: Optional[str]


@dataclass(frozen=True)
class Facts:
    """The stamps, by capture table, and the server's `now()`. A table with
    no row is absent from `stamps`. `unwatched` is the reason they could not
    be read; `switched_off` that the switch is off, whatever was read.
    `ga4_backlog` is the GA4 capture's periods not kept yet, oldest first
    (`ga4_capture.Backlog`), None where GA4 is not configured (nothing is
    captured there, and the stamp says so) or where it was not read.
    `missing_since` is, for each never-stamped capture with a streak in the
    journal, when the first run of it started (`missing_since`); a capture
    absent from it is found so for the first time now. None is a journal not
    read, and `journal_unread` says why."""
    now: Optional[datetime] = None
    stamps: Mapping[str, Stamp] = field(default_factory=dict)
    switched_off: bool = False
    unwatched: Optional[str] = None
    ga4_backlog: Optional["Backlog"] = None
    missing_since: Optional[Mapping[str, datetime]] = None
    journal_unread: Optional[str] = None


# The never-stamped captures' streaks are read off this layer's journal, this
# far back: past every capture's grace, so a streak older than the window
# still reads as past it (`tests/unit/test_analytics_captures.py`).
JOURNAL_LAYER = "integrity"
JOURNAL_LOOKBACK = timedelta(days=14)


def _streak_names() -> Tuple[str, ...]:
    """The names `missing_since` walks the journal for: the two a
    never-stamped capture is filed under, and the two that say a run did not
    judge the stamps."""
    return (UNWATCHED, SWITCHED_OFF) + tuple(
        condition(family, table)
        for table, spec in CAPTURES.items() if spec.max_age is not None
        for family in (UNSTARTED, STALLED))


def missing_since(runs: Sequence[Tuple[datetime, Collection[str]]]) -> Dict[str, datetime]:
    """When each never-stamped capture's current streak began. Pure.

    `runs` are the integrity layer's successful runs, newest first, each with
    the names of `_streak_names` it filed. A capture's streak is the newest
    of them, unbroken, that filed its `capture_unstarted` or
    `capture_stalled`; the answer is the start of the oldest. A run that
    could not read the stamps (`captures_unwatched`) did not look and is
    passed over; a run with the switch off (`captures_switched_off`) ends
    every streak, since no writer could stamp under it. A capture the newest
    run did not file has no streak and is absent: it is found missing first
    by the run reading this."""
    open_ = {table: (condition(UNSTARTED, table), condition(STALLED, table))
             for table, spec in CAPTURES.items() if spec.max_age is not None}
    out: Dict[str, datetime] = {}
    for started_at, filed in runs:
        names = set(filed)
        if SWITCHED_OFF in names:
            break
        if UNWATCHED in names:
            continue
        for table, (unstarted, stalled) in list(open_.items()):
            if unstarted in names or stalled in names:
                out[table] = started_at
            else:
                del open_[table]
        if not open_:
            break
    return out


async def _read_missing_since(conn) -> Tuple[Optional[Dict[str, datetime]], Optional[str]]:
    """`missing_since` over the journal, read on `conn` inside a savepoint, so
    that a journal refusing the read costs this answer and not the stamps'."""
    from core import dq_journal

    try:
        async with conn.transaction():
            runs = await dq_journal.recent_findings(
                conn, layer=JOURNAL_LAYER, names=_streak_names(),
                lookback=JOURNAL_LOOKBACK)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — no grace then: filed stalled
        reason = f"{type(exc).__name__}: {exc}"
        logger.warning("capture watch: the integrity journal could not be read, "
                       "never-stamped captures are filed stalled: %s", reason)
        return None, reason
    return missing_since(runs), None


async def read_facts(*, pool=None) -> Facts:
    """Every capture's stamp. Never raises, a cancellation apart: a failure
    comes back as `Facts(unwatched=...)`. Read with the switch off too: the
    stamps are not judged then, but the GA4 backlog is — a period that leaves
    GA4's horizon while the captures are off is lost for good, and reading it
    captures nothing. The journal is read only when a capture with a cadence
    has never stamped, which is all it is asked about."""
    off = not captures_on()
    if not os.getenv("KS_PG_DSN", "").strip():
        return Facts(switched_off=off, unwatched="KS_PG_DSN is not set")
    rows = {state_row(t): t for t in CAPTURES}
    try:
        from core import ga4, ga4_capture
        from core.pg import get_pool, require_revision

        pool = pool or await get_pool()
        async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
            await require_revision(conn=conn)
            async with conn.transaction(readonly=True):
                await conn.execute(
                    f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                now = await conn.fetchval("SELECT now()")
                found = await conn.fetch(_STAMPS_SQL, list(rows))
                backlog = (await ga4_capture.backlog(conn, now)
                           if ga4.configured() else None)
                stamped = {rows[r["table_name"]] for r in found
                           if r["last_ok_at"] is not None}
                since: Optional[Dict[str, datetime]] = {}
                unread = None
                if any(spec.max_age is not None and table not in stamped
                       for table, spec in CAPTURES.items()):
                    since, unread = await _read_missing_since(conn)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — becomes the blindness reason
        return Facts(switched_off=off, unwatched=f"{type(exc).__name__}: {exc}")
    return Facts(now=now, stamps={
        rows[r["table_name"]]: Stamp(r["last_ok_at"], int(r["failures_since_ok"] or 0),
                                     r["last_error"])
        for r in found}, switched_off=off, ga4_backlog=backlog,
        missing_since=since, journal_unread=unread)


def _issue(**kw):
    from core.data_quality import IntegrityIssue

    return IntegrityIssue(**kw)


def _age(delta: timedelta) -> str:
    hours = int(delta.total_seconds() // 3600)
    return f"{hours} h" if hours < 48 else f"{hours // 24} d {hours % 24} h"


def judge(facts: Facts) -> List:
    """The findings for one integrity run. Pure. WARN at most (D8 (b)), and a
    capture failing is not also called stalled. A never-stamped capture is
    INFO inside its grace and WARN past it, and the GA4 backlog INFO while it
    has `GA4_MARGIN_NIGHTS` to spare and WARN below: each WARN under a name
    of its own, so that the morning it turns is news.

    The switch off is a WARN, and blindness (`data_quality.blindness_names`),
    so it is news in every digest it stands in: every day it stays off is a
    day missing from each history, and an INFO would never reach anyone. It
    judges no stamp and holds every stamp condition, so a failure standing
    when the switch went off is not read as healed; the GA4 backlog is still
    judged where it was read."""
    from core.data_quality import Severity

    if facts.switched_off:
        backlog_read = facts.unwatched is None and facts.ga4_backlog is not None
        held = _stamp_conditions() + (() if backlog_read else _GA4_CONDITIONS)
        issues = [_issue(
            check_name=SWITCHED_OFF, table_name=WHOLE_PART, severity=Severity.WARN,
            count=len(CAPTURES), unverified=held,
            description=(
                f"{SWITCH_ENV} is off: none of the {len(CAPTURES)} captures "
                "runs, and every day it stays off is a day missing from each "
                "history. Their failures and stalls are held as they were, "
                "not judged."))]
        if facts.unwatched is not None:
            issues += judge(Facts(unwatched=facts.unwatched))
        elif backlog_read:
            issues += _judge_ga4(facts)
        return issues
    if facts.unwatched is not None:
        return [_issue(
            check_name=UNWATCHED, table_name=WHOLE_PART, severity=Severity.WARN,
            count=len(CAPTURES), unverified=conditions(),
            description=(
                f"The capture stamps could not be read this run: "
                f"{facts.unwatched}. Their failures and stalls are held as "
                "they were, not cleared."))]

    issues = []
    for table, spec in CAPTURES.items():
        stamp = facts.stamps.get(table)
        row = state_row(table)
        if stamp is not None and stamp.failures_since_ok > 0:
            error = stamp.last_error or ""
            refused = spec.self_checked and error.startswith(SELF_CHECK_MARK)
            family = SELF_CHECK_FAILED if refused else FAILED
            what = ("refused its self-check: nothing was written and the "
                    "history is intact"
                    if refused else
                    "failed and was rolled back alone; the writer's own write "
                    "was not touched")
            issues.append(_issue(
                check_name=condition(family, table), table_name=table,
                severity=Severity.WARN, count=stamp.failures_since_ok,
                description=(
                    f"The capture into {table} {what} — "
                    f"{stamp.failures_since_ok} run(s) in a row since its last "
                    f"success. meta.mirror_state '{row}': {error[:300]}")))
            continue
        limit = spec.max_age
        if limit is None:
            continue
        if stamp is None or stamp.last_ok_at is None:
            issues.append(_never_stamped(table, spec, limit, facts))
            continue
        age = facts.now - stamp.last_ok_at if facts.now is not None else None
        if age is not None and age > limit:
            issues.append(_issue(
                check_name=condition(STALLED, table), table_name=table,
                severity=Severity.WARN, count=1,
                description=(
                    f"The capture into {table} last succeeded {_age(age)} ago "
                    f"(meta.mirror_state '{row}'), past its {_age(limit)}: "
                    f"{spec.writer} has not run it since. Each period it "
                    "misses is a gap in that history.")))
    return issues + _judge_ga4(facts)


def _when(at: datetime) -> str:
    return at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _never_stamped(table: str, spec: CaptureSpec, limit: timedelta, facts: Facts):
    """A capture with a cadence and no ok stamp: INFO `capture_unstarted`
    until its grace has run from the first run that found it so, then WARN
    `capture_stalled`; WARN at once where the journal could not say when
    that was."""
    from core.data_quality import Severity

    what = (f"The capture into {table} has never stamped meta.mirror_state "
            f"'{state_row(table)}': {spec.writer} has not run it since revision "
            "0036, or ran it with the switch off.")
    if facts.missing_since is None or facts.now is None:
        return _issue(
            check_name=condition(STALLED, table), table_name=table,
            severity=Severity.WARN, count=1,
            description=(what + " The integrity journal could not be read ("
                         f"{facts.journal_unread or 'not read'}), so how long "
                         "it has been so is not known, and it is filed as "
                         "stalled rather than given a grace."))
    since = facts.missing_since.get(table, facts.now)
    age = facts.now - since
    if age > limit:
        return _issue(
            check_name=condition(STALLED, table), table_name=table,
            severity=Severity.WARN, count=1,
            description=(what + f" The integrity runs have found it so since "
                         f"{_when(since)}, {_age(age)} ago, past its "
                         f"{_age(limit)}: its writer has had its turn and not "
                         "stamped. Each period it misses is a gap in that "
                         "history."))
    return _issue(
        check_name=condition(UNSTARTED, table), table_name=table,
        severity=Severity.INFO, count=1,
        description=(what + f" Found so since {_when(since)}, {_age(age)} ago: "
                     f"inside its {_age(limit)}, the time its writer has to "
                     "run it. Still unstamped at "
                     f"{_when(since + limit)}, it is capture_stalled (WARN)."))


def _judge_ga4(facts: Facts) -> List:
    """The GA4 backlog by its slack (`ga4_capture.nights_to_spare`): INFO
    with `GA4_MARGIN_NIGHTS` or more to spare, else WARN under its own name."""
    from core.data_quality import Severity
    from core.ga4_capture import PERIODS_PER_RUN, leaves_window_on

    backlog = facts.ga4_backlog
    slack = backlog.slack() if backlog is not None else None
    if backlog is None or backlog.oldest is None or slack is None:
        return []
    oldest, tight = backlog.oldest, slack.period
    head = (f"{backlog.missing} closed period(s) inside GA4's 14-month horizon "
            f"are not kept in {GA4_TABLE} yet")
    where = (f"the oldest unkept period, the {oldest.grain} from {oldest.start}, "
             f"leaves GA4's window on {leaves_window_on(oldest)}")
    if tight != oldest:
        where += (f", and the tightest, the {tight.grain} from {tight.start}, "
                  f"on {slack.leaves_on}")
    if slack.spare >= GA4_MARGIN_NIGHTS:
        return [_issue(
            check_name=GA4_BACKFILL, table_name=GA4_TABLE,
            severity=Severity.INFO, count=backlog.missing,
            description=(
                f"{head}, with {slack.spare} nights to spare; {where}. At "
                f"{PERIODS_PER_RUN} periods a night, oldest first, that many "
                "nightly runs could fail and every period still be kept. With "
                f"fewer than {GA4_MARGIN_NIGHTS} to spare it is "
                f"{GA4_AT_RISK} (WARN)."))]
    spare = ("no night to spare" if slack.spare < 0 else
             f"only {slack.spare} night(s) to spare, under {GA4_MARGIN_NIGHTS}")
    subject = "it" if tight == oldest else "the tightest"
    lost = (" Even with no run failing, it leaves the window unkept."
            if slack.spare < 0 else "")
    return [_issue(
        check_name=GA4_AT_RISK, table_name=GA4_TABLE,
        severity=Severity.WARN, count=backlog.missing,
        description=(
            f"{head}, and {spare}: {where}. {subject.capitalize()} has "
            f"{slack.nights_left} run(s) left before then, and oldest first at "
            f"{PERIODS_PER_RUN} a night reaches it on run {slack.nights_needed}."
            f"{lost} A period that leaves the window unkept is lost for good."))]
