"""Chain 4: the buyers, their contacts and their gender, written to Postgres.

`bronze.buyers` and `bronze.buyer_contacts` land from KeyCRM, and until this
chain DuckDB was where they landed: the buyers step of the incremental sync,
and the admin sync-all, wrote DuckDB and the mirror (`core.pg_buyers`) shipped
the same parsed rows to Postgres. `app.buyer_gender` was DuckDB's alone —
derived there each hour, then full-replaced into Postgres by
`replicate_operational`. Under `KS_WRITE_BUYERS=postgres` all three are
written here and DuckDB's copies freeze.

WHY ALL THREE MOVE TOGETHER

A verdict is a fact about a buyer's NAME, so it has to be written where the
name is. Left behind in DuckDB, the gender would be derived from names DuckDB
no longer receives, and the hourly full replace would put that frozen answer
over whatever Postgres decided. And the buyers step is the writer of both
halves: moving the buyers and not the gender would leave every new buyer with
no verdict until somebody noticed `buyers_without_verdict`.

THE ONE WRITER OF BUYER ROWS, AND THE ONE PARSE

The rows come from `core.landing_rows.parse_buyers`, the reading the mirror is
handed too, and are written by `core.pg_buyer_rows._write_buyer_rows`, which
the mirror runs as well. So a buyer looks the same in Postgres whichever path
wrote it — the argument `landing_rows` makes one layer up. What only the chain
does lives here: the latch and its owner rows, the verdict beside the row, and
what to do with a buyer Postgres would refuse.

EVERY BUYER'S VERDICT IS WRITTEN WITH ITS NAME

`upsert_buyers` classifies the names it is writing and writes the verdicts in
the same transaction, not an hour later. That closes a race the hourly
derivation alone would leave open (the plan's critic, a11): a derivation that
read a buyer's old name, and wrote after a rename committed, would store the
old name's verdict at the current rules version — and nothing would ever
select that buyer again. So:

- the writer of a name writes that name's verdict, in the name's transaction;
- `derive_gender_pg` takes a `FOR SHARE` lock on the buyers it is about to
  decide, reads their names under it, and drops a verdict whose name moved —
  a rename then waits for the derivation, or the derivation sees the rename;
- the statement joins `bronze.buyers` on the name as well, so a verdict can
  only land beside the name it was decided from.

This differs from DuckDB on purpose: there, a renamed buyer keeps its old
verdict until the rules version moves. A verdict about a name nobody holds is
not worth keeping for parity with a store that has stopped receiving names.

A person's decision is never overwritten (`override_by_human`), and a verdict
equal to the stored one is not rewritten, so `decided_at` keeps the moment the
answer last changed. `decided_at` is the web process's UTC clock, passed in —
the clock DuckDB's rows carry and the copy-back's handover orders by
(`chain_transfer._shared_clock`) — never Postgres' `now()`.

FAILURE POLICY: THE BUYERS RAISE, THE VERDICTS DO NOT, THE DERIVATION NEVER

`upsert_buyers` raises like every chain writer before it: under the flag it IS
the write, and the buyers step records the raise, holds its watermark and
retries in ten minutes. Two things it does not raise for:

- **a buyer Postgres would refuse** — a loyalty figure over `NUMERIC(5,2)`, a
  missing id. Refused before the latch where it can be seen (`_refusal`), and
  retried buyer by buyer on a `DataError` it could not foresee; either way the
  buyer is skipped, named in the log and counted in `skipped_out`, and the
  rest land. A batch that fails the same way every tick until one value
  changes is not a writer that stopped, and it must not take the other 499
  buyers with it;
- **a verdict** — written in a savepoint and given up on any Postgres error,
  because a buyer without a verdict is what the hourly derivation exists to
  fix. The catch is `asyncpg.PostgresError` and nothing wider: anything else is
  not a verdict problem.

`derive_gender_pg` never raises, `derive_gender`'s contract — it rides the
replication runner beside the copy of tables nothing can rebuild.

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) inside
`pool.acquire()` and before its transaction, and claims the three owner rows
inside that transaction. From the first buyer written here only
`scripts/chain_copy_back.py buyers` can move the writes back to DuckDB. Two
things are done before the latch so that a write which could not have landed
does not spend the rollback: the refusal check above, and the derivation's
read of what is pending — an hourly run with nothing to decide latches
nothing.

A PRECONDITION THE FLAG ENFORCES

Three readers show buyers on screen: the SMS audience (`KS_SMS_STORE`), the
search index (`KS_READ_SEARCH_INDEX`) and the dashboard's buyer reads
(`KS_READ_DASHBOARD`). Each can still read DuckDB, and once this chain writes
Postgres, DuckDB's buyers are a copy that stopped. So `unmet_precondition()`
names any of the three not on postgres, and until all are an unlatched chain
runs as duckdb whatever `KS_WRITE_BUYERS` says — DN-26's rule, which chain 6a
made generic in the registry for this chain (DN-27). Each flag is read through
its own reader's parser, all of which raise on a typo; a raise is unmet.

The warning that says so is rate-limited. Chain 6a warns on every call, which
is once a week for it; this chain's key is read by every incremental tick, so
the same sentence would reach the log once a minute for as long as the
readers lag. `/api/health` publishes the reason on every read and the canary
warns on it (`write_chain_precondition_unmet`), which is where it is read.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_BUYERS"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_buyers_write"

# The tables this chain writes. `core.write_chains` reads this, and through it
# the hourly shipper (`app.buyer_gender`), the buyers mirror, the daily
# comparisons and the copy-back. The two bronze tables are `pg_buyers.BUYER_UNIT`
# — what one writer writes in one transaction changes hands as one — and the
# gender rides with them because it is decided from their names.
CHAIN_TABLES: Tuple[str, ...] = (
    "bronze.buyers", "bronze.buyer_contacts", "app.buyer_gender")

# The one `sync_metadata` key that moves with it — what the buyers step stamps
# after a batch lands. The store's getter and setter route it through
# `core.write_chains.chain_for_sync_key` to `meta.chain_watermarks`.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_buyers",)

# Not judged by `core.pg_chain_invariants`. The canary already pages
# `buyer_sync_stalled` at 90 minutes without a successful step, from the
# step's own state, and since this chain `buyer_sync_stalled_chain` at three
# hours; a second limit on the stamp the same step writes would say one stall
# twice under two names (chain 6a's reasoning, the plan critic's a10).
# `_freshness_check` still judges the key, at its own threshold.
CHAIN_WATERMARK_MAX_AGE_MIN: Optional[int] = None

# How many buyers one transaction carries — the tick's whole batch at most,
# and the sync-all's portion in two.
WRITE_PORTION = 500

# How many verdicts one derivation transaction carries: a full re-derivation is
# ~20 000 names, and one transaction holding a share lock on all of them would
# hold the buyers step for its whole length.
DERIVE_PORTION = 2000

# Per statement, inside every transaction here. Nothing on the client side
# cancels a statement in flight without leaving the connection in doubt
# (DN-05a: a bare `wait_for` over asyncpg waited 150 s past a 10 s bound), so
# the server is asked to give up instead.
STATEMENT_TIMEOUT = "30s"

# The whole write phase of one `upsert_buyers` call, checked between portions
# and before each buyer-by-buyer retry. The step holds the heavy-job lock while
# it writes, and the warehouse refresh waits behind it every two minutes.
WRITE_DEADLINE_S = 120.0

# How long an acquire may wait. The pool sets none (`core.pg.get_pool`), and
# the buyers step runs inside the incremental tick, under the heavy-job lock.
ACQUIRE_TIMEOUT_S = 10

# `NUMERIC(precision, scale)` of the buyer columns a KeyCRM number lands in —
# revision 0011's, and DuckDB's `DECIMAL`s beside them.
NUMERIC_COLUMNS = {
    "loyalty_discount": (5, 2),
    "loyalty_amount": (12, 2),
}

# How often the unmet-precondition warning may repeat for one reason.
UNMET_WARN_EVERY_S = 3600.0
_unmet_warned: Dict[str, float] = {}


def env_writes_postgres() -> bool:
    """What `KS_WRITE_BUYERS` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule. Since DN-01 that
    stops this chain and nothing else: the registry stands it down, and the
    buyers step refuses before it fetches (`SyncService.sync_missing_buyers`).
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


# The readers of the buyers that can still read DuckDB: `(flag, module,
# parser)`. Imported and looked up when asked, inside that reader's own
# `try`, so a module or a parser that is gone — should the search index's
# DuckDB twin and `KS_READ_SEARCH_INDEX` be retired (plan rev2 §5.4, which
# OD-10's #271 did not do) — makes that one reader unmet and says why, rather than raising out of the whole
# question, which the registry would read as "could not be read" and hold the
# chain on DuckDB for good. `tests/unit/test_pg_buyers_write.py` requires each
# entry to exist and to be the parser of the flag it names, so whoever removes
# one changes this list in the same commit.
READERS: Tuple[Tuple[str, str, str], ...] = (
    ("KS_SMS_STORE", "core.pg_sms", "sms_store_is_postgres"),
    ("KS_READ_SEARCH_INDEX", "core.pg_search_index_read", "enabled"),
    ("KS_READ_DASHBOARD", "core.pg_dashboard_read", "enabled"),
)


def unmet_precondition() -> Optional[str]:
    """Why `KS_WRITE_BUYERS=postgres` must not move the writes yet, or None.

    Never raises and never asks Postgres: `/api/health` reads it through the
    registry, and must still answer with Postgres down. A reader flag nobody
    can parse is unmet, because nobody can then say that reader follows the
    writes; so is a reader whose parser cannot be found."""
    import importlib

    lagging: List[str] = []
    for name, module, parser in READERS:
        try:
            if not getattr(importlib.import_module(module), parser)():
                lagging.append(f"{name} is not postgres")
        except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
            lagging.append(f"{name} is not understood ({exc})")
    if not lagging:
        return None
    return ("; ".join(lagging) + ", so that reader shows DuckDB's buyers, which "
            "stop changing the moment this chain writes Postgres; set every "
            "buyer reader to postgres first")


def _warn_unmet(reason: str) -> None:
    now = time.monotonic()
    last = _unmet_warned.get(reason)
    if last is not None and now - last < UNMET_WARN_EVERY_S:
        return
    _unmet_warned[reason] = now
    logger.warning("%s=postgres, but the chain stays on DuckDB: %s", WRITE_ENV, reason)


def writes_postgres() -> bool:
    """Whether this chain writes Postgres — the one answer every caller reads.

    The latch outranks the flag (OD-19 (a)), and outranks a value nobody can
    read. Unlatched, the flag moves the writes only once `unmet_precondition()`
    holds — until then the chain runs as duckdb. Raises on a flag nobody can
    read while unlatched; `mode()` is the question that never raises.
    """
    if chain_latch.latched(CHAIN):
        return True
    if not env_writes_postgres():
        return False
    unmet = unmet_precondition()
    if unmet:
        _warn_unmet(unmet)
        return False
    return True


def mode() -> Optional[str]:
    """Where this chain's writes go: "postgres", "duckdb", or None when
    `KS_WRITE_BUYERS` is not understood and no latch overrides it.

    The registry's answer (`write_chains.chain_modes`), for the callers that
    must decide without raising — the buyer selection, the gender rider, the
    stats, a route that must refuse before it starts. Never raises."""
    from core import write_chains

    return write_chains.chain_modes()[CHAIN]["mode"]


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Called with a connection already acquired, after `require_revision()` has
    passed — `pg_expenses_write._latch` has the reason: the latch is permanent,
    and a write that never reaches Postgres must not spend the rollback.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# What `bronze.buyers` (revision 0011) and `bronze.buyer_contacts` hold, by
# the type asyncpg must be able to send: INTEGER columns take an int in int32,
# TEXT columns a str that encodes as UTF-8, and the date and timestamps their
# Python types. The column lists are the shared parse's, never restated.
_INT_COLUMNS = frozenset({"id", "manager_id", "company_id"})
_TEXT_COLUMNS = frozenset({"full_name", "note", "phone", "email", "company_name",
                           "city", "region", "loyalty_program_name",
                           "loyalty_level_name"})
_INT32 = (-2 ** 31, 2 ** 31 - 1)


def _value_refusal(column: str, value) -> Optional[str]:
    """Why the driver or the server would refuse `value` in `column`, or None.
    Names the column and the kind of value, never the value itself: it may be
    somebody's phone number typed into the wrong field."""
    from datetime import date

    if value is None:
        return "id is NULL" if column == "id" else None
    if column in _INT_COLUMNS:
        if isinstance(value, bool) or not isinstance(value, int):
            return f"{column} is a {type(value).__name__}, not an integer"
        if not _INT32[0] <= value <= _INT32[1]:
            return f"{column} is outside INTEGER's range"
    elif column in _TEXT_COLUMNS:
        if not isinstance(value, str):
            return f"{column} is a {type(value).__name__}, not text"
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            # A lone surrogate — what a JSON "\ud800" escape decodes to. The
            # parse passes it through; asyncpg refuses it as a DataError.
            return f"{column} is not valid UTF-8"
    elif column == "birthday":
        if not isinstance(value, date) or isinstance(value, datetime):
            return f"birthday is a {type(value).__name__}, not a date"
    elif column in ("created_at", "updated_at"):
        if not isinstance(value, datetime):
            return f"{column} is a {type(value).__name__}, not a timestamp"
    return None


def _refusal(row, contacts) -> Optional[str]:
    """Why Postgres would refuse this buyer, or None. Asked before the latch.

    Everything the driver or the table would refuse, column by column: an id
    that is missing or not an int32, text that is not UTF-8 (a lone
    surrogate), a value of the wrong type, a loyalty figure outside its
    NUMERIC, a contact naming another buyer or carrying such text. The parse
    already strips NUL, writes 'Unknown' for a blank name and reads a birthday
    DuckDB would refuse as NULL. A first batch the server refused whole after
    the latch would leave the marker with no owner row behind it — CRITICAL
    `chain_latch_disagrees` the next morning and a copy-back that refuses — so
    what can be foreseen is refused here; what cannot is retried buyer by
    buyer and skipped (`upsert_buyers`)."""
    from core.pg_numeric import refusal

    for column in type(row)._fields:
        why = _value_refusal(column, getattr(row, column))
        if why:
            return why
    for column, (precision, scale) in NUMERIC_COLUMNS.items():
        why = refusal(column, getattr(row, column), precision, scale, nullable=True)
        if why:
            return why
    for contact in contacts:
        if contact.buyer_id != row.id:
            return f"a contact names buyer {contact.buyer_id!r}"
        for field in ("contact_type", "value"):
            value = getattr(contact, field)
            if not isinstance(value, str) or not value:
                return f"a contact's {field} is not text"
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                return f"a contact's {field} is not valid UTF-8"
    return None


def _verdicts(rows) -> List[Tuple[int, str, Any]]:
    """`(buyer_id, name, Verdict)` for each row — pure CPU, no connection."""
    from core.gender import classify

    return [(r.id, r.full_name, classify(r.full_name)) for r in rows]


async def _write_gender_rows(conn, verdicts: Sequence[Tuple[int, str, Any]],
                             decided_at: datetime) -> int:
    """Write one verdict per buyer; returns how many rows changed.

    Beside the name it was decided from, and only there: the JOIN on
    `full_name` drops a verdict whose buyer has since been renamed. Never over
    a human's decision, and never a rewrite of an equal verdict, so
    `decided_at` is the moment the answer last changed. In `buyer_id` order,
    like every buyer write, so two writers take their locks the same way round.
    Never deletes: a buyer's verdict outlives a sync that did not mention it.
    """
    from core.gender import RULES_VERSION

    if not verdicts:
        return 0
    ordered = sorted(verdicts, key=lambda v: v[0])
    status = await conn.execute(
        """
        INSERT INTO app.buyer_gender AS g
            (buyer_id, gender, method, confidence, decided_from,
             rules_version, decided_at)
        SELECT v.buyer_id, v.gender, v.method, v.confidence, v.decided_from,
               $7, $8
        FROM unnest($1::int[], $2::text[], $3::text[], $4::text[],
                    $5::text[], $6::text[])
             AS v(buyer_id, name, gender, method, confidence, decided_from)
        JOIN bronze.buyers b
          ON b.id = v.buyer_id AND b.full_name IS NOT DISTINCT FROM v.name
        ORDER BY v.buyer_id
        ON CONFLICT (buyer_id) DO UPDATE SET
            gender        = EXCLUDED.gender,
            method        = EXCLUDED.method,
            confidence    = EXCLUDED.confidence,
            decided_from  = EXCLUDED.decided_from,
            rules_version = EXCLUDED.rules_version,
            decided_at    = EXCLUDED.decided_at
        WHERE NOT g.override_by_human
          AND (g.gender, g.method, g.confidence, g.decided_from, g.rules_version)
              IS DISTINCT FROM
              (EXCLUDED.gender, EXCLUDED.method, EXCLUDED.confidence,
               EXCLUDED.decided_from, EXCLUDED.rules_version)
        """,
        [v[0] for v in ordered], [v[1] for v in ordered],
        [v[2].gender for v in ordered], [v[2].method for v in ordered],
        [v[2].confidence for v in ordered], [v[2].decided_from for v in ordered],
        RULES_VERSION, decided_at,
    )
    return int(status.rsplit(" ", 1)[-1])


def _is_data_error(exc: BaseException) -> bool:
    """A value this one buyer carries, not a writer that is broken.

    `DataError` (SQLSTATE class 22) only. A NOT NULL, unique or check
    violation is a defect in what this module writes — the parse never yields
    one — and retrying buyer by buyer would hide it behind a skipped count
    (the plan critic's a8)."""
    import asyncpg

    return isinstance(exc, asyncpg.DataError)


async def upsert_buyers(buyers: Sequence[Any], *,
                        skipped_out: Optional[List[int]] = None) -> int:
    """Write a batch of KeyCRM buyers, their contacts and their verdicts.
    Returns how many buyers were written. Raises (module docstring).

    A buyer Postgres would refuse is skipped rather than raised: its id is
    logged and appended to `skipped_out`, and the rest of the batch lands.
    """
    import asyncpg

    from core.landing_rows import parse_buyers
    from core.pg_buyer_rows import _write_buyer_rows

    if not buyers:
        return 0
    parsed = parse_buyers(buyers)
    if parsed.unreadable_birthdays:
        logger.warning(
            "buyers: %d unreadable birthday(s) stored as NULL, ids %s",
            len(parsed.unreadable_birthdays), parsed.unreadable_birthdays)
    # One row per buyer, the last one given — what DuckDB's `INSERT OR REPLACE`
    # leaves of a buyer named twice in a batch. Twice in one statement,
    # Postgres refuses the verdicts' `ON CONFLICT DO UPDATE` outright.
    contacts = dict(parsed.contacts)
    latest = {row.id: row for row in parsed.rows}
    skipped: List[int] = []
    rows = []
    # Everything Postgres could refuse is refused here, before the latch — a
    # first batch refused whole would otherwise latch the chain with no owner
    # row behind it (`core.pg_numeric`).
    for row in latest.values():
        why = _refusal(row, contacts.get(row.id, ()))
        if why:
            logger.error("buyers: buyer %r not written to Postgres: %s", row.id, why)
            skipped.append(row.id)
        else:
            rows.append(row)
    if skipped_out is not None:
        skipped_out.extend(skipped)
    if not rows:
        return 0
    verdicts = {v[0]: v for v in _verdicts(rows)}
    decided_at = _now_utc()

    def _portion_of(portion):
        return ([tuple(r) for r in portion],
                [(r.id, [tuple(c) for c in contacts.get(r.id, ())]) for r in portion],
                [verdicts[r.id] for r in portion])

    written = 0
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        # From the latch, not from the call: the wait for the pool and the
        # revision check are unbounded above it, and a deadline spent there
        # would expire with the marker on disk and nothing sent. The first
        # portion is always attempted.
        deadline = time.monotonic() + WRITE_DEADLINE_S
        for start in range(0, len(rows), WRITE_PORTION):
            if start and time.monotonic() > deadline:
                raise TimeoutError(
                    f"buyers: the write phase passed {WRITE_DEADLINE_S:.0f}s with "
                    f"{len(rows) - start} buyer(s) unwritten")
            portion = rows[start:start + WRITE_PORTION]
            buyer_rows, buyer_contacts, portion_verdicts = _portion_of(portion)
            try:
                async with conn.transaction():
                    await conn.execute(
                        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                    # The owner rows and the buyers land together or neither
                    # does, so a claim never outlives the write that earned it.
                    await chain_latch.claim(conn, CHAIN_TABLES, stamp)
                    await _write_buyer_rows(conn, buyer_rows, buyer_contacts)
                    await _write_verdicts(conn, portion_verdicts, decided_at)
                written += len(portion)
                continue
            except asyncpg.DataError as exc:
                logger.warning(
                    "buyers: a portion of %d was refused (%s); writing it buyer "
                    "by buyer", len(portion), type(exc).__name__)
            for row in portion:
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"buyers: the write phase passed {WRITE_DEADLINE_S:.0f}s "
                        "while retrying a refused portion buyer by buyer")
                one_row, one_contacts, one_verdict = _portion_of([row])
                try:
                    async with conn.transaction():
                        await conn.execute(
                            f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                        await chain_latch.claim(conn, CHAIN_TABLES, stamp)
                        await _write_buyer_rows(conn, one_row, one_contacts)
                        await _write_verdicts(conn, one_verdict, decided_at)
                    written += 1
                except asyncpg.DataError as exc:
                    # The id only: the value may be somebody's phone number
                    # typed into the wrong field.
                    logger.error("buyers: buyer %r refused by Postgres: %s",
                                 row.id, type(exc).__name__)
                    if skipped_out is not None:
                        skipped_out.append(row.id)
    return written


# A verdict that nothing will select again unless it is marked: decided by no
# rules at all, so `gender_backfill`'s pending question (`rules_version <
# RULES_VERSION`) takes it on the next hourly derivation. An UPDATE, never a
# DELETE, and never over a human's decision.
_STALE_VERDICTS = """
UPDATE app.buyer_gender SET rules_version = 0
WHERE buyer_id = ANY($1::int[]) AND NOT override_by_human
"""


async def _write_verdicts(conn, verdicts, decided_at: datetime) -> None:
    """The portion's verdicts in a savepoint, given up on any Postgres error:
    the buyers must land whatever the gender does. `asyncpg.PostgresError`
    and nothing wider.

    Given up is not left alone. This portion may have renamed a buyer, and the
    old name's verdict at the current rules version is one the hourly
    derivation would never select again — so the portion's verdicts are
    marked stale instead, and the derivation decides them within the hour
    (review of PR-3). If that fails too, the portion rolls back with it."""
    import asyncpg

    try:
        async with conn.transaction():
            await _write_gender_rows(conn, verdicts, decided_at)
    except asyncpg.PostgresError as exc:
        logger.error("gender derivation failed inline for %d buyer(s): %s; "
                     "their verdicts are marked stale for the hourly derivation",
                     len(verdicts), type(exc).__name__)
        async with conn.transaction():
            await conn.execute(_STALE_VERDICTS, [v[0] for v in verdicts])


async def _pending(conn, rebuild_all: bool) -> List[Tuple[int, str]]:
    """The buyers that need a verdict — `gender_backfill`'s question, asked of
    Postgres. Read before the latch, so a run with nothing to decide takes
    nothing."""
    from core.gender_backfill import pending_sql
    from core.sql_dialect import POSTGRES, numbered

    sql, params = pending_sql(POSTGRES.buyers, POSTGRES.buyer_gender,
                              rebuild_all=rebuild_all)
    return [(r[0], r[1]) for r in await conn.fetch(numbered(sql), *params)]


async def _current_names(conn, ids: Sequence[int]) -> Dict[int, Any]:
    """The names of these buyers as they stand, share-locked until the caller's
    transaction ends: a rename now waits for the verdicts, or was already
    seen. In id order, like every other lock taken on these rows."""
    rows = await conn.fetch(
        "SELECT id, full_name FROM bronze.buyers WHERE id = ANY($1::int[]) "
        "ORDER BY id FOR SHARE", list(ids))
    return {r["id"]: r["full_name"] for r in rows}


async def derive_gender_pg(*, rebuild_all: bool = False) -> Dict[str, Any]:
    """Decide every buyer Postgres holds without a verdict of the current rules.
    Never raises — `derive_gender`'s contract and its result's shape.

    One connection for the whole run: the pending read before the latch, then
    a transaction per `DERIVE_PORTION` holding share locks on that portion's
    buyers. A full re-derivation (`rebuild_all`) holds one of the pool's five
    connections while it runs.
    """
    from core.gender import RULES_VERSION, classify

    out: Dict[str, Any] = {
        "pending": 0, "written": 0, "female": 0, "male": 0, "null": 0,
        "rules_version": RULES_VERSION, "error": None, "ms": 0,
    }
    started = time.monotonic()
    try:
        pool = await _pool()
        async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
            rows = await _pending(conn, rebuild_all)
            out["pending"] = len(rows)
            if not rows:
                return out
            verdicts = [(bid, name, classify(name)) for bid, name in rows]
            out["female"] = sum(1 for v in verdicts if v[2].gender == "f")
            out["male"] = sum(1 for v in verdicts if v[2].gender == "m")
            out["null"] = sum(1 for v in verdicts if v[2].gender is None)
            decided_at = _now_utc()
            stamp = _latch()
            for start in range(0, len(verdicts), DERIVE_PORTION):
                portion = verdicts[start:start + DERIVE_PORTION]
                async with conn.transaction():
                    await conn.execute(
                        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                    await chain_latch.claim(conn, CHAIN_TABLES, stamp)
                    names = await _current_names(conn, [v[0] for v in portion])
                    kept = [v for v in portion
                            if v[0] in names and names[v[0]] == v[1]]
                    out["written"] += await _write_gender_rows(conn, kept, decided_at)
                # Let the buyers step take its locks between portions.
                await asyncio.sleep(0)
        logger.info(
            "gender (postgres): %d written of %d pending (f=%d m=%d null=%d, "
            "rules v%d)", out["written"], out["pending"], out["female"],
            out["male"], out["null"], RULES_VERSION)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — returned, never raised
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["error_class"] = type(exc).__name__
        logger.error("gender derivation failed (postgres): %s", out["error"],
                     exc_info=True)
    finally:
        out["ms"] = int((time.monotonic() - started) * 1000)
    return out
