"""Parsing `silver.order_utm` inside Postgres, from `bronze.orders` (step 9b).

Until now Postgres has never computed this table. `core/pg_order_utm.py` copies
it out of DuckDB, whose parser runs inside the warehouse tick — so the only
automatic UTM parse in the system lives in the code the DuckDB exit stops, and
a copy of a frozen DuckDB table (or, after a Sunday compaction, an empty one)
would keep shipping under an OK watermark. This module moves the parse to the
rows Postgres already holds: `bronze.orders.manager_comment` is written by the
same mirror as DuckDB's `orders.manager_comment`, from the same parsed payload,
with the same `COALESCE(new, stored)`.

**Wired behind `KS_UTM_PARSE`** (DN-19; default `duckdb`, which is today's
behaviour byte for byte). Under `duckdb` nothing here runs and
`ship_order_utm` stays the only writer of the table. Under `postgres` — which
needs `KS_PG_DERIVE=own` — the derivation's last step is
`parse_incremental_locked`, run once the derivation has let `PG_LAYER_LOCK`
go, and the DuckDB tick stops shipping. The two never both write the table in one
process: the choice is one cached mode, read before anything writes, and both
the tick and the four re-parse doors ask it — the doors through
`reparse_router`, which is the one place a re-parse that went round the tick
decides which store it reaches. See THE MODE and THE ROUTER below.

ONE PARSER, NOT TWO

The verdict comes from `core.utm_classify.utm_columns` — the function DuckDB's
parse calls — over the same selection: an order with a non-empty comment and
either no row yet or `updated_at > parsed_at`, stamped with `parsed_at =` the
order's own `updated_at`. `tests/integration/test_pg_utm_parse.py` runs both
engines over the golden fixtures and compares every column at zero tolerance.
A SQL re-derivation of the classifier would be a second one (revision 0018's
argument); running the first one against a second store is not.

TWO SHAPES

- `parse_incremental` — the tick's shape: what is new or changed, upserted.
  **Stamps the watermark on every success, including zero rows**, so
  `last_ok_at` measures that the parser is alive rather than that somebody
  ordered something. `mirror_orders` refuses to move its watermark on nothing
  shipped, and the ~90-minute canary limit would then page every quiet night.
- `parse_full` — the reclassify's shape: every commented order re-parsed, the
  table replaced whole in one transaction, so a reader sees the old verdicts
  or the new ones and never DuckDB's 281-second half-deleted middle. It is the
  only way a rule change reaches orders whose `updated_at` has not moved.

WHAT SERIALISES THEM

`PG_LAYER_LOCK` is per process, and every parse in production runs under it,
waited for at most `LOCK_WAIT_S`: doors behind HTTP handlers wait on it, and
the ClickHouse shippers hold it across network I/O. `parse_full` takes it
itself. `parse_incremental` does not — `parse_incremental_locked` takes it and
then calls it, and that wrapper is what every caller uses: the derivation's
last step and the two doors that only add. The derivation's step runs *after*
`_derive_and_journal_pg_layers` has released the lock, not inside it.
`asyncio.Lock` is not reentrant, so neither lock-taking shape may be called
by code already holding it: moved inside the derivation's
`async with PG_LAYER_LOCK`, the parse would wait `LOCK_WAIT_S` on its own
caller, then record a failure, on every run. `parse_incremental` on its own
assumes its caller holds the lock or needs no ordering against another
in-process writer of the table.

A full parse can also be run from a `docker exec` CLI, which is a process of
its own, so both shapes take `pg_advisory_xact_lock(ADVISORY_LOCK_KEY)` inside
their write transaction and read `bronze.orders` only once they hold it. Two
parses therefore never interleave anywhere, and the second always reads what
the first committed.

Every wait for a lock inside Postgres is bounded by `lock_timeout`, rendered
in milliseconds. A parse waits on the advisory key while it holds
`PG_LAYER_LOCK` and, as the derivation's last step, the scheduler's
`_heavy_job_lock` too, which every orders writer takes. An unbounded wait on a
key a stuck CLI or an idle psql holds would therefore hang the derivation and,
behind it, the sync.

WHY THE FULL PARSE DELETES AND NEVER TRUNCATES

TRUNCATE is not MVCC-safe: a REPEATABLE READ snapshot taken before it commits
reads the table empty afterwards. In process that is held off by
`PG_LAYER_LOCK` — `core/pg_warehouse_dq.py` takes its snapshot under it for
exactly that reason — but a CLI's full parse holds a `PG_LAYER_LOCK` of its
own, so its TRUNCATE would land inside the twins' snapshot and file 0 %
website attribution against a table that was only being replaced. `DELETE`
is versioned like every other write, so every snapshot sees the old verdicts
or the new ones. It also takes no ACCESS EXCLUSIVE lock, which would queue
every `/traffic` read behind the parse for up to `LOCK_WAIT_S`. The price is
~33 K dead tuples per full parse, which is rare, for autovacuum to reclaim.

A FULL PARSE REFUSES A SHRINK

Replacing the table whole replaces it with whatever `bronze.orders` holds, and
a `bronze.orders` that was truncated, half-restored or read from the wrong
database holds less than the truth. A missing row here is not a gap anybody
sees — the order falls through the readers' `COALESCE` to organic or
unattributed, on `/traffic` and in the Monday traffic report. So a full parse
yielding under `FULL_PARSE_FLOOR_PCT` of the rows the table holds is refused,
returned rather than raised, and logged at ERROR, unless `force=True`. It is
also written into the watermark, but that row is the incremental's liveness
stamp too, and the next successful incremental clears it — see `parse_full`.

FAILURES

Any error is written to `meta.mirror_state` through `core.pg_landing`'s
`_record_failure` and then raised — loud, like `ship_order_utm`: the caller
decides what a failure costs, and a parser that reported success while writing
nothing is the worst outcome available. Every write is one transaction, so a
failure leaves the table exactly as it was. No migration: `parsed_at` exists
since revision 0018.

THE MODE

`KS_UTM_PARSE` is `duckdb`, the default, or `postgres`. Read once, by
`configure_mode()` from `core.runtime_modes.configure_modes()` — before web's
boot sync, like every cached mode (DN-05b) — and never on a write path:
`parses_in_postgres()` reads the cache. `pg_derivation` is configured first
there, which is what lets this one read its answer.

`postgres` without `KS_PG_DERIVE=own` has no derivation to be the last step
of: the tick would stop shipping and nothing would parse, so the table would
freeze under the last OK watermark while every new order fell through the
COALESCE. So an unknown value, or `postgres` without `own`, **runs as
`duckdb`**, logs ERROR once, and publishes the error in `/api/health` under
`utm_parse`; the canary warns `utm_parse_mode_invalid`. It never raises. The
design's "startup raises" would be a crash loop in web, the only process that
syncs orders, over a setting about where one table is parsed — OD-09's answer,
and `KS_PG_DERIVE`'s and `KS_READ_FALLBACK`'s before it.

DuckDB's own parse is not stopped by this flag, in the tick or at the doors.
That is what makes the rollback one variable: unset it, and the next tick
ships a DuckDB copy that never stopped being current.

THE ROUTER

Four doors re-parse outside the tick: `POST /api/traffic/refresh`,
`/traffic/reclassify`, the `manager_comment` backfill behind
`/traffic/backfill-utm`, and `scripts/backfill_utm.py`. Each runs its DuckDB
parse and then calls `reparse_router`, and nothing else reaches Postgres from
them:

- under `duckdb`, `ship_after_reparse(store, force=force)`, as before;
- under `postgres`, the parse itself — `parse_full` for the two doors that
  DELETE and re-parse everything (the reclassify and the CLI),
  `parse_incremental_locked` for the two that only add.

`force` reaches `parse_full` from the CLI's flag and from nowhere else, as it
reached the ship. The router never raises: the DuckDB half has already
succeeded and the door reports it. `tests/unit/test_order_utm_shipping.py`
walks the code for every out-of-tick caller of `refresh_utm_silver_layer` and
requires the router, and for every call of a ship and requires the `duckdb`
branch around it.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from core.pg_derivation import DERIVED_MAX_AGE_S
from core.pg_order_utm import UTM_COLUMNS, UTM_TABLE
from core.utm_classify import utm_columns

logger = logging.getLogger(__name__)

# ─── Who writes silver.order_utm: DuckDB's copy, or Postgres' own parse ──────

ENV = "KS_UTM_PARSE"
DUCKDB = "duckdb"
POSTGRES = "postgres"
_VALID = (DUCKDB, POSTGRES)

_mode: Optional[str] = None
_mode_error: Optional[str] = None

# The age `/api/health` declares for this table's watermark under `postgres`,
# which the canary judges (never looser than its own ceiling). The parse is
# the derivation's last step and stamps the watermark on every success, zero
# rows included, so it runs exactly as often as the derivation does — at least
# once a heartbeat, past the floor — and the derived tables' limit is its
# limit. Its own name because it is its own watermark: a derivation that runs
# while this step keeps failing ages this row and not theirs.
MAX_AGE_S = DERIVED_MAX_AGE_S


def configure_mode() -> str:
    """Read `KS_UTM_PARSE` once. Never raises. Called by `configure_modes()`,
    after `pg_derivation.configure_mode()`, whose answer it depends on."""
    global _mode, _mode_error
    from core import pg_derivation

    value = os.getenv(ENV, DUCKDB).strip().lower() or DUCKDB
    if value not in _VALID:
        error: Optional[str] = (f"{ENV}={value!r} is not one of {_VALID}; "
                                f"running as {DUCKDB!r}")
    elif value == POSTGRES and not pg_derivation.owns():
        error = (f"{ENV}={POSTGRES!r} needs {pg_derivation.ENV}="
                 f"{pg_derivation.OWN!r}, and it is {pg_derivation.mode()!r}; "
                 f"running as {DUCKDB!r}")
    else:
        error = None
    # Logged when it changes, not on every call: web's startup and the
    # scheduler both configure, and one ERROR per cause is what a reader of
    # the log can use.
    if error and error != _mode_error:
        logger.error(error)
    _mode = DUCKDB if error else value
    _mode_error = error
    return _mode


def mode() -> str:
    """The configured mode; `duckdb` until `configure_mode` has run."""
    return _mode or DUCKDB


def mode_error() -> Optional[str]:
    return _mode_error


def parses_in_postgres() -> bool:
    """Postgres parses the table and nothing ships it. Reads the cache, never
    the environment."""
    return _mode == POSTGRES

# `int.from_bytes(b"ks:utm", "big")`. One key for every UTM parse in every
# process, and for nothing else: an advisory lock is a number, and a number
# another module happened to pick would serialise two unrelated jobs.
ADVISORY_LOCK_KEY = 118142646187117

# How long either parse waits for `PG_LAYER_LOCK`, in this process.
# `core/pg_order_utm.py`'s number, for its reason: a normal wait is one Silver
# tick, so this fires only on a genuine hang.
LOCK_WAIT_S = 120

# How long a wait inside Postgres may last (`lock_timeout`): the advisory lock
# and any row lock the writes meet. Kept below the production pool's
# `command_timeout` (`KS_PG_TIMEOUT`, 30 s, core/pg.py) on purpose. Above it,
# asyncpg cancels the statement first and the watermark records a bare
# `TimeoutError` — naming no lock, no key and no lever — which is what every
# failed tick would have said while a stuck CLI or an idle psql held the key.
# Below it, Postgres ends the wait itself and the watermark reads
# `LockNotAvailableError: canceling statement due to lock timeout`.
PG_LOCK_WAIT_S = 20

# The share of the table's current rows a full parse must yield to replace it,
# as a whole percentage so the comparison is integer arithmetic and 90 of 100
# is on the line rather than a float's rounding either side of it. Not zero
# tolerance: a comment KeyCRM has since emptied loses its verdict on a full
# parse (the DuckDB reclassify does the same), and that is legitimate. Ten per
# cent is ~3,300 orders at 2026-09 volume — far more than that, and nowhere
# near the whole-table loss an emptied `bronze.orders` makes.
FULL_PARSE_FLOOR_PCT = 90

# `core/pg_operational.py`'s chunk, for its reason.
CHUNK = 5000

# What needs parsing: `TrafficMixin._parse_utm_into_silver`'s predicate over
# DuckDB's `orders`, read against `bronze.orders`. Ordered so a run's writes
# and its log are reproducible.
INCREMENTAL_SELECT_SQL = f"""
SELECT o.id, o.manager_comment, o.updated_at
FROM bronze.orders o
LEFT JOIN {UTM_TABLE} u ON u.order_id = o.id
WHERE o.manager_comment IS NOT NULL
  AND o.manager_comment != ''
  AND (
      u.order_id IS NULL
      OR o.updated_at > u.parsed_at
  )
ORDER BY o.id
"""

# Every order the parse could say anything about. The DuckDB reclassify is a
# DELETE followed by the incremental parse, which after the DELETE selects
# exactly this.
FULL_SELECT_SQL = """
SELECT o.id, o.manager_comment, o.updated_at
FROM bronze.orders o
WHERE o.manager_comment IS NOT NULL
  AND o.manager_comment != ''
ORDER BY o.id
"""

COUNT_SQL = f"SELECT count(*) FROM {UTM_TABLE}"


def _values() -> str:
    # `parsed_at` is the order's `updated_at`, and the wall clock only when the
    # order has none — DuckDB's `COALESCE(?, CURRENT_TIMESTAMP)`, so an order
    # with no `updated_at` still gets a verdict and is not re-parsed forever.
    params = [f"${i}" for i in range(1, len(UTM_COLUMNS))]
    params.append(f"COALESCE(${len(UTM_COLUMNS)}::timestamptz, now())")
    return ", ".join(params)


def insert_sql() -> str:
    return f"INSERT INTO {UTM_TABLE} ({', '.join(UTM_COLUMNS)}) VALUES ({_values()})"


def upsert_sql() -> str:
    # Every column replaced, as DuckDB's `INSERT OR REPLACE` does: a re-parse
    # that now finds nothing in a field must clear it, not keep the old value.
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in UTM_COLUMNS if c != "order_id")
    return (
        f"{insert_sql()}\n"
        f"ON CONFLICT (order_id) DO UPDATE SET {sets}, mirrored_at = now()"
    )


def parse_rows(records: Sequence[Any]) -> List[tuple]:
    """`(id, manager_comment, updated_at)` records → rows in `UTM_COLUMNS` order.

    Pure and synchronous, so it runs in a thread: 0.39 s for 32,656 comments,
    measured, is long enough to matter on the event loop that serves the
    dashboard. The last element is `updated_at` itself, possibly None; the SQL
    turns None into the transaction's clock.
    """
    return [(r[0], *utm_columns(r[1]), r[2]) for r in records]


def refusal(parsed_rows: int, current_rows: int) -> Optional[str]:
    """Why a full parse must not replace the table, or None if it may.

    Pure. The text goes into `meta.mirror_state.last_error`, whose first 300
    characters `mirror_failing` quotes, so both counts and the lever come first.
    """
    # `current_rows > 0` is implied by the inequality and stated anyway: a
    # first parse into an empty table has nothing to protect.
    if current_rows > 0 and parsed_rows * 100 < current_rows * FULL_PARSE_FLOOR_PCT:
        return (
            f"refused: a full UTM parse of bronze.orders yields {parsed_rows} "
            f"rows against the {current_rows} in {UTM_TABLE}, under "
            f"{FULL_PARSE_FLOOR_PCT}%; nothing replaced. → check bronze.orders "
            f"is whole; a shrink that is meant runs with force=True"
        )
    return None


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


def lock_timeout_setting(seconds: float) -> str:
    """`seconds` as a `lock_timeout` value Postgres cannot read as "no limit".

    Milliseconds, and never under one: Postgres reads `0` as no timeout at all,
    so whole seconds would turn any bound under a second — a test's, or a
    tuned one — into an unbounded wait.
    """
    return f"{max(1, int(seconds * 1000))}ms"


async def _lock(conn) -> None:
    """Bound every lock wait in this transaction, then take the parse's own.

    `SET LOCAL`, so the bound ends with the transaction and never leaks into a
    pooled connection's next user.
    """
    await conn.execute(f"SET LOCAL lock_timeout = '{lock_timeout_setting(PG_LOCK_WAIT_S)}'")
    await conn.execute("SELECT pg_advisory_xact_lock($1)", ADVISORY_LOCK_KEY)


async def parse_incremental(pool=None) -> Dict[str, Any]:
    """Parse what is new or changed in `bronze.orders` into the table. Raises.

    One transaction: the advisory lock, the selection, the parse (in a thread),
    the upsert and the OK watermark. Reading after the lock is what makes a
    concurrent full parse harmless — this run sees what that one committed and
    never overwrites it with a verdict read before it.

    The caller holds `PG_LAYER_LOCK`, or needs no ordering against another
    in-process writer of this table; see the module docstring.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK, _record_failure

    started = time.monotonic()
    try:
        # Both inside the try, `ship_order_utm`'s reason: `require_revision`
        # raising is `web` deployed ahead of `migrate`, which is exactly the
        # failure worth naming in the watermark.
        pool = pool or await get_pool()
        await require_revision()

        async with pool.acquire() as conn:
            async with conn.transaction():
                await _lock(conn)
                records = await conn.fetch(INCREMENTAL_SELECT_SQL)
                rows = await asyncio.to_thread(parse_rows, records)
                if rows:
                    await _write_chunked(conn, upsert_sql(), rows)
                # The whole table, not this run's delta: the watermark's
                # `last_rows` reads as "how much should be here" everywhere
                # else, and a quiet run recording zero would read as empty.
                total = await conn.fetchval(COUNT_SQL)
                # Zero parsed is a success like any other. See the docstring.
                await conn.execute(_WATERMARK_OK, UTM_TABLE, total)
    except Exception as exc:
        await _record_failure(UTM_TABLE, f"{type(exc).__name__}: {exc}")
        raise

    result = {
        "parsed": len(rows),
        "rows": total,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Order UTM parsed in Postgres (incremental): %s", result)
    return result


async def parse_full(pool=None, *, force: bool = False) -> Dict[str, Any]:
    """Re-parse every commented order and replace the table whole. Raises on a
    fault; refuses a shrink.

    A refusal returns `{"refused": why, "parsed_rows": n, "current_rows": m}`
    rather than raising — `ship_order_utm`'s contract, for its reason: nothing
    is wrong with Postgres, the input is what is wrong, and the table holding
    its previous verdicts is the correct state.

    Where a refusal, or a failure, stays on record: in what this returns or
    raises to its caller, and — for a refusal — in the ERROR log. It is also written into the
    watermark, but `silver.order_utm`'s row there is the incremental's liveness
    stamp, and the next successful incremental resets `failures_since_ok` and
    clears `last_error`. Under `KS_PG_DERIVE=own` that is the next derivation,
    which a reclassify does not trigger, so it can be up to the 60-minute
    heartbeat away: the canary or the 07:30 check may or may not see the
    refusal in between. That is right for the row, which says whether the
    parser is alive, and it means the watermark is not where a turned-away
    reclassify is kept. A raised failure reaches a log only through the
    caller; this module logs refusals, not exceptions.

    Takes `PG_LAYER_LOCK` itself, so it must never be called by code already
    holding it: `asyncio.Lock` is not reentrant, and the call would wait out
    `LOCK_WAIT_S` on itself and then fail.
    """
    from core.pg_landing import _record_failure
    from core.pg_silver import PG_LAYER_LOCK

    try:
        await asyncio.wait_for(PG_LAYER_LOCK.acquire(), timeout=LOCK_WAIT_S)
    except Exception as exc:
        await _record_failure(
            UTM_TABLE,
            f"{type(exc).__name__}: PG_LAYER_LOCK was not free within "
            f"{LOCK_WAIT_S} s; the full UTM parse did not run",
        )
        raise
    try:
        return await _parse_full_locked(pool, force=force)
    finally:
        PG_LAYER_LOCK.release()


async def _parse_full_locked(pool, *, force: bool) -> Dict[str, Any]:
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK, _record_failure

    started = time.monotonic()
    refused: Optional[str] = None
    try:
        pool = pool or await get_pool()
        await require_revision()

        async with pool.acquire() as conn:
            async with conn.transaction():
                await _lock(conn)
                # Counted in the transaction that would replace the table,
                # after the lock, so the number compared is what it replaces.
                current = await conn.fetchval(COUNT_SQL)
                records = await conn.fetch(FULL_SELECT_SQL)
                rows = await asyncio.to_thread(parse_rows, records)
                if not force:
                    refused = refusal(len(rows), current)
                if refused is None:
                    # DELETE, never TRUNCATE: see the module docstring.
                    await conn.execute(f"DELETE FROM {UTM_TABLE}")
                    await _write_chunked(conn, insert_sql(), rows)
                    await conn.execute(_WATERMARK_OK, UTM_TABLE, len(rows))
    except Exception as exc:
        await _record_failure(UTM_TABLE, f"{type(exc).__name__}: {exc}")
        raise

    if refused is not None:
        # After the transaction has closed: the recorder takes a connection of
        # its own, and one asked for while this still held one would wait on
        # itself in a pool of size one.
        await _record_failure(UTM_TABLE, refused)
        logger.error("Order UTM full parse refused; the table keeps its rows: %s", refused)
        return {"refused": refused, "parsed_rows": len(rows), "current_rows": current}

    result = {
        "rows": len(rows),
        "replaced": current,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    if force:
        result["forced"] = True
    logger.info("Order UTM parsed in Postgres (full): %s", result)
    return result


async def parse_incremental_locked(pool=None) -> Dict[str, Any]:
    """`parse_incremental` under `PG_LAYER_LOCK`, waited for at most
    `LOCK_WAIT_S`. Raises.

    For the callers that do not hold the lock already: the derivation's last
    step, which runs after the derivations have let it go, and the two doors
    that only add. A bounded wait for `parse_full`'s reason — two of those
    callers are HTTP handlers, and the ClickHouse shippers hold the same lock
    across network I/O — and a wait that runs out is written into the
    watermark the way `parse_full` writes its own: the parse did not run.
    """
    from core.pg_landing import _record_failure
    from core.pg_silver import PG_LAYER_LOCK

    try:
        await asyncio.wait_for(PG_LAYER_LOCK.acquire(), timeout=LOCK_WAIT_S)
    except Exception as exc:
        await _record_failure(
            UTM_TABLE,
            f"{type(exc).__name__}: PG_LAYER_LOCK was not free within "
            f"{LOCK_WAIT_S} s; the incremental UTM parse did not run",
        )
        raise
    try:
        return await parse_incremental(pool)
    finally:
        PG_LAYER_LOCK.release()


async def reparse_router(store, *, full: bool = False,
                         force: bool = False) -> Dict[str, Any]:
    """Carry a re-parse that went round the tick to Postgres. Never raises.

    Called by the four doors after their DuckDB parse; see THE ROUTER in the
    module docstring. `full` says the door re-parsed everything (it DELETEd
    first), so under `postgres` the table is replaced whole rather than
    topped up; under `duckdb` it changes nothing, because the ship always
    replaces the table whole. `force` is the CLI's flag and nobody else's.

    Under `postgres` the DuckDB `store` is not read: the parse reads
    `bronze.orders`, which the backfills reach through `ship_orders_by_id`
    before they get here.
    """
    if not parses_in_postgres():
        from core.pg_order_utm import ship_after_reparse

        return await ship_after_reparse(store, force=force)

    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}
    try:
        if full:
            return await parse_full(force=force)
        return await parse_incremental_locked()
    except Exception as exc:  # noqa: BLE001 — see the docstring
        # The failure is already in the watermark (both parses record before
        # they raise); this is the line in the log beside it.
        logger.error(
            "Order UTM not parsed in Postgres after a re-parse — /traffic "
            "keeps the previous verdicts until the next derivation: %s",
            exc, exc_info=True,
        )
        return {"error": f"{type(exc).__name__}: {exc}"}
