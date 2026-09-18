"""The Postgres derivation's signal and journal — primitives, no callers yet.

Chain 2, step 2; revision 0033 says why the tables exist and why they are in
`meta`. This module is the only code that touches them.

THE ONE RULE: A MARK CAN NEVER COST THE ROWS IT MARKS

The marks will be raised inside the Postgres landing writers' transactions, and
`write_orders` captures `app.order_versions` in that same transaction — the
archive nothing can rebuild. A mark that raised there would roll back the order,
its line items and the version together, and `mirror_orders` swallows the error,
so the transition would be gone for good. The chain-2 review found exactly that
in the first design.

So `mark` runs in a savepoint with a one-second lock timeout, and any error is
logged, recorded against `meta.derivation_signal` in `meta.mirror_state`, and
swallowed: the caller's transaction commits. A lost mark costs a rebuild that
arrives with the next mark or the hourly heartbeat. A lost version costs history.
That is the trade, and it is made here, once, rather than at every call site.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

# ─── Who derives: DuckDB's tick (piggyback) or Postgres' own signal (own) ─────
#
# Read ONCE, when the scheduler starts, and cached. Never parsed on the write
# path: the marks ride inside `write_orders`' transaction, and a typo evaluated
# there must not be able to reach the archive of order versions. An unknown
# value therefore does not raise the way `KS_BOT_STORE` does — it falls back to
# `piggyback`, today's behaviour, logs at ERROR, publishes the error in
# `/api/health` under `derivation`, and the canary pages on it. Refusing to
# start would take the dashboard down over a derivation setting; falling back
# silently would let an operator believe a soak was running that was not.
ENV = "KS_PG_DERIVE"
PIGGYBACK = "piggyback"
OWN = "own"
_VALID = (PIGGYBACK, OWN)

_mode: Optional[str] = None
_mode_error: Optional[str] = None


def configure_mode() -> str:
    """Read `KS_PG_DERIVE` once. Called by the scheduler at start."""
    global _mode, _mode_error
    value = os.getenv(ENV, PIGGYBACK).strip().lower() or PIGGYBACK
    if value in _VALID:
        _mode, _mode_error = value, None
    else:
        _mode = PIGGYBACK
        _mode_error = (f"{ENV}={value!r} is not one of {_VALID}; "
                       f"falling back to {PIGGYBACK!r}")
        logger.error(_mode_error)
    return _mode


def mode() -> str:
    """The configured mode; `piggyback` until `configure_mode` has run."""
    return _mode or PIGGYBACK


def mode_error() -> Optional[str]:
    return _mode_error


def owns() -> bool:
    """Postgres derives on its own signal. Reads the cache, never the env."""
    return _mode == OWN


# ─── Signal reads that failed, in a row — this process only (DN-05a) ─────────
#
# A tick that cannot read `meta.derivation_signal` returns before
# `_derive_pg_layers`, so until DN-05a it wrote no journal row, no alert and no
# watermark failure: the only thing that ever noticed was the derived tables'
# 90-minute age, an hour and a half after the fact. The realistic trigger is a
# deploy window — `web` running ahead of `migrate`, `require_revision` raising
# SchemaVersionError every minute — and a Postgres that has gone away.
#
# In-process, not in Postgres, because the one thing known about this failure
# is that Postgres could not be asked. A restart forgets the count, and that is
# harmless: the first tick of a process derives whatever the signal says, so a
# fault that outlives the restart counts up again from its first tick.
#
# Five ticks is five minutes at the job's one-minute interval: past a pool
# reconnect or a migration that is simply finishing, and still eighty-five
# minutes before the age would have said anything.
#
# That arithmetic holds for a read that fails. A read that neither answers nor
# fails — a Postgres frozen rather than gone, where the kernel still completes
# the handshake — used to hold the tick for as long as the freeze lasted:
# asyncpg times the query out, sends a cancel request and waits, in the pool's
# release, for a server that never answers it, while `max_instances=1` skips
# every tick behind it. Measured on a paused container: the count stayed at
# zero. So a tick gives the read `signal_read_bound_s()` and counts it as
# unreadable past that. Such a tick occupies the job for the whole bound and
# the tick landing meanwhile is skipped, so a freeze pages in about ten minutes
# instead of five — still eighty before the age.
SIGNAL_UNREADABLE_TICKS = 5

_signal_failures = 0


def signal_read_bound_s() -> float:
    """How long one tick may spend reading the signal: two of the pool's
    command timeouts (`KS_PG_TIMEOUT`, 30 s), the revision check's query and
    the signal's own. A healthy read takes milliseconds; one that takes a
    minute is not a signal anyone should act on."""
    return 2 * float(os.getenv("KS_PG_TIMEOUT", "30"))


def note_signal_unreadable() -> int:
    """Count one more tick that could not read the signal; the new count."""
    global _signal_failures
    _signal_failures += 1
    return _signal_failures


def note_signal_read() -> int:
    """The signal was read: reset the count and return what it had reached."""
    global _signal_failures
    previous, _signal_failures = _signal_failures, 0
    return previous


def signal_unreadable_ticks() -> int:
    """Consecutive ticks of this process that could not read the signal."""
    return _signal_failures


# Every function that writes a table the derivation reads, and so must raise
# the signal in its own transaction. Silver reads `bronze.orders` and the
# manager classification; the customer profile reads `bronze.buyers`. A test
# walks the code for writers of those tables and fails on one not listed here,
# and fails on a listed one that does not call `mark_if_owned`.
MARK_SITES: Dict[str, str] = {
    "core.pg_landing": "write_orders",
    "core.pg_replication": "write_managers",
    "core.pg_buyers": "_write",
}
SOURCE_TABLES = ("bronze.orders", "bronze.managers",
                 "app.manager_classifications", "bronze.buyers")

# Rebuild at least this often with nothing owed, so a quiet night still moves
# the watermarks the canary judges. The canary's limit is this, plus the floor,
# plus a tick and some grace.
HEARTBEAT = timedelta(minutes=60)
DERIVED_TABLES = ("silver.orders", "gold.daily_revenue")
DERIVED_MAX_AGE_S = 90 * 60

LAYER = "warehouse"
SIGNAL_TABLE = "meta.derivation_signal"

# The journal's bound, as a count (revision 0033). At the ten-minute floor that
# is ~140 days of runs; at a run a minute, two weeks. Either is far more than
# the forensics this replaces ever needed at once.
RUNS_KEPT = 20_000

# Settle for a lost mark rather than queue the landing behind a lock — a
# migration's ACCESS EXCLUSIVE, say — until the pool's 30 s command timeout.
MARK_LOCK_TIMEOUT = "1s"

_MARK = """
    UPDATE meta.derivation_signal
       SET requested = requested + 1, requested_at = now()
     WHERE layer = $1
"""


async def mark_if_owned(conn, layer: str = LAYER) -> None:
    """The call every writer in `MARK_SITES` makes, last in its transaction."""
    if owns():
        await mark(conn, layer)


async def mark(conn, layer: str = LAYER) -> bool:
    """Raise the signal inside the caller's transaction. Never raises.

    True when the increment is part of the caller's transaction, False when it
    was dropped. The lock timeout is set for the savepoint only: it is put back
    before the savepoint is released, and a savepoint that rolls back takes its
    settings with it.
    """
    try:
        async with conn.transaction():
            previous = await conn.fetchval("SELECT current_setting('lock_timeout')")
            await conn.execute(
                "SELECT set_config('lock_timeout', $1, true)", MARK_LOCK_TIMEOUT)
            status = await conn.execute(_MARK, layer)
            await conn.execute(
                "SELECT set_config('lock_timeout', $1, true)", previous)
        if status.endswith(" 0"):
            raise LookupError(f"{SIGNAL_TABLE} has no row for layer {layer!r}")
        return True
    except Exception as exc:  # noqa: BLE001 — see the module docstring
        logger.error("derivation mark dropped for %s: %s: %s",
                     layer, type(exc).__name__, exc)
        try:
            from core.pg_landing import _record_failure

            await _record_failure(SIGNAL_TABLE, f"{type(exc).__name__}: {exc}")
        except Exception:  # noqa: BLE001 — best effort, and already logged
            pass
        return False


async def read_owed(conn, layer: str = LAYER) -> Tuple[int, int, Optional[datetime]]:
    """`(requested, built, built_at)` as the caller's snapshot sees them.

    Read inside the transaction the derivation rebuilds from, so `requested` is
    exactly the set of marks whose rows that snapshot contains.
    """
    row = await conn.fetchrow(
        "SELECT requested, built, built_at FROM meta.derivation_signal WHERE layer = $1",
        layer)
    if row is None:
        raise LookupError(f"{SIGNAL_TABLE} has no row for layer {layer!r}")
    return row["requested"], row["built"], row["built_at"]


async def complete(conn, seen: int, layer: str = LAYER) -> None:
    """Record that everything up to `seen` is built. Never moves `built` back,
    and never past `requested` — a mark the snapshot did not see stays owed."""
    await conn.execute(
        """
        UPDATE meta.derivation_signal
           SET built = GREATEST(built, LEAST($1, requested)), built_at = now()
         WHERE layer = $2
        """,
        seen, layer)


# ─── A dropped mark, once a rebuild has covered it ───────────────────────────
#
# `mark` records a drop against SIGNAL_TABLE in `meta.mirror_state`, and until
# DN-05b nothing ever wrote that row again: a mark that succeeds touches the
# signal, not the watermark, and `_record_failure` only counts up. So one lock
# timeout left `failures_since_ok` above zero for good, and a watchdog reading
# it would page for ever on a loss the next rebuild had repaired within the hour.
#
# The margin is for the writers outside `_heavy_job_lock`; see
# `heal_dropped_marks`. Thirty seconds is a round trip with room to spare, and
# with room for the web container's clock and Postgres' to disagree — `started`
# is read from the one and `last_attempted_at` written by the other.
DROPPED_MARK_MARGIN = timedelta(seconds=30)

_HEAL = """
    UPDATE meta.mirror_state
       SET failures_since_ok = 0,
           last_error        = NULL,
           last_attempted_at = now(),
           last_ok_at        = now()
     WHERE table_name = $1
       AND failures_since_ok > 0
       AND last_attempted_at < $2::timestamptz - $3::interval
"""


async def heal_dropped_marks(conn, started: datetime) -> bool:
    """Clear the drops a validated rebuild that began at `started` covered.

    Never raises: the rebuild has already succeeded and settled its debt, and a
    heal that failed costs one stale count the next validated run clears. True
    when a count was cleared.

    WHY A DROP RECORDED BEFORE `started` IS COVERED. `mark` records the drop on a
    second connection while the writer's transaction is still open, and the
    writer commits one statement later — so the rows a dropped mark stood for
    become visible just *after* `last_attempted_at`, not at it. Silver, Gold and
    the profile each read bronze in a transaction opened after `started`, so
    they see every writer that committed before `started`:

    - the writers that hold `_heavy_job_lock` across their write — the sync
      jobs and the orders backfill's chunks, which carry nearly every order —
      committed before the derivation could take that lock, and it takes the
      lock before it stamps `started`, so they are covered whatever the clocks
      say;
    - the rest — the retail-status endpoint among them, which does not hold
      the lock — commit a round trip after the drop is recorded, and
      `DROPPED_MARK_MARGIN` is that round trip with room to spare.

    A drop inside the margin, or recorded while this rebuild ran, is left for
    the next validated run. Only a row that owes something is rewritten, so
    `last_ok_at` keeps meaning "when the drops were last covered" rather than
    "when the last rebuild ran". Both stamps move together, because the table's
    constraint forbids a success after the last attempt, and because equal
    stamps are what that table has always meant by healthy.
    """
    try:
        status = await conn.execute(_HEAL, SIGNAL_TABLE, started, DROPPED_MARK_MARGIN)
    except Exception as exc:  # noqa: BLE001 — see the docstring
        logger.warning("dropped derivation marks not healed: %s: %s",
                       type(exc).__name__, exc)
        return False
    healed = not status.endswith(" 0")
    if healed:
        logger.info("dropped derivation marks healed by the rebuild started at %s",
                    started.isoformat())
    return healed


# ─── The run's own tail, as a row in meta.mirror_state (DN-05a) ─────────────
#
# Silver, Gold and the profile each stamp their watermark in the transaction
# that rebuilds them, so a failure there is recorded against the table that
# raised and the next rebuild clears it. The tail of a run — validate, settle
# the debt, write the journal row — has no table of its own to stamp, and until
# DN-05a a failure there was recorded nowhere but the journal it may have been
# unable to write. It is recorded against the journal: the verdict lives in
# `meta.derivation_runs.validation`, so a tail that failed is a run that could
# not say what it built.
#
# Cleared only by a run whose tail completed, and only when it owes something:
# a derivation that has never failed there has no row, like the signal's. A
# verdict of "not passed" is not a failure of the tail — it is recorded in the
# journal and alerted as `warehouse_pg:validation_failed`.
#
# Nothing reads this row yet, nor the profile's — a deliberate follow-up, and
# `BackgroundScheduler._derive_pg_layers` says why.
RUNS_TABLE = "meta.derivation_runs"

_TAIL_OK = """
    UPDATE meta.mirror_state
       SET failures_since_ok = 0,
           last_error        = NULL,
           last_attempted_at = now(),
           last_ok_at        = now()
     WHERE table_name = $1
       AND failures_since_ok > 0
"""


async def clear_tail_failures(conn) -> bool:
    """Reset the tail's count after a run that journaled itself. Never raises:
    the run has already succeeded, and a clear that failed leaves a count the
    next completed run clears. True when a count was cleared."""
    try:
        status = await conn.execute(_TAIL_OK, RUNS_TABLE)
    except Exception as exc:  # noqa: BLE001 — see the docstring
        logger.warning("derivation tail failures not cleared: %s: %s",
                       type(exc).__name__, exc)
        return False
    return not status.endswith(" 0")


async def record_run(
    conn,
    *,
    trigger: str,
    started_at: datetime,
    ended_at: Optional[datetime] = None,
    requested_seen: Optional[int] = None,
    counts: Optional[Mapping[str, Optional[int]]] = None,
    validation_passed: Optional[bool] = None,
    validation: Optional[Mapping[str, Any]] = None,
    error: Optional[str] = None,
    layer: str = LAYER,
    keep: int = RUNS_KEPT,
) -> int:
    """One journal row, and the bound applied in the same transaction."""
    counts = dict(counts or {})
    async with conn.transaction():
        run_id = await conn.fetchval(
            """
            INSERT INTO meta.derivation_runs
                   (layer, trigger, started_at, ended_at, requested_seen,
                    silver_rows, gold_rows, gold_cells, profile_rows,
                    validation_passed, validation, error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
            RETURNING id
            """,
            layer, trigger, started_at, ended_at, requested_seen,
            counts.get("silver_rows"), counts.get("gold_rows"),
            counts.get("gold_cells"), counts.get("profile_rows"),
            validation_passed,
            None if validation is None else json.dumps(validation, default=str),
            None if error is None else error[:4000],
        )
        await conn.execute(
            "DELETE FROM meta.derivation_runs WHERE id <= $1", run_id - keep)
    return run_id


# ─── Validation: DuckDB's refresh checks, ported to the Postgres derivation ──
#
# `refresh_warehouse_layers` validates every DuckDB tick: Bronze→Silver row
# count, Silver→Gold revenue checksum, the cell guard, and — reported apart —
# the sales_type partition. When DuckDB stops deriving those detectors go with
# it, so they run here, after the rebuild, from one REPEATABLE READ snapshot.
# Plus the roll-up check, which only Postgres' two-grain Gold needs.
#
# The row count is exact only because every orders writer holds
# `_heavy_job_lock`, which the derivation job holds too. A writer outside it
# would read here as a mismatch — loud, not silent — and that is the dependency
# stated rather than hidden.
#
# No retry ladder, unlike DuckDB's: its ladder exists for incremental-scope
# faults, and this rebuild is whole every time. A failure is recorded and
# alerted; the next owed rebuild is the retry.

_ADDITIVE = ("revenue", "orders_count", "returns_count", "returns_revenue")


async def validate(conn) -> Dict[str, Any]:
    """Checks over the derived layers. `passed` excludes the partition, for
    DuckDB's reason: no rebuild can repair a sales_type the code does not know."""
    from core.duckdb_constants import KNOWN_SALES_TYPES

    known = list(KNOWN_SALES_TYPES)
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        totals = await conn.fetchrow(
            """
            SELECT
                (SELECT count(*) FROM bronze.orders) AS bronze_orders,
                (SELECT count(*) FROM silver.orders) AS silver_rows,
                (SELECT COALESCE(SUM(grand_total), 0) FROM silver.orders
                  WHERE NOT is_return AND is_active_source) AS silver_revenue,
                (SELECT COALESCE(SUM(revenue), 0) FROM gold.daily_revenue
                  WHERE source_id IS NULL) AS gold_revenue,
                (SELECT COALESCE(SUM(revenue), 0) FROM gold.daily_revenue
                  WHERE source_id IS NULL AND sales_type = ANY($1::text[]))
                    AS gold_revenue_known
            """,
            known)
        missing_cells = await conn.fetchval(
            """
            SELECT count(*) FROM (
                SELECT DISTINCT order_date, sales_type FROM silver.orders
                EXCEPT
                SELECT date, sales_type FROM gold.daily_revenue WHERE source_id IS NULL
            ) m
            """)
        extra_cells = await conn.fetchval(
            """
            SELECT count(*) FROM (
                SELECT date, sales_type FROM gold.daily_revenue WHERE source_id IS NULL
                EXCEPT
                SELECT DISTINCT order_date, sales_type FROM silver.orders
            ) e
            """)
        differ = " OR ".join(
            f"COALESCE(SUM({c}) FILTER (WHERE source_id IS NOT NULL), 0)"
            f" <> COALESCE(SUM({c}) FILTER (WHERE source_id IS NULL), 0)"
            for c in _ADDITIVE)
        rollup_mismatch = await conn.fetchval(
            f"""
            SELECT count(*) FROM (
                SELECT date, sales_type FROM gold.daily_revenue
                GROUP BY date, sales_type
                HAVING bool_or(source_id IS NULL) AND ({differ})
            ) r
            """)
        unknown: List[Tuple[Optional[str], float]] = []
        silver_revenue = totals["silver_revenue"]
        partition_exhaustive = abs(silver_revenue - totals["gold_revenue_known"]) < 0.01
        if not partition_exhaustive:
            unknown = [
                (r["sales_type"], float(r["revenue"]))
                for r in await conn.fetch(
                    """
                    SELECT sales_type, COALESCE(SUM(revenue), 0) AS revenue
                    FROM gold.daily_revenue
                    WHERE source_id IS NULL
                      AND (sales_type IS NULL OR NOT sales_type = ANY($1::text[]))
                    GROUP BY sales_type ORDER BY revenue DESC
                    """,
                    known)
            ]

    row_count_match = totals["bronze_orders"] == totals["silver_rows"]
    checksum_match = abs(silver_revenue - totals["gold_revenue"]) < 0.01
    cells_match = missing_cells == 0 and extra_cells == 0
    rollup_match = rollup_mismatch == 0
    return {
        "passed": row_count_match and checksum_match and cells_match and rollup_match,
        "row_count_match": row_count_match,
        "checksum_match": checksum_match,
        "cells_match": cells_match,
        "rollup_match": rollup_match,
        "partition_exhaustive": partition_exhaustive,
        "bronze_orders": int(totals["bronze_orders"]),
        "silver_rows": int(totals["silver_rows"]),
        "silver_revenue": float(silver_revenue),
        "gold_revenue": float(totals["gold_revenue"]),
        "gold_revenue_known": float(totals["gold_revenue_known"]),
        "missing_cells": int(missing_cells),
        "extra_cells": int(extra_cells),
        "rollup_mismatch_cells": int(rollup_mismatch),
        "unknown_sales_types": [
            {"sales_type": t, "revenue": r} for t, r in unknown],
    }


async def last_attempt_at(conn, layer: str = LAYER) -> Optional[datetime]:
    return await conn.fetchval(
        "SELECT max(started_at) FROM meta.derivation_runs WHERE layer = $1", layer)


def due(
    *, requested: int, built: int, built_at: Optional[datetime],
    last_attempt: Optional[datetime], now: datetime, floor: timedelta,
    first_tick: bool, force: bool = False,
) -> Tuple[bool, str]:
    """Whether to derive now, and why. Pure, so the schedule is testable.

    The floor is measured against the last attempt recorded in Postgres, not a
    process clock: a deploy must neither lose an owed rebuild nor reset the
    floor. `force` is the admin lever and skips the floor.
    """
    if force:
        return True, "manual"
    owed = requested > built
    heartbeat = built_at is None or now - built_at >= HEARTBEAT
    if not (owed or first_tick or heartbeat):
        return False, "nothing owed"
    if last_attempt is not None and now - last_attempt < floor:
        return False, "floor"
    if owed:
        return True, "signal"
    return True, "first_tick" if first_tick else "heartbeat"
