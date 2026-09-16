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
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

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
