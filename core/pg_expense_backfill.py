"""Carrying the expense history across, once.

`mirror_expenses` ships what a sync fetched — the expenses hanging off the
orders in that page — so after the first deploy Postgres holds the last few
days and DuckDB holds 15,020 rows going back to the start. Every row older
than the mirror looks exactly like a lost one, which is why
`reconcile_expenses` is gated on `meta.mirror_state.backfilled_at` and reports
one `mirror_backfill_pending` instead of fifteen thousand CRITICALs.

`core/pg_backfill.py`'s design, at a tenth of the size and for its reasons:

**No cursor.** It asks both stores which ids they hold, ships the difference,
and stops. Interrupt it anywhere and the next run resumes by recomputing —
there is no stored position to be wrong, and nothing to reconcile after a
crash.

**It does not repair a row that exists on both sides and differs.** That is
the reconciliation's business, downstream of a check that can say which rows
disagree. This closes the gap in *history*, which no comparison can do on its
own.

**`backfilled_at` is stamped only on a run that finished with nothing
remaining.** A partial backfill claiming completion would open the row-level
comparison over rows it had not yet shipped — 0007's lesson, and the reason
that column exists rather than a boolean.

`expense_types` needs none of this: it is 27 rows re-shipped whole on every
sync, so its first successful ship *is* its history.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Sequence

logger = logging.getLogger(__name__)

EXPENSES_TABLE = "bronze.expenses"

# Smaller than the orders backfill's 2 000 because the whole table is 15 020
# rows: a chunk that size would make the loop a formality and the progress log
# a single line saying nothing.
DEFAULT_CHUNK_SIZE = 1000


async def _postgres_ids(pool) -> set:
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT id FROM {EXPENSES_TABLE}")
    return {r[0] for r in rows}


def _duckdb_ids(conn) -> set:
    return {r[0] for r in conn.execute("SELECT id FROM expenses").fetchall()}


def _read_chunk(conn, ids: Sequence[int]) -> list:
    from core.landing_rows import EXPENSE_COLUMNS

    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(
        f"SELECT {', '.join(EXPENSE_COLUMNS)} FROM expenses "
        f"WHERE id IN ({placeholders}) ORDER BY id",
        list(ids),
    ).fetchall()
    return [tuple(r) for r in rows]


async def _mark_backfilled(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO meta.mirror_state (table_name, backfilled_at)
            VALUES ($1, now())
            ON CONFLICT (table_name) DO UPDATE SET backfilled_at = now()
            """,
            EXPENSES_TABLE,
        )


async def backfill_expenses(
    store, *, chunk_size: int = DEFAULT_CHUNK_SIZE, pool=None,
) -> Dict[str, Any]:
    """Ship the expenses Postgres is missing. Idempotent, and raises.

    Loud rather than quiet, `rebuild_silver`'s rule: this is a deliberate
    repair somebody asked for, so a failure should reach them rather than be
    logged and forgotten. The hourly mirror is the one that must never raise.

    The DuckDB reads and the Postgres round trips are in separate blocks —
    every reader waits behind the store lock, so awaiting the network while
    holding it would stall the dashboard for the length of the copy.
    """
    from core.landing_rows import EXPENSE_COLUMNS
    from core.pg import get_pool, require_revision
    from core.pg_landing import _statement, _WATERMARK_OK

    started = time.monotonic()
    pool = pool or await get_pool()
    await require_revision()

    have = await _postgres_ids(pool)
    async with store.connection() as conn:
        missing = sorted(_duckdb_ids(conn) - have)

    shipped = 0
    for start in range(0, len(missing), chunk_size):
        ids = missing[start:start + chunk_size]
        async with store.connection() as conn:
            rows = _read_chunk(conn, ids)
        if not rows:
            continue
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    _statement(EXPENSES_TABLE, EXPENSE_COLUMNS), rows,
                )
        shipped += len(rows)
        logger.info(
            "Expense backfill: %d/%d shipped", shipped, len(missing),
        )

    # Recomputed, not assumed: a row that arrived from the mirror while this
    # ran is already there, and a row this failed to ship is still missing.
    # The stamp has to describe the store, not this run's intentions.
    #
    # The Postgres read happens *before* the store block, not inside it. An
    # `await` on the network while holding the store lock stalls every other
    # reader for its duration — the rule this module's own docstring states two
    # paragraphs up, and which the first draft of these three lines broke.
    after = await _postgres_ids(pool)
    async with store.connection() as conn:
        still_missing = len(_duckdb_ids(conn) - after)

    if still_missing == 0:
        await _mark_backfilled(pool)
        async with pool.acquire() as conn:
            await conn.execute(_WATERMARK_OK, EXPENSES_TABLE, shipped)

    result = {
        "missing_at_start": len(missing),
        "shipped": shipped,
        "still_missing": still_missing,
        "complete": still_missing == 0,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Expense backfill: %s", result)
    return result
