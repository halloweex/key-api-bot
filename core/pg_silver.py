"""Silver, computed inside Postgres.

The first thing this application asks Postgres to *derive* rather than store.
Everything before it was a copy — a KeyCRM payload written twice, a human's
classification replicated. This runs the projection.

ONE RULE, ONE STATEMENT

The rebuild is one `INSERT … SELECT` from `silver_recompute_ctes(POSTGRES)`
(`core/warehouse_sql.py`): every order projected by `silver_select_sql`, and
`is_new_customer` / `buyer_first_order_date` from each buyer's first order,
computed in the same SELECT. The row-values twin (`pg_warehouse_dq`)
recomputes stored Silver from that very text, so the check and the rebuild
cannot state the rule two ways. Writing a second Postgres-flavoured
projection here is the mistake charter rule 1 exists to prevent, and #101 is
the proof it takes under a day to bite.

Until the review's A23 (2026-11, under the owner's decision O1 (a)) it was two
passes: an INSERT that wrote `is_new_customer = FALSE` everywhere, then a
full-table UPDATE that rewrote every row with a buyer — a second version of
nearly every row, about half of the derivation's WAL. The UPDATE was shaped
for DuckDB's incremental path, which scoped it by buyer; nothing here was ever
incremental.

WHY A FULL REBUILD AND NOT AN INCREMENTAL ONE

~50 000 rows is a fraction of a second in Postgres, and an incremental rebuild
introduces a scope, and a scope is somewhere for a row to be quietly left
behind. The row-values twin would find it, but a design that cannot lose the
row is better than one that reports having lost it.

WHY ONE TRANSACTION

Clear, insert, stamp the watermark, in one transaction: a reader sees the
previous Silver whole until the COMMIT, then the new one whole.

`DELETE`, not `TRUNCATE`, since A23. `TRUNCATE` holds ACCESS EXCLUSIVE to
the COMMIT, so every read of Silver waited out the rebuild, and a rebuild
queued behind one long reader (a dump, a slow report) made every later read
queue behind *it*: the review measured a trivial read waiting 0.55 s during a
plain rebuild and 3.98 s behind a queued `TRUNCATE`. `DELETE` takes ROW
EXCLUSIVE, which no read conflicts with: readers keep the previous Silver
until the COMMIT and never wait. Its price is a dead version of every row for
autovacuum — about 89 rebuilds a day of ~50 000 rows — and that was measured
before it was chosen, on 50 300 synthetic orders with the default autovacuum:
at production's pace, which never rebuilds twice inside the 600-second floor
while autovacuum looks every 60 seconds, each rebuild's vacuum ran within 80
seconds of it, the files settled at 1.87 times a fresh build (14.4 MB, where
the two-pass table, its UPDATE's dead rows included, held 14.0) and stayed
there for 100 rebuilds, and
the last rebuilds were no slower than the first. Run back to back with no
pause, a pace the floor forbids, vacuum cannot keep up: 48.7 times a fresh
build after 100 — the reason the floor and this choice go together. A forced
`POST /api/warehouse/refresh` skips the floor, so a burst of them grows the
files by most of a fresh build each (5 to 6 MB back to back), space the
rebuilds after the next vacuum reuse. (The PR that made the change carries
the rest of the numbers.)

`lock_timeout` (`REBUILD_LOCK_TIMEOUT`, `SET LOCAL`) bounds the wait for the
table's lock. A rebuild that cannot take it in time raises
`LockNotAvailableError`: the derivation records the failure against the
table, journals the run, leaves the rebuild owed, and the next tick tries
again. Without the bound the wait ran to the pool's 30-second
`command_timeout`, and every read of the table queued behind it.

WHAT IT DOES NOT DO

It does not reconcile. Comparing what it wrote with what the rule says is the
row-values twin's job, not this module's, and a rebuild that also reconciled
would be marking its own homework.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# The Silver rule — `silver_recompute_ctes`, `REVENUE_SOURCE_IDS`,
# the return statuses, the manager constants — cannot change inside a process.
# So a rebuild this process committed ran the rule the row-values twin
# (`pg_warehouse_dq`) recomputes with, and a rebuild before it may have run the
# previous process's. Two moments carry that, and neither dates a row:
#
# FIRST_REBUILT_AT is when this process's first Silver rebuild committed, by
# Postgres' clock: the `now()` of that rebuild's transaction, which is the very
# watermark it stamped. None until then. The twin counts a rebuild as covering
# a row only when it began at or after this — so a deploy that changed the rule
# is a rebuild owed rather than a rebuild at fault, and this process's first
# rebuild, faulty or not, is judged at once rather than after its second.
#
# RULE_LOADED_AT is when this module was first imported — lazily, on the first
# rebuild's or the first integrity read's call path, so no earlier than the
# process started. It bounds how long the twin holds rows that only a previous
# process's rebuild had: held for the repair window past it, then paged, so a
# derivation that never runs again cannot hold them for ever.
#
# Found reviewing DN-13: dating every row no earlier than the import put each
# of them in flight after every restart — a standing page read as resolved —
# and, because the import falls on the first rebuild's own call path, made that
# rebuild unable to cover anything.
RULE_LOADED_AT = datetime.now(timezone.utc)
FIRST_REBUILT_AT: Optional[datetime] = None

# One process, several actors over the same Postgres layer: the scheduler's
# rebuild tick (silver → gold), the checks that read the layer in one
# snapshot, and the ClickHouse ship-and-derive. Unserialised, they race in
# three documented ways the review named: a silver rebuild landing between
# the reconcile's two reads turns the flagship engines-verdict into a false
# CRITICAL; a TRUNCATE is not MVCC-safe, so even REPEATABLE READ can compare
# against an emptied table; and two CH shippers interleaving TRUNCATE/EXCHANGE
# can swap an empty staging into place under a fresh OK watermark. (The
# витрина's own rebuild-and-check was one of the actors until the owner's
# decision O6.) One lock, taken by every actor for its whole
# read-derive-write span, removes all three. It is a coordination lock over
# network-bound work — deliberately NOT the DuckDB store lock, whose rule
# ("never held across the network") stays intact.
PG_LAYER_LOCK = asyncio.Lock()

SILVER_TABLE = "silver.orders"

# How long a rebuild of Silver or Gold waits for its table's lock before it
# raises and the run is retried on the next tick. Kept well under the pool's
# 30-second `command_timeout` (`KS_PG_TIMEOUT`, core/pg.py), so the error
# names the lock rather than a bare `TimeoutError`. `core.pg_gold` uses the
# same bound.
REBUILD_LOCK_TIMEOUT = "5s"

# The rebuild's first statement. `DELETE`, not `TRUNCATE`: the module
# docstring says why.
CLEAR_SQL = f"DELETE FROM {SILVER_TABLE}"

# The write order, and the projection's own order. `silver_select_sql` emits
# these columns in exactly this sequence; a mismatch here would be a silent
# column shift rather than an error, so the two are asserted equal in tests.
SILVER_COLUMNS = (
    "id", "source_id", "status_id", "grand_total", "ordered_at",
    "buyer_id", "manager_id", "order_date", "is_return", "sales_type",
    "is_active_source", "source_name", "is_new_customer",
    "buyer_first_order_date", "promocode",
)


def insert_sql() -> str:
    """The rebuild's one write: every order as the Silver rule projects it,
    its first-order columns included, in the projection's column order."""
    from core.sql_dialect import POSTGRES
    from core.warehouse_sql import silver_recompute_ctes

    columns = ", ".join(SILVER_COLUMNS)
    return (
        f"WITH {silver_recompute_ctes(POSTGRES)}\n"
        f"INSERT INTO {SILVER_TABLE} ({columns})\n"
        f"SELECT {columns} FROM recomputed"
    )


async def rebuild_silver(pool=None) -> Dict[str, Any]:
    """Replace `silver.orders` from `bronze.orders`. Raises.

    Unlike the mirror in the sync path, this may fail loudly: the derivation
    records the failure and the previous Silver stays whole, and a rebuild that
    reports success while writing nothing is the worst available outcome.
    """
    global FIRST_REBUILT_AT
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK

    started = time.monotonic()
    pool = pool or await get_pool()
    await require_revision()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL lock_timeout = '{REBUILD_LOCK_TIMEOUT}'")
            await conn.execute(CLEAR_SQL)
            await conn.execute(insert_sql())
            rows = await conn.fetchval(f"SELECT count(*) FROM {SILVER_TABLE}")
            # The same watermark table the mirror uses, so Reconciliation A can
            # tell "not rebuilt yet" from "rebuilt wrong" without a second
            # mechanism to learn.
            await conn.execute(_WATERMARK_OK, SILVER_TABLE, rows)
            # The watermark's own value: `now()` is the transaction's start.
            stamped = await conn.fetchval("SELECT now()")
    # Only after the COMMIT: a rebuild that rolled back ran nothing.
    if FIRST_REBUILT_AT is None:
        FIRST_REBUILT_AT = stamped

    result = {
        "rows": int(rows),
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Silver rebuilt in Postgres: %s", result)
    return result
