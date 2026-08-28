"""Silver, computed inside Postgres.

The first thing this application asks Postgres to *derive* rather than store.
Everything before it was a copy — a KeyCRM payload written twice, a human's
classification replicated. This runs the projection.

ONE RULE, RENDERED TWICE

Pass 1 is `silver_select_sql(POSTGRES)` and pass 2 is
`silver_pass2_sql(dialect=POSTGRES)` — the same text DuckDB runs, differing
only in the Kyiv date expression and four table names. Writing a second
Postgres-flavoured projection here is the mistake charter rule 1 exists to
prevent, and #101 is the proof it takes under a day to bite.

WHY A FULL REBUILD AND NOT AN INCREMENTAL ONE

DuckDB rebuilds Silver incrementally because a full pass there is expensive and
because `silver_mode` has never once been `full`. Neither reason transfers.
46,530 rows is a fraction of a second in Postgres, and the whole point of the
parallel period is that the two sides must agree — an incremental rebuild
introduces a scope, and a scope is somewhere for a row to be quietly left
behind. Reconciliation A would find it, but a design that cannot lose the row
is better than one that reports having lost it.

WHY ONE TRANSACTION

`TRUNCATE` then two passes. Between pass 1 and pass 2 every row carries
`is_new_customer = FALSE`, which is not merely incomplete — it is *wrong*, and
it is exactly the shape of a number somebody could read. Nothing outside the
transaction ever observes it.

`TRUNCATE`, not `DELETE`: the table is replaced whole, and it reclaims space
immediately instead of leaving 46,530 dead tuples for autovacuum. It also takes
an ACCESS EXCLUSIVE lock, which costs nothing today because nothing reads this
table yet — worth revisiting at the read switch, where `DELETE` or a staging
swap becomes the right shape.

WHAT IT DOES NOT DO

It does not touch DuckDB, and DuckDB keeps computing its own Silver for the
whole parallel period — the owner's decision of 2026-08-23. Comparing the two
is Reconciliation A's job, not this module's, and a rebuild that also
reconciled would be marking its own homework.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

# One process, several actors over the same Postgres layer: the scheduler's
# rebuild tick (silver → gold → витрина), the витрина's own rebuild-and-check,
# and the ClickHouse ship-and-derive. Unserialised, they race in three
# documented ways the review named: a silver rebuild landing between the
# reconcile's two reads turns the flagship engines-verdict into a false
# CRITICAL; a TRUNCATE is not MVCC-safe, so even REPEATABLE READ sees the
# витрина check compare against an emptied Silver; and two CH shippers
# interleaving TRUNCATE/EXCHANGE can swap an empty staging into place under a
# fresh OK watermark. One lock, taken by every actor for its whole
# read-derive-write span, removes all three. It is a coordination lock over
# network-bound work — deliberately NOT the DuckDB store lock, whose rule
# ("never held across the network") stays intact.
PG_LAYER_LOCK = asyncio.Lock()

SILVER_TABLE = "silver.orders"

# The write order, and the projection's own order. `silver_select_sql` emits
# these columns in exactly this sequence; a mismatch here would be a silent
# column shift rather than an error, so the two are asserted equal in tests.
SILVER_COLUMNS = (
    "id", "source_id", "status_id", "grand_total", "ordered_at",
    "buyer_id", "manager_id", "order_date", "is_return", "sales_type",
    "is_active_source", "source_name", "is_new_customer",
    "buyer_first_order_date", "promocode",
)


def pass1_sql() -> str:
    """Every order projected into Silver, in the projection's column order."""
    from core.duckdb_store import silver_select_sql
    from core.sql_dialect import POSTGRES

    return (
        f"INSERT INTO {SILVER_TABLE} ({', '.join(SILVER_COLUMNS)})\n"
        f"SELECT {silver_select_sql(POSTGRES)}\n"
        f"FROM {POSTGRES.orders} o"
    )


def pass2_sql() -> str:
    """`is_new_customer`, recomputed from each buyer's first order."""
    from core.duckdb_store import silver_pass2_sql
    from core.sql_dialect import POSTGRES

    return silver_pass2_sql(dialect=POSTGRES)


async def rebuild_silver(pool=None) -> Dict[str, Any]:
    """Replace `silver.orders` from `bronze.orders`. Raises.

    Unlike the mirror in the sync path, this may fail loudly: nothing reads the
    table yet, so there is no working system to protect by staying quiet, and a
    rebuild that reports success while writing nothing is the worst available
    outcome.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK

    started = time.monotonic()
    pool = pool or await get_pool()
    await require_revision()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"TRUNCATE {SILVER_TABLE}")
            await conn.execute(pass1_sql())
            await conn.execute(pass2_sql())
            rows = await conn.fetchval(f"SELECT count(*) FROM {SILVER_TABLE}")
            # The same watermark table the mirror uses, so Reconciliation A can
            # tell "not rebuilt yet" from "rebuilt wrong" without a second
            # mechanism to learn.
            await conn.execute(_WATERMARK_OK, SILVER_TABLE, rows)

    result = {
        "rows": int(rows),
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Silver rebuilt in Postgres: %s", result)
    return result
