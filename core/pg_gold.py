"""Gold, computed inside Postgres.

`core/pg_silver.py` was the first thing this application asked Postgres to
derive. This is the second, and it reads what that one wrote: `silver.orders`
in, `gold.daily_revenue` out, no KeyCRM and no DuckDB anywhere in the path.

ONE SET OF MEASURES, TWO SHAPES

The eight aggregates come from `GOLD_MEASURES` in `core/duckdb_store.py`,
verbatim — the same dict DuckDB renders into `GOLD_REVENUE_SELECT_SQL`. A test
asserts each expression appears in both. That is charter rule 1, and #101 is
the standing proof of how fast a second copy diverges.

The *shape* is where the two part company, and deliberately. DuckDB splits
channels into columns; this carries `source_id` as a dimension, because a
column can only name a channel somebody wrote a column for — ₴266,059.00
across 177 exhibition orders sits in DuckDB's `revenue` and in none of its
three channel columns. Revision 0007 has the numbers.

WHY GROUPING SETS AND NOT A FINER TABLE

Because three of the eight measures are `COUNT(DISTINCT buyer_id)` and distinct
counts do not add. Summing the per-source rows overstates `unique_customers`
in 29 of 2,107 production cells, `new_customers` in 12, `returning_customers`
in 17 — each by one buyer who used two channels the same day. Step 05 closes on
zero discrepancies, so "off by 29 across three years" is not a rounding
question, it is a failure.

`GROUP BY GROUPING SETS` computes both grains in one pass over one snapshot,
so the roll-up is the engine's own distinct count rather than an arithmetic
reconstruction of one. ClickHouse will do this differently and correctly with
`AggregateFunction(uniqExact)`; PostgreSQL has no such type, and this is its
answer.

WHY A FULL REBUILD, AND WHY ONE TRANSACTION

`pg_silver`'s reasons, unchanged and if anything stronger: 5,684 rows is
nothing, an incremental scope is somewhere for a cell to be left behind, and a
partially-written Gold is a number somebody could read. `TRUNCATE` then one
`INSERT`, and nothing outside the transaction sees the gap.

Gold is rebuilt *after* Silver and from the same caller, so it never reads a
Silver that the same tick is about to replace.

WHAT IT DOES NOT DO

It does not touch DuckDB, which keeps computing its own Gold for the whole
parallel period — the owner's decision of 2026-08-23. Comparing the two is
`reconcile_gold`'s job in `core/mirror_reconciliation.py`, not this module's:
a rebuild that also reconciled would be marking its own homework.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)

GOLD_TABLE = "gold.daily_revenue"

# The write order, and the projection's own order. Unlike DuckDB's positional
# INSERT this one names its columns, but the two must still agree — so the
# order lives here once and `insert_sql` uses it for both halves.
GOLD_DIMENSIONS: Tuple[str, ...] = ("date", "sales_type", "source_id")

# Every measure Postgres stores. The six DuckDB-only channel columns are absent
# on purpose: `source_id` is what replaced them.
GOLD_MEASURE_COLUMNS: Tuple[str, ...] = (
    "revenue", "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count", "returns_revenue",
    "avg_order_value",
)

GOLD_COLUMNS: Tuple[str, ...] = GOLD_DIMENSIONS + GOLD_MEASURE_COLUMNS


def select_sql(dialect=None) -> str:
    """Both grains of Gold, in one pass over Silver.

    `GROUPING(source_id)` rather than `source_id IS NULL`: they happen to mean
    the same thing today because `silver.orders.source_id` is NOT NULL, but
    only the first one *says* "this is the roll-up row" — and it keeps saying
    it if the column ever becomes nullable.

    Takes a dialect for the same reason `silver_select_sql` does, and it earns
    it here in a way it does not earn a second implementation: DuckDB speaks
    `GROUPING SETS` too, so rendering this against `silver_orders` lets the
    test suite *execute* the Postgres projection and compare its output to
    DuckDB's own Gold — row for row, without a Postgres to run it on. The
    mapping in `core/mirror_reconciliation.py` is asserted by running it, not
    by reading it.
    """
    from core.duckdb_store import GOLD_MEASURES
    from core.sql_dialect import POSTGRES

    dialect = dialect or POSTGRES
    # Every select item is `<expr> AS <column>`, including `sales_type AS
    # sales_type`, which reads redundant and is not: the list is built from
    # GOLD_COLUMNS, so the SELECT cannot drift out of step with the INSERT that
    # names them, and every item carries the name it lands in.
    dimensions = {
        "date": "order_date",
        "sales_type": "sales_type",
        "source_id":
            "CASE WHEN GROUPING(source_id) = 1 THEN NULL ELSE source_id END",
    }
    items = [f"    {dimensions[c]} AS {c}" for c in GOLD_DIMENSIONS]
    items += [f"    {GOLD_MEASURES[c]} AS {c}" for c in GOLD_MEASURE_COLUMNS]
    return (
        "SELECT\n"
        + ",\n".join(items) + "\n"
        f"FROM {dialect.silver_orders}\n"
        "WHERE order_date IS NOT NULL\n"
        "GROUP BY GROUPING SETS (\n"
        "    (order_date, sales_type, source_id),\n"
        "    (order_date, sales_type)\n"
        ")"
    )


def insert_sql() -> str:
    """The rebuild's one write."""
    return (
        f"INSERT INTO {GOLD_TABLE} ({', '.join(GOLD_COLUMNS)})\n"
        f"{select_sql()}"
    )


async def rebuild_gold(pool=None) -> Dict[str, Any]:
    """Replace `gold.daily_revenue` from `silver.orders`. Raises.

    Loud, for `pg_silver`'s reason: nothing reads this table yet, so there is
    no working system to protect by staying quiet, and a rebuild that reports
    success while writing nothing is the worst available outcome.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK

    started = time.monotonic()
    pool = pool or await get_pool()
    await require_revision()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"TRUNCATE {GOLD_TABLE}")
            await conn.execute(insert_sql())
            rows = await conn.fetchval(f"SELECT count(*) FROM {GOLD_TABLE}")
            cells = await conn.fetchval(
                f"SELECT count(*) FROM {GOLD_TABLE} WHERE source_id IS NULL"
            )
            # Same watermark table as Silver and the mirror, so the comparison
            # can tell "not rebuilt yet" from "rebuilt wrong" without a second
            # mechanism to learn.
            await conn.execute(_WATERMARK_OK, GOLD_TABLE, rows)

    result = {
        "rows": int(rows),
        "cells": int(cells),
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Gold rebuilt in Postgres: %s", result)
    return result
