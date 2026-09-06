"""Reading the `/inventory` tab from Postgres.

The tab reads eleven views and eight repository methods are their only
consumers. Revision 0014 put the same eleven in `gold`, rendered from the same
body as DuckDB's, so what is left is choosing which engine answers.

WHY POSTGRES AND NOT CLICKHOUSE

Three measured facts, none of them about speed. `app.sku_inventory_status` is
891 rows and `bronze.categories` is 28 — nobody needs a column store for that.
`PERCENTILE_CONT … WITHIN GROUP`, which `v_category_velocity` uses to make the
dead-stock threshold per-category, has no ClickHouse spelling. And neither of
the two stock tables is shipped to ClickHouse at all, so the move would have
begun by replicating them.

WHY THIS FALLS BACK AND `/sms` DOES NOT

`core/pg_sms_read.py` refuses to fall back because after that switch the two
stores diverge, and answering from the stale one would look perfectly
ordinary. Nothing diverges here: this tab writes nothing. Every view is a
projection over tables Postgres receives from DuckDB, so both engines are
reading the same facts and a fault costs the engine rather than the tab. Same
answer as `core/pg_gold_read.py` and `core/ch_cohorts.py`, and for the same
reason — a page that goes dark teaches everyone never to flip the flag again.

WHAT THE READER IS SEEING, AND HOW STALE

`app.sku_inventory_status` is replicated hourly by `replicate_operational`,
and DuckDB refreshes it hourly too, so a Postgres reader can sit up to an hour
further behind. `bronze.categories` moves only on the weekly full sync and
runs up to ~35 hours behind, so category *names* come from a lagging copy.
Neither is introduced here and neither is a defect, but a number called wrong
should be checked against them first.

WHAT THE FLAG DOES NOT COVER

Only the queries. Every computation above them — the NPV decision, the
quadrant matrix, the optimal-stock model, the brand scorecard — stays in its
one implementation. A second copy of that is how the two Silver definitions
diverged in a day.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_INVENTORY"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the inventory queries should ask Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule. A typo in the variable
    deciding which engine answers should stop the read loudly rather than
    quietly serve the other store.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    """Postgres is configured at all. Without a DSN there is nothing to ask,
    and the flag alone must not turn a working tab into an error."""
    return bool(os.getenv("KS_PG_DSN", "").strip())


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one inventory query against Postgres and return plain tuples.

    `sql` arrives with `?` markers, the shared bodies' convention, and is
    renumbered for asyncpg here rather than by every caller.

    The rows come back as tuples because the callers unpack positionally —
    `SELECT *` from two of the views included, which is why the column order
    of the shared body is part of its contract.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(row) for row in rows]


async def fetch_many(
    queries: Sequence[Tuple[str, Sequence[Any]]],
) -> List[List[Tuple]]:
    """Several inventory queries against one pooled connection.

    One connection, because the callers combine several results into one
    answer — the turnover KPIs read revenue, stock, ABC and sell-through and
    divide one by another. DuckDB's side holds its store lock across the same
    group, so running them apart here would give up a consistency the other
    engine still has.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    out: List[List[Tuple]] = []
    async with pool.acquire() as conn:
        for sql, params in queries:
            rows = await conn.fetch(numbered(sql), *params)
            out.append([tuple(row) for row in rows])
    return out
