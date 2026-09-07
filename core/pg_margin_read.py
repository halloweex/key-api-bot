"""Reading `/margin` from Postgres.

The cleanest of the ports. Six methods, three tables, and every one of them is
already there: `silver.order_lines` (revision 0011, a view whose body is the
same text DuckDB uses), `bronze.offer_stocks` and `bronze.categories`. No
migration, no replication, no new hole in `Dialect` — only a choice of engine.

WHY THE COST SIDE IS THE INTERESTING PART

Margin is revenue minus COGS, and COGS comes from `offer_stocks.purchased_price`
joined by SKU. That table reached Postgres as a passenger of the SMS work and
then **froze for seven hours** on 2026-09-06, when `KS_SMS_STORE=postgres` made
`replicate_sms` stand down and took a landing table with it. It ships with
`replicate_operational` now, and the daily comparison covers it — which is what
makes reading the cost side from here safe rather than merely possible.

WHY IT FALLS BACK

Nothing diverges: the tab writes nothing, and both engines read the same Silver
and the same catalogue. A fault costs the engine rather than the page —
`core/pg_inventory_read.py`'s answer, and the opposite of `/sms`.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_MARGIN"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the margin queries should ask Postgres.

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
    """Run one margin query against Postgres and return plain tuples.

    `sql` arrives with `?` markers, the shared bodies' convention, and is
    renumbered for asyncpg here rather than by every caller.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
