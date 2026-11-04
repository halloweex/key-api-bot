"""Reading `/margin` from Postgres.

The cleanest of the ports. Six methods, three tables, and every one of them is
already there: `silver.order_lines` (revision 0011, a view whose body is the
same text DuckDB uses), `bronze.offer_stocks` and `bronze.categories`. No
migration, no replication, no new hole in `Dialect` — only a choice of engine.

NO FALLBACK SINCE STEP 14 (S14-05)

`/margin` reads Postgres. The flag that chose the engine (`KS_READ_MARGIN`)
chooses nothing now: the router asks Postgres and nothing else, and a
failure — a lost DSN line included — is refused as a 503 naming `margin`
(`read_fallback.refuse`), which is what `KS_READ_FALLBACK=off` made of it
since 2026-10-09. The paragraphs below are the history of the port.

WHY THE COST SIDE IS THE INTERESTING PART

Margin is revenue minus COGS, and COGS comes from `offer_stocks.purchased_price`
joined by SKU. That table reached Postgres as a passenger of the SMS work and
then **froze for seven hours** on 2026-09-06, when `KS_SMS_STORE=postgres` made
`replicate_sms` stand down and took a landing table with it. It ships with
`replicate_operational` now, and the daily comparison covers it — which is what
makes reading the cost side from here safe rather than merely possible.

WHY IT FELL BACK, UNTIL S14-05

Nothing diverges: the tab writes nothing, and both engines read the same Silver
and the same catalogue. A fault costs the engine rather than the page —
`core/pg_inventory_read.py`'s answer, and the opposite of `/sms`.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


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
