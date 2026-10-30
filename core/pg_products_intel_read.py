"""Reading `/products` intelligence from Postgres.

Six methods behind three endpoints — basket distribution, brand affinity,
product momentum — and the interesting half is what they read *from*.

NO FALLBACK SINCE STEP 14 (S14-05)

The product-intelligence reads go to Postgres. The flag that chose the engine
(`KS_READ_PRODUCTS_INTEL`) chooses nothing now: the router asks Postgres and
nothing else, and a failure — a lost DSN line included — is refused as a 503
naming `products_intel` (`read_fallback.refuse`), which is what
`KS_READ_FALLBACK=off` made of it since 2026-10-09. The paragraphs below are
the history of the port.

FIVE OF THE SIX WERE ALREADY PORTABLE

They read `silver.order_lines` and `bronze.products`, both in Postgres, and
their SQL uses `any_value`, `GROUP BY` on an output alias and a self-join on
`<`. Each was checked against the running server rather than assumed —
`any_value` in particular exists only from **PostgreSQL 16**, so this tab is
the first thing here to carry a minimum server version. Production is 17.2.

THE SIXTH READ A GOLD THAT ONLY DUCKDB HAS

`get_product_momentum` summed `product_revenue` and `quantity_sold` out of
`gold_daily_products`. That table is DuckDB-only and deliberately so; the line
level reproduces it to the kopeck under Gold's own predicate, which
`core/duckdb_store.py` records and `/marketing` has already relied on.

Easier here than there: momentum never touches `order_count`, so it needs no
nested grain — only the two sums, under `NOT is_return AND is_active_source`,
grouped the way Gold groups. `product_name` stays in the grouping key because
Gold's rows carry the name *as sold*, and a product sold under two names is
two rows there; dropping it would quietly merge them.

WHY IT FELL BACK UNTIL S14-05, AND WHY THAT WAS NOT A LICENCE

Nothing diverges — the tab writes nothing and both engines read the same
Silver. But a fallback is also how a broken port hides: `/margin` shipped a
method PostgreSQL could not run and its differential test passed, because the
Postgres leg fell back and the comparison compared DuckDB with itself. Every
two-engine test makes the fallback fatal now, and so does the probe used
before flipping any flag.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one query against Postgres and return plain tuples.

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
