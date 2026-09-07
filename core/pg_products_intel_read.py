"""Reading `/products` intelligence from Postgres.

Six methods behind three endpoints — basket distribution, brand affinity,
product momentum — and the interesting half is what they read *from*.

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

WHY IT FALLS BACK, AND WHY THAT IS NOT A LICENCE

Nothing diverges — the tab writes nothing and both engines read the same
Silver. But a fallback is also how a broken port hides: `/margin` shipped a
method PostgreSQL could not run and its differential test passed, because the
Postgres leg fell back and the comparison compared DuckDB with itself. Every
two-engine test makes the fallback fatal now, and so does the probe used
before flipping any flag.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_PRODUCTS_INTEL"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the product-intelligence queries should ask Postgres.

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
