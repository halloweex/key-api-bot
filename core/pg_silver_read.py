"""The Silver-grain answers of `/api/summary` and `/api/revenue/trend`.

NO FALLBACK SINCE STEP 14 (S14-06)

These read Postgres. The flag that chose the engine (`KS_READ_SILVER`)
chooses nothing now: `_pg_silver` and `get_manager_sales_365d` ask Postgres
and nothing else, and a failure — a lost DSN line included — is refused as a
503 naming `dashboard` (or `managers`, for the managers screen), which is what
`KS_READ_FALLBACK=off` made of it since 2026-10-09. The paragraphs below are
the history of the port.

`KS_READ_GOLD` covered exactly two Gold primitives, and said so: "line filters
do not reach Postgres — the flag changes the engine and nothing else". That was
a deliberate scope for step 1 of «Одна бронза», and it left a hole that stage 1's
"16 of 16" could not see, because it counted repository methods and these are
branches *inside* two methods it had already counted.

The hole was every request Gold cannot answer:

- a **line-level** filter — category or brand — selects part of an order, so it
  reads the order-lines level;
- a **promocode**, which Gold has no column for;
- a **source** the dashboard does not ask Gold about — source 5, Виставка, ₴266k.

All of them fell through to DuckDB `silver_orders` / `silver_order_lines`.
While DuckDB still derived Silver that was merely a second engine serving one
endpoint depending on which dropdown was set. Once it stopped, it would have
been the dashboard's headline revenue showing a frozen number the moment
anybody picked a brand.

WHY IT HAD A FLAG OF ITS OWN

`KS_READ_GOLD` was on in production. Widening what it meant would have moved
these requests on deploy, with no step in between to compare the engines on
live data first — the ordering every other port here kept. This one shipped
off, and was turned on together with `KS_READ_GOLD`.

THE MANAGERS SCREEN RODE IT TOO

`GET /api/managers` sums a year of `silver_orders` per manager and sales type,
which is this grain and nothing else, so it took this switch rather than a
flag of its own.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one Silver-grain query against Postgres; plain tuples back.

    `sql` arrives with `?` markers and already rendered for Postgres; it is
    renumbered for asyncpg here rather than by every caller.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
