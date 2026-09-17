"""The Silver-grain answers of `/api/summary` and `/api/revenue/trend`.

`KS_READ_GOLD` covers exactly two Gold primitives, and says so: "line filters
do not reach Postgres — the flag changes the engine and nothing else". That was
a deliberate scope for step 1 of «Одна бронза», and it left a hole that stage 1's
"16 of 16" could not see, because it counted repository methods and these are
branches *inside* two methods it had already counted.

The hole is every request Gold cannot answer:

- a **line-level** filter — category or brand — selects part of an order, so it
  reads the order-lines level;
- a **promocode**, which Gold has no column for;
- a **source** Gold has no column for — source 5, Виставка, ₴266k.

All of them fall through to DuckDB `silver_orders` / `silver_order_lines`. While
DuckDB still derives Silver that is merely a second engine serving one endpoint
depending on which dropdown is set. Once it stops, it is the dashboard's headline
revenue showing a frozen number the moment anybody picks a brand.

WHY A FLAG OF ITS OWN

`KS_READ_GOLD` is on in production. Widening what it means would move these
requests on deploy, with no step in between to compare the engines on live data
first — the ordering every other port here has kept. This one ships off.

**Turn it on together with `KS_READ_GOLD`, or not at all.** One flag per grain
means that with only one set, the same endpoint answers from two engines
depending on whether a filter is present — which is today's state, and the
reason this exists.

THE MANAGERS SCREEN RIDES IT TOO

`GET /api/managers` sums a year of `silver_orders` per manager and sales type,
which is this grain and nothing else, so it takes this switch rather than a
flag of its own — one more variable to flip on the way off DuckDB would buy a
rollback nobody needs for an admin-only screen. Same fallback as the rest:
a failed Postgres read is an ERROR in the log and a DuckDB answer.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_SILVER"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the Silver-grain answers should ask Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule: a typo in the variable that
    decides which engine answers must stop the read, not serve the other store.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    """Postgres is configured at all."""
    return bool(os.getenv("KS_PG_DSN", "").strip())


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
