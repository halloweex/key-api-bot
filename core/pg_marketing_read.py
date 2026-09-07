"""Reading `/marketing` from Postgres.

The page has three components and two of them were already there: the ROI
calculator reads `/api/summary`, which `KS_READ_GOLD` has served from Postgres
since 2026-08-28, and the promocode chart reads `silver.orders`. This flag is
about the third, the monthly report, which needed all of the following before
it could move — and each turned out to be an answer rather than a blocker.

**The channel figures.** `_fetch_month` wants `instagram_revenue` and its five
siblings, and Postgres has no such columns *by design*: it carries `source_id`
as a dimension with `source_id IS NULL` as the roll-up, because a column can
only name a channel somebody wrote a column for — source 5 (Виставка) is
₴266,059.00 of standing proof. So the difference is one of shape, not of names,
and `core.sql_dialect.marketing_period_measures` renders it from one place.
`reconcile_gold` has been comparing those two shapes against each other on
production data every morning, which is what makes this a swap of two things
already known to agree.

**The brand table.** It read `gold_daily_products`, which exists only in DuckDB.
Deriving that Gold here was the obvious move and the wrong size: the line level
reproduces it *to the kopeck* under Gold's own predicate — ₴132,077,453.75 on
both sides, 986 dates, zero disagreeing, pinned by a test — so the query reads
`silver.order_lines` instead, which both engines have.

**The goal.** `revenue_goals` is three rows a human typed, and revision 0017
puts them here as an hourly read replica; the goals API still writes DuckDB.
An hour of lag on a target line drawn over a calendar month is a display lag,
not the security hole the same lag would have been on the user list.

WHY THIS FALLS BACK

Nothing diverges: the tab writes nothing, and every table it reads is one
Postgres receives or derives from what DuckDB holds. A fault costs the engine
rather than the page — `core/pg_inventory_read.py`'s answer, and the opposite
of `/sms`, where after its switch the two stores genuinely differ.

WHY ITS OWN FLAG AND NOT `KS_READ_REPORTS`

The two tabs read different things — `/reports` only Silver, this one Gold and
the goals as well — so they can fail differently, and a rollback should cost
one of them rather than both.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_MARKETING"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the marketing queries should ask Postgres.

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
    """Run one marketing query against Postgres and return plain tuples.

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
