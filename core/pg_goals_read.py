"""Reading `/goals` from Postgres — the four of its thirteen that can move.

WHAT IS HERE AND WHAT IS NOT

`/goals` is the last tab untouched by the migration, and it splits cleanly:

  * **here** — `get_goals`, `get_smart_goals`, `get_historical_revenue` and
    `get_daily_revenue_for_dates`. They read `revenue_goals` and the revenue
    history, and Postgres has both: `app.revenue_goals` since revision 0017,
    Silver since 0005.
  * **not here** — `get_predictions` reads `revenue_predictions`, and
    `generate_smart_goals` reads `seasonal_indices`, `weekly_patterns` and
    `growth_metrics`. **None of those four tables exists in Postgres at all.**
    They are on the migration's own list of sixteen with no home, and moving
    these two reads is a migration, not a flag.
  * **not here either** — the seven writes. This tab is the only one that
    writes from the interface, and `app.revenue_goals` is an hourly read
    replica: DuckDB is still the writer, so a write routed here would land in
    a copy and be overwritten within the hour.

WHAT THE PORT REMOVES

Two hand-rolled copies of rules Silver already carries as columns. The bodies
read raw `orders` today and re-apply `status_id NOT IN (…)` for returns; Silver
has `is_return`, which prefers KeyCRM's status *group* — the thing that caught
status 20 in July. `get_daily_revenue_for_dates` also carries its own
`source_id IN (1, 2, 4)`, and `get_historical_revenue` **carries none**, so the
two disagreed about which channels count.

Measured before changing anything, on the production backup over 28 days: the
disagreement costs **nothing today** — there are no retail orders outside those
three sources in the window, and `/goals` totals ₴3,209,005.26, which is
`gold_daily_revenue` for the same window to the kopeck. So this is a latent
divergence being closed, not a number being corrected.

The Kyiv date stops being computed too: `silver.orders.order_date` *is* it.

THE TRAP THAT WOULD HAVE SHIPPED

The bodies bound their windows with `CURRENT_DATE`, and the two engines do not
agree on what day that is: the `web` container runs `TZ=Europe/Kyiv` and the
Postgres server answers in UTC, so **between 21:00 and midnight UTC they name
different days** — and every one of these windows would have moved by one on
whichever engine answered. `core.sql_dialect.TODAY_IN_KYIV` is the expression
both engines accept and both mean the same by, and it is what these bodies use
now. Invisible for twenty-one hours out of twenty-four, which is why it is
written down rather than left to a differential test that runs under one
timezone.

WHY THIS FALLS BACK

Nothing diverges: these read derived layers and write nothing. A fault costs
the engine rather than the page — `core/pg_gold_read.py`'s answer.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_GOALS"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the goal reads should ask Postgres.

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
    """Run one goal query against Postgres and return plain tuples.

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
