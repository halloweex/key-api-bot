"""Reading the rest of `/dashboard` from Postgres.

WHY THE DASHBOARD ENDS UP WITH TWO FLAGS

`KS_READ_GOLD` already moves this tab's two Gold primitives — the totals of
`/api/summary` and the daily series of `/api/revenue/trend` — and it has been
`postgres` in production since 2026-08-28. It predates the tab-by-tab
convention and is documented as covering exactly those two.

Widening it to cover these as well would have turned them on **the moment they
deployed**, because the variable is already set. No staged decision, no way to
roll one back without the other. The whole point of a flag per surface is that
turning it on is a thing somebody does; inheriting an on switch is not that.

WHAT THIS ONE COVERS

The reads that were left behind when `/reports`, `/marketing` and `/margin`
each took their own: the source doughnut, the returns list, and the
subcategory breakdown. All three read Silver, the order-lines view, or Gold —
each of which Postgres has had since revisions 0005, 0011 and 0007.

The three that are *not* here read `gold_daily_products`, which exists only in
DuckDB. They move by being rewritten onto the line level, the way the same
three tabs above already rewrote theirs, and that is its own change.

THE ONE SHAPE DIFFERENCE

The doughnut's unfiltered branch reads per-channel columns out of Gold, and
Postgres does not have them: it carries `source_id` as a dimension instead.
`core.sql_dialect.channel_measures` renders that difference from the same home
`marketing_period_measures` uses, so a fourth channel is added once rather than
twice.

WHY THIS FALLS BACK

Nothing diverges — these read the same derived layers and write nothing — so a
fault costs the engine rather than the page. `core/pg_gold_read.py`'s answer,
and the opposite of `core/pg_sms_read.py`, where the two stores genuinely
differ after the switch and answering from the stale one would look perfectly
ordinary.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_DASHBOARD"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the dashboard's remaining reads should ask Postgres.

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
    """Run one dashboard query against Postgres and return plain tuples.

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
