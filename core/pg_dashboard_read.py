"""Reading the rest of `/dashboard` from Postgres.

NO FALLBACK SINCE STEP 14 (S14-06)

The rest of `/dashboard` reads Postgres. The flag that chose the engine
(`KS_READ_DASHBOARD`) chooses nothing now: the router (`_dashboard_run`) asks
Postgres and nothing else, and a failure — a lost DSN line included — is
refused as a 503 naming `dashboard` (`read_fallback.refuse`), which is what
`KS_READ_FALLBACK=off` made of it since 2026-10-09. The paragraphs below are
the history of the port.

WHY THE DASHBOARD ENDED UP WITH TWO FLAGS

`KS_READ_GOLD` already moved this tab's two Gold primitives — the totals of
`/api/summary` and the daily series of `/api/revenue/trend` — and it has been
`postgres` in production since 2026-08-28. It predates the tab-by-tab
convention and was documented as covering exactly those two.

Widening it to cover these as well would have turned them on **the moment they
deployed**, because the variable was already set. No staged decision, no way
to roll one back without the other. The whole point of a flag per surface was
that turning it on is a thing somebody does; inheriting an on switch is not
that.

WHAT THIS ONE COVERS

The reads that were left behind when `/reports`, `/marketing` and `/margin`
each took their own: the source doughnut, the returns list, the subcategory
breakdown, the three that came off `gold_daily_products` (top products,
product performance, brand analytics) and the customer block. All of them read
Silver, the order-lines view, or Gold — each of which Postgres has had since
revisions 0005, 0011 and 0007.

THE ONE SHAPE DIFFERENCE

The doughnut's unfiltered branch read per-channel columns out of DuckDB's
Gold, and Postgres does not have them: it carries `source_id` as a dimension
instead. `core.sql_dialect.channel_measures` renders that from the same home
`marketing_period_measures` uses, so a fourth channel is added once rather
than twice.

WHY IT FELL BACK, UNTIL S14-06

Nothing diverges — these read the same derived layers and write nothing — so a
fault cost the engine rather than the page. `core/pg_gold_read.py`'s answer,
and the opposite of `core/pg_sms_read.py`, where the two stores genuinely
differ after the switch and answering from the stale one would look perfectly
ordinary.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


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
