"""Running the audience query against Postgres.

`core/sql_dialect.sms_segments_select` renders one body for either engine; this
executes the Postgres rendering and hands back **DuckDB's row shape**, so
everything above the query — the level summaries, the funnel, the customer
dicts, the arm assignment — stays in its single implementation. A second copy
of that logic is how the two Silver definitions diverged in a day.

ONE FLAG, NOT TWO, AND THAT IS THE DECISION WORTH ARGUING

Step 1 gave Gold its own read switch (`KS_READ_GOLD`) separate from any writer,
because Gold is derived: there is no writer to move, and a read can be flipped
back with no consequence.

`/sms` is not that. It is operational, and the store that answers "who is in
this audience" must be the same store that owns the opt-outs, because the
opt-out list is *written* here. While DuckDB is the writer, Postgres holds an
hourly copy — so reading the audience from Postgres with DuckDB still writing
would mean an opt-out recorded fifty minutes ago is invisible, and somebody who
asked not to be messaged is messaged. That is not a stale number on a
dashboard; it is a promise broken to a person.

So reads follow `KS_SMS_STORE` — the same variable that moves the writer, the
same arrangement `KS_BOT_STORE` has for the bot's state. There is no
half-switched state to be in.

AND NO FALLBACK, DELIBERATELY

`core/pg_gold_read.py` falls back to DuckDB on error, because a dashboard that
goes dark teaches everyone never to flip the flag. Here the opposite is right:
after the switch the two stores diverge — every opt-out and every campaign
recorded since lives in Postgres only — so a silent fallback would answer from
a store that is missing them, and the roster it produced would look perfectly
ordinary. It raises instead. The rollback is the variable.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

from core.sql_dialect import POSTGRES, numbered, sms_segments_select

logger = logging.getLogger(__name__)




def render(
    *,
    ltv_column: str,
    sales_type_filter: str,
    tier_case: str,
    arm_expr: str,
    ok_tier_expr: str,
    filter_sql: str,
    tier_subset: str,
) -> str:
    """The Postgres rendering, placeholders already renumbered."""
    return numbered(sms_segments_select(
        POSTGRES,
        ltv_column=ltv_column,
        sales_type_filter=sales_type_filter,
        tier_case=tier_case,
        arm_expr=arm_expr,
        ok_tier_expr=ok_tier_expr,
        filter_sql=filter_sql,
        tier_subset=tier_subset,
    ))


async def fetch_segments(sql: str, params: Sequence[Any]) -> List[Tuple]:
    """Execute a rendered audience query and return plain tuples.

    Tuples rather than asyncpg `Record`s because the caller unpacks positionally
    — the same unpack that reads DuckDB's rows. Keeping the shape identical is
    what lets one Python loop serve both engines.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [tuple(row) for row in rows]


# ─── The rest of the tab: short statements, either engine ───────────────────
#
# The audience query earned a shared body because it carries rules. The
# statements below carry almost none — a preset is stored and read back, an
# opt-out is upserted — so what they need is not a second SQL dialect but one
# place that knows which engine is answering and how it spells a placeholder.
#
# The text is written once with `{table}` holes and `?` markers, exactly as the
# audience body is, and `run` fills both. Two engines, one string, and the
# statement is legible as SQL rather than as string-building.


async def fetch(sql: str, params: Sequence[Any]) -> List[Tuple]:
    """Rows as plain tuples — the shape DuckDB's `fetchall()` returns."""
    return await fetch_segments(sql, params)


async def fetch_one(sql: str, params: Sequence[Any]) -> Tuple | None:
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return tuple(row) if row is not None else None


async def execute(sql: str, params: Sequence[Any]) -> None:
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        await conn.execute(sql, *params)
