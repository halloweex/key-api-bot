"""The chat assistant's numbers, read from Postgres.

ONE ENGINE SINCE STEP 14 (S14-07)

The assistant's five numeric tools read Postgres and nowhere else:
`core.chat_tools._chat_run` and `_get_source_breakdown` ask this module, as
production has since `KS_READ_CHAT=postgres` went on. A failure — a lost DSN
line included — is a tool error the model reads, as it always was here (no
fallback, below). The flag chooses nothing now.

Stage 1b of the DuckDB exit, found by the chain-2 design review rather than by
stage 1's count — which, like the weekly report's, counted dashboard repository
methods and not readers. `core/chat_tools.py` answers five of the assistant's
tools with raw `store.connection()` SQL against `gold_daily_revenue` and
`silver_order_lines`, and no flag stood anywhere on that path. Once DuckDB stops
being derived, an admin asking the assistant "how did this week go" would get a
frozen week, stated with the confidence of a language model.

WHY A FLAG OF ITS OWN, UNTIL S14-07

`KS_READ_GOLD` and `KS_READ_SILVER` are on in production. Widening either would
move these reads on deploy, with no step in between to compare the engines on
live data first — the order every port here has kept. This ships off.

NO FALLBACK

For the weekly report's reason (`core/pg_weekly_read.py`), sharpened: a failed
tool returns `{"error": ...}` to the model, which then says it could not fetch
the numbers. A fallback would hand it a frozen store's numbers instead, and the
model has no way to know they are old.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one tool query against Postgres; plain tuples back.

    `sql` arrives with `?` markers and already rendered for Postgres, and is
    renumbered for asyncpg here rather than by every caller.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
