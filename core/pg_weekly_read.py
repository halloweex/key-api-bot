"""The weekly sales report, read from Postgres.

Stage 1 of the DuckDB exit moved sixteen *dashboard repository* methods behind
`KS_READ_*` flags. This module exists because that count was of repository
methods, not of readers — and the weekly report was never one of them. It has
been reading DuckDB `gold_daily_revenue` and `silver_orders` directly, through
a raw `store.connection()`, with no flag anywhere on the path.

WHY IT MATTERS MORE THAN A TAB

`warehouse_max_date` is the readiness gate for **both** weekly Telegram
messages — the sales report and the traffic report. If the warehouse it asks
stops moving, `MAX(date)` freezes behind the week end and both jobs return
`{"sent": False, "reason": "warehouse_behind"}` forever: at INFO, with the send
ledgers untouched, and nothing watching whether a send ever happened. Two
business-facing reports would stop arriving and the logs would look exactly
like the normal six-days-a-week quiet return.

THERE IS NO FALLBACK HERE, AND THAT IS THE POINT

Every ported tab falls back to DuckDB when Postgres fails, because a tab that
errors is better than a tab that is gone, and a stale-but-present DuckDB still
answers. This must not do that. The whole reason for moving is that DuckDB is
going to stop being written; a fallback would then build the report from a
frozen store — and `fetch_daily` fills absent days with zeros *by design*, so
the failure would not be an error, it would be a plausible-looking week of
zeros sent to everyone's phone.

So a Postgres failure raises, the job logs it and does not send, and — because
`weekly_report` ticks **daily** against its own ledger — tomorrow's tick tries
again. A broken read becomes a deferral, which is the behaviour the daily tick
was built for. A wrong report cannot be recalled from anybody's phone.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_WEEKLY"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the weekly report should ask Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule. Note this is the opposite
    of `KS_WEEKLY_REPORT_RICH`, which deliberately does *not* raise: that one
    decides what a message looks like, and taking the report down over a
    misspelling is the worse failure. This one decides which store the numbers
    come from, and serving the wrong store's numbers silently is worse than
    stopping.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    """Postgres is configured at all. Without a DSN there is nothing to ask."""
    return bool(os.getenv("KS_PG_DSN", "").strip())


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one weekly-report query against Postgres and return plain tuples.

    `sql` arrives with `?` markers, the shared bodies' convention, and is
    renumbered for asyncpg here rather than by every caller. Tuples because the
    callers unpack positionally.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
