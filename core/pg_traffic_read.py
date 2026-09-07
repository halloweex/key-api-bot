"""Reading the `/traffic` tab from Postgres.

Five methods behind the flag: the platform breakdown, the daily trend, the
transaction list, the campaign table and ROAS. What they read is
`silver.orders` — there since revision 0005 — joined to `silver.order_utm` and,
for ROAS only, `gold.daily_revenue` and `app.manual_expenses`. Revision 0018
brings the two that were missing.

WHAT MOVED, AND WHAT WAS TAKEN AWAY INSTEAD OF ADDED

Two of the five read `gold_daily_traffic`, which exists only in DuckDB, and the
expected shape of this change was to derive that Gold here as `core/pg_gold.py`
derives the revenue one. It is not here, because both of those queries fold it:
`SUM(orders_count)` and `SUM(revenue)` grouped by a *subset* of its own primary
key. `silver_order_utm` is keyed on `order_id`, so every order lands in exactly
one cell, and the fold is therefore the same arithmetic as the aggregate over
the rows underneath — identical **by construction** rather than to the kopeck
by measurement, which is the stronger of the two claims `/marketing` had to
pick between when its brand table went to the line level.

So both engines now read the join, one body, and this store gained no derived
layer, no deriver inside the Gold tick and no comparison to keep it honest.
The visible cost is that DuckDB's `gold_daily_traffic` is left with no reader
while still being rebuilt on every warehouse tick; whether to stop maintaining
it is the owner's call, and it is deliberately not made here.

THE TRAP IN THE ROAS QUERY

Its blended figure is `SUM(revenue)` over the revenue Gold, and the two Golds
are not the same shape: DuckDB grains on `(date, sales_type)`, Postgres adds
`source_id` with `source_id IS NULL` as the roll-up. A bare sum there reads
correctly in one engine and **doubles** in the other — 11,107,040.50 against a
true 5,553,520.25, measured over 30 days when `/marketing` moved. The body
carries `{gold_revenue_rollup}` for exactly that, which is a predicate in
Postgres and `TRUE` in DuckDB.

WHY THIS FALLS BACK

Nothing diverges: the tab writes nothing, and every table it reads is one
Postgres receives from DuckDB or derives from what DuckDB holds. A fault costs
the engine rather than the page — `core/pg_inventory_read.py`'s answer, and the
opposite of `/sms`, where after its switch the two stores genuinely differ and
answering from the stale one would look perfectly ordinary.

The freshness this depends on is not the hourly one. `silver.order_utm` rides
the Silver tick (`core/pg_order_utm.py`) because a UTM row that has not arrived
does not read as missing — it falls through the `COALESCE` to `organic` and
moves the split on the chart. `app.manual_expenses` does ride the hourly job,
which it can afford twice over: it holds zero rows in production, and an ad
spend typed at 10:00 reaching the ROAS by 11:00 is a display lag on a number
nobody is currently entering. If that changes, the one-line fix is to ship it
on the Silver tick beside the UTM.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_TRAFFIC"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the traffic queries should ask Postgres.

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
    """Run one traffic query against Postgres and return plain tuples.

    `sql` arrives with `?` markers, the shared bodies' convention, and is
    renumbered for asyncpg here rather than by every caller. Tuples because
    the callers unpack positionally, which is why the column order of the
    shared body is part of its contract.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
