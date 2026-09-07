"""Reading the `/expenses` tab from Postgres.

Six read methods, and they split cleanly in two — which is why this port is
smaller than it looks.

**Three need nothing new.** `list_expenses`, `get_ad_spend_by_platform` and
`get_expenses_summary` read only `manual_expenses`, and revision 0018 already
put that here: the ROAS calculation on `/traffic` had to be able to join it.
Half the tab moves with no migration at all.

**Two needed the landing.** `get_expense_summary` and `get_profit_analysis`
read `expenses` joined to orders, plus the type dictionary for its names.
Revision 0020 mirrors both — they come from KeyCRM, so they are `bronze` and
mirrored, the opposite of `manual_expenses` sitting in `app` because nothing
can re-derive a number a human typed.

`get_expense_types` is the sixth and reads the dictionary alone.

WHAT THE PORT CHANGED IN BOTH ENGINES

The two summary methods joined `expenses` to *raw* `orders` and then narrowed
by `sales_type` with an `EXISTS` against `silver_orders`. The only facts they
took from the order were its Kyiv date and its source — and `silver.orders`
carries both as columns, plus `sales_type` itself. So the shared body reads
Silver directly and the EXISTS is gone: one join instead of two, the same
answer, and one fewer place where a date is converted at read time rather than
being stored converted.

Sound because Silver covers every order — 46,272 of 46,272, verified
2026-08-20 — so nothing is excluded that the old join admitted, and because
`_build_sales_type_filter` was already reading Silver rather than re-deriving
the classification from `manager_id`. That subquery had been the fix for a
real defect (#101 gave source 5 its own `sales_type` while the filter, knowing
only about managers, still counted those 176 orders as retail); reading the
column directly is the same fix taken one step further.

WHY THIS FALLS BACK

Nothing diverges: the reads write nothing, and every table is one Postgres
receives from KeyCRM in the same call DuckDB does or replicates from what
DuckDB holds. A fault costs the engine rather than the page —
`core/pg_inventory_read.py`'s answer.

The writes are a different matter and stay on DuckDB. `add_expense`,
`update_expense` and `delete_expense` write `manual_expenses` there, and the
hourly `replicate_operational` carries the result. That lag was harmless while
nothing read the copy; with this flag on, a human who types an expense and
looks at the page would not see it. So the three write paths replicate
immediately, the way an admin UTM reparse ships immediately — see
`core/repositories/expenses.py`.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_EXPENSES"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the expense queries should ask Postgres.

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
    """Run one expense query against Postgres and return plain tuples.

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
