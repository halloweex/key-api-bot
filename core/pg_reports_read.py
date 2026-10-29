"""Reading the `/reports` tab from Postgres.

The page renders two tabs — the per-source summary and the product table — and
three repository methods are their only consumers. All three read Silver, and
Silver is already in Postgres: `silver.orders` since revision 0005 and
`silver.order_lines` since 0011, the latter a view whose body is the same text
DuckDB uses (`core.sql_dialect.order_lines_select`). So there is nothing to
migrate and nothing to replicate; what is left is choosing which engine
answers.

NO FALLBACK SINCE STEP 14 (S14-05)

`/reports` reads Postgres. The flag that chose the engine (`KS_READ_REPORTS`)
chooses nothing now: the router asks Postgres and nothing else, and a failure —
a lost DSN line included — is refused as a 503 naming `reports`
(`read_fallback.refuse`), which is what `KS_READ_FALLBACK=off` made of it
since 2026-10-09. The paragraphs below are the history of the port.

WHY NOT CLICKHOUSE

`silver.orders` is there, and `gold.daily_revenue` is derived there — but the
**line level is not**, and both of this tab's tabs are about products. Shipping
line items to ClickHouse is a new layer, not a flag; the same reason `/sms`
stayed on Postgres, and it is written down in `core/pg_sms_read.py` too.

WHY IT FELL BACK, UNTIL S14-05

Nothing diverges: the tab writes nothing, both engines read the same Silver,
and a fault costs the engine rather than the page. Same answer as
`core/pg_inventory_read.py` and `core/pg_gold_read.py`, and the opposite of
`core/pg_sms_read.py`, where after the switch the two stores genuinely differ
and answering from the stale one would look perfectly ordinary.

WHAT THE FLAG DOES NOT COVER

Only these three. `/reports/marketing-summary` — which is rendered on
`/marketing`, not here — reads `gold_daily_products` and `revenue_goals`, and
**neither exists in Postgres**: 0014 records the first as DuckDB-only, and the
second is in no migration at all. That endpoint keeps its own store until
somebody decides whether to derive that Gold in Postgres or rewrite the query
onto the line level, which is what `/inventory` did and found cheaper.

THE DUPLICATION, NAMED RATHER THAN HIDDEN

`pg_sms_read`, `pg_inventory_read` and `pg_dashboard_users` each carry a `fetch`
shaped like the one below. Twelve lines of pool handling with no rule in them,
so charter rule 1 does not bite — but this is the fourth, which is the point
where extracting a neutral home starts being worth more than the blast radius
of touching three live tabs to do it. Filed rather than done here.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one report query against Postgres and return plain tuples.

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
