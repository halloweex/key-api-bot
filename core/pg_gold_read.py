"""Reading gold.daily_revenue from Postgres — step 1 of «Одна бронза».

The switch covers exactly the two Gold primitives the dashboard has:
period totals for /api/summary and the daily series for /api/revenue/trend.
Everything above them — comparison periods, granularity, forecast merge,
label building — stays in the one existing implementation, because a second
copy of that logic is how the two Silver definitions diverged in a day.

`KS_READ_GOLD=postgres` turns it on; `duckdb` (and absence) is the default;
anything else raises — `KS_BOT_STORE`'s rule, for the same reason: a typo in
the variable that decides where revenue numbers come from should stop the
request loudly, not quietly serve the old store.

WHAT MAPS TO WHAT

DuckDB's Gold keys on (date, sales_type) and splits channels into columns;
Postgres keys on (date, sales_type, source_id) with `source_id IS NULL` as
the all-channels roll-up. So:

    all sources               → roll-up rows (source_id IS NULL)
    source_id in {1, 2, 4}    → the fine rows — including per-source returns,
                                which DuckDB has to fetch from Silver because
                                its Gold never grew return columns per channel

The interception mirrors DuckDB's own routing: sources Gold cannot answer in
DuckDB (5, or none-such) never reach these readers either, so flipping the
flag changes the engine and nothing else. Postgres *could* answer source 5
from its fine rows — deliberately not done here: parity first, better answers
as their own change with their own verification.

Failure falls back to DuckDB with an ERROR log: step 1 is обкатка чтения, and
a dashboard that goes down because the optional engine hiccuped would teach
everyone to never flip the flag again. The fallback is visible in logs, and
the flag itself is the rollback.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_GOLD"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the old store."
        )
    return value == "postgres" and bool(os.getenv("KS_PG_DSN", "").strip())


def _predicate(
    sales_type: str, source_id: Optional[int], params: List[object],
) -> str:
    """WHERE tail shared by both readers. `params` already holds the dates."""
    clauses = []
    if source_id is None:
        clauses.append("source_id IS NULL")
    else:
        params.append(source_id)
        clauses.append(f"source_id = ${len(params)}")
    if sales_type != "all":
        params.append(sales_type)
        clauses.append(f"sales_type = ${len(params)}")
    return " AND ".join(clauses)


async def fetch_summary(
    start_date: date,
    end_date: date,
    sales_type: str = "retail",
    source_id: Optional[int] = None,
) -> Tuple[int, float, int, float]:
    """(orders, revenue, returns, returns_revenue) for the period.

    One row of four aggregates. Unlike DuckDB — where per-source returns need
    a Silver query because Gold has no per-channel return columns — the fine
    rows here carry returns, so every case is one statement against one table.
    """
    from core.pg import get_pool

    params: List[object] = [start_date, end_date]
    where = _predicate(sales_type, source_id, params)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT COALESCE(SUM(orders_count), 0),
                   COALESCE(SUM(revenue), 0),
                   COALESCE(SUM(returns_count), 0),
                   COALESCE(SUM(returns_revenue), 0)
            FROM gold.daily_revenue
            WHERE date BETWEEN $1 AND $2 AND {where}
            """,
            *params,
        )
    return (int(row[0]), float(row[1]), int(row[2]), float(row[3]))


async def fetch_series(
    start_date: date,
    end_date: date,
    sales_type: str = "retail",
    source_id: Optional[int] = None,
) -> List[Tuple[date, float, int]]:
    """(day, revenue, order_count) rows, the shape the trend chart consumes.

    `sales_type='all'` groups across types the way DuckDB's query does; days
    with no row stay absent, and the caller fills the calendar — identical to
    the DuckDB contract, which is the point.
    """
    from core.pg import get_pool

    params: List[object] = [start_date, end_date]
    where = _predicate(sales_type, source_id, params)
    if sales_type == "all":
        sql = f"""
            SELECT date, SUM(revenue), SUM(orders_count)
            FROM gold.daily_revenue
            WHERE date BETWEEN $1 AND $2 AND {where}
            GROUP BY date ORDER BY date
        """
    else:
        sql = f"""
            SELECT date, revenue, orders_count
            FROM gold.daily_revenue
            WHERE date BETWEEN $1 AND $2 AND {where}
            ORDER BY date
        """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [(r[0], float(r[1]), int(r[2])) for r in rows]
