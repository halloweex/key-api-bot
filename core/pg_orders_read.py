"""The order selections, asked of Postgres once chain 3 writes the orders there.

Every repair job chooses its work by reading the order tables: the hourly gap
scan (ids missing between the lowest and the highest), the half-written repair
(revenue and no line items), the 05:15 refresh's backdated orders, the full
sync's checkpoint, and the boot's "is there anything at all". Each read DuckDB.
Under `KS_WRITE_ORDERS=postgres` DuckDB's orders stop moving, and a selection
read from them never sees the repair it caused: the gap scan would re-fetch the
same 200 ids from KeyCRM every hour for ever, the half-written repair the same
orders every two hours, and the backfill-miss ledger — the one table that
stops a repair asking KeyCRM for what it cannot supply — would be read where
the chain no longer writes it.

So each DuckDB method routes to the function here at its first statement
(`duckdb_store._orders_in_postgres()`, which is `pg_orders_write.writes_postgres()`),
the transliteration of its own SQL: same tables, same window, same order, same
limit.

These raise. A selection that could not be made is not an empty one: the
callers are repair jobs, each with its own failure handling, and "nothing to
repair" read from an unreachable Postgres would be the job reporting a clean
run over a store it never saw. The module name matches `core/pg_*_read*.py`, so
`tests/unit/test_read_fallback_sites.py` forbids a handler here that carries on
toward DuckDB — whose orders, under the chain, are the frozen copy.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, List, Optional

# How long an id KeyCRM could not supply is left alone before a repair asks
# again — `scheduler._run_halfwritten_repair`'s 30 days, spelled once here for
# the Postgres side.
MISS_RETRY_DAYS = 30


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def latest_order_time() -> Optional[datetime]:
    """`MAX(updated_at)` over the orders — the full sync's checkpoint."""
    pool = await _pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT MAX(updated_at) FROM bronze.orders")


async def order_count() -> int:
    """How many orders Postgres holds — the boot's "is there any history"."""
    pool = await _pool()
    async with pool.acquire() as conn:
        return int(await conn.fetchval("SELECT count(*) FROM bronze.orders"))


async def order_id_gaps(limit: int = 200) -> List[int]:
    """`DuckDBStore.find_order_id_gaps`, over `bronze.orders` and the ledger."""
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH bounds AS (SELECT MIN(id) AS lo, MAX(id) AS hi FROM bronze.orders)
            SELECT g.id FROM bounds, generate_series(bounds.lo, bounds.hi) AS g(id)
            WHERE NOT EXISTS (SELECT 1 FROM bronze.orders o WHERE o.id = g.id)
              AND NOT EXISTS (SELECT 1 FROM app.order_backfill_misses m
                              WHERE m.order_id = g.id)
            ORDER BY g.id
            LIMIT $1
            """, int(limit))
    return [int(r["id"]) for r in rows]


_KYIV = "(({col}) AT TIME ZONE 'Europe/Kyiv')::date"


async def backdated_order_ids(since: date, limit: int = 200) -> List[int]:
    """`DuckDBStore.find_backdated_order_ids`, with Kyiv dates on both sides."""
    ordered, created = _KYIV.format(col="ordered_at"), _KYIV.format(col="created_at")
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id FROM bronze.orders
            WHERE {ordered} >= $1 AND {created} <= $1 AND {ordered} > {created}
            ORDER BY id
            LIMIT $2
            """, since, int(limit))
    return [int(r["id"]) for r in rows]


_EMPTY_LINE_ITEMS = """
SELECT o.id FROM bronze.orders o
WHERE o.grand_total > 0
  AND NOT EXISTS (SELECT 1 FROM bronze.order_products p WHERE p.order_id = o.id)
"""


async def halfwritten_candidates(limit: int) -> List[int]:
    """Orders with revenue and no line items, not recorded as a miss in the
    last 30 days, largest first — the half-written repair's selection."""
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            {_EMPTY_LINE_ITEMS}
              AND NOT EXISTS (
                  SELECT 1 FROM app.order_backfill_misses m
                  WHERE m.order_id = o.id
                    AND m.checked_at > now() - make_interval(days => $2))
            ORDER BY o.grand_total DESC, o.id
            LIMIT $1
            """, int(limit), MISS_RETRY_DAYS)
    return [int(r["id"]) for r in rows]


async def halfwritten_among(ids: Iterable[int]) -> List[int]:
    """Which of `ids` still carry revenue and no line items."""
    wanted = [int(i) for i in ids]
    if not wanted:
        return []
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"{_EMPTY_LINE_ITEMS} AND o.id = ANY($1::int[]) ORDER BY o.id", wanted)
    return [int(r["id"]) for r in rows]
