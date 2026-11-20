"""A Gold the Rust pilot is compared on, built the way production builds it.

Shared by the pilot's harness (`tests/integration/test_rust_pilot_trend.py`)
and its benchmark (`rust/bench/bench.sh`, which runs this module). Orders land
in `bronze.orders`; `core.pg_silver.rebuild_silver` and then
`core.pg_gold.rebuild_gold` — production's own derivations — decide each
order's Kyiv day, its sales type, whether it is a return, and its Gold cell.
Nothing here computes a date or a sum the endpoint reads, so a Kyiv midnight
is placed by production's SQL, not by this file.

About forty thousand orders from December 2023 to January 2027, deterministic:
every source including the retired (3) and the exhibition (5), all four sales
types, returns by status group and by status alone, days with no orders at
all, and orders one second either side of every Kyiv midnight that a clock
change or a year end makes interesting.

Only for a throwaway database: it empties the three tables it writes.
"""
from __future__ import annotations

import asyncio
import os
import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from core.duckdb_constants import B2B_MANAGER_ID, RETAIL_MANAGER_IDS

KYIV = ZoneInfo("Europe/Kyiv")

TABLES = ("gold.daily_revenue", "silver.orders", "bronze.orders")
COLUMNS = ("id", "source_id", "status_id", "status_group_id", "grand_total",
           "ordered_at", "created_at", "updated_at", "buyer_id", "manager_id")

FIRST_DAY = date(2023, 12, 1)
LAST_DAY = date(2027, 1, 31)

# Weighted the way the shop is: Instagram and the website carry most of it.
SOURCES = (1, 1, 1, 1, 2, 2, 4, 4, 4, 3, 5)
# A manager decides `sales_type` (`silver_sales_type_case`): none is retail,
# the wholesale manager is b2b, a retail-list manager is retail, anyone else
# is internal. Source 5 is exhibition whoever sold it.
INTERNAL_MANAGER = 7
assert INTERNAL_MANAGER not in RETAIL_MANAGER_IDS and INTERNAL_MANAGER != B2B_MANAGER_ID
MANAGERS = (None, B2B_MANAGER_ID, RETAIL_MANAGER_IDS[0], INTERNAL_MANAGER)
MANAGER_WEIGHTS = (70, 8, 14, 8)

# One second either side of the midnights worth testing, and the Kyiv day
# production's Silver must put each on. EEST is UTC+3, EET is UTC+2.
BOUNDARY_ORDERS: Tuple[Tuple[str, date], ...] = (
    ("2025-10-25T20:59:59+00:00", date(2025, 10, 25)),  # 23:59:59 EEST
    ("2025-10-25T21:00:00+00:00", date(2025, 10, 26)),  # the 25-hour day begins
    ("2025-10-26T21:59:59+00:00", date(2025, 10, 26)),  # 23:59:59 EET
    ("2025-10-26T22:00:00+00:00", date(2025, 10, 27)),
    ("2026-03-28T21:59:59+00:00", date(2026, 3, 28)),   # 23:59:59 EET
    ("2026-03-28T22:00:00+00:00", date(2026, 3, 29)),   # the 23-hour day begins
    ("2026-03-29T20:59:59+00:00", date(2026, 3, 29)),   # 23:59:59 EEST
    ("2026-03-29T21:00:00+00:00", date(2026, 3, 30)),
    ("2026-10-24T20:59:59+00:00", date(2026, 10, 24)),
    ("2026-10-24T21:00:00+00:00", date(2026, 10, 25)),
    ("2026-10-25T21:59:59+00:00", date(2026, 10, 25)),
    ("2026-10-25T22:00:00+00:00", date(2026, 10, 26)),
    ("2026-12-31T21:59:59+00:00", date(2026, 12, 31)),
    ("2026-12-31T22:00:00+00:00", date(2027, 1, 1)),
)
BOUNDARY_ID_BASE = 9_000_000

Row = Tuple[int, int, int, Optional[int], Decimal, datetime, datetime, datetime,
            int, Optional[int]]


def _kyiv_midnight(day: date) -> datetime:
    return datetime.combine(day, time(0), KYIV).astimezone(timezone.utc)


def build_orders(seed: int = 20261021, per_day: int = 40) -> List[Row]:
    rng = random.Random(seed)
    rows: List[Row] = []
    order_id = 1
    day = FIRST_DAY
    while day <= LAST_DAY:
        start, end = _kyiv_midnight(day), _kyiv_midnight(day + timedelta(days=1))
        # 23, 24 or 25 hours: the clock-change days are not 86 400 seconds.
        span = int((end - start).total_seconds())
        count = 0 if rng.random() < 0.12 else rng.randint(0, 2 * per_day)
        for _ in range(count):
            at = start + timedelta(seconds=rng.randrange(span))
            manager = rng.choices(MANAGERS, weights=MANAGER_WEIGHTS)[0]
            cents = (rng.randint(1_000_000, 30_000_000) if manager == B2B_MANAGER_ID
                     else rng.randint(5_000, 900_000))
            if rng.random() < 0.06:
                # A return: by KeyCRM's group, or by status alone on the rows
                # synced before the group existed.
                status, group = 19, (6 if rng.random() < 0.7 else None)
            else:
                status, group = rng.choice((1, 12, 20)), rng.choice((None, 1, 2, 4))
            rows.append((order_id, rng.choice(SOURCES), status, group,
                         Decimal(cents) / 100, at, at, at, rng.randint(1, 4000), manager))
            order_id += 1
        day += timedelta(days=1)
    for i, (instant, _day) in enumerate(BOUNDARY_ORDERS):
        at = datetime.fromisoformat(instant)
        rows.append((BOUNDARY_ID_BASE + i, 1, 1, None, Decimal("1111.11"), at, at, at,
                     4001 + i, None))
    return rows


async def seed(pool, orders: Optional[Sequence[Row]] = None) -> Dict[str, object]:
    """Empty the three tables, land the orders, derive Silver and Gold."""
    from core.pg_gold import rebuild_gold
    from core.pg_silver import rebuild_silver

    rows = list(build_orders() if orders is None else orders)
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.copy_records_to_table(
            "orders", schema_name="bronze", columns=COLUMNS, records=rows)
    # The two-engine tests' arrangement: the derivations ask the global pool
    # for the schema revision, and the caller's pool is the one to use.
    with patch("core.pg.require_revision", new=AsyncMock()):
        silver = await rebuild_silver(pool=pool)
        gold = await rebuild_gold(pool=pool)
    return {"orders": len(rows), "silver": silver, "gold": gold}


async def boundary_days(pool) -> Dict[int, date]:
    """The Kyiv day production's Silver gave each boundary order."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, order_date FROM silver.orders WHERE id >= $1 ORDER BY id",
            BOUNDARY_ID_BASE)
    return {r["id"]: r["order_date"] for r in rows}


async def clear(pool) -> None:
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")


async def _main() -> None:
    import asyncpg

    pool = await asyncpg.create_pool(os.environ["KS_PG_DSN"], min_size=1, max_size=2)
    try:
        print(await seed(pool))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
