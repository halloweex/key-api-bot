"""A repeat customer's lifespan is counted in Kyiv days (L2-10).

`_INSIGHTS_CLV_SQL` measured a buyer's lifespan as
`CAST(MAX(ordered_at) AS DATE) - CAST(MIN(ordered_at) AS DATE)`, and the cast
takes the session's time zone — UTC in production's Postgres. An order at
2026-10-01 21:30 UTC is 2 October in Kyiv: a buyer who ordered then and on
3 October at noon has a lifespan of one day, which the cast read as two. It
reads Silver's `order_date`, the Kyiv date every other figure on the page
uses.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest

from tests.integration.pg_tab_seed import derive, seed, tab_in_postgres

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BUYER = 99_401
ORDERS = [
    # (id, source_id, status_id, grand_total, ordered_at, buyer_id, manager_id, promocode)
    (99_401_001, 4, 12, 100, datetime(2026, 10, 1, 21, 30, tzinfo=timezone.utc), BUYER, None, None),
    (99_401_002, 4, 12, 300, datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc), BUYER, None, None),
]


async def _populate(conn, pool):
    await seed(conn, orders=ORDERS)
    await derive(pool)


@pytest.mark.asyncio
async def test_the_lifespan_is_one_kyiv_day(monkeypatch):
    """Mutation: `CAST(MAX(s.ordered_at) AS DATE) - CAST(MIN(...))` again —
    two days in a UTC session."""
    from core.store import get_store

    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        async with pool.acquire() as conn:
            assert await conn.fetchval("SHOW TimeZone") in ("UTC", "Etc/UTC")
            kyiv = await conn.fetch(
                "SELECT order_date FROM silver.orders WHERE buyer_id = $1 ORDER BY id", BUYER)
        assert [r["order_date"] for r in kyiv] == [date(2026, 10, 2), date(2026, 10, 3)]

        store = await get_store()
        insights = await store.get_customer_insights(
            date(2026, 10, 1), date(2026, 10, 31), sales_type="all")

    assert insights["metrics"]["avgCustomerLifespanDays"] == 1, insights["metrics"]
