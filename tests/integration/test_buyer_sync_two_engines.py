"""Which buyers the hourly sync fetches — Postgres' answer, as DuckDB gave it.

Silver and buyers are seeded directly, so the selection is compared on its own
rather than through a warehouse rebuild.

ONE ENGINE SINCE STEP 14 (S14-07). `get_missing_buyer_ids` selects from
Postgres alone. The expected lists below are what both engines answered on
this fixture while the file compared them — DuckDB's answer, written out,
which is rule 9's literal. The store is never connected and its DuckDB
connection is a trap. The one difference the engines were meant to have — a
buyer DuckDB stored and the mirror never shipped, fetched again from Postgres
— went with DuckDB's selection: there is no DuckDB store to hold one.
"""
from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

KYIV = ZoneInfo("Europe/Kyiv")
TODAY = datetime.now(timezone.utc).astimezone(KYIV).date()

# (order_id, buyer_id, days_ago, is_return)
ORDERS = [
    (1, 10, 1, False),     # a named buyer — never selected
    (2, 11, 2, False),     # absent
    (3, 12, 3, False),     # present with an empty name — selected
    (4, 13, 5, True),      # absent, and a return: first despite being oldest
    (5, 14, 2, False),     # absent, level with 11 on both keys — the tiebreak
    (6, 11, 4, False),     # 11's older order; its latest stays two days ago
    (7, None, 1, False),   # no buyer at all
]
BUYERS = [(10, "Олена"), (12, "")]
COLUMNS = ("id, source_id, status_id, grand_total, ordered_at, buyer_id, manager_id,"
           " order_date, is_return, sales_type, is_active_source, source_name,"
           " is_new_customer, buyer_first_order_date, promocode")
# What both engines selected, in their order.
SELECTED = [13, 14, 11, 12]


def _silver_rows():
    for oid, buyer, days, is_ret in ORDERS:
        day = TODAY - timedelta(days=days)
        yield (oid, 1, 19 if is_ret else 1, 100.0,
               datetime.combine(day, time(12, 0), tzinfo=KYIV), buyer, None,
               day, is_ret, "retail", True, "Instagram", False, None, None)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb

    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM silver.orders")
        await conn.execute("DELETE FROM bronze.buyers")
        await conn.executemany(
            f"INSERT INTO silver.orders ({COLUMNS}) VALUES "
            f"({', '.join(f'${i}' for i in range(1, 16))})", list(_silver_rows()))
        await conn.executemany(
            "INSERT INTO bronze.buyers (id, full_name) VALUES ($1, $2)", BUYERS)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the selection")
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM silver.orders")
            await conn.execute("DELETE FROM bronze.buyers")
        await pool.close()


class TestTheSelection:
    @pytest.mark.asyncio
    async def test_the_whole_set_in_order(self, in_postgres):
        assert await in_postgres.get_missing_buyer_ids(100) == SELECTED

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [1, 2, 3])
    async def test_the_limit_cuts_in_the_same_place(self, in_postgres, limit):
        """A limit of 2 cuts between 14 and 11, which are level on both keys."""
        assert await in_postgres.get_missing_buyer_ids(limit) == SELECTED[:limit]

    @pytest.mark.asyncio
    async def test_a_buyer_the_landing_has_is_not_fetched_again(self, in_postgres):
        """Once Postgres holds buyer 11 with a name, the next hour leaves it."""
        from core import pg

        pool = await pg.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyers (id, full_name) VALUES (11, 'Ігор')")
        assert await in_postgres.get_missing_buyer_ids(100) == [13, 14, 12]
