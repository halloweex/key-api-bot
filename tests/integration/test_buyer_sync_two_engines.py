"""Which buyers the hourly sync fetches — the same answer from both engines.

Silver and buyers are seeded directly and identically into both stores, so the
selection is compared on its own rather than through a warehouse rebuild. Then
the one place the engines are meant to differ: a buyer DuckDB stored and the
mirror never shipped.
"""
from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_store import DuckDBStore

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


def _silver_rows():
    for oid, buyer, days, is_ret in ORDERS:
        day = TODAY - timedelta(days=days)
        yield (oid, 1, 19 if is_ret else 1, 100.0,
               datetime.combine(day, time(12, 0), tzinfo=KYIV), buyer, None,
               day, is_ret, "retail", True, "Instagram", False, None, None)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv("KS_READ_BUYER_SYNC", raising=False)
    store = DuckDBStore(db_path=tmp_path / "buyers.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    rows = list(_silver_rows())
    async with store.connection() as conn:
        conn.executemany(
            f"INSERT INTO silver_orders ({COLUMNS}) VALUES ({', '.join('?' * 15)})", rows)
        conn.executemany("INSERT INTO buyers (id, full_name) VALUES (?, ?)", BUYERS)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM silver.orders")
        await conn.execute("DELETE FROM bronze.buyers")
        await conn.executemany(
            f"INSERT INTO silver.orders ({COLUMNS}) VALUES "
            f"({', '.join(f'${i}' for i in range(1, 16))})", rows)
        await conn.executemany(
            "INSERT INTO bronze.buyers (id, full_name) VALUES ($1, $2)", BUYERS)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool, monkeypatch
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM silver.orders")
            await conn.execute("DELETE FROM bronze.buyers")
        await pool.close()
        await store.close()


async def _both(store, env, limit):
    env.delenv("KS_READ_BUYER_SYNC", raising=False)
    duck = await store.get_missing_buyer_ids(limit)
    env.setenv("KS_READ_BUYER_SYNC", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the selection reached DuckDB under KS_READ_BUYER_SYNC=postgres")

    with patch.object(DuckDBStore, "connection", _no_duckdb):
        postgres = await store.get_missing_buyer_ids(limit)
    env.delenv("KS_READ_BUYER_SYNC", raising=False)
    return duck, postgres


class TestTheSelectionAgrees:
    @pytest.mark.asyncio
    async def test_the_whole_set_in_the_same_order(self, both_engines):
        store, _pool, env = both_engines
        duck, postgres = await _both(store, env, 100)
        assert duck == postgres == [13, 14, 11, 12]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [1, 2, 3])
    async def test_the_limit_cuts_in_the_same_place(self, both_engines, limit):
        """A limit of 2 cuts between 14 and 11, which are level on both keys."""
        store, _pool, env = both_engines
        duck, postgres = await _both(store, env, limit)
        assert duck == postgres == [13, 14, 11, 12][:limit]


class TestTheOneIntendedDifference:
    @pytest.mark.asyncio
    async def test_a_buyer_the_mirror_never_shipped_is_fetched_again(self, both_engines):
        """DuckDB stored buyer 11; Postgres never received it. From DuckDB it
        looks done for good; from Postgres it is still missing, so the next
        hour fetches it and the mirror gets another try."""
        store, _pool, env = both_engines
        async with store.connection() as conn:
            conn.execute("INSERT INTO buyers (id, full_name) VALUES (11, 'Ігор')")
        duck, postgres = await _both(store, env, 100)
        assert 11 not in duck
        assert 11 in postgres
