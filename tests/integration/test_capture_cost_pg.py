"""The cost capture (`app.offer_cost_daily`) inside the per-SKU photograph,
against a real PostgreSQL.

Revision 0036's first in-transaction capture: chain 1's
`record_sku_inventory_snapshot` writes the day's photograph and, in the same
transaction under a SAVEPOINT of its own, every offer's cost and price as
`bronze.offer_stocks` holds them, dated with the photograph's day. What is
pinned, each named by the mutation it kills:

- the rows land with the photograph, one per offer, on the photograph's own
  Kyiv day, and the capture is stamped with how many;
- a capture that fails on the server rolls back alone: the photograph
  commits, no cost row is kept, the failure is stamped — and the next day's
  capture is kept again;
- with `KS_ANALYTICS_CAPTURES=off` the photograph is taken and nothing is
  captured or stamped;
- two runs on one day: the second finds the photograph taken and captures
  nothing; a run that does reach the capture again (the photograph retaken
  by hand) keeps the first cost (`DO NOTHING`) and adds only the offers the
  first run did not see.

The writers are called directly, as in `test_inventory_preflip.py`. The
inventory tables are the suite's to clear, as there; `app.offer_cost_daily`
is cleared whole, because every inventory test now writes it.

Skipped without `KS_PG_DSN`, like every store test.
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import chain_latch, pg_inventory_write

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLE = "app.offer_cost_daily"
STAMP = cap.state_row(TABLE)

# Days nothing else in the suite photographs, so the rows are ours to delete.
DAY = date(2031, 2, 3)
NEXT_DAY = date(2031, 2, 4)

OFFERS = [(9_920_001, 9_920_101, "COST-1"), (9_920_002, 9_920_102, "COST-2"),
          (9_920_003, 9_920_103, "COST-3")]
# (id, sku, price, purchased_price, quantity): a cost KeyCRM does not know is
# a fact too, and is kept as NULL.
STOCKS = [(9_920_001, "COST-1", Decimal("100.00"), Decimal("50.00"), 5),
          (9_920_002, "COST-2", Decimal("80.50"), None, 0),
          (9_920_003, "COST-3", Decimal("12.00"), Decimal("7.25"), 9)]
NEW_OFFER = (9_920_004, 9_920_104, "COST-4")


async def _kyiv_today(conn) -> date:
    return await conn.fetchval("SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")


async def _clean(pool):
    async with pool.acquire() as conn:
        today = await _kyiv_today(conn)
        await conn.execute("DELETE FROM app.sku_inventory_status")
        await conn.execute("DELETE FROM bronze.offer_stocks")
        await conn.execute("DELETE FROM bronze.offers")
        await conn.execute(
            "DELETE FROM app.inventory_sku_history WHERE date = ANY($1::date[])",
            [DAY, NEXT_DAY, today])
        await conn.execute(f"DELETE FROM {TABLE}")
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", STAMP)
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
            [chain_latch.owner_key(t) for t in pg_inventory_write.CHAIN_TABLES])


async def _seed(conn, offers=OFFERS, stocks=STOCKS):
    await conn.executemany(
        "INSERT INTO bronze.offers (id, product_id, sku, synced_at) VALUES ($1, $2, $3, now())",
        offers)
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price, quantity, "
        "reserve, mirrored_at) VALUES ($1, $2, $3, $4, $5, 0, now())",
        stocks)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    """Standing in for `core.pg.get_pool`, so the writers use it."""
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(p)
    async with p.acquire() as conn:
        await _seed(conn)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        await pg_inventory_write.rebuild_sku_inventory_status()
        yield p
    await _clean(p)
    await p.close()


async def _costs(conn, day):
    rows = await conn.fetch(
        f"SELECT offer_id, sku, purchased_price, price FROM {TABLE} "
        "WHERE date = $1 ORDER BY offer_id", day)
    return [tuple(r) for r in rows]


async def _photographed(conn, day):
    return await conn.fetchval(
        "SELECT count(*) FROM app.inventory_sku_history WHERE date = $1", day)


async def _stamp(conn):
    return await conn.fetchrow(
        "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
        "FROM meta.mirror_state WHERE table_name = $1", STAMP)


def _expected(stocks=STOCKS):
    return [(i, sku, cost, price) for i, sku, price, cost, _q in stocks]


class TestTheCostRidesThePhotograph:
    @pytest.mark.asyncio
    async def test_one_row_per_offer_lands_with_the_photograph(self, pool):
        """Kills: a capture that never runs, reads another table, drops the
        offer with no known cost, or is not stamped with its rows."""
        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is True
        async with pool.acquire() as conn:
            assert await _photographed(conn, DAY) == 3
            assert await _costs(conn, DAY) == _expected()
            stamp = await _stamp(conn)
        assert stamp["failures_since_ok"] == 0 and stamp["last_error"] is None
        assert stamp["last_rows"] == 3 and stamp["last_ok_at"] is not None

    @pytest.mark.asyncio
    async def test_its_day_is_the_photographs_own_kyiv_day(self, pool):
        """Unpinned, the photograph dates itself with the server's Kyiv day,
        and the cost takes that same day. Kills: a capture dated by its own
        clock (UTC, or the container's), which is a day off for three hours
        every night."""
        async with pool.acquire() as conn:
            before = await _kyiv_today(conn)
        assert await pg_inventory_write.record_sku_inventory_snapshot() is True
        async with pool.acquire() as conn:
            after = await _kyiv_today(conn)
            photo = {r["date"] for r in await conn.fetch(
                "SELECT DISTINCT date FROM app.inventory_sku_history "
                "WHERE offer_id = ANY($1::int[])", [o[0] for o in OFFERS])}
            costs = {r["date"] for r in await conn.fetch(f"SELECT DISTINCT date FROM {TABLE}")}
        assert len(photo) == 1 and photo == costs
        assert photo <= {before, after}


class TestItNeverFailsTheWriter:
    @pytest.mark.asyncio
    async def test_a_capture_that_fails_on_the_server_leaves_the_photograph(
            self, pool, monkeypatch):
        """The real statement, made to fail on the server mid-transaction.
        Kills: the capture outside its savepoint (the photograph rolls back
        with it, or the writer raises), a failure left unstamped, and a
        failed savepoint that poisons the next night's capture."""
        real = pg_inventory_write._COST_CAPTURE_SQL
        failing = real.replace("s.id,", "s.id / 0,")
        assert failing != real
        monkeypatch.setattr(pg_inventory_write, "_COST_CAPTURE_SQL", failing)

        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is True
        async with pool.acquire() as conn:
            assert await _photographed(conn, DAY) == 3
            assert await _costs(conn, DAY) == []
            stamp = await _stamp(conn)
        assert stamp["failures_since_ok"] == 1 and stamp["last_ok_at"] is None
        assert stamp["last_error"].startswith("DivisionByZeroError")

        monkeypatch.setattr(pg_inventory_write, "_COST_CAPTURE_SQL", real)
        assert await pg_inventory_write.record_sku_inventory_snapshot(NEXT_DAY) is True
        async with pool.acquire() as conn:
            assert await _costs(conn, NEXT_DAY) == _expected()
            stamp = await _stamp(conn)
        assert stamp["failures_since_ok"] == 0 and stamp["last_rows"] == 3

    @pytest.mark.asyncio
    async def test_the_switch_off_takes_the_photograph_and_captures_nothing(
            self, pool, monkeypatch):
        """Kills: a capture the switch does not reach, or a switch that
        stops the photograph too."""
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is True
        async with pool.acquire() as conn:
            assert await _photographed(conn, DAY) == 3
            assert await conn.fetchval(f"SELECT count(*) FROM {TABLE}") == 0
            assert await _stamp(conn) is None


class TestTwoRunsOnOneDay:
    @pytest.mark.asyncio
    async def test_the_second_run_finds_the_day_taken_and_captures_nothing(self, pool):
        """Every hourly stock step after the day's first asks for the
        photograph again. Kills: a capture placed before the "already taken"
        guard — it would run every hour, and its stamp would say so."""
        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is True
        async with pool.acquire() as conn:
            first = await _stamp(conn)
            await conn.execute(
                "UPDATE bronze.offer_stocks SET purchased_price = 70 WHERE id = $1",
                OFFERS[0][0])

        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is False
        async with pool.acquire() as conn:
            assert await _costs(conn, DAY) == _expected()
            assert tuple(await _stamp(conn)) == tuple(first)

    @pytest.mark.asyncio
    async def test_a_rerun_that_reaches_the_capture_keeps_the_first_cost(self, pool):
        """The photograph retaken by hand (its rows deleted, the snapshot run
        again): the capture runs a second time on the same day. Kills an
        `ON CONFLICT DO UPDATE` — the cost the day began with would be
        replaced by a later one, with no record that it existed — and a
        capture with no conflict clause, which would fail on the key."""
        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is True
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE bronze.offer_stocks SET purchased_price = 70, price = 140 "
                "WHERE id = $1", OFFERS[0][0])
            new_stock = (NEW_OFFER[0], NEW_OFFER[2], Decimal("30.00"), Decimal("11.00"), 4)
            await _seed(conn, [NEW_OFFER], [new_stock])
            await conn.execute("DELETE FROM app.inventory_sku_history WHERE date = $1", DAY)
        await pg_inventory_write.rebuild_sku_inventory_status()

        assert await pg_inventory_write.record_sku_inventory_snapshot(DAY) is True
        async with pool.acquire() as conn:
            assert await _photographed(conn, DAY) == 4
            assert await _costs(conn, DAY) == _expected(STOCKS + [new_stock])
            stamp = await _stamp(conn)
        assert stamp["failures_since_ok"] == 0 and stamp["last_rows"] == 1
