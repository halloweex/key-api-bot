"""One bad stock or offer row costs that row, never the catalogue (L1-06, L1-05).

`pg_inventory_write.upsert_stocks` wrote the whole catalogue of stock in one
transaction. One row Postgres refused — a quantity KeyCRM served as null, a
price over `NUMERIC(12,2)` — raised inside it, so every offer kept its old
quantity and no movement was recorded, every hour, until somebody read the
log. And an offer named twice in one payload (two pages fetched in parallel
while the list shifted) planned two movements off the same stored quantity:
one sale of three recorded twice.

Now every row is asked before the latch what Postgres would refuse, a refused
one is logged and skipped by id, an unforeseen `DataError` falls back to row
by row, and the payload is one row per offer, the last one given. Against a
real Postgres; skipped without `KS_PG_DSN`.
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import pg_inventory_write as inv
from core.landing_rows import OfferRow

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 9_930_000
IDS = [BASE + i for i in range(6)]


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.stock_movements WHERE offer_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.offer_stocks WHERE id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.offers WHERE id = ANY($1::int[])", IDS)


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(p)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield p
    await _clean(p)
    await p.close()


def _stock(i: int, quantity: int = 3, **over) -> dict:
    row = {"id": BASE + i, "sku": f"BAD-{i}", "price": 10, "purchased_price": 5,
           "quantity": quantity, "reserve": 0}
    row.update(over)
    return row


async def _quantities(pool) -> dict:
    async with pool.acquire() as conn:
        return {r["id"]: r["quantity"] for r in await conn.fetch(
            "SELECT id, quantity FROM bronze.offer_stocks WHERE id = ANY($1::int[])", IDS)}


async def _movements(pool) -> list:
    async with pool.acquire() as conn:
        return [tuple(r) for r in await conn.fetch(
            "SELECT offer_id, movement_type, quantity_before, quantity_after, delta "
            "FROM app.stock_movements WHERE offer_id = ANY($1::int[]) "
            "AND movement_type <> 'initial' ORDER BY offer_id, id", IDS)]


class TestAStockRowPostgresWouldRefuse:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad", [
        {"quantity": None},                  # KeyCRM served it as null
        {"reserve": None},
        {"quantity": "two"},
        {"price": 10 ** 11},                 # over NUMERIC(12,2)
        {"purchased_price": float("inf")},   # no infinity fits NUMERIC(12,2)
        {"quantity": float("nan")},          # asyncpg cannot send it as an integer
        {"sku": "NUL\x00"},
        {"id": 2 ** 31},                     # outside INTEGER: never the base row
    ], ids=["null-quantity", "null-reserve", "text-quantity", "price-overflow",
            "inf-cost", "nan-quantity", "nul-sku", "id-overflow"])
    async def test_the_other_offers_still_move(self, pool, bad):
        """Mutation: drop `stock_refusal` from `upsert_stocks` — the batch
        raises and the four good offers keep 3."""
        await inv.upsert_stocks([_stock(i) for i in range(5)])
        sold = [_stock(i, quantity=1) for i in range(4)] + [_stock(4, **{"quantity": 1, **bad})]

        written, moved = await inv.upsert_stocks(sold)

        assert (written, moved) == (4, 4)
        held = await _quantities(pool)
        assert [held[BASE + i] for i in range(4)] == [1, 1, 1, 1]
        assert held[BASE + 4] == 3, "the refused row was written"
        assert [m[0] for m in await _movements(pool)] == IDS[:4]

    @pytest.mark.asyncio
    async def test_a_missing_quantity_is_still_zero(self, pool):
        """Absent is 0, as it always was; only a null is refused."""
        row = _stock(0)
        del row["quantity"], row["reserve"]
        assert await inv.upsert_stocks([row]) == (1, 0)
        assert (await _quantities(pool))[BASE] == 0


class TestAValuePostgresTakes:
    """What asyncpg writes is written, as main wrote it (AR-01). A quantity
    KeyCRM serves as 3.0 or 1.5 goes into the INTEGER as 3 and 1, True as 1;
    a NaN cost is stored, Postgres' NUMERIC holding NaN. The refusal used to
    turn these away as "not an integer" or "not a finite number", and a
    catalogue served that way froze every SKU's stock."""

    @pytest.mark.asyncio
    async def test_a_float_quantity_is_stored_as_the_integer_asyncpg_sends(self, pool):
        """Mutation: refuse a float in `_int_refusal` — (0, 0) and nothing
        stored, the reviewer's reproduction."""
        written, moved = await inv.upsert_stocks([
            _stock(0, quantity=3.0),
            _stock(1, quantity=1.5, reserve=0.0),
            _stock(2, quantity=True),
        ])
        assert (written, moved) == (3, 3)
        async with pool.acquire() as conn:
            stored = [tuple(r) for r in await conn.fetch(
                "SELECT id, quantity, reserve FROM bronze.offer_stocks "
                "WHERE id = ANY($1::int[]) ORDER BY id", IDS)]
        assert stored == [(BASE, 3, 0), (BASE + 1, 1, 0), (BASE + 2, 1, 0)]

    @pytest.mark.asyncio
    async def test_a_nan_cost_is_stored(self, pool):
        """Mutation: refuse NaN in `_money_refusal` — (0, 0)."""
        assert await inv.upsert_stocks(
            [_stock(0, purchased_price=float("nan"))]) == (1, 1)
        async with pool.acquire() as conn:
            cost = await conn.fetchval(
                "SELECT purchased_price::text FROM bronze.offer_stocks WHERE id = $1", BASE)
        assert cost == "NaN"

    @pytest.mark.asyncio
    async def test_a_float_product_id_is_an_offer(self, pool):
        rows = [OfferRow(BASE, 701.0, "A"), OfferRow(BASE + 1, True, "B")]  # type: ignore[arg-type]
        assert await inv.upsert_offers(rows) == 2
        async with pool.acquire() as conn:
            held = [tuple(r) for r in await conn.fetch(
                "SELECT id, product_id FROM bronze.offers WHERE id = ANY($1::int[]) "
                "ORDER BY id", IDS)]
        assert held == [(BASE, 701), (BASE + 1, 1)]


class TestAValuePostgresRefusesThatNothingForesaw:
    @pytest.mark.asyncio
    async def test_the_batch_is_written_row_by_row(self, pool):
        """A `DataError` past the refusal (the refusal patched blind here, a NUL
        in the SKU then reaching Postgres): the other rows land one by one,
        each planned off its own read. Mutation: re-raise the DataError — the
        four good offers keep 3."""
        await inv.upsert_stocks([_stock(i) for i in range(5)])
        sold = [_stock(i, quantity=1) for i in range(4)] + [
            _stock(4, quantity=1, sku="NUL\x00")]

        with patch.object(inv, "stock_refusal", return_value=None):
            written, moved = await inv.upsert_stocks(sold)

        assert (written, moved) == (4, 4)
        held = await _quantities(pool)
        assert [held[BASE + i] for i in range(5)] == [1, 1, 1, 1, 3]
        assert [m[0] for m in await _movements(pool)] == IDS[:4]


class TestAnOfferNamedTwice:
    @pytest.mark.asyncio
    async def test_one_sale_is_one_movement(self, pool):
        """Mutation: drop `_deduplicated` — two `stock_out` of -3 for one sale."""
        await inv.upsert_stocks([_stock(0, quantity=5)])
        written, moved = await inv.upsert_stocks([_stock(0, quantity=2), _stock(0, quantity=2)])

        assert (written, moved) == (1, 1)
        assert await _movements(pool) == [(BASE, "stock_out", 5, 2, -3)]
        assert (await _quantities(pool))[BASE] == 2

    @pytest.mark.asyncio
    async def test_the_last_copy_wins(self, pool):
        await inv.upsert_stocks([_stock(0, quantity=5)])
        await inv.upsert_stocks([_stock(0, quantity=4), _stock(0, quantity=2)])
        assert await _movements(pool) == [(BASE, "stock_out", 5, 2, -3)]


class TestAnOfferPostgresWouldRefuse:
    @pytest.mark.asyncio
    async def test_the_other_offers_land(self, pool):
        """`bronze.offers.product_id` is NOT NULL: one offer without a product
        used to cost the whole offer→product map."""
        rows = [OfferRow(BASE, 701, "A"), OfferRow(BASE + 1, None, "B"),  # type: ignore[arg-type]
                OfferRow(BASE + 2, 703, "C\x00"), OfferRow(BASE + 3, 704, "D")]
        assert await inv.upsert_offers(rows) == 2
        async with pool.acquire() as conn:
            held = sorted(r["id"] for r in await conn.fetch(
                "SELECT id FROM bronze.offers WHERE id = ANY($1::int[])", IDS))
        assert held == [BASE, BASE + 3]

    @pytest.mark.asyncio
    async def test_an_unforeseen_refusal_is_written_row_by_row(self, pool):
        rows = [OfferRow(BASE, 701, "A"), OfferRow(BASE + 1, 702, "B\x00")]
        with patch.object(inv, "offer_refusal", return_value=None):
            assert await inv.upsert_offers(rows) == 1
        async with pool.acquire() as conn:
            held = [r["id"] for r in await conn.fetch(
                "SELECT id FROM bronze.offers WHERE id = ANY($1::int[])", IDS)]
        assert held == [BASE]
