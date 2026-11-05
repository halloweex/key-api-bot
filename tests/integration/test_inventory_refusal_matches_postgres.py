"""Chain 1's refusal says what Postgres says, value by value (AR-01).

`stock_refusal` and `offer_refusal` skip, before the latch, a row Postgres
would refuse, so that one such row costs that row and not the hour's whole
catalogue of stock (L1-06). A refusal wider than Postgres' is the opposite
defect: a row main wrote, now lost every hour. The first version refused any
quantity that was not a Python int, and asyncpg writes 3.0 into an INTEGER as
3 — so a catalogue KeyCRM served with float quantities would have frozen
every SKU's stock.

Here each JSON-shaped value KeyCRM could serve is written into the real
table, through the writer's own statement, in a transaction rolled back, and
the refusal's verdict must be the server's: refused exactly when Postgres (or
asyncpg, encoding it) refuses. Against a real Postgres; skipped without
`KS_PG_DSN`.
"""
from __future__ import annotations

import os
from decimal import Decimal

import pytest
import pytest_asyncio

from core import pg_inventory_write as inv
from core.landing_rows import OfferRow

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

OFFER = 9_931_000

INTEGERS = [
    3, 0, -1, 2 ** 31 - 1, -2 ** 31, 2 ** 31, -2 ** 31 - 1,
    3.0, 1.5, -0.7, 2147483647.9, 2147483648.0, -2147483648.9,
    True, False, float("nan"), float("inf"), float("-inf"),
    "3", "two", "", None, [3], {"q": 3},
]
MONEY = [
    10, 0, -5, 10.5, 10.555, True, False, "10.00", "10", " 5 ", "1_000",
    "abc", "", "NaN", float("nan"), float("inf"), "Infinity", None,
    9999999999.99, 9999999999.994, 9999999999.995, 1e10, 10 ** 11, -10 ** 10,
    Decimal("1e30"), [1], {"p": 1},
]
TEXTS = ["SKU-1", "", "NUL\x00", "\ud800", 5, 5.0, True, None, ["a"]]


@pytest_asyncio.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    try:
        yield c
    finally:
        await c.close()


async def _postgres_refuses(conn, sql: str, row: tuple) -> bool:
    tx = conn.transaction()
    await tx.start()
    try:
        await conn.executemany(sql, [row])
        return False
    except Exception:  # noqa: BLE001 — any refusal, asyncpg's or the server's
        return True
    finally:
        await tx.rollback()


STOCK_SQL = inv._upsert(
    "bronze.offer_stocks",
    ("id", "sku", "price", "purchased_price", "quantity", "reserve", "mirrored_at"),
    ("id",))
OFFER_SQL = inv._upsert("bronze.offers", ("id", "product_id", "sku", "synced_at"), ("id",))


def _stock(**over) -> dict:
    row = {"id": OFFER, "sku": "R-1", "price": 10, "purchased_price": 5,
           "quantity": 3, "reserve": 0}
    row.update(over)
    return row


def _stock_tuple(s: dict) -> tuple:
    # What `_write_stock_rows` hands executemany.
    return (s.get("id"), s.get("sku"), s.get("price"), s.get("purchased_price"),
            s.get("quantity", 0), s.get("reserve", 0), None)


def _cases(column: str, values: list) -> list:
    return [pytest.param(column, v, id=f"{column}={v!r}") for v in values]


@pytest.mark.asyncio
@pytest.mark.parametrize("column,value", (
    _cases("id", INTEGERS) + _cases("quantity", INTEGERS) + _cases("reserve", INTEGERS)
    + _cases("price", MONEY) + _cases("purchased_price", MONEY) + _cases("sku", TEXTS)))
async def test_a_stock_value_is_refused_exactly_when_postgres_refuses_it(conn, column, value):
    """Mutation: refuse any float in `_int_refusal` — `quantity=3.0` reads
    refused while Postgres stores 3. Refuse nothing — `quantity=None` and
    `price=1e10` read accepted while Postgres refuses them."""
    stock = _stock(**{column: value})
    # `mirrored_at` is NOT NULL: the writer passes `now()`'s answer.
    row = _stock_tuple(stock)[:-1] + (await conn.fetchval("SELECT now()"),)
    assert (inv.stock_refusal(stock) is not None) == await _postgres_refuses(
        conn, STOCK_SQL, row), inv.stock_refusal(stock)


@pytest.mark.asyncio
@pytest.mark.parametrize("column,value", (
    _cases("id", INTEGERS) + _cases("product_id", INTEGERS) + _cases("sku", TEXTS)))
async def test_an_offer_value_is_refused_exactly_when_postgres_refuses_it(conn, column, value):
    offer = OfferRow(**{"id": OFFER, "product_id": 701, "sku": "R-1", column: value})
    assert (inv.offer_refusal(offer) is not None) == await _postgres_refuses(
        conn, OFFER_SQL, (*offer, None)), inv.offer_refusal(offer)
