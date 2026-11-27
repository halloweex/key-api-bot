"""The /inventory status rebuild against a real Postgres (chain 1).

`rebuild_sku_inventory_status` replaces `app.sku_inventory_status` whole, one
row per offer in stock, from the offers, the orders and the stock movements.
Four rules inside it decide what the /inventory tab shows, and each is easy to
"tidy" away:

* `first_seen_at` is carried from the table's own previous contents, since
  nothing else records when a SKU was first seen; a new offer takes its first
  order's date, or today;
* "last sold" counts every order but four cancelled states (19, 21, 22, 23),
  so 15 and 18 count although revenue leaves them out;
* `last_stock_out_at` comes from `stock_out` movements alone;
* an offer with no SKU is keyed by its id.

Every date is a Kyiv date, today included: the pool's sessions run in a zone
whose date differs from Kyiv's, so a rebuild that read the session's date
(`CURRENT_DATE`) would date a new offer wrongly.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import DSN, needs_postgres

asyncpg = pytest.importorskip("asyncpg")

pytestmark = needs_postgres

KYIV = ZoneInfo("Europe/Kyiv")

OLD, SOLD, NEW, NO_SKU = 9_920_001, 9_920_002, 9_920_003, 9_920_004    # offer ids
PRODUCT = {OLD: 9_920_101, SOLD: 9_920_102, NEW: 9_920_103, NO_SKU: 9_920_104}
SKU = {OLD: "B2-OLD", SOLD: "B2-SOLD", NEW: "B2-NEW", NO_SKU: None}


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# `(order id, status_id, ordered_at, product)`
ORDERS = [
    (9_920_201, 1, _utc(2026, 3, 1, 9, 0), PRODUCT[OLD]),
    (9_920_202, 1, _utc(2026, 4, 1, 9, 0), PRODUCT[SOLD]),
    # 15 is in the lost/cancel group revenue excludes, and still a sale here.
    # 22:30 UTC on 10 May is 11 May in Kyiv.
    (9_920_203, 15, _utc(2026, 5, 10, 22, 30), PRODUCT[SOLD]),
    # 19 is one of the four cancelled states "last sold" leaves out.
    (9_920_204, 19, _utc(2026, 6, 20, 9, 0), PRODUCT[SOLD]),
]
# An older order of the old offer's product that lands after the first
# rebuild, as a backfill would land it.
LATE = (9_920_205, 1, _utc(2025, 12, 1, 9, 0), PRODUCT[OLD])
ORDER_IDS = [o[0] for o in ORDERS] + [LATE[0]]

# `(offer, movement_type, recorded_at)`
MOVEMENTS = [
    (OLD, "initial", _utc(2026, 1, 5, 9, 0)),
    (OLD, "stock_out", _utc(2026, 6, 1, 9, 0)),
    # 21:30 UTC on 1 July is 2 July in Kyiv.
    (OLD, "stock_out", _utc(2026, 7, 1, 21, 30)),
    (OLD, "stock_in", _utc(2026, 8, 1, 9, 0)),
    (OLD, "reserve_change", _utc(2026, 8, 5, 9, 0)),
]

# Emptied for the test and put back after it: the rebuild reads every offer in
# stock and replaces the status table whole.
WHOLE = ("app.sku_inventory_status", "bronze.offer_stocks", "bronze.offers")


def _far_from_kyiv() -> str:
    """A zone whose date is not Kyiv's at this moment. UTC+14 and UTC-12 are
    each a day off Kyiv for over half of its day, so one of them always is."""
    today = datetime.now(KYIV).date()
    for zone in ("Pacific/Kiritimati", "Etc/GMT+12"):
        if datetime.now(ZoneInfo(zone)).date() != today:
            return zone
    raise AssertionError("no zone's date differs from Kyiv's")


async def _orders(conn, orders) -> None:
    await conn.executemany(
        "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, ordered_at)"
        " VALUES ($1, 1, $2, 100, $3)", [(oid, status, at) for oid, status, at, _p in orders])
    await conn.executemany(
        "INSERT INTO bronze.order_products (id, order_id, product_id, name, quantity, price_sold)"
        " VALUES ($1, $2, $3, 'cream', 1, 100)",
        [(oid * 1000 + 1, oid, product) for oid, _s, _a, product in orders])


async def _seed(conn) -> None:
    await conn.executemany(
        "INSERT INTO bronze.products (id, name, brand) VALUES ($1, $2, 'B2 Brand')",
        [(product, f"product {product}") for product in PRODUCT.values()])
    await conn.executemany(
        "INSERT INTO bronze.offers (id, product_id, sku, synced_at) VALUES ($1, $2, $3, now())",
        [(offer, PRODUCT[offer], SKU[offer]) for offer in PRODUCT])
    await conn.executemany(
        "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price, quantity, reserve)"
        " VALUES ($1, $2, 300, 150, 4, 1)", [(offer, SKU[offer]) for offer in PRODUCT])
    await _orders(conn, ORDERS)
    await conn.executemany(
        "INSERT INTO app.stock_movements (offer_id, product_id, movement_type,"
        " quantity_before, quantity_after, delta, reserve_before, reserve_after,"
        " recorded_at, source) VALUES ($1, $2, $3, 0, 0, 0, 0, 0, $4, 'sync')",
        [(offer, PRODUCT[offer], kind, at) for offer, kind, at in MOVEMENTS])


async def _put_back(conn, table: str, rows) -> None:
    for row in rows:
        columns = list(row.keys())
        await conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
            f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})", *row.values())


@pytest_asyncio.fixture
async def inventory():
    """The seed, and a pool whose sessions run far from Kyiv standing in for
    `core.pg.get_pool`. Afterwards the emptied tables hold what they held,
    the seeded rows are gone, and chain 1's owner rows are as they were."""
    from core import chain_latch
    from core.pg_inventory_write import CHAIN_TABLES

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2,
                                     server_settings={"timezone": _far_from_kyiv()})
    owner_keys = [chain_latch.owner_key(table) for table in CHAIN_TABLES]
    async with pool.acquire() as conn:
        kept = {table: await conn.fetch(f"SELECT * FROM {table}") for table in WHOLE}
        owners = await conn.fetch(
            "SELECT * FROM meta.chain_watermarks WHERE key = ANY($1)", owner_keys)
        for table in WHOLE:
            await conn.execute(f"DELETE FROM {table}")
        await _seed(conn)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM app.stock_movements WHERE offer_id = ANY($1::int[])", list(PRODUCT))
                await conn.execute(
                    "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", ORDER_IDS)
                await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", ORDER_IDS)
                await conn.execute(
                    "DELETE FROM bronze.products WHERE id = ANY($1::int[])", list(PRODUCT.values()))
                for table in WHOLE:
                    await conn.execute(f"DELETE FROM {table}")
                    await _put_back(conn, table, kept[table])
                await conn.execute(
                    "DELETE FROM meta.chain_watermarks WHERE key = ANY($1)", owner_keys)
                await _put_back(conn, "meta.chain_watermarks", owners)
        finally:
            await pool.close()


async def _status(pool) -> dict:
    async with pool.acquire() as conn:
        return {r["offer_id"]: r for r in await conn.fetch(
            "SELECT * FROM app.sku_inventory_status")}


def _column(status: dict, name: str) -> dict:
    return {offer: status[offer][name] for offer in PRODUCT}


class TestTheStatusRebuild:
    @pytest.mark.asyncio
    async def test_first_seen_survives_a_rebuild_and_a_new_offer_is_dated_today_in_kyiv(
            self, inventory):
        """Mutation: drop the carry-forward from the COALESCE, and the old
        offer is re-dated from the order that landed later."""
        from core.pg_inventory_write import rebuild_sku_inventory_status

        today = datetime.now(KYIV).date()
        expected = {OLD: date(2026, 3, 1), SOLD: date(2026, 4, 1), NEW: today, NO_SKU: today}

        assert await rebuild_sku_inventory_status() == len(PRODUCT)
        assert _column(await _status(inventory), "first_seen_at") == expected

        async with inventory.acquire() as conn:
            await _orders(conn, [LATE])
        assert await rebuild_sku_inventory_status() == len(PRODUCT)
        assert _column(await _status(inventory), "first_seen_at") == expected

    @pytest.mark.asyncio
    async def test_last_sale_last_stock_out_and_the_sku_fallback(self, inventory):
        from core.pg_inventory_write import rebuild_sku_inventory_status

        assert await rebuild_sku_inventory_status() == len(PRODUCT)
        status = await _status(inventory)

        assert _column(status, "last_sale_date") == {
            OLD: date(2026, 3, 1), SOLD: date(2026, 5, 11), NEW: None, NO_SKU: None}
        assert _column(status, "last_stock_out_at") == {
            OLD: date(2026, 7, 2), SOLD: None, NEW: None, NO_SKU: None}
        assert _column(status, "sku") == {
            OLD: "B2-OLD", SOLD: "B2-SOLD", NEW: "B2-NEW", NO_SKU: str(NO_SKU)}
