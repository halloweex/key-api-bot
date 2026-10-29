"""The catalogue, line items and costs wave 1's tabs read, on the pilot's
orders.

`tests/rust_pilot_fixture.py` lands about forty thousand orders and derives
Silver and Gold with production's own code. Wave 1's tabs read more:
`silver.order_lines`, a view over `bronze.order_products`, `silver.orders`,
`bronze.products` and `bronze.categories` (revision 0011), and
`bronze.offer_stocks`, whose `purchased_price` is every cost on /margin.
This lands those, deterministic, in the shapes production has and the
arithmetic has to survive:

* categories: three roots, their children, a grandchild — margin's
  `root_cat` looks one level up, so it rolls up to its parent — and an
  orphan whose parent does not exist; Latin and Cyrillic names;
* products: brands Latin, Cyrillic, mixed, with an inner space, padded
  (` COSRX ` trims into `COSRX`), blank, spaces only, and NULL; a SKU NULL
  on some and one SKU on two products; a category NULL, one that does not
  exist, the grandchild, the orphan;
* line items: one to three per order, numbered `order_id * 1000 + position`
  as the landing numbers them; one in twenty with no product, one in thirty
  given away at 0;
* costs: per SKU a `purchased_price` NULL, 0, or a price below or above
  what it sold for; one SKU on two offers, so the join fans out — in both
  services alike — and an offer with no SKU;
* promocodes on one order in twelve, Latin and Cyrillic with a space, so
  the header's dropdown has something to offer; Silver is derived again.

Only for a throwaway database: it empties every table it writes.
"""
from __future__ import annotations

import random
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

from tests import rust_pilot_fixture

CATALOGUE_TABLES = ("bronze.offer_stocks", "bronze.order_products",
                    "bronze.products", "bronze.categories")

CATEGORIES: Tuple[Tuple[int, str, Optional[int]], ...] = (
    (1, "Догляд за шкірою", None), (2, "Makeup", None), (3, "Волосся", None),
    (11, "Креми", 1), (12, "Тонери", 1), (13, "Lips", 2), (31, "Шампуні", 3),
    (111, "Нічні креми", 11),     # a grandchild: `root_cat` stops at Креми
    (99, "Orphan", 98),           # its parent does not exist
)
BRANDS = ("COSRX", "Беauty Корея", "Some Brand", "MISSHA", "Етуде Хаус",
          " COSRX ", "", "  ", None)
CATEGORY_OF = (1, 11, 12, 2, 13, 3, 31, 111, 99, 77, None)   # 77: no such category
PRODUCTS = 120
SHARED_SKU = "KS-0001"            # products 1 and 2
FANNED_SKU = "KS-0005"            # two offers
PROMOCODES = ("SALE10", "ЗНИЖКА 15", "vip")


def _products(rng: random.Random) -> List[tuple]:
    rows = []
    for pid in range(1, PRODUCTS + 1):
        sku = None if pid % 19 == 0 else f"KS-{pid:04d}"
        if pid == 2:
            sku = SHARED_SKU
        rows.append((pid, f"Товар {pid}", CATEGORY_OF[pid % len(CATEGORY_OF)],
                     BRANDS[pid % len(BRANDS)], sku, Decimal(rng.randint(10_000, 250_000)) / 100))
    return rows


def _offers(products: List[tuple], rng: random.Random) -> List[tuple]:
    skus = sorted({p[4] for p in products if p[4] is not None})
    rows = []
    for i, sku in enumerate(skus, 1):
        cost = (None if i % 9 == 0 else Decimal(0) if i % 13 == 0
                else Decimal(rng.randint(2_000, 160_000)) / 100)
        rows.append((i, sku, None, cost, rng.randint(0, 40)))
    rows.append((len(rows) + 1, FANNED_SKU, None, Decimal("123.45"), 3))
    rows.append((len(rows) + 1, None, None, Decimal("10.00"), 1))
    return rows


def _lines(order_ids: List[int], rng: random.Random) -> List[tuple]:
    rows = []
    for oid in order_ids:
        for position in range(1, rng.randint(1, 3) + 1):
            product = None if rng.random() < 0.05 else rng.randint(1, PRODUCTS)
            price = (Decimal(0) if rng.random() < 1 / 30
                     else Decimal(rng.randint(5_000, 200_000)) / 100)
            rows.append((oid * 1000 + position, oid, product, f"Позиція {product}",
                         rng.randint(1, 4), price))
    return rows


async def seed(pool) -> Dict[str, object]:
    """The pilot's orders, then the catalogue, line items, costs and
    promocodes, then Silver again."""
    from core.pg_silver import rebuild_silver

    orders = await rust_pilot_fixture.seed(pool)
    rng = random.Random(20261028)
    async with pool.acquire() as conn:
        for table in CATALOGUE_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.copy_records_to_table("categories", schema_name="bronze",
                                         columns=("id", "name", "parent_id"), records=CATEGORIES)
        products = _products(rng)
        await conn.copy_records_to_table(
            "products", schema_name="bronze",
            columns=("id", "name", "category_id", "brand", "sku", "price"), records=products)
        order_ids = [r["id"] for r in await conn.fetch("SELECT id FROM bronze.orders ORDER BY id")]
        lines = _lines(order_ids, rng)
        await conn.copy_records_to_table(
            "order_products", schema_name="bronze",
            columns=("id", "order_id", "product_id", "name", "quantity", "price_sold"),
            records=lines)
        offers = _offers(products, rng)
        await conn.copy_records_to_table(
            "offer_stocks", schema_name="bronze",
            columns=("id", "sku", "price", "purchased_price", "quantity"), records=offers)
        await conn.execute(
            "UPDATE bronze.orders SET promocode = ($1::text[])[1 + (id / 12) % 3] WHERE id % 12 = 0",
            list(PROMOCODES))
    with patch("core.pg.require_revision", new=AsyncMock()):
        await rebuild_silver(pool=pool)
    return {"orders": orders["orders"], "lines": len(lines), "products": len(products),
            "offers": len(offers), "categories": len(CATEGORIES)}


async def clear(pool) -> None:
    async with pool.acquire() as conn:
        for table in CATALOGUE_TABLES:
            await conn.execute(f"DELETE FROM {table}")
    await rust_pilot_fixture.clear(pool)
