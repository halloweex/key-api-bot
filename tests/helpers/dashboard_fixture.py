"""The dashboard's original fixture in Postgres: Bronze seeded, Silver and Gold
derived.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest_asyncio

from core.duckdb_constants import B2B_MANAGER_ID
from tests.integration.pg_tab_seed import (
    derive, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)


DSN = os.getenv("KS_PG_DSN")


# Pinned (step 14, S14-07) for the search index and the assistant's tools,
# whose answers are recorded as literals from DuckDB on `both_engines`.
TODAY = date(2026, 9, 15)


WINDOW = (TODAY - timedelta(days=30), TODAY)


CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Ampoules", 1), (4, "Makeup", None)]


PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Ampoule", 3, "BrandB", "SKU-B", 100.0),
    (300, "Lipstick", 4, None, "SKU-C", 250.0),
    (400, "Toner", 2, "BrandA", "SKU-D", 200.0),   # BrandA's second product
]


# (buyer_id, full_name, phone). Order 3 below points at buyer 999, which is in
# neither store — that is the LEFT JOIN miss, and it is the portable way to
# make one.
#
# A buyer row with a NULL name would have been the obvious way, and it is not
# available: `buyers.full_name` is NOT NULL in DuckDB and nullable in
# `bronze.buyers`. That divergence is real and currently latent — measured on
# production 2026-09-12: zero NULL names, zero NULL manager names, zero orders
# pointing at a buyer that is not there. It matters the day KeyCRM serves one,
# because DuckDB would reject the row and Postgres accept it, and the two
# stores would then hold different numbers of buyers.
BUYERS = [(7, "Олена", "+380501112233"), (8, "Ігор", None)]


# (manager_id, name)
MANAGERS = [(B2B_MANAGER_ID, "Wholesale")]


# (order_id, source_id, grand_total, days_ago, is_return, manager_id, buyer_id)
ORDERS = [
    (1, 1, 1200.0, 2, False, None, 7),
    (2, 2,  600.0, 3, False, None, 7),
    (3, 1,  400.0, 4, True,  None, 999),    # a return whose buyer is in neither store
    (4, 3,  900.0, 5, False, None, 7),      # source 3 retired — reaches nothing
    (5, 4,  750.0, 6, False, None, 8),      # NO LINE ITEMS, and revenue
    (6, 1,  300.0, 7, False, B2B_MANAGER_ID, 7),   # b2b, and a named manager
    (7, 2,  500.0, 8, True,  B2B_MANAGER_ID, 7),   # a b2b return
]


# (line_id, order_id, product_id, quantity, price_sold, sold_name)
#
# `sold_name` is `order_products.name` — the name at the moment of the order,
# which is not the catalogue's. Product 100 is sold under two of them, which is
# the production shape: 193 of 268 products carry more than one name over
# thirty days, and grouping on it split each of them into several rows.
#
# Order 1 carries two BrandA lines (100 and 400). Summing Gold's per-product
# cells would count that order twice for BrandA; `COUNT(DISTINCT order_id)`
# counts it once, and the difference is what was 35.2 % on production.
LINES = [
    (1, 1, 100, 2, 500.0, "Serum"),
    (2, 1, 200, 2, 100.0, "Ampoule"),
    (3, 1, 400, 1, 200.0, "Toner"),          # same order, same brand as line 1
    (4, 2, 100, 1, 500.0, "Serum — renamed"),  # the same product, another name
    (5, 3, 200, 1, 100.0, "Ampoule"),
    (6, 4, 300, 4, 250.0, "Lipstick"),
    (7, 6, 200, 3, 100.0, "Ampoule"),
    (8, 7, 100, 1, 500.0, "Serum"),
]


async def _populate_original(conn, pool):
    """The original seven orders, as KeyCRM lands them, derived by Postgres."""
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS, buyers=BUYERS,
        managers=MANAGERS,
        orders=[(oid, src, 19 if is_ret else 1, total, NOW - timedelta(days=days),
                 buyer, manager, None)
                for oid, src, total, days, is_ret, manager, buyer in ORDERS],
        lines=[(lid, oid, pid, sold, qty, price)
               for lid, oid, pid, qty, price, sold in LINES])
    await derive(pool)


@pytest_asyncio.fixture
async def original_in_postgres(tmp_path, monkeypatch):
    """The original fixture's store, never connected: its DuckDB connection is
    a trap, so an answer can only be Postgres's. The search index and the
    assistant's tools read it (step 14, S14-07)."""
    async with tab_in_postgres(DSN, monkeypatch, _populate_original):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the index")


def _comparable(value):
    """Money to six places. DuckDB divides a DECIMAL into a DOUBLE and
    Postgres into a NUMERIC, so the two legitimately differ in the last bits —
    far below anything this tab renders, far above the noise.

    No test compares two engines any more; kept for
    `tests/integration/test_chat_tools_two_engines.py`, which imports it."""
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _comparable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_comparable(v) for v in value]
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)   # midday Kyiv
