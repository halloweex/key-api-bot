"""`refresh_sku_inventory_status` must be all-or-nothing.

`first_seen_at` is carried forward out of the table's own previous contents and
exists nowhere else. As four autocommit statements, a failure after the DELETE
left the table empty and committed, the leftover temp table made every later
call fail on "already exists" until a restart, and the next boot rebuilt from
an empty carry-forward — resetting every SKU's first-seen date to today, in
DuckDB and, one replica tick later, in Postgres.

The failure is injected for real: `offer_stocks.quantity` is nullable and the
target column is NOT NULL, so one bad stock row makes the INSERT…SELECT die
exactly where a schema drift or a kill would — after the DELETE.
"""
from datetime import date
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _store_with_two_offers(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "inv.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute("INSERT INTO offers (id, product_id, sku) VALUES (11, 1, 'A'), (12, 1, 'B')")
        conn.execute(
            "INSERT INTO offer_stocks (id, sku, price, quantity, reserve) "
            "VALUES (11, 'A', 10.00, 5, 0), (12, 'B', 20.00, 3, 1)"
        )
    return store


async def _rows(store):
    async with store.connection() as conn:
        return dict(conn.execute(
            "SELECT offer_id, first_seen_at FROM sku_inventory_status ORDER BY offer_id"
        ).fetchall())


async def _temp_table_exists(store) -> bool:
    async with store.connection() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = '_tmp_first_seen'"
        ).fetchone()[0] > 0


@pytest.mark.asyncio
async def test_a_failed_rebuild_leaves_the_table_and_the_carry_forward_intact(tmp_path):
    store = await _store_with_two_offers(tmp_path)
    assert await store.refresh_sku_inventory_status() == 2
    # A first-seen date that only this table remembers, and one stock row
    # that will make the rebuild's INSERT violate NOT NULL after the DELETE.
    async with store.connection() as conn:
        conn.execute("UPDATE sku_inventory_status SET first_seen_at = DATE '2025-03-01' WHERE offer_id = 11")
        conn.execute("INSERT INTO offer_stocks (id, sku, price, quantity, reserve) VALUES (13, 'C', 1.00, NULL, 0)")

    with pytest.raises(Exception):
        await store.refresh_sku_inventory_status()

    assert await _rows(store) == {11: date(2025, 3, 1), 12: date.today()}, \
        "the DELETE must have been rolled back with the failed INSERT"
    assert not await _temp_table_exists(store), \
        "a leftover temp table used to wedge every later refresh"

    # Nothing to restart: once the bad row is fixed the next call works and
    # still carries the date forward.
    async with store.connection() as conn:
        conn.execute("UPDATE offer_stocks SET quantity = 0 WHERE id = 13")
    assert await store.refresh_sku_inventory_status() == 3
    assert (await _rows(store))[11] == date(2025, 3, 1)


@pytest.mark.asyncio
async def test_a_leftover_temp_table_does_not_block_the_refresh(tmp_path):
    """The old failure mode, should any pre-fix process have left one behind."""
    store = await _store_with_two_offers(tmp_path)
    async with store.connection() as conn:
        conn.execute("CREATE TEMP TABLE _tmp_first_seen AS SELECT 1 AS offer_id, DATE '2020-01-01' AS first_seen_at")

    assert await store.refresh_sku_inventory_status() == 2
    assert not await _temp_table_exists(store)
