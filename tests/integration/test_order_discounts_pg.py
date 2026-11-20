"""The discount columns on a real Postgres (revision 0038).

What only a server can say: the sync's writer lands every figure with the
order and its lines, the JSONB included; rewriting an order whose discount
alone moved archives no version; the statement an image at 0037 or below
sends still runs against the wider table and leaves the discount as it was;
and the backfill (`pg_orders_write.restore_discounts`) fills an order landed
before 0038 without moving anything else, leaves an order in another version
or whose lines differ to the sync, and writes nothing the second time.

Synthetic orders only, in an id range of their own.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 990_700
T0 = datetime(2026, 11, 16, 10, 0, tzinfo=timezone.utc)

# The header statement every image below this revision sends, at 0037 as at
# 0036 (it did not change between them) — `pg_landing._ORDER_UPSERT` before
# this revision, frozen: what a rollback below 0038 runs against the wider
# table.
UPSERT_BEFORE_0038 = """
INSERT INTO bronze.orders
       (id, source_id, status_id, status_group_id, grand_total,
        ordered_at, created_at, updated_at, buyer_id, manager_id,
        manager_comment, promocode)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (id) DO UPDATE SET
    source_id       = EXCLUDED.source_id,
    status_id       = EXCLUDED.status_id,
    status_group_id = EXCLUDED.status_group_id,
    grand_total     = EXCLUDED.grand_total,
    ordered_at      = EXCLUDED.ordered_at,
    created_at      = EXCLUDED.created_at,
    updated_at      = EXCLUDED.updated_at,
    buyer_id        = EXCLUDED.buyer_id,
    manager_id      = EXCLUDED.manager_id,
    manager_comment = COALESCE(EXCLUDED.manager_comment, bronze.orders.manager_comment),
    promocode       = EXCLUDED.promocode,
    mirrored_at     = now()
"""

ORDER_DISCOUNT = ("products_total", "total_discount", "discount_amount",
                  "discount_percent", "discount_data")


def _payload(oid, *, updated=T0, status=1, lines=2, discount=True, **order):
    p = {
        "id": oid, "source_id": 4, "status_id": status, "status_group_id": 1,
        "grand_total": "3676.00", "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": "utm_source=ig", "promocode": "AUTUMN10",
        "products": [{"name": f"Товар {i}", "quantity": 2, "price_sold": "591.00",
                      "offer": {"product_id": 700 + i}} for i in range(lines)],
    }
    if discount:
        p.update(products_total="4085.00", total_discount=409, discount_percent=10,
                 discount_amount=0,
                 discount_data={"loyalty": {"name": "Клуб", "discount": 5}})
        for line in p["products"]:
            line.update(price="980.00", total_discount="389.00",
                        discount_percent="39.6939", discount_amount=0,
                        loyalty_discount=5, individual_discount=0)
    p.update(order)
    return p


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.order_products WHERE order_id BETWEEN $1 AND $2",
                           BASE, BASE + 99)
        await conn.execute("DELETE FROM bronze.orders WHERE id BETWEEN $1 AND $2",
                           BASE, BASE + 99)
        await conn.execute("DELETE FROM app.order_versions WHERE order_id BETWEEN $1 AND $2",
                           BASE, BASE + 99)
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


async def _header(pool, oid):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM bronze.orders WHERE id = $1", oid)


async def _lines(pool, oid):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM bronze.order_products WHERE order_id = $1 ORDER BY id", oid)


async def _versions(pool, oid):
    async with pool.acquire() as conn:
        return [r["kind"] for r in await conn.fetch(
            "SELECT kind FROM app.order_versions WHERE order_id = $1 ORDER BY id", oid)]


@pytest.mark.asyncio
async def test_the_sync_lands_every_figure_with_the_order(pool):
    oid = BASE + 1
    await chain.upsert_orders_with_expenses([_payload(oid)])
    head = await _header(pool, oid)
    assert head["products_total"] == Decimal("4085.00")
    assert head["total_discount"] == Decimal("409.00")
    assert head["discount_percent"] == Decimal("10.0000")
    assert json.loads(head["discount_data"]) == {"loyalty": {"name": "Клуб", "discount": 5}}
    lines = await _lines(pool, oid)
    assert len(lines) == 2
    for line in lines:
        assert line["price"] == Decimal("980.00")
        assert line["total_discount"] == Decimal("389.0000")
        assert line["price"] - line["price_sold"] == line["total_discount"]
        assert line["loyalty_discount"] == Decimal("5.0000")


@pytest.mark.asyncio
async def test_a_discount_that_alone_moved_archives_no_version(pool):
    """Kills: a discount column in `VERSIONED_COLUMNS` — on the sync it is a
    version per discount edit; on the backfill, one per order."""
    oid = BASE + 2
    await chain.upsert_orders_with_expenses([_payload(oid)])
    later = _payload(oid, updated=T0 + timedelta(minutes=5), total_discount=0,
                     discount_data=[])
    result, _ = await chain.upsert_orders_with_expenses([later])
    assert result.changed_ids == [oid]
    head = await _header(pool, oid)
    assert head["total_discount"] == Decimal("0.00") and head["discount_data"] is None
    assert await _versions(pool, oid) == ["create"]


@pytest.mark.asyncio
async def test_an_image_at_0037_still_writes_and_leaves_the_discount(pool):
    """A rollback below 0038 keeps the columns (the downgrade drops nothing),
    and the older writer names its columns one by one. Kills: a column made
    NOT NULL or given a default, which the older INSERT would trip over."""
    oid = BASE + 3
    await chain.upsert_orders_with_expenses([_payload(oid)])
    async with pool.acquire() as conn:
        await conn.execute(UPSERT_BEFORE_0038, oid, 4, 12, 4, Decimal("3676.00"),
                           T0, T0, T0 + timedelta(hours=1), 5401, 4, None, None)
        await conn.execute(UPSERT_BEFORE_0038, oid + 1, 4, 1, 1, Decimal("10.00"),
                           T0, T0, T0, None, None, None, None)
    head = await _header(pool, oid)
    assert head["status_id"] == 12 and head["total_discount"] == Decimal("409.00")
    fresh = await _header(pool, oid + 1)
    assert all(fresh[c] is None for c in ORDER_DISCOUNT)


async def _landed_before_0038(oid, **kw):
    """An order as the sync landed it before this revision: no figures."""
    await chain.upsert_orders_with_expenses([_payload(oid, discount=False, **kw)])


@pytest.mark.asyncio
async def test_the_backfill_fills_an_order_landed_before_and_nothing_else(pool):
    oid = BASE + 10
    await _landed_before_0038(oid)
    before = await _header(pool, oid)
    assert all(before[c] is None for c in ORDER_DISCOUNT)
    # KeyCRM moved the status without moving `updated_at` — the backfill must
    # not carry it in: that is the sync's and the reconciliation's.
    result = await chain.restore_discounts([_payload(oid, status=12)])
    assert (result.headers_written, result.lines_written) == (1, 1)
    after = await _header(pool, oid)
    assert after["total_discount"] == Decimal("409.00")
    for column in ("status_id", "grand_total", "updated_at", "manager_comment",
                   "buyer_id", "promocode"):
        assert after[column] == before[column], column
    assert all(line["total_discount"] == Decimal("389.0000")
               for line in await _lines(pool, oid))
    assert await _versions(pool, oid) == ["create"]

    again = await chain.restore_discounts([_payload(oid, status=12)])
    assert again.headers_written == 0 and again.already_current == (oid,)
    assert (await _header(pool, oid))["mirrored_at"] == after["mirrored_at"]


@pytest.mark.asyncio
async def test_the_backfill_leaves_another_version_and_other_lines(pool):
    moved, absent, other_lines = BASE + 20, BASE + 21, BASE + 22
    await _landed_before_0038(moved, updated=T0 + timedelta(minutes=1))
    await _landed_before_0038(other_lines, lines=1)
    result = await chain.restore_discounts(
        [_payload(moved), _payload(absent), _payload(other_lines, lines=2)])
    assert result.moved == (moved,) and result.absent == (absent,)
    assert result.lines_differ == (other_lines,)
    assert (await _header(pool, moved))["total_discount"] is None
    assert await _header(pool, absent) is None
    assert (await _header(pool, other_lines))["total_discount"] == Decimal("409.00")
    (line,) = await _lines(pool, other_lines)
    assert line["price"] is None


@pytest.mark.asyncio
async def test_the_backfill_waits_for_the_sync_s_lock_and_then_steps_aside(pool):
    """A sync writing a newer version holds the row; the backfill's locked
    read waits for it, then finds the version moved and writes nothing.
    Kills: the read inside the transaction taken without `FOR UPDATE`."""
    import asyncio

    oid = BASE + 30
    await _landed_before_0038(oid)
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        await conn.execute("SELECT id FROM bronze.orders WHERE id = $1 FOR UPDATE", oid)
        await conn.execute("UPDATE bronze.orders SET updated_at = $2 WHERE id = $1",
                           oid, T0 + timedelta(minutes=9))
        # The unlocked plan sees the committed version and asks for the lock.
        task = asyncio.create_task(chain.restore_discounts([_payload(oid)]))
        await asyncio.sleep(0.5)
        assert not task.done()
        await tx.commit()
    result = await task
    assert result.headers_written == 0 and result.moved == (oid,)
    assert (await _header(pool, oid))["total_discount"] is None


class TestTheRevisionOnAServer:
    """0038 as Alembic renders it and the server runs it, inside a transaction
    that is always rolled back."""

    @pytest.mark.asyncio
    async def test_the_downgrade_keeps_the_columns_and_the_upgrade_is_a_no_op(self):
        """Down to 0037 from the head, through 0038: the stamp moves,
        `require_revision` at 0037 passes, the eleven columns and their values
        stay. Up again: the stamp is the head and nothing moved. Kills: a DROP
        in the downgrade, an IF NOT EXISTS missing from the upgrade. (The head
        is 0039 since the promotions calendar descended from 0038.)"""
        from core import pg
        from tests.integration.test_analytics_captures_pg import _alembic_sql

        down_to, here = "0037_frozen_month_goals", pg.REQUIRED_REVISION
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute(
                "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                "total_discount) VALUES ($1, 4, 1, 1, 409)", BASE + 90)
            await conn.execute(_alembic_sql("downgrade", f"{here}:{down_to}"))
            assert await conn.fetchval("SELECT version_num FROM meta.alembic_version") == down_to
            await pg.require_revision(down_to, conn=conn)
            assert await conn.fetchval(
                "SELECT total_discount FROM bronze.orders WHERE id = $1", BASE + 90) == 409
            assert await conn.fetchval(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema = 'bronze' AND table_name IN ('orders', 'order_products') "
                "AND column_name IN ('products_total', 'total_discount', 'discount_amount', "
                "'discount_percent', 'discount_data', 'price', 'loyalty_discount', "
                "'individual_discount')") == 11   # three names on both tables
            await conn.execute(_alembic_sql("upgrade", f"{down_to}:{here}"))
            assert await conn.fetchval("SELECT version_num FROM meta.alembic_version") == here
            assert await conn.fetchval(
                "SELECT total_discount FROM bronze.orders WHERE id = $1", BASE + 90) == 409
        finally:
            await tx.rollback()
            await conn.close()
