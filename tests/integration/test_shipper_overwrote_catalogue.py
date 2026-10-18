"""E1 (`chain_shipper_overwrote`) against chains whose own writer stamps the
mirror's watermark, on a real Postgres.

From 2026-10-10 every morning's comparison paged chain 6's own hourly writes
CRITICAL as a shipment over the catalogue, because the catalogue writer stamps
`meta.mirror_state.last_ok_at` by design (OD-15 (a)) and E1 read any stamp
after the latch as a shipper's. Here the writes go through the repository
methods and the sync's one order call site, and the verdict through
`reconcile_operational` — the run that filed the pages:

- the chain's own writes, categories and products, a second write that
  retires a product, file nothing (the defect, inverted);
- the mirror's own statement after the latch is still CRITICAL, on the table
  it stamped and no other — including a table the chain latched only through
  its sibling;
- after the chain's next write the stamp that was not its own is out of E1's
  sight, and the record's `previous` is that stamp — which is why `previous`
  is never taken for the chain's;
- a record nobody can parse is CRITICAL and says it could not tell;
- the session's DateStyle does not move the verdict;
- chain 3's own write, which stamps three order watermarks and keeps no
  record, files nothing, and its lost marker is still paged.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import chain_latch, pg_catalogue_write as chain
from core import pg_orders_write, write_chains
from core.landing_rows import CATEGORY_COLUMNS, PRODUCT_COLUMNS, category_rows, product_rows

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

PRODUCTS, CATEGORIES = chain.CHAIN_TABLES
ORDER_TABLES = ("bronze.order_products", "bronze.expenses", "bronze.orders",
                "app.order_backfill_misses")
BASE = 990_700


def _product(pid):
    return {"id": pid, "name": f"Product {pid}", "category_id": 10,
            "sku": f"S-{pid}", "price": 100,
            "custom_fields": [{"uuid": "CT_1001", "value": [f"Brand {pid % 3}"]}]}


PAYLOAD = [_product(1), _product(2), _product(3)]
CATS = [{"id": 10, "name": "Care", "parent_id": None}]


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in (*chain.CHAIN_TABLES, *ORDER_TABLES):
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id BETWEEN $1 AND $2",
            BASE, BASE + 99)
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            [*chain.CHAIN_TABLES, *ORDER_TABLES])
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
            "OR key LIKE $1", chain.RECORD_PREFIX + "%")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_MIRROR_LANDING", "1")
    monkeypatch.setattr(chain, "unmet_precondition", lambda: None)
    monkeypatch.setattr(pg_orders_write, "unmet_precondition", lambda: None)
    store = DuckDBStore(db_path=tmp_path / "shipper.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _overwrote(store):
    from core.mirror_reconciliation import reconcile_operational

    issues = await reconcile_operational(store)
    return {i.table_name: i for i in issues if i.check_name == "chain_shipper_overwrote"}


async def _chain_findings(store):
    from core.mirror_reconciliation import reconcile_operational

    return [(i.check_name, i.table_name) for i in await reconcile_operational(store)
            if i.check_name.startswith("chain_")]


async def _ship(table, columns, rows):
    """The sync's mirror past a latch it did not see: `pg_landing._write` is
    the statement `_mirror` runs once it has decided to ship."""
    from core import pg_landing

    await pg_landing._write(table, columns, rows)


async def _last_ok(pool, table):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT last_ok_at FROM meta.mirror_state WHERE table_name = $1", table)


async def _record(pool, table):
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT value FROM meta.chain_watermarks WHERE key = $1",
            chain.record_key(table))
    return chain.parse_record(value)


def _us(moment: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (moment - epoch) // timedelta(microseconds=1)


class TestTheCatalogueChainsOwnWrites:
    @pytest.mark.asyncio
    async def test_file_nothing(self, stores):
        """Production's run 901, reproduced: the latching write, a Sunday
        categories write, and hourly products writes, one of them retiring a
        product. Every watermark is after the latch and every one is the
        chain's."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)             # the latch
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD[:2])         # product 3 retired

        assert chain_latch.latched(chain.CHAIN)
        for table in chain.CHAIN_TABLES:
            last_ok = await _last_ok(pool, table)
            assert _us(last_ok) in (await _record(pool, table)).stamps
        assert await _chain_findings(store) == []

    @pytest.mark.asyncio
    async def test_under_any_datestyle(self, stores):
        """The instant is compared as Postgres's integer, never a rendering —
        a session that renders dates another way reaches the same verdict."""
        from core.mirror_reconciliation import _read_mirror_stamps

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)
        odd = await asyncpg.create_pool(DSN, min_size=1, max_size=1,
                                        server_settings={"DateStyle": "SQL, DMY",
                                                         "TimeZone": "Pacific/Kiritimati"})
        try:
            stamps = await _read_mirror_stamps(odd, await chain_latch.read_owners(pool))
        finally:
            await odd.close()
        products = stamps[PRODUCTS]
        assert products.unreadable is None
        assert products.last_ok_us in products.stamps
        assert products.last_ok_us == _us(await _last_ok(pool, PRODUCTS))


class TestAStampThatIsNotTheChains:
    @pytest.mark.asyncio
    async def test_the_mirror_after_the_latch_is_critical(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)

        await _ship(PRODUCTS, PRODUCT_COLUMNS, product_rows(PAYLOAD))
        shipped = await _last_ok(pool, PRODUCTS)

        found = await _overwrote(store)
        assert set(found) == {PRODUCTS}
        assert found[PRODUCTS].severity.value == "CRITICAL"
        assert shipped.isoformat() in found[PRODUCTS].description
        assert "does not carry" in found[PRODUCTS].description

    @pytest.mark.asyncio
    async def test_on_a_table_latched_through_its_sibling(self, stores):
        """The chain has written only the products; the categories' owner row
        came with them and their record does not exist yet. A categories
        stamp after the latch is nobody's the chain kept."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)

        await _ship(CATEGORIES, CATEGORY_COLUMNS, category_rows(CATS))

        assert set(await _overwrote(store)) == {CATEGORIES}

    @pytest.mark.asyncio
    async def test_is_out_of_sight_after_the_chains_next_write(self, stores):
        """The limit, stated rather than hidden: the next write moves the
        watermark back onto a recorded instant. What is left of the shipment
        is the record's `previous` — which is exactly why `previous` must
        never be read as one of the chain's."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)
        await _ship(PRODUCTS, PRODUCT_COLUMNS, product_rows(PAYLOAD))
        shipped = await _last_ok(pool, PRODUCTS)

        await store.upsert_products(PAYLOAD)

        assert await _overwrote(store) == {}
        record = await _record(pool, PRODUCTS)
        assert record.previous == _us(shipped)
        assert record.previous not in record.stamps

    @pytest.mark.asyncio
    async def test_a_record_nobody_can_parse_is_critical(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE meta.chain_watermarks SET value = 'nonsense' WHERE key = $1",
                chain.record_key(PRODUCTS))

        found = await _overwrote(store)
        assert set(found) == {PRODUCTS}
        assert "cannot be read" in found[PRODUCTS].description


def _order(oid):
    t = "2026-09-20T12:00:00+00:00"
    return {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": t, "created_at": t, "updated_at": t,
        "buyer": {"id": 5401}, "manager": {"id": 4}, "manager_comment": None,
        "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 700}}],
        "expenses": [{"id": oid * 10, "expense_type_id": 1, "amount": 10.5,
                      "status": "paid"}],
    }


class TestTheOrdersChainStampsAndKeepsNoRecord:
    @pytest.mark.asyncio
    async def test_its_own_write_files_nothing(self, stores):
        """One page of orders with their costs through the sync's call site:
        three order watermarks move after the latch, and that is the chain's
        normal state, not a shipment."""
        from core.sync_service import SyncService

        store, pool, env = stores
        env.setenv(pg_orders_write.WRITE_ENV, "postgres")
        await SyncService(store)._upsert_orders_with_expenses(
            [_order(BASE)], changed_ids_out=[])

        owners = await chain_latch.read_owners(pool)
        since = datetime.fromisoformat(owners["bronze.orders"])
        for table in pg_orders_write.CHAIN_STAMPS_MIRROR_STATE:
            assert await _last_ok(pool, table) > since, (
                f"{table}'s watermark did not move: the declaration is wrong")
        assert await _chain_findings(store) == []

    @pytest.mark.asyncio
    async def test_its_lost_marker_is_still_paged(self, stores):
        from core.sync_service import SyncService

        store, _pool, env = stores
        env.setenv(pg_orders_write.WRITE_ENV, "postgres")
        await SyncService(store)._upsert_orders_with_expenses(
            [_order(BASE)], changed_ids_out=[])
        chain_latch.release(pg_orders_write.CHAIN)

        assert ("chain_latch_disagrees", pg_orders_write.CHAIN) in await _chain_findings(store)
