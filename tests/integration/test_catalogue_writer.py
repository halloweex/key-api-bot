"""Chain 6, the catalogue, written to a real Postgres and carried back.

Everything goes through `DuckDBStore.upsert_products` / `upsert_categories` —
the only path the catalogue is written by — so the routing is what is proved,
not the writer called on its own. The chain is the registered one; its
precondition (chain 1, the fallback, every warehouse reader) is taken as met
here and proved in `tests/unit/test_catalogue_chain.py`.

What is proved, in the design's numbering:

- I-1 one write is one transaction and one instant: owner rows, rows and the
  watermark share an `xmin`, and every row carries `last_ok_at` exactly;
- I-2 a product the next write does not carry is retired, not deleted;
- I-3 a payload naming an id twice: the last wins, `last_rows` counts it once;
- I-7 two full writes at once, in opposite orders, do not deadlock, and the
  later one's instant is on every row;
- I-8 planted defects reach the standing watch with their names;
- I-4/I-5 the way there and back: the pre-flip handover refuses a retired
  product until it is carried, then the flip, then the copy-back releases with
  the retired product in DuckDB.

The catalogue tables are emptied around each test, with the owner rows and the
two watermark rows.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import chain_latch, pg_catalogue_write as chain
from core import pg_chain_invariants as inv
from core import write_chains
from core.landing_rows import product_rows

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

PRODUCTS, CATEGORIES = chain.CHAIN_TABLES


def _product(pid, name=None, price=100):
    return {"id": pid, "name": name or f"Product {pid}", "category_id": 10,
            "sku": f"S-{pid}", "price": price,
            "custom_fields": [{"uuid": "CT_1001", "value": [f"Brand {pid % 3}"]}]}


PAYLOAD = [_product(1), _product(2), _product(3)]
CATS = [{"id": 10, "name": "Care", "parent_id": None}]


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in chain.CHAIN_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                           list(chain.CHAIN_TABLES))
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
            "OR key = ANY($1::text[])", list(chain.CHAIN_SYNC_KEYS))


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_MIRROR_LANDING", "1")
    monkeypatch.setattr(chain, "unmet_precondition", lambda: None)
    store = DuckDBStore(db_path=tmp_path / "catalogue.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _pg(pool, sql, *params):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *params)


async def _duck(store, sql, params=None):
    async with store.connection() as conn:
        return conn.execute(sql, params or []).fetchall()


async def _state(pool, table=PRODUCTS):
    rows = await _pg(pool, "SELECT last_ok_at, last_rows, xmin::text AS x "
                           "FROM meta.mirror_state WHERE table_name = $1", table)
    return rows[0] if rows else None


async def _facts(pool):
    return await inv.read_facts(pool=pool)


def _names(issues):
    return {(i.check_name, i.table_name, i.severity.value) for i in issues}


class TestTheWriter:
    @pytest.mark.asyncio
    async def test_one_write_is_one_transaction_and_one_instant(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        # Id 2 twice, renamed the second time: the last wins and is counted once.
        payload = PAYLOAD + [_product(2, "Product 2 renamed")]

        assert await store.upsert_products(payload) == 3

        assert await _duck(store, "SELECT COUNT(*) FROM products") == [(0,)], (
            "DuckDB was written under the flag")
        assert chain_latch.latched(chain.CHAIN)
        stamp = chain_latch.latched_at(chain.CHAIN)
        assert await chain_latch.read_owners(pool) == {t: stamp for t in chain.CHAIN_TABLES}

        rows = await _pg(pool, "SELECT id, name, brand, price, mirrored_at, xmin::text AS x "
                               "FROM bronze.products ORDER BY id")
        assert [(r["id"], r["name"]) for r in rows] == [
            (1, "Product 1"), (2, "Product 2 renamed"), (3, "Product 3")]
        assert rows[0]["brand"] == "Brand 1"
        state = await _state(pool)
        owners = await _pg(pool, "SELECT updated_at, xmin::text AS x FROM meta.chain_watermarks "
                                 "WHERE key LIKE 'owner:%'")
        # One transaction: the owner rows, the rows and the watermark.
        assert {r["x"] for r in rows} | {o["x"] for o in owners} | {state["x"]} == {rows[0]["x"]}
        # One instant: every row the write carried is exactly `last_ok_at`.
        assert {r["mirrored_at"] for r in rows} == {state["last_ok_at"]}
        assert {o["updated_at"] for o in owners} == {state["last_ok_at"]}
        assert state["last_rows"] == 3

    @pytest.mark.asyncio
    async def test_a_product_the_next_write_leaves_out_is_retired_not_deleted(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)
        first = (await _state(pool))["last_ok_at"]

        await store.upsert_products(PAYLOAD[:2] + [_product(4)])

        state = await _state(pool)
        assert state["last_ok_at"] > first and state["last_rows"] == 3
        rows = {r["id"]: r["mirrored_at"] for r in await _pg(
            pool, "SELECT id, mirrored_at FROM bronze.products")}
        assert set(rows) == {1, 2, 3, 4}, "the writer deleted a product"
        assert rows[3] == first < state["last_ok_at"]
        assert {rows[i] for i in (1, 2, 4)} == {state["last_ok_at"]}

        issues = inv.check_chain_invariants(await _facts(pool))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, PRODUCTS, "INFO")}
        (retired,) = issues
        assert retired.sample_ids == (3,)

    @pytest.mark.asyncio
    async def test_two_full_writes_at_once_neither_deadlock_nor_mix_instants(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        many = [_product(i) for i in range(1, 201)]
        await store.upsert_categories(CATS)
        await store.upsert_products(many)            # the latch, out of the race

        await asyncio.wait_for(asyncio.gather(
            chain.upsert_products(list(reversed(product_rows(many)))),
            chain.upsert_products(product_rows(many)),
        ), timeout=30)

        state = await _state(pool)
        stamps = {r["mirrored_at"] for r in await _pg(
            pool, "SELECT mirrored_at FROM bronze.products")}
        assert stamps == {state["last_ok_at"]}, "the later write's instant is not on every row"
        assert inv.check_chain_invariants(await _facts(pool)) == []

    @pytest.mark.asyncio
    async def test_a_write_takes_its_row_locks_in_id_order(self, stores):
        """Why two overlapping full writes cannot deadlock, shown without
        relying on a race (the test above can finish one write before the
        other starts): a write handed the catalogue in reverse still locks
        row 1 first. A session holding row 1 stops it there, before it has
        locked anything else — so the highest id is still free to take."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        many = [_product(i) for i in range(1, 51)]
        await store.upsert_categories(CATS)
        await store.upsert_products(many)            # the latch, out of the way

        async with pool.acquire() as holder:
            tx = holder.transaction()
            await tx.start()
            write = None
            try:
                await holder.execute("SELECT 1 FROM bronze.products WHERE id = 1 FOR UPDATE")
                write = asyncio.ensure_future(
                    chain.upsert_products(list(reversed(product_rows(many)))))
                for _ in range(200):
                    (waiting,) = await _pg(
                        pool, "SELECT count(*) AS n FROM pg_stat_activity "
                              "WHERE datname = current_database() "
                              "AND wait_event_type = 'Lock'")
                    if waiting["n"]:
                        break
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail("the write never came to wait on row 1")
                free = await holder.fetchval(
                    "SELECT count(*) FROM (SELECT 1 FROM bronze.products "
                    "WHERE id = 50 FOR UPDATE SKIP LOCKED) s")
                assert free == 1, (
                    "the write locked id 50 before id 1: two writes handed one "
                    "catalogue in different orders can deadlock")
            finally:
                await tx.rollback()
                if write is not None:
                    assert await asyncio.wait_for(write, timeout=30) == 50

    @pytest.mark.asyncio
    async def test_the_watermarks_move_with_the_chain(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        stamp = datetime(2026, 9, 30, 9, 0, tzinfo=timezone.utc)
        await store.set_last_sync_time("products", stamp)
        await store.set_last_sync_time("categories", stamp)

        assert await store.get_last_sync_time("products") == stamp
        keys = {r["key"] for r in await _pg(pool, "SELECT key FROM meta.chain_watermarks")}
        assert set(chain.CHAIN_SYNC_KEYS) <= keys
        assert await _duck(store, "SELECT COUNT(*) FROM sync_metadata WHERE key LIKE "
                                  "'last_sync_%'") == [(0,)]

    @pytest.mark.asyncio
    async def test_categories_are_written_the_same_way(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        cats = [{"id": 10, "name": "Care", "parent_id": None},
                {"id": 11, "name": "Serums", "parent_id": 10}]

        assert await store.upsert_categories(cats) == 2

        state = await _state(pool, CATEGORIES)
        rows = await _pg(pool, "SELECT id, parent_id, mirrored_at FROM bronze.categories "
                               "ORDER BY id")
        assert [(r["id"], r["parent_id"]) for r in rows] == [(10, None), (11, 10)]
        assert {r["mirrored_at"] for r in rows} == {state["last_ok_at"]}
        assert state["last_rows"] == 2


class TestPlantedDefectsReachTheWatch:
    @pytest_asyncio.fixture
    async def written(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)
        await store.upsert_categories([{"id": 10, "name": "Care", "parent_id": None}])
        assert inv.check_chain_invariants(await _facts(pool)) == []
        return pool

    @pytest.mark.asyncio
    async def test_a_deleted_served_row_is_lost(self, written):
        async with written.acquire() as conn:
            await conn.execute("DELETE FROM bronze.products WHERE id = 2")
        issues = inv.check_chain_invariants(await _facts(written))
        assert _names(issues) == {(inv.CATALOGUE_ROWS_LOST, PRODUCTS, "CRITICAL")}

    @pytest.mark.asyncio
    async def test_a_row_written_round_the_chain_is_named(self, written):
        async with written.acquire() as conn:
            await conn.execute("UPDATE bronze.products SET name = 'edited', "
                               "mirrored_at = now() + interval '1 second' WHERE id = 3")
        issues = inv.check_chain_invariants(await _facts(written))
        assert _names(issues) == {(inv.CATALOGUE_WRITTEN_AROUND, PRODUCTS, "CRITICAL")}
        (issue,) = issues
        assert issue.sample_ids == (3,)

    @pytest.mark.asyncio
    async def test_an_emptied_table_is_critical(self, written):
        async with written.acquire() as conn:
            await conn.execute("TRUNCATE bronze.categories")
        issues = inv.check_chain_invariants(await _facts(written))
        assert _names(issues) == {(inv.CATALOGUE_EMPTY, CATEGORIES, "CRITICAL")}


class TestTheWayThereAndBack:
    async def _mirrored(self, store, pool):
        """Today's state: DuckDB written and the mirror shipping the same
        payload, plus product 1055, which DuckDB holds from before the mirror
        existed and KeyCRM no longer serves."""
        from core.pg_landing import mirror_categories, mirror_products

        cats = [{"id": 10, "name": "Care", "parent_id": None}]
        # What the sync does at each site: the store, then the mirror.
        await store.upsert_products(PAYLOAD)
        assert (await mirror_products(PAYLOAD)).ok
        await store.upsert_categories(cats)
        assert (await mirror_categories(cats)).ok
        async with store.connection() as conn:
            conn.execute("INSERT INTO products (id, name, category_id, brand, sku, price, "
                         "synced_at) VALUES (1055, 'Retired', 10, NULL, 'OLD', 99.5, ?)",
                         [datetime(2026, 6, 13, tzinfo=timezone.utc)])

    @pytest.mark.asyncio
    async def test_the_carry_then_the_flip_then_the_copy_back(self, stores):
        from core.chain_transfer import copy_back, handover_check
        from core.pg_landing import carry_retired_catalogue

        store, pool, env = stores
        await self._mirrored(store, pool)
        before = await _state(pool)

        # I-5: before the carry the pre-flip handover refuses, naming the carry.
        found = [i for i in await handover_check(store, chain)
                 if i.severity.value == "CRITICAL"]
        assert [(i.check_name, i.table_name, i.sample_ids) for i in found] == [
            ("handover_rows_missing", PRODUCTS, (1055,))]
        assert "/api/mirror/backfill/catalogue" in found[0].description

        dry = await carry_retired_catalogue(store, dry_run=True)
        assert dry[PRODUCTS]["would_carry"] == 1 and dry[PRODUCTS]["carried"] == 0
        assert await _pg(pool, "SELECT id FROM bronze.products WHERE id = 1055") == []

        done = await carry_retired_catalogue(store, dry_run=False)
        assert done[PRODUCTS]["carried"] == 1 and done[PRODUCTS]["ids"] == [1055]
        assert done[CATEGORIES]["would_carry"] == 0
        after = await _state(pool)
        assert (after["last_ok_at"], after["last_rows"]) == (
            before["last_ok_at"], before["last_rows"]), "the carry moved the watermark"
        (carried,) = await _pg(pool, "SELECT name, price, mirrored_at FROM bronze.products "
                                     "WHERE id = 1055")
        assert carried["name"] == "Retired" and float(carried["price"]) == 99.5
        assert carried["mirrored_at"] < after["last_ok_at"], "the carried row does not read retired"
        # Idempotent: a second carry finds nothing.
        again = await carry_retired_catalogue(store, dry_run=False)
        assert again[PRODUCTS]["would_carry"] == 0

        assert await handover_check(store, chain) == []

        # The flip: the next hourly write latches, and 1055 stays retired.
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD[:2] + [_product(3, "Product 3 renamed")])
        await store.set_last_sync_time("products", datetime(2026, 10, 1, 9, tzinfo=timezone.utc))
        issues = inv.check_chain_invariants(await _facts(pool))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, PRODUCTS, "INFO")}
        assert issues[0].sample_ids == (1055,)

        # The way back.
        preview = await handover_check(store, chain)
        assert not [i for i in preview if i.severity.value == "CRITICAL"], preview
        plan = await copy_back(store, chain, dry_run=False)
        assert plan["executed"] and plan["committed"] and plan["released"], plan
        assert sorted(tuple(r) for r in await _duck(
            store, "SELECT id, name FROM products ORDER BY id")) == [
            (1, "Product 1"), (2, "Product 2"), (3, "Product 3 renamed"), (1055, "Retired")]
        assert await _duck(store, "SELECT value FROM sync_metadata "
                                  "WHERE key = 'last_sync_products'") == [
            ("2026-10-01T09:00:00+00:00",)]
        assert await chain_latch.read_owners(pool) == {}
        assert chain_latch.latched_at(chain.CHAIN) is None

        # And the flag decides again: the next write is DuckDB's, mirrored.
        env.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is False

    @pytest.mark.asyncio
    async def test_after_the_latch_a_retired_row_edited_in_duckdb_refuses(self, stores):
        from core.chain_transfer import copy_back, CopyBackRefused, handover_check
        from core.pg_landing import carry_retired_catalogue

        store, pool, env = stores
        await self._mirrored(store, pool)
        await carry_retired_catalogue(store, dry_run=False)
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)
        async with store.connection() as conn:
            conn.execute("UPDATE products SET name = 'Edited in DuckDB' WHERE id = 1055")

        found = [(i.check_name, i.table_name, i.sample_ids, i.severity.value)
                 for i in await handover_check(store, chain)]
        assert ("handover_rows_differ", PRODUCTS, (1055,), "CRITICAL") in found
        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)
        assert chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_the_carry_refuses_once_the_chain_owns_the_catalogue(self, stores):
        from core.pg_landing import CatalogueCarryRefused, carry_retired_catalogue

        store, pool, env = stores
        await self._mirrored(store, pool)
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)
        with pytest.raises(CatalogueCarryRefused, match="bronze.products"):
            await carry_retired_catalogue(store, dry_run=False)
        # The marker lost, the flag back: the owner rows still refuse it.
        env.setenv(chain.WRITE_ENV, "duckdb")
        chain_latch.marker_path(chain.CHAIN).unlink()
        chain_latch._latched = None
        with pytest.raises(CatalogueCarryRefused, match="bronze.products"):
            await carry_retired_catalogue(store, dry_run=False)
        assert await _pg(pool, "SELECT id FROM bronze.products WHERE id = 1055") == []
