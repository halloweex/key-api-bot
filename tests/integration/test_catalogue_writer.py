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
  the retired product in DuckDB;
- the record of the chain's own write instants: what it keeps, that a write
  round the chain outlives the next full write in the watch and refuses the
  copy-back, that it is locked before any product, and that one nobody can
  parse stops the write (the chain-6 review's first finding).

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
            "OR key LIKE $2 OR key = ANY($1::text[])", list(chain.CHAIN_SYNC_KEYS),
            chain.RECORD_PREFIX + "%")


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


def _writer_and_rows(which):
    from core.landing_rows import category_rows

    if which == PRODUCTS:
        return chain.upsert_products, product_rows(PAYLOAD)
    return chain.upsert_categories, category_rows(CATS)


async def _done_within(task, seconds):
    """`task`'s result if it finishes inside `seconds`; else it is cancelled
    and the test fails — a bound that is missing shows up as a wait, never as
    a hang of the suite."""
    done, _ = await asyncio.wait({task}, timeout=seconds)
    if not done:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        pytest.fail(f"still waiting after {seconds}s: the bound is gone")
    return task


class TestTheWriterIsBounded:
    """DN-05a's bounds, the fix for hazard F1: the hourly products step runs
    inside the incremental tick under the heavy-job lock, so a writer that
    waits without a bound stops order intake. Each removed survived the whole
    chain-6 set until these (review mutations M-nn and M-oo)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", [PRODUCTS, CATEGORIES])
    async def test_a_pool_with_no_connection_to_give_is_a_timeout(
            self, stores, table):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        env.setattr(chain, "ACQUIRE_TIMEOUT_S", 0.3)
        write, rows = _writer_and_rows(table)
        small = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
        try:
            async with small.acquire():                      # the only one there is
                with patch("core.pg.get_pool", new=AsyncMock(return_value=small)):
                    task = await _done_within(asyncio.ensure_future(write(rows)), 10)
                with pytest.raises(asyncio.TimeoutError):
                    task.result()
        finally:
            await small.close()
        assert not chain_latch.latched(chain.CHAIN), (
            "a write that never got a connection latched the chain")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", [PRODUCTS, CATEGORIES])
    async def test_a_table_another_session_holds_is_a_statement_timeout(
            self, stores, table):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        env.setattr(chain, "STATEMENT_TIMEOUT", "300ms")
        write, rows = _writer_and_rows(table)
        async with pool.acquire() as holder:
            tx = holder.transaction()
            await tx.start()
            task = None
            try:
                await holder.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
                task = await _done_within(asyncio.ensure_future(write(rows)), 10)
                with pytest.raises(asyncpg.exceptions.QueryCanceledError):
                    task.result()
            finally:
                await tx.rollback()
        # Cancelled inside the transaction: nothing of the write landed.
        assert await _pg(pool, f"SELECT id FROM {table}") == []
        assert await _state(pool, table) is None
        assert await chain_latch.read_owners(pool) == {}


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


async def _record(pool, table=PRODUCTS):
    rows = await _pg(pool, "SELECT value FROM meta.chain_watermarks WHERE key = $1",
                     chain.record_key(table))
    return chain.parse_record(rows[0]["value"]) if rows else None


async def _stamp(pool, ts):
    (row,) = await _pg(pool, "SELECT (extract(epoch FROM $1::timestamptz) * 1000000)"
                             "::bigint AS s", ts)
    return row["s"]


class TestTheRecordOfTheChainsWrites:
    """The review's finding: "later than `last_ok_at`" lasted one full write.
    The record of the chain's own instants is what outlives it."""

    @pytest.mark.asyncio
    async def test_it_keeps_exactly_the_instants_some_row_still_carries(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)
        first = (await _state(pool))["last_ok_at"]
        record = await _record(pool)
        assert record.stamps == {await _stamp(pool, first)}
        assert record.previous is None, "no full write had stamped the table before"

        await store.upsert_products(PAYLOAD[:2])             # 3 retired at `first`
        second = (await _state(pool))["last_ok_at"]
        record = await _record(pool)
        assert record.stamps == {await _stamp(pool, first), await _stamp(pool, second)}
        assert record.previous == await _stamp(pool, first)

        await store.upsert_products(PAYLOAD)                 # 3 served again
        third = (await _state(pool))["last_ok_at"]
        record = await _record(pool)
        assert record.stamps == {await _stamp(pool, third)}, (
            "a stamp no row carries was kept: the record would grow with every write")
        assert record.previous == await _stamp(pool, second)
        # Each table its own record, written by its own writes alone.
        assert await _record(pool, CATEGORIES) is None

    @pytest.mark.asyncio
    async def test_a_write_round_the_chain_outlives_the_next_full_write(self, stores):
        """The review's reproduction in the writer's terms: a stray insert of
        an id KeyCRM never served and a retired product edited with its
        `mirrored_at` moved. Both used to read as retired one write later."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)
        await store.upsert_products(PAYLOAD[:2])             # 3 retired by the chain
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.products (id, name, brand, sku, price) "
                               "VALUES (777, 'stray insert', 'X', 'STRAY', 1.00)")
            await conn.execute("UPDATE bronze.products SET name = 'tampered retired', "
                               "price = 0.01, mirrored_at = now() WHERE id = 3")
        before = inv.check_chain_invariants(await _facts(pool))
        assert _names(before) == {(inv.CATALOGUE_WRITTEN_AROUND, PRODUCTS, "CRITICAL")}

        await store.upsert_products(PAYLOAD[:2])             # the next hourly write

        issues = inv.check_chain_invariants(await _facts(pool))
        assert _names(issues) == {(inv.CATALOGUE_WRITTEN_AROUND, PRODUCTS, "CRITICAL")}, (
            "the next full write turned a write round the chain into 'retired'")
        (around,) = issues
        assert around.sample_ids == (3, 777) and around.count == 2
        assert "recorded" in around.description

        # KeyCRM serving the row again is what clears it: the chain re-stamps it.
        await store.upsert_products(PAYLOAD)
        (around,) = inv.check_chain_invariants(await _facts(pool))
        assert around.sample_ids == (777,)

    @pytest.mark.asyncio
    async def test_a_row_the_chain_retired_is_still_retired(self, stores):
        """The other half: the record must not turn the chain's own old
        instants into writes round it."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)
        await store.upsert_products(PAYLOAD[:2])
        await store.upsert_products(PAYLOAD[:1])
        issues = inv.check_chain_invariants(await _facts(pool))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, PRODUCTS, "INFO")}
        assert issues[0].sample_ids == (2, 3)

    @pytest.mark.asyncio
    async def test_a_short_write_is_one_write_against_the_one_before(self, stores):
        """The review's second finding on a real Postgres: 20 complete writes,
        each one product smaller, retire 10% of the table and are not a short
        write; one truncated write is, and the next complete one clears it."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        served = list(range(1, 201))
        await store.upsert_products([_product(i) for i in served])
        for retired in range(1, 21):
            served.remove(retired)                           # KeyCRM retires one
            await store.upsert_products([_product(i) for i in served])
        issues = inv.check_chain_invariants(await _facts(pool))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, PRODUCTS, "INFO")}
        assert issues[0].count == 20

        await store.upsert_products([_product(i) for i in served[:150]])   # truncated
        issues = inv.check_chain_invariants(await _facts(pool))
        (short,) = [i for i in issues if i.check_name == inv.CATALOGUE_SHORT_WRITE]
        assert (short.severity.value, short.count) == ("WARN", 30)
        assert short.sample_ids == tuple(served[150:160])

        await store.upsert_products([_product(i) for i in served])        # complete
        issues = inv.check_chain_invariants(await _facts(pool))
        assert _names(issues) == {(inv.CATALOGUE_RETIRED, PRODUCTS, "INFO")}

    @pytest.mark.asyncio
    async def test_the_record_is_locked_before_any_product(self, stores):
        """Two writes of one table queue on the record, so neither loses the
        other's stamp: shown without a race, the lock-order test's way. A
        session holding the record stops a write before it has locked any
        product."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)                 # the latch and the record
        async with pool.acquire() as holder:
            tx = holder.transaction()
            await tx.start()
            write = None
            try:
                await holder.execute("SELECT 1 FROM meta.chain_watermarks WHERE key = $1 "
                                     "FOR UPDATE", chain.record_key(PRODUCTS))
                write = asyncio.ensure_future(chain.upsert_products(product_rows(PAYLOAD)))
                for _ in range(200):
                    (waiting,) = await _pg(
                        pool, "SELECT count(*) AS n FROM pg_stat_activity "
                              "WHERE datname = current_database() "
                              "AND wait_event_type = 'Lock'")
                    if waiting["n"] or write.done():
                        break
                    await asyncio.sleep(0.05)
                assert not write.done(), "the write did not wait for the record"
                free = await holder.fetchval(
                    "SELECT count(*) FROM (SELECT 1 FROM bronze.products "
                    "FOR UPDATE SKIP LOCKED) s")
                assert free == 3, "the write locked a product before the record"
            finally:
                await tx.rollback()
                if write is not None:
                    assert await asyncio.wait_for(write, timeout=30) == 3

    @pytest.mark.asyncio
    async def test_a_record_nobody_can_parse_stops_the_write_and_blinds_the_watch(
            self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_categories(CATS)
        await store.upsert_products(PAYLOAD)
        before = await _pg(pool, "SELECT id, mirrored_at FROM bronze.products ORDER BY id")
        state = await _state(pool)
        async with pool.acquire() as conn:
            await conn.execute("UPDATE meta.chain_watermarks SET value = 'not json' "
                               "WHERE key = $1", chain.record_key(PRODUCTS))

        with pytest.raises(chain.WriteRecordUnreadable):
            await chain.upsert_products(product_rows(PAYLOAD[:2]))

        assert await _pg(pool, "SELECT id, mirrored_at FROM bronze.products "
                               "ORDER BY id") == before, "a write forgot the record"
        assert (await _state(pool))["last_ok_at"] == state["last_ok_at"]
        (issue,) = inv.check_chain_invariants(await _facts(pool))
        assert issue.check_name == inv.UNWATCHED
        assert "WriteRecordUnreadable" in issue.description


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
        # The record of the chain's writes goes with the latch.
        assert await _pg(pool, "SELECT key FROM meta.chain_watermarks WHERE key LIKE $1",
                         chain.RECORD_PREFIX + "%") == []

        # And the flag decides again: the next write is DuckDB's, mirrored.
        env.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is False

    @pytest.mark.asyncio
    async def test_a_row_the_mirror_ships_while_the_carry_runs_keeps_the_mirrors_copy(
            self, stores):
        """T-12: the carry's `ON CONFLICT (id) DO NOTHING`. Between the carry
        reading which ids Postgres holds and inserting, the mirror can ship one
        of them — KeyCRM serving the product again. That row is served and
        current; DuckDB's frozen copy must not overwrite it, and the race must
        not fail the carry. Changed to DO UPDATE, or removed, it survived the
        whole chain-6 set (review mutations R-T12e and M-m)."""
        from core import pg_landing

        store, pool, env = stores
        await self._mirrored(store, pool)
        async with pool.acquire() as conn:                   # the mirror, just now
            await conn.execute(
                "INSERT INTO bronze.products (id, name, category_id, brand, sku, price) "
                "VALUES (1055, 'Served again', 10, 'B', 'NEW', 120.00)")
        (served,) = await _pg(pool, "SELECT name, price, mirrored_at FROM bronze.products "
                                    "WHERE id = 1055")
        real = pg_landing.retired_rows

        def read_before_the_mirror_shipped(rows, synced, held, last_ok_at):
            return real(rows, synced, held - {1055}, last_ok_at)

        env.setattr(pg_landing, "retired_rows", read_before_the_mirror_shipped)
        done = await pg_landing.carry_retired_catalogue(store, dry_run=False)

        assert done[PRODUCTS]["ids"] == [1055] and done[PRODUCTS]["carried"] == 0
        (after,) = await _pg(pool, "SELECT name, price, mirrored_at FROM bronze.products "
                                   "WHERE id = 1055")
        assert tuple(after) == tuple(served), "the carry overwrote a row the mirror served"

    @pytest.mark.asyncio
    async def test_a_carried_row_stamped_at_the_watermark_still_reads_retired(self, stores):
        """T-12: `mirrored_at = LEAST(synced, W − 1 µs)`. A row DuckDB stamped
        at exactly W is retired by the comparison's `<=`; carried at W it
        would read as one the last full write carried — current, and in the
        lost count's `last_rows` it never was (review mutation R-T12d)."""
        from core.pg_landing import carry_retired_catalogue

        store, pool, env = stores
        await self._mirrored(store, pool)
        w = (await _state(pool))["last_ok_at"]
        async with store.connection() as conn:
            conn.execute("UPDATE products SET synced_at = ? WHERE id = 1055", [w])

        done = await carry_retired_catalogue(store, dry_run=False)

        assert done[PRODUCTS]["carried"] == 1
        (carried,) = await _pg(pool, "SELECT mirrored_at FROM bronze.products WHERE id = 1055")
        assert carried["mirrored_at"] == w - timedelta(microseconds=1)

    @pytest.mark.asyncio
    async def test_a_chain_write_after_a_failure_reads_healthy_again(self, stores):
        """The chain stamps its watermark with the mirror's statement, which
        resets the failure count — the copy-back's soak text tells an operator
        to see `failures_since_ok` at 0 (review mutation M-y)."""
        from core.pg_landing import _record_failure

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)
        assert await _record_failure(PRODUCTS, "ConnectionRefusedError: gone")
        assert await _record_failure(PRODUCTS, "ConnectionRefusedError: gone")
        (failing,) = await _pg(pool, "SELECT failures_since_ok, last_error "
                                     "FROM meta.mirror_state WHERE table_name = $1", PRODUCTS)
        assert failing["failures_since_ok"] == 2 and failing["last_error"]

        await store.upsert_products(PAYLOAD)

        (healthy,) = await _pg(pool, "SELECT failures_since_ok, last_error "
                                     "FROM meta.mirror_state WHERE table_name = $1", PRODUCTS)
        assert (healthy["failures_since_ok"], healthy["last_error"]) == (0, None)

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
    async def test_after_the_latch_a_row_written_round_the_chain_refuses(self, stores):
        """The review's reproduction, end to end: after the flip a stray
        product is inserted and the carried retired one is edited with its
        `mirrored_at` moved, then the next hourly write runs. Both were taken
        for the chain's work — at or after the latch — and the copy-back
        released with them in DuckDB. Only a recorded instant is the chain's."""
        from core.chain_transfer import copy_back, CopyBackRefused, handover_check
        from core.pg_landing import carry_retired_catalogue

        store, pool, env = stores
        await self._mirrored(store, pool)
        await carry_retired_catalogue(store, dry_run=False)
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)                 # the flip
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.products (id, name, brand, sku, price) "
                               "VALUES (777, 'stray insert', 'X', 'STRAY', 1.00)")
            await conn.execute("UPDATE bronze.products SET name = 'tampered retired', "
                               "price = 0.01, mirrored_at = now() WHERE id = 1055")
        await store.upsert_products(PAYLOAD[:2] + [_product(3, "Product 3 renamed")])

        found = {(i.check_name, i.sample_ids, i.severity.value)
                 for i in await handover_check(store, chain)}
        assert ("handover_rows_differ", (1055,), "CRITICAL") in found
        assert ("handover_rows_ahead", (777,), "CRITICAL") in found
        # The chain's own rename is still the copy's work.
        assert ("handover_rows_differ", (3,), "INFO") in found
        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)
        assert chain_latch.latched(chain.CHAIN)
        assert await chain_latch.read_owners(pool) != {}
        assert await _duck(store, "SELECT id, name FROM products WHERE id IN (777, 1055) "
                                  "ORDER BY id") == [(1055, "Retired")]

    @pytest.mark.asyncio
    async def test_a_recorded_instant_before_the_latch_is_not_the_chains(self, stores):
        """The latch is a floor under the record. A record that outlived its
        era — a release that did not know to delete it, a restore — can name
        an instant a row from before the flip still carries; an edit to that
        row in Postgres, `mirrored_at` left alone, is not the chain's work and
        must refuse rather than be copied into DuckDB."""
        from core.chain_transfer import copy_back, CopyBackRefused, handover_check
        from core.pg_landing import carry_retired_catalogue

        store, pool, env = stores
        await self._mirrored(store, pool)
        await carry_retired_catalogue(store, dry_run=False)
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_products(PAYLOAD)                 # the flip
        (old,) = await _pg(pool, "SELECT mirrored_at FROM bronze.products WHERE id = 1055")
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE meta.chain_watermarks SET value = jsonb_set(value::jsonb, "
                "'{stamps}', (value::jsonb -> 'stamps') || to_jsonb($2::bigint))::text "
                "WHERE key = $1", chain.record_key(PRODUCTS), await _stamp(pool, old["mirrored_at"]))
            await conn.execute("UPDATE bronze.products SET name = 'edited, stamp kept' "
                               "WHERE id = 1055")

        found = {(i.check_name, i.sample_ids, i.severity.value)
                 for i in await handover_check(store, chain)}
        assert ("handover_rows_differ", (1055,), "CRITICAL") in found
        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)

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
