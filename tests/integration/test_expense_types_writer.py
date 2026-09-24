"""Chain 6a: the expense-type dictionary, written to a real Postgres (DN-26).

Everything goes through `DuckDBStore.upsert_expense_types` or the full sync
that calls it — the only path the dictionary is ever written by — so the
routing is what is proved, not the writer called on its own.

The fixture is 27 rows, the production dictionary's size, made of the three
shapes `core.landing_rows.expense_type_row` distinguishes: a display name, a
localisation key with an alias, and a key with none. The names are
synthetic; the shapes are the parse's.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_expense_types_write as chain
from core import write_chains

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLE = "bronze.expense_types"
KEY = "last_sync_expense_types"


def _payload(renamed: dict | None = None, extra: list | None = None) -> list:
    rows = []
    for i in range(1, 28):
        if i % 3 == 1:
            row = {"id": i, "name": f"Витрата {i}", "alias": f"cost_{i}"}
        elif i % 3 == 2:
            row = {"id": i, "name": f"dictionaries.expense_types.kind_{i}",
                   "alias": f"alias_for_{i}"}
        else:
            row = {"id": i, "name": f"dictionaries.expense_types.bare_kind_{i}"}
        if i % 5 == 0:
            row["is_active"] = False
        elif i % 7 == 0:
            row["is_active"] = True
        # Otherwise `is_active` is absent, and the parse defaults it to True.
        rows.append(row)
    for row in rows:
        if renamed and row["id"] in renamed:
            row["name"] = renamed[row["id"]]
    return rows + list(extra or [])


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {TABLE}")
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' OR key = $1", KEY)
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = $1", TABLE)


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB with the whole schema and a live Postgres, flag off."""
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "expense_types.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _pg_rows(pool):
    async with pool.acquire() as conn:
        return [tuple(r) for r in await conn.fetch(
            f"SELECT id, name, alias, is_active FROM {TABLE} ORDER BY id")]


async def _duck_rows(store):
    async with store.connection() as conn:
        return [tuple(r) for r in conn.execute(
            "SELECT id, name, alias, is_active FROM expense_types ORDER BY id").fetchall()]


async def _owners(pool):
    return await chain_latch.read_owners(pool)


class TestTheWriter:
    @pytest.mark.asyncio
    async def test_under_the_flag_it_lands_in_postgres_latched_and_claimed(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")

        assert await store.upsert_expense_types(_payload()) == 27

        assert len(await _pg_rows(pool)) == 27
        assert await _duck_rows(store) == [], "DuckDB was written under the flag"
        assert chain_latch.latched(chain.CHAIN)
        assert await _owners(pool) == {TABLE: chain_latch.latched_at(chain.CHAIN)}

    @pytest.mark.asyncio
    async def test_a_second_sync_renames_in_place_and_deletes_nothing(self, stores):
        """DuckDB's INSERT OR REPLACE, faithfully: a renamed type takes its new
        name, and a type KeyCRM no longer serves keeps the one it had — an old
        expense carrying it must not fall into "Other"."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_expense_types(_payload())
        async with pool.acquire() as conn:
            first = await conn.fetchval(
                f"SELECT mirrored_at FROM {TABLE} WHERE id = 1")

        await store.upsert_expense_types(
            [r for r in _payload(renamed={1: "Доставка Нова пошта"}) if r["id"] != 27])

        rows = dict((r[0], r[1]) for r in await _pg_rows(pool))
        assert rows[1] == "Доставка Нова пошта"
        assert 27 in rows, "a type KeyCRM stopped serving was deleted"
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                f"SELECT mirrored_at FROM {TABLE} WHERE id = 1") > first


class TestTheNamesAreTheDuckDBPaths:
    @pytest.mark.asyncio
    async def test_on_the_27_row_fixture_every_path_names_every_type_alike(self, stores):
        """Three writes of one payload — DuckDB's own, the hourly copy of it
        into Postgres, and this chain's direct write — must agree on every
        name, alias and flag. The parse runs once, before the routing; this is
        what a second copy of it would break."""
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        await store.upsert_expense_types(_payload())               # flag off: DuckDB
        duck = await _duck_rows(store)
        shipped = await replicate_operational(store)
        assert "error" not in shipped, shipped
        via_shipper = await _pg_rows(pool)

        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {TABLE}")
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_expense_types(_payload())               # flag on: Postgres
        direct = await _pg_rows(pool)

        assert len(duck) == 27
        assert direct == via_shipper == duck
        names = {r[0]: r[1] for r in direct}
        assert names[1] == "Витрата 1"
        assert names[2] == "Alias For 2"                  # a key with an alias
        assert names[3] == "Bare Kind 3"                  # a key without one
        assert not any(n.startswith("dictionaries.") for n in names.values())


# ─── the full sync, and the hourly copy that must leave it alone ─────────────


class _Client:
    def __init__(self, payload):
        self.payload = payload

    async def paginate(self, endpoint, params=None, page_size=50):
        if endpoint == "order/expense-type":
            yield self.payload
        return


async def _full_sync(store, payload):
    from core import sync_service as sync_module

    service = sync_module.SyncService(store)
    service.sync_managers = AsyncMock(return_value=0)
    service.sync_offers = AsyncMock(return_value=0)
    service.sync_stocks = AsyncMock(return_value=0)
    service._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    client = _Client(payload)

    async def _get_client():
        return client

    with patch.object(sync_module, "get_async_client", new=_get_client), \
         patch.object(sync_module, "mirror_categories", new=AsyncMock()), \
         patch.object(sync_module, "mirror_products", new=AsyncMock()), \
         patch.object(store, "refresh_warehouse_layers", new=AsyncMock()):
        return await service.full_sync(days_back=30)


class TestTheFullSyncAndTheHourlyReplace:
    @pytest.mark.asyncio
    async def test_under_the_flag_a_full_sync_writes_postgres_and_the_replace_leaves_it(
        self, stores,
    ):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")

        stats = await _full_sync(store, _payload())

        assert stats["expense_types"] == 27 and "expense_types_error" not in stats
        assert len(await _pg_rows(pool)) == 27
        assert await _duck_rows(store) == []
        # The watermark went with the chain, and DuckDB's copy did not move.
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT value FROM meta.chain_watermarks WHERE key = $1", KEY)
        async with store.connection() as conn:
            assert conn.execute(
                "SELECT value FROM sync_metadata WHERE key = ?", [KEY]).fetchone() is None

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert TABLE in result.get("stood_down", []), result
        assert TABLE not in result["replaced"], result
        assert len(await _pg_rows(pool)) == 27, "the hourly replace rolled it back"

    @pytest.mark.asyncio
    async def test_control_once_released_the_same_replace_wipes_it(self, stores):
        """Proves the replace above is real, so the survival is the stand-down
        working and not a run that quietly did nothing. Clearing the variable
        is not enough since DN-06: both copies of the latch have to go."""
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await _full_sync(store, _payload())
        env.delenv(chain.WRITE_ENV)
        chain_latch.release(chain.CHAIN)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert result["replaced"][TABLE] == 0
        assert await _pg_rows(pool) == [], "the replace out of an empty DuckDB did not run"


# ─── the way back ────────────────────────────────────────────────────────────


class TestCopyBack:
    @pytest.mark.asyncio
    async def test_the_round_trip_lands_postgres_names_in_duckdb_and_releases(self, stores):
        from core.chain_transfer import copy_back
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        # The ordinary state: DuckDB writes, the hourly copy ships.
        await store.upsert_expense_types(_payload())
        assert "error" not in await replicate_operational(store)

        # The flip: a later full sync renames one type and adds one, in
        # Postgres only.
        env.setenv(chain.WRITE_ENV, "postgres")
        stamp = datetime(2026, 9, 27, 2, 5, tzinfo=timezone.utc)
        await store.upsert_expense_types(_payload(
            renamed={4: "Реклама"},
            extra=[{"id": 28, "name": "dictionaries.expense_types.gift_wrap"}]))
        await store.set_last_sync_time("expense_types", stamp)
        assert chain_latch.latched(chain.CHAIN)
        postgres = await _pg_rows(pool)
        assert len(postgres) == 28

        result = await copy_back(store, chain, dry_run=False)

        assert result["findings"] == [], result["findings"]
        assert result["released"] is True
        assert result["rows"] == {TABLE: 28}
        assert await _duck_rows(store) == postgres
        async with store.connection() as conn:
            assert conn.execute(
                "SELECT value FROM sync_metadata WHERE key = ?", [KEY]
            ).fetchone()[0] == stamp.isoformat()
        assert not chain_latch.latched(chain.CHAIN)
        assert await _owners(pool) == {}
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM meta.chain_watermarks WHERE key = $1", KEY) == 0

        # The flag decides again, the hourly copy resumes, and it writes
        # exactly what came back.
        env.setenv(chain.WRITE_ENV, "duckdb")
        shipped = await replicate_operational(store)
        assert "error" not in shipped, shipped
        assert shipped["replaced"][TABLE] == 28
        assert await _pg_rows(pool) == postgres

    @pytest.mark.asyncio
    async def test_a_type_only_duckdb_holds_is_refused_before_anything_is_written(
        self, stores,
    ):
        """A full replace would delete it, and a comparison run afterwards
        could not see what the copy destroyed — so the handover asks first."""
        from core.chain_transfer import CopyBackRefused, copy_back
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        await store.upsert_expense_types(_payload())
        assert "error" not in await replicate_operational(store)
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_expense_types(_payload(renamed={4: "Реклама"}))
        async with store.connection() as conn:
            conn.execute("INSERT INTO expense_types (id, name) VALUES (99, 'only here')")
        before = await _duck_rows(store)

        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)

        assert await _duck_rows(store) == before
        assert chain_latch.latched(chain.CHAIN)


# ─── the standing watch, on the real table ───────────────────────────────────


class TestTheStandingWatch:
    async def _findings(self, pool):
        from core import pg_chain_invariants as inv

        facts = await inv.read_facts(pool=pool)
        return {i.check_name: i for i in inv.check_chain_invariants(facts)}, facts

    @pytest.mark.asyncio
    async def test_the_written_dictionary_is_clean_and_a_bypassed_parse_is_named(
        self, stores,
    ):
        from core import pg_chain_invariants as inv

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_expense_types(_payload())

        found, facts = await self._findings(pool)
        assert facts.expense_types == inv.ExpenseTypes(rows=27)
        assert found == {}

        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {TABLE} SET name = 'dictionaries.expense_types.kind_5' "
                "WHERE id IN (5, 11)")
            # An underscore is LIKE's wildcard; a name that differs from the
            # prefix only there must not be counted.
            await conn.execute(
                f"UPDATE {TABLE} SET name = 'dictionariesXexpenseYtypesZkind' WHERE id = 7")
        found, _ = await self._findings(pool)
        issue = found[inv.NAME_UNRESOLVED]
        assert issue.count == 2 and issue.sample_ids == (5, 11)

    @pytest.mark.asyncio
    async def test_an_emptied_dictionary_is_critical(self, stores):
        from core import pg_chain_invariants as inv

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await store.upsert_expense_types(_payload())
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {TABLE}")

        found, _ = await self._findings(pool)
        assert found[inv.DICTIONARY_EMPTY].severity.value == "CRITICAL"
