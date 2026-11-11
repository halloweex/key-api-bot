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
    """A real DuckDB with the whole schema and a live Postgres, flag off —
    and `KS_READ_EXPENSES=postgres`, the precondition without which the flag
    moves nothing (`TestTheReadFlagComesFirst` takes it away)."""
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
    store = DuckDBStore()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()


async def _pg_rows(pool):
    async with pool.acquire() as conn:
        return [tuple(r) for r in await conn.fetch(
            f"SELECT id, name, alias, is_active FROM {TABLE} ORDER BY id")]




async def _owners(pool):
    return await chain_latch.read_owners(pool)


class TestTheWriter:
    @pytest.mark.asyncio
    async def test_under_the_flag_it_lands_in_postgres_latched_and_claimed(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")

        assert await store.upsert_expense_types(_payload()) == 27

        assert len(await _pg_rows(pool)) == 27
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


# What DuckDB's own writer stored for `_payload()` — `(id, name, alias,
# is_active)` — recorded on the commit before step 14's S14-09 (d544144c),
# which deleted that writer. In that run Postgres stored the same rows
# (rule 9: the literal is DuckDB's answer, proved equal to Postgres's).
DUCKDB_ANSWERED = [
    [1, 'Витрата 1', 'cost_1', True],
    [2, 'Alias For 2', 'alias_for_2', True],
    [3, 'Bare Kind 3', None, True],
    [4, 'Витрата 4', 'cost_4', True],
    [5, 'Alias For 5', 'alias_for_5', False],
    [6, 'Bare Kind 6', None, True],
    [7, 'Витрата 7', 'cost_7', True],
    [8, 'Alias For 8', 'alias_for_8', True],
    [9, 'Bare Kind 9', None, True],
    [10, 'Витрата 10', 'cost_10', False],
    [11, 'Alias For 11', 'alias_for_11', True],
    [12, 'Bare Kind 12', None, True],
    [13, 'Витрата 13', 'cost_13', True],
    [14, 'Alias For 14', 'alias_for_14', True],
    [15, 'Bare Kind 15', None, False],
    [16, 'Витрата 16', 'cost_16', True],
    [17, 'Alias For 17', 'alias_for_17', True],
    [18, 'Bare Kind 18', None, True],
    [19, 'Витрата 19', 'cost_19', True],
    [20, 'Alias For 20', 'alias_for_20', False],
    [21, 'Bare Kind 21', None, True],
    [22, 'Витрата 22', 'cost_22', True],
    [23, 'Alias For 23', 'alias_for_23', True],
    [24, 'Bare Kind 24', None, True],
    [25, 'Витрата 25', 'cost_25', False],
    [26, 'Alias For 26', 'alias_for_26', True],
    [27, 'Bare Kind 27', None, True]
]


class TestTheNamesAreTheDuckDBPaths:
    @pytest.mark.asyncio
    async def test_on_the_27_row_fixture_every_path_names_every_type_alike(self, stores):
        """This chain's write must name every type, alias and flag as DuckDB's
        own write of the same payload did. The parse runs once, before the
        writer; this is what a second copy of it would break. Until S14-09
        the two writes ran side by side here; DuckDB's is a literal since."""
        store, pool, env = stores
        await store.upsert_expense_types(_payload())               # Postgres
        direct = await _pg_rows(pool)

        assert len(DUCKDB_ANSWERED) == 27
        assert [list(r) for r in direct] == DUCKDB_ANSWERED
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

    with patch.object(sync_module, "get_async_client", new=_get_client):
        return await service.full_sync(days_back=30)


class TestTheFullSync:
    @pytest.mark.asyncio
    async def test_under_the_flag_a_full_sync_writes_postgres(self, stores):
        """Until step 14's S14-13 the hourly replace was run after it, and
        left the table alone; the replace went with `core.pg_operational`."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")

        stats = await _full_sync(store, _payload())

        assert stats["expense_types"] == 27 and "expense_types_error" not in stats
        assert len(await _pg_rows(pool)) == 27
        # The watermark went with the chain, and DuckDB's copy did not move.
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT value FROM meta.chain_watermarks WHERE key = $1", KEY)


# ─── the standing watch, on the real table ───────────────────────────────────


class TestTheStandingWatch:
    @pytest.fixture(autouse=True)
    def _this_chain_alone(self, monkeypatch):
        """Since S14-00 every chain is watched; this chain's reader, alone."""
        from core import pg_chain_invariants as inv

        every = inv.watched_chains
        monkeypatch.setattr(inv, "watched_chains", lambda: {
            name: at for name, at in every().items() if name == chain.CHAIN})

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
            # prefix only there is a display name and must not be counted.
            await conn.execute(
                f"UPDATE {TABLE} SET name = 'dictionaries.expenseXtypes.kind' WHERE id = 7")
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
