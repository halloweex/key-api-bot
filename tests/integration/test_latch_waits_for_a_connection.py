"""The latch waits for a connection: every chain's writers, against a live pool.

The latch is permanent — only `scripts/chain_copy_back.py` releases it — so a
write that never reaches Postgres must not take it. `pool.acquire()` fails on
its own: a pool that is closed, or one whose connections are all in use past
the caller's patience. Until 2026-09-25 chains 1, 8 and 7a took the marker
between getting the pool and the acquire, so each of those failures latched
the chain with nothing written; chain 6a was written the other way round.

`tests/unit/test_chain_latch.py` holds the order by parsing the writers. This
proves what the order buys, per writer, through the repository method every
caller actually reaches:

- an acquire that fails — the pool closed, or its one connection held past an
  acquire timeout — leaves no marker file and no owner row, and the flag is
  still the whole answer;
- a chain's first write that succeeds takes the marker and claims every owner
  row in the transaction that writes the row: the owner rows and the row carry
  one `xmin`.

Only the writer's own acquire is refused. Anything the store reaches first —
`set_goal` computes a suggestion before it writes — gets the live pool, so a
test cannot pass because something upstream failed first; the refusing pool
counts what it refused and every test requires exactly the writer's one.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import (
    chain_latch, pg_expense_types_write, pg_expenses_write, pg_goals_write,
    pg_inventory_write, write_chains,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# What the tests below write when a write is meant to land. Emptied before and
# after each test, with the owner rows — left behind, an owner row reads in the
# next module as a chain owning a table with no local marker.
_WRITTEN = ("bronze.offers", "app.manual_expenses", "app.revenue_goals",
            "bronze.expense_types")

STOCK = {"id": 1, "sku": "S-1", "price": 500, "purchased_price": 250,
         "quantity": 40, "reserve": 0}


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in _WRITTEN:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


class _RefusesTheWriter:
    """The live pool, except to the chain's writer module.

    asyncpg's `acquire()` only builds a context; the waiting happens on entry,
    inside the writer's `async with`. So the refusal is asyncpg's own: a closed
    pool raises `InterfaceError` on entry, and an exhausted one times out
    there. Who asks is read off the caller's frame — the writer calls
    `pool.acquire()` itself — so a read the store makes on the way is served.
    """

    def __init__(self, live, refusing, writer: str, timeout=None):
        self._live, self._refusing, self._writer = live, refusing, writer
        self._timeout = timeout
        self.refused = 0

    def acquire(self, *args, **kwargs):
        if sys._getframe(1).f_globals.get("__name__") != self._writer:
            return self._live.acquire(*args, **kwargs)
        self.refused += 1
        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        return self._refusing.acquire(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._live, name)


# (chain module, the repository call that reaches the writer). Every public
# writer the latch guard's walk finds, and nothing else — a test below checks
# the two lists against each other.
WRITERS = {
    "upsert_offers": (pg_inventory_write, lambda s: s.upsert_offers(
        [{"id": 1, "product_id": 101, "sku": "S-1"}])),
    "upsert_stocks": (pg_inventory_write, lambda s: s.upsert_stocks([STOCK])),
    "rebuild_sku_inventory_status": (
        pg_inventory_write, lambda s: s.refresh_sku_inventory_status()),
    "record_sku_inventory_snapshot": (
        pg_inventory_write, lambda s: s.record_sku_inventory_snapshot()),
    "record_inventory_snapshot": (
        pg_inventory_write, lambda s: s.record_inventory_snapshot()),
    "add_expense": (pg_expenses_write, lambda s: s.add_expense(
        date(2026, 9, 17), "marketing", "Facebook Ads", 10)),
    "update_expense": (pg_expenses_write, lambda s: s.update_expense(1, amount=20)),
    "delete_expense": (pg_expenses_write, lambda s: s.delete_expense(1)),
    "set_goal": (pg_goals_write, lambda s: s.set_goal("daily", 60_000.0)),
    "upsert_expense_types": (pg_expense_types_write, lambda s: s.upsert_expense_types(
        [{"id": 1, "name": "Delivery"}])),
}


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB, a live Postgres, and no chain's flag set.

    `KS_READ_EXPENSES=postgres` because chain 6a moves nothing without it
    (`pg_expense_types_write.unmet_precondition`); it routes reads only, and
    no test here reads expenses.
    """
    from core.duckdb_store import DuckDBStore

    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
    store = DuckDBStore(db_path=tmp_path / "latch-acquire.duckdb")
    await store.connect()
    live = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(live)
    with patch("core.pg.require_revision", new=AsyncMock()):
        yield store, live, monkeypatch
    await _clean(live)
    await live.close()
    await store.close()


@pytest_asyncio.fixture(params=["closed", "timeout"])
async def refusing(request):
    """A pool with no connection to give, in the two shapes asyncpg has.

    `closed`: the pool has been closed — entering its acquire raises
    `InterfaceError`. `timeout`: one connection, held here for the whole test,
    so the writer's acquire waits and gives up after the forced timeout — the
    Sunday full sync holding all five is this, at production's size.
    """
    if request.param == "closed":
        pool = await asyncpg.create_pool(DSN, min_size=0, max_size=1)
        await pool.close()
        yield pool, None, asyncpg.InterfaceError
        return
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    held = await pool.acquire()
    try:
        yield pool, 0.2, asyncio.TimeoutError
    finally:
        await pool.release(held)
        await pool.close()


def test_every_writer_the_latch_guard_finds_is_driven_here():
    """The list above is only as good as its coverage: a writer added to a
    chain without a line here would be held to the order by the AST guard and
    never by the pool. Derived from the same walk the guard uses."""
    from tests.unit.test_chain_latch import _writers

    found = {name for chain in write_chains.WRITE_CHAINS for name in _writers(chain)}
    assert set(WRITERS) == found
    for name, (chain, _call) in WRITERS.items():
        assert name in _writers(chain), f"{name} is not {chain.__name__}'s writer"


class TestAnAcquireThatFailsLeavesTheChainUnlatched:
    @pytest.mark.parametrize("writer", list(WRITERS))
    @pytest.mark.asyncio
    async def test_no_marker_no_owner_row_and_the_flag_still_decides(
        self, stores, refusing, writer,
    ):
        store, live, env = stores
        dead, timeout, raised = refusing
        chain, call = WRITERS[writer]
        env.setenv(chain.WRITE_ENV, "postgres")
        assert chain.writes_postgres(), "the flag did not move the chain"
        pool = _RefusesTheWriter(live, dead, chain.__name__, timeout)

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            with pytest.raises(raised):
                await call(store)

        assert pool.refused == 1, (
            "the writer's acquire was not the thing refused — the test would "
            "pass for a failure upstream of it")
        assert not chain_latch.marker_path(chain.CHAIN).exists(), (
            f"{writer}: an acquire that failed wrote the marker")
        chain_latch._latched = None                 # read the disk, not the cache
        assert not chain_latch.latched(chain.CHAIN)
        assert await chain_latch.read_owners(live) == {}

        # And so the rollback this chain came with is still there.
        env.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is False


# The first write of each chain, and the row it lands: (table, key column, key).
FIRST_WRITES = {
    "pg_inventory_write": ("upsert_offers", "bronze.offers", "id", 1),
    "pg_expenses_write": ("add_expense", "app.manual_expenses", None, None),
    "pg_goals_write": ("set_goal", "app.revenue_goals", "period_type", "daily"),
    "pg_expense_types_write": ("upsert_expense_types", "bronze.expense_types", "id", 1),
}


class TestAFirstWriteLatchesAndClaimsInItsOwnTransaction:
    def test_every_registered_chain_has_a_first_write_here(self):
        assert set(FIRST_WRITES) == {write_chains.chain_name(c)
                                     for c in write_chains.WRITE_CHAINS}

    @pytest.mark.parametrize("chain_name", list(FIRST_WRITES))
    @pytest.mark.asyncio
    async def test_the_owner_rows_and_the_row_share_one_transaction(
        self, stores, chain_name,
    ):
        store, live, env = stores
        writer, table, key_column, key = FIRST_WRITES[chain_name]
        chain, call = WRITERS[writer]
        env.setenv(chain.WRITE_ENV, "postgres")

        with patch("core.pg.get_pool", new=AsyncMock(return_value=live)):
            await call(store)

        assert chain_latch.marker_path(chain.CHAIN).exists()
        stamp = chain_latch.latched_at(chain.CHAIN)
        assert await chain_latch.read_owners(live) == {
            t: stamp for t in chain.CHAIN_TABLES}

        async with live.acquire() as conn:
            owners = {r["key"]: r["x"] for r in await conn.fetch(
                "SELECT key, xmin::text AS x FROM meta.chain_watermarks "
                "WHERE key LIKE 'owner:%'")}
            if key_column is None:                  # the one row there is
                (row_xmin,) = [r["x"] for r in await conn.fetch(
                    f"SELECT xmin::text AS x FROM {table}")]
            else:
                row_xmin = await conn.fetchval(
                    f"SELECT xmin::text FROM {table} WHERE {key_column} = $1", key)
        assert row_xmin is not None, f"{writer} wrote nothing to {table}"
        assert set(owners.values()) == {row_xmin}, (
            f"{writer}: the owner rows were written by a transaction other "
            f"than the one that wrote {table}")
