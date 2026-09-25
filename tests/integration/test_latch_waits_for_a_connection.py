"""The latch waits for a connection: every chain's writers, against a live pool.

The latch is permanent — only `scripts/chain_copy_back.py` releases it — so a
write that never reaches Postgres must not take it. `core.pg.get_pool` sets no
acquire timeout, so a pool with no free connection makes a writer *wait*, and
until 2026-09-25 chains 1, 8 and 7a took the marker before that wait began:
between getting the pool and `pool.acquire()`. Whatever then ended the wait
without a connection left the chain latched with nothing written. Chain 6a was
written the other way round.

`tests/unit/test_chain_latch.py` holds the order by parsing the writers. This
proves what the order buys, per writer, through the repository method every
caller actually reaches:

- while the writer waits for a connection there is no marker, and an acquire
  that then fails leaves no marker file and no owner row, and the flag is still
  the whole answer — for the three ways asyncpg's acquire ends without a
  connection (the `refusing` fixture);
- a write that rolls back takes no owner row with it, for every writer: the
  claim is in the writer's transaction, not beside it;
- a chain's first write that succeeds takes the marker and claims every owner
  row in the transaction that writes the row: the owner rows and the row carry
  one `xmin`.

Only the writer's own acquire is interfered with. Anything the store reaches
first — `set_goal` computes a suggestion before it writes — gets the live pool,
so a test cannot pass because something upstream failed first; the pool counts
what it handled and every test requires exactly the writer's one.
"""
from __future__ import annotations

import asyncio
import os
import socket
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


def _caller(depth: int = 2) -> str:
    return sys._getframe(depth).f_globals.get("__name__")


class _Entering:
    """asyncpg's acquire context, saying when the writer starts to wait on it.

    `pool.acquire()` only builds the context; the wait is on entry, inside the
    writer's `async with`, and so is every way the acquire fails."""

    def __init__(self, ctx, entered: asyncio.Event):
        self._ctx, self._entered = ctx, entered

    async def __aenter__(self):
        self._entered.set()
        return await self._ctx.__aenter__()

    async def __aexit__(self, *exc):
        return await self._ctx.__aexit__(*exc)


class _RefusesTheWriter:
    """The live pool, except to the chain's writer module, which is handed the
    refusing pool's acquire. Who asks is read off the caller's frame — the
    writer calls `pool.acquire()` itself — so a read the store makes on the way
    is served."""

    def __init__(self, live, refusing, writer: str):
        self._live, self._refusing, self._writer = live, refusing, writer
        self.refused = 0
        self.waiting = asyncio.Event()

    def acquire(self, *args, **kwargs):
        if _caller() != self._writer:
            return self._live.acquire(*args, **kwargs)
        self.refused += 1
        return _Entering(self._refusing.acquire(*args, **kwargs), self.waiting)

    def __getattr__(self, name):
        return getattr(self._live, name)


class _RolledBack(Exception):
    """Raised where the writer's transaction would have committed."""


class _RollbackAtTheEnd:
    def __init__(self, tx, pool):
        self._tx, self._pool = tx, pool

    async def __aenter__(self):
        await self._tx.start()
        return self._tx

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx.rollback()
        if exc_type is None:
            self._pool.rolled_back += 1
            raise _RolledBack()
        return False


class _TransactionsRollBack:
    """The connection the writer is handed, except that a transaction the
    writer's own module opens rolls back where it would commit. One opened
    anywhere else — inside `chain_latch`, say — is left alone, so a claim
    that opened its own transaction could not pass for one inside the
    writer's."""

    def __init__(self, conn, writer: str, pool):
        self._conn, self._writer, self._pool = conn, writer, pool

    def transaction(self, *args, **kwargs):
        tx = self._conn.transaction(*args, **kwargs)
        if _caller() != self._writer:
            return tx
        return _RollbackAtTheEnd(tx, self._pool)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _Wrapping:
    def __init__(self, ctx, wrap):
        self._ctx, self._wrap = ctx, wrap

    async def __aenter__(self):
        return self._wrap(await self._ctx.__aenter__())

    async def __aexit__(self, *exc):
        return await self._ctx.__aexit__(*exc)


class _RollsBackTheWriter:
    """The live pool, whose connection to the writer rolls its transaction
    back instead of committing it."""

    def __init__(self, live, writer: str):
        self._live, self._writer = live, writer
        self.handed = 0
        self.rolled_back = 0

    def acquire(self, *args, **kwargs):
        ctx = self._live.acquire(*args, **kwargs)
        if _caller() != self._writer:
            return ctx
        self.handed += 1
        return _Wrapping(ctx, lambda c: _TransactionsRollBack(c, self._writer, self))

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


def _nobody_listening() -> str:
    """A DSN whose connect is refused at once: a port just freed."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"postgresql://nobody:x@127.0.0.1:{port}/nothing"


class _Refusal:
    def __init__(self, pool, raised, refuse=None):
        self.pool, self.raised, self._refuse = pool, raised, refuse

    async def refuse(self, task):
        if self._refuse is not None:
            await self._refuse(task)


@pytest_asyncio.fixture(params=["restarted", "cancelled", "closed"])
async def refusing(request):
    """How asyncpg's `pool.acquire()` ends without a connection, as it is
    configured here: no acquire timeout (`core.pg.get_pool`), so a pool with
    nothing free is a wait, and each shape below is how a wait ends badly.

    `restarted` — the shape production has. One connection, held for the whole
    test (the Sunday full sync holding all five, at this pool's size); the
    writer queues behind it; Postgres ends the held connection, which hands
    its slot to the writer, and the reconnect the acquire then makes is
    refused (`OSError`) — a restart that is not back yet. A reset that fails
    on release, or an idle connection closed after five minutes, puts the
    writer on the same reconnect.

    `cancelled` — the same wait ended from outside. asyncpg's own acquire
    timeout is a cancellation of exactly this. Nothing in web cancels these
    writers today: `RequestTimeoutMiddleware` answers 504 and lets the handler
    run on (Starlette's `BaseHTTPMiddleware` runs the app in a task of its
    own; checked on 0.50 and 1.6.0).

    `closed` — the pool has been closed and entering its acquire raises
    `InterfaceError` at once. `core.pg.close_pool` has no caller in web, so
    this is asyncpg's shape rather than an incident on record.
    """
    if request.param == "closed":
        pool = await asyncpg.create_pool(DSN, min_size=0, max_size=1)
        await pool.close()
        yield _Refusal(pool, asyncpg.InterfaceError)
        return

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    held = await pool.acquire()

    async def restart(_task):
        pool.set_connect_args(dsn=_nobody_listening())
        killer = await asyncpg.connect(DSN)
        try:
            assert await killer.fetchval(
                "SELECT pg_terminate_backend($1)", held.get_server_pid())
        finally:
            await killer.close()

    async def cancel(task):
        await asyncio.sleep(0.1)          # still waiting, well inside the acquire
        task.cancel()

    refusal = (_Refusal(pool, OSError, restart) if request.param == "restarted"
               else _Refusal(pool, asyncio.CancelledError, cancel))
    try:
        yield refusal
    finally:
        try:
            await pool.release(held)
        except Exception:                 # the restart closed it; nothing to give back
            pass
        pool.terminate()


def test_every_writer_the_latch_guard_finds_is_driven_here():
    """The list above is only as good as its coverage: a writer added to a
    chain without a line here would be held to the order by the AST guard and
    never by the pool. Derived from the same walk the guard uses — every public
    async function of a chain module that reaches a connection or a writing
    statement, through the module's private helpers and constants, bar the
    readers it names and holds to writing nothing. A writer reached only
    through another module's function is past what the walk follows."""
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
        chain, call = WRITERS[writer]
        env.setenv(chain.WRITE_ENV, "postgres")
        assert chain.writes_postgres(), "the flag did not move the chain"
        pool = _RefusesTheWriter(live, refusing.pool, chain.__name__)

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            task = asyncio.ensure_future(call(store))
            waiting = asyncio.ensure_future(pool.waiting.wait())
            try:
                await asyncio.wait({task, waiting}, timeout=10,
                                   return_when=asyncio.FIRST_COMPLETED)
                assert pool.refused == 1, (
                    "the writer never reached its acquire — the test would "
                    "pass for a failure upstream of it")
                assert not chain_latch.marker_path(chain.CHAIN).exists(), (
                    f"{writer}: latched while it waited for a connection")
                await refusing.refuse(task)
                with pytest.raises(refusing.raised):
                    await asyncio.wait_for(task, 10)
            finally:
                waiting.cancel()
                if not task.done():
                    task.cancel()

        assert not chain_latch.marker_path(chain.CHAIN).exists(), (
            f"{writer}: an acquire that failed wrote the marker")
        chain_latch._latched = None                 # read the disk, not the cache
        assert not chain_latch.latched(chain.CHAIN)
        assert await chain_latch.read_owners(live) == {}

        # And so the rollback this chain came with is still there.
        env.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is False


class TestAClaimRollsBackWithItsWrite:
    """For every writer, not only each chain's first: the owner rows are
    claimed in the transaction that writes the rows, so a write that rolls
    back claims nothing. A claim beside that transaction autocommits, and
    ownership would then pass on a write that never landed — what
    `chain_latch.claim` exists to prevent. The xmin test below proves the
    same thing for one writer per chain on a write that commits; this one
    needs no row to read back, so `delete_expense` is covered too."""

    @pytest.mark.parametrize("writer", list(WRITERS))
    @pytest.mark.asyncio
    async def test_a_write_that_rolls_back_leaves_no_owner_row(self, stores, writer):
        store, live, env = stores
        chain, call = WRITERS[writer]
        env.setenv(chain.WRITE_ENV, "postgres")
        pool = _RollsBackTheWriter(live, chain.__name__)

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            with pytest.raises(_RolledBack):
                await call(store)

        assert pool.handed == 1 and pool.rolled_back == 1, (
            f"{writer}: expected one connection and one rolled-back "
            f"transaction, got {pool.handed} and {pool.rolled_back}")
        assert await chain_latch.read_owners(live) == {}, (
            f"{writer}: claimed its owner rows outside the transaction that "
            "rolled back")
        # The local half is taken before the transaction and stays: the
        # routing copy fails safe, the audit copy fails honest.
        assert chain_latch.marker_path(chain.CHAIN).exists()


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
