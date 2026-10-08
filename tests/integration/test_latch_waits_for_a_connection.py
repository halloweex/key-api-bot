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
    chain_latch, pg_buyers_write, pg_expense_types_write, pg_expenses_write,
    pg_goals_write, pg_inventory_write, pg_managers_write, pg_orders_write,
    write_chains,
)
from core import pg_catalogue_write
from core import (  # noqa: E402 — the shadow chains' block
    pg_dq_journal_write, pg_traffic_ledger_write, pg_watchdog_write,
    pg_weekly_ledger_write,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# What the tests below write when a write is meant to land. Emptied before and
# after each test, with the owner rows — left behind, an owner row reads in the
# next module as a chain owning a table with no local marker.
_WRITTEN = ("bronze.offers", "app.manual_expenses", "app.revenue_goals",
            "bronze.expense_types", "bronze.buyer_contacts", "app.buyer_gender",
            "bronze.buyers", "bronze.order_products", "bronze.expenses",
            "bronze.orders", "app.order_backfill_misses",
            # The shadow chains (OD-02 (c)).
            "app.data_quality_issues", "app.data_quality_diffs",
            "app.data_quality_runs",
            "app.disk_samples", "app.data_dir_samples", "app.memory_samples",
            "app.weekly_report_sends", "app.traffic_report_sends")
# Chain 6's two tables, and the watermark row each write stamps beside them.
_WRITTEN += ("bronze.products", "bronze.categories")

# Chain 3's order, and the archive rows its write captures: `app.order_versions`
# is append-only for every writer in the repository, so only this order's are
# removed, by a test, and never the table.
ORDER_ID = 990_301

# Chain 5's two, apart from the literal above so a merge stays a union.
_WRITTEN += ("app.manager_classifications", "bronze.managers")

STOCK = {"id": 1, "sku": "S-1", "price": 500, "purchased_price": 250,
         "quantity": 40, "reserve": 0}


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in _WRITTEN:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM app.order_versions WHERE order_id = $1", ORDER_ID)
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
                           "OR key = 'dq_digest_last_sent'")
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                           list(pg_catalogue_write.CHAIN_TABLES))


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


def _buyer(buyer_id: int = 1):
    from core.models import Buyer

    return Buyer.from_api({"id": buyer_id, "full_name": "Олена Петренко",
                           "phone": ["+380500000001"]})


async def _derive(_store):
    """Chain 4's derivation, with one buyer waiting for a verdict — without
    one it returns before the latch, which is the point of reading first.
    Seeded through the pool the test hands out, which serves anybody but the
    writer's module the live pool."""
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bronze.buyers (id, full_name) VALUES (2, 'Олена Петренко') "
            "ON CONFLICT (id) DO NOTHING")
    return await pg_buyers_write.derive_gender_pg()


def _order_payload(oid: int = ORDER_ID) -> dict:
    when = "2026-09-20T12:00:00+00:00"
    return {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": when, "created_at": when,
        "updated_at": when, "buyer": {"id": 5301}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
        "expenses": [{"id": 77_301, "expense_type_id": 1, "amount": 10.5,
                      "status": "paid"}],
    }


def _sync_orders(store):
    """Chain 3's order write, through the one call site every sync path
    reaches (`SyncService._upsert_orders_with_expenses`)."""
    from core.sync_service import SyncService

    return SyncService(store)._upsert_orders_with_expenses([_order_payload()])


async def _classify(store):
    """Chain 5's classification, of a manager Postgres holds — without one it
    refuses before the latch, which is the point of reading first. Seeded
    through the pool the test hands out, which serves anybody but the
    writer's module the live pool."""
    from core.pg import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO bronze.managers (id, name, is_retail) VALUES (34, 'M', FALSE) "
            "ON CONFLICT (id) DO NOTHING")
    return await store.set_manager_retail_status(34, True, date(2026, 10, 7), 1, "x")


def _journal_run(_store):
    """Chain 9's writer, with the values the router would hand it."""
    from datetime import datetime, timezone

    from core.data_quality import run_values

    now = datetime.now(timezone.utc)
    values = run_values(started_at=now, ended_at=now, as_of=now,
                        window_start=now.date(), window_end=now.date(),
                        layer="integrity", issues=[], discrepancies=[])
    return pg_dq_journal_write.persist_run(values, [], [])


def _disk_tick(_store):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    sample = {"sampled_at": now, "db_size_mb": 10.0, "disk_pct_used": 40.0,
              "disk_free_gb": 100.0}
    return pg_watchdog_write.disk_tick(
        sample, {"duckdb": 1024, "unattributed": 2048}, now=now,
        dir_sampled_at=now, disk_cutoff=now - timedelta(days=14),
        dir_cutoff=now - timedelta(days=21))


def _memory_tick(_store):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    mem = {"working_set": 512 * 1024 * 1024, "page_cache": 0,
           "limit": 1024 * 1024 * 1024, "oom_kills": 0}
    return pg_watchdog_write.memory_tick(mem, now=now, sampled_at=now,
                                         cutoff=now - timedelta(days=14))


def _ledger(chain):
    def call(_store):
        from datetime import date, datetime, timezone
        from decimal import Decimal

        return chain.mark_sent(date(2026, 9, 21), "retail", Decimal("10.00"), 3,
                               datetime.now(timezone.utc))
    return call


# Writers that return their failure rather than raise it — chain 4's
# derivation, `derive_gender`'s contract. A cancellation still goes through.
NEVER_RAISES = {"derive_gender_pg"}


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
    "upsert_buyers": (pg_buyers_write, lambda s: s.upsert_buyers([_buyer()])),
    "derive_gender_pg": (pg_buyers_write, _derive),
    "upsert_orders_with_expenses": (pg_orders_write, _sync_orders),
    "record_backfill_misses": (pg_orders_write, lambda s: s.record_backfill_misses(
        {ORDER_ID: "not_in_keycrm"})),
    # Reached from the two comment backfills, which call it directly; with no
    # NULL comment to fill it still opens its transaction and claims.
    "restore_manager_comments": (pg_orders_write, lambda _s: (
        pg_orders_write.restore_manager_comments({ORDER_ID: "utm_source=ig"}))),
    "upsert_managers": (pg_managers_write, lambda s: s.upsert_managers(
        [{"id": 1, "name": "M1"}])),
    "update_manager_stats": (pg_managers_write, lambda s: s.update_manager_stats()),
    "set_manager_retail_status": (pg_managers_write, _classify),
    "upsert_products": (pg_catalogue_write, lambda s: s.upsert_products(
        [{"id": 1, "name": "Toner", "category_id": 10, "sku": "T-1", "price": 250}])),
    "upsert_categories": (pg_catalogue_write, lambda s: s.upsert_categories(
        [{"id": 10, "name": "Care", "parent_id": None}])),
    # Chain 9 (OD-02 (c)): driven at the chain module rather than through
    # `dq_journal.journal_run`, which would also hand DuckDB the shadow — the
    # latch is the writer's alone.
    "persist_run": (pg_dq_journal_write, _journal_run),
    "set_digest_marker": (pg_dq_journal_write, lambda s: pg_dq_journal_write
                          .set_digest_marker("2026-10-01T06:00:00+00:00")),
    # Chain 10, at the chain module for the same reason.
    "disk_tick": (pg_watchdog_write, _disk_tick),
    "memory_tick": (pg_watchdog_write, _memory_tick),
    # Chains 11a/11b share one writer's name, so the key carries the chain
    # after a colon; the coverage test below compares (chain, writer) pairs.
    "mark_sent:weekly": (pg_weekly_ledger_write, _ledger(pg_weekly_ledger_write)),
    "mark_sent:traffic": (pg_traffic_ledger_write, _ledger(pg_traffic_ledger_write)),
}


def _writer_name(key: str) -> str:
    return key.split(":", 1)[0]


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB, a live Postgres, and no chain's flag set.

    `KS_READ_EXPENSES=postgres` because chain 6a moves nothing without it
    (`pg_expense_types_write.unmet_precondition`); it routes reads only, and
    no test here reads expenses. The three buyer readers for chain 4's, for
    the same reason: none of them is read here.
    """
    from core.duckdb_store import DuckDBStore

    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
    for reader in ("KS_SMS_STORE", "KS_READ_SEARCH_INDEX", "KS_READ_DASHBOARD"):
        monkeypatch.setenv(reader, "postgres")
    # Chain 3 moves nothing until chain 7b, step 13, chain 1 and the backup
    # evidence hold (`pg_orders_write.unmet_precondition`); none of them is
    # what this module is about, and every one is a local read.
    monkeypatch.setattr(pg_orders_write, "unmet_precondition", lambda: None)
    # Chain 5 is held on DuckDB by facts no environment variable sets (the
    # goal bridge, step 13, chain 3); this harness proves where its latch is
    # taken, not whether it may move.
    monkeypatch.setattr(pg_managers_write, "unmet_precondition", lambda: None)
    # Chain 6's precondition taken as met: setting KS_WRITE_INVENTORY=postgres
    # here would move chain 1's own calls; the precondition itself is proved
    # in tests/unit/test_catalogue_chain.py.
    monkeypatch.setattr(pg_catalogue_write, "unmet_precondition", lambda: None)
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

    found = {(chain.__name__, name) for chain in write_chains.WRITE_CHAINS
             for name in _writers(chain)}
    driven = {(chain.__name__, _writer_name(key)) for key, (chain, _call) in WRITERS.items()}
    assert driven == found
    for key, (chain, _call) in WRITERS.items():
        assert _writer_name(key) in _writers(chain), f"{key} is not {chain.__name__}'s writer"


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
                if (writer in NEVER_RAISES
                        and refusing.raised is not asyncio.CancelledError):
                    result = await asyncio.wait_for(task, 10)
                    assert result["error"] and result["written"] == 0, result
                else:
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
            if writer in NEVER_RAISES:
                result = await call(store)
                assert result["error_class"] == "_RolledBack", result
            else:
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
    "pg_buyers_write": ("upsert_buyers", "bronze.buyers", "id", 1),
    "pg_orders_write": ("upsert_orders_with_expenses", "bronze.orders", "id", ORDER_ID),
    "pg_managers_write": ("upsert_managers", "bronze.managers", "id", 1),
    "pg_catalogue_write": ("upsert_products", "bronze.products", "id", 1),
    "pg_dq_journal_write": ("persist_run", "app.data_quality_runs", "run_id", 1),
    "pg_watchdog_write": ("memory_tick", "app.memory_samples", None, None),
    "pg_weekly_ledger_write": ("mark_sent:weekly", "app.weekly_report_sends", None, None),
    "pg_traffic_ledger_write": ("mark_sent:traffic", "app.traffic_report_sends", None, None),
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
