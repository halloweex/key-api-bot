"""Chain 3: orders, their line items, their expenses and the misses, written
to a real Postgres (design §3, test A).

The orders go through `SyncService._upsert_orders_with_expenses`, the one call
site every sync path reaches, so the routing is proved and not the writer
called on its own; the misses through `DuckDBStore.record_backfill_misses`,
which routes. DuckDB is asked nothing about orders under the flag — a test
that passed because DuckDB was written in Postgres's place would be the
defect, so each test that writes also proves DuckDB stayed empty.

What only a real server can say is said here: the decider's read under
`FOR UPDATE` decides exactly what DuckDB decided, the version capture runs in
the row's own transaction and writes nothing for a re-write, the line items
are replaced whole, `manager_comment` survives a payload without one, a value
pre-validation could not foresee costs one order and not the batch, and the
hourly copy of the misses stands down for a chain that owns them.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_orders_write as chain, write_chains

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 990_400
T0 = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
TABLES = ("bronze.order_products", "bronze.expenses", "bronze.orders",
          "app.order_backfill_misses")


def _payload(oid: int, *, status: int = 12, updated: datetime = T0,
             items: int = 2, comment="utm_source=ig", expenses: bool = True,
             grand_total: str = "100.00") -> dict:
    p = {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": grand_total, "ordered_at": T0.isoformat(),
        "created_at": T0.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": comment, "promocode": None,
        "products": [{"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
                      "offer": {"product_id": 700 + i}} for i in range(items)],
    }
    if expenses:
        p["expenses"] = [{"id": oid * 10, "expense_type_id": 1, "amount": 10.5,
                          "status": "paid"}]
    return p


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
        # Append-only for every writer in the repository; a test removes only
        # the archive rows of the orders it wrote.
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id BETWEEN $1 AND $2",
            BASE, BASE + 99)
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            ["bronze.orders", "bronze.order_products", "bronze.expenses"])


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB, a live Postgres, and the flag on. The preconditions —
    chain 7b, step 13, chain 1, the backup evidence — are local reads with
    tests of their own (`tests/unit/test_pg_orders_write.py`); none is what
    this module is about."""
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv(chain.WRITE_ENV, "postgres")
    monkeypatch.setattr(chain, "unmet_precondition", lambda: None)
    store = DuckDBStore(db_path=tmp_path / "orders-chain.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _sync(store, payloads, **kw):
    from core.sync_service import SyncService

    changed: list = []
    counts = await SyncService(store)._upsert_orders_with_expenses(
        payloads, changed_ids_out=changed, **kw)
    return counts, changed


async def _versions(pool, oid):
    async with pool.acquire() as conn:
        return [(r["kind"], r["status_id"], r["manager_comment"]) for r in await conn.fetch(
            "SELECT kind, status_id, manager_comment FROM app.order_versions "
            "WHERE order_id = $1 ORDER BY id", oid)]


async def _lines(pool, oid):
    async with pool.acquire() as conn:
        return [r["id"] for r in await conn.fetch(
            "SELECT id FROM bronze.order_products WHERE order_id = $1 ORDER BY id", oid)]


async def _header(pool, oid):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT status_id, grand_total, manager_comment, updated_at, xmin::text AS x "
            "FROM bronze.orders WHERE id = $1", oid)


async def _duckdb_counts(store):
    async with store.connection() as conn:
        return tuple(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                     for t in ("orders", "order_products", "expenses",
                               "order_backfill_misses"))


class TestAPageLandsInOneTransaction:
    @pytest.mark.asyncio
    async def test_headers_items_expenses_versions_and_owner_rows_together(self, stores):
        store, pool, _env = stores
        (count, expenses), changed = await _sync(store, [_payload(BASE), _payload(BASE + 1)])
        assert (count, expenses) == (2, 2) and sorted(changed) == [BASE, BASE + 1]

        assert await _lines(pool, BASE) == [BASE * 1000, BASE * 1000 + 1]
        assert await _versions(pool, BASE) == [("create", 12, "utm_source=ig")]
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT amount FROM bronze.expenses WHERE id = $1", BASE * 10) == 10.5
            xmins = {await conn.fetchval(
                f"SELECT xmin::text FROM {t} WHERE {k} = $1", v)
                for t, k, v in (("bronze.orders", "id", BASE),
                                ("bronze.order_products", "order_id", BASE),
                                ("bronze.expenses", "id", BASE * 10))}
            owners = {r["x"] for r in await conn.fetch(
                "SELECT xmin::text AS x FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")}
            watermarks = {r["table_name"] for r in await conn.fetch(
                "SELECT table_name FROM meta.mirror_state WHERE last_ok_at IS NOT NULL "
                "AND table_name = ANY($1::text[])",
                ["bronze.orders", "bronze.order_products", "bronze.expenses"])}
        # One transaction: the header, its items, its costs and the claim.
        assert len(xmins) == 1 and owners == xmins
        # The watermarks the canary and the liveness checks read keep moving.
        assert watermarks == {"bronze.orders", "bronze.order_products", "bronze.expenses"}
        assert await chain_latch.read_owners(pool) == {
            t: chain_latch.latched_at(chain.CHAIN) for t in chain.CHAIN_TABLES}
        assert chain_latch.marker_path(chain.CHAIN).exists()
        assert await _duckdb_counts(store) == (0, 0, 0, 0), "DuckDB was written under the flag"

    @pytest.mark.asyncio
    async def test_a_rewrite_of_the_same_state_archives_nothing(self, stores):
        store, pool, _env = stores
        await _sync(store, [_payload(BASE)])
        before = await _header(pool, BASE)
        (count, _), changed = await _sync(store, [_payload(BASE)])
        assert count == 1 and changed == []
        assert await _versions(pool, BASE) == [("create", 12, "utm_source=ig")]
        assert (await _header(pool, BASE))["x"] == before["x"], "the header was rewritten"

    @pytest.mark.asyncio
    async def test_force_on_an_unmoved_stamp_writes_the_status_and_one_change(self, stores):
        """KeyCRM does not bump `updated_at` on a status change, which is why
        the 05:15 refresh forces; the archive records the move once."""
        store, pool, _env = stores
        await _sync(store, [_payload(BASE)])
        (_, _), changed = await _sync(store, [_payload(BASE, status=20)],
                                      force_update=True, skip_products=True)
        assert changed == [BASE]
        assert (await _header(pool, BASE))["status_id"] == 20
        assert await _versions(pool, BASE) == [
            ("create", 12, "utm_source=ig"), ("change", 20, "utm_source=ig")]
        # A header-only refresh leaves the basket as it was.
        assert await _lines(pool, BASE) == [BASE * 1000, BASE * 1000 + 1]

    @pytest.mark.asyncio
    async def test_a_newer_stamp_rewrites_and_an_older_one_does_not(self, stores):
        store, pool, _env = stores
        await _sync(store, [_payload(BASE, updated=T0)])
        await _sync(store, [_payload(BASE, status=19, updated=T0 - timedelta(hours=1))])
        assert (await _header(pool, BASE))["status_id"] == 12
        await _sync(store, [_payload(BASE, status=19, updated=T0 + timedelta(hours=1))])
        assert (await _header(pool, BASE))["status_id"] == 19

    @pytest.mark.asyncio
    async def test_a_header_only_refresh_never_creates_an_order(self, stores):
        store, pool, _env = stores
        (count, _), changed = await _sync(
            store, [_payload(BASE, expenses=False)], force_update=True, skip_products=True)
        assert (count, changed) == (0, [])
        assert await _header(pool, BASE) is None

    @pytest.mark.asyncio
    async def test_a_basket_that_shrank_loses_the_line_it_dropped(self, stores):
        store, pool, _env = stores
        await _sync(store, [_payload(BASE, items=3)])
        await _sync(store, [_payload(BASE, items=2, updated=T0 + timedelta(minutes=5))])
        assert await _lines(pool, BASE) == [BASE * 1000, BASE * 1000 + 1]

    @pytest.mark.asyncio
    async def test_a_payload_without_a_comment_keeps_the_stored_one(self, stores):
        """It carries UTM attribution a backfill restored."""
        store, pool, _env = stores
        await _sync(store, [_payload(BASE)])
        await _sync(store, [_payload(BASE, comment=None, status=19,
                                     updated=T0 + timedelta(minutes=5))])
        header = await _header(pool, BASE)
        assert (header["status_id"], header["manager_comment"]) == (19, "utm_source=ig")


class TestWhatPostgresWouldRefuse:
    @pytest.mark.asyncio
    async def test_a_nul_is_refused_before_the_latch_and_counted(self, stores):
        store, pool, _env = stores
        from core.sync_service import SyncService

        svc = SyncService(store)
        poisoned = _payload(BASE + 1, comment="utm\x00", expenses=False)
        (count, _) = await svc._upsert_orders_with_expenses(
            [_payload(BASE, expenses=False), poisoned])
        assert count == 1 and svc._last_refused == 1
        assert await _header(pool, BASE) is not None
        assert await _header(pool, BASE + 1) is None

    @pytest.mark.asyncio
    async def test_only_a_poisoned_batch_refused_alone_costs_nothing_else(self, stores):
        """Nothing pre-validated, so Postgres refuses the batch: the same
        statements again, order by order, and only the poisoned one is lost —
        never the whole page, never a stale owner row from the rolled-back
        batch."""
        store, pool, env = stores
        env.setattr(chain, "order_refusal", lambda row: None)
        result, _ = await chain.upsert_orders_with_expenses(
            [_payload(BASE), _payload(BASE + 1, comment="utm\x00"), _payload(BASE + 2)])
        assert sorted(result.changed_ids) == [BASE, BASE + 2]
        assert result.failed == 1
        assert await _header(pool, BASE + 1) is None
        async with pool.acquire() as conn:
            # Its expense goes with it: one transaction per order.
            assert await conn.fetchval(
                "SELECT count(*) FROM bronze.expenses WHERE order_id = $1", BASE + 1) == 0
        assert await _versions(pool, BASE + 2) == [("create", 12, "utm_source=ig")]


class TestAnIdleTickSpendsNoLatch:
    @pytest.mark.asyncio
    async def test_orders_already_in_their_state_and_no_expense(self, stores):
        """Shipped by the mirror before the flip, offered again after it: read
        on the connection before the latch, and nothing is taken."""
        from core import landing_rows, pg_landing

        store, pool, _env = stores
        landed = landing_rows.landed_orders([_payload(BASE, expenses=False)],
                                            with_products=True)
        await pg_landing.write_orders(landed.orders, landed.products,
                                      replace_products=True)
        result, expense_count = await chain.upsert_orders_with_expenses(
            [_payload(BASE, expenses=False)])
        assert (result.changed_ids, result.skipped_unchanged, expense_count) == ([], 1, 0)
        assert not chain_latch.marker_path(chain.CHAIN).exists()
        assert await chain_latch.read_owners(pool) == {}

    @pytest.mark.asyncio
    async def test_a_forced_refresh_of_orders_postgres_does_not_hold(self, stores):
        """The 05:15 refresh forces a header-only rewrite, which never creates
        an order. As the first write after a flip it wrote nothing and took
        the latch and the owner rows all the same. Mutation: skip the pre-read
        for a forced batch."""
        _store, pool, _env = stores
        result, _ = await chain.upsert_orders_with_expenses(
            [_payload(BASE, expenses=False)], force_update=True, skip_products=True)
        assert (result.changed_ids, result.deferred_to_full_sync) == ([], 1)
        assert not chain_latch.marker_path(chain.CHAIN).exists()
        assert await chain_latch.read_owners(pool) == {}


class TestAnExpenseIsNeverIdle:
    @pytest.mark.asyncio
    async def test_new_and_changed_expenses_under_orders_already_in_state(self, stores):
        """The chain is the only writer of `bronze.expenses`, and an order's
        costs move without its `updated_at`. A page whose orders are all in
        their state still carries expenses — a new one, one whose amount
        changed — and must write them. Mutation (the chain-3 review's): let
        `_already_in_state` answer idle for a batch with expenses — it
        survived all 909 chain-3 tests, and every such tick would have
        returned with none of its expenses written."""
        from core import landing_rows, pg_landing

        store, pool, _env = stores
        # Shipped by the mirror before the flip, as the order step finds it.
        landed = landing_rows.landed_orders([_payload(BASE, expenses=False)],
                                            with_products=True)
        await pg_landing.write_orders(landed.orders, landed.products,
                                      replace_products=True)
        offered = _payload(BASE)                        # same updated_at: in state
        offered["expenses"] = [
            {"id": BASE * 10, "expense_type_id": 1, "amount": 20.0, "status": "paid"},
            {"id": BASE * 10 + 1, "expense_type_id": 2, "amount": 3.5, "status": "paid"},
        ]
        result, expense_count = await chain.upsert_orders_with_expenses([offered])
        assert (result.changed_ids, result.skipped_unchanged, expense_count) == ([], 1, 2)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, amount FROM bronze.expenses WHERE order_id = $1 ORDER BY id",
                BASE)
        assert [(r["id"], float(r["amount"])) for r in rows] == [
            (BASE * 10, 20.0), (BASE * 10 + 1, 3.5)]
        # A write, so the chain is now the writer.
        assert chain_latch.marker_path(chain.CHAIN).exists()
        assert set(await chain_latch.read_owners(pool)) == set(chain.CHAIN_TABLES)

        # And a CHANGED amount alone, the order still in state, lands too.
        offered["expenses"][0]["amount"] = 25.0
        _result, expense_count = await chain.upsert_orders_with_expenses([offered])
        assert expense_count == 2
        async with pool.acquire() as conn:
            assert float(await conn.fetchval(
                "SELECT amount FROM bronze.expenses WHERE id = $1", BASE * 10)) == 25.0


class TestTheDeciderReadIsTheWriteDecision:
    @pytest.mark.asyncio
    async def test_a_newer_version_committed_while_it_waited_is_not_overwritten(self, stores):
        """The decider reads the stored `updated_at` `FOR UPDATE`, inside the
        writing transaction: a writer that commits a newer version of the
        order while this one waits is read AFTER it commits, and the older
        payload is skipped. Read without the lock, the decision is taken on
        the version before, and the upsert — which waits on the same row, then
        writes over whatever is there — puts the older payload over the newer
        one. Every caller holds the heavy-job lock today, so this is defence in
        depth; it is pinned because the module calls the read the decision.
        Mutation (the chain-3 review's M2): `lock=True` → `lock=False`."""
        import asyncio

        store, pool, _env = stores
        await _sync(store, [_payload(BASE, updated=T0)])
        newer = T0 + timedelta(hours=2)
        other = await asyncpg.connect(DSN)
        try:
            tx = other.transaction()
            await tx.start()
            await other.execute(
                "UPDATE bronze.orders SET status_id = 21, updated_at = $2 WHERE id = $1",
                BASE, newer)
            write = asyncio.ensure_future(_sync(
                store, [_payload(BASE, status=19, updated=T0 + timedelta(hours=1))]))
            # Commit only once the writer is waiting on that row, so the order
            # of events is the one under test and not a race.
            for _ in range(200):
                async with pool.acquire() as conn:
                    waiting = await conn.fetchval(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND wait_event_type = 'Lock'")
                if waiting:
                    break
                await asyncio.sleep(0.025)
            assert waiting, "the writer never waited on the row"
            assert not write.done()
            await tx.commit()
            (_count, _expenses), changed = await asyncio.wait_for(write, 10)
        finally:
            await other.close()
        header = await _header(pool, BASE)
        assert (header["status_id"], header["updated_at"]) == (21, newer)
        assert changed == []


class TestTheMissesLedger:
    @pytest.mark.asyncio
    async def test_routed_and_dated_by_the_web_clock(self, stores):
        store, pool, _env = stores
        stamp = datetime(2026, 9, 30, 6, 0, tzinfo=timezone.utc)
        with patch.object(chain, "_now_utc", return_value=stamp):
            assert await store.record_backfill_misses({BASE: "not_in_keycrm"}) == 1
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT checked_at, reason FROM app.order_backfill_misses "
                "WHERE order_id = $1", BASE)
        assert (row["checked_at"], row["reason"]) == (stamp, "not_in_keycrm")
        assert await _duckdb_counts(store) == (0, 0, 0, 0)

    @pytest.mark.asyncio
    async def test_the_hourly_copy_leaves_a_ledger_the_chain_owns(self, stores):
        """DuckDB's frozen ledger is not shipped over the chain's."""
        from core.pg_operational import replicate_operational

        store, pool, _env = stores
        await store.record_backfill_misses({BASE: "chain"})
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO order_backfill_misses (order_id, checked_at, reason) "
                "VALUES (?, CURRENT_TIMESTAMP, 'frozen')", [BASE + 1])
        await replicate_operational(store, full=True)
        async with pool.acquire() as conn:
            rows = {r["order_id"]: r["reason"] for r in await conn.fetch(
                "SELECT order_id, reason FROM app.order_backfill_misses")}
        assert rows == {BASE: "chain"}


class TestTheCommentRestore:
    @pytest.mark.asyncio
    async def test_only_a_null_comment_is_filled_and_archived_as_backfill(self, stores):
        store, pool, _env = stores
        await _sync(store, [_payload(BASE, comment=None), _payload(BASE + 1)])
        restored = await chain.restore_manager_comments(
            {BASE: "utm_source=fb", BASE + 1: "utm_source=tt"})
        assert restored == [BASE]
        assert (await _header(pool, BASE))["manager_comment"] == "utm_source=fb"
        assert (await _header(pool, BASE + 1))["manager_comment"] == "utm_source=ig"
        assert await _versions(pool, BASE) == [
            ("create", 12, None), ("backfill", 12, "utm_source=fb")]
        # Headers only: the basket is not touched.
        assert await _lines(pool, BASE) == [BASE * 1000, BASE * 1000 + 1]


class TestTheSelectionsReadPostgres:
    @pytest.mark.asyncio
    async def test_latest_order_time_and_gaps_come_from_the_chain_s_store(self, stores):
        store, pool, _env = stores
        later = T0 + timedelta(hours=3)
        await _sync(store, [_payload(BASE), _payload(BASE + 3, updated=later)])
        assert await store.get_latest_order_time() == later
        await store.record_backfill_misses({BASE + 1: "not_in_keycrm"})
        gaps = await store.find_order_id_gaps(limit=10)
        assert BASE + 2 in gaps and BASE + 1 not in gaps
