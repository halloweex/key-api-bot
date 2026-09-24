"""The ownership latch against a real Postgres: both copies, both chains.

DN-06, owner decision OD-19 (a). The unit tests pin the local marker; this pins
what the marker buys — that a flag flipped back does not move the writes, that
the id allocator does not change hands with it, that the hourly shipper cannot
roll the rows back, and that the two copies of the latch are compared daily.

Every test drives the repository methods or the real job, never the writer in
isolation: the flag was consulted per call at eight sites, and a test that
called `pg_expenses_write.add_expense` directly would prove nothing about the
route a typed expense actually takes.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

EXPENSES = "app.manual_expenses"


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.manual_expenses")
        await conn.execute("DELETE FROM app.stock_movements")
        await conn.execute("DELETE FROM bronze.offer_stocks")
        await conn.execute("DELETE FROM bronze.offers")
        await conn.execute("DELETE FROM meta.chain_watermarks")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            [EXPENSES, "app.stock_movements", "bronze.offers", "bronze.offer_stocks"],
        )


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB with the whole schema and an empty Postgres, both live.

    The DuckDB store is real rather than refused, because the point of most of
    these tests is that a row did NOT land in it.
    """
    from core.duckdb_store import DuckDBStore

    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS"):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "latch.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _expense_rows(pool):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM app.manual_expenses ORDER BY id")


async def _owners(pool):
    return await chain_latch.read_owners(pool)


class TestChain8AfterTheFlagGoesBack:
    """The rewritten control: flipping `KS_WRITE_EXPENSES` back changes nothing.

    Before DN-06 it changed four things at once — the writer, the id allocator,
    the hourly full replace and the daily comparison — and the first of those
    put the next typed amount in a store the page does not read.
    """

    @pytest.mark.asyncio
    async def test_the_writes_stay_in_postgres_and_the_ids_keep_counting(
        self, stores,
    ):
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        first = await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        assert chain_latch.latched("pg_expenses_write"), "the first write did not latch"

        env.delenv("KS_WRITE_EXPENSES")                      # the flag goes back

        second = await store.add_expense(date(2026, 9, 18), "marketing", "TikTok Ads", 900)

        assert second["id"] == first["id"] + 1, "the id came from somewhere else"
        rows = await _expense_rows(pool)
        assert [r["id"] for r in rows] == [first["id"], second["id"]]
        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM manual_expenses").fetchone()[0] == 0, \
                "the flag moved a typed expense back into DuckDB"

    @pytest.mark.asyncio
    async def test_the_owner_row_is_written_with_the_first_expense(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")

        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        owners = await _owners(pool)
        assert owners == {EXPENSES: chain_latch.latched_at("pg_expenses_write")}

        # A second write confirms ownership and must not move the moment: the
        # comparison reads it against the shipper's last_ok_at.
        await store.add_expense(date(2026, 9, 18), "marketing", "TikTok Ads", 20)
        assert await _owners(pool) == owners

    @pytest.mark.asyncio
    async def test_neither_replication_path_rolls_the_row_back(self, stores):
        """The hourly job and the one the expense form calls directly. Both
        stand down on the latch alone, and both must report it rather than
        report the table as replaced."""
        from core.pg_operational import replicate_after_manual_expense, replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        env.delenv("KS_WRITE_EXPENSES")

        hourly = await replicate_operational(store)
        direct = await replicate_after_manual_expense(store)

        assert "error" not in hourly, hourly
        assert EXPENSES in hourly["stood_down"] and EXPENSES not in hourly["replaced"]
        assert "error" not in direct, direct
        assert [r["id"] for r in await _expense_rows(pool)] == [typed["id"]]

    @pytest.mark.asyncio
    async def test_the_shipper_stamps_the_disagreement_on_the_table(self, stores):
        """An operator who flipped the variable back finds out from the store,
        not from a number that stopped moving."""
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        env.setenv("KS_WRITE_EXPENSES", "duckdb")

        result = await replicate_operational(store)

        assert result["chain_flag_mismatch"] == {
            "pg_expenses_write": chain_latch.latched_at("pg_expenses_write")}
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT failures_since_ok, last_error, last_ok_at FROM meta.mirror_state "
                "WHERE table_name = $1", EXPENSES)
        assert state["failures_since_ok"] >= 1
        assert "owned by Postgres since" in state["last_error"]
        assert "chain_copy_back" in state["last_error"]
        assert state["last_ok_at"] is None, "a stand-down must not look like a shipment"

    @pytest.mark.asyncio
    async def test_health_says_the_flag_and_the_latch_disagree(self, stores):
        from web.routes.api.health import _write_chains

        store, _pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        env.setenv("KS_WRITE_EXPENSES", "duckdb")

        block = _write_chains()["pg_expenses_write"]
        assert block["mode"] == "postgres" and block["mismatch"] is True
        assert block["latched_at"] == chain_latch.latched_at("pg_expenses_write")

    @pytest.mark.asyncio
    async def test_control_with_both_copies_gone_the_replace_is_still_real(
        self, stores,
    ):
        """What gives every test above its meaning. `replicate_operational`
        never raises, so "the row survived" could pass for the wrong reason;
        with the latch released the same run wipes the same row.

        This is also the shape of DN-08's copy-back, which is the only thing
        that may release a latch — and the only reason it is safe there is that
        it copies the rows to DuckDB first.
        """
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        env.delenv("KS_WRITE_EXPENSES")

        chain_latch.release("pg_expenses_write")
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert EXPENSES not in result.get("stood_down", [])
        assert await _expense_rows(pool) == [], (
            "the replace out of an empty DuckDB did not run — the tests above "
            "would then pass on a job that did nothing")
        assert typed["id"]                                    # the row did exist


class TestChain1AfterTheFlagGoesBack:
    """Inventory, where the cost of a second writer is a movement with two ids.

    `stock_movements` records a delta against the previous `offer_stocks`, and
    KeyCRM has no history endpoint: a movement written under the wrong
    allocator cannot be reconciled with anything afterwards.
    """

    STOCKS = [{"id": 1, "sku": "S-1", "price": 500, "purchased_price": 250,
               "quantity": 40, "reserve": 0}]

    @pytest.mark.asyncio
    async def test_the_movements_and_the_watermark_stay_in_postgres(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        await store.upsert_offers([{"id": 1, "product_id": 101, "sku": "S-1"}])
        assert await store.upsert_stocks(self.STOCKS) == 1
        await store.set_last_sync_time("stocks", datetime(2026, 9, 17, 12, tzinfo=timezone.utc))

        env.delenv("KS_WRITE_INVENTORY")                     # the flag goes back

        changed = [dict(self.STOCKS[0], quantity=37)]
        assert await store.upsert_stocks(changed) == 1

        async with pool.acquire() as conn:
            movements = await conn.fetch(
                "SELECT offer_id, movement_type, delta FROM app.stock_movements "
                "ORDER BY id")
            allocator = await conn.fetchval(
                "SELECT last_value FROM app.stock_movements_id_seq")
        assert [(m["movement_type"], m["delta"]) for m in movements] == [
            ("initial", 40), ("stock_out", -3)]
        assert allocator >= 2, "Postgres' sequence stopped allocating"

        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM stock_movements").fetchone()[0] == 0
            assert conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM stock_movements").fetchone()[0] == 0, \
                "DuckDB's seq_stock_movements_id was consulted"

        # And the watermark is read back from Postgres, not from DuckDB's
        # frozen copy — revision 0032's defect, one flag flip later.
        assert await store.get_last_sync_time("stocks") == datetime(
            2026, 9, 17, 12, tzinfo=timezone.utc)
        async with store.connection() as conn:
            assert conn.execute(
                "SELECT value FROM sync_metadata WHERE key = 'last_sync_stocks'"
            ).fetchone() is None

    @pytest.mark.asyncio
    async def test_all_six_tables_are_claimed_and_stood_down(self, stores):
        from core.pg_operational import replicate_operational
        from core.pg_inventory_write import CHAIN_TABLES

        store, pool, env = stores
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        await store.upsert_offers([{"id": 1, "product_id": 101, "sku": "S-1"}])
        env.delenv("KS_WRITE_INVENTORY")

        assert set(await _owners(pool)) == set(CHAIN_TABLES)

        result = await replicate_operational(store)
        assert "error" not in result, result
        assert set(CHAIN_TABLES) <= set(result["stood_down"])
        assert result["chain_flag_mismatch"] == {
            "pg_inventory_write": chain_latch.latched_at("pg_inventory_write")}


class TestWhenTheFirstWriteFails:
    @pytest.mark.asyncio
    async def test_the_marker_alone_latches_and_the_comparison_says_so(self, stores):
        """The marker is taken before the write and the owner row inside it, so
        a write that fails leaves the chain latched with no audit copy. That is
        the safe direction — the rows that did land stay reachable — and it is
        invisible to everything except the daily comparison, which is the only
        thing that reads both copies.
        """
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")

        with patch("core.pg_expenses_write._ensure_expense_id_floor",
                   new=AsyncMock(side_effect=asyncpg.PostgresError("boom"))):
            with pytest.raises(asyncpg.PostgresError):
                await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        assert chain_latch.latched("pg_expenses_write")
        assert await _owners(pool) == {}, "the owner row survived a rolled-back write"

        issues = await reconcile_operational(store)
        (found,) = [i for i in issues if i.check_name == "chain_latch_disagrees"]
        assert found.severity.value == "CRITICAL"
        assert found.table_name == "pg_expenses_write"
        assert "0 of 1" in found.description

    @pytest.mark.asyncio
    async def test_an_owner_row_with_no_marker_is_the_dangerous_direction(self, stores):
        """The marker is what routes the writes, so without it the flag decides
        again — and a second writer starts the moment it says duckdb."""
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        chain_latch.release("pg_expenses_write")             # the volume was lost

        issues = await reconcile_operational(store)
        (found,) = [i for i in issues if i.check_name == "chain_latch_disagrees"]
        assert "local marker is gone" in found.description

    @pytest.mark.asyncio
    async def test_the_shipper_stands_down_on_the_owner_row_alone(self, stores):
        """The finding above arrives at 07:30. The destruction it describes
        would have happened within the hour.

        `stood_down_tables_checked()` reads the local markers, and the markers
        live on a bind mount: an older `./data` snapshot, a wrong mount or a
        rebuilt data directory loses them while Postgres keeps both the owner
        rows and the rows they were taken for. With the flag back at `duckdb`
        — the rollback an operator would try, and the one this whole change
        exists to survive — the hourly full replace then rebuilt
        `app.manual_expenses` out of a DuckDB that never saw the row.
        Reproduced against this database before the fix: `stood_down` absent,
        `replaced` naming the table, and the row gone while its owner row sat
        in the same database that had just been truncated.

        The shipper is already holding that connection, twenty lines above the
        stand-down, so it asks.
        """
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        assert await _owners(pool) == {EXPENSES: chain_latch.latched_at("pg_expenses_write")}

        chain_latch.release("pg_expenses_write")             # only the routing copy
        env.setenv("KS_WRITE_EXPENSES", "duckdb")            # and the attempted rollback

        result = await replicate_operational(store)

        assert "error" not in result, result
        # The destruction first, because it is the finding: everything below is
        # how the run is supposed to say it did not happen.
        assert [r["id"] for r in await _expense_rows(pool)] == [typed["id"]], (
            "the hourly copy destroyed a row only Postgres held")
        assert EXPENSES in result.get("stood_down", []), "the copy did not stand down"
        assert EXPENSES not in result["replaced"]
        assert result["chain_marker_lost"] == {
            "pg_expenses_write": (await _owners(pool))[EXPENSES]}

        # And the stand-down is not silent: the watermark says why, rather
        # than ageing quietly (DN-01's rule, one state further on).
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT failures_since_ok, last_error FROM meta.mirror_state "
                "WHERE table_name = $1", EXPENSES)
        assert state["failures_since_ok"] >= 1
        assert "no local marker" in state["last_error"]

    @pytest.mark.asyncio
    async def test_the_comparison_stands_down_on_it_too(self, stores):
        """Same rows, same answer. Comparing a table Postgres owns against a
        DuckDB that stopped receiving it files one CRITICAL per row for a state
        `chain_latch_disagrees` already describes in one finding."""
        from core.mirror_reconciliation import reconcile_operational

        store, _pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        chain_latch.release("pg_expenses_write")
        env.setenv("KS_WRITE_EXPENSES", "duckdb")

        issues = await reconcile_operational(store)

        assert [i.check_name for i in issues if i.table_name == EXPENSES] == []
        assert [i for i in issues if i.check_name == "chain_latch_disagrees"]

    @pytest.mark.asyncio
    async def test_a_healthy_latch_files_nothing(self, stores):
        from core.mirror_reconciliation import reconcile_operational

        store, _pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        issues = await reconcile_operational(store)
        assert [i for i in issues if i.check_name.startswith("chain_")] == []


class TestTheShipperOverwroteDetector:
    """E1 of the stage-4 plan. The shipper full-replaces these tables out of
    DuckDB, so a successful shipment dated after the handover wrote over rows
    only Postgres held — and nothing else in the system would ever say so."""

    @pytest.mark.asyncio
    async def test_a_shipment_after_the_handover_is_critical(self, stores):
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        after = datetime.now(timezone.utc) + timedelta(minutes=5)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, "
                "last_ok_at, failures_since_ok, last_rows) "
                "VALUES ($1, $2, $2, 0, 0) ON CONFLICT (table_name) DO UPDATE "
                "SET last_ok_at = EXCLUDED.last_ok_at", EXPENSES, after)

        issues = await reconcile_operational(store)

        (found,) = [i for i in issues if i.check_name == "chain_shipper_overwrote"]
        assert found.severity.value == "CRITICAL" and found.table_name == EXPENSES
        assert "Do not re-run the shipper" in found.description

    @pytest.mark.asyncio
    async def test_the_owner_row_never_moves_forward(self, stores):
        """`ON CONFLICT DO NOTHING` is what this detector stands on, and it was
        asserted only in a comment.

        The sequence it has to survive is the one above: latched at T1, the
        marker lost, the shipper overwrites the table at T2, somebody restores
        `KS_WRITE_EXPENSES=postgres` and the next typed expense mints a *fresh*
        marker at T3 > T2. The finding compares `last_ok_at` against the owner
        row, so if the second write moved that row to T3 the CRITICAL about a
        shipment that did destroy rows would simply go quiet.

        Two writes in one process cannot see this — `latch()` is idempotent and
        hands both the same stamp — so the marker is released in between, which
        is exactly what the dangerous case does to it.
        """
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)
        first = (await _owners(pool))[EXPENSES]

        overwritten = datetime.now(timezone.utc) + timedelta(seconds=1)
        chain_latch.release("pg_expenses_write")
        await asyncio.sleep(1.1)                     # a later moment, honestly
        await store.add_expense(date(2026, 9, 18), "marketing", "TikTok Ads", 20)
        second = chain_latch.latched_at("pg_expenses_write")

        assert second > first, "the second marker is not later — the test proves nothing"
        assert (await _owners(pool))[EXPENSES] == first, (
            "the owner row drifted forward; the handover moment is not the "
            "first write any more")

        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, "
                "last_ok_at, failures_since_ok, last_rows) "
                "VALUES ($1, $2, $2, 0, 0) ON CONFLICT (table_name) DO UPDATE "
                "SET last_ok_at = EXCLUDED.last_ok_at", EXPENSES, overwritten)

        issues = await reconcile_operational(store)
        (found,) = [i for i in issues if i.check_name == "chain_shipper_overwrote"]
        assert found.table_name == EXPENSES and first in found.description

    @pytest.mark.asyncio
    async def test_a_shipment_before_the_handover_is_not(self, stores):
        """The ordinary case on the day of a switch: the table was shipped for
        months, and the last shipment is older than the first typed row."""
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        before = datetime.now(timezone.utc) - timedelta(hours=1)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, "
                "last_ok_at, failures_since_ok, last_rows) VALUES ($1, $2, $2, 0, 0)",
                EXPENSES, before)
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        issues = await reconcile_operational(store)
        assert [i for i in issues if i.check_name == "chain_shipper_overwrote"] == []


class TestTheOrderTablesOnAnOwnerRowAlone:
    """DN-22a against the real `meta.chain_watermarks`. An image rolled back to
    a build older than the orders chain keeps the chain's owner row and forgets
    the chain: no module here declares an order table, so `claimed_tables`
    cannot expand the row, and only the row itself can say the tables moved.
    The recorder in the unit tests hands back keys it made up; this reads the
    ones `read_owners` actually strips out of the table."""

    ORDER_ID = 990_001

    @pytest.mark.asyncio
    async def test_the_backfill_refuses_and_postgres_gains_no_order(
            self, stores, monkeypatch):
        from core import write_chains
        from core.pg_backfill import backfill_orders
        from core.pg_landing import (
            ORDER_PRODUCTS_TABLE, ORDERS_TABLE, order_tables_stood_down_or_owned,
        )

        store, pool, _env = stores
        orders = {ORDERS_TABLE, ORDER_PRODUCTS_TABLE}
        # The rolled-back build, made rather than assumed: once the orders
        # chain is registered for real this must still be a registry that
        # declares no order table, or the row is read through `claimed_tables`.
        monkeypatch.setattr(write_chains, "WRITE_CHAINS", tuple(
            c for c in write_chains.WRITE_CHAINS if not orders & set(c.CHAIN_TABLES)))

        when = "2026-09-20T12:00:00+00:00"
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([{
                "id": self.ORDER_ID, "source_id": 1, "status_id": 12,
                "status_group_id": 4, "grand_total": "100.00",
                "ordered_at": when, "created_at": when, "updated_at": when,
                "buyer": {"id": 5001}, "manager": {"id": 4},
                "manager_comment": None, "promocode": None,
                "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                              "offer": {"product_id": 701}}],
            }])
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ($1, $2, now())", chain_latch.owner_key(ORDERS_TABLE), when)
            versions = await conn.fetchval("SELECT count(*) FROM app.order_versions")

        assert await order_tables_stood_down_or_owned(pool) == frozenset(orders)
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)

        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM bronze.orders WHERE id = $1", self.ORDER_ID) == 0
            assert await conn.fetchval("SELECT count(*) FROM app.order_versions") == versions


class TestWhatTheOrderStandDownTellsAHuman:
    """DN-22a's review, against a real `meta.chain_watermarks` and a real
    comparison. With only the owner rows standing the order tables down, the
    finding stayed INFO, and its label, its lever and its registry entry all
    said "not a defect" while the sync's per-tick mirror — which asks only the
    local answer — went on writing DuckDB's copy over the chain's rows. A page
    and the digest print the label and the lever, never the description.

    Every state starts from the same clean comparison: one order in DuckDB,
    shipped by the real mirror, the backfill finished, and nothing to find.
    The findings are persisted into the DuckDB journal the way the 07:30 job
    persists them and judged from the rows read back."""

    ORDER_ID = 990_101
    ORDERS_CHAIN = "pg_orders_write"

    async def _reset(self, pool):
        async with pool.acquire() as conn:
            # The whole tables: the comparison is of the whole tables, and a
            # row another test left behind would be a finding in every state.
            await conn.execute("DELETE FROM bronze.order_products")
            await conn.execute("DELETE FROM bronze.orders")
            await conn.execute(
                "DELETE FROM app.order_versions WHERE order_id = $1", self.ORDER_ID)
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                ["bronze.orders", "bronze.order_products"])

    @pytest_asyncio.fixture
    async def compared(self, stores):
        """A clean, live comparison in the state production is in today."""
        from core.mirror_reconciliation import reconcile_orders
        from core.pg_backfill import backfill_orders

        store, pool, env = stores
        env.delenv("KS_WRITE_ORDERS", raising=False)
        await self._reset(pool)
        when = "2026-09-20T12:00:00+00:00"
        await store.upsert_orders([{
            "id": self.ORDER_ID, "source_id": 1, "status_id": 12,
            "status_group_id": 4, "grand_total": "100.00",
            "ordered_at": when, "created_at": when, "updated_at": when,
            "buyer": {"id": 5101}, "manager": {"id": 4},
            "manager_comment": None, "promocode": None,
            "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                          "offer": {"product_id": 701}}],
        }])
        await backfill_orders(store)                   # stamps backfilled_at
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM bronze.orders WHERE id = $1", self.ORDER_ID) == 1
        assert await reconcile_orders(store) == [], "the baseline is not clean"
        yield store, pool, env
        await self._reset(pool)

    def _declare_orders_chain(self, env, flag_postgres: bool) -> None:
        import types

        from core import write_chains

        fake = types.ModuleType(f"core.{self.ORDERS_CHAIN}")
        fake.WRITE_ENV = "KS_WRITE_ORDERS"
        fake.CHAIN_TABLES = ("bronze.orders", "bronze.order_products")
        fake.env_writes_postgres = lambda: flag_postgres
        env.setattr(write_chains, "WRITE_CHAINS", write_chains.WRITE_CHAINS + (fake,))

    @staticmethod
    async def _owner_row(pool) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ($1, $2, now())",
                chain_latch.owner_key("bronze.orders"), "2026-09-21T08:00:00+00:00")

    @staticmethod
    async def _persisted(store, issues):
        from core.data_quality import fetch_latest_run, fetch_run_issues, persist_run
        from core.mirror_reconciliation import MIRROR_LAYER

        now = datetime.now(timezone.utc)
        async with store.connection() as conn:
            run_id = persist_run(
                conn, started_at=now, ended_at=now, as_of=now,
                window_start=now.date(), window_end=now.date(),
                layer=MIRROR_LAYER, issues=issues, discrepancies=[],
            )
            return fetch_latest_run(conn, MIRROR_LAYER), fetch_run_issues(conn, run_id)

    @staticmethod
    def _page(run, rows) -> str:
        """What reaches a phone: the page when the run is CRITICAL, and the
        digest's line per finding — label and lever, never the description."""
        from core.data_quality import (
            IntegrityIssue, Severity, format_alert_message, human_check_name,
        )
        from core.mirror_reconciliation import MIRROR_LAYER

        issues = [IntegrityIssue(
            check_name=r["check_name"], table_name=r["table_name"],
            severity=Severity(r["severity"]), count=r["count"],
            description=r["description"]) for r in rows]
        lines = [f"• {human_check_name(r['check_name'])}: {r['count']}" for r in rows]
        if run["status"] == "CRITICAL":
            lines.insert(0, format_alert_message(
                MIRROR_LAYER, Severity.CRITICAL, issues, []))
        return "\n".join(lines)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", [
        "no_chain", "marker_present", "marker_lost", "rolled_back"])
    async def test_the_persisted_verdict(self, compared, state):
        from core import write_chains
        from core.mirror_reconciliation import reconcile_orders
        from core.pg_landing import order_tables_stood_down

        store, pool, env = compared
        if state == "marker_present":
            # Flag back at duckdb, both copies of the latch agree.
            self._declare_orders_chain(env, flag_postgres=False)
            chain_latch.latch(self.ORDERS_CHAIN)
            await self._owner_row(pool)
            assert order_tables_stood_down(), "the marker must route the writes"
        elif state == "marker_lost":
            self._declare_orders_chain(env, flag_postgres=False)
            await self._owner_row(pool)
        elif state == "rolled_back":
            env.setattr(write_chains, "WRITE_CHAINS", tuple(
                c for c in write_chains.WRITE_CHAINS
                if not {"bronze.orders", "bronze.order_products"} & set(c.CHAIN_TABLES)))
            await self._owner_row(pool)
        if state in ("marker_lost", "rolled_back"):
            assert not chain_latch.latched(self.ORDERS_CHAIN)
            assert order_tables_stood_down() == frozenset()

        run, rows = await self._persisted(store, await reconcile_orders(store))
        found = sorted((r["check_name"], r["table_name"], r["severity"], r["count"])
                       for r in rows)
        page = self._page(run, rows)

        if state == "no_chain":
            # Production today: the comparison ran clean and nothing is filed.
            assert found == [] and run["status"] == "PASS", (found, run)
        elif state == "marker_present":
            assert found == [
                ("mirror_stood_down", "bronze.order_products", "INFO", 1),
                ("mirror_stood_down", "bronze.orders", "INFO", 1),
            ]
            assert run["status"] == "PASS" and run["critical_count"] == 0, run
        else:
            assert found == [(
                "order_owner_row_without_marker",
                "bronze.order_products, bronze.orders", "CRITICAL", 2)]
            assert run["status"] == "CRITICAL" and run["critical_count"] == 1, run
            assert "not a defect" not in page.lower(), page
            assert "the sync is overwriting order tables a write chain owns" in page
            (lever,) = [ln for ln in page.splitlines() if ln.startswith("→ ")]
            assert "scripts/chain_copy_back.py" in lever, lever
            # And the claim is true of this database: the sync's mirror is
            # still shipping — the next tick wrote the order over again.
            async with pool.acquire() as conn:
                before = await conn.fetchval(
                    "SELECT mirrored_at FROM bronze.orders WHERE id = $1", self.ORDER_ID)
            later = "2026-09-22T12:00:00+00:00"
            await store.upsert_orders([{
                "id": self.ORDER_ID, "source_id": 1, "status_id": 20,
                "status_group_id": 4, "grand_total": "100.00",
                "ordered_at": later, "created_at": later, "updated_at": later,
                "buyer": {"id": 5101}, "manager": {"id": 4},
                "manager_comment": None, "promocode": None,
                "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                              "offer": {"product_id": 701}}],
            }])
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT status_id, mirrored_at FROM bronze.orders WHERE id = $1",
                    self.ORDER_ID)
            assert row["status_id"] == 20 and row["mirrored_at"] > before, row
        if state in ("no_chain", "marker_present"):
            assert "order_owner_row_without_marker" not in page
