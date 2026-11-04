"""The ownership latch against a real Postgres: both copies, both chains.

DN-06, owner decision OD-19 (a). The unit tests pin the local marker; this pins
what the marker buys — that a flag flipped back does not move the writes, that
the id allocator does not change hands with it, that the hourly shipper cannot
roll the rows back, and that the two copies of the latch are compared daily.

Every test drives the repository methods or the real job, never the writer in
isolation: the flag was consulted per call at eight sites, and a test that
called `pg_expenses_write.add_expense` directly would prove nothing about the
route a typed expense actually takes.

Since step 14's S14-00 every chain writes Postgres whatever its flag or its
marker says, so the tests whose setup needed a chain on DuckDB went with
that path: the controls proving the replace would otherwise wipe the rows,
and the order and classification shippers on an owner row alone — states the
shippers out of DuckDB (S14-13) can no longer reach from this code. What a
lost marker means for an image from before step 14 is still filed — by the
daily comparison until step 14's S14-12, by the chain invariants on the
integrity layer since (`chain_latch_disagrees`, decision 12).
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timezone
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


async def _latch_findings(pool):
    """`chain_latch_disagrees` as the integrity job files it: the chain
    invariants' read and verdict, over the live database and the markers on
    disk."""
    from core import pg_chain_invariants as inv

    facts = await inv.read_facts(pool=pool)
    return [i for i in inv.check_chain_invariants(facts)
            if i.check_name == "chain_latch_disagrees"]


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
    async def test_the_flag_put_back_is_read_by_nothing(self, stores):
        """Since step 14's S14-11 no flag is read: the copy stamps no
        disagreement on the table (until then `chain_flag_mismatch` and
        "owned by Postgres since …", so an operator who flipped the variable
        back found out from the store), it ships nothing over it, and
        `/api/health` publishes nothing of the flag."""
        from core.pg_operational import replicate_operational
        from web.routes.api.health import _write_chains

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        env.setenv("KS_WRITE_EXPENSES", "duckdb")

        result = await replicate_operational(store)

        assert "chain_flag_mismatch" not in result
        assert EXPENSES in result["stood_down"]
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT failures_since_ok, last_ok_at FROM meta.mirror_state "
                "WHERE table_name = $1", EXPENSES)
        assert state is None or (not state["failures_since_ok"]
                                 and state["last_ok_at"] is None), dict(state or {})
        assert _write_chains()["pg_expenses_write"] == {}


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
        assert "chain_flag_mismatch" not in result      # no flag read since S14-11


class TestWhenTheFirstWriteFails:
    @pytest.mark.asyncio
    async def test_the_marker_alone_latches_and_files_nothing(self, stores):
        """The marker is taken before the write and the owner row inside it, so
        a write that fails leaves the chain latched with no audit copy. That is
        the safe direction — the rows that did land stay reachable — and since
        step 14 nothing reads either copy to route, so it is not filed: the
        daily comparison filed it until S14-12, and decision 12 kept only the
        other direction. Mutation: judge every disagreement both ways.
        """
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")

        with patch("core.pg_expenses_write._ensure_expense_id_floor",
                   new=AsyncMock(side_effect=asyncpg.PostgresError("boom"))):
            with pytest.raises(asyncpg.PostgresError):
                await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        assert chain_latch.latched("pg_expenses_write")
        assert await _owners(pool) == {}, "the owner row survived a rolled-back write"

        assert await _latch_findings(pool) == []

    @pytest.mark.asyncio
    async def test_an_owner_row_with_no_marker_is_the_dangerous_direction(self, stores):
        """In an image from before step 14 — the rollback target — the marker
        is what routes the writes, so without it the flag decides again, and
        a second writer starts the moment it says duckdb. Filed CRITICAL by
        the chain invariants, read from the live owner rows and the markers on
        disk afresh (decision 12). Mutation: judge the markers by
        `watched_chains()`, which a step-14 build answers for every chain
        whatever is on disk."""
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)
        since = (await _owners(pool))[EXPENSES]

        chain_latch.release("pg_expenses_write")             # the volume was lost

        (found,) = await _latch_findings(pool)
        assert found.severity.value == "CRITICAL"
        assert found.table_name == "pg_expenses_write"
        assert "marker under data/write-chain-owners/ is gone" in found.description
        assert f"latched_at = {since}" in found.description
        assert "1 of its 1 table(s)" in found.description

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
        from core.rollback_lever import ROLLBACK_LEVER

        assert ROLLBACK_LEVER in state["last_error"]

    @pytest.mark.asyncio
    async def test_a_healthy_latch_files_nothing(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        assert await _latch_findings(pool) == []


class TestTheOwnerRowNeverMovesForward:
    """E1 of the stage-4 plan (`chain_shipper_overwrote`) read a shipment
    after the handover off this row, until step 14's S14-12 deleted it with
    the daily comparison; the row still dates the handover — the chain
    invariants' "since the latch" verdicts, and `chain_latch_disagrees`'s
    restore instruction, read it."""

    @pytest.mark.asyncio
    async def test_a_second_marker_does_not_move_it(self, stores):
        """`ON CONFLICT DO NOTHING`, asserted rather than read in a comment.
        Latched at T1, the marker lost, and the next typed expense mints a
        *fresh* marker at T2 > T1: the row stays at T1. Two writes in one
        process cannot see this — `latch()` is idempotent and hands both the
        same stamp — so the marker is released in between, which is exactly
        what the dangerous case does to it."""
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)
        first = (await _owners(pool))[EXPENSES]

        chain_latch.release("pg_expenses_write")
        await asyncio.sleep(1.1)                     # a later moment, honestly
        await store.add_expense(date(2026, 9, 18), "marketing", "TikTok Ads", 20)
        second = chain_latch.latched_at("pg_expenses_write")

        assert second > first, "the second marker is not later — the test proves nothing"
        assert (await _owners(pool))[EXPENSES] == first, (
            "the owner row drifted forward; the handover moment is not the "
            "first write any more")


class TestTheLandingTablesOnAnOwnerRowAlone:
    """DN-22b against the real `meta.chain_watermarks`: the buyers and the
    order-level expenses, on an image older than the chain that owns them. No
    module here declares either table, so only the owner rows can say they
    moved — and the paths that hold a pool must read them and write nothing.
    (The comparisons that paged it went in step 14's S14-12.) The control
    first, on the same database: without the rows the same calls ship."""

    BUYER_ID = 990_201
    EXPENSE_ID = 990_202
    BUYERS = ("bronze.buyers", "bronze.buyer_contacts")
    EXPENSES = "bronze.expenses"

    async def _reset(self, pool):
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM bronze.buyer_contacts")
            await conn.execute("DELETE FROM bronze.buyers")
            await conn.execute("DELETE FROM bronze.expenses")
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                [*self.BUYERS, self.EXPENSES])

    @pytest_asyncio.fixture
    async def landed(self, stores):
        """DuckDB holds one buyer with a contact and one expense; Postgres
        holds neither, and no chain here declares either table."""
        from core import write_chains

        store, pool, env = stores
        await self._reset(pool)
        tables = {*self.BUYERS, self.EXPENSES}
        env.setattr(write_chains, "WRITE_CHAINS", tuple(
            c for c in write_chains.WRITE_CHAINS if not tables & set(c.CHAIN_TABLES)))
        async with store.connection() as conn:
            conn.execute("INSERT INTO buyers (id, full_name) VALUES (?, 'Anna')",
                         [self.BUYER_ID])
            conn.execute("INSERT INTO buyer_contacts (buyer_id, contact_type, value, "
                         "is_primary) VALUES (?, 'phone', '+380500000201', TRUE)",
                         [self.BUYER_ID])
            conn.execute("INSERT INTO expenses (id, order_id, expense_type_id, amount) "
                         "VALUES (?, 1, 1, 100.00)", [self.EXPENSE_ID])
        yield store, pool
        await self._reset(pool)

    @staticmethod
    async def _own(pool, *tables):
        async with pool.acquire() as conn:
            for table in tables:
                await conn.execute(
                    "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                    "VALUES ($1, $2, now())",
                    chain_latch.owner_key(table), "2026-09-21T08:00:00+00:00")

    @staticmethod
    async def _count(pool, sql, *args):
        async with pool.acquire() as conn:
            return await conn.fetchval(sql, *args)

    @pytest.mark.asyncio
    async def test_the_control_ships_both(self, landed):
        from core.pg_buyers import hourly_ids_diff
        from core.pg_expense_backfill import hourly_expenses_ids_diff

        store, pool = landed
        assert (await hourly_ids_diff(store))["shipped"] >= 1
        assert (await hourly_expenses_ids_diff(store))["shipped"] >= 1
        assert await self._count(
            pool, "SELECT count(*) FROM bronze.buyers WHERE id = $1", self.BUYER_ID) == 1
        assert await self._count(
            pool, "SELECT count(*) FROM bronze.expenses WHERE id = $1", self.EXPENSE_ID) == 1

    @pytest.mark.asyncio
    async def test_the_owner_rows_hold_them_and_postgres_gains_nothing(self, landed):
        from core.pg_buyers import BUYER_UNIT, backfill_buyers, hourly_ids_diff
        from core.pg_expense_backfill import (
            EXPENSES_UNIT, backfill_expenses, hourly_expenses_ids_diff,
        )
        from core.pg_landing import tables_stood_down, tables_stood_down_or_owned

        store, pool = landed
        # One row per unit, and for the buyers the half that is not named.
        await self._own(pool, "bronze.buyer_contacts", self.EXPENSES)
        assert tables_stood_down(BUYER_UNIT) == frozenset()
        assert await tables_stood_down_or_owned(pool, BUYER_UNIT) == frozenset(self.BUYERS)
        assert await tables_stood_down_or_owned(pool, EXPENSES_UNIT) == {self.EXPENSES}

        assert await hourly_ids_diff(store) == {"stood_down": sorted(self.BUYERS)}
        assert await hourly_expenses_ids_diff(store) == {"stood_down": [self.EXPENSES]}
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_buyers(store)
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_expenses(store)

        assert await self._count(pool, "SELECT count(*) FROM bronze.buyers") == 0
        assert await self._count(pool, "SELECT count(*) FROM bronze.buyer_contacts") == 0
        assert await self._count(pool, "SELECT count(*) FROM bronze.expenses") == 0
        assert await self._count(
            pool, "SELECT count(*) FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            [*self.BUYERS, self.EXPENSES]) == 0

    @pytest.mark.asyncio
    async def test_the_buyers_mirror_stands_down_on_them_too(self, landed):
        """The sync's own shipper of the buyers: nothing lands, no watermark
        moves. The control is the same batch without the row."""
        from core.models import Buyer
        from core.pg_buyers import mirror_buyers

        store, pool = landed
        batch = [Buyer(id=self.BUYER_ID, full_name="Anna", phones=["+380500000201"])]
        await self._own(pool, "bronze.buyer_contacts")
        out = await mirror_buyers(batch)
        assert out["stood_down"] == sorted(self.BUYERS), out
        assert await self._count(pool, "SELECT count(*) FROM bronze.buyers") == 0
        assert await self._count(
            pool, "SELECT count(*) FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(self.BUYERS)) == 0

        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks")
        assert (await mirror_buyers(batch)) == {"rows": 1}
        assert await self._count(
            pool, "SELECT count(*) FROM bronze.buyers WHERE id = $1", self.BUYER_ID) == 1


class TestAnOwnerRowNoChainHereDeclares:
    """The DN-22b review, against the real `meta.chain_watermarks`: an image
    older than chain 4, owner rows for its three tables, and a verdict a human
    overrode in Postgres that DuckDB never saw. The buyers' own paths stood
    down on the rows; the hourly full replace did not, and put DuckDB's
    verdict back within the hour — after which the two copies agreed and the
    comparison had nothing to say. (The comparison, and its
    `chain_owner_unregistered`, went in step 14's S14-12; the copy stands down
    on the rows until S14-13 deletes it.) The control first, on the same
    database."""

    BUYER_ID = 990_301
    TABLES = ("bronze.buyers", "bronze.buyer_contacts", "app.buyer_gender")

    @pytest_asyncio.fixture
    async def overridden(self, stores):
        """DuckDB says 'f' by name; Postgres holds a human's 'm'. No chain in
        this build declares any of the three tables."""
        from core import write_chains

        store, pool, env = stores
        env.setattr(write_chains, "WRITE_CHAINS", tuple(
            c for c in write_chains.WRITE_CHAINS
            if not set(self.TABLES) & set(c.CHAIN_TABLES)))
        async with store.connection() as conn:
            conn.execute("INSERT INTO buyers (id, full_name) VALUES (?, 'Anna')",
                         [self.BUYER_ID])
            conn.execute(
                "INSERT INTO buyer_gender (buyer_id, gender, method, confidence, "
                "decided_from, rules_version, override_by_human, decided_at) VALUES "
                "(?, 'f', 'name', 'high', 'first_name', 1, FALSE, now())",
                [self.BUYER_ID])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.buyer_gender")
            await conn.execute(
                "INSERT INTO app.buyer_gender (buyer_id, gender, method, confidence, "
                "decided_from, rules_version, override_by_human, decided_at) VALUES "
                "($1, 'm', 'human', 'certain', 'first_name', 1, TRUE, now())",
                self.BUYER_ID)
        yield store, pool
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.buyer_gender")

    async def _verdict(self, pool):
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT gender, method, override_by_human FROM app.buyer_gender "
                "WHERE buyer_id = $1", self.BUYER_ID)
        return dict(row) if row else None

    @pytest.mark.asyncio
    async def test_the_control_replaces_the_override(self, overridden):
        from core.pg_operational import replicate_operational

        store, pool = overridden
        result = await replicate_operational(store)
        assert "error" not in result, result
        assert await self._verdict(pool) == {
            "gender": "f", "method": "name", "override_by_human": False}

    @pytest.mark.asyncio
    async def test_the_owner_rows_keep_it(self, overridden):
        from core.pg_buyers import hourly_ids_diff
        from core.pg_operational import replicate_operational

        store, pool = overridden
        stamp = "2026-09-21T08:00:00+00:00"
        async with pool.acquire() as conn:
            for table in self.TABLES:
                await conn.execute(
                    "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                    "VALUES ($1, $2, now())", chain_latch.owner_key(table), stamp)

        # The buyers' own path, as before this fix — the two now agree.
        assert "stood_down" in await hourly_ids_diff(store)
        result = await replicate_operational(store)

        assert "error" not in result, result
        assert await self._verdict(pool) == {
            "gender": "m", "method": "human", "override_by_human": True}, (
            "the hourly copy replaced a verdict only Postgres held")
        assert "app.buyer_gender" in result["stood_down"]
        assert result["chain_owner_unregistered"] == {"app.buyer_gender": stamp}
