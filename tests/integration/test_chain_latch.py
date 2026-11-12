"""The ownership latch against a real Postgres: both copies, both chains.

DN-06, owner decision OD-19 (a). The unit tests pin the local marker; this pins
what the marker buys — that a flag flipped back does not move the writes, that
the id allocator does not change hands with it, and that the two copies of the
latch are compared.

Every test drives the repository methods or the real job, never the writer in
isolation: the flag was consulted per call at eight sites, and a test that
called `pg_expenses_write.add_expense` directly would prove nothing about the
route a typed expense actually takes.

Since step 14's S14-00 every chain writes Postgres whatever its flag or its
marker says, so the tests whose setup needed a chain on DuckDB went with
that path: the controls proving the replace would otherwise wipe the rows,
and the order and classification shippers on an owner row alone. Since S14-13
the shippers out of DuckDB are gone too — the hourly copy, the ids-diffs, the
backfills and the buyers mirror — and with them the tests that they stood
down on the latch or on an owner row alone. What a
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
    from core.store import AnalyticsStore

    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS"):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = AnalyticsStore()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()


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
    async def test_the_flag_put_back_is_read_by_nothing(self, stores):
        """Since step 14's S14-11 no flag is read: `/api/health` publishes
        nothing of it (until then `chain_flag_mismatch`), and the next typed
        expense still lands in Postgres. Until S14-13 the hourly copy was
        asked too, and stamped nothing on the table; the copy is gone."""
        from web.routes.api.health import _write_chains

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        first = await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 4200)
        env.setenv("KS_WRITE_EXPENSES", "duckdb")

        second = await store.add_expense(date(2026, 9, 18), "marketing", "TikTok Ads", 900)

        assert [r["id"] for r in await _expense_rows(pool)] == [first["id"], second["id"]]
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


        # And the watermark is read back from Postgres, not from DuckDB's
        # frozen copy — revision 0032's defect, one flag flip later.
        assert await store.get_last_sync_time("stocks") == datetime(
            2026, 9, 17, 12, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_all_six_tables_are_claimed(self, stores):
        """The first write claims every table of the chain. Until step 14's
        S14-13 the hourly copy was asked here too, and stood down for all
        six; the copy is gone."""
        from core.pg_inventory_write import CHAIN_TABLES

        store, pool, env = stores
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        await store.upsert_offers([{"id": 1, "product_id": 101, "sku": "S-1"}])
        env.delenv("KS_WRITE_INVENTORY")

        assert set(await _owners(pool)) == set(CHAIN_TABLES)


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
    async def test_a_marker_lost_while_the_process_runs_is_read_from_disk(self, stores):
        """The marker is read afresh on every run, never the process's cache:
        a volume lost under a running web must not wait for a restart to be
        filed. The file is unlinked by hand — `release()` would also empty
        the cache, so a `read_facts` answering from the cache passed the test
        above. Mutation: `_latch_facts(owners, dict(chain_latch._cache()))`."""
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)
        assert await _latch_findings(pool) == []

        chain_latch.marker_path("pg_expenses_write").unlink()
        assert chain_latch.latched("pg_expenses_write"), "the cache forgot it"

        (found,) = await _latch_findings(pool)
        assert (found.table_name, found.severity.value) == ("pg_expenses_write",
                                                            "CRITICAL")

    @pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0,
                        reason="root lists a directory whatever its mode")
    @pytest.mark.asyncio
    async def test_a_directory_that_cannot_be_listed_is_blindness(self, stores):
        """Not a lost marker: an unreadable `data/write-chain-owners` says
        nothing about whether a marker is there. The run is
        `chain_invariants_unwatched` on that directory, which holds every
        condition, and never `chain_latch_disagrees` with a restore
        instruction. Through the real read: `Path.glob` swallowed the
        PermissionError and filed the CRITICAL. Mutation: list with
        `MARKER_DIR.glob('*')`."""
        from core import pg_chain_invariants as inv

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        chain_latch.MARKER_DIR.chmod(0o000)
        try:
            facts = await inv.read_facts(pool=pool)
        finally:
            chain_latch.MARKER_DIR.chmod(0o700)
        issues = inv.check_chain_invariants(facts)

        assert isinstance(facts.latch, inv.Unwatched), facts.latch
        assert [i for i in issues if i.check_name == inv.LATCH_DISAGREES] == []
        blind = [i for i in issues if (i.check_name, i.table_name)
                 == (inv.UNWATCHED, inv.MARKERS_PART)]
        assert len(blind) == 1, issues
        assert inv.LATCH_DISAGREES in inv.unverified_conditions(blind)

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
