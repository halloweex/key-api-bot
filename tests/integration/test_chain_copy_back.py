"""The way back from a latched chain, against a real Postgres.

DN-08. Since DN-06 a chain that has written Postgres is latched and editing
`KS_WRITE_*` back to `duckdb` undoes nothing — so `core/chain_transfer.py` is
the only rollback chains 1 and 8 have, and the only thing that may release a
latch. These tests are what says it works: the rows arrive, the comparison is
what decides, the allocator cannot reissue an id, and a chain whose copy did
not compare keeps its latch.

Since the review of the first draft, two more things are pinned here, because
both were reproduced against it: nothing DuckDB holds is destroyed by the copy
itself — the handover question is asked before anything is written — and a
copy that does not compare leaves DuckDB exactly as it found it, because the
write and the comparison are one transaction.

Everything goes through the repository methods, `tests/integration/
test_chain_latch.py`'s rule and for its reason: the flag was consulted per
call at eight sites, so a test that drove the writers directly would prove
nothing about the route a typed expense or a synced stock level actually
takes.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import tracemalloc
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, chain_transfer, pg_expenses_write, pg_inventory_write
from core.chain_transfer import CopyBackRefused, copy_back, handover_check
from core.duckdb_sequences import next_value

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

EXPENSES = "app.manual_expenses"
MOVEMENTS = "app.stock_movements"

# One offer, because the point of every inventory case here is a delta and a
# delta needs a previous value, not a population.
OFFERS = [{"id": 1, "product_id": 101, "sku": "S-1"}]
STOCKS = [{"id": 1, "sku": "S-1", "price": 500, "purchased_price": 250,
           "quantity": 40, "reserve": 2}]

CHAIN_TABLES = tuple(pg_inventory_write.CHAIN_TABLES) + tuple(
    pg_expenses_write.CHAIN_TABLES)


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in CHAIN_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.chain_watermarks")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(CHAIN_TABLES),
        )
        for sequence in ("app.manual_expenses_id_seq", "app.stock_movements_id_seq"):
            await conn.execute(f"SELECT setval('{sequence}', 1, false)")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB with the whole schema and an empty Postgres, both live."""
    from core.duckdb_store import DuckDBStore

    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "copyback.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _duck(store, sql, params=None):
    async with store.connection() as conn:
        return conn.execute(sql, params or []).fetchall()


async def _pg(pool, sql, *params):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *params)


async def _script(store, env, *argv):
    """`scripts/chain_copy_back.py` itself, on this test's DuckDB file.

    `main` is `asyncio.run(_run(...))`, which cannot nest inside the test's
    loop, so this awaits `_run` — which is everything `main` does besides.
    The script opens the file itself with `DuckDBStore()` on `DB_PATH` and
    closes it on the way out, so the fixture's store lets go first; it
    reopens on its next use.
    """
    from scripts import chain_copy_back as script

    await store.close()
    env.setattr("core.duckdb_store.DB_PATH", store.db_path)
    return await script._run(script._parse(list(argv)))


class _Hop:
    """A TCP hop between a pool and the real Postgres, which a test can cut.

    Cutting it is Postgres going away as a client sees it: every open
    connection is torn down mid-flight and every new one is refused. Nothing
    about the driver or the server is mocked — `release_chain` meets a dead
    connection and `_latch_state` a refused one, the way they would if the
    host dropped Postgres between the comparison and the release.
    """

    def __init__(self, dsn: str):
        parts = urlsplit(dsn)
        self._dsn = parts
        self._upstream = (parts.hostname, parts.port or 5432)
        self._links: list = []

    async def __aenter__(self) -> "_Hop":
        self._server = await asyncio.start_server(self._link, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        auth = self._dsn.netloc.rpartition("@")[0]
        self.dsn = urlunsplit(self._dsn._replace(netloc=f"{auth}@127.0.0.1:{port}"))
        return self

    async def __aexit__(self, *exc) -> None:
        await self.cut()

    async def _link(self, reader, writer):
        up_reader, up_writer = await asyncio.open_connection(*self._upstream)
        self._links += [writer, up_writer]
        await asyncio.gather(self._pipe(reader, up_writer),
                             self._pipe(up_reader, writer))

    @staticmethod
    async def _pipe(reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def cut(self) -> None:
        self._server.close()
        for writer in self._links:
            writer.transport.abort()
        await self._server.wait_closed()
        # One turn of the loop for the client side to see the teardown.
        # Whether the pooled connection then fails at acquire or in flight is
        # the selector's race — observed: in flight, as
        # ConnectionDoesNotExistError — and the outcome under test is the
        # same either way: an exception from a Postgres that is gone.
        await asyncio.sleep(0)


# Every DuckDB table a chain copy-back can touch, plus the watermarks.
DUCK_TABLES = ("offers", "offer_stocks", "stock_movements", "sku_inventory_status",
               "inventory_sku_history", "inventory_history", "manual_expenses",
               "sync_metadata")


async def _snapshot(store):
    """DuckDB as a refusal must leave it: every row of every chain table."""
    out = {}
    async with store.connection() as conn:
        for table in DUCK_TABLES:
            out[table] = sorted(conn.execute(f"SELECT * FROM {table}").fetchall(),
                                key=repr)
    return out


async def _latch_kept(pool, chain):
    stamp = chain_latch.latched_at(chain.CHAIN)
    owners = await chain_latch.read_owners(pool)
    return stamp is not None and all(owners.get(t) for t in chain.CHAIN_TABLES)


async def _before_the_flip(store):
    """DuckDB writes, the hourly copy ships: the ordinary state."""
    from core.pg_operational import replicate_operational

    await store.upsert_offers(OFFERS)
    await store.upsert_stocks(STOCKS)              # one `initial` movement
    await store.refresh_sku_inventory_status()
    await store.record_sku_inventory_snapshot()
    await store.record_inventory_snapshot()
    shipped = await replicate_operational(store)
    assert "error" not in shipped, shipped


async def _after_the_flip(store, env, quantity=37):
    """Postgres writes: a movement above DuckDB's MAX and a changed base."""
    env.setenv("KS_WRITE_INVENTORY", "postgres")
    await store.upsert_stocks([dict(STOCKS[0], quantity=quantity)])
    await store.refresh_sku_inventory_status()
    await store.record_inventory_snapshot(force=True)
    await store.set_last_sync_time(
        "stocks", datetime(2026, 9, 17, 12, tzinfo=timezone.utc))


class TestChain8Expenses:
    """Three amounts a human typed, carried back and proved."""

    @pytest.mark.asyncio
    async def test_the_rows_arrive_the_latch_goes_and_duckdb_allocates_again(
        self, stores,
    ):
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = [
            await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 4200),
            await store.add_expense(date(2026, 9, 16), "marketing", "TikTok Ads", 900),
            await store.add_expense(date(2026, 9, 17), "logistics", "Nova Poshta", 315.5),
        ]
        assert chain_latch.latched("pg_expenses_write")

        result = await copy_back(store, pg_expenses_write, dry_run=False)

        assert result["findings"] == [], result["findings"]
        assert result["released"] is True
        assert result["rows"] == {EXPENSES: 3}

        rows = await _duck(
            store, "SELECT id, expense_date, category, expense_type, amount, "
                   "currency, note, created_at, updated_at, platform "
                   "FROM manual_expenses ORDER BY id")
        assert [r[0] for r in rows] == [e["id"] for e in typed]
        assert [float(r[4]) for r in rows] == [4200.0, 900.0, 315.5]

        # Both copies of the latch, and only on a clean comparison.
        assert not chain_latch.latched("pg_expenses_write")
        assert await chain_latch.read_owners(pool) == {}

        # The flag decides again, so the hourly copy resumes — and writes
        # exactly what came back, which is the whole claim.
        from core.pg_operational import replicate_operational

        env.setenv("KS_WRITE_EXPENSES", "duckdb")
        shipped = await replicate_operational(store)
        assert "error" not in shipped, shipped
        assert shipped["replaced"][EXPENSES] == 3
        assert EXPENSES not in shipped.get("stood_down", [])
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT last_rows FROM meta.mirror_state WHERE table_name = $1",
                EXPENSES)
        assert state["last_rows"] == 3

        # And the next DuckDB expense takes the id after the last Postgres one,
        # rather than reissuing one Postgres already handed out.
        following = await store.add_expense(
            date(2026, 9, 18), "marketing", "Google Ads", 100)
        assert following["id"] == max(e["id"] for e in typed) + 1
        assert [r[0] for r in await _duck(
            store, "SELECT id FROM manual_expenses ORDER BY id")] == [
            *[e["id"] for e in typed], following["id"]]

    @pytest.mark.asyncio
    async def test_the_floor_clears_an_id_whose_row_was_deleted(self, stores):
        """The trap the compaction work found: an allocator is not MAX(id).

        A withdrawn expense leaves its id in the Postgres sequence and not in
        the table, so a floor computed from the copied rows alone hands the
        next DuckDB expense an id Postgres has already used — for a different
        row, in the store that still holds the forensic trail.
        """
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        kept = await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 10)
        withdrawn = await store.add_expense(date(2026, 9, 16), "marketing", "TikTok Ads", 20)
        assert await store.delete_expense(withdrawn["id"])

        await copy_back(store, pg_expenses_write, dry_run=False)

        env.setenv("KS_WRITE_EXPENSES", "duckdb")
        following = await store.add_expense(
            date(2026, 9, 17), "logistics", "Nova Poshta", 30)
        assert following["id"] > withdrawn["id"] > kept["id"]

    @pytest.mark.asyncio
    async def test_a_dry_run_writes_nothing_and_releases_nothing(self, stores):
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 4200)

        result = await copy_back(store, pg_expenses_write)

        assert result["executed"] is False
        assert result["rows"] == {EXPENSES: 1}
        assert (await _duck(store, "SELECT COUNT(*) FROM manual_expenses"))[0][0] == 0
        assert chain_latch.latched("pg_expenses_write")
        assert await chain_latch.read_owners(pool) == {EXPENSES: chain_latch.latched_at(
            "pg_expenses_write")}


class TestChain1Inventory:
    """Six tables, an append above a watermark, and a delta base.

    `stock_movements` is a delta against the PREVIOUS contents of
    `offer_stocks`, so restoring the movements and not the base is the failure
    the chain map warns about: the next tick reads a stale base and writes a
    day of drift as one giant movement per offer, labelled wrong.
    """

    @pytest.mark.asyncio
    async def test_the_base_comes_back_so_the_next_sync_sees_no_change(
        self, stores,
    ):
        store, pool, env = stores
        await _before_the_flip(store)
        await _after_the_flip(store, env)

        before = await _duck(store, "SELECT COUNT(*) FROM stock_movements")
        result = await copy_back(store, pg_inventory_write, dry_run=False)
        assert result["findings"] == [], result["findings"]
        assert result["released"] is True

        # The movement Postgres wrote is above what DuckDB held, and arrived.
        after = await _duck(
            store, "SELECT id, movement_type, delta FROM stock_movements ORDER BY id")
        assert len(after) == before[0][0] + 1
        assert after[-1][1:] == ("stock_out", -3)

        # The claim this whole ordering exists for: an unchanged sync now
        # writes nothing, because the base it compares against is the one
        # Postgres was working from.
        env.delenv("KS_WRITE_INVENTORY")
        assert await store.upsert_stocks([dict(STOCKS[0], quantity=37)]) == 1
        assert await _duck(
            store, "SELECT id, movement_type, delta FROM stock_movements ORDER BY id"
        ) == after, "the resumed sync recomputed a delta against a stale base"

    @pytest.mark.asyncio
    async def test_the_snapshots_the_first_seen_date_and_the_watermark(self, stores):
        store, pool, env = stores
        await _before_the_flip(store)
        seeded = (await _duck(
            store, "SELECT offer_id, first_seen_at FROM sku_inventory_status"))[0]
        await _after_the_flip(store, env)

        result = await copy_back(store, pg_inventory_write, dry_run=False)
        assert result["findings"] == [], result["findings"]

        days = await _duck(store, "SELECT date FROM inventory_sku_history ORDER BY date")
        assert days, "the per-SKU snapshot days did not come back"
        assert await _duck(store, "SELECT date FROM inventory_history ORDER BY date")

        # `first_seen_at` exists nowhere else — it is carried forward out of
        # the table's own previous contents on every rebuild, so a copy-back
        # that lost it would reset every SKU's age in both stores at once.
        assert (await _duck(
            store, "SELECT offer_id, first_seen_at FROM sku_inventory_status"
        ))[0] == seeded

        # And the watermark is back where DuckDB's sync reads it, with the
        # value Postgres held — revision 0032's pair, in reverse. Read after
        # the variable goes back, which is the runbook's own first step: until
        # it does, `get_last_sync_time` still routes on the flag.
        env.delenv("KS_WRITE_INVENTORY")
        assert await store.get_last_sync_time("stocks") == datetime(
            2026, 9, 17, 12, tzinfo=timezone.utc)
        assert await _pg(
            pool, "SELECT key FROM meta.chain_watermarks") == [], \
            "a stale watermark left in Postgres is read again by the next flip"

    @pytest.mark.asyncio
    async def test_the_duckdb_allocator_clears_every_postgres_movement(self, stores):
        store, pool, env = stores
        await _before_the_flip(store)
        await _after_the_flip(store, env)
        highest = (await _pg(pool, f"SELECT MAX(id) AS m FROM {MOVEMENTS}"))[0]["m"]

        await copy_back(store, pg_inventory_write, dry_run=False)

        env.delenv("KS_WRITE_INVENTORY")
        await store.upsert_stocks([dict(STOCKS[0], quantity=1)])
        fresh = await _duck(
            store, "SELECT id FROM stock_movements ORDER BY id DESC LIMIT 1")
        assert fresh[0][0] > highest


class TestRefusals:
    @pytest.mark.asyncio
    async def test_an_unlatched_chain_is_refused_before_anything_is_read(
        self, stores,
    ):
        store, _pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")      # the flag is not the latch

        with pytest.raises(CopyBackRefused, match="not latched"):
            await copy_back(store, pg_expenses_write, dry_run=False)

    @pytest.mark.asyncio
    async def test_after_a_release_the_flag_still_decides_and_re_latches(
        self, stores,
    ):
        """The window between the release and the operator's `.env` edit.

        It is the safe direction and it is deliberate: the next write goes to
        Postgres and takes the latch again, so nothing is written to a store
        that has stopped being the writer. What must not happen is a silent
        second writer, and that is what the re-latch prevents.
        """
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        first = await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 10)
        assert (await copy_back(store, pg_expenses_write, dry_run=False))["released"]

        again = await store.add_expense(date(2026, 9, 16), "marketing", "TikTok Ads", 20)

        assert chain_latch.latched("pg_expenses_write")
        assert await chain_latch.read_owners(pool) == {
            EXPENSES: chain_latch.latched_at("pg_expenses_write")}
        assert [r["id"] for r in await _pg(
            pool, f"SELECT id FROM {EXPENSES} ORDER BY id")] == [
            first["id"], again["id"]]

    @pytest.mark.asyncio
    async def test_the_owner_rows_alone_are_enough_to_run_it(self, stores):
        """The marker lives on a bind mount and the owner rows do not.

        An older `./data` snapshot or a rebuilt data directory loses the
        marker while Postgres keeps both the rows and the ownership — the
        state the shipper stamps and the comparison files as
        `chain_latch_disagrees`, whose own text sends the reader to this
        script. Refusing on the marker alone would leave the tables stood down
        with nothing able to release them.
        """
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = await store.add_expense(
            date(2026, 9, 15), "marketing", "Facebook Ads", 4200)
        chain_latch.release("pg_expenses_write")           # the volume was lost
        assert await chain_latch.read_owners(pool)

        result = await copy_back(store, pg_expenses_write, dry_run=False)

        assert result["latched_at"] is None and result["owned_since"]
        assert result["findings"] == [] and result["released"] is True
        assert [r[0] for r in await _duck(
            store, "SELECT id FROM manual_expenses")] == [typed["id"]]
        assert await chain_latch.read_owners(pool) == {}

    @pytest.mark.asyncio
    async def test_a_difference_keeps_the_latch(self, stores):
        """The comparison is what releases, not the copy.

        A row changed in Postgres between the read and the check stands in for
        every way a copy can be incomplete. The latch must survive it, because
        releasing here is what starts the second writer — and DuckDB must not
        keep the copy that did not compare.
        """
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = await store.add_expense(
            date(2026, 9, 15), "marketing", "Facebook Ads", 4200)

        real_verify = chain_transfer.verify

        async def _meddle(conn_, pool_, specs, **kwargs):
            async with pool_.acquire() as conn:
                await conn.execute(
                    f"UPDATE {EXPENSES} SET amount = 9999 WHERE id = $1", typed["id"])
            return await real_verify(conn_, pool_, specs, **kwargs)

        with patch.object(chain_transfer, "verify", _meddle):
            result = await copy_back(store, pg_expenses_write, dry_run=False)

        assert result["committed"] is False and result["released"] is False
        assert [f["check"] for f in result["findings"]] == ["mirror_row_values"]
        assert chain_latch.latched("pg_expenses_write")
        assert await chain_latch.read_owners(pool) == {
            EXPENSES: chain_latch.latched_at("pg_expenses_write")}
        assert (await _duck(store, "SELECT COUNT(*) FROM manual_expenses"))[0][0] == 0
        assert "Nothing was committed" in result["runbook"][0]

    @pytest.mark.asyncio
    async def test_a_rollback_is_exit_1_and_its_json_says_nothing_was_committed(
        self, stores, capsys,
    ):
        """`committed` is the field exit 3 was introduced to make readable.

        An operator or a script reading `--json` tells "rolled back, DuckDB as
        it was" from "committed, not released" by it — so it must say false
        here, on a real rollback through the script, not only on the
        exception path that sets it true."""
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        typed = await store.add_expense(
            date(2026, 9, 15), "marketing", "Facebook Ads", 4200)

        real_verify = chain_transfer.verify

        async def _meddle(conn_, pool_, specs, **kwargs):
            async with pool_.acquire() as conn:
                await conn.execute(
                    f"UPDATE {EXPENSES} SET amount = 9999 WHERE id = $1", typed["id"])
            return await real_verify(conn_, pool_, specs, **kwargs)

        with patch.object(chain_transfer, "verify", _meddle):
            code = await _script(store, env, "expenses", "--execute", "--json")

        printed = json.loads(capsys.readouterr().out)
        assert code == 1
        assert printed["executed"] is True
        assert printed["committed"] is False and printed["released"] is False
        assert [f["check"] for f in printed["findings"]] == ["mirror_row_values"]
        assert (await _duck(store, "SELECT COUNT(*) FROM manual_expenses"))[0][0] == 0
        assert await _latch_kept(pool, pg_expenses_write)

    @pytest.mark.asyncio
    async def test_a_marker_without_owner_rows_is_a_rewind_and_is_refused(
        self, stores,
    ):
        """`core/chain_latch.py` names this state: a first write that failed
        after the marker. Postgres received nothing, so copying it back is a
        rewind — the review's probe emptied four tables that way. The refusal
        carries the lever, and touches neither DuckDB nor the marker."""
        store, pool, env = stores
        # A typed expense DuckDB holds and Postgres never received: the
        # shipment that would have carried it is exactly what the marker
        # stood down.
        await _duck(
            store, "INSERT INTO manual_expenses (expense_date, category, "
                   "expense_type, amount) VALUES (DATE '2026-09-15', 'marketing', "
                   "'Facebook Ads', 4200)")
        chain_latch.latch("pg_expenses_write", "KS_WRITE_EXPENSES")
        assert await chain_latch.read_owners(pool) == {}
        before = await _snapshot(store)

        for dry_run in (True, False):
            with pytest.raises(CopyBackRefused, match="no owner rows") as refused:
                await copy_back(store, pg_expenses_write, dry_run=dry_run)
            said = str(refused.value)
            assert "rewind" in said and "Nothing was written" in said
            assert str(chain_latch.marker_path("pg_expenses_write")) in said
            # `--handover` before the marker is touched, and before keeping
            # postgres is even offered: in this very state it exits 1 on the
            # expense above, which the offered "leave it at postgres" would
            # have stranded under the next write's latch.
            assert said.index("--handover") < said.index("delete the marker")
            assert said.index("--handover") < said.index("Leave it at postgres")
        assert {i.check_name for i in await handover_check(
            store, pg_expenses_write)} == {"handover_rows_missing"}

        assert await _snapshot(store) == before
        assert chain_latch.latched("pg_expenses_write")


class TestTheHandoverIsAPrecondition:
    """What the first draft's DELETE destroyed before its comparison looked.

    Each case is a state the review reproduced or traced, produced here by the
    real repository methods, and each must be refused before anything is
    written: the dry run and `--execute` alike, DuckDB byte for byte as it was,
    both copies of the latch still standing.
    """

    async def _refused(self, store, pool, chain):
        before = await _snapshot(store)
        found = []
        for dry_run in (True, False):
            with pytest.raises(CopyBackRefused) as refused:
                await copy_back(store, chain, dry_run=dry_run)
            found.append({(i.check_name, i.table_name, i.severity.value, i.sample_ids)
                          for i in refused.value.issues})
            assert "nothing was written" in str(refused.value)
        assert found[0] == found[1], "the dry run and --execute must agree"
        assert await _snapshot(store) == before
        assert await _latch_kept(pool, chain)
        # And `--handover` names the same rows, so the pre-flight is the preview.
        preview = {(i.check_name, i.table_name, i.severity.value, i.sample_ids)
                   for i in await handover_check(store, chain)
                   if i.severity.value == "CRITICAL"}
        assert preview == found[0]
        return found[0]

    @pytest.mark.asyncio
    async def test_an_offer_only_duckdb_holds_is_not_deleted_by_the_copy(
        self, stores,
    ):
        """The review's reproduction. `sync_offers` catalogued offer 2 after the
        last hourly shipment — a stock fetch that errors leaves exactly this,
        because the two syncs keep separate watermarks — and the flip followed.
        The first draft's copy-back returned no findings and released the
        latch with DuckDB's offers gone from two to one."""
        store, pool, env = stores
        await _before_the_flip(store)
        await store.upsert_offers([{"id": 2, "product_id": 102, "sku": "S-2"}])
        await _after_the_flip(store, env)

        refused = await self._refused(store, pool, pg_inventory_write)

        assert refused == {("handover_rows_missing", "bronze.offers", "CRITICAL", (2,))}
        assert [r[0] for r in await _duck(store, "SELECT id FROM offers ORDER BY id")] == [1, 2]

    @pytest.mark.asyncio
    async def test_two_movements_under_one_id_are_refused(self, stores):
        """The collision behind the review's permanent finding. DuckDB wrote
        movement 2 after the last shipment; after the flip Postgres floored its
        sequence on its own MAX(id) = 1 and wrote a different movement 2. An
        append table has no newer, so this is two events under one id."""
        store, pool, env = stores
        await _before_the_flip(store)
        await store.upsert_stocks([dict(STOCKS[0], quantity=37)])   # DuckDB id 2
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        await store.upsert_stocks([dict(STOCKS[0], quantity=30)])   # Postgres id 2
        assert [tuple(r) for r in await _pg(
            pool, f"SELECT id, delta FROM {MOVEMENTS} ORDER BY id")][-1] == (2, -10)
        assert (await _duck(store, "SELECT id, delta FROM stock_movements "
                                   "ORDER BY id"))[-1] == (2, -3)

        refused = await self._refused(store, pool, pg_inventory_write)

        assert ("handover_rows_differ", MOVEMENTS, "CRITICAL", (2,)) in refused

    @pytest.mark.asyncio
    async def test_a_duckdb_edit_later_than_postgres_is_not_overwritten(
        self, stores,
    ):
        """"Equal or newer", the half with a clock: an edit whose own shipment
        failed, then the flip. Postgres holds the older version, and the row's
        clock — carried by both stores — says so."""
        store, pool, env = stores
        typed = await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 100)
        assert len(await _pg(pool, f"SELECT id FROM {EXPENSES}")) == 1
        await _duck(
            store, "UPDATE manual_expenses SET amount = 250, updated_at = ? "
                   "WHERE id = ?",
            [datetime.now(timezone.utc) + timedelta(hours=1), typed["id"]])
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 16), "marketing", "TikTok Ads", 20)

        refused = await self._refused(store, pool, pg_expenses_write)

        assert refused == {("handover_rows_newer_in_duckdb", EXPENSES, "CRITICAL",
                            (typed["id"],))}

    @pytest.mark.asyncio
    async def test_a_postgres_edit_later_than_duckdb_is_carried(self, stores):
        """The other half: Postgres is the writer after the latch, so its later
        version is the work of the copy and not a reason to refuse."""
        store, pool, env = stores
        typed = await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 100)
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.update_expense(typed["id"], amount=175)

        result = await copy_back(store, pg_expenses_write, dry_run=False)

        assert result["released"] is True, result["findings"]
        assert [(h["check"], h["severity"]) for h in result["handover"]] == [
            ("handover_rows_differ", "INFO")]
        assert [float(r[0]) for r in await _duck(
            store, "SELECT amount FROM manual_expenses")] == [175.0]


class TestTheCopyIsOneTransaction:
    @pytest.mark.asyncio
    async def test_a_difference_found_inside_it_rolls_every_table_back(self, stores):
        """Chain 1 has four full-replace tables that the copy DELETEs before
        the comparison runs. The first draft committed them first, so a copy
        that did not compare left DuckDB half-replaced under a latch that told
        the operator nothing had changed. Now the comparison runs inside the
        transaction and a finding is a ROLLBACK."""
        store, pool, env = stores
        await _before_the_flip(store)
        await _after_the_flip(store, env)
        before = await _snapshot(store)
        async with store.connection() as conn:
            allocator = next_value(conn, "seq_stock_movements_id")

        real_verify = chain_transfer.verify

        async def _meddle(conn_, pool_, specs, **kwargs):
            async with pool_.acquire() as conn:
                await conn.execute(
                    "UPDATE bronze.offer_stocks SET reserve = reserve + 5")
            return await real_verify(conn_, pool_, specs, **kwargs)

        with patch.object(chain_transfer, "verify", _meddle):
            result = await copy_back(store, pg_inventory_write, dry_run=False)

        assert result["released"] is False
        assert {f["table"] for f in result["findings"]} >= {"bronze.offer_stocks"}
        assert await _snapshot(store) == before
        assert await _latch_kept(pool, pg_inventory_write)
        # The one thing a ROLLBACK does not undo, measured: a burn stays burned
        # in the process. At or above where it was — a gap, never a reissue.
        async with store.connection() as conn:
            assert next_value(conn, "seq_stock_movements_id") >= allocator


    @pytest.mark.asyncio
    async def test_a_copy_whose_recorded_at_differs_keeps_the_latch(self, stores):
        """The column the daily spec uses as its clock and never compares.

        The copy writes `stock_movements.recorded_at` and the SKU rebuild
        reads it, so a copy that shifts it dates every carried movement to the
        wrong hour — and near midnight to the wrong day. The review shifted it
        by an hour inside the write and the latch was released on a clean
        comparison. Now it is a compared value like any other."""
        store, pool, env = stores
        await _before_the_flip(store)
        await _after_the_flip(store, env)
        before = await _snapshot(store)

        real_write = chain_transfer._write_duckdb

        def _shifted(conn, spec, rows):
            if spec.pg_table == MOVEMENTS:
                at = spec.columns.index("recorded_at")
                rows = [row[:at] + (row[at] + timedelta(hours=1),) + row[at + 1:]
                        for row in rows]
            return real_write(conn, spec, rows)

        with patch.object(chain_transfer, "_write_duckdb", _shifted):
            result = await copy_back(store, pg_inventory_write, dry_run=False)

        assert result["released"] is False
        assert [(f["check"], f["table"]) for f in result["findings"]] == [
            ("mirror_row_values", MOVEMENTS)]
        assert await _snapshot(store) == before
        assert await _latch_kept(pool, pg_inventory_write)


class TestAfterTheCommit:
    """A failure after the COMMIT says the copy landed, and what to run next.

    Exit 1 means "rolled back, DuckDB as it was". Until this class existed a
    checkpoint or a release that raised after the COMMIT left through the
    interpreter's own exit 1 with the copy in DuckDB — reproduced by the
    review with `release_chain` raising. It is `CommittedNotReleased` now,
    exit 3, and its message reads the latch rather than inferring it.
    """

    @staticmethod
    async def _latched_expense(store, env):
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        return await store.add_expense(
            date(2026, 9, 15), "marketing", "Facebook Ads", 4200)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stage", ["checkpoint", "release"])
    async def test_the_latch_holds_and_execute_again_releases(self, stores, stage):
        store, pool, env = stores
        typed = await self._latched_expense(store, env)
        if stage == "checkpoint":
            failing = patch.object(store, "checkpoint", new=AsyncMock(
                side_effect=OSError(28, "No space left on device")))
        else:
            failing = patch.object(chain_transfer, "release_chain", new=AsyncMock(
                side_effect=ConnectionError("connection was closed")))

        with failing, pytest.raises(chain_transfer.CommittedNotReleased) as raised:
            await copy_back(store, pg_expenses_write, dry_run=False)

        said, plan = str(raised.value), raised.value.plan
        # DuckDB HAS the copy — the one thing exit 1 must never be read as.
        assert [r[0] for r in await _duck(
            store, "SELECT id FROM manual_expenses")] == [typed["id"]]
        assert "COMMITTED to DuckDB" in said and f"the {stage} after it failed" in said
        assert plan["committed"] is True and plan["released"] is False
        assert "app.manual_expenses" in said
        # Both copies survive, and the message reads them rather than guessing.
        assert await _latch_kept(pool, pg_expenses_write)
        assert raised.value.latch["marker"] and raised.value.latch["owned_since"]
        assert "--execute again" in said

        # And the step it names is the one that works: the handover finds
        # DuckDB already equal, the copy commits again and releases.
        result = await copy_back(store, pg_expenses_write, dry_run=False)
        assert result["released"] is True, result["findings"]
        assert not chain_latch.latched("pg_expenses_write")
        assert await chain_latch.read_owners(pool) == {}

    @pytest.mark.asyncio
    async def test_a_release_that_dies_between_its_deletes_sends_the_operator_to_handover(
        self, stores,
    ):
        """Postgres first, then the marker: a failure between the two leaves
        the marker with no owner rows, which `--execute` refuses as a rewind.
        So the message prints that refusal's own steps, `--handover` first —
        and in this state the handover is clean, because DuckDB was just made
        equal to Postgres."""
        store, pool, env = stores
        await self._latched_expense(store, env)

        with patch.object(chain_latch, "release",
                          side_effect=PermissionError(13, "Permission denied")), \
             pytest.raises(chain_transfer.CommittedNotReleased) as raised:
            await copy_back(store, pg_expenses_write, dry_run=False)

        latch, said = raised.value.latch, str(raised.value)
        assert latch["marker"] and latch["owned_since"] is None
        assert "between its two deletes" in said
        assert said.index("--handover") < said.index("delete the marker")

        with pytest.raises(CopyBackRefused, match="no owner rows"):
            await copy_back(store, pg_expenses_write, dry_run=False)
        assert await handover_check(store, pg_expenses_write) == []

    @pytest.mark.asyncio
    async def test_postgres_gone_after_the_comparison_is_unreadable_and_waits_for_it(
        self, stores, capsys,
    ):
        """A failure after the COMMIT driven for real rather than mocked.

        Postgres goes away between the comparison and the release: the host
        drops it, the network does, the container restarts. `release_chain`
        meets a dead connection, and `_latch_state` then has to ask the very
        Postgres that failed whether the owner rows survived — so it cannot
        know, and the message must say UNREADABLE rather than guess "held"
        (and send nobody to look) or "gone" (and print the released runbook
        over a latch that still stands). The step it names has to be the one
        that works once Postgres answers: `--execute` again, exit 0.

        Driven through the script, because exit 3 and the sentence on stderr
        are what an operator in the stopped window actually gets.
        """
        store, pool, env = stores
        typed = await self._latched_expense(store, env)
        real_verify = chain_transfer.verify

        async with _Hop(DSN) as hop:
            hopped = await asyncpg.create_pool(hop.dsn, min_size=1, max_size=3)

            async def _then_postgres_goes(conn_, pool_, specs, **kwargs):
                issues = await real_verify(conn_, pool_, specs, **kwargs)
                await hop.cut()
                return issues

            try:
                with patch("core.pg.get_pool", new=AsyncMock(return_value=hopped)), \
                     patch.object(chain_transfer, "verify", _then_postgres_goes):
                    code = await _script(store, env, "expenses", "--execute", "--json")
            finally:
                hopped.terminate()

        out, said = capsys.readouterr()
        printed = json.loads(out)
        assert code == 3
        assert printed["committed"] is True and printed["released"] is False
        assert printed["latch"]["owners_error"] and printed["latch"]["marker"]
        assert printed["latch"]["owned_since"] is None      # unknown, not "gone"
        assert "COMMITTED, NOT RELEASED" in said and "the release after it failed" in said
        assert "owner rows UNREADABLE (" in said
        assert "once Postgres answers: run this script with --execute again" in said
        # The marker survived, so the one alternative it offers is the safe one.
        assert "Bringing web back up instead is safe" in said
        # Neither of the two guesses an unreadable answer could be mistaken for.
        assert "held since" not in said and "Both copies of the latch are gone" not in said
        assert "between its two deletes" not in said
        # DuckDB has the copy, and the latch really is still standing — the
        # release never reached Postgres.
        assert [r[0] for r in await _duck(
            store, "SELECT id FROM manual_expenses")] == [typed["id"]]
        assert await _latch_kept(pool, pg_expenses_write)

        # Postgres answers again: the step the message named releases.
        assert await _script(store, env, "expenses", "--execute", "--json") == 0
        assert json.loads(capsys.readouterr().out)["released"] is True
        assert not chain_latch.latched("pg_expenses_write")
        assert await chain_latch.read_owners(pool) == {}

    @pytest.mark.asyncio
    async def test_the_marker_is_read_from_the_disk_not_the_process_cache(
        self, stores,
    ):
        """An unlink whose directory fsync raised: the file is gone, the cache
        entry is not, because `chain_latch.release` pops the cache only after
        the fsync. Postgres's DELETE had already committed, so both copies of
        the latch are gone and the chain is released.

        Read from the cache, the marker would still be there and the message
        would print the between-its-two-deletes steps — `--handover`, then
        "delete the marker" — for a file that no longer exists, and never the
        one check this state needs: that the unlink reached the disk.
        """
        store, pool, env = stores
        await self._latched_expense(store, env)
        name = "pg_expenses_write"

        with patch.object(chain_latch, "_fsync_dir",
                          side_effect=OSError(5, "Input/output error")), \
             pytest.raises(chain_transfer.CommittedNotReleased) as raised:
            await copy_back(store, pg_expenses_write, dry_run=False)

        latch, said = raised.value.latch, str(raised.value)
        assert not chain_latch.marker_path(name).exists()
        assert latch["marker"] is None and latch["owned_since"] is None
        assert latch["owners_error"] is None
        assert "Both copies of the latch are gone" in said
        assert f"data/write-chain-owners/{name} is still absent" in said
        assert "between its two deletes" not in said
        # Reading the disk also corrects the process: nothing here goes on
        # answering "latched" off an entry whose file is gone.
        assert not chain_latch.latched(name)
        assert await chain_latch.read_owners(pool) == {}


class TestScale:
    @pytest.mark.asyncio
    async def test_a_production_sized_chain_1_comes_back_in_bounded_time_and_memory(
        self, stores,
    ):
        """Chain 1 at production's size: 162,883 `inventory_sku_history` rows
        and 56,277 `stock_movements` on 2026-09-18. Everything goes through the
        real path — the handover reads both sides whole, the copy writes in
        chunks, the comparison reads both sides whole again — and must land
        in bounded time and Python memory. Before the chunking the history
        alone took about eight minutes in `executemany`.

        The bounds are loose on purpose: they are there to fail on a return
        to one row per statement, or on a copy that holds the whole chain in
        memory at once, not to benchmark a laptop.
        """
        from core.pg_operational import MOVEMENT_COLUMNS, SKU_HISTORY_COLUMNS

        store, pool, env = stores
        days, offers = 184, 890                        # 163,760 rows
        first = date(2026, 1, 27)
        history = [(first + timedelta(days=d), o, o % 40, o % 3, Decimal("123.45"))
                   for d in range(days) for o in range(1, offers + 1)]
        stamp = datetime(2026, 9, 1, tzinfo=timezone.utc)
        movements = [(i, i % offers + 1, 100 + i % offers, "stock_out", 10, 9, -1,
                      0, 0, stamp + timedelta(minutes=i), "sync")
                     for i in range(1, 56_278)]
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "inventory_sku_history", schema_name="app",
                records=history, columns=list(SKU_HISTORY_COLUMNS))
            await conn.copy_records_to_table(
                "stock_movements", schema_name="app",
                records=movements, columns=list(MOVEMENT_COLUMNS))
            await conn.execute(
                "SELECT setval('app.stock_movements_id_seq', $1, true)",
                len(movements))
        # Latched the way a first Postgres write latches it: the marker, then
        # the owner rows inside a transaction.
        latched = chain_latch.latch("pg_inventory_write", "KS_WRITE_INVENTORY")
        async with pool.acquire() as conn:
            async with conn.transaction():
                await chain_latch.claim(conn, pg_inventory_write.CHAIN_TABLES, latched)

        tracemalloc.start()
        started = time.monotonic()
        try:
            result = await copy_back(store, pg_inventory_write, dry_run=False)
            elapsed = time.monotonic() - started
            _now, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert result["released"] is True, result["findings"]
        assert result["rows"]["app.inventory_sku_history"] == len(history)
        assert result["rows"][MOVEMENTS] == len(movements)
        counts = await _duck(
            store, "SELECT (SELECT COUNT(*) FROM inventory_sku_history), "
                   "(SELECT COUNT(*) FROM stock_movements)")
        assert counts == [(len(history), len(movements))]
        assert elapsed < 120, f"{elapsed:.1f}s"
        assert peak < 600 * 2**20, f"{peak / 2**20:.0f} MiB"
        print(f"\n  chain 1 at production size: {elapsed:.1f}s, "
              f"peak {peak / 2**20:.0f} MiB of Python memory")


class TestWebIsStopped:
    """The precondition that is a lock, not a container check."""

    def test_a_read_only_connection_is_refused(self, tmp_path):
        """The weaker restatement inside `copy_back`. The strong form is that
        `duckdb.connect` raised while web held the file — which is what the
        script turns into a sentence, below."""
        import duckdb

        path = tmp_path / "read-only.duckdb"
        duckdb.connect(str(path)).close()
        reader = duckdb.connect(str(path), read_only=True)
        try:
            with pytest.raises(CopyBackRefused, match="read-only"):
                chain_transfer._refuse_if_read_only(reader)
        finally:
            reader.close()

    def test_the_pre_flight_exits_1_on_a_stranded_row(self, capsys):
        """`--handover` is the read-only half, and its exit code is what a
        person checks before flipping a chain: CRITICAL means a row would be
        stranded, and nothing after the flip can carry it across."""
        from core.data_quality import IntegrityIssue, Severity
        from scripts import chain_copy_back as script

        stranded = [IntegrityIssue(
            check_name="handover_rows_missing", table_name="bronze.offers",
            severity=Severity.CRITICAL, count=2, sample_ids=(1, 2),
            description="two offers never reached Postgres")]
        with patch("core.duckdb_store.DuckDBStore.connect", new=AsyncMock()), \
             patch("core.duckdb_store.DuckDBStore.close", new=AsyncMock()), \
             patch("core.chain_transfer.handover_check",
                   new=AsyncMock(return_value=stranded)):
            assert script.main(["inventory", "--handover"]) == 1
        assert "handover_rows_missing" in capsys.readouterr().out

        with patch("core.duckdb_store.DuckDBStore.connect", new=AsyncMock()), \
             patch("core.duckdb_store.DuckDBStore.close", new=AsyncMock()), \
             patch("core.chain_transfer.handover_check",
                   new=AsyncMock(return_value=[])):
            assert script.main(["inventory", "--handover"]) == 0

    def test_the_script_names_the_containers_a_lock_error_does_not(self, capsys):
        """The driver says "Could not set lock on file ... held by PID n",
        which is true and does not say `docker compose stop web bot`."""
        import duckdb

        from scripts import chain_copy_back as script

        with patch("core.duckdb_store.DuckDBStore.connect",
                   new=AsyncMock(side_effect=duckdb.IOException("held by PID 1"))):
            code = script.main(["expenses"])

        assert code == 2
        said = capsys.readouterr().err
        assert "docker compose stop web bot" in said
        assert "nothing was written" in said


class TestHandoverCheck:
    @pytest.mark.asyncio
    async def test_a_duckdb_only_row_fails_it(self, stores):
        """The pre-flight. A row the shipper never carried is stranded by the
        flip: the shipper stands down on the latch and these tables have no
        backfill, so nothing after the flip can bring it across."""
        store, pool, env = stores
        await store.upsert_offers(OFFERS)
        await store.upsert_stocks(STOCKS)

        issues = await handover_check(store, pg_inventory_write)

        names = {i.check_name for i in issues}
        assert "handover_rows_missing" in names
        missing = [i for i in issues if i.check_name == "handover_rows_missing"]
        assert {"bronze.offers", "bronze.offer_stocks"} <= {i.table_name for i in missing}
        assert all(i.severity.value == "CRITICAL" for i in missing)

    @pytest.mark.asyncio
    async def test_a_shipped_chain_passes_it(self, stores):
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        await store.upsert_offers(OFFERS)
        await store.upsert_stocks(STOCKS)
        await store.refresh_sku_inventory_status()
        assert "error" not in await replicate_operational(store)

        assert await handover_check(store, pg_inventory_write) == []

    @pytest.mark.asyncio
    async def test_once_latched_a_difference_is_the_size_of_the_copy_back(
        self, stores,
    ):
        """The same difference means the opposite thing on either side of the
        latch, which is why the severity is read from the latch and not from
        the difference."""
        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 4200)

        issues = await handover_check(store, pg_expenses_write)

        (ahead,) = [i for i in issues if i.check_name == "handover_rows_ahead"]
        assert ahead.severity.value == "INFO" and ahead.count == 1
        assert [i for i in issues if i.severity.value == "CRITICAL"] == []

    @pytest.mark.asyncio
    async def test_a_marker_alone_does_not_mean_postgres_moved_on(self, stores):
        """The owner rows decide which half of the rule applies, not the
        marker. A marker with no owner row is a first write that failed:
        Postgres received nothing, so a difference is DuckDB's own write the
        shipper never carried — refused, not waved through as Postgres being
        newer."""
        store, pool, env = stores
        typed = await store.add_expense(date(2026, 9, 15), "marketing", "Facebook Ads", 100)
        await _duck(store, "UPDATE manual_expenses SET amount = 250 WHERE id = ?",
                    [typed["id"]])
        chain_latch.latch("pg_expenses_write", "KS_WRITE_EXPENSES")

        issues = await handover_check(store, pg_expenses_write)

        assert {(i.check_name, i.severity.value) for i in issues} == {
            ("handover_rows_differ", "CRITICAL")}


class TestSpecsAreDerived:
    def test_every_chain_table_has_a_shipping_shape_and_a_comparison(self):
        """The guard that makes the derivation a derivation: a chain that
        gains a table and gives it neither must fail loudly here, not be
        copied back incompletely and released on a clean comparison of
        everything else."""
        from core.write_chains import WRITE_CHAINS

        for chain in WRITE_CHAINS:
            specs = chain_transfer.chain_specs(chain)
            assert {s.pg_table for s in specs} == set(chain.CHAIN_TABLES)

    def test_an_unknown_table_raises_rather_than_being_skipped(self):
        class _Invented:
            CHAIN = "pg_invented_write"
            CHAIN_TABLES = ("app.nothing_ships_this",)

        with pytest.raises(LookupError, match="neither _FULL_REPLACE"):
            chain_transfer.chain_specs(_Invented)

    def test_the_sequences_come_from_the_boot_migration_s_own_list(self):
        from core.migrations import SEQUENCE_ID_COLUMNS

        known = {dk for _s, dk, _c in SEQUENCE_ID_COLUMNS}
        for chain, expected in (
            (pg_expenses_write, {"seq_manual_expenses_id"}),
            (pg_inventory_write, {"seq_stock_movements_id"}),
        ):
            specs = chain_transfer.chain_sequences(chain)
            assert {s.dk_sequence for s in specs} == expected
            assert all(s.dk_table in known for s in specs)
            assert all(s.pg_sequence.startswith(("app.", "bronze.")) for s in specs)
