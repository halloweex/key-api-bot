"""Chain 4: buyers, contacts and verdicts written to a real Postgres.

The buyers go through `DuckDBStore.upsert_buyers`, the one method every buyer
writer calls, so the routing is proved and not the writer called on its own.
The derivation is called directly: its router is the replication rider.

The names are synthetic and so are the phone numbers; the shapes are the
parse's. DuckDB is asked nothing about buyers under the flag — a test that
passed because DuckDB was written in Postgres's place would be the defect.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_buyers_write as chain, write_chains

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

READERS = ("KS_SMS_STORE", "KS_READ_SEARCH_INDEX", "KS_READ_DASHBOARD")
TABLES = ("bronze.buyer_contacts", "app.buyer_gender", "bronze.buyers")


def _buyer(buyer_id: int, name: str = "Олена Петренко", phones=None, **extra):
    from core.models import Buyer

    payload = {"id": buyer_id, "full_name": name,
               "phone": phones if phones is not None else [f"+38050{buyer_id:07d}"]}
    payload.update(extra)
    return Buyer.from_api(payload)


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in TABLES:
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB, a live Postgres, the flag on and every reader of the
    buyers on postgres — the precondition without which the flag moves
    nothing."""
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    for reader in READERS:
        monkeypatch.setenv(reader, "postgres")
    monkeypatch.setenv(chain.WRITE_ENV, "postgres")
    store = DuckDBStore(db_path=tmp_path / "buyers-chain.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _verdicts(pool):
    async with pool.acquire() as conn:
        return {r["buyer_id"]: dict(r) for r in await conn.fetch(
            "SELECT buyer_id, gender, method, rules_version, override_by_human, "
            "decided_at FROM app.buyer_gender ORDER BY buyer_id")}


async def _duckdb_buyers(store) -> int:
    async with store.connection() as conn:
        return conn.execute("SELECT COUNT(*) FROM buyers").fetchone()[0]


class TestABuyerLandsWithItsContactsAndItsVerdict:
    @pytest.mark.asyncio
    async def test_one_transaction_three_tables_and_duckdb_untouched(self, stores):
        store, pool, _env = stores
        written = await store.upsert_buyers([_buyer(1), _buyer(2, "Іван Коваль")])
        assert written == 2
        async with pool.acquire() as conn:
            buyers = await conn.fetch(
                "SELECT id, full_name, xmin::text AS x FROM bronze.buyers ORDER BY id")
            contacts = await conn.fetch(
                "SELECT buyer_id, value FROM bronze.buyer_contacts ORDER BY buyer_id")
        assert [(r["id"], r["full_name"]) for r in buyers] == [
            (1, "Олена Петренко"), (2, "Іван Коваль")]
        assert [r["buyer_id"] for r in contacts] == [1, 2]
        verdicts = await _verdicts(pool)
        assert verdicts[1]["gender"] == "f" and verdicts[2]["gender"] == "m"
        assert await _duckdb_buyers(store) == 0, "DuckDB was written under the flag"
        assert chain_latch.latched(chain.CHAIN)

    @pytest.mark.asyncio
    async def test_decided_at_is_the_writers_utc_clock_not_postgres_now(self, stores):
        store, pool, _env = stores
        stamp = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
        with patch.object(chain, "_now_utc", return_value=stamp):
            await store.upsert_buyers([_buyer(1)])
        assert (await _verdicts(pool))[1]["decided_at"] == stamp

    @pytest.mark.asyncio
    async def test_an_equal_verdict_is_not_rewritten(self, stores):
        """`decided_at` is the moment the answer last changed, and the
        copy-back orders two versions of a verdict by it."""
        store, pool, _env = stores
        first = datetime(2026, 9, 1, tzinfo=timezone.utc)
        with patch.object(chain, "_now_utc", return_value=first):
            await store.upsert_buyers([_buyer(1)])
        with patch.object(chain, "_now_utc", return_value=first + timedelta(days=1)):
            await store.upsert_buyers([_buyer(1)])
        assert (await _verdicts(pool))[1]["decided_at"] == first

    @pytest.mark.asyncio
    async def test_a_rename_is_decided_again(self, stores):
        store, pool, _env = stores
        await store.upsert_buyers([_buyer(1, "Олена Петренко")])
        await store.upsert_buyers([_buyer(1, "Іван Петренко")])
        assert (await _verdicts(pool))[1]["gender"] == "m"

    @pytest.mark.asyncio
    async def test_a_humans_decision_is_never_overwritten(self, stores):
        store, pool, _env = stores
        await store.upsert_buyers([_buyer(1, "Олена Петренко")])
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.buyer_gender SET gender = 'm', override_by_human = TRUE "
                "WHERE buyer_id = 1")
        await store.upsert_buyers([_buyer(1, "Олена Коваль")])
        verdict = (await _verdicts(pool))[1]
        assert verdict["gender"] == "m" and verdict["override_by_human"] is True
        assert (await chain.derive_gender_pg(rebuild_all=True))["error"] is None
        assert (await _verdicts(pool))[1]["gender"] == "m"

    @pytest.mark.asyncio
    async def test_a_buyer_named_twice_in_a_batch_is_the_last_one(self, stores):
        """Twice in one statement, Postgres refuses the verdicts' ON CONFLICT
        DO UPDATE outright; DuckDB's INSERT OR REPLACE keeps the last."""
        store, pool, _env = stores
        await store.upsert_buyers([_buyer(1, "Олена Петренко"),
                                   _buyer(1, "Іван Петренко")])
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT full_name FROM bronze.buyers WHERE id = 1") == "Іван Петренко"
        assert (await _verdicts(pool))[1]["gender"] == "m"


class TestAVerdictLandsOnlyBesideItsName:
    @pytest.mark.asyncio
    async def test_the_statement_drops_a_verdict_for_a_name_nobody_holds(self, stores):
        from core.gender import classify

        store, pool, _env = stores
        await store.upsert_buyers([_buyer(1, "Іван Петренко")])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.buyer_gender")
            written = await chain._write_gender_rows(
                conn, [(1, "Олена Петренко", classify("Олена Петренко"))],
                datetime.now(timezone.utc))
        assert written == 0 and await _verdicts(pool) == {}

    @pytest.mark.asyncio
    async def test_a_rename_that_commits_mid_derivation_is_seen(self, stores):
        """The critic's a11, run: the derivation read a buyer's old name, and
        a rename is in flight when it writes. Without the share lock the
        INSERT's snapshot still sees the old name and the old name's verdict
        lands at the current rules version — and nothing ever selects that
        buyer again. With it, the derivation waits for the rename and drops
        the verdict."""
        store, pool, _env = stores
        await store.upsert_buyers([_buyer(1, "Олена Петренко")])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.buyer_gender")

        renamer = await pool.acquire()
        tx = renamer.transaction()
        await tx.start()
        try:
            await renamer.execute(
                "UPDATE bronze.buyers SET full_name = 'Іван Петренко' WHERE id = 1")
            derive = asyncio.ensure_future(chain.derive_gender_pg())
            await asyncio.sleep(0.5)
            assert not derive.done(), "the derivation did not wait for the rename"
            await tx.commit()
        finally:
            await pool.release(renamer)
        out = await asyncio.wait_for(derive, 10)
        assert out["error"] is None and out["pending"] == 1
        assert out["written"] == 0
        assert await _verdicts(pool) == {}, "the old name's verdict landed"

    @pytest.mark.asyncio
    async def test_two_derivations_over_one_set_agree(self, stores):
        store, pool, _env = stores
        await store.upsert_buyers([_buyer(i, "Олена Петренко") for i in range(1, 40)])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.buyer_gender")
        a, b = await asyncio.wait_for(asyncio.gather(
            chain.derive_gender_pg(), chain.derive_gender_pg()), 20)
        assert a["error"] is None and b["error"] is None
        assert a["written"] + b["written"] == 39
        assert len(await _verdicts(pool)) == 39

    @pytest.mark.asyncio
    async def test_nothing_pending_latches_nothing(self, stores):
        store, pool, env = stores
        assert (await chain.derive_gender_pg())["pending"] == 0
        assert not chain_latch.marker_path(chain.CHAIN).exists()


class TestABuyerPostgresRefuses:
    @pytest.mark.asyncio
    async def test_it_is_skipped_by_id_and_the_rest_land(self, stores, caplog):
        """A lone surrogate passes every check the parse makes and is refused
        by the driver as a DataError — the shape a check before the latch
        cannot foresee. The portion is retried buyer by buyer."""
        store, pool, _env = stores
        skipped = []
        poisoned = _buyer(2, "Олена \ud800 Петренко")
        with caplog.at_level(logging.ERROR, logger=chain.__name__):
            written = await store.upsert_buyers(
                [_buyer(1), poisoned, _buyer(3)], skipped_out=skipped)
        assert written == 2 and skipped == [2]
        async with pool.acquire() as conn:
            ids = [r["id"] for r in await conn.fetch(
                "SELECT id FROM bronze.buyers ORDER BY id")]
        assert ids == [1, 3]
        assert set(await _verdicts(pool)) == {1, 3}
        assert "\ud800" not in caplog.text, "a value reached the log"

    @pytest.mark.asyncio
    async def test_a_first_batch_of_only_refused_buyers_leaves_no_marker(self, stores):
        store, _pool, _env = stores
        skipped = []
        huge = _buyer(1, loyalty=[{"discount": 5000}])
        assert await store.upsert_buyers([huge], skipped_out=skipped) == 0
        assert skipped == [1]
        assert not chain_latch.marker_path(chain.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_a_cancelled_statement_is_not_retried(self, stores):
        """The statement timeout firing is not bad data: retrying 500 buyers
        one by one, each against the same timeout, is the tick held for ten
        minutes. It raises at once."""
        store, _pool, _env = stores
        calls = []

        async def cancelled(conn, rows, contacts):
            calls.append(len(rows))
            raise asyncpg.QueryCanceledError("canceling statement due to statement timeout")

        with patch("core.pg_buyer_rows._write_buyer_rows", new=cancelled):
            with pytest.raises(asyncpg.QueryCanceledError):
                await store.upsert_buyers([_buyer(1), _buyer(2)])
        assert calls == [2]


class TestAVerdictThatFailsCostsNoBuyer:
    @pytest.mark.asyncio
    async def test_the_buyers_land_and_the_derivation_decides_them_later(self, stores):
        store, pool, _env = stores

        async def broken(conn, verdicts, decided_at):
            await conn.execute("SELECT 1/0")

        with patch.object(chain, "_write_gender_rows", new=broken):
            assert await store.upsert_buyers([_buyer(1)]) == 1
        assert await _verdicts(pool) == {}
        out = await chain.derive_gender_pg()
        assert out["written"] == 1 and set(await _verdicts(pool)) == {1}
