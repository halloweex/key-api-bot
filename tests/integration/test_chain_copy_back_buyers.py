"""Chain 4's way back, against a real Postgres and a real DuckDB (PR-2, 2.5).

PR-2 registers no chain, so these register a fake one declaring chain 4's
three tables — `bronze.buyers`, `bronze.buyer_contacts`, `app.buyer_gender` —
and latch it the way a chain writer does: the marker on disk, the owner rows
claimed inside the same Postgres transaction that writes the rows. The rows
themselves go through `core.pg_buyer_rows`, the writer the chain will run.

What they prove, in the plan's numbering:

1. a clean pre-flip handover says nothing;
2. after a latch that added a buyer, shrank a contact list 3 → 2 and changed a
   verdict with `override_by_human`, `--execute` copies, compares and releases;
3. DuckDB then equals Postgres, the override and `decided_at` included, the
   contacts' allocator sits above every id, and the watermark moved home;
4. the hourly copy afterwards leaves the override alone — and 5, the control:
   without the copy-back the same release and copy wipe it;
6. the refusals: a NULL name, a buyer only DuckDB holds, a contact of a buyer
   the chain never rewrote, and before a flip a contact only Postgres holds;
7. a whitespace name is written back without complaint;
8. the reship clears what the pre-flip handover refused on.

The copy-back reads WHOLE tables, so the three tables are emptied around each
test — `test_buyer_writes_mirror`'s precedent on this shared database.
"""
from __future__ import annotations

import os
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core.landing_rows import parse_buyers
from core.models import Buyer

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BUYERS = "bronze.buyers"
CONTACTS = "bronze.buyer_contacts"
GENDER = "app.buyer_gender"
TABLES = (BUYERS, CONTACTS, GENDER)
NAME = "pg_buyers_fake_write"
ENV = "KS_WRITE_BUYERS_FAKE"
DECIDED = datetime(2026, 9, 20, 8, 0, tzinfo=timezone.utc)


def _buyer(bid, *, name=None, phones=None):
    return Buyer.from_api({
        "id": bid, "full_name": name or f"Покупець {bid}",
        "created_at": "2026-09-01 10:00:00+00:00",
        "updated_at": "2026-09-02T11:30:00Z",
        "phone": phones if phones is not None else [f"+38050{bid:07d}"],
        "email": [],
    })


def _fake_chain():
    fake = types.ModuleType(f"core.{NAME}")
    fake.CHAIN = NAME
    fake.WRITE_ENV = ENV
    fake.CHAIN_TABLES = TABLES
    fake.CHAIN_SYNC_KEYS = ("last_sync_buyers",)
    fake.env_writes_postgres = lambda: False
    return fake


async def _clean(pool):
    async with pool.acquire() as conn:
        for table in (GENDER, CONTACTS, BUYERS):
            await conn.execute(f"DELETE FROM {table}")
        await conn.execute("DELETE FROM meta.chain_watermarks")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            list(TABLES))


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core import write_chains
    from core.duckdb_store import DuckDBStore

    monkeypatch.setenv("KS_PG_DSN", DSN)
    chain = _fake_chain()
    monkeypatch.setattr(write_chains, "WRITE_CHAINS", write_chains.WRITE_CHAINS + (chain,))
    store = DuckDBStore(db_path=tmp_path / "buyers-back.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, chain, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _verdict_both(store, pool, bid, *, gender="f", override=False):
    """The same verdict in both stores, as the hourly copy leaves them."""
    row = (bid, gender, "dictionary", "high", "given", 1, override, DECIDED)
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO buyer_gender (buyer_id, gender, method, confidence, "
            "decided_from, rules_version, override_by_human, decided_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", list(row))
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO app.buyer_gender (buyer_id, gender, method, confidence, "
            "decided_from, rules_version, override_by_human, decided_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", *row)


async def _mirrored(store, pool, buyers):
    """Buyers into DuckDB and, through the real mirror, into Postgres; each
    with a verdict in both stores."""
    await store.upsert_buyers(buyers)
    for b in buyers:
        await _verdict_both(store, pool, b.id)


async def _latch_and_write(pool, buyers=(), *, sync_value="2026-09-20T09:00:00+00:00"):
    """What a chain writer's first write does: marker, then one transaction
    that claims the owner rows and writes the rows through the one writer."""
    from core import chain_latch
    from core.pg_buyer_rows import _write_buyer_rows

    chain_latch.latch(NAME, ENV)
    parsed = parse_buyers(list(buyers))
    async with pool.acquire() as conn:
        async with conn.transaction():
            await chain_latch.claim(conn, TABLES, datetime.now(timezone.utc).isoformat())
            await _write_buyer_rows(conn, parsed.rows, parsed.contacts)
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ('last_sync_buyers', $1, now()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", sync_value)


def _critical(issues):
    return {(i.check_name, i.table_name) for i in issues if i.severity.value == "CRITICAL"}


async def _duck(store, sql, params=None):
    async with store.connection() as conn:
        return conn.execute(sql, params or []).fetchall()


async def _pg(pool, sql, *params):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *params)


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_a_clean_pre_flip_handover_says_nothing(self, stores):
        from core.chain_transfer import handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1, phones=["+380501", "+380502"]), _buyer(2)])

        assert await handover_check(store, chain) == []

    @pytest.mark.asyncio
    async def test_the_copy_back_carries_the_chains_writes_and_releases(self, stores):
        from core import chain_latch
        from core.chain_transfer import copy_back, handover_check
        from core.duckdb_sequences import next_value

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [
            _buyer(1, phones=["+380501", "+380502", "+380503"]), _buyer(2)])
        # The chain's first write: buyer 1's list shrinks to two, buyer 3 is new.
        await _latch_and_write(pool, [
            _buyer(1, phones=["+380501", "+380503"]), _buyer(3)])
        # A person overrides buyer 1's verdict after the latch.
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE app.buyer_gender SET gender = 'm', override_by_human = TRUE, "
                "decided_at = $2 WHERE buyer_id = $1", 1, DECIDED + timedelta(hours=2))

        preview = await handover_check(store, chain)
        assert _critical(preview) == set(), preview

        plan = await copy_back(store, chain, dry_run=False)

        assert plan["executed"] and plan["committed"] and plan["released"], plan
        assert plan["findings"] == []
        # 3. DuckDB equals Postgres, the override and decided_at included.
        assert sorted(tuple(r) for r in await _duck(
            store, "SELECT id, full_name FROM buyers ORDER BY id")) == [
            (1, "Покупець 1"), (2, "Покупець 2"), (3, "Покупець 3")]
        assert [r[0] for r in await _duck(
            store, "SELECT value FROM buyer_contacts WHERE buyer_id = 1 ORDER BY value")] == [
            "+380501", "+380503"]
        gender = await _duck(store, "SELECT gender, override_by_human, decided_at "
                                    "FROM buyer_gender WHERE buyer_id = 1")
        assert gender[0][0] == "m" and gender[0][1] is True
        # An instant, compared as one: DuckDB hands TIMESTAMPTZ back in the
        # process's zone, so re-labelling it UTC passed on a UTC laptop and
        # failed under Europe/Kyiv on the same, correct value.
        stamp = gender[0][2]
        stamp = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
        assert stamp == DECIDED + timedelta(hours=2)
        async with store.connection() as conn:
            top = conn.execute("SELECT MAX(id) FROM buyer_contacts").fetchone()[0]
            assert next_value(conn, "seq_buyer_contacts_id") > top
        assert await _duck(store, "SELECT value FROM sync_metadata "
                                  "WHERE key = 'last_sync_buyers'") == [
            ("2026-09-20T09:00:00+00:00",)]
        assert await _pg(pool, "SELECT key FROM meta.chain_watermarks") == []
        assert chain_latch.latched_at(NAME) is None

    @pytest.mark.asyncio
    async def test_the_hourly_copy_then_keeps_the_override(self, stores):
        """4. After the copy-back DuckDB holds the override, so the replace
        out of it puts the override back rather than removing it."""
        from core.chain_transfer import copy_back
        from core.pg_operational import replicate_operational

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1, name="Покупець Один")])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE app.buyer_gender SET override_by_human = TRUE, "
                               "decided_at = $2 WHERE buyer_id = $1",
                               1, DECIDED + timedelta(hours=1))
        assert (await copy_back(store, chain, dry_run=False))["released"]

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert GENDER in result["replaced"], result
        assert (await _pg(pool, "SELECT override_by_human FROM app.buyer_gender "
                                "WHERE buyer_id = 1"))[0][0] is True

    @pytest.mark.asyncio
    async def test_control_without_the_copy_back_the_same_copy_wipes_it(self, stores):
        """5. Release by hand and let the hourly copy run: DuckDB never learnt
        of the override, so the full replace removes it. What the test above
        keeps is the copy-back's doing."""
        from core.chain_transfer import release_chain
        from core.pg_operational import replicate_operational

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE app.buyer_gender SET override_by_human = TRUE "
                               "WHERE buyer_id = 1")
        await release_chain(pool, chain)

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert (await _pg(pool, "SELECT override_by_human FROM app.buyer_gender "
                                "WHERE buyer_id = 1"))[0][0] is False

    @pytest.mark.asyncio
    async def test_a_whitespace_name_goes_back_as_it_is(self, stores):
        """7. DuckDB's NOT NULL takes it; only NULL is refused."""
        from core.chain_transfer import copy_back

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.buyers SET full_name = '   ', "
                               "mirrored_at = now() WHERE id = 1")

        plan = await copy_back(store, chain, dry_run=False)

        assert plan["released"], plan
        assert (await _duck(store, "SELECT full_name FROM buyers WHERE id = 1"))[0][0] == "   "


class TestTheRefusals:
    """6. Each is a CRITICAL in `--handover` and a refusal before anything is
    written, in both the dry run and the execution."""

    async def _refused(self, store, chain, check, table):
        from core.chain_transfer import CopyBackRefused, copy_back, handover_check

        assert (check, table) in _critical(await handover_check(store, chain))
        for dry_run in (True, False):
            with pytest.raises(CopyBackRefused) as refused:
                await copy_back(store, chain, dry_run=dry_run)
            assert (check, table) in {(i.check_name, i.table_name)
                                      for i in refused.value.issues}

    @pytest.mark.asyncio
    async def test_a_null_name(self, stores):
        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("UPDATE bronze.buyers SET full_name = NULL, "
                               "mirrored_at = now() WHERE id = 1")
        await self._refused(store, chain, "handover_rows_unwritable", BUYERS)
        # Nothing was written: DuckDB still holds the old name.
        assert (await _duck(store, "SELECT full_name FROM buyers WHERE id = 1"))[0][0] == "Покупець 1"

    @pytest.mark.asyncio
    async def test_a_buyer_only_duckdb_holds(self, stores):
        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        # After the latch the mirror stands down, so this reaches DuckDB alone.
        await store.upsert_buyers([_buyer(9)])
        await self._refused(store, chain, "handover_rows_missing", BUYERS)

    @pytest.mark.asyncio
    async def test_a_contact_of_a_buyer_the_chain_never_rewrote(self, stores):
        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1), _buyer(2)])
        await _latch_and_write(pool, [_buyer(1)])     # buyer 2 untouched
        async with store.connection() as conn:
            conn.execute("INSERT INTO buyer_contacts (buyer_id, contact_type, value, "
                         "is_primary) VALUES (2, 'phone', '+380599', FALSE)")
        await self._refused(store, chain, "handover_rows_missing", CONTACTS)

    @pytest.mark.asyncio
    async def test_before_a_flip_a_contact_only_postgres_holds(self, stores):
        from core.chain_transfer import handover_check

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyer_contacts (buyer_id, "
                               "contact_type, value, is_primary) "
                               "VALUES (1, 'phone', '+380598', FALSE)")
        assert ("handover_rows_ahead", CONTACTS) in _critical(await handover_check(store, chain))


class TestTheReship:
    @pytest.mark.asyncio
    async def test_it_clears_what_the_pre_flip_handover_refused(self, stores):
        """8. A contact only Postgres holds and a buyer that differs: both
        CRITICAL before a flip, both gone after the reship."""
        from core.chain_transfer import handover_check
        from core.pg_buyers import reship_buyers

        store, pool, chain, _env = stores
        await _mirrored(store, pool, [_buyer(1), _buyer(2)])
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.buyer_contacts (buyer_id, "
                               "contact_type, value, is_primary) "
                               "VALUES (1, 'phone', '+380597', FALSE)")
            await conn.execute("UPDATE bronze.buyers SET full_name = 'Інше' WHERE id = 2")
        assert _critical(await handover_check(store, chain)) == {
            ("handover_rows_ahead", CONTACTS), ("handover_rows_differ", BUYERS)}

        result = await reship_buyers(store, chunk=1)

        assert result["status"] == "done" and result["buyers_shipped"] == 2, result
        assert await handover_check(store, chain) == []

    @pytest.mark.asyncio
    async def test_it_refuses_once_the_chain_is_latched(self, stores):
        from core.pg_buyers import reship_buyers

        store, pool, _chain, _env = stores
        await _mirrored(store, pool, [_buyer(1)])
        await _latch_and_write(pool, [_buyer(1)])
        with pytest.raises(RuntimeError, match="is written by a write chain"):
            await reship_buyers(store)
