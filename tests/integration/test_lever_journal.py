"""A copy-back leaves a durable record of itself (OD-17 (a)), against a real
Postgres.

The parallel period and the week of silence both restart when a rollback lever
is used, and the soak check that says so (`51_p2_rollback_levers.sql`) reads
`lever_used` rows in `app.alert_events`. Before `core/lever_journal.py` a
release left only an absence — owner rows deleted, a marker unlinked — which
nothing can date. What is held here:

- `release_chain` writes exactly one row, in the transaction that deletes the
  owner rows, so a release whose record fails does not commit;
- the script's exit 3 writes one when the owner rows still stand, and none when
  the release's own transaction already did;
- a record that cannot be written there is logged, never raised over the exit
  code the operator acts on.

The chain is a stand-in module, so no real chain's tables are touched.
"""
from __future__ import annotations

import json
import logging
import os
import types
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, lever_journal

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

NAME = "pg_lever_probe_write"
TABLE = "app.lever_probe_table"
SYNC_KEY = "last_sync_lever_probe"
KEY = lever_journal.lever_key(lever_journal.CHAIN_COPY_BACK)


def _chain() -> types.ModuleType:
    chain = types.ModuleType(f"core.{NAME}")
    chain.CHAIN_TABLES = (TABLE,)
    chain.CHAIN_SYNC_KEYS = (SYNC_KEY,)
    return chain


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
            [chain_latch.owner_key(TABLE), SYNC_KEY])
        await conn.execute(
            "DELETE FROM app.alert_events WHERE condition_key = $1 "
            "AND context->>'subject' = $2", KEY, NAME)


async def _latched(pool) -> None:
    chain_latch.latch(NAME)
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value) VALUES ($1, 'x'), ($2, 'y')",
            chain_latch.owner_key(TABLE), SYNC_KEY)


async def _records(pool) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT event_type, instance, message, context FROM app.alert_events "
            "WHERE condition_key = $1 AND context->>'subject' = $2 ORDER BY id",
            KEY, NAME)
    return [{**dict(r), "context": json.loads(r["context"])} for r in rows]


async def _owner_rows(pool) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
            [chain_latch.owner_key(TABLE), SYNC_KEY])


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    await _clean(p)
    yield p
    await _clean(p)
    await p.close()


class TestTheReleaseRecordsItself:
    @pytest.mark.asyncio
    async def test_one_row_with_the_chain_and_its_tables(self, pool, monkeypatch):
        from core.chain_transfer import release_chain

        monkeypatch.setenv("KS_INSTANCE", "lever-test")
        await _latched(pool)
        assert await release_chain(pool, _chain()) is True

        assert await _owner_rows(pool) == 0
        assert chain_latch.load().get(NAME) is None
        [row] = await _records(pool)
        assert row["event_type"] == lever_journal.LEVER_USED == "lever_used"
        assert row["instance"] == "lever-test"
        assert row["context"] == {"subject": NAME, "outcome": "released",
                                  "tables": [TABLE]}
        assert row["message"] == f"chain_copy_back: {NAME} released"

    @pytest.mark.asyncio
    async def test_a_release_whose_record_fails_does_not_commit(self, pool):
        """Mutation: move the `record` call out of the `transaction()` block in
        `release_chain` — the owner rows are then gone with no record."""
        from core.chain_transfer import release_chain

        await _latched(pool)
        failing = AsyncMock(side_effect=asyncpg.PostgresError("journal refused"))
        with patch.object(lever_journal, "record", new=failing):
            with pytest.raises(asyncpg.PostgresError):
                await release_chain(pool, _chain())
        failing.assert_awaited_once()
        assert await _owner_rows(pool) == 2, "the delete rolled back with it"
        assert chain_latch.load().get(NAME), "the marker is untouched"
        assert await _records(pool) == []

    @pytest.mark.asyncio
    async def test_the_record_shares_the_deletes_transaction(self, pool):
        """Read on the server: the row's xmin is the transaction that ran the
        delete. Mutation: open a second `pool.acquire()` for the record."""
        from core.chain_transfer import release_chain

        await _latched(pool)
        seen = {}
        real = lever_journal.record

        async def spy(conn, *a, **kw):
            seen["txid"] = await conn.fetchval("SELECT txid_current_if_assigned()")
            await real(conn, *a, **kw)

        with patch.object(lever_journal, "record", new=spy):
            await release_chain(pool, _chain())
        assert seen["txid"] is not None, "the delete had already written in it"
        async with pool.acquire() as conn:
            xmin = await conn.fetchval(
                "SELECT xmin::text::bigint FROM app.alert_events "
                "WHERE condition_key = $1 AND context->>'subject' = $2", KEY, NAME)
        assert xmin == seen["txid"] % (2 ** 32)


class TestExitThreeRecordsTheCopy:
    """`scripts/chain_copy_back.py` when the copy committed and the release did
    not finish: DuckDB has the rows, so the lever was used."""

    @staticmethod
    def _exc(latch):
        from core.chain_transfer import CommittedNotReleased

        return CommittedNotReleased("committed, not released", {"chain": NAME}, latch)

    @pytest.mark.asyncio
    async def test_owner_rows_standing_write_one(self, pool):
        from scripts import chain_copy_back as script

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            await script._record_committed(
                _chain(), self._exc({"owned_since": "2030-01-01", "owners_error": None}))
        [row] = await _records(pool)
        assert row["context"]["outcome"] == "committed_not_released"

    @pytest.mark.asyncio
    async def test_owner_rows_gone_the_release_already_wrote_it(self, pool):
        """The release's transaction committed and recorded itself; a second
        row would count one lever twice. Mutation: drop the early return."""
        from scripts import chain_copy_back as script

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            await script._record_committed(
                _chain(), self._exc({"owned_since": None, "owners_error": None}))
        assert await _records(pool) == []

    @pytest.mark.asyncio
    async def test_an_unreadable_postgres_is_logged_not_raised(self, caplog):
        from scripts import chain_copy_back as script

        dead = AsyncMock(side_effect=OSError("connection refused"))
        with patch("core.pg.get_pool", new=dead), \
                caplog.at_level(logging.ERROR, logger="chain_copy_back"):
            await script._record_committed(
                _chain(), self._exc({"owned_since": None,
                                     "owners_error": "OSError: refused"}))
        dead.assert_awaited_once()
        assert "could NOT be recorded" in caplog.text
