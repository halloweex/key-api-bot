"""The derivation signal and journal against a real Postgres (revision 0033).

What matters is not that the counters count — it is that a mark can never cost
the transaction it rides in, and that a mark the derivation's snapshot could not
see stays owed. Both are about concurrency and locks, which only a real server
can show.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=2, max_size=12)

    async def reset():
        async with p.acquire() as conn:
            await conn.execute(
                "UPDATE meta.derivation_signal SET requested = 1, built = 0,"
                " built_at = NULL WHERE layer = 'warehouse'")
            await conn.execute("DELETE FROM meta.derivation_runs")
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = 'meta.derivation_signal'")

    await reset()
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)):
        yield p
    await reset()
    await p.close()


async def _owed(p):
    from core.pg_derivation import read_owed
    async with p.acquire() as conn:
        return await read_owed(conn)


class TestTheSeed:
    @pytest.mark.asyncio
    async def test_the_migration_leaves_a_rebuild_owed(self, pool):
        """The reset puts back what revision 0033 seeds; a fresh consumer must
        rebuild rather than trust a table it never built."""
        requested, built, built_at = await _owed(pool)
        assert (requested, built, built_at) == (1, 0, None)


class TestMarks:
    @pytest.mark.asyncio
    async def test_fifty_concurrent_marks_on_separate_connections_add_fifty(self, pool):
        from core.pg_derivation import mark

        async def one():
            async with pool.acquire() as conn:
                async with conn.transaction():
                    assert await mark(conn) is True

        await asyncio.gather(*(one() for _ in range(50)))
        requested, built, _ = await _owed(pool)
        assert (requested, built) == (51, 0)

    @pytest.mark.asyncio
    async def test_a_successful_mark_leaves_the_callers_lock_timeout_alone(self, pool):
        """The one-second timeout is the mark's. A landing that keeps writing
        after it — or a caller with a timeout of its own — must not inherit it.
        The failure path cannot show this: a rolled-back savepoint takes its
        settings with it whether or not they were put back."""
        from core.pg_derivation import mark

        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL lock_timeout = '7s'")
                assert await mark(conn) is True
                assert await conn.fetchval("SELECT current_setting('lock_timeout')") == "7s"

    @pytest.mark.asyncio
    async def test_a_mark_in_a_rolled_back_transaction_never_happened(self, pool):
        """No mark without the rows: the landing that rolls back takes its mark."""
        from core.pg_derivation import mark

        async with pool.acquire() as conn:
            with pytest.raises(RuntimeError):
                async with conn.transaction():
                    await mark(conn)
                    raise RuntimeError("the landing failed")
        assert (await _owed(pool))[0] == 1


class TestAMarkCannotCostTheTransaction:
    @pytest.mark.asyncio
    async def test_a_locked_signal_drops_the_mark_and_the_rows_commit(self, pool):
        """A migration holding ACCESS EXCLUSIVE on the signal — the deploy-time
        case. The mark gives up after its lock timeout; the caller's row, which
        stands in for an order version, commits anyway."""
        from core.pg_derivation import mark

        async with pool.acquire() as holder, pool.acquire() as writer:
            await writer.execute("CREATE TEMP TABLE landed (id int)")
            locked = holder.transaction()
            await locked.start()
            await holder.execute("LOCK TABLE meta.derivation_signal IN ACCESS EXCLUSIVE MODE")
            try:
                async with writer.transaction():
                    await writer.execute("INSERT INTO landed VALUES (1)")
                    started = asyncio.get_running_loop().time()
                    assert await mark(writer) is False
                    waited = asyncio.get_running_loop().time() - started
                    await writer.execute("INSERT INTO landed VALUES (2)")
            finally:
                await locked.rollback()
            assert await writer.fetchval("SELECT count(*) FROM landed") == 2
            # The lock timeout was the mark's alone: put back for the rest.
            assert await writer.fetchval("SELECT current_setting('lock_timeout')") == "0"
        assert waited < 5, "the mark queued behind the lock instead of giving up"
        assert (await _owed(pool))[0] == 1
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT failures_since_ok, last_error FROM meta.mirror_state"
                " WHERE table_name = 'meta.derivation_signal'")
        assert state is not None and state["failures_since_ok"] >= 1

    @pytest.mark.asyncio
    async def test_a_missing_layer_row_is_a_dropped_mark_not_an_error(self, pool):
        from core.pg_derivation import mark

        async with pool.acquire() as conn:
            async with conn.transaction():
                assert await mark(conn, layer="no-such-layer") is False
                assert await conn.fetchval("SELECT 1") == 1


class TestOwedState:
    @pytest.mark.asyncio
    async def test_a_mark_the_snapshot_did_not_see_stays_owed(self, pool):
        """The derivation reads `requested` inside its snapshot, a writer marks
        after that, the derivation completes with what it saw."""
        from core.pg_derivation import complete, mark, read_owed

        async with pool.acquire() as deriver, pool.acquire() as writer:
            tx = deriver.transaction(isolation="repeatable_read")
            await tx.start()
            seen, _built, _ = await read_owed(deriver)
            async with writer.transaction():
                assert await mark(writer)
            await tx.commit()
            async with deriver.transaction():
                await complete(deriver, seen)
        requested, built, built_at = await _owed(pool)
        assert (requested, built) == (2, 1)
        assert built_at is not None

    @pytest.mark.asyncio
    async def test_complete_never_moves_built_back_or_past_requested(self, pool):
        from core.pg_derivation import complete

        async with pool.acquire() as conn:
            await complete(conn, 1)
            await complete(conn, 0)
            assert (await _owed(pool))[1] == 1
            await complete(conn, 99)
        requested, built, _ = await _owed(pool)
        assert built == requested == 1


class TestTheJournal:
    @pytest.mark.asyncio
    async def test_the_bound_is_a_count(self, pool):
        from core.pg_derivation import record_run

        now = datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            ids = [await record_run(conn, trigger="test", started_at=now, keep=3,
                                    counts={"silver_rows": i},
                                    validation={"cells": i})
                   for i in range(5)]
            kept = await conn.fetch(
                "SELECT id, silver_rows, validation->>'cells' AS cells"
                " FROM meta.derivation_runs ORDER BY id")
        assert [r["id"] for r in kept] == ids[-3:]
        assert [r["silver_rows"] for r in kept] == [2, 3, 4]
        assert [r["cells"] for r in kept] == ["2", "3", "4"]
