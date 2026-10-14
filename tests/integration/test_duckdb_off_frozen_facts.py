"""Stage 5 against a real Postgres: what `KS_DUCKDB=off` reads from the frozen
copy of DuckDB's `sync_metadata` instead of the file.

The unit tests (`tests/unit/test_warehouse_writer_off.py`) hand the readers a
connection that answers what the test says. These ask a Postgres at head the
same statements, on the tables the hourly copy writes — so a statement that
names a column the schema does not have, or a type the driver hands back
differently, fails here rather than on the morning the week starts. Off's
coherence verdict (PR-2, `core/duckdb_off.py`) reads the same copy and its
watermark at web's start, and is asked here the same way.
"""
from __future__ import annotations

import json
import os
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

RESOLVED = {"writer": "postgres", "since": "2026-10-01T06:00:00+00:00",
            "resolved": True, "resolved_at": "2026-10-01T06:00:01+00:00"}


@pytest_asyncio.fixture
async def pool():
    """A pool on the throwaway Postgres, with `app.sync_metadata`'s
    `warehouse_writer` row put back as it was found."""
    from core import warehouse_cutover as wc

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        held = await conn.fetchrow(
            "SELECT value, updated_at FROM app.sync_metadata WHERE key = $1",
            wc.WRITER_KEY)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                               wc.WRITER_KEY)
            if held is not None:
                await conn.execute(
                    "INSERT INTO app.sync_metadata (key, value, updated_at) "
                    "VALUES ($1, $2, $3)", wc.WRITER_KEY, held["value"],
                    held["updated_at"])
        await pool.close()


async def _seed_writer(pool, value):
    from core import warehouse_cutover as wc

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO app.sync_metadata (key, value, updated_at) "
            "VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            wc.WRITER_KEY, value)
        return await conn.fetchrow(
            "SELECT value, updated_at FROM app.sync_metadata WHERE key = $1",
            wc.WRITER_KEY)


class TestTheWriterRecordFromTheFrozenCopy:
    @pytest.mark.asyncio
    async def test_the_copy_reads_as_the_record(self, pool):
        from core import warehouse_cutover as wc

        await _seed_writer(pool, json.dumps(RESOLVED))
        assert await wc.read_frozen_writer() == RESOLVED
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                               wc.WRITER_KEY)
        assert await wc.read_frozen_writer() is None

    @pytest.mark.asyncio
    async def test_a_settle_under_off_reads_it_and_writes_nothing(
        self, pool, duckdb_off, monkeypatch,
    ):
        """Through the application's own reader — the revision check, the
        pool, the statement — and the row left exactly as it was."""
        from core import duckdb_switch, warehouse_cutover as wc

        before = await _seed_writer(pool, json.dumps(RESOLVED))
        monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
        resolve = AsyncMock(return_value=0)
        with patch("core.alerting.resolve_group", resolve):
            await wc.settle_writer(None)
        async with pool.acquire() as conn:
            after = await conn.fetchrow(
                "SELECT value, updated_at FROM app.sync_metadata WHERE key = $1",
                wc.WRITER_KEY)
        assert after == before
        assert wc._settled and wc.status()["writer"] == RESOLVED
        resolve.assert_not_awaited()
        assert duckdb_switch.opened() == {} and not duckdb_off.exists()


# ─── Off's coherence verdict reads the same copy (stage 5's PR-2) ────────────

@pytest_asyncio.fixture
async def verdict_rows(pool):
    """`meta.mirror_state`'s row for the copy and every `owner:` row, put back
    as they were found; the writer row is `pool`'s."""
    async with pool.acquire() as conn:
        state = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = 'app.sync_metadata'")
        owners = await conn.fetch(
            "SELECT key, value, updated_at FROM meta.chain_watermarks "
            "WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = 'app.sync_metadata'")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
    try:
        yield pool
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = 'app.sync_metadata'")
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
            for row in state:
                cols = list(row.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                    f"({', '.join(f'${i + 1}' for i in range(len(cols)))})",
                    *[row[c] for c in cols])
            for row in owners:
                await conn.execute(
                    "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                    "VALUES ($1, $2, $3)", row["key"], row["value"], row["updated_at"])


async def _latched(conn, chains, *, hours_ago: float) -> None:
    """An owner row for every table of `chains`, claimed `hours_ago`."""
    from core.chain_latch import owner_key

    for chain in chains:
        for table in chain.CHAIN_TABLES:
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ($1, 'x', now() - $2 * interval '1 hour')",
                owner_key(table), hours_ago)


async def _copied(pool, *, minutes_after_latch: int, chains=None):
    """An owner row for every table of every chain (or of `chains`), two
    hours old, and a copy `minutes_after_latch` after them, on one
    transaction's clock."""
    from core.write_chains import WRITE_CHAINS

    async with pool.acquire() as conn, conn.transaction():
        await _latched(conn, WRITE_CHAINS if chains is None else chains, hours_ago=2)
        await conn.execute(
            "INSERT INTO meta.mirror_state (table_name, last_ok_at) VALUES "
            "('app.sync_metadata', now() - interval '2 hours' + $1 * interval '1 minute')",
            minutes_after_latch)


class TestOffsVerdictReadsTheCopy:
    """`core.duckdb_off` asks Postgres on a connection of its own, from a
    worker, at web's start: the statements against the schema at head."""

    @pytest.mark.asyncio
    async def test_the_three_facts_as_the_schema_hands_them_back(self, verdict_rows):
        from core import duckdb_off

        await _seed_writer(verdict_rows, json.dumps(RESOLVED))
        await _copied(verdict_rows, minutes_after_latch=5)
        facts = await duckdb_off._frozen_facts_on_its_own_connection(DSN)
        assert facts["writer"] == RESOLVED
        assert set(facts["owners"]) == {
            t for c in _chains() for t in c.CHAIN_TABLES}
        assert {facts["copied_at"] - at for at in facts["owners"].values()} == {
            timedelta(minutes=5)}
        unmet = duckdb_off.evaluate({"KS_USER_STORE": "postgres"}, duckdb_off.Facts(
            writes_postgres=True, warehouse_mode="postgres", chains={
                k: {"mode": "postgres"} for k in _chain_names()},
            pg_asked=True, **facts))
        assert unmet == []

    @pytest.mark.asyncio
    async def test_nothing_copied_and_no_owner_row(self, verdict_rows):
        from core import duckdb_off, warehouse_cutover as wc

        async with verdict_rows.acquire() as conn:
            await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                               wc.WRITER_KEY)
        facts = await duckdb_off._frozen_facts_on_its_own_connection(DSN)
        assert facts == {"writer": None, "copied_at": None, "owners": {}}

    @pytest.mark.asyncio
    async def test_a_copy_older_than_the_latch_holds(self, verdict_rows):
        from core import duckdb_off

        await _seed_writer(verdict_rows, json.dumps(RESOLVED))
        await _copied(verdict_rows, minutes_after_latch=-5)
        facts = await duckdb_off._frozen_facts_on_its_own_connection(DSN)
        unmet = duckdb_off.evaluate({"KS_USER_STORE": "postgres"}, duckdb_off.Facts(
            writes_postgres=True, chains={k: {"mode": "postgres"} for k in _chain_names()},
            pg_asked=True, **facts))
        assert [u.key for u in unmet] == ["sync_metadata_copied"]

    @pytest.mark.asyncio
    async def test_the_start_reads_it_from_a_worker(self, verdict_rows, monkeypatch):
        """`settle` called synchronously inside a running loop, as web's
        startup calls `configure_modes(web=True)`: the worker reads Postgres
        on a loop and a connection of its own, and off holds."""
        from core import duckdb_off, duckdb_switch, warehouse_cutover as wc

        await _seed_writer(verdict_rows, json.dumps(RESOLVED))
        await _copied(verdict_rows, minutes_after_latch=5)
        monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
        monkeypatch.setattr("core.write_chains.chain_modes", lambda: {
            k: {"mode": "postgres"} for k in _chain_names()})
        monkeypatch.setenv("KS_USER_STORE", "postgres")
        monkeypatch.setenv("KS_PG_DSN", DSN)
        monkeypatch.setenv(duckdb_switch.ENV, "off")
        duckdb_switch.configure_mode()
        assert duckdb_off.settle() == "off", duckdb_off.unmet()
        assert duckdb_switch.held_back() == ()


def _chains():
    from core.write_chains import WRITE_CHAINS

    return WRITE_CHAINS


def _chain_names():
    from core.write_chains import WRITE_CHAINS, chain_name

    return [chain_name(c) for c in WRITE_CHAINS]


def _start(monkeypatch, chains):
    """A web start under off over `chains` (`chain_modes()`), every other
    fact met, its verdict and the switch forgotten first: `(mode, held_back,
    unmet)`."""
    from core import duckdb_off, duckdb_switch, warehouse_cutover as wc

    duckdb_switch.reset()
    duckdb_off.reset()
    monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
    monkeypatch.setattr("core.write_chains.chain_modes", lambda: chains)
    monkeypatch.setenv("KS_USER_STORE", "postgres")
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    duckdb_switch.configure_mode()
    return duckdb_off.settle(), duckdb_switch.held_back(), duckdb_off.unmet()


class TestTheWeekIsNotLostToALatchUnderOff:
    """Review of PR-2: every owner row used to count against the copy, and
    under off nothing moves the copy — it is DuckDB's table. A chain that is
    `postgres` by its flag and has not latched passes `chain:<name>` at the
    start of the week; its first write under off claims an owner row stamped
    then, and the next start — a deploy in the middle of the week — was held
    back on `sync_metadata_copied`: it opened the file, and the week was
    lost."""

    @pytest.mark.asyncio
    async def test_chain_8_latching_mid_week_does_not_hold_the_next_start(
        self, verdict_rows, monkeypatch,
    ):
        """Production's chain 8: `KS_WRITE_EXPENSES=postgres`, no expense
        typed yet. It keeps nothing in DuckDB's `sync_metadata`, so its latch
        says nothing about the copy."""
        from core import chain_latch, pg_expenses_write
        from core.write_chains import WRITE_CHAINS, chain_name

        await _seed_writer(verdict_rows, json.dumps(RESOLVED))
        await _copied(verdict_rows, minutes_after_latch=60, chains=[
            c for c in WRITE_CHAINS if c is not pg_expenses_write])
        chains = {k: {"mode": "postgres", "latched": True} for k in _chain_names()}
        chains[chain_name(pg_expenses_write)] = {
            "mode": "postgres", "latched": False, "env": pg_expenses_write.WRITE_ENV}
        assert _start(monkeypatch, chains)[:2] == ("off", ())

        # The first typed expense, under off: the owner row is claimed in the
        # writing transaction, as `pg_expenses_write` claims it.
        async with verdict_rows.acquire() as conn, conn.transaction():
            await chain_latch.claim(conn, pg_expenses_write.CHAIN_TABLES,
                                    "2026-10-15T10:00:00+00:00")
        chains[chain_name(pg_expenses_write)]["latched"] = True
        mode, held, unmet = _start(monkeypatch, chains)
        assert (mode, held) == ("off", ()), [(u.key, u.detail) for u in unmet]

    @pytest.mark.asyncio
    async def test_a_chain_that_keeps_a_value_there_and_has_not_latched_holds_the_first(
        self, verdict_rows, monkeypatch,
    ):
        """Its latch would come under off too, and it does freeze a value the
        copy must carry — so the first start of the week is held, while
        waiting costs nothing, rather than a start in the middle of it."""
        from core import pg_expense_types_write
        from core.write_chains import WRITE_CHAINS, chain_name

        await _seed_writer(verdict_rows, json.dumps(RESOLVED))
        await _copied(verdict_rows, minutes_after_latch=60, chains=[
            c for c in WRITE_CHAINS if c is not pg_expense_types_write])
        chains = {k: {"mode": "postgres", "latched": True} for k in _chain_names()}
        chains[chain_name(pg_expense_types_write)] = {
            "mode": "postgres", "latched": False,
            "env": pg_expense_types_write.WRITE_ENV}
        mode, held, unmet = _start(monkeypatch, chains)
        assert (mode, held) == ("on", ("sync_metadata_copied",))
        assert chain_name(pg_expense_types_write) in unmet[0].detail
