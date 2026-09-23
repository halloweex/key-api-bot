"""Where DN-06's owner rows meet the failure stamp of the serialised copy.

`replicate_operational` learns which tables Postgres owns in two steps: the
local markers and flags before the first round trip, then the owner rows in
`meta.chain_watermarks` once it holds a pool. A run that fails stamps every
table it was copying. These pin which set a failure is written against on each
side of the owner-row read.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_operational
from core.duckdb_store import DuckDBStore
from core.pg_operational import _APPEND_ABOVE, _FULL_REPLACE, replicate_operational
from core.write_chains import WRITE_CHAINS, chain_name

EVERY_TABLE = (*(t for t, _d, _c, _o in _FULL_REPLACE),
               *(s.pg_table for s in _APPEND_ABOVE))
STAMP = "2026-09-18T08:00:00+00:00"


class _Conn:
    def __init__(self, fail_at):
        self.fail_at = fail_at
        self.touched = []

    async def fetchval(self, sql, *args):
        if self.fail_at == "watermark":
            raise RuntimeError("failed after the owner rows: watermark")
        return None

    @asynccontextmanager
    async def _tx(self):
        yield

    def transaction(self):
        return self._tx()

    async def execute(self, sql, *args):
        self.touched.append(sql)
        if self.fail_at == "write":
            raise RuntimeError("failed after the owner rows: write")

    async def executemany(self, sql, rows):
        self.touched.append(sql)
        if self.fail_at == "write":
            raise RuntimeError("failed after the owner rows: write")


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def _acq(self):
        yield self.conn

    def acquire(self):
        return self._acq()


class _BrokenStore:
    def connection(self):
        raise RuntimeError("failed after the owner rows: duckdb")


@pytest.fixture
def no_flags(monkeypatch):
    """No KS_WRITE_* set; the conftest has pointed the markers at tmp_path, so
    nothing is latched locally — the marker-lost state when owner rows exist."""
    for chain in WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    monkeypatch.setattr(pg_operational, "OPERATIONAL_REPLICATION_LOCK", asyncio.Lock())
    return monkeypatch


async def _run(store, pool, owners, stamps):
    with patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()), \
         patch("core.chain_latch.read_owners", new=AsyncMock(return_value=owners)), \
         patch("core.pg_landing._record_failure", new=stamps):
        return await replicate_operational(store)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", ["watermark", "duckdb", "write"])
@pytest.mark.parametrize("chain", WRITE_CHAINS, ids=chain_name)
async def test_a_failure_after_the_owner_rows_never_stamps_an_owned_table(
    no_flags, tmp_path, chain, fail_at,
):
    """Owner rows and no marker (DN-06's dangerous direction), then a failure
    anywhere later. The owned tables were never going to be written, so their
    watermark must not read as a copy that failed: every other table is
    stamped, the chain's are not, and the lock is free afterwards."""
    owners = {t: STAMP for t in chain.CHAIN_TABLES}
    conn = _Conn(fail_at)
    if fail_at == "duckdb":
        store = _BrokenStore()
    else:
        store = DuckDBStore(db_path=tmp_path / "ops.duckdb")
        await store.connect()
    stamps = AsyncMock(return_value=True)
    try:
        result = await _run(store, _Pool(conn), owners, stamps)
    finally:
        if fail_at != "duckdb":
            await store.close()

    assert "failed after the owner rows" in result["error"], result
    stamped = [c.args[0] for c in stamps.await_args_list]
    assert stamped == [t for t in EVERY_TABLE if t not in chain.CHAIN_TABLES]
    assert {c.args[1] for c in stamps.await_args_list} == {result["error"]}
    for sql in conn.touched:
        assert not any(t in sql for t in chain.CHAIN_TABLES), sql
    assert not pg_operational.OPERATIONAL_REPLICATION_LOCK.locked()


@pytest.mark.asyncio
async def test_a_failure_before_the_owner_rows_can_only_use_the_local_answer(no_flags):
    """The other side of the read, pinned so that moving it is a decision:
    when `require_revision` raises, the owner rows were never read, and the
    failure is stamped on everything the markers and flags leave — including
    a table an owner row would have held down. Nothing is written either way."""
    from core import pg_expenses_write

    stamps = AsyncMock(return_value=True)
    read_owners = AsyncMock(return_value={"app.manual_expenses": STAMP})
    with patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=AsyncMock(return_value=_Pool(_Conn(None)))), \
         patch("core.pg.require_revision",
               new=AsyncMock(side_effect=RuntimeError("schema is behind"))), \
         patch("core.chain_latch.read_owners", new=read_owners), \
         patch("core.pg_landing._record_failure", new=stamps):
        result = await replicate_operational(object())

    assert read_owners.await_count == 0
    stamped = [c.args[0] for c in stamps.await_args_list]
    assert stamped == list(EVERY_TABLE)
    assert set(pg_expenses_write.CHAIN_TABLES) <= set(stamped)
    assert "schema is behind" in result["error"]


@pytest.mark.asyncio
async def test_a_cancelled_copy_releases_the_lock(no_flags):
    """CancelledError is not an Exception, so it skips the stamp — the lock
    must still come back, or every later copy (and every form submit) waits
    on a run that is gone."""
    entered = asyncio.Event()

    async def hang():
        entered.set()
        await asyncio.Event().wait()

    with patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=hang):
        task = asyncio.create_task(replicate_operational(object()))
        await asyncio.wait_for(entered.wait(), 2)
        assert pg_operational.OPERATIONAL_REPLICATION_LOCK.locked()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not pg_operational.OPERATIONAL_REPLICATION_LOCK.locked()
