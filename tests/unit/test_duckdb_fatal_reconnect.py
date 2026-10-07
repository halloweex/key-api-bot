"""A DuckDB FatalException drops the instance; the next use opens the file
again, and /api/health says it happened.

A FATAL invalidates the whole DuckDB instance: every later statement on it
raises FatalException "database has been invalidated", quoting the original
error (measured on 1.5.5, and pinned below). `connection()` reconnected only
when there was no connection, so one FATAL used to leave web's DuckDB dead
until a restart — the caller already queued on the lock included. Now the
instance is asked whether it still answers whenever a block raises.

The FATAL here is the real one, on a production-shaped file: the schema
checkpointed by `DuckDBStore`, then `order_products`' index made short the
way it happened before `open_read_write` — a writer killed with rows in its
WAL, and a restart that opened and closed the file without checkpointing
first. Dropping the instance does not heal that: the index is short in the
file, so the same write fails again after every reconnect. That is why the
status stays degraded — the reconnect must not make a write that keeps
failing look healthy.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from core import duckdb_store  # noqa: E402
from core.duckdb_store import DuckDBStore  # noqa: E402

_ROW = ("INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
        "VALUES (?, ?, 1, 'x', 1, 1.0)")
# Changes the indexed column, so the old entry must come out of the index —
# and the killed rows have none there.
_FATAL_WRITE = "UPDATE order_products SET order_id = 3 WHERE id = 2001"


async def _connect(path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=path)
    await store.connect()
    return store


def _child(path: str) -> None:
    store = asyncio.run(_connect(Path(path)))
    store._connection.executemany(_ROW, [(2001, 2), (2002, 2), (2003, 2)])
    os.kill(os.getpid(), signal.SIGKILL)   # `store` still referenced: no close


@pytest.fixture(scope="module")
def damaged_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("damaged") / "analytics.duckdb"

    async def seed():
        store = await _connect(path)
        try:
            store._connection.executemany(_ROW, [(1001, 1)])
        finally:
            await store.close()

    asyncio.run(seed())
    env = {k: v for k, v in os.environ.items() if k not in ("KS_PG_DSN", "KS_CH_URL")}
    env["KS_ALERTS_DISABLED"] = "1"
    rc = subprocess.run([sys.executable, __file__, "--child", str(path)],
                        cwd=REPO, env=env, timeout=120).returncode
    assert rc == -signal.SIGKILL, rc
    duckdb.connect(str(path)).close()   # the restart before the guard: lossy
    assert not Path(f"{path}.wal").exists()
    return path


def _copy(damaged: Path, into: Path) -> Path:
    into.mkdir(parents=True, exist_ok=True)
    dest = into / "analytics.duckdb"
    shutil.copy2(damaged, dest)
    return dest


async def _write(store):
    async with store.connection() as conn:
        await asyncio.sleep(0.05)       # long enough for a second caller to queue
        conn.execute(_FATAL_WRITE)


async def _count(store, delay: float = 0.0):
    await asyncio.sleep(delay)
    async with store.connection() as conn:
        return conn.execute("SELECT count(*) FROM order_products").fetchone()[0]


def _fresh_process_count(path: Path) -> str:
    code = ("import duckdb, sys\n"
            "c = duckdb.connect(sys.argv[1], read_only=True)\n"
            "print(c.execute('SELECT count(*) FROM order_products').fetchone()[0])\n")
    out = subprocess.run([sys.executable, "-c", code, str(path)],
                         capture_output=True, text=True, timeout=60)
    return out.stdout.strip() or out.stderr.strip()


@pytest.mark.asyncio
async def test_the_fatal_is_raised_and_everything_else_keeps_answering(
        damaged_file, tmp_path, caplog):
    path = _copy(damaged_file, tmp_path / "db")
    store = await _connect(path)
    try:
        assert store.fatal_status() is None
        with caplog.at_level(logging.ERROR, logger=duckdb_store.logger.name):
            fatal, queued = await asyncio.gather(_write(store), _count(store, 0.01),
                                                 return_exceptions=True)
        assert isinstance(fatal, duckdb.FatalException), fatal
        # Measured on 1.5.5: this text, and a dump of the rows after it.
        assert "Failed to delete all rows from index" in str(fatal)
        assert "\nChunk:" in str(fatal)
        logged = [r.getMessage() for r in caplog.records if "DuckDB FATAL" in r.getMessage()]
        assert len(logged) == 1 and "weekly_compact.sh" in logged[0], logged
        assert "Chunk" not in logged[0], "the log carries row values (phones are indexed)"
        assert queued == 4, f"the caller queued on the lock: {queued!r}"
        assert await _count(store) == 4
        status = store.fatal_status()
        assert status["count"] == 1 and status["kinds"] == {"index": 1}
        assert status["last_at"]

        # Not healed: the index is short in the file, not in the instance.
        with pytest.raises(duckdb.FatalException):
            await _write(store)
        assert store.fatal_status()["kinds"] == {"index": 2}
        assert await _count(store) == 4
    finally:
        await store.close()
    assert _fresh_process_count(path) == "4"


@pytest.mark.asyncio
async def test_a_fatal_its_caller_wrapped_still_drops_the_instance(damaged_file, tmp_path):
    path = _copy(damaged_file, tmp_path / "db")
    store = await _connect(path)
    try:
        with pytest.raises(RuntimeError):
            async with store.connection() as conn:
                try:
                    conn.execute(_FATAL_WRITE)
                except duckdb.Error as exc:
                    raise RuntimeError("upsert failed") from exc
        assert store.fatal_status()["kinds"] == {"index": 1}
        assert await _count(store) == 4
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_fatal_its_caller_replaced_with_no_chain_is_dropped_at_once(
        damaged_file, tmp_path):
    """The exception that leaves the block says nothing about DuckDB at all;
    the instance is asked, so it does not matter."""
    path = _copy(damaged_file, tmp_path / "db")
    store = await _connect(path)
    try:
        with pytest.raises(LookupError):
            async with store.connection() as conn:
                failed = False
                try:
                    conn.execute(_FATAL_WRITE)
                except duckdb.Error:
                    failed = True
                if failed:
                    raise LookupError("no such order")
        assert store._connection is None
        assert store.fatal_status()["kinds"] == {"index": 1}
        assert await _count(store) == 4
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_fatal_its_caller_swallowed_is_dropped_by_the_next_use(damaged_file, tmp_path):
    path = _copy(damaged_file, tmp_path / "db")
    store = await _connect(path)
    try:
        async with store.connection() as conn:
            with pytest.raises(duckdb.FatalException):
                conn.execute(_FATAL_WRITE)
        assert store.fatal_status() is None, "nothing reached connection() yet"

        with pytest.raises(duckdb.FatalException, match="invalidated"):
            await _count(store)
        # The echo carries the original error, so the kind is still named.
        assert store.fatal_status()["kinds"] == {"index": 1}
        assert await _count(store) == 4
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_fatal_from_another_instance_keeps_ours(damaged_file, tmp_path):
    """A FatalException in hand is not proof that *this* instance is dead."""
    store = await _connect(tmp_path / "ours.duckdb")
    other = _copy(damaged_file, tmp_path / "other")
    try:
        ours = store._connection
        with pytest.raises(duckdb.FatalException):
            async with store.connection():
                foreign = duckdb.connect(str(other))
                try:
                    foreign.execute(_FATAL_WRITE)
                finally:
                    foreign.close()
        assert store._connection is ours
        assert store.fatal_status() is None
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_checkpoint_fatal_is_other_and_the_reconnect_loses_nothing(tmp_path, caplog):
    """A FATAL that is not an index: the checkpoint aborted by DuckDB's own
    debug switch. Everything committed before it sits in the WAL, which the
    invalidated instance leaves behind and the reconnect replays — through
    `open_read_write`, which checkpoints it first and names it.

    The schema is checkpointed first, as on every production file: a WAL
    holding the first start's `ALTER TABLE ... ADD COLUMN` on a table with a
    function DEFAULT cannot be replayed at all on 1.5.5 (see CLAUDE.md)."""
    path = tmp_path / "a.duckdb"
    await (await _connect(path)).close()
    store = await _connect(path)
    try:
        async with store.connection() as conn:
            conn.executemany(_ROW, [(5001, 7), (5002, 7)])
            conn.execute("SET debug_checkpoint_abort = 'before_header'")
        with pytest.raises(duckdb.FatalException, match="checkpoint"):
            await store.checkpoint()
        assert store._connection is None
        assert store.fatal_status()["kinds"] == {"other": 1}
        assert Path(f"{path}.wal").stat().st_size > 0

        with caplog.at_level(logging.WARNING, logger=duckdb_store.logger.name):
            assert await _count(store) == 2
        assert any("replayed" in r.getMessage() and "FATAL" in r.getMessage()
                   for r in caplog.records), [r.getMessage() for r in caplog.records]
        async with store.connection() as conn:
            assert conn.execute(
                "SELECT count(*) FROM order_products WHERE order_id = 7").fetchone()[0] == 2
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_the_hourly_checkpoint_drops_an_invalidated_instance(damaged_file, tmp_path):
    path = _copy(damaged_file, tmp_path / "db")
    store = await _connect(path)
    try:
        async with store.connection() as conn:
            with pytest.raises(duckdb.FatalException):
                conn.execute(_FATAL_WRITE)
        with pytest.raises(duckdb.FatalException):
            await store.checkpoint()
        assert store._connection is None
        assert await _count(store) == 4
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_an_ordinary_error_keeps_the_instance(tmp_path):
    store = await _connect(tmp_path / "ok.duckdb")
    try:
        conn_before = store._connection
        with pytest.raises(duckdb.CatalogException):
            async with store.connection() as conn:
                conn.execute("SELECT * FROM no_such_table")
        assert store._connection is conn_before
        assert store.fatal_status() is None
    finally:
        await store.close()


def test_health_reads_degraded_and_names_the_kind(damaged_file, monkeypatch):
    """Through the endpoint the canary polls: DuckDB answering again after the
    reconnect, and the status degraded anyway."""
    from fastapi.testclient import TestClient

    from web.main import app
    from web.ratelimit import limiter
    from web.routes.api import health as health_routes

    shutil.copy2(damaged_file, duckdb_store.DB_PATH)
    monkeypatch.setitem(health_routes._stats_cache, "data", None)
    monkeypatch.setitem(health_routes._stats_cache, "expires_at", 0)

    async def fatal_once():
        store = await duckdb_store.get_store()
        with pytest.raises(duckdb.FatalException):
            await _write(store)

    asyncio.run(fatal_once())
    limiter.reset()
    try:
        body = TestClient(app).get("/api/health").json()
    finally:
        limiter.reset()
        asyncio.run(duckdb_store.close_store())

    assert body["duckdb"]["status"] == "connected"
    assert body["duckdb"]["orders"] is not None
    assert body["duckdb"]["fatal"]["kinds"] == {"index": 1}
    assert body["status"] == "degraded"


if __name__ == "__main__" and sys.argv[1:2] == ["--child"]:
    _child(sys.argv[2])
