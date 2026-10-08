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
way it happened before `open_file` — a writer killed with rows in its
WAL, and a restart that opened and closed the file without checkpointing
first. Dropping the instance does not heal that: the index is short in the
file, so the same write fails again after every reconnect. That is why the
status stays degraded — the reconnect must not make a write that keeps
failing look healthy.
"""
from __future__ import annotations

import asyncio
import io
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from core import duckdb_store  # noqa: E402
from core.duckdb_store import DuckDBStore, StoreClosedError  # noqa: E402

_ROW = ("INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
        "VALUES (?, ?, 1, 'x', 1, 1.0)")
# Changes the indexed column, so the old entry must come out of the index —
# and the killed rows have none there.
_FATAL_WRITE = "UPDATE order_products SET order_id = 3 WHERE id = 2001"


async def _connect(path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=path)
    await store.connect()
    return store


# A person, as the buyers step writes one. Synthetic, and distinctive enough
# that finding any of it in a log can only mean the row dump.
_NAME, _PHONE, _EMAIL = "Testbuyer Zzyzx", "+380671234567", "secret.person@example.com"
_BUYER = ("INSERT INTO buyers (id, full_name, phone, email) "
          f"VALUES (77, '{_NAME}', '{_PHONE}', '{_EMAIL}')")


def _child(path: str, what: str) -> None:
    store = asyncio.run(_connect(Path(path)))
    if what == "buyers":
        store._connection.execute(_BUYER)
    else:
        store._connection.executemany(_ROW, [(2001, 2), (2002, 2), (2003, 2)])
    os.kill(os.getpid(), signal.SIGKILL)   # `store` still referenced: no close


def _damage(path: Path, what: str) -> Path:
    """The schema checkpointed by the store, a child that wrote `what` and was
    killed, and a restart that opened and closed the file without the guard —
    the order of events that left indexes short before `open_file`."""
    async def seed():
        store = await _connect(path)
        try:
            store._connection.executemany(_ROW, [(1001, 1)])
        finally:
            await store.close()

    asyncio.run(seed())
    env = {k: v for k, v in os.environ.items() if k not in ("KS_PG_DSN", "KS_CH_URL")}
    env["KS_ALERTS_DISABLED"] = "1"
    rc = subprocess.run([sys.executable, __file__, "--child", str(path), what],
                        cwd=REPO, env=env, timeout=120).returncode
    assert rc == -signal.SIGKILL, rc
    duckdb.connect(str(path)).close()   # the restart before the guard: lossy
    assert not Path(f"{path}.wal").exists()
    return path


@pytest.fixture(scope="module")
def damaged_file(tmp_path_factory):
    return _damage(tmp_path_factory.mktemp("damaged") / "analytics.duckdb", "order_products")


@pytest.fixture(scope="module")
def damaged_buyers(tmp_path_factory):
    return _damage(tmp_path_factory.mktemp("buyers") / "analytics.duckdb", "buyers")


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
        # Measured on 1.5.5: this text — and DuckDB prints the rows after it
        # (`test_no_row_value_leaves_the_store`), which the caller never gets.
        assert "Failed to delete all rows from index" in str(fatal)
        assert "Chunk" not in str(fatal)
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
        with pytest.raises(RuntimeError) as wrapped:
            async with store.connection() as conn:
                try:
                    conn.execute(_FATAL_WRITE)
                except duckdb.Error as exc:
                    raise RuntimeError("upsert failed") from exc
        assert store.fatal_status()["kinds"] == {"index": 1}
        # The dump is cut out of the whole chain, not only what left the block.
        assert "Failed to delete" in str(wrapped.value.__cause__)
        assert "Chunk" not in str(wrapped.value.__cause__)
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
    `open_file`, which checkpoints it first and names it.

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
        with pytest.raises(duckdb.FatalException, match="invalidated") as echoed:
            await store.checkpoint()
        assert "Failed to delete" in str(echoed.value)
        assert "Chunk" not in str(echoed.value), "the echo quotes the dump"
        assert store._connection is None
        assert await _count(store) == 4
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_a_fatal_while_connecting_leaves_without_the_dump(
        damaged_file, tmp_path, monkeypatch):
    """`connect()` is the store's third exit: the schema runs on the new
    instance, and what it raises goes to web's startup and its log."""
    path = _copy(damaged_file, tmp_path / "db")

    async def meets_the_short_index(self):
        self._connection.execute(_FATAL_WRITE)

    monkeypatch.setattr(DuckDBStore, "_init_schema", meets_the_short_index)
    store = DuckDBStore(db_path=path)
    with pytest.raises(duckdb.FatalException) as failed:
        await store.connect()
    assert "Failed to delete" in str(failed.value)
    assert "Chunk" not in str(failed.value)
    assert store._connection is None


@pytest.mark.asyncio
async def test_the_executor_goes_with_the_dropped_instance(damaged_file, tmp_path):
    """`connect()` builds a new executor; the old one, left running, is a
    worker thread per FATAL waiting for work nothing will send it."""
    path = _copy(damaged_file, tmp_path / "db")
    store = await _connect(path)
    try:
        assert await store._fetch_one("SELECT 1") == (1,)   # its thread exists
        old = store._executor
        with pytest.raises(duckdb.FatalException):
            await _write(store)
        assert await store._fetch_one("SELECT count(*) FROM order_products") == (4,)
        assert store._executor is not old
        with pytest.raises(RuntimeError, match="shutdown"):
            old.submit(lambda: None)
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


@pytest.mark.asyncio
async def test_an_aborted_transaction_is_not_a_fatal(tmp_path):
    """The probe itself can fail without the instance being dead: a block
    that raised inside its own transaction leaves it aborted, and `SELECT 1`
    then raises a TransactionException. Taking that for a FATAL would drop a
    good instance and hold /api/health degraded until a restart."""
    store = await _connect(tmp_path / "ok.duckdb")
    try:
        conn_before = store._connection
        with pytest.raises(duckdb.ConversionException):
            async with store.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("SELECT 'not a number'::INTEGER")
        with pytest.raises(duckdb.TransactionException, match="aborted"):
            conn_before.execute("SELECT 1")              # what the probe met
        assert store._connection is conn_before
        assert store.fatal_status() is None
        async with store.connection() as conn:
            conn.execute("ROLLBACK")
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_no_row_value_leaves_the_store(damaged_buyers, tmp_path, caplog):
    """DuckDB prints every column of the rows an index FATAL could not
    remove — for buyers, a name, a phone and an email — and repeats it in
    every "invalidated" echo. The buyers step logs a failed write with its
    traceback (`core/sync_service.py`, `exc_info=True`), and an index short in
    the file fails that write on every retry. The store's own line was cut;
    the exception it handed the caller was not, until now."""
    path = _copy(damaged_buyers, tmp_path / "db")

    # Not vacuous: DuckDB itself prints the person.
    raw_copy = _copy(damaged_buyers, tmp_path / "raw")
    raw = duckdb.connect(str(raw_copy))
    try:
        with pytest.raises(duckdb.FatalException) as dumped:
            raw.execute(f"INSERT OR REPLACE INTO buyers (id, full_name, phone, email) "
                        f"VALUES (77, '{_NAME}', '{_PHONE}', '{_EMAIL}')")
        assert "\nChunk:" in str(dumped.value) and _PHONE in str(dumped.value)
        with pytest.raises(duckdb.FatalException, match="invalidated") as echoed:
            raw.execute("SELECT 1")
        assert _PHONE in str(echoed.value)
    finally:
        raw.close()

    from core.models import Buyer

    sink = io.StringIO()
    handler = logging.StreamHandler(sink)          # a plain handler: no filter
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    sync_log = logging.getLogger("core.sync_service")
    sync_log.addHandler(handler)
    store = await _connect(path)
    try:
        for _ in range(2):                         # the retry meets it again
            try:
                await store.upsert_buyers([Buyer(id=77, full_name=_NAME,
                                                 phone=_PHONE, email=_EMAIL)])
            except Exception as e:  # noqa: BLE001 — the buyers step's handler
                sync_log.error(f"Buyer sync failed, buyers watermark not moved: "
                               f"{type(e).__name__}", exc_info=True)
        assert store.fatal_status()["kinds"] == {"index": 2}
    finally:
        sync_log.removeHandler(handler)
        await store.close()
    logged = sink.getvalue() + "\n".join(r.getMessage() for r in caplog.records)
    assert "Failed to delete all rows from index" in logged, logged
    for value in (_NAME, _PHONE, _EMAIL):
        assert value not in logged, f"{value!r} reached the log:\n{logged}"


@pytest.mark.asyncio
async def test_web_log_handler_cuts_a_dump_logged_inside_a_block(damaged_file, tmp_path):
    """What the store's exit cannot see: a handler that logs the FATAL inside
    its own block, before the exception reaches `connection()`. web's handler
    (`setup_logging`) cuts it from the message and from the traceback."""
    from core import observability

    root = logging.getLogger()
    saved = (list(root.handlers), root.level)
    observability.setup_logging(level="INFO")
    handler = root.handlers[-1]
    sink = io.StringIO()
    handler.setStream(sink)
    store = await _connect(_copy(damaged_file, tmp_path / "db"))
    try:
        async with store.connection() as conn:
            try:
                conn.execute(_FATAL_WRITE)
            except duckdb.FatalException as e:
                logging.getLogger("core.somewhere").error(f"write failed: {e}", exc_info=True)
                logging.getLogger("core.somewhere").warning("again: %s", e)
    finally:
        await store.close()
        root.handlers[:] = saved[0]
        root.setLevel(saved[1])
    logged = sink.getvalue()
    assert logged.count("Failed to delete all rows from index") >= 3, logged
    assert "Chunk" not in logged and "FLAT INTEGER" not in logged, logged


@pytest.mark.asyncio
async def test_a_migration_that_meets_a_fatal_publishes_no_row_value(
        damaged_file, tmp_path, monkeypatch):
    """A migration's error text goes to /api/health, which is public. One
    that met a short index — and every one after it, on the instance it
    invalidated — would have published DuckDB's dump of the rows."""
    from core import migrations

    path = _copy(damaged_file, tmp_path / "db")

    def meets_the_short_index(store):
        store._connection.execute(_FATAL_WRITE)

    def runs_after_it(store):
        store._connection.execute("SELECT 1")

    m0027 = next(m for m in migrations.MIGRATIONS if m.id == migrations.M0027_ID)
    monkeypatch.setattr(migrations, "MIGRATIONS", [
        migrations.Migration("t_fatal", migrations.ALWAYS, meets_the_short_index),
        migrations.Migration("t_echo", migrations.ALWAYS, runs_after_it),
        m0027,   # catches its own errors and records them, per sequence
    ])
    store = DuckDBStore(db_path=path)
    try:
        await store.connect()
        failed = {m["id"]: m["error"] for m in store._failed_migrations}
        assert "Failed to delete all rows from index" in failed["t_fatal"], failed
        assert "invalidated" in failed["t_echo"], failed
        assert "invalidated" in failed[migrations.M0027_ID], failed
        assert any(k.startswith("view:") for k in failed), failed   # built after them
        # What /api/health publishes, and what it would publish with the
        # ledger readable: neither carries a value of the rows.
        for text in (str(store._failed_migrations), str(store.schema_status())):
            assert "Chunk" not in text and "FLAT" not in text, text
    finally:
        await store.close()


def test_the_cut_leaves_other_chunks_alone():
    """The sync logs "Chunk 3: Fetching orders..." — not a row dump."""
    from core.observability import without_row_dump

    for line in ("Chunk 3: Fetching orders from 2026-01-01", "  Chunk 2: Saved 5 orders",
                 "Processing Chunk: 5 of 9"):
        assert without_row_dump(line) == line
    dumped = 'rows.\nChunk: Chunk - [4 Columns]\n- FLAT VARCHAR: 1 = [ +380671234567]'
    assert without_row_dump(dumped) == "rows."
    assert without_row_dump('error: "x. Chunk - [2 Columns]\n- FLAT') == 'error: "x. '


@pytest.mark.asyncio
async def test_a_caller_queued_behind_close_does_not_reopen_the_store(tmp_path, caplog):
    """Shutdown closes the store while a handler past its 504 or a job past
    its cancellation may be queued on the lock. Such a caller used to find no
    connection, take it for one a FATAL dropped, and open the file again: the
    schema and the migrations during shutdown, a write, and an instance
    nothing would ever close, after `close()`'s checkpoint."""
    path = tmp_path / "a.duckdb"
    store = await _connect(path)

    async def holder():
        async with store.connection():
            await asyncio.sleep(0.2)

    async def closer():
        await asyncio.sleep(0.01)
        await store.close()

    async def late_caller():
        await asyncio.sleep(0.05)
        async with store.connection() as conn:
            conn.execute("CREATE TABLE after_close (x INTEGER)")

    # Only what happens from here: where an earlier test left the level at
    # INFO, the open above has already logged its own "DuckDB connected".
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=duckdb_store.logger.name):
        results = await asyncio.gather(holder(), closer(), late_caller(),
                                       return_exceptions=True)
    assert isinstance(results[2], StoreClosedError), results
    assert store._connection is None and store._executor is None
    assert not [r for r in caplog.records if "DuckDB connected" in r.getMessage()]
    with pytest.raises(StoreClosedError):
        await store._fetch_one("SELECT 1")
    con = duckdb.connect(str(path), read_only=True)
    try:
        assert not con.execute("SELECT count(*) FROM duckdb_tables() "
                               "WHERE table_name = 'after_close'").fetchone()[0]
    finally:
        con.close()

    # An explicit connect() reopens it: scripts and a new lifespan do that.
    # And it is an open store again, not one still marked closed: a FATAL
    # that drops its instance is answered by opening the file on the next
    # use, as on any store — not by StoreClosedError.
    await store.connect()
    try:
        assert await store._fetch_one("SELECT 1") == (1,)
        async with store.connection() as conn:
            conn.execute("CREATE TABLE after_reopen (x INTEGER)")
            conn.execute("INSERT INTO after_reopen VALUES (1)")
            conn.execute("SET debug_checkpoint_abort = 'before_header'")
        with pytest.raises(duckdb.FatalException, match="checkpoint"):
            await store.checkpoint()
        assert store._connection is None
        assert await store._fetch_one("SELECT count(*) FROM after_reopen") == (1,)
    finally:
        await store.close()


# Long enough that a leftover query is unmistakable, interruptible at once.
_SLOW = "SELECT count(*) FROM range(3000000000) a WHERE (a.range * 7) % 13 = 3"


@pytest.mark.asyncio
async def test_a_cancelled_read_leaves_the_connection_before_the_lock(tmp_path):
    """A `_fetch_*` cancelled mid-query used to release the lock with its
    executor thread still inside `conn.execute()`. The next block that
    raised made `connection()` probe the instance on the event loop, which
    waited for that query: every request stalled for seconds. A timeout
    already interrupted and waited; a cancellation now does the same."""
    store = await _connect(tmp_path / "p.duckdb")
    try:
        read = asyncio.create_task(store._fetch_all(_SLOW, timeout=600))
        await asyncio.sleep(0.3)               # the query is on the executor thread
        read.cancel()
        cancelled_at = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await read
        # Interrupted, not waited out: on its own `_SLOW` runs for many
        # seconds, and a cancellation that only waited would hold the caller
        # — a job at shutdown — for all of them.
        assert time.monotonic() - cancelled_at < 2.0, "the cancelled query ran to its end"

        started = time.monotonic()
        with pytest.raises(ValueError):
            async with store.connection():
                raise ValueError("a caller's own error; the probe runs")
        assert time.monotonic() - started < 1.0, "the probe waited for the cancelled query"
        assert await store._fetch_one("SELECT 1") == (1,)
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
    _child(sys.argv[2], sys.argv[3])
