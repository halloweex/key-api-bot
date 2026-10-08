"""A writer killed before a checkpoint costs no index entry on the next open.

DuckDB 1.5.5: rows a killed process left in the WAL are replayed into every
`CREATE INDEX` index as entries the index has not bound yet, and DuckDB's own
checkpoint — `close()`, or `wal_autocheckpoint` — writes those indexes
*without* them unless something bound the index first. From then on a lookup
by the indexed column misses rows a scan sees, and any write that must take
such a row out of the index is a FatalException that invalidates the
instance. `core.duckdb_switch.open_file`, the one opener, CHECKPOINTs right
after the replay, which keeps every entry.

Everything here runs through the product: the schema `DuckDBStore.connect()`
builds (every index it creates, composite ones included), a child process
that opens the store the same way, writes a row into every indexed table and
two runs into the DQ journal, then SIGKILLs itself — an OOM kill, or a stop
that ran out of grace — and a restart through `DuckDBStore.connect()` and
`close()`, which is web coming back and the next deploy stopping it.

The judge is the write that must remove every row from every index:
`DELETE FROM t`, on a copy per table. A short index is the FATAL. And the
read the product makes: `fetch_run_issues` by run id, through an index.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from core import duckdb_store  # noqa: E402
from core.duckdb_store import DuckDBStore  # noqa: E402
from core.duckdb_switch import open_file  # noqa: E402

_ISSUES_PER_RUN = (2, 5)


# ─── rows for every indexed table, whatever its columns ─────────────────────

def _value(dtype: str, i: int):
    t = dtype.upper()
    if t in ("TINYINT", "UTINYINT"):
        return 100 + i
    if t in ("SMALLINT", "USMALLINT"):
        return 30000 + i
    if any(t.startswith(x) for x in ("INTEGER", "BIGINT", "HUGEINT", "UINTEGER", "UBIGINT", "INT")):
        return 50_000 + i
    if t.startswith("DECIMAL") or t in ("DOUBLE", "FLOAT", "REAL"):
        return 7 + i + 0.25
    if t == "BOOLEAN":
        return i % 2 == 0
    if t == "DATE":
        return date(2031, 1, 1) + timedelta(days=i)
    if t.startswith("TIMESTAMP"):
        return datetime(2031, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i)
    if t == "JSON":
        return json.dumps({"k": i})
    if t == "VARCHAR":
        return f"k-{i:03d}"
    return None


def _indexed_tables(con) -> list:
    """Tables carrying a CREATE INDEX index — the ones with SQL; PK and
    UNIQUE constraint indexes have none and do not lose entries."""
    return [r[0] for r in con.execute(
        "SELECT DISTINCT table_name FROM duckdb_indexes() "
        "WHERE schema_name = 'main' AND sql IS NOT NULL ORDER BY 1").fetchall()]


def _write_rows(con, tables, numbers) -> dict:
    errors = {}
    for table in tables:
        cols = con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE schema_name = 'main' AND table_name = ? ORDER BY column_index",
            [table]).fetchall()
        names = ", ".join(f'"{c}"' for c, _ in cols)
        marks = ", ".join("?" for _ in cols)
        try:
            con.executemany(f'INSERT INTO "{table}" ({names}) VALUES ({marks})',
                            [[_value(t, i) for _, t in cols] for i in numbers])
        except Exception as exc:  # noqa: BLE001 — reported per table
            errors[table] = f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
    return errors


def _dq_run(con, issues: int) -> int:
    from core.data_quality import IntegrityIssue, Severity, persist_run

    now = datetime.now(timezone.utc)
    return persist_run(
        con, started_at=now, ended_at=now, as_of=now,
        window_start=date.today(), window_end=date.today(),
        layer="integrity", discrepancies=[],
        issues=[IntegrityIssue(f"check_{k}", "silver.orders", Severity.WARN, 1, (5,))
                for k in range(issues)],
    )


async def _connect(path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=path)
    await store.connect()
    return store


# ─── the process the kernel kills ───────────────────────────────────────────

def _child(path: str) -> None:
    store = asyncio.run(_connect(Path(path)))
    con = store._connection
    # The journal first: connect() moved its id allocator above every id the
    # table holds, and the generic rows below take ids far above those.
    runs = [_dq_run(con, n) for n in _ISSUES_PER_RUN]
    errors = _write_rows(con, _indexed_tables(con), range(10, 13))
    Path(path + ".child.json").write_text(json.dumps({"errors": errors, "runs": runs}))
    # `store` is still referenced: a store dropped before the kill would
    # close, and checkpoint, first — which is not what a kill does.
    os.kill(os.getpid(), signal.SIGKILL)


@pytest.fixture(scope="module")
def killed_file(tmp_path_factory):
    """A production-shaped file — schema and older rows checkpointed — with
    the WAL of a writer killed mid-life on top."""
    path = tmp_path_factory.mktemp("killed") / "analytics.duckdb"

    async def seed():
        store = await _connect(path)
        try:
            con = store._connection
            tables = _indexed_tables(con)
            errors = _write_rows(con, tables, range(0, 2))
            for _ in range(3):
                _dq_run(con, 1)
        finally:
            await store.close()
        return tables, errors

    tables, errors = asyncio.run(seed())
    env = {k: v for k, v in os.environ.items() if k not in ("KS_PG_DSN", "KS_CH_URL")}
    env["KS_ALERTS_DISABLED"] = "1"
    rc = subprocess.run([sys.executable, __file__, "--child", str(path)],
                        cwd=REPO, env=env, timeout=120).returncode
    assert rc == -signal.SIGKILL, f"the child was meant to be killed, exited {rc}"
    child = json.loads(Path(str(path) + ".child.json").read_text())
    errors.update(child["errors"])
    # Not vacuous: every table that carries a CREATE INDEX index took rows on
    # both sides of the kill, and the kill left them in a WAL.
    assert len(tables) >= 20 and not errors, errors
    assert Path(f"{path}.wal").stat().st_size > 0
    return {"path": path, "tables": tables, "runs": child["runs"]}


def _copy(killed: dict, into: Path) -> Path:
    """The killed file and its WAL, as the next process would find them."""
    into.mkdir(parents=True, exist_ok=True)
    dest = into / "analytics.duckdb"
    shutil.copy2(killed["path"], dest)
    shutil.copy2(f"{killed['path']}.wal", f"{dest}.wal")
    return dest


def _fatal_tables(path: Path, tables, scratch: Path) -> list:
    """Tables on which `DELETE FROM t` is a FATAL — a short index."""
    fatal = []
    for table in tables:
        copy = scratch / f"judge-{table}.duckdb"
        shutil.copy2(path, copy)
        con = duckdb.connect(str(copy))
        try:
            con.execute(f'DELETE FROM "{table}"')
        except duckdb.FatalException:
            fatal.append(table)
        finally:
            con.close()
            copy.unlink()
    return fatal


def _run_issues_seen(path: Path, runs) -> list:
    from core.data_quality import fetch_run_issues

    con = duckdb.connect(str(path), read_only=True)
    try:
        return [len(fetch_run_issues(con, run)) for run in runs]
    finally:
        con.close()


def _judge(path: Path, killed: dict, scratch: Path):
    assert not Path(f"{path}.wal").exists(), "the restart closed and left a WAL"
    return _fatal_tables(path, killed["tables"], scratch), _run_issues_seen(path, killed["runs"])


def _another_process_opens(path: Path) -> str:
    """'opened', or why another process could not open `path` read-only —
    the compaction or a CLI, while this one still holds an instance."""
    code = ("import duckdb, sys\n"
            "duckdb.connect(sys.argv[1], read_only=True).close()\n"
            "print('opened')\n")
    out = subprocess.run([sys.executable, "-c", code, str(path)],
                         capture_output=True, text=True, timeout=120)
    return out.stdout.strip() or (out.stderr.strip().splitlines() or ["?"])[-1]


class _InterruptedCheckpoint:
    """A connection whose CHECKPOINT is interrupted before it applies
    anything — `con.interrupt()` landing on the guard, or Ctrl-C in a CLI
    that opens through the store. Measured on 1.5.5 with a 58 MB WAL and a
    real interrupt: InterruptException, the instance still valid, the WAL
    whole — and closing that instance as it is was the lossy checkpoint."""

    def __init__(self, con, interrupt):
        self._con, self._interrupt = con, interrupt

    def execute(self, sql, *args, **kwargs):
        if sql.strip().upper() == "CHECKPOINT":
            raise self._interrupt()
        return self._con.execute(sql, *args, **kwargs)

    def close(self):
        return self._con.close()


# ─── the tests ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_a_restart_through_the_store_keeps_every_index_entry(
        killed_file, tmp_path, caplog):
    path = _copy(killed_file, tmp_path / "db")
    with caplog.at_level(logging.WARNING, logger=duckdb_store.logger.name):
        store = await _connect(path)
        await store.close()
    fatal, seen = _judge(path, killed_file, tmp_path)
    assert fatal == [], f"short indexes after a guarded restart: {fatal}"
    assert seen == list(_ISSUES_PER_RUN)
    # The trigger is named: a WAL at open means the last writer did not close.
    assert any("replayed" in r.getMessage() and "WAL" in r.getMessage()
               for r in caplog.records), [r.getMessage() for r in caplog.records]


@pytest.mark.asyncio
async def test_without_the_guard_the_same_restart_loses_them(killed_file, tmp_path):
    """The defect, pinned on the DuckDB this repository pins. If an upgrade
    makes this fail, DuckDB fixed it: re-measure before deleting the guard,
    do not just delete this test."""
    path = _copy(killed_file, tmp_path / "db")
    con = duckdb.connect(str(path))     # what DuckDBStore.connect() did before
    con.close()                         # ...and the next stop
    fatal, seen = _judge(path, killed_file, tmp_path)
    assert fatal == killed_file["tables"], "DuckDB no longer drops replayed entries?"
    assert seen == [0 for _ in _ISSUES_PER_RUN]


class _FailingSetting:
    """A connection on which every SET fails — what DuckDB does with a value
    it refuses. The guard's CHECKPOINT passes through."""

    def __init__(self, con):
        self._con = con

    def execute(self, sql, *args, **kwargs):
        if sql.strip().upper().startswith("SET "):
            raise duckdb.InvalidInputException("a setting DuckDB refuses")
        return self._con.execute(sql, *args, **kwargs)

    def close(self):
        return self._con.close()


@pytest.mark.asyncio
async def test_a_setting_that_fails_after_the_replay_costs_nothing(
        killed_file, tmp_path, monkeypatch):
    """Why the guard is the first statement. `connect()` closes the instance
    when anything after the open raises, and closing a valid instance is
    DuckDB's lossy checkpoint — so a failing SET must find the WAL already
    checkpointed. The SETs after the open (the memory limit moved into the
    open itself, below), each refused."""
    path = _copy(killed_file, tmp_path / "db")
    real_connect = duckdb.connect
    monkeypatch.setattr(duckdb, "connect", lambda database, *a, **k: _FailingSetting(
        real_connect(database, *a, **k)))
    store = DuckDBStore(db_path=path)
    with pytest.raises(duckdb.Error, match="refuses"):
        await store.connect()
    monkeypatch.setattr(duckdb, "connect", real_connect)
    assert store._connection is None
    fatal, seen = _judge(path, killed_file, tmp_path)
    assert fatal == [] and seen == list(_ISSUES_PER_RUN)


@pytest.mark.asyncio
async def test_a_memory_limit_duckdb_refuses_costs_nothing_either(
        killed_file, tmp_path, monkeypatch):
    """The limit is part of the open now, and a value DuckDB refuses there
    raises before any instance exists: nothing replayed, nothing to close,
    the WAL whole for the next open."""
    path = _copy(killed_file, tmp_path / "db")
    real_limit = duckdb_store._memory_limit
    monkeypatch.setattr(duckdb_store, "_memory_limit", lambda: "a lot")
    store = DuckDBStore(db_path=path)
    with pytest.raises(duckdb.Error):
        await store.connect()
    assert store._connection is None
    assert Path(f"{path}.wal").stat().st_size > 0, "refused, yet something replayed it"
    monkeypatch.setattr(duckdb_store, "_memory_limit", real_limit)
    await (await _connect(path)).close()
    fatal, seen = _judge(path, killed_file, tmp_path)
    assert fatal == [] and seen == list(_ISSUES_PER_RUN)


class _LimitAtCheckpoint:
    """The store's connection, recording the memory limit in force when the
    guard's CHECKPOINT runs."""

    def __init__(self, con, seen):
        self._con, self._seen = con, seen

    def execute(self, sql, *args, **kwargs):
        if sql.strip().upper() == "CHECKPOINT":
            self._seen.append(self._con.execute(
                "SELECT current_setting('memory_limit')").fetchone()[0])
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


@pytest.mark.asyncio
async def test_the_replay_and_the_guard_run_under_the_stores_memory_limit(
        killed_file, tmp_path, monkeypatch):
    """After a kill — an OOM kill likeliest, and the largest WAL — the open
    replays it and the guard checkpoints it before any SET can run. As a SET,
    the limit bounded neither: both ran under DuckDB's default, 80% of the
    memory it detects, ~5.6 GB in web's 7 GB container instead of its 4 GB
    (batch-E review). Mutation: SET the limit after the open again."""
    path = _copy(killed_file, tmp_path / "db")
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "300MB")
    seen: list = []
    real_connect = duckdb.connect
    monkeypatch.setattr(duckdb, "connect", lambda database, *a, **k: _LimitAtCheckpoint(
        real_connect(database, *a, **k), seen))
    store = DuckDBStore(db_path=path)
    await store.connect()
    try:
        limit = store._connection.execute(
            "SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        await store.close()
        monkeypatch.setattr(duckdb, "connect", real_connect)
    assert seen and seen[0] == limit, f"the guard ran under {seen}, the store under {limit}"
    fatal, seen_issues = _judge(path, killed_file, tmp_path)
    assert fatal == [] and seen_issues == list(_ISSUES_PER_RUN)


@pytest.mark.parametrize("abort", ["before_truncate", "before_header", "after_free_list_write"])
def test_a_guard_checkpoint_that_fails_leaves_the_wal_for_the_next_open(
        killed_file, tmp_path, monkeypatch, abort):
    """A CHECKPOINT that fails is FATAL in 1.5.5: the open raises, the
    invalidated instance writes nothing on close, the WAL stays — and the next
    open replays and checkpoints it whole."""
    path = _copy(killed_file, tmp_path / "db")
    real_connect = duckdb.connect

    def aborting(database, *args, **kwargs):
        kwargs["config"] = {**kwargs.get("config", {}), "debug_checkpoint_abort": abort}
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", aborting)
    with pytest.raises(duckdb.Error) as failed:
        open_file(path)
    monkeypatch.undo()
    assert Path(f"{path}.wal").stat().st_size > 0, "the failed guard lost the WAL"
    # Closed, not left to the traceback: while `failed` lives, an instance
    # still open would hold the file's lock against every other process.
    assert _another_process_opens(path) == "opened"
    del failed

    con = open_file(path)
    con.close()
    fatal, seen = _judge(path, killed_file, tmp_path)
    assert fatal == [] and seen == list(_ISSUES_PER_RUN)


@pytest.mark.parametrize("interrupt", [
    pytest.param(lambda: duckdb.InterruptException("INTERRUPT Error: Interrupted!"),
                 id="InterruptException"),
    pytest.param(KeyboardInterrupt, id="KeyboardInterrupt"),
])
def test_an_interrupted_guard_checkpoint_leaves_the_wal_for_the_next_open(
        killed_file, tmp_path, monkeypatch, interrupt):
    """Not every failed guard is FATAL. An interrupt leaves the instance
    valid with the WAL unapplied, and closing a valid instance is DuckDB's
    own, lossy checkpoint — the defect itself. `disable_checkpoint_on_shutdown`
    is what keeps that close from writing; without it every CREATE INDEX
    index comes back short."""
    path = _copy(killed_file, tmp_path / "db")
    real_connect = duckdb.connect
    monkeypatch.setattr(duckdb, "connect", lambda database, *a, **k: _InterruptedCheckpoint(
        real_connect(database, *a, **k), interrupt))
    with pytest.raises((duckdb.InterruptException, KeyboardInterrupt)) as failed:
        open_file(path)
    monkeypatch.undo()
    assert Path(f"{path}.wal").stat().st_size > 0, "the interrupted guard's close checkpointed"
    assert _another_process_opens(path) == "opened"
    del failed

    con = open_file(path)
    con.close()
    fatal, seen = _judge(path, killed_file, tmp_path)
    assert fatal == [] and seen == list(_ISSUES_PER_RUN)


@pytest.mark.asyncio
async def test_a_clean_start_says_nothing(tmp_path, caplog):
    path = tmp_path / "clean.duckdb"
    await (await _connect(path)).close()      # the schema, closed cleanly
    assert not Path(f"{path}.wal").exists()
    with caplog.at_level(logging.WARNING, logger=duckdb_store.logger.name):
        store = await _connect(path)
        await store.close()
    assert not [r for r in caplog.records if "replayed" in r.getMessage()]


# ─── the WAL nothing replays (not fixed; CLAUDE.md names these cases) ───────

_TS = "CREATE TABLE a (id INTEGER, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, v INTEGER)"


@pytest.mark.parametrize("ddl, alter, replays", [
    # A column-level ALTER of a table that, after it, has a DEFAULT calling a
    # function: the replay cannot bind it ("GetDefaultDatabase with no
    # default database set"), read-only included, guard or not.
    (_TS, "ALTER TABLE a ADD COLUMN x INTEGER", False),
    (_TS, "ALTER TABLE a ADD COLUMN IF NOT EXISTS x INTEGER", False),
    (_TS, "ALTER TABLE a DROP COLUMN v", False),
    (_TS, "ALTER TABLE a RENAME COLUMN v TO w", False),
    (_TS, "ALTER TABLE a ALTER COLUMN v TYPE BIGINT", False),
    (_TS, "ALTER TABLE a ALTER COLUMN v SET DEFAULT 3", False),
    (_TS, "ALTER TABLE a ALTER COLUMN v DROP DEFAULT", False),
    (_TS, "ALTER TABLE a ALTER COLUMN v SET NOT NULL", False),
    (_TS, "ALTER TABLE a ALTER COLUMN v DROP NOT NULL", False),
    ("CREATE SEQUENCE s; CREATE TABLE a (id INTEGER DEFAULT nextval('s'), v INTEGER)",
     "ALTER TABLE a ADD COLUMN x INTEGER", False),
    ("CREATE SEQUENCE s; CREATE TABLE a (id INTEGER, v INTEGER)",
     "ALTER TABLE a ADD COLUMN x INTEGER DEFAULT nextval('s')", False),
    # ...and what does replay.
    (_TS, "ALTER TABLE a RENAME TO b", True),
    (_TS, "ALTER TABLE a ALTER COLUMN ts DROP DEFAULT", True),
    (_TS, "ALTER TABLE a ADD COLUMN IF NOT EXISTS v INTEGER", True),   # writes nothing
    (_TS, "CREATE INDEX i ON a(v)", True),
    (_TS, "INSERT INTO a (id) VALUES (1)", True),
    ("CREATE TABLE a (id INTEGER DEFAULT 5, v INTEGER)", "ALTER TABLE a ADD COLUMN x INTEGER", True),
])
def test_which_wal_cannot_be_replayed(tmp_path, ddl, alter, replays):
    """Pinned on the DuckDB this repository pins: the cases CLAUDE.md lists
    under "Not fixed: a WAL that cannot be replayed at all"."""
    path = tmp_path / "a.duckdb"
    con = duckdb.connect(str(path))
    con.execute(ddl)
    con.execute("CHECKPOINT")
    con.close()
    code = ("import duckdb, os, signal, sys\n"
            "c = duckdb.connect(sys.argv[1])\n"
            "c.execute(sys.argv[2])\n"
            "os.kill(os.getpid(), signal.SIGKILL)\n")
    rc = subprocess.run([sys.executable, "-c", code, str(path), alter], timeout=60).returncode
    assert rc == -signal.SIGKILL, rc
    for read_only in (True, False):
        if replays:
            duckdb.connect(str(path), read_only=read_only).close()
        else:
            with pytest.raises(duckdb.InternalException, match="GetDefaultDatabase"):
                duckdb.connect(str(path), read_only=read_only)


if __name__ == "__main__" and sys.argv[1:2] == ["--child"]:
    _child(sys.argv[2])
