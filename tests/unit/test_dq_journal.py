"""Chain 9: the quality journal behind `KS_WRITE_DQ_JOURNAL`, shadowed in DuckDB.

The parts that need no database. What needs a real Postgres — the allocator
under two concurrent runs, every column equal across the two stores, the
Postgres readers rendering exactly as DuckDB's — is in
`tests/integration/test_dq_journal_pg.py`.

Every guard names, in its docstring, the mutation it exists to catch.
"""
from __future__ import annotations

import ast
import pathlib
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from core import dq_journal, pg_dq_journal_write as chain, shadow_writes
from core.data_quality import (
    Discrepancy, DiscrepancyClass, IntegrityIssue, Severity, fetch_latest_run,
    journal_rows, run_values,
)
from core.duckdb_store import DuckDBStore

ROOT = pathlib.Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 1, 7, 30, tzinfo=timezone.utc)
DAY = date(2026, 10, 1)


@pytest.fixture
def flag(monkeypatch):
    """No chain's flag, and nothing latched — the marker dir is the test's."""
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    shadow_writes.reset()

    def set_(value):
        if value is None:
            monkeypatch.delenv(chain.WRITE_ENV, raising=False)
        else:
            monkeypatch.setenv(chain.WRITE_ENV, value)
    yield set_
    shadow_writes.reset()


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "journal.duckdb")
    await s.connect()
    yield s
    await s.close()


def _issue(name="pk_uniqueness_orders", severity=Severity.WARN, count=3):
    return IntegrityIssue(check_name=name, table_name="orders", severity=severity,
                          count=count, sample_ids=(1, 2), description="d")


def _diff():
    return Discrepancy(month="2026-09", source_id=4,
                       diff_class=DiscrepancyClass.MISSING_IN_DK, field="orders",
                       dk_value=10.0, kc_value=12.0, severity=Severity.CRITICAL,
                       order_ids=(7, 8))


def _kwargs(**over):
    kw = dict(started_at=NOW - timedelta(seconds=3), ended_at=NOW, as_of=NOW,
              window_start=DAY, window_end=DAY, layer="integrity",
              issues=[_issue()], discrepancies=[_diff()])
    kw.update(over)
    return kw


def _no_postgres(monkeypatch):
    """Any reach for a pool fails the test — the duckdb branch asks Postgres
    nothing, not even for a pool."""
    boom = AsyncMock(side_effect=AssertionError("the duckdb branch asked for a pool"))
    monkeypatch.setattr("core.pg.get_pool", boom)
    monkeypatch.setattr("core.pg.require_revision", boom)
    return boom


def _count_connections(store):
    calls = []
    real = store.connection

    def connection():
        calls.append(1)
        return real()

    store.connection = connection
    return calls


# ─── The one door ─────────────────────────────────────────────────────────────

# The DuckDB journal's writer and readers. A call to any of these outside the
# modules allowed below goes round the router — and with the chain on
# Postgres, writes or reads the store that is no longer the writer.
_DOOR_FUNCS = {
    "persist_run", "insert_journal_rows", "fetch_latest_run", "fetch_run_issues",
    "fetch_run_diffs", "fetch_baseline_run", "fetch_previous_run",
    "fetch_latest_run_by_id", "fetch_last_success_ages",
}
_ROUTER = {"core/data_quality.py", "core/dq_journal.py", "core/pg_dq_journal_write.py"}
# Each exemption, with what makes it one:
# - `fetch_last_success_ages` inside a connection two callers already hold:
#   `get_stats` (whose answer `/api/health` replaces through
#   `dq_journal.reads_postgres`) and the catch-up (which replaces it after
#   the block). A test below holds each replacement to being there.
_EXEMPT_CALLS = {
    ("core/duckdb_store.py", "fetch_last_success_ages"),
    ("core/scheduler.py", "fetch_last_success_ages"),
}
# SQL naming the journal: the router, the stores' own machinery (DDL,
# migrations, the hourly copy and its comparison, the copy-back, the snapshot
# validation), the Postgres-side readers that already read the writer (chain
# 1's preflight, the standing invariants).
_SQL_ALLOWED = _ROUTER | {
    "core/migrations.py", "core/mirror_reconciliation.py", "core/pg_operational.py",
    "core/chain_transfer.py", "core/snapshot_validation.py",
    "core/pg_inventory_write.py", "core/pg_chain_invariants.py",
}
_JOURNAL_SQL = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|FROM)\b[\s\S]*\b"
    r"(data_quality_runs|data_quality_issues|data_quality_diffs)\b",
    re.IGNORECASE)
_MARKER = "dq_digest_last_sent"


def _py(*dirs):
    for d in dirs:
        for path in sorted((ROOT / d).rglob("*.py")):
            yield path, str(path.relative_to(ROOT))


def _door_calls(tree):
    for n in ast.walk(tree):
        if isinstance(n, ast.Call):
            name = getattr(n.func, "id", getattr(n.func, "attr", ""))
            if name in _DOOR_FUNCS:
                yield name, n.lineno


def _docstrings(tree) -> set:
    """`id()` of every docstring — prose about the journal is not a reader."""
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                          ast.AsyncFunctionDef)) and n.body:
            first = n.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                out.add(id(first.value))
    return out


def _journal_strings(tree):
    docs = _docstrings(tree)
    for n in ast.walk(tree):
        if (isinstance(n, ast.Constant) and isinstance(n.value, str)
                and id(n) not in docs):
            if _JOURNAL_SQL.search(n.value):
                yield "sql", n.lineno
            if _MARKER in n.value:
                yield "marker", n.lineno


def _walk_offenders():
    offenders = []
    for path, rel in _py("core", "web", "bot", "scripts"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if rel not in _ROUTER:
            for name, line in _door_calls(tree):
                if (rel, name) not in _EXEMPT_CALLS:
                    offenders.append(f"{rel}:{line} calls {name}")
        for kind, line in _journal_strings(tree):
            if kind == "sql" and rel not in _SQL_ALLOWED:
                offenders.append(f"{rel}:{line} spells SQL over the journal")
            if kind == "marker" and rel not in _ROUTER:
                offenders.append(f"{rel}:{line} spells the digest marker's key")
    return offenders


class TestTheOneDoor:
    def test_nothing_goes_round_the_router(self):
        """Mutation: `persist_run(conn, ...)` back in `_run_dq_integrity`, or
        `SELECT ... FROM data_quality_runs` added to `web/routes/api/health.py`
        — under `postgres` either writes or reads the store that stopped being
        the journal's writer, and nothing else would notice."""
        assert _walk_offenders() == []

    def test_the_walk_sees_a_call_and_a_statement(self):
        tree = ast.parse(
            "def f(conn):\n"
            "    persist_run(conn, layer='x')\n"
            "    conn.execute('SELECT run_id FROM data_quality_runs')\n"
            "    k = 'dq_digest_last_sent'\n")
        assert [n for n, _ in _door_calls(tree)] == ["persist_run"]
        assert {k for k, _ in _journal_strings(tree)} == {"sql", "marker"}

    def test_each_exemption_replaces_duckdbs_answer_under_postgres(self):
        """The two callers allowed to read DuckDB's ages in place must swap in
        Postgres's while the chain writes there. Mutation: drop either swap."""
        import inspect

        from core import scheduler
        from web.routes.api import health

        catchup = inspect.getsource(scheduler.BackgroundScheduler._schedule_catchup_runs)
        assert "dq_journal.reads_postgres()" in catchup
        assert "dq_journal.last_success_ages_pg()" in catchup
        assert "_journal_ages(data_quality)" in inspect.getsource(health.health_check)

    def test_one_spelling_of_the_marker_key(self):
        from core import scheduler

        assert (scheduler.DQ_DIGEST_LAST_SENT_KEY == dq_journal.DIGEST_MARKER_KEY
                == chain.DIGEST_MARKER_KEY == chain.CHAIN_MARKER_KEYS[0])


# ─── duckdb: the default, byte for byte ──────────────────────────────────────


class TestUnderDuckdbNothingChanged:
    @pytest.mark.asyncio
    async def test_one_connection_the_same_rows_and_no_postgres(
            self, flag, store, monkeypatch):
        """Mutation: the router reaches for a pool under `duckdb`, or opens a
        second connection — every check job would then depend on Postgres
        whose flag nobody switched."""
        flag(None)
        pool = _no_postgres(monkeypatch)
        calls = _count_connections(store)
        run_id = await dq_journal.journal_run(store, **_kwargs())
        assert calls == [1] and not pool.await_count
        async with store.connection() as conn:
            run = fetch_latest_run(conn, layer="integrity")
            n_issues = conn.execute("SELECT COUNT(*) FROM data_quality_issues "
                                    "WHERE run_id = ?", [run_id]).fetchone()[0]
            n_diffs = conn.execute("SELECT COUNT(*) FROM data_quality_diffs "
                                   "WHERE run_id = ?", [run_id]).fetchone()[0]
        assert run["run_id"] == run_id and run["status"] == "CRITICAL"
        assert (n_issues, n_diffs) == (1, 1)
        assert not chain.chain_latch.marker_path(chain.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_the_beat_is_written_where_it_always_was(self, flag, store, monkeypatch):
        flag(None)
        _no_postgres(monkeypatch)
        await dq_journal.mark_digest_sent(store, NOW.isoformat())
        async with store.connection() as conn:
            row = conn.execute("SELECT value FROM sync_metadata WHERE key = ?",
                               [dq_journal.DIGEST_MARKER_KEY]).fetchone()
        assert row[0] == NOW.isoformat()

    @pytest.mark.asyncio
    async def test_the_digest_reads_duckdb_on_one_connection(self, flag, store, monkeypatch):
        flag(None)
        _no_postgres(monkeypatch)
        await dq_journal.journal_run(store, **_kwargs())
        calls = _count_connections(store)
        last, sections = await dq_journal.digest_inputs(store, ("integrity",), NOW)
        assert calls == [1] and last is None
        assert sections[0].run["layer"] == "integrity"
        assert [i["check_name"] for i in sections[0].issues] == ["pk_uniqueness_orders"]

    def test_a_typo_refuses_the_write_and_keeps_the_reads(self, flag):
        """OD-18 (a) for the writer; chain 7a's rule for the readers — a flag
        about writes must not take every layer age down. Mutation: let the
        readers raise on the typo."""
        flag("postgrse")
        with pytest.raises(RuntimeError):
            dq_journal.writes_postgres()
        assert dq_journal.reads_postgres() is False


# ─── postgres: the writer, then the shadow ───────────────────────────────────


class _Recorder:
    def __init__(self):
        self.events = []


def _fake_chain_writer(monkeypatch, rec, *, fail=None, run_id=41):
    async def persist_run(values, issues, discrepancies):
        rec.events.append("postgres")
        if fail:
            raise fail
        return journal_rows(run_id, values, issues, discrepancies)

    monkeypatch.setattr(chain, "persist_run", persist_run)


def _record_duckdb(store, rec):
    real = store.connection

    def connection():
        rec.events.append("duckdb")
        return real()

    store.connection = connection


class TestUnderPostgres:
    @pytest.mark.asyncio
    async def test_postgres_commits_first_then_duckdb_is_handed_the_same_rows(
            self, flag, store, monkeypatch):
        """Mutation: write DuckDB first — a Postgres failure would then leave a
        run only DuckDB holds, the one state `compare_shadow` calls a defect."""
        flag("postgres")
        rec = _Recorder()
        _fake_chain_writer(monkeypatch, rec, run_id=41)
        _record_duckdb(store, rec)
        assert await dq_journal.journal_run(store, **_kwargs()) == 41
        assert rec.events == ["postgres", "duckdb"]
        async with store.connection() as conn:
            assert fetch_latest_run(conn)["run_id"] == 41

    @pytest.mark.asyncio
    async def test_a_postgres_failure_raises_and_writes_nothing_to_duckdb(
            self, flag, store, monkeypatch):
        """Mutation: fall back to DuckDB on a Postgres error."""
        flag("postgres")
        rec = _Recorder()
        _fake_chain_writer(monkeypatch, rec, fail=ConnectionRefusedError("down"))
        _record_duckdb(store, rec)
        with pytest.raises(ConnectionRefusedError):
            await dq_journal.journal_run(store, **_kwargs())
        assert rec.events == ["postgres"]

    @pytest.mark.asyncio
    async def test_a_duckdb_failure_is_counted_and_the_id_still_returned(
            self, flag, store, monkeypatch):
        """Mutation: re-raise the shadow's failure — the run is in Postgres,
        and the job would log "persist failed" and page without its run id."""
        flag("postgres")
        rec = _Recorder()
        _fake_chain_writer(monkeypatch, rec, run_id=5)
        # A run DuckDB already holds under id 5: the shadow's INSERT collides.
        async with store.connection() as conn:
            conn.execute("INSERT INTO data_quality_runs (run_id, started_at, as_of, "
                         "window_start, window_end, layer, status) VALUES "
                         "(5, ?, ?, ?, ?, 'x', 'PASS')", [NOW, NOW, DAY, DAY])
        assert await dq_journal.journal_run(store, **_kwargs()) == 5
        failures = shadow_writes.failures_of(chain.CHAIN)
        assert failures["count"] == 1 and failures["last_error_class"]

    @pytest.mark.asyncio
    async def test_a_naive_timestamp_is_refused_before_the_latch_and_the_pool(
            self, flag, monkeypatch):
        """DuckDB reads a naive stamp as Kyiv local time and asyncpg as UTC, so
        one value would land three hours apart in the two stores. Mutation:
        accept it — or refuse it only after `_latch()`, which spends a
        rollback on a write that never happens."""
        flag("postgres")
        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        monkeypatch.setattr("core.pg.get_pool", pool)
        naive = datetime(2026, 10, 1, 7, 0)
        values = run_values(**{**_kwargs(), "started_at": naive,
                               "ended_at": naive, "as_of": naive})
        with pytest.raises(ValueError, match="naive"):
            await chain.persist_run(values, [], [])
        assert not pool.await_count
        assert not chain.chain_latch.marker_path(chain.CHAIN).exists()

    def test_both_stores_are_handed_floats_and_ints(self):
        """asyncpg's float8 and DuckDB's DOUBLE read the same float; a Decimal
        from a rollup would reach one of them as a Decimal. Mutation: hand the
        raw value on."""
        from decimal import Decimal

        d = Discrepancy(month="2026-09", source_id=4,
                        diff_class=DiscrepancyClass.VALUE_MISMATCH,
                        field="revenue", dk_value=Decimal("10.5"),
                        kc_value=Decimal("11"), order_ids=())
        values = run_values(**_kwargs(discrepancies=[d], issues=[]))
        rows = journal_rows(9, values, [], [d])
        (diff,) = rows.diffs
        assert type(diff[5]) is float and type(diff[6]) is float
        assert diff[8] is None                      # no ids: NULL, not '[]'
        assert rows.run[0] == 9 and rows.run_id == 9

    @pytest.mark.asyncio
    async def test_the_beat_goes_to_postgres_then_duckdb(self, flag, store, monkeypatch):
        flag("postgres")
        rec = _Recorder()

        async def set_digest_marker(value):
            rec.events.append(("postgres", value))

        monkeypatch.setattr(chain, "set_digest_marker", set_digest_marker)
        _record_duckdb(store, rec)
        await dq_journal.mark_digest_sent(store, "2026-10-01T06:00:00+00:00")
        assert rec.events == [("postgres", "2026-10-01T06:00:00+00:00"), "duckdb"]
        async with store.connection() as conn:
            assert conn.execute("SELECT value FROM sync_metadata WHERE key = ?",
                                [dq_journal.DIGEST_MARKER_KEY]).fetchone()[0] == \
                "2026-10-01T06:00:00+00:00"


# ─── Readers follow the writer ───────────────────────────────────────────────


PG_RUN = {"run_id": 900, "started_at": "2026-10-01T10:00:00+03:00",
          "ended_at": "2026-10-01T10:00:03+03:00", "as_of": None,
          "window_start": "2026-10-01", "window_end": "2026-10-01",
          "layer": "integrity", "status": "WARN", "integrity_issues_count": 1,
          "discrepancies_count": 0, "critical_count": 0, "warn_count": 1,
          "api_calls_used": 0, "duration_ms": 3000, "error_message": None}
PG_ISSUES = [{"check_name": "from_postgres", "table_name": "t", "severity": "WARN",
              "count": 1, "sample_ids": [], "description": "pg"}]


@pytest.fixture
def pg_journal(monkeypatch):
    """The chain's Postgres readers, answering from a fixed journal that
    DuckDB does not hold — so a reader that read DuckDB is visible."""
    state = {"marker": "2026-09-30T06:00:00+00:00", "raise": None, "reads": 0}

    @asynccontextmanager
    async def reading():
        state["reads"] += 1
        if state["raise"]:
            raise state["raise"]
        yield object()

    async def latest_run(conn, layer=None):
        return dict(PG_RUN, layer=layer or "integrity", run_id=900)

    async def baseline_run(conn, layer, *, sent_at, before_run_id):
        state["sent_at"] = sent_at
        return dict(PG_RUN, run_id=899)

    async def run_issues(conn, run_id, limit=100):
        return list(PG_ISSUES)

    async def run_diffs(conn, run_id, limit=100):
        return []

    async def last_success_ages(conn, layers):
        return {layer: {"last_success_at": "pg", "age_seconds": 7} for layer in layers}

    async def digest_marker(conn):
        return state["marker"]

    for name, fn in dict(reading=reading, latest_run=latest_run,
                         baseline_run=baseline_run, run_issues=run_issues,
                         run_diffs=run_diffs, last_success_ages=last_success_ages,
                         digest_marker=digest_marker).items():
        monkeypatch.setattr(chain, name, fn)
    return state


class TestReadersFollowTheWriter:
    @pytest.mark.asyncio
    async def test_the_digest_reads_postgres(self, flag, store, pg_journal):
        """Mutation: leave the digest on DuckDB — the morning message would
        report the journal of a store that stopped being written."""
        flag("postgres")
        last, sections = await dq_journal.digest_inputs(store, ("integrity",), NOW)
        assert last == datetime.fromisoformat(pg_journal["marker"])
        assert sections[0].run["run_id"] == 900
        assert [i["check_name"] for i in sections[0].issues] == ["from_postgres"]
        assert pg_journal["sent_at"] == last

    @pytest.mark.asyncio
    async def test_the_data_quality_endpoint_reads_postgres(self, flag, store, pg_journal):
        flag("postgres")
        body = await dq_journal.health_runs(store)
        assert body["integrity"]["last_run"]["run_id"] == 900
        assert body["integrity"]["issues"] == PG_ISSUES

    @pytest.mark.asyncio
    async def test_the_canarys_ages_come_from_postgres(self, flag, pg_journal):
        """Mutation: keep `get_stats`' DuckDB ages under postgres — the canary
        would judge a journal nobody writes first."""
        from web.routes.api import health

        flag("postgres")
        health._journal_ages_cache.update(data=None, expires_at=0)
        ages = await health._journal_ages({"integrity": {"age_seconds": 999}})
        assert ages["integrity"] == {"last_success_at": "pg", "age_seconds": 7}

    @pytest.mark.asyncio
    async def test_under_duckdb_the_canary_keeps_duckdbs_ages(self, flag, pg_journal):
        from web.routes.api import health

        flag(None)
        mine = {"integrity": {"age_seconds": 999}}
        assert await health._journal_ages(mine) is mine
        assert pg_journal["reads"] == 0

    @pytest.mark.asyncio
    async def test_a_latched_chain_reads_postgres_whatever_the_flag(
            self, flag, store, pg_journal):
        flag("duckdb")
        chain.chain_latch.latch(chain.CHAIN)
        try:
            assert (await dq_journal.health_runs(store))["integrity"]["last_run"]["run_id"] == 900
        finally:
            chain.chain_latch.release(chain.CHAIN)


class TestNoFallback:
    @pytest.mark.asyncio
    async def test_an_unreachable_postgres_fails_the_digest(self, flag, store, pg_journal):
        """Mutation: wrap the Postgres read in a DuckDB fallback — the journal's
        writer failing would then read as "journal fine"."""
        flag("postgres")
        pg_journal["raise"] = ConnectionRefusedError("down")
        with pytest.raises(ConnectionRefusedError):
            await dq_journal.digest_inputs(store, ("integrity",), NOW)
        with pytest.raises(ConnectionRefusedError):
            await dq_journal.health_runs(store)

    @pytest.mark.asyncio
    async def test_the_canary_gets_no_block_rather_than_duckdbs(self, flag, pg_journal):
        from web.routes.api import health

        flag("postgres")
        health._journal_ages_cache.update(data=None, expires_at=0)
        pg_journal["raise"] = ConnectionRefusedError("down")
        assert await health._journal_ages({"integrity": {"age_seconds": 1}}) is None

    @pytest.mark.asyncio
    async def test_a_failure_is_not_cached(self, flag, pg_journal):
        from web.routes.api import health

        flag("postgres")
        health._journal_ages_cache.update(data=None, expires_at=0)
        pg_journal["raise"] = ConnectionRefusedError("down")
        assert await health._journal_ages({}) is None
        pg_journal["raise"] = None
        assert (await health._journal_ages({}))["integrity"]["age_seconds"] == 7


class TestTheBeatIsInherited:
    @pytest.mark.asyncio
    async def test_absent_in_postgres_it_is_duckdbs(self, flag, store, pg_journal):
        """The first digest after a flip would otherwise restate every
        standing finding as new. Mutation: no inherit."""
        flag("postgres")
        pg_journal["marker"] = None
        async with store.connection() as conn:
            conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                         "VALUES (?, ?, CURRENT_TIMESTAMP)",
                         [dq_journal.DIGEST_MARKER_KEY, "2026-09-29T06:00:00+00:00"])
        last, _ = await dq_journal.digest_inputs(store, ("integrity",), NOW)
        assert last == datetime(2026, 9, 29, 6, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_present_in_postgres_it_wins(self, flag, store, pg_journal):
        flag("postgres")
        async with store.connection() as conn:
            conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                         "VALUES (?, ?, CURRENT_TIMESTAMP)",
                         [dq_journal.DIGEST_MARKER_KEY, "2026-09-01T06:00:00+00:00"])
        last, _ = await dq_journal.digest_inputs(store, ("integrity",), NOW)
        assert last == datetime.fromisoformat(pg_journal["marker"])


# ─── Rendering: the Postgres branch prints what DuckDB printed ───────────────


class TestOneRendering:
    @pytest.mark.asyncio
    async def test_a_postgres_row_shapes_exactly_as_duckdbs(self, store, monkeypatch):
        """asyncpg hands back UTC and DuckDB its session's zone; the digest
        prints `started_at[:16]`, so an unconverted Postgres branch would date
        every run three hours early in Kyiv. Mutation: drop the conversion in
        `_local`."""
        from zoneinfo import ZoneInfo

        from core.data_quality import run_dict

        monkeypatch.setattr(chain, "_ZONE", ZoneInfo("Europe/Kyiv"))
        values = run_values(**_kwargs())
        rows = journal_rows(3, values, [], [])
        async with store.connection() as conn:
            conn.execute("SET TimeZone = 'Europe/Kyiv'")
            from core.data_quality import insert_journal_rows

            insert_journal_rows(conn, rows)
            duck = fetch_latest_run(conn)
        # What asyncpg would hand back: the same instants, in UTC.
        pg_row = tuple(v.astimezone(timezone.utc) if isinstance(v, datetime) else v
                       for v in rows.run)
        assert run_dict(chain._local(pg_row)) == duck
        assert duck["started_at"].endswith("+03:00")


# ─── The allocator's lock ────────────────────────────────────────────────────


class TestTheRunIdLock:
    def test_no_other_lock_key_in_core_is_the_same(self):
        """Mutation: reuse chain 1's `0x6B73_0000_0000_0001` — a DQ run would
        then wait behind a stock rebuild, and the other way round."""
        keys = {}
        for path, rel in _py("core"):
            for n in ast.parse(path.read_text(encoding="utf-8")).body:
                if isinstance(n, ast.Assign) and isinstance(n.value, ast.Constant):
                    for t in n.targets:
                        if isinstance(t, ast.Name) and t.id.endswith("LOCK_KEY"):
                            keys.setdefault(n.value.value, []).append(f"{rel}:{t.id}")
        assert len(keys[chain.RUN_ID_LOCK_KEY]) == 1, keys[chain.RUN_ID_LOCK_KEY]
        assert all(len(v) == 1 for v in keys.values()), keys

    def test_the_allocator_runs_under_it_in_the_writing_transaction(self):
        """Mutation: drop the advisory lock — two runs at 07:30 (the integrity
        catch-up and the mirror job) would read one MAX and collide."""
        import inspect
        import textwrap

        tree = ast.parse(textwrap.dedent(inspect.getsource(chain.persist_run)))
        strings = [n.value for n in ast.walk(tree)
                   if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        lock = next(i for i, s in enumerate(strings) if "pg_advisory_xact_lock" in s)
        alloc = next(i for i, s in enumerate(strings) if "MAX(run_id)" in s)
        assert lock < alloc


# ─── Chain 1's preflight, once the journal is written directly ───────────────


class TestChainOnesPreflight:
    @pytest.mark.asyncio
    async def test_a_direct_journal_is_not_gated_on_its_stood_down_copy(
            self, flag, monkeypatch):
        """The hourly copy stands down with the chain, so its `last_ok_at`
        freezes and would refuse chain 1's flip for ever. Mutation: keep the
        copy's reasons whatever chain 9 says."""
        from tests.unit import test_inventory_preflip as pre

        monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
        facts = pre._facts()
        facts["marks"]["app.data_quality_runs"] = pre._mark(600)
        flag(None)
        blocked = await pre._ask(facts)
        assert any("quality journal" in r for r in blocked["reasons"])
        flag("postgres")
        direct = await pre._ask(facts)
        assert direct["ok"] is True, direct["reasons"]
        assert direct["journal_copy_age_s"] is None


# ─── The standing watch ──────────────────────────────────────────────────────


class TestOrphanChildren:
    def test_an_orphan_is_critical_and_named(self):
        """Mutation: drop `_journal_issues` from the verdict."""
        from core import pg_chain_invariants as inv

        facts = inv.Facts(watched=(chain.CHAIN,), now=NOW,
                          journal=inv.Journal(orphan_issues=2, orphan_diffs=1,
                                              orphan_sample=(41, 42)))
        (issue,) = inv.check_chain_invariants(facts)
        assert issue.check_name == inv.JOURNAL_ORPHANS
        assert issue.severity is Severity.CRITICAL and issue.count == 3
        assert issue.sample_ids == (41, 42)

    def test_a_whole_journal_says_nothing(self):
        from core import pg_chain_invariants as inv

        facts = inv.Facts(watched=(chain.CHAIN,), now=NOW, journal=inv.Journal())
        assert inv.check_chain_invariants(facts) == []

    def test_an_unreadable_journal_is_blindness(self):
        from core import pg_chain_invariants as inv

        facts = inv.Facts(watched=(chain.CHAIN,), now=NOW,
                          journal=inv.Unwatched("boom"))
        (issue,) = inv.check_chain_invariants(facts)
        assert issue.check_name == inv.UNWATCHED
        assert inv.unverified_conditions([issue]) == sorted(inv.CONDITIONS)
