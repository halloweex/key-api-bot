"""Chain 9: the quality journal, written to and read from Postgres alone.

The parts that need no database. What needs a real Postgres — the allocator
under two concurrent runs, every column as written, the Postgres readers
rendering what DuckDB's rendered (literals taken from DuckDB's answer on the
base, step 14's rule 9) — is in `tests/integration/test_dq_journal_pg.py`.

Until step 14's S14-10 the journal had a DuckDB shadow and a `duckdb` branch
behind `KS_WRITE_DQ_JOURNAL`; their tests went with them. What is held here
is what stays: one door, Postgres first and only, no fallback, the beat's
inheritance from the copy, one rendering zone, the allocator's lock, the
standing watch and the fold at the door.

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

from core import dq_journal, pg_dq_journal_write as chain
from core.data_quality import (
    Discrepancy, DiscrepancyClass, IntegrityIssue, Severity, journal_rows,
    run_values,
)

ROOT = pathlib.Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 1, 7, 30, tzinfo=timezone.utc)
DAY = date(2026, 10, 1)


@pytest.fixture
def flag(monkeypatch):
    """No chain's flag, and nothing latched — the marker dir is the test's.
    The flag routes nothing since S14-00; it is set only to show that."""
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)

    def set_(value):
        if value is None:
            monkeypatch.delenv(chain.WRITE_ENV, raising=False)
        else:
            monkeypatch.setenv(chain.WRITE_ENV, value)
    yield set_


@pytest.fixture
def no_duckdb(monkeypatch):
    """Any reach for DuckDB's store fails the test: the journal's doors take
    no store and open no connection since S14-10."""
    from core import store as core_store

    boom = AsyncMock(side_effect=AssertionError("the journal reached for DuckDB"))
    monkeypatch.setattr(core_store, "get_store", boom)

    def connection(self):
        raise AssertionError("the journal opened a DuckDB connection")

    monkeypatch.setattr(core_store.AnalyticsStore, "connection", connection, raising=False)
    return boom


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


# ─── The one door ─────────────────────────────────────────────────────────────

# The journal's writer and readers. DuckDB's — `persist_run(conn, ...)` and
# the `fetch_*` family in `core/data_quality.py` — went with step 14's S14-10;
# their names stay here so one coming back is caught. Chain 9's Postgres
# functions are the other half: a call to one of them outside the door skips
# what the door does (the fold of repeated keys, the beat's inheritance).
_DUCKDB_GONE = {
    "persist_run", "insert_journal_rows", "fetch_latest_run", "fetch_run_issues",
    "fetch_run_diffs", "fetch_baseline_run", "fetch_previous_run",
    "fetch_latest_run_by_id", "fetch_last_success_ages",
}
_CHAIN_FUNCS = {
    "set_digest_marker", "latest_run", "run_by_id", "previous_run", "baseline_run",
    "run_issues", "run_diffs", "last_success_ages", "digest_marker",
    "findings_since",
}
_DOOR_FUNCS = _DUCKDB_GONE | _CHAIN_FUNCS
_ROUTER = {"core/data_quality.py", "core/dq_journal.py", "core/pg_dq_journal_write.py"}
# Calls allowed outside the door, each with what makes it one. None since
# S14-10: the two that read DuckDB's ages in place (`get_stats` and the
# catch-up) stopped reading them.
_EXEMPT_CALLS: set = set()
# SQL naming the journal: the router, the stores' own machinery (DDL,
# migrations, the hourly copy and its comparison, the snapshot validation;
# the copy-back went with step 14's S14-04), the Postgres-side readers that
# already read the writer (chain 1's preflight, the standing invariants). The
# one that gates on the hourly copy's age drops that gate under
# `pg_dq_journal_write.reads_postgres()` — the copy stands down with the
# chain. Chain 7b's goals dry run was the second, and was retired with the
# bridge it measured (chain 7b-4).
_SQL_ALLOWED = _ROUTER | {
    "core/migrations.py", "core/mirror_reconciliation.py",
    "core/snapshot_validation.py",
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
        """Mutation: `persist_run(conn, ...)` back in `_run_dq_integrity`,
        `pg_dq_journal_write.persist_run(...)` called there directly (the fold
        of repeated keys skipped), or `SELECT ... FROM data_quality_runs`
        added to `web/routes/api/health.py`."""
        assert _walk_offenders() == []

    def test_the_walk_sees_a_call_and_a_statement(self):
        tree = ast.parse(
            "def f(conn):\n"
            "    persist_run(conn, layer='x')\n"
            "    chain.latest_run(conn, 'integrity')\n"
            "    conn.execute('SELECT run_id FROM data_quality_runs')\n"
            "    k = 'dq_digest_last_sent'\n")
        assert [n for n, _ in _door_calls(tree)] == ["persist_run", "latest_run"]
        assert {k for k, _ in _journal_strings(tree)} == {"sql", "marker"}

    def test_duckdbs_writer_and_readers_of_the_journal_are_gone(self):
        """S14-10 deleted them. Mutation: put `fetch_last_success_ages` (or
        any of them) back in `core/data_quality.py`."""
        from core import data_quality

        assert not {n for n in _DUCKDB_GONE if hasattr(data_quality, n)}
        assert not hasattr(dq_journal, "_DuckReader")
        assert not hasattr(dq_journal, "writes_postgres")
        assert not hasattr(dq_journal, "reads_postgres")

    def test_the_two_callers_of_the_ages_ask_postgres_alone(self):
        """The catch-up and `/api/health` used to read DuckDB's ages in place
        and swap Postgres's in. Mutation: hand `_journal_ages` a block again,
        or read the ages out of a DuckDB connection in the catch-up."""
        import inspect

        from core import scheduler
        from web.routes.api import health

        catchup = inspect.getsource(scheduler.BackgroundScheduler._schedule_catchup_runs)
        # Once since step 14's S14-11: the catch-up's DuckDB branch, which
        # asked it a second time beside a DuckDB probe, went with chain 1's
        # flag.
        assert catchup.count("dq_journal.last_success_ages_pg()") == 1
        assert "fetch_last_success_ages" not in catchup
        assert "_journal_ages()" in inspect.getsource(health.health_check)
        assert list(inspect.signature(health._journal_ages).parameters) == []

    def test_one_spelling_of_the_marker_key(self):
        from core import scheduler

        assert (scheduler.DQ_DIGEST_LAST_SENT_KEY == dq_journal.DIGEST_MARKER_KEY
                == chain.DIGEST_MARKER_KEY == chain.CHAIN_MARKER_KEYS[0])


# ─── The writer: Postgres, and nothing after it ──────────────────────────────


def _fake_chain_writer(monkeypatch, events, *, fail=None, run_id=41):
    async def persist_run(values, issues, discrepancies):
        events.append("postgres")
        if fail:
            raise fail
        return journal_rows(run_id, values, issues, discrepancies)

    monkeypatch.setattr(chain, "persist_run", persist_run)


class TestTheWriter:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "duckdb", "postgres", "pgsql-typo"])
    async def test_the_run_goes_to_postgres_alone_and_its_id_comes_back(
            self, flag, no_duckdb, monkeypatch, value):
        """Whatever the flag says, no DuckDB half after it. Mutation: put a
        shadow write back after the Postgres commit, or route by the flag."""
        flag(value)
        events = []
        _fake_chain_writer(monkeypatch, events, run_id=41)
        assert await dq_journal.journal_run(**_kwargs()) == 41
        assert events == ["postgres"]
        assert not no_duckdb.await_count

    @pytest.mark.asyncio
    async def test_a_postgres_failure_raises(self, flag, no_duckdb, monkeypatch):
        """Mutation: swallow it, or fall back to DuckDB on a Postgres error."""
        events = []
        _fake_chain_writer(monkeypatch, events, fail=ConnectionRefusedError("down"))
        with pytest.raises(ConnectionRefusedError):
            await dq_journal.journal_run(**_kwargs())
        assert events == ["postgres"]

    @pytest.mark.asyncio
    async def test_a_naive_timestamp_is_refused_before_the_latch_and_the_pool(
            self, flag, monkeypatch):
        """asyncpg reads a naive stamp as UTC, so a Kyiv wall clock would land
        three hours off. Mutation: accept it — or refuse it only after
        `_latch()`, which spends a rollback on a write that never happens."""
        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        monkeypatch.setattr("core.pg.get_pool", pool)
        naive = datetime(2026, 10, 1, 7, 0)
        values = run_values(**{**_kwargs(), "started_at": naive,
                               "ended_at": naive, "as_of": naive})
        with pytest.raises(ValueError, match="naive"):
            await chain.persist_run(values, [], [])
        assert not pool.await_count
        assert not chain.chain_latch.marker_path(chain.CHAIN).exists()

    def test_postgres_is_handed_floats_and_ints(self):
        """asyncpg's float8 reads a float; a Decimal from a rollup would reach
        it as a Decimal. Mutation: hand the raw value on."""
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
    async def test_the_beat_goes_to_postgres_alone(self, flag, no_duckdb, monkeypatch):
        """Mutation: write DuckDB's `sync_metadata` after it again."""
        events = []

        async def set_digest_marker(value):
            events.append(("postgres", value))

        monkeypatch.setattr(chain, "set_digest_marker", set_digest_marker)
        await dq_journal.mark_digest_sent("2026-10-01T06:00:00+00:00")
        assert events == [("postgres", "2026-10-01T06:00:00+00:00")]
        assert not no_duckdb.await_count


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
    """The chain's Postgres readers, answering from a fixed journal, and the
    copy of DuckDB's `sync_metadata` the beat is inherited from."""
    state = {"marker": "2026-09-30T06:00:00+00:00", "raise": None, "reads": 0,
             "copy": {}, "copy_asked": []}

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

    async def values_on(conn, keys):
        state["copy_asked"].append(sorted(keys))
        return {k: v for k, v in state["copy"].items() if k in keys}

    for name, fn in dict(reading=reading, latest_run=latest_run,
                         baseline_run=baseline_run, run_issues=run_issues,
                         run_diffs=run_diffs, last_success_ages=last_success_ages,
                         digest_marker=digest_marker).items():
        monkeypatch.setattr(chain, name, fn)
    monkeypatch.setattr("core.pg_frozen_metadata.values_on", values_on)
    return state


class TestReadersReadPostgres:
    @pytest.mark.asyncio
    async def test_the_digest_reads_postgres(self, flag, no_duckdb, pg_journal):
        """Mutation: read the digest's journal anywhere else."""
        last, sections = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime.fromisoformat(pg_journal["marker"])
        assert sections[0].run["run_id"] == 900
        assert [i["check_name"] for i in sections[0].issues] == ["from_postgres"]
        assert pg_journal["sent_at"] == last
        assert pg_journal["copy_asked"] == []       # the beat was there

    @pytest.mark.asyncio
    async def test_the_data_quality_endpoint_reads_postgres(self, flag, no_duckdb,
                                                            pg_journal):
        body = await dq_journal.health_runs()
        assert body["integrity"]["last_run"]["run_id"] == 900
        assert body["integrity"]["issues"] == PG_ISSUES

    @pytest.mark.asyncio
    async def test_the_canarys_ages_come_from_postgres(self, flag, pg_journal):
        """Mutation: publish anything but Postgres's ages."""
        from web.routes.api import health

        health._journal_ages_cache.update(data=None, expires_at=0)
        ages = await health._journal_ages()
        assert ages["integrity"] == {"last_success_at": "pg", "age_seconds": 7}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "duckdb", "pgsql-typo"])
    async def test_no_flag_routes_the_reads(self, flag, no_duckdb, pg_journal, value):
        """Unlatched, at a flag that once kept the readers on DuckDB, or one
        nobody can read: Postgres answers. Mutation: route by the flag."""
        flag(value)
        assert (await dq_journal.health_runs())["integrity"]["last_run"]["run_id"] == 900
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime.fromisoformat(pg_journal["marker"])


class TestNoFallback:
    @pytest.mark.asyncio
    async def test_an_unreachable_postgres_fails_the_digest(self, flag, no_duckdb,
                                                           pg_journal):
        """Mutation: wrap the Postgres read in a DuckDB fallback — the journal's
        writer failing would then read as "journal fine"."""
        pg_journal["raise"] = ConnectionRefusedError("down")
        with pytest.raises(ConnectionRefusedError):
            await dq_journal.digest_inputs(("integrity",), NOW)
        with pytest.raises(ConnectionRefusedError):
            await dq_journal.health_runs()

    @pytest.mark.asyncio
    async def test_a_journal_lost_after_the_beat_still_fails_the_digest(
            self, flag, no_duckdb, pg_journal, monkeypatch):
        """The beat answered and the runs did not — a connection lost between
        the two reads. Mutation: answer the sections from anything else — the
        test above would still pass, since its first read already raises."""
        async def lost(conn, layer=None):
            raise ConnectionResetError("lost mid-digest")

        monkeypatch.setattr(chain, "latest_run", lost)
        with pytest.raises(ConnectionResetError):
            await dq_journal.digest_inputs(("integrity",), NOW)

    @pytest.mark.asyncio
    async def test_the_canary_gets_no_block_rather_than_another(self, flag, pg_journal):
        from web.routes.api import health

        health._journal_ages_cache.update(data=None, expires_at=0)
        pg_journal["raise"] = ConnectionRefusedError("down")
        assert await health._journal_ages() is None

    @pytest.mark.asyncio
    async def test_a_failure_is_not_cached(self, flag, pg_journal):
        from web.routes.api import health

        health._journal_ages_cache.update(data=None, expires_at=0)
        pg_journal["raise"] = ConnectionRefusedError("down")
        assert await health._journal_ages() is None
        pg_journal["raise"] = None
        assert (await health._journal_ages())["integrity"]["age_seconds"] == 7


class TestTheBeatIsInherited:
    """H6 (2026-10-29): production holds `dq_digest_last_sent` in
    `meta.chain_watermarks`. The inheritance stays for a key that went
    missing, and since S14-10 it reads the copy whatever `KS_DUCKDB` says."""

    @pytest.mark.asyncio
    async def test_absent_in_postgres_it_is_the_copys(self, flag, no_duckdb, pg_journal):
        """The first digest after a flip would otherwise restate every
        standing finding as new. (It ran under both values of `KS_DUCKDB`
        until step 14's S14-18 deleted the switch.) Mutation: no inherit, or
        inherit from DuckDB's own table."""
        pg_journal["marker"] = None
        pg_journal["copy"] = {dq_journal.DIGEST_MARKER_KEY: "2026-09-29T06:00:00+00:00"}
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime(2026, 9, 29, 6, tzinfo=timezone.utc)
        assert pg_journal["copy_asked"] == [[dq_journal.DIGEST_MARKER_KEY]]

    @pytest.mark.asyncio
    async def test_absent_in_both_it_is_no_beat(self, flag, no_duckdb, pg_journal):
        pg_journal["marker"] = None
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last is None

    @pytest.mark.asyncio
    async def test_present_in_postgres_it_wins(self, flag, no_duckdb, pg_journal):
        pg_journal["copy"] = {dq_journal.DIGEST_MARKER_KEY: "2026-09-01T06:00:00+00:00"}
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime.fromisoformat(pg_journal["marker"])
        assert pg_journal["copy_asked"] == []


# ─── Rendering: the Postgres rows print what DuckDB printed ──────────────────

# DuckDB's answer, on the base `e3112a7d` (rule 9): `fetch_latest_run` on a
# DuckDB connection set to Europe/Kyiv, after `insert_journal_rows` of
# `journal_rows(3, run_values(**_kwargs()), [], [])`. Postgres's row, as asyncpg
# hands it back in UTC, must still render as this.
DUCKDB_RENDERED_RUN = {
    "api_calls_used": 0, "as_of": "2026-10-01T10:30:00+03:00", "critical_count": 1,
    "discrepancies_count": 1, "duration_ms": 3000,
    "ended_at": "2026-10-01T10:30:00+03:00", "error_message": None,
    "integrity_issues_count": 1, "layer": "integrity", "run_id": 3,
    "started_at": "2026-10-01T10:29:57+03:00", "status": "CRITICAL", "warn_count": 1,
    "window_end": "2026-10-01", "window_start": "2026-10-01",
}


class TestOneRendering:
    def test_a_postgres_row_shapes_exactly_as_duckdbs_did(self, monkeypatch):
        """asyncpg hands back UTC; the digest prints `started_at[:16]`, so an
        unconverted row would date every run three hours early in Kyiv.
        Mutation: drop the conversion in `_local`."""
        from zoneinfo import ZoneInfo

        from core.data_quality import run_dict

        monkeypatch.setattr(chain, "_ZONE", ZoneInfo("Europe/Kyiv"))
        values = run_values(**_kwargs())
        rows = journal_rows(3, values, [], [])
        pg_row = tuple(v.astimezone(timezone.utc) if isinstance(v, datetime) else v
                       for v in rows.run)
        assert run_dict(chain._local(pg_row)) == DUCKDB_RENDERED_RUN

    def test_the_zone_is_the_hosts_and_never_asks_duckdb(self, monkeypatch):
        """The zone used to be asked of an in-memory DuckDB under `KS_DUCKDB=on`
        (until S14-10); the switch itself went in S14-18. Mutation: put the
        probe back."""
        import sys
        from zoneinfo import ZoneInfo

        monkeypatch.setattr(chain, "_ZONE", None)
        monkeypatch.setenv("TZ", "Europe/Kyiv")
        loaded = "duckdb" in sys.modules
        assert chain._zone() == ZoneInfo("Europe/Kyiv")
        assert ("duckdb" in sys.modules) == loaded, "the zone loaded the driver"
        assert not hasattr(chain, "_duckdb_zone")

    def test_the_zone_is_read_once(self, monkeypatch):
        """Every row of every read goes through it; the host's file is read
        once per process. Mutation: drop the cache."""
        calls = []
        monkeypatch.setattr(chain, "_ZONE", None)
        monkeypatch.setattr(chain, "_host_zone", lambda: calls.append(1) or timezone.utc)
        chain._zone()
        chain._zone()
        assert calls == [1]

    @pytest.mark.parametrize("tz", [None, "", "not/a zone"])
    def test_the_host_zone_falls_back_to_the_host(self, monkeypatch, tz):
        """No usable `TZ`: the host's zone, as ICU reads it next — never a
        raise on the path every journal row is rendered through."""
        from zoneinfo import ZoneInfo

        if tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", tz)
        zone = chain._host_zone()
        assert isinstance(zone, ZoneInfo)


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


# ─── One finding per key, at the door ─────────────────────────────────────────


def _twin(name="chain_invariants_unwatched", table="(write chains)",
          severity=Severity.WARN, count=1, samples=(), text="a"):
    return IntegrityIssue(check_name=name, table_name=table, severity=severity,
                          count=count, sample_ids=samples, description=text)


class TestOneFindingPerKey:
    """`app.data_quality_issues` is keyed on `(run_id, check_name,
    table_name)`; DuckDB's table is not. Two findings under one key in one run
    were refused whole by Postgres, or taken by DuckDB and then refused by the
    hourly copy every hour after. The door folds them."""

    def test_a_run_without_a_repeat_is_the_same_list(self):
        """The default path writes exactly what it wrote before. Mutation:
        rebuild the list (or reorder it) when nothing repeats."""
        issues = [_issue("a"), _issue("b"), _twin()]
        assert dq_journal.one_finding_per_key(issues) is issues

    def test_a_repeat_is_folded_into_the_first(self):
        """Mutation: keep the first and drop the second — its severity, its
        count and what it said would be lost from the journal."""
        issues = [_twin(count=1, samples=(1, 2), text="journal blind"),
                  _issue("other"),
                  _twin(severity=Severity.CRITICAL, count=2, samples=(2, 3),
                        text="watchdogs blind")]
        (first, other) = dq_journal.one_finding_per_key(issues)
        assert other.check_name == "other"
        assert first.severity is Severity.CRITICAL and first.count == 3
        assert first.sample_ids == (1, 2, 3)
        assert first.description == "journal blind\nwatchdogs blind"

    @pytest.mark.asyncio
    async def test_postgres_is_handed_one_row_per_key(self, flag, no_duckdb, monkeypatch):
        """The chain's writer is handed the folded list, and the run's counts
        are the folded ones. Mutation: fold after `run_values`, or not at all."""
        seen = []

        async def persist_run(values, issues, discrepancies):
            seen.append(([(i.check_name, i.table_name) for i in issues],
                         values.integrity_issues_count))
            return journal_rows(7, values, issues, discrepancies)

        monkeypatch.setattr(chain, "persist_run", persist_run)
        await dq_journal.journal_run(**_kwargs(issues=[_twin(), _issue(), _twin()]))
        assert seen == [([("chain_invariants_unwatched", "(write chains)"),
                          ("pk_uniqueness_orders", "orders")], 2)]
