"""Chain 9 against a real Postgres: the journal written and read there alone.

What `tests/unit/test_dq_journal.py` cannot prove without a database:

- one `journal_run` writes every column of a run and its findings as DuckDB's
  shadow held them — the literal is DuckDB's rows on the base `e3112a7d`, the
  last tree with a shadow (step 14's rule 9);
- two runs journaled at once get two consecutive ids (the advisory lock);
- the Postgres readers return what DuckDB's readers returned for the same
  journal, rendered in the same zone — again DuckDB's answer, as a literal;
- the digest's beat lives in Postgres and is read from there, and while the
  key is absent it is inherited from `app.sync_metadata`, the copy of
  DuckDB's table, whatever `KS_DUCKDB` says (S14-10);
- the standing watch reads an orphaned finding with its real SQL;
- two findings under one key in one run land as one row.

The way back — a Postgres-only run carried into DuckDB, its allocator floored,
the digest's beat carried back — went with the copy-back in step 14's S14-04;
the shadow, its daily comparison and the `duckdb` branch with S14-10.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, dq_journal, pg_dq_journal_write as chain
from core.data_quality import (
    Discrepancy, DiscrepancyClass, IntegrityIssue, Severity, run_values,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLES = chain.CHAIN_TABLES
# Fixed, so the literals below are DuckDB's answers on this very fixture.
NOW = datetime(2026, 10, 1, 7, 30, tzinfo=timezone.utc)
DAY = NOW.date()


async def _clean(pool):
    """The journal, the chain's owner rows and the beat in both places it
    is read from — the throwaway database's, put back as the suite found it."""
    async with pool.acquire() as conn:
        for t in reversed(TABLES):
            await conn.execute(f"DELETE FROM {t}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
                           "OR key = $1", chain.DIGEST_MARKER_KEY)
        await conn.execute("DELETE FROM app.sync_metadata WHERE key = $1",
                           chain.DIGEST_MARKER_KEY)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


def _kwargs(layer="integrity", started=None, issues=None, diffs=None):
    started = started or NOW - timedelta(minutes=5)
    return dict(
        started_at=started, ended_at=started + timedelta(seconds=4), as_of=started,
        window_start=DAY, window_end=DAY, layer=layer,
        issues=issues if issues is not None else [
            IntegrityIssue(check_name="pk_uniqueness_orders", table_name="orders",
                           severity=Severity.WARN, count=2, sample_ids=(5, 6),
                           description="двa дублі")],
        discrepancies=diffs if diffs is not None else [
            Discrepancy(month="2026-09", source_id=4,
                        diff_class=DiscrepancyClass.MISSING_IN_DK, field="orders",
                        dk_value=1.25, kc_value=3.5, severity=Severity.CRITICAL,
                        order_ids=(77,))],
    )


_COLUMNS = {
    "app.data_quality_runs": (
        "run_id, started_at, ended_at, as_of, window_start, window_end, layer, "
        "status, integrity_issues_count, discrepancies_count, critical_count, "
        "warn_count, api_calls_used, duration_ms, error_message", "run_id"),
    "app.data_quality_issues": (
        "run_id, check_name, table_name, severity, count, sample_ids, description",
        "run_id, check_name"),
    "app.data_quality_diffs": (
        "run_id, month, source_id, diff_class, field, dk_value, kc_value, "
        "severity, order_ids", "run_id, month"),
}


def _utc(row):
    return tuple(v.astimezone(timezone.utc) if isinstance(v, datetime) else v
                 for v in row)


def _at(text):
    return datetime.fromisoformat(text)


# DuckDB's rows after the same two `journal_run` calls on the base, shadow on
# (rule 9), every instant in UTC.
DUCKDB_JOURNAL_ROWS = {
    "app.data_quality_runs": [
        (1, _at("2026-10-01T07:25:00+00:00"), _at("2026-10-01T07:25:04+00:00"),
         _at("2026-10-01T07:25:00+00:00"), date(2026, 10, 1), date(2026, 10, 1),
         "integrity", "CRITICAL", 1, 1, 1, 1, 0, 4000, None),
        (2, _at("2026-10-01T07:25:00+00:00"), _at("2026-10-01T07:25:04+00:00"),
         _at("2026-10-01T07:25:00+00:00"), date(2026, 10, 1), date(2026, 10, 1),
         "mirror_landing", "WARN", 1, 0, 0, 1, 0, 4000, None),
    ],
    "app.data_quality_issues": [
        (1, "pk_uniqueness_orders", "orders", "WARN", 2, "[5, 6]", "двa дублі"),
        (2, "pk_uniqueness_orders", "orders", "WARN", 2, "[5, 6]", "двa дублі"),
    ],
    "app.data_quality_diffs": [
        (1, "2026-09", 4, "MISSING_IN_DK", "orders", 1.25, 3.5, "CRITICAL", "[77]"),
    ],
}


async def _pg_rows(pool, table):
    cols, order = _COLUMNS[table]
    async with pool.acquire() as conn:
        return [_utc(tuple(r)) for r in await conn.fetch(
            f"SELECT {cols} FROM {table} ORDER BY {order}")]


class TestOneRun:
    @pytest.mark.asyncio
    async def test_every_column_is_what_duckdbs_shadow_held(self, pool):
        """Mutation: compute `duration_ms` (or any column) per call, or let a
        column drift in `journal_rows` — the literal is DuckDB's answer."""
        first = await dq_journal.journal_run(**_kwargs())
        second = await dq_journal.journal_run(**_kwargs(layer="mirror_landing", diffs=[]))
        assert (first, second) == (1, 2)
        for table in TABLES:
            assert await _pg_rows(pool, table) == DUCKDB_JOURNAL_ROWS[table], table
        # The first write latched the chain and claimed all three tables.
        assert chain_latch.latched(chain.CHAIN)
        assert set(await chain_latch.read_owners(pool)) == set(TABLES)

    @pytest.mark.asyncio
    async def test_two_runs_at_once_get_two_ids(self, pool):
        """Mutation: drop `pg_advisory_xact_lock` — both read one MAX and the
        second INSERT dies on the primary key."""
        values = run_values(**_kwargs())
        a, b = await asyncio.gather(
            chain.persist_run(values, [], []), chain.persist_run(values, [], []))
        assert sorted([a.run_id, b.run_id]) == [1, 2]


def _issue_literal():
    return {"check_name": "pk_uniqueness_orders", "count": 2, "description": "двa дублі",
            "sample_ids": [5, 6], "severity": "WARN", "table_name": "orders"}


def _diff_literal():
    return {"diff_class": "MISSING_IN_DK", "dk_value": 1.25, "field": "orders",
            "kc_value": 3.5, "month": "2026-09", "order_ids": [77],
            "severity": "CRITICAL", "source_id": 4}


def _run_literal(run_id, layer):
    return {"api_calls_used": 0, "as_of": "2026-10-01T10:25:00+03:00",
            "critical_count": 1, "discrepancies_count": 1, "duration_ms": 4000,
            "ended_at": "2026-10-01T10:25:04+03:00", "error_message": None,
            "integrity_issues_count": 1, "layer": layer, "run_id": run_id,
            "started_at": "2026-10-01T10:25:00+03:00", "status": "CRITICAL",
            "warn_count": 1, "window_end": "2026-10-01", "window_start": "2026-10-01"}


# DuckDB's readers on the same journal (integrity, integrity, reconciliation),
# its session set to Europe/Kyiv, on the base (rule 9).
DUCKDB_HEALTH = {
    "integrity": {"issues": [_issue_literal()], "last_run": _run_literal(2, "integrity")},
    "mirror_landing": {"issues": [], "last_run": None},
    "reconciliation": {"diffs": [_diff_literal()],
                       "last_run": _run_literal(3, "reconciliation")},
    "reconciliation_pg": {"diffs": [], "last_run": None},
}
DUCKDB_SECTIONS = [
    {"layer": "integrity", "run": _run_literal(2, "integrity"),
     "issues": [_issue_literal()], "diffs": [_diff_literal()],
     "previous_issues": [_issue_literal()], "previous_diffs": [_diff_literal()],
     "age_hours": 0.08333333333333333},
    {"layer": "reconciliation", "run": _run_literal(3, "reconciliation"),
     "issues": [_issue_literal()], "diffs": [_diff_literal()],
     "previous_issues": [], "previous_diffs": [],
     "age_hours": 0.08333333333333333},
]
DUCKDB_LAST_SUCCESS_AT = {"integrity": "2026-10-01T10:25:00+03:00",
                          "reconciliation": "2026-10-01T10:25:00+03:00"}


class TestTheReadersAnswerAsDuckDBs:
    @pytest.mark.asyncio
    async def test_postgres_readers_return_what_duckdbs_returned(self, pool, monkeypatch):
        """Same journal, Postgres's readers, rendered in Kyiv; the literals are
        DuckDB's readers' answers on the base. Mutation: drop `_local`'s
        conversion, or let one shaper drift."""
        from zoneinfo import ZoneInfo

        for layer in ("integrity", "integrity", "reconciliation"):
            await dq_journal.journal_run(**_kwargs(layer=layer))
        monkeypatch.setattr(chain, "_ZONE", ZoneInfo("Europe/Kyiv"))
        async with chain.reading() as conn:
            health = await dq_journal._health_runs(dq_journal._PgReader(conn))
            sections = await dq_journal._sections(
                dq_journal._PgReader(conn), ("integrity", "reconciliation"), NOW, NOW)
            ages = await chain.last_success_ages(conn, ("integrity", "reconciliation"))
        assert health == DUCKDB_HEALTH
        assert [dict(layer=s.layer, run=s.run, issues=s.issues, diffs=s.diffs,
                     previous_issues=s.previous_issues,
                     previous_diffs=s.previous_diffs, age_hours=s.age_hours)
                for s in sections] == DUCKDB_SECTIONS
        assert {k: v["last_success_at"] for k, v in ages.items()} == DUCKDB_LAST_SUCCESS_AT
        assert all(v["age_seconds"] > 0 for v in ages.values())
        # The door's own answer for /api/health/data-quality is the same body.
        assert await dq_journal.health_runs() == DUCKDB_HEALTH


class TestTheBeat:
    @pytest.mark.asyncio
    async def test_the_beat_lives_in_postgres_and_is_read_from_there(self, pool):
        await dq_journal.mark_digest_sent("2026-10-01T06:00:00+00:00")
        async with chain.reading() as conn:
            assert await chain.digest_marker(conn) == "2026-10-01T06:00:00+00:00"
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime(2026, 10, 1, 6, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_absent_it_is_inherited_from_the_copy(self, pool, monkeypatch):
        """`meta.chain_watermarks` without the key, `app.sync_metadata` with
        it: the copy answers, and nothing opens DuckDB. (Until step 14's
        S14-18 it ran under both values of `KS_DUCKDB`; the switch is
        deleted.) Mutation: inherit only under off, as before S14-10."""
        from core import duckdb_store

        def _no_duckdb(self):
            raise AssertionError("the digest opened DuckDB")

        monkeypatch.setattr(duckdb_store.DuckDBStore, "connection", _no_duckdb)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.sync_metadata (key, value, updated_at) "
                "VALUES ($1, $2, now())",
                chain.DIGEST_MARKER_KEY, "2026-09-29T06:00:00+00:00")
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime(2026, 9, 29, 6, tzinfo=timezone.utc)
        # Once the beat moves, Postgres's own key wins over the copy.
        await dq_journal.mark_digest_sent("2026-10-01T06:00:00+00:00")
        last, _ = await dq_journal.digest_inputs(("integrity",), NOW)
        assert last == datetime(2026, 10, 1, 6, tzinfo=timezone.utc)


class TestTheStandingWatch:
    @pytest.mark.asyncio
    async def test_an_orphaned_finding_is_read(self, pool):
        from core import pg_chain_invariants as inv

        await dq_journal.journal_run(**_kwargs())
        facts = await inv.read_facts(pool=pool)
        assert facts.journal == inv.Journal()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.data_quality_runs")
        facts = await inv.read_facts(pool=pool)
        assert facts.journal.orphan_issues == 1 and facts.journal.orphan_diffs == 1
        (issue,) = [i for i in inv.check_chain_invariants(facts)
                    if i.check_name == inv.JOURNAL_ORPHANS]
        assert issue.count == 2


def _blind_twice():
    """Two blind parts of the standing watch in one integrity run — each
    `unwatched_issue` names its own part now, so the repeat is spelled out to
    prove the door holds against the next producer that files one."""
    return [IntegrityIssue(check_name="chain_invariants_unwatched",
                           table_name="(write chains)", severity=Severity.WARN,
                           count=1, description=f"{part} could not be read")
            for part in ("the journal", "the watchdogs")]


class TestOneTransaction:
    @pytest.mark.asyncio
    async def test_a_refused_finding_leaves_no_run_behind(self, pool):
        """A run and its findings land together or not at all: Postgres
        refuses a NUL in a text column, and the run row inserted before it in
        the same transaction is rolled back with it. (DuckDB's `persist_run`
        was held to the same bracket until step 14's S14-10.) Mutation: write
        the run in its own transaction — the digest would then read a run
        with counts and no findings, and call every standing one new."""
        bad = IntegrityIssue(check_name="pk_uniqueness_orders", table_name="orders",
                             severity=Severity.WARN, count=1, description="a\x00b")
        with pytest.raises(asyncpg.PostgresError):
            await dq_journal.journal_run(**_kwargs(issues=[bad]))
        for table in TABLES:
            assert await _pg_rows(pool, table) == [], table


class TestOneFindingPerKey:
    """`app.data_quality_issues` is keyed on `(run_id, check_name,
    table_name)`."""

    @pytest.mark.asyncio
    async def test_the_run_lands_with_one_row_per_key(self, pool):
        """The review's reproduction. Mutation: drop the fold from
        `journal_run` — Postgres refuses the run on its primary key, and the
        failed attempt has latched the chain."""
        run_id = await dq_journal.journal_run(**_kwargs(issues=_blind_twice()))
        rows = await _pg_rows(pool, "app.data_quality_issues")
        assert [(r[0], r[1], r[2], r[4]) for r in rows] == [
            (run_id, "chain_invariants_unwatched", "(write chains)", 2)]
