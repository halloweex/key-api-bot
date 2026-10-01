"""Chain 9 against a real Postgres: the journal written there, shadowed in DuckDB.

What `tests/unit/test_dq_journal.py` cannot prove without a database:

- both stores hold the same run, column for column, after one `journal_run`;
- two runs journaled at once get two consecutive ids (the advisory lock);
- the Postgres readers return what DuckDB's return for the same journal,
  rendered in the same zone;
- the daily comparison, facing Postgres→DuckDB, is silent on an honest shadow
  and names a shadow that failed;
- the standing watch reads an orphaned finding with its real SQL;
- the way back carries a Postgres-only run into DuckDB, floors DuckDB's
  allocator above it, carries the digest's beat back and releases.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, dq_journal, pg_dq_journal_write as chain, shadow_writes
from core.data_quality import (
    Discrepancy, DiscrepancyClass, IntegrityIssue, Severity, run_values,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TABLES = chain.CHAIN_TABLES
NOW = datetime.now(timezone.utc).replace(microsecond=0)
DAY = NOW.date()


async def _clean(pool):
    async with pool.acquire() as conn:
        for t in reversed(TABLES):
            await conn.execute(f"DELETE FROM {t}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%' "
                           "OR key = $1", chain.DIGEST_MARKER_KEY)


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core.duckdb_store import DuckDBStore
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    shadow_writes.reset()
    store = DuckDBStore(db_path=tmp_path / "journal.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()
    shadow_writes.reset()


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


async def _both(store, pool, table):
    cols, order = _COLUMNS[table]
    async with pool.acquire() as conn:
        pg = [_utc(tuple(r)) for r in await conn.fetch(
            f"SELECT {cols} FROM {table} ORDER BY {order}")]
    dk_table = table.split(".", 1)[1]
    async with store.connection() as conn:
        dk = [_utc(r) for r in conn.execute(
            f"SELECT {cols} FROM {dk_table} ORDER BY {order}").fetchall()]
    return pg, dk


class TestOneRunTwoStores:
    @pytest.mark.asyncio
    async def test_every_column_is_equal_in_both_stores(self, stores):
        """Mutation: compute `duration_ms` (or any column) once per store, or
        hand DuckDB its own `CURRENT_TIMESTAMP` — the comparison would then
        report every run as differing, every morning."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        first = await dq_journal.journal_run(store, **_kwargs())
        second = await dq_journal.journal_run(store, **_kwargs(layer="mirror_landing",
                                                               diffs=[]))
        assert second == first + 1
        for table in TABLES:
            pg, dk = await _both(store, pool, table)
            assert pg and pg == dk, table
        assert shadow_writes.failures_of(chain.CHAIN)["count"] == 0
        # The first write latched the chain and claimed all three tables.
        assert chain_latch.latched(chain.CHAIN)
        assert set(await chain_latch.read_owners(pool)) == set(TABLES)

    @pytest.mark.asyncio
    async def test_two_runs_at_once_get_two_ids(self, stores):
        """Mutation: drop `pg_advisory_xact_lock` — both read one MAX and the
        second INSERT dies on the primary key."""
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        values = run_values(**_kwargs())
        a, b = await asyncio.gather(
            chain.persist_run(values, [], []), chain.persist_run(values, [], []))
        assert sorted([a.run_id, b.run_id]) == [1, 2]


class TestTheReadersAgree:
    @pytest.mark.asyncio
    async def test_postgres_readers_return_what_duckdbs_return(self, stores, monkeypatch):
        """Same journal, both readers, both rendered in the zone DuckDB uses.
        Mutation: drop `_local`'s conversion, or let one shaper drift."""
        from zoneinfo import ZoneInfo

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        for layer in ("integrity", "integrity", "reconciliation"):
            await dq_journal.journal_run(store, **_kwargs(layer=layer))
        monkeypatch.setattr(chain, "_ZONE", ZoneInfo("Europe/Kyiv"))
        async with store.connection() as conn:
            conn.execute("SET TimeZone = 'Europe/Kyiv'")
            dk_health = await dq_journal._health_runs(dq_journal._DuckReader(conn))
            dk_sections = await dq_journal._sections(
                dq_journal._DuckReader(conn), ("integrity", "reconciliation"),
                NOW, NOW)
            from core.data_quality import fetch_last_success_ages

            dk_ages = fetch_last_success_ages(conn, ("integrity", "reconciliation"))
        async with chain.reading() as conn:
            pg_health = await dq_journal._health_runs(dq_journal._PgReader(conn))
            pg_sections = await dq_journal._sections(
                dq_journal._PgReader(conn), ("integrity", "reconciliation"),
                NOW, NOW)
            pg_ages = await chain.last_success_ages(conn, ("integrity", "reconciliation"))
        assert pg_health == dk_health
        assert pg_health["integrity"]["last_run"]["started_at"].endswith("+03:00")
        assert [(s.run, s.issues, s.diffs, s.previous_issues, s.previous_diffs)
                for s in pg_sections] == \
            [(s.run, s.issues, s.diffs, s.previous_issues, s.previous_diffs)
             for s in dk_sections]
        for layer in ("integrity", "reconciliation"):
            assert pg_ages[layer]["last_success_at"] == dk_ages[layer]["last_success_at"]
            assert abs(pg_ages[layer]["age_seconds"] - dk_ages[layer]["age_seconds"]) <= 5

    @pytest.mark.asyncio
    async def test_the_beat_lives_in_postgres_and_is_read_from_there(self, stores):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await dq_journal.mark_digest_sent(store, "2026-10-01T06:00:00+00:00")
        async with chain.reading() as conn:
            assert await chain.digest_marker(conn) == "2026-10-01T06:00:00+00:00"
        last, _ = await dq_journal.digest_inputs(store, ("integrity",), NOW)
        assert last == datetime(2026, 10, 1, 6, tzinfo=timezone.utc)


class TestTheShadowIsCompared:
    @pytest.mark.asyncio
    async def test_an_honest_shadow_is_silent_and_a_failed_one_is_named(self, stores):
        """The third state end to end: the hourly copy stands down for the
        journal, and the daily comparison keeps comparing it facing
        Postgres→DuckDB. Mutation: drop `- shadowed` in
        `reconcile_operational` (the journal is never compared), or keep the
        shipper running (it would delete the Postgres-only run)."""
        from core.mirror_reconciliation import reconcile_operational
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        old = NOW - timedelta(hours=5)
        await dq_journal.journal_run(store, **_kwargs(started=old))
        issues = [i for i in await reconcile_operational(store)
                  if i.table_name in TABLES]
        assert issues == [], issues

        # A shadow that failed: the run is in Postgres only.
        async with store.connection() as conn:
            conn.execute("DELETE FROM data_quality_diffs")
            conn.execute("DELETE FROM data_quality_issues")
            conn.execute("DELETE FROM data_quality_runs")
        found = {(i.check_name, i.table_name) for i in await reconcile_operational(store)
                 if i.table_name in TABLES}
        assert found == {("shadow_missing_in_duckdb", t) for t in TABLES}

        # And the hourly copy does not "repair" it out of DuckDB.
        shipped = await replicate_operational(store)
        assert set(TABLES) <= set(shipped.get("stood_down", [])), shipped
        pg, _ = await _both(store, pool, "app.data_quality_runs")
        assert len(pg) == 1


class TestTheStandingWatch:
    @pytest.mark.asyncio
    async def test_an_orphaned_finding_is_read(self, stores):
        from core import pg_chain_invariants as inv

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        await dq_journal.journal_run(store, **_kwargs())
        facts = await inv.read_facts(pool=pool)
        assert facts.journal == inv.Journal()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.data_quality_runs")
        facts = await inv.read_facts(pool=pool)
        assert facts.journal.orphan_issues == 1 and facts.journal.orphan_diffs == 1
        (issue,) = [i for i in inv.check_chain_invariants(facts)
                    if i.check_name == inv.JOURNAL_ORPHANS]
        assert issue.count == 2


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_a_postgres_only_run_comes_back_and_the_allocator_clears_it(
            self, stores):
        """The copy-back for a shadow chain is a near no-op copy plus the
        floor: DuckDB already holds every row but those whose shadow failed.
        Mutation: leave `data_quality_run_seq` where the shadow left it — the
        next DuckDB run would reissue an id Postgres already used."""
        from core.chain_transfer import copy_back

        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        old = NOW - timedelta(hours=5)
        await dq_journal.journal_run(store, **_kwargs(started=old))
        await dq_journal.mark_digest_sent(store, "2026-10-01T06:00:00+00:00")
        # The second run's shadow fails: Postgres alone holds run 2.
        real = shadow_writes.into_duckdb

        async def failing(store_, chain_name, write):
            return await real(store_, chain_name, lambda conn: (_ for _ in ()).throw(
                RuntimeError("shadow refused")))

        with patch.object(shadow_writes, "into_duckdb", failing):
            second = await dq_journal.journal_run(store, **_kwargs(started=old))
        assert second == 2

        result = await copy_back(store, chain, dry_run=False)
        assert result["findings"] == [], result["findings"]
        assert result["released"] is True
        assert not chain_latch.latched(chain.CHAIN)
        assert await chain_latch.read_owners(pool) == {}
        for table in TABLES:
            pg, dk = await _both(store, pool, table)
            assert pg == dk, table
        async with store.connection() as conn:
            marker = conn.execute("SELECT value FROM sync_metadata WHERE key = ?",
                                  [chain.DIGEST_MARKER_KEY]).fetchone()[0]
            nxt = conn.execute("SELECT nextval('data_quality_run_seq')").fetchone()[0]
        assert marker == "2026-10-01T06:00:00+00:00"
        assert nxt > 2
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT value FROM meta.chain_watermarks WHERE key = $1",
                chain.DIGEST_MARKER_KEY) is None
