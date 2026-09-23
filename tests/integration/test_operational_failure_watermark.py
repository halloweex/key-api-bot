"""A failed hourly copy of the operational tables, written where it is read.

`replicate_operational` writes every table it copies in one Postgres
transaction, so one bad row stops all of them. The row that does it in
practice is a duplicate id in `reconciliation_log`: DuckDB declares no key
there, `app.reconciliation_log` has a primary key, and a sequence left behind
MAX(id) — a compaction whose restore fell short, with the boot net failing too
— produces exactly that. Each hourly run then reads the same rows above the
same watermark and fails the same way.

It used to fail with an ERROR line and nothing else. These run against a real
PostgreSQL because the whole claim is about what `meta.mirror_state` holds
afterwards: the failure counted on every table the run was copying, the last
success left where it was, the count cleared by the next run that works — and
what the 07:30 comparison makes of it, WARN for one failed hour and CRITICAL
for two. The last test is the other way a failure used to be stamped: the
hourly copy and a manual expense's immediate one overlapping, which a lock
now serialises.
"""
from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# Well above anything another test writes to this table.
FIRST_ID = 960_001


def _all_shipped_tables():
    from core.pg_operational import _APPEND_ABOVE, _FULL_REPLACE

    return (
        *(table for table, _d, _c, _o in _FULL_REPLACE),
        *(spec.pg_table for spec in _APPEND_ABOVE),
    )


async def _clean(pool, tables):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.reconciliation_log WHERE id >= $1", FIRST_ID)
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])", list(tables))


async def _states(pool, tables):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT table_name, last_ok_at, failures_since_ok, last_error "
            "FROM meta.mirror_state WHERE table_name = ANY($1::text[])", list(tables))
    return {r["table_name"]: dict(r) for r in rows}


def _log_row(conn, row_id: int) -> None:
    conn.execute(
        "INSERT INTO reconciliation_log (id, check_date, api_count, db_count, "
        "discrepancy, discrepancy_pct, status) "
        "VALUES (?, DATE '2026-09-20', 14, 14, 0, 0, 'ok')",
        [row_id],
    )


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    import core.duckdb_store as duckdb_store
    import core.pg_operational as pg_operational
    from core.duckdb_store import DuckDBStore
    from core.write_chains import WRITE_CHAINS

    monkeypatch.setattr(duckdb_store, "DB_DIR", tmp_path)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    # The copy is serialised by a module lock; one bound to another test's
    # event loop would fail the first time a run here waits on it.
    monkeypatch.setattr(pg_operational, "OPERATIONAL_REPLICATION_LOCK", asyncio.Lock())
    # Every chain on DuckDB, so every table is shipped and every one stamped.
    for chain in WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)

    tables = _all_shipped_tables()
    store = DuckDBStore(db_path=tmp_path / "ops.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool, tables)
    async with pool.acquire() as conn:
        pg_max = await conn.fetchval("SELECT MAX(id) FROM app.reconciliation_log")
    assert pg_max is None or pg_max < FIRST_ID, (
        "rows at or below Postgres' watermark are never shipped, so the "
        "duplicate below could not reach the transaction"
    )
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, tables
    await _clean(pool, tables)
    await pool.close()
    await store.close()


@pytest.mark.asyncio
async def test_a_duplicate_id_is_counted_on_every_table_until_the_run_succeeds(stores):
    from core.data_quality import Severity
    from core.mirror_reconciliation import reconcile_operational
    from core.pg_operational import replicate_operational

    store, pool, tables = stores

    async with store.connection() as conn:
        _log_row(conn, FIRST_ID)
    first = await replicate_operational(store)
    assert "error" not in first, first
    healthy = await _states(pool, tables)
    assert set(healthy) == set(tables)
    assert all(s["failures_since_ok"] == 0 and s["last_ok_at"] for s in healthy.values())

    # The SEQ-6 shape: DuckDB stores the same id twice without a word.
    async with store.connection() as conn:
        _log_row(conn, FIRST_ID + 1)
        _log_row(conn, FIRST_ID + 1)
        assert conn.execute(
            "SELECT COUNT(*) FROM reconciliation_log WHERE id = ?", [FIRST_ID + 1]
        ).fetchone()[0] == 2

    # One failed hour, with the success moments ago, is WARN on every table:
    # the next run usually repairs it. The second failure in a row is
    # CRITICAL, which is how a duplicate that fails every run still pages
    # within two hourly runs.
    expected = {1: Severity.WARN, 2: Severity.CRITICAL}
    for hour in (1, 2):
        result = await replicate_operational(store)
        assert "UniqueViolation" in result["error"], result

        states = await _states(pool, tables)
        for table in tables:
            state = states[table]
            assert state["failures_since_ok"] == hour, table
            assert "duplicate key" in state["last_error"], table
            assert state["last_ok_at"] == healthy[table]["last_ok_at"], table

        failing = {
            issue.table_name: issue.severity
            for issue in await reconcile_operational(store)
            if issue.check_name == "mirror_failing"
        }
        assert failing == {table: expected[hour] for table in tables}, hour

    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT MAX(id) FROM app.reconciliation_log") == FIRST_ID

    # Somebody removes the duplicate; the next run clears every count through
    # the same OK watermark every copy uses.
    async with store.connection() as conn:
        conn.execute("DELETE FROM reconciliation_log WHERE id = ?", [FIRST_ID + 1])
        _log_row(conn, FIRST_ID + 1)
    recovered = await replicate_operational(store)
    assert "error" not in recovered, recovered

    states = await _states(pool, tables)
    for table in tables:
        assert states[table]["failures_since_ok"] == 0, table
        assert states[table]["last_error"] is None, table
        assert states[table]["last_ok_at"] > healthy[table]["last_ok_at"], table
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT MAX(id) FROM app.reconciliation_log") == FIRST_ID + 1


@pytest.mark.asyncio
async def test_two_overlapping_runs_both_ship_and_neither_stamps_a_failure(stores):
    """The hourly job and a manual expense's immediate copy, at the same time.

    Unserialised, both read the same watermark, both inserted the rows above
    it, and the second to commit failed on the primary key — then wrote that
    failure over the first one's OK on every table, so a copy that was
    complete read as failing for an hour. One at a time, the second run finds
    nothing above the watermark the first one left.
    """
    from core.pg_operational import replicate_after_manual_expense, replicate_operational

    store, pool, tables = stores

    async with store.connection() as conn:
        _log_row(conn, FIRST_ID)
    baseline = await replicate_operational(store)
    assert "error" not in baseline, baseline

    # Several rounds, because a race is a matter of timing and one clean round
    # proves little about the arrangement.
    for n in range(1, 6):
        async with store.connection() as conn:
            _log_row(conn, FIRST_ID + n)
        results = await asyncio.gather(
            replicate_operational(store), replicate_after_manual_expense(store),
        )
        assert all("error" not in r for r in results), results

        states = await _states(pool, tables)
        for table in tables:
            assert states[table]["failures_since_ok"] == 0, (table, states[table])
            assert states[table]["last_error"] is None, table

    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT COUNT(*) FROM app.reconciliation_log WHERE id >= $1", FIRST_ID
        ) == 6
