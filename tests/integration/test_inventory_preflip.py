"""Chain 1's pre-flip hardening (DN-24) against a real PostgreSQL.

KS_WRITE_INVENTORY stays off in production; two of the gaps that would bite
the day it is switched on are shown here:

* the 01:00 snapshot job takes no heavy lock, so two Postgres rebuilds can
  interleave — the second one's INSERT then collides with the first one's rows
  and the whole rebuild raises;
* a missing TEMPORARY grant makes every status rebuild raise, and nothing said
  so before the flip. The preflight now does, and here it reads the privilege,
  the replication and the quality journal as the real schema holds them.

The third, a stock-step failure escaping the sync tick, needs no database and
is in `tests/unit/test_inventory_preflip.py`.

The writers are called directly here, not through the repository: the property
under test belongs to the Postgres transaction, and the route to it is pinned
by `tests/integration/test_chain_latch.py`.

Skipped without `KS_PG_DSN`, like every store test; the TEMPORARY case also
needs `KS_PG_READONLY_DSN`, which CI provisions.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import pg_inventory_write

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
READONLY_DSN = os.getenv("KS_PG_READONLY_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")
# The sentence `.github/workflows/ci.yml` greps skip reasons for, so in CI the
# read-only case runs or the job fails.
PROVISIONED = ("needs a live PostgreSQL provisioned as .github/workflows/ci.yml "
               "provisions it (KS_PG_READONLY_DSN)")

# A day nothing else in the suite photographs, so the rows are ours to delete.
DAY = date(2031, 1, 7)

# How long a snapshot holds after its "already taken?" read for the other one
# to make the same read. Without the lock the other arrives in milliseconds and
# the hold ends there; with it, the hold runs out and costs the test this much.
_OVERLAP_WAIT_S = 1.0


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.sku_inventory_status")
        await conn.execute("DELETE FROM bronze.offer_stocks")
        await conn.execute("DELETE FROM bronze.offers")
        await conn.execute("DELETE FROM app.inventory_sku_history WHERE date = $1", DAY)
        await conn.execute("DELETE FROM app.inventory_history WHERE date = $1", DAY)
        await conn.execute("DELETE FROM meta.chain_watermarks")


@pytest_asyncio.fixture
async def pool():
    """A pool wide enough for two writers and a lock holder, standing in for
    `core.pg.get_pool` so the writers use it."""
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    await _clean(p)
    async with p.acquire() as conn:
        await conn.executemany(
            "INSERT INTO bronze.offers (id, product_id, sku, synced_at) "
            "VALUES ($1, $2, $3, now())",
            [(9_910_001, 9_910_101, "DN24-1"), (9_910_002, 9_910_102, "DN24-2"),
             (9_910_003, 9_910_103, "DN24-3")])
        await conn.executemany(
            "INSERT INTO bronze.offer_stocks (id, sku, price, purchased_price, "
            "quantity, reserve, mirrored_at) VALUES ($1, $2, 100, 50, $3, 0, now())",
            [(9_910_001, "DN24-1", 5), (9_910_002, "DN24-2", 0), (9_910_003, "DN24-3", 9)])
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield p
    await _clean(p)
    await p.close()


class TestTheChainLock:
    @pytest.mark.asyncio
    async def test_two_concurrent_rebuilds_both_commit(self, pool, monkeypatch):
        """The stock step and the 01:00 job arriving together. A pause between
        the DELETE and the INSERT makes the overlap certain rather than lucky:
        without the lock the second rebuild's DELETE sees none of the first's
        new rows, and its INSERT then dies on `offer_id`.

        One rebuild first, because production's owner rows exist from the first
        write on. On a chain that has never written, the two `claim` INSERTs
        meet on the same new owner key and the second waits for the first to
        commit — which serialises them by accident and hides the race."""
        assert await pg_inventory_write.rebuild_sku_inventory_status() == 3
        body = pg_inventory_write.sku_status_rebuild_select
        monkeypatch.setattr(
            pg_inventory_write, "sku_status_rebuild_select",
            lambda dialect: "SELECT pg_sleep(0.4); " + body(dialect))

        first, second = await asyncio.gather(
            pg_inventory_write.rebuild_sku_inventory_status(),
            pg_inventory_write.rebuild_sku_inventory_status(),
        )

        assert first == second == 3
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT COUNT(*) FROM app.sku_inventory_status") == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call", [
        pytest.param(lambda: pg_inventory_write.rebuild_sku_inventory_status(),
                     id="rebuild_sku_inventory_status"),
        pytest.param(lambda: pg_inventory_write.record_sku_inventory_snapshot(DAY),
                     id="record_sku_inventory_snapshot"),
        pytest.param(lambda: pg_inventory_write.record_inventory_snapshot(today=DAY),
                     id="record_inventory_snapshot"),
    ])
    async def test_each_one_waits_for_the_lock_and_then_commits(self, pool, call):
        """Deterministic, one writer at a time: while another transaction holds
        the chain-1 key the writer cannot finish, and it finishes the moment
        that transaction does."""
        holder = await pool.acquire()
        try:
            tx = holder.transaction()
            await tx.start()
            await holder.execute("SELECT pg_advisory_xact_lock($1)",
                                 pg_inventory_write.CHAIN_LOCK_KEY)
            task = asyncio.ensure_future(call())
            done, _ = await asyncio.wait({task}, timeout=0.5)
            assert not done, "the writer finished while another held the chain lock"
            await tx.commit()
            assert await asyncio.wait_for(task, timeout=10)
        finally:
            await pool.release(holder)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call, guard", [
        pytest.param(lambda: pg_inventory_write.record_sku_inventory_snapshot(DAY),
                     "FROM app.inventory_sku_history WHERE date", id="per-SKU"),
        pytest.param(lambda: pg_inventory_write.record_inventory_snapshot(today=DAY),
                     "FROM app.inventory_history WHERE date", id="rolled-up"),
    ])
    async def test_the_second_snapshot_of_the_day_waits_and_finds_it_taken(
            self, pool, call, guard):
        """Two photographs of one day, with the overlap forced: each writer
        holds after its "already taken?" read until the other has made the
        same read, so without the lock both find the day free and the second
        INSERT dies on the primary key.

        With the lock the second cannot reach its read while the first is
        open, so the first holds only until `_OVERLAP_WAIT_S` runs out, then
        commits — and the second reads the day as taken. Called one after the
        other on a cold pool, the two never overlapped at all: the second was
        still opening a connection when the first committed, and this passed
        with the lock removed."""
        await pg_inventory_write.rebuild_sku_inventory_status()   # three SKUs to photograph
        reached = []
        both = asyncio.Event()

        class _HoldAfterGuard:
            def __init__(self, conn):
                self._conn = conn

            def __getattr__(self, name):
                return getattr(self._conn, name)

            async def fetchval(self, sql, *args):
                value = await self._conn.fetchval(sql, *args)
                if guard in sql:
                    reached.append(value)
                    if len(reached) == 2:
                        both.set()
                    try:
                        await asyncio.wait_for(both.wait(), _OVERLAP_WAIT_S)
                    except asyncio.TimeoutError:
                        pass
                return value

        class _Holding:
            @asynccontextmanager
            async def acquire(self):
                async with pool.acquire() as conn:
                    yield _HoldAfterGuard(conn)

        with patch("core.pg.get_pool", new=AsyncMock(return_value=_Holding())):
            first, second = await asyncio.gather(call(), call())

        assert sorted([first, second]) == [False, True]
        # The second read saw the first one's commit: it came after, not beside.
        assert reached == [None, 1]


# ─── preflight() against the real schema ─────────────────────────────────────

# Above anything the rest of the suite writes, so "the latest mirror_landing
# run" is ours. The one before it is yesterday's, and exists so that "latest"
# is a choice the preflight has to make rather than the only row there is.
PREVIOUS_RUN_ID = 9_000_000_001
RUN_ID = PREVIOUS_RUN_ID + 1
JOURNAL = "app.data_quality_runs"


async def _clean_journal(conn):
    await conn.execute(
        "DELETE FROM app.data_quality_issues WHERE run_id >= $1", PREVIOUS_RUN_ID)
    await conn.execute(
        "DELETE FROM app.data_quality_runs WHERE run_id >= $1", PREVIOUS_RUN_ID)
    await conn.execute(
        "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
        [*pg_inventory_write.CHAIN_TABLES, JOURNAL])


@pytest_asyncio.fixture
async def ready(monkeypatch):
    """Everything the flip needs, as production would hold it on flip day:
    the six tables and the journal copied ten minutes ago, and this morning's
    comparison clean for chain 1 — with one finding elsewhere, which must not
    count. Yesterday's run failed in chain 1's own comparison and filed a lost
    movement; it is superseded, and must not count either."""
    monkeypatch.delenv("KS_WRITE_INVENTORY", raising=False)
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    async with p.acquire() as conn:
        await _clean_journal(conn)
        await conn.executemany(
            "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at, "
            "failures_since_ok, last_rows) VALUES ($1, now() - interval '10 minutes', "
            "now() - interval '10 minutes', 0, 1)",
            [(t,) for t in (*pg_inventory_write.CHAIN_TABLES, JOURNAL)])
        await conn.execute(
            "INSERT INTO app.data_quality_runs (run_id, started_at, ended_at, as_of, "
            "window_start, window_end, layer, status, error_message) VALUES ($1, "
            "now() - interval '26 hours', now() - interval '26 hours', now(), "
            "current_date - 1, current_date - 1, 'mirror_landing', 'FAILED', $2)",
            PREVIOUS_RUN_ID,
            "1 check(s) raised — reconcile_operational: ConnectionResetError: reset")
        await conn.execute(
            "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, severity, "
            "count) VALUES ($1, 'mirror_rows_lost', 'app.stock_movements', 'CRITICAL', 1)",
            PREVIOUS_RUN_ID)
        await conn.execute(
            "INSERT INTO app.data_quality_runs (run_id, started_at, ended_at, as_of, "
            "window_start, window_end, layer, status) VALUES ($1, now() - interval '2 hours', "
            "now() - interval '2 hours', now(), current_date, current_date, "
            "'mirror_landing', 'WARN')", RUN_ID)
        await conn.execute(
            "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, severity, "
            "count) VALUES ($1, 'mirror_buckets_disagree', 'app.order_versions', 'CRITICAL', 1)",
            RUN_ID)
    yield p
    async with p.acquire() as conn:
        await _clean_journal(conn)
    await p.close()


async def _finding(pool, table, check="mirror_rows_lost"):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, severity, "
            "count) VALUES ($1, $2, $3, 'CRITICAL', 1)", RUN_ID, check, table)


class TestPreflightOnTheRealSchema:
    @pytest.mark.asyncio
    async def test_a_ready_chain_is_ok_and_a_finding_elsewhere_does_not_count(self, ready):
        result = await pg_inventory_write.preflight(ready)
        assert result["reasons"] == [] and result["ok"] is True
        assert result["temporary"] is True                      # initdb grants it to ks_app
        assert result["operational_run"]["run_id"] == RUN_ID
        assert result["operational_run"]["findings"] == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table, check", [
        ("app.stock_movements", "mirror_buckets_disagree"),
        ("bronze.offer_stocks", "mirror_rows_lost"),
        ("pg_inventory_write", "chain_latch_disagrees"),     # filed against the chain
    ])
    async def test_an_open_finding_on_chain_1_is_not_ok(self, ready, table, check):
        await _finding(ready, table, check)
        result = await pg_inventory_write.preflight(ready)
        assert result["ok"] is False
        (reason,) = result["reasons"]
        assert f"{check} on {table}" in reason

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error_message, ok", [
        pytest.param("1 check(s) raised — reconcile_sms: ConnectionResetError: "
                     "reset by peer", True, id="another-store"),
        pytest.param("2 check(s) raised — reconcile_operational: OSError: refused "
                     "| reconcile_sms: ConnectionResetError: reset by peer", False,
                     id="chain-1s-own-comparison"),
    ])
    async def test_a_failed_run_blocks_only_when_chain_1_went_uncompared(
            self, ready, error_message, ok):
        """This morning's run failed. When the check that raised is another
        store's, chain 1's six tables were compared and came back clean, and
        the run failing is a note; when it is `reconcile_operational`, they
        were not compared at all."""
        async with ready.acquire() as conn:
            await conn.execute(
                "UPDATE app.data_quality_runs SET status = 'FAILED', error_message = $2 "
                "WHERE run_id = $1", RUN_ID, error_message)
        result = await pg_inventory_write.preflight(ready)
        assert result["ok"] is ok
        assert result["operational_run"]["run_id"] == RUN_ID
        assert result["operational_run"]["failed"] is True
        if ok:
            (note,) = result["notes"]
            assert "failed in reconcile_sms" in note
        else:
            (reason,) = result["reasons"]
            assert f"run ({RUN_ID}) failed in reconcile_operational" in reason

    @pytest.mark.asyncio
    async def test_a_copy_older_than_50_minutes_is_not_ok(self, ready):
        """On the database's clock alone. `last_ok_at` is Postgres' `now()`,
        and the preflight's default `now` is this process's: a container
        clock a few hundred milliseconds ahead of the laptop's floors 51 min
        to 50 (Docker Desktop, seen once). The limit is minutes; the test
        was measuring the skew."""
        async with ready.acquire() as conn:
            await conn.execute(
                "UPDATE meta.mirror_state SET last_ok_at = now() - interval '51 minutes' "
                "WHERE table_name = 'app.inventory_sku_history'")
            db_now = await conn.fetchval("SELECT now()")
        (reason,) = (await pg_inventory_write.preflight(ready, now=db_now))["reasons"]
        assert reason == "app.inventory_sku_history was last copied 51 min ago (limit 50)"

    @pytest.mark.asyncio
    @pytest.mark.skipif(not READONLY_DSN, reason=PROVISIONED)
    async def test_without_the_temporary_privilege_it_is_not_ok(self, ready):
        """A role that cannot create a temporary table — which is the state a
        cluster older than initdb's GRANT is in. `ks_app` holds the grant and
        cannot give it up (the database's owner granted it), so the role that
        lacks it by design stands in: `ks_readonly`, which reads everything the
        preflight reads and nothing else."""
        readonly = await asyncpg.create_pool(READONLY_DSN, min_size=1, max_size=1)
        try:
            result = await pg_inventory_write.preflight(readonly)
        finally:
            await readonly.close()
        assert result["ok"] is False and result["temporary"] is False
        (reason,) = result["reasons"]
        assert "ks_readonly cannot create a temporary table in ks" in reason
        assert "GRANT TEMPORARY ON DATABASE ks TO ks_readonly" in reason
