"""Chains 11a/11b against a real Postgres: the ledgers written and read there alone.

- one record puts the row DuckDB's shadow held in Postgres, and the upsert
  rewrites a week as DuckDB's `INSERT OR REPLACE` did — the literal is
  DuckDB's row on the base `e3112a7d`, the last tree with a shadow (step
  14's rule 9);
- the gate reads Postgres and drains a spool into it with its `sent_at`;
- end to end through the sales report's job, against this Postgres: a
  delivery whose record Postgres refuses three times is spooled, a tick with
  Postgres still refusing counts the spooled week as sent, the next drains it
  and the one after reads Postgres's row — one message in all (OD-16 (a));
- the standing watch reads a missing week and a NULL `sent_at`.

The way back — a Postgres-only week carried into DuckDB, a spool landed before
the copy — went with the copy-back in step 14's S14-04; the shadow, its daily
comparison and the adoption of a week only DuckDB held with S14-10.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import (
    chain_latch, pg_traffic_ledger_write, pg_weekly_ledger_write, report_ledger,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

WEEK = date(2026, 9, 21)
CHAINS = [(report_ledger.WEEKLY, pg_weekly_ledger_write),
          (report_ledger.TRAFFIC, pg_traffic_ledger_write)]


async def _clean(pool):
    """Both ledgers and the chains' owner rows — the throwaway database's."""
    async with pool.acquire() as conn:
        for _, chain in CHAINS:
            await conn.execute(f"DELETE FROM {chain.TABLE}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def pool(monkeypatch):
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setattr(report_ledger, "RETRY_DELAYS", (0, 0, 0))
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    await pool.close()


def _utc(row):
    return tuple(v.astimezone(timezone.utc) if isinstance(v, datetime) else v for v in row)


async def _rows(pool, chain):
    cols = "week_start, sales_type, revenue, orders, sent_at"
    async with pool.acquire() as conn:
        return sorted(_utc(tuple(r)) for r in await conn.fetch(
            f"SELECT {cols} FROM {chain.TABLE}"))


# DuckDB's row after the same two records on the base, shadow on (rule 9):
# week, sales type, revenue, orders — `sent_at` is the instant of the call.
DUCKDB_LEDGER_ROW = (date(2026, 9, 21), "retail", Decimal("99.00"), 2)


class TestOneRecord:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger,chain", CHAINS)
    async def test_the_row_and_the_upsert_are_duckdbs(self, pool, ledger, chain):
        before = datetime.now(timezone.utc)
        assert await report_ledger.mark_sent(ledger, WEEK, "retail", 1234.565, 7) == "recorded"
        assert await report_ledger.mark_sent(ledger, WEEK, "retail", 99.0, 2) == "recorded"
        (row,) = await _rows(pool, chain)
        assert row[:4] == DUCKDB_LEDGER_ROW
        assert before <= row[4] <= datetime.now(timezone.utc)
        assert await report_ledger.already_sent(ledger, WEEK, "retail")
        assert not await report_ledger.already_sent(ledger, WEEK, "b2b")
        assert chain_latch.latched(chain.CHAIN)


class TestTheGate:
    @pytest.mark.asyncio
    async def test_a_spool_drains_into_postgres_with_its_sent_at(self, pool):
        chain = pg_weekly_ledger_write
        went_out = datetime(2026, 9, 28, 6, 30, tzinfo=timezone.utc)
        report_ledger.spool(chain.CHAIN, WEEK, "retail", Decimal("5.00"), 1, went_out)
        assert await report_ledger.already_sent(report_ledger.WEEKLY, WEEK, "retail")
        assert await _rows(pool, chain) == [(WEEK, "retail", Decimal("5.00"), 1, went_out)]
        assert not report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")

    @pytest.mark.asyncio
    async def test_a_week_postgres_does_not_hold_is_not_sent(self, pool):
        assert not await report_ledger.already_sent(report_ledger.TRAFFIC, WEEK, "retail")


@pytest_asyncio.fixture
async def warehouse(pool, monkeypatch):
    """Postgres' Gold and Silver, emptied, for the sales report's reads, on
    the same database the ledger is written to."""
    from tests.integration.pg_tab_seed import tab_in_postgres

    async def _nothing(conn, pool):
        pass

    async with tab_in_postgres(DSN, monkeypatch, _nothing) as gold:
        # `tab_in_postgres` points `core.pg.get_pool` at its own pool; the
        # ledger's writer asks the same patched name, so both land here.
        yield gold


class TestTheJobSendsOnce:
    @pytest.mark.asyncio
    async def test_across_a_refusal_a_spool_and_a_drain_the_week_goes_out_once(
            self, pool, warehouse, tmp_path, monkeypatch):
        """The weekly report's own job and wiring against this Postgres: the
        delivery, then the record refused three times — spooled — then a tick
        with Postgres still refusing the drain, a tick that drains, and a
        tick that finds Postgres's row. Mutation: drop the spool branch from
        the gate (two messages), or let the drain stamp now() (the row's
        `sent_at` would be the drain's)."""
        from tests.unit.test_weekly_report import TestSchedulerJob, _store

        chain = pg_weekly_ledger_write
        real = chain.mark_sent
        refusals = {"left": 3}

        async def mark_sent(*args, **kwargs):
            if refusals["left"]:
                refusals["left"] -= 1
                raise ConnectionRefusedError("Postgres restarting")
            await real(*args, **kwargs)

        monkeypatch.setattr(chain, "mark_sent", mark_sent)
        store = await _store(tmp_path)
        try:
            job = TestSchedulerJob()
            week_start = await job._seed(warehouse, complete=True)
            sent = []
            scheduler = job._wire(monkeypatch, store, sent, tmp_path)

            first = await scheduler._run_weekly_report()
            assert first["sent"] is True and first["ledger"] == "pending"
            (spooled,) = report_ledger._spooled(chain.CHAIN)
            assert await _rows(pool, chain) == []

            quiet = {"sent": False, "reason": "already_sent", "week": week_start.isoformat()}
            refusals["left"] = 1
            assert await scheduler._run_weekly_report() == quiet        # still spooled
            assert report_ledger.is_spooled(chain.CHAIN, week_start, "retail")

            assert await scheduler._run_weekly_report() == quiet        # drained
            assert not report_ledger.is_spooled(chain.CHAIN, week_start, "retail")
            (row,) = await _rows(pool, chain)
            assert row[0] == week_start and row[4] == spooled["sent_at"]

            assert await scheduler._run_weekly_report() == quiet        # Postgres's row
            assert len(sent) == 1
        finally:
            await store.close()


class TestTheStandingWatch:
    @pytest.mark.asyncio
    async def test_a_missing_week_and_a_null_sent_at_are_read(self, pool):
        from core import pg_chain_invariants as inv

        chain = pg_weekly_ledger_write
        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {chain.TABLE} VALUES ('2020-01-06', 'retail', 1, 1, NULL)")
            led = await inv._read_ledger(conn, chain, date(2026, 9, 30))
        assert led.nulls.counts == {"sent_at": 1}
        assert led.due and not led.has_week and led.week_start == WEEK
        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {chain.TABLE} VALUES ($1, 'retail', 1, 1, now())", WEEK)
            led = await inv._read_ledger(conn, chain, date(2026, 9, 30))
        assert led.has_week
