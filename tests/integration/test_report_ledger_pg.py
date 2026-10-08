"""Chains 11a/11b against a real Postgres: the ledgers written there, shadowed in DuckDB.

- one record puts the same row in both stores, `sent_at` included, and the
  upsert rewrites a week as DuckDB's `INSERT OR REPLACE` does;
- the gate reads Postgres, drains a spool into it, and adopts a row only
  DuckDB holds;
- the daily comparison is silent on an honest shadow and names a failed one;
- the standing watch reads a missing week and a NULL `sent_at`;
- the way back carries a Postgres-only week into DuckDB and releases.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import (
    chain_latch, pg_traffic_ledger_write, pg_weekly_ledger_write, report_ledger,
    shadow_writes,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

WEEK = date(2026, 9, 21)
CHAINS = [(report_ledger.WEEKLY, pg_weekly_ledger_write),
          (report_ledger.TRAFFIC, pg_traffic_ledger_write)]


async def _clean(pool):
    async with pool.acquire() as conn:
        for _, chain in CHAINS:
            await conn.execute(f"DELETE FROM {chain.TABLE}")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    from core import write_chains
    from core.duckdb_store import DuckDBStore

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setattr(report_ledger, "RETRY_DELAYS", (0, 0, 0))
    shadow_writes.reset()
    store = DuckDBStore(db_path=tmp_path / "ledger.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()
    shadow_writes.reset()


def _utc(row):
    return tuple(v.astimezone(timezone.utc) if isinstance(v, datetime) else v for v in row)


async def _both(store, pool, chain):
    cols = "week_start, sales_type, revenue, orders, sent_at"
    async with pool.acquire() as conn:
        pg = sorted(_utc(tuple(r)) for r in await conn.fetch(f"SELECT {cols} FROM {chain.TABLE}"))
    async with store.connection() as conn:
        dk = sorted(_utc(r) for r in conn.execute(
            f"SELECT {cols} FROM {chain.TABLE.split('.', 1)[1]}").fetchall())
    return pg, dk


class TestOneRecordTwoStores:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger,chain", CHAINS)
    async def test_the_same_row_and_the_upsert(self, stores, ledger, chain):
        store, pool, env = stores
        env.setenv(chain.WRITE_ENV, "postgres")
        assert await report_ledger.mark_sent(store, ledger, WEEK, "retail", 1234.565, 7) == "recorded"
        assert await report_ledger.mark_sent(store, ledger, WEEK, "retail", 99.0, 2) == "recorded"
        pg, dk = await _both(store, pool, chain)
        assert pg == dk and len(pg) == 1
        assert pg[0][2:4] == (Decimal("99.00"), 2)
        assert await report_ledger.already_sent(store, ledger, WEEK, "retail")
        assert not await report_ledger.already_sent(store, ledger, WEEK, "b2b")
        assert chain_latch.latched(chain.CHAIN)


class TestTheGate:
    @pytest.mark.asyncio
    async def test_a_spool_drains_into_postgres_with_its_sent_at(self, stores):
        store, pool, env = stores
        chain = pg_weekly_ledger_write
        env.setenv(chain.WRITE_ENV, "postgres")
        went_out = datetime(2026, 9, 28, 6, 30, tzinfo=timezone.utc)
        report_ledger.spool(chain.CHAIN, WEEK, "retail", Decimal("5.00"), 1, went_out)
        assert await report_ledger.already_sent(store, report_ledger.WEEKLY, WEEK, "retail")
        pg, dk = await _both(store, pool, chain)
        assert pg == dk == [(WEEK, "retail", Decimal("5.00"), 1, went_out)]
        assert not report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")

    @pytest.mark.asyncio
    async def test_a_duckdb_only_week_is_adopted(self, stores):
        store, pool, env = stores
        chain = pg_traffic_ledger_write
        env.setenv(chain.WRITE_ENV, "postgres")
        from core.traffic_report import mark_sent as dk_mark

        async with store.connection() as conn:
            dk_mark(conn, WEEK, "retail", 12.5, 4)
        assert await report_ledger.already_sent(store, report_ledger.TRAFFIC, WEEK, "retail")
        pg, dk = await _both(store, pool, chain)
        assert pg == dk


class TestTheShadowIsCompared:
    @pytest.mark.asyncio
    async def test_honest_then_failed(self, stores):
        from core.mirror_reconciliation import reconcile_operational

        store, pool, env = stores
        chain = pg_weekly_ledger_write
        env.setenv(chain.WRITE_ENV, "postgres")
        went_out = datetime.now(timezone.utc) - timedelta(hours=5)
        report_ledger.spool(chain.CHAIN, WEEK, "retail", Decimal("5.00"), 1, went_out)
        await report_ledger.drain(store, report_ledger.WEEKLY)
        assert [i for i in await reconcile_operational(store)
                if i.table_name == chain.TABLE] == []
        async with store.connection() as conn:
            conn.execute("DELETE FROM weekly_report_sends")
        found = {i.check_name for i in await reconcile_operational(store)
                 if i.table_name == chain.TABLE}
        assert found == {"shadow_missing_in_duckdb"}


class TestTheStandingWatch:
    @pytest.mark.asyncio
    async def test_a_missing_week_and_a_null_sent_at_are_read(self, stores):
        from core import pg_chain_invariants as inv

        store, pool, env = stores
        chain = pg_weekly_ledger_write
        env.setenv(chain.WRITE_ENV, "postgres")
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


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_a_postgres_only_week_comes_back(self, stores):
        from core.chain_transfer import copy_back

        store, pool, env = stores
        chain = pg_traffic_ledger_write
        env.setenv(chain.WRITE_ENV, "postgres")
        went_out = datetime.now(timezone.utc) - timedelta(hours=5)
        await chain.mark_sent(WEEK, "retail", Decimal("7.00"), 2, went_out)  # no shadow
        result = await copy_back(store, chain, dry_run=False)
        assert result["findings"] == [] and result["released"] is True, result
        pg, dk = await _both(store, pool, chain)
        assert pg == dk and len(dk) == 1
        assert not chain_latch.latched(chain.CHAIN)


class TestTheWayBackLandsTheSpool:
    """The review's first reproduction: a week delivered under a latched chain
    and spooled, then a copy-back. The handover said "every row DuckDB holds
    is in Postgres", the copy released, and the week was in neither store."""

    async def _latched_with_a_spooled_week(self, store, env, chain, ledger):
        env.setenv(chain.WRITE_ENV, "postgres")
        await report_ledger.mark_sent(store, ledger, WEEK - timedelta(days=7),
                                      "retail", 10.0, 1)          # latches
        real = chain.mark_sent
        env.setattr(chain, "mark_sent", AsyncMock(side_effect=ConnectionError("pg down")))
        assert await report_ledger.mark_sent(store, ledger, WEEK, "retail", 20.0, 2) == "pending"
        env.setattr(chain, "mark_sent", real)
        return report_ledger._spooled(chain.CHAIN)[0]["sent_at"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger,chain", CHAINS)
    async def test_execute_lands_it_before_the_copy(self, stores, ledger, chain):
        """Mutation: drop `_land_spool` from `copy_back` — the release still
        comes, and the week is in neither store."""
        from core.chain_transfer import copy_back

        store, pool, env = stores
        spooled_at = await self._latched_with_a_spooled_week(store, env, chain, ledger)
        result = await copy_back(store, chain, dry_run=False)
        assert result["findings"] == [] and result["released"] is True, result
        assert result["landed_from_spool"] == 1
        assert chain.pending()["count"] == 0
        pg, dk = await _both(store, pool, chain)
        assert pg == dk
        assert (WEEK, "retail", Decimal("20.00"), 2, spooled_at) in dk
        env.setenv(chain.WRITE_ENV, "duckdb")
        assert await report_ledger.already_sent(store, ledger, WEEK, "retail")

    @pytest.mark.asyncio
    async def test_the_dry_run_and_the_handover_report_it_and_land_nothing(self, stores):
        """Mutation: land in the dry run (it would write Postgres on the one
        call that promises to read), or drop the spool from either report."""
        from core.chain_transfer import copy_back, handover_check

        store, pool, env = stores
        chain, ledger = pg_weekly_ledger_write, report_ledger.WEEKLY
        await self._latched_with_a_spooled_week(store, env, chain, ledger)
        plan = await copy_back(store, chain, dry_run=True)
        assert "handover_ledger_spooled" in {f["check"] for f in plan["handover"]}
        assert "landed_from_spool" not in plan
        assert {i.check_name for i in await handover_check(store, chain)} >= {
            "handover_ledger_spooled"}
        assert report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                f"SELECT count(*) FROM {chain.TABLE} WHERE week_start = $1", WEEK) == 0

    @pytest.mark.asyncio
    async def test_a_week_postgres_will_not_take_refuses_the_copy(self, stores):
        """Mutation: ignore what is still spooled after the landing — the copy
        releases the chain with the week in a file."""
        from core.chain_transfer import CopyBackRefused, copy_back

        store, pool, env = stores
        chain, ledger = pg_traffic_ledger_write, report_ledger.TRAFFIC
        await self._latched_with_a_spooled_week(store, env, chain, ledger)
        env.setattr(chain, "mark_sent", AsyncMock(side_effect=ConnectionError("pg down")))
        before = await _both(store, pool, chain)
        with pytest.raises(CopyBackRefused):
            await copy_back(store, chain, dry_run=False)
        assert await _both(store, pool, chain) == before
        assert chain_latch.latched(chain.CHAIN)
        assert report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")

    @pytest.mark.asyncio
    async def test_a_file_that_will_not_parse_refuses_before_anything_is_written(
            self, stores):
        """CRITICAL in the handover, so `--execute` refuses at exit 2 with
        nothing landed. Mutation: leave the spool out of `copy_back`'s own
        handover — it would land the readable weeks first and refuse after."""
        from core.chain_transfer import CopyBackRefused, copy_back, handover_check

        store, pool, env = stores
        chain, ledger = pg_weekly_ledger_write, report_ledger.WEEKLY
        await self._latched_with_a_spooled_week(store, env, chain, ledger)
        bad = report_ledger._spool_path(chain.CHAIN, WEEK + timedelta(days=7), "retail")
        bad.write_text("{", encoding="utf-8")
        found = {i.check_name: i.severity.value for i in await handover_check(store, chain)}
        assert found["handover_ledger_spool_unreadable"] == "CRITICAL"
        with pytest.raises(CopyBackRefused) as refused:
            await copy_back(store, chain, dry_run=False)
        assert "handover_ledger_spool_unreadable" in {i.check_name for i in refused.value.issues}
        assert report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")      # nothing landed
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                f"SELECT count(*) FROM {chain.TABLE} WHERE week_start = $1", WEEK) == 0
