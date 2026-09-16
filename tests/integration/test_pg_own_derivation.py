"""The Postgres derivation on its own signal, against a real Postgres.

Chain 2, steps 3–6 under `KS_PG_DERIVE=own`: landing writers raise the signal,
the job rebuilds Silver, Gold and the profile when something is owed, validates
what it built, journals every attempt, and on a failure leaves the rebuild owed
and says so.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

IDS = (970001, 970002, 970003)
DERIVED = ("silver.orders", "gold.daily_revenue", "app.customer_profile")


def _order(oid, *, source_id=1, total=100.0, days_ago=2, is_return=False):
    from core.landing_rows import OrderRow

    at = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return OrderRow(id=oid, source_id=source_id, status_id=19 if is_return else 1,
                    status_group_id=6 if is_return else 1, grand_total=total,
                    ordered_at=at, created_at=at, updated_at=at, buyer_id=None,
                    manager_id=None, manager_comment=None, promocode=None)


async def _reset(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", list(IDS))
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", list(IDS))
        for table in DERIVED:
            await conn.execute(f"TRUNCATE {table}")
        await conn.execute(
            "UPDATE meta.derivation_signal SET requested = 1, built = 0, built_at = NULL"
            " WHERE layer = 'warehouse'")
        await conn.execute("DELETE FROM meta.derivation_runs")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            ["silver.orders", "gold.daily_revenue", "meta.derivation_signal"])


@pytest_asyncio.fixture
async def own(monkeypatch):
    from core import pg_derivation
    from core.scheduler import BackgroundScheduler

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_PG_DERIVE", "own")
    monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
    before = (pg_derivation._mode, pg_derivation._mode_error, BackgroundScheduler._pg_derive_ran)
    pg_derivation.configure_mode()
    BackgroundScheduler._pg_derive_ran = False
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=8)
    await _reset(pool)
    raise_alert, resolve = AsyncMock(return_value=1), AsyncMock(return_value=0)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()), \
         patch("core.alerting.raise_alert", raise_alert), \
         patch("core.alerting.resolve_group", resolve):
        yield pool, BackgroundScheduler(), raise_alert
    await _reset(pool)
    await pool.close()
    pg_derivation._mode, pg_derivation._mode_error, BackgroundScheduler._pg_derive_ran = before


async def _signal(pool):
    async with pool.acquire() as conn:
        return tuple(await conn.fetchrow(
            "SELECT requested, built FROM meta.derivation_signal WHERE layer = 'warehouse'"))


async def _runs(pool):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM meta.derivation_runs ORDER BY id")


def _alerted(raise_alert):
    return [c.kwargs["conditions"][0] for c in raise_alert.await_args_list]


class TestTheWritersRaiseTheSignal:
    @pytest.mark.asyncio
    async def test_under_own_landing_orders_owes_a_rebuild(self, own):
        from core.pg_landing import write_orders

        pool, _s, _a = own
        await write_orders([_order(IDS[0])], [], replace_products=False)
        assert await _signal(pool) == (2, 0)

    @pytest.mark.asyncio
    async def test_under_piggyback_it_does_not(self, own, monkeypatch):
        from core import pg_derivation
        from core.pg_landing import write_orders

        pool, _s, _a = own
        monkeypatch.setenv("KS_PG_DERIVE", "piggyback")
        pg_derivation.configure_mode()
        await write_orders([_order(IDS[0])], [], replace_products=False)
        assert await _signal(pool) == (1, 0)


class TestTheJob:
    @pytest.mark.asyncio
    async def test_a_run_builds_everything_validates_and_settles_the_debt(self, own):
        from core.pg_landing import write_orders

        pool, scheduler, raise_alert = own
        await write_orders([_order(IDS[0], total=120), _order(IDS[1], source_id=2, total=80),
                            _order(IDS[2], total=50, is_return=True)], [], replace_products=False)

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success", result
        assert result["validation_passed"] is True
        assert result["silver_rows"] >= 3
        requested, built = await _signal(pool)
        assert requested == built == 2
        runs = await _runs(pool)
        assert len(runs) == 1 and runs[0]["error"] is None
        assert runs[0]["validation_passed"] is True
        assert raise_alert.await_count == 0

    @pytest.mark.asyncio
    async def test_nothing_owed_after_a_run_is_quiet(self, own):
        pool, scheduler, _a = own
        assert (await scheduler._run_pg_derivation())["status"] == "success"
        assert await scheduler._run_pg_derivation() == {"skipped": True, "reason": "nothing owed"}
        assert len(await _runs(pool)) == 1

    @pytest.mark.asyncio
    async def test_a_mark_after_a_run_is_owed_again(self, own):
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        await scheduler._run_pg_derivation()
        await write_orders([_order(IDS[0])], [], replace_products=False)
        result = await scheduler._run_pg_derivation()
        assert result["status"] == "success" and result["trigger"] == "signal"
        requested, built = await _signal(pool)
        assert requested == built == 2

    @pytest.mark.asyncio
    async def test_the_floor_holds_an_owed_rebuild(self, own, monkeypatch):
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        await scheduler._run_pg_derivation()
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "600")
        await write_orders([_order(IDS[0])], [], replace_products=False)
        assert await scheduler._run_pg_derivation() == {"skipped": True, "reason": "floor"}
        assert await _signal(pool) == (2, 1)


class TestAFailureLeavesTheRebuildOwed:
    @pytest.mark.asyncio
    async def test_gold_raising(self, own):
        pool, scheduler, raise_alert = own
        with patch("core.pg_gold.rebuild_gold", AsyncMock(side_effect=RuntimeError("boom"))):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "error" and result["stage"] == "gold.daily_revenue"
        assert await _signal(pool) == (1, 0), "a failed rebuild settled the debt"
        runs = await _runs(pool)
        assert len(runs) == 1 and "gold.daily_revenue" in runs[0]["error"]
        async with pool.acquire() as conn:
            failures = await conn.fetchval(
                "SELECT failures_since_ok FROM meta.mirror_state WHERE table_name = 'gold.daily_revenue'")
        assert failures and failures >= 1
        assert _alerted(raise_alert) == ["warehouse_pg:derive_failed"]


class TestValidation:
    @staticmethod
    def _tamper(sql):
        """Run the real profile rebuild, then damage Gold before validation."""
        from core import pg_vitrina

        real = pg_vitrina.rebuild_customer_profile

        async def hook(pool=None):
            out = await real(pool)
            async with pool.acquire() as conn:
                await conn.execute(sql)
            return out
        return patch("core.pg_vitrina.rebuild_customer_profile", hook)

    @pytest.mark.asyncio
    async def test_a_missing_roll_up_cell_fails_validation(self, own):
        from core.pg_landing import write_orders

        pool, scheduler, raise_alert = own
        await write_orders([_order(IDS[0]), _order(IDS[1], days_ago=3)], [], replace_products=False)
        with self._tamper(
            "DELETE FROM gold.daily_revenue WHERE source_id IS NULL AND ctid IN"
            " (SELECT ctid FROM gold.daily_revenue WHERE source_id IS NULL LIMIT 1)"):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["validation_passed"] is False
        run = (await _runs(pool))[-1]
        assert run["validation_passed"] is False
        assert "warehouse_pg:validation_failed" in _alerted(raise_alert)
        # Validation reports; it does not hold the debt — the next owed rebuild
        # is the retry, as the design says.
        requested, built = await _signal(pool)
        assert requested == built

    @pytest.mark.asyncio
    async def test_an_unknown_sales_type_is_reported_apart(self, own):
        from core.pg_landing import write_orders

        pool, scheduler, raise_alert = own
        await write_orders([_order(IDS[0], total=333)], [], replace_products=False)
        with self._tamper(
            "UPDATE gold.daily_revenue SET sales_type = 'mystery' WHERE ctid IN"
            " (SELECT ctid FROM gold.daily_revenue WHERE source_id IS NULL AND revenue > 0 LIMIT 1)"):
            await scheduler._run_pg_derivation()
        assert "warehouse_pg:sales_type_partition" in _alerted(raise_alert)
