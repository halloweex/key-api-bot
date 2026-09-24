"""The Postgres derivation on its own signal, against a real Postgres.

Chain 2, steps 3–6 under `KS_PG_DERIVE=own`: landing writers raise the signal,
the job rebuilds Silver, Gold and the profile when something is owed, validates
what it built, journals every attempt, and on a failure leaves the rebuild owed
and says so.
"""
from __future__ import annotations

import json
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
            ["silver.orders", "gold.daily_revenue", "app.customer_profile",
             "meta.derivation_signal", "meta.derivation_runs"])


@pytest_asyncio.fixture
async def own(monkeypatch):
    from core import pg_derivation
    from core.scheduler import BackgroundScheduler

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_PG_DERIVE", "own")
    monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
    before = (pg_derivation._mode, pg_derivation._mode_error, BackgroundScheduler._pg_derive_ran,
              BackgroundScheduler._pg_derive_started, pg_derivation._signal_failures)
    pg_derivation.configure_mode()
    BackgroundScheduler._pg_derive_ran = False
    BackgroundScheduler._pg_derive_started = None
    pg_derivation._signal_failures = 0
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
    (pg_derivation._mode, pg_derivation._mode_error, BackgroundScheduler._pg_derive_ran,
     BackgroundScheduler._pg_derive_started, pg_derivation._signal_failures) = before


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
    async def test_the_first_tick_of_a_process_rebuilds_with_nothing_owed(self, own):
        """A deploy is exactly when an owed rebuild used to be lost, so a new
        process does not trust a debt settled by the old one: nothing owed,
        heartbeat not due, and it still rebuilds once — then goes quiet."""
        pool, scheduler, _a = own
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE meta.derivation_signal SET requested = 1, built = 1,"
                " built_at = now() WHERE layer = 'warehouse'")

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["trigger"] == "first_tick"
        assert await scheduler._run_pg_derivation() == {"skipped": True, "reason": "nothing owed"}

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


async def _watermark(pool, table):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_attempted_at, last_ok_at, failures_since_ok, last_error, last_rows"
            " FROM meta.mirror_state WHERE table_name = $1", table)


class TestEveryStageLeavesAWatermark:
    """DN-05a: Silver and Gold recorded their failures and the profile and the
    run's tail did not, so a failure there left every row in meta.mirror_state
    reading clean. Each is recorded now, and each is cleared by the success
    that follows it — a count nothing resets says only that it happened once."""

    @pytest.mark.asyncio
    async def test_the_profile_raising_is_recorded_and_the_next_rebuild_clears_it(self, own):
        pool, scheduler, raise_alert = own
        with patch("core.pg_vitrina.rebuild_customer_profile",
                   AsyncMock(side_effect=RuntimeError("profile boom"))):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "error" and result["stage"] == "app.customer_profile"
        state = await _watermark(pool, "app.customer_profile")
        assert state["failures_since_ok"] == 1 and "profile boom" in state["last_error"]
        assert state["last_ok_at"] is None
        assert await _signal(pool) == (1, 0), "a failed rebuild settled the debt"
        assert _alerted(raise_alert) == ["warehouse_pg:derive_failed"]

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success", result
        state = await _watermark(pool, "app.customer_profile")
        assert state["failures_since_ok"] == 0 and state["last_error"] is None
        assert state["last_ok_at"] == state["last_attempted_at"]
        assert state["last_rows"] == result["profile_rows"]

    @pytest.mark.asyncio
    async def test_the_tail_raising_is_recorded_against_the_journal_and_cleared(self, own):
        from core import pg_derivation

        pool, scheduler, _a = own
        with patch.object(pg_derivation, "validate",
                          AsyncMock(side_effect=RuntimeError("validate boom"))):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "error" and result["stage"] == "validation"
        state = await _watermark(pool, "meta.derivation_runs")
        assert state["failures_since_ok"] == 1 and "validate boom" in state["last_error"]
        assert (await _runs(pool))[-1]["error"].startswith("validation:")

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["validation_passed"] is True, result
        state = await _watermark(pool, "meta.derivation_runs")
        assert state["failures_since_ok"] == 0 and state["last_error"] is None
        assert state["last_ok_at"] == state["last_attempted_at"]

    @pytest.mark.asyncio
    async def test_a_tail_that_never_failed_has_no_row(self, own):
        pool, scheduler, _a = own
        assert (await scheduler._run_pg_derivation())["status"] == "success"
        assert await _watermark(pool, "meta.derivation_runs") is None

    @pytest.mark.asyncio
    async def test_an_unwritable_journal_does_not_lift_the_floor(self, own, monkeypatch):
        """The floor read the journal alone: a run whose row could not be
        written was followed by another on the next tick, every minute."""
        from core import pg_derivation
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "600")
        with patch.object(pg_derivation, "record_run",
                          AsyncMock(side_effect=RuntimeError("journal boom"))):
            first = await scheduler._run_pg_derivation()
            assert first["status"] == "error" and first["stage"] == "validation"
            assert await _runs(pool) == []
            await write_orders([_order(IDS[0])], [], replace_products=False)
            requested, built = await _signal(pool)
            assert requested > built, "the test needs a debt for the floor to hold"
            assert await scheduler._run_pg_derivation() == {"skipped": True, "reason": "floor"}

        state = await _watermark(pool, "meta.derivation_runs")
        assert state["failures_since_ok"] == 1 and "journal boom" in state["last_error"]

    @pytest.mark.asyncio
    async def test_an_older_journal_row_does_not_undercut_it(self, own, monkeypatch):
        """Production's journal is never empty. With a row older than the floor
        in it, the floor is the later of that row and this process's own
        unjournaled run — the earlier is the journal-only floor, which re-ran
        every minute."""
        from core import pg_derivation
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "600")
        long_ago = datetime.now(timezone.utc) - timedelta(minutes=11)
        async with pool.acquire() as conn:
            await pg_derivation.record_run(conn, trigger="heartbeat", started_at=long_ago,
                                           ended_at=long_ago, error="an earlier run")
        with patch.object(pg_derivation, "record_run",
                          AsyncMock(side_effect=RuntimeError("journal boom"))):
            first = await scheduler._run_pg_derivation()
            assert first["status"] == "error" and first["stage"] == "validation"
            await write_orders([_order(IDS[0])], [], replace_products=False)
            requested, built = await _signal(pool)
            assert requested > built, "the test needs a debt for the floor to hold"
            assert await scheduler._run_pg_derivation() == {"skipped": True, "reason": "floor"}

        assert [r["started_at"] for r in await _runs(pool)] == [long_ago]


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


async def _dropped(pool, *, ago: timedelta, failures: int = 2):
    """A mark failure as `_record_failure` would have left it, `ago` in the past."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO meta.mirror_state
                   (table_name, last_attempted_at, failures_since_ok, last_error)
            VALUES ('meta.derivation_signal', now() - $1::interval, $2,
                    'LockNotAvailableError: simulated')
            ON CONFLICT (table_name) DO UPDATE SET
                last_attempted_at = EXCLUDED.last_attempted_at,
                last_ok_at        = NULL,
                failures_since_ok = EXCLUDED.failures_since_ok,
                last_error        = EXCLUDED.last_error
            """,
            ago, failures)


async def _signal_state(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_attempted_at, last_ok_at, failures_since_ok, last_error"
            " FROM meta.mirror_state WHERE table_name = 'meta.derivation_signal'")


class TestADroppedMarkHeals:
    """DN-05b: a successful mark never touched the watermark and a failure only
    counted up, so one lock timeout read as failing for ever. A validated
    rebuild that began after the drop covered its rows, and says so."""

    @staticmethod
    async def _write(pool):
        from core.pg_landing import write_orders

        await write_orders([_order(IDS[0], total=120), _order(IDS[1], source_id=2, total=80)],
                           [], replace_products=False)

    @pytest.mark.asyncio
    async def test_a_drop_before_the_run_is_healed_by_a_validated_rebuild(self, own):
        pool, scheduler, _a = own
        await self._write(pool)
        await _dropped(pool, ago=timedelta(minutes=5))

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["validation_passed"] is True, result
        state = await _signal_state(pool)
        assert state["failures_since_ok"] == 0 and state["last_error"] is None
        # Both stamps moved in one statement: equal is what this table calls
        # healthy, and the constraint forbids a success after the attempt.
        assert state["last_ok_at"] is not None
        assert state["last_ok_at"] == state["last_attempted_at"]

    @pytest.mark.asyncio
    async def test_a_drop_recorded_during_the_run_is_not(self, own):
        """Its writer may commit after Silver's snapshot, so this rebuild
        cannot vouch for its rows; the next validated run will."""
        from core import pg_vitrina
        from core.pg_landing import _record_failure

        pool, scheduler, _a = own
        await self._write(pool)
        real = pg_vitrina.rebuild_customer_profile

        async def drop_while_running(pool=None):
            await _record_failure("meta.derivation_signal", "LockNotAvailableError: mid-run")
            return await real(pool)

        with patch("core.pg_vitrina.rebuild_customer_profile", drop_while_running):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["validation_passed"] is True, result
        state = await _signal_state(pool)
        assert state["failures_since_ok"] == 1 and "mid-run" in state["last_error"]
        assert state["last_ok_at"] is None

    @pytest.mark.asyncio
    async def test_a_drop_inside_the_margin_before_the_run_is_not(self, own):
        """The writers outside `_heavy_job_lock` commit a round trip after the
        drop is recorded; inside the margin that commit may postdate `started`."""
        pool, scheduler, _a = own
        await self._write(pool)
        await _dropped(pool, ago=timedelta(seconds=5))

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["validation_passed"] is True, result
        assert (await _signal_state(pool))["failures_since_ok"] == 2

    @pytest.mark.asyncio
    async def test_a_failed_derivation_does_not_heal(self, own):
        pool, scheduler, _a = own
        await _dropped(pool, ago=timedelta(minutes=5))

        with patch("core.pg_gold.rebuild_gold", AsyncMock(side_effect=RuntimeError("boom"))):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "error"
        state = await _signal_state(pool)
        assert state["failures_since_ok"] == 2 and state["last_ok_at"] is None

    @pytest.mark.asyncio
    async def test_a_rebuild_that_fails_validation_does_not_heal(self, own):
        pool, scheduler, _a = own
        await _dropped(pool, ago=timedelta(minutes=5))
        await self._write(pool)
        with TestValidation._tamper(
            "DELETE FROM gold.daily_revenue WHERE source_id IS NULL AND ctid IN"
            " (SELECT ctid FROM gold.daily_revenue WHERE source_id IS NULL LIMIT 1)"):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and result["validation_passed"] is False
        assert (await _signal_state(pool))["failures_since_ok"] == 2

    @pytest.mark.asyncio
    async def test_a_signal_that_never_dropped_a_mark_gains_no_row(self, own):
        pool, scheduler, _a = own
        await self._write(pool)
        assert (await scheduler._run_pg_derivation())["status"] == "success"
        assert await _signal_state(pool) is None

    @pytest.mark.asyncio
    async def test_a_healed_row_is_not_rewritten_by_every_later_run(self, own):
        """Only a row that owes something moves, so `last_ok_at` keeps saying
        when the drops were last covered, not when the last rebuild ran."""
        pool, scheduler, _a = own
        await self._write(pool)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.mirror_state"
                " (table_name, last_attempted_at, last_ok_at, failures_since_ok)"
                " VALUES ('meta.derivation_signal', now() - interval '2 hours',"
                "         now() - interval '2 hours', 0)")
        before = await _signal_state(pool)

        assert (await scheduler._run_pg_derivation())["validation_passed"] is True

        after = await _signal_state(pool)
        assert (after["last_ok_at"], after["last_attempted_at"]) == (
            before["last_ok_at"], before["last_attempted_at"])


# ─── DN-14: the high-water mark and the signal it lets us check ──────────────


async def _unmarked(monkeypatch, *orders):
    """Land orders the way a process whose mode was never configured would:
    through the real writer, raising no mark."""
    from core import pg_derivation
    from core.pg_landing import write_orders

    monkeypatch.setenv("KS_PG_DERIVE", "piggyback")
    pg_derivation.configure_mode()
    try:
        await write_orders(list(orders), [], replace_products=False)
    finally:
        monkeypatch.setenv("KS_PG_DERIVE", "own")
        pg_derivation.configure_mode()


async def _missed(pool):
    """What the twins file for the journal the real derivation wrote."""
    from core.pg_warehouse_dq import check_pg_warehouse, read_facts

    facts = await read_facts(pool=pool)
    assert facts.whole is None, facts.whole
    return [i for i in check_pg_warehouse(facts) if i.check_name == "pg_signal_missed"]


async def _high_water(pool, run_id):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT (validation ->> 'bronze_max_mirrored_at')::timestamptz"
            " FROM meta.derivation_runs WHERE id = $1", run_id)


class TestTheHighWaterMark:
    @pytest.mark.asyncio
    async def test_a_run_records_the_latest_mirrored_at_bronze_held(self, own):
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        await write_orders([_order(IDS[0]), _order(IDS[1], days_ago=3)], [],
                           replace_products=False)
        assert (await scheduler._run_pg_derivation())["status"] == "success"

        (run,) = await _runs(pool)
        async with pool.acquire() as conn:
            truth = await conn.fetchval("SELECT max(mirrored_at) FROM bronze.orders")
        text = json.loads(run["validation"])["bronze_max_mirrored_at"]
        assert datetime.fromisoformat(text) == truth           # to the microsecond
        assert await _high_water(pool, run["id"]) == truth     # and as Postgres reads it

    @pytest.mark.asyncio
    async def test_a_failed_run_records_none(self, own):
        pool, scheduler, _a = own
        with patch("core.pg_gold.rebuild_gold", AsyncMock(side_effect=RuntimeError("boom"))):
            await scheduler._run_pg_derivation()
        (run,) = await _runs(pool)
        assert run["error"] and run["validation"] is None


class TestTheSignalMissed:
    """The real derivation writes the journal; the twins read it back. Between
    two runs an order lands with no mark, and the second run's pair is the
    finding — unless that run is a process's first.

    Every test lands one order, marked, before its first run: over an empty
    bronze the high-water mark is None and no pair is judged, which would let
    a quiet-hour test pass on nothing."""

    @staticmethod
    async def _first_run(pool, scheduler):
        from core.pg_landing import write_orders

        await write_orders([_order(IDS[2], days_ago=5)], [], replace_products=False)
        assert (await scheduler._run_pg_derivation())["trigger"] == "signal"
        (run,) = await _runs(pool)
        assert await _high_water(pool, run["id"]) is not None
        return run

    @pytest.mark.asyncio
    async def test_an_unmarked_write_between_two_runs_is_a_warning_naming_the_run(
        self, own, monkeypatch,
    ):
        from core.data_quality import Severity

        pool, scheduler, _a = own
        await self._first_run(pool, scheduler)
        await _unmarked(monkeypatch, _order(IDS[0]))
        result = await scheduler._run_pg_derivation(force=True)
        assert result["trigger"] == "manual" and result["requested_seen"] == 2

        first, second = await _runs(pool)
        assert await _high_water(pool, second["id"]) > await _high_water(pool, first["id"])
        (issue,) = await _missed(pool)
        assert (issue.severity, issue.count, issue.sample_ids) == (
            Severity.WARN, 1, (second["id"],))

    @pytest.mark.asyncio
    async def test_a_marked_write_between_two_runs_is_nothing(self, own):
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        await self._first_run(pool, scheduler)
        await write_orders([_order(IDS[0])], [], replace_products=False)
        assert (await scheduler._run_pg_derivation())["trigger"] == "signal"
        first, second = await _runs(pool)
        assert await _high_water(pool, second["id"]) > await _high_water(pool, first["id"])
        assert await _missed(pool) == []

    @pytest.mark.asyncio
    async def test_a_quiet_hour_is_nothing(self, own):
        pool, scheduler, _a = own
        await self._first_run(pool, scheduler)
        await scheduler._run_pg_derivation(force=True)
        first, second = await _runs(pool)
        assert await _high_water(pool, second["id"]) == await _high_water(pool, first["id"])
        assert await _missed(pool) == []

    @pytest.mark.asyncio
    async def test_a_first_tick_after_an_unmarked_boot_write_is_nothing_and_the_next_is_judged(
        self, own, monkeypatch,
    ):
        """A process that came up, let its boot sync land an order unmarked
        (before DN-05b, the mode was read after the sync), and derived with
        nothing owed: that run is `first_tick` and not judged. An unmarked write
        after it is."""
        from core.scheduler import BackgroundScheduler

        pool, scheduler, _a = own
        first = await self._first_run(pool, scheduler)
        await _unmarked(monkeypatch, _order(IDS[0]))
        BackgroundScheduler._pg_derive_ran = False              # the restart
        assert (await scheduler._run_pg_derivation())["trigger"] == "first_tick"
        tick = (await _runs(pool))[-1]
        assert await _high_water(pool, tick["id"]) > await _high_water(pool, first["id"])
        assert tick["requested_seen"] == first["requested_seen"]
        assert await _missed(pool) == []

        await _unmarked(monkeypatch, _order(IDS[1]))
        await scheduler._run_pg_derivation(force=True)
        runs = await _runs(pool)
        (issue,) = await _missed(pool)
        assert issue.sample_ids == (runs[-1]["id"],)


# ─── DN-19: under KS_UTM_PARSE=postgres the UTM parse is the last step ────────

UTM_COMMENT = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"


def _commented(oid, *, comment=UTM_COMMENT, days_ago=2):
    return _order(oid, days_ago=days_ago)._replace(manager_comment=comment)


@pytest_asyncio.fixture
async def utm_postgres(own, monkeypatch):
    """`own`, plus KS_UTM_PARSE=postgres. The table and its watermark row are
    shared with every other suite on this server, so what was there before is
    put back: rows this run added are removed, and the watermark row restored
    as it stood."""
    from core import pg_utm_parse
    from core.pg_order_utm import UTM_TABLE

    pool, _s, _a = own
    before_mode = (pg_utm_parse._mode, pg_utm_parse._mode_error)
    monkeypatch.setenv(pg_utm_parse.ENV, "postgres")
    assert pg_utm_parse.configure_mode() == "postgres"
    async with pool.acquire() as conn:
        kept = {r["order_id"] for r in await conn.fetch(f"SELECT order_id FROM {UTM_TABLE}")}
        watermark = await conn.fetchrow(
            "SELECT * FROM meta.mirror_state WHERE table_name = $1", UTM_TABLE)
        await conn.execute(f"DELETE FROM {UTM_TABLE} WHERE order_id = ANY($1::int[])",
                           list(IDS))
    try:
        yield own
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                f"DELETE FROM {UTM_TABLE} WHERE NOT (order_id = ANY($1::int[]))"
                " OR order_id = ANY($2::int[])", sorted(kept - set(IDS)), list(IDS))
            await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1",
                               UTM_TABLE)
            if watermark is not None:
                columns = list(watermark.keys())
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                    *watermark.values())
        pg_utm_parse._mode, pg_utm_parse._mode_error = before_mode


async def _utm_row(pool, oid):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT utm_campaign, traffic_type, platform, parsed_at"
            " FROM silver.order_utm WHERE order_id = $1", oid)


async def _utm_watermark(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_ok_at, failures_since_ok, last_error FROM meta.mirror_state"
            " WHERE table_name = 'silver.order_utm'")


def _no_ship():
    """A ship that fails the test if anything reaches it."""
    return patch("core.pg_order_utm.ship_order_utm",
                 new=AsyncMock(side_effect=AssertionError("shipped under postgres")))


class TestTheUtmParseIsTheLastStep:
    @pytest.mark.asyncio
    async def test_a_mirrored_order_with_a_utm_comment_has_its_row_in_the_same_run(
        self, utm_postgres,
    ):
        from core.pg_landing import write_orders

        pool, scheduler, raise_alert = utm_postgres
        order = _commented(IDS[0])
        await write_orders([order], [], replace_products=False)

        with _no_ship():
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "success", result
        assert result["order_utm"]["parsed"] >= 1
        row = await _utm_row(pool, IDS[0])
        assert row is not None, "the order's verdict did not land in the run"
        assert (row["utm_campaign"], row["traffic_type"], row["platform"]) == (
            "spring", "paid_confirmed", "facebook")
        assert row["parsed_at"] == order.updated_at
        stamp = await _utm_watermark(pool)
        assert stamp["last_ok_at"] is not None and stamp["failures_since_ok"] == 0
        assert raise_alert.await_count == 0

    @pytest.mark.asyncio
    async def test_a_gold_fault_does_not_stop_it(self, utm_postgres):
        """After step 9 this is the only classifier; a Gold fault must not
        file new orders as organic for its whole length."""
        from core.pg_landing import write_orders

        pool, scheduler, _a = utm_postgres
        await write_orders([_commented(IDS[1])], [], replace_products=False)

        with _no_ship(), patch("core.pg_gold.rebuild_gold",
                               new=AsyncMock(side_effect=RuntimeError("gold boom"))):
            result = await scheduler._run_pg_derivation()

        assert result["status"] == "error" and result["stage"] == "gold.daily_revenue"
        assert (await _runs(pool))[-1]["error"].startswith("gold.daily_revenue")
        assert result["order_utm"]["parsed"] >= 1
        assert (await _utm_row(pool, IDS[1])) is not None

    @pytest.mark.asyncio
    async def test_the_refresh_door_parses_a_new_order_without_a_derivation(
        self, utm_postgres,
    ):
        """`POST /api/traffic/refresh`'s half in Postgres: the incremental,
        under the layer lock, reading `bronze.orders` — the DuckDB store is
        not read at all."""
        from core.pg_landing import write_orders
        from core.pg_utm_parse import reparse_router

        pool, _s, _a = utm_postgres
        await write_orders([_commented(IDS[2])], [], replace_products=False)

        with _no_ship():
            result = await reparse_router(store=None)

        assert result["parsed"] >= 1, result
        assert (await _utm_row(pool, IDS[2]))["utm_campaign"] == "spring"

    @pytest.mark.asyncio
    async def test_under_duckdb_the_derivation_parses_nothing(self, own):
        """The default, byte for byte: the derivation's result carries no parse
        and no verdict appears; the table stays the tick's copy."""
        from core import pg_utm_parse
        from core.pg_landing import write_orders

        pool, scheduler, _a = own
        assert not pg_utm_parse.parses_in_postgres()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM silver.order_utm WHERE order_id = ANY($1::int[])", list(IDS))
        await write_orders([_commented(IDS[0])], [], replace_products=False)

        result = await scheduler._run_pg_derivation()

        assert result["status"] == "success" and "order_utm" not in result
        assert await _utm_row(pool, IDS[0]) is None
