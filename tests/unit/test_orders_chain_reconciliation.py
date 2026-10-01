"""Under chain 3, DuckDB's arm of `dq_reconciliation` stands down (G12, G13).

Once `KS_WRITE_ORDERS` moves the orders to Postgres, DuckDB stops receiving
them, and its arm of the 05:30 job would read every order created since the
flip as MISSING and every status change as STATUS_DRIFT: a CRITICAL page every
morning, and a repair re-fetching all of them from KeyCRM, 200 a day for ever.
So the arm is not extracted, persisted, paged or resolved; the Postgres arm,
classified against the same snapshot, drives the repair; and the layer it no
longer writes is neither paged by the canary, nor caught up at start, nor
digested. Each test names the mutation it exists to fail on.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import chain_latch, pg_orders_write
from core.data_quality import Discrepancy, DiscrepancyClass, Severity
from core.scheduler import BackgroundScheduler


@pytest.fixture
def moved(monkeypatch):
    """Chain 3 latched: its writes go to Postgres whatever else holds."""
    monkeypatch.delenv("KS_WRITE_ORDERS", raising=False)
    chain_latch.latch(pg_orders_write.CHAIN)
    return monkeypatch


@pytest.fixture
def off(monkeypatch):
    monkeypatch.delenv("KS_WRITE_ORDERS", raising=False)
    return monkeypatch


class _Conn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


class _Store:
    def connection(self):
        return _Conn()


def _missing(*ids):
    return Discrepancy(
        month="2026-09", source_id=4, diff_class=DiscrepancyClass.MISSING_IN_DK,
        field="orders", dk_value=0, kc_value=len(ids), severity=Severity.CRITICAL,
        order_ids=tuple(ids),
    )


async def _run(*, duckdb_orders, persist, pg_result, classify=()):
    sync = AsyncMock()
    sync.repair_orders = AsyncMock(return_value={"repaired": 1, "remaining": 0})
    sched = BackgroundScheduler()
    sched._send_dq_alert_throttled = AsyncMock()
    sched._resolve_dq_layer = AsyncMock()
    sched._reconcile_postgres = AsyncMock(return_value=pg_result)
    sched._persist_postgres_reconciliation = AsyncMock()
    sched._reconcile_clickhouse = AsyncMock(return_value=None)
    with patch("core.reconciliation_io.keycrm_orders_in_window",
               new=AsyncMock(return_value=({}, 3, set()))), \
         patch("core.reconciliation_io.duckdb_orders_in_window", new=duckdb_orders), \
         patch("core.data_quality.classify_discrepancies", return_value=list(classify)), \
         patch("core.data_quality.classify_order_discrepancies", return_value=[]), \
         patch("core.data_quality.persist_run", new=persist), \
         patch("core.data_quality.machine_attempts_note", return_value=None), \
         patch("core.sync_service.get_sync_service", new=AsyncMock(return_value=sync)), \
         patch("core.duckdb_store.get_store", new=AsyncMock(return_value=_Store())):
        result = await sched._run_dq_reconciliation()
    return sched, sync, result


class TestTheJob:
    @pytest.mark.asyncio
    async def test_under_the_chain_duckdb_is_not_asked_and_postgres_drives_the_repair(
            self, moved):
        """Mutations: extract DuckDB anyway (the extraction raises here); repair
        from DuckDB's set, which under the chain is the frozen copy's."""
        extract = MagicMock(side_effect=AssertionError("DuckDB extracted"))
        persist = MagicMock(side_effect=AssertionError("reconciliation persisted"))
        pg = {"issues": [], "discrepancies": [_missing(901, 902)], "error": None}
        sched, sync, result = await _run(duckdb_orders=extract, persist=persist,
                                         pg_result=pg)
        assert result["error"] is None
        assert result["stood_down"] == ["reconciliation"]
        sync.repair_orders.assert_awaited_once()
        assert sync.repair_orders.await_args.args[0] == [901, 902]
        sched._persist_postgres_reconciliation.assert_awaited_once()
        sched._send_dq_alert_throttled.assert_not_awaited()
        sched._resolve_dq_layer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_off_the_duckdb_arm_runs_as_it_always_has(self, off):
        extract = MagicMock(return_value={})
        persist = MagicMock(return_value=7)
        sched, sync, result = await _run(
            duckdb_orders=extract, persist=persist, classify=[_missing(5)],
            pg_result={"issues": [], "discrepancies": [_missing(77)], "error": None})
        extract.assert_called_once()
        assert persist.call_args.kwargs["layer"] == "reconciliation"
        assert sync.repair_orders.await_args.args[0] == [5]     # DuckDB's own set
        sched._resolve_dq_layer.assert_awaited_once()
        assert "stood_down" not in result


class TestTheLayerIsNotWatched:
    def test_the_predicate(self, off):
        assert pg_orders_write.stood_down_layers() == frozenset()
        chain_latch.latch(pg_orders_write.CHAIN)
        assert pg_orders_write.stood_down_layers() == frozenset({"reconciliation"})

    def test_the_canary_does_not_page_a_layer_stood_down(self):
        """Mutation: ignore `stood_down` — `dq_stale:reconciliation` pages at
        30 h, every day, about a layer nothing writes by design."""
        from bot.canary import check_dq_freshness

        old = {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": 40 * 86400}
        fresh = {"last_success_at": "2026-10-01T05:30:00+03:00", "age_seconds": 3600}
        failures, _ = check_dq_freshness({"data_quality": {
            "reconciliation": {**old, "stood_down": True},
            "reconciliation_pg": fresh, "reconciliation_ch": fresh,
            "integrity": fresh, "mirror_landing": fresh}})
        assert not [k for k, _ in failures if k.endswith(":reconciliation")]
        failures, _ = check_dq_freshness({"data_quality": {
            "reconciliation": old, "reconciliation_pg": fresh,
            "reconciliation_ch": fresh, "integrity": fresh, "mirror_landing": fresh}})
        assert ("dq_stale:reconciliation" in {k for k, _ in failures})

    def test_the_postgres_arm_still_pages(self):
        from bot.canary import check_dq_freshness

        old = {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": 40 * 86400}
        failures, _ = check_dq_freshness({"data_quality": {
            "reconciliation": {**old, "stood_down": True}, "reconciliation_pg": old}})
        assert "dq_stale:reconciliation_pg" in {k for k, _ in failures}

    def test_health_marks_the_layer_and_only_under_the_chain(self, off):
        """Mutation: mark it in the cached stats, or not at all."""
        from web.routes.api.health import _mark_stood_down
        from web.schemas import DataQualityFreshness

        block = {"reconciliation": {"last_success_at": None, "age_seconds": 9},
                 "reconciliation_pg": {"last_success_at": None, "age_seconds": 9}}
        assert _mark_stood_down(block) == block
        assert _mark_stood_down(None) is None
        chain_latch.latch(pg_orders_write.CHAIN)
        marked = _mark_stood_down(block)
        assert marked["reconciliation"]["stood_down"] is True
        assert "stood_down" not in marked["reconciliation_pg"]
        # The public schema carries it, with a description.
        assert DataQualityFreshness(**marked["reconciliation"]).stood_down is True
        assert DataQualityFreshness.model_fields["stood_down"].description

    @pytest.mark.asyncio
    async def test_the_catch_up_does_not_run_the_job_for_a_layer_stood_down(self, moved):
        """Mutation: keep judging the job's own layer — every start of a web
        under the chain would re-run the 05:30 job and spend a KeyCRM fetch."""
        from tests.unit.test_scheduler_catchup import _scheduler_with

        def entry(age):
            return {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": age}

        ages = {"reconciliation": entry(40 * 86400), "integrity": entry(3600),
                "reconciliation_pg": entry(3600)}
        scheduler = _scheduler_with(moved, ages)
        with patch("core.scheduler._writes_layer", return_value=True):
            await scheduler._schedule_catchup_runs()
        assert "dq_reconciliation_catchup" not in scheduler._scheduler.added

    @pytest.mark.asyncio
    async def test_a_stale_postgres_arm_is_still_caught_up(self, moved):
        from tests.unit.test_scheduler_catchup import _scheduler_with

        def entry(age):
            return {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": age}

        ages = {"reconciliation": entry(3600), "integrity": entry(3600),
                "reconciliation_pg": entry(30 * 3600)}
        scheduler = _scheduler_with(moved, ages)
        with patch("core.scheduler._writes_layer", return_value=True):
            await scheduler._schedule_catchup_runs()
        assert "dq_reconciliation_catchup" in scheduler._scheduler.added

    def test_the_digest_skips_it(self):
        """Mutation: digest the stood-down layer — a stale section every
        morning, in the one message WARN findings reach anyone by."""
        import inspect

        src = inspect.getsource(BackgroundScheduler._run_dq_digest)
        assert "stood_down_layers()" in src and "if layer in stood_down" in src
