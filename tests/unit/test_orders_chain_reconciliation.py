"""`dq_reconciliation` with no DuckDB arm: Postgres drives the repair (G12, G13).

Once `KS_WRITE_ORDERS` moved the orders to Postgres, DuckDB stopped receiving
them, and its arm of the 05:30 job stood down: not extracted, persisted, paged
or resolved, its layer `reconciliation` neither paged by the canary, nor caught
up at start, nor digested. Step 14's S14-12 deleted the arm and the layer. What
stays is what these pin: the Postgres arm, classified against the KeyCRM
snapshot, drives the repair of what Postgres lacks; no run is journalled on the
old layer; and nothing pages, catches up or digests a layer nothing writes.
Each test names the mutation it exists to fail on.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import Discrepancy, DiscrepancyClass, Severity
from core.scheduler import BackgroundScheduler


def _missing(*ids, diff_class=DiscrepancyClass.MISSING_IN_DK):
    return Discrepancy(
        month="2026-09", source_id=4, diff_class=diff_class,
        field="orders", dk_value=0, kc_value=len(ids), severity=Severity.CRITICAL,
        order_ids=tuple(ids),
    )


async def _run(*, pg_result, journal_run=None):
    sync = AsyncMock()
    sync.repair_orders = AsyncMock(return_value={"repaired": 1, "remaining": 0})
    sched = BackgroundScheduler()
    sched._send_dq_alert_throttled = AsyncMock()
    sched._resolve_dq_layer = AsyncMock()
    sched._reconcile_postgres = AsyncMock(return_value=pg_result)
    sched._persist_postgres_reconciliation = AsyncMock()
    sched._reconcile_clickhouse = AsyncMock(return_value=None)

    async def _refuse(**kwargs):
        raise AssertionError(f"journalled a run on {kwargs.get('layer')}")

    with patch("core.reconciliation_io.keycrm_orders_in_window",
               new=AsyncMock(return_value=({}, 3, set()))), \
         patch("core.dq_journal.journal_run", new=journal_run or _refuse), \
         patch("core.data_quality.machine_attempts_note", return_value=None), \
         patch("core.sync_service.get_sync_service", new=AsyncMock(return_value=sync)), \
         patch("core.duckdb_store.get_store",
               new=AsyncMock(side_effect=AssertionError("the job asked the store"))):
        result = await sched._run_dq_reconciliation()
    return sched, sync, result


class TestTheJob:
    @pytest.mark.asyncio
    async def test_an_order_postgres_lacks_is_repaired_by_its_id(self):
        """The plan's S14-12 test: an order KeyCRM holds and Postgres does not
        — MISSING_IN_DK in the Postgres arm's classification — is re-fetched
        by id. Mutations: repair from anything but the Postgres arm's
        discrepancies; drop the repair with the DuckDB arm."""
        pg = {"issues": [], "discrepancies": [_missing(901, 902)], "error": None}
        sched, sync, result = await _run(pg_result=pg)
        assert result["error"] is None
        sync.repair_orders.assert_awaited_once()
        assert sync.repair_orders.await_args.args[0] == [901, 902]
        assert result["repair"] == {"repaired": 1, "remaining": 0}
        sched._persist_postgres_reconciliation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_ghost_is_never_repaired_and_a_drift_is(self):
        """Additive only: an order KeyCRM no longer returns is never deleted
        on the strength of one reading. A status drift is re-fetched after
        the missing (A3, L1-04): no sync reaches an order created and ordered
        more than 30 days ago whose status moved without `updated_at`.
        (`tests/integration/test_recon_repairs_status_drift.py` proves the
        write.)"""
        pg = {"issues": [], "error": None, "discrepancies": [
            _missing(7, diff_class=DiscrepancyClass.MISSING_IN_KC),
            _missing(8, diff_class=DiscrepancyClass.STATUS_DRIFT),
            _missing(9)]}
        _sched, sync, _result = await _run(pg_result=pg)
        assert sync.repair_orders.await_args.args[0] == [9, 8]

    @pytest.mark.asyncio
    async def test_nothing_missing_repairs_nothing(self):
        pg = {"issues": [], "discrepancies": [], "error": None}
        _sched, sync, result = await _run(pg_result=pg)
        sync.repair_orders.assert_not_awaited()
        assert result["repair"] is None

    @pytest.mark.asyncio
    async def test_no_postgres_verdict_repairs_nothing(self):
        """`_reconcile_postgres` answers None without `KS_PG_DSN`: there is no
        classification to repair from."""
        _sched, sync, _result = await _run(pg_result=None)
        sync.repair_orders.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_fetch_repairs_nothing(self):
        sched = BackgroundScheduler()
        sched._send_dq_alert_throttled = AsyncMock()
        sched._resolve_dq_layer = AsyncMock()
        sched._reconcile_postgres = AsyncMock()
        sched._reconcile_clickhouse = AsyncMock()
        sync = AsyncMock()
        with patch("core.reconciliation_io.keycrm_orders_in_window",
                   new=AsyncMock(side_effect=RuntimeError("KeyCRM down"))), \
             patch("core.sync_service.get_sync_service",
                   new=AsyncMock(return_value=sync)):
            result = await sched._run_dq_reconciliation()
        assert "KeyCRM down" in result["error"]
        sched._reconcile_postgres.assert_not_awaited()
        sync.repair_orders.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_run_is_journalled_on_the_old_layer(self):
        """A run on `reconciliation` would be a fresh, clean verdict of a
        comparison nothing made. Recorded at the door every route goes through
        (`dq_journal.journal_run`), recording rather than raising: the job's
        own `except Exception` would swallow a raise. The Postgres arm's own
        persistence is stubbed, so nothing should reach the door at all."""
        journalled = []

        async def journal_run(**kwargs):
            journalled.append(kwargs["layer"])
            return 7

        sched, _sync, result = await _run(
            pg_result={"issues": [], "discrepancies": [], "error": None},
            journal_run=journal_run)
        assert result["error"] is None
        assert journalled == []
        assert "run_id" not in result and "stood_down" not in result
        sched._send_dq_alert_throttled.assert_not_awaited()
        sched._resolve_dq_layer.assert_not_awaited()


class TestTheLayerIsGone:
    def test_it_is_watched_by_nothing(self):
        """Mutation: leave `reconciliation` in any of the lists — the canary
        then pages `dq_missing:reconciliation` (nothing writes it), the
        digest prints "no successful run" every morning, and the catch-up
        re-runs the 05:30 job at every start."""
        from bot.canary import DQ_MAX_AGE_S
        from core.alerting import REGISTRY
        from core.data_quality import DIGEST_MAX_AGE_HOURS, WATCHED_LAYERS
        from core.scheduler import CATCHUP_CHECKS, CATCHUP_SIBLING_LAYERS

        assert "reconciliation" not in WATCHED_LAYERS
        assert "reconciliation" not in DIGEST_MAX_AGE_HOURS
        assert "reconciliation" not in DQ_MAX_AGE_S
        assert CATCHUP_CHECKS["dq_reconciliation"][0] is None
        assert "reconciliation" not in CATCHUP_SIBLING_LAYERS["dq_reconciliation"]
        assert not [k for k in REGISTRY if k.endswith(":reconciliation")]
        # The arms that stay are watched everywhere.
        for layer in ("reconciliation_pg", "reconciliation_ch"):
            assert layer in WATCHED_LAYERS and layer in DQ_MAX_AGE_S
            assert f"dq_stale:{layer}" in REGISTRY

    def test_the_canary_does_not_miss_a_layer_nothing_writes(self):
        from bot.canary import check_dq_freshness

        fresh = {"last_success_at": "2026-10-01T05:30:00+03:00", "age_seconds": 3600}
        failures, _ = check_dq_freshness({"data_quality": {
            "reconciliation_pg": fresh, "reconciliation_ch": fresh,
            "integrity": fresh, "mirror_landing": fresh}})
        assert failures == []

    def test_the_postgres_arm_still_pages(self):
        from bot.canary import check_dq_freshness

        old = {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": 40 * 86400}
        failures, _ = check_dq_freshness({"data_quality": {"reconciliation_pg": old}})
        assert "dq_stale:reconciliation_pg" in {k for k, _ in failures}

    def test_health_publishes_no_stand_down_mark(self):
        """`stood_down` marked the old layer for the canary (chain 3); with
        the layer gone the field went from the public schema too."""
        from web.routes.api import health
        from web.schemas import DataQualityFreshness

        assert "stood_down" not in DataQualityFreshness.model_fields
        assert not hasattr(health, "_mark_stood_down")

    def test_the_endpoint_publishes_the_journal_as_it_is(self, monkeypatch):
        """Through `/api/health` and the canary: the block chain 9 answers
        is published unmarked, and a fresh one pages nothing."""
        from fastapi.testclient import TestClient

        from bot.canary import check_dq_freshness
        from core import dq_journal
        from core.data_quality import WATCHED_LAYERS
        from web.main import app
        from web.ratelimit import limiter
        from web.routes.api import health

        fresh = {"last_success_at": "2026-10-01T05:30:00+03:00", "age_seconds": 3600}
        block = {layer: dict(fresh) for layer in WATCHED_LAYERS}

        async def from_postgres(layers=None):
            return {layer: dict(entry) for layer, entry in block.items()}

        monkeypatch.setattr(dq_journal, "last_success_ages_pg", from_postgres)
        monkeypatch.setitem(health._journal_ages_cache, "data", None)
        monkeypatch.setitem(health._journal_ages_cache, "expires_at", 0)

        limiter.reset()
        try:
            body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
        assert set(body["data_quality"]) == set(WATCHED_LAYERS)
        assert not [e for e in body["data_quality"].values() if "stood_down" in e]
        failures, _ = check_dq_freshness(body)
        assert not [k for k, _ in failures if k.startswith("dq_")], failures

    @pytest.mark.asyncio
    async def test_fresh_arms_queue_no_catch_up(self, monkeypatch):
        """Mutation: give the job back a layer of its own — absent, it would
        read as never succeeded, and every start would re-run the 05:30 job
        and spend a KeyCRM fetch."""
        from tests.unit.test_scheduler_catchup import _scheduler_with

        def entry(age):
            return {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": age}

        ages = {"integrity": entry(3600), "reconciliation_pg": entry(3600)}
        scheduler = _scheduler_with(monkeypatch, ages)
        with patch("core.scheduler._writes_layer", return_value=True):
            await scheduler._schedule_catchup_runs()
        assert "dq_reconciliation_catchup" not in scheduler._scheduler.added

    @pytest.mark.asyncio
    async def test_a_stale_postgres_arm_is_still_caught_up(self, monkeypatch):
        from tests.unit.test_scheduler_catchup import _scheduler_with

        def entry(age):
            return {"last_success_at": "2026-09-01T05:30:00+03:00", "age_seconds": age}

        ages = {"integrity": entry(3600), "reconciliation_pg": entry(30 * 3600)}
        scheduler = _scheduler_with(monkeypatch, ages)
        with patch("core.scheduler._writes_layer", return_value=True):
            await scheduler._schedule_catchup_runs()
        assert "dq_reconciliation_catchup" in scheduler._scheduler.added

    @pytest.mark.asyncio
    async def test_the_digest_reads_every_watched_layer_and_no_other(self, monkeypatch):
        """Run against a journal holding every watched layer fresh and clean:
        quiet. Mutation: digest a layer nothing writes — a "no successful run"
        section every morning, in the one message WARN findings reach anyone
        by. The journal is chain 9's functions over a list
        (`tests/journal_fake.py`)."""
        from datetime import date, timedelta

        from core import dq_journal
        from core.data_quality import WATCHED_LAYERS
        from tests.journal_fake import install_journal
        from tests.unit.test_dq_digest_beat import _install_outbox

        install_journal(monkeypatch)
        outbox = _install_outbox(monkeypatch)
        at = datetime.now(timezone.utc) - timedelta(minutes=30)
        for layer in WATCHED_LAYERS:
            await dq_journal.journal_run(
                started_at=at, ended_at=at, as_of=at,
                window_start=date(2026, 1, 1), window_end=date(2026, 9, 30),
                layer=layer, issues=[], discrepancies=[])
        result = await BackgroundScheduler()._run_dq_digest()
        assert result["quiet"] is True and result["sent"] is False
        assert outbox.messages == []
        assert result["layers"] == len(WATCHED_LAYERS)
