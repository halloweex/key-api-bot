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
    @pytest.mark.parametrize("latched", [True, False])
    async def test_a_stood_down_arm_journals_no_run(self, off, monkeypatch, latched):
        """A stood-down arm compared nothing, so a run on its layer would be a
        fresh, clean `reconciliation` verdict — through chain 9's door, into
        Postgres while chain 9 writes there. Recorded at the door every route
        goes through (`dq_journal.journal_run`), with `persist_run` recording
        too rather than raising: the test above raises from `persist`, and the
        job's own `except Exception` swallows it. Mutation: drop
        `if duckdb_arm:` in front of the journal write. The unlatched case
        proves the recorder sees the door."""
        from core import dq_journal

        journalled = []

        async def journal_run(store, **kwargs):
            journalled.append(kwargs["layer"])
            return 7

        monkeypatch.setattr(dq_journal, "journal_run", journal_run)
        if latched:
            chain_latch.latch(pg_orders_write.CHAIN)
        persist = MagicMock(return_value=7)
        sched, _sync, result = await _run(
            duckdb_orders=MagicMock(return_value={}), persist=persist,
            pg_result={"issues": [], "discrepancies": [], "error": None})
        assert result["error"] is None
        if latched:
            assert journalled == []
            persist.assert_not_called()
            assert result["run_id"] is None
        else:
            assert journalled == ["reconciliation"]
            assert result["run_id"] == 7
        sched._persist_postgres_reconciliation.assert_awaited_once()

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

    @pytest.mark.parametrize("journal_chain", [True, False])
    def test_the_endpoint_marks_the_block_chain_9_answers(self, off, journal_chain):
        """Through `/api/health` and the canary, with chain 3 latched. Under
        chain 9 `_journal_ages` answers Postgres's block and ignores the one
        it is handed, so the mark has to go on after it. Mutation: chain 3's
        original order — `_mark_stood_down(stats.pop(...))`, then
        `_journal_ages` — loses `stood_down`, and the canary pages
        `dq_stale:reconciliation` every day once both chains are on. Without
        chain 9 the block is DuckDB's either way, and marked either way."""
        import time

        from fastapi.testclient import TestClient

        from bot.canary import check_dq_freshness
        from core import dq_journal, pg_dq_journal_write
        from web.main import app
        from web.ratelimit import limiter
        from web.routes.api import health

        fresh = {"last_success_at": "2026-10-01T05:30:00+03:00", "age_seconds": 3600}
        stale = {"last_success_at": "2026-09-29T13:30:00+03:00", "age_seconds": 40 * 3600}
        layers = ("integrity", "reconciliation", "mirror_landing",
                  "reconciliation_pg", "reconciliation_ch")
        block = {layer: dict(stale if layer == "reconciliation" else fresh)
                 for layer in layers}

        async def from_postgres(layers=None):
            return {layer: dict(entry) for layer, entry in block.items()}

        if journal_chain:
            chain_latch.latch(pg_dq_journal_write.CHAIN)
            off.setattr(dq_journal, "last_success_ages_pg", from_postgres)
            from_duckdb = {layer: dict(fresh) for layer in layers}   # frozen copy
        else:
            from_duckdb = block
        chain_latch.latch(pg_orders_write.CHAIN)
        off.setitem(health._stats_cache, "data", {"data_quality": from_duckdb})
        off.setitem(health._stats_cache, "expires_at", time.time() + 600)
        off.setitem(health._journal_ages_cache, "data", None)
        off.setitem(health._journal_ages_cache, "expires_at", 0)

        limiter.reset()
        try:
            body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
        assert body["data_quality"]["reconciliation"]["age_seconds"] == 40 * 3600
        assert body["data_quality"]["reconciliation"]["stood_down"] is True
        failures, _ = check_dq_freshness(body)
        assert not [k for k, _ in failures if k.endswith(":reconciliation")], failures

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("latched", [True, False])
    async def test_the_digest_skips_it(self, off, tmp_path, latched):
        """Run against a real journal, every watched layer fresh and clean but
        `reconciliation`, which nothing has written. Mutation: digest the
        stood-down layer — a "no successful run" section every morning, in
        the one message WARN findings reach anyone by. Since the merge with
        chain 9 that is one word, `digest_inputs(store, WATCHED_LAYERS, now)`,
        and the source-text check this replaced passed it. The unlatched case
        proves the layer's absence is news, so the quiet is the skip's."""
        from datetime import date, timedelta

        from core.data_quality import WATCHED_LAYERS, persist_run
        from tests.unit.test_dq_digest_beat import _install_outbox, _make_store

        store = await _make_store(tmp_path, off)
        outbox = _install_outbox(off)
        at = datetime.now(timezone.utc) - timedelta(minutes=30)
        async with store.connection() as conn:
            for layer in WATCHED_LAYERS:
                if layer != "reconciliation":
                    persist_run(conn, started_at=at, ended_at=at, as_of=at,
                                window_start=date(2026, 1, 1),
                                window_end=date(2026, 9, 30),
                                layer=layer, issues=[], discrepancies=[])
        if latched:
            chain_latch.latch(pg_orders_write.CHAIN)
        try:
            result = await BackgroundScheduler()._run_dq_digest()
        finally:
            await store.close()
        if latched:
            assert result["quiet"] is True and result["sent"] is False
            assert outbox.messages == []
            assert result["layers"] == len(WATCHED_LAYERS) - 1
        else:
            assert result["sent"] is True
            assert "<b>reconciliation</b> — no successful run" in outbox.messages[0]
