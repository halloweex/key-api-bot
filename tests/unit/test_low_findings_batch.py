"""The last batch of the race audit's LOW findings, each with the guard it got."""
import asyncio
import inspect
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.duckdb_store import DuckDBStore


# `TestAHeaderOnlyRefreshNeverCreatesAnOrder` stood here, pinning the
# header-only refresh's deferral on DuckDB's order writer (with chain 3 routed
# back to DuckDB for it). Step 14's S14-09 deleted that writer; the same rule
# on chain 3's is `tests/integration/test_orders_chain_writer.py`
# (`test_a_header_only_refresh_never_creates_an_order`), and the decision
# itself `tests/unit/test_pg_orders_write.py` (`TestTheDecisionIsDuckDBs`).


class TestPersistRunIsOneTransaction:
    def test_begin_commit_bracket_the_inserts(self):
        from core.data_quality import persist_run

        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (7,)
        now = datetime.now(timezone.utc)
        run_id = persist_run(
            conn, started_at=now, ended_at=now, as_of=now,
            window_start=now.date(), window_end=now.date(), layer="integrity",
            issues=[], discrepancies=[],
        )
        assert run_id == 7
        sql = [str(c.args[0]).strip() for c in conn.execute.call_args_list]
        assert sql[0] == "BEGIN TRANSACTION" and sql[-1] == "COMMIT"

    def test_a_failed_insert_rolls_back(self):
        from core.data_quality import persist_run

        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (7,)
        conn.executemany.side_effect = RuntimeError("disk full")
        now = datetime.now(timezone.utc)
        from core.data_quality import IntegrityIssue, Severity

        with pytest.raises(RuntimeError):
            persist_run(
                conn, started_at=now, ended_at=now, as_of=now,
                window_start=now.date(), window_end=now.date(), layer="integrity",
                issues=[IntegrityIssue("x", "t", Severity.WARN, 1)], discrepancies=[],
            )
        sql = [str(c.args[0]).strip() for c in conn.execute.call_args_list]
        assert "ROLLBACK" in sql and "COMMIT" not in sql


class TestATimedOutQueryLeavesTheConnectionBeforeTheLockDoes:
    @pytest.mark.asyncio
    async def test_the_lock_is_free_and_usable_after_a_timeout(self, tmp_path):
        from core.exceptions import QueryTimeoutError

        store = DuckDBStore(db_path=tmp_path / "t.duckdb")
        await store.connect()
        try:
            with pytest.raises(QueryTimeoutError):
                await asyncio.wait_for(
                    store._fetch_all("SELECT count(*) FROM range(20000000000) r1, range(2) r2", timeout=0.05),
                    timeout=15,
                )
            assert not store._lock.locked()
            assert (await store._fetch_one("SELECT 1"))[0] == 1
        finally:
            await store.close()


class TestBackupSweepsItsOwnOrphans:
    @pytest.mark.asyncio
    async def test_a_stale_temp_file_is_removed(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "analytics.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                conn.execute("INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, buyer_id) "
                             "VALUES (1, 1, 1, 10.0, TIMESTAMPTZ '2026-08-01 10:00:00+00', 1)")
            dest = tmp_path / "backups"
            dest.mkdir()
            stale = dest / ".analytics-20260101-000000.duckdb.tmp"
            stale.write_bytes(b"x" * 10)
            result = await store.backup_database(dest_dir=dest, keep=2)
            assert result["status"] == "success", result
            assert not stale.exists()
            assert not list(dest.glob(".*.tmp"))
        finally:
            await store.close()


class TestTheUtmBackfillFlagIsAlwaysReleased:
    @pytest.mark.asyncio
    async def test_a_failure_before_the_body_does_not_wedge_the_endpoint(self, monkeypatch):
        from web.routes.api import traffic

        async def _boom():
            raise RuntimeError("store unavailable")

        monkeypatch.setattr(traffic, "get_store", _boom)
        traffic._backfill_status.update(running=True, result={"status": "started"})
        with pytest.raises(RuntimeError):
            await traffic._run_backfill(1)
        assert traffic._backfill_status["running"] is False


class TestTheRest:
    def test_the_bot_sync_client_serialises_its_callers(self):
        from core.keycrm import SyncKeyCRMClient

        assert "threading.Lock()" in inspect.getsource(SyncKeyCRMClient.__init__)
        assert "with self._lock:" in inspect.getsource(SyncKeyCRMClient._run)

    def test_utm_parse_stamps_the_updated_at_it_parsed(self):
        from core.repositories.traffic import TrafficMixin

        src = inspect.getsource(TrafficMixin.parse_utm_data) if hasattr(TrafficMixin, "parse_utm_data") else inspect.getsource(TrafficMixin)
        assert "SELECT o.id, o.manager_comment, o.updated_at" in src
        assert "COALESCE(?, CURRENT_TIMESTAMP)" in src

    def test_the_classification_endpoint_reports_its_replica(self):
        from web.routes.api import admin

        src = inspect.getsource(admin)
        assert 'replica = await replicate_managers(store)' in src
        assert '"replica": replica' in src
