"""Tests for data-platform-fixes branch (ultra-audit STEP 5).

Covers:
- A5-1 / F3: refresh self-heal — a thrown pipeline error marks the warehouse
  dirty (so the next scheduler tick rebuilds) and alerts, instead of leaving
  durable cross-layer inconsistency silently; consecutive-failure bound.
- A11-6: model-validation gate — an implausible model (high WAPE) is rejected
  before it overwrites the working model / feeds revenue_predictions; and
  negative/NaN predictions are never persisted.
"""
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from core.duckdb_store import DuckDBStore, MAX_VALIDATION_RETRIES
from core.data_quality import _freshness_check, Severity


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(conn, oid: int, buyer_id: int = 10):
    conn.execute(
        """INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at,
           created_at, updated_at, buyer_id, manager_id, manager_comment, promocode)
           VALUES (?, 4, 1, '100.00', '2026-01-15T10:00:00+03:00',
                   '2026-01-15T10:00:00+03:00', '2026-01-15T10:00:00+03:00', ?, NULL, NULL, NULL)""",
        [oid, buyer_id],
    )


# ─────────────────────────── A5-1 / F3 ───────────────────────────

class TestRefreshSelfHeal:
    @pytest.mark.asyncio
    async def test_consecutive_failure_counter(self, tmp_path):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            # oldest → newest. Most recent two are failures.
            for ts, vp in [
                ("2026-01-01", True),
                ("2026-01-02", False),
                ("2026-01-03", True),
                ("2026-01-04", False),
                ("2026-01-05", False),
            ]:
                conn.execute(
                    "INSERT INTO warehouse_refreshes (refreshed_at, trigger, validation_passed) "
                    "VALUES (?, 'test', ?)",
                    [ts + "T00:00:00+03:00", vp],
                )
        assert await store._count_consecutive_refresh_failures() == 2

    @pytest.mark.asyncio
    async def test_pipeline_exception_marks_dirty_and_alerts(self, tmp_path, monkeypatch):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            _insert_order(conn, oid=1)

        alerts: list[str] = []

        async def fake_alert(msg):
            alerts.append(msg)

        monkeypatch.setattr(store, "_send_warehouse_alert", fake_alert)
        # Poison the date helper so the Silver INSERT SELECT throws inside the
        # pipeline → exercises the outer except (self-heal) path.
        monkeypatch.setattr("core.duckdb_store._date_in_kyiv", lambda col: "not valid sql (")

        res = await store.refresh_warehouse_layers(trigger="manual", changed_order_ids=None)

        assert res["status"] == "error"
        is_dirty, _ = await store.consume_warehouse_dirty()
        assert is_dirty is True, "partial-failure must mark warehouse dirty to self-heal"
        assert alerts, "must alert on refresh error"
        assert "error" in alerts[0].lower()

    @pytest.mark.asyncio
    async def test_exception_stops_retry_after_max(self, tmp_path, monkeypatch):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            _insert_order(conn, oid=1)
            # Seed MAX_VALIDATION_RETRIES+1 prior consecutive failures so the
            # bound trips → no further dirty mark, CRITICAL alert instead.
            for i in range(MAX_VALIDATION_RETRIES + 1):
                conn.execute(
                    "INSERT INTO warehouse_refreshes (refreshed_at, trigger, validation_passed) "
                    "VALUES (?, 'test', FALSE)",
                    [f"2026-01-0{i+1}T00:00:00+03:00"],
                )
        alerts: list[str] = []

        async def fake_alert(msg):
            alerts.append(msg)

        monkeypatch.setattr(store, "_send_warehouse_alert", fake_alert)
        monkeypatch.setattr("core.duckdb_store._date_in_kyiv", lambda col: "not valid sql (")

        await store.refresh_warehouse_layers(trigger="manual", changed_order_ids=None)

        is_dirty, _ = await store.consume_warehouse_dirty()
        assert is_dirty is False, "must STOP retrying past the bound"
        assert any("CRITICAL" in a for a in alerts)


# ─────────────────────────── A9-1 backup ───────────────────────────

class TestBackup:
    @pytest.mark.asyncio
    async def test_backup_produces_valid_copy(self, tmp_path):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            _insert_order(conn, oid=1)
            _insert_order(conn, oid=2)
        dest = tmp_path / "backups"
        res = await store.backup_database(dest_dir=dest, keep=7)
        assert res["status"] == "success"
        assert res["orders"] == 2
        backups = list(dest.glob("*.duckdb"))
        assert len(backups) == 1
        # backup opens read-only and has the rows
        import duckdb
        con = duckdb.connect(str(backups[0]), read_only=True)
        try:
            assert con.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 2
        finally:
            con.close()

    @pytest.mark.asyncio
    async def test_backup_retention_prunes_old(self, tmp_path):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            _insert_order(conn, oid=1)
        dest = tmp_path / "backups"
        dest.mkdir(parents=True)
        # Pre-seed 3 fake older backups (same stem as the store db = "test");
        # keep=2 → after a real backup, only the 2 newest remain.
        for name in ("test-20260101-000000.duckdb",
                     "test-20260102-000000.duckdb",
                     "test-20260103-000000.duckdb"):
            (dest / name).write_bytes(b"old")
        res = await store.backup_database(dest_dir=dest, keep=2)
        assert res["status"] == "success"
        assert len(list(dest.glob("test-*.duckdb"))) == 2


# ─────────────────────── A2-RETURNS-3 lost/cancel group ───────────────────────

class TestReturnStatusGroup:
    def test_return_set_includes_lost_group(self):
        from core.models import OrderStatus
        s = OrderStatus.return_statuses()
        assert {15, 18, 19, 21, 22, 23} == {int(x) for x in s}

    @pytest.mark.asyncio
    async def test_status15_excluded_from_gold_revenue(self, tmp_path):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            _insert_order(conn, oid=1)              # status 1 → counts
            # status 15 (not_available) — must be treated as is_return now.
            conn.execute(
                """INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at,
                   created_at, updated_at, buyer_id, manager_id, manager_comment, promocode)
                   VALUES (2, 4, 15, '999.00', '2026-01-15T10:00:00+03:00',
                           '2026-01-15T10:00:00+03:00', '2026-01-15T10:00:00+03:00', 11, NULL, NULL, NULL)""",
            )
        await store.refresh_warehouse_layers(trigger="manual", changed_order_ids=None)
        async with store.connection() as conn:
            is_ret = conn.execute(
                "SELECT is_return FROM silver_orders WHERE id = 2"
            ).fetchone()[0]
            rev = conn.execute(
                "SELECT COALESCE(SUM(revenue),0) FROM gold_daily_revenue"
            ).fetchone()[0]
        assert is_ret is True
        assert float(rev) == 100.0, "status-15 ₴999 order must be excluded from revenue"


# ─────────────────────── A12-2 / A7-1 freshness ───────────────────────

class TestFreshnessCheck:
    @pytest.mark.asyncio
    async def test_stale_catalog_fires_fresh_orders_pass(self, tmp_path):
        store = await _make_store(tmp_path)
        now = datetime(2026, 5, 20, 22, 0, 0).astimezone()
        async with store.connection() as conn:
            # orders current; categories 45 days stale (the real incident).
            conn.execute(
                "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                ["last_sync_orders", (now - timedelta(minutes=5)).isoformat()],
            )
            conn.execute(
                "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                ["last_sync_categories", (now - timedelta(days=45)).isoformat()],
            )
            issues = _freshness_check(conn, now=now)
        names = {i.check_name for i in issues}
        assert "freshness_categories" in names
        assert "freshness_orders" not in names

    @pytest.mark.asyncio
    async def test_dead_orders_sync_is_critical(self, tmp_path):
        store = await _make_store(tmp_path)
        now = datetime(2026, 5, 20, 22, 0, 0).astimezone()
        async with store.connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                ["last_sync_orders", (now - timedelta(hours=12)).isoformat()],
            )
            issues = _freshness_check(conn, now=now)
        orders = [i for i in issues if i.check_name == "freshness_orders"]
        assert orders and orders[0].severity == Severity.CRITICAL

    @pytest.mark.asyncio
    async def test_no_sync_history_is_bootstrap_no_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            assert _freshness_check(conn) == []


# ─────────────────────────── A11-6 ───────────────────────────

def _valid_training_df(n: int = 120) -> pd.DataFrame:
    start = date(2025, 1, 1)
    dates = [start + timedelta(days=i) for i in range(n)]
    return pd.DataFrame({
        "date": dates,
        "revenue": np.linspace(10000, 20000, n),
    })


class TestModelValidationGate:
    @pytest.mark.asyncio
    async def test_high_wape_model_rejected(self, monkeypatch):
        from core import prediction_service as ps
        svc = ps.PredictionService()

        async def fake_get_store():
            return object()

        monkeypatch.setattr("core.duckdb_store.get_store", fake_get_store)
        monkeypatch.setattr(svc, "_query_daily_revenue",
                            AsyncMock(return_value=_valid_training_df()))
        # Garbage model: WAPE far above the reject threshold.
        monkeypatch.setattr(ps, "_train_model",
                            lambda df: (object(), {"wape": 252.88, "mape": 252.0, "mae": 1.0}, {}, 1.0))
        monkeypatch.setattr(svc, "_alert_model_rejected", AsyncMock())

        res = await svc._train_impl("retail")

        assert res["status"] == "rejected"
        assert svc._model is None, "rejected model must NOT be committed"
        svc._alert_model_rejected.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_good_model_accepted(self, monkeypatch):
        from core import prediction_service as ps
        svc = ps.PredictionService()

        async def fake_get_store():
            return object()

        monkeypatch.setattr("core.duckdb_store.get_store", fake_get_store)
        monkeypatch.setattr(svc, "_query_daily_revenue",
                            AsyncMock(return_value=_valid_training_df()))
        sentinel_model = object()
        monkeypatch.setattr(ps, "_train_model",
                            lambda df: (sentinel_model, {"wape": 27.66, "mape": 30.0, "mae": 1.0}, {}, 1.0))
        monkeypatch.setattr(svc, "_save_model", lambda: None)
        monkeypatch.setattr(svc, "predict_month", AsyncMock(return_value=[]))

        res = await svc._train_impl("retail")

        assert res["status"] == "success"
        assert svc._model is sentinel_model

    @pytest.mark.asyncio
    async def test_negative_predictions_not_stored(self, monkeypatch):
        from core import prediction_service as ps
        svc = ps.PredictionService()
        svc._model = object()  # is_ready
        svc._metrics = {"wape": 27.66}

        fake_store = AsyncMock()
        async def fake_get_store():
            return fake_store
        monkeypatch.setattr("core.duckdb_store.get_store", fake_get_store)

        hist = _valid_training_df()
        # predictions contain a negative value → must be rejected.
        monkeypatch.setattr(ps, "_predict_future",
                            lambda *a, **k: [{"date": date(2026, 2, 1), "predicted_revenue": -500.0}])
        monkeypatch.setattr(svc, "_alert_model_rejected", AsyncMock())

        await svc.predict_month(historical_df=hist, sales_type="retail")

        fake_store.store_predictions.assert_not_called()
        svc._alert_model_rejected.assert_awaited_once()
