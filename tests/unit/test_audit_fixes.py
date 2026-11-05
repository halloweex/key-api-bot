"""Tests for data-platform-fixes branch (ultra-audit STEP 5).

Covers:
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

from core.duckdb_store import DuckDBStore
from core.data_quality import _freshness_check, Severity


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore()
    return store


def _insert_order(conn, oid: int, buyer_id: int = 10):
    conn.execute(
        """INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at,
           created_at, updated_at, buyer_id, manager_id, manager_comment, promocode)
           VALUES (?, 4, 1, '100.00', '2026-01-15T10:00:00+03:00',
                   '2026-01-15T10:00:00+03:00', '2026-01-15T10:00:00+03:00', ?, NULL, NULL, NULL)""",
        [oid, buyer_id],
    )


# A5-1 / F3, the DuckDB warehouse refresh's self-heal and its consecutive-
# failure bound (`TestRefreshSelfHeal`), went with the refresh in step 14's
# S14-15.


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
        # The shared Silver projection and the Postgres Gold projection, both
        # rendered for DuckDB (DuckDB's own rebuild went in step 14's S14-15).
        from tests.silver_on_duckdb import derive_silver, gold_cells

        await derive_silver(store)
        async with store.connection() as conn:
            is_ret = conn.execute(
                "SELECT is_return FROM silver_orders WHERE id = 2"
            ).fetchone()[0]
            rev = sum(cell["revenue"] for cell in gold_cells(conn).values())
        assert is_ret is True
        assert float(rev) == 100.0, "status-15 ₴999 order must be excluded from revenue"


# ─────────────────────── A12-2 / A7-1 freshness ───────────────────────

def _history(now):
    """A row of DuckDB's `sync_metadata` as Postgres copied it, so the check
    is past its fresh-install gate; the watermarks it judges are the chains'.
    (DuckDB's own row, read under `on`, until step 14's S14-14.)"""
    return {"last_sync_orders": (now - timedelta(minutes=5)).isoformat()}


def _moved(now, **stamps):
    """Every watermark the write chains keep in Postgres — every chain's
    since step 14's S14-00, a constant — fresh, but for the ones given."""
    from core.write_chains import sync_keys as stood_down_sync_keys

    out = {k: (now - timedelta(minutes=5)).isoformat() for k in stood_down_sync_keys()}
    out.update({f"last_sync_{k}": v.isoformat() for k, v in stamps.items()})
    return out


class TestFreshnessCheck:
    def test_stale_catalog_fires_fresh_orders_pass(self):
        now = datetime(2026, 5, 20, 22, 0, 0).astimezone()
        # orders current; categories 45 days stale (the real incident).
        issues = _freshness_check(now=now, frozen_rows=_history(now), chain_watermarks=_moved(
            now, orders=now - timedelta(minutes=5),
            categories=now - timedelta(days=45)))
        names = {i.check_name for i in issues}
        assert "freshness_categories" in names
        assert "freshness_orders" not in names

    def test_dead_orders_sync_is_critical(self):
        now = datetime(2026, 5, 20, 22, 0, 0).astimezone()
        issues = _freshness_check(now=now, frozen_rows=_history(now), chain_watermarks=_moved(
            now, orders=now - timedelta(hours=12)))
        orders = [i for i in issues if i.check_name == "freshness_orders"]
        assert orders and orders[0].severity == Severity.CRITICAL

    def test_no_sync_history_is_bootstrap_no_issues(self):
        """Nothing in Postgres at all — the copy read and empty, no chain
        watermark: a fresh install, not a stall."""
        assert _freshness_check(frozen_rows={}, chain_watermarks={}) == []


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
