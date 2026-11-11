"""Tests for data-platform-fixes branch (ultra-audit STEP 5).

Covers:
- A11-6: model-validation gate — an implausible model (high WAPE) is rejected
  before it overwrites the working model / feeds revenue_predictions; and
  negative/NaN predictions are never persisted.
"""
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from core.data_quality import _freshness_check, Severity
from tests.integration.pg_tab_seed import (
    DSN, derive, needs_postgres, seed, tab_in_postgres,
)


# A5-1 / F3, the DuckDB warehouse refresh's self-heal and its consecutive-
# failure bound (`TestRefreshSelfHeal`), went with the refresh in step 14's
# S14-15.


# A9-1, the DuckDB backup's consistent copy and its retention
# (`TestBackup`), went with the backup in step 14's S14-18.


# ─────────────────────── A2-RETURNS-3 lost/cancel group ───────────────────────

@needs_postgres
class TestReturnStatusGroup:
    @pytest.mark.asyncio
    async def test_status15_excluded_from_gold_revenue(self, monkeypatch):
        """Postgres's Silver and Gold, derived from Bronze. Until step 14's
        S14-18 the shared projections ran on DuckDB here; the answer is the
        one DuckDB gave on the same two orders (rule 9)."""
        from datetime import timezone as tz

        when = datetime(2026, 1, 15, 10, tzinfo=tz(timedelta(hours=3)))

        async def populate(conn, pool):
            await seed(conn, orders=[
                (1, 4, 1, 100.00, when, 10, None, None),     # status 1 → counts
                # status 15 (not_available) — must be treated as is_return now.
                (2, 4, 15, 999.00, when, 11, None, None),
            ])

        async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
            await derive(pool)
            async with pool.acquire() as conn:
                is_ret = await conn.fetchval(
                    "SELECT is_return FROM silver.orders WHERE id = 2")
                rev = await conn.fetchval(
                    "SELECT SUM(revenue) FROM gold.daily_revenue WHERE source_id IS NULL")
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

        monkeypatch.setattr("core.store.get_store", fake_get_store)
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

        monkeypatch.setattr("core.store.get_store", fake_get_store)
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
        monkeypatch.setattr("core.store.get_store", fake_get_store)

        hist = _valid_training_df()
        # predictions contain a negative value → must be rejected.
        monkeypatch.setattr(ps, "_predict_future",
                            lambda *a, **k: [{"date": date(2026, 2, 1), "predicted_revenue": -500.0}])
        monkeypatch.setattr(svc, "_alert_model_rejected", AsyncMock())

        await svc.predict_month(historical_df=hist, sales_type="retail")

        fake_store.store_predictions.assert_not_called()
        svc._alert_model_rejected.assert_awaited_once()
