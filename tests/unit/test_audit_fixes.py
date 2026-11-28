"""Tests for data-platform-fixes branch (ultra-audit STEP 5).

Covers:
- A11-6: model-validation gate — an implausible model (high WAPE) is rejected
  before it overwrites the working model / feeds revenue_predictions; and
  negative/NaN predictions are never persisted.
"""
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import numpy as np
import pytest

from core.data_quality import _freshness_check, Severity
from tests.helpers.alerts_journal_pg import alert_ledger, settle
from tests.helpers.alerts_telegram import ADMINS, telegram_at_the_transport
from tests.integration.pg_tab_seed import (
    DSN, derive, needs_postgres, seed, tab_in_postgres,
)
from tests.helpers.training_frames import _valid_training_df


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
                            lambda df: (object(), {"wape": 252.88, "mape": 252.0, "mae": 1.0}))
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
                            lambda df: (sentinel_model, {"wape": 27.66, "mape": 30.0, "mae": 1.0}))
        monkeypatch.setattr(svc, "_save_model", lambda: None)
        monkeypatch.setattr(svc, "predict_month", AsyncMock(return_value=[]))

        res = await svc._train_impl("retail")

        assert res["status"] == "success"
        assert svc._model is sentinel_model


class _ModelThatAnswers:
    """In the trained model's place (a LightGBM booster): `revenue`, every day."""

    def __init__(self, revenue: float):
        self.revenue = revenue

    def predict(self, features):
        return np.full(len(features), self.revenue)


def _forecaster(revenue: float):
    """A ready service whose model forecasts `revenue` a day."""
    from core import prediction_service as ps

    svc = ps.PredictionService()
    svc._model = _ModelThatAnswers(revenue)
    svc._metrics = {"mae": 1.0, "mape": 30.0, "wape": 27.66}
    return svc


# The history a forecast is judged against: 10 000 to 20 000 a day.
HISTORY = _valid_training_df()
PLAUSIBLE = 15_000.0


async def _stored_forecast(pool):
    return [tuple(r) for r in await pool.fetch(
        "SELECT prediction_date, predicted_revenue, created_at"
        " FROM app.revenue_predictions WHERE sales_type = 'retail'"
        " ORDER BY prediction_date")]


@asynccontextmanager
async def _forecast_on_postgres(monkeypatch):
    """The forecast's tables empty on the test Postgres, the alert ledger
    beside them and Telegram at the transport. The chain's owner rows and the
    vintage capture's watermark, which a stored forecast writes, are put back
    as they were."""
    from core import chain_latch, pg_forecast_write
    from core.analytics_captures import state_row

    tables = ("app.revenue_predictions", pg_forecast_write.VINTAGES_TABLE)
    owners = [chain_latch.owner_key(t) for t in pg_forecast_write.CHAIN_TABLES]
    capture = state_row(pg_forecast_write.VINTAGES_TABLE)
    telegram = telegram_at_the_transport(monkeypatch)
    async with alert_ledger(monkeypatch) as ledger:
        pool = ledger.pool
        kept_owners = [r["key"] for r in await pool.fetch(
            "SELECT key FROM meta.chain_watermarks WHERE key = ANY($1::text[])", owners)]
        kept_capture = await pool.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = $1", capture)
        for table in tables:
            await pool.execute(f"DELETE FROM {table}")
        try:
            yield ledger, telegram
        finally:
            await settle()
            for table in tables:
                await pool.execute(f"DELETE FROM {table}")
            await pool.execute(
                "DELETE FROM meta.chain_watermarks WHERE key = ANY($1::text[])"
                " AND NOT key = ANY($2::text[])", owners, kept_owners)
            await pool.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", capture)
            for row in kept_capture:
                columns = list(row.keys())
                await pool.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                    f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                    *row.values())


@needs_postgres
class TestAnImplausibleForecastIsNeverStored:
    """A11-6: a model past its WAPE gate can still forecast nonsense. That
    forecast must not replace the one the dashboard shows, and the admins
    are told; a sane one is stored."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("revenue", [
        pytest.param(float("nan"), id="nan"),
        pytest.param(10 * float(HISTORY["revenue"].max()), id="ten-times-the-history"),
    ])
    async def test_it_keeps_the_stored_forecast_and_tells_the_admins(
            self, monkeypatch, revenue):
        async with _forecast_on_postgres(monkeypatch) as (ledger, telegram):
            await _forecaster(PLAUSIBLE).predict_month(
                historical_df=HISTORY, sales_type="retail")
            before = await _stored_forecast(ledger.pool)
            assert before and {r[1] for r in before} == {Decimal("15000.00")}
            assert telegram.messages == []

            await _forecaster(revenue).predict_month(
                historical_df=HISTORY, sales_type="retail")
            await settle()

            assert await _stored_forecast(ledger.pool) == before
            assert telegram.chats() == sorted(ADMINS)
            assert [(key, kind) for key, kind, _to in await ledger.events()] == [
                ("prediction:retrain_rejected:retail", "fired")]

    @pytest.mark.asyncio
    async def test_a_negative_answer_never_lands_as_negative_revenue(self, monkeypatch):
        async with _forecast_on_postgres(monkeypatch) as (ledger, _telegram):
            await _forecaster(-500.0).predict_month(
                historical_df=HISTORY, sales_type="retail")
            stored = await _stored_forecast(ledger.pool)
        assert [r for r in stored if r[1] < 0] == []
