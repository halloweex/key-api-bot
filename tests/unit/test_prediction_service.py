"""Tests for winsorized LightGBM revenue prediction."""
import json
import pathlib
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_daily_df(n_days: int = 400, base_revenue: float = 100_000,
                   spike_days: int = 15, spike_factor: float = 4.0,
                   start_date: str = "2024-01-01") -> pd.DataFrame:
    """Generate synthetic daily revenue data with occasional spikes."""
    np.random.seed(42)
    dates = pd.date_range(start_date, periods=n_days, freq="D")
    revenue = np.random.normal(base_revenue, base_revenue * 0.2, n_days).clip(min=10_000)

    # Inject promo spikes
    spike_idx = np.random.choice(n_days, spike_days, replace=False)
    revenue[spike_idx] *= spike_factor

    df = pd.DataFrame({"date": dates, "revenue": revenue})
    # Add extra columns that _build_features expects
    df["orders_count"] = np.random.randint(50, 200, n_days)
    df["unique_customers"] = np.random.randint(30, 150, n_days)
    df["new_customers"] = (df["unique_customers"] * 0.4).astype(int)
    df["returning_customers"] = df["unique_customers"] - df["new_customers"]
    df["returns_count"] = np.random.randint(0, 10, n_days)
    df["instagram_revenue"] = revenue * 0.4
    df["telegram_revenue"] = revenue * 0.3
    df["shopify_revenue"] = revenue * 0.3
    return df


# ---------------------------------------------------------------------------
# Tests for _build_features
# ---------------------------------------------------------------------------

class TestBuildFeatures:
    def test_no_is_weekend_column(self):
        """is_weekend was removed as a dead feature."""
        from core.prediction_service import _build_features
        df = _make_daily_df(n_days=60, spike_days=0)
        result = _build_features(df)
        assert "is_weekend" not in result.columns

    def test_no_log_trend_index_column(self):
        """log_trend_index was removed as a dead feature."""
        from core.prediction_service import _build_features
        df = _make_daily_df(n_days=60, spike_days=0)
        result = _build_features(df)
        assert "log_trend_index" not in result.columns

    def test_feature_columns_present(self):
        """All 31 FEATURE_COLUMNS should be present in output."""
        from core.prediction_service import _build_features, FEATURE_COLUMNS
        df = _make_daily_df(n_days=400, spike_days=0)
        result = _build_features(df)
        for col in FEATURE_COLUMNS:
            assert col in result.columns, f"Missing feature column: {col}"

    def test_feature_columns_count(self):
        from core.prediction_service import FEATURE_COLUMNS
        assert len(FEATURE_COLUMNS) == 31


# ---------------------------------------------------------------------------
# Tests for winsorized _train_model
# ---------------------------------------------------------------------------

class TestTrainModelWinsorized:
    @pytest.fixture
    def training_df(self):
        return _make_daily_df(n_days=400, spike_days=15, spike_factor=4.0)

    def test_returns_four_elements(self, training_df):
        """_train_model should return (model, metrics, dow_corrections, clip_ratio)."""
        from core.prediction_service import _train_model
        result = _train_model(training_df)
        assert len(result) == 4, f"Expected 4-tuple, got {len(result)}-tuple"

    def test_clip_ratio_gt_one(self, training_df):
        """With spikes, clip_ratio should be > 1 (original mean > clipped mean)."""
        from core.prediction_service import _train_model
        _, _, _, clip_ratio = _train_model(training_df)
        assert clip_ratio > 1.0, f"clip_ratio should be > 1 with spikes, got {clip_ratio}"
        assert clip_ratio < 2.0, f"clip_ratio unreasonably high: {clip_ratio}"

    def test_clip_ratio_near_one_without_spikes(self):
        """Without spikes, clip_ratio should be very close to 1."""
        from core.prediction_service import _train_model
        df = _make_daily_df(n_days=400, spike_days=0)
        _, _, _, clip_ratio = _train_model(df)
        assert 0.99 <= clip_ratio <= 1.10, f"clip_ratio={clip_ratio}, expected ~1.0 without spikes"

    def test_metrics_contain_expected_keys(self, training_df):
        """Metrics dict should have mae, mape, wape, validation_days."""
        from core.prediction_service import _train_model
        _, metrics, _, _ = _train_model(training_df)
        assert "mae" in metrics
        assert "mape" in metrics
        assert "wape" in metrics
        assert "validation_days" in metrics
        assert len(metrics["validation_days"]) == 60  # last 60 days

    def test_dow_corrections_all_present(self, training_df):
        """DOW corrections should have entries for all 7 days."""
        from core.prediction_service import _train_model
        _, _, dow_corrections, _ = _train_model(training_df)
        for d in range(7):
            assert d in dow_corrections
            assert 0.70 <= dow_corrections[d] <= 1.30

    def test_model_has_correct_feature_count(self, training_df):
        """Trained model should have 31 features."""
        from core.prediction_service import _train_model, FEATURE_COLUMNS
        model, _, _, _ = _train_model(training_df)
        assert model.num_feature() == len(FEATURE_COLUMNS)

    def test_does_not_mutate_input_df(self, training_df):
        """_train_model should not mutate the input DataFrame."""
        from core.prediction_service import _train_model
        original_revenue = training_df["revenue"].copy()
        _train_model(training_df)
        pd.testing.assert_series_equal(training_df["revenue"], original_revenue)


# ---------------------------------------------------------------------------
# Tests for _predict_future with clip_ratio
# ---------------------------------------------------------------------------

class TestPredictFutureClipRatio:
    @pytest.fixture
    def trained_artifacts(self):
        from core.prediction_service import _train_model
        df = _make_daily_df(n_days=400, spike_days=10)
        model, _, dow_corrections, clip_ratio = _train_model(df)
        return model, df, dow_corrections, clip_ratio

    def test_clip_ratio_affects_predictions(self, trained_artifacts):
        """Predictions with clip_ratio > 1 should be higher than with clip_ratio=1."""
        from core.prediction_service import _predict_future
        model, df, dow_corrections, clip_ratio = trained_artifacts

        future_dates = [date(2025, 2, 15) + timedelta(days=i) for i in range(5)]

        preds_with_ratio = _predict_future(model, df, future_dates, dow_corrections, clip_ratio)
        preds_without_ratio = _predict_future(model, df, future_dates, dow_corrections, 1.0)

        for p_with, p_without in zip(preds_with_ratio, preds_without_ratio):
            if clip_ratio > 1.0:
                assert p_with["predicted_revenue"] > p_without["predicted_revenue"], \
                    f"With clip_ratio={clip_ratio}, prediction should be higher"

    def test_predictions_positive(self, trained_artifacts):
        """All predictions should be non-negative."""
        from core.prediction_service import _predict_future
        model, df, dow_corrections, clip_ratio = trained_artifacts

        future_dates = [date(2025, 2, 15) + timedelta(days=i) for i in range(10)]
        preds = _predict_future(model, df, future_dates, dow_corrections, clip_ratio)

        for p in preds:
            assert p["predicted_revenue"] >= 0

    def test_default_clip_ratio_is_one(self):
        """Without clip_ratio param, default should be 1.0 (backward compat)."""
        from core.prediction_service import _predict_future
        import inspect
        sig = inspect.signature(_predict_future)
        assert sig.parameters["clip_ratio"].default == 1.0


# ---------------------------------------------------------------------------
# Tests for save/load clip_ratio
# ---------------------------------------------------------------------------

class TestSaveLoadClipRatio:
    def test_save_and_load_clip_ratio(self, tmp_path):
        """clip_ratio should round-trip through save/load."""
        from core.prediction_service import PredictionService

        service = PredictionService()
        service._clip_ratio = 1.0732

        clip_path = tmp_path / "clip_ratio.json"
        model_path = tmp_path / "revenue_model.joblib"
        dow_path = tmp_path / "dow_corrections.json"

        mock_model = MagicMock()
        mock_model.num_feature.return_value = 31
        service._model = mock_model
        service._dow_corrections = {i: 1.0 for i in range(7)}

        with patch("core.prediction_service.CLIP_RATIO_PATH", clip_path), \
             patch("core.prediction_service.MODEL_PATH", model_path), \
             patch("core.prediction_service.MODEL_DIR", tmp_path), \
             patch("core.prediction_service.DOW_CORRECTIONS_PATH", dow_path), \
             patch("joblib.dump",   # Skip the actual pickle, but leave a file:
                   # the writer dumps to a sibling temp path and os.replace()s
                   # it over MODEL_PATH, so the stub has to produce something.
                   side_effect=lambda obj, path: pathlib.Path(path).touch()):
            service._save_model()

        # Verify clip_ratio file content
        assert clip_path.exists()
        data = json.loads(clip_path.read_text())
        assert abs(data["clip_ratio"] - 1.0732) < 1e-6

        # Create a fake model file so MODEL_PATH.exists() returns True
        model_path.touch()

        # Load into new service
        service2 = PredictionService()
        with patch("core.prediction_service.MODEL_PATH", model_path), \
             patch("core.prediction_service.DOW_CORRECTIONS_PATH", dow_path), \
             patch("core.prediction_service.CLIP_RATIO_PATH", clip_path), \
             patch("joblib.load", return_value=mock_model):
            service2._load_model()

        assert abs(service2._clip_ratio - 1.0732) < 1e-6

    def test_load_missing_clip_ratio_defaults_to_one(self, tmp_path):
        """When clip_ratio.json doesn't exist, should default to 1.0."""
        from core.prediction_service import PredictionService

        service = PredictionService()
        clip_path = tmp_path / "clip_ratio.json"
        model_path = tmp_path / "revenue_model.joblib"
        dow_path = tmp_path / "dow_corrections.json"

        mock_model = MagicMock()
        mock_model.num_feature.return_value = 31

        with patch("core.prediction_service.MODEL_PATH", model_path), \
             patch("core.prediction_service.DOW_CORRECTIONS_PATH", dow_path), \
             patch("core.prediction_service.CLIP_RATIO_PATH", clip_path), \
             patch("joblib.load", return_value=mock_model):
            service._load_model()

        assert service._clip_ratio == 1.0


# ---------------------------------------------------------------------------
# Tests for _run_evaluation with winsorization
# ---------------------------------------------------------------------------

class TestEvaluationWinsorized:
    def test_evaluation_runs_with_winsorization(self):
        """_run_evaluation should complete without errors using winsorized folds."""
        from core.prediction_service import _run_evaluation

        # Need enough data for 6 monthly folds + training
        df = _make_daily_df(n_days=600, spike_days=25, start_date="2023-06-01")
        result = _run_evaluation(df)

        assert "summary" in result
        assert "cross_validation" in result
        assert result["summary"]["num_folds"] >= 3
        assert result["summary"]["lgbm_wape"] > 0

    def test_evaluation_baselines_present(self):
        """Evaluation should include baseline comparisons."""
        from core.prediction_service import _run_evaluation

        df = _make_daily_df(n_days=600, spike_days=25, start_date="2023-06-01")
        result = _run_evaluation(df)

        folds = result["cross_validation"]["folds"]
        for fold in folds:
            assert "baselines" in fold
            assert "naive_7d" in fold["baselines"]
            assert "weekday_avg_12w" in fold["baselines"]


# ---------------------------------------------------------------------------
# Tests for _tune_hyperparameters with winsorization
# ---------------------------------------------------------------------------

class TestTuneWinsorized:
    def test_tuning_runs_with_winsorization(self, tmp_path):
        """_tune_hyperparameters should complete with winsorized fold data."""
        from core.prediction_service import _tune_hyperparameters

        df = _make_daily_df(n_days=600, spike_days=25, start_date="2023-06-01")

        with patch("core.prediction_service.TUNED_PARAMS_PATH", tmp_path / "params.json"), \
             patch("core.prediction_service.MODEL_DIR", tmp_path):
            result = _tune_hyperparameters(df)

        assert result["status"] == "success"
        assert "best_params" in result
        assert "best_wape" in result
        assert result["best_wape"] > 0
        assert result["folds"] >= 3


# ---------------------------------------------------------------------------
# Tests for PredictionService class integration
# ---------------------------------------------------------------------------

class TestPredictionServiceInit:
    def test_clip_ratio_initialized(self):
        from core.prediction_service import PredictionService
        svc = PredictionService()
        assert svc._clip_ratio == 1.0


# ---------------------------------------------------------------------------
# The trend's forecast is the stored one
# ---------------------------------------------------------------------------

class TestTheRangeForecastIsTheStoredOne:
    """`/api/revenue/trend?include_forecast=true` shows the week, or a custom
    range's future days, from `revenue_predictions` — what the last training
    stored — as it shows the month (owner's decision, 2026-11-04). Until then
    it ran the model on the request (`predict_range`): one feature rebuild
    per day on the one ML thread training waits for too, over 780 days of
    Gold read live. Here no model is loaded and running one fails the test,
    so an answer proves it was never asked."""

    TODAY = date(2026, 10, 21)
    # 22:30 UTC on the 20th is 01:30 on the 21st in Kyiv (EEST): the day the
    # training ran there, which the UTC date would put a day early.
    TRAINED = datetime(2026, 10, 20, 22, 30, tzinfo=timezone.utc)
    METRICS = {"mae": 900.0, "mape": 30.5, "wape": 24.2}

    @pytest.fixture
    def service(self, monkeypatch):
        import core.prediction_service as ps

        asked = []
        trained = self.TRAINED

        class Store:
            async def get_stored_forecast(self, start_date, end_date, sales_type="retail"):
                asked.append((start_date, end_date, sales_type))
                if sales_type != "retail":           # the only one trained
                    return [], None
                return [{"date": (start_date + timedelta(days=i)).isoformat(),
                         "predicted_revenue": 1000.0 + i,
                         "model_mae": 900.0, "model_mape": 30.5, "model_wape": 24.2}
                        for i in range((end_date - start_date).days + 1)], trained

        async def store():
            return Store()

        def never(*args, **kwargs):
            raise AssertionError("the model ran on a request")

        monkeypatch.setattr(ps, "_today_kyiv", lambda: self.TODAY)
        monkeypatch.setattr(ps, "_predict_future", never)
        monkeypatch.setattr("core.duckdb_store.get_store", store)
        svc = ps.PredictionService()
        monkeypatch.setattr(svc, "_query_daily_revenue", never)
        assert not svc.is_ready
        return svc, asked

    def _days(self, first: int, last: int):
        return [(self.TODAY + timedelta(days=i)).isoformat() for i in range(first, last + 1)]

    @pytest.mark.asyncio
    async def test_a_week_is_its_days_from_today_on(self, service):
        svc, asked = service
        out = await svc.get_range_forecast(self.TODAY - timedelta(days=2),
                                           self.TODAY + timedelta(days=4))
        assert asked == [(self.TODAY, self.TODAY + timedelta(days=4), "retail")]
        assert [p["date"] for p in out["daily_predictions"]] == self._days(0, 4)
        assert out["predicted_total"] == out["predicted_remaining"] == 5010.0

    @pytest.mark.asyncio
    async def test_a_range_past_the_horizon_is_read_up_to_it(self, service):
        """The table holds nothing past it anyway — a training stores sixty
        days — and the date picker stops there."""
        from core.prediction_service import FORECAST_HORIZON_DAYS

        svc, asked = service
        out = await svc.get_range_forecast(self.TODAY - timedelta(days=5),
                                           self.TODAY + timedelta(days=400))
        assert asked == [(self.TODAY, self.TODAY + timedelta(days=FORECAST_HORIZON_DAYS),
                          "retail")]
        assert len(out["daily_predictions"]) == FORECAST_HORIZON_DAYS + 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("first,last", [(100, 130), (-10, -1)],
                             ids=["past-the-horizon", "in-the-past"])
    async def test_a_range_with_no_day_in_the_window_reads_nothing(self, service, first, last):
        svc, asked = service
        assert await svc.get_range_forecast(self.TODAY + timedelta(days=first),
                                            self.TODAY + timedelta(days=last)) is None
        assert asked == []

    @pytest.mark.asyncio
    async def test_a_sales_type_never_trained_has_no_forecast(self, service):
        """Training is retail's alone (`revenue_prediction_train`). The model
        used to run over b2b's history all the same, which answered numbers
        nobody had trained for."""
        svc, asked = service
        assert await svc.get_range_forecast(self.TODAY, self.TODAY + timedelta(days=6),
                                            "b2b") is None
        assert asked == [(self.TODAY, self.TODAY + timedelta(days=6), "b2b")]

    @pytest.mark.asyncio
    async def test_its_stamp_and_metrics_are_the_rows_not_the_process(self, service):
        """A training in this process left its own day and metrics in memory,
        sixty validation days with them; the rows are what any process can
        read, and what the forecast shown was made by."""
        svc, _asked = service
        svc._metrics = {"mae": 1.0, "mape": 2.0, "wape": 3.0,
                        "validation_days": [{"date": "2026-08-01"}]}
        out = await svc.get_range_forecast(self.TODAY, self.TODAY + timedelta(days=6))
        assert out["last_trained"] == "2026-10-21"
        assert out["model_metrics"] == self.METRICS

    @pytest.mark.asyncio
    async def test_the_month_reads_them_from_the_rows_too(self, service, monkeypatch):
        """`get_forecast`: null and the rows' metrics after every restart
        until a training, the in-memory dict after one — now the rows."""
        svc, asked = service
        svc._metrics = {"mae": 1.0, "validation_days": []}
        monkeypatch.setattr(svc, "_get_actual_month_revenue",
                            lambda *a, **k: _value(12_345.0))
        out = await svc.get_forecast("retail")
        assert asked[-1][:2] == (self.TODAY, self.TODAY + timedelta(days=60))
        assert out["last_trained"] == "2026-10-21"
        assert out["model_metrics"] == self.METRICS
        assert out["actual_to_date"] == 12_345.0


async def _value(value):
    return value


class TestTrainedOn:
    """`last_trained` is the Kyiv day of the stored stamp, ISO."""

    def test_none_is_none(self):
        from core.prediction_service import _trained_on
        assert _trained_on(None) is None

    def test_the_kyiv_day_not_the_utc_one(self):
        from core.prediction_service import _trained_on
        assert _trained_on(datetime(2026, 10, 20, 22, 30, tzinfo=timezone.utc)) == "2026-10-21"
        # Winter time: 23:30 UTC on 1 November is 01:30 on the 2nd (EET).
        assert _trained_on(datetime(2026, 11, 1, 23, 30, tzinfo=timezone.utc)) == "2026-11-02"

    def test_a_naive_stamp_is_utc(self):
        from core.prediction_service import _trained_on
        assert _trained_on(datetime(2026, 11, 2, 1, 30)) == "2026-11-02"
        assert _trained_on(datetime(2026, 11, 1, 22, 30)) == "2026-11-02"
