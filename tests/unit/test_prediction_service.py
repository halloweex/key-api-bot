"""Tests for the LightGBM revenue prediction: the training procedure of
2026-11-18 (Huber loss, NUM_BOOST_ROUND trees on every row, revenue-only
features, no winsorising, no weekday correction, no early stopping)."""
import pathlib
from datetime import date, datetime, timedelta, timezone
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


@pytest.fixture(scope="module")
def trained():
    """One training shared by the tests that only read its result."""
    from core.prediction_service import _train_model
    df = _make_daily_df(n_days=400, spike_days=15, spike_factor=4.0)
    model, metrics = _train_model(df)
    return model, metrics, df


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------

class TestBuildFeatures:

    def test_feature_columns_present(self):
        """Every FEATURE_COLUMNS entry is in the built frame."""
        from core.prediction_service import _build_features, FEATURE_COLUMNS
        df = _make_daily_df(n_days=400, spike_days=0)
        result = _build_features(df)
        for col in FEATURE_COLUMNS:
            assert col in result.columns, f"Missing feature column: {col}"

    def test_feature_columns_count(self):
        from core.prediction_service import FEATURE_COLUMNS
        assert len(FEATURE_COLUMNS) == 25

    def test_order_and_customer_columns_are_not_features(self):
        """A future day has no orders or customers: inside the recursive
        forecast these turned NaN → 0 within a week, a value the model never
        trained on. They are still built, and never read by the model."""
        from core.prediction_service import _build_features, FEATURE_COLUMNS
        exogenous = ["rolling_mean_7d_aov", "lag_7d_orders", "rolling_mean_7d_orders",
                     "rolling_mean_7d_new_cust_ratio", "rolling_mean_7d_returning_ratio",
                     "rolling_mean_7d_return_rate"]
        built = _build_features(_make_daily_df(n_days=60, spike_days=0))
        for col in exogenous:
            assert col in built.columns
            assert col not in FEATURE_COLUMNS


# ---------------------------------------------------------------------------
# The training procedure
# ---------------------------------------------------------------------------

class TestTheParameters:
    def test_huber_loss_at_the_pinned_delta(self):
        from core.prediction_service import _build_lgb_params, HUBER_DELTA
        p = _build_lgb_params()
        assert p["objective"] == "huber"
        assert p["alpha"] == HUBER_DELTA == 60000.0

    def test_the_validated_hyperparameters_are_pinned(self):
        """The values the month-total check measured, independent of any file."""
        from core.prediction_service import _build_lgb_params
        p = _build_lgb_params()
        assert (p["num_leaves"], p["learning_rate"], p["min_child_samples"],
                p["reg_alpha"], p["subsample"]) == (31, 0.01, 5, 0.1, 0.8)

    def test_a_stale_params_file_is_not_read(self, tmp_path, monkeypatch):
        """`lgbm_best_params.json` set the hyperparameters until 2026-11-18;
        one left on disk must not move the model away from the measured one."""
        import core.prediction_service as ps
        (tmp_path / "lgbm_best_params.json").write_text(
            '{"num_leaves": 63, "learning_rate": 0.1, "min_child_samples": 20,'
            ' "reg_alpha": 0, "subsample": 1.0}')
        monkeypatch.setattr(ps, "MODEL_DIR", tmp_path)
        assert ps._build_lgb_params()["num_leaves"] == 31
        assert not hasattr(ps, "TUNED_PARAMS_PATH")

    def test_a_candidate_set_is_used_when_given(self):
        from core.prediction_service import _build_lgb_params
        hp = {"num_leaves": 15, "learning_rate": 0.05, "min_child_samples": 20,
              "reg_alpha": 0, "subsample": 1.0}
        p = _build_lgb_params(hp)
        assert p["num_leaves"] == 15 and p["learning_rate"] == 0.05
        assert p["objective"] == "huber"


class TestTrainModel:
    def test_returns_model_and_metrics(self, trained):
        model, metrics, _df = trained
        assert hasattr(model, "predict")
        assert isinstance(metrics, dict)

    def test_fixed_number_of_trees(self, trained):
        """No early stopping: the model can no longer stop at one tree."""
        from core.prediction_service import NUM_BOOST_ROUND
        model, _metrics, _df = trained
        assert model.num_trees() == NUM_BOOST_ROUND == 300

    def test_metrics_contain_expected_keys(self, trained):
        from core.prediction_service import VALIDATION_DAYS
        _model, metrics, _df = trained
        for key in ("mae", "mape", "wape", "validation_days"):
            assert key in metrics
        assert len(metrics["validation_days"]) == VALIDATION_DAYS
        assert 0 < metrics["wape"] < 100

    def test_model_has_correct_feature_count(self, trained):
        from core.prediction_service import FEATURE_COLUMNS
        model, _metrics, _df = trained
        assert model.num_feature() == len(FEATURE_COLUMNS)

    def test_the_model_trains_on_every_row_and_the_twin_without_the_last_days(self, monkeypatch):
        """The forecast model sees the most recent days; the twin that the
        WAPE gate judges is fitted without the last VALIDATION_DAYS rows.
        Until 2026-11-18 the model never saw them at all."""
        import core.prediction_service as ps
        sizes = []
        real = ps._fit_booster

        def recording(X, y, params=None):
            sizes.append(len(X))
            return real(X, y, params)

        monkeypatch.setattr(ps, "_fit_booster", recording)
        monkeypatch.setattr(ps, "NUM_BOOST_ROUND", 10)
        df = _make_daily_df(n_days=200, spike_days=5)
        ps._train_model(df)
        n = len(ps._training_rows(df.assign(date=pd.to_datetime(df["date"]))))
        assert sizes == [n - ps.VALIDATION_DAYS, n]

    def test_promotions_are_not_winsorised(self):
        """Huber loss replaced the P95 clip, which erased Black Friday."""
        from core.prediction_service import _training_rows
        df = _make_daily_df(n_days=200, spike_days=10, spike_factor=4.0)
        rows = _training_rows(df.assign(date=pd.to_datetime(df["date"])))
        original = dict(zip(pd.to_datetime(df["date"]), df["revenue"]))
        assert all(v == original[d] for d, v in zip(rows["date"], rows["revenue"]))
        assert rows["revenue"].max() > df["revenue"].quantile(0.95)

    def test_not_enough_rows_raises(self):
        from core.prediction_service import _train_model
        with pytest.raises(ValueError, match="Not enough training data"):
            _train_model(_make_daily_df(n_days=100, spike_days=0))

    def test_does_not_mutate_input_df(self):
        from core.prediction_service import _train_model
        df = _make_daily_df(n_days=200, spike_days=5)
        original_revenue = df["revenue"].copy()
        with patch("core.prediction_service.NUM_BOOST_ROUND", 10):
            _train_model(df)
        pd.testing.assert_series_equal(df["revenue"], original_revenue)


# ---------------------------------------------------------------------------
# The forecast applies no multiplier
# ---------------------------------------------------------------------------

class TestPredictFuture:
    def test_signature_has_no_multiplier(self):
        """The weekday correction and the clip ratio are gone, so nothing can
        pass a stale one in."""
        import inspect
        from core.prediction_service import _predict_future
        assert list(inspect.signature(_predict_future).parameters) == [
            "model", "historical_df", "future_dates"]

    def test_the_forecast_is_the_model_output(self):
        from core.prediction_service import _predict_future
        model = MagicMock()
        model.predict.return_value = np.array([1234.5])
        df = _make_daily_df(n_days=120, spike_days=0)
        days = [date(2024, 4, 30) + timedelta(days=i) for i in range(7)]
        preds = _predict_future(model, df, days)
        assert [p["predicted_revenue"] for p in preds] == [1234.5] * 7

    def test_predictions_non_negative(self, trained):
        from core.prediction_service import _predict_future
        model, _metrics, df = trained
        days = [date(2025, 2, 4) + timedelta(days=i) for i in range(10)]
        preds = _predict_future(model, df, days)
        assert len(preds) == 10
        assert all(p["predicted_revenue"] >= 0 for p in preds)


# ---------------------------------------------------------------------------
# Save and load
# ---------------------------------------------------------------------------

class TestSaveLoad:
    def _paths(self, monkeypatch, tmp_path):
        import core.prediction_service as ps
        monkeypatch.setattr(ps, "MODEL_DIR", tmp_path)
        monkeypatch.setattr(ps, "MODEL_PATH", tmp_path / "revenue_model.joblib")
        return ps

    def test_save_writes_the_model_and_nothing_else(self, tmp_path, monkeypatch):
        ps = self._paths(monkeypatch, tmp_path)
        svc = ps.PredictionService()
        svc._model = MagicMock()
        # Skip the actual pickle, but leave a file: the writer dumps to a
        # sibling temp path and os.replace()s it over MODEL_PATH.
        with patch("joblib.dump", side_effect=lambda obj, path: pathlib.Path(path).touch()):
            svc._save_model()
        assert sorted(p.name for p in tmp_path.iterdir()) == ["revenue_model.joblib"]

    def test_load_round_trip_ignores_stale_multiplier_files(self, tmp_path, monkeypatch):
        """`dow_corrections.json` and `clip_ratio.json` from the procedure
        before 2026-11-18 stay on the server; read with the current model they
        would scale its forecast by factors fitted to another one."""
        ps = self._paths(monkeypatch, tmp_path)
        (tmp_path / "revenue_model.joblib").touch()
        (tmp_path / "dow_corrections.json").write_text('{"0": 1.3, "1": 0.7}')
        (tmp_path / "clip_ratio.json").write_text('{"clip_ratio": 1.9}')
        model = MagicMock()
        model.num_feature.return_value = len(ps.FEATURE_COLUMNS)
        svc = ps.PredictionService()
        with patch("joblib.load", return_value=model):
            assert svc._load_model() is True
        assert svc._model is model
        assert not hasattr(svc, "_dow_corrections") and not hasattr(svc, "_clip_ratio")

    def test_the_old_31_feature_model_is_rejected(self, tmp_path, monkeypatch):
        """So web's startup trains the current one (`web/main.py`)."""
        ps = self._paths(monkeypatch, tmp_path)
        (tmp_path / "revenue_model.joblib").touch()
        old = MagicMock()
        old.num_feature.return_value = 31
        svc = ps.PredictionService()
        with patch("joblib.load", return_value=old):
            assert svc._load_model() is False
        assert not svc.is_ready


# ---------------------------------------------------------------------------
# Evaluation and tuning use the same procedure
# ---------------------------------------------------------------------------

class TestEvaluation:
    def test_evaluation_runs(self):
        from core.prediction_service import _run_evaluation
        # Need enough data for 6 monthly folds + training
        df = _make_daily_df(n_days=600, spike_days=25, start_date="2023-06-01")
        with patch("core.prediction_service.NUM_BOOST_ROUND", 30):
            result = _run_evaluation(df)
        assert "summary" in result
        assert "cross_validation" in result
        assert result["summary"]["num_folds"] >= 3
        assert result["summary"]["lgbm_wape"] > 0
        for fold in result["cross_validation"]["folds"]:
            assert "naive_7d" in fold["baselines"]
            assert "weekday_avg_12w" in fold["baselines"]


class TestTune:
    def test_tuning_reports_and_changes_nothing(self, tmp_path, monkeypatch):
        """The grid scores one-step-ahead daily WAPE; the badge is a month
        total. Its best set is reported, never applied."""
        import core.prediction_service as ps
        monkeypatch.setattr(ps, "MODEL_DIR", tmp_path)
        monkeypatch.setattr(ps, "NUM_BOOST_ROUND", 10)
        before = dict(ps.PRODUCTION_LGB_PARAMS)
        df = _make_daily_df(n_days=600, spike_days=25, start_date="2023-06-01")
        result = ps._tune_hyperparameters(df)
        assert result["status"] == "success"
        assert result["applied"] is False
        assert result["best_wape"] > 0 and result["folds"] >= 3
        assert ps.PRODUCTION_LGB_PARAMS == before
        assert list(tmp_path.iterdir()) == []


class TestPredictionServiceInit:
    def test_no_multiplier_state(self):
        from core.prediction_service import PredictionService
        svc = PredictionService()
        assert not svc.is_ready
        assert not hasattr(svc, "_clip_ratio") and not hasattr(svc, "_dow_corrections")


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

        async def no_history(*args, **kwargs):
            # The month's range reads Gold (`TestTheMonthOutlook`); here it
            # has none, and never reaches a pool.
            return pd.DataFrame({"date": pd.to_datetime([]), "revenue": []})

        monkeypatch.setattr(ps, "_today_kyiv", lambda: self.TODAY)
        monkeypatch.setattr(ps, "_predict_future", never)
        monkeypatch.setattr("core.store.get_store", store)
        monkeypatch.setattr("core.pg_forecast_read.fetch_frame", no_history)
        _no_promotions(monkeypatch)
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


def _no_promotions(monkeypatch):
    """An empty calendar, read without a pool: `get_forecast` asks it for
    `planned_promotions` (revision 0039)."""
    async def latest_recorded_at():
        return None

    async def current(first, last, include_cancelled=False):
        return []

    monkeypatch.setattr("core.promotions.latest_recorded_at", latest_recorded_at)
    monkeypatch.setattr("core.promotions.current", current)


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


# ---------------------------------------------------------------------------
# The month's range and Black Friday line beside the forecast
# ---------------------------------------------------------------------------

class TestTheMonthOutlook:
    """`/api/revenue/forecast` carries `month_range` and `black_friday`
    (2026-11-18): the range K + F × [q10, q90] of `core/forecast_range.py`
    over 900 days of Gold through yesterday, F the stored forecast of the
    days left. Computed on the ML thread, kept per Kyiv day, sales type and
    training, and never a reason for the forecast to fail. The series is the
    synthetic shop of `tests/unit/test_forecast_range.py`, whose Black
    Friday was 2024-11-29..12-01."""

    TODAY = date(2025, 11, 20)
    TRAINED = datetime(2025, 11, 17, 0, 30, tzinfo=timezone.utc)

    @pytest.fixture
    def service(self, monkeypatch):
        from concurrent.futures import ThreadPoolExecutor

        import core.prediction_service as ps
        from tests.helpers.forecast_range_series import REVENUE

        state = {"trained": self.TRAINED, "missing": None, "fail": None, "reads": [],
                 "revenue": REVENUE, "promotions": [], "stamp": None,
                 "stamp_fail": None, "list_fail": None, "listed": []}

        class Store:
            async def get_stored_forecast(self, start_date, end_date, sales_type="retail"):
                rows = [{"date": (start_date + timedelta(days=i)).isoformat(),
                         "predicted_revenue": 125_000.0,
                         "model_mae": 1.0, "model_mape": 2.0, "model_wape": 3.0}
                        for i in range((end_date - start_date).days + 1)]
                return [r for r in rows if r["date"] != state["missing"]], state["trained"]

        async def store():
            return Store()

        async def fetch_frame(start, today, sales_type, exclude_today):
            state["reads"].append((start, today, sales_type, exclude_today))
            if state["fail"] is not None:
                raise state["fail"]
            revenue = state["revenue"]
            days = sorted(d for d in revenue if start <= d < today)
            return pd.DataFrame({"date": pd.to_datetime(days),
                                 "revenue": [revenue[d] for d in days]})

        async def latest_recorded_at():
            if state["stamp_fail"] is not None:
                raise state["stamp_fail"]
            return state["stamp"]

        async def current(first, last, include_cancelled=False):
            state["listed"].append((first, last, include_cancelled))
            if state["list_fail"] is not None:
                raise state["list_fail"]
            return state["promotions"]

        async def actual(*args, **kwargs):
            return 2_000_000.0

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ml-train")
        monkeypatch.setattr(ps, "_executor", executor)
        monkeypatch.setattr(ps, "_today_kyiv", lambda: self.TODAY)
        monkeypatch.setattr("core.store.get_store", store)
        monkeypatch.setattr("core.pg_forecast_read.fetch_frame", fetch_frame)
        monkeypatch.setattr("core.promotions.latest_recorded_at", latest_recorded_at)
        monkeypatch.setattr("core.promotions.current", current)
        svc = ps.PredictionService()
        monkeypatch.setattr(svc, "_get_actual_month_revenue", actual)
        yield svc, state
        executor.shutdown(wait=True)

    def _expected(self):
        from core import forecast_range as fr
        from tests.helpers.forecast_range_series import REVENUE

        rows = [(d, v) for d, v in sorted(REVENUE.items()) if d < self.TODAY]
        return fr.month_outlook(rows, self.TODAY, 11 * 125_000.0)

    @pytest.mark.asyncio
    async def test_the_month_carries_its_range_and_black_friday_line(self, service):
        from core.forecast_range import HISTORY_DAYS

        svc, state = service
        out = await svc.get_forecast("retail")
        expected = self._expected()

        assert out["month_range"] == expected["month_range"]
        assert out["black_friday"] == expected["black_friday"]
        assert out["month_range"]["days_left"] == 11 and out["month_range"]["coverage"] == 0.8
        assert out["black_friday"]["last_year_start"] == "2024-11-29"
        # Gold through yesterday, 900 days of it, by the training frame's reader.
        assert state["reads"] == [(self.TODAY - timedelta(days=HISTORY_DAYS), self.TODAY,
                                   "retail", True)]
        # The rest of the answer is what it was.
        assert out["actual_to_date"] == 2_000_000.0
        assert out["month_end"] == "2025-11-30"

    @pytest.mark.asyncio
    async def test_it_runs_on_the_ml_thread(self, service, monkeypatch):
        import threading

        from core import forecast_range as fr

        svc, _state = service
        threads = []
        original = fr.month_outlook

        def recorded(*args):
            threads.append(threading.current_thread().name)
            return original(*args)

        monkeypatch.setattr(fr, "month_outlook", recorded)
        await svc.get_forecast("retail")
        assert len(threads) == 1 and threads[0].startswith("ml-train")

    @pytest.mark.asyncio
    async def test_once_per_day_and_training(self, service, monkeypatch):
        import core.prediction_service as ps

        svc, state = service
        first = await svc.get_forecast("retail")
        assert (await svc.get_forecast("retail"))["month_range"] == first["month_range"]
        assert len(state["reads"]) == 1

        state["trained"] = self.TRAINED + timedelta(days=3)     # Thursday's training
        await svc.get_forecast("retail")
        assert len(state["reads"]) == 2

        later = ps._monotonic() + ps.OUTLOOK_TTL_S + 1      # an hour on: read again
        monkeypatch.setattr(ps, "_monotonic", lambda: later)
        await svc.get_forecast("retail")
        assert len(state["reads"]) == 3

    @pytest.mark.asyncio
    async def test_a_failure_is_nulls_and_the_forecast_stands(self, service, caplog):
        svc, state = service
        state["fail"] = ConnectionError("gold is not answering")
        with caplog.at_level("WARNING", logger="core.prediction_service"):
            out = await svc.get_forecast("retail")
        assert out["month_range"] is None and out["black_friday"] is None
        assert out["predicted_total"] > 0 and len(out["daily_predictions"]) == 61
        assert "gold is not answering" in caplog.text
        # Not kept: the next request tries again, and answers.
        state["fail"] = None
        assert (await svc.get_forecast("retail"))["month_range"] == self._expected()["month_range"]
        assert len(state["reads"]) == 2

    @pytest.mark.asyncio
    async def test_a_computation_stuck_behind_training_is_dropped(self, service, monkeypatch):
        import time as _time

        import core.prediction_service as ps
        from core import forecast_range as fr

        svc, state = service
        monkeypatch.setattr(ps, "OUTLOOK_TIMEOUT_S", 0.05)
        monkeypatch.setattr(fr, "month_outlook", lambda *a: _time.sleep(0.5) or {})
        out = await svc.get_forecast("retail")
        assert out["month_range"] is None and out["black_friday"] is None
        assert out["predicted_total"] > 0

    @pytest.mark.asyncio
    async def test_an_incomplete_forecast_of_the_month_has_no_range(self, service):
        svc, state = service
        state["missing"] = "2025-11-25"
        out = await svc.get_forecast("retail")
        assert out["month_range"] is None and out["black_friday"] is None
        assert state["reads"] == []

    @pytest.mark.asyncio
    async def test_only_retail_has_one(self, service):
        """Training is retail's alone; a forecast of anything else, were one
        ever stored, gets no range."""
        svc, state = service
        out = await svc.get_forecast("b2b")
        assert out["month_range"] is None and out["black_friday"] is None
        assert out["planned_promotions"] is None
        assert state["reads"] == [] and state["listed"] == []

    @pytest.mark.asyncio
    async def test_the_trend_overlay_is_not_touched(self, service):
        """The trend's forecast is `get_range_forecast`, mirrored by the Rust
        read API: no new keys there, and no Gold read."""
        svc, state = service
        week = await svc.get_range_forecast(self.TODAY, self.TODAY + timedelta(days=6))
        assert set(week) == {"predicted_remaining", "predicted_total", "daily_predictions",
                             "model_metrics", "last_trained"}
        assert state["reads"] == []

    # ── planned_promotions (revision 0039) ──────────────────────────────────

    def _calendar(self, state):
        """Two promotions this month — one ordinary, one Black Friday — over
        the typical-promotion test's shop, whose runs give a typical effect
        (`tests/unit/test_typical_promotion.py`)."""
        from core import promotions
        from tests.helpers.typical_promotion_series import REVENUE as SHOP

        def promotion(pid, name, first, last, kind):
            return promotions.Promotion(
                id=pid, version=1, status="planned", recorded_at=self.TRAINED,
                recorded_by=1, created_at=self.TRAINED,
                data=promotions.PromotionIn(name=name, starts_on=first, ends_on=last,
                                            kind=kind, scope="store"))

        state["revenue"] = SHOP
        state["stamp"] = datetime(2025, 11, 19, 9, 0, tzinfo=timezone.utc)
        state["promotions"] = [
            promotion(12, "Весняний розпродаж", date(2025, 11, 22), date(2025, 11, 23), "other"),
            promotion(13, "Чорна п'ятниця", date(2025, 11, 28), date(2025, 11, 30),
                      "black_friday"),
        ]
        rows = [(d, v) for d, v in sorted(SHOP.items()) if d < self.TODAY]
        typical = fr_promotion_outlook(rows, self.TODAY)
        assert typical is not None
        return round(typical.extra, 2)

    @pytest.mark.asyncio
    async def test_the_month_lists_what_is_planned_with_the_typical_effect(self, service):
        """`extra` for an ordinary promotion is the typical one, computed from
        the range's own read on the ML thread; Black Friday's is null, its own
        line covers it; the calendar is asked for today to the month's end.
        Kills: the wrong window, an extra on Black Friday, or an extra that is
        not `typical_promotion`'s."""
        svc, state = service
        extra = self._calendar(state)
        out = await svc.get_forecast("retail")
        assert out["planned_promotions"] == [
            {"id": 12, "name": "Весняний розпродаж", "starts_on": "2025-11-22",
             "ends_on": "2025-11-23", "kind": "other", "extra": extra},
            {"id": 13, "name": "Чорна п'ятниця", "starts_on": "2025-11-28",
             "ends_on": "2025-11-30", "kind": "black_friday", "extra": None},
        ]
        assert state["listed"] == [(self.TODAY, date(2025, 11, 30), False)]
        assert len(state["reads"]) == 1
        # The point and the range do not move with it.
        assert out["month_range"] is not None
        assert out["predicted_total"] == 2_000_000.0 + 61 * 125_000.0

    @pytest.mark.asyncio
    async def test_nothing_planned_is_an_empty_list(self, service):
        svc, state = service
        out = await svc.get_forecast("retail")
        assert out["planned_promotions"] == []

    @pytest.mark.asyncio
    async def test_a_save_shows_on_the_next_request(self, service):
        """The promotions' latest `recorded_at` is in the cache key. Kills: a
        key without it — a promotion added at 10:00 missing from the badge
        until 11:00."""
        svc, state = service
        self._calendar(state)
        first = await svc.get_forecast("retail")
        again = await svc.get_forecast("retail")
        assert again["planned_promotions"] == first["planned_promotions"]
        assert len(state["listed"]) == 1 and len(state["reads"]) == 1

        state["promotions"] = state["promotions"][1:]
        state["stamp"] = state["stamp"] + timedelta(minutes=5)
        after = await svc.get_forecast("retail")
        assert [p["id"] for p in after["planned_promotions"]] == [13]
        assert len(state["listed"]) == 2

    @pytest.mark.asyncio
    async def test_an_unreadable_calendar_nulls_its_key_alone(self, service):
        """The range and Black Friday stand; `planned_promotions` is null,
        and the next request whose calendar reads lists it. Kills: a failure
        of the calendar costing the badge its range, or kept for an hour."""
        svc, state = service
        self._calendar(state)
        state["stamp_fail"] = ConnectionError("calendar down")
        out = await svc.get_forecast("retail")
        assert out["planned_promotions"] is None
        assert out["month_range"] is not None
        assert state["listed"] == []

        state["stamp_fail"] = None
        later = await svc.get_forecast("retail")
        assert [p["id"] for p in later["planned_promotions"]] == [12, 13]

    @pytest.mark.asyncio
    async def test_a_list_that_fails_is_asked_again(self, service):
        svc, state = service
        self._calendar(state)
        state["list_fail"] = ConnectionError("list refused")
        out = await svc.get_forecast("retail")
        assert out["planned_promotions"] is None and out["month_range"] is not None
        state["list_fail"] = None
        later = await svc.get_forecast("retail")
        assert [p["id"] for p in later["planned_promotions"]] == [12, 13]

    @pytest.mark.asyncio
    async def test_without_a_forecast_of_the_month_the_list_has_no_extra(self, service):
        """No forecast of every day left: no history read, so no typical
        effect — the promotions are still said."""
        svc, state = service
        self._calendar(state)
        state["missing"] = "2025-11-25"
        out = await svc.get_forecast("retail")
        assert [(p["id"], p["extra"]) for p in out["planned_promotions"]] == [
            (12, None), (13, None)]
        assert state["reads"] == []


def fr_promotion_outlook(rows, origin):
    from core import forecast_range

    return forecast_range.promotion_outlook(rows, origin)
