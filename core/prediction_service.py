"""
LightGBM-based revenue prediction service.

Trains on historical daily revenue data and predicts remaining days of the current month.
Runs nightly via scheduler, stores predictions in DuckDB.
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Model storage
MODEL_DIR = Path(__file__).parent.parent / "data"
MODEL_PATH = MODEL_DIR / "revenue_model.joblib"

# Thread pool for CPU-bound training
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ml-train")


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer features from a DataFrame with 'date' and 'revenue' columns.

    Creates 20 features:
    - Calendar: day_of_week, month, day_of_month, is_weekend, week_of_year, quarter
    - Cyclical: month_sin, month_cos, dow_sin, dow_cos
    - Lags: 1d, 7d, 14d, 28d, 365d
    - Rolling: mean_7d, mean_14d, mean_28d, std_7d
    - Trend: yoy_ratio, linear trend index
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Calendar features
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['day_of_month'] = df['date'].dt.day
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    df['week_of_year'] = df['date'].dt.isocalendar().week.astype(int)
    df['quarter'] = df['date'].dt.quarter

    # Cyclical encoding
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)

    # Lag features
    df['lag_1d'] = df['revenue'].shift(1)
    df['lag_7d'] = df['revenue'].shift(7)
    df['lag_14d'] = df['revenue'].shift(14)
    df['lag_28d'] = df['revenue'].shift(28)
    df['lag_365d'] = df['revenue'].shift(365)

    # Rolling features
    df['rolling_mean_7d'] = df['revenue'].shift(1).rolling(7, min_periods=3).mean()
    df['rolling_mean_14d'] = df['revenue'].shift(1).rolling(14, min_periods=7).mean()
    df['rolling_mean_28d'] = df['revenue'].shift(1).rolling(28, min_periods=14).mean()
    df['rolling_std_7d'] = df['revenue'].shift(1).rolling(7, min_periods=3).std()

    # Year-over-year ratio
    df['yoy_ratio'] = df['revenue'] / df['lag_365d'].replace(0, np.nan)

    # Linear trend index
    df['trend_index'] = np.arange(len(df))

    return df


FEATURE_COLUMNS = [
    'day_of_week', 'month', 'day_of_month', 'is_weekend', 'week_of_year', 'quarter',
    'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
    'lag_1d', 'lag_7d', 'lag_14d', 'lag_28d', 'lag_365d',
    'rolling_mean_7d', 'rolling_mean_14d', 'rolling_mean_28d', 'rolling_std_7d',
    'yoy_ratio', 'trend_index',
]


def _train_model(df: pd.DataFrame) -> Tuple[Any, Dict[str, float]]:
    """Train LightGBM model on historical data. Returns (model, metrics).

    Runs in a thread pool to avoid blocking the event loop.
    """
    import lightgbm as lgb
    from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

    featured = _build_features(df)

    # Drop rows with NaN in features (due to lags)
    train_data = featured.dropna(subset=[c for c in FEATURE_COLUMNS if c != 'yoy_ratio'])
    # Fill yoy_ratio NaN (first year of data won't have it)
    train_data = train_data.copy()
    train_data['yoy_ratio'] = train_data['yoy_ratio'].fillna(1.0)

    if len(train_data) < 60:
        raise ValueError(f"Not enough training data: {len(train_data)} rows (need >= 60)")

    X = train_data[FEATURE_COLUMNS].values
    y = train_data['revenue'].values

    # Time-series split: last 60 days for validation
    split_idx = len(X) - 60
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    train_set = lgb.Dataset(X_train, label=y_train, feature_name=FEATURE_COLUMNS)
    val_set = lgb.Dataset(X_val, label=y_val, reference=train_set)

    params = {
        'objective': 'regression',
        'metric': 'mae',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'n_jobs': 1,
        'verbose': -1,
        'seed': 42,
    }

    model = lgb.train(
        params,
        train_set,
        num_boost_round=500,
        valid_sets=[val_set],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    # Calculate metrics on validation set
    val_pred = model.predict(X_val)
    mae = float(mean_absolute_error(y_val, val_pred))
    mape = float(mean_absolute_percentage_error(y_val, val_pred)) * 100

    metrics = {'mae': round(mae, 2), 'mape': round(mape, 2)}
    logger.info(f"Model trained: MAE={mae:.0f}, MAPE={mape:.1f}%, "
                f"rows={len(train_data)}, best_iter={model.best_iteration}")

    return model, metrics


def _predict_future(
    model: Any,
    historical_df: pd.DataFrame,
    future_dates: List[date],
) -> List[Dict[str, Any]]:
    """Generate predictions for future dates using trained model.

    Uses actual historical data for lag features, then iteratively fills in
    predictions as we go forward.
    """
    if not future_dates:
        return []

    # Build a combined dataframe: historical + future placeholder rows
    hist = historical_df.copy()
    hist['date'] = pd.to_datetime(hist['date'])

    future_rows = pd.DataFrame({
        'date': pd.to_datetime(future_dates),
        'revenue': np.nan,
    })

    combined = pd.concat([hist, future_rows], ignore_index=True)
    combined = combined.sort_values('date').reset_index(drop=True)
    combined = combined.drop_duplicates(subset='date', keep='first')
    combined = combined.sort_values('date').reset_index(drop=True)

    # Find the index where future starts
    future_start_idx = combined[combined['revenue'].isna()].index[0]

    # Iteratively predict each future day
    predictions = []
    for idx in range(future_start_idx, len(combined)):
        row_date = combined.loc[idx, 'date']

        # Rebuild features for the current state
        featured = _build_features(combined.iloc[:idx + 1])
        featured['yoy_ratio'] = featured['yoy_ratio'].fillna(1.0)

        # Fill remaining NaN features with 0
        row_features = featured.iloc[-1][FEATURE_COLUMNS].fillna(0).values.reshape(1, -1)

        pred = float(model.predict(row_features)[0])
        pred = max(pred, 0)  # Revenue can't be negative

        # Store prediction back so next iteration can use it as a lag
        combined.loc[idx, 'revenue'] = pred

        predictions.append({
            'date': row_date.strftime('%Y-%m-%d'),
            'predicted_revenue': round(pred, 2),
        })

    return predictions


class PredictionService:
    """Revenue prediction service using LightGBM."""

    def __init__(self):
        self._model = None
        self._metrics: Dict[str, float] = {}
        self._last_trained: Optional[str] = None
        self._training = False

    @property
    def is_ready(self) -> bool:
        return self._model is not None

    @property
    def metrics(self) -> Dict[str, float]:
        return self._metrics

    async def train(self, sales_type: str = "retail") -> Dict[str, Any]:
        """Train model on historical daily revenue data from DuckDB."""
        if self._training:
            return {"status": "already_training"}

        self._training = True
        try:
            from core.duckdb_store import get_store
            store = await get_store()

            # Query daily revenue for the last ~25 months
            df = await self._query_daily_revenue(store, sales_type, days_back=780)

            if df.empty or len(df) < 90:
                logger.warning(f"Insufficient data for training: {len(df)} rows")
                return {"status": "insufficient_data", "rows": len(df)}

            logger.info(f"Training revenue model on {len(df)} days of data")

            # Train in thread pool (CPU-bound)
            loop = asyncio.get_event_loop()
            model, metrics = await loop.run_in_executor(
                _executor, _train_model, df
            )

            self._model = model
            self._metrics = metrics
            self._last_trained = date.today().isoformat()

            # Save model to disk
            await loop.run_in_executor(_executor, self._save_model)

            # Generate and store predictions for rest of month
            predictions = await self.predict_month(df, sales_type)

            return {
                "status": "success",
                "metrics": metrics,
                "training_rows": len(df),
                "predictions_generated": len(predictions),
            }

        except Exception as e:
            logger.error(f"Model training failed: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}
        finally:
            self._training = False

    async def predict_month(
        self,
        historical_df: Optional[pd.DataFrame] = None,
        sales_type: str = "retail",
    ) -> List[Dict[str, Any]]:
        """Predict revenue for remaining days of the current month."""
        if not self.is_ready:
            return []

        from core.duckdb_store import get_store
        store = await get_store()

        if historical_df is None:
            historical_df = await self._query_daily_revenue(store, sales_type, days_back=780)

        # Calculate remaining days in current month
        today = date.today()
        if today.month == 12:
            month_end = date(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(today.year, today.month + 1, 1) - timedelta(days=1)

        future_dates = []
        d = today + timedelta(days=1)
        while d <= month_end:
            future_dates.append(d)
            d += timedelta(days=1)

        if not future_dates:
            logger.info("No remaining days to predict (end of month)")
            return []

        # Run prediction in thread pool
        loop = asyncio.get_event_loop()
        predictions = await loop.run_in_executor(
            _executor, _predict_future, self._model, historical_df, future_dates
        )

        # Store predictions in DuckDB
        try:
            await store.store_predictions(predictions, sales_type, self._metrics)
        except Exception as e:
            logger.error(f"Failed to store predictions: {e}")

        return predictions

    async def get_forecast(self, sales_type: str = "retail") -> Optional[Dict[str, Any]]:
        """Get stored forecast data for the current month."""
        from core.duckdb_store import get_store
        store = await get_store()

        today = date.today()
        month_start = date(today.year, today.month, 1)
        if today.month == 12:
            month_end = date(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(today.year, today.month + 1, 1) - timedelta(days=1)

        # Get actual revenue to date
        actual_to_date = await self._get_actual_month_revenue(store, sales_type, month_start, today)

        # Get stored predictions
        predictions = await store.get_predictions(
            start_date=today + timedelta(days=1),
            end_date=month_end,
            sales_type=sales_type,
        )

        if not predictions:
            return None

        predicted_remaining = sum(p['predicted_revenue'] for p in predictions)
        predicted_total = actual_to_date + predicted_remaining

        # Recover metrics from stored predictions if in-memory state was lost (server restart)
        metrics = self._metrics
        if not metrics and predictions:
            metrics = {
                'mae': predictions[0].get('model_mae', 0),
                'mape': predictions[0].get('model_mape', 0),
            }

        return {
            "actual_to_date": round(actual_to_date, 2),
            "predicted_remaining": round(predicted_remaining, 2),
            "predicted_total": round(predicted_total, 2),
            "daily_predictions": predictions,
            "model_metrics": metrics,
            "last_trained": self._last_trained,
            "month_start": month_start.isoformat(),
            "month_end": month_end.isoformat(),
        }

    async def _query_daily_revenue(
        self, store: Any, sales_type: str, days_back: int
    ) -> pd.DataFrame:
        """Query daily revenue from DuckDB orders table."""
        from core.models import OrderStatus

        return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
        sales_filter = store._build_sales_type_filter(sales_type)

        start_date = date.today() - timedelta(days=days_back)

        async with store.connection() as conn:
            result = conn.execute(f"""
                SELECT
                    DATE(timezone('Europe/Kyiv', o.ordered_at)) as date,
                    COALESCE(SUM(o.grand_total), 0) as revenue
                FROM orders o
                WHERE o.ordered_at >= ?
                  AND o.source_id IN (1, 2, 4)
                  AND o.status_id NOT IN {return_statuses}
                  AND {sales_filter}
                GROUP BY DATE(timezone('Europe/Kyiv', o.ordered_at))
                ORDER BY date
            """, [start_date.isoformat()]).fetchdf()

        return result

    async def _get_actual_month_revenue(
        self, store: Any, sales_type: str, month_start: date, up_to: date
    ) -> float:
        """Get actual revenue from month start to given date."""
        from core.models import OrderStatus

        return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())
        sales_filter = store._build_sales_type_filter(sales_type)

        async with store.connection() as conn:
            result = conn.execute(f"""
                SELECT COALESCE(SUM(o.grand_total), 0) as revenue
                FROM orders o
                WHERE DATE(timezone('Europe/Kyiv', o.ordered_at)) >= ?
                  AND DATE(timezone('Europe/Kyiv', o.ordered_at)) <= ?
                  AND o.source_id IN (1, 2, 4)
                  AND o.status_id NOT IN {return_statuses}
                  AND {sales_filter}
            """, [month_start.isoformat(), up_to.isoformat()]).fetchone()

        return float(result[0]) if result else 0.0

    def _save_model(self) -> None:
        """Save model to disk."""
        import joblib
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._model, MODEL_PATH)
        logger.info(f"Model saved to {MODEL_PATH}")

    def _load_model(self) -> bool:
        """Load model from disk if available."""
        import joblib
        if MODEL_PATH.exists():
            try:
                self._model = joblib.load(MODEL_PATH)
                logger.info(f"Model loaded from {MODEL_PATH}")
                return True
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════

_service: Optional[PredictionService] = None


def get_prediction_service() -> PredictionService:
    """Get singleton prediction service instance."""
    global _service
    if _service is None:
        _service = PredictionService()
        # Try to load existing model from disk
        _service._load_model()
    return _service
