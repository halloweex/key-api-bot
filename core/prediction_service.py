"""
LightGBM-based revenue prediction service.

Trains on historical daily revenue data and predicts remaining days of the current month.
Runs nightly via scheduler, stores predictions in DuckDB.
"""
import asyncio
import json
import logging
from calendar import monthrange
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
TUNED_PARAMS_PATH = MODEL_DIR / "lgbm_best_params.json"
DOW_CORRECTIONS_PATH = MODEL_DIR / "dow_corrections.json"

# Thread pool for CPU-bound training
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ml-train")

# How far ahead to predict (days)
FORECAST_HORIZON_DAYS = 60


def _get_holiday_features(dates: pd.Series) -> pd.DataFrame:
    """Compute holiday/event features for a series of dates.

    Returns DataFrame with columns:
    - is_holiday: 1 if public holiday (typically low-sales day)
    - is_high_sales_event: 1 if high-sales event (promotions, gifting)
    - days_to_nearest_event: distance to nearest event (capped at 14)
    - bf_countdown: 30→0 countdown during 30 days before Black Friday, 31 outside
    """

    # Orthodox Easter lookup (moves each year)
    _ORTHODOX_EASTER = {
        2023: date(2023, 4, 16), 2024: date(2024, 5, 5),
        2025: date(2025, 4, 20), 2026: date(2026, 4, 12),
        2027: date(2027, 5, 2),  2028: date(2028, 4, 16),
        2029: date(2029, 4, 8),  2030: date(2030, 4, 28),
    }

    def _black_friday(year: int) -> date:
        """Last Friday of November."""
        nov_last = date(year, 11, monthrange(year, 11)[1])
        offset = (nov_last.weekday() - 4) % 7  # Friday = 4
        return nov_last - timedelta(days=offset)

    def _build_events_for_year(year: int):
        """Return (holidays_set, high_sales_set, all_events_list) for a year."""
        holidays = {
            date(year, 1, 1),    # New Year
            date(year, 1, 7),    # Orthodox Christmas
            date(year, 5, 1),    # Labour Day
            date(year, 5, 9),    # Victory Day
            date(year, 6, 28),   # Constitution Day
            date(year, 8, 24),   # Independence Day
            date(year, 10, 14),  # Defender's Day
            date(year, 12, 25),  # Catholic Christmas
        }
        if year in _ORTHODOX_EASTER:
            holidays.add(_ORTHODOX_EASTER[year])

        bf = _black_friday(year)
        cm = bf + timedelta(days=3)  # Cyber Monday
        high_sales = {
            date(year, 1, 22),   # Owner's Birthday
            date(year, 2, 14),   # Valentine's Day
            date(year, 3, 8),    # Women's Day
            date(year, 3, 9),    # KoreanStory Birthday (discount day)
            date(year, 11, 11),  # Singles' Day (11.11)
            date(year, 12, 14),  # Second Owner's Birthday (discount day)
            date(year, 12, 26),  # LaLa Recipe brand day
            date(year, 12, 27),  # LaLa Recipe brand day
            bf,                  # Black Friday
            cm,                  # Cyber Monday
        }
        # Pre-New Year rush: Dec 20–30
        for day in range(20, 31):
            high_sales.add(date(year, 12, day))

        return holidays, high_sales, bf

    # Collect events across all years in the data
    dt = pd.to_datetime(dates)
    years = sorted(dt.dt.year.unique())

    all_holidays: set[date] = set()
    all_high_sales: set[date] = set()
    all_events: list[date] = []
    bf_dates: dict[int, date] = {}

    for y in years:
        h, hs, bf = _build_events_for_year(y)
        all_holidays |= h
        all_high_sales |= hs
        all_events.extend(h | hs)
        bf_dates[y] = bf

    all_events_sorted = np.array(sorted(set(all_events)), dtype='datetime64[D]')

    # Vectorised computation
    dates_np = dt.values.astype('datetime64[D]')
    is_holiday = np.array([d.date() in all_holidays if hasattr(d, 'date') else False
                           for d in pd.to_datetime(dates_np)], dtype=int)
    is_high = np.array([d.date() in all_high_sales if hasattr(d, 'date') else False
                        for d in pd.to_datetime(dates_np)], dtype=int)

    # days_to_nearest_event: min absolute distance, capped at 14
    days_dist = np.abs(dates_np[:, None].astype('int64') - all_events_sorted[None, :].astype('int64'))
    days_to_nearest = np.minimum(days_dist.min(axis=1), 14)

    # bf_countdown: 30→0 during 30 days before Black Friday, 31 outside
    bf_countdown = np.full(len(dates_np), 31, dtype=int)
    for y, bf in bf_dates.items():
        bf_np = np.datetime64(bf, 'D')
        diff = (bf_np.astype('int64') - dates_np.astype('int64'))  # days until BF
        mask = (diff >= 0) & (diff <= 30)
        bf_countdown[mask] = diff[mask]

    return pd.DataFrame({
        'is_holiday': is_holiday,
        'is_high_sales_event': is_high,
        'days_to_nearest_event': days_to_nearest,
        'bf_countdown': bf_countdown,
    }, index=dates.index)


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer features from a DataFrame with 'date', 'revenue', and optional extra columns.

    Creates 27 features:
    - Calendar: day_of_week, month, day_of_month
    - Cyclical: month_sin, month_cos, dow_sin, dow_cos
    - Lags: 1d, 7d, 14d, 28d, 365d
    - Rolling: mean_7d, mean_14d, mean_28d, std_7d
    - Trend: yoy_ratio, linear trend index
    - Events: days_to_nearest_event
    - Source shares: lag_7d_instagram_share, lag_7d_telegram_share, lag_7d_shopify_share
    - Orders + AOV: lag_7d_orders, rolling_mean_7d_orders, lag_7d_aov, rolling_mean_7d_aov
    - Customer mix: rolling_mean_7d_new_cust_ratio
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Calendar features
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['day_of_month'] = df['date'].dt.day

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

    # Holiday / event features
    holiday_df = _get_holiday_features(df['date'])
    for col in holiday_df.columns:
        df[col] = holiday_df[col].values

    # Source share features (lagged — avoids leakage)
    for src in ['instagram', 'telegram', 'shopify']:
        col_name = f'{src}_revenue'
        if col_name in df.columns:
            share = df[col_name] / df['revenue'].replace(0, np.nan)
            df[f'lag_7d_{src}_share'] = share.fillna(0).shift(7)
        else:
            df[f'lag_7d_{src}_share'] = 0.0

    # Order count + AOV features
    if 'orders_count' in df.columns:
        df['lag_7d_orders'] = df['orders_count'].shift(7)
        df['rolling_mean_7d_orders'] = df['orders_count'].shift(1).rolling(7, min_periods=3).mean()
        aov = df['revenue'] / df['orders_count'].replace(0, np.nan)
        aov = aov.fillna(0)
        df['lag_7d_aov'] = aov.shift(7)
        df['rolling_mean_7d_aov'] = aov.shift(1).rolling(7, min_periods=3).mean()
    else:
        df['lag_7d_orders'] = np.nan
        df['rolling_mean_7d_orders'] = np.nan
        df['lag_7d_aov'] = np.nan
        df['rolling_mean_7d_aov'] = np.nan

    # Customer mix
    if 'new_customers' in df.columns and 'unique_customers' in df.columns:
        new_ratio = df['new_customers'] / df['unique_customers'].replace(0, np.nan)
        df['rolling_mean_7d_new_cust_ratio'] = new_ratio.fillna(0).shift(1).rolling(7, min_periods=3).mean()
    else:
        df['rolling_mean_7d_new_cust_ratio'] = np.nan

    return df


FEATURE_COLUMNS = [
    # Calendar (3)
    'day_of_week', 'month', 'day_of_month',
    # Cyclical (4)
    'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
    # Lags (5)
    'lag_1d', 'lag_7d', 'lag_14d', 'lag_28d', 'lag_365d',
    # Rolling (4)
    'rolling_mean_7d', 'rolling_mean_14d', 'rolling_mean_28d', 'rolling_std_7d',
    # Trend (2)
    'yoy_ratio', 'trend_index',
    # Events (1)
    'days_to_nearest_event',
    # AOV (1) — validated via ablation study
    'rolling_mean_7d_aov',
]

# Default LightGBM hyperparameters
DEFAULT_LGB_PARAMS = {
    'num_leaves': 31,
    'learning_rate': 0.05,
    'min_child_samples': 20,
    'reg_alpha': 0,
    'subsample': 1.0,
}


def _load_tuned_params() -> Dict[str, Any]:
    """Load tuned hyperparameters from disk, or return defaults."""
    if TUNED_PARAMS_PATH.exists():
        try:
            with open(TUNED_PARAMS_PATH) as f:
                params = json.load(f)
            logger.info(f"Loaded tuned params from {TUNED_PARAMS_PATH}: {params}")
            return params
        except Exception as e:
            logger.warning(f"Failed to load tuned params: {e}")
    return DEFAULT_LGB_PARAMS.copy()


def _build_lgb_params(tuned: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build full LightGBM params dict from tuned hyperparameters."""
    hp = tuned or _load_tuned_params()
    return {
        'objective': 'regression',
        'metric': 'mae',
        'num_leaves': hp.get('num_leaves', 31),
        'learning_rate': hp.get('learning_rate', 0.05),
        'min_child_samples': hp.get('min_child_samples', 20),
        'reg_alpha': hp.get('reg_alpha', 0),
        'subsample': hp.get('subsample', 1.0),
        'n_jobs': 1,
        'verbose': -1,
        'seed': 42,
    }


def _train_model(df: pd.DataFrame) -> Tuple[Any, Dict[str, Any], Dict[int, float]]:
    """Train LightGBM model on historical data. Returns (model, metrics, dow_corrections).

    Computes day-of-week residual corrections on the validation set to
    adjust for systematic DOW bias.
    Runs in a thread pool to avoid blocking the event loop.
    """
    import lightgbm as lgb
    from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

    featured = _build_features(df)

    # Drop rows with NaN in features (due to lags)
    nan_check_cols = [c for c in FEATURE_COLUMNS if c not in ('yoy_ratio',)]
    train_data = featured.dropna(subset=nan_check_cols)
    # Fill remaining NaN in features
    train_data = train_data.copy()
    train_data['yoy_ratio'] = train_data['yoy_ratio'].fillna(1.0)
    for col in FEATURE_COLUMNS:
        if col != 'yoy_ratio':
            train_data[col] = train_data[col].fillna(0)

    if len(train_data) < 60:
        raise ValueError(f"Not enough training data: {len(train_data)} rows (need >= 60)")

    X = train_data[FEATURE_COLUMNS].values
    y = train_data['revenue'].values
    dates = train_data['date'].values

    # Time-series split: last 60 days for validation
    split_idx = len(X) - 60
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]
    dates_val = dates[split_idx:]

    train_set = lgb.Dataset(X_train, label=y_train, feature_name=FEATURE_COLUMNS)
    val_set = lgb.Dataset(X_val, label=y_val, reference=train_set)

    params = _build_lgb_params()

    model = lgb.train(
        params,
        train_set,
        num_boost_round=500,
        valid_sets=[val_set],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    val_pred = model.predict(X_val)
    val_pred = np.maximum(val_pred, 0)

    # DOW residual correction: mean(actual) / mean(predicted) per day-of-week
    dow_corrections: Dict[int, float] = {}
    val_dows = np.array([pd.Timestamp(d).dayofweek for d in dates_val])
    for dow in range(7):
        mask = val_dows == dow
        if mask.sum() > 0:
            mean_actual = float(np.mean(y_val[mask]))
            mean_pred = float(np.mean(val_pred[mask]))
            if mean_pred > 0:
                correction = mean_actual / mean_pred
                # Clamp to avoid overfitting
                correction = max(0.85, min(1.15, correction))
            else:
                correction = 1.0
            dow_corrections[dow] = round(correction, 4)
        else:
            dow_corrections[dow] = 1.0

    logger.info(f"DOW corrections: {dow_corrections}")

    # Apply DOW corrections to validation predictions for metrics
    val_pred_corrected = val_pred.copy()
    for i, dow in enumerate(val_dows):
        val_pred_corrected[i] *= dow_corrections[int(dow)]

    # Calculate metrics on original scale (with DOW correction)
    mae = float(mean_absolute_error(y_val, val_pred_corrected))
    mape = float(mean_absolute_percentage_error(y_val, val_pred_corrected)) * 100
    # WAPE: sum(|actual - predicted|) / sum(actual) — robust to low-revenue days
    wape = float(np.sum(np.abs(y_val - val_pred_corrected)) / np.sum(y_val)) * 100 if np.sum(y_val) > 0 else 0.0

    # Per-day validation breakdown
    days_of_week = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    daily_breakdown = []
    for i in range(len(y_val)):
        actual = float(y_val[i])
        pred = float(val_pred_corrected[i])
        error = abs(actual - pred)
        ape = (error / actual * 100) if actual > 0 else 0
        dt = pd.Timestamp(dates_val[i])
        daily_breakdown.append({
            'date': dt.strftime('%Y-%m-%d'),
            'dow': days_of_week[dt.dayofweek],
            'actual': round(actual, 0),
            'predicted': round(pred, 0),
            'error': round(error, 0),
            'ape': round(ape, 1),
        })

    metrics = {
        'mae': round(mae, 2),
        'mape': round(mape, 2),
        'wape': round(wape, 2),
        'validation_days': daily_breakdown,
    }
    logger.info(f"Model trained: MAE={mae:.0f}, MAPE={mape:.1f}%, WAPE={wape:.1f}%, "
                f"rows={len(train_data)}, best_iter={model.best_iteration}")

    return model, metrics, dow_corrections


def _predict_future(
    model: Any,
    historical_df: pd.DataFrame,
    future_dates: List[date],
    dow_corrections: Optional[Dict[int, float]] = None,
) -> List[Dict[str, Any]]:
    """Generate predictions for future dates using trained model.

    Uses actual historical data for lag features, then iteratively fills in
    predictions as we go forward. Predictions are made in log space and
    converted back with expm1, then adjusted by DOW corrections.
    """
    if not future_dates:
        return []

    # Build a combined dataframe: historical + future placeholder rows
    hist = historical_df.copy()
    hist['date'] = pd.to_datetime(hist['date'])

    # Extra columns needed by _build_features
    extra_cols = ['instagram_revenue', 'telegram_revenue', 'shopify_revenue',
                  'orders_count', 'unique_customers', 'new_customers']
    future_row_data: Dict[str, Any] = {
        'date': pd.to_datetime(future_dates),
        'revenue': np.nan,
    }
    for col in extra_cols:
        future_row_data[col] = np.nan
    future_rows = pd.DataFrame(future_row_data)

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

        # Apply DOW correction
        if dow_corrections:
            dow = row_date.dayofweek
            pred *= dow_corrections.get(dow, 1.0)

        # Store prediction back so next iteration can use it as a lag
        combined.loc[idx, 'revenue'] = pred

        predictions.append({
            'date': row_date.strftime('%Y-%m-%d'),
            'predicted_revenue': round(pred, 2),
        })

    return predictions


def _compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Compute MAE, WAPE, R², and directional accuracy."""
    mae = float(np.mean(np.abs(y_true - y_pred)))
    total_actual = float(np.sum(y_true))
    wape = float(np.sum(np.abs(y_true - y_pred)) / total_actual * 100) if total_actual > 0 else 0.0
    # R²
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    # Directional accuracy: did we predict correct direction of change from previous day?
    if len(y_true) >= 2:
        actual_dir = np.diff(y_true) >= 0
        pred_dir = np.diff(y_pred) >= 0
        directional_accuracy = float(np.mean(actual_dir == pred_dir) * 100)
    else:
        directional_accuracy = 0.0
    return {
        "mae": round(mae, 2),
        "wape": round(wape, 2),
        "r_squared": round(r_squared, 4),
        "directional_accuracy": round(directional_accuracy, 2),
    }


def _compute_baselines(
    test_df: pd.DataFrame,
    full_df: pd.DataFrame,
) -> Dict[str, Dict[str, Any]]:
    """Compute 4 baseline predictions and their metrics.

    test_df must have 'date' and 'revenue' columns.
    full_df is all historical data (sorted by date) up to and including test period.
    Returns dict of baseline_name -> {predictions: np.ndarray, metrics: dict}.
    """
    test_dates = test_df['date'].values
    test_actual = test_df['revenue'].values
    full = full_df.set_index('date')

    baselines: Dict[str, Dict[str, Any]] = {}

    for name in ['naive_7d', 'moving_avg_28d', 'same_day_last_year', 'weekday_avg_12w']:
        preds = np.full(len(test_dates), np.nan)

        for i, dt in enumerate(test_dates):
            dt_ts = pd.Timestamp(dt)

            if name == 'naive_7d':
                lookup = dt_ts - pd.Timedelta(days=7)
                if lookup in full.index:
                    preds[i] = full.loc[lookup, 'revenue']

            elif name == 'moving_avg_28d':
                prior = full[full.index < dt_ts].tail(28)
                if len(prior) >= 7:
                    preds[i] = prior['revenue'].mean()

            elif name == 'same_day_last_year':
                lookup = dt_ts - pd.Timedelta(days=365)
                if lookup in full.index:
                    preds[i] = full.loc[lookup, 'revenue']

            elif name == 'weekday_avg_12w':
                target_dow = dt_ts.dayofweek
                cutoff = dt_ts - pd.Timedelta(weeks=12)
                candidates = full[
                    (full.index >= cutoff)
                    & (full.index < dt_ts)
                    & (full.index.dayofweek == target_dow)
                ]
                if len(candidates) >= 3:
                    preds[i] = candidates['revenue'].mean()

        # Mask valid entries for metric calculation
        valid = ~np.isnan(preds)
        if valid.sum() > 0:
            y_t = test_actual[valid]
            y_p = preds[valid]
            m = {
                "mae": round(float(np.mean(np.abs(y_t - y_p))), 2),
                "wape": round(float(np.sum(np.abs(y_t - y_p)) / np.sum(y_t) * 100), 2) if np.sum(y_t) > 0 else 0.0,
                "coverage": int(valid.sum()),
            }
        else:
            m = {"mae": None, "wape": None, "coverage": 0}

        baselines[name] = {"predictions": preds, "metrics": m}

    return baselines


def _run_evaluation(df: pd.DataFrame) -> Dict[str, Any]:
    """Run walk-forward cross-validation with baselines and feature importance.

    df must have 'date' and 'revenue' columns, sorted by date.
    Excludes today's (potentially incomplete) row.
    Uses last 6 complete calendar months as test folds.
    """
    import lightgbm as lgb

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Exclude today's incomplete data
    today = pd.Timestamp(date.today())
    df = df[df['date'] < today].reset_index(drop=True)

    if len(df) < 120:
        raise ValueError(f"Not enough data for evaluation: {len(df)} days (need >= 120)")

    # Determine last 6 complete calendar months as test folds
    last_date = df['date'].max()
    folds = []
    cursor = last_date.replace(day=1)  # first day of last data month
    # If last_date is not the last day of its month, this month is incomplete — skip it
    last_day_of_month = cursor + pd.offsets.MonthEnd(0)
    if last_date < last_day_of_month:
        cursor = cursor - pd.DateOffset(months=1)

    for _ in range(6):
        fold_start = cursor
        fold_end = cursor + pd.offsets.MonthEnd(0)
        fold_mask = (df['date'] >= fold_start) & (df['date'] <= fold_end)
        if fold_mask.sum() >= 20:  # at least 20 days in the month
            folds.append((fold_start, fold_end))
        cursor = cursor - pd.DateOffset(months=1)

    folds.reverse()  # chronological order

    if len(folds) < 3:
        raise ValueError(f"Only {len(folds)} valid folds found (need >= 3)")

    # LightGBM params (same as production — uses tuned if available)
    lgb_params = _build_lgb_params()

    fold_results = []
    all_lgbm_preds = []
    all_actuals = []
    all_residuals = []  # (date, actual, pred, error)
    feature_importances = np.zeros(len(FEATURE_COLUMNS))
    n_folds_with_importance = 0

    for fold_idx, (fold_start, fold_end) in enumerate(folds):
        # Split: train = everything before fold_start, test = fold month
        train_mask = df['date'] < fold_start
        test_mask = (df['date'] >= fold_start) & (df['date'] <= fold_end)

        train_df = df[train_mask].copy()
        test_df = df[test_mask].copy()

        if len(train_df) < 60 or len(test_df) < 20:
            continue

        # Build features for train set
        featured_train = _build_features(train_df)
        featured_train = featured_train.dropna(subset=[c for c in FEATURE_COLUMNS if c != 'yoy_ratio'])
        featured_train = featured_train.copy()
        featured_train['yoy_ratio'] = featured_train['yoy_ratio'].fillna(1.0)
        for col in FEATURE_COLUMNS:
            if col != 'yoy_ratio':
                featured_train[col] = featured_train[col].fillna(0)

        if len(featured_train) < 60:
            continue

        X_full_train = featured_train[FEATURE_COLUMNS].values
        y_full_train = featured_train['revenue'].values

        # Internal early-stopping split: last 30 days of training data
        es_split = max(len(X_full_train) - 30, int(len(X_full_train) * 0.8))
        X_tr, X_es = X_full_train[:es_split], X_full_train[es_split:]
        y_tr, y_es = y_full_train[:es_split], y_full_train[es_split:]

        train_set = lgb.Dataset(X_tr, label=y_tr, feature_name=FEATURE_COLUMNS)
        es_set = lgb.Dataset(X_es, label=y_es, reference=train_set)

        model = lgb.train(
            lgb_params,
            train_set,
            num_boost_round=500,
            valid_sets=[es_set],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

        # Accumulate feature importance (gain-based)
        importance = model.feature_importance(importance_type='gain')
        feature_importances += importance
        n_folds_with_importance += 1

        # Predict test set: build features on train + test combined
        combined = pd.concat([train_df, test_df], ignore_index=True).sort_values('date').reset_index(drop=True)
        featured_combined = _build_features(combined)
        featured_combined = featured_combined.copy()
        featured_combined['yoy_ratio'] = featured_combined['yoy_ratio'].fillna(1.0)

        test_featured = featured_combined[featured_combined['date'] >= fold_start].copy()
        # Fill remaining NaN in features with 0
        X_test = test_featured[FEATURE_COLUMNS].fillna(0).values
        y_test = test_featured['revenue'].values
        test_dates = test_featured['date'].values

        lgbm_preds = model.predict(X_test)
        lgbm_preds = np.maximum(lgbm_preds, 0)

        # LightGBM metrics for this fold
        lgbm_metrics = _compute_metrics(y_test, lgbm_preds)

        # Baselines for this fold
        baseline_results = _compute_baselines(test_featured[['date', 'revenue']], df[df['date'] <= fold_end])
        baseline_fold_metrics = {
            name: data['metrics'] for name, data in baseline_results.items()
        }

        fold_results.append({
            "fold": fold_idx + 1,
            "period": f"{fold_start.strftime('%Y-%m-%d')} to {fold_end.strftime('%Y-%m-%d')}",
            "test_days": len(y_test),
            "lgbm": lgbm_metrics,
            "baselines": baseline_fold_metrics,
        })

        # Collect for aggregate metrics
        all_lgbm_preds.extend(lgbm_preds.tolist())
        all_actuals.extend(y_test.tolist())

        for i in range(len(y_test)):
            dt = pd.Timestamp(test_dates[i])
            all_residuals.append({
                'date': dt,
                'actual': float(y_test[i]),
                'predicted': float(lgbm_preds[i]),
                'error': float(lgbm_preds[i] - y_test[i]),
            })

    if not fold_results:
        raise ValueError("No valid folds could be evaluated")

    # ── Aggregate metrics ──
    all_actuals_arr = np.array(all_actuals)
    all_preds_arr = np.array(all_lgbm_preds)
    agg_lgbm = _compute_metrics(all_actuals_arr, all_preds_arr)

    # Aggregate baselines across all folds
    agg_baselines: Dict[str, Dict[str, float]] = {}
    for name in ['naive_7d', 'moving_avg_28d', 'same_day_last_year', 'weekday_avg_12w']:
        fold_maes = [f['baselines'][name]['mae'] for f in fold_results if f['baselines'][name]['mae'] is not None]
        fold_wapes = [f['baselines'][name]['wape'] for f in fold_results if f['baselines'][name]['wape'] is not None]
        if fold_maes:
            agg_baselines[name] = {
                "mae": round(float(np.mean(fold_maes)), 2),
                "wape": round(float(np.mean(fold_wapes)), 2),
            }
        else:
            agg_baselines[name] = {"mae": None, "wape": None}

    # Find best baseline
    valid_baselines = {k: v for k, v in agg_baselines.items() if v['wape'] is not None}
    if valid_baselines:
        best_baseline_name = min(valid_baselines, key=lambda k: valid_baselines[k]['wape'])
        best_baseline_wape = valid_baselines[best_baseline_name]['wape']
    else:
        best_baseline_name = "none"
        best_baseline_wape = 0.0

    improvement = round(best_baseline_wape - agg_lgbm['wape'], 2)

    # Verdict
    if improvement > 0:
        verdict = f"Model outperforms best baseline ({best_baseline_name}) by {improvement}% WAPE"
    elif improvement == 0:
        verdict = f"Model performs on par with best baseline ({best_baseline_name})"
    else:
        verdict = f"Model underperforms best baseline ({best_baseline_name}) by {abs(improvement)}% WAPE"

    # ── Feature importance ──
    if n_folds_with_importance > 0:
        avg_importance = feature_importances / n_folds_with_importance
        importance_list = sorted(
            [
                {"feature": FEATURE_COLUMNS[i], "gain": round(float(avg_importance[i]), 2), "rank": 0}
                for i in range(len(FEATURE_COLUMNS))
            ],
            key=lambda x: x['gain'],
            reverse=True,
        )
        for rank, item in enumerate(importance_list, 1):
            item['rank'] = rank
    else:
        importance_list = []

    # ── Residual analysis ──
    dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    residuals_by_dow: Dict[str, Dict[str, Any]] = {}
    for dow_idx, dow_name in enumerate(dow_names):
        entries = [r for r in all_residuals if r['date'].dayofweek == dow_idx]
        if entries:
            errors = np.array([e['error'] for e in entries])
            actuals = np.array([e['actual'] for e in entries])
            abs_errors = np.abs(errors)
            total_actual = np.sum(actuals)
            residuals_by_dow[dow_name] = {
                "mean_error": round(float(np.mean(errors)), 2),
                "wape": round(float(np.sum(abs_errors) / total_actual * 100), 2) if total_actual > 0 else 0.0,
                "count": len(entries),
            }

    residuals_by_month: Dict[str, Dict[str, Any]] = {}
    for month_idx, month_name in enumerate(month_names, 1):
        entries = [r for r in all_residuals if r['date'].month == month_idx]
        if entries:
            errors = np.array([e['error'] for e in entries])
            actuals = np.array([e['actual'] for e in entries])
            abs_errors = np.abs(errors)
            total_actual = np.sum(actuals)
            residuals_by_month[month_name] = {
                "mean_error": round(float(np.mean(errors)), 2),
                "wape": round(float(np.sum(abs_errors) / total_actual * 100), 2) if total_actual > 0 else 0.0,
                "count": len(entries),
            }

    # ── Data info ──
    date_min = df['date'].min().strftime('%Y-%m-%d')
    date_max = df['date'].max().strftime('%Y-%m-%d')

    return {
        "summary": {
            "lgbm_wape": agg_lgbm['wape'],
            "lgbm_mae": agg_lgbm['mae'],
            "best_baseline_wape": best_baseline_wape,
            "best_baseline_name": best_baseline_name,
            "improvement_over_baseline": improvement,
            "r_squared": agg_lgbm['r_squared'],
            "directional_accuracy": agg_lgbm['directional_accuracy'],
            "total_test_days": len(all_actuals),
            "num_folds": len(fold_results),
            "verdict": verdict,
        },
        "cross_validation": {"folds": fold_results},
        "feature_importance": importance_list,
        "residuals_by_dow": residuals_by_dow,
        "residuals_by_month": residuals_by_month,
        "data_info": {
            "total_days": len(df),
            "date_range": f"{date_min} to {date_max}",
        },
    }


def _tune_hyperparameters(df: pd.DataFrame) -> Dict[str, Any]:
    """Grid search over walk-forward CV folds to find best LightGBM hyperparameters.

    Reuses the same fold logic as _run_evaluation. Returns dict with best params,
    WAPE comparison, and search metadata.
    """
    import lightgbm as lgb
    from itertools import product as iterproduct

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Exclude today's incomplete data
    today = pd.Timestamp(date.today())
    df = df[df['date'] < today].reset_index(drop=True)

    if len(df) < 120:
        raise ValueError(f"Not enough data for tuning: {len(df)} days (need >= 120)")

    # Build folds (same as _run_evaluation)
    last_date = df['date'].max()
    folds = []
    cursor = last_date.replace(day=1)
    last_day_of_month = cursor + pd.offsets.MonthEnd(0)
    if last_date < last_day_of_month:
        cursor = cursor - pd.DateOffset(months=1)

    for _ in range(6):
        fold_start = cursor
        fold_end = cursor + pd.offsets.MonthEnd(0)
        fold_mask = (df['date'] >= fold_start) & (df['date'] <= fold_end)
        if fold_mask.sum() >= 20:
            folds.append((fold_start, fold_end))
        cursor = cursor - pd.DateOffset(months=1)

    folds.reverse()

    if len(folds) < 3:
        raise ValueError(f"Only {len(folds)} valid folds found (need >= 3)")

    # Pre-build features for each fold to avoid redundant computation
    fold_data = []
    for fold_start, fold_end in folds:
        train_mask = df['date'] < fold_start
        test_mask = (df['date'] >= fold_start) & (df['date'] <= fold_end)
        train_df = df[train_mask].copy()
        test_df = df[test_mask].copy()

        if len(train_df) < 60 or len(test_df) < 20:
            continue

        featured_train = _build_features(train_df)
        featured_train = featured_train.dropna(subset=[c for c in FEATURE_COLUMNS if c != 'yoy_ratio'])
        featured_train = featured_train.copy()
        featured_train['yoy_ratio'] = featured_train['yoy_ratio'].fillna(1.0)

        if len(featured_train) < 60:
            continue

        X_full_train = featured_train[FEATURE_COLUMNS].fillna(0).values
        y_full_train = featured_train['revenue'].values

        # Early-stopping split
        es_split = max(len(X_full_train) - 30, int(len(X_full_train) * 0.8))
        X_tr, X_es = X_full_train[:es_split], X_full_train[es_split:]
        y_tr, y_es = y_full_train[:es_split], y_full_train[es_split:]

        # Test features
        combined = pd.concat([train_df, test_df], ignore_index=True).sort_values('date').reset_index(drop=True)
        featured_combined = _build_features(combined)
        featured_combined = featured_combined.copy()
        featured_combined['yoy_ratio'] = featured_combined['yoy_ratio'].fillna(1.0)
        test_featured = featured_combined[featured_combined['date'] >= fold_start].copy()
        X_test = test_featured[FEATURE_COLUMNS].fillna(0).values
        y_test = test_featured['revenue'].values

        fold_data.append((X_tr, y_tr, X_es, y_es, X_test, y_test))

    if not fold_data:
        raise ValueError("No valid folds could be prepared for tuning")

    # Parameter grid
    grid = {
        'num_leaves':        [15, 31, 63],
        'learning_rate':     [0.01, 0.05, 0.1],
        'min_child_samples': [5, 20],
        'reg_alpha':         [0, 0.1],
        'subsample':         [0.8, 1.0],
    }

    keys = list(grid.keys())
    combos = list(iterproduct(*[grid[k] for k in keys]))

    best_wape = float('inf')
    best_combo = None

    for combo_vals in combos:
        hp = dict(zip(keys, combo_vals))
        params = {
            'objective': 'regression',
            'metric': 'mae',
            'num_leaves': hp['num_leaves'],
            'learning_rate': hp['learning_rate'],
            'min_child_samples': hp['min_child_samples'],
            'reg_alpha': hp['reg_alpha'],
            'subsample': hp['subsample'],
            'n_jobs': 1,
            'verbose': -1,
            'seed': 42,
        }

        all_actuals = []
        all_preds = []

        for X_tr, y_tr, X_es, y_es, X_test, y_test in fold_data:
            train_set = lgb.Dataset(X_tr, label=y_tr, feature_name=FEATURE_COLUMNS)
            es_set = lgb.Dataset(X_es, label=y_es, reference=train_set)

            model = lgb.train(
                params,
                train_set,
                num_boost_round=500,
                valid_sets=[es_set],
                callbacks=[lgb.early_stopping(50, verbose=False)],
            )

            preds = np.maximum(model.predict(X_test), 0)
            all_actuals.extend(y_test.tolist())
            all_preds.extend(preds.tolist())

        actuals_arr = np.array(all_actuals)
        preds_arr = np.array(all_preds)
        total_actual = float(np.sum(actuals_arr))
        wape = float(np.sum(np.abs(actuals_arr - preds_arr)) / total_actual * 100) if total_actual > 0 else float('inf')

        if wape < best_wape:
            best_wape = wape
            best_combo = hp

    # Compute default WAPE for comparison
    default_params = {
        'objective': 'regression',
        'metric': 'mae',
        **{k: DEFAULT_LGB_PARAMS[k] for k in keys},
        'n_jobs': 1,
        'verbose': -1,
        'seed': 42,
    }

    all_actuals = []
    all_preds = []
    for X_tr, y_tr, X_es, y_es, X_test, y_test in fold_data:
        train_set = lgb.Dataset(X_tr, label=y_tr, feature_name=FEATURE_COLUMNS)
        es_set = lgb.Dataset(X_es, label=y_es, reference=train_set)
        model = lgb.train(
            default_params,
            train_set,
            num_boost_round=500,
            valid_sets=[es_set],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )
        preds = np.maximum(model.predict(X_test), 0)
        all_actuals.extend(y_test.tolist())
        all_preds.extend(preds.tolist())

    actuals_arr = np.array(all_actuals)
    preds_arr = np.array(all_preds)
    total_actual = float(np.sum(actuals_arr))
    default_wape = float(np.sum(np.abs(actuals_arr - preds_arr)) / total_actual * 100) if total_actual > 0 else 0.0

    # Save best params
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(TUNED_PARAMS_PATH, 'w') as f:
        json.dump(best_combo, f, indent=2)
    logger.info(f"Saved tuned params to {TUNED_PARAMS_PATH}: {best_combo} (WAPE={best_wape:.2f}%)")

    return {
        "status": "success",
        "best_params": best_combo,
        "best_wape": round(best_wape, 2),
        "default_wape": round(default_wape, 2),
        "improvement": round(default_wape - best_wape, 2),
        "combos_tested": len(combos),
        "folds": len(fold_data),
    }


class PredictionService:
    """Revenue prediction service using LightGBM."""

    def __init__(self):
        self._model = None
        self._metrics: Dict[str, float] = {}
        self._dow_corrections: Dict[int, float] = {}
        self._last_trained: Optional[str] = None
        self._training = False

    @property
    def is_ready(self) -> bool:
        return self._model is not None

    @property
    def metrics(self) -> Dict[str, float]:
        return self._metrics

    async def evaluate(self, sales_type: str = "retail") -> Dict[str, Any]:
        """Run walk-forward CV evaluation with baselines.

        Excludes today's incomplete data, uses last 6 complete months as folds.
        Returns detailed metrics, feature importance, and residual analysis.
        """
        from core.duckdb_store import get_store
        store = await get_store()

        df = await self._query_daily_revenue(store, sales_type, days_back=780)
        if df.empty or len(df) < 120:
            return {"status": "insufficient_data", "rows": len(df)}

        logger.info(f"Running evaluation on {len(df)} days of data (sales_type={sales_type})")

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_executor, _run_evaluation, df)

        result["data_info"]["sales_type"] = sales_type
        return result

    async def tune(self, sales_type: str = "retail") -> Dict[str, Any]:
        """Run hyperparameter grid search using walk-forward CV.

        Saves best params to data/lgbm_best_params.json for use by train/evaluate.
        """
        from core.duckdb_store import get_store
        store = await get_store()

        df = await self._query_daily_revenue(store, sales_type, days_back=780)
        if df.empty or len(df) < 120:
            return {"status": "insufficient_data", "rows": len(df)}

        logger.info(f"Running hyperparameter tuning on {len(df)} days of data (sales_type={sales_type})")

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_executor, _tune_hyperparameters, df)

        result["sales_type"] = sales_type
        return result

    async def predict_range(
        self,
        start_date: date,
        end_date: date,
        sales_type: str = "retail",
    ) -> Optional[Dict[str, Any]]:
        """Generate predictions for an arbitrary future date range on-the-fly.

        Returns forecast dict compatible with get_forecast() response,
        or None if model is not ready or no future dates in range.
        """
        if not self.is_ready:
            return None

        today = date.today()
        # Include today (partial day) for stacked actual + remaining
        pred_start = max(start_date, today)
        if pred_start > end_date:
            return None

        future_dates = []
        d = pred_start
        while d <= end_date:
            future_dates.append(d)
            d += timedelta(days=1)

        if not future_dates:
            return None

        from core.duckdb_store import get_store
        store = await get_store()
        historical_df = await self._query_daily_revenue(store, sales_type, days_back=780)

        dow_corrections = self._dow_corrections
        loop = asyncio.get_event_loop()
        predictions = await loop.run_in_executor(
            _executor, _predict_future, self._model, historical_df, future_dates, dow_corrections
        )

        predicted_total = sum(p['predicted_revenue'] for p in predictions)

        return {
            "predicted_remaining": round(predicted_total, 2),
            "predicted_total": round(predicted_total, 2),
            "daily_predictions": predictions,
            "model_metrics": self._metrics,
            "last_trained": self._last_trained,
        }

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
            model, metrics, dow_corrections = await loop.run_in_executor(
                _executor, _train_model, df
            )

            self._model = model
            self._metrics = metrics
            self._dow_corrections = dow_corrections
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
        """Predict revenue for the next 60 days."""
        if not self.is_ready:
            return []

        from core.duckdb_store import get_store
        store = await get_store()

        if historical_df is None:
            historical_df = await self._query_daily_revenue(store, sales_type, days_back=780)

        today = date.today()
        forecast_end = today + timedelta(days=FORECAST_HORIZON_DAYS)

        # Include today (partial day) so the chart can show actual + remaining
        future_dates = []
        d = today
        while d <= forecast_end:
            future_dates.append(d)
            d += timedelta(days=1)

        if not future_dates:
            return []

        # Run prediction in thread pool
        dow_corrections = self._dow_corrections
        loop = asyncio.get_event_loop()
        predictions = await loop.run_in_executor(
            _executor, _predict_future, self._model, historical_df, future_dates, dow_corrections
        )

        # Store predictions in DuckDB
        try:
            await store.store_predictions(predictions, sales_type, self._metrics)
        except Exception as e:
            logger.error(f"Failed to store predictions: {e}")

        return predictions

    async def get_forecast(self, sales_type: str = "retail") -> Optional[Dict[str, Any]]:
        """Get stored forecast data (up to 60 days ahead)."""
        from core.duckdb_store import get_store
        store = await get_store()

        today = date.today()
        month_start = date(today.year, today.month, 1)
        if today.month == 12:
            month_end = date(today.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(today.year, today.month + 1, 1) - timedelta(days=1)
        forecast_end = today + timedelta(days=FORECAST_HORIZON_DAYS)

        # Get actual revenue for current month to date
        actual_to_date = await self._get_actual_month_revenue(store, sales_type, month_start, today)

        # Get stored predictions (today + future, up to 60 days)
        predictions = await store.get_predictions(
            start_date=today,
            end_date=forecast_end,
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
            "forecast_end": forecast_end.isoformat(),
        }

    async def _query_daily_revenue(
        self, store: Any, sales_type: str, days_back: int
    ) -> pd.DataFrame:
        """Query daily revenue from Gold layer (pre-aggregated)."""
        start_date = date.today() - timedelta(days=days_back)

        sales_filter = "sales_type = ?" if sales_type != "all" else "1=1"
        params = [start_date]
        if sales_type != "all":
            params.append(sales_type)

        async with store.connection() as conn:
            result = conn.execute(f"""
                SELECT
                    date,
                    revenue,
                    instagram_revenue,
                    telegram_revenue,
                    shopify_revenue,
                    orders_count,
                    unique_customers,
                    new_customers
                FROM gold_daily_revenue
                WHERE date >= ?
                  AND {sales_filter}
                ORDER BY date
            """, params).fetchdf()

        return result

    async def _get_actual_month_revenue(
        self, store: Any, sales_type: str, month_start: date, up_to: date
    ) -> float:
        """Get actual revenue from month start to given date (from Gold layer)."""
        sales_filter = "sales_type = ?" if sales_type != "all" else "1=1"
        params = [month_start, up_to]
        if sales_type != "all":
            params.append(sales_type)

        async with store.connection() as conn:
            result = conn.execute(f"""
                SELECT COALESCE(SUM(revenue), 0) as revenue
                FROM gold_daily_revenue
                WHERE date >= ? AND date <= ?
                  AND {sales_filter}
            """, params).fetchone()

        return float(result[0]) if result else 0.0

    def _save_model(self) -> None:
        """Save model and DOW corrections to disk."""
        import joblib
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._model, MODEL_PATH)
        logger.info(f"Model saved to {MODEL_PATH}")

        # Save DOW corrections
        if self._dow_corrections:
            # JSON keys must be strings
            corrections_str_keys = {str(k): v for k, v in self._dow_corrections.items()}
            with open(DOW_CORRECTIONS_PATH, 'w') as f:
                json.dump(corrections_str_keys, f, indent=2)
            logger.info(f"DOW corrections saved to {DOW_CORRECTIONS_PATH}")

    def _load_model(self) -> bool:
        """Load model and DOW corrections from disk if available.

        Validates that the loaded model has the expected number of features.
        If mismatched (e.g., old 24-feature model), sets model to None so
        the nightly scheduler will retrain with the current feature set.
        """
        import joblib
        if MODEL_PATH.exists():
            try:
                model = joblib.load(MODEL_PATH)
                # Validate feature count matches current FEATURE_COLUMNS
                if hasattr(model, 'num_feature') and model.num_feature() != len(FEATURE_COLUMNS):
                    logger.warning(
                        f"Model feature count mismatch: model has {model.num_feature()}, "
                        f"expected {len(FEATURE_COLUMNS)}. Will retrain on next cycle."
                    )
                    self._model = None
                    return False
                self._model = model
                logger.info(f"Model loaded from {MODEL_PATH}")
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")
                return False
        else:
            return False

        # Load DOW corrections
        if DOW_CORRECTIONS_PATH.exists():
            try:
                with open(DOW_CORRECTIONS_PATH) as f:
                    corrections_str_keys = json.load(f)
                # Convert string keys back to int
                self._dow_corrections = {int(k): v for k, v in corrections_str_keys.items()}
                logger.info(f"DOW corrections loaded from {DOW_CORRECTIONS_PATH}")
            except Exception as e:
                logger.warning(f"Failed to load DOW corrections: {e}")
                self._dow_corrections = {}

        return True


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


def shutdown_prediction_service() -> None:
    """Shutdown the prediction service and release resources.

    Call this during application shutdown to properly cleanup the
    ThreadPoolExecutor and prevent resource leaks.
    """
    global _service
    _executor.shutdown(wait=False)
    _service = None
    logger.info("Prediction service shutdown complete")
