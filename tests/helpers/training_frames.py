"""A training frame the forecast accepts."""
from datetime import date, timedelta

import numpy as np
import pandas as pd


def _valid_training_df(n: int = 120) -> pd.DataFrame:
    start = date(2025, 1, 1)
    dates = [start + timedelta(days=i) for i in range(n)]
    return pd.DataFrame({
        "date": dates,
        "revenue": np.linspace(10000, 20000, n),
    })
