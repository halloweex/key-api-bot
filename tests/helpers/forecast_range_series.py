"""The synthetic shop the forecast range's tests run on."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np


D = timedelta


FIRST = date(2023, 1, 2)


LAST = date(2025, 12, 31)


CLOSED = (date(2023, 5, 1), date(2024, 5, 5), date(2025, 1, 1))


PROMOS = {date(2024, 8, 20): 2.2, date(2025, 3, 8): 2.2, date(2025, 8, 21): 2.0,
          # one big spring promotion, a single day
          date(2024, 3, 8): 5.0,
          # Black Friday: three days at ×5.5, ×5.0, ×4.6
          date(2024, 11, 29): 5.5, date(2024, 11, 30): 5.0, date(2024, 12, 1): 4.6}


def _revenue() -> dict:
    """Three years of a shop: a weekday pattern, a level shift, noise, three
    closed days (no row, as in Gold), ordinary promotions and a Black Friday."""
    rng = np.random.default_rng(7)
    n = (LAST - FIRST).days + 1
    days = [FIRST + D(days=i) for i in range(n)]
    weekday = np.array([1.10, 1.05, 1.00, 1.00, 1.08, 0.85, 0.80])
    level = np.where(np.arange(n) < 500, 100_000.0, 120_000.0)
    rev = level * weekday[[d.weekday() for d in days]] * rng.lognormal(0, 0.2, n)
    out = {}
    for d, v in zip(days, rev):
        if d in CLOSED:
            continue
        out[d] = round(float(v) * PROMOS.get(d, 1.0), 2)
    return out


REVENUE = _revenue()
