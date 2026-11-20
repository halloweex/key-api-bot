"""The synthetic shop the typical promotion's tests run on."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np


D = timedelta


FIRST = date(2023, 6, 5)


LAST = date(2026, 1, 31)


CLOSED = (date(2024, 1, 1), date(2025, 1, 1))


# (first day, ratios, days after at ×0.8): runs of promotion days, some with
# the slump that follows them — what the three days either side count.
RUNS = [
    (date(2023, 10, 10), (2.4, 2.6), 0),       # more than 730 days before every origin
    (date(2024, 2, 13), (2.2, 2.5, 2.1), 2),
    (date(2024, 4, 9), (2.6, 2.3), 0),
    (date(2024, 6, 18), (2.0, 2.4, 2.2, 2.0), 1),
    (date(2024, 9, 3), (3.0, 2.2), 2),
    (date(2024, 11, 8), (2.3, 2.6, 2.4), 0),     # November, under 4×: counted
    (date(2024, 11, 29), (5.5, 5.0, 4.6), 2),    # Black Friday: left out
    (date(2024, 12, 20), (2.1, 2.8), 0),
    (date(2025, 2, 11), (2.5, 2.2, 2.4), 1),
    (date(2025, 3, 18), (3.1,), 0),              # one day: left out
    (date(2025, 5, 6), (2.0, 1.0, 2.3), 0),      # an ordinary day inside: one run
    (date(2025, 7, 15), (2.7, 2.4), 2),
    (date(2025, 9, 23), (2.2, 2.9, 2.1), 0),
    (date(2025, 11, 28), (5.8, 5.2, 4.9), 1),    # Black Friday again
    (date(2026, 1, 20), (2.6, 2.4), 0),
]


def _revenue() -> dict:
    """Two and a half years of a shop: a weekday pattern, a level shift,
    noise, two closed days (no row, as in Gold) and the runs above."""
    rng = np.random.default_rng(11)
    n = (LAST - FIRST).days + 1
    days = [FIRST + D(days=i) for i in range(n)]
    weekday = np.array([1.10, 1.05, 1.00, 1.00, 1.08, 0.85, 0.80])
    level = np.where(np.arange(n) < 400, 90_000.0, 110_000.0)
    rev = level * weekday[[d.weekday() for d in days]] * rng.lognormal(0, 0.12, n)
    factor = {}
    for start, ratios, slump in RUNS:
        for k, ratio in enumerate(ratios):
            factor[start + D(days=k)] = ratio
        for k in range(slump):
            factor[start + D(days=len(ratios) + k)] = 0.8
    return {d: round(float(v) * factor.get(d, 1.0), 2)
            for d, v in zip(days, rev) if d not in CLOSED}


REVENUE = _revenue()
