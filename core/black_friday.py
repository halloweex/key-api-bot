"""Black Friday's date: the last Friday of November.

Two places need it, and they must agree on the day: the forecast model's
calendar features (`core.prediction_service._get_holiday_features`, where it
was a function of its own until 2026-11-20) and the month badge's Black Friday
line (`core.forecast_range.year_shift`), which maps last year's November onto
this year's by how far this day moved. Pure arithmetic, the standard library
only.
"""
from __future__ import annotations

from datetime import date, timedelta

FRIDAY = 4  # date.weekday()


def black_friday_date(year: int) -> date:
    """The last Friday of November of `year`, one of its last seven days."""
    last = date(year, 11, 30)
    return last - timedelta(days=(last.weekday() - FRIDAY) % 7)
