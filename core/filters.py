"""
Date and period filtering utilities.

Shared between bot and web services to avoid duplication.
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from core.exceptions import ValidationError
from core.validators import validate_date_string

# DuckDB stores dates in Kyiv timezone, so all period calculations must use it
_KYIV_TZ = ZoneInfo("Europe/Kyiv")

# The longest range explicit dates may ask for, both ends counted: ten years
# and a week. Four endpoints build one list entry per day of the range in
# Python — /api/revenue/trend (twice, with its comparison), /api/expenses/summary,
# /api/expenses/profit and /api/customers/insights — so an unbounded range was
# a request for millions of entries, built synchronously on web's event loop.
# Measured on a throwaway store (Python 3.14, 2026-10-21): 0002-01-01 to
# 9999-12-30 cost the trend 6.8 s, 95 MB of JSON and +376 MB of memory, and
# each of the other three 5.5-6.2 s; ten years cost each of them 0.02 s and
# 0.1 MB. Ten years is longer than the store's whole history, so the cap
# refuses nothing a person asks for. Over it is a 400 naming `date_range`.
MAX_RANGE_DAYS = 3660


@dataclass
class DateRange:
    """Represents a date range with both date objects and string formats."""
    start: date
    end: date

    @property
    def start_str(self) -> str:
        """Start date as YYYY-MM-DD string."""
        return self.start.strftime("%Y-%m-%d")

    @property
    def end_str(self) -> str:
        """End date as YYYY-MM-DD string."""
        return self.end.strftime("%Y-%m-%d")

    def as_tuple(self) -> Tuple[date, date]:
        """Return as (start, end) tuple of dates."""
        return (self.start, self.end)

    def as_str_tuple(self) -> Tuple[str, str]:
        """Return as (start, end) tuple of strings."""
        return (self.start_str, self.end_str)


# Period name mappings (web uses 'week', bot uses 'thisweek')
PERIOD_ALIASES = {
    "thisweek": "week",
    "thismonth": "month",
}


def parse_period(
    period: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    reference_date: Optional[date] = None,
) -> DateRange:
    """
    Parse period shortcut or explicit dates into DateRange.

    Args:
        period: Period shortcut (today, yesterday, week, last_week, month, last_month)
                Also accepts bot aliases: thisweek, thismonth
        start_date: Explicit start date (YYYY-MM-DD), used if period is None
        end_date: Explicit end date (YYYY-MM-DD), used if period is None
        reference_date: Reference date for calculations (default: today)

    Returns:
        DateRange with start and end dates

    Raises:
        ValidationError: `start_date` or `end_date` that is not a date, or a
            range over MAX_RANGE_DAYS (`date_range`). Routes answer it 400.

    The explicit dates are read by `validate_date_string`, which is the same
    `strptime("%Y-%m-%d")` they always went through. So every form it forgives
    is still accepted and comes back zero-padded — `2026-1-5`, `2026-01- 5`, a
    year in Arabic-Indic digits — and that is a decision, not an oversight
    (2026-10-21): refusing them would turn requests that answer today into
    400s for no gain anyone can name. What it refuses — a day that does not
    exist, a two-digit year, a time after the date — used to escape as a bare
    ValueError and answer 500.

    Unchanged, also on purpose: a date without its pair is ignored, a known
    period shortcut wins over the dates, and a range whose start is after its
    end is accepted and covers no days.

    Examples:
        >>> parse_period("today")
        DateRange(start=date(2026, 1, 13), end=date(2026, 1, 13))

        >>> parse_period("week")
        DateRange(start=date(2026, 1, 13), end=date(2026, 1, 13))

        >>> parse_period(start_date="2026-01-01", end_date="2026-01-31")
        DateRange(start=date(2026, 1, 1), end=date(2026, 1, 31))
    """
    today = reference_date or datetime.now(_KYIV_TZ).date()

    # Normalize period aliases
    if period:
        period = PERIOD_ALIASES.get(period, period)

    # Handle period shortcuts
    if period == "today":
        return DateRange(today, today)

    elif period == "yesterday":
        yesterday = today - timedelta(days=1)
        return DateRange(yesterday, yesterday)

    elif period == "week":
        start_of_week = today - timedelta(days=today.weekday())
        return DateRange(start_of_week, today)

    elif period == "last_week":
        start_of_this_week = today - timedelta(days=today.weekday())
        end_of_last_week = start_of_this_week - timedelta(days=1)
        start_of_last_week = end_of_last_week - timedelta(days=6)
        return DateRange(start_of_last_week, end_of_last_week)

    elif period == "month":
        start_of_month = today.replace(day=1)
        return DateRange(start_of_month, today)

    elif period == "last_month":
        first_of_this_month = today.replace(day=1)
        last_of_last_month = first_of_this_month - timedelta(days=1)
        first_of_last_month = last_of_last_month.replace(day=1)
        return DateRange(first_of_last_month, last_of_last_month)

    # Rolling periods
    elif period == "last_7_days":
        start = today - timedelta(days=6)  # 7 days including today
        return DateRange(start, today)

    elif period == "last_28_days":
        start = today - timedelta(days=27)  # 28 days including today
        return DateRange(start, today)

    # Handle explicit dates
    if start_date and end_date:
        start = validate_date_string(start_date, "start_date")
        end = validate_date_string(end_date, "end_date")
        days = (end - start).days + 1
        if days > MAX_RANGE_DAYS:
            raise ValidationError(
                "date_range", f"Date range cannot exceed {MAX_RANGE_DAYS} days",
                f"{days} days",
            )
        return DateRange(start, end)

    # Default to today
    return DateRange(today, today)


def get_period_label(period: str) -> str:
    """
    Get human-readable label for period.

    Args:
        period: Period shortcut

    Returns:
        Human-readable label
    """
    labels = {
        "today": "Today",
        "yesterday": "Yesterday",
        "week": "This Week",
        "thisweek": "This Week",
        "last_week": "Last Week",
        "month": "This Month",
        "thismonth": "This Month",
        "last_month": "Last Month",
        "last_7_days": "Last 7 Days",
        "last_28_days": "Last 28 Days",
    }
    return labels.get(period, period.title() if period else "Today")
