"""
Date and period filtering utilities.

Shared between bot and web services to avoid duplication.
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional, Tuple


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

    Examples:
        >>> parse_period("today")
        DateRange(start=date(2026, 1, 13), end=date(2026, 1, 13))

        >>> parse_period("week")
        DateRange(start=date(2026, 1, 13), end=date(2026, 1, 13))

        >>> parse_period(start_date="2026-01-01", end_date="2026-01-31")
        DateRange(start=date(2026, 1, 1), end=date(2026, 1, 31))
    """
    today = reference_date or date.today()

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

    # Handle explicit dates
    if start_date and end_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
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
    }
    return labels.get(period, period.title() if period else "Today")
