"""`core.filters.parse_period`: how `period`, `start_date` and `end_date`
become the dates every dashboard route reads.

Two defects, both in the explicit dates, found by the Rust pilot of
/api/revenue/trend (draft PR #307):

- a malformed date — `2026-02-30`, `2026-13-01`, `26-01-01`,
  `2026-01-01T00:00` — left `strptime` as a bare ValueError, and every route
  answered it 500. It is a ValidationError naming the field now.
- the range had no bound, and four endpoints build one list entry per day of
  it in Python: `0002-01-01` to `9999-12-30` was 6.8 s of synchronous work on
  web's event loop and 95 MB of JSON for the trend alone. Over MAX_RANGE_DAYS
  is a ValidationError naming `date_range`.

And a third, found by putting every dated route against the edges of the
calendar: a well-formed date at either end of Python's calendar answered 500
on eight routes, whose arithmetic stepped off it. Outside EARLIEST_DATE ..
LATEST_DATE (2000-01-01 .. 2099-12-31, the owner's decision of 2026-10-22) is
a ValidationError naming the field.

Everything else is pinned as it was, because the pilot reproduces this
function answer for answer: the shortcuts, the forms `strptime` forgives, a
date without its pair, a shortcut beside dates, a range that runs backwards.
The routes' half — 400, in the shape their other validators use — is
tests/unit/test_date_params_http.py.
"""
from datetime import date, timedelta

import pytest

from core.exceptions import ValidationError
from core.filters import (
    EARLIEST_DATE,
    LATEST_DATE,
    MAX_RANGE_DAYS,
    DateRange,
    parse_period,
)

# A Wednesday, so `week` and `last_week` are not degenerate.
TODAY = date(2026, 10, 21)


def _parse(period=None, start_date=None, end_date=None) -> DateRange:
    return parse_period(period, start_date, end_date, reference_date=TODAY)


class TestShortcuts:
    @pytest.mark.parametrize("period, start, end", [
        ("today", "2026-10-21", "2026-10-21"),
        ("yesterday", "2026-10-20", "2026-10-20"),
        ("week", "2026-10-19", "2026-10-21"),
        ("thisweek", "2026-10-19", "2026-10-21"),
        ("last_week", "2026-10-12", "2026-10-18"),
        ("month", "2026-10-01", "2026-10-21"),
        ("thismonth", "2026-10-01", "2026-10-21"),
        ("last_month", "2026-09-01", "2026-09-30"),
        ("last_7_days", "2026-10-15", "2026-10-21"),
        ("last_28_days", "2026-09-24", "2026-10-21"),
    ])
    def test_each_shortcut(self, period, start, end):
        assert _parse(period).as_str_tuple() == (start, end)

    @pytest.mark.parametrize("period", [None, "", "TODAY", "fortnight"])
    def test_anything_else_without_dates_is_today(self, period):
        """A period the shortcuts do not know falls through to the dates, and
        with none to today — the routes' `validate_period` is what refuses an
        unknown one, and it lowercases without handing the result back."""
        assert _parse(period).as_str_tuple() == ("2026-10-21", "2026-10-21")


class TestExplicitDates:
    def test_iso_dates(self):
        r = _parse(start_date="2026-01-01", end_date="2026-01-31")
        assert (r.start, r.end) == (date(2026, 1, 1), date(2026, 1, 31))

    @pytest.mark.parametrize("raw, read", [
        ("2026-1-5", date(2026, 1, 5)),
        ("2026-01-5", date(2026, 1, 5)),
        ("2026-01- 5", date(2026, 1, 5)),
        # A year in Arabic-Indic digits: `%Y` matches any Unicode digit.
        ("٢٠٢٦-01-05", date(2026, 1, 5)),
    ])
    def test_what_strptime_forgives_is_still_accepted(self, raw, read):
        """A decision, written down in `parse_period`: these answered before
        the fix and answer the same now, normalised on the way out — every
        route reads `as_str_tuple()`, never the raw string."""
        r = _parse(start_date=raw, end_date=raw)
        assert (r.start, r.end) == (read, read)
        assert r.as_str_tuple() == (read.isoformat(), read.isoformat())

    # The four from the pilot's report, then a trailing space, the basic and
    # week ISO forms `date.fromisoformat` would take, and a day zero.
    MALFORMED = ["2026-02-30", "2026-13-01", "26-01-01", "2026-01-01T00:00",
                 "2026-01-01 ", "20260101", "2026-W01-1", "2026-01-00", "soon"]

    @pytest.mark.parametrize("raw", MALFORMED)
    def test_a_malformed_start_date_names_the_field(self, raw):
        with pytest.raises(ValidationError) as caught:
            _parse(start_date=raw, end_date="2026-01-31")
        assert caught.value.field == "start_date"
        assert str(caught.value) == (
            f"start_date: Invalid date format. Expected %Y-%m-%d (got: {raw!r})")

    @pytest.mark.parametrize("raw", MALFORMED)
    def test_a_malformed_end_date_names_the_field(self, raw):
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="2026-01-01", end_date=raw)
        assert caught.value.field == "end_date"
        assert str(caught.value) == (
            f"end_date: Invalid date format. Expected %Y-%m-%d (got: {raw!r})")

    def test_both_malformed_names_the_start(self):
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="2026-02-30", end_date="2026-13-01")
        assert caught.value.field == "start_date"

    @pytest.mark.parametrize("start_date, end_date", [
        ("2026-02-30", None),
        (None, "2026-02-30"),
        ("2026-02-30", ""),
    ])
    def test_a_date_without_its_pair_is_ignored_as_before(self, start_date, end_date):
        assert _parse(start_date=start_date, end_date=end_date).as_str_tuple() == (
            "2026-10-21", "2026-10-21")

    def test_a_shortcut_wins_over_dates_even_malformed_ones(self):
        r = _parse("month", start_date="2026-02-30", end_date="26-01-01")
        assert r.as_str_tuple() == ("2026-10-01", "2026-10-21")

    def test_a_backwards_range_is_accepted_as_before(self):
        r = _parse(start_date="2026-01-31", end_date="2026-01-01")
        assert (r.start, r.end) == (date(2026, 1, 31), date(2026, 1, 1))


class TestTheRangeIsCapped:
    def test_the_cap_holds_ten_calendar_years(self):
        """Ten years is longer than the store's history; three leap days is
        the most ten consecutive calendar years can hold."""
        assert MAX_RANGE_DAYS >= (date(2034, 1, 1) - date(2024, 1, 1)).days == 3653

    def test_exactly_the_cap_is_accepted(self):
        start = date(2016, 1, 1)
        end = start + timedelta(days=MAX_RANGE_DAYS - 1)
        r = _parse(start_date=start.isoformat(), end_date=end.isoformat())
        assert (r.end - r.start).days + 1 == MAX_RANGE_DAYS

    def test_one_day_over_is_refused(self):
        start = date(2016, 1, 1)
        end = start + timedelta(days=MAX_RANGE_DAYS)
        with pytest.raises(ValidationError) as caught:
            _parse(start_date=start.isoformat(), end_date=end.isoformat())
        assert caught.value.field == "date_range"
        assert str(caught.value) == (
            f"date_range: Date range cannot exceed {MAX_RANGE_DAYS} days "
            f"(got: '{MAX_RANGE_DAYS + 1} days')")

    def test_the_pilots_range_is_refused(self):
        """3 651 693 days, 6.8 s of web's event loop for the trend, measured.
        Both its ends are outside the window now, so the start is what it is
        refused on, before its days are counted."""
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="0002-01-01", end_date="9999-12-30")
        assert caught.value.field == "start_date"

    def test_the_whole_window_is_over_the_cap(self):
        """The window is a hundred years and the cap ten, so inside it the
        cap still binds."""
        with pytest.raises(ValidationError) as caught:
            _parse(start_date=EARLIEST_DATE.isoformat(), end_date=LATEST_DATE.isoformat())
        assert caught.value.field == "date_range"
        assert caught.value.value == "36525 days"

    def test_the_cap_is_counted_after_parsing(self):
        """A forgiven form counts the same days as its ISO spelling."""
        start = date(2016, 1, 1)
        end = start + timedelta(days=MAX_RANGE_DAYS)
        with pytest.raises(ValidationError):
            _parse(start_date="2016-1-1", end_date=end.isoformat())


class TestTheWindow:
    def test_the_window_is_the_owners(self):
        assert (EARLIEST_DATE, LATEST_DATE) == (date(2000, 1, 1), date(2099, 12, 31))

    @pytest.mark.parametrize("day", ["2000-01-01", "2099-12-31"])
    def test_both_ends_are_inside(self, day):
        assert _parse(start_date=day, end_date=day).as_str_tuple() == (day, day)

    # A day either side of the window, which answered zeros before, and the
    # calendar's last day as the end alone, named before the range is
    # counted. Then the three that answered 500: the calendar's last day under
    # a per-day loop, year 1 under a comparison, and a backwards range whose
    # previous period lands past 9999-12-31 — each starting outside too.
    OUTSIDE = [
        ("start_date", "1999-12-31", "2000-01-31"),
        ("end_date", "2099-12-01", "2100-01-01"),
        ("end_date", "2099-12-31", "9999-12-31"),
        ("start_date", "9999-12-01", "9999-12-31"),
        ("start_date", "0001-01-01", "0001-01-31"),
        ("start_date", "9999-12-31", "0001-01-01"),
    ]

    @pytest.mark.parametrize("field, start_date, end_date", OUTSIDE)
    def test_a_date_outside_names_its_field(self, field, start_date, end_date):
        with pytest.raises(ValidationError) as caught:
            _parse(start_date=start_date, end_date=end_date)
        raw = start_date if field == "start_date" else end_date
        assert caught.value.field == field
        assert str(caught.value) == (
            f"{field}: Date must be between 2000-01-01 and 2099-12-31 (got: {raw!r})")

    def test_a_forgiven_form_outside_is_named_as_written(self):
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="1999-1-5", end_date="2026-01-31")
        assert caught.value.value == "1999-1-5"

    def test_the_start_is_judged_whole_before_the_end(self):
        """Read, then the window, the start before the end: the first wrong
        date is the one named, whatever is wrong with the other."""
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="1999-12-31", end_date="2026-02-30")
        assert caught.value.field == "start_date"
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="2026-02-30", end_date="9999-12-31")
        assert str(caught.value).startswith("start_date: Invalid date format")

    def test_a_backwards_range_across_the_window_is_accepted_as_before(self):
        r = _parse(start_date="2099-12-31", end_date="2000-01-01")
        assert (r.start, r.end) == (LATEST_DATE, EARLIEST_DATE)

    @pytest.mark.parametrize("start_date, end_date", [
        ("0001-01-01", None),
        (None, "9999-12-31"),
    ])
    def test_a_date_outside_without_its_pair_is_ignored_as_before(self, start_date, end_date):
        assert _parse(start_date=start_date, end_date=end_date).as_str_tuple() == (
            "2026-10-21", "2026-10-21")

    def test_a_shortcut_wins_over_dates_outside(self):
        r = _parse("month", start_date="0001-01-01", end_date="9999-12-31")
        assert r.as_str_tuple() == ("2026-10-01", "2026-10-21")


class TestWhatTheWindowLeaves:
    """The routes' own arithmetic at the farthest ranges the window and the
    cap still let through. Every 500 the window closed was a comparison
    window or a per-day step off the calendar; inside it, they all land
    between 1989-12-24 and 2199-12-30, as `core/filters.py` says."""

    CAP = timedelta(days=MAX_RANGE_DAYS - 1)
    FARTHEST = [
        (EARLIEST_DATE, EARLIEST_DATE + CAP),
        (LATEST_DATE - CAP, LATEST_DATE),
        (LATEST_DATE, EARLIEST_DATE),
    ]

    @pytest.mark.parametrize("compare_type", ["previous_period", "year_ago", "month_ago"])
    @pytest.mark.parametrize("start, end", FARTHEST)
    def test_every_comparison_window_stays_on_the_calendar(self, start, end, compare_type):
        """The trend, the organic traffic block and the marketing report
        derive their comparison this way, and the product momentum its own
        copy of `previous_period`."""
        from core.repositories.revenue import RevenueMixin

        parse_period(None, start.isoformat(), end.isoformat())
        window = RevenueMixin._comparison_window(start, end, compare_type)
        assert date(1989, 12, 24) <= min(window) and max(window) <= date(2199, 12, 30)
