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

Everything else is pinned as it was, because the pilot reproduces this
function answer for answer: the shortcuts, the forms `strptime` forgives, a
date without its pair, a shortcut beside dates, a range that runs backwards.
The routes' half — 400, in the shape their other validators use — is
tests/integration/test_date_params_http.py.
"""
from datetime import date, timedelta

import pytest

from core.exceptions import ValidationError
from core.filters import MAX_RANGE_DAYS, DateRange, parse_period

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
        """3 651 693 days: 6.8 s of web's event loop for the trend, measured."""
        with pytest.raises(ValidationError) as caught:
            _parse(start_date="0002-01-01", end_date="9999-12-30")
        assert caught.value.field == "date_range"
        assert caught.value.value == "3651693 days"

    def test_the_cap_is_counted_after_parsing(self):
        """A forgiven form counts the same days as its ISO spelling."""
        start = date(2016, 1, 1)
        end = start + timedelta(days=MAX_RANGE_DAYS)
        with pytest.raises(ValidationError):
            _parse(start_date="2016-1-1", end_date=end.isoformat())
