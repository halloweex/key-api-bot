"""The trend's comparison is the whole period's (owner's decision, 2026-11-12).

`month` and `week` run from their first day to today, and the chart draws
them to the month's or the week's last day, with forecast bars on the days
ahead. Until this decision the comparison was that of the days so far, and
each forecast day was set beside the day as many days back as there were
forecast days: on 12 November the month view set 1–12 November beside 20–31
October, and under 19–30 November the dashed "previous period" line drew
1–12 November, this month's own days. The week did the same.

Now the comparison is the window before the whole period, day by day: for
November the thirty days before it, 2–31 October, and for a week the week
before it, Monday beside Monday. The growth badge adds up the days through
today on both sides, so 1–12 November is set beside 2–13 October.

Every day's revenue here names its day (`_value`), so a comparison that reads
the wrong day says which one it read.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

import pytest

from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture
from web.services.dashboard_service import fit_trend, last_day_of_period

KYIV = ZoneInfo("Europe/Kyiv")
TODAY = date(2026, 11, 12)                      # a Thursday, the day it was decided


def _value(day: date) -> float:
    """A day's revenue that names the day: 2 October 2026 is 261002."""
    return float((day.year % 100) * 10_000 + day.month * 100 + day.day)


def _days(first: date, last: date) -> List[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def _labels(days: List[date]) -> List[str]:
    return [d.strftime("%d.%m") for d in days]


class _Frozen(datetime):
    """15:00 in Kyiv on TODAY, for `parse_period` and the route alike."""

    @classmethod
    def now(cls, tz=None):
        moment = datetime(TODAY.year, TODAY.month, TODAY.day, 15, 0, tzinfo=KYIV)
        return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)


@pytest.fixture
def gold(monkeypatch):
    """Gold's daily series, a row a day up to today, and the windows asked."""
    asked = []

    async def fetch_series(start, end, sales_type, source_id):
        asked.append((start, end))
        return [(d, _value(d), 1) for d in _days(start, end) if d <= TODAY]

    monkeypatch.setattr("core.filters.datetime", _Frozen)
    monkeypatch.setattr("web.routes.api.analytics._datetime", _Frozen)
    monkeypatch.setattr("core.pg_gold_read.fetch_series", fetch_series)
    return asked


@pytest.fixture
def forecast(monkeypatch):
    """The stored forecast, every day of the range from today on but the
    days put in the returned set. Reading the month's actuals fails."""
    missing = set()

    async def get_range_forecast(self, first, last, sales_type="retail"):
        preds = [{"date": d.isoformat(), "predicted_revenue": 5000.0}
                 for d in _days(max(first, TODAY), last) if d not in missing]
        total = 5000.0 * len(preds)
        return {"predicted_remaining": total, "predicted_total": total,
                "daily_predictions": preds, "model_metrics": {}, "last_trained": None}

    async def never(*args, **kwargs):
        raise AssertionError("the trend read the month's actuals")

    monkeypatch.setattr("core.prediction_service.PredictionService.get_range_forecast",
                        get_range_forecast)
    monkeypatch.setattr("web.services.dashboard_service.get_forecast_data", never)
    return missing


def _trend(client, **params) -> dict:
    response = client.get("/api/revenue/trend", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def _totals(current: List[date], previous: List[date]) -> dict:
    now = sum(_value(d) for d in current)
    then = sum(_value(d) for d in previous)
    return {"current": round(now, 2), "previous": round(then, 2),
            "growth_percent": round((now - then) / then * 100, 1)}


# ─── Through the route ──────────────────────────────────────────────────────

class TestTheMonth:
    NOVEMBER = _days(date(2026, 11, 1), date(2026, 11, 30))
    BEFORE = _days(date(2026, 10, 2), date(2026, 10, 31))     # the thirty days before it

    def test_the_line_runs_under_the_whole_month_day_by_day(
            self, admin_client, gold, forecast):  # noqa: F811
        out = _trend(admin_client, period="month", compare_type="previous_period",
                     include_forecast="true")
        assert out["labels"] == _labels(self.NOVEMBER)
        assert out["comparison"]["revenue"] == [_value(d) for d in self.BEFORE]
        assert out["comparison"]["period"] == {
            "start": "2026-10-02", "end": "2026-10-31", "type": "previous_period"}
        # The days ahead are forecast bars, not revenue.
        assert out["revenue"] == [_value(d) if d <= TODAY else 0 for d in self.NOVEMBER]

    def test_the_badge_counts_the_days_so_far_on_both_sides(
            self, admin_client, gold, forecast):  # noqa: F811
        out = _trend(admin_client, period="month", compare_type="previous_period",
                     include_forecast="true")
        assert out["comparison"]["totals"] == _totals(self.NOVEMBER[:12], self.BEFORE[:12])

    def test_without_a_forecast_the_line_stops_today_and_the_badge_is_the_same(
            self, admin_client, gold, forecast):  # noqa: F811
        out = _trend(admin_client, period="month", compare_type="previous_period")
        assert out["labels"] == _labels(self.NOVEMBER[:12])
        assert out["comparison"]["revenue"] == [_value(d) for d in self.BEFORE[:12]]
        assert out["comparison"]["totals"] == _totals(self.NOVEMBER[:12], self.BEFORE[:12])

    def test_gold_is_asked_once_for_each_window(self, admin_client, gold, forecast):  # noqa: F811
        """The forecast days' comparison came from a second read, of Silver,
        while the rest came from Gold. It is one read of one grain now."""
        _trend(admin_client, period="month", compare_type="previous_period",
               include_forecast="true")
        assert gold == [(date(2026, 11, 1), TODAY), (date(2026, 10, 2), date(2026, 10, 31))]

    def test_a_year_ago_is_as_it_was(self, admin_client, gold, forecast):  # noqa: F811
        """A year back already went day by day: its window runs on to the
        month's end, and the badge still sets 1–12 November beside 1–12
        November a year earlier. The chart's default comparison."""
        out = _trend(admin_client, period="month", compare_type="year_ago",
                     include_forecast="true")
        year_ago = [d.replace(year=2025) for d in self.NOVEMBER]
        assert out["comparison"]["revenue"] == [_value(d) for d in year_ago]
        assert out["comparison"]["totals"] == _totals(self.NOVEMBER[:12], year_ago[:12])

    def test_a_forecast_day_is_placed_by_its_date(
            self, admin_client, gold, forecast):  # noqa: F811
        """A day the stored forecast lacks is not drawn, and the days after
        it keep their own comparison: by date, never by place in a list."""
        forecast.add(date(2026, 11, 20))
        out = _trend(admin_client, period="month", compare_type="previous_period",
                     include_forecast="true")
        drawn = [d for d in self.NOVEMBER if d != date(2026, 11, 20)]
        assert out["labels"] == _labels(drawn)
        assert out["comparison"]["revenue"] == [
            _value(d - timedelta(days=30)) for d in drawn]


class TestTheWeek:
    WEEK = _days(date(2026, 11, 9), date(2026, 11, 15))       # Monday to Sunday
    BEFORE = _days(date(2026, 11, 2), date(2026, 11, 8))

    def test_monday_beside_monday(self, admin_client, gold, forecast):  # noqa: F811
        """It set Monday to Thursday beside the Thursday to Sunday before."""
        out = _trend(admin_client, period="week", compare_type="previous_period",
                     include_forecast="true")
        assert out["labels"] == _labels(self.WEEK)
        assert out["comparison"]["revenue"] == [_value(d) for d in self.BEFORE]
        assert out["comparison"]["totals"] == _totals(self.WEEK[:4], self.BEFORE[:4])


class TestARangeIntoTheFuture:
    def test_the_badge_counts_only_the_days_behind_it(
            self, admin_client, gold, forecast):  # noqa: F811
        """Its days ahead are zeros on one side and whole days on the other:
        the badge set three days of November beside eleven before them."""
        out = _trend(admin_client, start_date="2026-11-10", end_date="2026-11-20",
                     compare_type="previous_period", include_forecast="true")
        days = _days(date(2026, 11, 10), date(2026, 11, 20))
        before = _days(date(2026, 10, 30), date(2026, 11, 9))
        assert out["labels"] == _labels(days)
        assert out["comparison"]["revenue"] == [_value(d) for d in before]
        assert out["comparison"]["totals"] == _totals(days[:3], before[:3])


class TestAPastPeriod:
    def test_is_as_it_was(self, admin_client, gold, forecast):  # noqa: F811
        out = _trend(admin_client, period="last_month", compare_type="previous_period")
        october = _days(date(2026, 10, 1), date(2026, 10, 31))
        before = _days(date(2026, 8, 31), date(2026, 9, 30))
        assert out["labels"] == _labels(october)
        assert out["comparison"]["revenue"] == [_value(d) for d in before]
        assert out["comparison"]["totals"] == _totals(october, before)


# ─── The pieces ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("period,start,end,last", [
    ("month", date(2028, 2, 1), date(2028, 2, 10), date(2028, 2, 29)),     # leap
    ("month", date(2026, 12, 1), date(2026, 12, 5), date(2026, 12, 31)),
    ("week", date(2026, 11, 9), date(2026, 11, 12), date(2026, 11, 15)),
    ("last_month", date(2026, 10, 1), date(2026, 10, 31), date(2026, 10, 31)),
    (None, date(2026, 11, 10), date(2026, 11, 20), date(2026, 11, 20)),
])
def test_the_last_day_of_a_period(period, start, end, last):
    assert last_day_of_period(period, start, end) == last


class TestFitTrend:
    START = date(2026, 11, 1)

    def _result(self, window: List[float]) -> dict:
        labels = ["01.11", "02.11"]
        return {"labels": labels, "revenue": [1.0, 2.0], "orders": [1, 1],
                "datasets": [{"data": [1.0, 2.0]}, {"data": window}],
                "comparison": {"labels": labels, "revenue": window}}

    def test_the_window_is_cut_back_to_the_days_drawn(self):
        result = self._result([10.0, 20.0, 30.0])
        fit_trend(result, self.START)
        assert result["comparison"]["revenue"] == [10.0, 20.0]
        # The same list: the "Previous Period" dataset says the same.
        assert result["datasets"][1]["data"] == [10.0, 20.0]

    def test_a_forecast_day_takes_its_own_position(self):
        result = self._result([10.0, 20.0, 30.0, 40.0])
        fit_trend(result, self.START, [date(2026, 11, 4)])
        assert result["labels"] == ["01.11", "02.11", "04.11"]
        assert result["comparison"]["labels"] == ["01.11", "02.11", "04.11"]
        assert result["revenue"] == [1.0, 2.0, 0]
        assert result["orders"] == [1, 1, 0]
        assert result["comparison"]["revenue"] == [10.0, 20.0, 40.0]

    def test_a_position_past_the_window_draws_zero(self):
        """31 October beside a month back: September has thirty days."""
        result = self._result([10.0])
        fit_trend(result, self.START)
        assert result["comparison"]["revenue"] == [10.0, 0]

    def test_no_comparison_still_draws_the_forecast_days(self):
        result = {"labels": ["01.11"], "revenue": [1.0], "orders": [1]}
        fit_trend(result, self.START, [date(2026, 11, 2)])
        assert result == {"labels": ["01.11", "02.11"], "revenue": [1.0, 0],
                          "orders": [1, 0]}

    def test_an_empty_answer_is_left_alone(self):
        result: dict = {}
        fit_trend(result, self.START, [date(2026, 11, 2)])
        assert result == {}
