"""The trend's forecast badge says what the chart's bars add up to.

The chart draws each day's actual and stacks `max(0, forecast − actual)` on a
day with a forecast (`useRevenueTrendChartData`), so that day stands at the
larger of the two. The badge used to say something else: the month added
today's revenue so far to today's whole forecast — ₴116 546 counted twice on
2026-11-04 — and the week held only its days from today on, ₴568 930 where
the bars came to ₴813 920.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture
from web.services.dashboard_service import stack_forecast_on_actuals

KYIV = ZoneInfo("Europe/Kyiv")


def _p(day: str, value: float) -> dict:
    return {"date": day, "predicted_revenue": value}


class TestTheSumOfTheBars:
    START = date(2026, 11, 2)                       # a Monday

    def test_today_below_its_forecast_adds_the_rest_of_it(self):
        out = stack_forecast_on_actuals(
            self.START, [100.0, 200.0, 40.0],          # Mon, Tue, Wed so far
            [_p("2026-11-04", 100.0), _p("2026-11-05", 1000.0)])
        assert out == {"actual_to_date": 340.0, "predicted_remaining": 1060.0,
                       "predicted_total": 1400.0}

    def test_a_day_that_beat_its_forecast_adds_nothing(self):
        out = stack_forecast_on_actuals(
            self.START, [100.0, 200.0, 300.0],
            [_p("2026-11-04", 100.0), _p("2026-11-05", 1000.0)])
        assert out["predicted_remaining"] == 1000.0
        assert out["predicted_total"] == 1600.0

    def test_no_forecast_is_the_actuals(self):
        out = stack_forecast_on_actuals(self.START, [100.0, None, 5.5], [])
        assert out == {"actual_to_date": 105.5, "predicted_remaining": 0.0,
                       "predicted_total": 105.5}

    def test_the_month_measured_on_production(self):
        """2026-11-04, retail: ₴458 893 so far (₴116 546 of it that day),
        today's forecast ₴114 994 and ₴2 870 070 for the rest of November.
        The badge read ₴3 443 957; the bars come to ₴3 328 963."""
        out = stack_forecast_on_actuals(
            date(2026, 11, 1), [114_000.0, 114_000.0, 114_347.0, 116_546.0],
            [_p("2026-11-04", 114_994.0), _p("2026-11-05", 2_870_070.0)])
        assert out["actual_to_date"] == 458_893.0
        assert out["predicted_total"] == 3_328_963.0


# ─── Through the route ──────────────────────────────────────────────────────

ACTUAL_TODAY = 300.0          # above today's forecast of 100: adds nothing


def _fake_trend(today: date):
    async def get_revenue_trend(start, end, **_kwargs):
        first = datetime.strptime(start, "%Y-%m-%d").date()
        last = datetime.strptime(end, "%Y-%m-%d").date()
        days = [first + timedelta(days=i) for i in range((last - first).days + 1)]
        return {"labels": [d.strftime("%d.%m") for d in days],
                "revenue": [ACTUAL_TODAY if d == today else 0.0 for d in days],
                "orders": [0 for _ in days]}
    return get_revenue_trend


def _days(first: date, last: date, today: date) -> list:
    return [_p((first + timedelta(days=i)).isoformat(),
               100.0 if first + timedelta(days=i) == today else 1000.0)
            for i in range((last - first).days + 1)]


@pytest.fixture
def today(monkeypatch):
    now = datetime.now(KYIV).date()
    monkeypatch.setattr("web.services.dashboard_service.get_revenue_trend", _fake_trend(now))
    return now


def test_the_week_counts_the_days_behind_it(admin_client, today, monkeypatch):  # noqa: F811
    async def get_range_forecast(self, first, last, sales_type="retail"):
        preds = _days(first, last, today)
        total = sum(p["predicted_revenue"] for p in preds)
        return {"predicted_remaining": total, "predicted_total": total,
                "daily_predictions": preds, "model_metrics": {}, "last_trained": None}

    monkeypatch.setattr("core.prediction_service.PredictionService.get_range_forecast",
                        get_range_forecast)
    response = admin_client.get("/api/revenue/trend", params={
        "period": "week", "include_forecast": "true"})
    assert response.status_code == 200, response.text
    forecast = response.json()["forecast"]
    later = len(forecast["daily_predictions"]) - 1
    assert forecast["actual_to_date"] == ACTUAL_TODAY
    assert forecast["predicted_remaining"] == 1000.0 * later
    assert forecast["predicted_total"] == ACTUAL_TODAY + 1000.0 * later


def test_the_month_counts_today_once(admin_client, today, monkeypatch):  # noqa: F811
    month_end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    async def get_forecast_data(sales_type="retail"):
        preds = _days(today, month_end + timedelta(days=3), today)
        return {"actual_to_date": 999_999.0,       # the series decides, not this
                "predicted_remaining": 0.0, "predicted_total": 0.0,
                "daily_predictions": preds, "model_metrics": {}, "last_trained": None,
                "month_start": today.replace(day=1).isoformat(),
                "month_end": month_end.isoformat(),
                "forecast_end": (month_end + timedelta(days=3)).isoformat()}

    monkeypatch.setattr("web.services.dashboard_service.get_forecast_data",
                        get_forecast_data)
    response = admin_client.get("/api/revenue/trend", params={
        "period": "month", "include_forecast": "true"})
    assert response.status_code == 200, response.text
    forecast = response.json()["forecast"]
    later = (month_end - today).days
    assert [p["date"] for p in forecast["daily_predictions"]][-1] == month_end.isoformat()
    assert forecast["actual_to_date"] == ACTUAL_TODAY
    assert forecast["predicted_total"] == ACTUAL_TODAY + 1000.0 * later
