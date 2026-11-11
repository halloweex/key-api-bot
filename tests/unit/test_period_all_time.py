"""`all_time`: the whole history as a period, and the halves it has none of.

The owner asked for a period filter that shows every metric over the whole
history (2026-11-05). It is a shortcut like the others, from the first day
the store holds an order (`core.filters.HISTORY_START`) to today, so every
route that reads `period` answers it without learning anything new.

What it changes is what can be set beside it. Its previous period lies wholly
before the first order, so it is zeros, and a year back still covers two
thirds of the history — a baseline made of the same orders. So the routes
that compare ask `has_comparison` and leave that half out: the trend reads
no comparison window, the marketing report reads neither of its two and its
CSV carries no columns for them.

And the trend answers it by month when asked (`granularity=monthly`, which
the page sends for `all_time`): by day it is a thousand bars whose `dd.mm`
labels repeat every year.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core.filters import HISTORY_START, MAX_RANGE_DAYS, by_month, has_comparison, parse_period
from core.repositories.customers import CustomersMixin
from core.repositories.revenue import RevenueMixin
from core.validators import validate_period
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
)

ROOT = Path(__file__).resolve().parents[2]
TODAY = date(2026, 11, 5)


class TestThePeriod:

    def test_from_the_first_order_to_today(self):
        r = parse_period("all_time", reference_date=TODAY)
        assert (r.start, r.end) == (date(2023, 12, 2), TODAY)

    def test_the_routes_accept_it(self):
        """Mutation: leave it out of `validate_period`'s set and every route
        answers it 400."""
        assert validate_period("all_time") == "all_time"
        assert validate_period("ALL_TIME") == "all_time"

    def test_only_it_has_nothing_to_be_compared_with(self):
        assert not has_comparison("all_time")
        for period in (None, "today", "week", "month", "last_month",
                       "last_7_days", "last_28_days", "custom"):
            assert has_comparison(period), period

    def test_the_page_starts_it_on_the_same_day(self):
        """The header line says the period in words from the page's own copy
        of the date (`utils/periodDates.ts`). Mutation: move either."""
        source = (ROOT / "web/frontend/src/utils/periodDates.ts").read_text()
        found = re.findall(r"export const HISTORY_START = '(\d{4}-\d{2}-\d{2})'", source)
        assert found == [HISTORY_START.isoformat()]

    def test_the_picker_offers_it(self):
        source = (ROOT / "web/frontend/src/components/PeriodFilter.tsx").read_text()
        assert "{ value: 'all_time', labelKey: 'filter.allTime' }" in source

    @pytest.mark.parametrize("days", [0, 365 * 10])
    def test_it_fits_the_range_cap_for_a_decade(self, days):
        """Explicit dates over MAX_RANGE_DAYS are refused; a shortcut is not
        held to it, and this one grows by a day every day. It stays inside
        the cap to the end of 2033."""
        r = parse_period("all_time", reference_date=HISTORY_START + timedelta(days=days))
        assert (r.end - r.start).days + 1 <= MAX_RANGE_DAYS


# ─── The trend by month ───────────────────────────────────────────────────────

class _Gold(RevenueMixin):
    """The trend over Gold rows written out, and a record of every window
    it was asked for."""

    def __init__(self, rows):
        self.rows = rows
        self.asked: list[tuple[date, date]] = []

    async def _pg_gold_series(self, start_date, end_date, sales_type, source_id):
        self.asked.append((start_date, end_date))
        return [r for r in self.rows if start_date <= r[0] <= end_date]


ROWS = [
    (date(2023, 12, 2), 100.0, 1),
    (date(2023, 12, 31), 50.5, 2),
    (date(2024, 1, 1), 10.0, 1),
    (date(2024, 2, 29), 7.25, 3),
]


class TestTheTrendByMonth:

    @pytest.mark.asyncio
    async def test_one_point_a_month_with_its_days_summed(self):
        """Every month of the range is a point, an empty one included, and a
        month's revenue and orders are its days'. Mutation: label `%m.%y`, or
        sum into the wrong bucket."""
        gold = _Gold(ROWS)
        result = await gold.get_revenue_trend(
            date(2023, 12, 2), date(2024, 3, 10), granularity="monthly")
        assert result["labels"] == ["12.2023", "01.2024", "02.2024", "03.2024"]
        assert result["revenue"] == [150.5, 10.0, 7.25, 0.0]
        assert result["orders"] == [3, 1, 3, 0]
        assert result["granularity"] == "monthly"

    @pytest.mark.asyncio
    async def test_a_month_carries_no_comparison(self):
        """A month has no counterpart in a window of the same number of
        days: the comparison window is never read, whatever was asked."""
        gold = _Gold(ROWS)
        result = await gold.get_revenue_trend(
            date(2023, 12, 2), date(2024, 3, 10), granularity="monthly",
            include_comparison=True, compare_type="year_ago")
        assert "comparison" not in result
        assert gold.asked == [(date(2023, 12, 2), date(2024, 3, 10))]

    @pytest.mark.asyncio
    async def test_by_day_is_as_it_was(self):
        """The default answer keeps its shape: day labels, no `granularity`
        key (the recorded goldens hold the whole body), a comparison."""
        gold = _Gold(ROWS)
        result = await gold.get_revenue_trend(date(2024, 2, 28), date(2024, 3, 1))
        assert result["labels"] == ["28.02", "29.02", "01.03"]
        assert result["revenue"] == [0, 7.25, 0]
        assert "granularity" not in result
        assert result["comparison"]["period"]["type"] == "previous_period"


class TestByMonth:

    def test_every_month_touched_with_its_days_inside(self):
        """The first and last month are partial, a month with no orders is a
        zero point, and orders stay integers."""
        daily = {date(2023, 12, 30): (10.0, 1), date(2024, 1, 31): (2.5, 2),
                 date(2024, 3, 1): (4.0, 1), date(2024, 3, 9): (99.0, 9)}
        assert by_month(date(2023, 12, 30), date(2024, 3, 1), daily) == [
            ("12.2023", 10.0, 1), ("01.2024", 2.5, 2),
            ("02.2024", 0.0, 0), ("03.2024", 4.0, 1)]

    def test_an_empty_range_is_no_months(self):
        assert by_month(date(2024, 2, 2), date(2024, 2, 1), {}) == []


class _Insights(CustomersMixin):
    """Customer insights over Gold rows written out: each statement is
    answered by what it selects, so a monthly AOV can only come from the
    day's two sums."""

    def __init__(self, days):
        self.days = days  # {day: (revenue, orders)}
        self.asked: list[str] = []

    async def _dashboard_run(self, sql, params=()):
        self.asked.append(sql)
        if len(params) < 2 or not isinstance(params[0], date):
            return [(0, 0, 0, 0)]  # the lifetime statements, no window
        a, b = params[0], params[1]
        days = {d: v for d, v in self.days.items() if a <= d <= b}
        if "SUM(revenue) AS revenue, SUM(orders_count) AS orders" in sql:
            return [(d, rev, n) for d, (rev, n) in sorted(days.items())]
        if "AS aov" in sql:
            return [(d, rev / n) for d, (rev, n) in sorted(days.items())]
        if "AS total_orders" in sql:
            return [(sum(n for _, n in days.values()), sum(r for r, _ in days.values()))]
        return [(0, 0, 0, 0)]


DAYS = {
    date(2024, 1, 1): (1000.0, 1),    # one big order: AOV 1 000
    date(2024, 1, 2): (100.0, 10),    # ten small ones: AOV 10
    date(2024, 2, 1): (300.0, 3),
}


class TestTheAovByMonth:

    @pytest.mark.asyncio
    async def test_a_months_aov_is_its_revenue_over_its_orders(self):
        """January is ₴1 100 over 11 orders, ₴100 — not 505, the mean of its
        two days' AOVs. Mutation: average the daily AOVs."""
        store = _Insights(DAYS)
        result = await store.get_customer_insights(
            date(2024, 1, 1), date(2024, 3, 31), granularity="monthly")
        trend = result["aovTrend"]
        assert trend["labels"] == ["01.2024", "02.2024", "03.2024"]
        assert trend["datasets"][0]["data"] == [100.0, 100.0, 0]
        assert trend["granularity"] == "monthly"

    @pytest.mark.asyncio
    async def test_by_day_is_as_it_was(self):
        store = _Insights(DAYS)
        result = await store.get_customer_insights(date(2024, 1, 1), date(2024, 1, 3))
        trend = result["aovTrend"]
        assert trend["labels"] == ["01.01", "02.01", "03.01"]
        assert trend["datasets"][0]["data"] == [1000.0, 10.0, 0]
        assert "granularity" not in trend


# ─── Through the routes ───────────────────────────────────────────────────────

def _trend_spy(monkeypatch):
    from web.services import dashboard_service

    spy = AsyncMock(return_value={"labels": [], "revenue": [], "orders": []})
    monkeypatch.setattr(dashboard_service, "get_revenue_trend", spy)
    return spy


class TestTheTrendRoute:

    def test_all_time_reads_no_comparison(self, admin_client, monkeypatch):
        spy = _trend_spy(monkeypatch)
        r = admin_client.get("/api/revenue/trend?period=all_time&granularity=monthly"
                             "&compare_type=year_ago")
        assert r.status_code == 200, r.text
        assert spy.await_args.args[0] == HISTORY_START.isoformat()
        assert spy.await_args.kwargs["include_comparison"] is False
        assert spy.await_args.kwargs["granularity"] == "monthly"

    def test_any_other_period_still_compares(self, admin_client, monkeypatch):
        spy = _trend_spy(monkeypatch)
        r = admin_client.get("/api/revenue/trend?period=month")
        assert r.status_code == 200, r.text
        assert spy.await_args.kwargs["include_comparison"] is True
        assert spy.await_args.kwargs["granularity"] == "daily"

    def test_an_unknown_granularity_is_named(self, admin_client, monkeypatch):
        _trend_spy(monkeypatch)
        r = admin_client.get("/api/revenue/trend?period=month&granularity=weekly")
        assert r.status_code == 400
        assert r.json()["detail"].startswith("granularity")


class TestTheInsightsRoute:

    def _spy(self, monkeypatch):
        from web.services import dashboard_service

        spy = AsyncMock(return_value={})
        monkeypatch.setattr(dashboard_service, "get_customer_insights", spy)
        return spy

    def test_it_hands_the_granularity_on(self, admin_client, monkeypatch):
        spy = self._spy(monkeypatch)
        r = admin_client.get("/api/customers/insights?period=all_time&granularity=monthly")
        assert r.status_code == 200, r.text
        assert spy.await_args.kwargs["granularity"] == "monthly"
        r = admin_client.get("/api/customers/insights?period=month")
        assert spy.await_args.kwargs["granularity"] == "daily"

    def test_an_unknown_granularity_is_named(self, admin_client, monkeypatch):
        self._spy(monkeypatch)
        r = admin_client.get("/api/customers/insights?period=month&granularity=weekly")
        assert r.status_code == 400
        assert r.json()["detail"].startswith("granularity")


def _report(compared: bool) -> dict:
    stats = {"revenue": 1000.0, "orders": 2, "avg_check": 500.0, "customers": 2,
             "new_customers": 1, "returning_customers": 1, "return_rate": 50.0}
    return {
        "start_date": "2023-12-02", "end_date": TODAY.isoformat(),
        "prev_start_date": "2020-01-01" if compared else None,
        "prev_end_date": "2023-12-01" if compared else None,
        "yoy_start_date": "2022-12-02" if compared else None,
        "yoy_end_date": "2025-11-05" if compared else None,
        "general_sales": {"current": stats,
                          "previous": stats if compared else None,
                          "year_ago": stats if compared else None,
                          "monthly_goal": None},
        "product_filter": None, "brands": [], "sources": [],
    }


class TestTheMarketingReport:

    def _store(self, monkeypatch, compared):
        store = AsyncMock()
        store.get_marketing_report_by_dates.return_value = _report(compared)
        monkeypatch.setattr("web.routes.api.reports.get_store",
                            AsyncMock(return_value=store))
        return store

    def test_all_time_reads_neither_comparison(self, admin_client, monkeypatch):
        store = self._store(monkeypatch, compared=False)
        r = admin_client.get("/api/reports/marketing-summary?period=all_time")
        assert r.status_code == 200, r.text
        assert store.get_marketing_report_by_dates.await_args.kwargs[
            "include_comparison"] is False

    def test_its_csv_has_no_comparison_columns(self, admin_client, monkeypatch):
        self._store(monkeypatch, compared=False)
        r = admin_client.get("/api/reports/marketing-summary/export/csv?period=all_time")
        assert r.status_code == 200, r.text
        rows = list(csv.reader(io.StringIO(r.content.decode("utf-8-sig"))))
        header = next(row for row in rows if row and row[0] == "Показник")
        assert header == ["Показник", f"2023-12-02 — {TODAY.isoformat()}"]
        revenue = next(row for row in rows if row and row[0] == "Виручка (грн)")
        assert revenue == ["Виручка (грн)", "₴1,000"]

    def test_a_month_keeps_both(self, admin_client, monkeypatch):
        store = self._store(monkeypatch, compared=True)
        r = admin_client.get("/api/reports/marketing-summary/export/csv?period=month")
        assert r.status_code == 200, r.text
        assert store.get_marketing_report_by_dates.await_args.kwargs[
            "include_comparison"] is True
        rows = list(csv.reader(io.StringIO(r.content.decode("utf-8-sig"))))
        header = next(row for row in rows if row and row[0] == "Показник")
        assert len(header) == 6
