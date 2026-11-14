"""The Rust trend's contract, against the frontend's code.

`tests/frontend_contract.py` reads what the dashboard sends to
`GET /api/revenue/trend` out of the TypeScript; the trend's harness sends
exactly that. This file writes down what it reads today. When the page starts
sending something new — a period, a sales type, a source, a comparison, a
parameter — the reading changes, this fails, and whoever changed the page has
to teach the harness (and the Rust trend) the new input before it can pass: a
port is never proven on requests nobody makes while the real ones go unasked.
"""
from __future__ import annotations

from datetime import date

import pytest

from tests import frontend_contract as fc

TODAY = date(2026, 11, 12)


def test_the_page_builds_the_trend_query_as_read():
    fc.check_shapes()


def test_what_the_header_and_the_chart_offer():
    assert fc.periods() == ("today", "yesterday", "week", "last_week", "month", "last_month",
                            "last_7_days", "last_28_days", "all_time")
    assert fc.sales_types() == ("retail", "b2b", "exhibition", "internal", "all")
    assert fc.sources() == (1, 2, 4), (
        "the header offers another source: a source Gold has no channel for is "
        "handed to Python, so teach the Rust trend first")
    assert fc.compare_types() == ("previous_period", "year_ago", "month_ago")
    assert fc.chart_default_compare() == "year_ago"
    assert fc.dashboard_filters() == frozenset(
        {"period", "salesType", "sourceId", "categoryId", "brand", "promocode"})


@pytest.mark.parametrize("kwargs, expected", [
    (dict(period="month", sales_type="retail", compare_type="year_ago"),
     "period=month&sales_type=retail&compare_type=year_ago&include_forecast=true"),
    (dict(period="month", sales_type="retail"),
     "period=month&sales_type=retail&include_forecast=true"),
    (dict(period="week", sales_type="b2b", source_id=4), "period=week&sales_type=b2b&source_id=4"),
    (dict(period="last_month", sales_type="internal"), "period=last_month&sales_type=internal"),
    (dict(period="all_time", sales_type="all", compare_type="month_ago"),
     "period=all_time&sales_type=all&compare_type=month_ago&granularity=monthly"),
    (dict(period="custom", sales_type="retail", start_date="2026-11-01", end_date="2026-11-30"),
     "start_date=2026-11-01&end_date=2026-11-30&sales_type=retail&include_forecast=true"),
    # The UTC day decides, and a range ending today has no future day.
    (dict(period="custom", sales_type="retail", start_date="2026-11-01", end_date="2026-11-12"),
     "start_date=2026-11-01&end_date=2026-11-12&sales_type=retail"),
    # A custom period with one date sends neither.
    (dict(period="custom", sales_type="retail", start_date="2026-11-01"), "sales_type=retail"),
    (dict(period="month", sales_type="retail", brand="A & B~*", category_id=7, promocode="x y"),
     "period=month&sales_type=retail&category_id=7&brand=A+%26+B%7E*&promocode=x+y"),
])
def test_the_query_the_hook_sends(kwargs, expected):
    assert fc.trend_query(today_utc=TODAY, **kwargs) == expected


def test_a_moved_construct_fails_loudly(monkeypatch):
    real = fc._read

    def edited(rel: str) -> str:
        text = real(rel)
        return text.replace("'&include_forecast=true'", "'&forecast=1'") if rel == "hooks/useApi.ts" else text

    monkeypatch.setattr(fc, "_read", edited)
    with pytest.raises(fc.FrontendShapeError):
        fc.check_shapes()
