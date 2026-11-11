"""Goal parameters Postgres or the calendar would refuse (L3-03).

`POST /api/goals?amount=1e30` (or `inf`) reached `NUMERIC(12,2)` and answered
500; `GET /api/goals/smart?year=2031` answered 422 from a fixed `le=2030`, so
from 2031 the page — which sends the year it shows — would have had no goal.
The amount is bounded and finite now, and the year's ceiling is this year plus
`YEARS_AHEAD`, judged when asked.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture
from web.routes.api import goals as api


@pytest.fixture
def store(monkeypatch):
    fake = AsyncMock()
    fake.set_goal = AsyncMock(return_value={"ok": True})
    fake.get_smart_goals = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr("web.routes.api.goals.get_store", AsyncMock(return_value=fake))
    return fake


@pytest.mark.parametrize("amount", ["1e30", "inf", "nan", "10000000000"])
def test_an_amount_past_its_column_is_refused(admin_client, store, amount):  # noqa: F811
    response = admin_client.post(f"/api/goals?period_type=monthly&amount={amount}")
    assert response.status_code == 422, response.text
    store.set_goal.assert_not_awaited()


def test_a_good_amount_is_written(admin_client, store):  # noqa: F811
    response = admin_client.post("/api/goals?period_type=monthly&amount=4000000")
    assert response.status_code == 200, response.text


def _this_year() -> int:
    return datetime.now(ZoneInfo("Europe/Kyiv")).year


def test_a_year_past_2030_is_asked(admin_client, store):  # noqa: F811
    """Mutation: `le=2030` again — 422 for 2031 even when 2031 is now."""
    year = min(2031, _this_year() + api.YEARS_AHEAD)
    response = admin_client.get(f"/api/goals/smart?year={year}&month=1")
    assert response.status_code == 200, response.text
    assert store.get_smart_goals.await_args.kwargs["year"] == year


def test_too_far_ahead_is_a_400(admin_client, store):  # noqa: F811
    response = admin_client.get(
        f"/api/goals/smart?year={_this_year() + api.YEARS_AHEAD + 1}&month=1")
    assert response.status_code == 400, response.text
    assert response.json()["detail"].startswith("year: ")
    store.get_smart_goals.assert_not_awaited()
