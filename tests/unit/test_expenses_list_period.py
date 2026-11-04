"""The manual-expenses list reads its window the way every other route does.

`GET /api/expenses` (the manual-expenses table on the dashboard, the recent ad
spend on /traffic) carried its own copy of the period rules. It knew six of the
eight periods the header offers and took no dates, so `last_7_days`,
`last_28_days` and a custom range listed, and summed, every expense ever
typed; and the page sent a custom range as `period=custom` with no dates at
all. Both now go through what every other route uses: `core.filters.
parse_period` on the server, `setPeriodParams` (`store/filterStore.ts`) on the
page. Latent while `app.manual_expenses` holds no rows; the first expense typed
into it would have shown it.

A malformed date, a date outside the window and a range over the cap are
`tests/unit/test_date_params_http.py`'s, which finds this route by its
`start_date` and `end_date` like every other dated route.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from core.filters import parse_period
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

PERIODS = ["today", "yesterday", "week", "last_week", "month", "last_month",
           "last_7_days", "last_28_days"]
RANGE = {"start_date": "2026-10-03", "end_date": "2026-10-17"}


@pytest.fixture
def store(monkeypatch):
    store = AsyncMock()
    store.list_expenses.return_value = []
    store.get_expenses_summary.return_value = {}
    monkeypatch.setattr("web.routes.api.expenses.get_store", AsyncMock(return_value=store))
    return store


def _windows(store):
    """The window each of the route's two reads was asked for."""
    listed = store.list_expenses.await_args.kwargs
    summed = store.get_expenses_summary.await_args.kwargs
    return ((listed["start_date"], listed["end_date"]),
            (summed["start_date"], summed["end_date"]))


@pytest.mark.parametrize("period", PERIODS)
def test_every_period_is_the_window_every_route_reads(admin_client, store, period):
    r = admin_client.get("/api/expenses", params={"period": period})
    assert r.status_code == 200, r.text
    window = parse_period(period).as_tuple()
    assert _windows(store) == (window, window)


@pytest.mark.parametrize("params", [RANGE, {"period": "custom", **RANGE}],
                         ids=["dates", "custom-from-an-older-page"])
def test_a_custom_range_is_its_dates(admin_client, store, params):
    r = admin_client.get("/api/expenses", params=params)
    assert r.status_code == 200, r.text
    window = parse_period(**RANGE).as_tuple()
    assert _windows(store) == (window, window)


@pytest.mark.parametrize("params", [{}, {"period": "custom"}, {"start_date": "2026-10-03"}],
                         ids=["nothing", "custom-without-dates", "a-lone-date"])
def test_without_a_window_it_is_the_whole_list(admin_client, store, params):
    r = admin_client.get("/api/expenses", params=params)
    assert r.status_code == 200, r.text
    assert _windows(store) == ((None, None), (None, None))


def test_an_unknown_period_is_a_400_before_anything_is_read(admin_client, store):
    r = admin_client.get("/api/expenses", params={"period": "fortnight"})
    assert r.status_code == 400
    assert r.json()["detail"].startswith("period: Must be one of:")
    store.list_expenses.assert_not_awaited()
