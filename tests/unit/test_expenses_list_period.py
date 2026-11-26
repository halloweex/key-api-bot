"""The manual-expenses list and its sum cover the window the visitor picked.

`GET /api/expenses` (the manual-expenses table on the dashboard, the recent ad
spend on /traffic) carried its own copy of the period rules. It knew six of the
eight periods the header offers and took no dates, so `last_7_days`,
`last_28_days` and a custom range listed, and summed, every expense ever
typed; and the page sent a custom range as `period=custom` with no dates at
all. Latent while `app.manual_expenses` held no rows; the first expense typed
into it would have shown it.

So the route is asked here as a signed-in editor over the test Postgres, with
five typed expenses around today (Kyiv): which ones it lists, and what its
summary adds up, for each way the page and older pages ask.

A malformed date, a date outside the window and a range over the cap are
`tests/unit/test_date_params_http.py`'s, which finds this route by its
`start_date` and `end_date` like every other dated route.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import DSN, needs_postgres, tab_in_postgres

pytestmark = needs_postgres

KYIV = ZoneInfo("Europe/Kyiv")

# (id, days from today, amount). The amounts are distinct powers, so a total
# names the rows it summed. One expense is dated ahead: planned spend typed in
# advance, which a window ending today must leave out.
EXPENSES = [(1, 0, 1000.0), (2, -5, 200.0), (3, -20, 30.0), (4, -60, 4.0),
            (5, 3, 0.5)]
NEWEST_FIRST = [5, 1, 2, 3, 4]


def _today() -> date:
    """The day the dashboard's periods are counted from: Kyiv's."""
    return datetime.now(KYIV).date()


def _day(offset: int) -> str:
    return (_today() + timedelta(days=offset)).isoformat()


async def _typed(conn, _pool):
    await conn.executemany(
        "INSERT INTO app.manual_expenses (id, expense_date, category, expense_type,"
        " amount, currency, note, created_at, updated_at, platform)"
        " VALUES ($1, $2, 'marketing', 'Facebook Ads', $3, 'UAH', NULL, now(), now(),"
        " 'facebook')",
        [(eid, _today() + timedelta(days=offset), amount)
         for eid, offset, amount in EXPENSES])


@pytest_asyncio.fixture
async def typed(monkeypatch):
    """The five expenses in `app.manual_expenses`, and nothing else there."""
    async with tab_in_postgres(DSN, monkeypatch, _typed) as pool:
        yield pool


# (case, query string, the ids listed). The query string is built at run time:
# the windows are counted from today.
CASES = [
    ("today", lambda: {"period": "today"}, [1]),
    ("last_7_days", lambda: {"period": "last_7_days"}, [1, 2]),
    ("last_28_days", lambda: {"period": "last_28_days"}, [1, 2, 3]),
    ("all_time", lambda: {"period": "all_time"}, [1, 2, 3, 4]),
    ("dates", lambda: {"start_date": _day(-25), "end_date": _day(-10)}, [3]),
    # What a page loaded before the fix sends for a custom range.
    ("custom-with-dates",
     lambda: {"period": "custom", "start_date": _day(-25), "end_date": _day(-10)}, [3]),
    # No window at all is the whole list, as it always was.
    ("custom-without-dates", lambda: {"period": "custom"}, NEWEST_FIRST),
    ("nothing", lambda: {}, NEWEST_FIRST),
    # One date alone is ignored, as on every other route.
    ("a-lone-date", lambda: {"start_date": _day(-25)}, NEWEST_FIRST),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("params,listed", [(p, ids) for _, p, ids in CASES],
                         ids=[name for name, _, _ in CASES])
async def test_the_list_and_its_sum_are_the_window(typed, params, listed):
    async with signed_in("editor") as editor:
        r = await editor.get("/api/expenses", params=params())
    assert r.status_code == 200, r.text
    body = r.json()
    assert [e["id"] for e in body["expenses"]] == listed
    amounts = {eid: amount for eid, _, amount in EXPENSES}
    assert body["summary"]["count"] == len(listed)
    assert body["summary"]["total"] == pytest.approx(sum(amounts[i] for i in listed))


@pytest.mark.asyncio
async def test_an_unknown_period_is_a_400(typed):
    async with signed_in("editor") as editor:
        r = await editor.get("/api/expenses", params={"period": "fortnight"})
    assert r.status_code == 400
