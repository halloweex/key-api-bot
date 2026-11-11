"""The assistant counts the way the dashboard counts (the review's A22, L2-05).

Two defects in `core/chat_tools.py`, both from reading Gold's daily rows as if
they added up:

* the average order value was `AVG(avg_order_value)` — the mean of the days'
  averages — where the period's is its revenue over its orders. A day of two
  ₴100 orders and a day of one ₴1 000 order is ₴400 an order, and the mean of
  the days says ₴550; a day Gold holds a row of zeros for (a return, a retired
  source) pulls it lower still;
* the customers were `SUM(unique_customers)` and its two siblings — distinct
  counts per day, added up — so one buyer who bought on two days was two
  customers, one new and one returning: a 50 % repeat rate for one person.

Since A22 the average is `SUM(revenue) / SUM(orders_count)` over the roll-up
rows, and the customers come from Silver through the dashboard's own statement
(`core.repositories.customers.insights_period_query`), so the assistant and
the customer block cannot disagree.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

from tests.integration.pg_tab_seed import (
    DSN, derive, needs_postgres, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

pytestmark = needs_postgres

TODAY = date(2026, 10, 20)
WINDOW = (date(2026, 10, 1), TODAY)


def _at(day: int) -> datetime:
    """Midday in Kyiv on 1..20 October 2026."""
    return datetime(2026, 10, day, 9, 0, tzinfo=timezone.utc)


# (id, source, status, total, ordered_at, buyer, manager, promocode)
ORDERS = [
    # 5 October: two orders of ₴100, two new buyers.
    (61, 1, 1, 100.0, _at(5), 51, None, None),
    (62, 4, 1, 100.0, _at(5), 53, None, None),
    # 9 October: buyer 51 again, ₴1 000.
    (63, 1, 1, 1000.0, _at(9), 51, None, None),
    # 12 October: a return, and an order on the retired source — Gold keeps a
    # retail roll-up row of zeros for each of these days.
    (64, 1, 19, 300.0, _at(12), 54, None, None),
    (65, 3, 1, 700.0, _at(14), 55, None, None),
]
# Retail, active, not returned: ₴1 200 over three orders, two buyers — 51 and
# 53, both new on the 5th, and 51 back on the 9th.
REVENUE, ORDERS_COUNT, AOV = 1200.0, 3, 400.0


async def _populate(conn, pool):
    await seed(conn, orders=ORDERS)
    await derive(pool)


@pytest_asyncio.fixture
async def window(tmp_path, monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        from core import chat_tools

        with patch.object(chat_tools, "_get_date_range", return_value=WINDOW):
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the assistant")


class TestTheAverageOrderValue:
    @pytest.mark.asyncio
    async def test_the_period_summary_is_revenue_over_orders(self, window):
        """Mutation: `AVG(avg_order_value)` back — ₴550 over the two days that
        sold, less over the days of zeros (₴275 here), never ₴400."""
        from core import chat_tools

        answer = await chat_tools._get_revenue_summary("month", "retail")
        assert (answer["total_revenue"], answer["total_orders"]) == (REVENUE, ORDERS_COUNT)
        assert answer["avg_order_value"] == pytest.approx(AOV)

    @pytest.mark.asyncio
    async def test_the_dated_summary_too(self, window):
        from core import chat_tools

        answer = await chat_tools._get_revenue_by_dates(
            WINDOW[0].isoformat(), WINDOW[1].isoformat(), "retail")
        assert answer["avg_order_value"] == pytest.approx(AOV)

    @pytest.mark.asyncio
    async def test_the_customer_insights_too(self, window):
        from core import chat_tools

        answer = await chat_tools._get_customer_insights("month")
        assert answer["avg_order_value"] == pytest.approx(AOV)


class TestTheCustomers:
    @pytest.mark.asyncio
    async def test_a_buyer_who_came_back_is_one_customer(self, window):
        """Two buyers, not three: buyer 51 bought on the 5th and the 9th.
        Mutation: sum Gold's daily `unique_customers` again — three
        customers, two new, one returning, a 33.3 % repeat rate."""
        from core import chat_tools

        answer = await chat_tools._get_customer_insights("month")
        assert answer["total_customers"] == 2
        assert answer["new_customers"] == 2
        assert answer["returning_customers"] == 1
        assert answer["repeat_rate"] == 50.0

    @pytest.mark.asyncio
    async def test_it_is_the_dashboards_customer_block(self, window):
        """The same statement, so the same four numbers as the block on
        screen, for the same retail window."""
        from core import chat_tools

        answer = await chat_tools._get_customer_insights("month")
        block = (await window.get_customer_insights(*WINDOW, sales_type="retail"))["metrics"]
        assert (answer["total_customers"], answer["new_customers"],
                answer["returning_customers"], answer["repeat_rate"],
                round(answer["avg_order_value"], 2)) == (
            block["totalCustomers"], block["newCustomers"],
            block["returningCustomers"], block["repeatRate"],
            block["averageOrderValue"])

    @pytest.mark.asyncio
    async def test_one_buyer_on_two_days_is_one_customer(self, window):
        """The review's shape exactly: a period holding only buyer 51's two
        days. One customer, who is both new (the 5th) and back (the 9th)."""
        from core import chat_tools

        with patch.object(chat_tools, "_get_date_range",
                          return_value=(date(2026, 10, 9), date(2026, 10, 9))):
            ninth = await chat_tools._get_customer_insights("today")
        assert (ninth["total_customers"], ninth["new_customers"],
                ninth["returning_customers"], ninth["repeat_rate"]) == (1, 0, 1, 100.0)
        with patch.object(chat_tools, "_get_date_range",
                          return_value=(date(2026, 10, 5), date(2026, 10, 9))):
            both = await chat_tools._get_customer_insights("week")
        assert both["total_customers"] == 2, "buyer 51 counted once per day"


def test_the_tools_keep_their_names_and_arguments():
    """A22 changes what two tools answer, not how the model calls them."""
    from core import chat_tools

    names = {t["name"] for t in chat_tools.TOOLS}
    assert {"get_revenue_summary", "get_customer_insights",
            "get_revenue_by_dates"} <= names
    insights = next(t for t in chat_tools.TOOLS if t["name"] == "get_customer_insights")
    assert set(insights["input_schema"]["properties"]) == {"period"}

