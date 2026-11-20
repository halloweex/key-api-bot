"""`all_time` and `granularity=monthly` through the routes, over Postgres.

The routes are called as a signed-in visitor, over Bronze seeded and Silver
and Gold derived the way production derives them (`tests/helpers/app_client.py`).
They replace tests that answered the store's SQL by matching its text and
read back the arguments a mock was called with: an alias renamed in the SQL
changed what that fake answered, and nothing here depends on how the
statements are written.
"""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import DSN, derive, needs_postgres, seed, tab_in_postgres

pytestmark = needs_postgres

KYIV = ZoneInfo("Europe/Kyiv")
SHOPIFY, NEW = 4, 1

# One big order on 1 January, ten small ones on the 2nd, three on 1 February.
DAYS = {date(2024, 1, 1): [1000.0], date(2024, 1, 2): [10.0] * 10,
        date(2024, 2, 1): [100.0] * 3}


async def _populate(conn, pool):
    orders, next_id = [], 1
    for day, totals in DAYS.items():
        for total in totals:
            ordered_at = datetime.combine(day, time(12), KYIV)
            orders.append((next_id, SHOPIFY, NEW, total, ordered_at, None, None, None))
            next_id += 1
    await seed(conn, orders=orders)
    await derive(pool)


@pytest_asyncio.fixture
async def shop(monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        yield pool


def _aov(body: dict) -> dict:
    trend = body["aovTrend"]
    return {"labels": trend["labels"], "data": trend["datasets"][0]["data"],
            "granularity": trend.get("granularity")}


class TestTheAovByMonth:
    @pytest.mark.asyncio
    async def test_a_months_aov_is_its_revenue_over_its_orders(self, shop):
        """January is ₴1 100 over 11 orders, ₴100, not 505, the mean of its
        two days' AOVs; by day the two days keep their own. Mutation: average
        the daily AOVs into the month, or drop `granularity` on its way from
        the route to the store."""
        async with signed_in("viewer") as visitor:
            monthly = await visitor.get("/api/customers/insights", params={
                "start_date": "2024-01-01", "end_date": "2024-03-31",
                "granularity": "monthly"})
            daily = await visitor.get("/api/customers/insights", params={
                "start_date": "2024-01-01", "end_date": "2024-01-03"})
        assert monthly.status_code == 200, monthly.text
        assert _aov(monthly.json()) == {"labels": ["01.2024", "02.2024", "03.2024"],
                                        "data": [100.0, 100.0, 0],
                                        "granularity": "monthly"}
        assert daily.status_code == 200, daily.text
        assert _aov(daily.json()) == {"labels": ["01.01", "02.01", "03.01"],
                                      "data": [1000.0, 10.0, 0], "granularity": None}


class TestTheMarketingReport:
    @staticmethod
    def _header(response) -> list:
        rows = list(csv.reader(io.StringIO(response.content.decode("utf-8-sig"))))
        return next(row for row in rows if row and row[0] == "Показник")

    @pytest.mark.asyncio
    async def test_all_time_reads_neither_comparison_and_a_month_reads_both(self, shop):
        """The whole history has nothing before it to compare with, and a
        year back is two thirds the same orders: the report and its CSV carry
        neither comparison. A month carries both. Mutation: let `all_time`
        ask for the comparison."""
        async with signed_in("viewer", tabs="marketing") as visitor:
            report = await visitor.get("/api/reports/marketing-summary",
                                       params={"period": "all_time"})
            whole = await visitor.get("/api/reports/marketing-summary/export/csv",
                                      params={"period": "all_time"})
            month = await visitor.get("/api/reports/marketing-summary/export/csv",
                                      params={"period": "month"})
        assert report.status_code == 200, report.text
        body = report.json()
        assert [body[k] for k in ("prev_start_date", "prev_end_date",
                                  "yoy_start_date", "yoy_end_date")] == [None] * 4
        assert body["general_sales"]["previous"] is None
        assert body["general_sales"]["year_ago"] is None
        assert body["general_sales"]["current"]["orders"] == 14
        assert whole.status_code == 200, whole.text
        assert len(self._header(whole)) == 2
        assert month.status_code == 200, month.text
        assert len(self._header(month)) == 6
