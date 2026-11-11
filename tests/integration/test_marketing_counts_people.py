"""The marketing report counts people, as the dashboard does.

Until 2026-11-11 its «Кол-во клиентов» summed a distinct count per day — Gold's
grain — so a buyer who came on two days was two customers: within a few of the
orders on a month, and 42 880 against 19 425 people over the whole history.
The owner chose people. These hold that, with the brand filter on and off, and
hold the report to the dashboard's own answer for the same window.

`test_marketing_two_engines.py` cannot say it: every buyer there buys on one
day, so buyer-days and people agree on its fixture, and its literals are
DuckDB's answers, not to be edited (rule 9 of the step-14 plan).

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest
import pytest_asyncio

from core.duckdb_constants import B2B_MANAGER_ID
from tests.integration.pg_tab_seed import (
    derive, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

WINDOW = (date(2026, 9, 1), date(2026, 9, 15))
PREVIOUS = (date(2026, 8, 17), date(2026, 8, 31))   # the same length before

CATEGORIES = [(1, "Care", None)]
PRODUCTS = [
    (100, "Serum", 1, "BrandA", "SKU-A", 500.0),
    (200, "Toner", 1, "BrandB", "SKU-B", 300.0),
]


def _at(day: date) -> datetime:
    """Midday Kyiv, so the Kyiv date is the day named."""
    return datetime(day.year, day.month, day.day, 9, 0, tzinfo=timezone.utc)


# (order_id, source_id, total, day, returned, manager_id, buyer_id, product)
ORDERS = [
    # Buyer 21 first bought before both windows, then twice in the previous
    # one and on two days of this one: returning, one person in each.
    (1, 1, 500.0, date(2026, 8, 10), False, None, 21, 100),
    (2, 1, 500.0, date(2026, 8, 20), False, None, 21, 100),
    (3, 1, 500.0, date(2026, 8, 25), False, None, 21, 100),
    (4, 1, 500.0, date(2026, 9, 3), False, None, 21, 100),
    (5, 2, 500.0, date(2026, 9, 9), False, None, 21, 100),
    # Buyer 22 is new on 2 September and back on the 8th: new and
    # returning both, as on the dashboard.
    (6, 4, 300.0, date(2026, 9, 2), False, None, 22, 200),
    (7, 4, 300.0, date(2026, 9, 8), False, None, 22, 200),
    # Buyer 23, new, once.
    (8, 1, 500.0, date(2026, 9, 5), False, None, 23, 100),
    # Buyer 24, new, on two channels in one day: once.
    (9, 1, 300.0, date(2026, 9, 5), False, None, 24, 200),
    (10, 2, 300.0, date(2026, 9, 5), False, None, 24, 200),
    # Neither counts in retail: a return, and a wholesale order.
    (11, 1, 500.0, date(2026, 9, 6), True, None, 25, 100),
    (12, 1, 500.0, date(2026, 9, 7), False, B2B_MANAGER_ID, 26, 100),
]


async def _populate(conn, pool):
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS,
        orders=[(oid, src, 19 if returned else 1, total, _at(day),
                 buyer, manager, None)
                for oid, src, total, day, returned, manager, buyer, _ in ORDERS],
        lines=[(oid, oid, product, "item", 1, total)
               for oid, _, total, _, _, _, _, product in ORDERS])
    await derive(pool)


@pytest_asyncio.fixture
async def store(tmp_path, monkeypatch):
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the report")


def _people(general: dict) -> tuple:
    return (general["customers"], general["new_customers"],
            general["returning_customers"])


@pytest.mark.asyncio
async def test_a_buyer_on_two_days_is_one_customer(store):
    """Buyers 21, 22, 23 and 24: four people on six buyer-days."""
    report = await store.get_marketing_report_by_dates(*WINDOW)
    current = report["general_sales"]["current"]
    assert _people(current) == (4, 3, 2), current
    assert current["orders"] == 7
    assert current["return_rate"] == 50.0


@pytest.mark.asyncio
async def test_the_comparison_window_counts_people_too(store):
    """Buyer 21 twice in the previous window is one customer there."""
    report = await store.get_marketing_report_by_dates(*WINDOW)
    previous = report["general_sales"]["previous"]
    assert (report["prev_start_date"], report["prev_end_date"]) == tuple(
        d.isoformat() for d in PREVIOUS)
    assert _people(previous) == (1, 0, 1), previous
    assert previous["orders"] == 2


@pytest.mark.asyncio
async def test_a_brand_counts_the_people_who_bought_it(store):
    """BrandA: buyer 21 on two days and buyer 23 — two people, not three."""
    report = await store.get_marketing_report_by_dates(*WINDOW, brand="BrandA")
    current = report["general_sales"]["current"]
    assert _people(current) == (2, 1, 1), current
    assert current["orders"] == 3
    assert current["revenue"] == 1500.0


@pytest.mark.asyncio
@pytest.mark.parametrize("sales_type", ["retail", "all", "b2b"])
async def test_the_report_and_the_dashboard_count_alike(store, sales_type):
    """One window, two screens, one answer."""
    report = await store.get_marketing_report_by_dates(
        *WINDOW, sales_type=sales_type)
    insights = await store.get_customer_insights(*WINDOW, sales_type=sales_type)
    metrics = insights["metrics"]
    assert _people(report["general_sales"]["current"]) == (
        metrics["totalCustomers"], metrics["newCustomers"],
        metrics["returningCustomers"])
