"""The marketing report's brand table lists brands that earned something.

The brand card's statement has no HAVING, so a brand whose every line in the
window was free (samples, gifts) comes back with zero revenue; the report
drops such a brand rather than showing a row of zeros and a 0% share. Through
the route, over Silver derived from seeded Bronze.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pytest

from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import derive, needs_postgres, seed, tab_in_postgres

DAY = date(2026, 9, 3)
PRODUCTS = [
    (100, "Serum", 1, "BrandPaid", "SKU-A", 500.0),
    (200, "Sample", 1, "BrandFree", "SKU-B", 0.0),
]


async def _populate(conn, pool):
    at = datetime(DAY.year, DAY.month, DAY.day, 9, tzinfo=timezone.utc)   # midday, Kyiv
    await seed(
        conn, categories=[(1, "Care", None)], products=PRODUCTS,
        orders=[(1, 1, 1, 500.0, at, 21, None, None),
                (2, 1, 1, 0.0, at, 22, None, None)],
        lines=[(1, 1, 100, "Serum", 1, 500.0), (2, 2, 200, "Sample", 1, 0.0)])
    await derive(pool)


@needs_postgres
@pytest.mark.asyncio
async def test_a_brand_that_sold_nothing_is_not_a_row(monkeypatch):
    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate):
        async with signed_in("viewer", tabs="marketing") as viewer:
            r = await viewer.get("/api/reports/marketing-summary", params={
                "start_date": DAY.isoformat(), "end_date": DAY.isoformat()})
    assert r.status_code == 200, r.text
    assert r.json()["brands"] == [
        {"brand": "BrandPaid", "revenue": 500.0, "orders": 1,
         "avg_check": 500.0, "share_pct": 100.0}]
