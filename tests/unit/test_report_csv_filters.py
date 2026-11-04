"""The CSV summary's product lists are filtered as its summary is.

`GET /api/reports/export/csv?type=summary` writes a per-source summary that
honours the header's source, category and brand, and under each source the
products it sold. Those lists came from `get_report_products_by_source`, which
took none of the three, so the summary of one brand listed every brand's
products. The page sends the header's filters with the download
(`useDownloadCsv`), so that is what anybody exporting one brand's summary got.
Found by the Rust port of /reports (2026-10-28).

The lists now take the filters the top-products report takes. Held two ways:
the two methods' WHERE clauses are compared for every combination of the
filters, so the lists cannot drift from the report beside them, and the route
is held to asking for the lists exactly what it asks the summary.
"""
from __future__ import annotations

import itertools
import re
from datetime import date
from unittest.mock import AsyncMock

import pytest

from core.duckdb_store import DuckDBStore
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

WINDOW = (date(2026, 10, 1), date(2026, 10, 31))
FILTERS = {"source_id": 1, "category_id": 7, "brand": "BrandA"}
COMBINATIONS = [
    {k: FILTERS[k] for k in keys}
    for n in range(len(FILTERS) + 1)
    for keys in itertools.combinations(FILTERS, n)
]


def _capturing_store():
    """A store whose report queries are recorded instead of run."""
    store = DuckDBStore.__new__(DuckDBStore)
    calls: list[tuple[str, list]] = []

    async def run(sql, params=None):
        calls.append((sql, list(params or [])))
        return []

    store._reports_run = run
    store._category_ids = AsyncMock(return_value=[7, 8])  # a parent and its child
    return store, calls


def _where(sql: str) -> str:
    match = re.search(r"\bWHERE\b(.*?)\bGROUP BY\b", sql, re.S)
    assert match, sql
    return " ".join(match.group(1).split())


@pytest.mark.asyncio
@pytest.mark.parametrize("sales_type", ["retail", "all"])
@pytest.mark.parametrize("kwargs", COMBINATIONS, ids=lambda k: "+".join(k) or "none")
async def test_the_lists_are_filtered_as_the_top_products_are(sales_type, kwargs):
    store, calls = _capturing_store()
    await store.get_report_top_products(*WINDOW, sales_type, limit=5000, **kwargs)
    await store.get_report_products_by_source(*WINDOW, sales_type, **kwargs)
    (top_sql, top_params), (lists_sql, lists_params) = calls
    assert _where(lists_sql) == _where(top_sql)
    assert lists_params == top_params[:-1], "the same values, bar the top list's LIMIT"


def test_the_csv_summary_asks_its_lists_what_it_asks_the_summary(admin_client, monkeypatch):
    store = AsyncMock()
    store.get_report_summary.return_value = {
        "totals": {"orders_count": 3},
        "sources": [{"source_id": 1, "source_name": "Instagram", "orders_count": 3,
                     "avg_check": 100.0, "returns_count": 0, "return_rate": 0}],
    }
    store.get_report_products_by_source.return_value = {
        1: [{"product_name": "Serum", "quantity": 2}],
    }
    monkeypatch.setattr("web.routes.api.reports.get_store", AsyncMock(return_value=store))

    r = admin_client.get("/api/reports/export/csv", params={
        "type": "summary", "start_date": "2026-10-01", "end_date": "2026-10-31",
        "source_id": 1, "category_id": 7, "brand": "BrandA", "sales_type": "retail",
    })

    assert r.status_code == 200, r.text
    assert (store.get_report_products_by_source.await_args.args
            == store.get_report_summary.await_args.args)
    assert "Serum,2" in r.text
