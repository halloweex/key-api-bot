"""The CSV summary's product lists honour the header's filters.

`GET /api/reports/export/csv?type=summary` writes a per-source summary and,
under each source, the products it sold. The lists once took no source,
category or brand, so the summary of one brand listed every brand's products
(found by the Rust port of /reports, 2026-10-28). Through the route, over
Silver derived from seeded Bronze.
"""
from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timezone

import pytest

from tests.helpers.app_client import signed_in
from tests.integration.pg_tab_seed import derive, needs_postgres, seed, tab_in_postgres

WINDOW = {"start_date": "2026-09-03", "end_date": "2026-09-03", "type": "summary"}
AT = datetime(2026, 9, 3, 9, tzinfo=timezone.utc)    # midday, Kyiv

CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Toner", 3, "BrandA", "SKU-B", 300.0),
    (300, "Lipstick", 3, "BrandB", "SKU-C", 100.0),
]
# Source 1 (Instagram) sold Serum and Lipstick, source 2 (Telegram) all three.
ORDERS = [(1, 1, 1, 1600.0, AT, 21, None, None), (2, 2, 1, 1400.0, AT, 22, None, None)]
LINES = [
    (1, 1, 100, "Serum", 2, 500.0), (2, 1, 300, "Lipstick", 3, 100.0),
    (3, 2, 100, "Serum", 1, 500.0), (4, 2, 200, "Toner", 5, 300.0),
    (5, 2, 300, "Lipstick", 4, 100.0),
]


async def _populate(conn, pool):
    await seed(conn, categories=CATEGORIES, products=PRODUCTS, orders=ORDERS, lines=LINES)
    await derive(pool)


def _lists(text: str) -> dict:
    """`{source: {product: quantity}}` out of the CSV."""
    out, source, reading = {}, None, False
    for row in csv.reader(io.StringIO(text.lstrip("﻿"))):
        if row[:1] and row[0].startswith("Source: "):
            source, reading = row[0][len("Source: "):], False
            out[source] = {}
        elif row == ["Product", "Quantity"]:
            reading = True
        elif not row:
            reading = False
        elif reading:
            out[source][row[0]] = int(row[1])
    return out


async def _export(**filters) -> dict:
    async with signed_in("viewer", tabs="reports") as viewer:
        r = await viewer.get("/api/reports/export/csv", params={**WINDOW, **filters})
    assert r.status_code == 200, r.text
    return _lists(r.text)


@needs_postgres
class TestTheListsFollowTheFilters:
    @pytest.mark.asyncio
    async def test_one_brand_lists_only_its_products(self, monkeypatch):
        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate):
            assert await _export(brand="BrandA") == {
                "Instagram": {"Serum": 2},
                "Telegram": {"Serum": 1, "Toner": 5},
            }

    @pytest.mark.asyncio
    async def test_one_source_lists_only_what_it_sold(self, monkeypatch):
        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate):
            assert await _export(source_id=1) == {
                "Instagram": {"Serum": 2, "Lipstick": 3}}

    @pytest.mark.asyncio
    async def test_a_category_takes_its_children(self, monkeypatch):
        """Care holds Serums; Makeup's products are not in it."""
        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate):
            assert await _export(category_id=1) == {
                "Instagram": {"Serum": 2}, "Telegram": {"Serum": 1}}

    @pytest.mark.asyncio
    async def test_without_filters_every_product_is_listed(self, monkeypatch):
        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _populate):
            assert await _export() == {
                "Instagram": {"Serum": 2, "Lipstick": 3},
                "Telegram": {"Serum": 1, "Toner": 5, "Lipstick": 4},
            }
