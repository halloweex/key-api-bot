"""A read whose other engine faults falls back — it does not raise NameError.

`CustomersMixin._analytics_rows` (ClickHouse → DuckDB) and
`ProductsIntelMixin._intel_run` (Postgres → DuckDB) both log the fault with
`logger.error` and then read DuckDB. Neither module defined `logger`, so the
line that was meant to record the fault raised instead, and the request failed
with a 500 at exactly the moment the fallback existed for. Nothing had ever
driven the except branch.
"""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest


class _Cursor:
    def fetchall(self):
        return [("duckdb",)]

    def fetchone(self):
        return ("duckdb",)


class _Conn:
    def execute(self, sql, params=None):
        return _Cursor()


class _Ctx:
    async def __aenter__(self):
        return _Conn()

    async def __aexit__(self, *a):
        return False


class _Store:
    def connection(self):
        return _Ctx()


@pytest.mark.asyncio
async def test_customer_analytics_fall_back_when_clickhouse_faults(caplog):
    from core.repositories.customers import CustomersMixin

    with patch("core.ch_cohorts.enabled", return_value=True), \
         patch("core.ch_cohorts.available", return_value=True), \
         patch("core.ch_cohorts.fetch",
               new=AsyncMock(side_effect=RuntimeError("clickhouse down"))), \
         caplog.at_level(logging.ERROR):
        rows = await CustomersMixin._analytics_rows(
            _Store(), lambda dialect: "SELECT 1", [], [],
        )

    assert rows == [("duckdb",)]
    assert "falling back to DuckDB" in caplog.text


@pytest.mark.asyncio
async def test_product_intel_falls_back_when_postgres_faults(caplog):
    from core.repositories.products_intel import ProductsIntelMixin

    with patch("core.pg_products_intel_read.enabled", return_value=True), \
         patch("core.pg_products_intel_read.available", return_value=True), \
         patch("core.pg_products_intel_read.fetch",
               new=AsyncMock(side_effect=RuntimeError("postgres down"))), \
         caplog.at_level(logging.ERROR):
        rows = await ProductsIntelMixin._intel_run(_Store(), "SELECT 1")

    assert rows == [("duckdb",)]
    assert "falling back to DuckDB" in caplog.text
