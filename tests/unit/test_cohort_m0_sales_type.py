"""A retail cohort's first-month revenue counts its retail orders only (L2-04).

`enhanced_cohort_retention_select` filters the sales type where it picks each
buyer's cohort month and where it reads later orders, but its
`customer_cohorts` CTE summed the first month's revenue — the M0 denominator
of every revenue retention — over every order of that month. A buyer with a
retail ₴1 000 and a b2b ₴500 in September and a retail ₴1 000 in October read
M0 78.3 % and M1 43.5 % of a retail cohort; they are 100 % and 55.6 %.

Run here on DuckDB in memory (in requirements-dev for the host tools): the
body's constructs are the ones verified to agree on DuckDB 1.5.5 and
ClickHouse 24.8, and production renders the same text for ClickHouse.
"""
from __future__ import annotations

import re

import pytest

from core.sql_dialect import (
    CLICKHOUSE_ANALYTICS, AnalyticsDialect, enhanced_cohort_retention_select,
)

RETAIL = "AND o.sales_type = 'retail'"


def _ctes(sql: str) -> dict:
    """CTE name → body, for the CTEs of the WITH clause."""
    out = {}
    for m in re.finditer(r"(\w+) AS \(", sql):
        depth, i = 1, m.end()
        while depth:
            depth += {"(": 1, ")": -1}.get(sql[i], 0)
            i += 1
        out[m.group(1)] = sql[m.end():i - 1]
    return out


class TestTheRenderedText:
    def test_every_cte_reading_orders_carries_the_filter(self):
        """Mutation: drop `{sales_type_filter}` from customer_cohorts' ON."""
        sql = enhanced_cohort_retention_select(
            CLICKHOUSE_ANALYTICS, sales_type_filter=RETAIL, months_back=12)
        reading = {name: body for name, body in _ctes(sql).items()
                   if CLICKHOUSE_ANALYTICS.silver_orders in body}
        assert set(reading) == {"customer_first_order", "customer_cohorts", "customer_orders"}
        for name, body in reading.items():
            assert RETAIL in body, f"{name} reads orders past the sales type"

    def test_all_renders_no_filter(self):
        sql = enhanced_cohort_retention_select(
            CLICKHOUSE_ANALYTICS, sales_type_filter="", months_back=12)
        assert "sales_type" not in sql


duckdb = pytest.importorskip("duckdb")

DUCK = AnalyticsDialect(name="duckdb", silver_orders="silver_orders",
                        today="DATE '2026-11-05'")


def _run(rows, sales_type_filter):
    con = duckdb.connect(":memory:")
    try:
        # `is_active_source` since the owner's decision O5: every row here
        # is on an active source (tests/unit/test_cohort_active_sources.py
        # holds the retired ones).
        con.execute("CREATE TABLE silver_orders (id INTEGER, buyer_id INTEGER, "
                    "order_date DATE, is_return BOOLEAN, sales_type VARCHAR, "
                    "grand_total DECIMAL(14,2), is_active_source BOOLEAN DEFAULT TRUE)")
        con.executemany("INSERT INTO silver_orders (id, buyer_id, order_date, is_return,"
                        " sales_type, grand_total) VALUES (?, ?, ?, ?, ?, ?)", rows)
        sql = enhanced_cohort_retention_select(
            DUCK, sales_type_filter=sales_type_filter, months_back=12)
        return [tuple(float(v) if hasattr(v, "is_finite") else v for v in r)
                for r in con.execute(sql, [6]).fetchall()]
    finally:
        con.close()


class TestTheAnswer:
    ROWS = [
        (1, 7, "2026-09-03", False, "retail", 1000),
        (2, 7, "2026-09-20", False, "b2b", 500),
        (3, 7, "2026-10-05", False, "retail", 1000),
        (4, 8, "2026-09-10", False, "retail", 800),
    ]

    def test_m0_is_the_retail_first_month(self):
        rows = _run(self.ROWS, RETAIL)
        assert rows == [
            ("2026-09", 2, 1800.0, 0, 2, 100.0, 1800.0, 100.0),
            ("2026-09", 2, 1800.0, 1, 1, 50.0, 1000.0, 55.6),
        ]

    def test_all_still_counts_every_type(self):
        rows = _run(self.ROWS, "")
        assert rows[0] == ("2026-09", 2, 2300.0, 0, 2, 100.0, 2300.0, 100.0)
