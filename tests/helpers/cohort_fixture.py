"""The cohorts' fixture: orders on active and retired sources, and the
retention each sales type should read off them.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

import core.sql_dialect as dialects
from core.sql_dialect import AnalyticsDialect, CLICKHOUSE_ANALYTICS


RETAIL = "AND o.sales_type = 'retail'"


# (id, buyer, order_date, source_id, is_active_source, grand_total)
ORDERS = [
    # Buyer 11: first order on Opencart, then twice on Instagram.
    (101, 11, "2026-03-05", 3, False, 5000),
    (102, 11, "2026-05-20", 1, True, 1000),
    (103, 11, "2026-06-15", 1, True, 700),
    # Buyer 12: one Instagram order in the same month — the control.
    (104, 12, "2026-03-10", 1, True, 400),
    # Buyer 13: Opencart only.
    (105, 13, "2026-03-20", 3, False, 300),
]


RETIRED_MONEY = (5000, 300, 5300, 5700, 6700, 7400)


# A second purchase on a retired source, for the one body ORDERS cannot tell
# apart: buyer 21 bought on Instagram, ten days later on Opencart, and thirty
# days after that on Instagram again. Their second purchase is the second
# Instagram order, 40 days after the first; the Opencart one, 10 days after,
# was what the body read until O5.
SECOND_ON_A_RETIRED_SOURCE = [
    (201, 21, "2026-03-01", 1, True, 600),
    (202, 21, "2026-03-11", 3, False, 250),
    (203, 21, "2026-04-10", 1, True, 350),
]


SECOND_ON_A_RETIRED_SOURCE_EXPECTED = [("31-60", 1, 40.0, 40.0, 40.0, 1)]


# (body, bound values) — the values the tab binds.
BODIES = {
    "cohort_retention_select": [12],
    "enhanced_cohort_retention_select": [12],
    "days_to_second_purchase_select": [],
    "cohort_ltv_select": [12],
    "at_risk_customers_select": [90, 180, 90, 90, 90, 180],
}


# What each body answers on ORDERS, with today 2026-11-05.
EXPECTED = {
    # (cohort, size, month, retained, pct): all three buyers are in March's
    # cohort, and only buyer 12 was active on an active source in March.
    "cohort_retention_select": [
        ("2026-03", 3, 0, 1, 33.3),
        ("2026-03", 3, 2, 1, 33.3),
        ("2026-03", 3, 3, 1, 33.3),
    ],
    # (cohort, size, m0_revenue, month, retained, pct, revenue, revenue_pct)
    "enhanced_cohort_retention_select": [
        ("2026-03", 3, 400.0, 0, 1, 33.3, 400.0, 100.0),
        ("2026-03", 3, 400.0, 2, 1, 33.3, 1000.0, 250.0),
        ("2026-03", 3, 400.0, 3, 1, 33.3, 700.0, 175.0),
    ],
    # (bucket, customers, avg, median, avg overall, total): buyer 11's first
    # purchase is the Opencart one, the second their first on Instagram.
    "days_to_second_purchase_select": [
        ("61-90", 1, 76.0, 76.0, 76.0, 1),
    ],
    # (cohort, size, month, revenue, active)
    "cohort_ltv_select": [
        ("2026-03", 3, 0, 400.0, 1),
        ("2026-03", 3, 2, 1000.0, 1),
        ("2026-03", 3, 3, 700.0, 1),
    ],
    # (cohort, customers, at risk, pct past 90 days, revenue past 90 days,
    # orders past 90 days, churned): buyer 11 last bought 143 days ago, buyer
    # 12 240 days ago, buyer 13 never on an active source.
    "at_risk_customers_select": [
        ("2026-03", 3, 1, 66.7, 2100.0, 1.5, 1),
    ],
}


TODAY = "DATE '2026-11-05'"


def _render(name, dialect=CLICKHOUSE_ANALYTICS):
    return getattr(dialects, name)(dialect, sales_type_filter=RETAIL, months_back=24)


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


duckdb = pytest.importorskip("duckdb")


DUCK = AnalyticsDialect(name="duckdb", silver_orders="silver_orders", today=TODAY)


def _plain(value):
    if hasattr(value, "is_finite"):         # Decimal
        return float(value)
    if isinstance(value, float):
        return round(value, 1)
    return value


def run_on_duckdb(name, orders=ORDERS):
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE silver_orders (id INTEGER, buyer_id INTEGER, "
                    "order_date DATE, is_return BOOLEAN, sales_type VARCHAR, "
                    "grand_total DECIMAL(14,2), source_id INTEGER, "
                    "is_active_source BOOLEAN)")
        con.executemany(
            "INSERT INTO silver_orders VALUES (?, ?, ?, FALSE, 'retail', ?, ?, ?)",
            [(i, b, d, total, src, active) for i, b, d, src, active, total in orders])
        rows = con.execute(_render(name, DUCK), BODIES[name]).fetchall()
        return [tuple(_plain(v) for v in r) for r in rows]
    finally:
        con.close()


TYPES = {
    "cohort_retention_select": "COHORT_RETENTION_TYPES",
    "enhanced_cohort_retention_select": "ENHANCED_RETENTION_TYPES",
    "days_to_second_purchase_select": "DAYS_TO_SECOND_TYPES",
    "cohort_ltv_select": "COHORT_LTV_TYPES",
    "at_risk_customers_select": "AT_RISK_TYPES",
}


async def _seed(orders=ORDERS):
    """`silver.orders` by the shipper's own DDL, so the column types are
    production's (`is_active_source Bool`, `LowCardinality` sales type)."""
    from core.ch_common import execute
    from core.ch_silver import _SILVER_DDL

    await execute("CREATE DATABASE IF NOT EXISTS silver")
    await execute("DROP TABLE IF EXISTS silver.orders")
    await execute(_SILVER_DDL.format(name="silver.orders"))
    values = ", ".join(
        f"({oid}, {buyer}, toDate('{day}'), false, 'retail', {total}, {source},"
        f" {'true' if active else 'false'})"
        for oid, buyer, day, source, active, total in orders)
    await execute("INSERT INTO silver.orders (id, buyer_id, order_date, is_return,"
                  " sales_type, grand_total, source_id, is_active_source)"
                  f" VALUES {values}")


async def _ask(name):
    from core import ch_cohorts

    sql = getattr(dialects, name)(CLICKHOUSE_ANALYTICS, sales_type_filter=RETAIL,
                                  months_back=24)
    rows = await ch_cohorts.fetch(sql, BODIES[name], getattr(dialects, TYPES[name]))
    return [tuple(round(v, 1) if isinstance(v, float) else v for v in r) for r in rows]


def _at_risk_today():
    """The at-risk row for the server's today, which is Kyiv's: buyer 11 last
    bought on an active source on 15 June (₴1 700 over two orders), buyer 12
    on 10 March (₴400, one order), buyer 13 never."""
    today = datetime.now(ZoneInfo("Europe/Kyiv")).date()
    last = {11: (date(2026, 6, 15), 1700.0, 2), 12: (date(2026, 3, 10), 400.0, 1)}
    days = {b: (today - d).days for b, (d, _r, _o) in last.items()}
    past_90 = [b for b, d in days.items() if d > 90]
    return [(
        "2026-03", 3,
        sum(1 for d in days.values() if 90 < d <= 180),
        round(100.0 * len(past_90) / 3, 1),
        sum(last[b][1] for b in past_90) if past_90 else None,
        round(sum(last[b][2] for b in past_90) / len(past_90), 1) if past_90 else None,
        sum(1 for d in days.values() if d > 180),
    )]
