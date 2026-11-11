"""Cohorts read retired sources the way the rest of the dashboard does (O5).

The five cohort bodies (`core/sql_dialect.py`) counted Opencart's orders —
source 3, retired, outside `REVENUE_SOURCE_IDS` — as revenue, retained
customers, order counts and recency, while Gold, the SMS audience and
customer insights exclude them. The owner's decision O5 (2026-11-11), option
(a): a buyer's cohort — membership and month — still reads every order, as
Silver's first-order rule reads a buyer's first order (a purchase on a
retired channel is still a purchase); everything measured inside the cohort
reads active sources only.

The case that tells the two apart: a buyer whose first order was on Opencart
and who later bought on Instagram. Assigned to the Opencart order's month,
never acquired a second time, and the Opencart money in no cell.

Executed here on DuckDB in memory (in requirements-dev for the host tools):
the bodies' constructs are the ones verified to agree on DuckDB 1.5.5 and
ClickHouse 24.8, and production renders the same text for ClickHouse, where
`tests/integration/test_cohorts_active_sources.py` runs this fixture again.
"""
from __future__ import annotations

import re

import pytest

import core.sql_dialect as dialects
from core.sql_dialect import CLICKHOUSE_ANALYTICS, AnalyticsDialect

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


def _code(body: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in body.splitlines())


# Where each body decides who is in a cohort: these read every order.
ASSIGNMENT = {
    "cohort_retention_select": {"customer_cohorts"},
    "enhanced_cohort_retention_select": {"customer_first_order"},
    "days_to_second_purchase_select": set(),
    "cohort_ltv_select": {"customer_cohorts"},
    "at_risk_customers_select": set(),
}


class TestTheRenderedText:
    @pytest.mark.parametrize("name", sorted(BODIES))
    def test_every_cte_measuring_orders_reads_active_sources_only(self, name):
        """Every CTE that reads the orders table, but the one that assigns the
        cohort, names `is_active_source`; the assignment does not. Mutation:
        drop the predicate from any measuring CTE, or add it to an
        assignment."""
        sql = _render(name)
        reading = {cte: _code(body) for cte, body in _ctes(sql).items()
                   if CLICKHOUSE_ANALYTICS.silver_orders in body}
        assert reading, name
        for cte, body in reading.items():
            if cte in ASSIGNMENT[name]:
                assert "is_active_source" not in body, f"{name}.{cte} assigns from active sources only"
            else:
                assert "is_active_source" in body, f"{name}.{cte} measures retired sources"

    def test_the_at_risk_cohort_month_reads_every_order(self):
        """The at-risk body assigns and measures in one CTE: its MIN is bare,
        each measure is conditional."""
        body = _code(_ctes(_render("at_risk_customers_select"))["customer_activity"])
        select = body.split("SELECT", 1)[1].split("FROM", 1)[0]
        items, depth, start = {}, 0, 0
        for i, ch in enumerate(select + ","):
            depth += {"(": 1, ")": -1}.get(ch, 0)
            if ch == "," and depth == 0:
                expr, _, alias = select[start:i].strip().rpartition(" AS ")
                items[alias.strip()] = expr
                start = i + 1
        assert set(items) == {"o.buyer_id", "cohort_month", "last_order_date",
                              "days_since_last", "total_orders", "total_revenue"}
        assert "is_active_source" not in items["cohort_month"]
        for measure in ("last_order_date", "days_since_last", "total_orders", "total_revenue"):
            assert "is_active_source" in items[measure], measure


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


class TestTheAnswer:
    @pytest.mark.parametrize("name", sorted(BODIES))
    def test_each_body_answers_without_the_retired_source(self, name):
        """Mutation: drop `AND o.is_active_source` from the body's measuring
        CTE — the Opencart ₴5 000 and ₴300 come back into its cells, or
        Opencart's buyers into its counts."""
        assert run_on_duckdb(name) == EXPECTED[name]

    @pytest.mark.parametrize("name", sorted(BODIES))
    def test_no_cell_carries_retired_money(self, name):
        for row in run_on_duckdb(name):
            assert not set(row) & set(RETIRED_MONEY), (name, row)

    def test_the_second_purchase_is_the_next_on_an_active_source(self):
        """Mutation: join the second purchase on `order_num = 2` again, or
        drop `AND c2.is_active_source` — the Opencart order ten days on is
        then the second purchase, in the 0-30 bucket."""
        assert run_on_duckdb("days_to_second_purchase_select",
                             SECOND_ON_A_RETIRED_SOURCE) == SECOND_ON_A_RETIRED_SOURCE_EXPECTED

    def test_the_opencart_buyer_is_never_acquired_again(self):
        """Their Instagram orders in May and June open no cohort of their own:
        the buyer belongs to March, the month of their first order."""
        for name in ("cohort_retention_select", "enhanced_cohort_retention_select",
                     "cohort_ltv_select", "at_risk_customers_select"):
            assert {row[0] for row in run_on_duckdb(name)} == {"2026-03"}, name

    def test_with_every_source_active_the_answer_is_what_it_was(self):
        """The fixture with Opencart's two orders on an active source instead:
        the bodies then answer what they answered before O5, the retired
        money back in every cell it reached."""
        everything = [(i, b, d, src, True, total) for i, b, d, src, _a, total in ORDERS]
        assert run_on_duckdb("cohort_retention_select", everything)[0] == \
            ("2026-03", 3, 0, 3, 100.0)
        assert run_on_duckdb("enhanced_cohort_retention_select", everything)[0][2] == 5700.0
        assert run_on_duckdb("at_risk_customers_select", everything) == [
            ("2026-03", 3, 1, 100.0, 7400.0, 1.7, 2)]
