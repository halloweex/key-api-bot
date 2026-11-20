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


import pytest

from core.sql_dialect import CLICKHOUSE_ANALYTICS
from tests.helpers.cohort_fixture import (
    BODIES, EXPECTED, ORDERS, RETIRED_MONEY, SECOND_ON_A_RETIRED_SOURCE,
    SECOND_ON_A_RETIRED_SOURCE_EXPECTED, _ctes, _render, run_on_duckdb,
)


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
