"""The retention matrix ClickHouse answers is the one DuckDB answered.

The cohort queries are the one part of this dashboard ClickHouse is genuinely
for: order-level, read-only, and `silver.orders` is already shipped there
hourly. Nothing new has to be replicated for them to run — which is exactly
why it is worth checking that they *do* run and answer as before, rather than
assuming a store that already holds the rows will answer the same questions
about them.

ONE ENGINE SINCE STEP 14 (S14-07). The cohorts read ClickHouse alone
(`CustomersMixin._analytics_rows`). Every body is compared with a literal
(`tests/integration/golden/cohorts.json`): DuckDB's answer on this fixture,
recorded on the code before S14-07 — where it was recorded no ClickHouse was
available, so that run did not compare the two; this file asserted them equal
wherever one was, which is the gate (rule 9 of the step-14 plan). The at-risk
body counts days since each buyer's last order from today, so its literal
holds its shape only.

The ClickHouse tests are skipped without `KS_CH_URL`, the contract every
ClickHouse-touching path in this repository has: point it at a throwaway 24.8
server — production's version. The shape of the literal is checked
everywhere.

The fixture is built so a matrix that agreed by accident would not: two
cohorts, a customer who returns in a later month, one who never returns, a
return that must not count as a purchase, and a b2b order that must be
invisible to a retail cohort.
"""
from __future__ import annotations

import os

import pytest

from core.sql_dialect import CLICKHOUSE_ANALYTICS, cohort_retention_select
from tests.integration.pg_tab_seed import call_key, comparable, golden

needs_clickhouse = pytest.mark.skipif(
    not os.getenv("KS_CH_URL"), reason="needs a live ClickHouse at KS_CH_URL",
)

# (id, buyer, order_date, sales_type, is_return, grand_total)
ORDERS = [
    # cohort 2026-05: two buyers, one of them comes back twice
    (1, 1, "2026-05-04", "retail", False, 1200),
    (2, 1, "2026-06-11", "retail", False, 800),
    (3, 1, "2026-08-02", "retail", False, 1500),
    (4, 2, "2026-05-20", "retail", False, 600),
    # cohort 2026-06: one buyer who never returns
    (5, 3, "2026-06-07", "retail", False, 900),
    # a return in a later month must not count as a purchase
    (6, 2, "2026-07-01", "retail", True, 400),
    # b2b is a different population entirely
    (7, 4, "2026-05-09", "b2b", False, 5000),
]

GOLDEN = golden("cohorts")


async def _seed_clickhouse():
    from core.ch_common import execute

    await execute("CREATE DATABASE IF NOT EXISTS silver")
    await execute("DROP TABLE IF EXISTS silver.orders")
    await execute(
        "CREATE TABLE silver.orders ("
        " id Int64, buyer_id Nullable(Int64), order_date Date,"
        " is_return UInt8, sales_type String, grand_total Decimal(14,2),"
        " ordered_at DateTime64(3, 'UTC'), source_id Int32,"
        " is_active_source UInt8, manager_id Nullable(Int64)"
        ") ENGINE = MergeTree ORDER BY id"
    )
    values = ", ".join(
        f"({oid}, {buyer}, toDate('{day}'), {1 if ret else 0}, '{stype}',"
        f" {total}, toDateTime64('{day} 10:00:00', 3, 'UTC'), 1, 1, NULL)"
        for oid, buyer, day, stype, ret, total in ORDERS
    )
    await execute(f"INSERT INTO silver.orders VALUES {values}")


@needs_clickhouse
@pytest.mark.asyncio
@pytest.mark.parametrize("sales_type", ("retail", "all"))
async def test_clickhouse_returns_the_recorded_retention_matrix(sales_type):
    from core import ch_cohorts
    from core.sql_dialect import COHORT_RETENTION_TYPES

    await _seed_clickhouse()
    sales_filter = "" if sales_type == "all" else f"AND o.sales_type = '{sales_type}'"
    clickhouse = await ch_cohorts.fetch(
        cohort_retention_select(CLICKHOUSE_ANALYTICS, sales_type_filter=sales_filter,
                                months_back=24),
        [12], COHORT_RETENTION_TYPES,
    )
    assert comparable(clickhouse) == GOLDEN[call_key("cohort_retention_select", [sales_type])], (
        f"ClickHouse does not answer the {sales_type} retention matrix DuckDB did")


def test_the_literal_would_notice_an_accidental_agreement():
    """A matrix both engines got wrong the same way would still pass above, so
    this asserts the shape the data actually implies, on the literal itself."""
    rows = GOLDEN[call_key("cohort_retention_select", ["retail"])]
    matrix = {(r[0], r[2]): (r[1], r[3]) for r in rows}
    # May cohort holds two buyers; both are present in month 0.
    assert matrix[("2026-05", 0)] == (2, 2)
    # Only buyer 1 comes back in month 1, and again in month 3.
    assert matrix[("2026-05", 1)] == (2, 1)
    assert matrix[("2026-05", 3)] == (2, 1)
    # Buyer 2's July row is a return and must not appear as month 2.
    assert ("2026-05", 2) not in matrix
    # June cohort is buyer 3 alone, who never comes back.
    assert matrix[("2026-06", 0)] == (1, 1)
    assert ("2026-06", 1) not in matrix
    # The b2b buyer is not in a retail cohort at all.
    assert all(size <= 2 for size, _ in matrix.values())


# The other four bodies, each with the bound values it needs. The column types
# are NOT repeated here: they are imported from beside the body, so this test
# validates the list production actually uses.
OTHER_BODIES = (
    ("enhanced_cohort_retention_select", [12], "ENHANCED_RETENTION_TYPES"),
    ("days_to_second_purchase_select", [], "DAYS_TO_SECOND_TYPES"),
    ("cohort_ltv_select", [12], "COHORT_LTV_TYPES"),
    # Six bound values: the at-risk window and the churn threshold, reused
    # across the five aggregates in the projection.
    ("at_risk_customers_select", [90, 180, 90, 90, 90, 180], "AT_RISK_TYPES"),
)
# Counts days since a buyer's last order from today: its rows move with the
# calendar, so only their width and number are the literal's.
CLOCK_BOUND = {"at_risk_customers_select"}


@needs_clickhouse
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fn_name,params,types_name", OTHER_BODIES, ids=[b[0] for b in OTHER_BODIES],
)
async def test_the_other_four_bodies_answer_the_literal(fn_name, params, types_name):
    """The same body that ran on DuckDB parses and runs on ClickHouse, typed
    the way the tab reads it, and returns the rows DuckDB returned."""
    import core.sql_dialect as dialects
    from core import ch_cohorts

    await _seed_clickhouse()
    sql = getattr(dialects, fn_name)(
        CLICKHOUSE_ANALYTICS, sales_type_filter="AND o.sales_type = 'retail'",
        months_back=24)
    clickhouse = comparable(await ch_cohorts.fetch(
        sql, params, getattr(dialects, types_name)))
    recorded = GOLDEN[call_key(fn_name, ["retail"])]
    if fn_name in CLOCK_BOUND:
        assert len(clickhouse) == len(recorded)
        assert [len(r) for r in clickhouse] == [len(r) for r in recorded]
    else:
        assert clickhouse == recorded


def test_every_body_has_a_literal_of_its_types_width():
    """Each literal row is as wide as the type list beside its body: the
    repository once claimed seven columns where the enhanced matrix has eight
    and six where the at-risk projection has seven, with every test green."""
    import core.sql_dialect as dialects

    assert set(GOLDEN) == ({call_key("cohort_retention_select", [s]) for s in ("retail", "all")}
                           | {call_key(fn, ["retail"]) for fn, _p, _t in OTHER_BODIES})
    for fn_name, _params, types_name in OTHER_BODIES:
        width = len(getattr(dialects, types_name))
        assert all(len(row) == width for row in GOLDEN[call_key(fn_name, ["retail"])]), fn_name
