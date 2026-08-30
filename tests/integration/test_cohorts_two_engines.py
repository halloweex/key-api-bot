"""The retention matrix means the same thing in DuckDB and ClickHouse.

The cohort queries are the one part of this dashboard ClickHouse is genuinely
for: order-level, read-only, and `silver.orders` is already shipped there
hourly. Nothing new has to be replicated for them to run — which is exactly
why it is worth checking that they *do* run and agree, rather than assuming a
store that already holds the rows will answer the same questions about them.

Skipped without `KS_CH_URL`, the contract every ClickHouse-touching path in
this repository has. Point it at a throwaway 24.8 server — production's
version — and the same synthetic customers go into both engines.

The fixture is built so a matrix that agreed by accident would not: two
cohorts, a customer who returns in a later month, one who never returns, a
return that must not count as a purchase, and a b2b order that must be
invisible to a retail cohort.
"""
from __future__ import annotations

import os
from datetime import date

import pytest

from core.duckdb_store import DuckDBStore
from core.sql_dialect import (
    CLICKHOUSE_ANALYTICS, DUCKDB_ANALYTICS, cohort_retention_select,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("KS_CH_URL"), reason="needs a live ClickHouse at KS_CH_URL",
)

# (id, buyer, order_date, sales_type, is_return)
ORDERS = [
    # cohort 2026-05: two buyers, one of them comes back twice
    (1, 1, "2026-05-04", "retail", False),
    (2, 1, "2026-06-11", "retail", False),
    (3, 1, "2026-08-02", "retail", False),
    (4, 2, "2026-05-20", "retail", False),
    # cohort 2026-06: one buyer who never returns
    (5, 3, "2026-06-07", "retail", False),
    # a return in a later month must not count as a purchase
    (6, 2, "2026-07-01", "retail", True),
    # b2b is a different population entirely
    (7, 4, "2026-05-09", "b2b", False),
]


async def _seed_duckdb(store):
    async with store.connection() as conn:
        for oid, buyer, day, stype, ret in ORDERS:
            conn.execute(
                "INSERT INTO silver_orders (id, source_id, status_id,"
                " grand_total, ordered_at, buyer_id, manager_id, order_date,"
                " is_return, sales_type, is_active_source, source_name,"
                " is_new_customer, buyer_first_order_date, promocode)"
                " VALUES (?,1,1,1000,?,?,NULL,?,?,?,TRUE,'src1',FALSE,?,NULL)",
                [oid, f"{day} 10:00:00+00", buyer, day, ret, stype, day])


async def _seed_clickhouse():
    from core.ch_common import execute

    await execute("CREATE DATABASE IF NOT EXISTS silver")
    await execute("DROP TABLE IF EXISTS silver.orders")
    await execute(
        "CREATE TABLE silver.orders ("
        " id Int64, buyer_id Nullable(Int64), order_date Date,"
        " is_return UInt8, sales_type String"
        ") ENGINE = MergeTree ORDER BY id"
    )
    values = ", ".join(
        f"({oid}, {buyer}, toDate('{day}'), {1 if ret else 0}, '{stype}')"
        for oid, buyer, day, stype, ret in ORDERS
    )
    await execute(f"INSERT INTO silver.orders VALUES {values}")


@pytest.fixture
def retail_filter():
    return "AND o.sales_type = 'retail'"


@pytest.mark.asyncio
@pytest.mark.parametrize("sales_type", ("retail", "all"))
async def test_both_engines_return_the_same_retention_matrix(tmp_path, sales_type):
    from core import ch_cohorts

    store = DuckDBStore(db_path=tmp_path / "cohorts.duckdb")
    await store.connect()
    try:
        await _seed_duckdb(store)
        await _seed_clickhouse()

        sales_filter = "" if sales_type == "all" else f"AND o.sales_type = '{sales_type}'"
        kw = dict(sales_type_filter=sales_filter, months_back=24)

        async with store.connection() as conn:
            duck = conn.execute(
                cohort_retention_select(DUCKDB_ANALYTICS, **kw), [12],
            ).fetchall()
        duck = [tuple(r) for r in duck]

        clickhouse = await ch_cohorts.fetch(
            cohort_retention_select(CLICKHOUSE_ANALYTICS, **kw),
            [12], ch_cohorts.RETENTION_TYPES,
        )

        assert duck == clickhouse, (
            f"the two engines disagree about the {sales_type} retention matrix"
        )
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_the_fixture_would_notice_an_accidental_agreement(tmp_path):
    """A matrix both engines got wrong the same way would still pass above, so
    this asserts the shape the data actually implies."""
    store = DuckDBStore(db_path=tmp_path / "shape.duckdb")
    await store.connect()
    try:
        await _seed_duckdb(store)
        async with store.connection() as conn:
            rows = conn.execute(
                cohort_retention_select(
                    DUCKDB_ANALYTICS,
                    sales_type_filter="AND o.sales_type = 'retail'",
                    months_back=24,
                ), [12],
            ).fetchall()

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
    finally:
        await store.close()
