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


async def _seed_duckdb(store):
    async with store.connection() as conn:
        for oid, buyer, day, stype, ret, total in ORDERS:
            conn.execute(
                "INSERT INTO silver_orders (id, source_id, status_id,"
                " grand_total, ordered_at, buyer_id, manager_id, order_date,"
                " is_return, sales_type, is_active_source, source_name,"
                " is_new_customer, buyer_first_order_date, promocode)"
                " VALUES (?,1,1,?,?,?,NULL,?,?,?,TRUE,'src1',FALSE,?,NULL)",
                [oid, total, f"{day} 10:00:00+00", buyer, day, ret, stype, day])


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


# The other four bodies. Each is (render function, bound params, column types),
# and they run through the same fixture as the retention matrix above.
OTHER_BODIES = (
    ("enhanced_cohort_retention_select", [12],
     ("TEXT", "INT", "FLOAT", "INT", "INT", "FLOAT", "FLOAT", "FLOAT")),
    ("days_to_second_purchase_select", [],
     ("TEXT", "INT", "FLOAT", "FLOAT", "FLOAT", "INT")),
    ("cohort_ltv_select", [12], ("TEXT", "INT", "INT", "FLOAT", "FLOAT")),
    # Six bound values: the at-risk window and the churn threshold, reused
    # across the five aggregates in the projection.
    ("at_risk_customers_select", [90, 180, 90, 90, 90, 180],
     ("TEXT", "INT", "INT", "FLOAT", "FLOAT", "FLOAT", "INT")),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fn_name,params,_types", OTHER_BODIES, ids=[b[0] for b in OTHER_BODIES],
)
async def test_the_other_four_bodies_run_on_both_engines(
    tmp_path, fn_name, params, _types,
):
    """Executed on both, compared as text rather than typed.

    The retention matrix above is compared through the typed reader, which is
    what the tab actually uses. These four are checked one level lower: that
    the *same body* parses and runs on ClickHouse at all, and returns the same
    rows. Their typed readers land with their own routing; what would break
    silently before then is a construct ClickHouse cannot parse, and that is
    what this catches.
    """
    import core.sql_dialect as dialects
    from core import ch_cohorts
    from core.ch_common import execute

    store = DuckDBStore(db_path=tmp_path / f"{fn_name}.duckdb")
    await store.connect()
    try:
        await _seed_duckdb(store)
        await _seed_clickhouse()

        render = getattr(dialects, fn_name)
        kw = dict(sales_type_filter="AND o.sales_type = 'retail'", months_back=24)

        duck_sql = render(dialects.DUCKDB_ANALYTICS, **kw)
        ch_sql = render(dialects.CLICKHOUSE_ANALYTICS, **kw)
        for value in params:
            duck_sql_bound = duck_sql
            ch_sql = ch_sql.replace("?", str(int(value)), 1)
        duck_sql_bound = duck_sql

        async with store.connection() as conn:
            duck = [tuple(r) for r in conn.execute(duck_sql_bound, params).fetchall()]

        raw = await execute(ch_sql + "\nFORMAT TabSeparated")
        rows = [line.split("\t") for line in raw.splitlines() if line]
        clickhouse = [ch_cohorts._typed(r, _types) for r in rows]

        assert len(duck) == len(clickhouse), (
            f"{fn_name}: {len(duck)} rows from DuckDB, {len(clickhouse)} from "
            f"ClickHouse"
        )
        # Typed, not textual. The two engines render the same number
        # differently — DuckDB's DECIMAL prints `900.00` where ClickHouse
        # prints `900` — which is exactly what the typed reader exists to undo,
        # and comparing the text would fail on a difference that is not one.
        for i, (a, b) in enumerate(zip(duck, clickhouse)):
            assert len(a) == len(b), f"{fn_name} row {i}: different widths"
            for j, (x, y) in enumerate(zip(a, b)):
                if isinstance(x, (int, float)) or hasattr(x, "as_tuple"):
                    assert float(x) == pytest.approx(float(y), abs=1e-6), (
                        f"{fn_name} row {i} col {j}: {x} vs {y}"
                    )
                else:
                    assert str(x) == str(y), f"{fn_name} row {i} col {j}"
    finally:
        await store.close()
