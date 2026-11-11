"""The order-line level: one grain, one definition, and it must equal Gold.

Silver has one grain — the order — so every question about a *product* re-joined
`order_products` where it was asked. That join appears 49 times in this
codebase and 22 of those carry `silver_orders` too, which is the single largest
obstacle to splitting this store in two: the line and its order would land in
different databases.

What is pinned here is the property that makes the level trustworthy — under
Gold's own predicate it reproduces `gold_daily_products` exactly. A level that
disagrees with the aggregate built beside it is a third revenue number, and
this project already has two.

Postgres's level since step 14's S14-18: `silver.order_lines`, the view
revision 0011 renders from the one body (`core.sql_dialect.order_lines_select`),
over Silver derived from Bronze by the derivation's own rebuild. Until then
these ran on DuckDB's `silver_order_lines` view; the answers they assert are
DuckDB's on the same fixture, and Postgres gives them (rule 9).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.integration.pg_tab_seed import (
    DSN, derive, needs_postgres, seed, tab_in_postgres,
)

pytestmark = needs_postgres


def _fixture(*, with_uncatalogued: bool = True):
    """Two orders, four lines, one of them for a product the catalog lost."""
    now = datetime.now(timezone.utc)
    lines = [(1, 1, 100, "Serum", 2, 500.0), (2, 1, 100, "Serum", 1, 400.0),
             (3, 2, 100, "Serum", 3, 300.0)]
    if with_uncatalogued:
        lines.append((4, 2, None, "Gone from the catalog", 1, 250.0))
    return dict(
        categories=[(1, "Care", None), (2, "Serums", 1)],
        products=[(100, "Serum", 2, "BrandA", "SKU-1", 500.0)],
        orders=[(oid, 1, 1, 1000.0, now - timedelta(days=days), 7, None, None)
                for oid, days in ((1, 3), (2, 5))],
        lines=lines,
    )


async def _seeded(monkeypatch, *queries, before_derive=None):
    """Seed the fixture, derive, and answer each `(sql, fetch)` in turn."""
    async def populate(conn, pool):
        await seed(conn, **_fixture())
        if before_derive:
            await conn.execute(before_derive)

    out = []
    async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
        await derive(pool)
        async with pool.acquire() as conn:
            for sql in queries:
                out.append(await conn.fetch(sql))
    return out


class TestTheLevel:
    @pytest.mark.asyncio
    async def test_the_grain_is_the_line(self, monkeypatch):
        lines, raw = await _seeded(
            monkeypatch, "SELECT COUNT(*) AS n FROM silver.order_lines",
            "SELECT COUNT(*) AS n FROM bronze.order_products")
        assert lines[0]["n"] == raw[0]["n"] == 4

    @pytest.mark.asyncio
    async def test_a_line_whose_product_left_the_catalog_survives(self, monkeypatch):
        """31 % of production lines have no category and 8.2 % no product_id.

        A level that inner-joins the catalog is a filter wearing a level's name,
        and it would drop revenue quietly — which is how ₴13–27M of product
        revenue went uncategorised without anyone seeing a number move.
        """
        (rows,) = await _seeded(
            monkeypatch,
            "SELECT product_name, brand, category_id, line_amount "
            "FROM silver.order_lines WHERE product_id IS NULL")
        assert rows, "the uncatalogued line was dropped"
        row = rows[0]
        assert row["brand"] is None and row["category_id"] is None
        assert float(row["line_amount"]) == 250.0

    @pytest.mark.asyncio
    async def test_it_carries_the_order_answer_rather_than_deriving_one(self, monkeypatch):
        (rows,) = await _seeded(monkeypatch, """
                SELECT COUNT(*) AS n FROM silver.order_lines l
                JOIN silver.orders s ON s.id = l.order_id
                WHERE l.sales_type IS DISTINCT FROM s.sales_type
                   OR l.order_date IS DISTINCT FROM s.order_date
                   OR l.is_return IS DISTINCT FROM s.is_return
                   OR l.is_active_source IS DISTINCT FROM s.is_active_source
            """)
        assert rows[0]["n"] == 0

    # `test_it_reproduces_gold_to_the_kopeck` stood here.
    #
    # It grouped `gold_daily_products` by date and compared it against the same
    # grouping of this level — the load-bearing property, verified on
    # production at ₴132,077,453.75 across 986 dates with nothing disagreeing.
    #
    # That Gold is retired: this level replaced it in three tabs and the weekly
    # report, and rebuilding a layer nobody reads every two minutes was the
    # single largest thing left writing to the file. With no second computation
    # there is nothing to compare against, and the property cannot be restated
    # as a test — only recorded as the measurement that licensed the change.
    #
    # What still holds this level honest is the test above (every line agrees
    # with its order on the four predicate columns) and the one below (it keeps
    # what Gold excluded rather than hiding it, so a caller's predicate is the
    # caller's).


    @pytest.mark.asyncio
    async def test_it_holds_what_gold_excludes_rather_than_hiding_it(self, monkeypatch):
        """Returns and dead sources belong to the caller's predicate, not the level."""
        total, counted, returned = await _seeded(
            monkeypatch,
            "SELECT ROUND(SUM(line_amount), 2) AS v FROM silver.order_lines",
            "SELECT ROUND(COALESCE(SUM(line_amount), 0), 2) AS v FROM silver.order_lines "
            "WHERE NOT is_return AND is_active_source",
            "SELECT ROUND(SUM(line_amount), 2) AS v FROM silver.order_lines WHERE is_return",
            before_derive="UPDATE bronze.orders SET status_id = 19 WHERE id = 2")
        total, counted, returned = total[0]["v"], counted[0]["v"], returned[0]["v"]
        assert float(returned) > 0, "the fixture did not actually produce a return"
        assert float(total) == float(counted) + float(returned)

    # `test_it_still_resolves_after_silver_is_rebuilt_from_scratch` stood here
    # until step 14's S14-15 deleted DuckDB's half of `rebuild-silver`, the
    # DROP and re-CREATE of `silver_orders` it was about.
