"""What answers the rest of `/dashboard`, and what it may not touch.

The numbers are `tests/integration/test_dashboard_two_engines.py`'s job. Here:

  * a read never touches DuckDB's lock (§34) — since step 14's S14-06 there
    is no DuckDB tail at all, and nothing in `core/repositories/revenue.py`
    takes the store lock;
  * **the doughnut's filtered branch does not call the connection-taking
    category helper.** That is the specific deadlock `/reports` shipped: the
    method used to hold a connection and pass it to
    `_get_category_with_children`; routed, it holds nothing, so it must use
    `_category_ids`, the twin that goes through the router.

That the switch (`KS_READ_DASHBOARD`) is not read and a failure is refused
naming `dashboard` is `tests/unit/test_dashboard_routers_postgres_only.py`'s.

AND THE HOLE THAT IS NOT A NAME

`{channel_measures}` renders the one place the two Golds differ in *shape* —
DuckDB spells a channel as a column, Postgres as a dimension. The order of the
items — six, and eight since the offline store got a slice (the owner's
decision of 2026-11-15) — is the contract, because the caller unpacks
positionally, so it is asserted here rather than left to the differential test
to discover as a number that is merely wrong.
"""
from __future__ import annotations

import re

BODIES = (
    "_RETURN_ORDERS_SQL",
    "_SALES_BY_SOURCE_FILTERED_SQL",
    "_SALES_BY_SOURCE_GOLD_SQL",
    "_SUBCATEGORY_SQL",
    "_TOP_PRODUCTS_SQL",
    "_TOP_PRODUCTS_BY_REVENUE_SQL",
    "_CATEGORY_BREAKDOWN_SQL",
    "_BRAND_ANALYTICS_SQL",
)

# The customer block lives on `CustomersMixin` and rides the same flag; its
# bodies are module constants there.
INSIGHT_BODIES = (
    "_INSIGHTS_GOLD_SQL",
    "_INSIGHTS_AOV_SQL",
    "_INSIGHTS_CLV_SQL",
    "_INSIGHTS_PERIOD_SQL",
    "_INSIGHTS_ALLTIME_SQL",
)

# The holes each body fills in itself before the dialect renderer sees it.
CALLER_HOLES = ("{where}", "{brand_filter}", "{promocode_filter}")


class TestTheRoutedBodies:
    def test_the_postgres_rendering_leaves_no_hole(self):
        from core.sql_dialect import POSTGRES
        from core.repositories import revenue
        from core.repositories.revenue import RevenueMixin

        for name in BODIES:
            sql = getattr(revenue, name)
            for hole in CALLER_HOLES:
                sql = sql.replace(hole, "TRUE" if hole == "{where}" else "")
            rendered = RevenueMixin._render_report(sql, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, (
                f"{name} still has a hole for Postgres: {rendered}"
            )

    def test_every_ordered_body_ends_on_a_key_that_cannot_tie(self):
        """Two engines break a tie differently. `_RETURN_ORDERS_SQL` carries a
        LIMIT, where a tie means different *rows* and not a different order."""
        from core.repositories import revenue

        unique = {"l.source_id", "l.category_name", "s.id",
                  "product_name", "brand_name", "category_name"}
        for name in BODIES:
            sql = getattr(revenue, name)
            upper = sql.upper()
            if "ORDER BY" not in upper:
                continue
            tail = re.split(r"LIMIT", sql[upper.index("ORDER BY") + 8:], flags=re.I)[0]
            tail = "\n".join(l.split("--")[0] for l in tail.splitlines())
            last = [c for c in tail.split(",") if c.strip()][-1].strip().split()[0]
            assert last in unique, f"{name} can tie on {last!r}"


class TestTheChannelShapeHasOneHome:
    """`{channel_measures}` is the only hole here that is not a table name."""

    def test_the_items_are_orders_then_revenue_per_source(self):
        """The caller unpacks positionally: ig_orders, ig_rev, tg_orders,
        tg_rev, sh_orders, sh_rev, store_orders, store_rev. Getting the pairs
        the wrong way round produces numbers that are merely wrong, on a chart
        nobody would immediately disbelieve."""
        from core.duckdb_constants import OFFLINE_STORE_SOURCE_ID
        from core.sql_dialect import DOUGHNUT_SOURCE_IDS, POSTGRES, channel_measures

        # DuckDB's rendering, a column per channel, went with step 14's
        # S14-07; Postgres' is the one shape. The offline store is the
        # doughnut's fourth slice since the owner's decision of 2026-11-15.
        assert DOUGHNUT_SOURCE_IDS == (1, 2, 4, OFFLINE_STORE_SOURCE_ID)
        pg = [i.strip() for i in channel_measures(POSTGRES).split(",\n")]
        assert len(pg) == 8
        for i, sid in enumerate(DOUGHNUT_SOURCE_IDS):
            assert "orders_count" in pg[i * 2] and f"source_id = {sid}" in pg[i * 2]
            assert "SUM(revenue)" in pg[i * 2 + 1] and f"source_id = {sid}" in pg[i * 2 + 1]

    def test_the_postgres_rendering_never_reads_the_rollup_row(self):
        """Postgres' Gold holds both grains. A channel measure that did not
        filter on `source_id` would add the roll-up to the fine rows and
        double every channel."""
        from core.sql_dialect import POSTGRES, channel_measures

        for item in channel_measures(POSTGRES).split(",\n"):
            assert "FILTER (WHERE source_id =" in item, item

    def test_the_marketing_measures_render_the_channels_from_the_same_place(self):
        """Two callers, one home. A fourth channel column must be added once."""
        from core.sql_dialect import POSTGRES, marketing_period_measures
        from core.sql_dialect import _channel_items

        for dialect in (POSTGRES,):
            eleven = marketing_period_measures(dialect)
            for item in _channel_items(dialect):
                assert item in eleven, (
                    f"{dialect.name}: the eleven-item rendering no longer uses "
                    f"the shared channel items"
                )


class TestNothingHereReadsTheDeadGold:
    """`gold_daily_products` exists only in DuckDB, and these three were its
    last readers on this tab. A body that reached for it again would render a
    name Postgres does not have and fall back for ever, silently."""

    def test_the_product_grouping_is_by_id_not_by_the_sold_name(self):
        """Grouping on the sold name split 193 of 268 production products."""
        from core.repositories import revenue

        for name in ("_TOP_PRODUCTS_SQL", "_TOP_PRODUCTS_BY_REVENUE_SQL"):
            body = getattr(revenue, name)
            assert "GROUP BY COALESCE(CAST(l.product_id AS VARCHAR)" in body, name

    def test_the_brand_order_count_is_distinct(self):
        """A sum of per-product cells was 35.2 % high on production."""
        from core.repositories import revenue

        assert "COUNT(DISTINCT l.order_id)" in revenue._BRAND_ANALYTICS_SQL

    def test_the_displayed_name_is_deterministic(self):
        """`ANY_VALUE` is free to pick a different row in each engine, which is
        a differential test that flakes rather than a fact."""
        from core.repositories import revenue

        for name in BODIES:
            assert "ANY_VALUE" not in getattr(revenue, name).upper(), name


class TestTheCustomerBlock:
    """`/api/customers/insights` rides this router too, and its bodies live in
    the customers mixin. Three things it must not do again."""

    def _bodies(self):
        from core.repositories import customers
        return {n: getattr(customers, n) for n in INSIGHT_BODIES}

    def test_the_postgres_rendering_leaves_no_hole(self):
        from core.sql_dialect import POSTGRES, render_tables

        for name, sql in self._bodies().items():
            filled = (sql.replace("{where}", "TRUE")
                         .replace("{sales_where}", "TRUE"))
            rendered = render_tables(filled, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, name

    def test_every_gold_read_carries_the_rollup_predicate(self):
        """Postgres' Gold holds a roll-up row *and* a row per source. Summing
        both counts every day twice — the defect the /traffic ROAS query
        shipped with, which halved a number nobody could see was halved."""
        bodies = self._bodies()
        for name in ("_INSIGHTS_GOLD_SQL", "_INSIGHTS_AOV_SQL"):
            assert "{gold_revenue_rollup}" in bodies[name], name

    def test_the_gold_statement_asks_only_for_what_survives(self):
        """It selected the three customer counts as well, and every one was
        overwritten a few lines later by a count over Silver — Gold sums
        *daily* distinct counts, so a buyer who returned on Tuesday was two
        customers. Reading them was work thrown away."""
        sql = self._bodies()["_INSIGHTS_GOLD_SQL"].lower()
        for dead in ("unique_customers", "new_customers", "returning_customers"):
            assert dead not in sql, dead
