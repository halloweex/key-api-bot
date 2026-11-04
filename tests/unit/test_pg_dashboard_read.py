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
six items is the contract, because the caller unpacks positionally, so it is
asserted here rather than left to the differential test to discover as a
number that is merely wrong.
"""
from __future__ import annotations

import ast
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore


REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "revenue.py"

TODAY = date.today()
WINDOW = (TODAY - timedelta(days=30), TODAY)

# The routed methods that live in `revenue.py` — the file the structural tests
# below parse. `get_customer_insights` rides the same router but lives in the
# customers mixin, so it is checked by `TestTheCustomerBlock`, against its own
# file. Adding it here instead made two tests raise `StopIteration` looking for
# it in the wrong tree. The last three are the Gold and Silver readers' (step
# 14, S14-06), which had a DuckDB tail of their own until then.
METHODS = ("get_return_orders", "get_sales_by_source", "get_subcategory_breakdown",
           "get_top_products", "get_product_performance", "get_brand_analytics",
           "get_summary_stats", "get_revenue_trend", "get_manager_sales_365d")

CALLS = (
    ("get_return_orders", {}),
    ("get_return_orders", {"sales_type": "all"}),
    ("get_sales_by_source", {}),                       # the Gold branch
    ("get_sales_by_source", {"brand": "BrandA"}),      # the line-level branch
    ("get_sales_by_source", {"category_id": 1}),       # …and the category walk
    ("get_subcategory_breakdown", {"parent_category_name": "Care"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "brand": "BrandA"}),
    ("get_top_products", {}),
    ("get_top_products", {"category_id": 1}),          # …and the category walk
    ("get_product_performance", {}),                   # two statements, one call
    ("get_brand_analytics", {}),
    ("get_customer_insights", {}),             # five statements, one call
    ("get_customer_insights", {"sales_type": "all"}),
)

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


def _call(store, name, kwargs):
    if name == "get_subcategory_breakdown":
        return getattr(store, name)(*WINDOW, **kwargs)
    return getattr(store, name)(*WINDOW, **kwargs)


class TestItDoesNotTakeTheDuckDbLock:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.values()) if k else ''}" for n, k in CALLS],
    )
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch, name, kwargs,
    ):
        # The category walk is filter chrome, so it belongs to the filter
        # bar's router and not to whichever tab is asking (it rode
        # `KS_READ_REPORTS` until this test caught it reaching for DuckDB
        # with this tab's flag on). Both routers read Postgres whatever any
        # switch says (step 14, S14-05 and S14-06); their fetches are stubbed
        # below.
        # A store never connected, as web's is under `KS_DUCKDB=off`.
        store = DuckDBStore(db_path=tmp_path / f"d-{name}-{len(kwargs)}.duckdb")

        def boom(*_a, **_k):
            raise AssertionError(f"{name} reached for DuckDB")

        with patch.object(type(store), "connection", boom), \
             patch("core.pg_lookups_read.fetch",
                   new=AsyncMock(return_value=[])), \
             patch("core.pg_dashboard_read.fetch",
                   new=AsyncMock(return_value=[])) as fetch:
            await _call(store, name, kwargs)
        assert fetch.await_count >= 1
        assert not (tmp_path / f"d-{name}-{len(kwargs)}.duckdb").exists()


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

    def test_no_dashboard_method_opens_the_store_itself(self):
        """Walked as calls in the tree, never as a substring of the source:
        the comment beside the fix names the helper it replaced, and a grep
        finds the comment. That mistake has been made three times here."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in METHODS:
            node = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            calls = {c.func.attr for c in ast.walk(node)
                     if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)}
            assert "connection" not in calls, f"{name} still opens DuckDB"
            assert "_get_category_with_children" not in calls, (
                f"{name} calls the connection-taking helper — that is the "
                f"deadlock, and it does not raise"
            )

    def test_the_file_never_takes_the_store_lock(self):
        """Was "the router is never called under the lock": a router called
        inside `self.connection()` held the lock DuckDB's single writer
        queues on across a network round trip. Since step 14's S14-06 the
        file has no DuckDB tail to take the lock for — no `connection()`,
        no `_fetch_*`, no `nullcontext` standing in for it — so the question
        is answered for every method at once, read off the tree."""
        tree = ast.parse(REPOSITORY.read_text(encoding="utf-8"))
        reached = sorted(
            (n.lineno, n.func.attr) for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr in ("connection", "_fetch_one", "_fetch_all"))
        assert not reached, reached
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert "nullcontext" not in names and "duckdb_switch" not in names

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

    def test_the_six_items_are_orders_then_revenue_per_channel(self):
        """The caller unpacks positionally: ig_orders, ig_rev, tg_orders,
        tg_rev, sh_orders, sh_rev. Getting the pairs the wrong way round
        produces numbers that are merely wrong, on a chart nobody would
        immediately disbelieve."""
        from core.sql_dialect import POSTGRES, channel_measures

        # DuckDB's rendering, a column per channel, went with step 14's
        # S14-07; Postgres' is the one shape.
        pg = [i.strip() for i in channel_measures(POSTGRES).split(",\n")]
        assert len(pg) == 6
        for i, sid in enumerate((1, 2, 4)):
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

    def test_no_routed_body_names_it(self):
        from core.repositories import revenue

        for name in BODIES:
            assert "gold_daily_products" not in getattr(revenue, name), name

    def test_no_dashboard_method_names_it_either(self):
        """Asserted on the tree, and with the docstring taken out of it.

        The first version of this walked every string constant in the method,
        and the methods *explain* why they left that table — so it failed on
        its own prose. That is the fourth time in this file's history the
        difference between structure and prose has had to be paid for; the
        docstring is a `Constant` like any other, and it is the one that has
        to be excluded by name."""
        source = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for name in METHODS:
            node = next(n for n in ast.walk(tree)
                        if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
            doc = ast.get_docstring(node, clean=False)
            strings = [n.value for n in ast.walk(node)
                       if isinstance(n, ast.Constant) and isinstance(n.value, str)
                       and n.value != doc]
            for text in strings:
                assert "gold_daily_products" not in text, (
                    f"{name} still names the DuckDB-only Gold"
                )

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

    def test_no_body_uses_the_duckdb_only_date_function(self):
        """`DATE_DIFF` does not exist in PostgreSQL. Subtracting the dates is
        the portable spelling and the same answer — measured over all 6,822
        retail buyers with more than one order: zero differing."""
        for name, sql in self._bodies().items():
            assert "DATE_DIFF" not in sql.upper(), name

    def test_the_gold_statement_asks_only_for_what_survives(self):
        """It selected the three customer counts as well, and every one was
        overwritten a few lines later by a count over Silver — Gold sums
        *daily* distinct counts, so a buyer who returned on Tuesday was two
        customers. Reading them was work thrown away."""
        sql = self._bodies()["_INSIGHTS_GOLD_SQL"].lower()
        for dead in ("unique_customers", "new_customers", "returning_customers"):
            assert dead not in sql, dead

    def test_it_does_not_open_the_store_itself(self):
        import ast as _ast
        from pathlib import Path as _Path

        source = (_Path(__file__).resolve().parents[2]
                  / "core" / "repositories" / "customers.py").read_text(encoding="utf-8")
        node = next(n for n in _ast.walk(_ast.parse(source))
                    if isinstance(n, _ast.AsyncFunctionDef)
                    and n.name == "get_customer_insights")
        calls = {c.func.attr for c in _ast.walk(node)
                 if isinstance(c, _ast.Call) and isinstance(c.func, _ast.Attribute)}
        assert "connection" not in calls
        assert "_dashboard_run" in calls
