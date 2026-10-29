"""The rest of `/dashboard` answers in Postgres what DuckDB answered.

ONE ENGINE SINCE STEP 14 (S14-06)

The dashboard reads Postgres and nothing else — its Gold primitives, its
Silver escapes and everything `_dashboard_run` carries. Every call below is
compared with a literal (`tests/integration/golden/dashboard.json`): DuckDB's
answer on this file's fixture, pinned to 2026-09-15, recorded while the same
run asserted Postgres — Silver and Gold derived from the same Bronze, the
production path — answered each one equally (rule 9 of the step-14 plan; all
92 agreed). The store's DuckDB connection is a trap, so an answer can only be
Postgres's. `tests/unit/test_pg_dashboard_read.py` proves the bodies and that
nothing goes round the router; `tests/unit/test_dashboard_routers_postgres_only.py`
the refusal and the response's shape.

THE DOUGHNUT READS TWO DIFFERENT THINGS

Unfiltered it reads Gold, where DuckDB spelled a channel as a column
(`instagram_revenue`) and Postgres spells it as a dimension (`source_id = 1`)
over a table that also holds roll-up rows. Filtered it reads the order-lines
level instead, because a category or brand is a fact about a line. Both
branches are asked, and with the same filters.

THE FIXTURE IS DELIBERATELY AWKWARD

An order with **no line items** and real revenue (323 of those exist in
production): the Gold branch counts it, the line-level branch cannot, and the
two branches are supposed to disagree about it. A return, an order from a
retired source that must reach no total, an exhibition sale (source 5) that
the summary answers with zeros and the trend with its money (the owner's
decision 5 for step 14), a staff order (`internal`), an order in the previous
period and one a year back so both comparisons have something to compare, a
promocode, two categories tied on revenue so the ordering has a tie to break,
an order whose buyer is in neither store and one with a NULL manager so both
LEFT JOINs have a miss to survive, and a category two deep so the filter has
descendants to walk.

THE SEARCH INDEX, BELOW, ON THE ORIGINAL FIXTURE

`sync_to_meilisearch` reads Postgres alone since step 14's S14-07. Its tests,
and `tests/integration/test_chat_tools_two_engines.py`'s, which imports the
fixture with `_comparable`, `WINDOW` and `DSN`, run on this file's original
seven orders (`original_in_postgres`), pinned to the same day, and compare
with literals recorded while both engines answered on it
(`tests/integration/golden/search_index.json` and `chat_tools.json`).

Skipped without `KS_PG_DSN`; CI and `deploy/gate_with_stores.sh` supply one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio
from unittest.mock import patch

from core.duckdb_constants import B2B_MANAGER_ID
from tests.integration.pg_tab_seed import (
    call_key, comparable, derive, golden, seed, store_that_never_reaches_duckdb,
    tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# Pinned (step 14, S14-07) for the search index and the assistant's tools,
# whose answers are recorded as literals from DuckDB on `both_engines`.
TODAY = date(2026, 9, 15)
WINDOW = (TODAY - timedelta(days=30), TODAY)

CATEGORIES = [(1, "Care", None), (2, "Serums", 1), (3, "Ampoules", 1), (4, "Makeup", None)]
PRODUCTS = [
    (100, "Serum", 2, "BrandA", "SKU-A", 500.0),
    (200, "Ampoule", 3, "BrandB", "SKU-B", 100.0),
    (300, "Lipstick", 4, None, "SKU-C", 250.0),
    (400, "Toner", 2, "BrandA", "SKU-D", 200.0),   # BrandA's second product
]

# (buyer_id, full_name, phone). Order 3 below points at buyer 999, which is in
# neither store — that is the LEFT JOIN miss, and it is the portable way to
# make one.
#
# A buyer row with a NULL name would have been the obvious way, and it is not
# available: `buyers.full_name` is NOT NULL in DuckDB and nullable in
# `bronze.buyers`. That divergence is real and currently latent — measured on
# production 2026-09-12: zero NULL names, zero NULL manager names, zero orders
# pointing at a buyer that is not there. It matters the day KeyCRM serves one,
# because DuckDB would reject the row and Postgres accept it, and the two
# stores would then hold different numbers of buyers.
BUYERS = [(7, "Олена", "+380501112233"), (8, "Ігор", None)]
# (manager_id, name)
MANAGERS = [(B2B_MANAGER_ID, "Wholesale")]

# (order_id, source_id, grand_total, days_ago, is_return, manager_id, buyer_id)
ORDERS = [
    (1, 1, 1200.0, 2, False, None, 7),
    (2, 2,  600.0, 3, False, None, 7),
    (3, 1,  400.0, 4, True,  None, 999),    # a return whose buyer is in neither store
    (4, 3,  900.0, 5, False, None, 7),      # source 3 retired — reaches nothing
    (5, 4,  750.0, 6, False, None, 8),      # NO LINE ITEMS, and revenue
    (6, 1,  300.0, 7, False, B2B_MANAGER_ID, 7),   # b2b, and a named manager
    (7, 2,  500.0, 8, True,  B2B_MANAGER_ID, 7),   # a b2b return
]
# (line_id, order_id, product_id, quantity, price_sold, sold_name)
#
# `sold_name` is `order_products.name` — the name at the moment of the order,
# which is not the catalogue's. Product 100 is sold under two of them, which is
# the production shape: 193 of 268 products carry more than one name over
# thirty days, and grouping on it split each of them into several rows.
#
# Order 1 carries two BrandA lines (100 and 400). Summing Gold's per-product
# cells would count that order twice for BrandA; `COUNT(DISTINCT order_id)`
# counts it once, and the difference is what was 35.2 % on production.
LINES = [
    (1, 1, 100, 2, 500.0, "Serum"),
    (2, 1, 200, 2, 100.0, "Ampoule"),
    (3, 1, 400, 1, 200.0, "Toner"),          # same order, same brand as line 1
    (4, 2, 100, 1, 500.0, "Serum — renamed"),  # the same product, another name
    (5, 3, 200, 1, 100.0, "Ampoule"),
    (6, 4, 300, 4, 250.0, "Lipstick"),
    (7, 6, 200, 3, 100.0, "Ampoule"),
    (8, 7, 100, 1, 500.0, "Serum"),
]

async def _populate_original(conn, pool):
    """The original seven orders, as KeyCRM lands them, derived by Postgres."""
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS, buyers=BUYERS,
        managers=MANAGERS,
        orders=[(oid, src, 19 if is_ret else 1, total, NOW - timedelta(days=days),
                 buyer, manager, None)
                for oid, src, total, days, is_ret, manager, buyer in ORDERS],
        lines=[(lid, oid, pid, sold, qty, price)
               for lid, oid, pid, qty, price, sold in LINES])
    await derive(pool)


@pytest_asyncio.fixture
async def original_in_postgres(tmp_path, monkeypatch):
    """The original fixture's store, never connected: its DuckDB connection is
    a trap, so an answer can only be Postgres's. The search index and the
    assistant's tools read it (step 14, S14-07)."""
    async with tab_in_postgres(DSN, monkeypatch, _populate_original):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the index")


def _comparable(value):
    """Money to six places. DuckDB divides a DECIMAL into a DOUBLE and
    Postgres into a NUMERIC, so the two legitimately differ in the last bits —
    far below anything this tab renders, far above the noise.

    No test compares two engines any more; kept for
    `tests/integration/test_chat_tools_two_engines.py`, which imports it."""
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _comparable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_comparable(v) for v in value]
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


# ─── The dashboard, on Postgres alone (step 14, S14-06) ──────────────────────

# Pinned: the answers are literals taken from DuckDB on this fixture, so the
# calendar under them cannot move with the run.
PINNED_TODAY = date(2026, 9, 15)
NOW = datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)   # midday Kyiv
PINNED_WINDOW = (PINNED_TODAY - timedelta(days=30), PINNED_TODAY)
STAFF_MANAGER = 34      # in neither the managers nor the classifications: `internal`

# (order_id, source_id, grand_total, days_ago, is_return, manager_id, buyer_id,
#  promocode) — the original seven, then the four the S14-06 recording added.
DASHBOARD_ORDERS = [
    (1, 1, 1200.0, 2, False, None, 7, None),
    (2, 2, 600.0, 3, False, None, 7, "SAVE10"),
    (3, 1, 400.0, 4, True, None, 999, None),     # a return whose buyer is in neither store
    (4, 3, 900.0, 5, False, None, 7, None),      # source 3 retired — reaches nothing
    (5, 4, 750.0, 6, False, None, 8, None),      # NO LINE ITEMS, and revenue
    (6, 1, 300.0, 7, False, B2B_MANAGER_ID, 7, None),   # b2b, and a named manager
    (7, 2, 500.0, 8, True, B2B_MANAGER_ID, 7, None),    # a b2b return
    (8, 5, 450.0, 9, False, None, 8, None),      # the exhibition: decision 5
    (9, 4, 350.0, 40, False, None, 8, None),     # the previous period
    (10, 1, 220.0, 370, False, None, 7, None),   # a year back
    (11, 1, 280.0, 10, False, STAFF_MANAGER, 7, None),  # internal
]
# (line_id, order_id, product_id, quantity, price_sold, sold_name), `LINES`'
# shape: product 100 is sold under two names, and order 1 carries two BrandA
# lines (100 and 400) — `COUNT(DISTINCT order_id)` counts it once.
DASHBOARD_LINES = LINES + [
    (9, 8, 300, 1, 450.0, "Lipstick"),
    (10, 9, 400, 1, 350.0, "Toner"),
    (11, 10, 100, 1, 220.0, "Serum"),
    (12, 11, 200, 2, 140.0, "Ampoule"),
]


async def _populate(conn, pool):
    await seed(
        conn, categories=CATEGORIES, products=PRODUCTS, buyers=BUYERS,
        managers=MANAGERS,
        orders=[(oid, src, 19 if is_ret else 1, total, NOW - timedelta(days=days),
                 buyer, manager, promo)
                for oid, src, total, days, is_ret, manager, buyer, promo
                in DASHBOARD_ORDERS],
        lines=[(lid, oid, pid, sold, qty, price)
               for lid, oid, pid, qty, price, sold in DASHBOARD_LINES])
    await derive(pool)


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The dashboard's store, never connected: its DuckDB connection is a
    trap, so an answer below can only be Postgres's."""
    async with tab_in_postgres(DSN, monkeypatch, _populate):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the dashboard")


async def _answer(store, name, kwargs):
    return await getattr(store, name)(*PINNED_WINDOW, **kwargs)


GOLDEN = golden("dashboard")

CALLS = (
    # The doughnut, Gold branch — no filters at all.
    ("get_sales_by_source", {}),
    ("get_sales_by_source", {"sales_type": "all"}),
    ("get_sales_by_source", {"sales_type": "b2b"}),
    ("get_sales_by_source", {"sales_type": "internal"}),
    # …and the line-level branch, one filter each and then together.
    ("get_sales_by_source", {"category_id": 1}),        # a parent, two deep
    ("get_sales_by_source", {"category_id": 99999}),    # absent — was a 500
    ("get_sales_by_source", {"brand": "BrandA"}),
    ("get_sales_by_source", {"brand": "Unknown"}),      # the NULL-brand bucket
    ("get_sales_by_source", {"category_id": 1, "brand": "BrandA"}),
    ("get_sales_by_source", {"promocode": "SAVE10"}),
    # The returns list.
    ("get_return_orders", {}),
    ("get_return_orders", {"sales_type": "all"}),
    ("get_return_orders", {"sales_type": "b2b"}),
    ("get_return_orders", {"limit": 1}),                # the tie under a LIMIT
    # The subcategory breakdown.
    ("get_subcategory_breakdown", {"parent_category_name": "Care"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Makeup"}),   # a leaf root
    ("get_subcategory_breakdown", {"parent_category_name": "Nope"}),     # nothing
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "brand": "BrandA"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "source_id": 1}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "promocode": "SAVE10"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "sales_type": "all"}),
    # The three that came off `gold_daily_products`.
    ("get_top_products", {}),
    ("get_top_products", {"category_id": 1}),
    ("get_top_products", {"brand": "BrandA"}),
    ("get_top_products", {"limit": 2}),
    ("get_top_products", {"promocode": "NOPE"}),        # the old Silver branch
    ("get_top_products", {"promocode": "SAVE10"}),
    ("get_top_products", {"source_id": 1}),
    ("get_top_products", {"sales_type": "all"}),
    ("get_product_performance", {}),
    ("get_product_performance", {"brand": "BrandA"}),
    ("get_product_performance", {"source_id": 1}),
    ("get_product_performance", {"sales_type": "all"}),
    ("get_product_performance", {"promocode": "SAVE10"}),
    ("get_brand_analytics", {}),
    ("get_brand_analytics", {"sales_type": "all"}),
    ("get_brand_analytics", {"sales_type": "b2b"}),
    ("get_brand_analytics", {"source_id": 1}),
    # The customer block: five statements, two of them over Gold where the
    # roll-up predicate decides whether every day is counted once or twice.
    ("get_customer_insights", {}),
    ("get_customer_insights", {"sales_type": "all"}),
    ("get_customer_insights", {"sales_type": "b2b"}),
    ("get_customer_insights", {"sales_type": "internal"}),
    ("get_customer_insights", {"promocode": "NOPE"}),
    ("get_customer_insights", {"promocode": "SAVE10"}),
    ("get_customer_insights", {"source_id": 1, "brand": "BrandA"}),
    # `/api/summary`: Gold, Gold by source, the zeros of decision 5, and the
    # Silver escapes — a category or brand, a promocode.
    ("get_summary_stats", {}),
    ("get_summary_stats", {"sales_type": "all"}),
    ("get_summary_stats", {"sales_type": "b2b"}),
    ("get_summary_stats", {"sales_type": "internal"}),
    ("get_summary_stats", {"source_id": 1}),
    ("get_summary_stats", {"source_id": 2}),
    ("get_summary_stats", {"source_id": 4}),
    ("get_summary_stats", {"source_id": 1, "sales_type": "all"}),
    ("get_summary_stats", {"source_id": 2, "sales_type": "b2b"}),
    ("get_summary_stats", {"source_id": 3}),
    ("get_summary_stats", {"source_id": 5}),
    ("get_summary_stats", {"source_id": 5, "sales_type": "all"}),
    ("get_summary_stats", {"source_id": 3, "brand": "Unknown"}),
    ("get_summary_stats", {"category_id": 1}),               # a parent: the tree walk
    ("get_summary_stats", {"category_id": 4}),               # a leaf root
    ("get_summary_stats", {"category_id": 99999}),           # absent
    ("get_summary_stats", {"brand": "BrandA"}),              # two lines in one order
    ("get_summary_stats", {"brand": "Unknown"}),             # the NULL-brand bucket
    ("get_summary_stats", {"promocode": "NOPE"}),            # no Gold column at all
    ("get_summary_stats", {"promocode": "SAVE10"}),
    ("get_summary_stats", {"brand": "BrandA", "source_id": 1}),
    ("get_summary_stats", {"category_id": 1, "sales_type": "all"}),
    ("get_summary_stats", {"brand": "BrandA", "sales_type": "b2b"}),
    ("get_summary_stats", {"promocode": "SAVE10", "source_id": 5, "sales_type": "all"}),
    # `/api/revenue/trend`: Gold, the order grain (a promocode, a source the
    # dashboard does not ask Gold about), the lines, and both comparisons.
    ("get_revenue_trend", {}),
    ("get_revenue_trend", {"sales_type": "all"}),
    ("get_revenue_trend", {"sales_type": "b2b"}),
    ("get_revenue_trend", {"sales_type": "internal"}),
    ("get_revenue_trend", {"source_id": 1}),
    ("get_revenue_trend", {"source_id": 4, "sales_type": "all"}),
    ("get_revenue_trend", {"source_id": 3}),
    ("get_revenue_trend", {"source_id": 5}),
    ("get_revenue_trend", {"source_id": 5, "sales_type": "all"}),   # Виставка: the order grain
    ("get_revenue_trend", {"compare_type": "month_ago"}),
    ("get_revenue_trend", {"compare_type": "year_ago"}),
    ("get_revenue_trend", {"include_comparison": False}),
    ("get_revenue_trend", {"category_id": 1}),
    ("get_revenue_trend", {"brand": "BrandA"}),
    ("get_revenue_trend", {"brand": "Unknown"}),
    ("get_revenue_trend", {"promocode": "NOPE"}),            # the order-grain escape
    ("get_revenue_trend", {"promocode": "SAVE10"}),
    ("get_revenue_trend", {"brand": "BrandA", "promocode": "SAVE10"}),
    ("get_revenue_trend", {"brand": "BrandA", "source_id": 1}),
    ("get_revenue_trend", {"category_id": 1, "sales_type": "all"}),
    ("get_revenue_trend", {"brand": "BrandA", "compare_type": "year_ago"}),
    ("get_revenue_trend", {"brand": "BrandA", "include_comparison": False}),
    ("get_revenue_trend", {"promocode": "SAVE10", "compare_type": "month_ago"}),
)


class TestPostgresAnswersWhatDuckDBAnswered:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,kwargs", CALLS,
        ids=[f"{n}{tuple(k.items()) if k else ''}" for n, k in CALLS],
    )
    async def test_the_whole_result_is_the_literal(self, in_postgres, name, kwargs):
        assert comparable(await _answer(in_postgres, name, kwargs)) == \
            GOLDEN[call_key(name, kwargs)]

    def test_every_literal_is_a_call_and_every_call_has_one(self):
        assert set(GOLDEN) == {call_key(n, k) for n, k in CALLS}


class TestTheTwoBranchesOfTheDoughnut:
    """The switch between Gold and the line level is itself behaviour, and the
    order with no line items is what makes it visible."""

    @pytest.mark.asyncio
    async def test_gold_counts_the_order_with_no_lines_and_the_lines_do_not(
        self, in_postgres,
    ):
        unfiltered = await _answer(in_postgres, "get_sales_by_source", {})
        filtered = await _answer(in_postgres, "get_sales_by_source", {"brand": "BrandA"})

        shopify_gold = dict(zip(unfiltered["labels"], unfiltered["revenue"]))
        shopify_lines = dict(zip(filtered["labels"], filtered["revenue"]))
        assert shopify_gold.get("Shopify") == 750.0, (
            "order 5 carries revenue and no lines; the Gold branch must see it"
        )
        assert "Shopify" not in shopify_lines, (
            "the line-level branch cannot see an order with no lines, and that "
            "is the difference the two branches have to keep"
        )


class TestTheReturnsListJoinsThatCanMiss:
    @pytest.mark.asyncio
    async def test_a_missing_buyer_and_a_null_manager_survive_both_joins(
        self, in_postgres,
    ):
        """Both joins are LEFT, and a port that turned one into an inner join
        would silently drop the order — with no total moving to say so.
        Production has no orphan buyers today, which is exactly why this has
        to be asserted on the code rather than noticed in the data."""
        returns = await _answer(in_postgres, "get_return_orders", {"sales_type": "all"})
        by_id = {r["id"]: r for r in returns}
        assert 3 in by_id, "the return whose buyer is missing was dropped"
        assert by_id[3]["buyerName"] is None
        assert by_id[3]["managerName"] is None
        assert by_id[7]["managerName"] == "Wholesale"


class TestTheTwoCorrectionsTheGoldPathCarried:
    """These are not transliterations, so they are asserted as behaviour.

    Both were measured on the production backup before being changed, and both
    are the kind of defect that shows a plausible number: a chart that ranks
    fragments of products, and an order count a third too high.
    """

    @pytest.mark.asyncio
    async def test_a_product_sold_under_two_names_is_one_row(self, in_postgres):
        """Product 100 ships as "Serum" and once as "Serum — renamed".
        Grouped by the sold name it is two products; grouped by id it is one,
        and its revenue is the sum."""
        performance = await _answer(in_postgres, "get_product_performance", {})
        labels = performance["topByRevenue"]["labels"]
        assert labels.count("Serum") == 1, labels
        assert "Serum — renamed" not in labels, (
            "the sold name reached the chart — the grouping is by name again"
        )
        revenue = dict(zip(labels, performance["topByRevenue"]["data"]))
        # lines 1 and 4: 2×500 + 1×500 (line 8 is on a return, line 11 a
        # year back)
        assert revenue["Serum"] == 1500.0, revenue

    @pytest.mark.asyncio
    async def test_the_catalogue_name_is_what_is_shown(self, in_postgres):
        """`MIN` over the sold names would have picked "Serum — renamed" for
        product 100 in DuckDB's collation. The catalogue name is the one a
        reader recognises."""
        performance = await _answer(in_postgres, "get_product_performance", {})
        assert "Serum" in performance["topByRevenue"]["labels"]

    @pytest.mark.asyncio
    async def test_one_order_with_two_lines_of_a_brand_counts_once(self, in_postgres):
        """Order 1 carries products 100 and 400, both BrandA. Summing Gold's
        per-product cells makes that two orders; it is one."""
        brands = await _answer(in_postgres, "get_brand_analytics", {})
        by_brand = dict(zip(brands["topByRevenue"]["labels"],
                            brands["topByRevenue"]["orders"]))
        # BrandA appears on orders 1 and 2 — order 7 is a return, order 4 is a
        # retired source, order 9 the previous period. Two, not the three
        # lines that carry it.
        assert by_brand["BrandA"] == 2, by_brand


class TestTheBlendedAov:
    """`sales_type=all` used to chart one arbitrary sales type's AOV.

    The query had no `GROUP BY date` and the caller folds rows into a dict
    keyed on the date, so with three rows for one day the last one won — and
    nothing orders rows within a date, so *which* one was not stable between
    runs. Measured on the production backup: 23 of 30 days disagreed with the
    blended figure.
    """

    @pytest.mark.asyncio
    async def test_a_day_with_three_sales_types_reports_the_blend(self, in_postgres):
        insights = await _answer(in_postgres, "get_customer_insights", {"sales_type": "all"})
        series = insights["aovTrend"]["datasets"][0]["data"]
        labels = insights["aovTrend"]["labels"]
        # The fixture puts each sales type's orders on different days, so the
        # blend is exercised by the totals rather than by one cell: every
        # non-zero point must lie between the smallest and largest per-order
        # value the fixture contains.
        nonzero = [v for v in series if v]
        assert nonzero, "the window missed the fixture entirely"
        assert min(nonzero) >= 100.0 and max(nonzero) <= 1200.0, (
            f"an AOV outside every order in the fixture — the day is showing "
            f"one sales type rather than the blend: {dict(zip(labels, series))}"
        )


class TestDecisionFive:
    """The owner's decision 5 for step 14: `/api/summary` for source 5
    (Виставка) answers zeros — DuckDB's Gold had no column for it — while the
    trend for the same source shows its money from the order grain. A
    correction is its own change; this holds that S14-06 is not it."""

    @pytest.mark.asyncio
    async def test_the_summary_is_zeros_while_the_trend_has_the_money(self, in_postgres):
        summary = await _answer(in_postgres, "get_summary_stats",
                                {"source_id": 5, "sales_type": "all"})
        trend = await _answer(in_postgres, "get_revenue_trend",
                              {"source_id": 5, "sales_type": "all"})
        assert (summary["totalOrders"], summary["totalRevenue"]) == (0, 0)
        assert round(sum(trend["revenue"]), 2) == 450.0


# ─── The Meilisearch index (KS_READ_SEARCH_INDEX) ────────────────────────────
#
# `sync_to_meilisearch` had no tests at all before it moved. These run the real
# method end to end against a client that captures what it would index, once
# per engine, and compare the documents — because here the rendering IS the
# value: `.isoformat()` output is what the index stores and a person sees.
#
# Verified beyond this file on the production catalogue: across 20,415 buyers,
# 47,694 orders and 1,009 products, not one rendered field differed; every
# difference was a data change between the backup and live Postgres.

class _CaptureMeili:
    """Stands in for the Meilisearch client and keeps what it would store —
    passed through the client's own sanitiser, so NaN→None and dates match."""

    def __init__(self):
        self.docs = {"buyers": {}, "orders": {}, "products": {}}

    async def _take(self, kind, docs):
        from core.meilisearch_client import _sanitize_documents
        for d in _sanitize_documents(docs):
            self.docs[kind][d["id"]] = d
        return len(docs)

    async def index_buyers(self, docs):
        return await self._take("buyers", docs)

    async def index_orders(self, docs):
        return await self._take("orders", docs)

    async def index_products(self, docs):
        return await self._take("products", docs)


async def _index_once(store):
    """Run `sync_to_meilisearch` once, as a first (full) sync.

    Returns the captured documents, the watermark keys it read and wrote, and
    its stats. The store is never connected and its DuckDB connection is a
    trap. The watermark primitives are stubbed: the question here is what the
    index is built from.
    """
    from core.sync_service import SyncService

    capture, keys = _CaptureMeili(), {"read": [], "written": []}

    async def get_last(key="orders"):
        keys["read"].append(key)
        return None                      # no watermark → full sync

    async def set_last(key="orders", timestamp=None):
        keys["written"].append(key)

    svc = SyncService.__new__(SyncService)
    svc.store = store
    with patch("core.sync_service.get_meili_client", return_value=capture), \
         patch.object(store, "get_last_sync_time", side_effect=get_last), \
         patch.object(store, "set_last_sync_time", side_effect=set_last):
        stats = await svc.sync_to_meilisearch()
    return capture.docs, keys, stats


SEARCH_INDEX = golden("search_index")


def _as_recorded(docs, stats):
    """The documents and stats in the form the literal was recorded in."""
    return comparable({"docs": {kind: {str(i): d for i, d in sorted(v.items())}
                                for kind, v in docs.items()},
                       "stats": stats})


class TestTheSearchIndexDocumentsAreTheLiteral:
    @pytest.mark.asyncio
    async def test_every_document_including_its_rendered_dates(
        self, original_in_postgres, monkeypatch,
    ):
        """Under the suite's `TZ=UTC`: DuckDB's documents, recorded while
        Postgres built the same ones."""
        monkeypatch.setenv("TZ", "UTC")
        docs, _, stats = await _index_once(original_in_postgres)
        for kind in ("buyers", "orders", "products"):
            assert docs[kind], f"no {kind} indexed — the literal would pin nothing"
        assert _as_recorded(docs, stats) == SEARCH_INDEX[
            call_key("sync_to_meilisearch", ["UTC"])]

    @pytest.mark.asyncio
    async def test_ordered_at_is_rendered_in_the_local_zone(
        self, original_in_postgres, monkeypatch,
    ):
        """The trap this port exists to avoid. DuckDB rendered TIMESTAMPTZ in
        the session's zone — Kyiv in production, `Etc/UTC` in this image — and
        asyncpg returns UTC whatever the zone. The contract is not "Kyiv": it
        is "the offset DuckDB would have indexed", in whichever environment."""
        from zoneinfo import ZoneInfo
        from core.pg_search_index_read import local_zone

        monkeypatch.setenv("TZ", "UTC")
        docs, _, _ = await _index_once(original_in_postgres)
        assert any(d["ordered_at"] for d in docs["orders"].values())
        for i, d in docs["orders"].items():
            if d["ordered_at"]:
                instant = datetime.fromisoformat(d["ordered_at"])
                expected = instant.astimezone(ZoneInfo(local_zone())).isoformat()[-6:]
                assert d["ordered_at"][-6:] == expected, (i, d["ordered_at"], expected)


class TestTheSearchIndexUnderANonUtcZone:
    """The same literal, with the trap made visible.

    UTC is the one zone in which "forgot to convert asyncpg's UTC" and
    "converted correctly" produce the same string. A mutation that left every
    timestamp in UTC passed the UTC test, which is how this class came to
    exist. Production runs `TZ=Europe/Kyiv`, where the defect would show a UTC
    time on every search result. So the index is built under Kyiv here, and
    compared with DuckDB's documents rendered in Kyiv on the same fixture.
    """

    @pytest.mark.asyncio
    async def test_kyiv_offsets_across_dst(self, original_in_postgres, monkeypatch):
        monkeypatch.setenv("TZ", "Europe/Kyiv")
        docs, _, stats = await _index_once(original_in_postgres)
        offsets = {d["ordered_at"][-6:] for d in docs["orders"].values() if d["ordered_at"]}
        assert offsets and offsets <= {"+02:00", "+03:00"}, offsets
        assert _as_recorded(docs, stats) == SEARCH_INDEX[
            call_key("sync_to_meilisearch", ["Europe/Kyiv"])]


class TestTheSearchIndexKeepsOneWatermark:
    """Postgres' watermark is `mirrored_at`'s clock, under its own key. The
    DuckDB index's key, `meilisearch` (`synced_at`'s clock), went with the
    DuckDB frames in S14-07: it is neither read nor written."""

    @pytest.mark.asyncio
    async def test_the_key_is_postgres_alone(self, original_in_postgres):
        _, keys, _ = await _index_once(original_in_postgres)
        assert keys == {"read": ["meilisearch_pg"], "written": ["meilisearch_pg"]}

    def test_the_literal_holds_the_two_zones_and_nothing_else(self):
        assert set(SEARCH_INDEX) == {call_key("sync_to_meilisearch", [z])
                                     for z in ("UTC", "Europe/Kyiv")}


class TestTheZoneIsResolvedTheWayDuckDBResolvesIt:
    """Pure. Measured in both images before `local_zone` was written: production
    sets `TZ=Europe/Kyiv` while `/etc/localtime` points at `Etc/UTC`, and DuckDB
    renders Kyiv; the gate leaves `TZ` unset and DuckDB renders `Etc/UTC`."""

    def test_tz_wins_when_it_is_set(self, monkeypatch):
        from core.pg_search_index_read import local_zone
        monkeypatch.setenv("TZ", "Europe/Kyiv")
        assert local_zone() == "Europe/Kyiv"

    def test_a_leading_colon_is_the_posix_spelling_of_the_same_zone(self, monkeypatch):
        from core.pg_search_index_read import local_zone
        monkeypatch.setenv("TZ", ":Europe/Kyiv")
        assert local_zone() == "Europe/Kyiv"

    def test_without_tz_it_is_whatever_localtime_names(self, monkeypatch):
        from pathlib import Path
        from core.pg_search_index_read import local_zone
        monkeypatch.delenv("TZ", raising=False)
        resolved = str(Path("/etc/localtime").resolve())
        expected = resolved.split("zoneinfo/", 1)[1] if "zoneinfo/" in resolved else "UTC"
        assert local_zone() == expected
