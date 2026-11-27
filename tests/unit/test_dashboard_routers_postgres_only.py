"""Step 14's S14-06: the dashboard asks Postgres and nothing else.

`_dashboard_run` (the doughnut, the returns list, the subcategories, the
three that left `gold_daily_products`, the customer block), the two Gold
primitives (`_pg_gold_summary`, `_pg_gold_series`), the Silver escapes of
`/api/summary` and `/api/revenue/trend` (`_pg_silver`) and the managers
screen's year (`get_manager_sales_365d`) each used to choose an engine by a
`KS_READ_*` switch (`KS_READ_DASHBOARD`, `KS_READ_GOLD`, `KS_READ_SILVER`),
ask `read_fallback.no_address` about it, try Postgres, and on a failure call
`read_fallback.fall_back` — which under `KS_READ_FALLBACK=off`, production's
mode since 2026-10-09 and since S14-00 the only one, refused the read: a 503
naming the surface, counted under `read_fallback_mode.refused`. The DuckDB
tails below that call were reachable by nothing in production.

So each keeps exactly the path production took: the Postgres read, and on any
exception `raise read_fallback.refuse(surface, exc)`. What is held here,
without a database:

- the switch is not read — unset, `duckdb`, a typo or junk, the read goes to
  Postgres and nothing reaches for the DuckDB store (step 13's readiness on
  `/api/warehouse/status` read the three variables, as `reader:*` keys,
  until S14-15 deleted it);
- a failure is the refusal `fall_back` made under `off`: the surface, the
  count, the log line, the cause, and no fallback counted;
- without `KS_PG_DSN` the pool's own refusal to guess a database is that
  failure, so the answer is the same 503 — `no_address` named the switch in
  the log, the cause names the DSN now;
- junk in any of the three variables starts web and imports every module
  (rule 3 of the plan);
- the shape of what the card row and the chart receive: the summary's seven
  keys, and the trend's labels, series, datasets, comparison and measure.

The numbers are `tests/integration/test_dashboard_two_engines.py`'s, against a
real Postgres and literals DuckDB answered before its side went; every route
through the real app, a failing engine answered 503 naming its tab, is
`tests/unit/test_read_fallback_http.py`'s — with each route's default
parameters. Every reader of `core/repositories/revenue.py` and the customer
block, in every branch the dashboard asks it, refuses naming its surface
while Postgres is down: `TestEveryReadRefusesWhilePostgresIsDown`, below. A
reader that reached an engine round its router would let the engine's own
error through instead, which the route answers 500 and nobody counts.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from core import read_fallback

D1, D2 = date(2026, 8, 1), date(2026, 8, 3)


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A store that is never connected, whose connection is a trap: nothing
    here may reach for DuckDB, under any value of any switch."""
    from core.store import AnalyticsStore

    store = AnalyticsStore()

    def trap(*_a, **_k):
        raise AssertionError("a Postgres-only dashboard read reached for DuckDB")

    # `_fetch_one`/`_fetch_all` went with the store's DuckDB half in step
    # 14's S14-18 and `connection()` is a refusal since; trapped all the same,
    # so a reader that reaches for any of them fails here by name.
    for name in ("connection", "_fetch_one", "_fetch_all"):
        monkeypatch.setattr(type(store), name, trap, raising=False)
    read_fallback.reset_counts()
    yield store
    read_fallback.reset_counts()
    assert not (tmp_path / "never.duckdb").exists()


@pytest.fixture
def postgres_down(monkeypatch):
    """Every Postgres read fails where it asks for the pool — a router's and
    any other — as with the database gone."""
    monkeypatch.setattr("core.pg.get_pool",
                        AsyncMock(side_effect=OSError("postgres down")))


def _the_category_tree_answers(monkeypatch):
    """The filter bar's read answers, so a category filter reaches the read
    behind it."""
    monkeypatch.setattr("core.pg_lookups_read.fetch", AsyncMock(return_value=[(1,)]))


# The dashboard's readers in every branch the dashboard asks them: the calls
# of `tests/integration/test_dashboard_two_engines.py` (copied — a test module
# imports no other), less the summary's three that answer zeros without a
# read (`NO_READ`, below), and with the filters the brand and customer blocks
# take since 2026-11-27 and the monthly granularity.
DASHBOARD_READS = (
    ("get_sales_by_source", {}),
    ("get_sales_by_source", {"sales_type": "all"}),
    ("get_sales_by_source", {"sales_type": "b2b"}),
    ("get_sales_by_source", {"sales_type": "internal"}),
    ("get_sales_by_source", {"category_id": 1}),
    ("get_sales_by_source", {"category_id": 99999}),
    ("get_sales_by_source", {"brand": "BrandA"}),
    ("get_sales_by_source", {"brand": "Unknown"}),
    ("get_sales_by_source", {"category_id": 1, "brand": "BrandA"}),
    ("get_sales_by_source", {"promocode": "SAVE10"}),
    ("get_return_orders", {}),
    ("get_return_orders", {"sales_type": "all"}),
    ("get_return_orders", {"sales_type": "b2b"}),
    ("get_return_orders", {"limit": 1}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Makeup"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Nope"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "brand": "BrandA"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "source_id": 1}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "promocode": "SAVE10"}),
    ("get_subcategory_breakdown", {"parent_category_name": "Care", "sales_type": "all"}),
    ("get_top_products", {}),
    ("get_top_products", {"category_id": 1}),
    ("get_top_products", {"brand": "BrandA"}),
    ("get_top_products", {"limit": 2}),
    ("get_top_products", {"promocode": "NOPE"}),
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
    ("get_brand_analytics", {"category_id": 1}),
    ("get_brand_analytics", {"brand": "BrandA"}),
    ("get_brand_analytics", {"promocode": "SAVE10"}),
    ("get_customer_insights", {}),
    ("get_customer_insights", {"sales_type": "all"}),
    ("get_customer_insights", {"sales_type": "b2b"}),
    ("get_customer_insights", {"sales_type": "internal"}),
    ("get_customer_insights", {"promocode": "NOPE"}),
    ("get_customer_insights", {"promocode": "SAVE10"}),
    ("get_customer_insights", {"source_id": 1, "brand": "BrandA"}),
    ("get_customer_insights", {"category_id": 1}),
    ("get_customer_insights", {"granularity": "monthly"}),
    ("get_summary_stats", {}),
    ("get_summary_stats", {"sales_type": "all"}),
    ("get_summary_stats", {"sales_type": "b2b"}),
    ("get_summary_stats", {"sales_type": "internal"}),
    ("get_summary_stats", {"source_id": 1}),
    ("get_summary_stats", {"source_id": 2}),
    ("get_summary_stats", {"source_id": 4}),
    ("get_summary_stats", {"source_id": 1, "sales_type": "all"}),
    ("get_summary_stats", {"source_id": 2, "sales_type": "b2b"}),
    ("get_summary_stats", {"source_id": 3, "brand": "Unknown"}),
    ("get_summary_stats", {"category_id": 1}),
    ("get_summary_stats", {"category_id": 4}),
    ("get_summary_stats", {"category_id": 99999}),
    ("get_summary_stats", {"brand": "BrandA"}),
    ("get_summary_stats", {"brand": "Unknown"}),
    ("get_summary_stats", {"promocode": "NOPE"}),
    ("get_summary_stats", {"promocode": "SAVE10"}),
    ("get_summary_stats", {"brand": "BrandA", "source_id": 1}),
    ("get_summary_stats", {"category_id": 1, "sales_type": "all"}),
    ("get_summary_stats", {"brand": "BrandA", "sales_type": "b2b"}),
    ("get_summary_stats", {"promocode": "SAVE10", "source_id": 5, "sales_type": "all"}),
    ("get_revenue_trend", {}),
    ("get_revenue_trend", {"sales_type": "all"}),
    ("get_revenue_trend", {"sales_type": "b2b"}),
    ("get_revenue_trend", {"sales_type": "internal"}),
    ("get_revenue_trend", {"source_id": 1}),
    ("get_revenue_trend", {"source_id": 4, "sales_type": "all"}),
    ("get_revenue_trend", {"source_id": 3}),
    ("get_revenue_trend", {"source_id": 5}),
    ("get_revenue_trend", {"source_id": 5, "sales_type": "all"}),
    ("get_revenue_trend", {"compare_type": "month_ago"}),
    ("get_revenue_trend", {"compare_type": "year_ago"}),
    ("get_revenue_trend", {"include_comparison": False}),
    ("get_revenue_trend", {"granularity": "monthly"}),
    ("get_revenue_trend", {"category_id": 1}),
    ("get_revenue_trend", {"brand": "BrandA"}),
    ("get_revenue_trend", {"brand": "Unknown"}),
    ("get_revenue_trend", {"promocode": "NOPE"}),
    ("get_revenue_trend", {"promocode": "SAVE10"}),
    ("get_revenue_trend", {"brand": "BrandA", "promocode": "SAVE10"}),
    ("get_revenue_trend", {"brand": "BrandA", "source_id": 1}),
    ("get_revenue_trend", {"category_id": 1, "sales_type": "all"}),
    ("get_revenue_trend", {"brand": "BrandA", "compare_type": "year_ago"}),
    ("get_revenue_trend", {"brand": "BrandA", "include_comparison": False}),
    ("get_revenue_trend", {"promocode": "SAVE10", "compare_type": "month_ago"}),
)

# The summary for a source Gold is not asked about (Opencart, the exhibition)
# is zeros by the owner's decision 5, read from nowhere.
NO_READ = ({"source_id": 3}, {"source_id": 5}, {"source_id": 5, "sales_type": "all"})

# The file's readers for the other surfaces it serves: the filter bar, the
# managers screen, `/reports` and `/marketing`.
OTHER_READS = (
    ("get_categories", (), {}, "lookups"),
    ("get_child_categories", (1,), {}, "lookups"),
    ("get_brands", (), {}, "lookups"),
    ("get_promocodes", (), {}, "lookups"),
    ("get_manager_sales_365d", (), {}, "managers"),
    ("get_report_summary", (D1, D2), {}, "reports"),
    ("get_report_summary", (D1, D2), {"brand": "BrandA"}, "reports"),
    ("get_report_summary", (D1, D2), {"category_id": 1, "source_id": 1}, "reports"),
    ("get_report_top_products", (D1, D2), {}, "reports"),
    ("get_report_top_products", (D1, D2), {"category_id": 1}, "reports"),
    ("get_report_products_by_source", (D1, D2), {}, "reports"),
    ("get_report_products_by_source", (D1, D2), {"category_id": 1}, "reports"),
    ("get_promocode_analytics", (D1, D2), {}, "marketing"),
    ("get_promocode_analytics", (D1, D2), {"sales_type": "all"}, "marketing"),
    ("get_marketing_report_by_dates", (D1, D2), {}, "marketing"),
    ("get_marketing_report_by_dates", (D1, D2), {"include_comparison": False}, "marketing"),
    ("get_marketing_report_by_dates", (D1, D2), {"brand": "BrandA"}, "marketing"),
    ("get_marketing_report_by_dates", (D1, D2), {"category_id": 1}, "marketing"),
)


def _call_id(name, kwargs) -> str:
    return f"{name}{tuple(kwargs.items()) if kwargs else ''}"


class TestEveryReadRefusesWhilePostgresIsDown:
    """A failure is the refusal `fall_back` made under `off`: a
    `ReadUnavailable` naming the surface — which the route answers 503 — and
    counted under that surface. A reader that reached Postgres round its
    router would let the pool's `OSError` through instead: the 500 no one
    counts that the merge review's MD2 found, a call made before a helper's
    `try` on `/api/summary?source_id=2`."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,kwargs", DASHBOARD_READS,
                             ids=[_call_id(n, k) for n, k in DASHBOARD_READS])
    async def test_the_dashboard_refuses_naming_itself(
        self, store, postgres_down, monkeypatch, method, kwargs,
    ):
        _the_category_tree_answers(monkeypatch)

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await getattr(store, method)(D1, D2, **kwargs)

        assert raised.value.surface == "dashboard"
        assert set(read_fallback.refusals()) == {"dashboard"}

    CATEGORY_READS = (
        [(n, (D1, D2), k) for n, k in DASHBOARD_READS if "category_id" in k]
        + [(n, a, k) for n, a, k, _s in OTHER_READS if "category_id" in k])

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,args,kwargs", CATEGORY_READS,
                             ids=[_call_id(n, k) for n, _a, k in CATEGORY_READS])
    async def test_a_category_filter_is_refused_by_the_filter_bar_first(
        self, store, postgres_down, method, args, kwargs,
    ):
        """The tree under a category is the filter bar's read, made before the
        tab's own: down with the rest, it is that refusal the page gets."""
        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await getattr(store, method)(*args, **kwargs)

        assert raised.value.surface == "lookups"
        assert set(read_fallback.refusals()) == {"lookups"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kwargs", NO_READ, ids=[_call_id("", k) for k in NO_READ])
    async def test_a_source_gold_is_not_asked_about_reads_nothing(
        self, store, postgres_down, kwargs,
    ):
        out = await store.get_summary_stats(D1, D2, **kwargs)

        assert (out["totalOrders"], out["totalRevenue"], out["totalReturns"]) == (0, 0, 0)
        assert read_fallback.refusals() == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method,args,kwargs,surface", OTHER_READS,
                             ids=[_call_id(n, k) for n, _a, k, _s in OTHER_READS])
    async def test_the_file_s_other_readers_refuse_naming_theirs(
        self, store, postgres_down, monkeypatch, method, args, kwargs, surface,
    ):
        if surface != "lookups":
            _the_category_tree_answers(monkeypatch)

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await getattr(store, method)(*args, **kwargs)

        assert raised.value.surface == surface
        assert set(read_fallback.refusals()) == {surface}


SUMMARY_KEYS = {"totalOrders", "totalRevenue", "avgCheck", "totalReturns",
                "returnsRevenue", "startDate", "endDate"}
DATASET_KEYS = {"label", "data", "borderColor", "backgroundColor", "fill",
                "tension", "borderWidth"}


class TestTheShape:
    """What the card row and the chart receive, held apart from the numbers:
    one key gone or renamed and the frontend draws a blank, with no number
    in any literal changing to say so."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kwargs", [{}, {"source_id": 1}, {"source_id": 3},
                                        {"brand": "BrandA"}, {"category_id": 1}],
                             ids=["gold", "gold_source", "zeros", "brand", "category"])
    async def test_the_summary_has_its_seven_keys(self, store, monkeypatch, kwargs):
        monkeypatch.setattr("core.pg_gold_read.fetch_summary",
                            AsyncMock(return_value=(4, 1000.0, 1, 50.0)))
        monkeypatch.setattr("core.pg_silver_read.fetch",
                            AsyncMock(return_value=[(4, 1000.0)]))
        monkeypatch.setattr("core.pg_lookups_read.fetch",
                            AsyncMock(return_value=[(1,)]))
        out = await store.get_summary_stats(D1, D2, **kwargs)
        assert set(out) == SUMMARY_KEYS
        assert (out["startDate"], out["endDate"]) == ("2026-08-01", "2026-08-03")
        assert all(isinstance(out[k], int) for k in ("totalOrders", "totalReturns"))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kwargs,measure", [
        ({}, "revenue"), ({"source_id": 5}, "revenue"), ({"promocode": "X"}, "revenue"),
        ({"brand": "BrandA"}, "goods_value"), ({"category_id": 1}, "goods_value"),
    ], ids=["gold", "order_grain_source", "order_grain_promocode", "brand", "category"])
    async def test_the_trend_with_its_comparison(self, store, monkeypatch, kwargs, measure):
        rows = [(D1, 100.0, 1), (D2, 50.0, 2)]
        monkeypatch.setattr("core.pg_gold_read.fetch_series", AsyncMock(return_value=rows))
        monkeypatch.setattr("core.pg_silver_read.fetch", AsyncMock(return_value=rows))
        monkeypatch.setattr("core.pg_lookups_read.fetch", AsyncMock(return_value=[(1,)]))

        out = await store.get_revenue_trend(D1, D2, **kwargs)

        assert set(out) == {"labels", "revenue", "orders", "datasets", "measure",
                            "comparison"}
        assert out["measure"] == measure
        assert out["labels"] == ["01.08", "02.08", "03.08"]
        assert out["revenue"] == [100.0, 0, 50.0] and out["orders"] == [1, 0, 2]
        this, previous = out["datasets"]
        assert set(this) == DATASET_KEYS and this["label"] == "This Period"
        assert set(previous) == DATASET_KEYS | {"borderDash"}
        assert previous["label"] == "Previous Period"
        comparison = out["comparison"]
        assert set(comparison) == {"labels", "revenue", "orders", "period", "totals"}
        assert comparison["period"] == {"start": "2026-07-29", "end": "2026-07-31",
                                        "type": "previous_period"}
        assert set(comparison["totals"]) == {"current", "previous", "growth_percent"}

    @pytest.mark.asyncio
    async def test_the_trend_without_its_comparison_asks_one_period(
        self, store, monkeypatch,
    ):
        series = AsyncMock(return_value=[])
        monkeypatch.setattr("core.pg_gold_read.fetch_series", series)
        out = await store.get_revenue_trend(D1, D2, include_comparison=False)
        assert set(out) == {"labels", "revenue", "orders", "datasets", "measure"}
        assert len(out["datasets"]) == 1
        assert series.await_count == 1
