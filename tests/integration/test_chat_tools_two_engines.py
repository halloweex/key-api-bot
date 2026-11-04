"""The chat assistant's five numeric tools answer in Postgres what DuckDB did.

Reuses the dashboard's original catalogue — orders across three sources, a
retired source, returns, b2b, line items under two names — because these tools
ask the dashboard's questions in a different voice. Postgres derives Silver and
Gold from it, roll-up and fine rows both, which is what makes the trap real: a
bare `SUM(revenue)` over that Gold counts every order twice.

ONE ENGINE SINCE STEP 14 (S14-07). The tools read Postgres alone
(`core.chat_tools._chat_run`). Each call is compared with a literal
(`tests/integration/golden/chat_tools.json`): DuckDB's answer on this
fixture, pinned to 2026-09-15, recorded while the same run asserted Postgres
answered each one equally (rule 9 of the step-14 plan). The store is never
connected and its DuckDB connection is a trap.
"""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from tests.integration.pg_tab_seed import call_key, golden
from tests.integration.test_dashboard_two_engines import (  # noqa: F401 — fixture
    DSN, WINDOW, _comparable, original_in_postgres,
)

pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

GOLDEN = golden("chat_tools")


def _iso(args):
    return [a.isoformat() if hasattr(a, "isoformat") else a for a in args]


async def _answer(store, tool, *args):
    """One tool's answer, on the pinned window, and its literal."""
    from core import chat_tools

    fn = getattr(chat_tools, tool)
    with patch.object(chat_tools, "get_store", new=AsyncMock(return_value=store)), \
         patch.object(chat_tools, "_get_date_range", return_value=WINDOW):
        answer = await fn(*args)
    return answer, GOLDEN[call_key(tool, _iso(args))]


class TestTheTotals:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_period_summary(self, original_in_postgres, sales_type):
        answer, recorded = await _answer(
            original_in_postgres, "_get_revenue_summary", "month", sales_type)
        assert answer["total_revenue"] > 0, "nothing in the window — the literal pins nothing"
        assert _comparable(answer) == recorded

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_dated_summary(self, original_in_postgres, sales_type):
        start, end = (d.isoformat() for d in WINDOW)
        answer, recorded = await _answer(
            original_in_postgres, "_get_revenue_by_dates", start, end, sales_type)
        assert answer["total_orders"] > 0
        assert _comparable(answer) == recorded

    @pytest.mark.asyncio
    async def test_the_roll_up_is_not_counted_twice(self, original_in_postgres):
        """Stated outright rather than left inside an equality: the number is
        the retail revenue the catalogue actually holds."""
        answer, _ = await _answer(
            original_in_postgres, "_get_revenue_summary", "month", "retail")
        # Retail, not returned, active source: orders 1 (₴1200), 2 (₴600), 5 (₴750).
        assert answer["total_revenue"] == 2550.0
        assert answer["total_orders"] == 3

    @pytest.mark.asyncio
    async def test_the_date_range_comparison(self, original_in_postgres):
        start, end = WINDOW
        mid = start + (end - start) / 2
        args = (start.isoformat(), mid.isoformat(),
                (mid + timedelta(days=1)).isoformat(), end.isoformat())
        answer, recorded = await _answer(original_in_postgres, "_compare_date_ranges", *args)
        assert _comparable(answer) == recorded


class TestTheBreakdowns:
    @pytest.mark.asyncio
    async def test_the_channels(self, original_in_postgres):
        answer, recorded = await _answer(original_in_postgres, "_get_source_breakdown", "month")
        assert [s["name"] for s in answer["sources"]] == ["Instagram", "Telegram", "Shopify"]
        assert any(s["revenue"] for s in answer["sources"])
        assert _comparable(answer) == recorded

    @pytest.mark.asyncio
    async def test_the_customers(self, original_in_postgres):
        answer, recorded = await _answer(original_in_postgres, "_get_customer_insights", "month")
        assert answer["total_customers"] > 0
        assert _comparable(answer) == recorded

    @pytest.mark.asyncio
    @pytest.mark.parametrize("by", ["revenue", "quantity"])
    @pytest.mark.parametrize("limit", [1, 3, 10])
    async def test_the_top_products(self, original_in_postgres, by, limit):
        answer, recorded = await _answer(
            original_in_postgres, "_get_top_products", "month", by, limit)
        assert answer["products"]
        assert _comparable(answer) == recorded

    @pytest.mark.asyncio
    async def test_a_tie_at_the_limit_is_broken_on_the_product(self, original_in_postgres):
        """Ampoule and Toner both took ₴200. Third place is one of them, and
        before the tiebreak which one was up to the engine."""
        answer, _ = await _answer(
            original_in_postgres, "_get_top_products", "month", "revenue", 3)
        assert [p["name"] for p in answer["products"]][2] == "Ampoule"


def test_every_literal_is_asked():
    asked = {k.split("[", 1)[0] for k in GOLDEN}
    assert asked == {"_get_revenue_summary", "_get_revenue_by_dates",
                     "_compare_date_ranges", "_get_source_breakdown",
                     "_get_customer_insights", "_get_top_products"}
