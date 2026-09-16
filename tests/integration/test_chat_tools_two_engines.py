"""The chat assistant's five numeric tools mean the same thing in both engines.

Reuses the dashboard's two-engine catalogue — orders across three sources, a
retired source, returns, b2b, line items under two names — because these tools
ask the dashboard's questions in a different voice. Postgres Gold is derived by
the production path there, roll-up and fine rows both, which is what makes the
trap real: a bare `SUM(revenue)` over it counts every order twice.

The Postgres leg runs with `DuckDBStore.connection` fatal. There is no fallback
in `_chat_run`, so this is belt and braces — it stays because the day somebody
adds one, a comparison that silently compared DuckDB with itself is exactly what
would ship.
"""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from tests.integration.test_dashboard_two_engines import (  # noqa: F401 — fixture
    DSN, WINDOW, _comparable, both_engines,
)

pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


async def _both(store, monkeypatch, tool, *args):
    from core import chat_tools
    from core.duckdb_store import DuckDBStore

    fn = getattr(chat_tools, tool)
    with patch.object(chat_tools, "get_store", new=AsyncMock(return_value=store)), \
         patch.object(chat_tools, "_get_date_range", return_value=WINDOW):
        monkeypatch.delenv("KS_READ_CHAT", raising=False)
        duck = await fn(*args)

        monkeypatch.setenv("KS_READ_CHAT", "postgres")

        def _no_duckdb(*_a, **_k):
            raise AssertionError(f"{tool} reached DuckDB under KS_READ_CHAT=postgres")

        with patch.object(DuckDBStore, "connection", _no_duckdb):
            postgres = await fn(*args)
        monkeypatch.delenv("KS_READ_CHAT", raising=False)
    return duck, postgres


class TestTheTotalsAgree:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_period_summary(self, both_engines, monkeypatch, sales_type):
        duck, postgres = await _both(
            both_engines, monkeypatch, "_get_revenue_summary", "month", sales_type)
        assert duck["total_revenue"] > 0, "nothing in the window — the comparison is empty"
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_dated_summary(self, both_engines, monkeypatch, sales_type):
        start, end = (d.isoformat() for d in WINDOW)
        duck, postgres = await _both(
            both_engines, monkeypatch, "_get_revenue_by_dates", start, end, sales_type)
        assert duck["total_orders"] > 0
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    async def test_the_roll_up_is_not_counted_twice(self, both_engines, monkeypatch):
        """Stated outright rather than left inside an equality: the number is
        the retail revenue the catalogue actually holds, in both engines."""
        _duck, postgres = await _both(
            both_engines, monkeypatch, "_get_revenue_summary", "month", "retail")
        # Retail, not returned, active source: orders 1 (₴1200), 2 (₴600), 5 (₴750).
        assert postgres["total_revenue"] == 2550.0
        assert postgres["total_orders"] == 3

    @pytest.mark.asyncio
    async def test_the_date_range_comparison(self, both_engines, monkeypatch):
        start, end = WINDOW
        mid = start + (end - start) / 2
        args = (start.isoformat(), mid.isoformat(),
                (mid + timedelta(days=1)).isoformat(), end.isoformat())
        duck, postgres = await _both(
            both_engines, monkeypatch, "_compare_date_ranges", *args)
        assert _comparable(duck) == _comparable(postgres)


class TestTheBreakdownsAgree:
    @pytest.mark.asyncio
    async def test_the_channels(self, both_engines, monkeypatch):
        duck, postgres = await _both(
            both_engines, monkeypatch, "_get_source_breakdown", "month")
        assert [s["name"] for s in duck["sources"]] == ["Instagram", "Telegram", "Shopify"]
        assert any(s["revenue"] for s in duck["sources"])
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    async def test_the_customers(self, both_engines, monkeypatch):
        duck, postgres = await _both(
            both_engines, monkeypatch, "_get_customer_insights", "month")
        assert duck["total_customers"] > 0
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("by", ["revenue", "quantity"])
    @pytest.mark.parametrize("limit", [1, 3, 10])
    async def test_the_top_products(self, both_engines, monkeypatch, by, limit):
        duck, postgres = await _both(
            both_engines, monkeypatch, "_get_top_products", "month", by, limit)
        assert duck["products"]
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    async def test_a_tie_at_the_limit_is_broken_the_same_way(self, both_engines, monkeypatch):
        """Ampoule and Toner both took ₴200. Third place is one of them, and
        before the tiebreak which one was up to the engine."""
        duck, postgres = await _both(
            both_engines, monkeypatch, "_get_top_products", "month", "revenue", 3)
        names = [p["name"] for p in postgres["products"]]
        assert names == [p["name"] for p in duck["products"]]
        assert names[2] == "Ampoule"
