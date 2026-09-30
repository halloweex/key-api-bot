"""The DuckDB-only doors the owner retired under OD-10 (2026-09-30) stay retired.

Step 13 stops DuckDB deriving Silver, Gold and the UTM verdicts. Every door
that read those tables with no read switch would, after the flip, answer from
a Silver as old as the switch and after the first Sunday compaction from none —
so each was ported or retired (`core.warehouse_cutover.OD10_DOORS`, which the
walk in `tests/unit/test_warehouse_writer.py` keeps equal to what is left).
The owner retired all of them, in one change. This file is what keeps them
retired: a route that comes back under the same path, a tool the model is
offered again, fails here — and the walk would put the door back on the list
the switch waits for.
"""
from __future__ import annotations

import asyncio

import pytest

from tests.routes_helper import iter_endpoints

# (method, path) of every retired HTTP door.
RETIRED_ROUTES = (
    # Never answered anything but 404 for a real id: `dict()` of a DuckDB
    # tuple since 2026-02-06 (and `op.sku`, a column order_products lacks).
    ("GET", "/api/buyers/{buyer_id}"),
    ("GET", "/api/orders/{order_id}"),
    ("GET", "/api/products/{product_id}"),
    # DuckDB Bronze against DuckDB Silver, with no caller; the KeyCRM half of
    # the second raised on every stored order since the day it was written.
    ("GET", "/api/debug/stale-returns"),
    ("GET", "/api/debug/order-status/{order_id}"),
    # DuckDB buyer counters with no caller; "all synced" after a compaction.
    ("GET", "/api/buyers/stats"),
)

# The assistant's tools that rode the retired detail service.
RETIRED_TOOLS = ("get_buyer_details", "get_order_details")


@pytest.fixture(scope="module")
def registered():
    from web.main import app

    return {(method, endpoint.path)
            for endpoint in iter_endpoints(app)
            for method in (endpoint.methods or ())}


@pytest.mark.parametrize("method, path", RETIRED_ROUTES)
def test_the_route_is_not_registered(registered, method, path):
    assert (method, path) not in registered, f"{method} {path} is back"


def test_the_walk_sees_routes_at_all(registered):
    """A helper that found nothing would pass every assertion above."""
    assert ("GET", "/api/search/buyers") in registered
    assert ("GET", "/api/health") in registered


@pytest.mark.parametrize("name", RETIRED_TOOLS)
def test_the_assistant_is_not_offered_the_tool(name):
    from core.chat_tools import TOOLS

    assert name not in {tool["name"] for tool in TOOLS}


@pytest.mark.parametrize("name", RETIRED_TOOLS)
def test_the_assistant_cannot_call_it_either(name):
    """A model that remembers a tool from an older conversation is told it
    does not exist, not answered from a service that is gone."""
    from core.chat_tools import execute_tool

    assert asyncio.run(execute_tool(name, {"buyer_id": 1, "order_id": 1})) == {
        "error": f"Unknown tool: {name}"}


def test_the_search_tools_stay():
    """OD-10 retired the detail tools; the Meilisearch searches are not doors."""
    from core.chat_tools import TOOLS

    names = {tool["name"] for tool in TOOLS}
    assert {"search_buyer", "search_order", "search_product"} <= names


def test_the_search_service_holds_no_detail_readers():
    from web.services.search_service import SearchService

    for name in ("get_buyer_details", "get_order_details", "get_product_details"):
        assert not hasattr(SearchService, name), name
