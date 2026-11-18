"""The DuckDB-only doors the owner retired under OD-10 (2026-09-30) stay retired.

Step 13 stops DuckDB deriving Silver, Gold and the UTM verdicts. Every door
that read those tables with no read switch would, after the flip, answer from
a Silver as old as the switch and after the first Sunday compaction from none —
so each was ported or retired (`core.warehouse_cutover.OD10_DOORS`, which a
walk in `tests/unit/test_warehouse_writer.py` kept equal to what was left,
until step 14's S14-15 deleted the switch, the list and the walk). The owner
retired all of them, in one change. This file is what keeps them retired: a
route that comes back under the same path, a tool the model is offered
again, fails here.
"""
from __future__ import annotations

import asyncio

import pytest

# The assistant's tools that rode the retired detail service.
RETIRED_TOOLS = ("get_buyer_details", "get_order_details")


@pytest.mark.parametrize("path", ["/api", "/api/", "/api/no-such-door",
                                  "/api/admin/retired/thing.json"])
def test_no_unmatched_api_get_gets_the_shell_even_without_a_session(path):
    """Answered before the session check: a 404 says only that no such
    route exists, which `/openapi.json` says to anyone already. Before, an
    anonymous caller was redirected to /login and an admin got the shell."""
    from fastapi.testclient import TestClient

    from web.main import app

    response = TestClient(app).get(path, follow_redirects=False)
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


@pytest.mark.parametrize("name", RETIRED_TOOLS)
def test_the_assistant_cannot_call_it_either(name):
    """A model that remembers a tool from an older conversation is told it
    does not exist, not answered from a service that is gone."""
    from core.chat_tools import execute_tool

    assert asyncio.run(execute_tool(name, {"buyer_id": 1, "order_id": 1})) == {
        "error": f"Unknown tool: {name}"}
