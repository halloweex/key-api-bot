"""The DuckDB-only doors the owner retired under OD-10 (2026-09-30) stay retired.

Step 13 stops DuckDB deriving Silver, Gold and the UTM verdicts. Every door
that read those tables with no read switch would, after the flip, answer from
a Silver as old as the switch and after the first Sunday compaction from none —
so each was ported or retired (`core.warehouse_cutover.OD10_DOORS`, which a
walk in `tests/unit/test_warehouse_writer.py` kept equal to what was left,
until step 14's S14-15 deleted the switch, the list and the walk). The owner
retired all of them, in one change. This file is what keeps them retired: a
route that comes back under the same path fails here. The two assistant
tools that rode the retired detail service went with it; a model that still
names one is answered as for any tool that does not exist.
"""
from __future__ import annotations

import asyncio

import pytest

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


def test_a_tool_that_does_not_exist_is_an_error_not_an_exception():
    """A model naming a tool it is not offered — one it remembers from an
    older conversation — is answered with an error it can read, and the
    conversation goes on."""
    from core.chat_tools import execute_tool

    result = asyncio.run(execute_tool("no_such_tool", {"buyer_id": 1}))
    assert set(result) == {"error"}
