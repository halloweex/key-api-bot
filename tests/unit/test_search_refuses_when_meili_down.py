"""A Meilisearch that does not answer is a counted refusal, not "nothing found" (L3-04).

The three searches answered [] on any exception, so with Meilisearch down
the admin search answered 200 `{buyers: [], orders: [], products: [],
total_hits: 0}` — after 15.3 s on an unroutable host, through a client with
no timeout — and nothing was counted. They raise `MeiliUnavailable` now,
through a client bounded at `SEARCH_TIMEOUT_S`; the routes refuse it as a
503 naming `search` (counted on /api/health's `read_fallback_mode.refused`),
and the assistant's tools answer it inside the conversation as
`data_unavailable`.
"""
from __future__ import annotations

import asyncio
import socket
import time

import pytest
from fastapi.testclient import TestClient

from core import read_fallback
from core.meilisearch_client import SEARCH_TIMEOUT_S, MeiliClient, MeiliUnavailable

DOWN = "http://127.0.0.1:1"          # nothing listens: refused at once


@pytest.fixture
def meili_down(monkeypatch):
    client = MeiliClient(url=DOWN, master_key="test-only")
    monkeypatch.setattr("core.meilisearch_client._meili_client", client)
    monkeypatch.setattr("web.services.search_service._search_service", None)
    read_fallback.reset_counts()
    yield client
    read_fallback.reset_counts()


class TestTheClient:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["search_buyers", "search_orders", "search_products"])
    async def test_a_search_raises_rather_than_answering_nothing(self, meili_down, method):
        """Mutation: `return []` in the handler again."""
        with pytest.raises(MeiliUnavailable):
            await getattr(meili_down, method)("abc", 5)

    @pytest.mark.asyncio
    async def test_a_host_that_never_answers_costs_a_search_its_bound_and_no_more(self):
        """L3-04 as it happened: the connection is taken and nothing comes
        back. The search gives up at its bound and says so; a writer beside
        it, which nobody is waiting on, is not cut off at a search's bound.
        Mutation: build the search client without its timeout (the 15.3 s)."""
        silent = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        silent.bind(("127.0.0.1", 0))
        silent.listen(8)  # the kernel takes the connections; nothing reads them
        host, port = silent.getsockname()
        client = MeiliClient(url=f"http://{host}:{port}", master_key="test-only")
        writer = asyncio.create_task(client.index_buyers([{"id": 1, "full_name": "x"}]))
        try:
            started = time.monotonic()
            with pytest.raises(MeiliUnavailable):
                await asyncio.wait_for(client.search_buyers("abc", 5), SEARCH_TIMEOUT_S + 5)
            assert time.monotonic() - started < SEARCH_TIMEOUT_S + 1
            await asyncio.sleep(0.5)  # past the bound for the writer too
            assert not writer.done(), "the writer gave up at a search's bound"
        finally:
            silent.close()  # what is still waiting on it is reset and returns
            await asyncio.wait_for(asyncio.gather(writer, return_exceptions=True), 10)


@pytest.fixture
def admin(monkeypatch):
    from tests.helpers.sms_api import _headers
    from web.ratelimit import limiter

    limiter.reset()
    yield _headers()
    limiter.reset()


class TestTheRoutes:
    @pytest.mark.parametrize("path", [
        "/api/search?q=abc", "/api/search/buyers?q=abc",
        "/api/search/orders?q=abc", "/api/search/products?q=abc"])
    def test_a_503_naming_search_and_counted(self, meili_down, admin, path):
        from web.main import app

        started = time.monotonic()
        response = TestClient(app, raise_server_exceptions=False).get(path, headers=admin)
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "search"
        assert read_fallback.refusals()["search"]["count"] == 1
        assert time.monotonic() - started < SEARCH_TIMEOUT_S + 2

    def test_still_admin_only(self, meili_down):
        from web.main import app

        response = TestClient(app, raise_server_exceptions=False).get("/api/search?q=abc")
        assert response.status_code in (401, 403)


class TestTheAssistant:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool", ["search_buyer", "search_order", "search_product"])
    async def test_its_tools_answer_data_unavailable(self, meili_down, tool):
        from core.chat_tools import DATA_UNAVAILABLE, execute_tool

        result = await execute_tool(tool, {"query": "abc", "limit": 5})
        assert result["error"] == DATA_UNAVAILABLE and result["surface"] == "search"
