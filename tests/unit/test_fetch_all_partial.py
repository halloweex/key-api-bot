"""A listing with a page missing is a failed listing, not a shorter one (L1-09).

`KeyCRMClient.fetch_all` asks for the pages after the first in parallel and
used to log a page that failed and carry on: 900 stocks with page 3 refused
(429, after its retries) came back as 850 stocks, and the stocks step wrote
them, rebuilt the status from them and moved `last_sync_stocks` as if the
catalogue were complete. The caller cannot tell a hole from a shorter list, so
the fetch now raises `KeyCRMConnectionError`, which every caller already
handles as "KeyCRM failed, try again".
"""
from __future__ import annotations

import asyncio

import pytest

from core.exceptions import KeyCRMConnectionError
from core.keycrm import KeyCRMClient


class _Pages(KeyCRMClient):
    """`total` items, 50 a page; `failing` pages raise as `_request` does
    after its own retries."""

    def __init__(self, total: int, failing=(), error=None):
        super().__init__(api_key="test-only")
        self.total, self.failing = total, set(failing)
        self.error = error or KeyCRMConnectionError("API returned 429")
        self.asked = []

    async def _request(self, method, endpoint, params=None, json=None):
        page = params["page"]
        self.asked.append(page)
        if page in self.failing:
            raise self.error
        start = (page - 1) * 50
        return {"data": [{"id": i, "quantity": 1}
                         for i in range(start, min(start + 50, self.total))]}


class TestAHoleInTheListing:
    @pytest.mark.asyncio
    async def test_a_failed_page_before_the_last_fails_the_fetch(self):
        """Mutation: `continue` past the failure as before — 850 come back."""
        client = _Pages(900, failing={3})
        with pytest.raises(KeyCRMConnectionError) as err:
            await client.fetch_all_stocks()
        assert "offers/stocks" in str(err.value)

    @pytest.mark.asyncio
    async def test_any_exception_class_fails_it(self):
        client = _Pages(900, failing={7}, error=RuntimeError("boom"))
        with pytest.raises(KeyCRMConnectionError):
            await client.fetch_all_offers()

    @pytest.mark.asyncio
    async def test_a_complete_listing_still_comes_back_whole(self):
        assert len(await _Pages(900).fetch_all_stocks()) == 900

    @pytest.mark.asyncio
    async def test_a_failure_past_the_end_costs_nothing(self):
        """900 items end on page 18 (short); page 20 is asked in the same
        parallel batch and has nothing to give."""
        client = _Pages(890, failing={20})
        got = await client.fetch_all_stocks()
        assert len(got) == 890 and 20 in client.asked

    @pytest.mark.asyncio
    async def test_a_cancellation_is_not_turned_into_a_keycrm_error(self):
        client = _Pages(900, failing={4}, error=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await client.fetch_all_stocks()
