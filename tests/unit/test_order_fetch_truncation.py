"""A listing that stops at its page cap says so, and the order window splits (L1-08).

`paginate` and `fetch_all` stopped at `max_pages` (100 pages of 50: KeyCRM's
5 000 rows) without a word. With 5 050 orders in the window, newest first,
the order step wrote 5 000, moved `last_sync_orders` to the newest, and the 50
oldest were never fetched by an incremental tick again. Now both raise
`KeyCRMTruncated` when their last allowed page is full; the order fetch splits
a truncated window in halves down to an hour and merges by id, and a window
still truncated fails the step, which holds the watermark.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from core.exceptions import KeyCRMTruncated
from core.keycrm import KeyCRMClient
from core.sync_service import SyncService

KYIV = ZoneInfo("Europe/Kyiv")
FMT = "%Y-%m-%d %H:%M:%S"


def _orders(n: int, start: datetime, step: timedelta) -> list:
    return [{"id": 2_000_000 + i, "source_id": 1,
             "created_at": (start + step * i).isoformat(),
             "updated_at": (start + step * i).isoformat()} for i in range(n)]


class FakeKeyCRM(KeyCRMClient):
    """KeyCRM's order listing: inclusive date filters, newest first."""

    def __init__(self, orders):
        super().__init__(api_key="test-only")
        self.orders, self.asked = orders, 0

    async def _request(self, method, endpoint, params=None, json=None):
        self.asked += 1
        key = next(k for k in params if k.startswith("filter["))
        a, b = [datetime.strptime(x.strip(), FMT).replace(tzinfo=KYIV)
                for x in params[key].split(",")]
        field = "created_at" if "created" in key else "updated_at"
        hits = sorted((o for o in self.orders
                       if a <= datetime.fromisoformat(o[field]) <= b),
                      key=lambda o: -o["id"])
        page, limit = params["page"], params["limit"]
        return {"data": hits[(page - 1) * limit: page * limit]}


NOW = datetime.now(KYIV).replace(microsecond=0)


class TestTheCaps:
    @pytest.mark.asyncio
    async def test_paginate_raises_on_a_full_last_page(self):
        """Mutation: drop the `else: raise` — 5 000 come back, silently."""
        client = FakeKeyCRM(_orders(5050, NOW - timedelta(days=1), timedelta(seconds=10)))
        params = {"filter[created_between]":
                  f"{(NOW - timedelta(days=2)).strftime(FMT)}, {NOW.strftime(FMT)}"}
        got = []
        with pytest.raises(KeyCRMTruncated):
            async for batch in client.paginate("order", params=params):
                got.extend(batch)
        assert len(got) == 5000

    @pytest.mark.asyncio
    async def test_paginate_ending_short_does_not(self):
        client = FakeKeyCRM(_orders(4999, NOW - timedelta(days=1), timedelta(seconds=10)))
        params = {"filter[created_between]":
                  f"{(NOW - timedelta(days=2)).strftime(FMT)}, {NOW.strftime(FMT)}"}
        got = [o async for batch in client.paginate("order", params=params) for o in batch]
        assert len(got) == 4999

    @pytest.mark.asyncio
    async def test_fetch_all_raises_on_a_full_last_page(self):
        class Full(KeyCRMClient):
            async def _request(self, method, endpoint, params=None, json=None):
                return {"data": [{"id": i} for i in range(50)]}

        with pytest.raises(KeyCRMTruncated):
            await Full(api_key="test-only").fetch_all_stocks(max_pages=3)
        with pytest.raises(KeyCRMTruncated):
            await Full(api_key="test-only").fetch_all("offers", max_pages=1)


def _service(client):
    store = SimpleNamespace(
        get_last_sync_time=AsyncMock(return_value=NOW - timedelta(minutes=1)),
        set_last_sync_time=AsyncMock(),
    )
    svc = SyncService(store=store)  # type: ignore[arg-type]
    written: list = []

    async def upsert(orders, force_update=False, skip_products=False, changed_ids_out=None):
        written.extend(o["id"] for o in orders)
        if changed_ids_out is not None:
            changed_ids_out.extend(o["id"] for o in orders)
        return len(orders), 0

    svc._upsert_orders_with_expenses = upsert
    return svc, store, written


class TestTheOrderWindow:
    @pytest.mark.asyncio
    async def test_a_truncated_window_is_fetched_whole_by_halves(self, monkeypatch):
        """The reproduction: 5 050 orders in the step's window, every one written."""
        import core.sync_service as mod

        monkeypatch.setattr(mod.events, "emit", AsyncMock())
        orders = _orders(5050, NOW - timedelta(hours=20), timedelta(seconds=14))
        svc, store, written = _service(FakeKeyCRM(orders))

        await svc._orders_step(FakeKeyCRM(orders), {"orders": 0, "expenses": 0})

        assert sorted(written) == sorted(o["id"] for o in orders)
        store.set_last_sync_time.assert_awaited()

    @pytest.mark.asyncio
    async def test_still_truncated_at_an_hour_holds_the_watermark(self, monkeypatch):
        import core.sync_service as mod

        monkeypatch.setattr(mod.events, "emit", AsyncMock())
        # Every order at one instant, so no split can divide them: spread
        # over minutes, a halving boundary could fall among them (it moves
        # with the wall clock) and both halves fit, which is a pass by luck.
        orders = _orders(5050, NOW - timedelta(minutes=10), timedelta(0))
        svc, store, written = _service(FakeKeyCRM(orders))

        with pytest.raises(KeyCRMTruncated):
            await svc._orders_step(FakeKeyCRM(orders), {"orders": 0, "expenses": 0})
        assert written == []
        store.set_last_sync_time.assert_not_awaited()
