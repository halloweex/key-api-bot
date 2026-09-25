"""The buyers step cannot take the incremental tick down with it (chain 4, PR-1).

It runs BEFORE offers and stocks, and until this change nothing caught what it
raised except KeyCRM errors. A write it could not complete escaped the tick and
cost the inventory syncs behind it; and because a failed step never moves its
watermark, the next tick sixty seconds later fetched the same buyers and failed
the same way — up to 500 GETs a minute against a store that was refusing them.

Every test below drives the REAL `SyncService.incremental_sync` — the fakes are
the store and KeyCRM around it, never the method under test — because the
defect was in how the tick composes its steps, and a test of the step alone
would have passed on the old code.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.config import DEFAULT_TIMEZONE
from zoneinfo import ZoneInfo

TZ = ZoneInfo(DEFAULT_TIMEZONE)


class _Buyer:
    def __init__(self, bid, birthday=None):
        self.id = bid
        self.birthday = birthday


class _DataError(Exception):
    """Named like DuckDB's refusal of a value, which is how the step tells it
    from a broken store without importing a driver."""


_DataError.__name__ = "ConversionException"


def _service(monkeypatch, *, upsert=None, getter=None):
    from core import sync_service as mod
    from core.sync_service import SyncService

    now = datetime.now(TZ)
    recent, stale = now - timedelta(minutes=1), now - timedelta(hours=2)
    # Orders and the hourly catalogue branches ran a minute ago and stay quiet;
    # buyers, offers and stocks are due.
    due = {"orders": recent, "products": recent, "managers": recent}

    store = MagicMock()
    store.get_last_sync_time = getter or AsyncMock(side_effect=lambda k: due.get(k, stale))
    store.set_last_sync_time = AsyncMock()
    store.get_missing_buyer_ids = AsyncMock(return_value=[3, 1, 2])
    store.upsert_buyers = upsert or AsyncMock(return_value=3)
    store.mark_warehouse_dirty = AsyncMock()
    store.refresh_sku_inventory_status = AsyncMock(return_value=0)
    store.record_sku_inventory_snapshot = AsyncMock()
    store.record_inventory_snapshot = AsyncMock()

    client = MagicMock()
    client.fetch_buyers_by_ids = AsyncMock(
        return_value=[_Buyer(2), _Buyer(3, "0000-00-00"), _Buyer(1)])
    monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))

    svc = SyncService(store=store)
    svc._should_skip_sync = lambda: (False, None)
    svc._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    svc.sync_offers = AsyncMock(return_value=5)
    svc.sync_stocks = AsyncMock(return_value=7)
    return svc, store, client


def _buyers_stamped(store) -> bool:
    return any(c.args[:1] == ("buyers",) for c in store.set_last_sync_time.await_args_list)


@pytest.fixture
def no_mirror():
    with patch("core.pg_buyers.mirror_buyers", new=AsyncMock(return_value={"rows": 3})):
        yield


class TestAFailedWriteIsContained:
    @pytest.mark.asyncio
    async def test_offers_and_stocks_still_run_and_the_watermark_holds(
            self, monkeypatch, no_mirror):
        svc, store, _ = _service(
            monkeypatch, upsert=AsyncMock(side_effect=RuntimeError("store gone")))

        stats = await svc.incremental_sync()      # does not raise

        svc.sync_offers.assert_awaited_once()
        svc.sync_stocks.assert_awaited_once()
        store.refresh_sku_inventory_status.assert_awaited_once()
        assert not _buyers_stamped(store), "a failed step moved its watermark"
        # The `finally` sums this dict: a string in it would be a TypeError
        # that loses the tick's completion event as well.
        assert all(isinstance(v, int) for v in stats.values()), stats
        state = svc.buyer_sync_state
        assert (state.consecutive_failures, state.last_error_class) == (1, "RuntimeError")

    @pytest.mark.asyncio
    async def test_the_next_tick_inside_ten_minutes_asks_nothing_of_anyone(
            self, monkeypatch, no_mirror):
        svc, store, client = _service(
            monkeypatch, upsert=AsyncMock(side_effect=RuntimeError("store gone")))
        await svc.incremental_sync()
        store.get_last_sync_time.reset_mock()

        await svc.incremental_sync()

        asked = [c.args[0] for c in store.get_last_sync_time.await_args_list]
        assert "buyers" not in asked, "the retry window let the getter through"
        assert store.get_missing_buyer_ids.await_count == 1
        assert client.fetch_buyers_by_ids.await_count == 1
        svc.sync_offers.assert_awaited()          # the rest of the tick ran

    @pytest.mark.asyncio
    async def test_after_the_window_it_retries_and_a_success_closes_it(
            self, monkeypatch, no_mirror):
        upsert = AsyncMock(side_effect=[RuntimeError("store gone"), 3])
        svc, store, _ = _service(monkeypatch, upsert=upsert)
        await svc.incremental_sync()
        svc._buyers_retry_after = time.monotonic() - 1     # the window has passed

        await svc.incremental_sync()

        assert upsert.await_count == 2
        assert _buyers_stamped(store)
        state = svc.buyer_sync_state
        assert state.consecutive_failures == 0 and state.last_error_class is None
        assert svc._buyers_retry_in() is None

    @pytest.mark.asyncio
    async def test_a_data_error_is_recorded_but_does_not_count_as_a_failure(
            self, monkeypatch, no_mirror):
        svc, _, _ = _service(monkeypatch, upsert=AsyncMock(side_effect=_DataError("x")))
        await svc.incremental_sync()
        state = svc.buyer_sync_state
        assert state.last_error_class == "ConversionException"
        assert state.consecutive_failures == 0
        assert svc._buyers_retry_in() is not None     # but it still backs off



def _real_client(answer):
    """The REAL `KeyCRMClient.fetch_buyers_by_ids`, with only the network call
    replaced. The first draft of these tests faked `fetch_buyers_by_ids` itself
    to raise — something the real method never does, since it skips every
    failed buyer — and so pinned a path production cannot take."""
    from core.keycrm import KeyCRMClient

    client = KeyCRMClient.__new__(KeyCRMClient)

    async def get_customer(buyer_id):
        return answer(buyer_id)

    client.get_customer = get_customer
    return client


def _raise(exc):
    def answer(_bid):
        raise exc
    return answer


class TestAKeyCRMOutageIsAFailureNotAnEmptySelection:
    """Every buyer fetch failing used to come back as [] — "nothing to fetch" —
    and was stamped as a completed sync: watermark moved, no retry window, the
    canary's age never grew. Reproduced by PR-1's review with the real client."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc,recorded", [
        ("connection", "KeyCRMConnectionError"),
        ("forbidden", "KeyCRMAPIError"),
        ("breaker", "KeyCRMConnectionError"),     # CircuitOpenError, wrapped
    ])
    async def test_it_holds_the_watermark_and_backs_off(
            self, monkeypatch, no_mirror, exc, recorded):
        from core import sync_service as mod
        from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError
        from core.resilience import CircuitOpenError

        error = {"connection": KeyCRMConnectionError("API returned 429"),
                 "forbidden": KeyCRMAPIError("forbidden", status_code=403),
                 "breaker": CircuitOpenError("circuit open")}[exc]
        svc, store, _ = _service(monkeypatch)
        monkeypatch.setattr(mod, "get_async_client",
                            AsyncMock(return_value=_real_client(_raise(error))))

        await svc.incremental_sync()

        state = svc.buyer_sync_state
        assert state.last_error_class == recorded
        assert state.last_ok_at is None
        assert state.consecutive_failures == 0          # KeyCRM: the age pages
        assert svc._buyers_retry_in() is not None
        assert not _buyers_stamped(store)
        store.upsert_buyers.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_every_buyer_gone_is_an_empty_selection_not_an_outage(
            self, monkeypatch, no_mirror):
        from core import sync_service as mod
        from core.exceptions import KeyCRMAPIError

        svc, store, _ = _service(monkeypatch)
        monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=_real_client(
            _raise(KeyCRMAPIError("not found", status_code=404)))))

        await svc.incremental_sync()

        assert _buyers_stamped(store)
        assert svc.buyer_sync_state.last_error_class is None
        assert svc._buyers_retry_in() is None

    @pytest.mark.asyncio
    async def test_a_partial_fetch_writes_what_came_back(self, monkeypatch, no_mirror):
        from core import sync_service as mod
        from core.exceptions import KeyCRMAPIError

        def answer(bid):
            if bid == 2:
                raise KeyCRMAPIError("boom", status_code=500)
            return {"id": bid, "full_name": f"Покупець {bid}"}

        svc, store, _ = _service(monkeypatch)
        monkeypatch.setattr(mod, "get_async_client",
                            AsyncMock(return_value=_real_client(answer)))

        await svc.incremental_sync()

        written = store.upsert_buyers.await_args.args[0]
        assert [b.id for b in written] == [1, 3]
        assert _buyers_stamped(store)      # buyer 2 is still missing: re-selected

    @pytest.mark.asyncio
    async def test_the_ids_reach_keycrm_in_the_sorted_order(self, monkeypatch, no_mirror):
        """`set` used to shuffle them inside the client."""
        from core import sync_service as mod

        asked = []

        def answer(bid):
            asked.append(bid)
            return {"id": bid, "full_name": "x"}

        svc, _, _ = _service(monkeypatch)
        monkeypatch.setattr(mod, "get_async_client",
                            AsyncMock(return_value=_real_client(answer)))
        await svc.sync_missing_buyers()
        assert asked == [1, 2, 3]


class TestAFailedGetterIsContained:
    @pytest.mark.asyncio
    async def test_a_getter_that_raises_costs_nothing_else(self, monkeypatch, no_mirror):
        now = datetime.now(TZ)

        async def last(key):
            if key == "buyers":
                raise OSError("pool closed")
            return {"orders": now, "products": now, "managers": now}.get(
                key, now - timedelta(hours=2))

        svc, store, _ = _service(monkeypatch, getter=AsyncMock(side_effect=last))
        await svc.incremental_sync()

        svc.sync_offers.assert_awaited_once()
        svc.sync_stocks.assert_awaited_once()
        store.get_missing_buyer_ids.assert_not_awaited()
        assert svc.buyer_sync_state.last_error_class == "OSError"

    @pytest.mark.asyncio
    async def test_a_getter_that_hangs_is_cut_off(self, monkeypatch, no_mirror):
        from core import sync_service as mod

        monkeypatch.setattr(mod, "BUYER_WATERMARK_TIMEOUT_S", 0.05)
        now = datetime.now(TZ)

        async def last(key):
            if key == "buyers":
                await asyncio.sleep(30)
            return {"orders": now, "products": now, "managers": now}.get(
                key, now - timedelta(hours=2))

        svc, _, _ = _service(monkeypatch, getter=AsyncMock(side_effect=last))
        await asyncio.wait_for(svc.incremental_sync(), 5)
        svc.sync_offers.assert_awaited_once()
        assert svc.buyer_sync_state.last_error_class == "TimeoutError"


class TestAGetterThatUnwindsSlowly:
    @pytest.mark.asyncio
    async def test_the_tick_returns_at_the_bound_not_when_the_read_unwinds(
            self, monkeypatch, no_mirror):
        """A Postgres read cut inside a query unwinds into asyncpg's cancel
        request; a bare `wait_for` waits for that and a shielded one does not
        (DN-05a). The fake unwinds for three seconds on cancel."""
        from core import sync_service as mod

        monkeypatch.setattr(mod, "BUYER_WATERMARK_TIMEOUT_S", 0.05)
        now = datetime.now(TZ)

        async def last(key):
            if key == "buyers":
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    await asyncio.shield(asyncio.sleep(3))
                    raise
            return {"orders": now, "products": now, "managers": now}.get(
                key, now - timedelta(hours=2))

        svc, _, _ = _service(monkeypatch, getter=AsyncMock(side_effect=last))
        started = time.monotonic()
        await svc.incremental_sync()
        assert time.monotonic() - started < 1.5
        assert svc.buyer_sync_state.last_error_class == "TimeoutError"
        svc.sync_offers.assert_awaited_once()


class TestTheStepItself:
    @pytest.mark.asyncio
    async def test_ids_are_sorted_before_the_request_and_before_the_write(
            self, monkeypatch, no_mirror):
        svc, store, client = _service(monkeypatch)
        await svc.sync_missing_buyers()
        assert client.fetch_buyers_by_ids.await_args.args[0] == [1, 2, 3]
        assert [b.id for b in store.upsert_buyers.await_args.args[0]] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_a_success_records_what_it_did(self, monkeypatch, no_mirror):
        svc, store, _ = _service(monkeypatch)
        assert await svc.sync_missing_buyers() == 3
        state = svc.buyer_sync_state
        assert (state.last_selected, state.last_written) == (3, 3)
        assert state.unreadable_birthdays == 1        # buyer 3's '0000-00-00'
        assert state.last_ok_at is not None
        assert _buyers_stamped(store)


class TestTheStepNeverRaises:
    """The step is also called directly — by the manual endpoint — where there
    is no tick around it to catch anything. So its own contract is pinned on
    its own, not only through `incremental_sync`, which contains it a second
    time and would hide the first containment going missing."""

    @pytest.mark.asyncio
    async def test_a_failed_write_returns_zero_and_records_it(self, monkeypatch, no_mirror):
        svc, store, _ = _service(
            monkeypatch, upsert=AsyncMock(side_effect=RuntimeError("store gone")))
        assert await svc.sync_missing_buyers() == 0
        assert svc.buyer_sync_state.last_error_class == "RuntimeError"
        assert not _buyers_stamped(store)

    @pytest.mark.asyncio
    async def test_a_manual_success_inside_the_window_closes_it(self, monkeypatch, no_mirror):
        """Fail, then succeed by hand while the window is still open: the next
        tick must not sit out the rest of the ten minutes for nothing."""
        upsert = AsyncMock(side_effect=[RuntimeError("store gone"), 3])
        svc, store, _ = _service(monkeypatch, upsert=upsert)
        await svc.incremental_sync()
        assert svc._buyers_retry_in() is not None       # the window is open

        assert await svc.sync_missing_buyers() == 3     # the manual endpoint's call
        store.get_last_sync_time.reset_mock()
        await svc.incremental_sync()

        asked = [c.args[0] for c in store.get_last_sync_time.await_args_list]
        assert "buyers" in asked, "a success left the retry window open"


class TestTheBootPath:
    @pytest.mark.asyncio
    async def test_init_and_sync_still_reaches_inventory_and_search(
            self, monkeypatch, no_mirror):
        """At boot the tick is `init_and_sync`; a buyer failure there used to
        stop web's start before the inventory status and the search index."""
        from core import sync_service as mod

        svc, store, _ = _service(
            monkeypatch, upsert=AsyncMock(side_effect=RuntimeError("store gone")))
        store.get_stats = AsyncMock(return_value={"orders": 10, "products": 5})
        svc.sync_to_meilisearch = AsyncMock(return_value={})
        monkeypatch.setattr(mod, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(mod, "_sync_service", svc)
        monkeypatch.setattr(mod, "init_meilisearch", AsyncMock(return_value=True))

        await mod.init_and_sync()

        store.refresh_sku_inventory_status.assert_awaited()
        svc.sync_to_meilisearch.assert_awaited_once()


class TestTheHealthBlock:
    def test_it_publishes_ages_and_counts_never_error_text(self):
        from core.sync_service import BuyerSyncState

        state = BuyerSyncState(started_at=datetime.now(TZ) - timedelta(minutes=5))
        state.last_attempt_at = datetime.now(TZ)
        state.failed(RuntimeError("buyer +380501112233 refused"))
        block = state.published(retry_in_s=600)

        assert block["ever_ok"] is False
        assert 290 <= block["last_ok_age_s"] <= 310, "floored at the process start"
        assert block["last_error_class"] == "RuntimeError"
        assert "+380" not in repr(block)
        assert block["retry_in_s"] == 600

    def test_health_reads_the_running_service_and_never_starts_one(self, monkeypatch):
        from core import sync_service as mod
        from web.routes.api.health import _buyer_sync

        monkeypatch.setattr(mod, "_sync_service", None)
        assert _buyer_sync() is None

        svc = mod.SyncService(store=MagicMock())
        monkeypatch.setattr(mod, "_sync_service", svc)
        assert _buyer_sync()["consecutive_failures"] == 0


class TestTheManualEndpoint:
    def _client(self):
        from fastapi.testclient import TestClient

        from core.permissions import ADMIN_USER_IDS
        from web.main import app
        from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

        cookie = session_serializer.dumps(create_session_data({
            "id": str(sorted(ADMIN_USER_IDS)[0]), "first_name": "T", "last_name": "U",
            "username": "t", "auth_date": str(int(time.time()))}, role="admin"))
        client = TestClient(app)
        client.cookies.set(SESSION_COOKIE, cookie)
        return client

    def test_a_failed_sync_is_an_error_not_zero_synced(self, monkeypatch, no_mirror):
        from core import sync_service as mod

        svc, _, _ = _service(
            monkeypatch, upsert=AsyncMock(side_effect=RuntimeError("buyer +380 refused")))
        monkeypatch.setattr(mod, "_sync_service", svc)

        r = self._client().post("/api/duckdb/sync-buyers")

        assert r.status_code == 500
        assert "RuntimeError" in r.json()["detail"]
        assert "+380" not in r.text

    def test_a_good_sync_still_answers_success(self, monkeypatch, no_mirror):
        from core import sync_service as mod

        svc, _, _ = _service(monkeypatch)
        monkeypatch.setattr(mod, "_sync_service", svc)

        r = self._client().post("/api/duckdb/sync-buyers")

        assert r.status_code == 200, r.text
        assert r.json()["buyers_synced"] == 3
