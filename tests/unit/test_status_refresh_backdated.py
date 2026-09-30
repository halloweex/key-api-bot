"""The 05:15 status refresh reaches the backdated orders its fetch cannot.

KeyCRM lets a B2B order's `ordered_at` sit weeks after its `created_at`, and
does not move `updated_at` when a status changes. `refresh_order_statuses`
fetches by `created_between` over 30 days, so an order created 35 days ago and
ordered 5 days ago is outside the fetch while its order date is inside every
window that reads it. The weekly full sync skips it as unchanged, and
`dq_reconciliation` repairs only the orders we do not hold, so a status that
moved on it was STATUS_DRIFT, CRITICAL, every morning.

Until OD-10 (2026-09-30) the legacy 06:00 `reconciliation_check` repaired it:
its fetch was padded by 30 days for exactly this. The job was retired and its
repair was kept. The refresh now re-fetches those orders by id.
The store's selection runs as real SQL on a real DuckDB file here, in Kyiv
dates, because the boundary is a date and the store reads dates in Kyiv.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from core.duckdb_store import DuckDBStore

KYIV = ZoneInfo("Europe/Kyiv")


def _kyiv_noon(days_ago: int) -> datetime:
    return (datetime.now(KYIV) - timedelta(days=days_ago)).replace(
        hour=12, minute=0, second=0, microsecond=0)


async def _store(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "t.duckdb")
    await store.connect()
    return store


def _insert(conn, oid, *, created: datetime, ordered: datetime, status_id=12):
    conn.execute("""
        INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, created_at)
        VALUES (?, 1, ?, 5000.0, ?, ?)
    """, [oid, status_id, ordered, created])


class TestFindBackdatedOrderIds:
    @pytest.mark.asyncio
    async def test_it_selects_orders_dated_into_the_window_and_created_before_it(
        self, tmp_path,
    ):
        store = await _store(tmp_path)
        since = _kyiv_noon(30).date()
        try:
            async with store.connection() as conn:
                # The review's case: created 35 days ago, ordered 5 days ago.
                _insert(conn, 777, created=_kyiv_noon(35), ordered=_kyiv_noon(5))
                # Dated weeks ahead of a creation long before the window.
                _insert(conn, 778, created=_kyiv_noon(100), ordered=_kyiv_noon(-2))
                # Created on the window's first day, ordered after it.
                _insert(conn, 779, created=_kyiv_noon(30), ordered=_kyiv_noon(29))
                # Created and ordered inside the window: the fetch returns it.
                _insert(conn, 1, created=_kyiv_noon(5), ordered=_kyiv_noon(5))
                # Created and ordered on the first day: the fetch returns it.
                _insert(conn, 2, created=_kyiv_noon(30), ordered=_kyiv_noon(30))
                # Ordered before the window: nothing reads it as recent.
                _insert(conn, 3, created=_kyiv_noon(40), ordered=_kyiv_noon(35))
                # Ordered before it was created: created inside, so fetched.
                _insert(conn, 4, created=_kyiv_noon(3), ordered=_kyiv_noon(10))

            assert await store.find_backdated_order_ids(since) == [777, 778, 779]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_boundary_is_the_kyiv_date(self, tmp_path):
        """23:30 Kyiv is the next day in UTC+4 and the same day in UTC. The
        store reads it as Kyiv reads it."""
        store = await _store(tmp_path)
        since = _kyiv_noon(30).date()
        late = datetime.combine(since - timedelta(days=1), datetime.min.time(),
                                tzinfo=KYIV).replace(hour=23, minute=30)
        try:
            async with store.connection() as conn:
                _insert(conn, 10, created=late, ordered=late + timedelta(hours=1))
                _insert(conn, 11, created=late, ordered=late)
            assert await store.find_backdated_order_ids(since) == [10]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_nothing_backdated_is_nothing_to_do(self, tmp_path):
        store = await _store(tmp_path)
        try:
            assert await store.find_backdated_order_ids(_kyiv_noon(30).date()) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_batch_is_bounded(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                for oid in range(1, 8):
                    _insert(conn, oid, created=_kyiv_noon(40), ordered=_kyiv_noon(3))
            assert await store.find_backdated_order_ids(
                _kyiv_noon(30).date(), limit=3) == [1, 2, 3]
        finally:
            await store.close()


class _KeyCRM:
    """Serves the list only for a `created_between` window that holds an
    order's creation date, and every order by id. That is how KeyCRM answers."""

    def __init__(self, orders):
        self.orders = {o["id"]: o for o in orders}
        self.windows, self.by_id = [], []

    async def paginate(self, endpoint, params=None, page_size=50):
        lo, hi = (part.strip()[:10] for part in
                  params["filter[created_between]"].split(","))
        self.windows.append((lo, hi))
        yield [o for o in self.orders.values()
               if lo <= o["created_at"][:10] <= hi]

    async def get_order(self, order_id, include=None):
        self.by_id.append(order_id)
        return self.orders.get(order_id)


def _keycrm_order(oid, *, created: datetime, ordered: datetime, status_id: int):
    return {"id": oid, "status_id": status_id, "grand_total": 5000.0,
            "created_at": created.isoformat(), "ordered_at": ordered.isoformat(),
            "source_id": 1, "manager_id": 15}


async def _refresh(store, client, upsert):
    """`refresh_order_statuses` against `store`, with KeyCRM and the order
    writer replaced — the writer is `upsert_orders`' business, tested there."""
    from unittest.mock import AsyncMock

    from core import sync_service as sync_module

    service = sync_module.SyncService(store)
    store.refresh_warehouse_layers = AsyncMock(return_value={"status": "success"})

    async def get_client():
        return client

    with patch.object(sync_module, "get_async_client", new=get_client), \
            patch.object(service, "_upsert_orders_with_expenses", side_effect=upsert):
        return await service.refresh_order_statuses(days_back=30)


class TestTheRefreshReachesThem:
    def test_a_status_moved_on_a_backdated_order_is_written(self, tmp_path):
        """The review's reproduction, end to end: DuckDB holds order 777 at
        status 12, KeyCRM says 19, and only the by-id pass can see it."""
        created, ordered = _kyiv_noon(35), _kyiv_noon(5)
        recent = _keycrm_order(1, created=_kyiv_noon(3), ordered=_kyiv_noon(3),
                               status_id=12)
        client = _KeyCRM([recent, _keycrm_order(777, created=created, ordered=ordered,
                                                status_id=19)])
        writes = []

        async def upsert(orders, force_update=False, skip_products=False):
            writes.append(([o["id"] for o in orders], force_update, skip_products))
            return len(orders), 0

        async def run():
            store = await _store(tmp_path)
            try:
                async with store.connection() as conn:
                    _insert(conn, 777, created=created, ordered=ordered, status_id=12)
                    _insert(conn, 1, created=_kyiv_noon(3), ordered=_kyiv_noon(3))
                return await _refresh(store, client, upsert)
            finally:
                await store.close()

        stats = asyncio.run(run())
        # The window pass: headers only, and 777 is not in it.
        assert writes[0] == ([1], True, True), writes
        assert client.by_id == [777]
        # Then 777 by id, whole, and forced: KeyCRM did not move updated_at.
        assert writes[1] == ([777], True, False)
        assert len(writes) == 2
        assert stats["backdated"] == {"found": 1, "repaired": 1, "failed": 0}

    def test_with_none_backdated_nothing_is_fetched_by_id(self, tmp_path):
        client = _KeyCRM([_keycrm_order(1, created=_kyiv_noon(3),
                                        ordered=_kyiv_noon(3), status_id=12)])

        async def upsert(orders, force_update=False, skip_products=False):
            return len(orders), 0

        async def run():
            store = await _store(tmp_path)
            try:
                return await _refresh(store, client, upsert)
            finally:
                await store.close()

        stats = asyncio.run(run())
        assert client.by_id == [] and stats["backdated"] == {"found": 0}

    def test_a_failure_there_costs_the_refresh_nothing(self, tmp_path, caplog):
        """The window pass has committed by then; the by-id pass says why it
        did not run and the job still returns its stats."""
        client = _KeyCRM([_keycrm_order(1, created=_kyiv_noon(3),
                                        ordered=_kyiv_noon(3), status_id=12)])

        async def upsert(orders, force_update=False, skip_products=False):
            return len(orders), 0

        class Broken(DuckDBStore):
            async def find_backdated_order_ids(self, since, limit=200):
                raise OSError("disk")

        async def run():
            store = Broken(db_path=tmp_path / "t.duckdb")
            await store.connect()
            try:
                return await _refresh(store, client, upsert)
            finally:
                await store.close()

        stats = asyncio.run(run())
        assert stats["orders"] == 1
        assert stats["backdated"] == {"error": "OSError"}
        assert "backdated orders were not re-fetched" in caplog.text
