"""A window of orders nothing could read fails the order step (AR-02).

L1-01 made one KeyCRM order the parse cannot read cost that order and not the
window: it is refused by id and the rest lands. The review of it found the
other side. When the parse can read **none** of the window — KeyCRM changed a
field's shape for every order, or a parser defect shipped — the step wrote
nothing, reported success, moved `last_sync_orders` past every order and kept
`orders_sync_failing` quiet; before L1-01 the same window raised a TypeError
on every tick and paged within fifteen minutes.

So a window that landed nothing (no order written or found unchanged, no
expense) and in which the parse refused orders is `OrdersRefused`: the step
fails, the watermark holds. A window that landed something keeps the
per-order skip, and one whose every order Postgres would refuse before the
latch keeps main's answer, a success counted in `last_refused`. Driven
through `SyncService._orders_step` with the writer stubbed;
`tests/integration/test_orders_poison_payload.py` runs the tick and the
canary against a real Postgres.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import pg_orders_write as pow_
from core.pg_orders_write import UpsertResult

UTC = timezone.utc
NOW = datetime.now(UTC).replace(microsecond=0)


def _payload(oid: int, **over) -> dict:
    stamp = (NOW - timedelta(minutes=5)).isoformat()
    p = {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": stamp, "created_at": stamp,
        "updated_at": stamp, "buyer": {"id": 1}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "x", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 7}}],
        "expenses": [],
    }
    p.update(over)
    return p


def _unreadable(oid: int) -> dict:
    return _payload(oid, products=[{"name": "x", "quantity": 1, "price_sold": None,
                                    "offer": {"product_id": 7}}])


class TestTheWriterCountsWhatTheParseRefused:
    """No Postgres: a window with nothing to write returns before the pool."""

    @pytest.mark.asyncio
    async def test_every_order_unreadable(self):
        result, expenses = await pow_.upsert_orders_with_expenses(
            [_unreadable(1), _unreadable(2), _payload("x", grand_total="abc")])
        assert (result.count, result.failed, result.unparseable, expenses) == (0, 3, 3, 0)

    @pytest.mark.asyncio
    async def test_an_expense_the_parse_cannot_read_is_its_order_unreadable(self):
        result, _ = await pow_.upsert_orders_with_expenses(
            [_payload(1, expenses=["not an object"])])
        assert (result.count, result.failed, result.unparseable) == (0, 1, 1)
        assert result.refused[0][1].startswith("unparseable expenses: ")

    @pytest.mark.asyncio
    async def test_a_refusal_before_the_latch_is_not_the_parse(self):
        result, _ = await pow_.upsert_orders_with_expenses(
            [_payload(1, manager_comment="NUL\x00"), _payload(2, manager_comment="\x00")])
        assert (result.count, result.failed, result.unparseable) == (0, 2, 0)


def _service(monkeypatch, writer_result):
    from core.sync_service import SyncService

    store = MagicMock()
    store.get_last_sync_time = AsyncMock(return_value=NOW - timedelta(minutes=30))
    store.set_last_sync_time = AsyncMock()
    svc = SyncService(store)
    monkeypatch.setattr(SyncService, "_fetch_orders_with_date_filter",
                        AsyncMock(return_value=[_payload(1), _payload(2), _payload(3)]))
    monkeypatch.setattr(pow_, "upsert_orders_with_expenses",
                        AsyncMock(return_value=writer_result))
    return svc, store


class TestTheOrderStep:
    @pytest.mark.asyncio
    async def test_a_window_nothing_could_read_fails_and_holds_the_watermark(self, monkeypatch):
        """Mutation: drop the `OrdersRefused` raise from `_orders_step` — it
        returns, and `last_sync_orders` moves past three orders nobody wrote."""
        from core.sync_service import OrdersRefused

        svc, store = _service(monkeypatch, (UpsertResult(
            count=0, changed_ids=[], skipped_unchanged=0, failed=3,
            refused=((1, "unparseable: x"), (2, "unparseable: x"), (3, "unparseable: x")),
            unparseable=3), 0))
        with pytest.raises(OrdersRefused, match="none of the 3"):
            await svc._orders_step(MagicMock(), {"orders": 0, "expenses": 0})
        store.set_last_sync_time.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_one_order_landed_and_the_rest_is_skipped_by_id(self, monkeypatch):
        """L1-01 stands: the window moves on past an order it could not read."""
        svc, store = _service(monkeypatch, (UpsertResult(
            count=1, changed_ids=[], skipped_unchanged=1, failed=2,
            refused=((2, "unparseable: x"), (3, "unparseable: x")), unparseable=2), 0))
        await svc._orders_step(MagicMock(), {"orders": 0, "expenses": 0})
        store.set_last_sync_time.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_window_postgres_would_refuse_whole_is_mains_success(self, monkeypatch):
        """Refused before the latch, not by the parse: main's answer, a success
        counted in `last_refused`. Mutation: raise on any window that landed
        nothing."""
        svc, store = _service(monkeypatch, (UpsertResult(
            count=0, changed_ids=[], skipped_unchanged=0, failed=3,
            refused=((1, "manager_comment carries a NUL"),), unparseable=0), 0))
        await svc._orders_step(MagicMock(), {"orders": 0, "expenses": 0})
        store.set_last_sync_time.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_tick_counts_it_as_a_failure(self, monkeypatch):
        """Through `incremental_sync`: counted, recorded in `meta.mirror_state`
        by class, and the canary's three-in-a-row pages."""
        from bot.canary import check_orders_sync_chain
        from core import sync_service as ss

        svc, store = _service(monkeypatch, (UpsertResult(
            count=0, changed_ids=[], skipped_unchanged=0, failed=3, unparseable=3), 0))
        monkeypatch.setattr(ss, "get_async_client", AsyncMock(return_value=MagicMock()))
        for step in ("_catalogue_step_postgres", "_inventory_step_postgres"):
            monkeypatch.setattr(ss.SyncService, step, AsyncMock())
        monkeypatch.setattr(ss.SyncService, "_managers_step_postgres", AsyncMock(return_value=0))
        monkeypatch.setattr(ss.SyncService, "_buyers_retry_in", lambda self: 60)
        recorded = AsyncMock()
        monkeypatch.setattr(ss.SyncService, "_record_orders_failure", recorded)
        for _ in range(3):
            svc._last_sync_time = None          # no adaptive skip
            await svc.incremental_sync()
        step = svc.orders_step.published()
        assert (step["consecutive_failures"], step["last_error_class"]) == (3, "OrdersRefused")
        assert [type(c.args[0]).__name__ for c in recorded.await_args_list] == ["OrdersRefused"] * 3
        store.set_last_sync_time.assert_not_awaited()
        pages = check_orders_sync_chain({"write_chains": {"pg_orders_write": {"sync_step": step}}})
        assert [key for key, _ in pages] == ["orders_sync_failing"]
