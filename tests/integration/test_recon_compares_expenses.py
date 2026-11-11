"""The 05:30 reconciliation compares each order's expenses, and repairs them (O3 (a)).

Until O3 (a) nothing compared an order's expenses with KeyCRM: the snapshot
did not ask for them, and the only watch on `bronze.expenses` was the orphan
check. An expense KeyCRM removed, added or re-priced without the order's
`updated_at` moving stayed wrong in /expenses for ever.

Driven through the real `_run_dq_reconciliation`, the real Postgres arm and the
real repair, over a KeyCRM snapshot made by the job's own `_process_batch` from
KeyCRM-shaped payloads — so the same function reads both the snapshot and, on
the repair, the payload the chain writes. Whatever else the shared database
holds compares equal: the snapshot carries it as Postgres holds it, without
expense facts. On the commit before the fix the snapshot carried no expenses
and no order here was reported or re-fetched.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 990_700
REMOVED, EXTRA, REPRICED, NO_KEY, SAME, INFLIGHT = (BASE + i for i in range(6))
IDS = [REMOVED, EXTRA, REPRICED, NO_KEY, SAME, INFLIGHT]
OLD = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=40)


def _cost(eid: int, amount: str = "10.50") -> dict:
    return {"id": eid, "expense_type_id": 1, "amount": amount, "status": "paid"}


def _payload(oid: int, costs, *, updated: datetime = OLD) -> dict:
    p = {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": OLD.isoformat(),
        "created_at": OLD.isoformat(), "updated_at": updated.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
    }
    if costs is not None:
        p["expenses"] = costs
    return p


# What Postgres holds, and what KeyCRM serves at 05:30.
HELD = {
    REMOVED: [_cost(REMOVED * 10), _cost(REMOVED * 10 + 1, "7.25")],
    EXTRA: [_cost(EXTRA * 10)],
    REPRICED: [_cost(REPRICED * 10)],
    NO_KEY: [_cost(NO_KEY * 10)],
    SAME: [_cost(SAME * 10), _cost(SAME * 10 + 1, "3")],
    INFLIGHT: [_cost(INFLIGHT * 10)],
}
SERVED = {
    REMOVED: [_cost(REMOVED * 10)],
    EXTRA: [_cost(EXTRA * 10), _cost(EXTRA * 10 + 1, "4.00")],
    REPRICED: [_cost(REPRICED * 10, "10.51")],
    NO_KEY: None,                                   # fetched without the key
    SAME: [_cost(SAME * 10, "10.5"), _cost(SAME * 10 + 1, "3.00")],
    INFLIGHT: [],                                   # updated after the watermark
}


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM bronze.expenses WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute(
            "DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)


@pytest_asyncio.fixture
async def pool():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        saved = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = 'bronze.orders'")
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield pool
    await _clean(pool)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = 'bronze.orders'")
        for row in saved:
            cols = list(row.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                f"({', '.join(f'${i + 1}' for i in range(len(cols)))})", *row.values())
    await pool.close()


async def _stored(pool, oid):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, amount FROM bronze.expenses WHERE order_id = $1 ORDER BY id", oid)
    return [(r["id"], r["amount"]) for r in rows]


class _KeyCRM:
    """KeyCRM by id, as the repair asks it: the order as served at 05:30."""

    def __init__(self):
        self.asked: list = []

    async def get_order(self, order_id, include=None):
        self.asked.append(int(order_id))
        assert "expenses" in (include or "")
        return _payload(int(order_id), SERVED[int(order_id)])


async def _run(pool, sched):
    from core.reconciliation_io import _process_batch, postgres_orders_in_window

    async def snapshot(window_start, window_end, *, watermark):
        held = await postgres_orders_in_window(
            pool, window_start, window_end, watermark=watermark)
        assert set(IDS) - {INFLIGHT} <= set(held), "the fixture is not in the window"
        kc = {oid: facts for oid, facts in held.items() if oid not in IDS}
        inflight: set = set()
        _process_batch([_payload(oid, SERVED[oid],
                                 updated=(datetime.now(timezone.utc) if oid == INFLIGHT
                                          else OLD))
                        for oid in IDS],
                       kc, watermark, window_start, window_end, inflight)
        assert inflight == {INFLIGHT}
        return kc, 3, inflight

    keycrm = _KeyCRM()
    with patch("core.reconciliation_io.keycrm_orders_in_window", new=snapshot), \
         patch("core.sync_service.get_async_client", new=AsyncMock(return_value=keycrm)):
        result = await sched._run_dq_reconciliation()
    return result, keycrm


@pytest.mark.asyncio
async def test_an_order_whose_expenses_differ_is_reported_and_repaired(pool):
    from core.scheduler import BackgroundScheduler

    await chain.upsert_orders_with_expenses([_payload(oid, HELD[oid]) for oid in IDS])
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE meta.mirror_state SET backfilled_at = now() "
            "WHERE table_name = 'bronze.orders'")
        # The in-flight order moved after the watermark: what the job's
        # exclusion holds back, on both sides.
        await conn.execute("UPDATE bronze.orders SET updated_at = now() WHERE id = $1",
                           INFLIGHT)

    sched = BackgroundScheduler()
    sched._persist_postgres_reconciliation = AsyncMock()
    sched._reconcile_clickhouse = AsyncMock(return_value=None)
    result, keycrm = await _run(pool, sched)

    assert result["error"] is None
    persisted = sched._persist_postgres_reconciliation.await_args.args[0]
    assert persisted["error"] is None
    found = {}
    for d in persisted["discrepancies"]:
        for oid in d.order_ids:
            if oid in IDS:
                found.setdefault(oid, set()).add((d.diff_class.value, d.field, d.severity.value))
    mismatch = ("VALUE_MISMATCH", "n_expenses", "WARN")
    amount = ("VALUE_MISMATCH", "expense_amount", "WARN")
    # Removed and added: both the count and the sum; re-priced by one
    # kopeck: the sum alone. Nothing else about these orders differs, and
    # neither the order fetched without its expenses, the one whose
    # expenses agree in another spelling, nor the one in flight is named.
    assert found == {REMOVED: {mismatch, amount}, EXTRA: {mismatch, amount},
                     REPRICED: {amount}}

    # Re-fetched by id, in the existing batch — and nothing else of ours.
    assert sorted(keycrm.asked) == [REMOVED, EXTRA, REPRICED]
    assert result["repair"]["repaired"] == 3
    assert await _stored(pool, REMOVED) == [(REMOVED * 10, Decimal("10.50"))]
    assert await _stored(pool, EXTRA) == [(EXTRA * 10, Decimal("10.50")),
                                          (EXTRA * 10 + 1, Decimal("4.00"))]
    assert await _stored(pool, REPRICED) == [(REPRICED * 10, Decimal("10.51"))]
    # Untouched: what KeyCRM said nothing about, and what was held back.
    assert await _stored(pool, NO_KEY) == [(NO_KEY * 10, Decimal("10.50"))]
    assert await _stored(pool, INFLIGHT) == [(INFLIGHT * 10, Decimal("10.50"))]


@pytest.mark.asyncio
async def test_a_second_run_after_the_repair_is_clean(pool):
    """The repair is what makes the finding go away: the next morning the
    same snapshot compares equal."""
    from core.scheduler import BackgroundScheduler

    await chain.upsert_orders_with_expenses(
        [_payload(oid, SERVED[oid] if SERVED[oid] is not None else HELD[oid])
         for oid in IDS])
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE meta.mirror_state SET backfilled_at = now() "
            "WHERE table_name = 'bronze.orders'")
        await conn.execute("UPDATE bronze.orders SET updated_at = now() WHERE id = $1",
                           INFLIGHT)

    sched = BackgroundScheduler()
    sched._persist_postgres_reconciliation = AsyncMock()
    sched._reconcile_clickhouse = AsyncMock(return_value=None)
    result, keycrm = await _run(pool, sched)

    persisted = sched._persist_postgres_reconciliation.await_args.args[0]
    assert not [d for d in persisted["discrepancies"] if set(d.order_ids) & set(IDS)]
    assert keycrm.asked == []
    assert result["error"] is None

