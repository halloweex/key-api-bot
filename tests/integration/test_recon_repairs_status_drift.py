"""The 05:30 reconciliation re-fetches an order Postgres holds wrong (A3, L1-04).

KeyCRM does not bump `updated_at` on a status change. An order created and
ordered more than 30 days ago whose status then moves is reached by no sync:
the incremental tick reads `updated_between`, the 05:15 refresh reads
`created_between` over 30 days and re-fetches by id only orders backdated into
that window, and the weekly full sync, unforced, skips it as unchanged. The
05:30 job filed it STATUS_DRIFT (CRITICAL) every morning and repaired only
MISSING_IN_DK, so nothing ever wrote KeyCRM's status.

Driven through the real `_run_dq_reconciliation` and the real Postgres arm,
over a KeyCRM snapshot built from what Postgres holds — so whatever else the
shared database holds compares equal — with one order's status moved, one
order KeyCRM no longer returns (MISSING_IN_KC) and one Postgres lacks.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import pg_orders_write as chain

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

BASE = 990_800
DRIFTED, GHOST, MISSING = BASE, BASE + 1, BASE + 2
IDS = [DRIFTED, GHOST, MISSING]
OLD = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=40)


def _payload(oid: int, status: int = 12) -> dict:
    return {
        "id": oid, "source_id": 1, "status_id": status, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": OLD.isoformat(),
        "created_at": OLD.isoformat(), "updated_at": OLD.isoformat(),
        "buyer": {"id": 5401}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 701}}],
        "expenses": [],
    }


async def _clean(pool):
    async with pool.acquire() as conn:
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


async def _status(pool, oid):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT status_id FROM bronze.orders WHERE id = $1", oid)


class _KeyCRM:
    """KeyCRM by id: the drifted order cancelled, the one Postgres lacks."""

    def __init__(self):
        self.asked: list = []

    async def get_order(self, order_id, include=None):
        self.asked.append(int(order_id))
        if order_id == DRIFTED:
            return _payload(DRIFTED, status=19)
        if order_id == MISSING:
            return _payload(MISSING)
        return None


@pytest.mark.asyncio
async def test_a_status_drift_is_written_and_a_ghost_is_not_fetched(pool):
    from core.reconciliation_io import postgres_orders_in_window
    from core.scheduler import BackgroundScheduler

    await chain.upsert_orders_with_expenses([_payload(DRIFTED), _payload(GHOST)])
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE meta.mirror_state SET backfilled_at = now() "
            "WHERE table_name = 'bronze.orders'")
    assert await _status(pool, DRIFTED) == 12

    sched = BackgroundScheduler()
    sched._persist_postgres_reconciliation = AsyncMock()
    sched._reconcile_clickhouse = AsyncMock(return_value=None)
    keycrm = _KeyCRM()

    async def snapshot(window_start, window_end, *, watermark):
        held = await postgres_orders_in_window(
            pool, window_start, window_end, watermark=watermark)
        assert DRIFTED in held and GHOST in held
        kc = dict(held)
        kc[DRIFTED] = {**held[DRIFTED], "status_id": 19}
        kc.pop(GHOST)
        kc[MISSING] = {**held[DRIFTED], "status_id": 12}
        return kc, 3, set()

    with patch("core.reconciliation_io.keycrm_orders_in_window", new=snapshot), \
         patch("core.sync_service.get_async_client", new=AsyncMock(return_value=keycrm)):
        result = await sched._run_dq_reconciliation()

    assert result["error"] is None
    assert sorted(keycrm.asked) == [DRIFTED, MISSING]
    assert GHOST not in keycrm.asked
    assert result["repair"]["repaired"] == 2
    assert await _status(pool, DRIFTED) == 19
    assert await _status(pool, MISSING) == 12
    # The ghost is left exactly where it was: additive only.
    assert await _status(pool, GHOST) == 12
    persisted = sched._persist_postgres_reconciliation.await_args.args[0]
    classes = {d.diff_class.value for d in persisted["discrepancies"]}
    assert {"STATUS_DRIFT", "MISSING_IN_KC", "MISSING_IN_DK"} <= classes
