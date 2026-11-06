"""A healthy order step clears the failure it recorded (L4-03).

`_record_orders_failure` marks `meta.mirror_state` `bronze.orders` failing when
chain 3's step fails. Only a write of an order zeroed it again
(`pg_landing._WATERMARK_OK`), and a healthy tick that finds nothing changed
writes nothing: one failed step left the row failing — the canary paging
`mirror_failing:bronze.orders` and the soak's S0 failing — until some order
actually changed. The step now clears it after any success, idle included,
without moving `last_ok_at`.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import pg_orders_write as chain
from core.pg_landing import ORDERS_TABLE

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

IDS = [990_881, 990_882]


def _payload(oid: int, stamp: datetime) -> dict:
    return {"id": oid, "source_id": 4, "status_id": 12, "status_group_id": 4,
            "grand_total": "50.00", "ordered_at": stamp.isoformat(),
            "created_at": stamp.isoformat(), "updated_at": stamp.isoformat(),
            "buyer": {"id": 77}, "manager": None, "manager_comment": None,
            "promocode": None,
            "products": [{"name": "y", "quantity": 1, "price_sold": "50.00",
                          "offer": {"product_id": 9}}]}


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", IDS)
        await conn.execute("DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", IDS)


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with p.acquire() as conn:
        saved = await conn.fetchrow("SELECT * FROM meta.mirror_state WHERE table_name = $1",
                                    ORDERS_TABLE)
    await _clean(p)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=p)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield p
    await _clean(p)
    async with p.acquire() as conn:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", ORDERS_TABLE)
        if saved is not None:
            cols = list(saved.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(cols)}) VALUES "
                f"({', '.join(f'${i + 1}' for i in range(len(cols)))})", *saved.values())
    await p.close()


async def _state(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT failures_since_ok, last_error, last_ok_at, last_attempted_at "
            "FROM meta.mirror_state WHERE table_name = $1", ORDERS_TABLE)


def _service():
    from core.sync_service import SyncService

    return SyncService.__new__(SyncService)


class TestTheHealthyTickAfterAFailure:
    @pytest.mark.asyncio
    async def test_an_idle_success_clears_it_and_leaves_last_ok_at(self, pool):
        """Mutation: drop the clear — the row stays 1 / failing after three
        idle ticks (the reproduction's shape)."""
        page = [_payload(oid, datetime.now(timezone.utc) - timedelta(hours=1)) for oid in IDS]
        await chain.upsert_orders_with_expenses(page)
        landed = await _state(pool)

        svc = _service()
        await svc._record_orders_failure(ConnectionResetError("x"))
        failed = await _state(pool)
        assert failed["failures_since_ok"] == 1 and failed["last_error"] == "ConnectionResetError"

        result, _ = await chain.upsert_orders_with_expenses(page)
        assert result.changed_ids == []               # idle: nothing to write
        await svc._clear_orders_failure()

        cleared = await _state(pool)
        assert cleared["failures_since_ok"] == 0 and cleared["last_error"] is None
        assert cleared["last_ok_at"] == landed["last_ok_at"], "last_ok_at moved"
        assert cleared["last_attempted_at"] >= failed["last_attempted_at"]

    @pytest.mark.asyncio
    async def test_the_canary_stops_paging(self, pool):
        from bot.canary import check_mirror_freshness
        from core.mirror_reconciliation import fetch_mirror_freshness

        page = [_payload(IDS[0], datetime.now(timezone.utc) - timedelta(minutes=5))]
        await chain.upsert_orders_with_expenses(page)
        svc = _service()
        await svc._record_orders_failure(ConnectionResetError("x"))

        def failing(fresh):
            return [k for k, _ in check_mirror_freshness({"mirrors": fresh})[0]
                    if k == f"mirror_failing:{ORDERS_TABLE}"]

        assert failing(await fetch_mirror_freshness(pool))
        await svc._clear_orders_failure()
        assert not failing(await fetch_mirror_freshness(pool))

    @pytest.mark.asyncio
    async def test_asked_once_then_only_after_a_failure(self, pool):
        """Not a statement on every healthy tick: once at a start, then only
        after a failure this process recorded."""
        svc = _service()
        await svc._clear_orders_failure()
        counted = AsyncMock(return_value=pool)
        with patch("core.pg.get_pool", new=counted):
            await svc._clear_orders_failure()
            assert counted.await_count == 0
            await svc._record_orders_failure(ConnectionResetError("x"))
            await svc._clear_orders_failure()
            assert counted.await_count == 2          # the record, then the clear
        assert (await _state(pool))["failures_since_ok"] == 0


class TestTheTickAsksIt:
    def test_the_clear_follows_the_steps_success(self):
        """By tree: in `incremental_sync`, the `try` that records the order
        step's success awaits the clear in the same body, after it — so a
        failure (caught below it) never clears anything."""
        import ast
        import inspect
        import textwrap

        from core.sync_service import SyncService

        tree = ast.parse(textwrap.dedent(inspect.getsource(SyncService.incremental_sync)))

        def names(stmts):
            return [n.func.attr for st in stmts for n in ast.walk(st)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]

        bodies = [names(t.body) for t in ast.walk(tree) if isinstance(t, ast.Try)
                  and not any(isinstance(n, ast.Try) for st in t.body for n in ast.walk(st))
                  and "succeeded" in names(t.body) and "_orders_step" in names(t.body)]
        assert len(bodies) == 1
        body = bodies[0]
        assert "_clear_orders_failure" in body
        assert body.index("_clear_orders_failure") > body.index("succeeded")
