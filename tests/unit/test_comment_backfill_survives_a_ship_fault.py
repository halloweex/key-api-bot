"""A Postgres fault must not cost the UTM re-parse.

DN-17 gave both `manager_comment` backfills a second store to write to, and
`core.pg_backfill.ship_orders_by_id` raises on failure — `backfill_orders`'
contract, and right for a helper whose caller has nothing else to do.

These two callers do have something else to do. The run ends by deleting and
rebuilding `silver_order_utm` from the comments it has just restored, and that
work is DuckDB's alone: it was Postgres-independent before DN-17 and must stay
so. An unreachable Postgres — or `web` deployed ahead of `migrate`, where
`require_revision()` raises `SchemaVersionError` — would otherwise leave an
operator with the comments written into DuckDB, the classification not
re-parsed, and no way to ask for those rows again, because the SELECT that
chooses them only offers comments that are still NULL.

So the ship is caught per chunk in both callers, the ids are recorded, and the
run carries on. Recorded rather than retried: nothing else will name them, and
the daily `mirror_landing` fingerprint would otherwise hand them back one
disagreeing bucket at a time.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.duckdb_store import DuckDBStore

WHEN = "TIMESTAMPTZ '2026-08-01 10:00:00+00'"


async def _two_orders(tmp_path, name="backfill.duckdb") -> DuckDBStore:
    """One order that already has a comment, one that does not."""
    store = DuckDBStore(db_path=tmp_path / name)
    await store.connect()
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO orders (id, source_id, status_id, grand_total,"
            f" ordered_at, buyer_id, manager_comment) VALUES"
            f" (1, 1, 1, 10.0, {WHEN}, 1, 'kept'),"
            f" (2, 1, 1, 10.0, {WHEN}, 2, NULL)"
        )
    return store


class _Client:
    """KeyCRM, returning a comment for both orders."""

    async def paginate(self, *_a, **_k):
        yield [{"id": 1, "manager_comment": "utm_source=fb"},
               {"id": 2, "manager_comment": "utm_source=tg"}]


async def _exploding_ship(*_a, **_kw):
    raise RuntimeError("postgres is unreachable")


@pytest.mark.asyncio
async def test_the_endpoint_reparses_and_names_the_ids_it_could_not_ship(tmp_path):
    """The route's whole second half — the re-parse and `reparse_router` —
    must still run, and the run must say which orders the two stores now
    disagree about."""
    from web.routes.api import traffic

    store = await _two_orders(tmp_path)
    reparsed = []
    scheduler = MagicMock()
    scheduler._heavy_job_lock = asyncio.Lock()
    before = dict(traffic._backfill_status)

    async def reparse():
        reparsed.append("reparse")
        return []

    try:
        with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
             patch("core.keycrm.get_async_client",
                   AsyncMock(return_value=_Client())), \
             patch("core.scheduler.get_scheduler", return_value=scheduler), \
             patch("core.pg_backfill.ship_orders_by_id", new=_exploding_ship), \
             patch.object(store, "refresh_utm_silver_layer", new=reparse), \
             patch.object(traffic, "reparse_router", new=AsyncMock()) as after, \
             patch("asyncio.sleep", new=AsyncMock()):
            # Bounded: this run takes a lock per chunk, and a test that hangs
            # on one fails nobody in time.
            await asyncio.wait_for(traffic._run_backfill_inner(1), timeout=20)
        result = dict(traffic._backfill_status["result"])
        async with store.connection() as conn:
            stored = dict(conn.execute(
                "SELECT id, manager_comment FROM orders ORDER BY id").fetchall())
    finally:
        traffic._backfill_status.clear()
        traffic._backfill_status.update(before)
        await store.close()

    assert reparsed == ["reparse"], "the DuckDB re-parse was skipped"
    assert after.await_count == 1, "the parsed rows never reached Postgres"
    # `partial`, not `success`: the two stores disagree about order 2 until
    # somebody re-ships it, and a green status is how that gets forgotten.
    assert result["status"] == "partial"
    assert result["pg_failed_ids"] == [2]
    assert result["pg_shipped"] == 0
    assert result["db_updated"] == 1
    # DuckDB kept the restore. That is what makes the failure permanent for a
    # re-run, and therefore what makes the id list worth recording.
    assert stored == {1: "kept", 2: "utm_source=tg"}


@pytest.mark.asyncio
async def test_the_cli_reparses_and_names_the_ids_it_could_not_ship(tmp_path):
    """The same claim for `scripts/backfill_utm.py`, where the ship used to sit
    outside the try that guards the DuckDB write — so the script died mid-loop
    and never reached its own `DELETE FROM silver_order_utm` either."""
    import scripts.backfill_utm as cli

    store = await _two_orders(tmp_path, name="cli.duckdb")
    reparsed = []

    async def reparse():
        reparsed.append("reparse")
        return []

    try:
        with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
             patch("core.keycrm.get_async_client",
                   AsyncMock(return_value=_Client())), \
             patch("core.runtime_modes.configure_modes", MagicMock()), \
             patch("core.pg_backfill.ship_orders_by_id", new=_exploding_ship), \
             patch.object(store, "refresh_utm_silver_layer", new=reparse), \
             patch("core.pg_order_utm.ship_after_reparse",
                   new=AsyncMock(return_value={})) as after:
            await asyncio.wait_for(cli.backfill_utm(days_back=1), timeout=20)
        async with store.connection() as conn:
            stored = dict(conn.execute(
                "SELECT id, manager_comment FROM orders ORDER BY id").fetchall())
    finally:
        await store.close()

    assert reparsed == ["reparse"], "the script stopped at the failed ship"
    assert after.await_count == 1
    assert stored == {1: "kept", 2: "utm_source=tg"}


@pytest.mark.asyncio
async def test_a_successful_run_still_reports_success_and_no_failed_ids(tmp_path):
    """The other half of the claim, so the guard above cannot be satisfied by
    a route that reports `partial` whatever happens."""
    from web.routes.api import traffic

    store = await _two_orders(tmp_path, name="ok.duckdb")
    scheduler = MagicMock()
    scheduler._heavy_job_lock = asyncio.Lock()
    before = dict(traffic._backfill_status)

    async def ship(_store, ids, *, version_kind, **_kw):
        return {"orders_shipped": len(ids)}

    try:
        with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
             patch("core.keycrm.get_async_client",
                   AsyncMock(return_value=_Client())), \
             patch("core.scheduler.get_scheduler", return_value=scheduler), \
             patch("core.pg_backfill.ship_orders_by_id", new=ship), \
             patch.object(store, "refresh_utm_silver_layer",
                          new=AsyncMock(return_value=[])), \
             patch.object(traffic, "reparse_router", new=AsyncMock()), \
             patch("asyncio.sleep", new=AsyncMock()):
            await asyncio.wait_for(traffic._run_backfill_inner(1), timeout=20)
        result = dict(traffic._backfill_status["result"])
    finally:
        traffic._backfill_status.clear()
        traffic._backfill_status.update(before)
        await store.close()

    assert result["status"] == "success"
    assert result["pg_failed_ids"] == []
    assert result["pg_shipped"] == 1
