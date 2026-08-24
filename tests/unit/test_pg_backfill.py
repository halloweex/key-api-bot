"""The backfill: history the mirror cannot carry on its own.

`upsert_orders` mirrors `updated_ids` — what it wrote, not what the page held —
so Postgres starts at the last few minutes of activity with 46,446 orders
behind it. This closes that gap, and the property worth testing is that it has
no memory: it asks both stores what they hold, ships the difference, and stops.
Interrupt it anywhere and the next call resumes by recomputing, which is why
there is no cursor to get wrong.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore
from core.pg_backfill import backfill_orders

WHEN = "2026-08-20T12:00:00+00:00"


def _payload(order_id, products=2):
    return {
        "id": order_id, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": WHEN, "created_at": WHEN,
        "updated_at": WHEN, "buyer": {"id": 500 + order_id},
        "manager": {"id": 4}, "manager_comment": None, "promocode": None,
        "products": [
            {"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
             "offer": {"product_id": 700 + i}}
            for i in range(products)
        ],
    }


async def _store_with(tmp_path: Path, ids) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "backfill.duckdb")
    await store.connect()
    with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
        await store.upsert_orders([_payload(i) for i in ids])
    return store


class _Ships:
    """Stands in for `write_orders`, remembering what each chunk carried."""

    def __init__(self):
        self.chunks = []

    async def __call__(self, orders, products, *, replace_products=True):
        self.chunks.append((
            [int(o[0]) for o in orders],
            [int(p[0]) for p in products],
            replace_products,
        ))


def _patches(ships, *, already_in_pg=()):
    return (
        patch("core.pg_landing.write_orders", new=ships),
        patch("core.pg.get_pool", new=AsyncMock(return_value=object())),
        patch("core.pg.require_revision", new=AsyncMock()),
        patch("core.pg_backfill._postgres_order_ids",
              new=AsyncMock(return_value=set(already_in_pg))),
    )


async def _run(store, *, already_in_pg=(), **kwargs):
    ships = _Ships()
    a, b, c, d = _patches(ships, already_in_pg=already_in_pg)
    with a, b, c, d:
        result = await backfill_orders(store, **kwargs)
    return result, ships


class TestItShipsTheDifference:
    @pytest.mark.asyncio
    async def test_an_empty_postgres_gets_everything(self, tmp_path):
        store = await _store_with(tmp_path, [1, 2, 3])
        result, ships = await _run(store)

        assert result["missing"] == 3
        assert result["orders_shipped"] == 3
        assert result["line_items_shipped"] == 6
        assert result["complete"] is True
        assert ships.chunks[0][0] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_what_postgres_already_holds_is_not_re_shipped(self, tmp_path):
        store = await _store_with(tmp_path, [1, 2, 3])
        result, ships = await _run(store, already_in_pg=[1, 3])

        assert result["missing"] == 1
        assert ships.chunks[0][0] == [2]

    @pytest.mark.asyncio
    async def test_a_second_run_after_a_complete_one_ships_nothing(self, tmp_path):
        """No cursor to be wrong: 'what is left' is recomputed every call."""
        store = await _store_with(tmp_path, [1, 2, 3])
        result, ships = await _run(store, already_in_pg=[1, 2, 3])

        assert result["missing"] == 0
        assert ships.chunks == []
        assert result["complete"] is True

    @pytest.mark.asyncio
    async def test_line_items_travel_with_their_order(self, tmp_path):
        store = await _store_with(tmp_path, [1, 2])
        _, ships = await _run(store, already_in_pg=[1])

        order_ids, line_ids, replace = ships.chunks[0]
        assert order_ids == [2]
        assert line_ids == [2000, 2001]
        assert replace is True


class TestChunking:
    @pytest.mark.asyncio
    async def test_the_lock_is_released_between_chunks(self, tmp_path):
        """Chunk boundaries fall between orders, never inside one."""
        store = await _store_with(tmp_path, [1, 2, 3, 4, 5])
        result, ships = await _run(store, chunk_size=2)

        assert [c[0] for c in ships.chunks] == [[1, 2], [3, 4], [5]]
        assert result["chunks_run"] == 3
        for order_ids, line_ids, _ in ships.chunks:
            for oid in order_ids:
                assert {oid * 1000, oid * 1000 + 1} <= set(line_ids)

    @pytest.mark.asyncio
    async def test_a_capped_run_stops_early_and_says_what_is_left(self, tmp_path):
        store = await _store_with(tmp_path, [1, 2, 3, 4, 5])
        result, ships = await _run(store, chunk_size=2, max_chunks=1)

        assert result["chunks_planned"] == 3
        assert result["chunks_run"] == 1
        assert result["orders_shipped"] == 2
        assert result["remaining"] == 3
        assert result["complete"] is False
        assert len(ships.chunks) == 1


class TestItRefusesRatherThanMisleads:
    @pytest.mark.asyncio
    async def test_it_will_not_fill_a_store_the_sync_has_abandoned(
        self, tmp_path, monkeypatch,
    ):
        """With the mirror off, a full Postgres is a snapshot pretending to
        be a mirror — worse than an empty one, which is at least honest."""
        from core.pg_landing import MIRROR_ENV

        monkeypatch.setenv(MIRROR_ENV, "0")
        store = await _store_with(tmp_path, [1])
        with pytest.raises(RuntimeError, match="switched off"):
            await backfill_orders(store)

    @pytest.mark.asyncio
    async def test_a_failed_chunk_raises_instead_of_reporting_success(self, tmp_path):
        """Unlike the mirror in the sync path, nothing here needs protecting."""
        store = await _store_with(tmp_path, [1, 2])
        with patch("core.pg_landing.write_orders",
                   side_effect=RuntimeError("postgres is down")), \
             patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.pg_backfill._postgres_order_ids",
                   new=AsyncMock(return_value=set())):
            with pytest.raises(RuntimeError, match="postgres is down"):
                await backfill_orders(store)
