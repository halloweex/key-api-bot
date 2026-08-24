"""Orders into the mirror: the parse, the write, and the backfill behind it.

The catalogue was easy — 1,003 rows, re-shipped whole every hour, so Postgres
was complete an hour after the mirror shipped. Orders are none of those things,
and three differences are what these tests are about.

**The mirror ships what the store wrote, not what the page contained.**
`upsert_orders` skips rows whose `updated_at` has not moved, so a page of 500
orders usually writes a handful. Shipping the page would send hundreds of
identical rows a minute; shipping `updated_ids` sends what changed.

**`manager_comment` is never overwritten with NULL.** DuckDB updates it as
`COALESCE(?, manager_comment)` because it carries UTM attribution a backfill
restored. 14,179 of 46,446 orders have it NULL, so a plain `EXCLUDED` on the
Postgres side would erase attribution on one store only — a divergence the
mirror would have created and then reported.

**Line items are replaced, not upserted.** Their ids are `order_id * 1000 +
position`, so an order dropping from three items to two leaves `…002` behind
under a plain upsert.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_landing
from core.duckdb_store import DuckDBStore
from core.landing_rows import (
    LINE_ITEM_ID_STRIDE,
    ORDER_COLUMNS,
    ORDER_PRODUCT_COLUMNS,
    landed_orders,
    order_product_rows,
    order_row,
)
from core.models import Order

WHEN = "2026-08-20T12:00:00+00:00"


def _payload(order_id, *, ordered_at=WHEN, total="100.00", products=2,
             manager_comment=None, updated_at=WHEN):
    return {
        "id": order_id,
        "source_id": 1,
        "status_id": 12,
        "status_group_id": 4,
        "grand_total": total,
        "ordered_at": ordered_at,
        "created_at": WHEN,
        "updated_at": updated_at,
        "buyer": {"id": 500 + order_id, "full_name": "Тест"},
        "manager": {"id": 4, "full_name": "Manager"},
        "manager_comment": manager_comment,
        "promocode": None,
        "products": [
            {"name": f"Товар {i}", "quantity": i + 1, "price_sold": "50.00",
             "offer": {"product_id": 700 + i}}
            for i in range(products)
        ],
    }


@pytest.fixture(autouse=True)
def _clean():
    pg_landing.reset_health()
    yield
    pg_landing.reset_health()


class TestTheSharedRow:
    def test_a_header_carries_exactly_the_columns_both_stores_write(self):
        assert ORDER_COLUMNS == (
            "id", "source_id", "status_id", "status_group_id", "grand_total",
            "ordered_at", "created_at", "updated_at", "buyer_id", "manager_id",
            "manager_comment", "promocode",
        )
        assert ORDER_PRODUCT_COLUMNS == (
            "id", "order_id", "product_id", "name", "quantity", "price_sold",
        )

    def test_no_bookkeeping_column_is_shared(self):
        """Two correct copies differ on these by construction."""
        for column in ("synced_at", "first_seen_at", "update_count", "mirrored_at"):
            assert column not in ORDER_COLUMNS
            assert column not in ORDER_PRODUCT_COLUMNS

    def test_a_missing_buyer_or_manager_is_a_null_not_a_crash(self):
        payload = _payload(1)
        payload["buyer"] = None
        payload["manager"] = None
        row = order_row(Order.from_api(payload))
        assert row.buyer_id is None and row.manager_id is None

    def test_line_item_ids_are_positional(self):
        rows = order_product_rows(Order.from_api(_payload(42, products=3)))
        assert [r.id for r in rows] == [
            42 * LINE_ITEM_ID_STRIDE, 42 * LINE_ITEM_ID_STRIDE + 1,
            42 * LINE_ITEM_ID_STRIDE + 2,
        ]
        assert all(r.order_id == 42 for r in rows)

    def test_an_order_with_no_date_takes_its_line_items_with_it(self):
        """A store must never hold line items whose order it refused."""
        landed = landed_orders([_payload(1), _payload(2, ordered_at=None)])
        assert [o.id for o in landed.orders] == [1]
        assert {p.order_id for p in landed.products} == {1}

    def test_products_can_be_left_unparsed(self):
        landed = landed_orders([_payload(1)], with_products=False)
        assert landed.orders and landed.products == []

    def test_duplicates_are_not_collapsed_here(self):
        """One home for the rule about which repeat wins, and it is upstream."""
        landed = landed_orders([_payload(1, total="1.00"), _payload(1, total="2.00")])
        assert len(landed.orders) == 2


class TestTheWriteStatement:
    def test_manager_comment_is_coalesced_not_overwritten(self):
        """The one column DuckDB refuses to null out, matched exactly."""
        sql = pg_landing._ORDER_UPSERT
        assert "manager_comment = COALESCE(EXCLUDED.manager_comment, bronze.orders.manager_comment)" in sql
        # Every other column is a plain overwrite.
        assert "grand_total     = EXCLUDED.grand_total" in sql

    def test_money_is_handed_over_as_decimal(self):
        """`OrderRow.grand_total` is a float; NUMERIC(12,2) wants a Decimal."""
        row = order_row(Order.from_api(_payload(1, total="100.10")))
        params = pg_landing._order_params(row)
        assert params[4] == Decimal("100.10")
        assert isinstance(params[4], Decimal)

    def test_a_float_does_not_become_a_binary_expansion(self):
        assert pg_landing._numeric(100.10) == Decimal("100.10")
        assert pg_landing._numeric(None) is None
        assert pg_landing._numeric(Decimal("7.00")) == Decimal("7.00")


class TestTheMirrorNeverRaises:
    @pytest.mark.asyncio
    async def test_a_failed_write_is_reported_not_thrown(self):
        rows = landed_orders([_payload(1)])
        with patch("core.pg_landing.write_orders", side_effect=RuntimeError("nope")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            outcomes = await pg_landing.mirror_orders(rows.orders, rows.products)

        assert [o.ok for o in outcomes] == [False, False]
        assert all("RuntimeError" in o.error for o in outcomes)
        assert pg_landing.health()["failures"] == {
            "bronze.orders": 1, "bronze.order_products": 1,
        }

    @pytest.mark.asyncio
    async def test_a_success_clears_the_failure_count(self):
        rows = landed_orders([_payload(1)])
        with patch("core.pg_landing.write_orders", side_effect=RuntimeError("nope")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            await pg_landing.mirror_orders(rows.orders, rows.products)
        with patch("core.pg_landing.write_orders", new=AsyncMock()):
            outcomes = await pg_landing.mirror_orders(rows.orders, rows.products)

        assert all(o.ok for o in outcomes)
        assert pg_landing.health()["failures"] == {}

    @pytest.mark.asyncio
    async def test_the_kill_switch_skips_without_touching_postgres(self, monkeypatch):
        monkeypatch.setenv(pg_landing.MIRROR_ENV, "0")
        rows = landed_orders([_payload(1)])
        with patch("core.pg_landing.write_orders", new=AsyncMock()) as w:
            outcomes = await pg_landing.mirror_orders(rows.orders, rows.products)
        w.assert_not_called()
        assert all(o.skipped for o in outcomes)

    @pytest.mark.asyncio
    async def test_nothing_to_ship_does_not_move_the_watermark(self):
        """A watermark is a claim about a shipment; an empty one made none."""
        with patch("core.pg_landing.write_orders", new=AsyncMock()) as w:
            outcomes = await pg_landing.mirror_orders([], [])
        w.assert_not_called()
        assert [o.skipped for o in outcomes] == ["no rows", "no rows"]

    @pytest.mark.asyncio
    async def test_a_status_only_refresh_leaves_the_products_watermark_alone(self):
        """`skip_products=True` never looked at line items, so it must not
        date-stamp them as current."""
        rows = landed_orders([_payload(1)], with_products=False)
        with patch("core.pg_landing.write_orders", new=AsyncMock()):
            outcomes = await pg_landing.mirror_orders(
                rows.orders, [], replace_products=False,
            )
        assert [o.table for o in outcomes] == ["bronze.orders"]


class TestWhatTheSyncActuallyShips:
    """The integration that matters: DuckDB decides, the mirror obeys."""

    async def _store(self, tmp_path: Path) -> DuckDBStore:
        store = DuckDBStore(db_path=tmp_path / "orders.duckdb")
        await store.connect()
        return store

    @pytest.mark.asyncio
    async def test_a_first_sync_ships_everything_it_wrote(self, tmp_path):
        store = await self._store(tmp_path)
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()) as m:
            await store.upsert_orders([_payload(1), _payload(2)])

        orders, products = m.call_args.args
        assert sorted(o.id for o in orders) == [1, 2]
        assert len(products) == 4
        assert m.call_args.kwargs["replace_products"] is True

    @pytest.mark.asyncio
    async def test_a_resync_of_unchanged_orders_ships_nothing(self, tmp_path):
        """The whole reason the mirror takes `updated_ids` and not the page."""
        store = await self._store(tmp_path)
        payloads = [_payload(1), _payload(2)]
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders(payloads)

        with patch("core.pg_landing.mirror_orders", new=AsyncMock()) as m:
            await store.upsert_orders(payloads)
        m.assert_not_called()

    @pytest.mark.asyncio
    async def test_only_the_order_that_moved_is_shipped(self, tmp_path):
        store = await self._store(tmp_path)
        later = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([_payload(1), _payload(2)])

        with patch("core.pg_landing.mirror_orders", new=AsyncMock()) as m:
            await store.upsert_orders(
                [_payload(1), _payload(2, total="999.00", updated_at=later)]
            )

        orders, products = m.call_args.args
        assert [o.id for o in orders] == [2]
        assert {p.order_id for p in products} == {2}

    @pytest.mark.asyncio
    async def test_a_status_refresh_ships_headers_and_says_so(self, tmp_path):
        store = await self._store(tmp_path)
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([_payload(1)])

        with patch("core.pg_landing.mirror_orders", new=AsyncMock()) as m:
            await store.upsert_orders(
                [_payload(1)], force_update=True, skip_products=True,
            )

        orders, products = m.call_args.args
        assert [o.id for o in orders] == [1]
        assert products == []
        assert m.call_args.kwargs["replace_products"] is False

    @pytest.mark.asyncio
    async def test_duckdb_still_holds_what_it_always_held(self, tmp_path):
        """The parse moved out of this function; the rows must not have."""
        store = await self._store(tmp_path)
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([_payload(7, total="250.50", products=3)])

        async with store.connection() as conn:
            row = conn.execute(
                "SELECT id, source_id, status_id, status_group_id, grand_total, "
                "buyer_id, manager_id FROM orders WHERE id = 7"
            ).fetchone()
            items = conn.execute(
                "SELECT id, order_id, product_id, name, quantity, price_sold "
                "FROM order_products WHERE order_id = 7 ORDER BY id"
            ).fetchall()

        assert row == (7, 1, 12, 4, Decimal("250.50"), 507, 4)
        assert [i[0] for i in items] == [7000, 7001, 7002]
        assert items[1][3] == "Товар 1"
        assert items[2][4] == 3
