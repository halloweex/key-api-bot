"""Reclassifying a manager must not restate the reports already written.

`sales_type` is materialised into Silver from the manager's classification at
*rebuild* time. That made every classification retroactive by construction:
flipping one manager moved every order they had ever taken, so last year's
revenue changed on the next two-minute refresh and nothing said so. The owner
ruled that out on 2026-08-20 — the answer is resolved as of the order's own
date, from `manager_classifications`.

The second class of test here covers the other half of the same seam. Silver
stores the answer; ten of the eleven `_build_sales_type_filter` call sites used
to re-derive it from `manager_id`, which is a second definition wearing a
different hat — and the two had already diverged over source 5.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_constants import EXHIBITION_SOURCE_ID, RETAIL_MANAGER_IDS
from core.duckdb_store import DuckDBStore

UNLISTED = 34  # deliberately not in RETAIL_MANAGER_IDS


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _api_manager(mid: int, name: str = "Someone"):
    return {"id": mid, "name": name, "email": f"{mid}@example.com", "status": "active"}


def _insert_order(conn, oid: int, manager_id, when: datetime, source_id: int = 1):
    conn.execute(
        "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
        "buyer_id, manager_id) VALUES (?, ?, 1, 1000.0, ?, 1, ?)",
        [oid, source_id, when, manager_id],
    )


async def _sales_types(store) -> dict[int, str]:
    async with store.connection() as conn:
        return dict(conn.execute("SELECT id, sales_type FROM silver_orders").fetchall())


class TestClassificationIsResolvedAsOfTheOrderDate:
    @pytest.mark.asyncio
    async def test_a_change_applies_forward_and_leaves_the_past_alone(self, tmp_path):
        """The test that fails against every version of this code before 2026-08-20."""
        store = await _make_store(tmp_path)
        try:
            assert UNLISTED not in RETAIL_MANAGER_IDS
            await store.upsert_managers([_api_manager(UNLISTED)])

            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                _insert_order(conn, 1, UNLISTED, now - timedelta(days=400))
                _insert_order(conn, 2, UNLISTED, now - timedelta(days=100))

            await store.set_manager_retail_status(
                UNLISTED, True,
                effective_from=(now - timedelta(days=200)).date(),
            )
            await store.refresh_warehouse_layers(trigger="manual")

            types = await _sales_types(store)
            assert types[1] == "internal", "an order before the change was restated"
            assert types[2] == "retail", "the change did not apply going forward"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_without_a_date_the_change_starts_today(self, tmp_path):
        """Silence means forward-only. Backdating is a thing you ask for."""
        store = await _make_store(tmp_path)
        try:
            await store.upsert_managers([_api_manager(UNLISTED)])
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                _insert_order(conn, 1, UNLISTED, now - timedelta(days=30))

            await store.set_manager_retail_status(UNLISTED, True)
            await store.refresh_warehouse_layers(trigger="manual")

            assert (await _sales_types(store))[1] == "internal"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_two_changes_leave_three_eras_intact(self, tmp_path):
        """Intervals compose; the middle era is the one a single flag cannot hold."""
        store = await _make_store(tmp_path)
        try:
            await store.upsert_managers([_api_manager(UNLISTED)])
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                _insert_order(conn, 1, UNLISTED, now - timedelta(days=400))
                _insert_order(conn, 2, UNLISTED, now - timedelta(days=200))
                _insert_order(conn, 3, UNLISTED, now - timedelta(days=30))

            await store.set_manager_retail_status(
                UNLISTED, True, effective_from=(now - timedelta(days=300)).date())
            await store.set_manager_retail_status(
                UNLISTED, False, effective_from=(now - timedelta(days=100)).date())
            await store.refresh_warehouse_layers(trigger="manual")

            types = await _sales_types(store)
            assert [types[1], types[2], types[3]] == ["internal", "retail", "internal"]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_manager_nobody_classified_behaves_exactly_as_before(self, tmp_path):
        """The fallback matters more than the feature: no interval, no change."""
        store = await _make_store(tmp_path)
        try:
            known_retail = RETAIL_MANAGER_IDS[0]
            await store.upsert_managers([_api_manager(known_retail)])
            async with store.connection() as conn:
                conn.execute("DELETE FROM manager_classifications")
                _insert_order(conn, 1, known_retail,
                              datetime.now(timezone.utc) - timedelta(days=10))

            await store.refresh_warehouse_layers(trigger="manual")
            assert (await _sales_types(store))[1] == "retail"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_every_synced_manager_gets_a_baseline_interval(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await store.upsert_managers(
                [_api_manager(RETAIL_MANAGER_IDS[0]), _api_manager(UNLISTED)]
            )
            await store._run_migrations()

            async with store.connection() as conn:
                rows = dict(conn.execute(
                    "SELECT manager_id, is_retail FROM manager_classifications "
                    "WHERE valid_from = DATE '1970-01-01'"
                ).fetchall())
            assert rows[RETAIL_MANAGER_IDS[0]] is True
            assert rows[UNLISTED] is False
        finally:
            await store.close()


class TestConsumersReadTheStoredAnswer:
    @pytest.mark.asyncio
    async def test_an_exhibition_order_is_not_counted_as_retail(self, tmp_path):
        """#101 gave source 5 its own sales_type; the filter never heard about it.

        On production that was 176 orders and ₴267,416 counted as exhibition by
        Gold and as retail by every endpoint that went through this filter.
        """
        store = await _make_store(tmp_path)
        try:
            staffed_by = RETAIL_MANAGER_IDS[0]
            await store.upsert_managers([_api_manager(staffed_by)])
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                _insert_order(conn, 1, staffed_by, now - timedelta(days=10))
                _insert_order(conn, 2, staffed_by, now - timedelta(days=10),
                              source_id=EXHIBITION_SOURCE_ID)

            await store.refresh_warehouse_layers(trigger="manual")

            async with store.connection() as conn:
                retail = [r[0] for r in conn.execute(
                    "SELECT o.id FROM orders o WHERE "
                    + store._build_sales_type_filter("retail")
                ).fetchall()]
                exhibition = [r[0] for r in conn.execute(
                    "SELECT o.id FROM orders o WHERE "
                    + store._build_sales_type_filter("exhibition")
                ).fetchall()]

            assert retail == [1], "the exhibition order leaked back into retail"
            assert exhibition == [2]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_unknown_sales_type_is_refused_rather_than_read_as_retail(self, tmp_path):
        """It used to fall through the else and quietly mean 'retail'."""
        store = await _make_store(tmp_path)
        try:
            with pytest.raises(ValueError):
                store._build_sales_type_filter("wholsale")
            assert store._build_sales_type_filter("all") == "1=1"
        finally:
            await store.close()
