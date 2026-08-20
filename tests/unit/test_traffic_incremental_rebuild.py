"""gold_daily_traffic must be rebuilt for the dates that moved, not all of them.

The call site used to pass affected_dates=None unconditionally, so every one of
~240 warehouse refreshes a day rewrote the whole table. DuckDB cannot reclaim
the row versions that leaves while a writer is live, and the table reached 3.86M
stored rows behind 5 781 live ones — 667x amplification, and the largest single
contributor to the ~90 MB a day the database file grew.

The reason given for the full rebuild was real: UTM parsing can touch dates
outside affected_dates. These tests pin the answer to it — ask which ones.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore, UTM_DATE_LOOKUP_LIMIT


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "traffic.duckdb")
    await s.connect()
    return s


async def _silver_row(store: DuckDBStore, order_id: int, day: date) -> None:
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES (?, 1, 12, 100.0, NULL, NULL, NULL, ?, FALSE, 'retail',
                      TRUE, 'Instagram', FALSE, NULL, NULL)
            """,
            [order_id, day],
        )


class TestTrafficRebuildScope:

    @pytest.mark.asyncio
    async def test_full_silver_rebuild_means_full_traffic_rebuild(self, tmp_path):
        """Gold follows Silver's decision — the rule the other layers obey."""
        store = await _make_store(tmp_path)
        assert await store._traffic_rebuild_dates(None, {1, 2, 3}) is None

    @pytest.mark.asyncio
    async def test_no_utm_parsed_leaves_the_scope_alone(self, tmp_path):
        store = await _make_store(tmp_path)
        scope = {date(2026, 8, 1), date(2026, 8, 2)}
        assert await store._traffic_rebuild_dates(scope, set()) == scope

    @pytest.mark.asyncio
    async def test_utm_dates_outside_the_scope_are_added(self, tmp_path):
        """The exact case the old comment named as the reason for a full rebuild."""
        store = await _make_store(tmp_path)
        await _silver_row(store, 501, date(2026, 7, 4))   # parsed, outside scope
        await _silver_row(store, 502, date(2026, 8, 1))   # parsed, inside scope

        scope = {date(2026, 8, 1)}
        result = await store._traffic_rebuild_dates(scope, {501, 502})

        assert result == {date(2026, 8, 1), date(2026, 7, 4)}

    @pytest.mark.asyncio
    async def test_a_backfill_sized_parse_falls_back_to_a_full_rebuild(self, tmp_path):
        """Resolving that many ids costs more than the rebuild it would save."""
        store = await _make_store(tmp_path)
        huge = set(range(UTM_DATE_LOOKUP_LIMIT + 2))
        assert await store._traffic_rebuild_dates({date(2026, 8, 1)}, huge) is None

    @pytest.mark.asyncio
    async def test_nothing_moved_yields_an_empty_set_not_none(self, tmp_path):
        """The distinction the caller depends on.

        `refresh_traffic_gold_layer` reads a falsy affected_dates as "rebuild
        everything", so "nothing moved" must be distinguishable from "rebuild
        all" before it reaches that call. Empty set != None is the whole
        safety property; collapsing them restores the bug.
        """
        store = await _make_store(tmp_path)
        result = await store._traffic_rebuild_dates(set(), set())
        assert result == set()
        assert result is not None
