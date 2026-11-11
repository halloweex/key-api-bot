"""The ClickHouse arm reconciles only a copy that can hold the snapshot (L2-03).

KeyCRM's snapshot is cut at the job's watermark, two hours back; an order last
touched before it is compared, and ClickHouse holds it only if its last ship
read Silver after the order reached it — the watermark plus Silver's
derivation floor (`KS_PG_SILVER_INTERVAL_S`). The gate was "shipped within
3 h": a copy that last shipped 2 h 40 min ago (two hourly ships refused)
passed, and an order updated 2 h 20 min ago was filed MISSING_IN_DK, CRITICAL.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.scheduler import BackgroundScheduler

KC = {101: {"status_id": 12, "source_id": 4, "manager_id": None, "buyer_id": 7,
            "grand_total": 1500.0, "order_date": date(2026, 11, 5), "n_lines": 1,
            "qty": 1, "line_amount": 1500.0}}


async def _arm(last_ship_ago: timedelta, monkeypatch, floor_s: int = 600):
    monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", str(floor_s))
    started = datetime.now(timezone.utc)
    query = AsyncMock(return_value={})                  # the copy predates the order
    with patch("core.ch_common.configured", return_value=True), \
         patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=AsyncMock()), \
         patch("core.mirror_reconciliation.fetch_watermarks", new=AsyncMock(
             return_value={"clickhouse.silver_orders": {
                 "last_ok_at": started - last_ship_ago}})), \
         patch("core.reconciliation_io.pg_ids_updated_since",
               new=AsyncMock(return_value=set())), \
         patch("core.reconciliation_io.clickhouse_orders_in_window", new=query):
        scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
        result = await scheduler._reconcile_clickhouse(
            KC, window_start=date(2026, 8, 7), window_end=date(2026, 11, 5),
            as_of=started - timedelta(hours=2), inflight_ids=set())
    return result, query.await_count


class TestTheGate:
    @pytest.mark.asyncio
    async def test_a_copy_older_than_the_watermark_is_gated(self, monkeypatch):
        """Mutation: the 3 h gate again — the order is filed MISSING_IN_DK."""
        result, queried = await _arm(timedelta(hours=2, minutes=40), monkeypatch)
        assert queried == 0 and result["discrepancies"] == []
        assert [i.check_name for i in result["issues"]] == ["ch_reconcile_pending"]
        assert result["error"].startswith("ch_reconcile_pending: ")

    @pytest.mark.asyncio
    async def test_inside_the_floor_is_gated_too(self, monkeypatch):
        """Shipped 1 h 55 min ago: after the watermark, but before Silver had
        derived what Bronze held at it."""
        result, queried = await _arm(timedelta(hours=1, minutes=55), monkeypatch)
        assert queried == 0 and result["error"]

    @pytest.mark.asyncio
    async def test_the_floor_is_read_from_the_variable(self, monkeypatch):
        result, queried = await _arm(timedelta(hours=1, minutes=55), monkeypatch,
                                     floor_s=60)
        assert queried == 1 and result["error"] is None

    @pytest.mark.asyncio
    async def test_a_copy_shipped_after_it_is_compared(self, monkeypatch):
        result, queried = await _arm(timedelta(minutes=40), monkeypatch)
        assert queried == 1 and result["error"] is None
        assert [d.diff_class.value for d in result["discrepancies"]] == ["MISSING_IN_DK"]
