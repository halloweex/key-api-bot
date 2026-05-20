"""Tests for duckdb_monthly_source_rollup.

The KeyCRM helper is exercised by integration tests on a real prod run —
no unit tests because the API client requires network and credentials.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.reconciliation_io import duckdb_monthly_source_rollup


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _insert_order(conn, oid: int, **kw):
    defaults = {
        "source_id": 1, "status_id": 12, "grand_total": "100.00",
        "ordered_at": "2026-04-01T10:00:00+03:00",
        "created_at": "2026-04-01T10:00:00+03:00",
        "updated_at": "2026-04-01T10:00:00+03:00",
    }
    defaults.update(kw)
    conn.execute("""
        INSERT INTO orders (id, source_id, status_id, grand_total,
                            ordered_at, created_at, updated_at,
                            buyer_id, manager_id, manager_comment, promocode)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
    """, [oid, defaults["source_id"], defaults["status_id"],
          defaults["grand_total"], defaults["ordered_at"],
          defaults["created_at"], defaults["updated_at"]])


def _insert_line(conn, lid: int, order_id: int, qty: int):
    conn.execute("""
        INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [lid, order_id, 1, "x", qty, 100.0])


class TestDuckdbMonthlySourceRollup:
    @pytest.mark.asyncio
    async def test_empty_db_empty_rollup(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            future_watermark = datetime.now(timezone.utc) + timedelta(days=1)
            async with store.connection() as conn:
                rollup = duckdb_monthly_source_rollup(
                    conn, date(2026, 1, 1), date(2026, 5, 1),
                    watermark=future_watermark,
                )
            assert rollup == {}
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_single_month_single_source(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1, source_id=1, grand_total="500.00",
                              ordered_at="2026-04-15T10:00:00+03:00")
                _insert_line(conn, 1, 1, 3)
                _insert_order(conn, 2, source_id=1, grand_total="200.00",
                              ordered_at="2026-04-20T10:00:00+03:00")
                _insert_line(conn, 2, 2, 1)

                future_watermark = datetime.now(timezone.utc) + timedelta(days=1)
                rollup = duckdb_monthly_source_rollup(
                    conn, date(2026, 4, 1), date(2026, 4, 30),
                    watermark=future_watermark,
                )

            assert rollup == {
                ("2026-04", 1): {
                    "orders": 2, "qty": 4, "revenue": 700.0,
                    "returns_count": 0, "returns_revenue": 0.0,
                },
            }
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_separates_returns_from_revenue(self, tmp_path):
        """Status 19 (RETURNED) must land in returns_count/revenue, not orders."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1, source_id=1, status_id=12,
                              grand_total="500.00",
                              ordered_at="2026-04-15T10:00:00+03:00")
                _insert_line(conn, 1, 1, 2)
                _insert_order(conn, 2, source_id=1, status_id=19,  # RETURNED
                              grand_total="100.00",
                              ordered_at="2026-04-20T10:00:00+03:00")
                _insert_line(conn, 2, 2, 1)

                future_watermark = datetime.now(timezone.utc) + timedelta(days=1)
                rollup = duckdb_monthly_source_rollup(
                    conn, date(2026, 4, 1), date(2026, 4, 30),
                    watermark=future_watermark,
                )

            cell = rollup[("2026-04", 1)]
            assert cell["orders"] == 1
            assert cell["revenue"] == 500.0
            assert cell["returns_count"] == 1
            assert cell["returns_revenue"] == 100.0
            # Returned order's qty does NOT count in qty (it's a return)
            assert cell["qty"] == 2

        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_watermark_excludes_recent_updates(self, tmp_path):
        """An order with updated_at >= watermark must not appear in rollup."""
        store = await _make_store(tmp_path)
        try:
            past = "2026-04-15T10:00:00+03:00"
            recent = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
            async with store.connection() as conn:
                _insert_order(conn, 1, source_id=1, grand_total="500.00",
                              ordered_at=past, updated_at=past)  # stable
                _insert_order(conn, 2, source_id=1, grand_total="200.00",
                              ordered_at=past, updated_at=recent)  # in-flight
                _insert_line(conn, 1, 1, 3)
                _insert_line(conn, 2, 2, 1)

                watermark = datetime.now(timezone.utc) - timedelta(hours=2)
                rollup = duckdb_monthly_source_rollup(
                    conn, date(2026, 4, 1), date(2026, 4, 30),
                    watermark=watermark,
                )

            cell = rollup[("2026-04", 1)]
            assert cell["orders"] == 1  # only the stable order
            assert cell["revenue"] == 500.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_groups_by_source_id(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1, source_id=1, grand_total="100.00",
                              ordered_at="2026-04-15T10:00:00+03:00")
                _insert_order(conn, 2, source_id=2, grand_total="200.00",
                              ordered_at="2026-04-15T10:00:00+03:00")
                _insert_order(conn, 3, source_id=4, grand_total="300.00",
                              ordered_at="2026-04-15T10:00:00+03:00")

                future_watermark = datetime.now(timezone.utc) + timedelta(days=1)
                rollup = duckdb_monthly_source_rollup(
                    conn, date(2026, 4, 1), date(2026, 4, 30),
                    watermark=future_watermark,
                )

            assert ("2026-04", 1) in rollup
            assert ("2026-04", 2) in rollup
            assert ("2026-04", 4) in rollup
            assert rollup[("2026-04", 1)]["revenue"] == 100.0
            assert rollup[("2026-04", 2)]["revenue"] == 200.0
            assert rollup[("2026-04", 4)]["revenue"] == 300.0
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_groups_by_kyiv_month_not_utc(self, tmp_path):
        """An order at 23:30 UTC on March 31 is April 1 in Kyiv (UTC+3).
        Rollup must respect Kyiv tz."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1, source_id=1, grand_total="100.00",
                              # 23:30 UTC March 31 → 02:30 April 1 in Kyiv
                              ordered_at="2026-03-31T23:30:00+00:00")
                future_watermark = datetime.now(timezone.utc) + timedelta(days=1)
                rollup = duckdb_monthly_source_rollup(
                    conn, date(2026, 3, 1), date(2026, 5, 1),
                    watermark=future_watermark,
                )
            assert ("2026-04", 1) in rollup  # April in Kyiv
            assert ("2026-03", 1) not in rollup
        finally:
            await store.close()
