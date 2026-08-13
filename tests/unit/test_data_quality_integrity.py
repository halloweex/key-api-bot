"""Tests for Layer-1 integrity checks.

These take a real DuckDB connection (set up in tmp_path) and validate that
each check fires on synthetic violations and stays silent on clean data.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from core.data_quality import (
    KNOWN_SOURCE_IDS,
    KNOWN_STATUS_IDS,
    Severity,
    check_internal_integrity,
    summarize_issues,
    _fk_orphan_check,
    _null_constraint_check,
    _pk_uniqueness_check,
    _value_domain_check,
)
from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _insert_minimal_order(conn, oid: int, **kw):
    defaults = {
        "source_id": 1, "status_id": 12, "grand_total": "100.00",
        "ordered_at": "2026-04-01T10:00:00+03:00",
        "created_at": "2026-04-01T10:00:00+03:00",
        "updated_at": "2026-04-01T10:00:00+03:00",
        "buyer_id": None, "manager_id": None,
        "manager_comment": None, "promocode": None,
    }
    defaults.update(kw)
    conn.execute(
        "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
        "created_at, updated_at, buyer_id, manager_id, manager_comment, promocode) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [oid, defaults["source_id"], defaults["status_id"], defaults["grand_total"],
         defaults["ordered_at"], defaults["created_at"], defaults["updated_at"],
         defaults["buyer_id"], defaults["manager_id"], defaults["manager_comment"],
         defaults["promocode"]],
    )


# ─── PK uniqueness check ──────────────────────────────────────────────────────


class TestPkUniqueness:
    @pytest.mark.asyncio
    async def test_clean_orders_no_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1)
                _insert_minimal_order(conn, 2)
                issues = _pk_uniqueness_check(conn, "orders")
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_empty_table_no_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                issues = _pk_uniqueness_check(conn, "orders")
            assert issues == []
        finally:
            await store.close()


# ─── FK orphan check ──────────────────────────────────────────────────────────


class TestFkOrphans:
    @pytest.mark.asyncio
    async def test_clean_no_orphans(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 100)
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [1, 100, 1, "test", 1, 100.0],
                )
                issues = _fk_orphan_check(conn, "order_products", "order_id", "orders")
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_orphan_detected(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [1, 99999, 1, "orphan", 1, 100.0],
                )
                issues = _fk_orphan_check(conn, "order_products", "order_id", "orders")
            assert len(issues) == 1
            assert issues[0].severity == Severity.CRITICAL
            assert issues[0].count == 1
            assert issues[0].check_name == "fk_orphan_order_products_order_id"
            assert 99999 in issues[0].sample_ids
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_multiple_orphans_sampled(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                for i, fake_oid in enumerate(range(90000, 90015)):
                    conn.execute(
                        "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        [i + 1, fake_oid, 1, "orphan", 1, 100.0],
                    )
                issues = _fk_orphan_check(conn, "order_products", "order_id", "orders")
            assert len(issues) == 1
            assert issues[0].count == 15
            assert 1 <= len(issues[0].sample_ids) <= 10
        finally:
            await store.close()


# ─── NULL constraint check ────────────────────────────────────────────────────


class TestNullChecks:
    @pytest.mark.asyncio
    async def test_clean_no_nulls(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1)
                issues = _null_constraint_check(conn, "orders", "ordered_at")
            assert issues == []
        finally:
            await store.close()


# ─── Value domain check ───────────────────────────────────────────────────────


class TestValueDomain:
    @pytest.mark.asyncio
    async def test_known_values_pass(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, status_id=12)
                _insert_minimal_order(conn, 2, status_id=19)
                issues = _value_domain_check(
                    conn, "orders", "status_id", KNOWN_STATUS_IDS, Severity.WARN,
                )
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_unknown_status_detected(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, status_id=999)
                issues = _value_domain_check(
                    conn, "orders", "status_id", KNOWN_STATUS_IDS, Severity.WARN,
                )
            assert len(issues) == 1
            assert issues[0].count == 1
            assert issues[0].severity == Severity.WARN
            assert "999" in issues[0].description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_unknown_source_detected(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, source_id=99)
                issues = _value_domain_check(
                    conn, "orders", "source_id", KNOWN_SOURCE_IDS, Severity.WARN,
                )
            assert len(issues) == 1
            assert "99" in issues[0].description
        finally:
            await store.close()


# ─── Aggregate check_internal_integrity ───────────────────────────────────────


class TestCheckInternalIntegrity:
    @pytest.mark.asyncio
    async def test_clean_db_zero_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                issues = check_internal_integrity(conn)
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_combined_violations_aggregated(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, status_id=999)
                _insert_minimal_order(conn, 2, source_id=99)
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [1, 88888, 1, "orphan", 1, 100.0],
                )
                issues = check_internal_integrity(conn)

            check_names = {i.check_name for i in issues}
            assert "fk_orphan_order_products_order_id" in check_names
            assert "value_domain_orders_status_id" in check_names
            assert "value_domain_orders_source_id" in check_names

            critical = [i for i in issues if i.severity == Severity.CRITICAL]
            warn = [i for i in issues if i.severity == Severity.WARN]
            assert len(critical) >= 1
            assert len(warn) >= 2
        finally:
            await store.close()


# ─── summarize_issues ─────────────────────────────────────────────────────────


class TestSummarizeIssues:
    def test_stable_shape(self):
        s = summarize_issues([])
        assert set(s.keys()) == {sv.value for sv in Severity}
        assert all(v == 0 for v in s.values())


class TestIntendedShipmentsAreCountedNotWarnedAbout:
    """An influence manager ships cosmetics to bloggers: line items, zero
    money, forever. That is the job. A check that reports it as a defect is
    one people stop reading — and this one had ₴1.19M of it inside a WARN."""

    @pytest.mark.asyncio
    async def test_internal_shipments_leave_the_warning_and_get_their_own_line(
        self, tmp_path,
    ):
        from core.data_quality import check_internal_integrity

        store = DuckDBStore(db_path=tmp_path / "t.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                # A retail order billed at zero — the real disagreement.
                conn.execute("""
                    INSERT INTO silver_orders
                    (id, source_id, status_id, grand_total, ordered_at, buyer_id,
                     manager_id, order_date, is_return, sales_type,
                     is_active_source, source_name)
                    VALUES (1, 4, 12, 0, now(), 10, 4, CURRENT_DATE, FALSE,
                            'retail', TRUE, 'Shopify')
                """)
                # An influence shipment — intended.
                conn.execute("""
                    INSERT INTO silver_orders
                    (id, source_id, status_id, grand_total, ordered_at, buyer_id,
                     manager_id, order_date, is_return, sales_type,
                     is_active_source, source_name)
                    VALUES (2, 4, 12, 0, now(), 20, 28, CURRENT_DATE, FALSE,
                            'internal', TRUE, 'Shopify')
                """)
                for oid, amount in ((1, 300.0), (2, 5000.0)):
                    conn.execute(
                        "INSERT INTO order_products (id, order_id, product_id, "
                        "name, quantity, price_sold) VALUES (?, ?, 1, 'x', 1, ?)",
                        [oid * 1000, oid, amount],
                    )
                issues = {i.check_name: i for i in check_internal_integrity(conn)}

            warned = issues["headline_vs_line_items"]
            assert warned.count == 1, "only the retail order is a disagreement"
            assert "300" in warned.description

            counted = issues["goods_shipped_without_sale"]
            assert counted.count == 1
            assert counted.severity == Severity.INFO, "a number, not a warning"
            assert "5,000.00" in counted.description
        finally:
            await store.close()


# ─── Inventory snapshot continuity ────────────────────────────────────────────


from datetime import date, datetime, timedelta, timezone  # noqa: E402

from core.data_quality import (  # noqa: E402
    INVENTORY_CONTINUITY_WINDOW_DAYS,
    _inventory_snapshot_continuity_check,
)


def _snapshot(conn, day: date, offers: int = 3):
    """Write one day's per-SKU snapshot, the way the 01:00 job would."""
    for offer_id in range(1, offers + 1):
        conn.execute(
            "INSERT INTO inventory_sku_history (date, offer_id, quantity, reserve, price) "
            "VALUES (?, ?, ?, ?, ?)",
            [day, offer_id, 10, 0, "9.99"],
        )


def _days_back(now: datetime, n: int) -> date:
    return now.date() - timedelta(days=n)


class TestInventorySnapshotContinuity:
    """A missed snapshot is the one loss here that cannot be re-fetched.

    Stock history is written from *current* stock and KeyCRM serves current
    stock only, so a day the job did not run is gone the moment the day is.
    """

    @pytest.mark.asyncio
    async def test_empty_history_is_not_an_accusation(self, tmp_path):
        """A fresh install has no snapshots and has lost nothing."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                assert _inventory_snapshot_continuity_check(conn) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_unbroken_run_is_silent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in range(1, 8):
                    _snapshot(conn, _days_back(now, n))
                assert _inventory_snapshot_continuity_check(conn, now=now) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_missing_today_alone_is_silent(self, tmp_path):
        """The job runs at 01:00; before that, today legitimately has none."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in range(1, 5):
                    _snapshot(conn, _days_back(now, n))
                assert _inventory_snapshot_continuity_check(conn, now=now) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_missing_yesterday_warns(self, tmp_path):
        """Yesterday absent means the job is failing now, and today is next."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in (2, 3, 4, 5):
                    _snapshot(conn, _days_back(now, n))
                issues = _inventory_snapshot_continuity_check(conn, now=now)
            assert len(issues) == 1
            assert issues[0].severity == Severity.WARN
            assert issues[0].check_name == "inventory_snapshot_gaps"
            assert issues[0].count == 1
            assert "cannot be backfilled" in issues[0].description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_old_gap_is_counted_not_warned(self, tmp_path):
        """Already-permanent losses are reported so the number is visible, but
        they are not a live fault — there is nothing left to act on."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in range(1, 11):
                    if n in (6, 7):      # a two-day hole, safely in the past
                        continue
                    _snapshot(conn, _days_back(now, n))
                issues = _inventory_snapshot_continuity_check(conn, now=now)
            assert len(issues) == 1
            assert issues[0].severity == Severity.INFO
            assert issues[0].count == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_gaps_before_the_first_snapshot_are_not_gaps(self, tmp_path):
        """History starting three days ago has not "missed" the days before it."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in (1, 2, 3):
                    _snapshot(conn, _days_back(now, n))
                assert _inventory_snapshot_continuity_check(conn, now=now) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_losses_older_than_the_window_stop_being_reported(self, tmp_path):
        """The 2026 hole is permanent. Reporting it forever is noise, and noise
        is the failure mode that let it happen."""
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            old_gap = INVENTORY_CONTINUITY_WINDOW_DAYS + 5
            async with store.connection() as conn:
                _snapshot(conn, _days_back(now, old_gap + 1))
                for n in range(1, INVENTORY_CONTINUITY_WINDOW_DAYS + 1):
                    _snapshot(conn, _days_back(now, n))
                assert _inventory_snapshot_continuity_check(conn, now=now) == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_wired_into_the_full_integrity_run(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in (2, 3, 4):
                    _snapshot(conn, _days_back(now, n))
                names = {i.check_name for i in check_internal_integrity(conn)}
            assert "inventory_snapshot_gaps" in names
        finally:
            await store.close()
