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
                # Chain 1's calendar, pre-read as the job reads it from
                # Postgres: none yet. Without it the continuity check says
                # it is not watching (`inventory_continuity_unwatched`).
                issues = check_internal_integrity(
                    conn, inventory_calendar=(None, frozenset()))
            assert issues == []
        finally:
            await store.close()


# ─── summarize_issues ─────────────────────────────────────────────────────────


class TestSummarizeIssues:
    def test_stable_shape(self):
        s = summarize_issues([])
        assert set(s.keys()) == {sv.value for sv in Severity}
        assert all(v == 0 for v in s.values())


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


def _calendar(now: datetime, days) -> tuple:
    """The `(first_day, present_days)` chain 1's Postgres reader hands the
    check (`pg_inventory_write.read_snapshot_calendar`), for snapshots taken
    `days` days back."""
    present = frozenset(_days_back(now, n) for n in days)
    return (min(present) if present else None), present


def _check(now: datetime, days):
    return _inventory_snapshot_continuity_check(None, now=now, calendar=_calendar(now, days))


class TestInventorySnapshotContinuity:
    """A missed snapshot is the one loss here that cannot be re-fetched.

    Stock history is written from *current* stock and KeyCRM serves current
    stock only, so a day the job did not run is gone the moment the day is.

    Judged over the calendar chain 1's Postgres reader pre-reads. Until step
    14's S14-11 these drove DuckDB's own `inventory_sku_history` (the check
    read it when no calendar was handed in, under a chain on DuckDB), and a
    test held that reading and the calendar's to the same findings; the
    expectations below are the ones those DuckDB-driven tests asserted.
    """

    def test_empty_history_is_not_an_accusation(self):
        """A fresh install has no snapshots and has lost nothing."""
        assert _check(datetime.now().astimezone(), ()) == []

    def test_unbroken_run_is_silent(self):
        assert _check(datetime.now().astimezone(), range(1, 8)) == []

    def test_missing_today_alone_is_silent(self):
        """The job runs at 01:00; before that, today legitimately has none."""
        assert _check(datetime.now().astimezone(), range(1, 5)) == []

    def test_missing_yesterday_warns(self):
        """Yesterday absent means the job is failing now, and today is next."""
        issues = _check(datetime.now().astimezone(), (2, 3, 4, 5))
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARN
        assert issues[0].check_name == "inventory_snapshot_gaps"
        assert issues[0].count == 1
        assert "cannot be backfilled" in issues[0].description

    def test_old_gap_is_counted_not_warned(self):
        """Already-permanent losses are reported so the number is visible, but
        they are not a live fault — there is nothing left to act on."""
        issues = _check(datetime.now().astimezone(),
                        [n for n in range(1, 11) if n not in (6, 7)])
        assert len(issues) == 1
        assert issues[0].severity == Severity.INFO
        assert issues[0].count == 2

    def test_gaps_before_the_first_snapshot_are_not_gaps(self):
        """History starting three days ago has not "missed" the days before it."""
        assert _check(datetime.now().astimezone(), (1, 2, 3)) == []

    def test_losses_older_than_the_window_stop_being_reported(self):
        """The 2026 hole is permanent. Reporting it forever is noise, and noise
        is the failure mode that let it happen."""
        old_gap = INVENTORY_CONTINUITY_WINDOW_DAYS + 5
        days = [old_gap + 1, *range(1, INVENTORY_CONTINUITY_WINDOW_DAYS + 1)]
        assert _check(datetime.now().astimezone(), days) == []

    @pytest.mark.asyncio
    async def test_wired_into_the_full_integrity_run(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                names = {i.check_name for i in check_internal_integrity(
                    conn, inventory_calendar=_calendar(now, (2, 3, 4)))}
            assert "inventory_snapshot_gaps" in names
        finally:
            await store.close()

    def test_without_a_calendar_it_says_it_is_not_watching(self):
        """A caller that forgot is loud, not silent and not wrong.

        This is the one table here that cannot be reconstructed at any price,
        so 'I am not watching it' has to reach a human. It must not report
        gaps either. Since step 14's S14-11 this holds whatever
        `KS_WRITE_INVENTORY` says: the check reads no DuckDB table at all.
        """
        issues = _inventory_snapshot_continuity_check(None, now=datetime.now().astimezone())
        assert [i.check_name for i in issues] == ["inventory_continuity_unwatched"]

    @pytest.mark.asyncio
    async def test_a_supplied_calendar_is_believed_over_the_local_table(self, tmp_path):
        """The calendar decides, so the check genuinely follows the writer.

        DuckDB is given a complete run of snapshots and the calendar a gap at
        yesterday. Reading the wrong one returns [], so this fails loudly if
        the local table is still consulted.
        """
        store = await _make_store(tmp_path)
        try:
            now = datetime.now().astimezone()
            async with store.connection() as conn:
                for n in range(1, 6):
                    _snapshot(conn, _days_back(now, n))
                first = _days_back(now, 5)
                present = frozenset(_days_back(now, n) for n in range(2, 6))
                issues = _inventory_snapshot_continuity_check(
                    conn, now=now, calendar=(first, present))
            assert [i.check_name for i in issues] == ["inventory_snapshot_gaps"]
            assert _days_back(now, 1).isoformat() in issues[0].description
        finally:
            await store.close()
