"""Reconciliation A: Postgres landing against DuckDB landing.

The check exists because step 05 closes on it — zero findings, tolerance zero
— and the hard part was never the comparison. It was telling the one
difference that is not a defect from the one that is, and both look like "one
row short" from a row count.

Production, the morning the mirror first ran: DuckDB 1,004 products, Postgres
1,003. The extra is id 1055, last synced 2026-06-13; KeyCRM stopped serving it
and `upsert_products` never deletes, so a payload-fed mirror can never learn of
it again. `meta.mirror_state.last_ok_at` is what separates that from a lost
row, because a successful mirror ships the whole catalogue — so anything DuckDB
wrote *before* the last success and Postgres still lacks is a row KeyCRM has
retired, and anything written *after* it is either in flight or lost.

Those three cases, and the suppression that keeps an unshipped table from
reporting every one of its rows as missing, are what these tests pin.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from core.data_quality import (
    DIGEST_MAX_AGE_HOURS,
    WATCHED_LAYERS,
    Severity,
)
from core.duckdb_store import DuckDBStore
from core.mirror_reconciliation import (
    MIRROR_LAYER,
    MIRRORED_TABLES,
    MirroredTable,
    compare_table,
    configured,
    fetch_duckdb_rows,
    read_duckdb_side,
    reconcile_mirror,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
LAST_OK = NOW - timedelta(hours=4)

PRODUCTS = MIRRORED_TABLES[0]
CATEGORIES = MIRRORED_TABLES[1]


def _watermark(last_ok_at=LAST_OK, failures=0, last_error=None):
    return {
        "table_name": "bronze.products",
        "last_attempted_at": last_ok_at,
        "last_ok_at": last_ok_at,
        "failures_since_ok": failures,
        "last_error": last_error,
        "last_rows": 3,
    }


def _row(id_, name="Toner", cat=11, brand="Wellage", sku="1678", price="700.00"):
    return (id_, name, cat, brand, sku, Decimal(price))


def _sides(ids=(1, 2, 3), synced=None):
    """Three identical products on both sides, all synced before the mirror ran."""
    synced = synced or (LAST_OK - timedelta(days=1))
    dk = {i: _row(i) for i in ids}
    return dk, {i: synced for i in ids}, {i: _row(i) for i in ids}


def _names(issues):
    return {i.check_name for i in issues}


def _by_name(issues, name):
    return next(i for i in issues if i.check_name == name)


class TestAgreement:
    def test_identical_sides_report_nothing(self):
        dk, synced, pg = _sides()
        assert compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW) == []

    def test_decimal_and_float_price_are_the_same_price(self):
        """`0.00` against `0.0` is a repr, not a discrepancy.

        Both drivers return Decimal for these columns today. The normalisation
        is for the day one of them stops, and this pins that a difference in
        type never reaches the report.
        """
        dk, synced, pg = _sides(ids=(1,))
        dk[1] = (1, "Toner", 11, "Wellage", "1678", 700.0)
        pg[1] = (1, "Toner", 11, "Wellage", "1678", Decimal("700.00"))
        # Both sides go through the same normaliser in the real read path.
        from core.mirror_reconciliation import _normalise_row
        dk[1] = _normalise_row(dk[1], PRODUCTS.columns, PRODUCTS.numeric)
        pg[1] = _normalise_row(pg[1], PRODUCTS.columns, PRODUCTS.numeric)
        assert compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW) == []


class TestTheRowKeyCRMForgot:
    """The 1004/1003 case. INFO, counted, never a defect."""

    def test_row_written_before_the_last_success_is_retired_not_lost(self):
        dk, synced, pg = _sides()
        dk[1055] = _row(1055)
        synced[1055] = datetime(2026, 6, 13, 9, 46, tzinfo=timezone.utc)
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)

        assert _names(issues) == {"mirror_retired_rows"}
        found = _by_name(issues, "mirror_retired_rows")
        assert found.severity is Severity.INFO
        assert found.count == 1
        assert found.sample_ids == (1055,)

    def test_retired_rows_never_summon_a_critical(self):
        """An INFO finding is content; it must not make the run CRITICAL."""
        from core.data_quality import overall_severity

        dk, synced, pg = _sides()
        dk[1055] = _row(1055)
        synced[1055] = LAST_OK - timedelta(days=70)
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)
        assert overall_severity(issues, []) is Severity.INFO


class TestTheRowTheMirrorLost:
    def test_written_after_the_last_success_and_past_grace_is_critical(self):
        dk, synced, pg = _sides()
        dk[9001] = _row(9001)
        synced[9001] = LAST_OK + timedelta(minutes=30)   # after the mirror ran
        issues = compare_table(
            PRODUCTS, dk, synced, pg, _watermark(), now=NOW, grace_minutes=15,
        )

        found = _by_name(issues, "mirror_missing_rows")
        assert found.severity is Severity.CRITICAL
        assert found.sample_ids == (9001,)
        assert "mirror_retired_rows" not in _names(issues)

    def test_written_inside_the_grace_window_is_in_flight(self):
        """The two writes are consecutive statements; a sync mid-flight is not a fault."""
        dk, synced, pg = _sides()
        dk[9001] = _row(9001)
        synced[9001] = NOW - timedelta(minutes=5)
        issues = compare_table(
            PRODUCTS, dk, synced, pg, _watermark(), now=NOW, grace_minutes=15,
        )
        assert issues == []

    def test_a_null_synced_at_is_treated_as_old(self):
        """Unknown provenance must not be reported as a mirror failure."""
        dk, synced, pg = _sides()
        dk[9001] = _row(9001)
        synced[9001] = None
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)
        assert _names(issues) == {"mirror_retired_rows"}


class TestTheOtherDirections:
    def test_row_only_in_postgres_is_an_orphan(self):
        dk, synced, pg = _sides()
        pg[4242] = _row(4242)
        found = _by_name(
            compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW),
            "mirror_orphan_rows",
        )
        assert found.severity is Severity.WARN
        assert found.sample_ids == (4242,)

    def test_a_value_disagreement_is_critical_and_names_the_column(self):
        """No arithmetic sits between the two sides, so drift is impossible."""
        dk, synced, pg = _sides()
        pg[2] = _row(2, brand="Wellagе")   # Cyrillic е — the classic
        pg[3] = _row(3, price="701.00")
        issues = compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW)

        found = _by_name(issues, "mirror_row_values")
        assert found.severity is Severity.CRITICAL
        assert found.count == 2
        assert found.sample_ids == (2, 3)
        assert "brand (1)" in found.description
        assert "price (1)" in found.description

    def test_null_on_one_side_only_is_a_disagreement(self):
        """423 of 1,004 products have no brand; a NULL that appears is still news."""
        dk, synced, pg = _sides()
        pg[2] = _row(2, brand=None)
        found = _by_name(
            compare_table(PRODUCTS, dk, synced, pg, _watermark(), now=NOW),
            "mirror_row_values",
        )
        assert found.sample_ids == (2,)


class TestTheWatermarkItself:
    def test_never_shipped_reports_once_and_suppresses_the_rows(self):
        """`bronze.categories` was empty for a schedule, not a fault.

        Without the suppression the check's first act would be 28 categories
        reported missing every morning until the weekly full sync runs.
        """
        dk = {i: (i, f"cat {i}", None) for i in range(1, 29)}
        synced = {i: LAST_OK - timedelta(days=3) for i in dk}
        issues = compare_table(CATEGORIES, dk, synced, {}, None, now=NOW)

        assert _names(issues) == {"mirror_never_shipped"}
        found = issues[0]
        assert found.severity is Severity.WARN
        assert found.count == 28

    def test_a_watermark_with_no_success_counts_as_never_shipped(self):
        dk, synced, pg = _sides()
        issues = compare_table(
            PRODUCTS, dk, synced, pg, _watermark(last_ok_at=None), now=NOW,
        )
        assert _names(issues) == {"mirror_never_shipped"}

    def test_a_failing_mirror_is_critical_and_carries_its_error(self):
        """The state `/api/health` was documented to publish and does not."""
        dk, synced, pg = _sides()
        issues = compare_table(
            PRODUCTS, dk, synced, pg,
            _watermark(failures=7, last_error="NotNullViolationError: name"),
            now=NOW,
        )
        found = _by_name(issues, "mirror_failing")
        assert found.severity is Severity.CRITICAL
        assert found.count == 7
        assert "NotNullViolationError" in found.description

    def test_failing_and_never_shipped_are_both_reported(self):
        dk, synced, pg = _sides()
        issues = compare_table(
            PRODUCTS, dk, synced, pg,
            _watermark(last_ok_at=None, failures=3, last_error="boom"),
            now=NOW,
        )
        assert _names(issues) == {"mirror_failing", "mirror_never_shipped"}

    def test_a_naive_timestamp_does_not_crash_the_comparison(self):
        """asyncpg returns tz-aware; a fixture or a driver change may not."""
        dk, synced, pg = _sides()
        dk[1055] = _row(1055)
        synced[1055] = (LAST_OK - timedelta(days=1)).replace(tzinfo=None)
        naive = _watermark(last_ok_at=LAST_OK.replace(tzinfo=None))
        assert _names(compare_table(PRODUCTS, dk, synced, pg, naive, now=NOW)) == {
            "mirror_retired_rows"
        }


class TestTheKillSwitch:
    @pytest.mark.asyncio
    async def test_mirror_off_reports_that_and_compares_nothing(self):
        """"1,004 rows missing" would be true and useless."""
        with patch("core.pg_landing.enabled", return_value=False):
            issues = await reconcile_mirror({})
        assert _names(issues) == {"mirror_disabled"}
        assert issues[0].severity is Severity.WARN

    def test_configured_is_false_without_a_dsn(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert configured() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        assert configured() is True


class TestAgainstARealDuckDB:
    """The read half, against the schema it actually runs on."""

    async def _store(self, tmp_path: Path) -> DuckDBStore:
        store = DuckDBStore(db_path=tmp_path / "mirror.duckdb")
        await store.connect()
        return store

    @pytest.mark.asyncio
    async def test_reads_the_shared_contract_and_synced_at(self, tmp_path):
        store = await self._store(tmp_path)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO products (id, name, category_id, brand, sku, price) "
                "VALUES (7, 'Тонер', 11, 'Wellage', '1678', 700.00)"
            )
            conn.execute(
                "INSERT INTO categories (id, name, parent_id) VALUES (11, 'Face', NULL)"
            )
            rows, synced = fetch_duckdb_rows(conn, PRODUCTS)
            side = read_duckdb_side(conn)

        assert rows == {7: (7, "Тонер", 11, "Wellage", "1678", Decimal("700.00"))}
        assert synced[7] is not None
        assert set(side) == {"bronze.products", "bronze.categories"}
        assert side["bronze.categories"][0] == {11: (11, "Face", None)}

    @pytest.mark.asyncio
    async def test_a_real_read_compares_clean_against_an_identical_mirror(self, tmp_path):
        """End to end over the read path, with Postgres played by a dict."""
        store = await self._store(tmp_path)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO products (id, name, category_id, brand, sku, price) "
                "VALUES (7, 'Тонер', 11, NULL, '1678', 0.00)"
            )
            dk_rows, synced = fetch_duckdb_rows(conn, PRODUCTS)

        assert compare_table(
            PRODUCTS, dk_rows, synced, dict(dk_rows), _watermark(), now=NOW,
        ) == []


class TestWiring:
    def test_the_layer_is_watched(self):
        """Absent from WATCHED_LAYERS, the catch-up re-queues it every restart."""
        assert MIRROR_LAYER in WATCHED_LAYERS
        assert MIRROR_LAYER in DIGEST_MAX_AGE_HOURS

    def test_the_job_is_registered_daily_and_clear_of_the_sunday_cron(self):
        from core.scheduler import CATCHUP_CHECKS

        assert CATCHUP_CHECKS["dq_mirror_landing"][0] == MIRROR_LAYER

    def test_both_landing_tables_are_covered(self):
        assert {s.pg_table for s in MIRRORED_TABLES} == {
            "bronze.products", "bronze.categories",
        }
        for spec in MIRRORED_TABLES:
            assert isinstance(spec, MirroredTable)
            assert spec.columns[0] == "id"
