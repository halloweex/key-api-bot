"""Tests for persist_run + fetch_latest_run.

These exercise the new data_quality_runs / _issues / _diffs schema and
verify the round-trip: insert run with issues/diffs, then read it back.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.data_quality import (
    Discrepancy,
    DiscrepancyClass,
    IntegrityIssue,
    Severity,
    fetch_last_success_ages,
    fetch_latest_run,
    persist_run,
)
from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _now_pair():
    started = datetime.now(timezone.utc)
    return started, started + timedelta(seconds=3)


class TestPersistRun:
    @pytest.mark.asyncio
    async def test_empty_run_pass(self, tmp_path):
        """A run with no issues and no discrepancies → status PASS."""
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            async with store.connection() as conn:
                run_id = persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined",
                    issues=[], discrepancies=[],
                )
                assert run_id > 0
                row = fetch_latest_run(conn)
            assert row is not None
            assert row["run_id"] == run_id
            assert row["status"] == "PASS"
            assert row["integrity_issues_count"] == 0
            assert row["discrepancies_count"] == 0
            assert row["critical_count"] == 0
            assert row["layer"] == "combined"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_critical_severity_propagated_to_status(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            issues = [IntegrityIssue(
                check_name="fk_orphan_order_products_order_id",
                table_name="order_products",
                severity=Severity.CRITICAL,
                count=3, sample_ids=(1, 2, 3),
                description="3 orphan rows",
            )]
            discrepancies = [Discrepancy(
                month="2026-04", source_id=1,
                diff_class=DiscrepancyClass.MISSING_IN_DK,
                field="orders", dk_value=0, kc_value=5,
                severity=Severity.CRITICAL,
            )]
            async with store.connection() as conn:
                run_id = persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined",
                    issues=issues, discrepancies=discrepancies,
                )
                row = fetch_latest_run(conn)

                # Children persisted?
                n_issues = conn.execute(
                    "SELECT COUNT(*) FROM data_quality_issues WHERE run_id = ?",
                    [run_id],
                ).fetchone()[0]
                n_diffs = conn.execute(
                    "SELECT COUNT(*) FROM data_quality_diffs WHERE run_id = ?",
                    [run_id],
                ).fetchone()[0]

            assert row["status"] == "CRITICAL"
            assert row["critical_count"] == 2
            assert n_issues == 1
            assert n_diffs == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_failed_run_status_overrides_severity(self, tmp_path):
        """When error_message is set, status is FAILED regardless of issues."""
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            async with store.connection() as conn:
                persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation",
                    issues=[], discrepancies=[],
                    error_message="KeyCRM API 500",
                )
                row = fetch_latest_run(conn)
            assert row["status"] == "FAILED"
            assert row["error_message"] == "KeyCRM API 500"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_sample_ids_persisted_as_json(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            started, ended = _now_pair()
            issues = [IntegrityIssue(
                check_name="pk_uniqueness_orders",
                table_name="orders",
                severity=Severity.CRITICAL,
                count=2, sample_ids=(101, 202),
                description="2 dupes",
            )]
            async with store.connection() as conn:
                run_id = persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity",
                    issues=issues, discrepancies=[],
                )
                stored = conn.execute(
                    "SELECT sample_ids FROM data_quality_issues WHERE run_id = ?",
                    [run_id],
                ).fetchone()[0]
            assert json.loads(stored) == [101, 202]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_duration_ms_computed(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            started = datetime.now(timezone.utc)
            ended = started + timedelta(milliseconds=2_500)
            async with store.connection() as conn:
                persist_run(
                    conn,
                    started_at=started, ended_at=ended,
                    as_of=started,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined",
                    issues=[], discrepancies=[],
                )
                row = fetch_latest_run(conn)
            assert 2000 <= row["duration_ms"] <= 3000
        finally:
            await store.close()


class TestFetchLatestRun:
    @pytest.mark.asyncio
    async def test_returns_none_when_empty(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                assert fetch_latest_run(conn) is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_returns_most_recent(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            t1 = datetime.now(timezone.utc) - timedelta(hours=2)
            t2 = datetime.now(timezone.utc)
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=t1, ended_at=t1 + timedelta(seconds=1),
                    as_of=t1, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined", issues=[], discrepancies=[],
                )
                new_id = persist_run(
                    conn, started_at=t2, ended_at=t2 + timedelta(seconds=1),
                    as_of=t2, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="combined", issues=[], discrepancies=[],
                )
                row = fetch_latest_run(conn)
            assert row["run_id"] == new_id
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_filter_by_layer(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            base = datetime.now(timezone.utc)
            async with store.connection() as conn:
                integrity_id = persist_run(
                    conn, started_at=base, ended_at=base + timedelta(seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                reconciliation_id = persist_run(
                    conn, started_at=base + timedelta(minutes=1),
                    ended_at=base + timedelta(minutes=1, seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                )
                latest_integrity = fetch_latest_run(conn, layer="integrity")
                latest_recon = fetch_latest_run(conn, layer="reconciliation")

            assert latest_integrity["run_id"] == integrity_id
            assert latest_recon["run_id"] == reconciliation_id
        finally:
            await store.close()


class TestFetchLastSuccessAges:
    """Freshness for the run-age watchdog — keyed on success, not on rows."""

    @pytest.mark.asyncio
    async def test_failed_runs_do_not_count_as_freshness(self, tmp_path):
        """The bug this exists for: 57 failed runs in a row read as 'ran recently'."""
        store = await _make_store(tmp_path)
        try:
            old = datetime.now(timezone.utc) - timedelta(days=3)
            recent = datetime.now(timezone.utc) - timedelta(minutes=5)
            async with store.connection() as conn:
                # A real verdict, three days ago.
                persist_run(
                    conn, started_at=old, ended_at=old + timedelta(seconds=1),
                    as_of=old, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                )
                # …then nothing but crashes, the most recent five minutes ago.
                persist_run(
                    conn, started_at=recent, ended_at=recent + timedelta(seconds=1),
                    as_of=recent, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                    error_message="429 Too Many Attempts",
                )
                ages = fetch_last_success_ages(conn)

            recon = ages["reconciliation"]
            # Must reflect the 3-day-old success, not the 5-minute-old failure.
            assert recon["age_seconds"] > 2 * 86400
            # Compare instants, not rendered dates: DuckDB returns TIMESTAMPTZ
            # in the session timezone, whose calendar day can differ from UTC's.
            returned = datetime.fromisoformat(recon["last_success_at"])
            assert abs((returned - old).total_seconds()) < 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_never_run_layer_reports_null_age(self, tmp_path):
        """A layer with no successes gets a key with a null age, not a missing key."""
        store = await _make_store(tmp_path)
        try:
            base = datetime.now(timezone.utc)
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=base, ended_at=base + timedelta(seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                ages = fetch_last_success_ages(conn)

            assert set(ages) == {"integrity", "reconciliation"}
            assert ages["integrity"]["age_seconds"] is not None
            assert ages["integrity"]["age_seconds"] < 60
            assert ages["reconciliation"] == {
                "last_success_at": None, "age_seconds": None,
            }
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_warn_and_critical_runs_still_count_as_fresh(self, tmp_path):
        """A check that found problems still ran — only an errored run did not."""
        store = await _make_store(tmp_path)
        try:
            base = datetime.now(timezone.utc)
            issue = IntegrityIssue(
                check_name="orphan_products", table_name="order_products",
                severity=Severity.CRITICAL, count=3, sample_ids=[1, 2, 3],
                description="orphans",
            )
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=base, ended_at=base + timedelta(seconds=1),
                    as_of=base, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[issue], discrepancies=[],
                )
                ages = fetch_last_success_ages(conn)

            assert ages["integrity"]["age_seconds"] is not None
        finally:
            await store.close()
