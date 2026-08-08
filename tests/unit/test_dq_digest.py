"""The daily digest, and the fingerprint that keys a DQ alert.

WARN findings were persisted on every run and pushed on none: only CRITICAL
was alerted at the moment it happened. Two standing WARNs worth ₴5.6M had
therefore never been said out loud. These tests pin the surface that says them.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.data_quality import (
    Discrepancy,
    DiscrepancyClass,
    DigestSection,
    IntegrityIssue,
    Severity,
    alert_fingerprint,
    build_digest,
    fetch_previous_run,
    fetch_run_issues,
    persist_run,
)
from core.duckdb_store import DuckDBStore


def _issue(check_name: str, count: int, severity=Severity.WARN, description=""):
    return IntegrityIssue(
        check_name=check_name, table_name="silver_orders", severity=severity,
        count=count, sample_ids=(1, 2, 3), description=description,
    )


def _row(check_name: str, count: int, description: str = ""):
    """A persisted issue as fetch_run_issues returns it."""
    return {
        "check_name": check_name, "table_name": "silver_orders",
        "severity": "WARN", "count": count, "sample_ids": [1],
        "description": description,
    }


def _run(status="WARN", started_at="2026-08-09T07:00:00+03:00", run_id=1):
    return {"run_id": run_id, "status": status, "started_at": started_at}


# ─── alert_fingerprint ──────────────────────────────────────────────────────

class TestAlertFingerprint:
    def test_same_problems_with_different_counts_share_a_key(self):
        """Counts move between runs; the identity of the problem does not."""
        a = alert_fingerprint("integrity", Severity.CRITICAL, [_issue("fk_orphans", 3)], [])
        b = alert_fingerprint("integrity", Severity.CRITICAL, [_issue("fk_orphans", 91)], [])
        assert a == b

    def test_a_different_check_gets_a_different_key(self):
        a = alert_fingerprint("integrity", Severity.CRITICAL, [_issue("fk_orphans", 3)], [])
        b = alert_fingerprint("integrity", Severity.CRITICAL, [_issue("pk_dupes", 3)], [])
        assert a != b

    def test_key_is_order_independent(self):
        one = [_issue("b_check", 1), _issue("a_check", 2)]
        two = [_issue("a_check", 5), _issue("b_check", 9)]
        assert (
            alert_fingerprint("integrity", Severity.WARN, one, [])
            == alert_fingerprint("integrity", Severity.WARN, two, [])
        )

    def test_discrepancies_key_on_field_and_class_not_amounts(self):
        def disc(dk, kc):
            return Discrepancy(
                month="2026-04", source_id=1,
                diff_class=DiscrepancyClass.MISSING_IN_DK,
                field="orders", dk_value=dk, kc_value=kc,
                severity=Severity.CRITICAL,
            )
        a = alert_fingerprint("reconciliation", Severity.CRITICAL, [], [disc(0, 5)])
        b = alert_fingerprint("reconciliation", Severity.CRITICAL, [], [disc(0, 40)])
        assert a == b

    def test_severity_and_layer_are_part_of_the_key(self):
        issues = [_issue("fk_orphans", 3)]
        assert (
            alert_fingerprint("integrity", Severity.WARN, issues, [])
            != alert_fingerprint("integrity", Severity.CRITICAL, issues, [])
        )
        assert (
            alert_fingerprint("integrity", Severity.WARN, issues, [])
            != alert_fingerprint("reconciliation", Severity.WARN, issues, [])
        )


# ─── build_digest ───────────────────────────────────────────────────────────

class TestBuildDigest:
    def test_quiet_day_sends_nothing(self):
        sections = [
            DigestSection(layer="integrity", run=_run("PASS"), age_hours=2),
            DigestSection(layer="reconciliation", run=_run("PASS"), age_hours=4),
        ]
        assert build_digest(sections) is None

    def test_warn_findings_are_surfaced(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[
                _row("headline_vs_line_items", 1184, "grand_total = 0 but 4,196,208.36 in line items"),
                _row("orders_without_line_items", 523),
            ],
        )]
        msg = build_digest(sections)
        assert msg is not None
        assert "headline_vs_line_items: 1,184" in msg
        assert "orders_without_line_items: 523" in msg
        assert "4,196,208.36" in msg

    def test_delta_against_the_previous_run(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[_row("headline_vs_line_items", 1184), _row("new_check", 7)],
            previous_issues=[_row("headline_vs_line_items", 1182)],
        )]
        msg = build_digest(sections)
        assert "1,184 (+2 since the last run)" in msg
        assert "7 (new)" in msg

    def test_a_standing_problem_reads_as_unchanged(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[_row("orders_without_line_items", 523)],
            previous_issues=[_row("orders_without_line_items", 523)],
        )]
        assert "(unchanged)" in build_digest(sections)

    def test_stale_layer_is_called_out(self):
        sections = [
            DigestSection(layer="reconciliation", run=_run("PASS"), age_hours=52),
        ]
        msg = build_digest(sections)
        assert msg is not None
        assert "52h old" in msg

    def test_layer_that_never_ran_is_called_out(self):
        msg = build_digest([DigestSection(layer="reconciliation", run=None)])
        assert msg is not None
        assert "no successful run on record" in msg

    def test_discrepancies_are_listed(self):
        sections = [DigestSection(
            layer="reconciliation", run=_run("CRITICAL"), age_hours=3,
            diffs=[{
                "month": "2026-04", "source_id": 1, "field": "orders",
                "dk_value": 565.0, "kc_value": 566.0,
                "diff_class": "MISSING_IN_DK", "severity": "CRITICAL",
            }],
        )]
        msg = build_digest(sections)
        assert "2026-04 / src=1: orders DK=565 KC=566 (MISSING_IN_DK)" in msg

    def test_long_lists_are_truncated(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=1,
            issues=[_row(f"check_{i}", i) for i in range(20)],
        )]
        msg = build_digest(sections, max_issue_lines=3)
        assert "…and 17 more" in msg


# ─── fetch_previous_run ─────────────────────────────────────────────────────

async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


class TestFetchPreviousRun:
    @pytest.mark.asyncio
    async def test_skips_failed_runs(self, tmp_path):
        """A failed run's zero counts would read as 'fixed, then broke again'."""
        store = await _make_store(tmp_path)
        try:
            t0 = datetime.now(timezone.utc) - timedelta(days=2)
            async with store.connection() as conn:
                first = persist_run(
                    conn, started_at=t0, ended_at=t0, as_of=t0,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[_issue("headline_vs_line_items", 1182)],
                    discrepancies=[],
                )
                persist_run(
                    conn, started_at=t0 + timedelta(days=1), ended_at=t0 + timedelta(days=1),
                    as_of=t0, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                    error_message="KeyCRMAPIError: 429",
                )
                current = persist_run(
                    conn, started_at=t0 + timedelta(days=2), ended_at=t0 + timedelta(days=2),
                    as_of=t0, window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[_issue("headline_vs_line_items", 1184)],
                    discrepancies=[],
                )
                previous = fetch_previous_run(conn, "integrity", current)
                previous_issues = fetch_run_issues(conn, previous["run_id"])

            assert previous["run_id"] == first
            assert previous_issues[0]["count"] == 1182
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_returns_none_when_there_is_no_earlier_run(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                run_id = persist_run(
                    conn, started_at=now, ended_at=now, as_of=now,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                assert fetch_previous_run(conn, "integrity", run_id) is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_does_not_cross_layers(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:
                persist_run(
                    conn, started_at=now, ended_at=now, as_of=now,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="reconciliation", issues=[], discrepancies=[],
                )
                integrity_run = persist_run(
                    conn, started_at=now, ended_at=now, as_of=now,
                    window_start=date(2026, 1, 1), window_end=date(2026, 5, 1),
                    layer="integrity", issues=[], discrepancies=[],
                )
                assert fetch_previous_run(conn, "integrity", integrity_run) is None
        finally:
            await store.close()


# ─── _send_dq_alert_throttled keys per problem, not per layer ───────────────

class TestDqAlertThrottleKeying:
    def setup_method(self):
        from core.scheduler import BackgroundScheduler
        BackgroundScheduler._dq_last_alert.clear()

    teardown_method = setup_method

    @pytest.mark.asyncio
    async def test_a_second_problem_in_the_same_layer_is_not_swallowed(self, monkeypatch):
        """The cooldown used to be per layer: one CRITICAL muted every other
        CRITICAL in that layer for 24 hours."""
        import importlib
        from unittest.mock import AsyncMock
        from core.scheduler import BackgroundScheduler
        from core.telegram_alerts import reset_throttle

        # `bot.main` is also a *function* on the `bot` package, so the module
        # has to be fetched explicitly.
        bot_main = importlib.import_module("bot.main")
        reset_throttle()
        sender = AsyncMock()
        monkeypatch.setattr(bot_main, "send_admin_message", sender)

        scheduler = BackgroundScheduler()
        first = alert_fingerprint(
            "integrity", Severity.CRITICAL, [_issue("fk_orphans", 3)], [])
        second = alert_fingerprint(
            "integrity", Severity.CRITICAL, [_issue("pk_dupes", 9)], [])

        assert await scheduler._send_dq_alert_throttled("integrity", "orphans", first)
        assert await scheduler._send_dq_alert_throttled("integrity", "dupes", second)
        # …and the same problem again within the cooldown is not resent.
        assert not await scheduler._send_dq_alert_throttled("integrity", "orphans again", first)
        assert sender.await_count == 2
