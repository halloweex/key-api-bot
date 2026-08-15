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


def _row(check_name: str, count: int, description: str = "", severity: str = "WARN"):
    """A persisted issue as fetch_run_issues returns it."""
    return {
        "check_name": check_name, "table_name": "silver_orders",
        "severity": severity, "count": count, "sample_ids": [1],
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

    def test_an_info_finding_alone_does_not_summon_a_digest(self):
        """Goods shipped to bloggers is a number, not a problem. A digest that
        arrives every day regardless stops being read on the day it matters."""
        sections = [DigestSection(
            layer="integrity", run=_run("PASS"), age_hours=2,
            issues=[_row("goods_shipped_without_sale", 448, severity="INFO")],
        )]
        assert build_digest(sections) is None

    def test_but_it_rides_along_when_there_is_something_to_say(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[
                _row("headline_vs_line_items", 12),
                _row("goods_shipped_without_sale", 448, severity="INFO"),
            ],
        )]
        msg = build_digest(sections)
        assert msg is not None
        assert "goods_shipped_without_sale: 448" in msg

    def test_long_lists_are_truncated(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=1,
            issues=[_row(f"check_{i}", i) for i in range(20)],
        )]
        msg = build_digest(sections, max_issue_lines=3)
        assert "…and 17 more" in msg


# ─── The unchanged standing WARN ────────────────────────────────────────────

class TestStandingFindingsGoQuiet:
    """`headline_vs_line_items: 414 (unchanged)` went out every morning for six
    days. The delta that says nobody needs to hear it again was already being
    computed, and the send decision ignored it."""

    NOW = datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc)

    def _standing(self, count=414):
        return [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[_row("headline_vs_line_items", count)],
            previous_issues=[_row("headline_vs_line_items", count)],
        )]

    def test_unchanged_warn_stays_quiet_the_day_after_it_was_sent(self):
        assert build_digest(
            self._standing(),
            last_sent_at=self.NOW - timedelta(days=1),
            now=self.NOW,
        ) is None

    def test_still_quiet_six_days_on(self):
        assert build_digest(
            self._standing(),
            last_sent_at=self.NOW - timedelta(days=6, hours=23),
            now=self.NOW,
        ) is None

    def test_restated_once_a_week(self):
        msg = build_digest(
            self._standing(),
            last_sent_at=self.NOW - timedelta(days=7),
            now=self.NOW,
        )
        assert msg is not None
        assert "headline_vs_line_items: 414" in msg
        assert "Repeated weekly" in msg

    def test_a_finding_that_moves_by_one_is_news_at_once(self):
        """+1 order is the whole signal this check exists to give. It must not
        wait six days behind the cooldown of the number it grew from."""
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[_row("headline_vs_line_items", 415)],
            previous_issues=[_row("headline_vs_line_items", 414)],
        )]
        msg = build_digest(sections, last_sent_at=self.NOW - timedelta(hours=1), now=self.NOW)
        assert msg is not None
        assert "415 (+1 since the last run)" in msg
        assert "Repeated weekly" not in msg

    def test_a_new_check_beside_a_standing_one_is_news(self):
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[_row("headline_vs_line_items", 414), _row("fk_orphans", 3)],
            previous_issues=[_row("headline_vs_line_items", 414)],
        )]
        msg = build_digest(sections, last_sent_at=self.NOW - timedelta(hours=1), now=self.NOW)
        assert msg is not None
        assert "fk_orphans: 3 (new)" in msg

    def test_a_stale_layer_speaks_through_the_quiet(self):
        """Silence has to mean "nothing changed", never "the checks stopped"."""
        sections = self._standing() + [
            DigestSection(layer="reconciliation", run=_run("PASS"), age_hours=52),
        ]
        msg = build_digest(sections, last_sent_at=self.NOW - timedelta(hours=1), now=self.NOW)
        assert msg is not None
        assert "52h old" in msg

    def test_an_unchanged_warn_below_the_truncation_line_is_not_swallowed(self):
        """A finding with no line has no delta the reader can check."""
        sections = [DigestSection(
            layer="integrity", run=_run("WARN"), age_hours=2,
            issues=[_row(f"check_{i}", 5) for i in range(6)],
            previous_issues=[_row(f"check_{i}", 5) for i in range(6)],
        )]
        msg = build_digest(
            sections, max_issue_lines=2,
            last_sent_at=self.NOW - timedelta(hours=1), now=self.NOW,
        )
        assert msg is not None
        assert "…and 4 more" in msg

    def test_info_alone_does_not_get_restated_either(self):
        """A week of silence does not turn blogger seeding into a problem."""
        sections = [DigestSection(
            layer="integrity", run=_run("PASS"), age_hours=2,
            issues=[_row("goods_shipped_without_sale", 774, severity="INFO")],
            previous_issues=[_row("goods_shipped_without_sale", 774, severity="INFO")],
        )]
        assert build_digest(
            sections, last_sent_at=self.NOW - timedelta(days=30), now=self.NOW,
        ) is None

    def test_a_caller_that_remembers_nothing_still_gets_told(self):
        """No marker means no evidence the reader has heard it. Say it."""
        msg = build_digest(self._standing(), now=self.NOW)
        assert msg is not None
        assert "headline_vs_line_items: 414" in msg

    def test_a_naive_marker_is_read_as_utc_rather_than_crashing(self):
        msg = build_digest(
            self._standing(),
            last_sent_at=datetime(2026, 8, 1, 9, 0),
            now=self.NOW,
        )
        assert msg is not None


class TestStandingDiscrepancies:
    """`2026-04 / src=1: qty DK=981 KC=2,112` went out word for word on nine
    consecutive mornings in July, because a discrepancy had no delta at all."""

    NOW = datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc)

    def _diff(self, dk=981.0, kc=2112.0, month="2026-04", field="qty"):
        return {
            "month": month, "source_id": 1, "field": field,
            "dk_value": dk, "kc_value": kc,
            "diff_class": "TOTAL_MISMATCH", "severity": "WARN",
            "order_ids": [1, 2, 3],
        }

    def _section(self, diffs, previous_diffs):
        return [DigestSection(
            layer="reconciliation", run=_run("WARN"), age_hours=3,
            diffs=diffs, previous_diffs=previous_diffs,
        )]

    def test_the_same_drift_at_the_same_distance_is_not_news(self):
        assert build_digest(
            self._section([self._diff()], [self._diff()]),
            last_sent_at=self.NOW - timedelta(days=1), now=self.NOW,
        ) is None

    def test_a_drift_that_widened_is_news(self):
        msg = build_digest(
            self._section([self._diff(dk=890.0)], [self._diff(dk=981.0)]),
            last_sent_at=self.NOW - timedelta(days=1), now=self.NOW,
        )
        assert msg is not None
        assert "DK=890" in msg

    def test_a_second_drift_beside_a_standing_one_is_news(self):
        msg = build_digest(
            self._section(
                [self._diff(), self._diff(month="2026-05")],
                [self._diff()],
            ),
            last_sent_at=self.NOW - timedelta(days=1), now=self.NOW,
        )
        assert msg is not None

    def test_a_drift_that_healed_is_news(self):
        """A layer going clean is the one change nobody should have to notice
        by the absence of a message."""
        msg = build_digest(
            [DigestSection(layer="reconciliation", run=_run("PASS"), age_hours=3,
                           diffs=[], previous_diffs=[self._diff()])],
            last_sent_at=self.NOW - timedelta(days=1), now=self.NOW,
        )
        assert msg is not None
        assert "clean" in msg

    def test_a_reshuffled_order_id_sample_is_not_a_change(self):
        a = self._diff()
        b = dict(self._diff(), order_ids=[9, 8, 7])
        assert build_digest(
            self._section([a], [b]),
            last_sent_at=self.NOW - timedelta(days=1), now=self.NOW,
        ) is None

    def test_a_standing_drift_is_restated_weekly(self):
        msg = build_digest(
            self._section([self._diff()], [self._diff()]),
            last_sent_at=self.NOW - timedelta(days=7, hours=1), now=self.NOW,
        )
        assert msg is not None
        assert "Repeated weekly" in msg


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
