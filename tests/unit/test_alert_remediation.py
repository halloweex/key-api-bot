"""A page that does not name a lever is a page people learn to swipe.

Rule 1 of the alerts charter, and rule 5's second half: the alert must not
*trigger* a repair, but it owes the reader the count of what the machine has
already tried — otherwise the honest reading of a CRITICAL is "and nothing is
being done", which is false for every mirror finding.
"""
from datetime import datetime, timedelta, timezone

import pytest

from core.data_quality import (
    DEFAULT_REMEDIATION,
    IntegrityIssue,
    REMEDIATION,
    Severity,
    format_alert_message,
    machine_attempts_note,
    remediation_for,
)


def _issue(check_name: str, count: int = 1) -> IntegrityIssue:
    return IntegrityIssue(
        check_name=check_name, table_name="t", severity=Severity.CRITICAL,
        count=count, sample_ids=[1], description="d",
    )


class TestRemediationLookup:
    def test_longest_prefix_wins(self):
        """`mirror_never_shipped` has its own answer — "wait for Sunday" —
        which is the opposite of the generic mirror advice."""
        generic = remediation_for(["mirror_missing_rows"])[0]
        specific = remediation_for(["mirror_never_shipped"])[0]
        assert generic != specific
        assert "Sunday" in specific

    def test_generated_check_names_are_matched_by_prefix(self):
        """Half the check names are built at runtime; an exact-name table
        would silently miss every one of them."""
        for name in (
            "fk_orphan_order_products_order_id",
            "pk_uniqueness_orders",
            "not_null_orders_grand_total",
            "value_domain_orders_source_id",
            "freshness_orders",
        ):
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name

    def test_an_unknown_check_gets_the_anchor_not_silence(self):
        assert remediation_for(["something_nobody_wrote_yet"]) == [
            DEFAULT_REMEDIATION
        ]
        assert "CLAUDE.md" in DEFAULT_REMEDIATION

    def test_one_useful_thing_said_once(self):
        """Eight orphan checks in one alert are still one lever."""
        assert len(remediation_for([
            "fk_orphan_a_b", "fk_orphan_c_d", "fk_orphan_e_f",
        ])) == 1

    def test_order_is_stable_and_follows_the_issues(self):
        assert remediation_for(["silver_missing_rows", "gold_cell_values"]) == list(
            reversed(remediation_for(["gold_cell_values", "silver_missing_rows"]))
        )

    def test_the_archive_is_never_told_to_repair_itself(self):
        """The one table whose 'repair' would mean inventing the history it is
        the only record of."""
        line = remediation_for(["order_versions_stalled"])[0]
        assert "never" in line.lower() or "not repairable" in line.lower()

    def test_every_entry_says_something(self):
        for prefix, line in REMEDIATION:
            assert prefix and len(line) > 30, prefix


class TestMachineAttemptsNote:
    def test_quiet_when_nothing_has_healed(self, monkeypatch):
        """The common case. An empty line in a CRITICAL is worse than none."""
        import core.ch_history as ch_history
        import core.pg_buyers as pg_buyers

        monkeypatch.setattr(pg_buyers, "last_heal", {})
        monkeypatch.setattr(ch_history, "last_heal", {})
        assert machine_attempts_note() is None

    def test_counts_a_recent_heal(self, monkeypatch):
        import core.ch_history as ch_history
        import core.pg_buyers as pg_buyers

        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(pg_buyers, "last_heal",
                            {"at": now - timedelta(minutes=41), "shipped": 3})
        monkeypatch.setattr(ch_history, "last_heal", {})
        note = machine_attempts_note(now=now)
        assert "buyers" in note and "3" in note and "41 min" in note

    def test_both_ledgers_are_reported(self, monkeypatch):
        import core.ch_history as ch_history
        import core.pg_buyers as pg_buyers

        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(pg_buyers, "last_heal",
                            {"at": now - timedelta(minutes=5), "shipped": 1})
        monkeypatch.setattr(ch_history, "last_heal",
                            {"at": now - timedelta(hours=2), "shipped": 9})
        note = machine_attempts_note(now=now)
        assert "buyers" in note and "archive" in note

    def test_a_heal_older_than_a_day_is_not_this_incident(self, monkeypatch):
        import core.ch_history as ch_history
        import core.pg_buyers as pg_buyers

        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(pg_buyers, "last_heal",
                            {"at": now - timedelta(days=3), "shipped": 3})
        monkeypatch.setattr(ch_history, "last_heal", {})
        assert machine_attempts_note(now=now) is None


class TestTheAlertBody:
    def test_critical_carries_a_lever(self):
        msg = format_alert_message(
            "mirror_landing", Severity.CRITICAL, [_issue("mirror_missing_rows")], [],
        )
        assert "── What to do ──" in msg
        assert "meta.mirror_state" in msg

    def test_the_lever_is_last_because_it_is_what_gets_acted_on(self):
        msg = format_alert_message(
            "integrity", Severity.CRITICAL, [_issue("pk_uniqueness_orders")], [],
        )
        body, _, tail = msg.partition("── What to do ──")
        assert "pk_uniqueness_orders" in body
        assert tail.strip()

    def test_info_asks_nobody_to_do_anything(self):
        msg = format_alert_message(
            "integrity", Severity.INFO, [_issue("mirror_retired_rows")], [],
        )
        assert "What to do" not in msg

    def test_machine_note_rides_along_when_given(self):
        msg = format_alert_message(
            "mirror_landing", Severity.CRITICAL, [_issue("mirror_missing_rows")], [],
            machine_note="Machine already tried: buyers ids-diff re-shipped 3 row(s) 4 min ago.",
        )
        assert "Machine already tried" in msg

    def test_no_machine_note_leaves_no_blank_line(self):
        msg = format_alert_message(
            "mirror_landing", Severity.CRITICAL, [_issue("mirror_missing_rows")], [],
        )
        assert not msg.endswith("\n")
        assert "\n\n" not in msg

    def test_a_pure_reconciliation_difference_still_gets_a_lever(self):
        """Discrepancies name no check, so the check table cannot answer them —
        and 'no lever written down' would be wrong, because there is one."""
        from core.data_quality import Discrepancy, DiscrepancyClass

        d = Discrepancy(
            month="2026-04", source_id=1, field="orders",
            dk_value=565, kc_value=566, diff_class=DiscrepancyClass.MISSING_IN_DK,
            severity=Severity.CRITICAL,
        )
        msg = format_alert_message("reconciliation", Severity.CRITICAL, [], [d])
        assert "── What to do ──" in msg
        assert "KeyCRM" in msg
