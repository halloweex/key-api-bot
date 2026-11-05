"""Tests for Layer-1 integrity checks.

Since step 14's S14-14 the scan judges only facts the integrity job pre-read
from Postgres (the snapshot calendar, the sync watermarks), so these hand it
facts. Until then they took a real DuckDB connection and planted violations
for the PK, orphan, NULL and domain checks, deleted with the DuckDB scan; their
Postgres twins are `core/pg_warehouse_dq.py`'s.
"""
from __future__ import annotations


from core.data_quality import (
    Severity,
    check_internal_integrity,
    summarize_issues,
)


# ─── Aggregate check_internal_integrity ───────────────────────────────────────


class TestCheckInternalIntegrity:
    def test_clean_facts_zero_issues(self):
        # Chain 1's calendar, pre-read as the job reads it from Postgres:
        # none yet. Without it the continuity check says it is not watching
        # (`inventory_continuity_unwatched`). No sync history anywhere is a
        # fresh install, not a stall.
        issues = check_internal_integrity(
            inventory_calendar=(None, frozenset()), chain_watermarks={},
            chain_stamps={}, frozen_sync_rows={})
        assert issues == []

    def test_it_takes_no_connection(self):
        """Mutation: give the scan a DuckDB connection back."""
        import inspect

        assert "conn" not in inspect.signature(check_internal_integrity).parameters


# ─── summarize_issues ─────────────────────────────────────────────────────────


class TestSummarizeIssues:
    def test_stable_shape(self):
        s = summarize_issues([])
        assert set(s.keys()) == {sv.value for sv in Severity}
        assert all(v == 0 for v in s.values())


# ─── Inventory snapshot continuity ────────────────────────────────────────────


from datetime import date, datetime, timedelta  # noqa: E402

from core.data_quality import (  # noqa: E402
    INVENTORY_CONTINUITY_WINDOW_DAYS,
    _inventory_snapshot_continuity_check,
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
    return _inventory_snapshot_continuity_check(now=now, calendar=_calendar(now, days))


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

    def test_wired_into_the_full_integrity_run(self):
        now = datetime.now().astimezone()
        names = {i.check_name for i in check_internal_integrity(
            inventory_calendar=_calendar(now, (2, 3, 4)), chain_watermarks={},
            chain_stamps={}, frozen_sync_rows={})}
        assert "inventory_snapshot_gaps" in names

    def test_without_a_calendar_it_says_it_is_not_watching(self):
        """A caller that forgot is loud, not silent and not wrong.

        This is the one table here that cannot be reconstructed at any price,
        so 'I am not watching it' has to reach a human. It must not report
        gaps either. Since step 14's S14-11 this holds whatever
        `KS_WRITE_INVENTORY` says: the check reads no DuckDB table at all.
        """
        issues = _inventory_snapshot_continuity_check(now=datetime.now().astimezone())
        assert [i.check_name for i in issues] == ["inventory_continuity_unwatched"]
