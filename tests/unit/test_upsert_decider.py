"""Tests for should_update_order — the pure decider behind the
upsert_orders write-amplification fix.

This is the contract for which writes the sync loop will issue. Getting
it wrong has two failure modes:
- Too permissive  → 1440× amplification persists, DB bloats.
- Too restrictive → silent data drift; status changes never land.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.upsert_decider import should_update_order


UTC = timezone.utc


def _ts(year=2026, month=4, day=1, hour=10, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


class TestForceFlag:
    """force=True bypasses every other check — used by status_refresh."""

    def test_force_always_returns_true_even_if_stale(self):
        existing = _ts(hour=12)
        incoming = _ts(hour=10)  # older
        assert should_update_order(existing, incoming, force=True) is True

    def test_force_true_with_equal_timestamps(self):
        ts = _ts()
        assert should_update_order(ts, ts, force=True) is True

    def test_force_true_with_nones(self):
        assert should_update_order(None, None, force=True) is True


class TestNullHandling:
    """Defensive: missing timestamps cannot silently cause SKIPs."""

    def test_existing_none_means_update(self):
        """Legacy row pre-migration — must write to populate updated_at."""
        assert should_update_order(None, _ts()) is True

    def test_incoming_none_means_update(self):
        """KeyCRM payload missing updated_at — be safe, write."""
        assert should_update_order(_ts(), None) is True

    def test_both_none_means_update(self):
        assert should_update_order(None, None) is True


class TestNormalComparison:
    """Strict > because == means identity write (the whole point)."""

    def test_incoming_strictly_newer_updates(self):
        existing = _ts(hour=10)
        incoming = _ts(hour=11)
        assert should_update_order(existing, incoming) is True

    def test_equal_timestamps_skip(self):
        """The amplification-reduction case: KeyCRM hasn't changed the row."""
        ts = _ts()
        assert should_update_order(ts, ts) is False

    def test_incoming_older_skips(self):
        """Defends against stale paginated batches re-arriving."""
        existing = _ts(hour=12)
        incoming = _ts(hour=10)
        assert should_update_order(existing, incoming) is False

    def test_microsecond_difference_updates(self):
        """Make sure the comparison is precise — no truncation."""
        existing = datetime(2026, 4, 1, 10, 0, 0, 100_000, tzinfo=UTC)
        incoming = datetime(2026, 4, 1, 10, 0, 0, 100_001, tzinfo=UTC)
        assert should_update_order(existing, incoming) is True

    def test_microsecond_equal_skips(self):
        existing = datetime(2026, 4, 1, 10, 0, 0, 100_000, tzinfo=UTC)
        incoming = datetime(2026, 4, 1, 10, 0, 0, 100_000, tzinfo=UTC)
        assert should_update_order(existing, incoming) is False


class TestProductionScenarios:
    """The cases I expect to hit on prod, exercised by name."""

    def test_idle_minute_sync_skips_278_orders(self):
        """Run-of-the-mill incremental_sync tick: no manager changes for
        the past minute, all 278 orders in window have same updated_at as
        last cycle. All 278 must SKIP."""
        existing = _ts(hour=10)
        incoming = _ts(hour=10)  # same value as last sync
        for _ in range(278):
            assert should_update_order(existing, incoming) is False

    def test_manager_edited_one_order_only_that_one_updates(self):
        """Manager edits one order out of 278. That row's updated_at
        advances by a few seconds. Others stay put."""
        last_sync = _ts(hour=10)
        edited_at = _ts(hour=10, minute=15)
        # 277 orders unchanged
        unchanged = should_update_order(last_sync, last_sync)
        # 1 order edited
        changed = should_update_order(last_sync, edited_at)
        assert unchanged is False
        assert changed is True

    def test_status_refresh_with_keycrm_quirk(self):
        """KeyCRM doesn't bump updated_at on pure status changes.
        Without force=True, the status drift would never propagate."""
        existing = _ts(hour=10)
        # Status changed but KeyCRM kept updated_at identical:
        incoming_same_ts = _ts(hour=10)

        # Without force: would skip (and miss the status change)
        assert should_update_order(existing, incoming_same_ts) is False
        # With force: writes
        assert should_update_order(existing, incoming_same_ts, force=True) is True

    def test_backdated_order_appears_in_window_first_time(self):
        """A new (to us) order appears in the 24-hour sync window with
        updated_at older than now. Since we don't have a stored row,
        the caller does INSERT, not UPDATE — this function isn't even
        consulted. But if it were, existing=None → True (safe)."""
        existing = None
        incoming = _ts(hour=10)
        assert should_update_order(existing, incoming) is True
