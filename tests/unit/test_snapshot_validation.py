"""Tests for the snapshot validator.

The case this exists for is on record. `backup_database` validated with
`SELECT COUNT(*) FROM orders > 0`, reported success every night, and four of
the six tables the runbook named as the reason for backing up at all were
empty the whole time. The check never looked, so the check never said.

Counts below are production shapes as of 2026-08-12.
"""
from __future__ import annotations

import pytest

from core.snapshot_validation import (
    MAY_BE_EMPTY,
    MUST_BE_NONEMPTY,
    validate_snapshot,
)


def _healthy(**overrides) -> dict:
    counts = {
        "orders": 46_023,
        "order_products": 145_323,
        "products": 986,
        "buyers": 19_749,
        "expense_types": 5,
        "sync_metadata": 9,
        "users": 24,
        "role_permissions": 21,
        "expenses": 14_534,
        "stock_movements": 45_626,
        "inventory_sku_history": 130_092,
        "buyer_contacts": 32_743,
        "sms_campaign_members": 6_196,
        "reconciliation_log": 1_711,
        "data_quality_runs": 78,
        "warehouse_refreshes": 66_332,
        # Live features nobody has used yet.
        "revenue_goals": 0,
        "manual_expenses": 0,
        "user_preferences": 0,
        "celebrated_milestones": 0,
        "marketing_optouts": 0,
    }
    counts.update(overrides)
    return counts


class TestAcceptance:
    def test_a_healthy_snapshot_ships(self):
        v = validate_snapshot(_healthy(), previous_counts=_healthy())
        assert v.ok
        assert not v.errors

    def test_the_first_ever_snapshot_ships(self):
        """Nothing to compare against is not the same as a failed comparison."""
        v = validate_snapshot(_healthy(), previous_counts=None)
        assert v.ok

    def test_an_empty_manifest_is_rejected(self):
        v = validate_snapshot({})
        assert not v.ok
        assert "no table counts" in v.errors[0]


class TestMustBeNonEmpty:
    @pytest.mark.parametrize("table", sorted(MUST_BE_NONEMPTY))
    def test_zero_rows_is_rejected(self, table):
        v = validate_snapshot(_healthy(**{table: 0}))
        assert not v.ok
        assert any(table in e for e in v.errors)

    def test_a_table_missing_entirely_is_rejected(self):
        counts = _healthy()
        del counts["orders"]
        v = validate_snapshot(counts)
        assert not v.ok
        assert any("absent from the manifest" in e for e in v.errors)


class TestMonotone:
    def test_a_ledger_that_shrank_is_rejected(self):
        """Append-only means append-only. A fall is loss, wherever it happened."""
        v = validate_snapshot(
            _healthy(stock_movements=45_000),
            previous_counts=_healthy(),
        )
        assert not v.ok
        assert any("stock_movements" in e and "only ever appended" in e for e in v.errors)

    def test_growth_is_fine(self):
        v = validate_snapshot(
            _healthy(orders=46_100),
            previous_counts=_healthy(),
        )
        assert v.ok

    def test_without_a_previous_count_no_judgement_is_made(self):
        v = validate_snapshot(_healthy(stock_movements=1), previous_counts=None)
        assert v.ok

    def test_a_table_new_since_the_last_snapshot_is_not_a_fall(self):
        previous = _healthy()
        del previous["sms_campaign_members"]
        v = validate_snapshot(_healthy(), previous_counts=previous)
        assert v.ok


class TestEmptyMustBeDeclared:
    def test_the_historical_case_passes_but_is_named(self):
        """The four runbook tables were empty for the whole period the nightly
        backup reported success. They are allowed to be empty. They are not
        allowed to be empty silently."""
        v = validate_snapshot(_healthy(), previous_counts=_healthy())
        assert v.ok
        for table in ("revenue_goals", "manual_expenses",
                      "user_preferences", "celebrated_milestones"):
            assert table in v.empty_tables

    def test_every_declared_empty_table_is_classified(self):
        v = validate_snapshot(_healthy(), previous_counts=_healthy())
        for table in v.empty_tables:
            assert table in MAY_BE_EMPTY, f"{table} is empty but unclassified"
        assert not v.warnings

    def test_an_unclassified_empty_table_warns_rather_than_guesses(self):
        """A table nobody has put in a tier is exactly what the old validator
        walked past. Report it; do not pick a side on its behalf."""
        v = validate_snapshot(
            _healthy(seasonal_indices=0), previous_counts=_healthy(),
        )
        assert v.ok
        assert any("seasonal_indices" in w and "not classified" in w for w in v.warnings)


class TestChecksums:
    def _sums(self, revenue=135_522_559.87, last_order="2026-08-12"):
        return {
            "total_revenue": revenue,
            "orders_date_range": ["2023-12-02", last_order],
        }

    def test_revenue_going_backwards_is_rejected(self):
        v = validate_snapshot(
            _healthy(), previous_counts=_healthy(),
            checksums=self._sums(revenue=100_000_000),
            previous_checksums=self._sums(),
        )
        assert not v.ok
        assert any("total_revenue fell" in e for e in v.errors)

    def test_returns_move_it_a_little_and_that_is_allowed(self):
        """Returns genuinely reduce revenue. A 0.5% move is business, not loss."""
        v = validate_snapshot(
            _healthy(), previous_counts=_healthy(),
            checksums=self._sums(revenue=135_522_559.87 * 0.995),
            previous_checksums=self._sums(),
        )
        assert v.ok

    def test_the_newest_order_going_backwards_is_rejected(self):
        v = validate_snapshot(
            _healthy(), previous_counts=_healthy(),
            checksums=self._sums(last_order="2026-08-01"),
            previous_checksums=self._sums(last_order="2026-08-12"),
        )
        assert not v.ok
        assert any("newest order went backwards" in e for e in v.errors)

    def test_missing_checksums_are_not_an_error(self):
        v = validate_snapshot(_healthy(), previous_counts=_healthy(), checksums=None)
        assert v.ok
