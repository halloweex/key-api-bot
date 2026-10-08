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
    DERIVED,
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
        # Live features nobody has used yet. user_preferences and
        # celebrated_milestones were here too until 2026-08-20, when the DuckDB
        # duplicates were dropped — their rows live in the bot's SQLite and
        # always did, so a snapshot of the analytics file should not carry them
        # at all, empty or otherwise.
        "revenue_goals": 0,
        "manual_expenses": 0,
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

    def test_fewer_buyer_contacts_is_a_buyer_who_dropped_a_number(self):
        """Both writers replace a buyer's contacts wholesale, and chain 4's
        copy-back writes back whatever Postgres holds — a few fewer contacts
        is not a lost row, and rejecting it would reject every night after."""
        v = validate_snapshot(
            _healthy(buyer_contacts=32_700),
            previous_counts=_healthy(),
        )
        assert v.ok, v.errors

    @pytest.mark.parametrize("contacts", [0, 10, 32_000])
    def test_but_a_contacts_table_that_fell_a_lot_is_rejected(self, contacts):
        """What leaving MONOTONE must not cost: an export down to nothing, to
        ten, or by 2% still fails — the review of that change showed the first
        two shipping as the new baseline."""
        v = validate_snapshot(
            _healthy(buyer_contacts=contacts),
            previous_counts=_healthy(),
        )
        assert not v.ok
        assert any("buyer_contacts" in e for e in v.errors), v.errors

    def test_buyers_still_may_not_be_empty(self):
        v = validate_snapshot(_healthy(buyers=0), previous_counts=_healthy())
        assert not v.ok
        assert any("buyers" in e for e in v.errors)

    # ── The contacts' two tiers, pinned by value (F8, review of #265) ──
    #
    # `TestMustBeNonEmpty` parametrises over the set itself, so it shrinks
    # with the set: removing `buyer_contacts` from it passed with one test
    # fewer. And the falls above bracket the bound only between 0.13% and
    # 2.3%, so 2% — or `1 - 2*bound` — passed too. The rule is CLAUDE.md's,
    # "an export that falls by more than 1% (~330 rows at 32 700), or to
    # zero, is rejected", and these say it in numbers.

    def test_the_contacts_tiers_are_exactly_these(self):
        """Mutations killed: `buyer_contacts` dropped from MUST_BE_NONEMPTY,
        or its bound moved off 1%."""
        from core.snapshot_validation import BOUNDED_SHRINK

        assert "buyer_contacts" in MUST_BE_NONEMPTY
        assert BOUNDED_SHRINK == {"buyer_contacts": 0.01}

    def test_no_contacts_and_no_baseline_is_rejected(self):
        """The baseline moves only on an accepted snapshot, so a first run, or
        a `.last_snapshot.json` deleted to resume after a rejection, has none —
        and then only MUST_BE_NONEMPTY stands between an export of zero
        contacts and the copy that survives. Mutation killed: the entry
        removed (the review saw ok=True, with a warning)."""
        v = validate_snapshot(_healthy(buyer_contacts=0), previous_counts=None)
        assert not v.ok
        assert any("buyer_contacts" in e for e in v.errors), v.errors

    @pytest.mark.parametrize("now, ok", [
        (32_416, True),     # 32 743 × 0.99 = 32 415.57: inside the 1%
        (32_415, False),    # and the first row past it
    ])
    def test_the_bound_is_one_percent_to_the_row(self, now, ok):
        """Mutations killed: a bound of 2%, `1 - 2*bound`, or any bound a row
        either side of 1% of 32 743."""
        v = validate_snapshot(_healthy(buyer_contacts=now), previous_counts=_healthy())
        assert v.ok is ok, v.errors
        if not ok:
            assert any("buyer_contacts" in e and "1%" in e for e in v.errors), v.errors

    @pytest.mark.parametrize("now, ok", [
        (32_373, True),     # 32 700 × 0.99 = 32 373 exactly: a fall OF 1% ships
        (32_372, False),    # one row more than 1%
    ])
    def test_a_fall_of_exactly_one_percent_ships(self, now, ok):
        """The review of F8. The count above cannot land on exactly 1%
        (32 415.57), so the boundary's direction was free: `<=` rejected a
        fall of exactly 1% and all 35 tests passed. CLAUDE.md's rule is
        "falls by MORE than 1%", and at 32 700 — the count it names — 1% is
        a whole row, 32 373.0 exactly in floating point too. Mutation
        killed: `now <= before * (1 - bound)`."""
        assert 32_700 * (1 - 0.01) == 32_373          # the boundary is exact
        before = _healthy(buyer_contacts=32_700)
        v = validate_snapshot(_healthy(buyer_contacts=now), previous_counts=before)
        assert v.ok is ok, v.errors


class TestEmptyMustBeDeclared:
    def test_the_historical_case_passes_but_is_named(self):
        """The runbook tables were empty for the whole period the nightly backup
        reported success. They are allowed to be empty. They are not allowed to
        be empty silently.

        Two of the original four — user_preferences and celebrated_milestones —
        are no longer in this database at all. Their emptiness here was not a
        quiet feature waiting to be used; it was a duplicate of a table the bot
        owns, and naming it as protected data is how the bot's own database
        stayed out of every backup.
        """
        v = validate_snapshot(_healthy(), previous_counts=_healthy())
        assert v.ok
        for table in ("revenue_goals", "manual_expenses"):
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


class TestDerivedLayers:
    """The manifest counts every table, but the derived layers are deliberately
    not exported — the app rebuilds them from bronze in seconds, which is most
    of why the snapshot is small enough to ship nightly. An empty one is the
    design working."""

    def test_an_empty_derived_table_is_neither_error_nor_warning(self):
        v = validate_snapshot(
            _healthy(gold_product_pairs=0, silver_orders=0),
            previous_counts=_healthy(),
        )
        assert v.ok
        assert not v.warnings

    def test_derived_tables_are_not_listed_as_declared_empty(self):
        """They are not part of the snapshot, so saying they are empty in it
        would be answering a question nobody asked."""
        v = validate_snapshot(
            _healthy(gold_daily_revenue=0), previous_counts=_healthy(),
        )
        assert "gold_daily_revenue" not in v.empty_tables
