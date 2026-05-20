"""Tests for core.data_quality.classify_discrepancies + helpers.

The classifier is pure (no I/O), so it can be tested with synthetic rollups.
These tests are the contract for what counts as a "material" data quality
issue — change the thresholds in core.data_quality MATERIAL_THRESHOLDS and
some of these may need to follow.
"""
from __future__ import annotations

import pytest

from core.data_quality import (
    Discrepancy,
    DiscrepancyClass,
    MATERIAL_THRESHOLDS,
    Severity,
    classify_discrepancies,
    is_material,
    overall_severity,
    summarize_discrepancies,
)


# ─── is_material thresholds ───────────────────────────────────────────────────


class TestIsMaterial:
    def test_orders_diff_of_one_is_material(self):
        """One missed order is the whole point of reconciliation —
        threshold for `orders` is 0, so any non-zero diff fires."""
        assert is_material("orders", 100, 99)

    def test_orders_zero_diff_not_material(self):
        """Equal values are not a discrepancy."""
        assert not is_material("orders", 100, 100)

    def test_revenue_diff_below_100_uah_not_material(self):
        """100 UAH abs floor catches the noise."""
        assert not is_material("revenue", 50_100, 50_000)  # diff=100

    def test_revenue_above_floor_below_ceiling_filtered(self):
        """AND rule: above abs floor but below pct ceiling → filtered.
        ₴200 drift on ₴50K base = 0.4% < 0.5% ceiling → noise."""
        assert not is_material("revenue", 50_200, 50_000)

    def test_revenue_large_abs_but_tiny_pct_filtered(self):
        """The AND rule: even ₴500 absolute drift is noise when KC is huge
        (relative drift << 0.5%). This prevents alert spam on a multi-million-UAH
        base where rounding artefacts accumulate."""
        assert not is_material("revenue", 100_000_500, 100_000_000)  # 0.0005%

    def test_revenue_above_both_floor_and_ceiling_is_material(self):
        """Both thresholds exceeded → real signal."""
        assert is_material("revenue", 101_000, 100_000)  # 1000 abs, 1%

    def test_returns_count_any_nonzero_is_material(self):
        assert is_material("returns_count", 5, 4)
        assert is_material("returns_count", 5, 6)

    def test_returns_count_zero_diff_not_material(self):
        assert not is_material("returns_count", 5, 5)

    def test_qty_diff_of_two_is_material(self):
        """qty threshold is 1 — allows for 1-unit line-item recombination noise."""
        assert is_material("qty", 1000, 998)

    def test_qty_diff_of_one_not_material(self):
        assert not is_material("qty", 1000, 999)

    def test_returns_revenue_diff_below_floor_not_material(self):
        """50 UAH abs floor on returns_revenue."""
        assert not is_material("returns_revenue", 5_040, 5_000)  # diff=40

    def test_returns_revenue_diff_above_floor_is_material(self):
        assert is_material("returns_revenue", 6_000, 5_000)  # diff=1000, 20%

    def test_unknown_field_raises(self):
        with pytest.raises(ValueError, match="unknown field"):
            is_material("nonsense_field", 1, 0)


# ─── classify_discrepancies — empty inputs ────────────────────────────────────


class TestClassifyEmpty:
    def test_empty_rollups_no_discrepancies(self):
        assert classify_discrepancies({}, {}) == []

    def test_identical_rollups_no_discrepancies(self):
        cell = {"orders": 100, "qty": 200, "revenue": 50000,
                "returns_count": 5, "returns_revenue": 1000}
        dk = {("2026-04", 1): cell}
        kc = {("2026-04", 1): cell}
        assert classify_discrepancies(dk, kc) == []


# ─── classify_discrepancies — missing-side classes ────────────────────────────


class TestMissingInDuckDB:
    """KeyCRM has the cell, DuckDB does not. Means we lost orders in sync."""

    def test_kc_only_cell_yields_missing_in_dk(self):
        kc = {("2026-04", 1): {"orders": 10, "revenue": 5000}}
        diffs = classify_discrepancies({}, kc)
        # 'orders' AND 'revenue' both above threshold → 2 discrepancies
        classes = {d.diff_class for d in diffs}
        assert classes == {DiscrepancyClass.MISSING_IN_DK}
        assert all(d.severity == Severity.CRITICAL for d in diffs)
        fields = {d.field for d in diffs}
        assert fields == {"orders", "revenue"}

    def test_missing_in_dk_orders_has_correct_values(self):
        kc = {("2026-04", 1): {"orders": 10}}
        [d] = classify_discrepancies({}, kc)
        assert d.dk_value == 0
        assert d.kc_value == 10
        assert d.diff_abs == -10
        # diff_pct: KC=10, DK=0 → -100%
        assert d.diff_pct == pytest.approx(-100.0)


class TestMissingInKeyCRM:
    """DuckDB has the cell, KeyCRM does not. Ghost orders — investigation."""

    def test_dk_only_cell_yields_missing_in_kc(self):
        dk = {("2026-04", 1): {"orders": 10, "revenue": 5000}}
        diffs = classify_discrepancies(dk, {})
        assert all(d.diff_class == DiscrepancyClass.MISSING_IN_KC for d in diffs)
        assert all(d.severity == Severity.CRITICAL for d in diffs)


# ─── classify_discrepancies — both sides present ──────────────────────────────


class TestBothSidesPresent:
    def test_orders_count_drift_is_critical(self):
        dk = {("2026-04", 1): {"orders": 105, "revenue": 51000}}
        kc = {("2026-04", 1): {"orders": 100, "revenue": 50000}}
        diffs = classify_discrepancies(dk, kc)
        orders_diff = next(d for d in diffs if d.field == "orders")
        assert orders_diff.severity == Severity.CRITICAL
        assert orders_diff.diff_class == DiscrepancyClass.TOTAL_DRIFT
        assert orders_diff.diff_abs == 5

    def test_revenue_drift_only_is_warn(self):
        """Counts match, only revenue drifted (status change without status field
        change). WARN severity."""
        dk = {("2026-04", 1): {"orders": 100, "revenue": 51000}}
        kc = {("2026-04", 1): {"orders": 100, "revenue": 50000}}
        diffs = classify_discrepancies(dk, kc)
        revenue_diff = next(d for d in diffs if d.field == "revenue")
        assert revenue_diff.severity == Severity.WARN
        assert revenue_diff.diff_abs == 1000

    def test_returns_count_drift_is_critical(self):
        """One missed return is operationally important — counts changed."""
        dk = {("2026-04", 1): {"returns_count": 8}}
        kc = {("2026-04", 1): {"returns_count": 10}}
        diffs = classify_discrepancies(dk, kc)
        [d] = diffs
        assert d.severity == Severity.CRITICAL
        assert d.field == "returns_count"

    def test_sub_threshold_noise_filtered_out(self):
        """Small rounding diffs must not pollute the alert stream."""
        dk = {("2026-04", 1): {"orders": 100, "qty": 200, "revenue": 50_000_100}}
        kc = {("2026-04", 1): {"orders": 100, "qty": 200, "revenue": 50_000_000}}
        # revenue diff 100 < 500 abs threshold → suppressed
        assert classify_discrepancies(dk, kc) == []


# ─── classify_discrepancies — ordering + grouping ─────────────────────────────


class TestOutputOrdering:
    def test_critical_first(self):
        # One CRITICAL (orders drift) + one WARN (revenue drift different cell)
        dk = {
            ("2026-04", 1): {"orders": 105, "revenue": 50000},   # orders CRIT
            ("2026-04", 2): {"orders": 100, "revenue": 51000},   # revenue WARN
        }
        kc = {
            ("2026-04", 1): {"orders": 100, "revenue": 50000},
            ("2026-04", 2): {"orders": 100, "revenue": 50000},
        }
        diffs = classify_discrepancies(dk, kc)
        assert diffs[0].severity == Severity.CRITICAL
        assert all(d.severity == Severity.WARN for d in diffs[1:])

    def test_multiple_months_stable_order(self):
        dk = {("2026-02", 1): {"orders": 50}, ("2026-04", 1): {"orders": 100}}
        kc = {("2026-02", 1): {"orders": 45}, ("2026-04", 1): {"orders": 95}}
        diffs = classify_discrepancies(dk, kc)
        # Both CRITICAL — secondary sort by month
        assert diffs[0].month == "2026-02"
        assert diffs[1].month == "2026-04"


# ─── Production-scenario tests (regression markers) ───────────────────────────


class TestProductionScenarios:
    def test_compact_post_sync_window_match(self):
        """Today's actual prod state: 6 months perfect match. Reconciliation
        on this MUST emit zero discrepancies — otherwise the system would
        page admins every day."""
        rollup = {
            ("2026-04", 1): {"orders": 566, "qty": 2115, "revenue": 1665529,
                             "returns_count": 24, "returns_revenue": 56737},
            ("2026-04", 2): {"orders": 233, "qty": 4378, "revenue": 1802085,
                             "returns_count": 5, "returns_revenue": 29878},
        }
        assert classify_discrepancies(rollup, rollup) == []

    def test_missing_single_order_caught(self):
        """One order vanishing from DuckDB MUST be detected. This is the
        core promise of reconciliation."""
        dk = {("2026-04", 1): {"orders": 565, "revenue": 1665529 - 3000}}
        kc = {("2026-04", 1): {"orders": 566, "revenue": 1665529}}
        diffs = classify_discrepancies(dk, kc)
        assert any(d.field == "orders" and d.severity == Severity.CRITICAL
                   for d in diffs)

    def test_2024_11_zero_revenue_anomaly_not_flagged_falsely(self):
        """The 2024-11 Shopify row in real data: 1 order, ₴0, qty=50. If
        KC says the same, no discrepancy."""
        cell = {"orders": 1, "qty": 50, "revenue": 0,
                "returns_count": 0, "returns_revenue": 0}
        assert classify_discrepancies({("2024-11", 4): cell}, {("2024-11", 4): cell}) == []


# ─── overall_severity ─────────────────────────────────────────────────────────


class TestOverallSeverity:
    def test_empty_returns_info(self):
        assert overall_severity([], []) == Severity.INFO

    def test_one_critical_dominates(self):
        d = Discrepancy(
            month="2026-04", source_id=1,
            diff_class=DiscrepancyClass.TOTAL_DRIFT,
            field="orders", dk_value=100, kc_value=95,
            severity=Severity.CRITICAL,
        )
        assert overall_severity([], [d]) == Severity.CRITICAL

    def test_warn_overrides_info(self):
        d = Discrepancy(
            month="2026-04", source_id=1,
            diff_class=DiscrepancyClass.TOTAL_DRIFT,
            field="revenue", dk_value=51000, kc_value=50000,
            severity=Severity.WARN,
        )
        assert overall_severity([], [d]) == Severity.WARN


class TestSummarize:
    def test_stable_shape(self):
        """Summary always contains all classes, even when zero — schema
        stability for downstream consumers (Telegram template, dashboard)."""
        s = summarize_discrepancies([])
        assert set(s.keys()) == {c.value for c in DiscrepancyClass}
        assert all(v == 0 for v in s.values())

    def test_counts_by_class(self):
        d1 = Discrepancy(month="m", source_id=1,
                         diff_class=DiscrepancyClass.MISSING_IN_DK,
                         field="orders", dk_value=0, kc_value=5)
        d2 = Discrepancy(month="m", source_id=1,
                         diff_class=DiscrepancyClass.MISSING_IN_DK,
                         field="revenue", dk_value=0, kc_value=2000)
        d3 = Discrepancy(month="m", source_id=2,
                         diff_class=DiscrepancyClass.TOTAL_DRIFT,
                         field="orders", dk_value=10, kc_value=8)
        s = summarize_discrepancies([d1, d2, d3])
        assert s["MISSING_IN_DK"] == 2
        assert s["TOTAL_DRIFT"] == 1
        assert s["MISSING_IN_KC"] == 0


class TestMaterialThresholdsCoverage:
    def test_all_default_fields_covered(self):
        """If a new metric is added to the system, this fence forces
        explicit thresholds rather than silent inheritance."""
        expected = {"orders", "qty", "revenue", "returns_count", "returns_revenue"}
        assert set(MATERIAL_THRESHOLDS.keys()) == expected
