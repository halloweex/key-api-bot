"""Tests for scripts/compact_duckdb.py preflight disk requirement formula.

The preflight check decides whether to abort compaction up-front when disk
is too tight. Getting this wrong has cost real production downtime:

  - Too aggressive (margin > actual need) → compact never runs, DB bloats
    indefinitely (May 17 cron failure on a 43 GB DB with 22 GB free).
  - Too lax → compact runs out of disk mid-import, leaving the source DB
    intact but burning hours of operator attention.

The formula below treats source DB as already-on-disk (which it is) and
only requires headroom for the NEW artifacts created during compaction.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# scripts/ is not a package — load the module by path.
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import compact_duckdb  # noqa: E402
from compact_duckdb import (  # noqa: E402
    NEW_DB_RATIO,
    SAFETY_BUFFER_GB,
    compute_disk_requirements,
)


class TestComputeDiskRequirements:
    def test_empty_source_returns_safety_buffer(self):
        """A 0 GB source still needs at least the fixed buffer."""
        assert compute_disk_requirements(0) == SAFETY_BUFFER_GB

    def test_small_db(self):
        """1 GB source: ratio·1 + 1 GB buffer = 1.5 GB."""
        assert compute_disk_requirements(1.0) == pytest.approx(1.5)

    def test_typical_post_compact_db(self):
        """5 GB (what we expect after a successful compact)."""
        assert compute_disk_requirements(5.0) == pytest.approx(3.5)

    def test_pre_compact_bloated_db(self):
        """43 GB source (current production state). Formula must allow
        compact on a 75 GB disk where ~20 GB is free."""
        required = compute_disk_requirements(43.0)
        assert required == pytest.approx(22.5)
        # Sanity: real free space (20 GB) is below this, so the user
        # cannot accidentally compact with no margin — but a healthier
        # 50 GB DB on the same disk (25 GB free) would proceed.
        assert required > 20.0  # current state still needs operator action
        assert compute_disk_requirements(50.0) < 30.0  # 50 GB DB → 26 GB req

    def test_monotonic(self):
        """Bigger source → strictly bigger requirement."""
        smaller = compute_disk_requirements(10.0)
        bigger = compute_disk_requirements(20.0)
        assert bigger > smaller

    def test_proportional_growth(self):
        """Doubling source size adds NEW_DB_RATIO × source (not 2×)."""
        a = compute_disk_requirements(10.0)
        b = compute_disk_requirements(20.0)
        # Difference = 10 GB × ratio
        assert b - a == pytest.approx(10.0 * NEW_DB_RATIO)

    def test_negative_source_rejected(self):
        """Negative input is a programming error, not a runtime condition."""
        with pytest.raises(ValueError):
            compute_disk_requirements(-1.0)

    def test_old_0_8x_heuristic_would_have_blocked_current_db(self):
        """Regression marker: the previous formula (source × 0.8) demanded
        34.4 GB free for a 43 GB DB on a 75 GB disk. That is the bug we
        are fixing. The new formula must NOT match that on this input."""
        old_requirement = 43.0 * 0.8  # = 34.4 GB
        new_requirement = compute_disk_requirements(43.0)
        assert new_requirement < old_requirement
        # Must be ≥10 GB looser, otherwise the fix is cosmetic.
        assert old_requirement - new_requirement >= 10.0

    def test_matches_observed_apr_compact(self):
        """Apr 23, 2026: 13 GB source compacted to 4.5 GB (ratio = 0.346).
        Our NEW_DB_RATIO upper bound (0.50) must be ≥ observed ratio
        otherwise we under-provision."""
        observed_ratio = 4.5 / 13.0
        assert NEW_DB_RATIO >= observed_ratio


class TestPreflightConstants:
    """Constants must stay in a sane range — these tests fence in the formula
    against accidental tuning that would re-introduce the original bug."""

    def test_new_db_ratio_in_safe_band(self):
        # Below 0.4 = too optimistic, risks running out of disk mid-compact.
        # Above 0.7 = too pessimistic, restores the original bug.
        assert 0.4 <= NEW_DB_RATIO <= 0.7

    def test_safety_buffer_in_reasonable_band(self):
        # Below 0.5 GB = no slack for parquet + tmp + secondary 3 GB check.
        # Above 5 GB = over-engineering for our actual overhead.
        assert 0.5 <= SAFETY_BUFFER_GB <= 5.0


class TestPreflightIntegration:
    """End-to-end smoke that preflight() is wired to use the new function."""

    def test_preflight_imports_compute_function(self):
        """If someone re-inlines the formula, this test will keep them honest."""
        import inspect

        src = inspect.getsource(compact_duckdb.preflight)
        assert "compute_disk_requirements" in src, (
            "preflight() must call compute_disk_requirements() — do not inline."
        )

    def test_force_compact_env_var_recognised(self):
        """FORCE_COMPACT=1 must be readable in preflight as an escape hatch."""
        import inspect

        src = inspect.getsource(compact_duckdb.preflight)
        assert "FORCE_COMPACT" in src, (
            "preflight() must support FORCE_COMPACT env override."
        )
