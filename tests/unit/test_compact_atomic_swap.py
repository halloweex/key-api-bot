"""Tests for scripts/compact_duckdb.py:atomic_swap.

Validates the file-rename contract that replaces the old "operator runs
mv commands manually" workflow. The May 19-20 2026 outage happened
because the operator's SSH died after Phase 3 validation but before
those mv commands ran. With atomic_swap inside the sidecar, the swap is
guaranteed to complete if validation passed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from compact_duckdb import atomic_swap  # noqa: E402


def _write(path: Path, content: str) -> None:
    """Write a stub file so we can verify which one survived the swap."""
    path.write_text(content)


def _read(path: Path) -> str:
    return path.read_text() if path.exists() else ""


class TestAtomicSwapHappyPath:
    def test_clean_swap_moves_new_into_canonical(self, tmp_path):
        """Most common case: stale .old absent, fresh clean file present."""
        _write(tmp_path / "analytics.duckdb", "old-data")
        _write(tmp_path / "analytics_clean.duckdb", "new-data")

        atomic_swap(tmp_path)

        # canonical now holds new data
        assert _read(tmp_path / "analytics.duckdb") == "new-data"
        # .old preserves the rollback
        assert _read(tmp_path / "analytics.duckdb.old") == "old-data"
        # clean file consumed
        assert not (tmp_path / "analytics_clean.duckdb").exists()

    def test_wal_file_removed(self, tmp_path):
        """WAL belongs to the OLD db; if we kept it, DuckDB would try to
        replay the stale WAL against the new file and corrupt it."""
        _write(tmp_path / "analytics.duckdb", "old")
        _write(tmp_path / "analytics.duckdb.wal", "STALE-WAL-BYTES")
        _write(tmp_path / "analytics_clean.duckdb", "new")

        atomic_swap(tmp_path)

        assert not (tmp_path / "analytics.duckdb.wal").exists()

    def test_no_wal_is_fine(self, tmp_path):
        """Compact ran on a DB that had been checkpointed clean — no WAL."""
        _write(tmp_path / "analytics.duckdb", "old")
        _write(tmp_path / "analytics_clean.duckdb", "new")

        atomic_swap(tmp_path)  # must not raise

        assert _read(tmp_path / "analytics.duckdb") == "new"


class TestAtomicSwapGuardrails:
    def test_refuses_to_clobber_existing_backup(self, tmp_path):
        """The .old file is the only rollback after swap. Overwriting it
        is data loss; refuse and force the operator to clean up first."""
        _write(tmp_path / "analytics.duckdb", "current")
        _write(tmp_path / "analytics.duckdb.old", "PRECIOUS-PRIOR-ROLLBACK")
        _write(tmp_path / "analytics_clean.duckdb", "new")

        with pytest.raises(RuntimeError, match="already exists"):
            atomic_swap(tmp_path)

        # On error, NOTHING was moved
        assert _read(tmp_path / "analytics.duckdb") == "current"
        assert _read(tmp_path / "analytics.duckdb.old") == "PRECIOUS-PRIOR-ROLLBACK"
        assert _read(tmp_path / "analytics_clean.duckdb") == "new"

    def test_refuses_when_clean_missing(self, tmp_path):
        """Phase 2 didn't produce a clean DB — the script should never
        have reached here. If it did, fail loudly rather than nuking the
        current DB to nothing."""
        _write(tmp_path / "analytics.duckdb", "current")

        with pytest.raises(RuntimeError, match="missing"):
            atomic_swap(tmp_path)

        assert _read(tmp_path / "analytics.duckdb") == "current"

    def test_bootstrap_when_no_source_db(self, tmp_path):
        """Edge case: no existing DB at all (fresh install scenario).
        Should still install the clean file as canonical."""
        _write(tmp_path / "analytics_clean.duckdb", "fresh")

        atomic_swap(tmp_path)

        assert _read(tmp_path / "analytics.duckdb") == "fresh"
        # No .old produced because there was no source to back up
        assert not (tmp_path / "analytics.duckdb.old").exists()


class TestIdempotency:
    def test_second_call_after_success_is_blocked_safely(self, tmp_path):
        """If someone re-invokes the swap by mistake, the previous .old
        is the only rollback and must be protected."""
        _write(tmp_path / "analytics.duckdb", "old")
        _write(tmp_path / "analytics_clean.duckdb", "new")
        atomic_swap(tmp_path)

        # Simulate a second compact run that also tried to swap
        _write(tmp_path / "analytics_clean.duckdb", "newer")
        with pytest.raises(RuntimeError, match="already exists"):
            atomic_swap(tmp_path)


class TestRegression:
    def test_may_2026_outage_scenario(self, tmp_path):
        """The May 19-20 2026 incident: compact validation passed, but
        operator SSH died before manual swap. With atomic_swap inside
        the sidecar this cannot happen — the swap completes synchronously
        before the sidecar exits 0.

        This test models the post-validation state and checks that ONE
        call to atomic_swap fully reaches the desired end state."""
        # State after Phase 3 (validation passed)
        _write(tmp_path / "analytics.duckdb", "43GB-bloated-old-db")
        _write(tmp_path / "analytics.duckdb.wal", "stale-wal-from-old")
        _write(tmp_path / "analytics_clean.duckdb", "7GB-fresh-compact")

        atomic_swap(tmp_path)

        # Single call produced the final operational state
        assert _read(tmp_path / "analytics.duckdb") == "7GB-fresh-compact"
        assert _read(tmp_path / "analytics.duckdb.old") == "43GB-bloated-old-db"
        assert not (tmp_path / "analytics.duckdb.wal").exists()
        assert not (tmp_path / "analytics_clean.duckdb").exists()
