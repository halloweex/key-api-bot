"""Tests for SyncConfig mode/bronze-shadow behaviour.

The decision "should we write to bronze on this sync?" is centralised in
SyncConfig.should_write_bronze. Sync service consults this single property,
so testing the property covers the behaviour without instantiating a full
SyncService.
"""
from __future__ import annotations

import pytest

from core.config import SyncConfig


def _make_config(
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str | None = None,
    shadow: str | None = None,
) -> SyncConfig:
    """Construct a fresh SyncConfig with explicit env state."""
    if mode is None:
        monkeypatch.delenv("SYNC_MODE", raising=False)
    else:
        monkeypatch.setenv("SYNC_MODE", mode)
    if shadow is None:
        monkeypatch.delenv("LEGACY_BRONZE_SHADOW", raising=False)
    else:
        monkeypatch.setenv("LEGACY_BRONZE_SHADOW", shadow)
    return SyncConfig()


class TestSyncModeProperties:
    def test_default_mode_is_legacy(self, monkeypatch):
        cfg = _make_config(monkeypatch)
        assert cfg.mode == "legacy"
        assert cfg.is_legacy
        assert not cfg.is_staging

    def test_staging_mode(self, monkeypatch):
        cfg = _make_config(monkeypatch, mode="staging")
        assert cfg.is_staging
        assert not cfg.is_legacy


class TestShouldWriteBronze:
    """The headline robustness fix: bronze write is opt-in in legacy mode."""

    def test_staging_always_writes(self, monkeypatch):
        """Staging mode is the ingestion path — bronze must always be written."""
        cfg = _make_config(monkeypatch, mode="staging")
        assert cfg.should_write_bronze

    def test_staging_writes_even_if_shadow_flag_off(self, monkeypatch):
        """The shadow flag has no effect in staging mode."""
        cfg = _make_config(monkeypatch, mode="staging", shadow="false")
        assert cfg.should_write_bronze

    def test_legacy_default_does_not_write(self, monkeypatch):
        """The regression fence for the 2026-05-18 incident.

        Default-off means no operator action and no env var is required
        to STOP writing bronze in legacy mode. Without this default the
        production DB will grow ~150K rows/day from a never-read audit
        log that prune can't reach (processed_at is permanently NULL)."""
        cfg = _make_config(monkeypatch, mode="legacy")
        assert not cfg.should_write_bronze
        assert not cfg.legacy_bronze_shadow

    def test_legacy_with_shadow_flag_writes(self, monkeypatch):
        """Opt-in escape hatch: someone debugging sync can re-enable
        the shadow log via LEGACY_BRONZE_SHADOW=1 without changing code."""
        cfg = _make_config(monkeypatch, mode="legacy", shadow="1")
        assert cfg.should_write_bronze
        assert cfg.legacy_bronze_shadow

    @pytest.mark.parametrize("truthy", ["1", "true", "yes", "TRUE", "Yes"])
    def test_shadow_flag_accepts_truthy_values(self, monkeypatch, truthy):
        cfg = _make_config(monkeypatch, mode="legacy", shadow=truthy)
        assert cfg.should_write_bronze

    @pytest.mark.parametrize("falsy", ["0", "false", "no", "", "random"])
    def test_shadow_flag_rejects_non_truthy(self, monkeypatch, falsy):
        cfg = _make_config(monkeypatch, mode="legacy", shadow=falsy)
        assert not cfg.should_write_bronze
