"""Chain 3's backup evidence, read from the markers the deploy scripts leave.

Each test names the mutation it exists to fail on.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from core import backup_evidence as ev

NOW = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)


def _write(root, name, at, **labels):
    text = at.strftime("%Y-%m-%dT%H:%M:%SZ") + "".join(
        f" {k}={v}" for k, v in labels.items())
    (root / name).write_text(text + "\n")


def _all_fresh(root):
    _write(root, ev.PITR_DRILL, NOW - timedelta(days=2), base="20260928T081000Z")
    _write(root, ev.REMOTE_DRILL, NOW - timedelta(days=2), provider="hetzner")
    _write(root, ev.OFFSITE_PREFIX, NOW - timedelta(hours=2), provider="hetzner")
    _write(root, ev.OFFSITE_PREFIX + ".second", NOW - timedelta(hours=3),
           provider="rsyncnet")


def _keys(unmet):
    return sorted(line.split(":", 1)[0] for line in unmet)


def test_everything_fresh_is_met(tmp_path):
    _all_fresh(tmp_path)
    assert ev.unmet(NOW, tmp_path) == []


def test_nothing_on_disk_is_every_piece_missing(tmp_path):
    """Mutation: treat an absent marker as met."""
    assert _keys(ev.unmet(NOW, tmp_path)) == [
        "pg_offsite", "pitr_drill", "remote_restore", "second_provider"]


@pytest.mark.parametrize("name,days,key", [
    (ev.PITR_DRILL, 9, "pitr_drill"),
    (ev.REMOTE_DRILL, 9, "remote_restore"),
])
def test_a_drill_older_than_a_week_and_a_day_is_unmet(tmp_path, name, days, key):
    """Mutation: compare against hours where the limit is days, or drop the
    age check — a drill that passed once would vouch for ever."""
    _all_fresh(tmp_path)
    _write(tmp_path, name, NOW - timedelta(days=days), provider="hetzner")
    assert _keys(ev.unmet(NOW, tmp_path)) == [key]
    _write(tmp_path, name, NOW - timedelta(days=7, hours=23), provider="hetzner")
    assert ev.unmet(NOW, tmp_path) == []


def test_a_stale_primary_shipment_is_unmet_at_offsite_checks_limit(tmp_path):
    _all_fresh(tmp_path)
    _write(tmp_path, ev.OFFSITE_PREFIX, NOW - timedelta(hours=37), provider="hetzner")
    # Stale primary: its own key, and it no longer counts as a provider either.
    assert _keys(ev.unmet(NOW, tmp_path)) == ["pg_offsite", "second_provider"]


def test_two_copies_at_one_provider_are_one_provider(tmp_path):
    """OD-01 (c) is about suppliers, not files. Mutation: count markers."""
    _all_fresh(tmp_path)
    _write(tmp_path, ev.OFFSITE_PREFIX + ".second", NOW - timedelta(hours=3),
           provider="hetzner")
    assert _keys(ev.unmet(NOW, tmp_path)) == ["second_provider"]


def test_an_unlabelled_copy_vouches_for_nobody_s_second(tmp_path):
    """Mutation: let `unlabelled` count as a provider."""
    _all_fresh(tmp_path)
    _write(tmp_path, ev.OFFSITE_PREFIX + ".second", NOW - timedelta(hours=3))
    assert _keys(ev.unmet(NOW, tmp_path)) == ["second_provider"]


def test_a_half_written_temporary_file_is_not_a_shipment(tmp_path):
    _all_fresh(tmp_path)
    (tmp_path / (ev.OFFSITE_PREFIX + ".second")).unlink()
    _write(tmp_path, ev.OFFSITE_PREFIX + ".tmp", NOW, provider="rsyncnet")
    assert _keys(ev.unmet(NOW, tmp_path)) == ["second_provider"]


def test_an_unreadable_line_falls_back_to_the_file_s_own_time(tmp_path):
    """The file exists only because the script got that far."""
    path = tmp_path / ev.PITR_DRILL
    path.write_text("garbage\n")
    old = (NOW - timedelta(days=20)).timestamp()
    os.utime(path, (old, old))
    marker = ev.read_markers(tmp_path)[0]
    assert marker is not None and marker.age_h(NOW) == pytest.approx(480)


def test_a_missing_directory_is_evidence_missing_not_a_raise(tmp_path):
    assert len(ev.unmet(NOW, tmp_path / "nope")) == 4


def test_the_published_block_carries_ages_and_counts_only(tmp_path):
    """Public endpoint: no path, host or label leaves this module."""
    _all_fresh(tmp_path)
    out = ev.published(NOW, tmp_path)
    assert out == {"pitr_drill_age_h": 48.0, "remote_restore_age_h": 48.0,
                   "pg_offsite_age_h": 2.0, "pg_offsite_copies": 2,
                   "fresh_providers": 2}
    assert not any(isinstance(v, str) for v in out.values())
