"""The rule that decides what the WAL archive may forget.

Every test here is really one assertion: an unproven case keeps the segment.
Disk is cheap and a missing segment is only discovered at restore time.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from deploy.wal_retention import (  # noqa: E402
    SIDECAR,
    oldest_kept_segment,
    parse_backup_label,
    read_anchors,
)

LABEL = (
    "START WAL LOCATION: 3/91000028 (file 000000010000000300000091)\n"
    "CHECKPOINT LOCATION: 3/91000060\n"
    "BACKUP METHOD: streamed\n"
    "BACKUP FROM: primary\n"
    "START TIME: 2026-08-31 09:00:00 UTC\n"
)


class TestParseBackupLabel:
    def test_reads_the_segment_the_server_named(self):
        assert parse_backup_label(LABEL) == "000000010000000300000091"

    def test_a_label_without_the_line_is_not_a_guess(self):
        assert parse_backup_label("BACKUP METHOD: streamed\n") is None

    def test_empty_and_none_are_refusals_not_crashes(self):
        assert parse_backup_label("") is None
        assert parse_backup_label(None) is None  # type: ignore[arg-type]

    def test_a_truncated_segment_name_is_rejected(self):
        """23 hex is not a segment. Anchoring on a prefix would compare short
        against long and delete more than intended."""
        assert parse_backup_label(
            "START WAL LOCATION: 3/91000028 (file 00000001000000030000009)\n"
        ) is None


class TestOldestKeptSegment:
    def test_the_earliest_requirement_wins(self):
        assert oldest_kept_segment([
            "000000010000000300000091",
            "000000010000000300000075",
            "0000000100000003000000A2",
        ]) == "000000010000000300000075"

    def test_no_base_backup_keeps_everything(self):
        """The 2026-08-31 state: 3.9 GB of WAL and nothing to replay it onto.
        That is when deletion looks free and is not — the first base backup
        makes every later segment valuable."""
        assert oldest_kept_segment([]) is None

    def test_one_unreadable_anchor_refuses_the_whole_run(self):
        """Taking the minimum of the rest would delete exactly what the
        unreadable backup needs — and an unreadable sidecar usually belongs to
        the run that was interrupted."""
        assert oldest_kept_segment(["000000010000000300000091", None]) is None

    def test_a_malformed_anchor_refuses_too(self):
        assert oldest_kept_segment(["000000010000000300000091", "garbage"]) is None

    def test_lexicographic_order_is_wal_order_across_a_timeline_bump(self):
        """A restore bumps the timeline, and the new timeline's segments sort
        after the old one's — which is the same comparison pg_archivecleanup
        makes."""
        assert oldest_kept_segment([
            "000000020000000300000005",
            "0000000100000003000000FF",
        ]) == "0000000100000003000000FF"

    def test_hex_is_compared_as_fixed_width_not_as_a_number(self):
        """0x9 < 0x10 numerically, and '009' < '010' as text too, because the
        field is zero-padded. This is the property the whole scheme rests on."""
        assert oldest_kept_segment([
            "000000010000000300000010",
            "000000010000000300000009",
        ]) == "000000010000000300000009"


class TestReadAnchors:
    def _make_backup(self, root: Path, name: str, segment: str | None,
                     with_tar: bool = True) -> Path:
        d = root / name
        d.mkdir(parents=True)
        if with_tar:
            (d / "base.tar.gz").write_bytes(b"\x1f\x8b")
        if segment is not None:
            (d / SIDECAR).write_text(segment + "\n")
        return d

    def test_reads_one_anchor_per_backup(self, tmp_path):
        self._make_backup(tmp_path, "20260830-090000", "000000010000000300000075")
        self._make_backup(tmp_path, "20260831-090000", "000000010000000300000091")
        assert read_anchors(tmp_path) == [
            "000000010000000300000075",
            "000000010000000300000091",
        ]

    def test_a_backup_without_a_sidecar_reads_as_unproven(self, tmp_path):
        self._make_backup(tmp_path, "20260831-090000", None)
        assert read_anchors(tmp_path) == [None]
        assert oldest_kept_segment(read_anchors(tmp_path)) is None

    def test_a_directory_that_is_not_a_backup_is_skipped_not_counted(self, tmp_path):
        """A stray directory must not freeze retention forever — but it must
        not be mistaken for a backup either."""
        self._make_backup(tmp_path, "20260831-090000", "000000010000000300000091")
        (tmp_path / "lost+found").mkdir()
        assert read_anchors(tmp_path) == ["000000010000000300000091"]

    def test_a_half_written_sidecar_is_unproven(self, tmp_path):
        self._make_backup(tmp_path, "20260831-090000", "00000001000000030000")
        assert read_anchors(tmp_path) == [None]

    def test_a_missing_directory_is_no_backups_at_all(self, tmp_path):
        assert read_anchors(tmp_path / "nope") == []
        assert oldest_kept_segment(read_anchors(tmp_path / "nope")) is None

    def test_end_to_end_the_oldest_backup_anchors_the_archive(self, tmp_path):
        self._make_backup(tmp_path, "20260829-090000", "000000010000000300000060")
        self._make_backup(tmp_path, "20260830-090000", "000000010000000300000075")
        self._make_backup(tmp_path, "20260831-090000", "000000010000000300000091")
        assert oldest_kept_segment(read_anchors(tmp_path)) == "000000010000000300000060"
