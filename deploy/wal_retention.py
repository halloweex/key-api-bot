#!/usr/bin/env python3
"""Decide which WAL the archive is still required to hold.

`pg_receivewal` streams into `backups/pg_wal` and nothing has ever deleted
from it: 910 segments and 3.9 GB by 2026-08-31, growing ~450 MB/day. The
comment that licensed that (`deploy/pg_backup.sh`) pointed at the server's
`max_slot_wal_keep_size`, which bounds the server's *own* pg_wal under the
replication slot and says nothing about the directory the receiver writes.

The reason it was never pruned is sound, though, and survives here: a WAL
archive is only a recovery point in company with a base backup, and deleting
a segment some base backup still needs ends PITR silently — the loss shows up
at restore time, which is the worst possible moment to discover it.

So the anchor is not an age and not a count. **It is the START WAL LOCATION of
the oldest base backup still retained**, read out of that backup's own
`backup_label`. Everything strictly older than that segment can be proven
unnecessary; nothing else can.

This module only *decides*. The deletion is `pg_archivecleanup`, which ships
with Postgres, understands timelines, and leaves `.partial` and `.history`
alone — all things a hand-rolled `find -delete` gets wrong on the day it
matters. Keeping the decision here and the deletion there is deliberate: the
part that needs proving is testable, and the part that needs to be exactly
right is somebody else's well-tested C.

Refusing is always safe and is therefore the default. No base backups, an
unreadable label, a malformed segment name: every one of them returns "clean
nothing". An archive that grew too large costs disk; an archive missing one
segment costs the recovery point.

Usage:  wal_retention.py <base-backup-dir>
        prints the oldest segment to keep, or exits 1 having printed why.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional, Sequence

# A WAL segment name: 8 hex of timeline, 8 of log, 8 of segment. Anchored,
# because a label naming anything else is a label we have not understood, and
# guessing from a partial match is how the wrong anchor gets chosen.
SEGMENT_RE = re.compile(r"^[0-9A-F]{24}$")

# `backup_label` line, written by the server:
#     START WAL LOCATION: 3/91000028 (file 000000010000000300000091)
_START_WAL_RE = re.compile(
    r"^START WAL LOCATION:\s+\S+\s+\(file\s+([0-9A-F]{24})\)\s*$",
    re.MULTILINE,
)

# The sidecar this repo writes next to each base backup, holding just the
# segment name. The label itself lives inside base.tar.gz; re-opening a
# gzipped tar on every retention run to re-read one line would make the
# cheap, frequent operation depend on the expensive, rare one.
SIDECAR = "START_WAL"


def parse_backup_label(text: str) -> Optional[str]:
    """Return the segment named by START WAL LOCATION, or None.

    None means "this label did not tell us", which the caller must treat as a
    refusal rather than as an absence of constraint.
    """
    match = _START_WAL_RE.search(text or "")
    return match.group(1) if match else None


def oldest_kept_segment(anchors: Sequence[Optional[str]]) -> Optional[str]:
    """The earliest segment any retained base backup still needs.

    Args:
        anchors: one START WAL segment per retained base backup. A None entry
            is a base backup whose requirement could not be read.

    Returns:
        The smallest segment name, or None when nothing may be deleted.

    Lexicographic order is WAL order: the name is timeline, then log, then
    segment, each zero-padded hex and fixed width, and a timeline only ever
    increases. That is the same comparison `pg_archivecleanup` makes.

    **One unreadable anchor refuses the whole run.** The alternative — ignore
    it and take the minimum of the rest — deletes exactly the segments the
    backup we could not read is the one most likely to need, because a base
    backup whose sidecar is missing is usually the one that was interrupted.
    """
    if not anchors:
        # No base backups at all. This is the 2026-08-31 state, and it is
        # precisely when deleting WAL is most tempting and least defensible:
        # with no base to replay onto, every segment looks useless — and the
        # first base backup taken makes the ones after it valuable again.
        return None
    if any(a is None for a in anchors):
        return None
    if any(not SEGMENT_RE.match(a) for a in anchors):  # type: ignore[arg-type]
        return None
    return min(anchors)  # type: ignore[type-var]


def read_anchors(base_dir: Path) -> list[Optional[str]]:
    """One anchor per retained base backup directory, in name order.

    A directory with no sidecar yields None — see `oldest_kept_segment`. A
    directory that is not a backup at all (no `base.tar.gz`) is skipped
    outright rather than counted as unreadable, so a stray `lost+found` or an
    editor's leftover cannot freeze retention forever.
    """
    anchors: list[Optional[str]] = []
    if not base_dir.is_dir():
        return anchors
    for entry in sorted(base_dir.iterdir()):
        if not entry.is_dir():
            continue
        if not (entry / "base.tar.gz").is_file():
            continue
        sidecar = entry / SIDECAR
        try:
            value = sidecar.read_text().strip()
        except OSError:
            anchors.append(None)
            continue
        anchors.append(value if SEGMENT_RE.match(value) else None)
    return anchors


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: wal_retention.py <base-backup-dir>", file=sys.stderr)
        return 2

    base_dir = Path(sys.argv[1])
    anchors = read_anchors(base_dir)
    keep = oldest_kept_segment(anchors)

    if keep is None:
        if not anchors:
            print("no base backup — keeping every segment", file=sys.stderr)
        else:
            print(
                f"{len(anchors)} base backup(s), "
                f"{sum(1 for a in anchors if a is None)} without a readable "
                f"{SIDECAR} — keeping every segment",
                file=sys.stderr,
            )
        return 1

    print(keep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
