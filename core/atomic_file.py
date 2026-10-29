"""A file that is a decision, written whole or not at all.

The write-chain latch markers (`core.chain_latch`), the report ledgers' spool
(`core.report_ledger.spool`) and the UTM re-parse marker
(`core.warehouse_cutover`) are each read as a decision by the next process —
a chain that owns its tables, a week already delivered, a re-parse owed — so a
half-written one is worse than none, and a rename or an unlink that never
reached the disk is undone by the next power cut.

These were `chain_latch._write_marker` and `chain_latch._fsync_dir` until
step 14's S14-01 moved them here, unchanged, so that the other two stopped
importing the latch module to write a file. `core.chain_latch` keeps both
private names, bound to these objects: its own `latch` and `release` call
them, and the suite patches them there.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path


def fsync_dir(path: Path) -> None:
    """Push a directory entry — a create or an unlink — to the disk itself."""
    dir_fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_atomic(path: Path, payload: str) -> None:
    """Temp file, fsync, rename, fsync the directory.

    The latch marker decides where a chain's writes go for the rest of the
    machine's life, so a half-written one is worse than none:
    `chain_latch._stamp_of` would have to guess, and a rename that never
    reached the disk would unlatch the chain on the next power cut. This is
    the arrangement the rest of the repository uses where a file is a
    decision rather than a cache.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    fsync_dir(path.parent)
