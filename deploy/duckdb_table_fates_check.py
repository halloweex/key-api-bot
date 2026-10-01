#!/usr/bin/env python3
"""Which tables does the production DuckDB file hold that the fate manifest
does not name? Read-only, by hand, against a copy — never the live file.

Stage 5's precondition is a manifest covering every DuckDB table, walked
against `_init_schema` *and a live file*. The unit test walks the code; this
walks the file, because the file holds what the code no longer declares — the
five tables whose DDL went and which only the compaction's `DERIVED_TABLES`
still names, and whatever a migration that died half-way left behind.

WHAT IT READS

The newest nightly backup, `data/backups/analytics-*.duckdb`, unless `--file`
names another copy. The backup is written by the application itself and never
again, so it is consistent; a `cp` of the live file while web runs is torn and
looks exactly like index corruption. The live file is refused outright — by
name, by inode (a hard link is the same file) — and so is any file with a
`.wal` beside it, which is a file somebody holds open or closed uncleanly:
opening it would replay a log into a picture nobody wrote down.

It opens the copy with `read_only=True` and runs nothing but SELECTs against
the catalogue. It writes nothing, schedules nothing and is imported by nothing.

WHAT IT SAYS

- UNKNOWN   — a table or view in the file the manifest does not name, or one
              outside the `main` schema. Exit 1: decide its fate in
              `core/duckdb_table_fates.py` before anything else moves.
- MISMATCH  — a name the manifest declares as a table that the file holds as a
              view, or the other way round. Exit 1.
- SCRATCH   — a migration's scratch table is present, so that migration died
              half-way; the compaction would export it and fail to import it.
              Exit 1.
- absent    — declared, not in this file (a backup older than the table, or a
              retired table the compaction has already dropped). Information.

Exit 0 clean, 1 something to decide, 2 refused or nothing to read.

HOW TO RUN IT ON THE HOST

The host has no DuckDB; the web image does. Mount this file, the manifest and
the backups read-only into a throwaway container of the running image:

    cd /opt/key-api-bot && docker run --rm --user 0 \\
        -v "$PWD/data/backups:/app/data/backups:ro" \\
        -v "$PWD/core/duckdb_table_fates.py:/app/core/duckdb_table_fates.py:ro" \\
        -v "$PWD/deploy/duckdb_table_fates_check.py:/app/deploy/duckdb_table_fates_check.py:ro" \\
        -w /app --entrypoint python halloweex/keycrm-web:latest \\
        /app/deploy/duckdb_table_fates_check.py

`--user 0` because the image runs as uid 999 and the backups are root's.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.duckdb_table_fates import FATES, NONE, SCHEMA, SCRATCH  # noqa: E402

DATA_DIR = ROOT / "data"
LIVE_DB = DATA_DIR / "analytics.duckdb"
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_GLOB = "analytics-*.duckdb"

EXIT_CLEAN, EXIT_DECIDE, EXIT_REFUSED = 0, 1, 2

# DuckDB's information_schema spells these two; the manifest's `object` uses
# the same strings so a comparison needs no translation.
_CATALOGUE_SQL = """
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_catalog = current_database()
    ORDER BY table_schema, table_name
"""


def newest_backup(backup_dir: Path) -> Optional[Path]:
    """The newest `analytics-YYYYMMDD-HHMMSS.duckdb` by name — the stamp is in
    the name, and an mtime can be moved by a copy."""
    found = sorted(p for p in backup_dir.glob(BACKUP_GLOB) if p.is_file())
    return found[-1] if found else None


def refusal(path: Path, live: Path) -> Optional[str]:
    """Why `path` must not be opened, or None. Checked before anything opens
    it, so a refusal costs the running system nothing."""
    if not path.is_file():
        return f"{path} is not a file"
    if path.name == live.name:
        return (f"{path} is named like the live database; read the newest "
                f"backup instead, or a copy under another name")
    try:
        if live.exists() and os.path.samefile(path, live):
            return f"{path} is the live database ({live}) under another name"
    except OSError as exc:
        return f"cannot compare {path} with {live}: {exc}"
    wal = path.with_name(path.name + ".wal")
    if wal.exists():
        return (f"{wal} exists: the file is held open or was not closed "
                f"cleanly, so it is not a copy anybody can vouch for")
    return None


def read_catalogue(path: Path) -> List[Tuple[str, str, str]]:
    import duckdb

    conn = duckdb.connect(str(path), read_only=True)
    try:
        return [tuple(r) for r in conn.execute(_CATALOGUE_SQL).fetchall()]
    finally:
        conn.close()


def judge(catalogue: List[Tuple[str, str, str]]) -> Dict[str, List[str]]:
    """Pure: the file's catalogue against the manifest."""
    out: Dict[str, List[str]] = {
        "unknown": [], "mismatch": [], "scratch": [], "absent": [],
        "retired_present": [], "known": [],
    }
    present = set()
    for schema, name, kind in catalogue:
        if schema != "main":
            out["unknown"].append(f"{schema}.{name} ({kind})")
            continue
        present.add(name)
        fate = FATES.get(name)
        if fate is None:
            out["unknown"].append(f"{name} ({kind})")
            continue
        if fate.object != kind:
            out["mismatch"].append(
                f"{name}: the file holds a {kind}, the manifest declares a "
                f"{fate.object}")
            continue
        if fate.ddl == SCRATCH:
            out["scratch"].append(name)
        elif fate.ddl == NONE:
            out["retired_present"].append(name)
        out["known"].append(name)
    for name, fate in sorted(FATES.items()):
        if name not in present and fate.ddl in (SCHEMA, NONE):
            out["absent"].append(f"{name} ({fate.ddl})")
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report DuckDB tables the fate manifest does not name.")
    parser.add_argument("--file", type=Path,
                        help="a copy of the database to read (default: the "
                             "newest nightly backup)")
    parser.add_argument("--backups", type=Path, default=BACKUP_DIR,
                        help=f"where the nightly backups are (default {BACKUP_DIR})")
    parser.add_argument("--live", type=Path, default=LIVE_DB,
                        help=f"the live database, which is refused (default {LIVE_DB})")
    args = parser.parse_args(argv)

    path = args.file if args.file is not None else newest_backup(args.backups)
    if path is None:
        print(f"REFUSED: no {BACKUP_GLOB} under {args.backups}", file=sys.stderr)
        return EXIT_REFUSED
    why = refusal(path, args.live)
    if why:
        print(f"REFUSED: {why}", file=sys.stderr)
        return EXIT_REFUSED

    try:
        catalogue = read_catalogue(path)
    except Exception as exc:  # noqa: BLE001 — reported, and nothing was written
        print(f"REFUSED: cannot read {path} read-only: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    verdict = judge(catalogue)
    print(f"file: {path}")
    print(f"{len(catalogue)} tables and views; {len(verdict['known'])} named "
          f"by the manifest ({len(FATES)} entries)")
    for label, key in (("UNKNOWN", "unknown"), ("MISMATCH", "mismatch"),
                       ("SCRATCH", "scratch")):
        for line in verdict[key]:
            print(f"{label}: {line}")
    for name in verdict["retired_present"]:
        print(f"retired, still in the file (the compaction drops it): {name}")
    for line in verdict["absent"]:
        print(f"absent: {line}")

    if verdict["unknown"] or verdict["mismatch"] or verdict["scratch"]:
        print("→ decide each name's fate in core/duckdb_table_fates.py")
        return EXIT_DECIDE
    print("clean: every table in the file has a declared fate")
    return EXIT_CLEAN


if __name__ == "__main__":
    sys.exit(main())
