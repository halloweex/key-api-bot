#!/usr/bin/env python3
"""Export a logical snapshot from a static database file, and validate it.

Runs in a sidecar with a scratch directory mounted at /app/data, containing
`analytics.duckdb` — a **hard link** to the newest nightly backup, which is a
consistent copy and is never written again. That indirection is the point:
compact_duckdb.py addresses /app/data by module constant, so linking the source
into place lets the proven export run unchanged, against a file no process
holds. No lock is taken, no live database is opened, and the daily job costs
the running system nothing.

Writes `export_parquet/` beside it, then decides whether it is fit to ship.
See core/snapshot_validation.py for why a logical export needs a validator that
a file copy does not: it can succeed, weigh the right amount, and be empty
where it matters.

Exits non-zero if the snapshot must not be shipped.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/app")
sys.path.insert(0, "/app/scripts")

import compact_duckdb as compact  # noqa: E402
from core.snapshot_validation import validate_snapshot  # noqa: E402

# Written next to the export so the next run can compare against it. Kept
# outside export_parquet/ because that directory is wiped on every export.
PREVIOUS_PATH = Path("/app/data/.last_snapshot.json")


def main() -> None:
    compact.section("DAILY SNAPSHOT: export + validate")

    if not compact.SOURCE_DB.exists():
        compact.log(f"No source at {compact.SOURCE_DB}", "ERROR")
        sys.exit(1)

    size_mb = compact.SOURCE_DB.stat().st_size / (1024 ** 2)
    compact.log(f"Source: {compact.SOURCE_DB} ({size_mb:,.0f} MB, read-only)")

    manifest = compact.phase1_export()

    previous = {}
    if PREVIOUS_PATH.exists():
        try:
            previous = json.loads(PREVIOUS_PATH.read_text())
        except (ValueError, OSError) as exc:
            compact.log(f"Previous snapshot unreadable, skipping comparison: {exc}", "WARN")

    verdict = validate_snapshot(
        manifest.get("counts", {}),
        previous_counts=previous.get("counts"),
        checksums=manifest.get("checksums"),
        previous_checksums=previous.get("checksums"),
    )

    compact.section("VALIDATION")
    for err in verdict.errors:
        compact.log(err, "ERROR")
    for warn in verdict.warnings:
        compact.log(warn, "WARN")

    # Declared, not discovered. An empty table that the manifest names is a
    # fact; an empty table nobody mentions is how four of them stayed empty
    # under a nightly "success" for a week.
    if verdict.empty_tables:
        compact.log(f"Declared empty ({len(verdict.empty_tables)}): "
                    + ", ".join(verdict.empty_tables))
    manifest["empty_tables"] = verdict.empty_tables
    manifest["validated"] = verdict.ok
    compact.MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, default=str))

    if not verdict.ok:
        compact.log(verdict.summary(), "ERROR")
        compact.log("Refusing to ship this snapshot.", "ERROR")
        sys.exit(1)

    # Only an accepted snapshot becomes the baseline. A rejected one must not
    # move the goalposts for the next comparison.
    PREVIOUS_PATH.write_text(json.dumps(
        {"counts": manifest.get("counts", {}), "checksums": manifest.get("checksums", {})},
        indent=2, default=str,
    ))

    compact.log(f"Snapshot {verdict.summary()}", "OK")
    compact.log(f"{len(manifest.get('tables', []))} tables ready to ship", "OK")


if __name__ == "__main__":
    main()
