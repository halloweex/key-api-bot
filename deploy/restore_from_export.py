#!/usr/bin/env python3
"""Rebuild a DuckDB warehouse from an exported Parquet snapshot, and validate it.

This is phases 2 and 3 of scripts/compact_duckdb.py and nothing else. It
deliberately adds no restore logic of its own: phase2_import and
phase3_validate are the code that rebuilds the *live* database from Parquet
every Sunday, 12 runs out of 12, and the whole argument for shipping Parquet
off-site is that the restore path is already proven. Reimplementing it here
would throw that away.

What is skipped:
  - phase 0 (preflight) — sized for compacting a multi-GB source in place
  - phase 1 (export)    — the snapshot is what we are restoring FROM
  - phase 4 (swap)      — a drill must never touch a canonical database

Run inside the app image with a scratch directory mounted at /app/data, holding
`export_parquet/` from an archive. It writes analytics_clean.duckdb next to it
and leaves the caller to throw the whole directory away:

    docker run --rm -v /tmp/drill:/app/data --env-file .env \\
        halloweex/keycrm-web:latest python /app/deploy/restore_from_export.py

Exits non-zero if the snapshot does not restore, or does not validate.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/app/scripts")

import compact_duckdb as compact  # noqa: E402


def main() -> None:
    compact.section("RESTORE DRILL: import + validate (no swap)")

    manifest_path = Path(compact.MANIFEST_PATH)
    if not manifest_path.exists():
        compact.log(f"No manifest at {manifest_path}", "ERROR")
        compact.log(
            "Expected the archive to unpack as export_parquet/_manifest.json",
            "ERROR",
        )
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text())
    compact.log(f"Snapshot exported at: {manifest.get('exported_at', '?')}")
    compact.log(f"DuckDB at export time: {manifest.get('duckdb_version', '?')}")
    compact.log(f"Tables in manifest:    {len(manifest.get('tables', []))}")

    # The commit the snapshot was taken at. phase2_import rebuilds the schema
    # from the application's own DDL, so restoring an old snapshot with new code
    # can silently drop columns the export has and the target does not — see the
    # column-coverage WARN inside phase2_import. If these differ, read that
    # report rather than trusting the row counts alone.
    deploy_path = manifest_path.parent / "_deploy.json"
    if deploy_path.exists():
        deploy = json.loads(deploy_path.read_text())
        compact.log(f"Taken at deploy SHA:   {deploy.get('deploy_sha', '?')}")

    new_size_mb = compact.phase2_import(manifest)
    compact.phase3_validate(manifest)

    compact.log(f"Restored database: {new_size_mb:.1f} MB at {compact.NEW_DB}", "OK")
    compact.log("This was a drill. Nothing canonical was touched.", "OK")


if __name__ == "__main__":
    main()
