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

Run inside the pinned image with a scratch directory mounted at /app/data,
holding `export_parquet/` from an archive. It writes analytics_clean.duckdb next
to it and leaves the caller to throw the whole directory away:

    docker run --rm --pull never -v /tmp/drill:/app/data --env-file .env -e KS_DUCKDB=on \\
        -v "$PWD/deploy/restore_from_export.py:/app/deploy/restore_from_export.py:ro" \\
        halloweex/keycrm-web:rollback-step14 python /app/deploy/restore_from_export.py

The pinned image is `rollback-step14` on the host, the last image before step
14, pinned at S14-00's deploy (step 14's H1/H2): DuckDB 1.5.5, the version
that wrote every archive, and the `compact_duckdb` this file imports from its
/app/scripts. Never `:latest`: from step 14's S14-19 on it carries no DuckDB,
and step 14 takes the application's DDL, which phase 2 rebuilds the schema
from, out of main. On a machine that has lost the host, load that image from
wherever it was saved (`docker save`, or pull the numbered tag it was pinned
from — the version production ran when S14-00 deployed — from the registry
and tag it `rollback-step14`). This file runs in that image, so it imports
nothing the image does not carry — tests/unit/test_archive_image.py holds it
to the standard library and the names it uses from `compact_duckdb`.

`-e KS_DUCKDB=on` because a `.env` from the week of silence — the live one, or
one restored from `env-<stamp>.gpg` — says `off`, and the switch would refuse
the rebuild before it imported a row; `-e` outranks `--env-file`. The sidecar
sees only the scratch directory.

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
    # meets every column dropped since. Nothing is dropped silently: INSERT BY
    # NAME raises a Binder Error for a column the target lacks, the column-
    # coverage WARN inside phase2_import names it first, and the drill exits 1
    # with a FAIL line naming the table. Such an archive restores only with
    # code at or before this SHA — check out that commit, or run the image
    # built from it, rather than reading the failure as a damaged snapshot. A
    # table dropped since is the other case, and it is skipped with a WARN
    # saying how many archived rows are not restored.
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
