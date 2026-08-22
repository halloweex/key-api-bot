#!/usr/bin/env python3
"""Freeze the warehouse so it can be read after the code that wrote it is gone.

This is not a backup. Backups rotate — `keep=2` for the file, thirty days for
the off-site parquet — and rotation is deletion on a schedule. The owner's
requirement is the opposite one: keep everything, be able to restore anything,
for as long as it takes. A mechanism that runs every night cannot satisfy it,
because every night it also throws something away.

So the Ark is taken **twice in the whole migration** and never rotated:

  1. before the first landing write to Postgres — after that moment DuckDB
     stops being the only writer of truth, and "what was in DuckDB" stops
     being reproducible;
  2. in the same deploy that removes DuckDB from production.

WHAT GOES IN, AND WHY EACH PART IS NECESSARY

  analytics.duckdb.frozen   A byte copy. The only artefact that restores the
                            database exactly as it was, with no application
                            code involved. Opens in seconds with any `duckdb`
                            binary of the right version.

  tables/ and views/        Parquet for **every** base table and every view,
                            materialised. This is insurance against the format,
                            not against loss: in five years DuckDB 1.5.5 may not
                            build for this architecture, and Parquet will still
                            be read by ClickHouse (`file()`), Postgres
                            (`parquet_fdw`), pandas and everything else.

                            Note it exports the *derived* tables too, which the
                            nightly export deliberately skips. The nightly is
                            right to skip them — they rebuild from Bronze. The
                            Ark is not, because after step 05 there is no Bronze
                            in DuckDB to rebuild them from.

  schema.sql                The DDL of every object, read from DuckDB's own
                            catalog. Today this is stored nowhere: restoring
                            means calling `_init_schema()` from the application,
                            which is precisely the dependency that makes the
                            archive unreadable once the application changes.
                            Parquet without DDL is data without meaning.

  manifest.json             COUNT(*) and SHA-256 per file, the DuckDB version,
                            the source file and its mtime. What makes a later
                            verification possible at all.

  RESTORE.md                Written into the directory, not into a wiki, because
                            the person who needs it may not have the wiki.

WHAT IS DELIBERATELY NOT HERE

  Not PITR, not continuous replication, not a daemon. The requirement is "keep
  and restore", not "roll back one minute", and charter rule 7 forbids a second
  store that accepts writes.

  And one honest limit, learned the hard way: an Ark cannot save what was never
  written. Twenty-one days of `inventory_sku_history` are missing because the
  snapshot job stopped writing on 2026-02-20, three hours after a commit
  rewrote it — the rescue dump taken at the deletion already ended there.
  Against that class, only a presence check helps (see PR #79). Freezing is for
  what exists; watching is for what should.

USAGE

    python3 deploy/ark_freeze.py --source data/backups/analytics-YYYYMMDD.duckdb \\
                                --out    data/ark

    python3 deploy/ark_freeze.py --verify data/ark/20260822T120000Z

Only `duckdb` and the standard library. No import from `core/`, by design: this
script must keep working after the modules it archives have been deleted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover - environment problem, not logic
    print("ark: duckdb is not importable; run inside the app image or a venv "
          "that has it", file=sys.stderr)
    raise SystemExit(2)

CHUNK = 1024 * 1024


def log(msg: str, level: str = "INFO") -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {level:<5} {msg}", flush=True)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def _quote(name: str) -> str:
    """Quote an identifier for DuckDB. Doubling is the escape, as in SQL."""
    return '"' + name.replace('"', '""') + '"'


def dump_schema(conn) -> str:
    """Reconstruct DDL for every object from DuckDB's own catalog.

    Order matters and is not alphabetical: sequences first, because column
    defaults reference them; then tables; then indexes; then views, which
    reference tables. A file that restores in its own order is the difference
    between an archive and a pile of statements.
    """
    parts: list[str] = [
        "-- Schema of the frozen warehouse, read from DuckDB's catalog.",
        "-- Restore order is significant: sequences, tables, indexes, views.",
        "-- Data lives in ../tables/*.parquet and ../views/*.parquet.",
        "",
    ]

    seqs = conn.execute("""
        SELECT sequence_name, start_value, increment_by, min_value, max_value, cycle
        FROM duckdb_sequences() ORDER BY sequence_name
    """).fetchall()
    if seqs:
        parts.append("-- ── sequences ──")
        for name, start, inc, lo, hi, cyc in seqs:
            parts.append(
                f"CREATE SEQUENCE IF NOT EXISTS {_quote(name)} "
                f"START {start} INCREMENT {inc};"
            )
        parts.append("")

    tables = [r[0] for r in conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchall()]

    # duckdb_constraints() is the only place PRIMARY KEY / UNIQUE / CHECK
    # survive as text. information_schema alone would silently drop them.
    #
    # NOT NULL is excluded on purpose, and the reason is a trap worth naming:
    # DuckDB reports it as a constraint like any other, but its
    # `constraint_text` is the bare string "NOT NULL" with no column in it —
    # 175 of them in this database. Appended as table-level constraints they
    # produce a line saying only `NOT NULL`, and the whole file stops parsing.
    # Nullability is already emitted per column above, from information_schema.
    #
    # This was caught by the L2 replay in verify(), with L0 and L1 both green:
    # the archive opened, counted correctly, and could not be restored without
    # the application. That is the exact failure this script exists to prevent,
    # which is why L2 is not optional.
    cons: dict[str, list[str]] = {}
    for tname, ctext in conn.execute("""
        SELECT table_name, constraint_text FROM duckdb_constraints()
        WHERE constraint_text IS NOT NULL
          AND constraint_type <> 'NOT NULL'
    """).fetchall():
        cons.setdefault(tname, []).append(ctext)

    parts.append("-- ── tables ──")
    for t in tables:
        cols = conn.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            ORDER BY ordinal_position
        """, [t]).fetchall()
        lines = []
        for cname, dtype, nullable, default in cols:
            line = f"    {_quote(cname)} {dtype}"
            if nullable == "NO":
                line += " NOT NULL"
            if default is not None:
                line += f" DEFAULT {default}"
            lines.append(line)
        for c in sorted(set(cons.get(t, []))):
            lines.append(f"    {c}")
        parts.append(f"CREATE TABLE IF NOT EXISTS {_quote(t)} (")
        parts.append(",\n".join(lines))
        parts.append(");")
        parts.append("")

    idx = conn.execute("""
        SELECT index_name, sql FROM duckdb_indexes() WHERE sql IS NOT NULL
        ORDER BY index_name
    """).fetchall()
    if idx:
        parts.append("-- ── indexes ──")
        for _name, sql in idx:
            parts.append(f"{sql.rstrip(';')};")
        parts.append("")

    views = conn.execute("""
        SELECT view_name, sql FROM duckdb_views()
        WHERE internal = false ORDER BY view_name
    """).fetchall()
    if views:
        parts.append("-- ── views ──")
        parts.append("-- Materialised into views/*.parquet as well: a view whose")
        parts.append("-- base tables changed shape will not recreate, and the")
        parts.append("-- numbers it produced are part of what is being kept.")
        parts.append("-- Emitted in dependency order, not alphabetical: views here")
        parts.append("-- build on other views, and the catalog does not say so.")
        for _name, sql in _order_views(views):
            parts.append(f"{sql.rstrip(';')};")
        parts.append("")

    return "\n".join(parts)


def _order_views(views: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Sort views so each is created after the views it references.

    Alphabetical order is wrong and fails loudly: `v_sku_analysis` is referenced
    by a view that sorts before it, and the replay stops with a catalog error.
    DuckDB's catalog exposes no dependency edges, so the reference is found the
    only way available — by looking for the other view's name in the SQL text.

    That test is deliberately generous. A false edge costs nothing but position;
    a missed one costs a broken file. Anything still unresolved after no further
    progress (a cycle, or a name that only appears quoted oddly) is appended
    as-is rather than dropped: a statement in the wrong place can be moved by
    hand, a statement that is not there cannot.
    """
    import re

    names = {name for name, _ in views}
    ordered: list[tuple[str, str]] = []
    emitted: set[str] = set()
    remaining = list(views)

    progress = True
    while remaining and progress:
        progress = False
        for item in list(remaining):
            name, sql = item
            deps = {
                other for other in names
                if other != name and re.search(rf"\b{re.escape(other)}\b", sql)
            }
            if deps <= emitted:
                ordered.append(item)
                emitted.add(name)
                remaining.remove(item)
                progress = True

    ordered.extend(remaining)
    return ordered


def freeze(source: Path, out_root: Path) -> Path:
    if not source.exists():
        log(f"source not found: {source}", "ERROR")
        raise SystemExit(1)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ark = out_root / stamp
    (ark / "tables").mkdir(parents=True, exist_ok=True)
    (ark / "views").mkdir(parents=True, exist_ok=True)

    log(f"source  {source}  ({source.stat().st_size / 1024**2:,.0f} MB)")
    log(f"ark     {ark}")

    # Read-only, and from a backup rather than the live file: copying the live
    # database yields a torn file that looks exactly like corruption.
    conn = duckdb.connect(str(source), read_only=True)
    duckdb_version = conn.execute("PRAGMA version").fetchone()[0]

    tables = [r[0] for r in conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchall()]
    views = [r[0] for r in conn.execute("""
        SELECT view_name FROM duckdb_views() WHERE internal = false ORDER BY view_name
    """).fetchall()]

    manifest: dict = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "duckdb_version": duckdb_version,
        "source": {
            "path": str(source),
            "bytes": source.stat().st_size,
            "mtime": datetime.fromtimestamp(source.stat().st_mtime, timezone.utc).isoformat(),
        },
        "tables": {},
        "views": {},
        "files": {},
    }

    log(f"exporting {len(tables)} tables and {len(views)} views")
    for t in tables:
        dest = ark / "tables" / f"{t}.parquet"
        conn.execute(
            f"COPY (SELECT * FROM {_quote(t)}) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        n = conn.execute(f"SELECT COUNT(*) FROM {_quote(t)}").fetchone()[0]
        manifest["tables"][t] = n

    for v in views:
        dest = ark / "views" / f"{v}.parquet"
        try:
            conn.execute(
                f"COPY (SELECT * FROM {_quote(v)}) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            n = conn.execute(f"SELECT COUNT(*) FROM {_quote(v)}").fetchone()[0]
            manifest["views"][v] = n
        except duckdb.Error as exc:
            # A view over a table that no longer exists is a fact about the
            # database worth recording, not a reason to abandon the freeze.
            log(f"view {v} not materialisable: {exc}", "WARN")
            manifest["views"][v] = None

    (ark / "schema.sql").write_text(dump_schema(conn), encoding="utf-8")
    conn.close()

    # The byte copy goes last: everything above reads the source, and a copy
    # taken after those reads is no less faithful while failing earlier if the
    # source is unreadable.
    frozen = ark / "analytics.duckdb.frozen"
    log("copying the database file")
    shutil.copy2(source, frozen)

    log("hashing")
    for f in sorted(ark.rglob("*")):
        if f.is_file() and f.name != "manifest.json":
            manifest["files"][str(f.relative_to(ark))] = {
                "bytes": f.stat().st_size,
                "sha256": sha256_of(f),
            }

    total = sum(v["bytes"] for v in manifest["files"].values())
    manifest["total_bytes"] = total
    (ark / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (ark / "RESTORE.md").write_text(RESTORE_DOC.format(
        stamp=stamp, version=duckdb_version,
        tables=len(tables), views=len(views),
    ), encoding="utf-8")

    log(f"{len(tables)} tables, {len(views)} views, {total / 1024**2:,.0f} MB", "OK")
    log(f"ark complete: {ark}", "OK")
    log("make it immutable and exclude it from every retention script:", "OK")
    log(f"    chattr +i -R {ark}", "OK")
    return ark


def verify(ark: Path) -> int:
    """Check an Ark without the application, the way a stranger would."""
    mpath = ark / "manifest.json"
    if not mpath.exists():
        log(f"no manifest at {mpath}", "ERROR")
        return 1
    manifest = json.loads(mpath.read_text())
    failures = 0

    log(f"verifying {ark}")
    log(f"written {manifest['created_at']} by DuckDB {manifest['duckdb_version']}")

    # L0 — the files are the files.
    for rel, meta in manifest["files"].items():
        f = ark / rel
        if not f.exists():
            log(f"missing: {rel}", "ERROR")
            failures += 1
            continue
        if f.stat().st_size != meta["bytes"]:
            log(f"size differs: {rel}", "ERROR")
            failures += 1
            continue
        if sha256_of(f) != meta["sha256"]:
            log(f"checksum differs: {rel}", "ERROR")
            failures += 1
    log(f"L0 files: {len(manifest['files'])} checked, {failures} bad",
        "OK" if not failures else "ERROR")

    # L1 — the frozen database still opens and still holds what was counted.
    frozen = ark / "analytics.duckdb.frozen"
    if frozen.exists():
        try:
            conn = duckdb.connect(str(frozen), read_only=True)
            mismatched = 0
            for t, expected in manifest["tables"].items():
                got = conn.execute(f"SELECT COUNT(*) FROM {_quote(t)}").fetchone()[0]
                if got != expected:
                    log(f"{t}: manifest {expected}, file {got}", "ERROR")
                    mismatched += 1
            conn.close()
            failures += mismatched
            log(f"L1 frozen db: {len(manifest['tables'])} tables, {mismatched} mismatched",
                "OK" if not mismatched else "ERROR")
        except duckdb.Error as exc:
            log(f"L1 cannot open frozen database: {exc}", "ERROR")
            failures += 1

    # L2 — the parquet answers on its own, through schema.sql and nothing else.
    # This is the level that survives the deletion of the application, and the
    # only one that proves the archive is readable rather than merely present.
    schema = ark / "schema.sql"
    if schema.exists():
        try:
            probe = duckdb.connect(":memory:")
            probe.execute(schema.read_text())
            checked = bad = 0
            for t, expected in manifest["tables"].items():
                pq = ark / "tables" / f"{t}.parquet"
                if not pq.exists():
                    continue
                got = probe.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{pq}')").fetchone()[0]
                checked += 1
                if got != expected:
                    log(f"parquet {t}: manifest {expected}, file {got}", "ERROR")
                    bad += 1
            probe.close()
            failures += bad
            log(f"L2 schema+parquet: {checked} tables replayed, {bad} mismatched",
                "OK" if not bad else "ERROR")
        except duckdb.Error as exc:
            log(f"L2 schema.sql does not replay: {exc}", "ERROR")
            failures += 1

    if failures:
        log(f"VERIFY FAILED: {failures} problem(s)", "ERROR")
        return 1
    log("VERIFY OK — the archive is readable without the application", "OK")
    return 0


RESTORE_DOC = """# How to read this archive

Frozen {stamp} from DuckDB {version}: {tables} tables, {views} views.

You do not need the application. You do not need this repository. You need a
`duckdb` binary and this directory.

## The fastest path — look at what was there

    duckdb analytics.duckdb.frozen
    -- read-only is safer, and enough:
    -- duckdb -readonly analytics.duckdb.frozen

Everything is where it was: same tables, same columns, same rows.

## If DuckDB {version} will not build any more

Parquet outlives engines. `schema.sql` holds the DDL of every object;
`tables/` and `views/` hold the data.

    duckdb
    .read schema.sql
    INSERT INTO orders SELECT * FROM read_parquet('tables/orders.parquet');
    -- ... one line per table, or loop in a shell

The same Parquet reads directly in ClickHouse (`file()`), in Postgres via
`parquet_fdw`, and in pandas. Nothing here is DuckDB-specific except the
convenience.

## Check it is intact

    python3 ark_freeze.py --verify .

Three levels: file checksums, then the frozen database's own counts, then —
the one that matters — replaying `schema.sql` into an empty engine and reading
the Parquet through it. The third level is what proves this archive can be read
after the code that wrote it is gone.

## What is not here

Rows that were never written. This archive preserves what the database held at
{stamp}; it cannot recover a snapshot the collector failed to take. For that,
see the presence checks in `core/data_quality.py`.
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--source", type=Path,
                    help="database file to freeze; use a nightly backup, never the live file")
    ap.add_argument("--out", type=Path, default=Path("data/ark"),
                    help="root directory for Arks (default: data/ark)")
    ap.add_argument("--verify", type=Path,
                    help="verify an existing Ark directory and exit")
    args = ap.parse_args()

    if args.verify:
        raise SystemExit(verify(args.verify))
    if not args.source:
        ap.error("--source is required unless --verify is given")
    freeze(args.source, args.out)


if __name__ == "__main__":
    main()
