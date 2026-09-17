#!/usr/bin/env python3
"""
DuckDB compaction: Export → Schema-first Import → Validate.

Purges MVCC tombstones, write-write conflict artifacts, and bloat
by exporting all bronze/essential tables to Parquet, creating a fresh
database with the app's own schema (preserving PKs, constraints, indexes),
and importing data via INSERT BY NAME.

Silver/Gold derived tables are skipped — rebuilt on first app startup
via refresh_warehouse_layers().

Usage (inside container, with web stopped):
    python /app/scripts/compact_duckdb.py

Or via sidecar:
    docker run --rm -v /opt/key-api-bot/data:/app/data \
        --env-file /opt/key-api-bot/.env \
        halloweex/keycrm-web:latest \
        python /app/scripts/compact_duckdb.py
"""
import asyncio
import sys
import os
import time
import shutil
import json
import re
import duckdb
from pathlib import Path

DATA_DIR = Path("/app/data")
SOURCE_DB = DATA_DIR / "analytics.duckdb"
EXPORT_DIR = DATA_DIR / "export_parquet"
NEW_DB = DATA_DIR / "analytics_clean.duckdb"
MANIFEST_PATH = EXPORT_DIR / "_manifest.json"

DERIVED_TABLES = frozenset({
    "silver_orders", "silver_order_utm",
    "gold_daily_revenue",

    # ── Dropped from the schema, still present in the production database ──
    #
    # Neither of these has a DDL any more, so _init_schema() does not create
    # them in the new database. They are listed here — the set that decides
    # what is *not* exported — because all_tables is read from the *source*
    # database, which still has both.
    #
    # Leaving either one out is not a no-op, and the two failure modes differ:
    #
    #   gold_product_pairs (5,185 rows) would be exported to Parquet and
    #   imported back, recreating the table this deletion removed and putting
    #   its rows in the off-site archive on the way.
    #
    #   orders_v2 (0 rows) would be exported too, and then `INSERT INTO
    #   "orders_v2"` would fail against a schema that no longer defines it.
    #   That error is not a duplicate-key error, so it lands in import_errors
    #   and phase 2 exits 1 — the whole weekly compact aborts, and with it the
    #   off-site export that runs after it.
    #
    #   gold_daily_traffic (5,874 rows) is the third and newest of these. Its
    #   DDL went when `/traffic` finished moving to Postgres and the layer was
    #   left with no reader; the physical table was deliberately not dropped in
    #   the same change, so it is exactly `gold_product_pairs`' case — omit it
    #   and the next compact puts the table back and ships its rows off-site.
    #
    # Remove a name from here only after a compact has run and the table is
    # gone from the production database.
    "gold_product_pairs",
    "orders_v2",
    "gold_daily_traffic",
    #
    #   gold_daily_products (90,696 rows) is the largest of them and went the
    #   same way, one change later: three tabs and the weekly report each moved
    #   to the order-lines level, which reproduces it to the kopeck, and a
    #   layer with no reader is not worth rebuilding every two minutes.
    "gold_daily_products",
    #
    #   bronze_order_events (0 rows) is `orders_v2`' case exactly, and is here
    #   for the same reason: its DDL went when the H3 staging-merge subsystem
    #   was retired on 2026-09-14, so an export would be followed by an
    #   `INSERT INTO "bronze_order_events"` against a schema that no longer
    #   defines it — which aborts the whole weekly compact and the off-site
    #   export behind it. Nothing is lost by excluding it: the table has been
    #   empty since 2026-05-19, when the shadow write became opt-in and the
    #   default went off. This exclusion is also what physically removes it —
    #   the next compact builds the new database from `_init_schema()`, which
    #   no longer declares it.
    "bronze_order_events",
})

MEM_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")

# The sidecar runs `python /app/scripts/compact_duckdb.py`. Python puts the
# script's own directory at sys.path[0] — /app/scripts, not /app and not the
# working directory — and the image sets no PYTHONPATH, so `core` cannot be
# imported until the application root is added by hand. A module-level
# `from core ...` here would raise ModuleNotFoundError at 02:00 on a Sunday
# while every unit test stayed green, because the tests import this file with
# the repository already on sys.path.
#
# Derived from this file rather than written as "/app" so that the same lines
# work from a checkout: tests/unit/test_compact_sequences.py runs the script by
# path from another directory, which is the only check that sees the imports
# the way the sidecar does. In the image this file is /app/scripts/..., so the
# root is /app, exactly what the literal used to say.
APP_ROOT = Path(__file__).resolve().parent.parent


def _ensure_app_importable() -> None:
    """Put the application root on sys.path; the imports stay at the call site.

    At the call site, and through the module attribute, on purpose: a test that
    replaces core.duckdb_sequences.advance_to must reach the call this script
    makes, or a mutation test would pass against code it never touched.
    """
    root = str(APP_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def log(msg: str, level: str = "INFO"):
    color = {"INFO": "", "OK": GREEN, "WARN": YELLOW, "ERROR": RED}
    ts = time.strftime("%H:%M:%S")
    prefix = f"{color.get(level, '')}{level}{RESET}" if level != "INFO" else "INFO"
    print(f"[{ts}] {prefix}  {msg}", flush=True)


def section(title: str):
    log(f"\n{BOLD}{'='*60}")
    log(f"  {title}")
    log(f"{'='*60}{RESET}\n")


# ─── Phase 1: Pre-flight ────────────────────────────────────────────────────

# Empirical compaction ratio: post-compact DB / source DB.
# Observed Apr 23, 2026: 13 GB → 4.5 GB = 0.35. When a DB is bloated by MVCC
# tombstones the ratio tends to be MORE aggressive (most bytes are dead), so
# 0.50 is a safe upper bound that still allows compact on a 75 GB disk when
# DB has grown to ~50 GB.
NEW_DB_RATIO = 0.50

# Fixed overhead, regardless of source size: parquet export (~50-200 MB),
# duckdb_tmp spill files during ZSTD compression, the secondary 3 GB check
# in phase2_import() before INSERT, and rounding slack.
SAFETY_BUFFER_GB = 1.0


def compute_disk_requirements(source_size_gb: float) -> float:
    """Minimum additional free disk space required to compact safely.

    Source DB already occupies its bytes on disk — those bytes are NOT counted
    in the requirement. We only need space for new artifacts created during
    compaction:

      1. Parquet export of bronze/essential tables  → ~50-200 MB
      2. Newly built compact DB                     → NEW_DB_RATIO × source

    The source stays read-only on disk until weekly_compact.sh performs the
    atomic swap AFTER validation passes. Worst-case peak disk usage is
    therefore: source + parquet + new_db ≤ source × (1 + NEW_DB_RATIO) + buffer.

    Returns required free space in GB (always ≥ SAFETY_BUFFER_GB so we don't
    try to compact onto a near-full disk even when source is tiny).
    """
    if source_size_gb < 0:
        raise ValueError(f"source_size_gb must be non-negative, got {source_size_gb}")
    return source_size_gb * NEW_DB_RATIO + SAFETY_BUFFER_GB


def preflight() -> None:
    section("PHASE 0: PRE-FLIGHT CHECKS")

    if not SOURCE_DB.exists():
        log(f"Source DB not found: {SOURCE_DB}", "ERROR")
        sys.exit(1)

    source_size_gb = SOURCE_DB.stat().st_size / (1024**3)
    log(f"Source DB: {source_size_gb:.2f} GB")

    wal_path = Path(str(SOURCE_DB) + ".wal")
    if wal_path.exists():
        wal_mb = wal_path.stat().st_size / (1024**2)
        log(f"WAL file: {wal_mb:.1f} MB (will be replayed on connect)")

    total, used, free = shutil.disk_usage(str(DATA_DIR))
    free_gb = free / (1024**3)
    log(f"Disk free: {free_gb:.1f} GB")

    required_gb = compute_disk_requirements(source_size_gb)

    # Operator override: set FORCE_COMPACT=1 to bypass the preflight disk check.
    # Safe because source DB is read-only throughout phase 1 (export) and is
    # NOT touched again until weekly_compact.sh performs the atomic swap AFTER
    # phase 3 validation passes. If we run out of disk mid-import, the new DB
    # is incomplete, the script exits non-zero, and weekly_compact.sh restarts
    # services on the unchanged source DB. No data loss path.
    force = os.getenv("FORCE_COMPACT", "").strip().lower() in {"1", "true", "yes"}

    if free_gb < required_gb:
        if force:
            log(f"FORCE_COMPACT=1 set — bypassing preflight "
                f"(need {required_gb:.1f} GB, have {free_gb:.1f} GB). "
                f"Source DB stays intact; abort-mid-import is a recoverable "
                f"no-op.", "WARN")
        else:
            log(f"Need at least {required_gb:.1f} GB free "
                f"(parquet + new DB at ratio {NEW_DB_RATIO:.0%}). "
                f"Set FORCE_COMPACT=1 to override if you accept the risk "
                f"of abort-mid-import (no data loss; just wasted run).",
                "ERROR")
            sys.exit(1)
    else:
        log(f"Disk OK (need ~{required_gb:.1f} GB, have {free_gb:.1f} GB)", "OK")


# ─── Phase 1: Export ─────────────────────────────────────────────────────────

# `CREATE SEQUENCE s INCREMENT BY 1 MINVALUE 1 MAXVALUE ... START 42 NO CYCLE;`
# The rendering core/duckdb_sequences.py reads too; its docstring records the
# states in which `START` was measured to be the stored counter.
_SEQUENCE_START = re.compile(r"\bSTART\s+(-?\d+)\b")


def _rendered_start(sql):
    match = _SEQUENCE_START.search(sql or "")
    return int(match.group(1)) if match else None


def phase1_export() -> dict:
    section("PHASE 1: EXPORT TO PARQUET")

    src = duckdb.connect(str(SOURCE_DB), read_only=True)
    src.execute(f"SET memory_limit='{MEM_LIMIT}'")
    tmp_dir = DATA_DIR / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    src.execute(f"SET temp_directory='{tmp_dir}'")

    all_tables = [r[0] for r in src.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()]

    export_tables = sorted(t for t in all_tables if t not in DERIVED_TABLES)
    skip_tables = sorted(t for t in all_tables if t in DERIVED_TABLES)

    log(f"Total tables: {len(all_tables)}")
    log(f"Exporting: {len(export_tables)} bronze/essential tables")
    log(f"Skipping: {len(skip_tables)} derived (Silver/Gold): {', '.join(skip_tables)}")

    # ── Row counts ──
    counts = {}
    log("\nRow counts:")
    for t in all_tables:
        try:
            cnt = src.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        except Exception as e:
            log(f"  {t}: ERROR reading — {e}", "WARN")
            cnt = -1
        counts[t] = cnt
        marker = " [derived, skip]" if t in DERIVED_TABLES else ""
        log(f"  {t}: {cnt:,}{marker}")

    # ── Sequences ──
    # What is recorded is the value the source would hand out next, and it
    # takes two readings to get it. last_value, on a fresh read-only open, is
    # the stored counter: after a clean close that is the next value, but after
    # a WAL replay (web killed without a checkpoint) it is the counter as of the
    # last checkpoint. Measured on 1.5.5 with a checkpoint at 50,001 and the WAL
    # burning on to 300,004: last_value and start_value both read 50,001, while
    # the rendered `START` in duckdb_sequences().sql read 300,004 — the value a
    # real nextval then returned. A sequence first used after the checkpoint
    # read last_value 1 against START 31. core.duckdb_sequences.next_value
    # cannot be asked here: it trusts the rendering only within one increment
    # of last_value, so in exactly this state it returns the low reading.
    #
    # So the larger of the two is recorded. The rendering is not a documented
    # interface, and taking the maximum makes either way it could go wrong
    # safe: a reading too high costs a gap in the ids, never a collision, and
    # a reading too low still meets phase 2's MAX(id) floor. Both raw readings
    # stay in the manifest, and the log gives both wherever they differ, so a
    # gap can be explained later.
    seq_values = {}
    seq_readings = {}
    log("\nSequences:")
    seq_rows = src.execute(
        "SELECT sequence_name, start_value, last_value, sql FROM duckdb_sequences()"
    ).fetchall()
    for seq_name, start_val, last_val, sql in seq_rows:
        # NULL last_value: never used, so the counter is still the start.
        counter = last_val if last_val is not None else start_val
        rendered = _rendered_start(sql)
        candidates = [v for v in (counter, rendered) if v is not None]
        seq_values[seq_name] = max(candidates) if candidates else None
        seq_readings[seq_name] = {"last_value": last_val, "rendered_start": rendered}
        recorded = _fmt_saved(seq_values[seq_name])
        if rendered is None:
            # The same change in DuckDB fails test_duckdb_sequences in CI first.
            log(f"  {seq_name}: next {recorded} from the stored counter alone — "
                f"the rendered START could not be read, and after a WAL replay "
                f"the counter is as of the last checkpoint; MAX(id) is the "
                f"floor that still holds", "WARN")
        elif counter is not None and rendered > counter:
            log(f"  {seq_name}: next {recorded} (stored counter {counter:,}, as "
                f"of the last checkpoint; the WAL replay moved it on)")
        elif counter is not None and rendered < counter:
            log(f"  {seq_name}: next {recorded} from the stored counter — the "
                f"rendered START, {rendered:,}, reads lower, which no measured "
                f"state does", "WARN")
        else:
            log(f"  {seq_name}: next {recorded}")

    # ── Checksums for validation ──
    log("\nChecksums:")
    checksums = {}

    min_date, max_date = src.execute(
        "SELECT MIN(DATE(ordered_at)), MAX(DATE(ordered_at)) FROM orders"
    ).fetchone()
    checksums["orders_date_range"] = [str(min_date), str(max_date)]
    log(f"  Orders date range: {min_date} → {max_date}")

    total_rev = float(src.execute(
        "SELECT COALESCE(ROUND(SUM(grand_total), 2), 0) FROM orders "
        "WHERE status_id NOT IN (6,7,10)"
    ).fetchone()[0])
    checksums["total_revenue"] = total_rev
    log(f"  Total revenue (excl returns): ₴{total_rev:,.2f}")

    orders_by_source = {str(r[0]): r[1] for r in src.execute(
        "SELECT source_id, COUNT(*) FROM orders GROUP BY source_id"
    ).fetchall()}
    checksums["orders_by_source"] = orders_by_source
    log(f"  Orders by source: {orders_by_source}")

    # ── Export to Parquet ──
    if EXPORT_DIR.exists():
        shutil.rmtree(EXPORT_DIR)
    EXPORT_DIR.mkdir(parents=True)

    log("\nExporting tables:")
    total_export_mb = 0
    for t in export_tables:
        if counts.get(t, 0) <= 0:
            log(f"  {t}: empty, skip")
            continue
        outpath = EXPORT_DIR / f"{t}.parquet"
        src.execute(f'COPY "{t}" TO \'{outpath}\' (FORMAT PARQUET, COMPRESSION ZSTD)')
        size_mb = outpath.stat().st_size / (1024**2)
        total_export_mb += size_mb
        log(f"  {t}: {counts[t]:,} rows → {size_mb:.1f} MB")

    log(f"\nTotal export size: {total_export_mb:.1f} MB", "OK")

    src.close()
    log("Source database closed")

    manifest = {
        "exported_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "source_db": str(SOURCE_DB),
        "tables": export_tables,
        "counts": counts,
        # The value each sequence would hand out next. Phases 2 and 3 read
        # this key; archives written before 2026-09-17 hold last_value here.
        "seq_values": seq_values,
        "seq_readings": seq_readings,
        "checksums": checksums,
        "duckdb_version": duckdb.__version__,
    }
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    log(f"Manifest saved: {MANIFEST_PATH}")

    return manifest


# ─── Phase 2: Import ─────────────────────────────────────────────────────────

def _fmt_saved(saved) -> str:
    return "unknown" if saved is None else f"{int(saved):,}"


def _fail_line(failures) -> str:
    """The summary weekly_compact.sh quotes in its alert, on one line.

    Flattened because DuckDB's messages run over several lines — the Binder
    Error for a column the target lacks ends two lines down in a `Did you
    mean` hint — and the wrapper quotes the last *line* that matches. Left as
    they come, the summary is no longer the last line of the run, its tail is
    cut from the alert, and a continuation that happens to say Error would be
    quoted in its place.
    """
    return "FAIL: " + "; ".join(" ".join(str(f).split()) for f in failures)


def restore_sequences(conn, seq_values: dict, created_tables) -> None:
    """Leave every id sequence above the ids its table holds, and no lower than
    where the source left it.

    The new database is built from DDL, so every sequence in it starts at 1
    while the imported tables keep their ids. DuckDB 1.5.5 cannot *set* a
    sequence: `ALTER SEQUENCE ... RESTART` is not implemented, and in this
    session — the one that has just created the tables whose DEFAULTs name
    these sequences — DROP SEQUENCE raises DependencyException. The one lever
    is burning values, and core/duckdb_sequences.advance_to is the tested form
    of it.

    Until 2026-09-17 this function burned with `SELECT nextval(..) FROM
    range(n)` and never read the result. DuckDB evaluates only the chunks a
    client pulls, so the burn stopped at 129,024 values whatever n was, the
    abandoned statement wrote no position to the WAL, and the log line printed
    the target rather than a reading. A table whose MAX(id) reached 129,025
    would have gone live handing out ids below that MAX — a primary-key error
    where its ids are dense, a silent write into a hole where they are not —
    behind a phase 3 that passed. advance_to burns with a fetched aggregate and
    reads the position back; the line logged here is that reading.

    Floor, per sequence, is max(MAX(id), saved - 1):

    * `saved` is the value the source would hand out next, as phase 1 records
      it, so the last id it handed out is saved - 1 and the new file hands out
      exactly `saved`. The old `saved + 1` skipped one id per sequence every
      Sunday. It also keeps ids a table no longer holds from being handed out
      twice — reconciliation_seq stood at 2,614 over a MAX of 2,597 in the
      2026-09-17 backup.
    * MAX(id) stays a floor because `saved` has not always been that value.
      Until 2026-09-17 phase 1 recorded last_value alone, which after a WAL
      replay is the counter as of the last checkpoint — measured 50,001
      against a true next value of 300,004 — and an archive written then still
      holds it. Phase 1 now also reads the rendered START, which carried
      300,004, and records the larger. Should that rendering ever stop being
      readable, MAX(id) still protects every row that exists; what it cannot
      protect is an id handed out after the checkpoint and deleted since.

    The map is core.migrations.SEQUENCE_ID_COLUMNS, the list the boot
    migration 0027 repairs from. This file used to keep a second copy of it.
    """
    _ensure_app_importable()
    from core import duckdb_sequences
    from core.migrations import SEQUENCE_ID_COLUMNS

    log("\nRestoring sequences:")
    mapped = set()
    for seq_name, table, col in SEQUENCE_ID_COLUMNS:
        mapped.add(seq_name)
        # A table dropped from the schema while its entry stayed behind. The
        # DuckDB exit is dropping tables one write chain at a time, and an
        # entry that outlives its table must cost a warning, not every Sunday's
        # compaction; migration 0027 treats the same state the same way.
        # tests/unit/test_compact_sequences.py fails CI on the stale entry.
        if table not in created_tables:
            log(f"  sequence {seq_name}: {table} is not in this version's "
                f"schema — skipped", "WARN")
            continue
        saved = seq_values.get(seq_name)
        try:
            max_id = int(conn.execute(
                f'SELECT COALESCE(MAX("{col}"), 0) FROM "{table}"'
            ).fetchone()[0])
            floor = max_id if saved is None else max(max_id, int(saved) - 1)
            burned = duckdb_sequences.advance_to(conn, seq_name, floor)
            after = duckdb_sequences.next_value(conn, seq_name)
        except Exception as e:
            log(f"  sequence {seq_name}: restore FAILED — {e}", "ERROR")
            raise
        log(f"  sequence {seq_name}: next {after:,} ({table}.{col} MAX "
            f"{max_id:,}, source next {_fmt_saved(saved)}, burned {burned:,})",
            "OK")

    unmapped = sorted(set(seq_values) - mapped)
    if not unmapped:
        return
    declared = {r[0] for r in conn.execute(
        "SELECT sequence_name FROM duckdb_sequences()"
    ).fetchall()}
    for seq_name in unmapped:
        saved = seq_values.get(seq_name)
        if seq_name not in declared:
            # The source still holds a sequence whose DDL is gone. This build
            # is what removes it, so the warning appears on one Sunday only.
            log(f"  sequence {seq_name}: in the source only — this schema no "
                f"longer declares it, nothing to restore", "WARN")
            continue
        # Declared, but nobody said which table it numbers, so there is no
        # MAX to floor on. The source's own position is still known and is the
        # most that can be done — what the old loop did for every sequence.
        # The schema-walk test exists so this never runs.
        burned = 0
        if saved is not None:
            burned = duckdb_sequences.advance_to(conn, seq_name, int(saved) - 1)
        after = duckdb_sequences.next_value(conn, seq_name)
        log(f"  sequence {seq_name}: not in SEQUENCE_ID_COLUMNS — next {after:,} "
            f"from the source's position alone (source next {_fmt_saved(saved)}, "
            f"burned {burned:,}), no MAX(id) floor", "WARN")


def phase2_import(manifest: dict) -> float:
    section("PHASE 2: CREATE CLEAN DB + IMPORT")

    counts = manifest["counts"]
    seq_values = manifest["seq_values"]
    export_tables = manifest["tables"]

    total, used, free = shutil.disk_usage(str(DATA_DIR))
    free_gb = free / (1024**3)
    log(f"Disk free before import: {free_gb:.1f} GB")
    if free_gb < 3:
        log("ABORT: Less than 3 GB free", "ERROR")
        sys.exit(1)

    for p in [NEW_DB, Path(str(NEW_DB) + ".wal")]:
        if p.exists():
            p.unlink()

    # ── Create schema via app's own DuckDBStore ──
    log("Creating schema via DuckDBStore._init_schema()...")
    _ensure_app_importable()
    from core.duckdb_store import DuckDBStore

    async def create_schema():
        store = DuckDBStore(db_path=NEW_DB)
        await store.connect()
        return store

    store = asyncio.run(create_schema())
    conn = store._connection

    conn.execute(f"SET memory_limit='{MEM_LIMIT}'")
    conn.execute("SET preserve_insertion_order=false")
    tmp_dir = DATA_DIR / "duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    conn.execute(f"SET temp_directory='{tmp_dir}'")

    created_tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()]
    log(f"Schema created: {len(created_tables)} tables (incl. empty Silver/Gold)")

    # ── Verify target columns cover source columns ──
    log("\nColumn coverage check:")
    for t in export_tables:
        parquet_path = EXPORT_DIR / f"{t}.parquet"
        if not parquet_path.exists():
            continue
        # The import loop below reports a table this schema no longer defines,
        # and every one of its columns would otherwise be "missing" here too.
        if t not in created_tables:
            continue
        # DESCRIBE, not parquet_schema(). parquet_schema() lists every element
        # of the file's schema tree: the root, which DuckDB names
        # `duckdb_schema`, and the children of nested columns (`list`,
        # `element`, struct fields). The root alone made this check report
        # `duckdb_schema` as a dropped column on every table, every Sunday.
        # DESCRIBE gives the top-level columns of the very SELECT * the import
        # below runs, so nothing real can hide behind the change.
        pq_cols = set(r[0] for r in conn.execute(
            f"SELECT column_name FROM "
            f"(DESCRIBE SELECT * FROM read_parquet('{parquet_path}'))"
        ).fetchall())
        tbl_cols = set(r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{t}'"
        ).fetchall())
        missing = pq_cols - tbl_cols
        if missing:
            log(f"  {t}: source columns not in target: {sorted(missing)}", "WARN")
            # Not dropped: measured on 1.5.5, INSERT BY NAME raises a Binder
            # Error for a column the target lacks, so the import below fails
            # and phase 2 exits 1. Saying "dropped" sent the reader to the
            # wrong conclusion about why the run stopped.
            log("    INSERT BY NAME refuses a column the target does not have — "
                "this table's import will fail and abort the run", "WARN")
        extra = tbl_cols - pq_cols
        if extra:
            log(f"  {t}: target columns not in source: {extra} (will be NULL/DEFAULT)")

    # ── Import data ──
    log("\nImporting data:")
    import_errors = []
    for t in export_tables:
        parquet_path = EXPORT_DIR / f"{t}.parquet"
        if not parquet_path.exists():
            continue

        # The table list comes from the snapshot's manifest, and the schema
        # comes from today's code, so a restore of an older archive can name a
        # table this version no longer defines. Without this check the INSERT
        # below raises a Catalog Error, which is not a duplicate-key error, so
        # it reaches import_errors and aborts the run — meaning one table
        # dropped from the schema would make every archive taken before that
        # commit unrestorable. Announced rather than passed over in silence:
        # the rows are real and they are not being restored.
        if t not in created_tables:
            log(f"  {t}: not in this version's schema — {counts.get(t, 0):,} "
                f"rows in the archive are NOT being restored", "WARN")
            continue

        expected = counts.get(t, 0)
        try:
            conn.execute(
                f'INSERT INTO "{t}" BY NAME '
                f"SELECT * FROM read_parquet('{parquet_path}')"
            )
            imported = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            if imported == expected:
                log(f"  {t}: {imported:,} rows", "OK")
            else:
                log(f"  {t}: {imported:,} rows (expected {expected:,})", "WARN")
        except Exception as e:
            err_str = str(e)
            if "Duplicate" in err_str or "UNIQUE" in err_str or "PRIMARY" in err_str:
                log(f"  {t}: duplicate key — deduplicating...", "WARN")
                conn.execute(f'DELETE FROM "{t}"')
                # duckdb_constraints() has no `column_name`; it returns one row
                # per constraint carrying a LIST in constraint_column_names.
                # Asking for the wrong name raised a Binder Error here — inside
                # the handler meant to recover from duplicates — so the
                # recovery itself crashed the import it was written to rescue.
                pk_cols = [
                    col
                    for (cols,) in conn.execute(
                        f"SELECT constraint_column_names FROM duckdb_constraints() "
                        f"WHERE table_name='{t}' AND constraint_type='PRIMARY KEY'"
                    ).fetchall()
                    for col in cols
                ]
                if pk_cols:
                    partition = ", ".join(f'"{c}"' for c in pk_cols)
                    conn.execute(f"""
                        INSERT INTO "{t}" BY NAME
                        SELECT * EXCLUDE(_rn) FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY {partition}
                            ) AS _rn
                            FROM read_parquet('{parquet_path}')
                        ) WHERE _rn = 1
                    """)
                else:
                    conn.execute(f"""
                        INSERT INTO "{t}" BY NAME
                        SELECT DISTINCT * FROM read_parquet('{parquet_path}')
                    """)
                imported = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                dropped = expected - imported
                log(f"  {t}: {imported:,} rows ({dropped} duplicates removed)", "WARN")
            else:
                log(f"  {t}: FATAL — {e}", "ERROR")
                import_errors.append((t, str(e)))

    if import_errors:
        log(f"\n{len(import_errors)} tables failed to import:", "ERROR")
        for t, err in import_errors:
            log(f"  {t}: {err}", "ERROR")
        log("ABORTING — clean DB is incomplete", "ERROR")
        # Last, as in phase 3: weekly_compact.sh quotes the final ERROR/FAIL
        # line, and without this that is ABORTING, which names neither the
        # table nor the error. This is the abort a table or column dropped from
        # the schema produces — see DERIVED_TABLES — so it is the one the alert
        # most needs to name.
        log(_fail_line(f"{t}: {err}" for t, err in import_errors), "ERROR")
        sys.exit(1)

    # ── Restore sequences ──
    # Deliberately not wrapped: a restore that fails leaves the clean file with
    # sequences that may hand out ids its tables already hold. The exception
    # ends the run here, before phase 3 and before any swap, with the source
    # untouched; weekly_compact.sh restarts the services on it.
    restore_sequences(conn, seq_values, created_tables)

    # ── Checkpoint ──
    log("\nFlushing WAL...")
    conn.execute("CHECKPOINT")

    new_size_mb = NEW_DB.stat().st_size / (1024**2)
    # The source is present when compacting, and absent when restoring an
    # export onto a machine that has no database yet — which is the case a
    # backup exists for. This line only reports a ratio, so it must not be the
    # thing that fails an import that has already succeeded.
    if SOURCE_DB.exists():
        source_size_mb = SOURCE_DB.stat().st_size / (1024**2)
        reduction = 100 * (1 - new_size_mb / source_size_mb) if source_size_mb > 0 else 0
        log(f"New DB: {new_size_mb:.1f} MB (was {source_size_mb:.0f} MB, {reduction:.0f}% smaller)", "OK")
    else:
        log(f"New DB: {new_size_mb:.1f} MB (no source to compare — restoring, not compacting)", "OK")

    conn.close()
    store._connection = None
    if store._executor:
        store._executor.shutdown(wait=False)
    return new_size_mb


# ─── Phase 3: Validate ───────────────────────────────────────────────────────

def phase3_validate(manifest: dict) -> None:
    section("PHASE 3: VALIDATION")

    counts = manifest["counts"]
    checksums = manifest["checksums"]
    export_tables = manifest["tables"]

    v = duckdb.connect(str(NEW_DB), read_only=True)
    failures = []

    # Tables this version of the schema actually defines. The manifest lists
    # what the *snapshot* held, and a restore of an older archive legitimately
    # names tables since dropped — phase 2 skips those, so counting rows in
    # them here would fail the run for a table nobody expected to be present.
    present = {r[0] for r in v.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_type='BASE TABLE'"
    ).fetchall()}

    # ── Row count validation ──
    log("Row counts:")
    for t in export_tables:
        expected = counts.get(t, 0)
        if t not in present:
            log(f"  {t}: not in this version's schema — {expected:,} rows "
                f"in the archive were not restored", "WARN")
            continue
        if expected <= 0:
            continue
        try:
            actual = v.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            ok = actual == expected
            # Allow small difference from dedup
            if not ok and actual >= expected * 0.999:
                log(f"  {t}: {actual:,}/{expected:,} (minor dedup diff)", "WARN")
            elif ok:
                log(f"  {t}: {actual:,}", "OK")
            else:
                log(f"  {t}: {actual:,} expected {expected:,}", "ERROR")
                failures.append(f"{t} count mismatch")
        except Exception as e:
            log(f"  {t}: {e}", "ERROR")
            failures.append(f"{t} missing")

    # ── Date range ──
    log("\nDate range:")
    min_date, max_date = v.execute(
        "SELECT MIN(DATE(ordered_at)), MAX(DATE(ordered_at)) FROM orders"
    ).fetchone()
    expected_range = checksums["orders_date_range"]
    ok = str(min_date) == expected_range[0] and str(max_date) == expected_range[1]
    log(f"  {min_date} → {max_date} (expected {expected_range[0]} → {expected_range[1]})"
        f" {'✓' if ok else '✗'}")
    if not ok:
        failures.append("date range mismatch")

    # ── Revenue checksum ──
    log("\nRevenue checksum:")
    total_rev = float(v.execute(
        "SELECT COALESCE(ROUND(SUM(grand_total), 2), 0) FROM orders "
        "WHERE status_id NOT IN (6,7,10)"
    ).fetchone()[0])
    expected_rev = checksums["total_revenue"]
    rev_diff = abs(total_rev - expected_rev)
    ok = rev_diff < 1.0
    log(f"  ₴{total_rev:,.2f} (expected ₴{expected_rev:,.2f}, diff ₴{rev_diff:.2f})"
        f" {'✓' if ok else '✗'}")
    if not ok:
        failures.append(f"revenue mismatch: diff ₴{rev_diff:.2f}")

    # ── PK uniqueness ──
    log("\nPrimary key integrity:")
    pk_tables = ["orders", "products", "categories", "expenses", "buyers",
                 "managers", "order_products", "offers", "expense_types"]
    for t in pk_tables:
        try:
            pk_cols = [
                col
                for (cols,) in v.execute(
                    f"SELECT constraint_column_names FROM duckdb_constraints() "
                    f"WHERE table_name='{t}' AND constraint_type='PRIMARY KEY'"
                ).fetchall()
                for col in cols
            ]
            if not pk_cols:
                continue
            col = pk_cols[0]
            dupes = v.execute(
                f'SELECT COUNT(*) - COUNT(DISTINCT "{col}") FROM "{t}"'
            ).fetchone()[0]
            if dupes > 0:
                log(f"  {t}.{col}: {dupes} duplicates", "ERROR")
                failures.append(f"{t} PK duplicates")
            else:
                log(f"  {t}.{col}: unique", "OK")
        except Exception as e:
            log(f"  {t}: {e}", "WARN")

    # ── Write test (can we actually insert?) ──
    log("\nWrite test:")
    v.close()
    w = duckdb.connect(str(NEW_DB))
    try:
        w.execute("INSERT INTO sync_metadata VALUES ('_compaction_test', 'ok', CURRENT_TIMESTAMP)")
        w.execute("DELETE FROM sync_metadata WHERE key = '_compaction_test'")
        log("  INSERT + DELETE on sync_metadata: success", "OK")
    except Exception as e:
        log(f"  Write test FAILED: {e}", "ERROR")
        failures.append(f"write test failed: {e}")
    finally:
        w.execute("CHECKPOINT")
        w.close()

    v2 = duckdb.connect(str(NEW_DB), read_only=True)

    # ── Sequences, read back from the closed file ──
    # Read on a fresh read-only connection after the write test has
    # checkpointed and closed, so what is checked is what reached disk rather
    # than a position one process still holds in memory. Nothing checked this
    # until 2026-09-17: a file whose sequences handed out ids its tables
    # already held passed every line above and was swapped in.
    _ensure_app_importable()
    from core import duckdb_sequences
    from core.migrations import SEQUENCE_ID_COLUMNS

    log("\nSequence check (reopened read-only):")
    source_next = manifest.get("seq_values", {})
    mapped = set()
    for seq_name, table, col in SEQUENCE_ID_COLUMNS:
        mapped.add(seq_name)
        if table not in present:
            log(f"  sequence {seq_name}: {table} is not in this version's "
                f"schema — not checked", "WARN")
            continue
        try:
            # next_value can only err low, so a surprise in how the engine
            # reports a sequence refuses a good file rather than passing a bad one.
            nxt = duckdb_sequences.next_value(v2, seq_name)
            max_id = int(v2.execute(
                f'SELECT COALESCE(MAX("{col}"), 0) FROM "{table}"'
            ).fetchone()[0])
        except Exception as e:
            msg = f"sequence {seq_name}: could not be read back — {e}"
            log(f"  {msg}", "ERROR")
            failures.append(msg)
            continue
        saved = source_next.get(seq_name)
        if nxt <= max_id:
            msg = (f"sequence {seq_name}: hands out {nxt:,} but "
                   f"{table}.{col} MAX is {max_id:,}")
        elif saved is not None and nxt < int(saved):
            # Ids between the two were handed out by the source and may already
            # sit in Postgres, which ships these tables by `id > MAX`.
            msg = (f"sequence {seq_name}: hands out {nxt:,}, below the "
                   f"source's next value {int(saved):,}")
        else:
            log(f"  sequence {seq_name}: next {nxt:,} > {table}.{col} MAX "
                f"{max_id:,} (source next {_fmt_saved(saved)})", "OK")
            continue
        log(f"  {msg}", "ERROR")
        failures.append(msg)

    # A warning, not a failure: an unlisted sequence is a collision risk on its
    # own table only, and refusing the swap for it would turn one forgotten
    # entry into a compaction that aborts every Sunday. The schema-walk test in
    # tests/unit/test_compact_sequences.py fails CI on it instead.
    declared = sorted(r[0] for r in v2.execute(
        "SELECT sequence_name FROM duckdb_sequences()"
    ).fetchall())
    for seq_name in declared:
        if seq_name not in mapped:
            log(f"  sequence {seq_name}: not in SEQUENCE_ID_COLUMNS — nothing "
                f"checks it against the ids of its table", "WARN")

    # ── Derived tables exist (empty, for app startup) ──
    log("\nDerived table placeholders:")
    for t in sorted(DERIVED_TABLES):
        try:
            cnt = v2.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            log(f"  {t}: exists ({cnt} rows, will be rebuilt on startup)", "OK")
        except Exception:
            # DERIVED_TABLES also carries names kept only to keep them out of
            # the export while the production database still holds them — see
            # the comment on the set. Those have no DDL, so "startup will
            # create it" is not true of them and saying so sends the reader
            # looking for a bug in _init_schema.
            log(f"  {t}: absent — no DDL defines it; nothing will create it", "OK")

    db_size = v2.execute("SELECT database_size FROM pragma_database_size()").fetchone()[0]
    log(f"\nFinal DB size: {db_size}")
    v2.close()

    # ── No WAL left behind ──
    # atomic_swap renames the database file alone, and weekly_compact.sh
    # deletes analytics_clean.duckdb.wal as an artifact. Anything a WAL here
    # still held — a sequence position among it — would not be in the file
    # that goes live, and every check above read it back through that WAL.
    # Measured on 1.5.5: none exists after CHECKPOINT, after close, or while
    # the file is open read-only, so this fires only on a real change.
    wal = Path(str(NEW_DB) + ".wal")
    if wal.exists():
        msg = (f"clean DB left a WAL after close ({wal.stat().st_size:,} bytes) "
               f"that the swap would strand")
        log(f"  {msg}", "ERROR")
        failures.append(msg)

    if failures:
        log(f"\n{len(failures)} VALIDATION FAILURES:", "ERROR")
        for f in failures:
            log(f"  • {f}", "ERROR")
        log("\nDO NOT SWAP — investigate failures before proceeding", "ERROR")
        # One line that says why, last, for weekly_compact.sh to put in the
        # alert. The line above it names no failure.
        log(_fail_line(failures), "ERROR")
        sys.exit(1)

    log("\nALL VALIDATIONS PASSED", "OK")


# ─── Phase 4: Atomic swap (opt-in) ───────────────────────────────────────────


def atomic_swap(data_dir: Path) -> None:
    """Move the freshly-built clean DB into the canonical position.

    Sequence (all on a single mounted filesystem — rename is atomic):
        1. analytics.duckdb       → analytics.duckdb.old   (preserve rollback)
        2. rm analytics.duckdb.wal                          (stale, was for old DB)
        3. analytics_clean.duckdb → analytics.duckdb        (canonical)

    Refuses to clobber an existing .old backup — that file is the only
    rollback path if the new DB later turns out to be bad. The caller
    (operator or weekly_compact.sh) must clean up the previous .old
    before invoking compact again.

    This function is unit-tested with a tmpdir and stub files — see
    tests/unit/test_compact_atomic_swap.py.
    """
    source = data_dir / "analytics.duckdb"
    backup = data_dir / "analytics.duckdb.old"
    wal = data_dir / "analytics.duckdb.wal"
    clean = data_dir / "analytics_clean.duckdb"

    if not clean.exists():
        raise RuntimeError(
            f"atomic_swap precondition: {clean.name} missing — "
            f"phase 2 did not produce a clean DB."
        )
    if backup.exists():
        raise RuntimeError(
            f"atomic_swap precondition: {backup.name} already exists. "
            f"Previous compact's rollback file is still on disk. "
            f"Remove it explicitly before swapping again."
        )

    if source.exists():
        source.rename(backup)
    if wal.exists():
        wal.unlink()
    clean.rename(source)


def phase4_swap_if_enabled() -> None:
    """Perform the file swap when COMPACT_AUTO_SWAP=1 is set.

    Without the env var, prints the manual instructions (legacy behaviour).
    With it, completes the swap atomically inside the script so an
    operator's SSH disconnect after phase 3 cannot leave services pointed
    at the old DB. Service restart is still the wrapper's job (sidecar
    has no Docker access).
    """
    auto_swap = os.getenv("COMPACT_AUTO_SWAP", "").strip().lower() in {"1", "true", "yes"}

    if not auto_swap:
        section("COMPACTION COMPLETE — READY FOR SWAP")
        log("Set COMPACT_AUTO_SWAP=1 to perform the swap automatically.\n")
        log("Manual commands to run on the host:")
        log("  mv /opt/key-api-bot/data/analytics.duckdb /opt/key-api-bot/data/analytics.duckdb.old")
        log("  rm -f /opt/key-api-bot/data/analytics.duckdb.wal")
        log("  mv /opt/key-api-bot/data/analytics_clean.duckdb /opt/key-api-bot/data/analytics.duckdb")
        log("  cd /opt/key-api-bot && docker compose up -d")
        return

    section("PHASE 4: ATOMIC SWAP (COMPACT_AUTO_SWAP=1)")
    try:
        atomic_swap(DATA_DIR)
    except RuntimeError as e:
        log(f"Swap aborted: {e}", "ERROR")
        sys.exit(1)

    log("Swap complete:", "OK")
    log(f"  • analytics.duckdb    = {NEW_DB.name} (new, canonical)")
    log(f"  • analytics.duckdb.old = previous DB, kept as rollback")
    log("Service restart is the wrapper's job — sidecar has no docker access.")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    section("DuckDB Compaction Tool")
    log(f"Source: {SOURCE_DB}")
    log(f"Target: {NEW_DB}")
    log(f"DuckDB: {duckdb.__version__}")
    log(f"Memory limit: {MEM_LIMIT}")

    # Resolve the application code before anything else. Phases 2 and 3 import
    # it after phase 1 has spent minutes exporting with the services stopped;
    # a path that cannot resolve should cost seconds and say so first.
    _ensure_app_importable()
    import core.duckdb_sequences  # noqa: F401
    import core.duckdb_store  # noqa: F401
    import core.migrations  # noqa: F401
    log(f"Application code: {APP_ROOT}")

    preflight()
    manifest = phase1_export()
    phase2_import(manifest)
    phase3_validate(manifest)
    phase4_swap_if_enabled()


if __name__ == "__main__":
    main()
