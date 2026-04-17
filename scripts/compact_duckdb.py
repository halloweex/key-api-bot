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
import duckdb
from pathlib import Path

DATA_DIR = Path("/app/data")
SOURCE_DB = DATA_DIR / "analytics.duckdb"
EXPORT_DIR = DATA_DIR / "export_parquet"
NEW_DB = DATA_DIR / "analytics_clean.duckdb"
MANIFEST_PATH = EXPORT_DIR / "_manifest.json"

DERIVED_TABLES = frozenset({
    "silver_orders", "silver_order_utm",
    "gold_daily_revenue", "gold_daily_products",
    "gold_daily_traffic", "gold_product_pairs",
})

MEM_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")

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

    required_gb = source_size_gb * 1.5
    if free_gb < required_gb:
        log(f"Need at least {required_gb:.1f} GB free. "
            f"Delete old backups to free space.", "ERROR")
        sys.exit(1)

    log(f"Disk OK (need ~{required_gb:.1f} GB, have {free_gb:.1f} GB)", "OK")


# ─── Phase 1: Export ─────────────────────────────────────────────────────────

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
    seq_values = {}
    log("\nSequences:")
    seq_rows = src.execute(
        "SELECT sequence_name, last_value FROM duckdb_sequences()"
    ).fetchall()
    for seq_name, last_val in seq_rows:
        seq_values[seq_name] = last_val
        log(f"  {seq_name} = {last_val}")

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
        "seq_values": seq_values,
        "checksums": checksums,
        "duckdb_version": duckdb.__version__,
    }
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    log(f"Manifest saved: {MANIFEST_PATH}")

    return manifest


# ─── Phase 2: Import ─────────────────────────────────────────────────────────

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
    sys.path.insert(0, "/app")
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
        pq_cols = set(r[0] for r in conn.execute(
            f"SELECT name FROM parquet_schema('{parquet_path}')"
        ).fetchall())
        tbl_cols = set(r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{t}'"
        ).fetchall())
        missing = pq_cols - tbl_cols
        if missing:
            log(f"  {t}: source columns not in target: {missing}", "WARN")
            log(f"    These columns will be DROPPED during import", "WARN")
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
                pk_cols = [r[0] for r in conn.execute(
                    f"SELECT column_name FROM duckdb_constraints() "
                    f"WHERE table_name='{t}' AND constraint_type='PRIMARY KEY'"
                ).fetchall()]
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
        sys.exit(1)

    # ── Restore sequences ──
    log("\nRestoring sequences:")
    seq_table_map = {
        "seq_stock_movements_id": ("stock_movements", "id"),
        "seq_buyer_contacts_id": ("buyer_contacts", "id"),
        "seq_manual_expenses_id": ("manual_expenses", "id"),
        "seq_report_history_id": ("report_history", "id"),
        "warehouse_refresh_seq": ("warehouse_refreshes", "id"),
        "reconciliation_seq": ("reconciliation_log", "id"),
    }
    for seq_name, saved_val in seq_values.items():
        try:
            restart_val = max((saved_val or 0) + 1, 1)
            if seq_name in seq_table_map:
                table, col = seq_table_map[seq_name]
                try:
                    max_id = conn.execute(
                        f'SELECT COALESCE(MAX("{col}"), 0) FROM "{table}"'
                    ).fetchone()[0]
                    restart_val = max(restart_val, max_id + 1)
                except Exception:
                    pass
            conn.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH {restart_val}")
            log(f"  {seq_name}: restart at {restart_val}", "OK")
        except Exception as e:
            log(f"  {seq_name}: {e}", "WARN")

    # ── Checkpoint ──
    log("\nFlushing WAL...")
    conn.execute("CHECKPOINT")

    new_size_mb = NEW_DB.stat().st_size / (1024**2)
    source_size_mb = SOURCE_DB.stat().st_size / (1024**2)
    reduction = 100 * (1 - new_size_mb / source_size_mb) if source_size_mb > 0 else 0
    log(f"New DB: {new_size_mb:.1f} MB (was {source_size_mb:.0f} MB, {reduction:.0f}% smaller)", "OK")

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

    # ── Row count validation ──
    log("Row counts:")
    for t in export_tables:
        expected = counts.get(t, 0)
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
            pk_cols = [r[0] for r in v.execute(
                f"SELECT column_name FROM duckdb_constraints() "
                f"WHERE table_name='{t}' AND constraint_type='PRIMARY KEY'"
            ).fetchall()]
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

    # ── Derived tables exist (empty, for app startup) ──
    log("\nDerived table placeholders:")
    v2 = duckdb.connect(str(NEW_DB), read_only=True)
    for t in sorted(DERIVED_TABLES):
        try:
            cnt = v2.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            log(f"  {t}: exists ({cnt} rows, will be rebuilt on startup)", "OK")
        except Exception as e:
            log(f"  {t}: missing — {e}", "WARN")
            log(f"    Will be created by _init_schema() on app startup")

    db_size = v2.execute("SELECT database_size FROM pragma_database_size()").fetchone()[0]
    log(f"\nFinal DB size: {db_size}")
    v2.close()

    if failures:
        log(f"\n{len(failures)} VALIDATION FAILURES:", "ERROR")
        for f in failures:
            log(f"  • {f}", "ERROR")
        log("\nDO NOT SWAP — investigate failures before proceeding", "ERROR")
        sys.exit(1)

    log("\nALL VALIDATIONS PASSED", "OK")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    section("DuckDB Compaction Tool")
    log(f"Source: {SOURCE_DB}")
    log(f"Target: {NEW_DB}")
    log(f"DuckDB: {duckdb.__version__}")
    log(f"Memory limit: {MEM_LIMIT}")

    preflight()
    manifest = phase1_export()
    phase2_import(manifest)
    phase3_validate(manifest)

    section("COMPACTION COMPLETE — READY FOR SWAP")
    log("Run these commands on the host:\n")
    log("  # 1. Atomic file swap")
    log("  mv /opt/key-api-bot/data/analytics.duckdb /opt/key-api-bot/data/analytics.duckdb.old")
    log("  rm -f /opt/key-api-bot/data/analytics.duckdb.wal")
    log("  mv /opt/key-api-bot/data/analytics_clean.duckdb /opt/key-api-bot/data/analytics.duckdb")
    log("")
    log("  # 2. Start web (rebuilds Silver/Gold on first sync)")
    log("  cd /opt/key-api-bot && docker compose up -d web")
    log("")
    log("  # 3. Verify dashboard at https://ksanalytics.duckdns.org")
    log("")
    log("  # 4. Cleanup (after dashboard confirmed working)")
    log("  rm -rf /opt/key-api-bot/data/analytics.duckdb.old")
    log("  rm -rf /opt/key-api-bot/data/export_parquet")


if __name__ == "__main__":
    main()
