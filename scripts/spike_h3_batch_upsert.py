"""H3 Phase 0 spike — prove DuckDB 1.5.2 handles atomic batch DELETE+INSERT on orders.

Four tests:
  1. Baseline       — 500 fresh rows, batch DELETE+INSERT in one txn.
  2. Hot keys       — churn 10 keys via per-row autocommit, then batch DELETE+INSERT.
  3. Interleaved    — two concurrent writers on same keys (violates single-writer invariant).
  4. Serialized     — per-row + batch interleaved in TIME, single writer at any moment (matches H3).

Must run inside .venv-spike (duckdb==1.5.2), NOT system python (1.4.3).

Usage:
    .venv-spike/bin/python scripts/spike_h3_batch_upsert.py
"""
from __future__ import annotations

import random
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REQUIRED_VERSION = "1.5.2"


ORDERS_DDL = """
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL,
    grand_total DECIMAL(12, 2) NOT NULL,
    ordered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    buyer_id INTEGER,
    manager_id INTEGER,
    manager_comment TEXT,
    promocode VARCHAR,
    synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    update_count INTEGER DEFAULT 0
);
CREATE INDEX idx_orders_ordered_at ON orders(ordered_at);
CREATE INDEX idx_orders_status ON orders(status_id);
"""


def make_row(order_id: int, bump: int = 0) -> tuple:
    now = datetime.now(timezone.utc)
    return (
        order_id,
        random.choice([1, 2, 4]),                       # source_id
        random.choice([1, 2, 3, 19, 22, 21, 23]),       # status_id
        round(random.uniform(100.0, 5000.0) + bump, 2), # grand_total
        now, now, now,
        random.randint(1000, 9999),                     # buyer_id
        random.choice([4, 15, 16, 22]),                 # manager_id
        f"spike_row v{bump}",
        None,
        now, now, bump,
    )


ROW_COLS = (
    "id, source_id, status_id, grand_total, ordered_at, created_at, updated_at, "
    "buyer_id, manager_id, manager_comment, promocode, synced_at, first_seen_at, update_count"
)
ROW_PLACEHOLDERS = "(" + ",".join(["?"] * 14) + ")"


@dataclass
class TestResult:
    name: str
    passed: bool
    duration_s: float
    rows_written: int = 0
    errors: list[str] = field(default_factory=list)
    extra: dict[str, object] = field(default_factory=dict)

    def format(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        header = f"[{status}] {self.name}  ({self.duration_s:.3f}s, rows={self.rows_written})"
        lines = [header]
        for k, v in self.extra.items():
            lines.append(f"    {k}: {v}")
        if self.errors:
            lines.append(f"    errors ({len(self.errors)}):")
            for e in self.errors[:5]:
                lines.append(f"      - {e}")
            if len(self.errors) > 5:
                lines.append(f"      - ... +{len(self.errors) - 5} more")
        return "\n".join(lines)


def fresh_db() -> tuple[duckdb.DuckDBPyConnection, Path]:
    path = Path(tempfile.mkdtemp(prefix="spike_h3_")) / "spike.duckdb"
    con = duckdb.connect(str(path))
    con.execute(ORDERS_DDL)
    return con, path


def batch_delete_insert(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """The exact pattern promotion job will use: atomic batch in single txn."""
    ids = [r[0] for r in rows]
    con.execute("BEGIN")
    try:
        con.execute("DELETE FROM orders WHERE id = ANY(?)", [ids])
        con.executemany(
            f"INSERT INTO orders ({ROW_COLS}) VALUES {ROW_PLACEHOLDERS}",
            rows,
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def test_1_baseline() -> TestResult:
    con, _ = fresh_db()
    t0 = time.perf_counter()
    errors: list[str] = []
    try:
        rows = [make_row(i) for i in range(10_000, 10_500)]
        batch_delete_insert(con, rows)
        (n,) = con.execute("SELECT COUNT(*) FROM orders").fetchone()
        passed = n == 500
        return TestResult(
            name="1. Baseline: 500 fresh rows batch DELETE+INSERT",
            passed=passed,
            duration_s=time.perf_counter() - t0,
            rows_written=n,
            extra={"orders in table": n},
        )
    except Exception as e:
        errors.append(repr(e))
        return TestResult(
            name="1. Baseline: 500 fresh rows batch DELETE+INSERT",
            passed=False,
            duration_s=time.perf_counter() - t0,
            errors=errors,
        )
    finally:
        con.close()


def test_2_hot_keys() -> TestResult:
    con, _ = fresh_db()
    t0 = time.perf_counter()
    hot_ids = list(range(20_000, 20_010))
    churn_count = 100
    errors: list[str] = []

    try:
        # Seed hot keys
        for i in hot_ids:
            con.execute(
                f"INSERT INTO orders ({ROW_COLS}) VALUES {ROW_PLACEHOLDERS}",
                make_row(i, 0),
            )

        # Churn each hot key 100x via autocommit per-row UPDATE — the exact pattern
        # that caused MVCC poisoning in prod.
        churn_errors = 0
        for bump in range(1, churn_count + 1):
            for i in hot_ids:
                try:
                    con.execute(
                        "UPDATE orders SET grand_total=?, update_count=?, "
                        "manager_comment=?, synced_at=CURRENT_TIMESTAMP "
                        "WHERE id=?",
                        [round(random.uniform(100.0, 5000.0) + bump, 2), bump, f"churn v{bump}", i],
                    )
                except Exception as e:
                    churn_errors += 1
                    errors.append(f"churn bump={bump} id={i}: {e!r}")

        # Now the real test: batch DELETE+INSERT 500 rows including the 10 hot keys
        batch_rows = [make_row(i, 999) for i in hot_ids]
        batch_rows += [make_row(i) for i in range(25_000, 25_490)]
        batch_delete_insert(con, batch_rows)

        (n,) = con.execute("SELECT COUNT(*) FROM orders").fetchone()
        (hot_n,) = con.execute(
            "SELECT COUNT(*) FROM orders WHERE id = ANY(?)", [hot_ids]
        ).fetchone()
        passed = n == 500 and hot_n == 10 and not errors
        return TestResult(
            name="2. Hot keys: churn 10 keys 100x, then batch DELETE+INSERT",
            passed=passed,
            duration_s=time.perf_counter() - t0,
            rows_written=n,
            errors=errors,
            extra={
                "churn updates attempted": len(hot_ids) * churn_count,
                "churn errors": churn_errors,
                "hot keys present after batch": hot_n,
                "total orders after batch": n,
            },
        )
    except Exception as e:
        errors.append(repr(e))
        return TestResult(
            name="2. Hot keys: churn 10 keys 100x, then batch DELETE+INSERT",
            passed=False,
            duration_s=time.perf_counter() - t0,
            errors=errors,
        )
    finally:
        con.close()


def test_3_interleaved(duration_s: float = 30.0) -> TestResult:
    """Two threads on separate connections to same file:
    - Writer A: rapid per-row autocommit upserts on a hot-key pool (the pre-H3 pattern).
    - Writer B: every 2s, batch DELETE+INSERT 500 rows incl. half the hot-key pool.
    Fails if either path sees write-write conflict or any commit error.
    """
    tmp = Path(tempfile.mkdtemp(prefix="spike_h3_interleave_")) / "spike.duckdb"
    con = duckdb.connect(str(tmp))
    con.execute(ORDERS_DDL)
    con.close()

    hot_pool = list(range(30_000, 30_050))  # 50 hot ids
    stop = threading.Event()
    errors: list[str] = []
    errors_lock = threading.Lock()
    counters = {"per_row_upserts": 0, "batch_runs": 0}
    counters_lock = threading.Lock()

    def record_error(tag: str, exc: Exception) -> None:
        with errors_lock:
            errors.append(f"{tag}: {exc!r}")

    def writer_per_row() -> None:
        con_local = duckdb.connect(str(tmp))
        try:
            bump = 0
            while not stop.is_set():
                bump += 1
                for oid in hot_pool:
                    try:
                        # SELECT + INSERT/UPDATE pattern used by prod upsert_orders
                        (exists,) = con_local.execute(
                            "SELECT COUNT(*) FROM orders WHERE id=?", [oid]
                        ).fetchone()
                        if exists:
                            con_local.execute(
                                "UPDATE orders SET grand_total=?, update_count=?, "
                                "synced_at=CURRENT_TIMESTAMP WHERE id=?",
                                [round(random.uniform(100.0, 5000.0) + bump, 2), bump, oid],
                            )
                        else:
                            con_local.execute(
                                f"INSERT INTO orders ({ROW_COLS}) VALUES {ROW_PLACEHOLDERS}",
                                make_row(oid, bump),
                            )
                        with counters_lock:
                            counters["per_row_upserts"] += 1
                    except Exception as e:
                        record_error("per_row", e)
                    if stop.is_set():
                        break
                time.sleep(0.01)
        finally:
            con_local.close()

    def writer_batch() -> None:
        con_local = duckdb.connect(str(tmp))
        try:
            next_run = time.monotonic()
            while not stop.is_set():
                now = time.monotonic()
                if now < next_run:
                    time.sleep(min(0.1, next_run - now))
                    continue
                next_run = now + 2.0
                try:
                    # half the batch overlaps with hot pool, half fresh
                    sample = random.sample(hot_pool, 25)
                    fresh_base = 40_000 + counters["batch_runs"] * 1000
                    rows = [make_row(i, 999) for i in sample]
                    rows += [make_row(i) for i in range(fresh_base, fresh_base + 475)]
                    batch_delete_insert(con_local, rows)
                    with counters_lock:
                        counters["batch_runs"] += 1
                except Exception as e:
                    record_error("batch", e)
        finally:
            con_local.close()

    t0 = time.perf_counter()
    t_per_row = threading.Thread(target=writer_per_row, daemon=True)
    t_batch = threading.Thread(target=writer_batch, daemon=True)
    t_per_row.start()
    t_batch.start()

    time.sleep(duration_s)
    stop.set()
    t_per_row.join(timeout=10)
    t_batch.join(timeout=10)

    con_check = duckdb.connect(str(tmp))
    try:
        (n,) = con_check.execute("SELECT COUNT(*) FROM orders").fetchone()
    finally:
        con_check.close()

    passed = not errors and counters["batch_runs"] >= 5 and counters["per_row_upserts"] > 0
    return TestResult(
        name=f"3. Interleaved per-row + batch promotion ({duration_s:.0f}s)",
        passed=passed,
        duration_s=time.perf_counter() - t0,
        rows_written=n,
        errors=errors,
        extra={
            "per_row upserts completed": counters["per_row_upserts"],
            "batch runs completed": counters["batch_runs"],
            "total orders after run": n,
            "per_row errors": sum(1 for e in errors if e.startswith("per_row")),
            "batch errors": sum(1 for e in errors if e.startswith("batch")),
        },
    )


def test_4_serialized(duration_s: float = 30.0) -> TestResult:
    """Single writer at any moment, but per-row and batch phases interleave in TIME.
    This matches the H3 invariant: only promotion writes to orders, and it alternates
    with inbound bronze INSERTs (which don't touch orders). We simulate by using a
    single connection, alternating 1.9s of per-row upserts + one batch DELETE+INSERT
    every 2s, for `duration_s`. Expect 0 errors.
    """
    con, _ = fresh_db()
    hot_pool = list(range(50_000, 50_050))
    errors: list[str] = []
    counters = {"per_row": 0, "batch_runs": 0}
    deadline = time.monotonic() + duration_s
    t0 = time.perf_counter()
    try:
        bump = 0
        batch_cycle = 0
        while time.monotonic() < deadline:
            bump += 1
            phase_end = min(time.monotonic() + 1.9, deadline)
            while time.monotonic() < phase_end:
                for oid in hot_pool:
                    try:
                        (exists,) = con.execute(
                            "SELECT COUNT(*) FROM orders WHERE id=?", [oid]
                        ).fetchone()
                        if exists:
                            con.execute(
                                "UPDATE orders SET grand_total=?, update_count=?, "
                                "synced_at=CURRENT_TIMESTAMP WHERE id=?",
                                [round(random.uniform(100.0, 5000.0) + bump, 2), bump, oid],
                            )
                        else:
                            con.execute(
                                f"INSERT INTO orders ({ROW_COLS}) VALUES {ROW_PLACEHOLDERS}",
                                make_row(oid, bump),
                            )
                        counters["per_row"] += 1
                    except Exception as e:
                        errors.append(f"per_row: {e!r}")
                    if time.monotonic() >= phase_end:
                        break

            if time.monotonic() >= deadline:
                break
            try:
                sample = random.sample(hot_pool, 25)
                fresh_base = 60_000 + batch_cycle * 1000
                rows = [make_row(i, 999) for i in sample]
                rows += [make_row(i) for i in range(fresh_base, fresh_base + 475)]
                batch_delete_insert(con, rows)
                counters["batch_runs"] += 1
                batch_cycle += 1
            except Exception as e:
                errors.append(f"batch: {e!r}")

        (n,) = con.execute("SELECT COUNT(*) FROM orders").fetchone()
        passed = not errors and counters["batch_runs"] >= 5 and counters["per_row"] > 0
        return TestResult(
            name=f"4. Serialized per-row + batch interleaved in time ({duration_s:.0f}s)",
            passed=passed,
            duration_s=time.perf_counter() - t0,
            rows_written=n,
            errors=errors,
            extra={
                "per_row upserts": counters["per_row"],
                "batch runs": counters["batch_runs"],
                "total orders": n,
            },
        )
    finally:
        con.close()


def main() -> int:
    if duckdb.__version__ != REQUIRED_VERSION:
        print(
            f"ERROR: duckdb {duckdb.__version__} found, need {REQUIRED_VERSION}. "
            "Run under .venv-spike (see header comment).",
            file=sys.stderr,
        )
        return 2

    print(f"DuckDB {duckdb.__version__}\n")
    results: list[TestResult] = []
    results.append(test_1_baseline())
    print(results[-1].format(), "\n", flush=True)
    results.append(test_2_hot_keys())
    print(results[-1].format(), "\n", flush=True)
    results.append(test_3_interleaved())
    print(results[-1].format(), "\n", flush=True)
    results.append(test_4_serialized())
    print(results[-1].format(), "\n", flush=True)

    print("=" * 70)
    # Tests 1, 2, 4 gate the H3 UPSERT path (single-writer invariant = load-bearing).
    # Test 3 intentionally violates the invariant to size the risk of accidental concurrency.
    gating = [r for r in results if not r.name.startswith("3.")]
    gating_pass = all(r.passed for r in gating)
    t3 = next(r for r in results if r.name.startswith("3."))
    print("GATING tests (1, 2, 4):", "PASS" if gating_pass else "FAIL")
    print(f"Test 3 (invariant violation): {'clean' if t3.passed else 'conflicts observed'} "
          f"— {len(t3.errors)} errors out of {t3.extra.get('batch runs completed', '?')} batch runs")
    print("VERDICT:", "PASS — proceed to Phase 1 UPSERT path (enforce single-writer strictly)"
          if gating_pass else "FAIL — pivot to CTAS-rebuild")
    return 0 if gating_pass else 1


if __name__ == "__main__":
    sys.exit(main())
