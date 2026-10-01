"""The DuckDB half of a shadow chain's write (OD-02 (c)).

A shadow chain (`core.write_chains.is_shadow`) writes Postgres first and then
hands DuckDB the same values, so DuckDB stays a copy — fed from the other side
— and the daily comparison keeps comparing it. This module is that second
half, and it has exactly one rule set:

- **After the Postgres commit, never before.** DuckDB ⊆ Postgres then holds by
  construction, which is what lets `compare_shadow` read a row only DuckDB
  holds as a defect rather than as a write in flight. A Postgres failure
  raises in the chain's writer, so this is never reached and DuckDB is not
  written.
- **A DuckDB failure is counted, never raised.** Postgres is the writer of
  record and already holds the row; raising here would turn a delivered
  weekly report into a job that "failed" and sends again tomorrow. The
  failure is logged at ERROR with its class, counted per chain, published in
  `/api/health` as `write_chains.<chain>.shadow_failures` (class only — that
  endpoint is public), and found the next morning by the comparison as
  `shadow_missing_in_duckdb`.
- **One transaction, rolled back on any error.** A failed statement inside an
  open DuckDB transaction poisons the shared connection
  (`core.data_quality.persist_run` learned it), so the ROLLBACK is not
  optional, and a run's parent never lands without its children.
- **Never while the caller holds `store.connection()`.** The store lock is not
  reentrant (`core/weekly_report.py` names the hang), so a caller holding it
  would wait on itself. `tests/unit/test_shadow_state.py` walks every caller.

It never uses the phrase `core/read_fallback.py` logs. A shadow is not a
fallback — the soak greps for that phrase, and a log line here carrying it
would read as a read port failing (`tests/unit/test_shadow_state.py`).
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

_lock = threading.Lock()
# `{chain: {"count", "last_at", "last_error_class"}}` since this process
# started — `read_fallback.counts()`'s shape. Process-local on purpose: the
# comparison is the durable record, this is the "when" a reader of the health
# block needs to go and look at the log.
_failures: Dict[str, Dict[str, Any]] = {}


def _record(chain: str, exc: BaseException) -> None:
    with _lock:
        entry = _failures.setdefault(
            chain, {"count": 0, "last_at": None, "last_error_class": None})
        entry["count"] += 1
        entry["last_at"] = datetime.now(timezone.utc).isoformat()
        entry["last_error_class"] = type(exc).__name__


def failures() -> Dict[str, Dict[str, Any]]:
    """A copy of the per-chain failure counters. No I/O, never raises."""
    with _lock:
        return {chain: dict(entry) for chain, entry in _failures.items()}


def failures_of(chain: str) -> Dict[str, Any]:
    """One chain's counters, zeroes when it never failed."""
    with _lock:
        entry = _failures.get(chain)
        return dict(entry) if entry else {
            "count": 0, "last_at": None, "last_error_class": None}


def reset() -> None:
    """For tests: forget every counter."""
    with _lock:
        _failures.clear()


async def into_duckdb(store, chain: str, write: Callable[[Any], Any]) -> bool:
    """Run `write(conn)` inside one DuckDB transaction; True when it committed.

    Called only after the chain's Postgres transaction has committed, with
    `write` built from the values Postgres was handed — the chain's existing
    DuckDB statements, so each table has one DuckDB spelling. Never raises an
    `Exception`; a cancellation still goes through.
    """
    try:
        async with store.connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                write(conn)
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except Exception:  # noqa: BLE001 — the first error is the one to report
                    pass
                raise
    except Exception as exc:  # noqa: BLE001 — counted and published, never raised
        _record(chain, exc)
        logger.error(
            "shadow write failed: %s: %s. Postgres, the writer of record, holds "
            "the row; DuckDB does not until the copy-back carries it, and the "
            "07:30 comparison will report it as shadow_missing_in_duckdb.",
            chain, type(exc).__name__, exc_info=True,
        )
        return False
    return True
