"""Chain 9: the data-quality journal, written to Postgres and shadowed in DuckDB.

`data_quality_runs` and its two children are the record every check leaves
behind — what the 09:00 digest reads, what `/api/health` turns into the layer
ages the canary pages on, and what `/api/health/data-quality` publishes.
Nothing re-derives a run: it is what a check found at the moment it ran.

THE SHAPE: SHADOW, NOT FROZEN (owner decision OD-02 (c), 2026-10-01)

Chains 1, 6a, 7a and 8 freeze DuckDB when they move. This chain does not:
under `KS_WRITE_DQ_JOURNAL=postgres` a run is committed to Postgres first and
DuckDB is handed the very same rows right after (`core/shadow_writes.py`), so
DuckDB's journal stays whole and the daily comparison keeps comparing the two
— facing Postgres→DuckDB (`core.mirror_reconciliation.compare_shadow`). The
hourly copy stands down, as for every moved chain: its full replace out of
DuckDB would delete each run whose shadow failed. `CHAIN_SHADOW` says so to
`core.write_chains`.

Everything else is the frozen chains' machinery unchanged. The first Postgres
write latches the chain (OD-19 (a)), because a run whose shadow failed exists
in Postgres alone and the flag is then no more a rollback than chain 8's; the
way back is `scripts/chain_copy_back.py dq_journal`.

THE ALLOCATOR: MAX + 1 UNDER AN ADVISORY LOCK, NO REVISION

`app.data_quality_runs.run_id` has no sequence (revision 0028 carried
DuckDB's values). Under this chain Postgres issues the id and DuckDB is handed
it. `MAX(run_id) + 1` is safe here and only here because the transaction-
scoped advisory lock serialises the allocators, READ COMMITTED's next
statement after the lock sees the previous holder's commit, nothing deletes
from these tables in Postgres once the copy stands down, and the rate is ~10
runs a day. A revision with a sequence is the alternative the design names;
`core.chain_transfer` already derives that sequence's name and would floor
from it.

THE DIGEST'S BEAT

`dq_digest_last_sent` moves with the chain (`CHAIN_MARKER_KEYS`): under
Postgres it lives in `meta.chain_watermarks`, which nothing copies out of
DuckDB — the hourly full replace of `app.sync_metadata` would wipe it.
`core.dq_journal` reads it there first and inherits DuckDB's value while the
key is absent, so the first digest after a flip does not restate everything.

READS FOLLOW THE WRITER

While this chain writes Postgres (`reads_postgres`), every reader of the
journal — the digest, the layer ages, the data-quality endpoint, the startup
catch-up — reads Postgres, through `core.dq_journal`, and never falls back to
DuckDB: a fallback would make an unreachable Postgres read as "journal fine"
at the moment the journal's writer is the thing that failed.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_DQ_JOURNAL"

# One spelling for the registry, the local marker and the health block.
CHAIN = "pg_dq_journal_write"

# One unit: a run and its findings land in one transaction, so ownership of
# one is ownership of all three.
CHAIN_TABLES: Tuple[str, ...] = (
    "app.data_quality_runs", "app.data_quality_issues", "app.data_quality_diffs",
)

# The third registry state (`core.write_chains.is_shadow`): DuckDB still
# receives every row after the Postgres commit.
CHAIN_SHADOW = True

# Not a sync watermark — `_freshness_check` would judge a `last_sync_*` key as
# a stalled sync — but kept in `meta.chain_watermarks` for the same reason,
# and carried back by the copy-back with the chain (`_carried_keys`).
DIGEST_MARKER_KEY = "dq_digest_last_sent"
CHAIN_MARKER_KEYS: Tuple[str, ...] = (DIGEST_MARKER_KEY,)

# The run-id allocator's lock, beside chain 1's `0x6B73_0000_0000_0001`.
# `tests/unit/test_dq_journal.py` walks `core/` for every `*_LOCK_KEY` and
# fails on a reuse: two allocators sharing a key would serialise for nothing,
# and one sharing chain 1's would block a stock rebuild behind a DQ run.
RUN_ID_LOCK_KEY = 0x6B73_0000_0000_0009


def env_writes_postgres() -> bool:
    """What `KS_WRITE_DQ_JOURNAL` alone says. An unrecognised value raises —
    chain 8's rule (OD-18 (a)): it stops this chain and nothing else."""
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def writes_postgres() -> bool:
    """Whether this chain writes Postgres — the latch outranks the flag."""
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres()


def reads_postgres() -> bool:
    """Whether a read of the journal asks Postgres. `writes_postgres()`, but a
    flag nobody can read on a chain that never wrote here answers False rather
    than raising: the writer then refuses in both stores, so neither moves,
    and DuckDB holds the whole journal — chain 7a's `reads_postgres` reason.
    Raising would take the layer ages, and with them every canary verdict,
    down over a variable about writes; `write_chain_flag_invalid` pages it."""
    if chain_latch.latched(CHAIN):
        return True
    try:
        return env_writes_postgres()
    except RuntimeError:
        return False


def _latch() -> str:
    """The local half of the latch — inside `pool.acquire()`, never before it
    (`core/pg_expenses_write.py._latch` has the reasoning)."""
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


# ─── Writers ─────────────────────────────────────────────────────────────────


async def persist_run(values, issues: Sequence[Any], discrepancies: Sequence[Any]):
    """Write one run and its findings; returns the `JournalRows` committed.

    Every row is built — and a naive timestamp refused — before the pool is
    asked for anything, so an input Postgres or DuckDB would read two ways
    never latches the chain (chain 7a's `_fits` reason). Inside the one
    transaction: the owner rows, the allocator's lock, the id, then the three
    INSERTs. The caller hands the same rows to DuckDB after this returns.
    """
    from core.data_quality import journal_rows

    journal_rows(0, values, issues, discrepancies)      # refuses before the latch
    pool = await _pool()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.execute("SELECT pg_advisory_xact_lock($1)", RUN_ID_LOCK_KEY)
            run_id = await conn.fetchval(
                "SELECT COALESCE(MAX(run_id), 0) + 1 FROM app.data_quality_runs")
            rows = journal_rows(int(run_id), values, issues, discrepancies)
            await conn.execute(
                "INSERT INTO app.data_quality_runs ("
                "run_id, started_at, ended_at, as_of, window_start, window_end, "
                "layer, status, integrity_issues_count, discrepancies_count, "
                "critical_count, warn_count, api_calls_used, duration_ms, "
                "error_message) VALUES "
                "($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                *rows.run,
            )
            if rows.issues:
                await conn.executemany(
                    "INSERT INTO app.data_quality_issues "
                    "(run_id, check_name, table_name, severity, count, "
                    "sample_ids, description) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    rows.issues,
                )
            if rows.diffs:
                await conn.executemany(
                    "INSERT INTO app.data_quality_diffs "
                    "(run_id, month, source_id, diff_class, field, dk_value, "
                    "kc_value, severity, order_ids) VALUES "
                    "($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    rows.diffs,
                )
    return rows


async def set_digest_marker(value: str) -> None:
    """Move the digest's beat in Postgres — after a delivered digest only.

    A write of the chain's state like any other: it latches and claims, so a
    chain whose first Postgres write is a beat is still a chain that owns its
    journal from then on.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"digest marker {value!r} is not a timestamp string")
    pool = await _pool()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ($1, $2, now()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "
                "updated_at = EXCLUDED.updated_at",
                DIGEST_MARKER_KEY, value,
            )


# ─── Readers ─────────────────────────────────────────────────────────────────
#
# `reading()` is the one that takes a connection (named a reader in
# `tests/unit/test_chain_latch.py._READERS` and held there to writing
# nothing). The rest are handed that connection and run the journal's shared
# statements (`core.data_quality.journal_sql`), shaped by the same functions
# DuckDB's readers use — so one reader cannot drift from the other.


@asynccontextmanager
async def reading() -> AsyncIterator[Any]:
    """A pooled connection to read the journal on. Reads only."""
    pool = await _pool()
    async with pool.acquire() as conn:
        yield conn


_ZONE: Optional[Any] = None


def _duckdb_zone():
    """The zone DuckDB renders a TIMESTAMPTZ in — its session `TimeZone`,
    which it takes from the process's at start and never sets here. Read once
    from an in-memory connection rather than assumed to be the libc zone:
    the two part company in a process whose `TZ` changed after DuckDB loaded,
    and the point of converting is to render exactly as DuckDB would."""
    global _ZONE
    if _ZONE is None:
        from zoneinfo import ZoneInfo

        import duckdb

        probe = duckdb.connect(":memory:")
        try:
            name = probe.execute("SELECT current_setting('TimeZone')").fetchone()[0]
        finally:
            probe.close()
        _ZONE = ZoneInfo(name)
    return _ZONE


def _local(row) -> Optional[Tuple[Any, ...]]:
    """A Postgres row with every timestamp in DuckDB's rendering zone.

    asyncpg hands back UTC; DuckDB hands back its session's zone. The shapers
    render `isoformat()` and the digest prints the first sixteen characters,
    so an unconverted Postgres branch would date every run three hours early
    in Kyiv.
    """
    if row is None:
        return None
    zone = _duckdb_zone()
    return tuple(v.astimezone(zone) if isinstance(v, datetime) and v.tzinfo else v
                 for v in row)


async def latest_run(conn, layer: Optional[str] = None) -> Optional[Dict[str, Any]]:
    from core.data_quality import (
        LATEST_RUN_OF_LAYER_SQL, LATEST_RUN_SQL, journal_sql, run_dict,
    )

    if layer is None:
        row = await conn.fetchrow(journal_sql(LATEST_RUN_SQL, "postgres"))
    else:
        row = await conn.fetchrow(
            journal_sql(LATEST_RUN_OF_LAYER_SQL, "postgres"), layer)
    return run_dict(_local(row))


async def run_by_id(conn, run_id: int) -> Optional[Dict[str, Any]]:
    from core.data_quality import RUN_BY_ID_SQL, journal_sql, run_dict

    row = await conn.fetchrow(journal_sql(RUN_BY_ID_SQL, "postgres"), int(run_id))
    return run_dict(_local(row))


async def previous_run(conn, layer: str, before_run_id: int) -> Optional[Dict[str, Any]]:
    from core.data_quality import PREVIOUS_RUN_ID_SQL, journal_sql

    found = await conn.fetchval(
        journal_sql(PREVIOUS_RUN_ID_SQL, "postgres"), layer, int(before_run_id))
    return None if found is None else await run_by_id(conn, int(found))


async def baseline_run(conn, layer: str, *, sent_at, before_run_id: int):
    """`core.data_quality.fetch_baseline_run`, against Postgres."""
    from core.data_quality import BASELINE_RUN_ID_SQL, journal_sql

    if sent_at is not None:
        found = await conn.fetchval(
            journal_sql(BASELINE_RUN_ID_SQL, "postgres"),
            layer, int(before_run_id), sent_at)
        if found is not None:
            return await run_by_id(conn, int(found))
    return await previous_run(conn, layer, before_run_id)


async def run_issues(conn, run_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    from core.data_quality import RUN_ISSUES_SQL, issue_dicts, journal_sql

    rows = await conn.fetch(journal_sql(RUN_ISSUES_SQL, "postgres"),
                            int(run_id), int(limit))
    return issue_dicts(_local(r) for r in rows)


async def run_diffs(conn, run_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    from core.data_quality import RUN_DIFFS_SQL, diff_dicts, journal_sql

    rows = await conn.fetch(journal_sql(RUN_DIFFS_SQL, "postgres"),
                            int(run_id), int(limit))
    return diff_dicts(_local(r) for r in rows)


async def last_success_ages(conn, layers: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    from core.data_quality import last_success_sql, success_ages

    rows = await conn.fetch(last_success_sql(len(layers), "postgres"), *layers)
    return success_ages([_local(r) for r in rows], layers)


async def digest_marker(conn) -> Optional[str]:
    """The beat as Postgres holds it, or None while the key is absent."""
    return await conn.fetchval(
        "SELECT value FROM meta.chain_watermarks WHERE key = $1",
        DIGEST_MARKER_KEY)
