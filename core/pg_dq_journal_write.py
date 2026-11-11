"""Chain 9: the data-quality journal, written to Postgres.

`data_quality_runs` and its two children are the record every check leaves
behind — what the 09:00 digest reads, what `/api/health` turns into the layer
ages the canary pages on, and what `/api/health/data-quality` publishes.
Nothing re-derives a run: it is what a check found at the moment it ran.

THE SHAPE: SHADOW, UNTIL STEP 14'S S14-10 (owner decision OD-02 (c), 2026-10-01)

Chains 1, 6a, 7a and 8 freeze DuckDB when they move. This chain did not:
under `KS_WRITE_DQ_JOURNAL=postgres` a run was committed to Postgres first
and DuckDB was handed the very same rows right after (`core/shadow_writes.py`),
so DuckDB's journal stayed whole and the daily comparison compared the two,
facing Postgres→DuckDB (`compare_shadow`). Since S14-00 the shadow stood down
in web, which runs `KS_DUCKDB=off`, and the comparison compared nothing; S14-10
deleted both, and the journal is Postgres's alone. The hourly copy stands
down, as for every moved chain: its full replace out of DuckDB would delete
each run DuckDB never received. `CHAIN_SHADOW` said the shape to
`core.write_chains`, which published it, until S14-11 deleted both.

Everything else is the frozen chains' machinery unchanged. The first Postgres
write latches the chain (OD-19 (a)), because a run whose shadow failed exists
in Postgres alone and the flag is then no more a rollback than chain 8's; the
way back was `scripts/chain_copy_back.py dq_journal` (deleted in step 14's
S14-04; the way back since is an image from before step 14,
`core.rollback_lever`).

THE ALLOCATOR: MAX + 1 UNDER AN ADVISORY LOCK, NO REVISION

`app.data_quality_runs.run_id` has no sequence (revision 0028 carried
DuckDB's values). Under this chain Postgres issues the id (and handed it to
DuckDB's shadow until S14-10). `MAX(run_id) + 1` is safe here and only here
because the transaction-scoped advisory lock serialises the allocators,
READ COMMITTED's next statement after the lock sees the previous holder's
commit, nothing deletes from these tables in Postgres once the copy stands
down, and the rate is ~10 runs a day. A revision with a sequence is the alternative the design names;
`core.chain_transfer` derived that sequence's name and would have floored
from it (deleted in S14-04).

THE DIGEST'S BEAT

`dq_digest_last_sent` moves with the chain (`CHAIN_MARKER_KEYS`): it lives in
`meta.chain_watermarks`, which nothing copies out of DuckDB — the hourly full
replace of `app.sync_metadata` would wipe it. `core.dq_journal` reads it there
first and, while the key is absent, inherits it from `app.sync_metadata`
(DuckDB's value as the hourly copy last shipped it), so a first digest after
a flip does not restate everything. Production holds the key in
`meta.chain_watermarks` (H6, 2026-10-29), so there the inheritance answers
nothing.

READS FOLLOW THE WRITER

Every reader of the journal — the digest, the layer ages, the data-quality
endpoint, the startup catch-up — reads Postgres, through `core.dq_journal`,
and never falls back to DuckDB: a fallback would make an unreachable Postgres
read as "journal fine" at the moment the journal's writer is the thing that
failed.
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


# Not a sync watermark — `_freshness_check` would judge a `last_sync_*` key as
# a stalled sync — but kept in `meta.chain_watermarks` for the same reason.
# `CHAIN_MARKER_KEYS` declares it chain 9's. No production code reads the
# declaration since step 14's S14-04: its one reader was the copy-back
# (`chain_transfer._carried_keys`, which carried the key back with the chain;
# an image from before step 14 reads its own copy of this module). What reads
# it here: the week-of-silence runbook's test, which names chain 9 among the
# chains that keep a value in DuckDB's `sync_metadata` from it
# (`tests/unit/test_week_of_silence_host.py`), the one-spelling test in
# `tests/unit/test_dq_journal.py`, and `duckdb_table_fates`' text.
DIGEST_MARKER_KEY = "dq_digest_last_sent"
CHAIN_MARKER_KEYS: Tuple[str, ...] = (DIGEST_MARKER_KEY,)

# The run-id allocator's lock, beside chain 1's `0x6B73_0000_0000_0001`.
# `tests/unit/test_dq_journal.py` walks `core/` for every `*_LOCK_KEY` and
# fails on a reuse: two allocators sharing a key would serialise for nothing,
# and one sharing chain 1's would block a stock rebuild behind a DQ run.
RUN_ID_LOCK_KEY = 0x6B73_0000_0000_0009


# `env_writes_postgres`, `unmet_precondition` and `writes_postgres` stood
# here until step 14's S14-11, with `CHAIN_SHADOW` (the shape the registry
# published as `shadow` after S14-10 deleted the shadow itself) and
# `LOCKOUT_REVISION` (the revision a flip waited for — 0035, which
# `tests/unit/test_chain_lockout.py` keeps as a literal). S14-00 made the
# answers constants and S14-11 deleted them with the flag: the chain writes
# Postgres, a constant. `WRITE_ENV` stays a name (revision 0035's comments and
# an image from before step 14 read it).

# `reads_postgres` went with them; its last consumer was chain 1's
# preflight, which S14-11 deleted too.


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
    asked for anything, so an input Postgres would read as another instant
    never latches the chain (chain 7a's `_fits` reason). Inside the one
    transaction: the owner rows, the allocator's lock, the id, then the three
    INSERTs. (Until S14-10 the caller handed the same rows to DuckDB after
    this returned.)
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
# nothing). The rest are handed that connection and run the journal's
# statements (`core.data_quality.journal_sql`), shaped by `core.data_quality`'s
# functions — DuckDB's readers used the same ones until S14-10, which is what
# kept the digest's text unchanged when the journal moved.


@asynccontextmanager
async def reading() -> AsyncIterator[Any]:
    """A pooled connection to read the journal on. Reads only."""
    pool = await _pool()
    async with pool.acquire() as conn:
        yield conn


_ZONE: Optional[Any] = None


def _zone():
    """The zone every Postgres row of the journal is rendered in: the host's
    (`_host_zone`), read once per process.

    It used to be DuckDB's session `TimeZone`, asked of an in-memory DuckDB
    through the one opener (`_duckdb_zone`), whenever the process ran
    `KS_DUCKDB=on` — the two stores' renderings were compared, so Postgres's
    had to print what DuckDB printed. Under `off` the host's zone was read
    without DuckDB, and web has run `off` since 2026-10-16. Step 14's S14-10
    collapsed the two into the second: for a process started with `TZ` set
    they are one zone, which `tests/unit/test_dq_digest_zone.py` pins on
    the digest's labels, taken on the base where DuckDB was asked."""
    global _ZONE
    if _ZONE is None:
        _ZONE = _host_zone()
    return _ZONE


def _host_zone():
    """The process's zone: `TZ` as the process has it, else the host's
    `/etc/localtime`, else UTC — the order ICU reads them in, which is how
    DuckDB took its session `TimeZone` (S14-10 kept the order so the digest
    prints what it printed)."""
    from zoneinfo import ZoneInfo

    name = (os.environ.get("TZ") or "").strip().lstrip(":")
    if name:
        try:
            return ZoneInfo(name)
        except Exception:  # noqa: BLE001 — not a zone name; the host decides
            pass
    try:
        with open("/etc/localtime", "rb") as fh:
            return ZoneInfo.from_file(fh, key="localtime")
    except Exception:  # noqa: BLE001 — no host zone to read
        return ZoneInfo("UTC")


def _local(row) -> Optional[Tuple[Any, ...]]:
    """A Postgres row with every timestamp in the process's zone (`_zone`).

    asyncpg hands back UTC. The shapers render `isoformat()` and the digest
    prints the first sixteen characters, so an unconverted row would date
    every run three hours early in Kyiv.
    """
    if row is None:
        return None
    zone = _zone()
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
    """The run of `layer` immediately preceding `before_run_id`, if any.

    Used by the digest to say whether a standing problem is growing. Failed
    runs are skipped: their zero counts would read as "fixed, then broke
    again" and turn every 429 into a fake recovery."""
    from core.data_quality import PREVIOUS_RUN_ID_SQL, journal_sql

    found = await conn.fetchval(
        journal_sql(PREVIOUS_RUN_ID_SQL, "postgres"), layer, int(before_run_id))
    return None if found is None else await run_by_id(conn, int(found))


async def baseline_run(conn, layer: str, *, sent_at, before_run_id: int):
    """The run of `layer` that the last *delivered* digest reported on.

    `previous_run` answers a different question — the run before this one —
    and the digest was using it to say "(=)". For a layer that runs four
    times a day against a digest that goes out once, that compares a
    six-hour window and prints the answer under a heading the reader takes
    to mean "since yesterday". A finding that stepped and settled between
    two digests was marked unchanged in both: `goods_shipped_without_sale`
    went 809 → 812 at the 13:00 Kyiv run on 2026-09-14, and the mornings
    either side of it both said "=". Nothing ever announced it.

    So the baseline is the newest run that had **finished** when the marker
    was written — `ended_at <= sent_at`, because a run still in flight
    cannot have been in a message. Derived from `dq_digest_last_sent`, which
    already exists, rather than recorded per layer: a stored cursor is a
    second thing to be wrong, and there is nothing here that recomputing
    cannot recover.

    Falls back to the previous run when there is no marker yet or no run
    finished before it — a first digest, and a layer younger than the
    marker; both must read as an ordinary morning rather than marking
    everything `new`. Failed runs are skipped for `previous_run`'s reason.
    (DuckDB's twin, `data_quality.fetch_baseline_run`, carried this
    reasoning until step 14's S14-10 deleted it.)"""
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
