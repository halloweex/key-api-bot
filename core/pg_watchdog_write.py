"""Chain 10: the watchdogs' samples, written to Postgres and shadowed in DuckDB.

`disk_samples`, `data_dir_samples` and `memory_samples` are the history the
two resource watchdogs difference against: the disk job reads the sample
nearest 24 h ago, the directory sets nearest 168 h and 6 h ago and the
remainder's last 180 h; the memory job reads the previous sample (the only
way an OOM kill before a recreate is still visible after it) and the 24 h
peak. Nothing re-derives a sample: it is what the disk or the cgroup said at
that moment.

THE SHAPE: SHADOW (owner decision OD-02 (c), 2026-10-01)

Under `KS_WRITE_WATCHDOGS=postgres` each job's whole persistence — the
differencing reads, the insert and the prune — is one Postgres transaction,
and DuckDB is handed the same insert and the same prune right after it
(`core/shadow_writes.py`). The chain map's warning is met by construction:
"move an insert to Postgres and leave its read on DuckDB and the watchdog
reports growth of zero forever" — here the reads travel with the insert, in
the same transaction. The hourly copy stands down; the daily comparison keeps
comparing, facing Postgres→DuckDB (`compare_shadow`), and reads a DuckDB prune
that lagged as `shadow_pruned_rows` INFO, not as a defect.

One value set for both stores: the sample's `sampled_at`, the directory set's
one `sampled_at`, and both prune cutoffs are computed once by the router
(`core/watchdog_samples.py`) and handed to both, so the stores prune at the
same instant and hold the same rows.

THE EVALUATION NEVER WAITS ON THE STORE

When the disk fills, Postgres is the first thing to refuse writes, and the
capacity page is the one that must still go out. So the router catches a
failure here and the job evaluates the fresh sample with no history — no
growth verdict, no OD-16 file needed. OD-16's recommendation was samples in a
file; the task this was built for put them in Postgres, and the design names
what the file variant would change.

Everything else — the latch on the first Postgres write (OD-19 (a)), the
owner rows, the copy-back (`scripts/chain_copy_back.py watchdog`) — is the
frozen chains' machinery unchanged.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_WATCHDOGS"
CHAIN = "pg_watchdog_write"
CHAIN_TABLES: Tuple[str, ...] = (
    "app.disk_samples", "app.data_dir_samples", "app.memory_samples",
)
CHAIN_SHADOW = True

# The revision that refuses every image built before this chain; the flag
# moves nothing until this build requires it (`chain_latch.lockout_unmet`).
LOCKOUT_REVISION = "0035_batch_e_chains"

# How far back the disk job's three differencing reads look — the job's own
# numbers, spelled once for both stores.
DISK_HISTORY = (24, 2)          # (hours, slack) for the 24 h DB delta
DIR_WEEK = (168, 12)            # the 168 h baseline
DIR_SIX = (6, 2)                # the bootstrap's 6 h step
REMAINDER_HOURS = 180
PEAK_HOURS = 24


# What the standing watch (`core/pg_chain_invariants.py`) judges the three
# tables by once the chain writes them: the newest sample no older than its
# job's cadence plus slack (`chain_samples_stale` — the watchdog that watches
# the watchdog, which the hourly copy and its comparison used to be by
# accident), and the oldest no older than its retention plus a day
# (`chain_retention_unbounded` — "anything that accumulates declares its bound
# in the same change", checked where the bound now runs).
STALE_AFTER = {
    "app.disk_samples": timedelta(hours=7),        # 01/07/13/19: every 6 h
    "app.data_dir_samples": timedelta(hours=7),
    "app.memory_samples": timedelta(minutes=90),   # every 30 min
}


def retention_days() -> Dict[str, int]:
    from core.disk_monitor import DIR_RETENTION_DAYS, DISK_RETENTION_DAYS
    from core.memory_monitor import MEMORY_RETENTION_DAYS

    return {"app.disk_samples": DISK_RETENTION_DAYS,
            "app.data_dir_samples": DIR_RETENTION_DAYS,
            "app.memory_samples": MEMORY_RETENTION_DAYS}


def env_writes_postgres() -> bool:
    """What `KS_WRITE_WATCHDOGS` alone says; an unknown value raises (OD-18 (a))."""
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def unmet_precondition() -> Optional[str]:
    """Why `KS_WRITE_WATCHDOGS=postgres` must not move the writes yet, or None:
    the lock-out alone (`chain_latch.lockout_unmet`). Never raises and
    never asks Postgres."""
    return chain_latch.lockout_unmet(LOCKOUT_REVISION)


def writes_postgres() -> bool:
    """The latch outranks the flag, and the flag moves the writes only
    once `unmet_precondition()` holds."""
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres() and unmet_precondition() is None


def _latch() -> str:
    """Inside `pool.acquire()`, never before it (`pg_expenses_write._latch`)."""
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def _dir_set_at(conn, now: datetime, hours: float, slack: float) -> Optional[Dict[str, int]]:
    from core.disk_monitor import (
        DIR_NEAREST_SQL, DIR_SET_SQL, at_age_window, sample_sql,
    )

    lo, hi, target = at_age_window(now, hours, slack)
    found = await conn.fetchval(sample_sql(DIR_NEAREST_SQL, "postgres"), lo, hi, target)
    if found is None:
        return None
    rows = await conn.fetch(sample_sql(DIR_SET_SQL, "postgres"), found)
    return {r[0]: int(r[1]) for r in rows}


async def disk_tick(
    sample: Dict[str, Any],
    dir_now: Optional[Dict[str, int]],
    *,
    now: datetime,
    dir_sampled_at: datetime,
    disk_cutoff: datetime,
    dir_cutoff: datetime,
) -> Dict[str, Any]:
    """The disk job's persistence, in the order it always ran, in one
    transaction: the 24 h read, the insert, the prune; the 168 h and 6 h
    directory sets (before their insert, so "168 h ago" cannot find today),
    the insert, the prune; the remainder series (after, so the window holds
    this very sample). Returns what the job evaluates."""
    from core.disk_monitor import (
        DISK_AT_AGE_SQL, DISK_INSERT_SQL, DISK_PRUNE_SQL, DIR_INSERT_SQL,
        DIR_PRUNE_SQL, REMAINDER_SQL, UNATTRIBUTED, at_age_window, dir_rows,
        disk_row, disk_sample_dict, sample_sql,
    )

    pool = await _pool()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            lo, hi, target = at_age_window(now, *DISK_HISTORY)
            history = disk_sample_dict(await conn.fetchrow(
                sample_sql(DISK_AT_AGE_SQL, "postgres"), lo, hi, target))
            await conn.execute(sample_sql(DISK_INSERT_SQL, "postgres"), *disk_row(sample))
            deleted = len(await conn.fetch(
                sample_sql(DISK_PRUNE_SQL, "postgres"), disk_cutoff))
            week_ago = await _dir_set_at(conn, now, *DIR_WEEK)
            six_ago = await _dir_set_at(conn, now, *DIR_SIX)
            if dir_now:
                await conn.executemany(sample_sql(DIR_INSERT_SQL, "postgres"),
                                       dir_rows(dir_now, dir_sampled_at))
                await conn.execute(sample_sql(DIR_PRUNE_SQL, "postgres"), dir_cutoff)
            remainder = [(r[0], int(r[1])) for r in await conn.fetch(
                sample_sql(REMAINDER_SQL, "postgres"), UNATTRIBUTED,
                now - timedelta(hours=REMAINDER_HOURS))]
    return {"history": history, "dir_week_ago": week_ago, "dir_six_ago": six_ago,
            "remainder": remainder, "deleted": deleted}


async def memory_tick(
    mem: Dict[str, Any],
    *,
    now: datetime,
    sampled_at: datetime,
    cutoff: datetime,
) -> Dict[str, Any]:
    """The memory job's persistence in one transaction: the previous sample,
    the insert, the 24 h peak, the prune. Returns `{last, peak_24h}`."""
    from core.memory_monitor import (
        MEMORY_INSERT_SQL, MEMORY_LAST_SQL, MEMORY_PEAK_SQL, MEMORY_PRUNE_SQL,
        memory_row, memory_sample_dict, memory_sql,
    )

    pool = await _pool()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            last = memory_sample_dict(await conn.fetchrow(
                memory_sql(MEMORY_LAST_SQL, "postgres")))
            await conn.execute(memory_sql(MEMORY_INSERT_SQL, "postgres"),
                               *memory_row(mem, sampled_at))
            peak = await conn.fetchval(memory_sql(MEMORY_PEAK_SQL, "postgres"),
                                       now - timedelta(hours=PEAK_HOURS))
            await conn.execute(memory_sql(MEMORY_PRUNE_SQL, "postgres"), cutoff)
    return {"last": last, "peak_24h": float(peak) if peak is not None else None}
