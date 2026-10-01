"""The one door to the watchdogs' samples: where each tick persists and reads.

Chain 10 (`core/pg_watchdog_write.py`, OD-02 (c)). The disk and memory jobs
each hand their fresh sample here and get back the history they difference
against, from whichever store writes the samples.

UNDER `duckdb` (THE DEFAULT) NOTHING CHANGED

Each "duckdb" branch is the block that stood in the job, moved: one
`store.connection()`, the same statements in the same order — the directory
rows still take a `now()` of their own — and an exception propagates exactly
as before (the memory job catches it, the disk job does not). Postgres is not
asked for anything.

UNDER `postgres`, AND UNDER A FLAG NOBODY CAN READ

The tick is one Postgres transaction (`pg_watchdog_write`), then DuckDB is
handed the same insert and prune (`core.shadow_writes.into_duckdb`). Every
instant both stores see — the sample's, the directory set's, both prune
cutoffs — is computed here, once.

A failure of the Postgres tick, or a `KS_WRITE_WATCHDOGS` nobody can read on
an unlatched chain, returns **empty reads** and logs: the disk job still
judges capacity on the fresh sample and the memory job still judges memory,
with no history. When the disk fills, Postgres is the first thing to refuse
writes, and the capacity page is the one that must still go out — the reason
OD-16 recommended a file, kept here without one. The typo is paged by the
canary as `write_chain_flag_invalid`; the stale samples by the standing watch
as `chain_samples_stale`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# What a tick that could not persist hands back: no history, so no growth
# verdict and no OOM delta — and `persisted: False` for the job's result.
EMPTY_DISK = {"history": None, "dir_week_ago": None, "dir_six_ago": None,
              "remainder": [], "deleted": 0, "persisted": False}
EMPTY_MEMORY = {"last": None, "peak_24h": None, "persisted": False}


def _chain():
    from core import pg_watchdog_write

    return pg_watchdog_write


def _route(job: str) -> Optional[bool]:
    """True for Postgres, False for DuckDB, None for a flag nobody can read
    on an unlatched chain — which persists nowhere (OD-18 (a)) and evaluates
    anyway."""
    try:
        return _chain().writes_postgres()
    except RuntimeError as exc:
        logger.error("%s sample not persisted: %s", job, exc)
        return None


async def disk_tick(store, sample: Dict[str, Any],
                    dir_now: Optional[Dict[str, int]]) -> Dict[str, Any]:
    """Persist the disk job's sample and directory set; return its history."""
    from core.disk_monitor import (
        DIR_RETENTION_DAYS, DISK_RETENTION_DAYS, fetch_dir_sample_at_age,
        fetch_remainder_series, fetch_sample_at_age, insert_dir_samples,
        insert_sample, prune_cutoff, prune_old_dir_samples, prune_old_samples,
    )

    route = _route("Disk")
    if route is None:
        return dict(EMPTY_DISK)
    if not route:
        # duckdb — the job's block, moved unchanged.
        async with store.connection() as conn:
            history = fetch_sample_at_age(conn, hours=24, slack_hours=2)
            insert_sample(conn, sample)
            # Keep the table tiny: ~56 rows max (14 days x 4 samples/day).
            deleted = prune_old_samples(conn, retention_days=14)

            dir_week_ago = fetch_dir_sample_at_age(conn, hours=168, slack_hours=12)
            dir_six_ago = fetch_dir_sample_at_age(conn, hours=6, slack_hours=2)
            if dir_now:
                insert_dir_samples(conn, dir_now)
                prune_old_dir_samples(conn, retention_days=21)
            # After the insert, deliberately: the recent window is
            # supposed to contain this very sample. The two lookups above
            # are read first for the opposite reason — a "168h ago" search
            # must not be able to find today.
            remainder = fetch_remainder_series(conn, hours=180)
        return {"history": history, "dir_week_ago": dir_week_ago,
                "dir_six_ago": dir_six_ago, "remainder": remainder,
                "deleted": deleted, "persisted": True}

    from core import shadow_writes

    chain = _chain()
    now = datetime.now(timezone.utc)
    dir_sampled_at = now
    disk_cutoff = prune_cutoff(DISK_RETENTION_DAYS, now)
    dir_cutoff = prune_cutoff(DIR_RETENTION_DAYS, now)
    try:
        reads = await chain.disk_tick(
            sample, dir_now, now=now, dir_sampled_at=dir_sampled_at,
            disk_cutoff=disk_cutoff, dir_cutoff=dir_cutoff)
    except Exception as exc:  # noqa: BLE001 — capacity is judged regardless
        logger.error("Disk sample not persisted (Postgres: %s); judging capacity "
                     "without history", type(exc).__name__, exc_info=True)
        return dict(EMPTY_DISK)

    def shadow(conn):
        insert_sample(conn, sample)
        prune_old_samples(conn, cutoff=disk_cutoff)
        if dir_now:
            insert_dir_samples(conn, dir_now, sampled_at=dir_sampled_at)
            prune_old_dir_samples(conn, cutoff=dir_cutoff)

    await shadow_writes.into_duckdb(store, chain.CHAIN, shadow)
    return {**reads, "persisted": True}


async def memory_tick(store, mem: Dict[str, Any]) -> Dict[str, Any]:
    """Persist the memory job's sample; return `{last, peak_24h}`.

    Under `duckdb` an exception propagates, as the block always did — the
    job catches it and still evaluates."""
    from core.memory_monitor import (
        MEMORY_RETENTION_DAYS, fetch_last_sample, fetch_peak_working_set_mb,
        insert_sample, prune_old_samples,
    )

    route = _route("Memory")
    if route is None:
        return dict(EMPTY_MEMORY)
    if not route:
        # duckdb — the job's block, moved unchanged.
        async with store.connection() as conn:
            last = fetch_last_sample(conn)
            insert_sample(conn, mem)
            peak_24h = fetch_peak_working_set_mb(conn, hours=24)
            prune_old_samples(conn, retention_days=14)
        return {"last": last, "peak_24h": peak_24h, "persisted": True}

    from datetime import timedelta

    from core import shadow_writes

    chain = _chain()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MEMORY_RETENTION_DAYS)
    try:
        reads = await chain.memory_tick(mem, now=now, sampled_at=now, cutoff=cutoff)
    except Exception as exc:  # noqa: BLE001 — memory is judged regardless
        logger.error("Memory sample not persisted (Postgres: %s); judging memory "
                     "without history", type(exc).__name__, exc_info=True)
        return dict(EMPTY_MEMORY)

    def shadow(conn):
        insert_sample(conn, mem, sampled_at=now)
        prune_old_samples(conn, cutoff=cutoff)

    await shadow_writes.into_duckdb(store, chain.CHAIN, shadow)
    return {**reads, "persisted": True}
