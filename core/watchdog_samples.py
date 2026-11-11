"""The one door to the watchdogs' samples: where each tick persists and reads.

Chain 10 (`core/pg_watchdog_write.py`, OD-02 (c)). The disk and memory jobs
each hand their fresh sample here and get back the history they difference
against, from Postgres.

ONE STORE SINCE STEP 14'S S14-10

The tick is one Postgres transaction (`pg_watchdog_write`). Every instant it
writes and reads by — the sample's, the directory set's, both prune cutoffs
— is computed here, once. Until S14-10 DuckDB was then handed the same insert
and prune as a shadow (`core/shadow_writes.py`), and a chain left at `duckdb`
ran the job's old block on DuckDB alone; since S14-00 neither ran in web,
which runs `KS_DUCKDB=off`, and S14-10 deleted both, with the `store` the two
doors used to take.

A failure of the Postgres tick returns **empty reads** and logs: the disk job
still judges capacity on the fresh sample and the memory job still judges
memory, with no history. When the disk fills, Postgres is the first thing to
refuse writes, and the capacity page is the one that must still go out — the
reason OD-16 recommended a file, kept here without one. The stale samples are
paged by the standing watch as `chain_samples_stale`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
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


async def disk_tick(sample: Dict[str, Any],
                    dir_now: Optional[Dict[str, int]]) -> Dict[str, Any]:
    """Persist the disk job's sample and directory set; return its history."""
    from core.disk_monitor import DIR_RETENTION_DAYS, DISK_RETENTION_DAYS, prune_cutoff

    now = datetime.now(timezone.utc)
    dir_sampled_at = now
    disk_cutoff = prune_cutoff(DISK_RETENTION_DAYS, now)
    dir_cutoff = prune_cutoff(DIR_RETENTION_DAYS, now)
    try:
        reads = await _chain().disk_tick(
            sample, dir_now, now=now, dir_sampled_at=dir_sampled_at,
            disk_cutoff=disk_cutoff, dir_cutoff=dir_cutoff)
    except Exception as exc:  # noqa: BLE001 — capacity is judged regardless
        logger.error("Disk sample not persisted (Postgres: %s); judging capacity "
                     "without history", type(exc).__name__, exc_info=True)
        return dict(EMPTY_DISK)
    return {**reads, "persisted": True}


async def memory_tick(mem: Dict[str, Any]) -> Dict[str, Any]:
    """Persist the memory job's sample; return `{last, peak_24h}`.

    A Postgres failure returns no history and logs; the job evaluates the
    fresh sample anyway."""
    from core.memory_monitor import MEMORY_RETENTION_DAYS

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MEMORY_RETENTION_DAYS)
    try:
        reads = await _chain().memory_tick(mem, now=now, sampled_at=now, cutoff=cutoff)
    except Exception as exc:  # noqa: BLE001 — memory is judged regardless
        logger.error("Memory sample not persisted (Postgres: %s); judging memory "
                     "without history", type(exc).__name__, exc_info=True)
        return dict(EMPTY_MEMORY)
    return {**reads, "persisted": True}
