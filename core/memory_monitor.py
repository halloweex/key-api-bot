"""Container memory watchdog.

Pages when the container is genuinely close to its limit, and — separately,
unconditionally — when the kernel has actually killed something.

**What this does not measure, and why.** The previous version alerted on
`memory.current / memory.max`. In cgroup v2 `memory.current` includes the page
cache, and this process streams a multi-GB database file, so that number rises
with the file and falls only when the kernel reclaims. Measured on the host in
August 2026: `anon` 650 MB against `file` 677 MB — **page cache was 50.3% of
the reading the alert was judging.** It is memory the kernel hands back on
demand; counting it as usage means the alert tracks database size rather than
memory pressure, which is the same defect the disk watchdog had in a different
unit (see core/disk_monitor.py).

So the quantity here is the *unreclaimable working set*: anonymous pages,
socket buffers and unreclaimable slab. That is what actually has to fit.

**Headroom is no longer computed from `memory.peak`.** That counter is monotonic
for the container's lifetime: it never falls, so a "Headroom" derived from it
only ever shrinks and is reset to nothing by any restart. The old message
reported it anyway, next to a threshold the author had correctly refused to
trigger on. Headroom is now limit minus the current working set, which can
recover.

**Samples are persisted.** A spike used to exist only in a kernel counter and a
json log, both destroyed by `docker compose up`, which is why a 5.3 GB reading
in August could never be decomposed afterwards. `memory_samples` outlives the
container, and it is also what makes OOM detection work across a restart.

The evaluator is pure (no I/O). Sampling and persistence live below it; the
scheduler orchestrates.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.data_quality import Severity


# Thresholds as a fraction of the container limit, applied to the working set.
#
# Deliberately not tighter. DUCKDB_MEMORY_LIMIT is 4 GB against a 7 GB container
# cap, so DuckDB alone may legitimately hold 57% while doing exactly what it is
# supposed to; a warehouse refresh plus pandas on top of that is a healthy 70%.
# A threshold below ~80% would fire on correct behaviour, and an alert that
# fires on correct behaviour is the thing this codebase has just spent a
# release removing.
#
# At 80% of 7 GB the working set is 5.6 GB with a 4 GB-capped engine inside it,
# which is genuinely tight. At 90% there is 700 MB left.
WARN_PCT = 0.80
CRITICAL_PCT = 0.90


@dataclass(frozen=True)
class MemoryAlert:
    """A memory concern worth sending. `oom` outranks any threshold."""
    severity: Severity
    reason: str
    working_set_mb: float
    page_cache_mb: float
    limit_mb: float
    headroom_mb: float
    oom_kills_delta: int = 0


def evaluate_memory(
    *,
    working_set_bytes: int,
    page_cache_bytes: int,
    limit_bytes: Optional[int],
    oom_kills: int,
    previous_oom_kills: Optional[int] = None,
) -> Optional[MemoryAlert]:
    """Return a MemoryAlert if memory is a problem, else None.

    Args:
        working_set_bytes: anon + sock + unreclaimable slab. The bytes that
            must fit; page cache is excluded because the kernel reclaims it
            before it OOMs anything.
        page_cache_bytes: file-backed pages, reported for context only.
        limit_bytes: memory.max, or None when the cgroup has no limit.
        oom_kills: memory.events oom_kill counter, which resets when the
            container is recreated.
        previous_oom_kills: the same counter at the previous sample, read from
            persisted state so a kill is still detectable after a restart.
            None on the first sample ever.

    Returns:
        MemoryAlert when something is wrong, else None.
    """
    mb = 1024 * 1024
    working_mb = working_set_bytes / mb
    cache_mb = page_cache_bytes / mb

    # An OOM kill is the only unambiguous evidence of saturation: not a
    # prediction that memory might run out, but a record that it did and the
    # kernel chose a victim. It outranks the thresholds and ignores them.
    #
    # A reset counter (previous > current) means the container was recreated,
    # which is not itself a kill.
    delta = 0
    if previous_oom_kills is not None and oom_kills > previous_oom_kills:
        delta = oom_kills - previous_oom_kills
    elif previous_oom_kills is None and oom_kills > 0:
        delta = oom_kills

    limit_mb = (limit_bytes / mb) if limit_bytes else 0.0
    headroom_mb = (limit_mb - working_mb) if limit_bytes else 0.0

    if delta > 0:
        return MemoryAlert(
            severity=Severity.CRITICAL,
            reason=(
                f"the kernel OOM-killed {delta} process(es) in this container; "
                f"working set {working_mb:,.0f} MB"
                + (f" of {limit_mb:,.0f} MB" if limit_bytes else "")
            ),
            working_set_mb=round(working_mb, 1),
            page_cache_mb=round(cache_mb, 1),
            limit_mb=round(limit_mb, 1),
            headroom_mb=round(headroom_mb, 1),
            oom_kills_delta=delta,
        )

    # Without a limit there is no percentage to take. The host has its own
    # memory pressure and this job is not the thing that should report it.
    if not limit_bytes:
        return None

    usage = working_set_bytes / limit_bytes
    if usage >= CRITICAL_PCT:
        severity, threshold = Severity.CRITICAL, CRITICAL_PCT
    elif usage >= WARN_PCT:
        severity, threshold = Severity.WARN, WARN_PCT
    else:
        return None

    return MemoryAlert(
        severity=severity,
        reason=(
            f"working set {working_mb:,.0f} MB of {limit_mb:,.0f} MB "
            f"({usage:.0%}, >= {threshold:.0%}); {headroom_mb:,.0f} MB free, "
            f"page cache {cache_mb:,.0f} MB not counted"
        ),
        working_set_mb=round(working_mb, 1),
        page_cache_mb=round(cache_mb, 1),
        limit_mb=round(limit_mb, 1),
        headroom_mb=round(headroom_mb, 1),
    )


# ─── I/O: reading the cgroup ─────────────────────────────────────────────────


def read_cgroup_memory(cgroup_root: str = "/sys/fs/cgroup") -> Optional[dict]:
    """Read cgroup v2 memory state, or None when not in a limited container.

    Returns {working_set, page_cache, current, limit, oom_kills}, all bytes
    except oom_kills.
    """
    import pathlib

    try:
        cgroup = pathlib.Path(cgroup_root)
        current = int((cgroup / "memory.current").read_text().strip())

        raw_max = (cgroup / "memory.max").read_text().strip()
        limit = int(raw_max) if raw_max != "max" else None

        stat = {}
        stat_path = cgroup / "memory.stat"
        if stat_path.exists():
            for line in stat_path.read_text().splitlines():
                parts = line.split()
                if len(parts) == 2:
                    try:
                        stat[parts[0]] = int(parts[1])
                    except ValueError:
                        continue

        anon = stat.get("anon", 0)
        page_cache = stat.get("file", 0)
        sock = stat.get("sock", 0)
        # slab_unreclaimable is the honest term; older kernels only expose slab,
        # which over-counts slightly. Over-counting the working set is the safe
        # direction to be wrong in.
        slab = stat.get("slab_unreclaimable", stat.get("slab", 0))

        working_set = anon + sock + slab
        # No memory.stat (or an unexpected shape) — fall back to the whole
        # reading rather than reporting a zero working set, which would read as
        # "healthy" and be the worst possible failure mode for this file.
        if not stat or working_set <= 0:
            working_set = current
            page_cache = 0

        oom_kills = 0
        events_path = cgroup / "memory.events"
        if events_path.exists():
            for line in events_path.read_text().splitlines():
                if line.startswith("oom_kill "):
                    oom_kills = int(line.split()[1])

        return {
            "working_set": working_set,
            "page_cache": page_cache,
            "current": current,
            "limit": limit,
            "oom_kills": oom_kills,
        }
    except (FileNotFoundError, PermissionError, ValueError):
        return None


# ─── Persistence (memory_samples) ────────────────────────────────────────────


def insert_sample(conn, sample: dict, sampled_at: Optional[datetime] = None) -> None:
    """Persist one memory sample. Single INSERT, cheap."""
    from datetime import timezone
    if sampled_at is None:
        sampled_at = datetime.now(timezone.utc)
    mb = 1024 * 1024
    conn.execute(
        "INSERT INTO memory_samples "
        "(sampled_at, working_set_mb, page_cache_mb, limit_mb, oom_kills) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            sampled_at,
            round(sample["working_set"] / mb, 2),
            round(sample["page_cache"] / mb, 2),
            round(sample["limit"] / mb, 2) if sample.get("limit") else None,
            sample["oom_kills"],
        ],
    )


def fetch_last_sample(conn) -> Optional[dict]:
    """Most recent persisted sample, or None when the table is empty.

    This is what makes an OOM kill detectable across a container recreate: the
    kernel counter resets, ours does not.
    """
    row = conn.execute(
        "SELECT sampled_at, working_set_mb, page_cache_mb, limit_mb, oom_kills "
        "FROM memory_samples ORDER BY sampled_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return {
        "sampled_at": row[0],
        "working_set_mb": float(row[1]),
        "page_cache_mb": float(row[2]),
        "limit_mb": float(row[3]) if row[3] is not None else None,
        "oom_kills": int(row[4]),
    }


def fetch_peak_working_set_mb(conn, hours: int = 24) -> Optional[float]:
    """Highest working set seen in the last `hours`, across restarts.

    The replacement for memory.peak, which a restart erases.
    """
    from datetime import timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    row = conn.execute(
        "SELECT MAX(working_set_mb) FROM memory_samples WHERE sampled_at >= ?",
        [cutoff],
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def prune_old_samples(conn, retention_days: int = 14) -> int:
    """Delete samples older than retention_days. Tiny table; cheap to clean."""
    from datetime import timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    result = conn.execute(
        "DELETE FROM memory_samples WHERE sampled_at < ? RETURNING sampled_at",
        [cutoff],
    ).fetchall()
    return len(result)
