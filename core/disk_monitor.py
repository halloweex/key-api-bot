"""Disk growth watchdog.

Catches two failure modes early:

1. **Capacity** — disk fills slowly (e.g. WAL never checkpointed,
   bronze regrows, log rotation broken). We want a Telegram alert at
   75% used, escalating to CRITICAL at 90%. The 90% threshold is below
   the empirical "compact preflight needs 22 GB free" line on our 75 GB
   Hetzner box, so we still have headroom to act.

2. **Growth rate** — DB file grows much faster than business volume.
   This signals a sync amplification regression (like the 1440x bug we
   fixed) or a bronze-style accumulation. Compare current DB size vs a
   sample from ~24 h ago.

The evaluator is pure (no I/O). Sample storage + scheduler job live
elsewhere; this file owns only the contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.data_quality import Severity


# Thresholds — tuned for the 75 GB Hetzner host.
WARN_DISK_PCT = 75.0
CRITICAL_DISK_PCT = 90.0

# Growth thresholds: 10% / 24h is fast (would fill disk in ~10 days from
# 50% used). 25% / 24h is a runaway — the May 2026 bronze regression hit
# this kind of rate before we noticed.
WARN_GROWTH_PCT_24H = 10.0
CRITICAL_GROWTH_PCT_24H = 25.0

# Below this DB size, percentage growth is noisy. Skip growth check.
MIN_DB_SIZE_FOR_GROWTH_CHECK_MB = 100.0


@dataclass(frozen=True)
class DiskAlert:
    """A single disk/DB health concern. Multiple concerns roll up via
    severity — caller picks the worst one for paging."""
    severity: Severity
    reason: str
    disk_pct_used: float
    disk_free_gb: float
    db_size_mb: float
    growth_pct_24h: Optional[float]  # None if no 24h-ago sample


def evaluate_disk_growth(
    *,
    disk_pct_used: float,
    disk_free_gb: float,
    db_size_mb: float,
    db_size_24h_ago_mb: Optional[float],
) -> Optional[DiskAlert]:
    """Return a DiskAlert if anything is concerning, else None.

    The alert reflects the WORST of:
      - disk_pct_used vs WARN/CRITICAL_DISK_PCT
      - growth_pct_24h vs WARN/CRITICAL_GROWTH_PCT_24H

    Bootstrap (db_size_24h_ago_mb is None or DB too small):
      - Capacity check still fires.
      - Growth check is skipped.

    Args:
        disk_pct_used: 0.0-100.0, current % of filesystem used.
        disk_free_gb: free GB on the same filesystem.
        db_size_mb: current DuckDB file size in MB.
        db_size_24h_ago_mb: closest available sample from ~24h ago,
            or None on the first run / after compaction.

    Returns:
        DiskAlert when severity >= WARN, else None.
    """
    # Capacity check
    capacity_severity: Optional[Severity] = None
    capacity_reason = ""
    if disk_pct_used >= CRITICAL_DISK_PCT:
        capacity_severity = Severity.CRITICAL
        capacity_reason = (
            f"disk {disk_pct_used:.1f}% used (>= {CRITICAL_DISK_PCT:.0f}% "
            f"critical); {disk_free_gb:.1f} GB free"
        )
    elif disk_pct_used >= WARN_DISK_PCT:
        capacity_severity = Severity.WARN
        capacity_reason = (
            f"disk {disk_pct_used:.1f}% used (>= {WARN_DISK_PCT:.0f}% "
            f"warn); {disk_free_gb:.1f} GB free"
        )

    # Growth check (only when we have history and the DB is non-trivially sized)
    growth_pct_24h: Optional[float] = None
    growth_severity: Optional[Severity] = None
    growth_reason = ""
    if (db_size_24h_ago_mb is not None
            and db_size_24h_ago_mb >= MIN_DB_SIZE_FOR_GROWTH_CHECK_MB):
        growth_pct_24h = 100.0 * (db_size_mb - db_size_24h_ago_mb) / db_size_24h_ago_mb
        # Negative growth (e.g. after compact) is never an alert
        if growth_pct_24h >= CRITICAL_GROWTH_PCT_24H:
            growth_severity = Severity.CRITICAL
            growth_reason = (
                f"DB grew {growth_pct_24h:+.1f}% in 24h "
                f"({db_size_24h_ago_mb:.0f} → {db_size_mb:.0f} MB; "
                f">= {CRITICAL_GROWTH_PCT_24H:.0f}% critical)"
            )
        elif growth_pct_24h >= WARN_GROWTH_PCT_24H:
            growth_severity = Severity.WARN
            growth_reason = (
                f"DB grew {growth_pct_24h:+.1f}% in 24h "
                f"({db_size_24h_ago_mb:.0f} → {db_size_mb:.0f} MB; "
                f">= {WARN_GROWTH_PCT_24H:.0f}% warn)"
            )

    # Pick worst-of severity. Each Severity has rank() — higher = more urgent.
    candidates = []
    if capacity_severity is not None:
        candidates.append((capacity_severity, capacity_reason))
    if growth_severity is not None:
        candidates.append((growth_severity, growth_reason))

    if not candidates:
        return None

    # Sort by severity rank descending; pick first
    candidates.sort(key=lambda c: -c[0].rank())
    chosen_severity, chosen_reason = candidates[0]

    # If both fired, mention both in reason — operator sees the whole picture
    if len(candidates) > 1:
        chosen_reason = " | ".join(c[1] for c in candidates)

    return DiskAlert(
        severity=chosen_severity,
        reason=chosen_reason,
        disk_pct_used=disk_pct_used,
        disk_free_gb=disk_free_gb,
        db_size_mb=db_size_mb,
        growth_pct_24h=growth_pct_24h,
    )


# ─── I/O: sample collection (uses shutil + os, side-effects allowed) ──────────


def sample_disk_state(db_path: str, mount_path: str = "/") -> dict:
    """Return current disk + DB sample. Pure I/O wrapper, no logic.

    Args:
        db_path: path to the analytics.duckdb file.
        mount_path: the filesystem to measure (defaults to root).

    Returns:
        {sampled_at, db_size_mb, disk_pct_used, disk_free_gb}.
    """
    import os
    import shutil
    from datetime import timezone

    db_size_mb = 0.0
    if os.path.exists(db_path):
        db_size_mb = os.path.getsize(db_path) / (1024 ** 2)

    total, used, free = shutil.disk_usage(mount_path)
    return {
        "sampled_at": datetime.now(timezone.utc),
        "db_size_mb": round(db_size_mb, 2),
        "disk_pct_used": round(100.0 * used / total, 2) if total else 0.0,
        "disk_free_gb": round(free / (1024 ** 3), 2),
    }


# ─── Persistence (writes to / reads from disk_samples) ────────────────────────


def insert_sample(conn, sample: dict) -> None:
    """Persist a sample to disk_samples. Single INSERT, cheap."""
    conn.execute(
        "INSERT INTO disk_samples (sampled_at, db_size_mb, disk_pct_used, disk_free_gb) "
        "VALUES (?, ?, ?, ?)",
        [sample["sampled_at"], sample["db_size_mb"],
         sample["disk_pct_used"], sample["disk_free_gb"]],
    )


def fetch_sample_at_age(conn, hours: int = 24, slack_hours: int = 2) -> Optional[dict]:
    """Return the sample taken closest to `hours` ago.

    Looks for samples within [hours-slack_hours, hours+slack_hours]. This
    handles missed runs (e.g. job didn't fire exactly 24h ago because the
    scheduler was down for a deploy) without going stale by too much.

    Returns None when no sample exists in window — caller should skip
    growth check (bootstrap behaviour).
    """
    from datetime import timedelta, timezone
    now = datetime.now(timezone.utc)
    target = now - timedelta(hours=hours)
    lo = target - timedelta(hours=slack_hours)
    hi = target + timedelta(hours=slack_hours)
    row = conn.execute("""
        SELECT sampled_at, db_size_mb, disk_pct_used, disk_free_gb
        FROM disk_samples
        WHERE sampled_at BETWEEN ? AND ?
        ORDER BY ABS(EXTRACT(EPOCH FROM (sampled_at - ?)))
        LIMIT 1
    """, [lo, hi, target]).fetchone()
    if not row:
        return None
    return {
        "sampled_at": row[0],
        "db_size_mb": float(row[1]),
        "disk_pct_used": float(row[2]),
        "disk_free_gb": float(row[3]),
    }


def prune_old_samples(conn, retention_days: int = 14) -> int:
    """Delete samples older than retention_days. Tiny table; cheap to clean."""
    from datetime import timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    result = conn.execute(
        "DELETE FROM disk_samples WHERE sampled_at < ? RETURNING sampled_at",
        [cutoff],
    ).fetchall()
    return len(result)
