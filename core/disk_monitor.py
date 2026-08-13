"""Disk capacity watchdog.

Pages when the filesystem is filling: WARN at 75% used, CRITICAL at 90%.
The scheduler job also samples (db_size, disk_pct) every 6 h into
`disk_samples`; nothing pages on that history yet, and it must keep
accruing anyway — see below.

**There is deliberately no percentage-growth alert here.** There was one,
and it could not work. The weekly compact (`scripts/weekly_compact.sh`,
Sunday 02:00 UTC) rebuilds the DB from Parquet and drops it to ~80 MB, so
the 24h-ago baseline resets every week. Against a baseline that small a
perfectly healthy +620 MB/day reads as +456% on Monday and +33.7% by
Wednesday: the same measured rate, with a deceleration manufactured
entirely by the denominator. Ten consecutive real samples in August 2026
varied by ±6% as MB/day and by 1350% as reported percentages.

To read below the old 10% WARN line the baseline had to exceed 6300 MB.
The weekly pre-compact peak is 4.4 GB. So the check could not return "OK"
at any point in any week — 15 CRITICAL, 9 WARN and 0 quiet evaluations
per week, by construction, forever. The better the compact worked, the
lower the floor and the louder the false alarm. It ran that way for
weeks, and when a real regression finally arrived it named the database
as the cause — while what had actually consumed the space was a pile of
files alongside it that `sample_disk_state` does not look at.

Its replacement is absolute growth (MB/day) measured over the whole data
directory with per-path attribution, so an alert can name what grew. That
is not written yet: its thresholds depend on the backup retention design,
which is changing. Until then this module measures the 24h delta and logs
it (`BackgroundScheduler._run_disk_watchdog`) but pages on capacity only.

The evaluator is pure (no I/O). Sample storage + scheduler job live
elsewhere; this file owns only the contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.data_quality import Severity


# Thresholds — tuned for the 75 GB Hetzner host.
#
# 90% of 75 GB leaves 7.5 GB free, and the compact needs
# `source × 0.50 + 1.0 GB` (compact_duckdb.compute_disk_requirements) —
# ~3.2 GB at the current 4.4 GB weekly peak. So CRITICAL still leaves room
# to compact our way out.
#
# An earlier comment here justified the 90% line against an empirical
# "compact preflight needs 22 GB free". That number was
# compute_disk_requirements(43.0): correct for the 43 GB database of April
# 2026, wrong by 7x for the one we have, and never re-derived.
WARN_DISK_PCT = 75.0
CRITICAL_DISK_PCT = 90.0


@dataclass(frozen=True)
class DiskAlert:
    """A filesystem capacity concern, ready to page on."""
    severity: Severity
    reason: str
    disk_pct_used: float
    disk_free_gb: float
    db_size_mb: float


def evaluate_disk_capacity(
    *,
    disk_pct_used: float,
    disk_free_gb: float,
    db_size_mb: float,
) -> Optional[DiskAlert]:
    """Return a DiskAlert if the filesystem is filling, else None.

    Args:
        disk_pct_used: 0.0-100.0, current % of filesystem used.
        disk_free_gb: free GB on the same filesystem.
        db_size_mb: current DuckDB file size in MB. Carried into the alert
            for context only — it is never a trigger. The DB is not the
            only thing on this disk, and in August 2026 it was not the
            thing that filled it.

    Returns:
        DiskAlert when severity >= WARN, else None.
    """
    if disk_pct_used >= CRITICAL_DISK_PCT:
        severity, threshold, label = Severity.CRITICAL, CRITICAL_DISK_PCT, "critical"
    elif disk_pct_used >= WARN_DISK_PCT:
        severity, threshold, label = Severity.WARN, WARN_DISK_PCT, "warn"
    else:
        return None

    return DiskAlert(
        severity=severity,
        reason=(
            f"disk {disk_pct_used:.1f}% used (>= {threshold:.0f}% "
            f"{label}); {disk_free_gb:.1f} GB free"
        ),
        disk_pct_used=disk_pct_used,
        disk_free_gb=disk_free_gb,
        db_size_mb=db_size_mb,
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
