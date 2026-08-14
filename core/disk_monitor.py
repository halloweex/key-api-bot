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


# ─── Growth: the whole data directory, attributed by path ────────────────────
#
# This is the replacement promised when the percentage rule was deleted, and it
# differs from it in the two ways that mattered.
#
# **It measures the directory, not one file.** In August 2026 a change put 27 GB
# of backup copies next to the database. The old check sampled `analytics.duckdb`
# alone, so the growth was invisible to it while the disk percentage it caused
# was blamed on the database — the alert named the one component that had not
# changed. An alert that cannot say *what* grew is worse than none, because it
# sends the reader somewhere specific and wrong.
#
# **It compares at the signal's own period.** The compact runs weekly, so the
# sawtooth is cron-locked to 168 hours. Differencing at exactly that lag cancels
# everything periodic and leaves only trend, which is the quantity anyone
# actually wants. That is why this survived a change of retention policy while
# a threshold derived from a fixed disk budget did not: ratios to a trailing
# window and differences at the signal's period travel; budgets do not.

# Path groups. Deliberately coarse — the point is to name a culprit, not to
# itemise. `other` is the catch-all that makes the parts sum to the total, so a
# newcomer cannot hide by not having a group.
def _classify_path(name: str) -> str:
    if name == "analytics.duckdb":
        return "live_db"
    if name.startswith("analytics.duckdb.wal"):
        return "wal"
    if name == "analytics.duckdb.old":
        return "old"
    if name == "backups":
        return "backups"
    if name == "export_parquet":
        return "export_parquet"
    return "other"


# Week-over-week drift on a healthy system is business growth: pre-compact peaks
# moved 3.3 → 4.4 GB over ten weeks, and under the current retention the
# residual footprint drift is ≈ +0.22 GB/week. These are 3.4x and 9x that.
#
# Sized to catch a step change, not a trend. The 2026-08-05 event was +8.93 GB
# in an afternoon: 12x the CRITICAL line, detected at the next 6-hourly sample
# rather than 108 hours later.
WARN_GROWTH_GB_168H = 0.75
CRITICAL_GROWTH_GB_168H = 2.0

# Until a week of history exists there is nothing to difference against, and a
# detector that says nothing for its first seven days is a detector that is
# absent exactly when a fresh deploy is most likely to regress. So: any single
# group gaining this much between two consecutive 6-hourly samples is a step
# change regardless of how much history we have.
BOOTSTRAP_STEP_GB = 1.0

_GB = 1024 ** 3


@dataclass(frozen=True)
class GrowthAlert:
    """Growth in the data directory, with the path that caused it named."""
    severity: Severity
    reason: str
    total_delta_gb: float
    top_group: str
    top_delta_gb: float
    window_hours: int


def evaluate_dir_growth(
    *,
    current: dict,
    baseline: Optional[dict],
    window_hours: int = 168,
    warn_gb: float = WARN_GROWTH_GB_168H,
    critical_gb: float = CRITICAL_GROWTH_GB_168H,
) -> Optional[GrowthAlert]:
    """Compare two per-group byte counts and return an alert, or None.

    Args:
        current: {group: bytes} now.
        baseline: {group: bytes} one window ago, or None when history is short.
        window_hours: the lag the baseline was taken at. Passed in rather than
            assumed so the bootstrap path can reuse this with a 6h window.
        warn_gb / critical_gb: thresholds for this window.

    Returns:
        GrowthAlert when the total grew past a threshold, else None. Shrinkage
        is never an alert: that is the compact, or a prune, doing its job.
    """
    if not baseline:
        return None

    deltas = {
        group: (current.get(group, 0) - baseline.get(group, 0))
        for group in set(current) | set(baseline)
    }
    total = sum(deltas.values())
    total_gb = total / _GB

    if total_gb >= critical_gb:
        severity, threshold = Severity.CRITICAL, critical_gb
    elif total_gb >= warn_gb:
        severity, threshold = Severity.WARN, warn_gb
    else:
        return None

    # Name the culprit. Without this the alert is only a number, and a number
    # sent the last reader to the wrong component for a week.
    top_group, top_delta = max(deltas.items(), key=lambda kv: kv[1])
    movers = ", ".join(
        f"{g} {d / _GB:+.2f} GB"
        for g, d in sorted(deltas.items(), key=lambda kv: -kv[1])
        if abs(d) >= 0.05 * _GB
    )

    return GrowthAlert(
        severity=severity,
        reason=(
            f"data dir grew {total_gb:+.2f} GB in {window_hours}h "
            f"(>= {threshold:.2f} GB); mostly {top_group} "
            f"{top_delta / _GB:+.2f} GB" + (f" [{movers}]" if movers else "")
        ),
        total_delta_gb=round(total_gb, 3),
        top_group=top_group,
        top_delta_gb=round(top_delta / _GB, 3),
        window_hours=window_hours,
    )


def sample_data_dir(data_dir: str) -> dict:
    """Bytes per path group for the whole data directory.

    One os.scandir plus a walk of two subdirectories — cheap enough for a
    6-hourly job, and it counts what `du` counts rather than what one
    os.path.getsize call happens to see.
    """
    import os

    totals: dict = {}
    try:
        entries = list(os.scandir(data_dir))
    except (FileNotFoundError, PermissionError):
        return {}

    for entry in entries:
        group = _classify_path(entry.name)
        size = 0
        try:
            if entry.is_dir(follow_symlinks=False):
                for root, _dirs, files in os.walk(entry.path):
                    for f in files:
                        try:
                            size += os.stat(os.path.join(root, f)).st_size
                        except OSError:
                            continue
            else:
                size = entry.stat(follow_symlinks=False).st_size
        except OSError:
            continue
        totals[group] = totals.get(group, 0) + size

    return totals


def insert_dir_samples(conn, totals: dict, sampled_at=None) -> None:
    """Persist one row per path group."""
    from datetime import timezone
    if sampled_at is None:
        sampled_at = datetime.now(timezone.utc)
    for group, size in totals.items():
        conn.execute(
            "INSERT INTO data_dir_samples (sampled_at, path_group, bytes) VALUES (?, ?, ?)",
            [sampled_at, group, int(size)],
        )


def fetch_dir_sample_at_age(conn, hours: int = 168, slack_hours: int = 12) -> Optional[dict]:
    """Per-group bytes from the sample set closest to `hours` ago.

    Rows share a `sampled_at`, so this finds the nearest timestamp inside the
    window and returns that whole set — never a mix of two runs, which would
    difference groups against different moments and invent growth.
    """
    from datetime import timedelta, timezone
    now = datetime.now(timezone.utc)
    target = now - timedelta(hours=hours)
    lo, hi = target - timedelta(hours=slack_hours), target + timedelta(hours=slack_hours)
    row = conn.execute(
        "SELECT sampled_at FROM data_dir_samples WHERE sampled_at BETWEEN ? AND ? "
        "ORDER BY ABS(EXTRACT(EPOCH FROM (sampled_at - ?))) LIMIT 1",
        [lo, hi, target],
    ).fetchone()
    if not row:
        return None
    rows = conn.execute(
        "SELECT path_group, bytes FROM data_dir_samples WHERE sampled_at = ?", [row[0]]
    ).fetchall()
    return {g: int(b) for g, b in rows}


def prune_old_dir_samples(conn, retention_days: int = 21) -> int:
    """Delete samples older than retention_days.

    Longer than disk_samples keeps: differencing at a 168h lag needs a week of
    history to still be there, plus slack for a scheduler that missed runs.
    """
    from datetime import timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    return len(conn.execute(
        "DELETE FROM data_dir_samples WHERE sampled_at < ? RETURNING sampled_at", [cutoff],
    ).fetchall())
