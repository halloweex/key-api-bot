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

Its replacement is absolute growth measured over a whole directory with
per-path attribution, so an alert can name what grew. That is written and
live — `evaluate_dir_growth` below, differenced at the compact's own 168h
period so the sawtooth cancels and only trend survives.

It reaches past the directory as well. Only `./data` and `./logs` are
mounted into this container, so three of the largest things on the host
are unwalkable from here; `statvfs` still sees the filesystem, and the
shortfall between it and the walked groups is carried as `unattributed`
with thresholds of its own. See FS_WARN_GROWTH_GB_168H.

The evaluator is pure (no I/O). Sample storage + scheduler job live
elsewhere; this file owns only the contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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
        {sampled_at, db_size_mb, disk_pct_used, disk_free_gb, disk_used_bytes}.
        `disk_used_bytes` is not persisted in `disk_samples` — it is what
        `sample_data_dir` subtracts the walked groups from, and storing a
        second copy of the same quantity would invite the two to disagree.
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
        "disk_used_bytes": used,
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

# The rest of the filesystem, which `data/` does not contain and this container
# cannot walk. Only `./data` and `./logs` are bind-mounted, so Docker images,
# journald and `backups/` are all invisible to `sample_data_dir` — and on
# 2026-08-30 that was the entire event: ~11 GB accumulated in one week across
# Docker images and build cache from the manual deploys, an uncapped journald,
# and the new WAL archive, while every group this module could name held still.
# The weekly floor check caught it; this one could not, and a 6-hourly detector
# that reports a subset of the disk it is guarding is the same
# name-the-wrong-component failure the module docstring above is about.
#
# `statvfs` sees the whole filesystem from inside the container, so the
# unattributable remainder is measurable even when the paths are not. It gets
# its own thresholds rather than being folded into the total: this figure moves
# with ordinary deploy churn, and putting it in the same sum would raise the
# data directory's own limits to cover somebody else's noise.
#
# **Provisional, and deliberately so.** There is no history for this number yet
# — the change that adds it is what starts collecting it. Sized so the 11 GB
# week would have been CRITICAL and an ordinary deploy week stays quiet;
# re-derive once `data_dir_samples` holds a few weeks of `unattributed` rows.
FS_WARN_GROWTH_GB_168H = 2.0
FS_CRITICAL_GROWTH_GB_168H = 5.0

# Not a path — the remainder, so it can never collide with `_classify_path`.
UNATTRIBUTED = "unattributed"

# THE RE-DERIVATION THE THRESHOLDS ABOVE ASKED FOR.
#
# `FS_WARN/FS_CRITICAL` were shipped provisional with a note to re-derive them
# "once `data_dir_samples` holds a few weeks of `unattributed` rows". It now
# holds 67, and they say the numbers were never the problem — the *comparison*
# was. Measured over every 168h window the watchdog could have judged:
#
#     mean 168h delta   -0.93 GB   (the remainder SHRINKS on average)
#     sd of that delta   4.14 GB
#     sd between two consecutive 6-hourly samples   1.63 GB
#
# A 2.0 GB line under a delta whose own spread is 4.14 GB is a coin toss.
# Back-tested: 11 of 40 judgeable windows crossed WARN and 3 crossed CRITICAL,
# on a series with no upward trend at all. That is the eight `disk:WARN` fires
# between 2026-09-07 and 09-17, and it is why the alert stopped meaning
# anything.
#
# The remainder is deploy churn: images pulled and superseded, gate containers
# built and destroyed, a build cache breathing. It is genuinely noisy, and no
# amount of naming its parts makes a point-to-point difference of it stable.
# (An attribution sampler was designed for exactly that and abandoned on this
# measurement: labelling noise produces confidently-named artefacts, which are
# worse than an honest "unattributed" because people believe them.)
#
# So the remainder is judged by PERSISTENCE instead: the whole recent window
# must sit above the whole baseline window. `min(recent) - max(baseline)` is
# that sentence as arithmetic — the least favourable reading now against the
# most favourable reading a week ago. Growth survives it only by holding.
#
# Back-test of the replacement over the same 40 windows: 2 WARN, 0 CRITICAL —
# and both survivors are 2026-09-08/09, the anonymous-volume leak that was
# real and that this alert correctly found. Every other fire disappears.
REMAINDER_RECENT_HOURS = 24
REMAINDER_SLACK_HOURS = 12
# A window needs at least this many samples to be judged. Two is enough to
# have a spread; one is a point, which is what we are moving away from.
REMAINDER_MIN_SAMPLES = 2

# The cost of persistence is latency, and it is paid where it is cheapest.
# Measured: a sustained +2 GB step is confirmed after ~96h, +5 GB after 24h.
# For a weekly-growth detector that is the right trade — capacity (75%/90%)
# is the emergency detector and is untouched by any of this.
#
# The one case that cannot wait is the cliff: 2026-08-05 put +8.93 GB on the
# disk in an afternoon. So a single 6-hourly rise this large is CRITICAL on
# its own, with no confirmation. Calibrated against the real consecutive-sample
# noise rather than chosen: sd 1.63 GB, largest rise ever observed +3.60 GB,
# and ZERO rises >= 4 GB in 66 samples. 5.0 GB is ~3 sd and clear of the
# observed maximum, and the persistence path catches anything smaller within
# 24h anyway — so the fast path is free to be deaf and is worth nothing if it
# is not silent.
FS_CLIFF_STEP_GB = 5.0

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
    subject: str = "data dir",
) -> Optional[GrowthAlert]:
    """Compare two per-group byte counts and return an alert, or None.

    Args:
        current: {group: bytes} now.
        baseline: {group: bytes} one window ago, or None when history is short.
        window_hours: the lag the baseline was taken at. Passed in rather than
            assumed so the bootstrap path can reuse this with a 6h window.
        warn_gb / critical_gb: thresholds for this window.
        subject: what grew, for the message. The caller names it because this
            function is used twice against different halves of the disk, and a
            message that says "data dir" about the rest of the filesystem is
            the same wrong-component error the module docstring warns about.

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
            f"{subject} grew {total_gb:+.2f} GB in {window_hours}h "
            f"(>= {threshold:.2f} GB); mostly {top_group} "
            f"{top_delta / _GB:+.2f} GB" + (f" [{movers}]" if movers else "")
        ),
        total_delta_gb=round(total_gb, 3),
        top_group=top_group,
        top_delta_gb=round(top_delta / _GB, 3),
        window_hours=window_hours,
    )


def evaluate_remainder_growth(
    *,
    series,
    now=None,
    warn_gb: float = FS_WARN_GROWTH_GB_168H,
    critical_gb: float = FS_CRITICAL_GROWTH_GB_168H,
    cliff_gb: float = FS_CLIFF_STEP_GB,
    recent_hours: int = REMAINDER_RECENT_HOURS,
    window_hours: int = 168,
    slack_hours: int = REMAINDER_SLACK_HOURS,
    min_samples: int = REMAINDER_MIN_SAMPLES,
) -> Optional[GrowthAlert]:
    """Judge the unattributable remainder by whether growth *held*.

    Pure: takes the samples already read and decides. `series` is any iterable
    of `(sampled_at, bytes)`; order does not matter.

    Two paths, and they answer different questions.

    **Persistence** is the ordinary one. The whole recent window must sit above
    the whole baseline window — `min(recent) - max(baseline)` — so a spike in
    either window cannot carry the verdict on its own. This is what makes the
    remainder judgeable at all: point-to-point, its 168h delta has a spread of
    4.14 GB around a mean of -0.93, and a 2 GB line under that fires on a
    quarter of all windows with nothing happening.

    **The cliff** is the exception. Persistence needs ~24h to confirm even a
    large step, and the 2026-08-05 event put +8.93 GB down in an afternoon. A
    single sample-to-sample rise past `cliff_gb` is CRITICAL immediately. It is
    set well above the observed noise precisely so it stays quiet: a fast path
    that speaks when nothing happened costs more than the hours it saves.

    Returns None — never an alert — when either window is too thin to have a
    spread. That is the honest answer in the first days after this ships, and
    it is also what stops a fresh deploy inventing a week of growth from a
    baseline that does not exist. Shrinkage is never an alert.
    """
    rows = sorted(
        ((t, int(b)) for t, b in series if t is not None),
        key=lambda tb: tb[0],
    )
    if not rows:
        return None
    if now is None:
        now = rows[-1][0]

    recent = [b for t, b in rows if now - timedelta(hours=recent_hours) <= t <= now]
    baseline = [
        b for t, b in rows
        if (now - timedelta(hours=window_hours + slack_hours)
            <= t <=
            now - timedelta(hours=window_hours - slack_hours))
    ]
    if len(recent) < min_samples or len(baseline) < min_samples:
        return None

    held_gb = (min(recent) - max(baseline)) / _GB

    # The cliff looks only at the newest pair, which is the whole point: it is
    # asking "did the disk just move", not "has it been moving".
    step_gb = None
    tail = [b for t, b in rows if t <= now]
    if len(tail) >= 2:
        step_gb = (tail[-1] - tail[-2]) / _GB

    if step_gb is not None and step_gb >= cliff_gb:
        return GrowthAlert(
            severity=Severity.CRITICAL,
            reason=(
                f"disk outside data/ jumped {step_gb:+.2f} GB since the last "
                f"sample (>= {cliff_gb:.2f} GB); {UNATTRIBUTED} is now "
                f"{tail[-1] / _GB:.2f} GB"
            ),
            total_delta_gb=round(step_gb, 3),
            top_group=UNATTRIBUTED,
            top_delta_gb=round(step_gb, 3),
            window_hours=0,
        )

    if held_gb >= critical_gb:
        severity, threshold = Severity.CRITICAL, critical_gb
    elif held_gb >= warn_gb:
        severity, threshold = Severity.WARN, warn_gb
    else:
        return None

    return GrowthAlert(
        severity=severity,
        reason=(
            f"disk outside data/ grew {held_gb:+.2f} GB in {window_hours}h "
            f"and stayed there (>= {threshold:.2f} GB); every reading in the "
            f"last {recent_hours}h is above every reading a week ago"
        ),
        total_delta_gb=round(held_gb, 3),
        top_group=UNATTRIBUTED,
        top_delta_gb=round(held_gb, 3),
        window_hours=window_hours,
    )


def evaluate_growth(
    *,
    current: dict,
    baseline: Optional[dict],
    window_hours: int = 168,
    warn_gb: float = WARN_GROWTH_GB_168H,
    critical_gb: float = CRITICAL_GROWTH_GB_168H,
    fs_warn_gb: float = FS_WARN_GROWTH_GB_168H,
    fs_critical_gb: float = FS_CRITICAL_GROWTH_GB_168H,
    remainder_series=None,
) -> Optional[GrowthAlert]:
    """Judge the data directory and the rest of the disk, each on its own terms.

    Two comparisons rather than one sum, for the reason the thresholds carry:
    the remainder moves with deploy churn, and adding it to the total would
    mean raising the data directory's limits until they no longer catch what
    they were calibrated to catch.

    When both halves breach, both are reported — which of the two is "the"
    cause is exactly what the reader is being asked to decide, and dropping
    the quieter one has historically been how the wrong component got named.
    """
    if not baseline:
        return None

    inside_now = {g: b for g, b in current.items() if g != UNATTRIBUTED}
    inside_was = {g: b for g, b in baseline.items() if g != UNATTRIBUTED}
    outside_now = {g: b for g, b in current.items() if g == UNATTRIBUTED}
    outside_was = {g: b for g, b in baseline.items() if g == UNATTRIBUTED}

    inside = evaluate_dir_growth(
        current=inside_now, baseline=inside_was, window_hours=window_hours,
        warn_gb=warn_gb, critical_gb=critical_gb, subject="data dir",
    )
    # Only when both samples carry the remainder. A baseline taken before this
    # existed has no `unattributed` key, and treating its absence as zero would
    # report the whole disk as one week's growth on the first run after deploy.
    # The remainder is judged by persistence when the caller supplies its
    # series, and point-to-point only when it cannot — the bootstrap path with
    # a 6h window, and every existing caller that predates the series. See
    # `evaluate_remainder_growth` for why the point comparison is not
    # trustworthy on this particular quantity.
    outside = None
    if remainder_series is not None:
        outside = evaluate_remainder_growth(
            series=remainder_series, window_hours=window_hours,
            warn_gb=fs_warn_gb, critical_gb=fs_critical_gb,
        )
    elif outside_now and outside_was:
        outside = evaluate_dir_growth(
            current=outside_now, baseline=outside_was, window_hours=window_hours,
            warn_gb=fs_warn_gb, critical_gb=fs_critical_gb,
            subject="disk outside data/",
        )

    if inside is None:
        return outside
    if outside is None:
        return inside

    worst = max(inside, outside, key=lambda a: a.severity.rank())
    louder = max(inside, outside, key=lambda a: a.top_delta_gb)
    return GrowthAlert(
        severity=worst.severity,
        reason=f"{inside.reason} | {outside.reason}",
        total_delta_gb=round(inside.total_delta_gb + outside.total_delta_gb, 3),
        top_group=louder.top_group,
        top_delta_gb=louder.top_delta_gb,
        window_hours=window_hours,
    )


def evidence_for_growth(
    *,
    current: dict,
    baseline: Optional[dict],
    sample: dict,
    window_hours: int = 168,
) -> dict:
    """The whole group table, for the ledger the diagnostician can read.

    The message carries one mover and a total, because it is three lines. This
    is every group with its delta, which is the difference between "mostly
    unattributed" and knowing that nothing else moved at all.

    It does **not** try to say what inside the remainder grew — this process
    cannot see it, and pretending otherwise is how a confident wrong cause gets
    written down. Naming the remainder and proving the named groups held still
    is the honest half; the other half is the agent's `du`, which runs on the
    host.

    Pure. Bounded by construction: there are seven groups, not seven thousand.
    """
    groups = sorted(set(current) | set(baseline or {}))
    rows = []
    for g in groups:
        now_b = int(current.get(g, 0))
        was_b = int((baseline or {}).get(g, 0))
        row = {"group": g, "gb": round(now_b / _GB, 3)}
        if baseline is not None:
            row["delta_gb"] = round((now_b - was_b) / _GB, 3)
        rows.append(row)
    rows.sort(key=lambda r: -r.get("delta_gb", r["gb"]))

    payload: dict = {
        "window_hours": window_hours,
        "disk_pct_used": sample.get("disk_pct_used"),
        "disk_free_gb": sample.get("disk_free_gb"),
        "groups": rows,
    }
    if baseline is None:
        # Says why there are no deltas, rather than leaving a reader to infer
        # that nothing moved.
        payload["baseline"] = "none in window — first samples after a deploy"
    return payload


def sample_data_dir(data_dir: str, disk_used_bytes: Optional[int] = None) -> dict:
    """Bytes per path group for the whole data directory.

    One os.scandir plus a walk of two subdirectories — cheap enough for a
    6-hourly job, and it counts what `du` counts rather than what one
    os.path.getsize call happens to see.

    Args:
        data_dir: the directory to attribute.
        disk_used_bytes: used bytes for the filesystem holding it, from
            `sample_disk_state`. When given, the shortfall between it and the
            groups above is recorded as `unattributed` — everything on this
            disk that the container cannot walk. Optional so the function stays
            usable as a plain directory sizer.
    """
    import os

    totals: dict = {}
    try:
        entries = list(os.scandir(data_dir))
    except (FileNotFoundError, PermissionError):
        # No remainder either: a total with nothing to subtract from it would
        # book the entire filesystem as `unattributed` and read as a step
        # change, when all that happened is that the directory went unreadable.
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

    if disk_used_bytes is not None:
        # Clamped at zero: `du` sums apparent sizes and `statvfs` counts
        # allocated blocks, so a sparse file or a hardlink can make the parts
        # exceed the whole by a little. A negative remainder is that artefact,
        # never a fact about the disk, and it must not read as a shrink.
        totals[UNATTRIBUTED] = max(0, disk_used_bytes - sum(totals.values()))

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


def fetch_remainder_series(conn, hours: int = 180):
    """`(sampled_at, bytes)` for the remainder over the last `hours`, oldest first.

    The whole series rather than two points, because that is the difference
    this detector turns on: `evaluate_remainder_growth` needs a spread at both
    ends to tell a held rise from a spike, and 180h covers the 168h baseline
    plus its slack in one read.

    Retention caps what is available at 21 days (`prune_old_dir_samples`), so
    this can never grow unbounded.
    """
    from datetime import timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = conn.execute(
        "SELECT sampled_at, bytes FROM data_dir_samples "
        "WHERE path_group = ? AND sampled_at >= ? ORDER BY sampled_at",
        [UNATTRIBUTED, cutoff],
    ).fetchall()
    return [(ts, int(b)) for ts, b in rows]


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
