#!/usr/bin/env python3
"""Watch the floor the weekly compact leaves behind.

Twelve consecutive compacts left the database at 66, 67, 69, 70, 71, 72, 74, 73,
74, 76, 81 MB and the disk at 20, 20, 22, 23, 23, 24, 24, 25, 25, 26 %. Then one
week put the disk floor at 58. Nothing watched either number, although the
compact has printed both to its own log every Sunday since May.

That is the cheapest regression detector this system has available, and it was
lying on the floor. A post-compact size is the database with every dead byte
removed — no sawtooth, no MVCC noise, nothing periodic left to cancel. If it
moves, something is being *retained* that was not retained before, which is a
different and more alarming statement than "the database grew this week".

Compared against the trailing median rather than a fixed number, because the
floor drifts upward with the business and a constant would need re-deriving
every few months — which, on the evidence of this codebase, means it would not
be. Ratios travel; budgets do not.

Runs on the host from weekly_compact.sh, straight after the swap. Stdlib only:
it must work with the host's python3 and no virtualenv.

Usage:  compact_floor_check.py /var/log/keycrm-compact.log
Exit 0 quiet · 1 concerning (message on stdout for the caller to send)
"""
import re
import sys
from statistics import median

# A floor 25% above the trailing median is drift; 50% is a step. Derived from
# the twelve-week record above, where the largest honest week-over-week move
# was 76 → 81 MB (+6.6%) and the regression the detector is for was +32 points
# of disk in one week.
DB_WARN_RATIO = 1.25
DB_CRITICAL_RATIO = 1.50
# Absolute backstop: the floor has never been near this, and a ratio alone
# cannot catch slow inflation that drags the median along with it.
DB_CRITICAL_MB = 250.0

# Disk floor moves in points, not ratios — it is already a percentage.
DISK_WARN_POINTS = 3.0
DISK_CRITICAL_POINTS = 6.0

# Enough history for a median to mean something, short enough to follow real
# drift rather than average it away.
WINDOW = 8

_DONE = re.compile(
    r"Done:\s*\S+\s*→\s*(?P<size>[\d.]+)(?P<unit>[KMGT])\s*\|\s*"
    r"disk:\s*\d+%\s*→\s*(?P<disk>\d+)%"
)
_UNITS = {"K": 1 / 1024, "M": 1.0, "G": 1024.0, "T": 1024.0 * 1024}


def parse_floors(log_text: str):
    """Return [(db_floor_mb, disk_floor_pct), ...] oldest first.

    Reads the compact's own summary line. Deliberately not a new datastore: the
    number has been recorded every week for months, and the failure was never
    that it went unrecorded.
    """
    out = []
    for m in _DONE.finditer(log_text):
        out.append((
            float(m.group("size")) * _UNITS[m.group("unit")],
            float(m.group("disk")),
        ))
    return out


def evaluate_floors(history, window: int = WINDOW):
    """Judge the newest floor against the ones before it.

    Args:
        history: [(db_mb, disk_pct), ...] oldest first, newest last.
        window: how many prior compacts form the baseline.

    Returns:
        (severity, message) with severity in {None, "WARN", "CRITICAL"}.
    """
    if len(history) < 3:
        # Two points cannot say what is normal, and inventing a verdict from
        # them is how a threshold ends up calibrated against noise.
        return None, "not enough history yet"

    db_now, disk_now = history[-1]
    prior = history[-(window + 1):-1]
    db_median = median(d for d, _ in prior)
    disk_prev = prior[-1][1]

    problems = []
    severity = None

    def raise_to(level):
        nonlocal severity
        if level == "CRITICAL" or severity is None:
            severity = level

    if db_now >= DB_CRITICAL_MB:
        problems.append(
            f"post-compact DB floor {db_now:,.0f} MB, over the {DB_CRITICAL_MB:,.0f} MB line"
        )
        raise_to("CRITICAL")
    elif db_median > 0 and db_now / db_median >= DB_CRITICAL_RATIO:
        problems.append(
            f"post-compact DB floor {db_now:,.0f} MB is {db_now / db_median:.2f}x "
            f"the {len(prior)}-week median ({db_median:,.0f} MB)"
        )
        raise_to("CRITICAL")
    elif db_median > 0 and db_now / db_median >= DB_WARN_RATIO:
        problems.append(
            f"post-compact DB floor {db_now:,.0f} MB is {db_now / db_median:.2f}x "
            f"the {len(prior)}-week median ({db_median:,.0f} MB)"
        )
        raise_to("WARN")

    disk_delta = disk_now - disk_prev
    if disk_delta >= DISK_CRITICAL_POINTS:
        problems.append(
            f"post-compact disk floor {disk_prev:.0f}% → {disk_now:.0f}% "
            f"(+{disk_delta:.0f} points in one week)"
        )
        raise_to("CRITICAL")
    elif disk_delta >= DISK_WARN_POINTS:
        problems.append(
            f"post-compact disk floor {disk_prev:.0f}% → {disk_now:.0f}% "
            f"(+{disk_delta:.0f} points in one week)"
        )
        raise_to("WARN")

    if severity is None:
        return None, (
            f"floors steady: DB {db_now:,.0f} MB "
            f"(median {db_median:,.0f}), disk {disk_now:.0f}%"
        )

    # A floor that rose means something is now permanently retained. Say that,
    # because "the database grew" is the conclusion the last incident reached
    # and it sent the reader to the wrong place.
    return severity, (
        "; ".join(problems)
        + ". A floor that rises means something is being retained that was not "
          "before — look at what is in the data directory, not at the database."
    )


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: compact_floor_check.py <compact.log>", file=sys.stderr)
        sys.exit(2)
    try:
        with open(sys.argv[1], "r", errors="replace") as fh:
            history = parse_floors(fh.read())
    except OSError as exc:
        print(f"cannot read compact log: {exc}", file=sys.stderr)
        sys.exit(2)

    severity, message = evaluate_floors(history)
    print(f"{severity or 'OK'}: {message}")
    sys.exit(1 if severity else 0)


if __name__ == "__main__":
    main()
