"""What the host's backup scripts last proved, read from the markers they leave.

Chain 3 moves the orders — the one table every page reads — to Postgres with
no DuckDB copy behind them, and OD-13 (a) made its flip wait for evidence that
Postgres can be got back: a PITR drill that passed, Postgres dumps fresh off
this host, and the off-site copy restored. Until 2026-10-01 only the shipment
left anything the application could read. The drills printed PASS to a log
nobody reads.

One off-site provider, on purpose: the owner cancelled OD-01 (c), the second
provider, on 2026-10-01 and accepted the risk in writing. Nothing here counts
providers.

Each script now writes one line on success, and only on success, through a
temporary file and a rename (`deploy/`):

    data/.pg_offsite_last_ok                pg_offsite.sh, after verification
        2026-10-01T07:41:00Z
    data/.pg_pitr_drill_last_ok             pg_pitr_drill.sh, after the checks
        2026-09-28T08:44:12Z base=<stamp>
    data/.pg_restore_drill_remote_last_ok   pg_restore_drill.sh --from-remote
        2026-09-28T08:21:05Z

Read from `./data`, which both containers mount, without opening a database:
the flip's precondition (`core.pg_orders_write.unmet_precondition`) is asked
through the write-chain registry by `/api/health`, which must answer with
Postgres down. The time is the line's own; a file whose text cannot be read
falls back to its mtime, `chain_latch._stamp_of`'s rule, since the file exists
only because the script got that far.

Ages only leave this module for `/api/health`: never a path, a host or an
account — that endpoint is public.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from core.duckdb_constants import DB_DIR

logger = logging.getLogger(__name__)

OFFSITE = ".pg_offsite_last_ok"
PITR_DRILL = ".pg_pitr_drill_last_ok"
REMOTE_DRILL = ".pg_restore_drill_remote_last_ok"

# `deploy/offsite_check.sh` alarms on the Postgres marker past 36 hours: one
# missed nightly run plus slack. The flip reads the same limit, so the two
# never disagree about whether last night's copy counts.
OFFSITE_MAX_AGE_H = 36
# Both drills run on Mondays (07:20 and 08:40). A week and a day is one drill
# plus the day it can slip by; the owner's question Q5 in the chain 3 design.
DRILL_MAX_AGE_H = 8 * 24


@dataclass(frozen=True)
class Marker:
    name: str
    at: datetime
    labels: Dict[str, str]

    def age_h(self, now: datetime) -> float:
        return (now - self.at).total_seconds() / 3600


def _parse(path: Path) -> Optional[Marker]:
    """One marker, or None when there is no such file. Never raises."""
    try:
        if not path.is_file():
            return None
        text = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.warning("backup evidence: %s is unreadable (%s)", path.name, exc)
        return None
    head, *rest = text.split() or [""]
    labels = dict(part.split("=", 1) for part in rest if "=" in part)
    try:
        at = datetime.fromisoformat(head.replace("Z", "+00:00"))
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        except OSError:
            return None
    return Marker(name=path.name, at=at, labels=labels)


def _directory(base: Optional[Path]) -> Path:
    return base if base is not None else DB_DIR


def read_markers(base: Optional[Path] = None) -> Tuple[Optional[Marker], Optional[Marker],
                                                      Optional[Marker]]:
    """`(pitr drill, remote drill, off-site shipment)`. Never raises; a marker
    that cannot be read is None, which every caller takes as evidence
    missing."""
    root = _directory(base)
    return _parse(root / PITR_DRILL), _parse(root / REMOTE_DRILL), _parse(root / OFFSITE)


def unmet(now: Optional[datetime] = None, base: Optional[Path] = None) -> List[str]:
    """Each piece of chain 3's backup evidence that does not hold, as a key
    and a sentence. Empty when all of it does. Never raises. No path beyond
    the marker's own name and no host appears: these sentences reach
    `/api/health` through the registry's `unmet_precondition`, and that
    endpoint is public."""
    now = now or datetime.now(timezone.utc)
    pitr, remote, offsite = read_markers(base)
    out: List[str] = []

    if pitr is None:
        out.append("pitr_drill: no PITR drill has passed on this host "
                   "(deploy/pg_pitr_drill.sh leaves data/.pg_pitr_drill_last_ok)")
    elif pitr.age_h(now) > DRILL_MAX_AGE_H:
        out.append(f"pitr_drill: the last PITR drill to pass is "
                   f"{pitr.age_h(now) / 24:.1f} days old (limit "
                   f"{DRILL_MAX_AGE_H // 24})")

    if offsite is None:
        out.append("pg_offsite: no Postgres dump has left this host "
                   "(deploy/pg_offsite.sh leaves data/.pg_offsite_last_ok)")
    elif offsite.age_h(now) > OFFSITE_MAX_AGE_H:
        out.append(f"pg_offsite: the last Postgres dump shipped off this host "
                   f"is {offsite.age_h(now):.0f} h old (limit {OFFSITE_MAX_AGE_H})")

    if remote is None:
        out.append("remote_restore: the off-site copy has never been restored "
                   "(deploy/pg_restore_drill.sh --from-remote)")
    elif remote.age_h(now) > DRILL_MAX_AGE_H:
        out.append(f"remote_restore: the off-site copy was last restored "
                   f"{remote.age_h(now) / 24:.1f} days ago (limit "
                   f"{DRILL_MAX_AGE_H // 24})")
    return out


def published(now: Optional[datetime] = None, base: Optional[Path] = None) -> Dict[str, object]:
    """Ages in hours, for `/api/health`. Never raises. No path, host or
    label: the endpoint is public."""
    now = now or datetime.now(timezone.utc)
    pitr, remote, offsite = read_markers(base)

    def age(marker: Optional[Marker]) -> Optional[float]:
        return round(marker.age_h(now), 1) if marker else None

    return {
        "pitr_drill_age_h": age(pitr),
        "remote_restore_age_h": age(remote),
        "pg_offsite_age_h": age(offsite),
    }
