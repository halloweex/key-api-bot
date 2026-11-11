"""The one door to the two report ledgers: the gate before a send, the record after.

Chains 11a and 11b (`core/pg_weekly_ledger_write.py`,
`core/pg_traffic_ledger_write.py`, OD-02 (c)) keep `weekly_report_sends` and
`traffic_report_sends` in Postgres. Both reports ask `already_sent` before
they build anything and call `mark_sent` after a delivery;
`tests/unit/test_report_ledger.py` walks the tree for a ledger read or write
that goes round this module.

AT LEAST ONCE, NEVER TWICE (OD-16 (a))

- **The record after delivery is retried, then spooled.** `sent_at` is taken
  once, right after the delivery, and carried through every attempt: three
  tries within ~12 s, then a file under `data/report-ledger-pending/<chain>/`,
  written the way the latch marker is (temp, fsync, rename, fsync the
  directory). The job's result says `pending`; `/api/health` publishes the
  spooled weeks and the canary warns `report_ledger_pending`.
- **The gate counts a spooled week as sent**, and drains the spool first — a
  drained entry lands in Postgres with its original `sent_at`. The spool's
  path and format are the ones every image before step 14 reads too, so a
  rollback to one drains what this build spooled.
- **Postgres unreachable at the gate raises**: the job fails, nothing is sent,
  and tomorrow's tick asks again.

The residual, named: a delivery after which Postgres refuses three times
**and** the spool cannot be written raises from the job, and the next tick
sends again.

ONE STORE SINCE STEP 14'S S14-10

Until S14-10 a recorded row was also handed to DuckDB as a shadow
(`core/shadow_writes.py`), the gate adopted a week only DuckDB held, and a
chain left at `duckdb` gated and recorded on DuckDB alone, counting a
still-spooled week as sent. Since S14-00 none of it ran in web, which runs
`KS_DUCKDB=off` — the adoption was skipped there, the shadow stood down, and
every chain wrote Postgres — and S14-10 deleted all three, with the `store`
the two doors took and `Ledger.report_module`, the report module whose DuckDB
statements they ran.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Sequence

from core.paths import DATA_DIR

logger = logging.getLogger(__name__)

# Beside the latch markers, for their reason: `./data` is the directory both
# containers mount and the web container can write.
SPOOL_DIR: Path = DATA_DIR / "report-ledger-pending"

# Seconds before each attempt after the first — ~12 s in all, inside the job.
RETRY_DELAYS: Sequence[float] = (0, 2, 10)

# One spelling of each Postgres statement for both ledgers.
UPSERT_SQL = (
    "INSERT INTO {table} (week_start, sales_type, revenue, orders, sent_at) "
    "VALUES ($1, $2, $3, $4, $5) "
    "ON CONFLICT (week_start, sales_type) DO UPDATE SET "
    "revenue = EXCLUDED.revenue, orders = EXCLUDED.orders, "
    "sent_at = EXCLUDED.sent_at"
)
FIND_SQL = (
    "SELECT week_start, sales_type, revenue, orders, sent_at FROM {table} "
    "WHERE week_start = $1 AND sales_type = $2"
)


@dataclass(frozen=True)
class Ledger:
    """One report's ledger: the chain module that writes it in Postgres."""
    chain_module: str

    def chain(self) -> ModuleType:
        return importlib.import_module(self.chain_module)


WEEKLY = Ledger("core.pg_weekly_ledger_write")
TRAFFIC = Ledger("core.pg_traffic_ledger_write")
LEDGERS = (WEEKLY, TRAFFIC)


def check_row(week_start: date, sales_type: str, revenue: Any, orders: Any,
              sent_at: Any) -> None:
    """Refuse, before any latch, a row Postgres would store as something else
    than it was handed: a naive `sent_at` (asyncpg reads it as UTC — a Kyiv
    wall clock would land three hours off), a revenue that is not a Decimal of
    two places, an order count that is not an int."""
    if not isinstance(week_start, date) or isinstance(week_start, datetime):
        raise ValueError(f"week_start={week_start!r} is not a date")
    if not isinstance(sales_type, str) or not sales_type:
        raise ValueError(f"sales_type={sales_type!r} is not a sales type")
    if not isinstance(revenue, Decimal) or revenue != revenue.quantize(Decimal("0.01")):
        raise ValueError(f"revenue={revenue!r} is not a Decimal of two places")
    if isinstance(orders, bool) or not isinstance(orders, int):
        raise ValueError(f"orders={orders!r} is not an int")
    if not isinstance(sent_at, datetime) or sent_at.tzinfo is None:
        raise ValueError(f"sent_at={sent_at!r} is not an aware datetime")


def money(revenue: float) -> Decimal:
    """Two places, rounded once here, so the row stores the number the report
    printed rather than whatever Postgres' float→NUMERIC makes of a half-cent
    (one rounding for DuckDB's shadow and Postgres alike until S14-10)."""
    return Decimal(str(round(float(revenue), 2)))


# ─── The spool ───────────────────────────────────────────────────────────────


def _spool_dir(chain_name: str) -> Path:
    return SPOOL_DIR / chain_name


def _spool_path(chain_name: str, week_start: date, sales_type: str) -> Path:
    return _spool_dir(chain_name) / f"{week_start.isoformat()}__{sales_type}.json"


def spool(chain_name: str, week_start: date, sales_type: str, revenue: Decimal,
          orders: int, sent_at: datetime) -> Path:
    """Write the pending row durably. Raises if it cannot."""
    from core.atomic_file import write_atomic

    path = _spool_path(chain_name, week_start, sales_type)
    write_atomic(path, json.dumps({
        "week_start": week_start.isoformat(), "sales_type": sales_type,
        "revenue": str(revenue), "orders": int(orders),
        "sent_at": sent_at.isoformat(),
    }))
    return path


def _spooled(chain_name: str) -> List[Dict[str, Any]]:
    out = []
    folder = _spool_dir(chain_name)
    if not folder.is_dir():
        return out
    for path in sorted(folder.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            out.append({
                "path": path,
                "week_start": date.fromisoformat(data["week_start"]),
                "sales_type": data["sales_type"],
                "revenue": Decimal(data["revenue"]),
                "orders": int(data["orders"]),
                "sent_at": datetime.fromisoformat(data["sent_at"]),
            })
        except (OSError, ValueError, KeyError) as exc:
            # Unreadable is still pending: the week went out. The gate counts
            # it by its name; the drain leaves it for a human.
            logger.error("report ledger: %s is unreadable (%s)", path, exc)
            out.append({"path": path, "unreadable": True})
    return out


def is_spooled(chain_name: str, week_start: date, sales_type: str) -> bool:
    return _spool_path(chain_name, week_start, sales_type).exists()


def pending_of(chain_name: str) -> Dict[str, Any]:
    """`{count, weeks, unreadable}` of spooled entries — what `/api/health`
    publishes. `unreadable` names the files no drain will land: a delivered
    week whose row a human writes."""
    entries = _spooled(chain_name)
    return {"count": len(entries),
            "weeks": sorted(e["path"].stem for e in entries),
            "unreadable": sorted(e["path"].stem for e in entries
                                 if e.get("unreadable"))}


async def drain(ledger: Ledger) -> int:
    """Land every spooled row in Postgres, then delete the file. Never
    raises; returns how many landed."""
    chain = ledger.chain()
    landed = 0
    for entry in _spooled(chain.CHAIN):
        if entry.get("unreadable"):
            continue
        try:
            await chain.mark_sent(entry["week_start"], entry["sales_type"],
                                  entry["revenue"], entry["orders"], entry["sent_at"])
        except Exception as exc:  # noqa: BLE001 — stays spooled for the next tick
            logger.warning("report ledger: %s still pending (%s)",
                           entry["path"].name, type(exc).__name__)
            continue
        try:
            entry["path"].unlink()
        except OSError as exc:
            logger.error("report ledger: landed %s and could not remove it: %s",
                         entry["path"], exc)
            continue
        landed += 1
        logger.info("report ledger: drained %s into Postgres", entry["path"].name)
    return landed


# ─── The gate and the record ─────────────────────────────────────────────────


async def already_sent(ledger: Ledger, week_start: date, sales_type: str) -> bool:
    """Has this week gone out? Asked before anything is built.

    Each report's job runs daily and reports the last *complete* week, so six
    of seven firings find their week already sent and go quiet. That is the
    design: a weekly CronTrigger that misses its instant does not run late,
    it does not run at all — the next fire is computed a week out, and
    misfire grace has nothing to forgive. A daily tick plus this ledger turns
    a missed Monday into a Tuesday delivery instead of a silent gap.

    The spool is drained first, and a week still spooled counts as sent: it
    went out, and only its row is late. Then Postgres answers, and a Postgres
    that cannot raises — the job fails and nothing is sent.
    """
    chain = ledger.chain()
    await drain(ledger)
    if is_spooled(chain.CHAIN, week_start, sales_type):
        logger.info("Report ledger: %s/%s delivered, its row still spooled",
                    week_start, sales_type)
        return True
    return await chain.find_sent(week_start, sales_type) is not None


async def mark_sent(ledger: Ledger, week_start: date, sales_type: str,
                    revenue: float, orders: int) -> str:
    """Record a delivery that happened. `recorded`, or `pending` when the row
    is spooled for the next tick to land."""
    chain = ledger.chain()
    sent_at = datetime.now(timezone.utc)
    amount = money(revenue)
    orders = int(orders)
    for delay in RETRY_DELAYS:
        if delay:
            await asyncio.sleep(delay)
        try:
            await chain.mark_sent(week_start, sales_type, amount, orders, sent_at)
        except Exception as exc:  # noqa: BLE001 — retried, then spooled
            logger.warning("Report ledger: recording %s/%s failed (%s)",
                           week_start, sales_type, type(exc).__name__)
            continue
        return "recorded"
    path = spool(chain.CHAIN, week_start, sales_type, amount, orders, sent_at)
    logger.error("Report ledger: %s/%s delivered and not recorded; spooled to %s "
                 "for the next tick", week_start, sales_type, path)
    return "pending"
