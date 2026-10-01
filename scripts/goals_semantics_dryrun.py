#!/usr/bin/env python3
"""What `KS_GOALS_HISTORY=silver` would change, measured before the flip.

    docker compose run --rm --no-deps -T web \\
        python /app/scripts/goals_semantics_dryrun.py \\
            --backup /app/data/backups/analytics-<today>.duckdb
    # exit 0 no difference · 1 differences, each with its cause · 2 refused

Chain 7b-2's flip precondition (`.planning/DUCKDB_EXIT_STAGE4_CHAINS.md` §7):
run on the flip day's backup, and flip only on exit 0.

WHAT IT ANSWERS

The goal calculators — seasonality, YoY, weekly patterns, the growth caps and
the smart goal's last-year and recent-months reads — count the order history
two ways, chosen by `KS_GOALS_HISTORY`:

- `bridge` (today): DuckDB `orders`, returns by the status-id list, the Kyiv
  date computed per row, `sales_type` from `silver_sales_type_case` rendered
  over DuckDB `managers` and `manager_classifications` (DN-12);
- `silver`: `{silver_orders}` — `is_return`, `order_date`, `sales_type` — every
  synced source (no `is_active_source`, OQ-1).

This runs the real `GoalsMixin` both ways over one copy of the backup and
reports every difference: first the orders each side counts, per sales type,
each difference filed under its cause (not in Silver, the return rule, other);
then every number the calculators answer, the two tables the Monday job stores
and the smart goal for this month and the next three, for retail, b2b and all.

WHAT IT DOES NOT ANSWER

The engine. Both sides read DuckDB here — the backup's `silver_orders` stands
in for Postgres' — because a backup and a live Postgres are hours apart, and
that gap would read as a difference of semantics. That Postgres Silver answers
the same bodies the same way is proved on every pull request by
`tests/integration/test_goals_history_two_engines.py`, and after the flip by
the soak (the smart goal before and after).

WHAT IT WRITES: NOTHING

The backup is attached `READ_ONLY` and copied into an in-memory database; the
Monday job's store goes there, once per side, and is dropped with the process.
`KS_READ_GOALS` is forced to `duckdb` in this process alone, so no read is
routed off the copy, and the Postgres reader is replaced by one that raises:
the script opens no connection to any server. Nothing it computes reads
`revenue_goals`, the one table a write chain could route regardless of the
flag. It needs the database file to itself only if it is the live one, which
web holds; a backup is not.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

# Every table the goal computations read or the Monday job writes.
TABLES = ("orders", "silver_orders", "managers", "manager_classifications",
          "seasonal_indices", "weekly_patterns", "growth_metrics",
          "revenue_predictions", "revenue_goals", "gold_daily_revenue")
# The keys `ON CONFLICT` needs; `CREATE TABLE AS` does not carry them.
KEYS = {"seasonal_indices": ("month",),
        "weekly_patterns": ("month", "week_of_month"),
        "growth_metrics": ("metric_type",),
        "revenue_goals": ("period_type",)}

SIDES = ("bridge", "silver")
SMART_SALES_TYPES = ("retail", "b2b", "all")
MONTHS_AHEAD = 4


class Refused(Exception):
    """Nothing was compared: exit 2."""


# ─── the copy ────────────────────────────────────────────────────────────────

def copy_backup(backup: Path):
    """An in-memory database holding the backup's goal tables. The backup is
    attached READ_ONLY and detached before anything runs."""
    import duckdb

    if not backup.is_file():
        raise Refused(f"no backup at {backup}")
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(f"ATTACH '{backup}' AS backup (READ_ONLY)")
        present = {r[0] for r in conn.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'backup'"
        ).fetchall()}
        missing = [t for t in TABLES if t not in present]
        if missing:
            raise Refused(f"{backup} has no {', '.join(missing)}")
        for table in TABLES:
            conn.execute(f"CREATE TABLE {table} AS SELECT * FROM backup.main.{table}")
        for table, columns in KEYS.items():
            conn.execute(f"CREATE UNIQUE INDEX {table}_key ON {table} ({', '.join(columns)})")
        conn.execute("DETACH backup")
    except Refused:
        conn.close()
        raise
    except Exception as exc:  # noqa: BLE001 — a backup that cannot be read
        conn.close()
        raise Refused(f"{backup} could not be read: {type(exc).__name__}: {exc}")
    return conn


def _store_class():
    from core.repositories.goals import GoalsMixin

    class CopyStore(GoalsMixin):
        """The real goal methods, over the in-memory copy."""

        def __init__(self, conn):
            self._conn = conn
            self._lock = asyncio.Lock()

        @asynccontextmanager
        async def connection(self):
            async with self._lock:
                yield self._conn

    return CopyStore


# ─── the clock ───────────────────────────────────────────────────────────────

def frozen_clock(today: date):
    """`datetime` whose `now()` is midday in Kyiv on `today`."""
    from core.duckdb_constants import DEFAULT_TZ

    class _Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            moment = datetime.combine(today, time(12, 0), tzinfo=DEFAULT_TZ)
            return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

    return _Clock


def targets(today: date) -> List[Tuple[int, int]]:
    year, month = today.year, today.month
    out = []
    for _ in range(MONTHS_AHEAD):
        out.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


# ─── the two sides ───────────────────────────────────────────────────────────

def _settled(value):
    """Floats to six decimals, the measurement's grain; `calculatedAt` out."""
    if isinstance(value, dict):
        return {str(k): _settled(v) for k, v in value.items() if k != "calculatedAt"}
    if isinstance(value, (list, tuple)):
        return [_settled(v) for v in value]
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float) or hasattr(value, "as_tuple"):
        return round(float(value), 6)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


async def answers(conn, today: date) -> Dict[str, Any]:
    """Everything the history decides, on `conn`, under the history the
    environment names. The Monday job's store first — the smart goal reads
    what it stored, as it does in production."""
    from core.duckdb_constants import KNOWN_SALES_TYPES

    store = _store_class()(conn)
    out: Dict[str, Any] = {}
    tables = await store.recalculate_goal_tables(include_weekly=False)
    out["monday_job"] = {"seasonal": tables["seasonal"], "yoy": tables["yoy"],
                         "history_bounds": list(tables["history_bounds"])}
    for sales_type in (*KNOWN_SALES_TYPES, "all"):
        out[f"seasonality/{sales_type}"] = await store.calculate_seasonality_indices(sales_type)
        out[f"yoy/{sales_type}"] = await store.calculate_yoy_growth(sales_type)
        out[f"weekly/{sales_type}"] = await store.calculate_weekly_patterns(sales_type)
        out[f"caps/{sales_type}"] = [await store._dynamic_growth_cap(m, sales_type)
                                     for m in range(1, 13)]
    for year, month in targets(today):
        for sales_type in SMART_SALES_TYPES:
            out[f"smart/{year}-{month:02d}/{sales_type}"] = \
                await store.generate_smart_goals(year, month, sales_type)
    return _settled(out)


def _differences(bridge, silver, path="") -> List[Tuple[str, Any, Any]]:
    if isinstance(bridge, dict) and isinstance(silver, dict):
        found = []
        for key in sorted(set(bridge) | set(silver)):
            found += _differences(bridge.get(key), silver.get(key), f"{path}/{key}")
        return found
    if isinstance(bridge, list) and isinstance(silver, list) and len(bridge) == len(silver):
        found = []
        for i, (b, s) in enumerate(zip(bridge, silver)):
            found += _differences(b, s, f"{path}[{i}]")
        return found
    return [] if bridge == silver else [(path.lstrip("/"), bridge, silver)]


# ─── the orders each side counts ─────────────────────────────────────────────

def order_sets(conn, sales_type: str) -> Tuple[set, set]:
    from core.models import OrderStatus
    from core.repositories.goals import (
        _orders_sales_type_predicate,
        _silver_history_where,
    )

    statuses = tuple(int(s) for s in OrderStatus.return_statuses())
    clause, params = _orders_sales_type_predicate(sales_type)
    bridge = {r[0] for r in conn.execute(
        f"SELECT o.id FROM orders o WHERE o.status_id NOT IN {statuses} AND {clause}",
        params).fetchall()}
    clause, params = _silver_history_where(sales_type)
    silver = {r[0] for r in conn.execute(
        f"SELECT s.id FROM silver_orders s WHERE {clause}", params).fetchall()}
    return bridge, silver


def causes(conn, ids: set) -> Dict[str, Dict[str, Any]]:
    """Why each order is counted by one side only: orders, hryvnia and the
    first and last Kyiv date per cause."""
    from core.duckdb_constants import _date_in_kyiv
    from core.models import OrderStatus

    if not ids:
        return {}
    statuses = {int(s) for s in OrderStatus.return_statuses()}
    listed = ", ".join(str(i) for i in sorted(ids))
    rows = conn.execute(f"""
        SELECT o.id, o.status_id, o.status_group_id, o.grand_total,
               {_date_in_kyiv('o.ordered_at')}, s.id IS NOT NULL, s.is_return,
               s.order_date, s.sales_type, o.source_id, s.is_active_source
        FROM orders o LEFT JOIN silver_orders s ON s.id = o.id
        WHERE o.id IN ({listed})""").fetchall()
    found = {r[0] for r in rows}
    out: Dict[str, Dict[str, Any]] = {}

    def file(cause, total, day):
        entry = out.setdefault(cause, {"orders": 0, "uah": 0.0, "first": None, "last": None})
        entry["orders"] += 1
        entry["uah"] = round(entry["uah"] + float(total or 0), 2)
        if day is not None:
            day = day.isoformat()
            entry["first"] = min(entry["first"] or day, day)
            entry["last"] = max(entry["last"] or day, day)

    for (oid, status, group, total, day, in_silver, is_return, order_date, stype,
         source, active) in rows:
        if not in_silver:
            file("not in silver_orders", total, day)
        elif (status in statuses) != bool(is_return):
            file(f"return rule: status {status}, group {group}", total, day)
        elif order_date != day:
            file("Kyiv date", total, day)
        elif not active:
            # What adopting `is_active_source` (OQ-1) would drop.
            file(f"source {source}: not a revenue source (is_active_source)", total, day)
        else:
            file(f"other (silver sales_type {stype})", total, day)
    for _missing in ids - found:
        file("in silver_orders only", 0, None)
    return out


# ─── the report ──────────────────────────────────────────────────────────────

@dataclass
class Report:
    backup: str
    today: str
    orders: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    differences: List[Tuple[str, Any, Any]] = field(default_factory=list)
    compared: int = 0

    @property
    def clean(self) -> bool:
        return not self.differences and all(
            not v["bridge_only"] and not v["silver_only"] for v in self.orders.values())


def _leaves(value) -> int:
    if isinstance(value, dict):
        return sum(_leaves(v) for v in value.values())
    if isinstance(value, list):
        return sum(_leaves(v) for v in value)
    return 1


async def measure(backup: Path, today: date) -> Report:
    from core.duckdb_constants import KNOWN_SALES_TYPES
    from core import pg_goals_read

    async def _no_postgres(sql, params=()):
        raise RuntimeError("the dry run routed a goal read to Postgres; it "
                           "compares the backup's copy with itself only")

    report = Report(backup=str(backup), today=today.isoformat())
    results: Dict[str, Any] = {}
    with patch.dict(os.environ, {"KS_READ_GOALS": "duckdb"}), \
            patch("core.pg_goals_read.fetch", _no_postgres), \
            patch("core.repositories.goals.datetime", frozen_clock(today)):
        for side in SIDES:
            conn = copy_backup(backup)
            try:
                if side == SIDES[0]:
                    for sales_type in (*KNOWN_SALES_TYPES, "all"):
                        bridge, silver = order_sets(conn, sales_type)
                        report.orders[sales_type] = {
                            "bridge": len(bridge), "silver": len(silver),
                            "bridge_only": causes(conn, bridge - silver),
                            "silver_only": causes(conn, silver - bridge),
                        }
                with patch.dict(os.environ, {pg_goals_read.HISTORY_ENV: side}):
                    results[side] = await answers(conn, today)
            finally:
                conn.close()
    report.differences = _differences(results["bridge"], results["silver"])
    report.compared = _leaves(results["bridge"])
    return report


def render(report: Report) -> str:
    out = [f"Goal history, bridge against silver — {report.backup}, as of {report.today}",
           "", "Orders counted (not a return; one sales type, or all)"]
    for sales_type, entry in report.orders.items():
        out.append(f"  {sales_type:<11} bridge {entry['bridge']:>7,}   "
                   f"silver {entry['silver']:>7,}")
        for side in ("bridge_only", "silver_only"):
            for cause, c in entry[side].items():
                out.append(f"      {side.replace('_', ' ')}: {cause} — {c['orders']:,} "
                           f"orders, ₴{c['uah']:,.2f}, {c['first']} … {c['last']}")
    out.append("")
    out.append(f"Numbers compared: {report.compared:,} — calculators for every sales "
               f"type, the Monday job's store, the smart goal for {MONTHS_AHEAD} months")
    if not report.differences:
        out.append("  no difference")
    for path, bridge, silver in report.differences[:200]:
        out.append(f"  {path}: {bridge!r} → {silver!r}")
    if len(report.differences) > 200:
        out.append(f"  … and {len(report.differences) - 200} more")
    out.append("")
    out.append("CLEAN — the flip moves no goal." if report.clean else
               "DIFFERENCES — each needs a stated reason before the flip.")
    return "\n".join(out)


def render_json(report: Report) -> str:
    return json.dumps({
        "backup": report.backup, "today": report.today, "clean": report.clean,
        "orders": report.orders, "compared": report.compared,
        "differences": [{"path": p, "bridge": b, "silver": s}
                        for p, b, s in report.differences],
    }, ensure_ascii=False, indent=2, default=str)


def _parse(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Goal history, bridge against silver, on a DuckDB backup. "
                    "Reads only; exit 0 clean, 1 differences, 2 refused.")
    parser.add_argument("--backup", required=True, type=Path,
                        help="a DuckDB file — the flip day's backup")
    parser.add_argument("--today", type=date.fromisoformat, default=None,
                        help="the Kyiv date to compute as of (default: today in Kyiv)")
    parser.add_argument("--json", action="store_true", help="print JSON")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse(argv)
    from core.duckdb_constants import DEFAULT_TZ

    today = args.today or datetime.now(DEFAULT_TZ).date()
    try:
        report = asyncio.run(measure(args.backup, today))
    except Refused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    print(render_json(report) if args.json else render(report))
    return 0 if report.clean else 1


if __name__ == "__main__":
    sys.exit(main())
