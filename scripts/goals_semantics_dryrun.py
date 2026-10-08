#!/usr/bin/env python3
"""What `KS_GOALS_HISTORY=silver` would change, measured before the flip.

    docker compose run --rm --no-deps -T web \\
        python /app/scripts/goals_semantics_dryrun.py \\
            --backup /app/data/backups/analytics-<today>.duckdb
    # exit 0 both halves clean · 1 differences or Postgres Silver not proved,
    # each with its cause · 2 refused · 3 the backup alone, clean (--backup-only)

Chain 7b-2's flip precondition (`.planning/DUCKDB_EXIT_STAGE4_CHAINS.md` §7):
run on the flip day's backup, and flip only on exit 0. Run as the web service,
so the one-off carries web's `.env` — `KS_READONLY_PASSWORD` is how it logs in
to Postgres, and the compose network is how it reaches it.

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

THE ENGINE THE FLIP SWITCHES TO: POSTGRES SILVER

The production flip reads Postgres `silver.orders` (`KS_READ_GOALS=postgres`),
and the comparison above reads the backup's DuckDB `silver_orders` on both
sides: a backup and a live Postgres are hours apart, and that gap would read
as a difference of semantics. So the Postgres half is asked as a verdict
instead — the one comparison that sets the two Silvers against each other at
one instant: `reconcile_silver`, inside the daily `mirror_landing` run,
column by column over every column the history reads (`order_date`,
`is_return`, `sales_type`, `grand_total`, `is_active_source`) with a
tolerance of zero. It is read out of Postgres' copy of the quality journal,
as `ks_readonly`, and the run is clean only when:

- the journal copy is fresh — under 75 min old and not failing — or a copy
  that stopped would keep showing an older clean run;
- the latest `mirror_landing` run is under the canary's 30 h;
- that run did not fail in `reconcile_silver`, between its checks (`setup`),
  or with an error that does not say which check raised; a run that failed
  elsewhere still compared Silver, and is a note;
- it filed nothing against `silver.orders` — every finding there is WARN or
  worse, so none is noise;
- `KS_MIRROR_LANDING` is not off here: with the landing mirror off,
  `reconcile_silver` compares nothing and files nothing, and its silence is
  not a verdict. Read from this process's environment, which is web's when it
  runs as the web service;
- `KS_WRITE_WAREHOUSE` is `duckdb` (or unset) here, for the same reason:
  `reconcile_silver` stands down while Postgres alone derives, and after a
  switch a value web does not understand takes the way back, which holds it
  down too.

Backup half clean and Postgres half clean together say: DuckDB's bridge and
DuckDB's Silver count the same orders the same way, and Postgres' Silver
holds what DuckDB's holds. `reconcile_silver` stands down while the
warehouse checks do — `KS_WRITE_WAREHOUSE=postgres`, or the way back from it
before its first validated full DuckDB tick. The switch cannot take effect
before this flip (`goals_bridge` is one of its preconditions), but a later
re-flip can follow one, so it is not assumed: a backup whose
`sync_metadata.warehouse_writer` reads `postgres` — switched, or a way back
still owed — is refused (exit 2) before anything is compared, since its
DuckDB Silver is frozen as well, and the variable above is a reason on the
Postgres half. That the same bodies answer the same way on
Postgres is proved on every pull request by
`tests/integration/test_goals_history_two_engines.py`, and after the flip by
the soak (the smart goal before and after).

`--backup-only` skips the Postgres half — the measurement on a laptop — and a
clean result then exits 3, never 0: it is not the flip's gate.

WHAT IT WRITES: NOTHING

The backup is attached `READ_ONLY` and copied into an in-memory database; the
Monday job's store goes there, once per side, and is dropped with the process.
`KS_READ_GOALS` is forced to `duckdb` in this process alone, so no goal read
is routed off the copy, and the goal methods' Postgres reader is replaced by
one that raises. The one connection the script opens is the Postgres half's,
and it is `scripts/utm_reclassify_dryrun.py`'s door: `KS_PG_READONLY_DSN`
(or `--dsn`), else `ks_readonly` with `KS_READONLY_PASSWORD` — never
`KS_PG_DSN`, the application's read-write login — a login that could change
anything is refused before a row is read, the session is read-only by
default, the transaction is declared READ ONLY, and before it ends the server
is asked whether it assigned a transaction id. Nothing it computes reads
`revenue_goals`, chain 7a's table. Chain 7b-3's four tables it does read and
write — the Monday job's store and the smart goal — and that chain routes
them to Postgres once it is flagged or latched, through `KS_PG_DSN`, and
latches itself in web's `./data` on the first write. So `measure` pins both
of the chain's answers to DuckDB and replaces its pool with one that raises:
whatever the flag or the latch says, the store goes to the in-memory copy.
It needs the database file to itself only if it is the live one, which web
holds; a backup is not.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple
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
        # The warehouse writer the backup records, and nothing else of
        # sync_metadata: `refuse_after_the_switch` reads it by web's rules.
        conn.execute("CREATE TABLE sync_metadata (key VARCHAR, value VARCHAR)")
        if "sync_metadata" in present:
            conn.execute(
                "INSERT INTO sync_metadata SELECT key, CAST(value AS VARCHAR) "
                "FROM backup.main.sync_metadata WHERE key = ?", [_writer_key()])
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


def _writer_key() -> str:
    from core.warehouse_cutover import WRITER_KEY

    return WRITER_KEY


async def refuse_after_the_switch(conn, backup: Path) -> None:
    """Refuse a backup that records Postgres as the warehouse writer.

    Under it Postgres alone derives Silver — or the way back from that is
    still owed its first validated full DuckDB tick — and both halves go
    blind at once: the backup's `silver_orders` is frozen, so the backup half
    compares the bridge with a stale Silver, and `reconcile_silver` stands
    down in the job, so the journal holds no verdict on `silver.orders` and
    its silence would read clean. Read by `core.warehouse_cutover.read_writer`,
    whose rules decide it for web: a value it did not write counts as
    postgres. This is the gate before the warehouse switch, which needs this
    flip (`goals_bridge`), not after it."""
    from core import warehouse_cutover as cutover

    record = await cutover.read_writer(_store_class()(conn))
    if record is not None and record.get("writer") != cutover.DUCKDB:
        how = (" (a value this build did not write, read as postgres)"
               if record.get("unreadable") else
               f" since {record.get('since')}")
        raise Refused(
            f"{backup} records {record.get('writer')} as the warehouse writer"
            f"{how}: Postgres alone derives Silver, or a way back is still "
            "owed its first validated full DuckDB tick, so its silver_orders "
            "is frozen and reconcile_silver stands down — neither half can "
            "answer")


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


def _differences(bridge, silver) -> List[Tuple[str, Any, Any]]:
    """Every leaf that differs, by its path. A loop over a stack rather than
    a recursion that builds the path as it descends."""
    found: List[Tuple[str, Any, Any]] = []
    stack: List[Tuple[str, Any, Any]] = [("", bridge, silver)]
    while stack:
        path, b, s = stack.pop()
        if isinstance(b, dict) and isinstance(s, dict):
            for key in sorted(set(b) | set(s), reverse=True):
                stack.append((path + "/" + str(key), b.get(key), s.get(key)))
        elif isinstance(b, list) and isinstance(s, list) and len(b) == len(s):
            for i in reversed(range(len(b))):
                stack.append((path + "[" + str(i) + "]", b[i], s[i]))
        elif b != s:
            found.append((path.lstrip("/"), b, s))
    return found


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


# ─── Postgres Silver: the daily comparison's verdict ─────────────────────────

# The comparison whose verdict counts, and what it is about.
VERDICT_LAYER = "mirror_landing"
SILVER_CHECK = "reconcile_silver"
SILVER_TABLE = "silver.orders"
APPLICATION_NAME = "goals_semantics_dryrun"


def _journal_limits() -> Tuple[str, timedelta, timedelta]:
    """The quality journal's Postgres copy, its freshness limit and the
    verdict's age limit — chain 1's preflight asks the same journal the same
    two questions (DN-03's 75 min, the canary's 30 h for `mirror_landing`),
    and `tests/unit/test_inventory_preflip.py` pins the second to the canary.
    One definition, so the two gates cannot drift apart."""
    from core import pg_inventory_write as journal

    return (journal.JOURNAL_COPY, journal.PREFLIGHT_JOURNAL_COPY_WITHIN,
            journal.PREFLIGHT_VERDICT_WITHIN)


@dataclass
class PostgresVerdict:
    """What the latest `mirror_landing` run says about `silver.orders`, read
    as a read-only login. `reasons` empty is the clean answer; `notes` are
    said and do not count."""
    role: Optional[str] = None
    taken_at: Optional[datetime] = None
    journal_copy_age_s: Optional[int] = None
    run_id: Optional[int] = None
    run_started_at: Optional[datetime] = None
    run_age_s: Optional[int] = None
    findings: List[Tuple[str, str, int]] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.reasons


async def read_postgres_state(conn) -> Dict[str, Any]:
    """The journal copy's mark, the latest `mirror_landing` run and its
    findings against `silver.orders`, in one READ ONLY REPEATABLE READ
    transaction — the run and its findings out of one snapshot — asking the
    server before the end whether anything wrote."""
    from scripts.utm_reclassify_dryrun import assert_wrote_nothing

    journal_copy, _, _ = _journal_limits()
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        taken_at = await conn.fetchval("SELECT statement_timestamp()")
        role = await conn.fetchval("SELECT current_user")
        mark = await conn.fetchrow(
            "SELECT last_ok_at, failures_since_ok FROM meta.mirror_state "
            "WHERE table_name = $1", journal_copy)
        run = await conn.fetchrow(
            "SELECT run_id, started_at, error_message "
            "FROM app.data_quality_runs WHERE layer = $1 "
            "ORDER BY run_id DESC LIMIT 1", VERDICT_LAYER)
        findings = [] if run is None else await conn.fetch(
            "SELECT check_name, severity, count FROM app.data_quality_issues "
            "WHERE run_id = $1 AND table_name = $2 ORDER BY check_name",
            run["run_id"], SILVER_TABLE)
        await assert_wrote_nothing(conn)
    return {
        "taken_at": taken_at, "role": role,
        "journal_ok_at": mark["last_ok_at"] if mark else None,
        "journal_failures": int(mark["failures_since_ok"] or 0) if mark else 0,
        "run": None if run is None else {
            "run_id": int(run["run_id"]), "started_at": run["started_at"],
            "error_message": run["error_message"]},
        "findings": [(f["check_name"], f["severity"], int(f["count"]))
                     for f in findings],
    }


def judge_postgres(state: Mapping[str, Any], *, landing_on: bool,
                   warehouse_value: Optional[str] = None) -> PostgresVerdict:
    """Whether the state read proves Postgres Silver holds what DuckDB's
    holds. Pure: `now` is the server's own `taken_at`, and the two switches
    that stand `reconcile_silver` down are handed in — `landing_on`, and
    `warehouse_value`, `KS_WRITE_WAREHOUSE` as the environment holds it."""
    from core import warehouse_cutover as cutover
    from core.pg_inventory_write import _raised_checks

    journal_copy, copy_within, verdict_within = _journal_limits()
    now = state["taken_at"]
    verdict = PostgresVerdict(role=state.get("role"), taken_at=now)
    reasons, notes = verdict.reasons, verdict.notes

    if not landing_on:
        reasons.append(
            "KS_MIRROR_LANDING is off in this environment: reconcile_silver "
            "compares nothing then and files nothing, so silence about "
            f"{SILVER_TABLE} is not a verdict")
    # Read as `configure_mode` reads it. A value web does not understand runs
    # as duckdb — unless a switch came first, when it is the way back and
    # holds the comparison down — so only `duckdb` itself is trusted.
    warehouse = (warehouse_value or cutover.DUCKDB).strip().lower() or cutover.DUCKDB
    if warehouse != cutover.DUCKDB:
        reasons.append(
            f"{cutover.ENV}={warehouse_value!r} in this environment: "
            "reconcile_silver stands down while Postgres alone derives, and "
            "while a way back is owed its first validated full DuckDB tick, "
            f"so silence about {SILVER_TABLE} is not a verdict")

    ok_at = state.get("journal_ok_at")
    if ok_at is not None:
        verdict.journal_copy_age_s = int((now - ok_at).total_seconds())
    if ok_at is None:
        reasons.append(
            f"the quality journal has never been copied into Postgres "
            f"({journal_copy}), so no verdict can be read")
    elif state.get("journal_failures"):
        reasons.append(
            f"the copy of the quality journal is failing "
            f"({state['journal_failures']} in a row)")
    elif now - ok_at >= copy_within:
        reasons.append(
            f"the copy of the quality journal is "
            f"{int((now - ok_at).total_seconds() // 60)} min old (limit "
            f"{int(copy_within.total_seconds() // 60)}), so a newer run may "
            "not be in it")

    run = state.get("run")
    if run is None:
        reasons.append(f"no {VERDICT_LAYER} run in the quality journal")
        return verdict
    verdict.run_id, verdict.run_started_at = run["run_id"], run["started_at"]
    age = now - run["started_at"]
    verdict.run_age_s = int(age.total_seconds())
    named = f"the latest {VERDICT_LAYER} run ({run['run_id']})"
    if age >= verdict_within:
        reasons.append(
            f"{named} is {int(age.total_seconds() // 3600)} h old (limit "
            f"{int(verdict_within.total_seconds() // 3600)})")
    if run["error_message"] is not None:
        # Check names only: the text after them is a driver's message.
        raised = _raised_checks(run["error_message"])
        if raised is None:
            reasons.append(
                f"{named} failed, and its error does not say which check "
                f"raised, so {SILVER_TABLE} may not have been compared")
        elif SILVER_CHECK in raised:
            reasons.append(
                f"{named} failed in {SILVER_CHECK}, so {SILVER_TABLE} was not "
                "compared")
        elif "setup" in raised:
            reasons.append(
                f"{named} failed between its checks, so {SILVER_TABLE} may not "
                "have been compared")
        else:
            notes.append(
                f"{named} failed in {', '.join(raised)}; {SILVER_CHECK} "
                "completed, so that does not count here")
    verdict.findings = list(state.get("findings") or [])
    if verdict.findings:
        reasons.append(
            f"{named} filed {len(verdict.findings)} finding(s) against "
            f"{SILVER_TABLE}: " + ", ".join(
                f"{name} ({severity}, {count})"
                for name, severity, count in verdict.findings))
    return verdict


async def connect_readonly(dsn: Optional[str] = None):
    """`scripts/utm_reclassify_dryrun.py`'s door: `--dsn`, else
    `KS_PG_READONLY_DSN`, else `ks_readonly` at the compose alias with
    `KS_READONLY_PASSWORD` — never `KS_PG_DSN`. The session is read-only by
    default and in UTC."""
    import asyncpg

    from scripts import utm_reclassify_dryrun as door

    options = {"server_settings": {**door.SESSION,
                                   "application_name": APPLICATION_NAME},
               "timeout": 15}
    dsn = dsn or os.environ.get(door.DSN_ENV)
    if dsn:
        return await asyncpg.connect(dsn, **options)
    password = os.environ.get(door.PASSWORD_ENV)
    if password:
        return await asyncpg.connect(
            host=door.COMPOSE_HOST, port=5432, user=door.READONLY_ROLE,
            database=door.DATABASE, password=password, **options)
    raise Refused(
        f"no read-only login for the Postgres half: pass --dsn, or set "
        f"{door.DSN_ENV}, or {door.PASSWORD_ENV} for "
        f"{door.READONLY_ROLE}@{door.COMPOSE_HOST} — or --backup-only, which "
        "is not the flip's gate")


async def postgres_half(dsn: Optional[str] = None) -> PostgresVerdict:
    """Read the verdict as a login that can change nothing, and judge it.
    Any failure to connect or read is a refusal: nothing was proved."""
    from core import pg_landing
    from scripts import utm_reclassify_dryrun as door

    try:
        conn = await connect_readonly(dsn)
    except Refused:
        raise
    except Exception as exc:  # noqa: BLE001 — named, then refused
        raise Refused(f"Postgres could not be reached as a read-only login: "
                      f"{type(exc).__name__}: {exc}")
    try:
        refusals = await door.write_privileges(conn)
        if refusals:
            shown = "; ".join(refusals[:10])
            if len(refusals) > 10:
                shown += f"; and {len(refusals) - 10} more"
            raise Refused(
                "this login can write, and the Postgres half reads only as a "
                f"role that cannot ({door.READONLY_ROLE}): " + shown)
        try:
            state = await read_postgres_state(conn)
        except door.WroteSomething:
            raise
        except Exception as exc:  # noqa: BLE001 — named, then refused
            raise Refused(f"the quality journal could not be read: "
                          f"{type(exc).__name__}: {exc}")
    finally:
        await conn.close()
    from core import warehouse_cutover as cutover

    return judge_postgres(state, landing_on=pg_landing.enabled(),
                          warehouse_value=os.environ.get(cutover.ENV))


# ─── the report ──────────────────────────────────────────────────────────────

@dataclass
class Report:
    backup: str
    today: str
    orders: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    differences: List[Tuple[str, Any, Any]] = field(default_factory=list)
    compared: int = 0
    # None under --backup-only: the Postgres half was not asked.
    postgres: Optional[PostgresVerdict] = None

    @property
    def backup_clean(self) -> bool:
        return not self.differences and all(
            not v["bridge_only"] and not v["silver_only"] for v in self.orders.values())

    @property
    def clean(self) -> bool:
        """The flip's gate: both halves, and the Postgres one asked."""
        return (self.backup_clean and self.postgres is not None
                and self.postgres.clean)

    @property
    def exit_code(self) -> int:
        if self.clean:
            return 0
        if self.backup_clean and self.postgres is None:
            return 3
        return 1


def _leaves(value) -> int:
    if isinstance(value, dict):
        return sum(_leaves(v) for v in value.values())
    if isinstance(value, list):
        return sum(_leaves(v) for v in value)
    return 1


NOT_THE_COPY = ("the dry run reached chain 7b-3's Postgres writer; it stores "
                "into the in-memory copy only")


@contextmanager
def held_off_chain_7b3():
    """Chain 7b-3's two answers pinned to DuckDB, and its writer's pool made
    to raise, for as long as the block runs (module docstring).

    The pins are the first wall: the Monday job's store and the smart goal's
    read ask them and stay on the copy. The pool is the second, for a route
    to either writer that does not ask `writes_postgres`: it raises before the
    writer acquires a connection, so before the latch is taken.
    """
    async def _no_forecast_pool():
        raise RuntimeError(NOT_THE_COPY)

    with patch("core.pg_forecast_write.writes_postgres", lambda: False), \
            patch("core.pg_forecast_write.reads_postgres", lambda: False), \
            patch("core.pg_forecast_write._pool", _no_forecast_pool):
        yield


async def measure(backup: Path, today: date) -> Report:
    from core.duckdb_constants import KNOWN_SALES_TYPES
    from core import pg_goals_read

    async def _no_postgres(sql, params=()):
        raise RuntimeError("the dry run routed a goal read to Postgres; it "
                           "compares the backup's copy with itself only")

    report = Report(backup=str(backup), today=today.isoformat())
    results: Dict[str, Any] = {}
    # Chain 7b-3 routes the Monday job's store and the smart goal's read to
    # Postgres once flagged or latched; here both stay on the copy, and the
    # writer's pool raises as a second wall (module docstring).
    with patch.dict(os.environ, {"KS_READ_GOALS": "duckdb"}), \
            patch("core.pg_goals_read.fetch", _no_postgres), \
            held_off_chain_7b3(), \
            patch("core.repositories.goals.datetime", frozen_clock(today)):
        for side in SIDES:
            conn = copy_backup(backup)
            try:
                if side == SIDES[0]:
                    await refuse_after_the_switch(conn, backup)
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
    out.append(f"Postgres silver.orders against DuckDB's — the latest "
               f"{VERDICT_LAYER} run, read from Postgres")
    pg = report.postgres
    if pg is None:
        out.append("  not asked (--backup-only)")
    else:
        out.append(f"  as {pg.role} at {pg.taken_at}: run {pg.run_id}, started "
                   f"{pg.run_started_at}; journal copy "
                   f"{'never' if pg.journal_copy_age_s is None else f'{pg.journal_copy_age_s // 60} min'} old")
        if pg.clean:
            out.append(f"  {SILVER_CHECK} filed nothing against {SILVER_TABLE}")
        for reason in pg.reasons:
            out.append(f"  NOT PROVED: {reason}")
        for note in pg.notes:
            out.append(f"  note: {note}")
    out.append("")
    if report.clean:
        out.append("CLEAN — the flip moves no goal.")
    elif report.exit_code == 3:
        out.append("CLEAN ON THE BACKUP ALONE — Postgres Silver was not asked; "
                   "this is not the flip's gate.")
    else:
        out.append("DIFFERENCES — each needs a stated reason before the flip.")
    return "\n".join(out)


def render_json(report: Report) -> str:
    pg = report.postgres
    return json.dumps({
        "backup": report.backup, "today": report.today, "clean": report.clean,
        "backup_clean": report.backup_clean,
        "orders": report.orders, "compared": report.compared,
        "differences": [{"path": p, "bridge": b, "silver": s}
                        for p, b, s in report.differences],
        "postgres": None if pg is None else {
            "clean": pg.clean, "role": pg.role, "taken_at": pg.taken_at,
            "run_id": pg.run_id, "run_started_at": pg.run_started_at,
            "run_age_s": pg.run_age_s, "journal_copy_age_s": pg.journal_copy_age_s,
            "findings": [{"check_name": n, "severity": sv, "count": c}
                         for n, sv, c in pg.findings],
            "reasons": pg.reasons, "notes": pg.notes,
        },
    }, ensure_ascii=False, indent=2, default=str)


def _parse(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Goal history, bridge against silver, on a DuckDB backup, "
                    "and Postgres Silver against DuckDB's by the latest "
                    "mirror_landing run. Reads only; exit 0 both clean, 1 "
                    "differences or Postgres Silver not proved, 2 refused, 3 "
                    "the backup alone clean (--backup-only).")
    parser.add_argument("--backup", required=True, type=Path,
                        help="a DuckDB file — the flip day's backup")
    parser.add_argument("--today", type=date.fromisoformat, default=None,
                        help="the Kyiv date to compute as of (default: today in Kyiv)")
    parser.add_argument("--json", action="store_true", help="print JSON")
    parser.add_argument(
        "--dsn", help="a read-only login for the Postgres half; prefer "
                      "KS_PG_READONLY_DSN, which keeps the password off the "
                      "command line")
    parser.add_argument(
        "--backup-only", action="store_true",
        help="skip the Postgres half; a clean run then exits 3, not 0")
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace, today: date) -> Report:
    # The Postgres half first: it is a second's read, and a refusal there
    # should not wait for the backup's minutes.
    verdict = None if args.backup_only else await postgres_half(args.dsn)
    report = await measure(args.backup, today)
    report.postgres = verdict
    return report


def main(argv=None) -> int:
    args = _parse(argv)
    from core.duckdb_constants import DEFAULT_TZ
    from scripts.utm_reclassify_dryrun import Refused as DoorRefused

    today = args.today or datetime.now(DEFAULT_TZ).date()
    try:
        report = asyncio.run(_run(args, today))
    except (Refused, DoorRefused) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    print(render_json(report) if args.json else render(report))
    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
