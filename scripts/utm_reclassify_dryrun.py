#!/usr/bin/env python3
"""What a UTM reclassify would move, measured before anybody runs one.

    install -d -m 700 /root/utm-snapshots
    docker run --rm --name utm-dryrun --user root \
        --network key-api-bot_default --env-file /opt/key-api-bot/.env \
        -v /root/utm-snapshots:/snap halloweex/keycrm-web:latest \
        python /app/scripts/utm_reclassify_dryrun.py \
            --snapshot /snap/order_utm-$(date -u +%Y%m%d-%H%M%S).csv.gz
    # exit 0 reported · 1 failed, no snapshot left behind · 2 refused before reading

The one-off container is `scripts/chain_copy_back.py`'s shape: the image
carries every module, `--env-file` supplies `KS_READONLY_PASSWORD`, and the
network is the one Postgres is on (it publishes no port). `--user root`
because the image runs as `appuser`, which cannot write a directory only root
may enter; the snapshot directory is outside the repository tree because the
repository is public. Being root in the container does not make the database
login any less read-only — that is decided below, by the role.

WHY IT EXISTS (DN-15, for the owner's decision OD-06)

`silver.order_utm` is not a pure function of today's inputs. A verdict is
written when the parser first sees an order and is rewritten only when the
order's `updated_at` moves, so a rule change reaches old orders through one
door alone: `POST /api/traffic/reclassify`, which deletes the table and parses
everything again. On the 2026-08-31 copy that moved 3,769 of 32,656 verdicts,
most of them pixel-only orders the 2026-09-09 rule stopped calling Facebook.
Step 9's Postgres parser will do the same on its first full parse whether
anybody decides to or not. Both overwrite the only record of what the tab and
the weekly traffic report said about those orders, and nothing keeps a
history of verdicts.

So before either happens: this reads what is stored, recomputes every verdict
in memory with the parser the next parse would run (`core.utm_classify`), and
reports what would move — per transition, with order counts and hryvnia over
the last 12 complete Monday–Sunday weeks, in the tab's own terms. `--snapshot`
saves the table as it stands first.

WHAT IT READS, AND WHY FROM POSTGRES

`bronze.orders` (the comment, `updated_at`, the source), `silver.order_utm`
(the stored verdicts) and `silver.orders` (date, amount, sales type, the
return flag), in ONE read-only REPEATABLE READ transaction, so the verdicts,
the comments and the snapshot are one instant. It is the store the tab reads,
and `bronze.orders` is what step 9's parser will read. The DuckDB reclassify
reads DuckDB's `orders`, which the mirror keeps identical and the daily
comparison checks.

The transaction is kept short on purpose: every Silver tick `TRUNCATE`s
`silver.orders` and `silver.order_utm` under ACCESS EXCLUSIVE, which waits for
this read, and every reader of /traffic then waits behind that. So the rows
come out first and all the work happens after COMMIT, `lock_timeout` refuses
to queue behind a tick for long, and the report prints how long the snapshot
was held.

WHAT IT WRITES: NOTHING, EXCEPT THE ONE FILE YOU NAME

Not a row in any store, not a temporary file. Four layers, because this runs
against production and "read-only" in a docstring has been wrong before — a
benchmark once inherited the web container's `KS_PG_DSN` and truncated a live
table:

1. It never reads `KS_PG_DSN`, the application's read-write DSN. Only
   `KS_PG_READONLY_DSN` (or `--dsn`), or else `KS_READONLY_PASSWORD` for the
   `ks_readonly` role at the compose alias. A test walks this module for every
   environment name it reads.
2. It refuses a role that could change anything — superuser, any write
   privilege on any table, CREATE on a schema, CREATE or TEMPORARY on the
   database — before reading a row. `ks_readonly` holds none of those.
3. The session starts with `default_transaction_read_only`, the transaction is
   declared READ ONLY, and before it ends the script asks the server whether a
   transaction id was ever assigned. One is assigned on the first write and
   only then, so "none" is the server's own word that nothing was written, and
   "some" rolls the transaction back and fails the run.
4. The snapshot is opened with exclusive create, so an earlier snapshot — the
   record of verdicts that may since have been overwritten — is never replaced.
   The file is removed if anything fails before it is complete and verified;
   no other path is ever opened for writing. A test runs the script under an
   audit hook and fails on any other write, and on any network connection.

The sha256 is printed rather than written beside the file, so the named file
stays the only one; saved as it is, the printed line is what `sha256sum -c`
reads in the snapshot's directory.

THE SNAPSHOT

`silver.order_utm` whole, every column, ordered by `order_id`, as a gzipped CSV
with a header. The session runs in UTC, so every timestamp in it is
unambiguous. It is read inside the same transaction as the report, re-read
from disk after writing, and refused if its row count differs from the table's
in that snapshot. The gzip header carries no name and no time, so the same
table gives the same bytes and the same sha256. It is COPY's own CSV — a NULL
is an empty field and an empty string is `""` — so `COPY ... FROM ... (FORMAT
csv, HEADER)` puts it back as it was.

HOW TO READ THE REPORT

Every change is filed under the one cause that explains it:

- `rule_change` — the stored verdict was written under older rules and only a
  reclassify (or a full parse) reaches it. This is what OD-06 is about.
- `orphaned` — a row whose order has no comment any more. The reclassify's
  DELETE removes it and nothing puts it back; the incremental parse never
  looks at it.
- `pending` — the order changed after it was parsed; the next tick reparses it
  anyway, reclassify or not.
- `unparsed` — a comment with no row yet; the next tick parses it anyway.

Verdicts are compared as /traffic shows them: an order with no row, or a row
of NULLs, is placed by the source-based fallback the tab's SQL applies
(`TrafficMixin._PLATFORM_EXPR` and `_TRAFFIC_TYPE_EXPR`, pinned to
`fallback_verdict` below by a test). The 12-week figures use the tab's own
predicate — not a return, an active source — for `retail`, which the tab and
the Monday report default to, and for all sales types.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import hashlib
import json
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.utm_classify import UTM_VERDICT_COLUMNS, utm_columns  # noqa: E402
from core.weekly_report import last_complete_week  # noqa: E402

KYIV = ZoneInfo("Europe/Kyiv")
WEEKS = 12

# The two ways to name a read-only login, and deliberately not the third. The
# application's `KS_PG_DSN` is ks_app, which owns every table; the one-off
# container this runs in carries it, and reading it by accident is how a
# benchmark truncated a live table once.
DSN_ENV = "KS_PG_READONLY_DSN"
PASSWORD_ENV = "KS_READONLY_PASSWORD"
READONLY_ROLE = "ks_readonly"
# The compose service alias, which is what `KS_PG_DSN` names too: Postgres
# publishes no port, so this resolves only from a container on its network.
COMPOSE_HOST = "postgres"
DATABASE = "ks"

# Sent at connection start, so they hold for every statement this session runs.
# `lock_timeout`: a Silver tick holds ACCESS EXCLUSIVE on both Silver tables
# for its TRUNCATE+INSERT (a few seconds); queueing behind it longer than this
# would park every /traffic reader behind this script too.
SESSION = {
    "default_transaction_read_only": "on",
    "TimeZone": "UTC",
    "application_name": "utm_reclassify_dryrun",
    "lock_timeout": "10s",
    "statement_timeout": "120s",
    "idle_in_transaction_session_timeout": "300s",
}

VERDICT_TYPE = UTM_VERDICT_COLUMNS.index("traffic_type")
VERDICT_PLATFORM = UTM_VERDICT_COLUMNS.index("platform")

CAUSES = ("rule_change", "orphaned", "pending", "unparsed")
CAUSE_NOTES = {
    "rule_change": "only a reclassify (or a full parse) reaches these",
    "orphaned": "comment now empty; the reclassify DELETE drops the row for good",
    "pending": "changed since parsed; the next tick reparses these anyway",
    "unparsed": "no row yet; the next tick parses these anyway",
}
SCOPES = ("retail", "all")


# ─── SQL: reads only ─────────────────────────────────────────────────────────

# What makes a role able to change something. `has_table_privilege` with a
# list is true when ANY of them is held.
WRITABLE_TABLES_SQL = r"""
SELECT format('%I.%I', n.nspname, c.relname) AS name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg\_%'
  AND has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE')
ORDER BY 1
LIMIT 5
"""
CREATABLE_SCHEMAS_SQL = r"""
SELECT nspname AS name
FROM pg_namespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND nspname NOT LIKE 'pg\_%'
  AND has_schema_privilege(oid, 'CREATE')
ORDER BY 1
LIMIT 5
"""
ROLE_SQL = """
SELECT current_user AS role, rolsuper AS superuser,
       has_database_privilege(current_database(), 'CREATE, TEMPORARY') AS db_write
FROM pg_roles WHERE rolname = current_user
"""

NOW_SQL = "SELECT now()"
UTM_ROWS_SQL = "SELECT count(*) FROM silver.order_utm"
ORPHAN_ROWS_SQL = """
SELECT count(*) FROM silver.order_utm u
WHERE NOT EXISTS (SELECT 1 FROM bronze.orders b WHERE b.id = u.order_id)
"""
# Every order, not only those with a comment: the per-platform totals need the
# ones the fallback places too. `counted` is the tab's predicate less the date.
ORDERS_SQL = f"""
SELECT b.id, b.source_id, b.manager_comment, b.updated_at,
       u.order_id IS NOT NULL AS has_row,
       {", ".join(f"u.{column}" for column in UTM_VERDICT_COLUMNS)},
       u.parsed_at,
       s.order_date, s.grand_total, s.sales_type,
       (s.id IS NOT NULL AND NOT s.is_return AND s.is_active_source) AS counted
FROM bronze.orders b
LEFT JOIN silver.order_utm u ON u.order_id = b.id
LEFT JOIN silver.orders s ON s.id = b.id
ORDER BY b.id
"""
SNAPSHOT_SQL = "SELECT * FROM silver.order_utm ORDER BY order_id"
# NULL until the transaction writes something; PostgreSQL 13+.
XID_SQL = "SELECT pg_current_xact_id_if_assigned()::text"


class Refused(Exception):
    """Stopped before reading anything. Exit 2."""


class WroteSomething(RuntimeError):
    """The server assigned this transaction an id, which means it wrote."""


class SnapshotMismatch(RuntimeError):
    """The file on disk does not hold the rows the table held."""


# ─── the model ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Order:
    """One `bronze.orders` row with what is stored about it."""
    order_id: int
    source_id: Optional[int]
    comment: Optional[str]
    updated_at: Optional[datetime]
    # `UTM_VERDICT_COLUMNS` as stored, or None when the order has no row.
    stored: Optional[Tuple[Optional[str], ...]]
    parsed_at: Optional[datetime]
    order_date: Optional[date]
    grand_total: Optional[Decimal]
    sales_type: Optional[str]
    counted: bool


@dataclass
class Transition:
    cause: str
    before: Tuple[str, str]
    after: Tuple[str, str]
    orders: int = 0
    window_orders: Dict[str, int] = field(default_factory=lambda: {s: 0 for s in SCOPES})
    window_revenue: Dict[str, Decimal] = field(
        default_factory=lambda: {s: Decimal("0") for s in SCOPES})


@dataclass
class Report:
    window: Tuple[date, date]
    orders_read: int
    with_comment: int
    rewrites: Counter
    verdict_moves: Counter
    column_changes: Counter
    transitions: List[Transition]
    # scope -> platform -> [orders before, revenue before, orders after, revenue after]
    platforms: Dict[str, Dict[str, list]]
    # scope -> traffic_type -> the same four
    traffic_types: Dict[str, Dict[str, list]]


@dataclass(frozen=True)
class Snapshot:
    path: Path
    rows: int
    size: int
    sha256: str


@dataclass(frozen=True)
class State:
    """Everything read, in one transaction."""
    taken_at: datetime
    role: str
    held_s: float
    utm_rows: int
    orphan_rows: int
    orders: List[Order]
    snapshot: Optional[Snapshot]


# ─── the pure part ───────────────────────────────────────────────────────────

def report_window(today: date, weeks: int = WEEKS) -> Tuple[date, date]:
    """The last `weeks` complete Monday–Sunday weeks before `today`.

    Built on the weekly reports' own `last_complete_week`, so "complete week"
    means here exactly what it means in the Monday message these numbers are
    about.
    """
    last_start, end = last_complete_week(today)
    return last_start - timedelta(weeks=weeks - 1), end


def fallback_verdict(source_id: Optional[int]) -> Tuple[str, str]:
    """Where /traffic places an order its UTM row does not name.

    The Python spelling of `TrafficMixin._TRAFFIC_TYPE_EXPR` and
    `_PLATFORM_EXPR` with the row's two columns NULL. A test evaluates those
    two SQL expressions and holds this to them.
    """
    platform = {1: "instagram", 2: "telegram"}.get(source_id, "unattributed")
    traffic_type = "organic" if source_id in (1, 2) else "unknown"
    return traffic_type, platform


def shown_verdict(
    row: Optional[Sequence[Optional[str]]], source_id: Optional[int],
) -> Tuple[str, str]:
    """`(traffic_type, platform)` as the tab shows them for this row.

    Column by column, as `COALESCE` does: a row with a platform and no type
    keeps its platform.
    """
    fallback_type, fallback_platform = fallback_verdict(source_id)
    if row is None:
        return fallback_type, fallback_platform
    traffic_type = row[VERDICT_TYPE]
    platform = row[VERDICT_PLATFORM]
    return (
        traffic_type if traffic_type is not None else fallback_type,
        platform if platform is not None else fallback_platform,
    )


def _has_comment(comment: Optional[str]) -> bool:
    # The parser's own selection: `manager_comment IS NOT NULL AND != ''`.
    return comment is not None and comment != ""


def _cause(order: Order, after: Optional[tuple]) -> Optional[str]:
    """Why a reclassify would rewrite this order's row, or None if it would not."""
    before = order.stored
    if before is None and after is None:
        return None
    if before is None:
        return "unparsed"
    if after is None:
        return "orphaned"
    if tuple(before) == tuple(after):
        return None
    pending = (
        order.updated_at is not None and order.parsed_at is not None
        and order.updated_at > order.parsed_at
    )
    return "pending" if pending else "rule_change"


def diff_verdicts(
    orders: Iterable[Order],
    window: Tuple[date, date],
    *,
    reclassify: Callable[[Optional[str]], tuple] = utm_columns,
) -> Report:
    """What a reclassify would do to `orders`, computed without doing it.

    `reclassify` is the row the next parse would write for a comment; it is
    `utm_columns`, the parse's own function, everywhere but in tests of this
    diff.
    """
    start, end = window
    rewrites: Counter = Counter()
    verdict_moves: Counter = Counter()
    column_changes: Counter = Counter()
    transitions: Dict[tuple, Transition] = {}
    platforms = {s: {} for s in SCOPES}
    traffic_types = {s: {} for s in SCOPES}
    orders_read = with_comment = 0

    for order in orders:
        orders_read += 1
        has_comment = _has_comment(order.comment)
        with_comment += has_comment
        after_row = tuple(reclassify(order.comment)) if has_comment else None
        before = shown_verdict(order.stored, order.source_id)
        after = shown_verdict(after_row, order.source_id)

        in_window = (
            order.counted and order.order_date is not None
            and start <= order.order_date <= end
        )
        scopes = ()
        if in_window:
            scopes = ("all", "retail") if order.sales_type == "retail" else ("all",)
            amount = order.grand_total or Decimal("0")
            for scope in scopes:
                for table, b, a in ((platforms, before[1], after[1]),
                                    (traffic_types, before[0], after[0])):
                    table[scope].setdefault(b, [0, Decimal("0"), 0, Decimal("0")])
                    table[scope].setdefault(a, [0, Decimal("0"), 0, Decimal("0")])
                    table[scope][b][0] += 1
                    table[scope][b][1] += amount
                    table[scope][a][2] += 1
                    table[scope][a][3] += amount

        cause = _cause(order, after_row)
        if cause is None:
            continue
        rewrites[cause] += 1
        if order.stored is not None:
            new = after_row if after_row is not None else (None,) * len(UTM_VERDICT_COLUMNS)
            for column, old_value, new_value in zip(UTM_VERDICT_COLUMNS, order.stored, new):
                if old_value != new_value:
                    column_changes[column] += 1
        if before == after:
            continue
        verdict_moves[cause] += 1
        key = (cause, before, after)
        transition = transitions.get(key)
        if transition is None:
            transition = transitions[key] = Transition(cause, before, after)
        transition.orders += 1
        for scope in scopes:
            transition.window_orders[scope] += 1
            transition.window_revenue[scope] += order.grand_total or Decimal("0")

    ordered = sorted(
        transitions.values(),
        key=lambda t: (CAUSES.index(t.cause), -t.orders, t.before, t.after),
    )
    return Report(
        window=window, orders_read=orders_read, with_comment=with_comment,
        rewrites=rewrites, verdict_moves=verdict_moves,
        column_changes=column_changes, transitions=ordered,
        platforms=platforms, traffic_types=traffic_types,
    )


def order_from_record(record: Any) -> Order:
    """One row of `ORDERS_SQL` as an `Order`."""
    stored = None
    if record["has_row"]:
        stored = tuple(record[column] for column in UTM_VERDICT_COLUMNS)
    return Order(
        order_id=record["id"],
        source_id=record["source_id"],
        comment=record["manager_comment"],
        updated_at=record["updated_at"],
        stored=stored,
        parsed_at=record["parsed_at"],
        order_date=record["order_date"],
        grand_total=record["grand_total"],
        sales_type=record["sales_type"],
        counted=bool(record["counted"]),
    )


# ─── rendering ───────────────────────────────────────────────────────────────

def _n(value: int) -> str:
    return f"{value:,}"


def _uah(value: Decimal) -> str:
    return f"₴{value:,.2f}"


def _verdict(pair: Tuple[str, str]) -> str:
    return f"{pair[0]}/{pair[1]}"


def render_text(report: Report, state: State) -> str:
    start, end = report.window
    out: List[str] = []
    out.append("UTM reclassify dry run (DN-15) — nothing was written")
    out.append(
        f"  read as {state.role} at {state.taken_at:%Y-%m-%d %H:%M:%S} UTC, one "
        f"REPEATABLE READ READ ONLY snapshot held {state.held_s:.2f} s; "
        "the server assigned it no transaction id")
    out.append(
        f"  silver.order_utm {_n(state.utm_rows)} rows "
        f"({_n(state.orphan_rows)} with no order in bronze) · bronze.orders "
        f"{_n(report.orders_read)}, {_n(report.with_comment)} with a comment")
    out.append("")
    out.append("What a reclassify would rewrite")
    for cause in CAUSES:
        out.append(
            f"  {cause:<12} {_n(report.rewrites[cause]):>8} rows, "
            f"{_n(report.verdict_moves[cause]):>8} verdicts move   {CAUSE_NOTES[cause]}")
    if report.column_changes:
        changed = ", ".join(
            f"{column} {_n(report.column_changes[column])}"
            for column in UTM_VERDICT_COLUMNS if report.column_changes[column])
        out.append(f"  stored columns that change: {changed}")
    out.append("")
    out.append(
        f"Verdicts that move, as /traffic shows them (traffic_type/platform); "
        f"12w = {start:%d.%m.%Y}–{end:%d.%m.%Y}")
    if not report.transitions:
        out.append("  none")
    else:
        header = (f"  {'cause':<12} {'before':<30} {'after':<30} {'orders':>8} "
                  f"{'12w':>7} {'12w ₴':>16} {'12w retail':>10} {'12w retail ₴':>16}")
        out.append(header)
        for t in report.transitions:
            out.append(
                f"  {t.cause:<12} {_verdict(t.before):<30} {_verdict(t.after):<30} "
                f"{_n(t.orders):>8} {_n(t.window_orders['all']):>7} "
                f"{_uah(t.window_revenue['all']):>16} "
                f"{_n(t.window_orders['retail']):>10} "
                f"{_uah(t.window_revenue['retail']):>16}")
    for title, table in (("platform", report.platforms),
                         ("traffic_type", report.traffic_types)):
        out.append("")
        out.append(
            f"12 complete weeks {start:%d.%m.%Y}–{end:%d.%m.%Y} by {title}: "
            "now → after a reclassify (not returns, active sources)")
        for scope in SCOPES:
            out.append(f"  {scope}")
            rows = table[scope]
            if not rows:
                out.append("    no orders")
                continue
            for name in sorted(rows, key=lambda k: (-rows[k][1], k)):
                b_orders, b_rev, a_orders, a_rev = rows[name]
                out.append(
                    f"    {name:<16} {_n(b_orders):>7} {_uah(b_rev):>16} → "
                    f"{_n(a_orders):>7} {_uah(a_rev):>16}   "
                    f"Δ {a_orders - b_orders:+,} orders, {a_rev - b_rev:+,.2f} ₴")
    if state.snapshot is not None:
        snap = state.snapshot
        out.append("")
        out.append(
            f"Snapshot {snap.path}: {_n(snap.rows)} rows, {_n(snap.size)} bytes, "
            "checked against the table in the same snapshot")
        out.append(f"{snap.sha256}  {snap.path.name}")
    return "\n".join(out)


def render_json(report: Report, state: State) -> str:
    def money(value: Decimal) -> str:
        return f"{value:.2f}"

    def table(rows: Dict[str, list]) -> Dict[str, dict]:
        return {
            name: {"orders_now": v[0], "revenue_now": money(v[1]),
                   "orders_after": v[2], "revenue_after": money(v[3])}
            for name, v in sorted(rows.items())
        }

    payload = {
        "taken_at": state.taken_at.isoformat(),
        "role": state.role,
        "held_s": round(state.held_s, 3),
        "wrote_nothing": True,
        "utm_rows": state.utm_rows,
        "orphan_rows": state.orphan_rows,
        "orders_read": report.orders_read,
        "with_comment": report.with_comment,
        "window": [report.window[0].isoformat(), report.window[1].isoformat()],
        "rewrites": {c: report.rewrites[c] for c in CAUSES},
        "verdict_moves": {c: report.verdict_moves[c] for c in CAUSES},
        "column_changes": {c: report.column_changes[c] for c in UTM_VERDICT_COLUMNS},
        "transitions": [
            {
                "cause": t.cause,
                "before": list(t.before), "after": list(t.after),
                "orders": t.orders,
                "window_orders": t.window_orders,
                "window_revenue": {s: money(v) for s, v in t.window_revenue.items()},
            }
            for t in report.transitions
        ],
        "platforms": {s: table(report.platforms[s]) for s in SCOPES},
        "traffic_types": {s: table(report.traffic_types[s]) for s in SCOPES},
        "snapshot": None if state.snapshot is None else {
            "path": str(state.snapshot.path), "rows": state.snapshot.rows,
            "size": state.snapshot.size, "sha256": state.snapshot.sha256,
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


# ─── the database ────────────────────────────────────────────────────────────

async def connect(args: argparse.Namespace):
    """A session that cannot start a write, as a login that holds none.

    `--dsn`, else `KS_PG_READONLY_DSN`, else `ks_readonly` at the compose alias
    with `KS_READONLY_PASSWORD` — which is what makes the one-off container in
    the usage above work with the host's `.env` and nothing typed.
    """
    options = {"server_settings": SESSION, "timeout": 15}
    dsn = args.dsn or os.environ.get(DSN_ENV)
    if dsn:
        return await asyncpg.connect(dsn, **options)
    password = os.environ.get(PASSWORD_ENV)
    if password:
        return await asyncpg.connect(
            host=COMPOSE_HOST, port=5432, user=READONLY_ROLE, database=DATABASE,
            password=password, **options)
    raise Refused(
        f"no read-only login: pass --dsn, or set {DSN_ENV}, or {PASSWORD_ENV} "
        f"for {READONLY_ROLE}@{COMPOSE_HOST}")


async def write_privileges(conn) -> List[str]:
    """Everything this login could change, or [] for a read-only one."""
    role = await conn.fetchrow(ROLE_SQL)
    found: List[str] = []
    if role is None:
        return [f"role {await conn.fetchval('SELECT current_user')} is not in pg_roles"]
    if role["superuser"]:
        found.append(f"{role['role']} is a superuser")
    if role["db_write"]:
        found.append("CREATE or TEMPORARY on the database")
    for record in await conn.fetch(CREATABLE_SCHEMAS_SQL):
        found.append(f"CREATE on schema {record['name']}")
    for record in await conn.fetch(WRITABLE_TABLES_SQL):
        found.append(f"INSERT/UPDATE/DELETE/TRUNCATE on {record['name']}")
    return found


async def assert_wrote_nothing(conn) -> None:
    """Raise unless the current transaction never wrote.

    PostgreSQL assigns a transaction id lazily, on the first write, so an
    unassigned id is the server saying no row, no temporary table and no
    sequence value came from this transaction. Checked before the transaction
    ends, so the raise rolls back whatever it was.
    """
    xid = await conn.fetchval(XID_SQL)
    if xid is not None:
        raise WroteSomething(
            f"the read transaction was assigned xid {xid}: something wrote. "
            "It has been rolled back.")


def check_snapshot_path(path: Path) -> None:
    """Refuse before connecting rather than after reading."""
    if path.exists():
        raise Refused(f"{path} exists; a snapshot is never overwritten")
    if not path.parent.is_dir():
        raise Refused(f"{path.parent} is not a directory")


def _verify(path: Path) -> Tuple[str, int, int]:
    """sha256 of the bytes on disk, the CSV data rows in them, and the size."""
    digest = hashlib.sha256()
    with open(path, "rb") as raw:
        for block in iter(lambda: raw.read(1 << 20), b""):
            digest.update(block)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as text:
        rows = sum(1 for _ in csv.reader(text)) - 1  # less the header
    return digest.hexdigest(), rows, path.stat().st_size


async def write_snapshot(conn, path: Path, expected_rows: int) -> Snapshot:
    """`silver.order_utm` to `path`, inside the caller's transaction.

    Exclusive create: an existing file is an earlier snapshot, possibly the
    only record of verdicts overwritten since, and it is left alone — which
    is also why the cleanup below runs only for a file this call created.
    """
    created = False
    try:
        with open(path, "xb") as raw:
            created = True
            # No name and no time in the gzip header, so the bytes depend on
            # the rows alone.
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
                async def sink(chunk: bytes) -> None:
                    gz.write(chunk)

                await conn.copy_from_query(
                    SNAPSHOT_SQL, output=sink, format="csv", header=True)
            raw.flush()
            os.fsync(raw.fileno())
        sha256, rows, size = _verify(path)
        if rows != expected_rows:
            raise SnapshotMismatch(
                f"{path} holds {rows} rows; silver.order_utm held {expected_rows} "
                "in the same snapshot")
        return Snapshot(path=path, rows=rows, size=size, sha256=sha256)
    except BaseException:
        if created:
            path.unlink(missing_ok=True)
        raise


async def read_state(conn, snapshot_path: Optional[Path] = None) -> State:
    """Everything the report needs, and the snapshot, in one read-only transaction."""
    started = time.monotonic()
    snapshot = None
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            taken_at = await conn.fetchval(NOW_SQL)
            role = await conn.fetchval("SELECT current_user")
            records = await conn.fetch(ORDERS_SQL)
            utm_rows = await conn.fetchval(UTM_ROWS_SQL)
            orphan_rows = await conn.fetchval(ORPHAN_ROWS_SQL)
            if snapshot_path is not None:
                snapshot = await write_snapshot(conn, snapshot_path, utm_rows)
            await assert_wrote_nothing(conn)
    except BaseException:
        # A snapshot is only worth keeping from a run that proved it read.
        if snapshot is not None:
            snapshot.path.unlink(missing_ok=True)
        raise
    held = time.monotonic() - started
    return State(
        taken_at=taken_at, role=role, held_s=held, utm_rows=utm_rows,
        orphan_rows=orphan_rows, orders=[order_from_record(r) for r in records],
        snapshot=snapshot,
    )


# ─── the command ─────────────────────────────────────────────────────────────

def _parse(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report what a UTM reclassify would move, reading as a "
                    "read-only role and writing nothing.",
    )
    parser.add_argument(
        "--dsn",
        help=f"a read-only login; prefer {DSN_ENV} in the environment, which "
             "keeps the password off the command line",
    )
    parser.add_argument(
        "--snapshot", type=Path, metavar="PATH",
        help="also save silver.order_utm as a gzipped CSV at PATH (never "
             "overwritten); its sha256 is printed",
    )
    parser.add_argument(
        "--today", type=date.fromisoformat, metavar="YYYY-MM-DD",
        help="the day the 12 complete weeks are counted back from "
             "(default: today in Kyiv)",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="print the report as JSON",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    if args.snapshot is not None:
        check_snapshot_path(args.snapshot)
    today = args.today or datetime.now(KYIV).date()
    conn = await connect(args)
    try:
        refusals = await write_privileges(conn)
        if refusals:
            raise Refused(
                "this login can write, and this script reads only as a role that "
                f"cannot ({READONLY_ROLE}): " + "; ".join(refusals))
        state = await read_state(conn, args.snapshot)
    finally:
        await conn.close()
    report = diff_verdicts(state.orders, report_window(today))
    print(render_json(report, state) if args.json else render_text(report, state))
    return 0


def main(argv=None) -> int:
    args = _parse(argv)
    try:
        return asyncio.run(_run(args))
    except Refused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
