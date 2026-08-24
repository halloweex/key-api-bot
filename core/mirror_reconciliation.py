"""Reconciliation A: does Postgres hold what DuckDB holds?

Step 05 writes landing twice — DuckDB, which every dashboard number still
comes from, and Postgres, which nothing reads yet. The write is one call after
another over the same parsed rows (`core.landing_rows`), which makes agreement
*likely*. This is what makes it *checked*, and the closing criterion for the
step is this check reporting zero, not a date on a calendar.

WHY THE TOLERANCE IS ZERO AND STAYS ZERO

Landing is a copy. There is no aggregation between the two sides, no timezone
arithmetic, no projection — the same tuple is handed to two stores. Any
difference at all is a defect in the mirror, so a tolerance would only be a
place for one to hide. The reconciliation against KeyCRM is where tolerances
belong, because there the two sides are computed differently.

THE ONE DIFFERENCE THAT IS NOT A DEFECT, AND WHY IT NEEDED A RULE

Measured on production the morning the mirror first ran: DuckDB held 1,004
products, Postgres 1,003. The extra one is id 1055, last synced 2026-06-13 —
KeyCRM stopped serving it and `upsert_products` never deletes, so DuckDB keeps
it forever while a payload-fed mirror can never learn of it again.

Left alone that is a permanent finding for a permanent reason, which is the
`headline_vs_line_items` disease: a check that fires every morning teaches the
reader to skip the message it arrives in. But it must not be a *tolerance*
either, because "one row short" is also exactly what a broken mirror looks
like.

The watermark separates them, and this is what `meta.mirror_state` was written
for. Every successful mirror ships the **whole** current catalogue, so after a
success at time T, Postgres holds everything KeyCRM served at T. Therefore a
row DuckDB holds and Postgres does not:

* `synced_at <= last_ok_at` — KeyCRM was serving the catalogue when the mirror
  last succeeded and did not include this row. DuckDB is holding something
  KeyCRM has retired. **INFO**, counted and named, never a defect.
* `synced_at >  last_ok_at` — it arrived after the last successful mirror.
  Inside the grace window it is in flight; past it, the mirror lost a row.
  **CRITICAL**.

Retired rows are counted rather than ignored because they are a real question
for a later step — when the read switches, does the dashboard still need to
name a product KeyCRM has forgotten? — and a number nobody can see is a
question nobody will ask. That decision is not this check's to make.

WHAT ELSE THIS SEES THAT NOTHING ELSE DOES

`meta.mirror_state.failures_since_ok` is the mirror saying it is failing. The
module docstring in `core/pg_landing.py` says that state is published on
`/api/health`; it is not — nothing under `web/` imports `pg_landing` at all —
so until that is wired, a mirror failing every hour is visible only to someone
who queries Postgres by hand. It is read here, and a non-zero count is
CRITICAL.

A table that has never been mirrored is its own finding and **suppresses the
row-level ones**. On the day this was written `bronze.categories` was empty for
a scheduling reason, not a fault: categories are only written by the weekly
full sync. Without that suppression the check's first act would have been to
report 28 categories missing every morning until Sunday.

REPORT ONLY

Nothing here repairs anything, on the same grounds as `_silver_arc_check` and
the Gold cell check: a finding cannot reach `validation_passed`, so none of it
can drive a rebuild. A mirror that silently re-ships whatever it notices
missing would also destroy the signal this exists to produce.

SIZE

Both sides are pulled whole and compared in Python — 1,004 rows against 1,004.
That is the right shape for the catalogue and the wrong one for orders: when
`orders` and `order_products` join the mirror, the comparison has to move into
a per-day checksum with a drill-down, not 46,000 tuples in a list.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core.data_quality import IntegrityIssue, Severity
from core.landing_rows import CATEGORY_COLUMNS, PRODUCT_COLUMNS

logger = logging.getLogger(__name__)

# How long a row is allowed to be in DuckDB and not yet in Postgres before it
# counts as lost. The two writes are consecutive statements in one sync call,
# so the real gap is milliseconds; this is slack for a sync that is mid-flight
# when the check runs, not a tolerance.
MIRROR_GRACE_MINUTES = 15

# The layer name these runs are persisted under. Distinct from `integrity`
# (DuckDB alone) and `reconciliation` (DuckDB against KeyCRM): this is the one
# arc between two stores, and mixing it into either would make a single layer
# age cover two jobs with different cadences.
MIRROR_LAYER = "mirror_landing"


@dataclass(frozen=True)
class MirroredTable:
    """One table that exists on both sides, and how to compare it."""
    pg_table: str                      # bronze.products
    dk_table: str                      # products
    columns: Tuple[str, ...]           # the shared contract, from landing_rows
    numeric: Tuple[str, ...] = ()      # compared as Decimal, not as text


# Order matters only for reporting. Products first: it is the table that moves.
MIRRORED_TABLES: Tuple[MirroredTable, ...] = (
    MirroredTable(
        pg_table="bronze.products",
        dk_table="products",
        columns=tuple(PRODUCT_COLUMNS),
        numeric=("price",),
    ),
    MirroredTable(
        pg_table="bronze.categories",
        dk_table="categories",
        columns=tuple(CATEGORY_COLUMNS),
    ),
)


def configured() -> bool:
    """Is there a Postgres to compare against at all?

    False on a developer's machine and in the test suite, where the job must
    skip rather than persist a failed run every morning — a layer whose age is
    permanently stale reads as a broken check, not an absent one.
    """
    return bool(os.getenv("KS_PG_DSN", "").strip())


# ─── Normalisation ────────────────────────────────────────────────────────────


def _as_decimal(value: Any) -> Optional[Decimal]:
    """`DECIMAL(12,2)` on one side, `NUMERIC(12,2)` on the other, one type here.

    Both drivers already return `Decimal` for these columns; the conversion is
    for the case where one day one of them does not, and the quantize is so
    that a `0.0` never compares unequal to a `0.00` and sends somebody looking
    for a discrepancy that is a repr.
    """
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _normalise_row(
    row: Sequence[Any], columns: Sequence[str], numeric: Sequence[str],
) -> Tuple[Any, ...]:
    numeric_set = set(numeric)
    return tuple(
        _as_decimal(v) if c in numeric_set else v
        for c, v in zip(columns, row)
    )


# ─── Reads ────────────────────────────────────────────────────────────────────


def fetch_duckdb_rows(
    conn, spec: MirroredTable,
) -> Tuple[Dict[int, Tuple[Any, ...]], Dict[int, Optional[datetime]]]:
    """Every row of one landing table, plus when this store last wrote it.

    `synced_at` is read alongside but never compared: it is DuckDB's own
    bookkeeping and Postgres keeps `mirrored_at` for the same purpose, so two
    correct copies disagree on it by construction. It is here because it is
    what tells a retired row from a lost one.
    """
    cols = ", ".join(spec.columns)
    rows = conn.execute(
        f"SELECT {cols}, synced_at FROM {spec.dk_table}"
    ).fetchall()

    values: Dict[int, Tuple[Any, ...]] = {}
    synced: Dict[int, Optional[datetime]] = {}
    width = len(spec.columns)
    for row in rows:
        row_id = row[0]
        if row_id is None:
            continue
        values[int(row_id)] = _normalise_row(
            row[:width], spec.columns, spec.numeric,
        )
        synced[int(row_id)] = row[width]
    return values, synced


async def fetch_pg_rows(pool, spec: MirroredTable) -> Dict[int, Tuple[Any, ...]]:
    """Every row of the mirrored table, in the same shape as the DuckDB side."""
    cols = ", ".join(spec.columns)
    async with pool.acquire() as conn:
        records = await conn.fetch(f"SELECT {cols} FROM {spec.pg_table}")
    return {
        int(r[0]): _normalise_row(
            [r[c] for c in spec.columns], spec.columns, spec.numeric,
        )
        for r in records
        if r[0] is not None
    }


async def fetch_watermarks(pool) -> Dict[str, Dict[str, Any]]:
    """`meta.mirror_state`, keyed by table name."""
    async with pool.acquire() as conn:
        records = await conn.fetch(
            """
            SELECT table_name, last_attempted_at, last_ok_at,
                   failures_since_ok, last_error, last_rows, backfilled_at
            FROM meta.mirror_state
            """
        )
    return {r["table_name"]: dict(r) for r in records}


# ─── The comparison (pure) ────────────────────────────────────────────────────


def _watermark_findings(
    table: str, watermark: Optional[Mapping[str, Any]], dk_count: int,
) -> Tuple[List[IntegrityIssue], Optional[datetime]]:
    """What `meta.mirror_state` alone says about a table.

    Returns the findings and the last successful shipment, or `None` for that
    second value when the row-level comparison must not run at all. Shared by
    both comparison shapes because both need exactly this gate first: a table
    the mirror has never reached, or is failing to reach, cannot be compared
    row by row without the report becoming a restatement of that one fact.
    """
    issues: List[IntegrityIssue] = []

    failures = int((watermark or {}).get("failures_since_ok") or 0)
    if failures:
        last_error = (watermark or {}).get("last_error") or "no message recorded"
        issues.append(IntegrityIssue(
            check_name="mirror_failing",
            table_name=table,
            severity=Severity.CRITICAL,
            count=failures,
            description=(
                f"The mirror has failed {failures} time(s) in a row since its "
                f"last success, writing {table}. Last error: {last_error[:300]}. "
                "Postgres is behind by everything that has changed since, and "
                "the row counts below are measured against a stale copy."
            ),
        ))

    last_ok_at = (watermark or {}).get("last_ok_at")
    if last_ok_at is None:
        # Never shipped. Report that once, and do not also report every row in
        # the table as missing — the first is a fact about the schedule, the
        # second would be thousands of lines saying the same thing.
        issues.append(IntegrityIssue(
            check_name="mirror_never_shipped",
            table_name=table,
            severity=Severity.WARN,
            count=dk_count,
            description=(
                f"{table} has never been mirrored: no successful write is "
                f"recorded in meta.mirror_state, and DuckDB holds "
                f"{dk_count} row(s). Categories are only written by the "
                "weekly full sync, so this is expected until the first one "
                "runs; for any other table it means the mirror is not reaching "
                "this table at all. Row-level findings are suppressed until it "
                "has shipped once."
            ),
        ))
        return issues, None

    if last_ok_at.tzinfo is None:
        last_ok_at = last_ok_at.replace(tzinfo=timezone.utc)
    return issues, last_ok_at


def compare_table(
    spec: MirroredTable,
    dk_rows: Mapping[int, Tuple[Any, ...]],
    dk_synced: Mapping[int, Optional[datetime]],
    pg_rows: Mapping[int, Tuple[Any, ...]],
    watermark: Optional[Mapping[str, Any]],
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = MIRROR_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Both sides of one table, already read. No I/O, so it is testable whole."""
    now = now or datetime.now(timezone.utc)
    issues: List[IntegrityIssue] = []
    table = spec.pg_table

    issues, last_ok_at = _watermark_findings(table, watermark, len(dk_rows))
    if last_ok_at is None:
        return issues
    cutoff = now - timedelta(minutes=int(grace_minutes))

    # ── DuckDB has it, Postgres does not ──
    lost: List[int] = []
    retired: List[int] = []
    for row_id in dk_rows.keys() - pg_rows.keys():
        synced = dk_synced.get(row_id)
        if synced is not None and synced.tzinfo is None:
            synced = synced.replace(tzinfo=timezone.utc)
        if synced is None or synced <= last_ok_at:
            # The mirror shipped the whole catalogue after this row was last
            # written and Postgres still does not have it: KeyCRM is no longer
            # serving it. Not a mirror fault.
            retired.append(row_id)
        elif synced > cutoff:
            continue        # in flight: written since, mirror not run yet
        else:
            lost.append(row_id)

    if lost:
        issues.append(IntegrityIssue(
            check_name="mirror_missing_rows",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(lost),
            sample_ids=tuple(sorted(lost)[:max_samples]),
            description=(
                f"{len(lost)} row(s) written to DuckDB after the mirror's last "
                f"success at {last_ok_at.isoformat()} are absent from {table}, "
                f"and older than the {int(grace_minutes)}-minute grace window. "
                "The mirror lost them: it reported success and did not carry "
                "them. Check meta.mirror_state and the sync log for the run "
                "that covered them."
            ),
        ))

    if retired:
        issues.append(IntegrityIssue(
            check_name="mirror_retired_rows",
            table_name=table,
            severity=Severity.INFO,
            count=len(retired),
            sample_ids=tuple(sorted(retired)[:max_samples]),
            description=(
                f"{len(retired)} row(s) exist in DuckDB and cannot exist in "
                f"{table}: KeyCRM has stopped serving them, and a mirror fed by "
                "KeyCRM payloads can never learn of them again. DuckDB keeps "
                "them because upsert never deletes. Not a defect — but when the "
                "read switches, something has to decide whether the dashboard "
                "still needs to name them."
            ),
        ))

    # ── Postgres has it, DuckDB does not ──
    orphans = sorted(pg_rows.keys() - dk_rows.keys())
    if orphans:
        issues.append(IntegrityIssue(
            check_name="mirror_orphan_rows",
            table_name=table,
            severity=Severity.WARN,
            count=len(orphans),
            sample_ids=tuple(orphans[:max_samples]),
            description=(
                f"{len(orphans)} row(s) in {table} have nothing behind them in "
                "DuckDB. Nothing writes this table except the mirror, and the "
                "mirror only ever writes what DuckDB was given — so either "
                "DuckDB lost a row it once had, or something wrote to Postgres "
                "that is not the mirror."
            ),
        ))

    # ── both hold it, and disagree ──
    offenders: Dict[str, int] = {}
    differing: List[int] = []
    for row_id in dk_rows.keys() & pg_rows.keys():
        dk_row, pg_row = dk_rows[row_id], pg_rows[row_id]
        if dk_row == pg_row:
            continue
        differing.append(row_id)
        for column, dk_value, pg_value in zip(spec.columns, dk_row, pg_row):
            if dk_value != pg_value:
                offenders[column] = offenders.get(column, 0) + 1

    if differing:
        worst = ", ".join(
            f"{c} ({n})"
            for c, n in sorted(offenders.items(), key=lambda kv: -kv[1])[:5]
        )
        issues.append(IntegrityIssue(
            check_name="mirror_row_values",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(differing),
            sample_ids=tuple(sorted(differing)[:max_samples]),
            description=(
                f"{len(differing)} row(s) are present in both stores and "
                f"disagree. Columns: {worst}. Both sides are written from the "
                "same parsed tuple in the same call, so there is no arithmetic "
                "between them that could differ — a disagreement here is a "
                "defect in the write path, not drift."
            ),
        ))

    return issues


# ─── Orchestration ────────────────────────────────────────────────────────────


def read_duckdb_side(
    conn, specs: Sequence[MirroredTable] = MIRRORED_TABLES,
) -> Dict[str, Tuple[Dict[int, Tuple[Any, ...]], Dict[int, Optional[datetime]]]]:
    """Read every mirrored table out of DuckDB, keyed by its Postgres name.

    Separate from the comparison, and called separately, because the DuckDB
    connection is a lock the whole application shares: `asyncio.Lock` is not
    reentrant here and every other reader waits behind it. Awaiting a network
    round-trip to Postgres while holding it would stall the dashboard for the
    length of the check.
    """
    return {spec.pg_table: fetch_duckdb_rows(conn, spec) for spec in specs}


async def reconcile_mirror(
    dk_side: Mapping[
        str, Tuple[Mapping[int, Tuple[Any, ...]], Mapping[int, Optional[datetime]]]
    ],
    *,
    specs: Sequence[MirroredTable] = MIRRORED_TABLES,
    now: Optional[datetime] = None,
    grace_minutes: int = MIRROR_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare an already-read DuckDB side against Postgres.

    Raises rather than swallowing: unlike the mirror itself — which must never
    break a sync — a check that cannot run has produced no verdict, and the job
    above records that as a failed run so the layer goes stale instead of
    reading clean. See `fetch_last_success_ages`.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        # The kill switch is off, so Postgres is deliberately behind. Saying
        # "1,004 rows missing" would be true and useless.
        return [IntegrityIssue(
            check_name="mirror_disabled",
            table_name="meta.mirror_state",
            severity=Severity.WARN,
            count=0,
            description=(
                f"The mirror is switched off ({pg_landing.MIRROR_ENV}), so "
                "Postgres is behind by everything written since. Nothing is "
                "compared while it is off — this finding is the whole report."
            ),
        )]

    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    issues: List[IntegrityIssue] = []
    for spec in specs:
        dk_rows, dk_synced = dk_side[spec.pg_table]
        pg_rows = await fetch_pg_rows(pool, spec)
        issues += compare_table(
            spec,
            dk_rows,
            dk_synced,
            pg_rows,
            watermarks.get(spec.pg_table),
            now=now,
            grace_minutes=grace_minutes,
            max_samples=max_samples,
        )
    return issues


# ─── Orders: the same question, at a size that forbids the same method ────────
#
# The catalogue is compared by pulling both sides whole — 1,004 tuples against
# 1,004. Orders are 46,487 rows and 147,648 line items, and a daily job that
# materialises 194,000 tuples from two databases into one Python process, in a
# container that already gives DuckDB a 4 GB ceiling, is a memory incident
# waiting for a busy morning.
#
# So: fingerprint, then drill down.
#
# **Phase 1** groups both sides into buckets of 1,000 ids and computes an
# additive fingerprint per bucket — a count and one SUM per column. Roughly 47
# rows come back from each store. Buckets whose fingerprints agree are not
# looked at again.
#
# **Phase 2** pulls the full rows for the buckets that disagree, from both
# sides, and compares them column by column. A bucket is at most 1,000 orders,
# so the drill-down is bounded even when something is badly wrong — and capped
# again by `max_buckets`, because "every bucket disagrees" is a finding in
# itself and not a reason to read the whole table.
#
# WHAT AN ADDITIVE FINGERPRINT CAN AND CANNOT SEE
#
# It catches any change to a number, any change to a timestamp, and any change
# to a text column's *length*. It cannot see a text edit of exactly equal
# length that happens to fall in a bucket where nothing else moved — swapping
# one Cyrillic character for its Latin twin, say. That is the trade, and it is
# taken knowingly: the alternative is a cross-engine row hash, which needs both
# databases to format numerics and timestamps into text identically, and they
# do not.
#
# WHY NOT `updated_at`, WHICH WOULD BE EXACT AND CHEAP
#
# Because it lies here, and the code says so out loud: `upsert_orders` takes
# `force_update` "for status refresh since KeyCRM doesn't update updated_at when
# status changes". An order can move from status 12 to status 20 — from revenue
# to revenue, or out of it — with `updated_at` untouched on both sides. A
# comparison keyed on it would have been blind to precisely the field the
# business cares about most.
#
# THE GATE
#
# `last_ok_at` cannot license a tolerance of zero here the way it does for the
# catalogue. That rule works because a catalogue mirror ships the whole table,
# so a successful write proves what Postgres held. The orders mirror ships
# `updated_ids`, which proves only that the last delta landed. Until the
# backfill has carried history across, "missing from Postgres" means "not
# carried yet" — so the comparison is gated on
# `meta.mirror_state.backfilled_at`, and once that is set, any difference is a
# defect with nothing left to excuse it.

BUCKET_SIZE = 1000

# One SUM per column, and the column's kind decides which SUM.
_INT, _NUMERIC, _TS, _TEXT = "int", "numeric", "ts", "text"


@dataclass(frozen=True)
class BucketedTable:
    """A table too large to compare row by row every morning."""
    pg_table: str
    dk_table: str
    columns: Tuple[str, ...]
    fields: Tuple[Tuple[str, str], ...]     # (column, kind), for the fingerprint
    bucket_column: str
    numeric: Tuple[str, ...]
    dk_rows_sql: str                        # one bucket, plus DuckDB's synced_at
    pg_rows_sql: str


_ORDER_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("id", _INT), ("source_id", _INT), ("status_id", _INT),
    ("status_group_id", _INT), ("grand_total", _NUMERIC),
    ("ordered_at", _TS), ("created_at", _TS), ("updated_at", _TS),
    ("buyer_id", _INT), ("manager_id", _INT),
    ("manager_comment", _TEXT), ("promocode", _TEXT),
)

_ORDER_PRODUCT_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("id", _INT), ("order_id", _INT), ("product_id", _INT),
    ("name", _TEXT), ("quantity", _INT), ("price_sold", _NUMERIC),
)

_ORDER_COLS = tuple(c for c, _ in _ORDER_FIELDS)
_ORDER_PRODUCT_COLS = tuple(c for c, _ in _ORDER_PRODUCT_FIELDS)

ORDER_TABLES: Tuple[BucketedTable, ...] = (
    BucketedTable(
        pg_table="bronze.orders",
        dk_table="orders",
        columns=_ORDER_COLS,
        fields=_ORDER_FIELDS,
        bucket_column="id",
        numeric=("grand_total",),
        dk_rows_sql=(
            f"SELECT {', '.join(_ORDER_COLS)}, synced_at FROM orders "
            f"WHERE (id // {BUCKET_SIZE}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_ORDER_COLS)} FROM bronze.orders "
            f"WHERE (id / {BUCKET_SIZE}) = $1"
        ),
    ),
    BucketedTable(
        pg_table="bronze.order_products",
        dk_table="order_products",
        columns=_ORDER_PRODUCT_COLS,
        fields=_ORDER_PRODUCT_FIELDS,
        bucket_column="order_id",
        numeric=("price_sold",),
        # A line item has no `synced_at` of its own; its order's is the right
        # one, because the two are written in the same call on both sides.
        dk_rows_sql=(
            "SELECT "
            + ", ".join(f"p.{c}" for c in _ORDER_PRODUCT_COLS)
            + ", o.synced_at FROM order_products p "
            "LEFT JOIN orders o ON o.id = p.order_id "
            f"WHERE (p.order_id // {BUCKET_SIZE}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_ORDER_PRODUCT_COLS)} FROM bronze.order_products "
            f"WHERE (order_id / {BUCKET_SIZE}) = $1"
        ),
    ),
)


def _dk_term(column: str, kind: str) -> str:
    if kind == _TS:
        # Microseconds as an integer. Not `extract(epoch ...)`, which is a
        # float on one side and a numeric on the other and would compare two
        # roundings rather than two values.
        return f"SUM(COALESCE(epoch_us({column}), 0))"
    if kind == _TEXT:
        return f"SUM(LENGTH(COALESCE({column}, '')))"
    return f"SUM(COALESCE({column}, 0))"


def _pg_term(column: str, kind: str) -> str:
    if kind == _TS:
        return (
            f"SUM(COALESCE((EXTRACT(EPOCH FROM {column}) * 1000000)::bigint, 0))"
        )
    if kind == _TEXT:
        return f"SUM(LENGTH(COALESCE({column}, '')))"
    return f"SUM(COALESCE({column}, 0))"


def duckdb_fingerprint_sql(spec: BucketedTable) -> str:
    terms = ", ".join(_dk_term(c, k) for c, k in spec.fields)
    return (
        f"SELECT ({spec.bucket_column} // {BUCKET_SIZE})::BIGINT AS bucket, "
        f"COUNT(*), {terms} FROM {spec.dk_table} GROUP BY 1 ORDER BY 1"
    )


def postgres_fingerprint_sql(spec: BucketedTable) -> str:
    terms = ", ".join(_pg_term(c, k) for c, k in spec.fields)
    return (
        f"SELECT ({spec.bucket_column} / {BUCKET_SIZE})::bigint AS bucket, "
        f"COUNT(*), {terms} FROM {spec.pg_table} GROUP BY 1 ORDER BY 1"
    )


def _as_comparable(value: Any) -> Any:
    """One numeric type for a fingerprint cell.

    DuckDB returns int, Decimal or HUGEINT; asyncpg returns int or Decimal for
    the same expressions. `int == Decimal` already holds in Python, so this
    only has to make sure nothing arrives as a float — which would compare two
    roundings instead of two values.
    """
    if value is None:
        return Decimal(0)
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def fingerprints(conn, spec: BucketedTable) -> Dict[int, Tuple[Any, ...]]:
    """One row per bucket from DuckDB: the count and one SUM per column."""
    return {
        int(row[0]): tuple(_as_comparable(v) for v in row[1:])
        for row in conn.execute(duckdb_fingerprint_sql(spec)).fetchall()
    }


async def pg_fingerprints(pool, spec: BucketedTable) -> Dict[int, Tuple[Any, ...]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(postgres_fingerprint_sql(spec))
    return {
        int(r[0]): tuple(_as_comparable(v) for v in tuple(r)[1:])
        for r in rows
    }


def disagreeing_buckets(
    dk: Mapping[int, Tuple[Any, ...]], pg: Mapping[int, Tuple[Any, ...]],
) -> List[int]:
    """Every bucket the two stores do not agree on, present-on-one-side included."""
    return sorted(
        b for b in set(dk) | set(pg)
        if dk.get(b) != pg.get(b)
    )


def _read_dk_bucket(
    conn, spec: BucketedTable, bucket: int,
) -> Tuple[Dict[int, Tuple[Any, ...]], Dict[int, Optional[datetime]]]:
    rows = conn.execute(spec.dk_rows_sql, [bucket]).fetchall()
    width = len(spec.columns)
    values, synced = {}, {}
    for row in rows:
        row_id = int(row[0])
        values[row_id] = _normalise_row(row[:width], spec.columns, spec.numeric)
        synced[row_id] = row[width]
    return values, synced


async def _read_pg_bucket(
    pool, spec: BucketedTable, bucket: int,
) -> Dict[int, Tuple[Any, ...]]:
    async with pool.acquire() as conn:
        records = await conn.fetch(spec.pg_rows_sql, bucket)
    return {
        int(r[0]): _normalise_row(
            [r[c] for c in spec.columns], spec.columns, spec.numeric,
        )
        for r in records
    }


@dataclass
class _Divergence:
    """What the drill-down found, accumulated across buckets."""
    missing: List[int] = field(default_factory=list)     # in DuckDB, not in Postgres
    orphans: List[int] = field(default_factory=list)     # in Postgres, not in DuckDB
    differing: List[int] = field(default_factory=list)   # in both, and unequal
    offenders: Dict[str, int] = field(default_factory=dict)


def compare_bucket(
    spec: BucketedTable,
    dk_rows: Mapping[int, Tuple[Any, ...]],
    dk_synced: Mapping[int, Optional[datetime]],
    pg_rows: Mapping[int, Tuple[Any, ...]],
    found: _Divergence,
    *,
    now: datetime,
    grace_minutes: int,
) -> None:
    """One bucket's rows, compared exactly. Accumulates into `found`.

    The grace window is the only leniency, and it is not a tolerance: the two
    writes are consecutive statements in one sync call, so a row DuckDB wrote
    seconds ago may legitimately still be in flight. Everything older than the
    window is a defect, because after the backfill there is nothing else a
    difference could be.
    """
    cutoff = now - timedelta(minutes=int(grace_minutes))

    def _in_flight(row_id: int) -> bool:
        synced = dk_synced.get(row_id)
        if synced is None:
            return False
        if synced.tzinfo is None:
            synced = synced.replace(tzinfo=timezone.utc)
        return synced > cutoff

    for row_id in dk_rows.keys() - pg_rows.keys():
        if not _in_flight(row_id):
            found.missing.append(row_id)

    found.orphans.extend(pg_rows.keys() - dk_rows.keys())

    for row_id in dk_rows.keys() & pg_rows.keys():
        dk_row, pg_row = dk_rows[row_id], pg_rows[row_id]
        if dk_row == pg_row or _in_flight(row_id):
            continue
        found.differing.append(row_id)
        for column, dk_value, pg_value in zip(spec.columns, dk_row, pg_row):
            if dk_value != pg_value:
                found.offenders[column] = found.offenders.get(column, 0) + 1


def _divergence_findings(
    spec: BucketedTable, found: _Divergence, *, max_samples: int,
) -> List[IntegrityIssue]:
    issues: List[IntegrityIssue] = []
    table = spec.pg_table

    if found.missing:
        issues.append(IntegrityIssue(
            check_name="mirror_missing_rows",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(found.missing),
            sample_ids=tuple(sorted(found.missing)[:max_samples]),
            description=(
                f"{len(found.missing)} row(s) are in DuckDB and not in {table}. "
                "History has been carried across (meta.mirror_state.backfilled_at "
                "is set) and these are older than the grace window, so there is "
                "nothing left for this to be except rows the mirror lost. "
                "Re-running the backfill ships them; find out why they were "
                "missed first."
            ),
        ))

    if found.orphans:
        issues.append(IntegrityIssue(
            check_name="mirror_orphan_rows",
            table_name=table,
            severity=Severity.WARN,
            count=len(found.orphans),
            sample_ids=tuple(sorted(found.orphans)[:max_samples]),
            description=(
                f"{len(found.orphans)} row(s) in {table} have nothing behind "
                "them in DuckDB. Nothing writes this table except the mirror "
                "and the backfill, and both only ever write what DuckDB holds — "
                "so either DuckDB lost a row it once had, or something else "
                "wrote to Postgres."
            ),
        ))

    if found.differing:
        worst = ", ".join(
            f"{c} ({n})"
            for c, n in sorted(found.offenders.items(), key=lambda kv: -kv[1])[:5]
        )
        issues.append(IntegrityIssue(
            check_name="mirror_row_values",
            table_name=table,
            severity=Severity.CRITICAL,
            count=len(found.differing),
            sample_ids=tuple(sorted(found.differing)[:max_samples]),
            description=(
                f"{len(found.differing)} row(s) are in both stores and "
                f"disagree. Columns: {worst}. Both sides are written from the "
                "same parsed tuple in the same call, so there is no arithmetic "
                "between them that could drift — a disagreement here is a "
                "defect in the write path."
            ),
        ))

    return issues


async def reconcile_orders(
    store,
    *,
    specs: Sequence[BucketedTable] = ORDER_TABLES,
    now: Optional[datetime] = None,
    grace_minutes: int = MIRROR_GRACE_MINUTES,
    max_samples: int = 10,
    max_buckets: int = 20,
) -> List[IntegrityIssue]:
    """Reconciliation A for the tables that are too big to pull whole.

    Takes the store rather than an open connection, because this interleaves:
    fingerprints from DuckDB, fingerprints from Postgres, then the rows of the
    buckets that disagree from both. Each DuckDB acquisition is short and none
    of them spans a network round-trip — the store's lock is what every
    dashboard request queues behind.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []          # `reconcile_mirror` already says so, once.

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    # Phase 1, DuckDB side: one acquisition for every table.
    async with store.connection() as conn:
        dk_prints = {s.pg_table: fingerprints(conn, s) for s in specs}

    issues: List[IntegrityIssue] = []
    suspects: Dict[str, List[int]] = {}
    for spec in specs:
        dk_print = dk_prints[spec.pg_table]
        dk_count = sum(int(v[0]) for v in dk_print.values())
        watermark = watermarks.get(spec.pg_table)

        found, last_ok_at = _watermark_findings(spec.pg_table, watermark, dk_count)
        issues += found
        if last_ok_at is None:
            continue

        if (watermark or {}).get("backfilled_at") is None:
            # The gate. Without it every order that predates the mirror reads
            # as lost, and the first run of this check would file tens of
            # thousands of CRITICAL findings for a job nobody has run yet.
            pg_count = sum(
                int(v[0]) for v in (await pg_fingerprints(pool, spec)).values()
            )
            issues.append(IntegrityIssue(
                check_name="mirror_backfill_pending",
                table_name=spec.pg_table,
                severity=Severity.WARN,
                count=max(dk_count - pg_count, 0),
                description=(
                    f"{spec.pg_table} holds {pg_count} row(s) against DuckDB's "
                    f"{dk_count}, and no completed backfill is recorded. The "
                    "mirror ships only what a sync writes, so history does not "
                    "arrive on its own: run POST /api/mirror/backfill/orders. "
                    "Row-level comparison is suppressed until it completes — "
                    "before that, 'missing' and 'lost' are the same picture."
                ),
            ))
            continue

        pg_print = await pg_fingerprints(pool, spec)
        differing = disagreeing_buckets(dk_print, pg_print)
        if len(differing) > max_buckets:
            issues.append(IntegrityIssue(
                check_name="mirror_buckets_disagree",
                table_name=spec.pg_table,
                severity=Severity.CRITICAL,
                count=len(differing),
                sample_ids=tuple(differing[:max_samples]),
                description=(
                    f"{len(differing)} of {len(set(dk_print) | set(pg_print))} "
                    f"id buckets disagree between the two stores — more than "
                    f"the {max_buckets} this check will open. That is a whole-"
                    "table problem, not a row problem, and reading them all "
                    "would turn a daily check into a table scan of both stores. "
                    f"Buckets are {BUCKET_SIZE} ids wide."
                ),
            ))
            differing = differing[:max_buckets]
        suspects[spec.pg_table] = differing

    if not any(suspects.values()):
        return issues

    # Phase 2: only the buckets that disagree, one acquisition for all of them.
    dk_buckets: Dict[str, Dict[int, Any]] = {}
    async with store.connection() as conn:
        for spec in specs:
            dk_buckets[spec.pg_table] = {
                bucket: _read_dk_bucket(conn, spec, bucket)
                for bucket in suspects.get(spec.pg_table, ())
            }

    for spec in specs:
        found = _Divergence()
        for bucket in suspects.get(spec.pg_table, ()):
            dk_rows, dk_synced = dk_buckets[spec.pg_table][bucket]
            pg_rows = await _read_pg_bucket(pool, spec, bucket)
            compare_bucket(
                spec, dk_rows, dk_synced, pg_rows, found,
                now=now, grace_minutes=grace_minutes,
            )
        issues += _divergence_findings(spec, found, max_samples=max_samples)

    return issues
