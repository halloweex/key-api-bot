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
from dataclasses import dataclass
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
                   failures_since_ok, last_error, last_rows
            FROM meta.mirror_state
            """
        )
    return {r["table_name"]: dict(r) for r in records}


# ─── The comparison (pure) ────────────────────────────────────────────────────


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
        # second would be 1,004 lines of noise saying the same thing.
        issues.append(IntegrityIssue(
            check_name="mirror_never_shipped",
            table_name=table,
            severity=Severity.WARN,
            count=len(dk_rows),
            description=(
                f"{table} has never been mirrored: no successful write is "
                f"recorded in meta.mirror_state, and DuckDB holds "
                f"{len(dk_rows)} row(s). Categories are only written by the "
                "weekly full sync, so this is expected until the first one "
                "runs; for any other table it means the mirror is not reaching "
                "this table at all. Row-level findings are suppressed until it "
                "has shipped once."
            ),
        ))
        return issues

    if last_ok_at.tzinfo is None:
        last_ok_at = last_ok_at.replace(tzinfo=timezone.utc)
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
