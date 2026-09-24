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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from core.data_quality import IntegrityIssue, Severity
from core.landing_rows import CATEGORY_COLUMNS, PRODUCT_COLUMNS
from core.pg_bot_state import (
    AUTHORIZED_COLUMNS,
    MILESTONE_COLUMNS,
    PREFERENCE_COLUMNS,
    REPORT_HISTORY_COLUMNS,
)
from core.pg_operational import (
    BUYER_GENDER_COLUMNS,
    DATA_DIR_SAMPLE_COLUMNS,
    DISK_SAMPLE_COLUMNS,
    GOAL_COLUMNS,
    GROWTH_METRIC_COLUMNS,
    INVENTORY_HISTORY_COLUMNS,
    MANUAL_EXPENSE_COLUMNS,
    MEMORY_SAMPLE_COLUMNS,
    MISS_COLUMNS,
    OFFER_STOCK_COLUMNS,
    PREDICTION_COLUMNS,
    SEASONAL_COLUMNS,
    DQ_DIFF_COLUMNS,
    DQ_ISSUE_COLUMNS,
    DQ_RUN_COLUMNS,
    OFFER_COLUMNS,
    SYNC_METADATA_COLUMNS,
    SYNC_METADATA_SOURCE,
    RECONCILIATION_LOG_COLUMNS,
    REFRESH_COLUMNS,
    SKU_STATUS_COLUMNS,
    TRAFFIC_SEND_COLUMNS,
    WEEKLY_PATTERN_COLUMNS,
    WEEKLY_SEND_COLUMNS,
)
from core.landing_rows import EXPENSE_COLUMNS, EXPENSE_TYPE_COLUMNS
from core.pg_order_utm import UTM_COLUMNS
from core import pg_order_versions as _versions
from core.pg_dashboard_users import USER_COLUMNS
from core.pg_replication import CLASSIFICATION_COLUMNS, MANAGER_COLUMNS
from core.pg_sms import (
    CAMPAIGN_COLUMNS,
    DLR_COLUMNS,
    MEMBER_COLUMNS,
    MEMBER_STAMP,
    OPTOUT_COLUMNS,
    PRESET_COLUMNS,
)

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
    # What identifies a row. `manager_classifications` is effective-dated and
    # keyed `(manager_id, valid_from)`; everything else has an `id`.
    key_columns: Tuple[str, ...] = ("id",)
    # Where the grace window reads "when did this store last write the row".
    # None means the table has no such column and nothing is ever in flight.
    synced_column: Optional[str] = "synced_at"
    # Why a disagreement here is a defect, in this table's own terms. The
    # default is the landing mirror's story and it is true only of landing:
    # `bronze.products` really is one parsed tuple handed to two stores. The
    # replicated tables are a *copy* — of DuckDB, or of SQLite through a type
    # conversion — and telling their reader otherwise sends them looking for a
    # write path that does not exist. Found by reading a real finding: a
    # changed `language` in `app.user_preferences` was reported as impossible
    # arithmetic between two parsers.
    origin_note: str = (
        "Both sides are written from the same parsed tuple in the same call, "
        "so there is no arithmetic between them that could differ — a "
        "disagreement here is a defect in the write path, not drift."
    )
    # Columns both stores hold, both stores ship, and no comparison can learn
    # anything from. `app.sku_inventory_status.updated_at` is the case that
    # forced this: the DuckDB refresh is a `DELETE`+`INSERT` that writes
    # `CURRENT_TIMESTAMP` to every row (core/repositories/inventory.py:251),
    # so the stamp records when the table was last rebuilt, not anything about
    # the SKU. The replica is copied ~10 minutes before the next rebuild, so
    # the two copies hold different stamps on every row, always — 891 of 891
    # rows on 2026-09-01, screaming CRITICAL for days while the three rows
    # that really did differ (`reserve`) sat invisible inside the number.
    #
    # `fetch_duckdb_rows` already promises the stamp is "read alongside but
    # never compared". That promise silently fails when `synced_column` is
    # also listed in `columns`, which is exactly this table's shape. This is
    # the promise made explicit and per-column, rather than widened into a
    # rule: `app.order_backfill_misses.checked_at` is deliberately both the
    # clock and a compared value, and it is right to be, because it moves only
    # when that row is genuinely re-recorded.
    ignore_columns: Tuple[str, ...] = ()

    # True when the writer replaces the whole table rather than upserting.
    #
    # This decides what a row present in DuckDB and absent from Postgres
    # *means*, and the two answers are opposites. For an upserting mirror fed
    # by KeyCRM payloads, a row written before the last successful ship and
    # still absent is one KeyCRM has retired — INFO, not a defect. For a full
    # replace there is no such category: the writer wrote every row it holds,
    # so anything missing was lost.
    full_replace: bool = False

    # True when the DuckDB writer deletes rows by age on a schedule of its own.
    #
    # This is the third answer to "Postgres has a row and DuckDB does not", and
    # it exists because `full_replace` alone gets it wrong. A full replace ends
    # each run with the two copies identical, so an orphan can only be a row
    # that left DuckDB *after* the copy — and for these tables that is exactly
    # what retention does. `core/disk_monitor.py` and `core/memory_monitor.py`
    # both run `DELETE FROM <t> WHERE sampled_at < <cutoff>`, the memory sweep
    # every thirty minutes, against an hourly replication and a daily 07:30
    # comparison. Left as plain orphans, roughly one or two rows a day would be
    # WARN for ever, and the finding would be entirely manufactured by the
    # migration that created the copy.
    #
    # The test is derived, not configured: an orphan whose clock is older than
    # *everything DuckDB still holds* was removed by the sweep, because the
    # sweep is the only statement that removes anything and it always removes
    # the oldest. Nothing here restates "14 days"; a retention constant copied
    # into the comparison is a second place for it to be wrong.
    #
    # Opt-in rather than inferred, because for every other table the same shape
    # means the opposite. An `app.order_backfill_misses` row older than
    # anything DuckDB holds is a row DuckDB lost, and calling that a sweep
    # would silence the one finding that matters.
    prunes_by_age: bool = False

    # True for a table `core.pg_operational.replicate_operational` copies.
    #
    # That copy is one transaction every hour, and a failed run stamps the
    # failure on every table it was copying. One failed run is usually repaired
    # by the next — a lock held through a deploy's migration, the old web
    # container meeting the new revision, a pool blip — and read as CRITICAL
    # it would page about ~26 tables at 07:30 for a copy an hour from healthy.
    # So `mirror_failing` is WARN while that repair is still on schedule: one
    # failure, with the last success inside the comparison's grace (the same
    # grace that forgives rows in flight). It is CRITICAL from the second
    # failure in a row, or once that success is older than the grace — which,
    # with only the hourly copy running and a 90-minute grace, is a lone
    # failure more than half an hour before the check; by then rows written
    # since the success read as lost too. A failure that repeats — a duplicate id in DuckDB's
    # `reconciliation_log` fails every run the same way — is CRITICAL within
    # two hourly runs.
    #
    # Opt-in, and set only on `OPERATIONAL_TABLES` and `APPEND_ONLY_TABLES`.
    # The landing mirrors and the other replicated copies keep every failure
    # CRITICAL: they have their own cadences, and whether one failure is noise
    # there is a separate question this flag does not answer.
    one_failure_warns: bool = False

    @property
    def stamp_is_per_row(self) -> bool:
        """Whether `synced_column` dates *this row* or the whole rebuild.

        The grace window forgives a row that was written between the copy and
        the check, and it is a per-row leniency — it asks "was this row in
        flight". That question only means something when the stamp moves for
        the row it belongs to.

        `app.sku_inventory_status.updated_at` is the exception that made this
        explicit: the DuckDB refresh restamps all 891 rows on every rebuild, so
        the answer is the same for every row and the leniency stops being per
        row — it forgives the whole table or none of it, and the first of those
        would hide a real defect behind a recent rebuild.

        Derived rather than declared a second time: a whole-table stamp is
        exactly the thing that had to be dropped from the comparison, so
        `ignore_columns` already carries the answer and the two cannot drift.
        A stamp that is an expression over the row's own columns
        (`COALESCE(delivered_at, ...)`) is per-row like any other.
        """
        return (
            self.synced_column is not None
            and self.synced_column not in self.ignore_columns
        )

    @property
    def sample_index(self) -> int:
        """Which column supplies `IntegrityIssue.sample_ids`.

        Always the first key column: sample ids are integers, and a composite
        key cannot be one. For `manager_classifications` that is the manager,
        which is what a human would go looking for anyway.
        """
        return self.columns.index(self.key_columns[0])


# What a disagreement means for a table that is *copied* rather than mirrored.
# Landing is one parsed tuple handed to two stores; these are not, and a
# finding that says otherwise sends its reader hunting a shared write path that
# does not exist.
_COPIED_FROM_DUCKDB = (
    "This table is copied out of DuckDB as it stands, not parsed twice, so a "
    "disagreement is the copy: it did not run, it ran against a different "
    "snapshot, or something other than the replicator wrote this table."
)
_COPIED_FROM_SQLITE = (
    "This table is copied out of data/bot.db through a type conversion "
    "(SQLite has no boolean and no timezone), so a disagreement is either the "
    "copy not having run or the conversion — check the column named above "
    "against core/pg_bot_state._convert before anything else."
)


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
    # Not a mirror — a replica. `is_retail` and the effective-dated intervals
    # are decisions KeyCRM cannot supply, so `core/pg_replication.py` copies
    # what DuckDB holds rather than re-deriving them. `full_replace` because
    # that writer rewrites both tables whole, which is also what makes a
    # missing row here unambiguous: there is no "KeyCRM retired it" to mean.
    MirroredTable(
        pg_table="bronze.managers",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="managers",
        columns=MANAGER_COLUMNS,
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.manager_classifications",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="manager_classifications",
        columns=CLASSIFICATION_COLUMNS,
        key_columns=("manager_id", "valid_from"),
        # `set_at` is when a human decided, which is exactly the right clock:
        # the endpoint replicates immediately, so a classification decided
        # seconds ago may legitimately still be in flight.
        synced_column="set_at",
        full_replace=True,
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


# Substituted for a column both stores hold and neither can agree on — see
# `MirroredTable.ignore_columns`. A sentinel rather than dropping the column,
# because `_row_key` and `sample_index` address values by position and a
# shorter tuple would silently move the key.
_IGNORED = "<ignored>"


def _normalise_row(
    row: Sequence[Any], columns: Sequence[str], numeric: Sequence[str],
    ignored: Sequence[str] = (),
) -> Tuple[Any, ...]:
    numeric_set = set(numeric)
    ignored_set = set(ignored)
    return tuple(
        _IGNORED if c in ignored_set
        else _as_decimal(v) if c in numeric_set else v
        for c, v in zip(columns, row)
    )


# ─── Reads ────────────────────────────────────────────────────────────────────


def _row_key(spec: MirroredTable, row: Sequence[Any]) -> Any:
    """The identity of a row: an int for a simple key, a tuple otherwise.

    A single key column is not always a number — `app.inventory_history` is
    keyed by the day. Coercing it to `int` used to be unconditional, which was
    correct for every table that existed at the time and a `TypeError` for the
    first one that did not.
    """
    values = tuple(row[spec.columns.index(c)] for c in spec.key_columns)
    if len(values) != 1:
        return values
    try:
        return int(values[0])
    except (TypeError, ValueError):
        return values[0]


def fetch_duckdb_rows(
    conn, spec: MirroredTable,
) -> Tuple[Dict[Any, Tuple[Any, ...]], Dict[Any, Optional[datetime]]]:
    """Every row of one landing table, plus when this store last wrote it.

    The timestamp is read alongside but never compared: it is DuckDB's own
    bookkeeping and Postgres keeps `mirrored_at` for the same purpose, so two
    correct copies disagree on it by construction. It is here because it is
    what tells a row still in flight from one that was lost.
    """
    cols = ", ".join(spec.columns)
    stamp = spec.synced_column
    rows = conn.execute(
        f"SELECT {cols}" + (f", {stamp}" if stamp else "") + f" FROM {spec.dk_table}"
    ).fetchall()

    values: Dict[Any, Tuple[Any, ...]] = {}
    synced: Dict[Any, Optional[datetime]] = {}
    width = len(spec.columns)
    for row in rows:
        if any(row[spec.columns.index(c)] is None for c in spec.key_columns):
            continue
        key = _row_key(spec, row)
        values[key] = _normalise_row(
            row[:width], spec.columns, spec.numeric, spec.ignore_columns,
        )
        synced[key] = row[width] if stamp else None
    return values, synced


async def fetch_pg_rows(pool, spec: MirroredTable) -> Dict[Any, Tuple[Any, ...]]:
    """Every row of the mirrored table, in the same shape as the DuckDB side."""
    cols = ", ".join(spec.columns)
    async with pool.acquire() as conn:
        records = await conn.fetch(f"SELECT {cols} FROM {spec.pg_table}")
    out: Dict[Any, Tuple[Any, ...]] = {}
    for record in records:
        row = [record[c] for c in spec.columns]
        if any(record[c] is None for c in spec.key_columns):
            continue
        out[_row_key(spec, row)] = _normalise_row(
            row, spec.columns, spec.numeric, spec.ignore_columns,
        )
    return out


# The tables whose freshness a watchdog outside this container is expected to
# judge. `bronze.orders` alone, deliberately: it is the copy that carries the
# money, and it is the one whose failure used to wait for 07:30 — a whole day
# in which the main copy could be broken with nobody told. The ClickHouse rows
# are NOT here; that store is optional and its daily window of silence was
# accepted and written down when step 4 shipped.
#
# Deliberately NOT stood down when a write chain owns the order tables
# (DN-22b, settling DN-22a's review): the chain's writer ships through
# `pg_landing.write_orders`, the only writer of `bronze.orders` there may be,
# so the watermark keeps moving under the chain and an 8-hour silence still
# means orders stopped landing. `tests/unit/test_write_chains.py` pins both
# halves — one writer, and this watch not asking the registry.
WATCHED_MIRRORS: Tuple[str, ...] = ("bronze.orders",)


async def fetch_mirror_freshness(
    pool, tables: Tuple[str, ...] = WATCHED_MIRRORS, now: Optional[datetime] = None,
) -> Dict[str, Dict[str, Any]]:
    """Age of the last successful shipment per watched table, plus its failures.

    Two numbers, because on their own each one lies in a different direction:

    - **age alone cannot be tight.** `mirror_orders` refuses to move the
      watermark when there was nothing to ship, so a quiet night is
      indistinguishable from a dead mirror by age. Measured on production,
      the orders watermark legitimately stands still from about 01:00 until
      the 05:15 status refresh, and again until traffic resumes around 07:00.
    - **failures alone cannot be complete.** A mirror switched off, or a sync
      that stopped feeding it, never raises and so never counts a failure.

    `failures_since_ok` is what makes the age threshold affordable: an actively
    failing mirror is caught on the next sync tick regardless of how generous
    the age limit is.

    A table with no row at all reports every field None — it has never shipped,
    which is a different thing from having shipped long ago and must not be
    flattened into a large age.
    """
    reference = now or datetime.now(timezone.utc)
    rows = await fetch_watermarks(pool)
    out: Dict[str, Dict[str, Any]] = {}
    for table in tables:
        state = rows.get(table)
        last_ok = (state or {}).get("last_ok_at")
        out[table] = {
            "last_ok_at": last_ok.isoformat() if last_ok else None,
            "age_seconds": (
                int((reference - last_ok).total_seconds()) if last_ok else None
            ),
            "failures_since_ok": (
                int(state["failures_since_ok"] or 0) if state else None
            ),
            # The text is deliberately not published: it is an exception string
            # from a database driver, and /api/health is public. Whether there
            # is one is the part a watchdog acts on.
            "failing": bool(state and state.get("last_error")),
        }
    return out


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


def _as_sample_id(value: Any) -> Optional[int]:
    """One key rendered as the integer `IntegrityIssue.sample_ids` holds.

    A date becomes `20260827`, which is the only integer form of a day anybody
    reads back as a day. Anything else that is not a number is dropped rather
    than forced: a sample id nobody can look anything up with is worse than no
    sample id, and the count and the table name are in the description anyway.
    """
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.year * 10000 + value.month * 100 + value.day
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _sample(spec: MirroredTable, keys, limit: int) -> Tuple[int, ...]:
    """Up to `limit` sample ids, from the first key column.

    `IntegrityIssue.sample_ids` is a tuple of ints and an effective-dated key
    is a tuple of (manager, date). The manager is what a human goes looking
    for, so that is what is reported.
    """
    out = []
    for key in sorted(keys)[:limit]:
        rendered = _as_sample_id(key[0] if isinstance(key, tuple) else key)
        if rendered is not None:
            out.append(rendered)
    return tuple(out)


def _watermark_findings(
    table: str, watermark: Optional[Mapping[str, Any]], dk_count: int,
    *,
    one_failure_warns_within: Optional[timedelta] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[IntegrityIssue], Optional[datetime]]:
    """What `meta.mirror_state` alone says about a table.

    Returns the findings and the last successful shipment, or `None` for that
    second value when the row-level comparison must not run at all. Shared by
    both comparison shapes because both need exactly this gate first: a table
    the mirror has never reached, or is failing to reach, cannot be compared
    row by row without the report becoming a restatement of that one fact.

    `one_failure_warns_within` is the grace, passed only for a table whose
    spec sets `one_failure_warns`: a single failure since a success inside it
    is WARN rather than CRITICAL. None keeps every failure CRITICAL.
    """
    issues: List[IntegrityIssue] = []

    failures = int((watermark or {}).get("failures_since_ok") or 0)
    if failures:
        last_error = (watermark or {}).get("last_error") or "no message recorded"
        if _repair_still_on_schedule(
            failures, (watermark or {}).get("last_ok_at"),
            one_failure_warns_within, now,
        ):
            issues.append(IntegrityIssue(
                check_name="mirror_failing",
                table_name=table,
                severity=Severity.WARN,
                count=failures,
                description=(
                    f"The last copy of {table} failed, once since its last "
                    "success, which is still inside the "
                    f"{int(one_failure_warns_within.total_seconds() // 60)}-minute "
                    "grace this schedule allows; the next hourly run usually "
                    f"repairs it. Last error: {last_error[:300]}. CRITICAL if "
                    "the next run fails too, or once the last success is older "
                    "than the grace."
                ),
            ))
        else:
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


def _repair_still_on_schedule(
    failures: int,
    last_ok_at: Optional[datetime],
    grace: Optional[timedelta],
    now: Optional[datetime],
) -> bool:
    """One failure, and a last success recent enough that the next run can
    still repair it before the copy is staler than its schedule allows."""
    if grace is None or failures != 1 or last_ok_at is None:
        return False
    if last_ok_at.tzinfo is None:
        last_ok_at = last_ok_at.replace(tzinfo=timezone.utc)
    return (now or datetime.now(timezone.utc)) - last_ok_at <= grace


def _pruned_by_age(
    spec: MirroredTable,
    dk_synced: Mapping[Any, Optional[datetime]],
    pg_rows: Mapping[Any, Tuple[Any, ...]],
    orphans: Sequence[Any],
) -> set:
    """Which orphans DuckDB's own retention sweep removed.

    The rule is "older than everything DuckDB still holds", and it is sound
    only because the sweep is the sole statement that deletes from these
    tables and it always deletes the oldest — so nothing else can produce a
    row below that floor. It deliberately does not know the retention period:
    a copy of "14 days" here is a second place for it to be wrong, and it
    would go stale silently the day somebody changed the sweep.

    A genuine loss of the oldest rows is, by construction, indistinguishable
    from retention — retention *is* deleting the oldest rows. That is why the
    caller reports the count rather than dropping it.
    """
    if not spec.prunes_by_age or not orphans or spec.synced_column is None:
        return set()
    stamps = [_as_utc(v) for v in dk_synced.values()]
    stamps = [v for v in stamps if v is not None]
    if not stamps:
        # DuckDB holds no dated row at all. There is no floor to be below, and
        # an empty source is a much larger finding than a sweep — leave every
        # orphan standing so it is reported as one.
        return set()
    floor = min(stamps)
    try:
        clock = spec.columns.index(spec.synced_column)
    except ValueError:
        # The clock is read alongside but not shipped, so the orphan's own age
        # is not available here. Nothing can be established; report as orphans.
        return set()
    pruned = set()
    for row_id in orphans:
        value = _as_utc(pg_rows[row_id][clock])
        if value is not None and value < floor:
            pruned.add(row_id)
    return pruned


def _as_utc(value: Any) -> Optional[datetime]:
    """A naive timestamp here is UTC — `compare_table` has always read it so."""
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


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

    grace = timedelta(minutes=int(grace_minutes))
    issues, last_ok_at = _watermark_findings(
        table, watermark, len(dk_rows),
        one_failure_warns_within=grace if spec.one_failure_warns else None,
        now=now,
    )
    if last_ok_at is None:
        return issues
    cutoff = now - grace

    # ── DuckDB has it, Postgres does not ──
    lost: List[int] = []
    retired: List[int] = []
    for row_id in dk_rows.keys() - pg_rows.keys():
        synced = dk_synced.get(row_id)
        if synced is not None and synced.tzinfo is None:
            synced = synced.replace(tzinfo=timezone.utc)
        if spec.full_replace:
            # No retired category exists here: the writer replaces the whole
            # table on every run, so it wrote every row it holds. Anything
            # absent past the grace window was lost, not retired.
            if synced is None or synced <= cutoff:
                lost.append(row_id)
            continue
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
            sample_ids=_sample(spec, lost, max_samples),
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
            sample_ids=_sample(spec, retired, max_samples),
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

    # One of those is not a defect on a table that sweeps by age: a row that
    # left DuckDB after the copy, because it aged out. Separated rather than
    # forgiven — the count still reaches the journal, so a sweep that took two
    # rows and a sweep that took two hundred do not look alike. See
    # `MirroredTable.prunes_by_age`.
    pruned = _pruned_by_age(spec, dk_synced, pg_rows, orphans)
    if pruned:
        orphans = [row_id for row_id in orphans if row_id not in pruned]
        issues.append(IntegrityIssue(
            check_name="mirror_pruned_rows",
            table_name=table,
            severity=Severity.INFO,
            count=len(pruned),
            sample_ids=_sample(spec, sorted(pruned), max_samples),
            description=(
                f"{len(pruned)} row(s) in {table} are older than anything "
                "DuckDB still holds, on a table DuckDB sweeps by age — so they "
                "aged out between the copy and this check. The next full "
                "replace removes them. Not a defect; a number worth watching, "
                "because a sweep that suddenly takes far more than usual is "
                "the one thing this cannot tell apart from a loss."
            ),
        ))

    if orphans:
        issues.append(IntegrityIssue(
            check_name="mirror_orphan_rows",
            table_name=table,
            severity=Severity.WARN,
            count=len(orphans),
            sample_ids=_sample(spec, orphans, max_samples),
            description=(
                f"{len(orphans)} row(s) in {table} have nothing behind them in "
                "DuckDB. Nothing writes this table except the mirror, and the "
                "mirror only ever writes what DuckDB was given — so either "
                "DuckDB lost a row it once had, or something wrote to Postgres "
                "that is not the mirror."
            ),
        ))

    # ── both hold it, and disagree ──
    # A row written between the copy and the check legitimately differs, and
    # this branch did not ask until 2026-09-11. `compare_bucket` — the
    # other half of this module — has always asked, with the same grace and for
    # the same reason; the two paths simply disagreed, and the whole-table path
    # is the one every small table uses. The cost was a CRITICAL every morning
    # naming three rows of `reserve` that had moved eight minutes earlier, nine
    # firings over ten days with the sample ids rotating each time.
    #
    # Gated on `stamp_is_per_row`, because on a whole-table stamp this would
    # forgive everything at once — see the property.
    def _differing_in_flight(row_id: Any) -> bool:
        if not spec.stamp_is_per_row:
            return False
        synced = dk_synced.get(row_id)
        if synced is None:
            return False
        if synced.tzinfo is None:
            synced = synced.replace(tzinfo=timezone.utc)
        return synced > cutoff

    offenders: Dict[str, int] = {}
    differing: List[int] = []
    for row_id in dk_rows.keys() & pg_rows.keys():
        dk_row, pg_row = dk_rows[row_id], pg_rows[row_id]
        if dk_row == pg_row or _differing_in_flight(row_id):
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
            sample_ids=_sample(spec, differing, max_samples),
            description=(
                f"{len(differing)} row(s) are present in both stores and "
                f"disagree. Columns: {worst}. {spec.origin_note}"
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

    # A write chain that owns one of these tables stands its unit down here,
    # as it stands down the shipper that feeds them (DN-22b): the catalogue
    # mirror or the classification copy stops shipping DuckDB's rows, so every
    # row the chain writes would read as lost from DuckDB — findings the check
    # itself created. The local answer before Postgres is asked anything; the
    # owner rows once the pool is in hand, after `require_revision()`, where a
    # stand-down seen on them alone is a CRITICAL, not a decision.
    from core import chain_latch

    issues, held = _landing_stood_down([s.pg_table for s in specs])
    specs = tuple(s for s in specs if s.pg_table not in held)
    if not specs:
        return issues

    pool = await get_pool()
    await require_revision()
    found, held = _landing_stood_down(
        [s.pg_table for s in specs], await chain_latch.read_owners(pool))
    issues += found
    specs = tuple(s for s in specs if s.pg_table not in held)
    if not specs:
        return issues
    watermarks = await fetch_watermarks(pool)

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
_BOOL, _DATE = "bool", "date"


@dataclass(frozen=True)
class BucketedTable:
    """A table too large to compare row by row every morning."""
    pg_table: str
    dk_table: str
    columns: Tuple[str, ...]
    fields: Tuple[Tuple[str, str], ...]     # (column, kind), for the fingerprint
    # How a row is assigned to a bucket, once per dialect. An expression and
    # not a column name, because the right bucket is not always `id // 1000`:
    # `inventory_sku_history` is keyed (date, offer_id) with only ~900 offers,
    # so dividing an id would put the whole table in one bucket and the
    # drill-down would read 143,274 rows to find one. Its bucket is the day.
    dk_bucket: str
    pg_bucket: str
    numeric: Tuple[str, ...]
    dk_rows_sql: str                        # one bucket, plus DuckDB's synced_at
    pg_rows_sql: str
    # `MirroredTable.one_failure_warns`, for the appended tables the same
    # hourly copy ships.
    one_failure_warns: bool = False


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
        dk_bucket="id // {}".format(BUCKET_SIZE),
        pg_bucket="id / {}".format(BUCKET_SIZE),
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
        dk_bucket="order_id // {}".format(BUCKET_SIZE),
        pg_bucket="order_id / {}".format(BUCKET_SIZE),
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


# ─── Silver: computed on both sides, compared the same way ───────────────────
#
# Not landing. `silver.orders` is the output of `silver_select_sql(POSTGRES)`
# over `bronze.orders`, and DuckDB's `silver_orders` is the same projection
# over its own copy. So this compares two *computations* of one rule, which is
# the thing the parallel period exists to prove.
#
# `full_replace` in spirit: `rebuild_silver` writes every row it holds, so a
# row present in DuckDB and absent here was lost, never "retired".
#
# THE GRACE WINDOW IS WIDER, AND HAS TO BE
#
# DuckDB rebuilds Silver whenever the warehouse goes dirty. Postgres rebuilds
# on a floor — `KS_PG_SILVER_INTERVAL_S`, ten minutes by default — because a
# full 46,000-row rebuild costs the same whatever moved. So Postgres is
# *legitimately* behind by up to that floor, and a fifteen-minute grace would
# report every order touched in the last ten minutes as a defect.
#
# Twenty minutes: the floor plus half of it again. Raise both together or this
# starts reporting the schedule.
SILVER_GRACE_MINUTES = 20

_SILVER_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("id", _INT), ("source_id", _INT), ("status_id", _INT),
    ("grand_total", _NUMERIC), ("ordered_at", _TS),
    ("buyer_id", _INT), ("manager_id", _INT), ("order_date", _DATE),
    ("is_return", _BOOL), ("sales_type", _TEXT),
    ("is_active_source", _BOOL), ("source_name", _TEXT),
    ("is_new_customer", _BOOL), ("buyer_first_order_date", _DATE),
    ("promocode", _TEXT),
)
_SILVER_COLS = tuple(c for c, _ in _SILVER_FIELDS)

SILVER_TABLES: Tuple[BucketedTable, ...] = (
    BucketedTable(
        pg_table="silver.orders",
        dk_table="silver_orders",
        columns=_SILVER_COLS,
        fields=_SILVER_FIELDS,
        dk_bucket="id // {}".format(BUCKET_SIZE),
        pg_bucket="id / {}".format(BUCKET_SIZE),
        numeric=("grand_total",),
        # Silver carries no timestamp of its own on either side. The grace
        # window therefore reads the *order's* — a Silver row is only ever as
        # fresh as the landing row it was projected from, and that is what
        # decides whether Postgres has had a chance to catch up.
        dk_rows_sql=(
            "SELECT " + ", ".join(f"s.{c}" for c in _SILVER_COLS)
            + ", o.synced_at FROM silver_orders s "
            "LEFT JOIN orders o ON o.id = s.id "
            f"WHERE (s.id // {BUCKET_SIZE}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_SILVER_COLS)} FROM silver.orders "
            f"WHERE (id / {BUCKET_SIZE}) = $1"
        ),
    ),
)


def _dk_term(column: str, kind: str) -> str:
    if kind == _BOOL:
        # Counted, not summed as text: DuckDB and Postgres render booleans
        # differently and `SUM(LENGTH(...))` would compare 'true' against 't'.
        return f"SUM(CASE WHEN {column} THEN 1 ELSE 0 END)"
    if kind == _DATE:
        return f"SUM(COALESCE(epoch({column})::BIGINT, 0))"
    if kind == _TS:
        # Microseconds as an integer. Not `extract(epoch ...)`, which is a
        # float on one side and a numeric on the other and would compare two
        # roundings rather than two values.
        return f"SUM(COALESCE(epoch_us({column}), 0))"
    if kind == _TEXT:
        return f"SUM(LENGTH(COALESCE({column}, '')))"
    return f"SUM(COALESCE({column}, 0))"


def _pg_term(column: str, kind: str) -> str:
    if kind == _BOOL:
        return f"SUM(CASE WHEN {column} THEN 1 ELSE 0 END)"
    if kind == _DATE:
        return f"SUM(COALESCE(EXTRACT(EPOCH FROM {column})::bigint, 0))"
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
        f"SELECT ({spec.dk_bucket})::BIGINT AS bucket, "
        f"COUNT(*), {terms} FROM {spec.dk_table} GROUP BY 1 ORDER BY 1"
    )


def postgres_fingerprint_sql(spec: BucketedTable) -> str:
    terms = ", ".join(_pg_term(c, k) for c, k in spec.fields)
    return (
        f"SELECT ({spec.pg_bucket})::bigint AS bucket, "
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


def _orders_stood_down(
    specs: Sequence[BucketedTable], moved: FrozenSet[str],
    *, owner_rows_only: bool = False,
) -> Tuple[List[IntegrityIssue], Tuple[BucketedTable, ...]]:
    """The findings for the order tables a write chain holds, and the specs
    left to compare. Nothing and every spec when `moved` is empty.

    One function for both of `reconcile_orders`' questions — the local answer
    and the owner rows — because both stand the same two tables down. What
    they file differs, because what is true differs.

    **On the local answer, `mirror_stood_down` (INFO).** The marker or the
    flag routes the order writes to the chain, and the sync's mirror asks
    that same answer, so it has stopped too: a decision somebody took, with
    nothing left writing DuckDB's copy over the chain's rows.

    **On the owner rows alone (`owner_rows_only`),
    `order_owner_row_without_marker` (CRITICAL).** The per-tick mirror in
    `upsert_orders` asks only the local answer, which is empty, so it is
    still shipping DuckDB's copy over the chain's rows and archiving every
    overwrite in `app.order_versions` — damage done once a minute, not a
    decision. It was the same INFO once, and every surface a human reads
    prints the check's name, its label and its lever, never the
    description: all three said "not a defect" about the one state here that
    is. One finding, not one per table: it is one fact, and the page would
    otherwise print the same line twice."""
    from core import pg_landing

    specs = tuple(specs)
    if not moved:
        return [], specs
    order_tables = {pg_landing.ORDERS_TABLE, pg_landing.ORDER_PRODUCTS_TABLE}
    not_compared = [s.pg_table for s in specs if s.pg_table in order_tables]
    left = tuple(s for s in specs if s.pg_table not in order_tables)
    if not not_compared:
        return [], left
    tables = ", ".join(sorted(moved))
    if owner_rows_only:
        issues = [IntegrityIssue(
            check_name="order_owner_row_without_marker",
            table_name=", ".join(sorted(not_compared)),
            severity=Severity.CRITICAL,
            count=len(not_compared),
            description=_owner_rows_only_description(tables, order_tables),
        )]
        return issues, left
    description = (
        f"Not compared: {tables} is written by a write chain, so the "
        "orders mirror no longer ships DuckDB's copy of either order "
        "table — ownership of the order tables passes as a unit. A "
        "difference here would be the chain's own writes, not a loss."
    )
    issues = [
        IntegrityIssue(
            check_name="mirror_stood_down",
            table_name=table,
            severity=Severity.INFO,
            count=1,
            description=description,
        )
        for table in not_compared
    ]
    return issues, left


def _owner_rows_only_description(tables: str, order_tables: set) -> str:
    """What `order_owner_row_without_marker` says: the ownership that only the
    owner rows in Postgres could see.

    Two roads lead here and they take different levers. A chain this build
    declares, flagged `duckdb`, with no marker: the marker was lost, and
    putting it back (or releasing the chain properly) is the way out. No
    chain in this build declaring an order table at all: an image older than
    the chain, where a restored marker names a chain nothing here reads."""
    from core.write_chains import WRITE_CHAINS

    if any(order_tables & set(c.CHAIN_TABLES) for c in WRITE_CHAINS):
        why = (
            "the marker is missing; the sync mirror is still shipping — "
            "restore data/write-chain-owners or run scripts/chain_copy_back.py"
        )
    else:
        why = (
            "no chain in this build declares the order tables (an image older "
            "than the chain?); the sync mirror is still shipping — redeploy a "
            "build that declares the chain, or run scripts/chain_copy_back.py "
            "from one"
        )
    return (
        f"An owner row in Postgres says a write chain owns {tables}, but this "
        f"process's own answer says DuckDB still writes them: {why}. Until "
        "then every sync tick writes DuckDB's copy over the chain's rows and "
        "archives each overwrite in app.order_versions. Not compared: the "
        "ids-diff, the comment ship and this comparison stand down on the "
        "owner row, and only the sync's mirror does not."
    )


def _landing_stood_down(
    tables: Sequence[str], owners: Optional[Mapping[str, str]] = None,
) -> Tuple[List[IntegrityIssue], FrozenSet[str]]:
    """The findings for the tables among `tables` a write chain holds, and the
    tables not to compare (DN-22b). Nothing and nothing when none is held.

    `_orders_stood_down`'s two answers for every other landing and replicated
    table — the catalogue, the order-level expenses, the buyers, the manager
    classification — asked per unit (`pg_landing.unit_of`), because the
    shipper stands a unit down whole: whoever takes the buyers takes their
    contacts, and a comparison that went on comparing the contacts would file
    every change the chain makes to them as a loss.

    **On the local answer, `mirror_stood_down` (INFO), one per table.** The
    shipper asks the same answer and has stopped: a decision, with nothing
    left writing DuckDB's copy over the chain's rows.

    **With `owners` given and the owner rows alone naming a unit, it depends
    on the unit's sync shipper** (`pg_landing.sync_reads_the_owner_rows`):

    - one that reads the owner rows too — the buyers mirror, the
      classification copy — has stopped: `mirror_stood_down` (INFO) per
      table, saying so. The latch is still wrong, but that is one fact about
      a chain and it has its own page: `chain_latch_disagrees` for a chain
      this build declares, `chain_owner_unregistered` for one it does not,
      both filed by `reconcile_operational` on the same layer;
    - one that asks the local answer alone — the catalogue and expense
      mirrors — writes DuckDB's copy over the chain's rows each time it runs:
      `owner_row_without_marker` (CRITICAL), one for all of them — damage,
      not a decision, and a page and the digest print the label and lever,
      never this text.
    """
    from core import chain_latch, pg_landing
    from core.write_chains import WRITE_CHAINS

    wanted = set(tables)
    units = []
    for table in tables:
        unit = pg_landing.unit_of(table)
        if unit not in units:
            units.append(unit)

    issues: List[IntegrityIssue] = []
    held: set = set()
    owned_only: List[str] = []
    for unit in units:
        if owners is not None and not pg_landing.tables_stood_down(unit) \
                and pg_landing.owned_among(owners, unit) \
                and pg_landing.sync_reads_the_owner_rows(unit):
            held |= set(unit)
            owned = ", ".join(sorted(set(unit) & chain_latch.owned_tables(owners)))
            for table in unit:
                if table not in wanted:
                    continue
                issues.append(IntegrityIssue(
                    check_name="mirror_stood_down",
                    table_name=table,
                    severity=Severity.INFO,
                    count=1,
                    description=(
                        f"Not compared: an owner row in Postgres says a write "
                        f"chain owns {owned}, and the sync no longer ships "
                        f"DuckDB's copy of {table} — its shipper reads the "
                        "owner rows too, and ownership passes for "
                        f"{', '.join(unit)} as a unit. This process's own "
                        "answer still says DuckDB writes it; why is "
                        "chain_latch_disagrees' or chain_owner_unregistered's "
                        "finding, not this one's."
                    ),
                ))
            continue
        local = pg_landing.tables_stood_down(unit)
        if local:
            held |= set(unit)
            declared = ", ".join(sorted(local))
            for table in unit:
                if table not in wanted:
                    continue
                issues.append(IntegrityIssue(
                    check_name="mirror_stood_down",
                    table_name=table,
                    severity=Severity.INFO,
                    count=1,
                    description=(
                        f"Not compared: {declared} is written by a write "
                        f"chain, so the sync no longer ships DuckDB's copy of "
                        f"{table} — ownership passes for "
                        f"{', '.join(unit)} as a unit. A difference here "
                        "would be the chain's own writes, not a loss."
                    ),
                ))
        elif owners is not None and pg_landing.owned_among(owners, unit):
            held |= set(unit)
            owned_only += [t for t in unit if t in wanted]

    if owned_only:
        named = ", ".join(sorted(owned_only))
        if any(set(owned_only) & set(c.CHAIN_TABLES) for c in WRITE_CHAINS):
            why = ("the marker is missing; the sync is still shipping — restore "
                   "data/write-chain-owners or run scripts/chain_copy_back.py")
        else:
            why = ("no chain in this build declares them (an image older than "
                   "the chain?); the sync is still shipping — redeploy a build "
                   "that declares the chain, or run scripts/chain_copy_back.py "
                   "from one")
        issues.append(IntegrityIssue(
            check_name="owner_row_without_marker",
            table_name=named,
            severity=Severity.CRITICAL,
            count=len(owned_only),
            description=(
                f"An owner row in Postgres says a write chain owns {named}, but "
                f"this process's own answer says DuckDB still writes them: "
                f"{why}. Until then each sync that ships these tables writes "
                "DuckDB's copy over the chain's rows. Not compared: the "
                "backfills, the hourly diffs, the buyers and classification "
                "copies and this comparison stand down on the owner row; the "
                "per-tick mirror of these tables does not."
            ),
        ))
    return issues, frozenset(held)


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

    # A write chain that owns either order table stands both down here, as it
    # stands down the mirror that feeds them (DN-22a): the mirror stops
    # shipping DuckDB's copy, so every order the chain writes would read as
    # lost from DuckDB and every one DuckDB receives as missing from Postgres
    # — findings the check itself created, `reconcile_operational`'s reason.
    # On this, the local answer, it is said at INFO rather than left as
    # silence, because unlike that function's tables these have no standing
    # watch of their own yet, and a comparison that stopped without a word
    # reads the same as one that passed. Asked before any read, so a
    # stood-down run costs neither store anything.
    issues, specs = _orders_stood_down(
        specs, pg_landing.order_tables_stood_down())
    if not specs:
        return issues

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()

    # And the owner rows, now that the pool is in hand — `reconcile_operational`'s
    # union, for its reason: a lost marker leaves the chain's rows in Postgres
    # and the comparison would file each one as lost from DuckDB. Before the
    # watermarks and the fingerprints, so this run too reads no order table.
    # Here it is normally the owner rows alone that stand a table down — had
    # the local answer named one, the first call would already have taken both
    # order tables out of `specs` — and then it is not a stand-down at all but
    # `order_owner_row_without_marker`, CRITICAL: the sync's mirror asks the
    # local answer alone, so it is still shipping over the chain's rows. Asked
    # again rather than assumed, so a chain that latched between the two
    # questions is not paged as a lost marker.
    moved = await pg_landing.order_tables_stood_down_or_owned(pool)
    found, specs = _orders_stood_down(
        specs, moved, owner_rows_only=not pg_landing.order_tables_stood_down())
    issues += found
    if not specs:
        return issues
    watermarks = await fetch_watermarks(pool)

    # Phase 1, DuckDB side: one acquisition for every table.
    async with store.connection() as conn:
        dk_prints = {s.pg_table: fingerprints(conn, s) for s in specs}

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


async def reconcile_silver(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = SILVER_GRACE_MINUTES,
    max_samples: int = 10,
    max_buckets: int = 20,
) -> List[IntegrityIssue]:
    """Compare the two computations of Silver, fingerprint then drill-down.

    The same shape as `reconcile_orders`, with one gate swapped. Orders are
    gated on `backfilled_at`, because a delta mirror cannot be judged before
    history has been carried across. Silver has no backfill: `rebuild_silver`
    writes the table whole every time, so the only question is whether it has
    ever run — which `last_ok_at` answers, and `_watermark_findings` already
    reports as `mirror_never_shipped`.

    Reports only. A finding here cannot start a rebuild, on the same grounds as
    every other check in this module: a comparison that repairs what it finds
    destroys the evidence that it found anything.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    async with store.connection() as conn:
        dk_prints = {s.pg_table: fingerprints(conn, s) for s in SILVER_TABLES}

    issues: List[IntegrityIssue] = []
    suspects: Dict[str, List[int]] = {}
    for spec in SILVER_TABLES:
        dk_print = dk_prints[spec.pg_table]
        dk_count = sum(int(v[0]) for v in dk_print.values())
        found, last_ok_at = _watermark_findings(
            spec.pg_table, watermarks.get(spec.pg_table), dk_count,
        )
        issues += found
        if last_ok_at is None:
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
                    f"{len(differing)} id buckets disagree between the two "
                    "computations of Silver — more than this check will open. "
                    "A whole-table problem: the projection, the classification "
                    "the two sides read, or a rebuild that did not finish."
                ),
            ))
            differing = differing[:max_buckets]
        suspects[spec.pg_table] = differing

    if not any(suspects.values()):
        return issues

    dk_buckets: Dict[str, Dict[int, Any]] = {}
    async with store.connection() as conn:
        for spec in SILVER_TABLES:
            dk_buckets[spec.pg_table] = {
                bucket: _read_dk_bucket(conn, spec, bucket)
                for bucket in suspects.get(spec.pg_table, ())
            }

    for spec in SILVER_TABLES:
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


# ─── Gold: one aggregate, two shapes, compared through the mapping ───────────
#
# Silver was the same projection on both sides, so it could be compared column
# for column. Gold cannot: DuckDB splits the channels into columns and Postgres
# carries `source_id` as a dimension, because a column can only name a channel
# somebody wrote a column for — see revision 0007 for the ₴266,059.00 that
# proves it.
#
# So the comparison goes through a mapping instead, and the mapping is total:
# every one of DuckDB's fourteen measures has an exact counterpart here, and
# nothing is compared by folding one grain into the other.
#
#     revenue … avg_order_value   ↔  the Postgres roll-up row (source_id NULL)
#     instagram_revenue           ↔  the fine row for source 1, column revenue
#     telegram_orders             ↔  the fine row for source 2, orders_count
#     …and so on for 2 and 4
#
# THE ROLL-UP IS COMPARED, NOT RECONSTRUCTED
#
# `unique_customers`, `new_customers` and `returning_customers` are
# `COUNT(DISTINCT buyer_id)`, and adding the per-source rows up overstates them
# in 29, 12 and 17 of 2,107 production cells — one buyer who used two channels
# in a day. That is why `gold.daily_revenue` stores the roll-up rather than
# leaving it to be derived, and why this reads it rather than summing.
#
# WHAT IS NOT COMPARED, AND WHY THAT IS NOT A HOLE
#
# The fine rows for sources 3 and 5 have no DuckDB counterpart — that absence
# is the entire reason the grain changed. Their contribution is still checked,
# twice over: they are inside DuckDB's `revenue`, which the roll-up comparison
# covers exactly, and `_gold_internal_findings` asserts the fine rows add up to
# the roll-up for the four measures that are additive.

GOLD_PG_TABLE = "gold.daily_revenue"
GOLD_DK_TABLE = "gold_daily_revenue"

# Gold is rebuilt in the same call as Silver, on the same floor, so it is
# behind by the same amount and forgiven for the same window.
GOLD_GRACE_MINUTES = SILVER_GRACE_MINUTES

_GOLD_ROLLUP_MEASURES: Tuple[str, ...] = (
    "revenue", "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count", "returns_revenue",
    "avg_order_value",
)

# DuckDB column → (source_id, the Postgres column holding the same number).
#
# Sound only while 1, 2 and 4 are all in REVENUE_SOURCE_IDS: DuckDB's channel
# columns filter `source_id = n` where Postgres filters `is_active_source`, and
# the two agree exactly because those sources are active. A test pins that.
_GOLD_SOURCE_MEASURES: Dict[str, Tuple[int, str]] = {
    "instagram_revenue": (1, "revenue"),
    "telegram_revenue": (2, "revenue"),
    "shopify_revenue": (4, "revenue"),
    "instagram_orders": (1, "orders_count"),
    "telegram_orders": (2, "orders_count"),
    "shopify_orders": (4, "orders_count"),
}

# Zero everywhere except the one column that is a division. DuckDB's DECIMAL
# `/` promotes to DOUBLE and rounds the result; PostgreSQL divides exactly and
# rounds once. The warehouse's own Gold check has allowed exactly this cent
# since it was written — `MATERIAL_THRESHOLDS` in core/data_quality.py.
_GOLD_TOLERANCES: Dict[str, Decimal] = {"avg_order_value": Decimal("0.01")}

# The four measures that survive being added up across sources. The other four
# are distinct counts and a division, and summing any of them is the mistake
# this table's shape exists to avoid.
_GOLD_ADDITIVE: Tuple[str, ...] = (
    "revenue", "orders_count", "returns_count", "returns_revenue",
)


def _gold_cell(cell: Tuple[Any, str]) -> str:
    return f"{cell[0]} {cell[1]}"


def _gold_cells(cells: Sequence[Tuple[Any, str]], limit: int) -> str:
    shown = ", ".join(_gold_cell(c) for c in sorted(cells)[:limit])
    return shown + ("…" if len(cells) > limit else "")


def fetch_duckdb_gold(conn) -> Tuple[
    Dict[Tuple[Any, str], Dict[str, Optional[Decimal]]],
    Dict[Tuple[Any, str], Optional[datetime]],
]:
    """DuckDB's Gold, and how fresh each cell's newest order is.

    2,107 rows of 16 columns — small enough to read whole, which is why this
    needs none of the bucketed fingerprint machinery that `orders` and Silver
    do.

    The freshness comes from the orders behind the cell, because a Gold cell
    carries no timestamp of its own on either side. A cell holding an order
    synced two minutes ago is legitimately different between the stores until
    Postgres rebuilds, exactly as a Silver row is.
    """
    columns = list(_GOLD_ROLLUP_MEASURES) + list(_GOLD_SOURCE_MEASURES)
    rows = conn.execute(
        f"SELECT date, sales_type, {', '.join(columns)} FROM {GOLD_DK_TABLE}"
    ).fetchall()
    values = {
        (r[0], r[1]): {c: _as_decimal(v) for c, v in zip(columns, r[2:])}
        for r in rows
    }

    fresh = conn.execute(
        "SELECT s.order_date, s.sales_type, MAX(o.synced_at) "
        "FROM silver_orders s LEFT JOIN orders o ON o.id = s.id "
        "GROUP BY s.order_date, s.sales_type"
    ).fetchall()
    return values, {(r[0], r[1]): r[2] for r in fresh}


async def fetch_pg_gold(pool) -> Tuple[
    Dict[Tuple[Any, str], Dict[str, Optional[Decimal]]],
    Dict[Tuple[Any, str, int], Dict[str, Optional[Decimal]]],
]:
    """The Postgres roll-up rows and fine rows, keyed apart.

    Keyed apart rather than filtered later because they answer different
    questions and a caller that confused them would compare a channel against
    a day.
    """
    columns = list(_GOLD_ROLLUP_MEASURES)
    rows = await pool.fetch(
        f"SELECT date, sales_type, source_id, {', '.join(columns)} "
        f"FROM {GOLD_PG_TABLE}"
    )
    rollup: Dict[Tuple[Any, str], Dict[str, Optional[Decimal]]] = {}
    fine: Dict[Tuple[Any, str, int], Dict[str, Optional[Decimal]]] = {}
    for r in rows:
        measures = {c: _as_decimal(r[c]) for c in columns}
        if r["source_id"] is None:
            rollup[(r["date"], r["sales_type"])] = measures
        else:
            fine[(r["date"], r["sales_type"], int(r["source_id"]))] = measures
    return rollup, fine


_GOLD_ZERO: Dict[str, Decimal] = {c: Decimal(0) for c in _GOLD_ROLLUP_MEASURES}


def _gold_expected(
    cell: Tuple[Any, str],
    rollup: Mapping[Tuple[Any, str], Mapping[str, Optional[Decimal]]],
    fine: Mapping[Tuple[Any, str, int], Mapping[str, Optional[Decimal]]],
) -> Dict[str, Optional[Decimal]]:
    """What Postgres says DuckDB's fourteen measures should be, for one cell.

    An absent fine row is zero, not a missing row: Postgres writes a channel
    row only for channels that sold something that day, where DuckDB writes a
    zero into the column regardless. Reporting that as a missing row would
    report the shape rather than the data.
    """
    expected = dict(rollup[cell])
    for dk_column, (source_id, pg_column) in _GOLD_SOURCE_MEASURES.items():
        row = fine.get((cell[0], cell[1], source_id), _GOLD_ZERO)
        expected[dk_column] = row[pg_column]
    return expected


def compare_gold(
    dk_rows: Mapping[Tuple[Any, str], Mapping[str, Optional[Decimal]]],
    dk_fresh: Mapping[Tuple[Any, str], Optional[datetime]],
    pg_rollup: Mapping[Tuple[Any, str], Mapping[str, Optional[Decimal]]],
    pg_fine: Mapping[Tuple[Any, str, int], Mapping[str, Optional[Decimal]]],
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = GOLD_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Both sides, already read. No I/O, so it is testable whole."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=int(grace_minutes))
    issues: List[IntegrityIssue] = []

    def in_flight(cell: Tuple[Any, str]) -> bool:
        synced = dk_fresh.get(cell)
        if synced is None:
            return False
        if synced.tzinfo is None:
            synced = synced.replace(tzinfo=timezone.utc)
        return synced > cutoff

    # ── the cell sets ──
    #
    # This is the August incident's shape one layer up: 100 → 90 → 84 missing
    # cells with zero value mismatches, invisible to every scalar. The
    # warehouse's own cell guard catches it between Silver and Gold in one
    # store; this catches it between the stores.
    missing = [c for c in dk_rows.keys() - pg_rollup.keys() if not in_flight(c)]
    if missing:
        issues.append(IntegrityIssue(
            check_name="gold_missing_cells",
            table_name=GOLD_PG_TABLE,
            severity=Severity.CRITICAL,
            count=len(missing),
            description=(
                f"{len(missing)} (date, sales_type) cell(s) exist in DuckDB's "
                f"Gold and have no roll-up row in {GOLD_PG_TABLE}: "
                f"{_gold_cells(missing, max_samples)}. Gold is rebuilt whole on "
                "both sides, so a missing cell was not aggregated — there is no "
                "'retired' to mean here."
            ),
        ))

    orphans = [c for c in pg_rollup.keys() - dk_rows.keys() if not in_flight(c)]
    if orphans:
        issues.append(IntegrityIssue(
            check_name="gold_orphan_cells",
            table_name=GOLD_PG_TABLE,
            severity=Severity.CRITICAL,
            count=len(orphans),
            description=(
                f"{len(orphans)} roll-up row(s) in {GOLD_PG_TABLE} have no cell "
                f"behind them in DuckDB's Gold: {_gold_cells(orphans, max_samples)}. "
                "Either DuckDB's Gold lost a cell it should hold, or the two "
                "stores disagree about which orders exist at all — check the "
                "Silver comparison in the same run before this one."
            ),
        ))

    # ── the values ──
    offenders: Dict[str, int] = {}
    differing: List[Tuple[Any, str]] = []
    worst_example = None
    for cell in sorted(dk_rows.keys() & pg_rollup.keys()):
        if in_flight(cell):
            continue
        expected = _gold_expected(cell, pg_rollup, pg_fine)
        cell_differs = False
        for column, dk_value in dk_rows[cell].items():
            pg_value = expected.get(column)
            if dk_value is None or pg_value is None:
                if dk_value is pg_value:
                    continue
            else:
                gap = abs(dk_value - pg_value)
                if gap <= _GOLD_TOLERANCES.get(column, Decimal(0)):
                    continue
            cell_differs = True
            offenders[column] = offenders.get(column, 0) + 1
            if worst_example is None:
                worst_example = (cell, column, dk_value, pg_value)
        if cell_differs:
            differing.append(cell)

    if differing:
        worst = ", ".join(
            f"{c} ({n})"
            for c, n in sorted(offenders.items(), key=lambda kv: -kv[1])[:5]
        )
        cell, column, dk_value, pg_value = worst_example
        issues.append(IntegrityIssue(
            check_name="gold_cell_values",
            table_name=GOLD_PG_TABLE,
            severity=Severity.CRITICAL,
            count=len(differing),
            description=(
                f"{len(differing)} cell(s) are aggregated by both stores and "
                f"disagree. Columns: {worst}. First: {_gold_cell(cell)} "
                f"{column} DuckDB={dk_value} Postgres={pg_value}. Cells: "
                f"{_gold_cells(differing, max_samples)}. Both sides run the "
                "same measures from GOLD_MEASURES over a Silver the same run "
                "already proved equal, so a difference here is the aggregation "
                "or the shape mapping, not the data underneath."
            ),
        ))

    issues += _gold_internal_findings(pg_rollup, pg_fine, max_samples=max_samples)
    return issues


def _gold_internal_findings(
    rollup: Mapping[Tuple[Any, str], Mapping[str, Optional[Decimal]]],
    fine: Mapping[Tuple[Any, str, int], Mapping[str, Optional[Decimal]]],
    *,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Do the Postgres fine rows add up to the Postgres roll-up?

    The only check that sees the fine rows for sources 3 and 5 at all, because
    those have no DuckDB column to be compared against — which is the whole
    reason `source_id` became a dimension. It covers the four additive
    measures; the three distinct counts and the division are excluded because
    summing them is exactly the error the roll-up rows exist to prevent.

    Postgres-only, and deliberately so: it needs no DuckDB, so it keeps working
    unchanged after the parallel period ends and the other half of this module
    is deleted.
    """
    sums: Dict[Tuple[Any, str], Dict[str, Decimal]] = {}
    for (date, sales_type, _source), row in fine.items():
        acc = sums.setdefault((date, sales_type), {c: Decimal(0) for c in _GOLD_ADDITIVE})
        for column in _GOLD_ADDITIVE:
            acc[column] += row[column] or Decimal(0)

    offenders: Dict[str, int] = {}
    bad: List[Tuple[Any, str]] = []
    for cell, totals in sums.items():
        if cell not in rollup:
            continue
        hit = False
        for column in _GOLD_ADDITIVE:
            if (rollup[cell][column] or Decimal(0)) != totals[column]:
                offenders[column] = offenders.get(column, 0) + 1
                hit = True
        if hit:
            bad.append(cell)

    if not bad:
        return []
    worst = ", ".join(
        f"{c} ({n})" for c, n in sorted(offenders.items(), key=lambda kv: -kv[1])
    )
    return [IntegrityIssue(
        check_name="gold_rollup_mismatch",
        table_name=GOLD_PG_TABLE,
        severity=Severity.CRITICAL,
        count=len(bad),
        description=(
            f"{len(bad)} cell(s) whose per-source rows do not add up to their "
            f"own roll-up row. Columns: {worst}. Cells: "
            f"{_gold_cells(bad, max_samples)}. Both grains come from one "
            "GROUPING SETS over one snapshot, so these cannot disagree unless "
            "the rebuild wrote them from different reads — a partial write, or "
            "something outside core/pg_gold.py writing this table."
        ),
    )]


async def reconcile_gold(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = GOLD_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare the two aggregations of Gold.

    Gated like Silver and for the same reason: `rebuild_gold` writes the table
    whole every time, so the only question the watermark has to answer is
    whether it has ever run — `_watermark_findings` reports that as
    `mirror_never_shipped` and suppresses everything below it.

    Reports only. A finding here cannot start a rebuild, on this module's
    standing grounds: a comparison that repairs what it finds destroys the
    evidence that it found anything.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    # The DuckDB read and the Postgres round-trips stay in separate blocks: the
    # store's lock is not held across the network.
    async with store.connection() as conn:
        dk_rows, dk_fresh = fetch_duckdb_gold(conn)

    issues, last_ok_at = _watermark_findings(
        GOLD_PG_TABLE, watermarks.get(GOLD_PG_TABLE), len(dk_rows),
    )
    if last_ok_at is None:
        return issues

    pg_rollup, pg_fine = await fetch_pg_gold(pool)
    return issues + compare_gold(
        dk_rows, dk_fresh, pg_rollup, pg_fine,
        now=now, grace_minutes=grace_minutes, max_samples=max_samples,
    )


# ─── The operational history: five tables with no source to be rebuilt from ──
#
# `bronze.*` can be re-fetched from KeyCRM and Silver and Gold can be
# recomputed. These five cannot — see revision 0008 for what each one knows
# that KeyCRM does not. That is what makes the comparison worth its cost: a
# difference here is not a copy that will be repaired by the next sync, it is
# the only record of something drifting away from the only record of it.
#
# Three shapes, and each one is chosen by how DuckDB writes the table rather
# than by its size.

# Replicated hourly, so Postgres is legitimately behind by up to an hour. Same
# relationship SILVER_GRACE_MINUTES has to KS_PG_SILVER_INTERVAL_S — the
# interval plus half of it again — and the same warning: raise the job's
# interval and this must move with it, or the check starts reporting the
# schedule instead of the data.
OPERATIONAL_GRACE_MINUTES = 90

# ── the four replaced whole ──────────────────────────────────────────────────
#
# `full_replace=True` on all four, and it is not a formality. The replicator
# writes every row it holds, so "in DuckDB and not in Postgres" has exactly one
# meaning here: lost. There is no retired category to excuse it with, which is
# the whole reason revision 0005 added the flag. `offer_stocks` is the one
# place that reads oddly — landing data usually earns the retired category —
# but it is replaced whole like the rest, so the flag describes the shipping
# shape correctly.

# Imported, never restated. The replicator owns the column list because it is
# the thing that writes it; a second copy here would compare a set of columns
# that had quietly stopped being the set of columns being shipped, and would
# report clean while doing it.

OPERATIONAL_TABLES: Tuple[MirroredTable, ...] = (
    # Landing, not irreplaceable — and compared here rather than with the SMS
    # tables because `reconcile_sms` stands down on `KS_SMS_STORE=postgres`,
    # which would leave the one table on that list DuckDB still writes with no
    # comparison at all. See `core/pg_operational.py`.
    MirroredTable(
        pg_table="bronze.offer_stocks",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="offer_stocks",
        columns=OFFER_STOCK_COLUMNS,
        key_columns=("id",),
        synced_column="synced_at",
        numeric=("price", "purchased_price"),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.revenue_goals",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="revenue_goals",
        columns=GOAL_COLUMNS,
        key_columns=("period_type",),
        # The row's own value doubles as its clock, `order_backfill_misses`'
        # arrangement: a goal is written and then rewritten, never appended to.
        synced_column="updated_at",
        numeric=("goal_amount", "calculated_goal", "growth_factor"),
        full_replace=True,
        one_failure_warns=True,
    ),
    # Landing, replicated rather than mirrored — `bronze.offer_stocks`'
    # situation exactly. KeyCRM serves this dictionary only to the weekly full
    # sync, so a payload-fed mirror leaves Postgres empty for up to a week, and
    # `/expenses` renders its breakdown by *name*. `full_replace` because the
    # replicator rewrites it whole, which also makes a missing row unambiguous:
    # there is no "KeyCRM retired it" to mean.
    MirroredTable(
        pg_table="bronze.expense_types",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="expense_types",
        columns=tuple(EXPENSE_TYPE_COLUMNS),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.manual_expenses",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="manual_expenses",
        columns=MANUAL_EXPENSE_COLUMNS,
        key_columns=("id",),
        # `updated_at` is NULL until the expense is edited, so the clock falls
        # back to when it was created — `authorized_users`' arrangement, and
        # for its reason: a row that has never been touched still has to have
        # an age, or it is CRITICAL the hour it is typed.
        synced_column="COALESCE(updated_at, created_at)",
        numeric=("amount",),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.order_backfill_misses",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="order_backfill_misses",
        columns=MISS_COLUMNS,
        key_columns=("order_id",),
        # `checked_at` is refreshed every time an id is re-recorded, so it is
        # both the value being compared and the clock that says whether a
        # difference has had time to travel. That is fine — it is the same
        # column, read for two purposes, and the grace window only ever
        # forgives a row it also compared.
        synced_column="checked_at",
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.inventory_history",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="inventory_history",
        columns=INVENTORY_HISTORY_COLUMNS,
        key_columns=("date",),
        synced_column="recorded_at",
        numeric=("total_value",),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.sku_inventory_status",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="sku_inventory_status",
        columns=SKU_STATUS_COLUMNS,
        key_columns=("offer_id",),
        synced_column="updated_at",
        # Still shipped, still the grace clock, never compared: it is stamped
        # on all 891 rows by every rebuild, so comparing it asks the two copies
        # to have been taken at the same instant, which they never are.
        ignore_columns=("updated_at",),
        numeric=("price", "purchased_price"),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.buyer_gender",
        # Not landing's story: the shared tuple never existed here, because no
        # KeyCRM payload carries a gender. DuckDB decides it and Postgres
        # receives the decision, so a difference is the copy having drifted.
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="buyer_gender",
        columns=BUYER_GENDER_COLUMNS,
        key_columns=("buyer_id",),
        synced_column="decided_at",
        # Shipped and never compared, `sku_inventory_status.updated_at`'s
        # situation exactly: a full re-derivation stamps all 20 145 rows in one
        # pass, so comparing it would ask the two copies to have been taken at
        # the same instant. What matters is the verdict, and the verdict IS
        # compared.
        ignore_columns=("decided_at",),
        full_replace=True,
        one_failure_warns=True,
    ),
    # ── the forecast group (revision 0025) ──
    #
    # `full_replace=True` for the reason the four above carry it: the
    # replicator writes every row it holds, so a row in DuckDB and not in
    # Postgres means lost, with no retired category to excuse it.
    #
    # The clock column is the one each table actually stamps, and for
    # `revenue_predictions` that is `created_at` — written once per prediction
    # and never touched again, which is what makes it a clock rather than a
    # timestamp of the copy.
    MirroredTable(
        pg_table="app.revenue_predictions",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="revenue_predictions",
        columns=PREDICTION_COLUMNS,
        key_columns=("prediction_date", "sales_type"),
        synced_column="created_at",
        # Shipped and never compared — a whole-table stamp.
        # `store_predictions` DELETEs the forecast and INSERTs it whole after each
        # daily retrain, so every row carries one stamp from one run.
        # A re-derivation between the copy and the comparison would then make
        # every row differ on the clock alone, while the values it guards are
        # identical. `app.sku_inventory_status` and `app.buyer_gender` are the
        # same case.
        ignore_columns=("created_at",),
        numeric=("predicted_revenue", "model_mae", "model_mape", "model_wape"),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.seasonal_indices",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="seasonal_indices",
        columns=SEASONAL_COLUMNS,
        key_columns=("month",),
        synced_column="updated_at",
        # Shipped and never compared — a whole-table stamp.
        # `calculate_seasonality_indices` upserts all twelve months in one
        # statement, stamping every one of them with the same value.
        # A re-derivation between the copy and the comparison would then make
        # every row differ on the clock alone, while the values it guards are
        # identical. `app.sku_inventory_status` and `app.buyer_gender` are the
        # same case.
        ignore_columns=("updated_at",),
        numeric=("seasonality_index", "avg_revenue", "min_revenue",
                 "max_revenue", "yoy_growth"),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.weekly_patterns",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="weekly_patterns",
        columns=WEEKLY_PATTERN_COLUMNS,
        key_columns=("month", "week_of_month"),
        synced_column="updated_at",
        # Shipped and never compared — a whole-table stamp.
        # `calculate_weekly_patterns` writes all sixty month-weeks in one pass.
        # A re-derivation between the copy and the comparison would then make
        # every row differ on the clock alone, while the values it guards are
        # identical. `app.sku_inventory_status` and `app.buyer_gender` are the
        # same case.
        ignore_columns=("updated_at",),
        numeric=("weight",),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.growth_metrics",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="growth_metrics",
        columns=GROWTH_METRIC_COLUMNS,
        key_columns=("metric_type",),
        synced_column="updated_at",
        # Shipped and never compared — a whole-table stamp.
        # One row, rewritten whole by `calculate_yoy_growth`.
        # A re-derivation between the copy and the comparison would then make
        # every row differ on the clock alone, while the values it guards are
        # identical. `app.sku_inventory_status` and `app.buyer_gender` are the
        # same case.
        ignore_columns=("updated_at",),
        numeric=("value",),
        full_replace=True,
        one_failure_warns=True,
    ),
    # ── the watchdog samples and the send ledgers (revision 0026) ──
    #
    # Every clock here is PER ROW, which makes these the first replicated
    # tables in a while with nothing in `ignore_columns`. The four specs above
    # all carry a whole-table stamp — one re-derivation writes one value to
    # every row — and so must hide the stamp or report every row as differing.
    # These do the opposite: a sample is one reading taken at one moment and a
    # ledger row is one delivery at one moment, so `sampled_at` and `sent_at`
    # date the row they sit on. They are both the grace clock and real data,
    # `app.order_backfill_misses.checked_at`'s arrangement, and comparing them
    # is how a sample copied with the wrong timestamp is caught at all.
    #
    # No measure column appears in `numeric=` on the three watchdogs, and that
    # absence is load-bearing rather than an omission: they are DOUBLE in
    # DuckDB and DOUBLE PRECISION in Postgres, so both sides return a float,
    # and coercing one side to Decimal is what would manufacture a difference.
    # `revenue` on the two ledgers is the mirror image — DECIMAL(14, 2) against
    # NUMERIC(14, 2), a Decimal on both sides — and is listed.
    MirroredTable(
        pg_table="app.disk_samples",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="disk_samples",
        columns=DISK_SAMPLE_COLUMNS,
        key_columns=("sampled_at",),
        synced_column="sampled_at",
        full_replace=True,
        # Swept by age every thirty minutes; see the field.
        prunes_by_age=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.data_dir_samples",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="data_dir_samples",
        columns=DATA_DIR_SAMPLE_COLUMNS,
        # The composite is the whole point: one sweep writes seven rows, one
        # per path group, all sharing a `sampled_at`. Keyed on the timestamp
        # alone, six of the seven would collide and the comparison would read
        # a healthy sweep as six lost rows.
        key_columns=("sampled_at", "path_group"),
        synced_column="sampled_at",
        full_replace=True,
        # Swept by age every thirty minutes; see the field.
        prunes_by_age=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.memory_samples",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="memory_samples",
        columns=MEMORY_SAMPLE_COLUMNS,
        key_columns=("sampled_at",),
        synced_column="sampled_at",
        full_replace=True,
        # Swept by age every thirty minutes; see the field.
        prunes_by_age=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.weekly_report_sends",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="weekly_report_sends",
        columns=WEEKLY_SEND_COLUMNS,
        key_columns=("week_start", "sales_type"),
        # `sent_at` is written once, when the message actually went out, and
        # never touched again — which is what makes it a clock rather than a
        # timestamp of the copy, and what makes it worth comparing: a row whose
        # `sent_at` drifted would mean the two stores disagree about when a
        # week was reported.
        synced_column="sent_at",
        numeric=("revenue",),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.traffic_report_sends",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="traffic_report_sends",
        columns=TRAFFIC_SEND_COLUMNS,
        key_columns=("week_start", "sales_type"),
        synced_column="sent_at",
        numeric=("revenue",),
        full_replace=True,
        one_failure_warns=True,
    ),
    # ── the data-quality journal (revision 0028) ──
    #
    # The parent has a clock of its own. Its two children have no timestamp
    # column at all — seven and nine columns, none of them a time — and without
    # one every finding written by the 07:00 integrity scan would be CRITICAL
    # at the 07:30 comparison, most mornings.
    #
    # So they borrow the run's. `synced_column` is interpolated into the SELECT
    # list on the DuckDB side ONLY (`fetch_duckdb_rows`; `fetch_pg_rows` never
    # reads it), so a correlated subquery is a legal clock and needs no new
    # machinery. It is sound rather than merely convenient: `persist_run` writes
    # all three tables inside one DuckDB transaction, so a child is in Postgres
    # if and only if its parent is, and the parent's clock is exactly this
    # child's clock. There is no third state for it to be wrong about.
    MirroredTable(
        pg_table="app.data_quality_runs",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="data_quality_runs",
        columns=DQ_RUN_COLUMNS,
        key_columns=("run_id",),
        synced_column="started_at",
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.data_quality_issues",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="data_quality_issues",
        columns=DQ_ISSUE_COLUMNS,
        key_columns=("run_id", "check_name", "table_name"),
        synced_column=(
            "(SELECT r.started_at FROM data_quality_runs r "
            "WHERE r.run_id = data_quality_issues.run_id)"
        ),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.data_quality_diffs",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="data_quality_diffs",
        columns=DQ_DIFF_COLUMNS,
        key_columns=("run_id", "month", "source_id", "diff_class", "field"),
        synced_column=(
            "(SELECT r.started_at FROM data_quality_runs r "
            "WHERE r.run_id = data_quality_diffs.run_id)"
        ),
        # dk_value and kc_value stay floats on both sides — DOUBLE against
        # DOUBLE PRECISION. They look like money, which is the whole trap:
        # declaring them here would coerce one side to Decimal and report every
        # row as differing.
        full_replace=True,
        one_failure_warns=True,
    ),
    # ── the offer catalogue and the sync watermarks (revision 0029) ──
    MirroredTable(
        pg_table="bronze.offers",
        # Landing data, and still not landing's story: it is copied out of
        # DuckDB by the replicator rather than parsed twice, because it travels
        # with `offer_stocks` on one clock. See `core/pg_operational.py`.
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="offers",
        columns=OFFER_COLUMNS,
        key_columns=("id",),
        synced_column="synced_at",
        # Shipped and never compared. The writer is a loop of
        # `INSERT OR REPLACE ... CURRENT_TIMESTAMP` inside ONE DuckDB
        # transaction, and `CURRENT_TIMESTAMP` is transaction-stable there, so
        # every row the sync touched takes one value — which is what makes
        # comparing it meaningless: a sync between the copy and the check moves
        # the stamp on all of them at once, and the grace stops being per row.
        #
        # Measured rather than assumed, and the measurement has a tail: of
        # 1 057 rows, 1 055 carry the last sync's stamp and TWO are frozen
        # months back (2026-04-21 and 2026-06-13). Those two are offers KeyCRM
        # has stopped serving — `INSERT OR REPLACE` never deletes, so they keep
        # the stamp of their last sighting, the same way product 1055 does one
        # table up. They are copied like any other row and agree on both sides;
        # they are noted here because "one stamp for the whole table" is the
        # obvious thing to write and is not quite what the data says.
        ignore_columns=("synced_at",),
        full_replace=True,
        one_failure_warns=True,
    ),
    MirroredTable(
        pg_table="app.sync_metadata",
        origin_note=_COPIED_FROM_DUCKDB,
        # The same projection the shipper writes, imported rather than
        # restated: if these two ever disagreed about which keys belong in
        # Postgres, the difference would be reported as a defect in the copy.
        dk_table=SYNC_METADATA_SOURCE,
        columns=SYNC_METADATA_COLUMNS,
        key_columns=("key",),
        # Per row, and provably: no statement anywhere touches more than one
        # key. Each writer sets its own `last_sync_<entity>` and stamps it at
        # that moment, so `updated_at` dates the row it sits on.
        synced_column="updated_at",
        full_replace=True,
        one_failure_warns=True,
    ),
)

# ── the two shipped above a watermark ────────────────────────────────────────

_SKU_HISTORY_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("offer_id", _INT), ("date", _DATE), ("quantity", _INT),
    ("reserve", _INT), ("price", _NUMERIC),
)

_MOVEMENT_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("id", _INT), ("offer_id", _INT), ("product_id", _INT),
    ("movement_type", _TEXT), ("quantity_before", _INT),
    ("quantity_after", _INT), ("delta", _INT), ("reserve_before", _INT),
    ("reserve_after", _INT), ("source", _TEXT),
)

_SKU_HISTORY_COLS = tuple(c for c, _ in _SKU_HISTORY_FIELDS)
_MOVEMENT_COLS = tuple(c for c, _ in _MOVEMENT_FIELDS)

# ── the two forensic logs (revision 0027) ──
#
# Both fingerprinted rather than read whole, for `stock_movements`' reason:
# 74 388 rows cannot be pulled out of both stores every morning. The cost is
# the fingerprint's one blind spot — a text column rewritten to exactly the
# same length in a bucket where nothing else moved — and here that is `error`
# and `status`, both of which are written once when the row is inserted and
# never touched again, on tables with no UPDATE anywhere.
#
# `checksum_match` and `validation_passed` are BOOLEAN and _BOOL folds NULL
# into FALSE, so the fingerprint cannot tell "we never got far enough to
# compare" from "they did not match". That is a real gap and it is accepted
# rather than papered over: both facts live in the same row as `error`, whose
# LENGTH the fingerprint does see, so an error-path row cannot masquerade as a
# successful one.
_REFRESH_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("id", _INT), ("refreshed_at", _TS), ("trigger", _TEXT),
    ("duration_ms", _NUMERIC), ("bronze_orders", _INT), ("silver_rows", _INT),
    ("gold_revenue_rows", _INT), ("gold_products_rows", _INT),
    ("silver_revenue_checksum", _NUMERIC), ("gold_revenue_checksum", _NUMERIC),
    ("checksum_match", _BOOL), ("validation_passed", _BOOL),
    ("error", _TEXT), ("silver_mode", _TEXT),
)

_RECONCILIATION_LOG_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("id", _INT), ("check_date", _DATE), ("api_count", _INT),
    ("db_count", _INT), ("discrepancy", _INT), ("discrepancy_pct", _NUMERIC),
    ("status", _TEXT), ("checked_at", _TS),
)

_REFRESH_COLS = tuple(c for c, _ in _REFRESH_FIELDS)
_RECONCILIATION_LOG_COLS = tuple(c for c, _ in _RECONCILIATION_LOG_FIELDS)

# Days since 1970-01-01, one bucket per day. Checked against both engines
# rather than reasoned about: `epoch(DATE '2026-08-27') // 86400` in DuckDB and
# `DATE '2026-08-27' - DATE '1970-01-01'` in PostgreSQL both give 20692.
_DAY_BUCKET_DK = "epoch(date)::BIGINT // 86400"
_DAY_BUCKET_PG = "(date - DATE '1970-01-01')"

APPEND_ONLY_TABLES: Tuple[BucketedTable, ...] = (
    BucketedTable(
        pg_table="app.inventory_sku_history",
        dk_table="inventory_sku_history",
        columns=_SKU_HISTORY_COLS,
        fields=_SKU_HISTORY_FIELDS,
        dk_bucket=_DAY_BUCKET_DK,
        pg_bucket=_DAY_BUCKET_PG,
        numeric=("price",),
        # One bucket is one day, and `offer_id` is unique inside a day — so the
        # row identity the drill-down needs is a plain integer after all, even
        # though the table's key is composite. That is why it is selected
        # first.
        #
        # The table has no timestamp of its own. The day it belongs to is the
        # day it was written, so the date doubles as the freshness signal: only
        # today's snapshot can still be in flight.
        dk_rows_sql=(
            "SELECT " + ", ".join(_SKU_HISTORY_COLS)
            + ", date::TIMESTAMP FROM inventory_sku_history "
            f"WHERE ({_DAY_BUCKET_DK}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_SKU_HISTORY_COLS)} "
            "FROM app.inventory_sku_history "
            f"WHERE ({_DAY_BUCKET_PG}) = $1"
        ),
        one_failure_warns=True,
    ),
    BucketedTable(
        pg_table="app.stock_movements",
        dk_table="stock_movements",
        columns=_MOVEMENT_COLS,
        fields=_MOVEMENT_FIELDS,
        dk_bucket="id // {}".format(BUCKET_SIZE),
        pg_bucket="id / {}".format(BUCKET_SIZE),
        numeric=(),
        # `recorded_at` is a real per-row timestamp and is used as the clock,
        # which is why it is absent from the compared columns above: a movement
        # is stamped by DuckDB when it is detected and copied verbatim, so the
        # two stores do agree on it — but making the clock one of the compared
        # values means a disagreement can never be forgiven by itself.
        dk_rows_sql=(
            "SELECT " + ", ".join(_MOVEMENT_COLS)
            + ", recorded_at FROM stock_movements "
            f"WHERE (id // {BUCKET_SIZE}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_MOVEMENT_COLS)} FROM app.stock_movements "
            f"WHERE (id / {BUCKET_SIZE}) = $1"
        ),
        one_failure_warns=True,
    ),
    # ── the two forensic logs (revision 0027) ──
    #
    # Bucketed on `id // 1000` like `stock_movements`, and unlike
    # `inventory_sku_history`, which buckets by the day because its ~900 offers
    # would otherwise all land in one bucket. These are keyed by a real
    # monotone id, so the arithmetic bucket is the natural one: 75 buckets for
    # `warehouse_refreshes`, 3 for `reconciliation_log`.
    BucketedTable(
        pg_table="app.warehouse_refreshes",
        dk_table="warehouse_refreshes",
        columns=_REFRESH_COLS,
        fields=_REFRESH_FIELDS,
        dk_bucket="id // {}".format(BUCKET_SIZE),
        pg_bucket="id / {}".format(BUCKET_SIZE),
        numeric=("duration_ms", "silver_revenue_checksum",
                 "gold_revenue_checksum"),
        # `refreshed_at` is both the clock and a compared value, and here that
        # is right for `app.order_backfill_misses`' reason: it is written once,
        # when that tick happened, and nothing ever rewrites it. Comparing it
        # is how a row copied with the wrong timestamp is caught at all, and
        # the grace window still answers per row because every row carries its
        # own moment.
        dk_rows_sql=(
            "SELECT " + ", ".join(_REFRESH_COLS)
            + ", refreshed_at FROM warehouse_refreshes "
            f"WHERE (id // {BUCKET_SIZE}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_REFRESH_COLS)} FROM app.warehouse_refreshes "
            f"WHERE (id / {BUCKET_SIZE}) = $1"
        ),
        one_failure_warns=True,
    ),
    BucketedTable(
        pg_table="app.reconciliation_log",
        dk_table="reconciliation_log",
        columns=_RECONCILIATION_LOG_COLS,
        fields=_RECONCILIATION_LOG_FIELDS,
        dk_bucket="id // {}".format(BUCKET_SIZE),
        pg_bucket="id / {}".format(BUCKET_SIZE),
        numeric=("discrepancy_pct",),
        dk_rows_sql=(
            "SELECT " + ", ".join(_RECONCILIATION_LOG_COLS)
            + ", checked_at FROM reconciliation_log "
            f"WHERE (id // {BUCKET_SIZE}) = ?"
        ),
        pg_rows_sql=(
            f"SELECT {', '.join(_RECONCILIATION_LOG_COLS)} "
            "FROM app.reconciliation_log "
            f"WHERE (id / {BUCKET_SIZE}) = $1"
        ),
        one_failure_warns=True,
    ),
)


# ─── The UTM classification: shipped on the Silver tick, compared whole ──────
#
# `silver.order_utm` is the one Silver object Postgres cannot compute, because
# its body is a Python parser (revision 0018). It is therefore shipped, and it
# is compared here rather than inside `reconcile_silver` for one reason that
# is about this table specifically:
#
# **Fingerprints would be the wrong instrument.** The bucket fingerprint sums
# numbers and text *lengths*, which is exactly right for orders — 46,487 rows
# that are mostly numeric and timestamped. This table is almost entirely text,
# and its content is campaign names: renaming `spring` to `autumn` changes no
# number and no length, and in a bucket where nothing else moved the
# fingerprint would report the two stores identical while every chart split
# them differently. So both sides are read whole, 32,905 rows, and compared
# column by column with a tolerance of zero.
#
# It costs more than any other whole-table comparison here — the next largest
# is `bronze.products` at 1,003 rows — and it is a daily check, once, against
# a table this store replaces whole every ten minutes anyway.
#
# The grace is Silver's, not the operational 90 minutes: this rides the Silver
# tick, so a row that is stale by more than that floor is late by the clock
# that actually governs it.

ORDER_UTM_TABLE: MirroredTable = MirroredTable(
    pg_table="silver.order_utm",
    origin_note=(
        "`silver.order_utm` is parsed out of `manager_comment` in Python and "
        "shipped whole from DuckDB — not derived here, because a SQL "
        "reimplementation of the classifier would drift from it. A difference "
        "is therefore a defect in the shipper, and it does not read as missing "
        "data: an order whose UTM row is absent falls through the COALESCE to "
        "`organic` and moves the paid/organic split on the chart."
    ),
    dk_table="silver_order_utm",
    columns=UTM_COLUMNS,
    key_columns=("order_id",),
    # When the parser last looked at the order. Carried across rather than
    # regenerated, so it is the same clock on both sides.
    synced_column="parsed_at",
    full_replace=True,
)


async def reconcile_order_utm(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = SILVER_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare the shipped UTM classification against DuckDB's. Reports only.

    `full_replace=True` removes the retired category, and correctly: every
    ship writes the whole table, so a row Postgres is missing was lost rather
    than retired. That is the same argument `app.manager_classifications`
    makes, and it holds here for the same reason — the shipper has no delta
    path to have skipped a row through.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    async with store.connection() as conn:
        dk_side = read_duckdb_side(conn, (ORDER_UTM_TABLE,))

    dk_rows, dk_synced = dk_side[ORDER_UTM_TABLE.pg_table]
    pg_rows = await fetch_pg_rows(pool, ORDER_UTM_TABLE)
    return compare_table(
        ORDER_UTM_TABLE, dk_rows, dk_synced, pg_rows,
        watermarks.get(ORDER_UTM_TABLE.pg_table),
        now=now, grace_minutes=grace_minutes, max_samples=max_samples,
    )


# ─── The UTM classification's completeness: Postgres alone (DN-16) ───────────
#
# `reconcile_order_utm` asks whether Postgres holds the verdicts DuckDB holds.
# That is not the question /traffic depends on, and it stops meaning anything
# at step 9: once the parse runs inside Postgres there is no DuckDB copy to
# agree with, and two copies missing the same order agree perfectly today. What
# the tab needs is that every order the parser reads has a verdict, and a
# current one — because an order with no row does not read as missing. It falls
# through the COALESCE to organic or unattributed, on the chart and in the
# Monday traffic report, with nothing on either to say so.
#
# **The population is the parser's own predicate**, `_parse_utm_into_silver`
# in core/repositories/traffic.py: an order whose `manager_comment` is neither
# NULL nor empty, with no row or with `updated_at > parsed_at`. `parsed_at` is
# the order's `updated_at` at the moment its comment was read, not the wall
# clock, so a finished parse leaves the two equal and a later edit compares
# greater. An order with no comment never gets a row, by design, and is not
# asked about.
#
# **Read from `bronze.orders`, which is not the table the parser reads.** The
# parser reads DuckDB's `orders`; `bronze.orders` is the only copy of that
# input Postgres holds, and the one step 9's parser will read. Both are fed the
# same tuple in the same call, so they normally hold the same comment — but
# not always. The Postgres upsert keeps a stored `manager_comment` when the
# payload carries NULL (`COALESCE` on conflict), while DuckDB applies that rule
# only on its UPDATE path: an order it INSERTs afresh — the next sync after
# `POST /api/duckdb/purge-orders` — stores whatever the payload carries, NULL
# included, and a restore from an export taken before a backfill brings the
# NULL back with it. A resync does neither; it updates rows that exist. Such
# an order has its comment here and none in DuckDB, so the parser never reads
# it and no parse or ship can clear the finding: it would page CRITICAL every
# morning with a lever that cannot move it. So the findings and the
# REMEDIATION name this second cause beside the first. The same morning's
# orders fingerprint sees the divergence too — it compares `manager_comment`
# and names it in `mirror_row_values` on `bronze.orders` — and the lever is
# putting the comment back in DuckDB from this copy, never the ship. Not the
# `manager_comment` backfill either: it re-fetches the field from KeyCRM,
# whose payload after a purge is the NULL DuckDB stored, skips it as empty,
# and still reports success. It helps only where KeyCRM serves the comment
# today, as after a restore from an export older than a backfill.
#
# **The grace is absolute, on `bronze.orders.mirrored_at`.** Under
# KS_PG_DERIVE=own the order reaches `silver.orders` on Postgres' own signal,
# while its verdict waits for DuckDB's next warehouse tick (two minutes) and
# then the ship's floor (`KS_PG_SILVER_INTERVAL_S`, 600 s by default) — the
# gap #213 accepted rather than patched. So the grace is resolved from that
# variable when the check runs, through
# `pg_warehouse_dq.silver_grace_minutes()`: the floor plus the twins'
# ten-minute margin, twenty minutes at the default — the number
# `SILVER_GRACE_MINUTES` fixes for the comparison above. A constant here would
# go on paging the old floor's width the day somebody raised it, and the floor
# and the grace would then be two numbers somebody has to remember to move
# together. `deploy/stage4_soak/11_utm_gap.sql` asks the same question by hand
# with fifteen minutes: right for a reading, and a page on a slow tick here.
#
# **`mirrored_at` is when the row was last written, not when the order
# arrived**, and that is the grace's blind spot. The upsert stamps it on every
# write, identical rewrites included: the 05:15 status refresh force-writes
# ~1,400 unchanged orders, and a backfill ship re-stamps every order it
# carries. A verdict missing for days therefore reads as in flight — INFO,
# counted, not paged — for one grace after such a rewrite. Not changed here:
# the stamp belongs to the mirror's upsert, and Reconciliation A reads it too.
# The scheduled run does not meet the 05:15 case, since 07:30 is two hours
# past that window; a backfill run by hand in the grace before 07:30 would
# hide the orders it re-stamps for that one morning, as INFO.
#
# **Not the ship's watermark**, which was the other clock available. A ship
# that stops being *called* records nothing at all — a warehouse refresh that
# errors returns before `_rebuild_postgres_layers` ships anything, so
# `last_ok_at` just ages and `failures_since_ok` stays at zero — and this
# check is then the only witness left. It is also an ordinary morning: the
# 05:15 status refresh, the weekly full sync and the sync of today each parse
# in DuckDB without marking the warehouse dirty, and a restart forgets the
# in-memory deferral (`_pg_layers_pending`), so their verdicts wait for the
# next dirty tick. The watermark is also the wrong shape:
# a ship can land between an order's arrival and DuckDB's parse of it, so
# "shipped since the order arrived" does not mean "should have carried it".
#
# **Inside the grace is INFO, counted, never nothing.** The plan asked for the
# width of the two-clocks gap to be measured before step 9's flip, and a daily
# count of orders caught in flight at 07:30 is that measurement; INFO rides the
# digest and never summons it, so a busy morning costs a line and not a page.
#
# Report only, like everything in this module: no finding here repairs
# anything or reaches `validation_passed`. The lever is the parse, the ship, or
# DuckDB's copy of the comment, and a check that re-shipped what it found
# missing would hide which one broke. It reads Postgres alone and takes no
# store, so it holds whichever store does the parsing.

ORDER_UTM_COMPLETENESS_SQL = """
WITH owed AS (
    SELECT b.id,
           u.order_id IS NULL     AS missing,
           b.mirrored_at >= $1    AS in_flight,
           b.mirrored_at
    FROM bronze.orders b
    LEFT JOIN silver.order_utm u ON u.order_id = b.id
    WHERE b.manager_comment IS NOT NULL
      AND b.manager_comment <> ''
      AND (u.order_id IS NULL OR b.updated_at > u.parsed_at)
)
SELECT
    count(*) FILTER (WHERE missing AND NOT in_flight)                   AS missing,
    (array_agg(id ORDER BY id)
        FILTER (WHERE missing AND NOT in_flight))[1:$2]                 AS missing_ids,
    min(mirrored_at) FILTER (WHERE missing AND NOT in_flight)           AS missing_since,
    count(*) FILTER (WHERE NOT missing AND NOT in_flight)               AS stale,
    (array_agg(id ORDER BY id)
        FILTER (WHERE NOT missing AND NOT in_flight))[1:$2]             AS stale_ids,
    min(mirrored_at) FILTER (WHERE NOT missing AND NOT in_flight)       AS stale_since,
    count(*) FILTER (WHERE in_flight)                                   AS in_flight,
    (array_agg(id ORDER BY id) FILTER (WHERE in_flight))[1:$2]          AS in_flight_ids
FROM owed
"""


# Why the `pg_order_utm_` REMEDIATION line names its levers in the order it
# does. That line is the single "→" of a Telegram alert and has room for the
# levers alone, so the reasons ride in the two paging descriptions, which
# /api/health/data-quality carries whole. One text for both, because both page
# with the same line.
_ORDER_UTM_LEVER_REASONS = (
    "The alert's levers, in order. If the fingerprint names manager_comment, "
    "copy the comment into DuckDB from bronze.orders, which holds it after a "
    "purge and after a restore alike. POST /api/traffic/backfill-utm is not "
    "that lever: it re-fetches the comment from KeyCRM, so it restores one "
    "only where KeyCRM serves it today (a restore from an export older than "
    "a backfill), and after a purge, where KeyCRM's payload was the NULL "
    "DuckDB stored, it skips the order and the run still reports success. "
    "Otherwise read {table}'s row in meta.mirror_state before anything "
    "ships: a refused ship names itself in last_error, and the next "
    "successful one clears it. Then POST /api/traffic/refresh, which parses "
    "and ships whether or not the warehouse is dirty; POST "
    "/api/warehouse/refresh never ships this table."
)


def order_utm_completeness_findings(
    row: Mapping[str, Any], *, grace_minutes: int,
) -> List[IntegrityIssue]:
    """The findings one aggregate row of `ORDER_UTM_COMPLETENESS_SQL` makes.

    Pure, so what each count means is testable without a server; the SQL that
    produces the counts is tested against a real one.
    """
    issues: List[IntegrityIssue] = []
    table = ORDER_UTM_TABLE.pg_table

    def _ids(key: str) -> Tuple[int, ...]:
        return tuple(int(i) for i in (row[key] or ()))

    def _since(key: str) -> str:
        value = row[key]
        return value.isoformat() if isinstance(value, datetime) else "unknown"

    missing = int(row["missing"] or 0)
    if missing:
        issues.append(IntegrityIssue(
            check_name="pg_order_utm_missing",
            table_name=table,
            severity=Severity.CRITICAL,
            count=missing,
            sample_ids=_ids("missing_ids"),
            description=(
                f"{missing} order(s) in bronze.orders carry a manager_comment "
                f"and have no row in {table}, and were last written to Postgres "
                f"more than {grace_minutes} minutes ago (the oldest at "
                f"{_since('missing_since')}). /traffic and the weekly traffic "
                "report file each of them through the COALESCE as organic or "
                "unattributed, with nothing on screen to say so. Either the "
                "parse did not reach them or the ship stopped carrying them, "
                "or DuckDB's copy of the order has no comment: the parser "
                "reads DuckDB's orders, not bronze.orders, so it never sees "
                "them, and no parse or ship clears this. The orders "
                "fingerprint names that case as manager_comment in "
                "mirror_row_values on bronze.orders. "
                + _ORDER_UTM_LEVER_REASONS.format(table=table)
            ),
        ))

    stale = int(row["stale"] or 0)
    if stale:
        issues.append(IntegrityIssue(
            check_name="pg_order_utm_stale",
            table_name=table,
            severity=Severity.CRITICAL,
            count=stale,
            sample_ids=_ids("stale_ids"),
            description=(
                f"{stale} order(s) have a verdict in {table} older than the "
                "order itself (updated_at > parsed_at), and the newer order "
                "was last written to Postgres more than "
                f"{grace_minutes} minutes ago (the oldest at "
                f"{_since('stale_since')}). parsed_at is the order's "
                "own updated_at when its comment was read, so the parser has "
                "not caught up with an edit, or its re-parse was not shipped, "
                "or DuckDB's copy of the order has lost its comment and the "
                "parser, which reads DuckDB, no longer re-reads it (the orders "
                "fingerprint then names manager_comment in mirror_row_values "
                "on bronze.orders). "
                "/traffic shows the previous classification for these. "
                + _ORDER_UTM_LEVER_REASONS.format(table=table)
            ),
        ))

    in_flight = int(row["in_flight"] or 0)
    if in_flight:
        issues.append(IntegrityIssue(
            check_name="pg_order_utm_in_flight",
            table_name=table,
            severity=Severity.INFO,
            count=in_flight,
            sample_ids=_ids("in_flight_ids"),
            description=(
                f"{in_flight} order(s) were written to Postgres in the last "
                f"{grace_minutes} minutes without a current verdict in "
                f"{table}. Not a defect yet: the verdict follows on DuckDB's "
                "tick and the ship's floor, the gap #213 accepted, and /traffic "
                "shows these without their current verdict until it lands. "
                "The clock is the row's last write, so a rewrite of an "
                "unchanged order (the 05:15 refresh, a backfill) puts one long "
                "without a verdict here for one grace too. "
                "Counted because that width is what step 9's parse inside "
                "Postgres is meant to close."
            ),
        ))

    return issues


async def order_utm_completeness_row(
    conn, *, now: datetime, grace_minutes: int, max_samples: int,
) -> Mapping[str, Any]:
    """One aggregate row, on a connection the caller holds.

    Split from the job so a test can run it inside a transaction it then rolls
    back: the query reads all of `bronze.orders`, and a scenario seeded in a
    shared database is only a scenario if nothing else is in the table.
    """
    return await conn.fetchrow(
        ORDER_UTM_COMPLETENESS_SQL,
        now - timedelta(minutes=int(grace_minutes)),
        int(max_samples),
    )


async def reconcile_order_utm_completeness(
    *,
    now: Optional[datetime] = None,
    grace_minutes: Optional[int] = None,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Does every order the UTM parser reads have a current verdict in
    Postgres? Reports only.

    Takes no store: both sides of the question are in Postgres, which is what
    lets it outlive `reconcile_order_utm` when the parse moves there.

    `grace_minutes=None` — what the job passes — is the ship's floor plus the
    twins' margin, read from `KS_PG_SILVER_INTERVAL_S` now rather than at
    import, so the two move together. A floor that is not a number raises
    here, and the job's `check` names the error on the run.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision
    from core.pg_warehouse_dq import silver_grace_minutes

    if not pg_landing.enabled():
        return []

    if grace_minutes is None:
        grace_minutes = silver_grace_minutes()
    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        row = await order_utm_completeness_row(
            conn, now=now, grace_minutes=grace_minutes, max_samples=max_samples,
        )
    return order_utm_completeness_findings(row, grace_minutes=grace_minutes)


# ─── The order-level expenses: a delta mirror read whole ─────────────────────
#
# `bronze.expenses` ships what a sync fetched, so it is gated on
# `backfilled_at` exactly as orders are: until history has been carried across,
# every expense older than the mirror looks the same as a lost one.
#
# **Read whole rather than fingerprinted, unlike orders, and the reason is
# size rather than principle.** 15,020 rows against orders' 46,487 and their
# 147,648 line items — small enough that both sides fit in memory once a day,
# which buys an exact comparison instead of one that cannot see a text edit of
# equal length. `description` is a text column here and carries KeyCRM's own
# wording, so that blind spot would have been a real one.
#
# `expense_types` needs none of this and is compared with the catalogue above:
# 27 rows re-shipped whole on every sync, so its first successful ship *is* its
# history.

EXPENSES_TABLE: MirroredTable = MirroredTable(
    pg_table="bronze.expenses",
    dk_table="expenses",
    columns=tuple(EXPENSE_COLUMNS),
    key_columns=("id",),
    # `synced_at` — DuckDB's own bookkeeping, and the default — not the
    # payload's `created_at`, which was the first choice here and was wrong
    # twice over.
    #
    # It is the wrong *clock*: `created_at` is KeyCRM's stamp, so an expense
    # created weeks ago and only just synced would read as lost rather than as
    # in flight, which is precisely the distinction the grace window exists to
    # draw. And it is in `columns`, so it would have been both the clock and a
    # compared value — the shape that made `sku_inventory_status` scream
    # CRITICAL over 891 rows while three real differences hid inside the
    # number. `synced_at` is in neither store's shared row, so it is read
    # alongside and never compared, which is what `fetch_duckdb_rows` promises.
    synced_column="synced_at",
    numeric=("amount",),
)


async def reconcile_expenses(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = MIRROR_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare the mirrored order-level expenses. Reports only.

    Not `full_replace`: this mirror upserts a delta, so "in DuckDB and not in
    Postgres" has the retired reading available to it — an expense KeyCRM
    stopped serving is kept by both stores, and one the mirror simply never
    carried is the defect. `synced_column` tells them apart the way it does for
    the catalogue.
    """
    from core import chain_latch, pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []

    # Stood down with its shipper once a write chain owns the table (DN-22b):
    # the local answer before either store is read, the owner rows after
    # `require_revision()` and before the watermarks — `reconcile_mirror`'s
    # arrangement, and the same two findings.
    tables = [EXPENSES_TABLE.pg_table]
    issues, held = _landing_stood_down(tables)
    if held:
        return issues

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    issues, held = _landing_stood_down(tables, await chain_latch.read_owners(pool))
    if held:
        return issues
    watermarks = await fetch_watermarks(pool)
    watermark = watermarks.get(EXPENSES_TABLE.pg_table)

    async with store.connection() as conn:
        dk_rows, dk_synced = fetch_duckdb_rows(conn, EXPENSES_TABLE)

    if (watermark or {}).get("last_ok_at") and not (watermark or {}).get("backfilled_at"):
        # The gate, orders' reasoning at a twentieth of the scale: without it
        # the first run would file one CRITICAL per expense that predates the
        # mirror, for a job nobody has run yet.
        pg_rows = await fetch_pg_rows(pool, EXPENSES_TABLE)
        return [IntegrityIssue(
            check_name="mirror_backfill_pending",
            table_name=EXPENSES_TABLE.pg_table,
            severity=Severity.WARN,
            count=max(len(dk_rows) - len(pg_rows), 0),
            description=(
                f"{EXPENSES_TABLE.pg_table} holds {len(pg_rows)} row(s) against "
                f"DuckDB's {len(dk_rows)}, and no completed backfill is "
                "recorded. The mirror ships only what a sync writes, so "
                "history does not arrive on its own: run "
                "POST /api/mirror/backfill/expenses. Row-level comparison is "
                "suppressed "
                "until it completes — before that, 'missing' and 'lost' are "
                "the same picture."
            ),
        )]

    pg_rows = await fetch_pg_rows(pool, EXPENSES_TABLE)
    return compare_table(
        EXPENSES_TABLE, dk_rows, dk_synced, pg_rows, watermark,
        now=now, grace_minutes=grace_minutes, max_samples=max_samples,
    )


def _chain_flag_findings(chain_errors: Dict[str, str]) -> List[IntegrityIssue]:
    """A KS_WRITE_* value no chain understands: its tables were stood down here
    and by the shipper, so nothing compares or copies them until it is fixed.
    CRITICAL — the chain's own writers raise on every call in the meantime,
    unless it is latched, in which case they keep writing Postgres and the
    finding below says the flag and the latch disagree as well."""
    if not chain_errors:
        return []
    return [IntegrityIssue(
        check_name="write_chain_flag_invalid",
        table_name="(write chains)",
        severity=Severity.CRITICAL,
        count=len(chain_errors),
        description=(
            "Write chain flag(s) not understood — "
            + "; ".join(f"{name}: {err}" for name, err in sorted(chain_errors.items()))
            + ". Those chains' writers raise, and their tables are neither "
            "shipped nor compared until the value is corrected."
        ),
    )]


def _chain_latch_findings(
    owners: Dict[str, str], watermarks: Dict[str, Dict[str, Any]],
) -> List[IntegrityIssue]:
    """The two ways the ownership latch can be wrong (DN-06).

    The latch has two copies — a local marker file that decides where writes go
    and an `owner:<table>` row in `meta.chain_watermarks` written inside the
    first writing transaction — and this is the only place that sees both.

    **`chain_latch_disagrees`.** A marker with no owner row means the first
    Postgres write failed after the marker was taken: the chain is latched,
    which is the safe direction, and nothing else would ever say so. An owner
    row with no marker is the dangerous direction — the marker is what routes
    the writes, so without it the flag decides again and a second writer starts
    the moment somebody flips it back. Both are CRITICAL and neither is
    repaired here: a "repair" would be guessing which store a human meant.

    **`chain_shipper_overwrote`.** The shipper full-replaces these tables out
    of DuckDB, so a successful shipment dated after the handover wrote over
    rows only Postgres held. `last_ok_at` newer than the latch is exactly that
    event, and it is the detector the stage-4 plan calls E1. It cannot fire on
    a shipment that predates the handover, which is why the owner row stores
    the moment ownership passed and never moves it forward.

    **`chain_owner_unregistered`.** An owner row naming a table no chain in
    this build declares: the build is older than the chain (a rollback, a
    pinned `sha-*`), or the chain left `WRITE_CHAINS` without being copied
    back. The two findings above walk the registered chains and so cannot see
    it, and the hourly copy and the landing paths stand down on it
    (`chain_latch.owned_tables`) — which makes this the one place it is said.
    One finding for all of them, like the landing's owner-row page.

    Takes the owner rows the caller has already read rather than reading them
    itself: the same query decides which tables this run may compare at all,
    and two reads could disagree about that within one run.
    """
    from core import chain_latch
    from core.write_chains import WRITE_CHAINS, chain_name

    issues: List[IntegrityIssue] = []
    for chain in WRITE_CHAINS:
        name = chain_name(chain)
        tables = tuple(chain.CHAIN_TABLES)
        local = chain_latch.latched_at(name)
        claimed = {t: owners[t] for t in tables if t in owners}

        if local and len(claimed) < len(tables):
            issues.append(IntegrityIssue(
                check_name="chain_latch_disagrees",
                table_name=name,
                severity=Severity.CRITICAL,
                count=len(tables) - len(claimed),
                description=(
                    f"{name} is latched locally since {local}, but only "
                    f"{len(claimed)} of {len(tables)} of its tables carry an "
                    "owner row in Postgres. The first write there failed after "
                    "the marker was taken, or the row was deleted. The chain "
                    "keeps writing Postgres; run scripts/chain_copy_back.py to "
                    "go back deliberately."
                ),
            ))
        elif claimed and not local:
            issues.append(IntegrityIssue(
                check_name="chain_latch_disagrees",
                table_name=name,
                severity=Severity.CRITICAL,
                count=len(claimed),
                description=(
                    f"{name} owns {len(claimed)} table(s) in Postgres since "
                    f"{min(claimed.values())}, and the local marker is gone. "
                    "The marker is what routes the writes, so this chain will "
                    "follow its KS_WRITE_* again and a second writer starts "
                    "the moment it says duckdb."
                ),
            ))

        for table in tables:
            # The owner row where there is one, the marker otherwise: a table
            # whose first write failed is still a table the chain has taken.
            since = claimed.get(table) or local
            if not since:
                continue
            last_ok = (watermarks.get(table) or {}).get("last_ok_at")
            if last_ok is None or not _after(last_ok, since):
                continue
            issues.append(IntegrityIssue(
                check_name="chain_shipper_overwrote",
                table_name=table,
                severity=Severity.CRITICAL,
                count=1,
                description=(
                    f"{table} changed hands at {since}, and the hourly "
                    f"replication recorded a successful shipment at "
                    f"{last_ok.isoformat()} — after it. That shipment replaced "
                    "the table out of a DuckDB that has stopped receiving "
                    "these rows, so whatever was written here since the "
                    "handover is gone. Do not re-run the shipper."
                ),
            ))

    unregistered = chain_latch.unregistered_owned_tables(owners)
    if unregistered:
        named = ", ".join(sorted(unregistered))
        issues.append(IntegrityIssue(
            check_name="chain_owner_unregistered",
            table_name=named,
            severity=Severity.CRITICAL,
            count=len(unregistered),
            description=(
                f"Owner rows in Postgres say a write chain owns {named} since "
                f"{min(unregistered.values())}, and no chain in this build "
                "declares them — an image older than the chain, or a chain "
                "dropped from WRITE_CHAINS without its copy-back. The hourly "
                "copy and this comparison stand down on them, but this build "
                "has no writer for them in Postgres, so whatever it writes goes "
                "to DuckDB alone. Redeploy a build that declares the chain, or "
                "run scripts/chain_copy_back.py from one."
            ),
        ))
    return issues


def _after(moment: datetime, stamp: str) -> bool:
    """Is `moment` later than the ISO-8601 `stamp`? Unparseable reads as no.

    A latch stamp that cannot be parsed must not manufacture a CRITICAL about
    an overwrite: the marker's own fallback already covers a marker written by
    a future release, and a false alarm here would send somebody looking for
    rows nothing lost.
    """
    try:
        latched = datetime.fromisoformat(stamp)
    except (TypeError, ValueError):
        return False
    if latched.tzinfo is None:
        latched = latched.replace(tzinfo=timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment > latched


async def reconcile_operational(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = OPERATIONAL_GRACE_MINUTES,
    max_samples: int = 10,
    max_buckets: int = 20,
) -> List[IntegrityIssue]:
    """Compare the six replicated tables, whole ones then fingerprinted ones.

    Gated on `last_ok_at` per table, like Silver and Gold: the replicator either
    replaces a table whole or writes strictly above what Postgres holds, so the
    only question the watermark has to answer is whether it has ever run. A
    table it has not reached reports `mirror_never_shipped` and suppresses its
    row-level findings, which is what keeps the first morning after a deploy
    from filing 143,274 CRITICALs.

    Reports only, on this module's standing grounds: a comparison that repairs
    what it finds destroys the evidence that it found anything. That matters
    more here than anywhere else in the file — a "repair" of `stock_movements`
    would be writing the only record of a stock change from the only other
    record of it.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    # Tables DuckDB no longer writes are not compared, `reconcile_sms`'
    # arrangement and its reason: once the writer moves, the DuckDB side is a
    # frozen artefact, and every row written since the switch would be reported
    # as a discrepancy the check itself created. Standing down is not losing
    # the guarantee — it is declining to assert one about a copy that has
    # stopped being a copy.
    #
    # It reads the same tuple the shipper stands down on, so the two cannot
    # disagree about which tables have changed hands.
    from core import chain_latch
    from core.write_chains import stood_down_tables_checked

    stood_down, chain_errors = stood_down_tables_checked()

    # The owner rows, read once: they say which tables to stand down as well as
    # what the two copies of the latch disagree about. A chain whose local
    # marker was lost still owns its rows here, and comparing its tables
    # against a DuckDB that stopped receiving them would file one CRITICAL per
    # row for a state one finding already describes — `chain_latch_disagrees`,
    # built below out of these same rows. Same query as the shipper's, for the
    # standing reason that the two must not disagree about which tables have
    # changed hands.
    owners = await chain_latch.read_owners(pool)
    # Each owner row as itself too (`owned_tables`), the shipper's union: on an
    # image older than the chain nothing here declares the table, and the
    # comparison would either file every chain-written row as a discrepancy or
    # — once the shipper had overwritten them — report two agreeing copies.
    # `chain_owner_unregistered` below says which tables and why.
    stood_down = stood_down | chain_latch.owned_tables(owners)

    whole = tuple(s for s in OPERATIONAL_TABLES if s.pg_table not in stood_down)

    # ── what the chains themselves say, before any row is read ──
    #
    # First, because these findings are about the chains and not about the
    # tables: a flag typo or a broken latch reports the same way whether or not
    # a fingerprint happened to disagree, and the drill-down below returns
    # early when nothing does. Filed after it, `write_chain_flag_invalid` was
    # unreachable on every run where the copies agreed — which is every healthy
    # run, and the only kind this has had.
    issues: List[IntegrityIssue] = _chain_flag_findings(chain_errors)
    issues += _chain_latch_findings(owners, watermarks)

    # ── the four read whole ──
    async with store.connection() as conn:
        dk_side = read_duckdb_side(conn, whole)

    for spec in whole:
        dk_rows, dk_synced = dk_side[spec.pg_table]
        pg_rows = await fetch_pg_rows(pool, spec)
        issues += compare_table(
            spec, dk_rows, dk_synced, pg_rows,
            watermarks.get(spec.pg_table),
            now=now, grace_minutes=grace_minutes, max_samples=max_samples,
        )

    # ── the two fingerprinted ──
    appended = tuple(
        s for s in APPEND_ONLY_TABLES if s.pg_table not in stood_down)
    async with store.connection() as conn:
        dk_prints = {s.pg_table: fingerprints(conn, s) for s in appended}

    suspects: Dict[str, List[int]] = {}
    for spec in appended:
        dk_print = dk_prints[spec.pg_table]
        dk_count = sum(int(v[0]) for v in dk_print.values())
        found, last_ok_at = _watermark_findings(
            spec.pg_table, watermarks.get(spec.pg_table), dk_count,
            one_failure_warns_within=(
                timedelta(minutes=int(grace_minutes))
                if spec.one_failure_warns else None
            ),
            now=now,
        )
        issues += found
        if last_ok_at is None:
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
                    f"{len(differing)} buckets disagree in {spec.pg_table} — "
                    "more than this check will open. A whole-table problem: "
                    "the replicator not running, or a watermark that moved "
                    "without the rows behind it."
                ),
            ))
            differing = differing[:max_buckets]
        suspects[spec.pg_table] = differing

    if not any(suspects.values()):
        return issues

    dk_buckets: Dict[str, Dict[int, Any]] = {}
    async with store.connection() as conn:
        for spec in APPEND_ONLY_TABLES:
            dk_buckets[spec.pg_table] = {
                bucket: _read_dk_bucket(conn, spec, bucket)
                for bucket in suspects.get(spec.pg_table, ())
            }

    for spec in APPEND_ONLY_TABLES:
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


# ─── The bot's own state: the third store, and the one with no backup ────────
#
# Everything above compares DuckDB against Postgres. This compares **SQLite**
# against Postgres, because `data/bot.db` is the third store in this system and
# the only one that is in no backup at all — `data/backups/` and
# `deploy/daily_offsite.sh` both glob `analytics-*.duckdb`, and the Ark froze
# the warehouse. Revision 0009 has the measurement.
#
# `compare_table` is reused unchanged, which is the point: it is pure and takes
# two already-read dictionaries, so it does not care that one of them came out
# of a different engine. Only the reader is new.
#
# `full_replace=True` on all four. `deny_user` and `revoke_user` really do
# delete rows, so "in SQLite and not in Postgres" has one meaning here and it
# is not "retired" — a ghost in `authorized_users` reads as an approved person
# who is not, which is the worst shape this particular table has.

BOT_DB_GRACE_MINUTES = OPERATIONAL_GRACE_MINUTES

# `dk_table` names the SQLite table. The field means "the other store's table"
# and the other store is not always DuckDB from here on.
BOT_STATE_TABLES: Tuple[MirroredTable, ...] = (
    MirroredTable(
        pg_table="app.authorized_users",
        origin_note=_COPIED_FROM_SQLITE,
        dk_table="authorized_users",
        columns=AUTHORIZED_COLUMNS,
        key_columns=("user_id",),
        # A row always has `requested_at`; the other two are often NULL. Read
        # in that order so a person approved four minutes ago is in flight
        # rather than CRITICAL — the copy runs hourly.
        synced_column="COALESCE(last_activity, reviewed_at, requested_at)",
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.user_preferences",
        origin_note=_COPIED_FROM_SQLITE,
        dk_table="user_preferences",
        columns=PREFERENCE_COLUMNS,
        key_columns=("user_id",),
        synced_column="updated_at",
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.celebrated_milestones",
        origin_note=_COPIED_FROM_SQLITE,
        dk_table="celebrated_milestones",
        columns=MILESTONE_COLUMNS,
        key_columns=("id",),
        synced_column="celebrated_at",
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.report_history",
        origin_note=_COPIED_FROM_SQLITE,
        dk_table="report_history",
        columns=REPORT_HISTORY_COLUMNS,
        key_columns=("id",),
        synced_column="created_at",
        full_replace=True,
    ),
)


def fetch_sqlite_rows(
    conn, spec: MirroredTable,
) -> Tuple[Dict[Any, Tuple[Any, ...]], Dict[Any, Optional[datetime]]]:
    """One `bot.db` table, in the shape `compare_table` already understands.

    The values go through `core.pg_bot_state._convert`, the same function the
    copy uses. That is not tidiness: SQLite has no boolean and no timezone, so
    an uncoverted `1` would never equal Postgres' `True` and a naive string
    would never equal a `timestamptz`. Two conversions would have to agree, and
    a rule with two homes is a rule that will differ.
    """
    from core.pg_bot_state import _convert

    cols = ", ".join(spec.columns)
    rows = conn.execute(
        f"SELECT {cols}, {spec.synced_column} FROM {spec.dk_table}"
    ).fetchall()

    width = len(spec.columns)
    values: Dict[Any, Tuple[Any, ...]] = {}
    synced: Dict[Any, Optional[datetime]] = {}
    for row in rows:
        converted = _convert(spec.dk_table, spec.columns, row[:width])
        if any(converted[spec.columns.index(c)] is None for c in spec.key_columns):
            continue
        key = _row_key(spec, converted)
        values[key] = _normalise_row(converted, spec.columns, spec.numeric)
        synced[key] = _as_utc_stamp(row[width])
    return values, synced


def _as_utc_stamp(value: Any) -> Optional[datetime]:
    from core.pg_bot_state import as_utc

    return as_utc(value)


async def reconcile_bot_state(
    db_path=None,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = BOT_DB_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare `data/bot.db` against `app.*`.

    Skips silently when the file is absent, which is a developer's checkout and
    not a defect. A missing `bot.db` on production would show up as the copy's
    watermark going stale, which `_watermark_findings` reports without needing
    the file to exist.

    Skips just as silently once `KS_BOT_STORE=postgres`. At that point the bot
    is the writer and `bot.db` is a frozen artefact, so every approval made
    since the switch would read as a discrepancy — a check that reports a
    difference it created is worse than no check, and this is the same guard
    the copy in `core/pg_bot_state.py` carries for the same moment.

    Reports only, like every other comparison in this module.
    """
    import sqlite3

    from core import pg_landing
    from core.bot_prefs import BOT_DB_PATH
    from core.bot_store import ENGINE_ENV
    from core.pg import get_pool, require_revision
    from core.pg_bot_state import BUSY_TIMEOUT_SECONDS

    if not pg_landing.enabled():
        return []
    if os.getenv(ENGINE_ENV, "sqlite").strip().lower() == "postgres":
        return []

    path = Path(db_path) if db_path is not None else BOT_DB_PATH
    if not path.exists():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    with sqlite3.connect(
        f"file:{path}?mode=ro", uri=True, timeout=BUSY_TIMEOUT_SECONDS,
    ) as conn:
        sqlite_side = {
            spec.pg_table: fetch_sqlite_rows(conn, spec)
            for spec in BOT_STATE_TABLES
        }

    issues: List[IntegrityIssue] = []
    for spec in BOT_STATE_TABLES:
        rows, synced = sqlite_side[spec.pg_table]
        pg_rows = await fetch_pg_rows(pool, spec)
        issues += compare_table(
            spec, rows, synced, pg_rows, watermarks.get(spec.pg_table),
            now=now, grace_minutes=grace_minutes, max_samples=max_samples,
        )
    return issues


# ─── the SMS tab's state (revision 0013) ─────────────────────────────────────
#
# Five tables, all compared whole, and the reason none of them is fingerprinted
# is size rather than principle: a campaign is capped at 5 000 recipients and
# the roster tables are a few thousand rows between them. (Revision 0013's
# sixth, `bronze.offer_stocks`, is compared with the operational tables — it is
# the only one of the six DuckDB still writes after the switch, and this list
# stops being compared at all then.) Fingerprinting exists
# in this file for `stock_movements` and `inventory_sku_history`, where pulling
# 143 274 rows out of both stores every morning is the thing being avoided.
# When `sms_dlr_events` passes ~50 000 rows — roughly ten campaigns, since it
# takes one row per delivery report and is never pruned — it should move to
# `BucketedTable` alongside them.
#
# `full_replace` answers "what does a row DuckDB has and Postgres does not
# *mean*", and for all five the answer is the same: lost. Four are replaced
# whole every hour. `sms_dlr_events` ships above a watermark instead, but it is
# append-only at the source — nothing ever deletes a binding — so a missing row
# still cannot be a retirement. The flag is set for the meaning, not the
# shipping shape.
SMS_TABLES: Tuple[MirroredTable, ...] = (
    MirroredTable(
        pg_table="app.marketing_optouts",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="marketing_optouts",
        columns=OPTOUT_COLUMNS,
        key_columns=("buyer_id", "channel"),
        # The row's own value doubles as its clock, `order_backfill_misses`'
        # arrangement: an opt-out is written once and never revised.
        synced_column="opted_out_at",
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.sms_audience_presets",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="sms_audience_presets",
        columns=PRESET_COLUMNS,
        key_columns=("name",),
        # `updated_at` is NULL until the preset is first edited, so the
        # fallback is what dates an untouched one.
        synced_column="COALESCE(updated_at, created_at)",
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.sms_campaigns",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="sms_campaigns",
        columns=CAMPAIGN_COLUMNS,
        key_columns=("campaign",),
        synced_column="COALESCE(sent_at, exported_at)",
        numeric=("price_per_part", "cost_total"),
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.sms_campaign_members",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="sms_campaign_members",
        columns=MEMBER_COLUMNS,
        key_columns=("buyer_id", "campaign"),
        # The one table here with no clock of its own; see core/pg_sms.py.
        # `buyer_id` leads the key so the drill-down's sample id is an integer.
        synced_column=MEMBER_STAMP,
        numeric=("revenue_ltv_at_export", "margin_ltv_at_export"),
        full_replace=True,
    ),
    MirroredTable(
        pg_table="app.sms_dlr_events",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="sms_dlr_events",
        columns=DLR_COLUMNS,
        key_columns=("event_id",),
        synced_column="first_seen_at",
        full_replace=True,
    ),
)


# ─── the dashboard's user list (revision 0016) ───────────────────────────────
#
# One table, compared whole — 24 rows. `full_replace=True` because the
# replicator writes every row it holds, so "in DuckDB and not in Postgres"
# means lost; there is no retired category for a person's access.
#
# Stands down with the switch, `reconcile_sms`' reason: once
# `KS_USER_STORE=postgres` moves the writer, DuckDB is a frozen artefact and
# every approval made since would read as a discrepancy this check created.
DASHBOARD_USER_TABLES: Tuple[MirroredTable, ...] = (
    MirroredTable(
        pg_table="app.dashboard_users",
        origin_note=_COPIED_FROM_DUCKDB,
        dk_table="users",
        columns=USER_COLUMNS,
        key_columns=("user_id",),
        # `reviewed_at` moves on approve, deny and role change; `created_at`
        # dates a row that has never been touched since. Neither is ideal
        # alone, and together they are what says a row has had time to travel.
        synced_column="COALESCE(reviewed_at, created_at)",
        full_replace=True,
    ),
)


async def reconcile_dashboard_users(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = OPERATIONAL_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare who may open the dashboard, between the two stores.

    Report-only, and more firmly than most: a "repair" here would be one store
    silently granting or withdrawing somebody's access on the strength of the
    other, which is a decision a human made and a check must not re-make.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision
    from core.pg_dashboard_users import user_store_is_postgres

    if not pg_landing.enabled() or user_store_is_postgres():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    async with store.connection() as conn:
        dk_side = read_duckdb_side(conn, DASHBOARD_USER_TABLES)

    issues: List[IntegrityIssue] = []
    for spec in DASHBOARD_USER_TABLES:
        dk_rows, dk_synced = dk_side[spec.pg_table]
        pg_rows = await fetch_pg_rows(pool, spec)
        issues += compare_table(
            spec, dk_rows, dk_synced, pg_rows, watermarks.get(spec.pg_table),
            now=now, grace_minutes=grace_minutes, max_samples=max_samples,
        )
    return issues


async def reconcile_sms(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = OPERATIONAL_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare the SMS tab's five state tables between DuckDB and Postgres.

    Stands down once `KS_SMS_STORE=postgres`, and not because the comparison
    would be expensive: after the switch DuckDB is a frozen artefact, so every
    opt-out and every campaign recorded since would read as a discrepancy this
    check itself created. `reconcile_bot_state` stands down beside
    `replicate_bot_state` for exactly this reason.

    Reports only. A "repair" here would mean writing a frozen roster from a
    copy of it — and the roster is the campaign's only control group.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision
    from core.pg_sms import sms_store_is_postgres

    if not pg_landing.enabled() or sms_store_is_postgres():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    watermarks = await fetch_watermarks(pool)

    # Only what the source actually has. `sms_dlr_events` arrives with the
    # TurboSMS signature fix and is not in every checkout yet; comparing a
    # table one side has never had reports the merge schedule rather than the
    # data. See `core.pg_sms.source_tables`.
    from core.pg_sms import source_tables

    async with store.connection() as conn:
        present = source_tables(conn)
        specs = tuple(s for s in SMS_TABLES if s.dk_table in present)
        dk_side = read_duckdb_side(conn, specs)

    issues: List[IntegrityIssue] = []
    for spec in specs:
        dk_rows, dk_synced = dk_side[spec.pg_table]
        pg_rows = await fetch_pg_rows(pool, spec)
        issues += compare_table(
            spec, dk_rows, dk_synced, pg_rows, watermarks.get(spec.pg_table),
            now=now, grace_minutes=grace_minutes, max_samples=max_samples,
        )
    return issues


# ─── the order-version archive: not a comparison, a liveness check ───────────
#
# Every other check in this file asks whether two stores agree. This one cannot:
# `app.order_versions` has no counterpart anywhere, which is the entire reason
# it exists. KeyCRM serves current state and has no history, DuckDB never held
# one, and a transition that was not written when it happened is not
# recoverable from either.
#
# So the question changes. Not "do the copies match" but "is it still writing" —
# and that question is genuinely hard to answer by looking at the table, because
# a healthy archive is *supposed* to be quiet. At ~100 rows a day with long gaps
# between them, a writer that died looks exactly like a morning when nothing
# moved.
#
# What breaks the tie is that a brand-new order can never be silent: it has no
# previous version, so the comparison in `core/pg_order_versions.py` always
# writes one. Production creates ~48 orders a day, measured over 31 days. A
# whole day with no version at all therefore is not a quiet day, it is a broken
# writer.
#
# Three findings, and each fires only on a condition that is actually wrong. In
# particular there is no daily INFO reciting the row count: a finding that
# arrives every morning for a permanent reason teaches its reader to skip the
# message it arrives in, which is how the August incident stayed invisible for a
# month.

ORDER_VERSIONS_TABLE = "app.order_versions"

# A day with no new version at all. Not tunable per environment on purpose: the
# argument for it is the ~48 orders a day, and an installation without those has
# nothing for this check to say.
ORDER_VERSIONS_STALL_HOURS = 24

# Ten times the expected daily volume. Above this the content comparison has
# almost certainly stopped discriminating — the 05:15 status refresh alone
# offers ~1,400 ids every morning, and if those are landing wholesale then this
# table is becoming `bronze_order_events`, which is the one failure mode the
# design is built to avoid.
ORDER_VERSIONS_FLOOD_PER_DAY = 1000

# Rendered from `core.pg_order_versions.NOT_THE_WRITER`, which is where the
# argument for each excluded kind is written down. Named, rather than inline,
# so a test can assert on the statement instead of on the source text around
# it: the comments here explain the exclusion and therefore contain the words
# it is made of, and a test grepping this module would pass with the clause
# deleted — the seventh time that trap has come up here.
_NOT_THE_WRITER_SQL = ", ".join(f"'{k}'" for k in _versions.NOT_THE_WRITER)

ORDER_VERSIONS_RECENT_SQL = (
    f"SELECT count(*) FROM {ORDER_VERSIONS_TABLE} "
    f"WHERE captured_at >= $1 AND kind NOT IN ({_NOT_THE_WRITER_SQL})"
)

# The baseline is deliberately *not* excluded here — see the two paragraphs in
# `reconcile_order_versions`. The backfill is, and the two are not the same
# case: a baseline exists only in the hours after the migration and its whole
# job there is to give a fresh archive 24 h of quiet, while a backfill is run
# by a human on a system that has been writing for months. Counting one as
# proof the writer is alive would hide a dead sync for a day after somebody
# repaired a comment, which is the one moment nobody is watching the sync.
ORDER_VERSIONS_NEWEST_SQL = (
    f"SELECT max(captured_at) FROM {ORDER_VERSIONS_TABLE} "
    f"WHERE kind <> '{_versions.BACKFILL}'"
)


async def reconcile_order_versions(
    *,
    now: Optional[datetime] = None,
    stall_hours: int = ORDER_VERSIONS_STALL_HOURS,
    flood_per_day: int = ORDER_VERSIONS_FLOOD_PER_DAY,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Is the archive still being written, does it cover the catalogue, and is
    it writing too much?

    Takes no store: there is nothing on the DuckDB side to take. Reports only,
    like everything else here — and more absolutely than anything else here,
    because a "repair" would mean inventing the history the table exists to be
    the only record of.

    **It does not stand down when a write chain owns the order tables**
    (DN-22b). The stand-down stops the sync's mirror, not the capture: the
    chain's writer ships through `write_orders`, which captures in the same
    transaction, so a day with no version is still a broken writer — and this
    is the one liveness check on the one table nothing can rebuild, at the
    moment its writer changes hands.
    """
    from core import pg_landing
    from core.pg import get_pool, require_revision

    if not pg_landing.enabled():
        return []

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()

    issues: List[IntegrityIssue] = []

    async with pool.acquire() as conn:
        total = await conn.fetchval(
            f"SELECT count(*) FROM {ORDER_VERSIONS_TABLE}"
        )
        newest = await conn.fetchval(ORDER_VERSIONS_NEWEST_SQL)
        # The baseline exclusion in the next statement is load-bearing, and it
        # was found by running this against production rather than by reading
        # it. Revision 0010 seeds one row per order inside the migration, all
        # stamped `now()`, so on the day of any deploy that count is the whole
        # catalogue — 46,695 rows, forty-six times the threshold. Counting the
        # seed as the writer's output files a WARN on every install for its
        # first 24 hours, and a finding that arrives for a permanent reason is
        # how a reader learns to skip the message it arrives in.
        #
        # `newest` above deliberately does *not* exclude it: there the baseline
        # is the right answer, because it gives a fresh archive its first 24
        # hours before anyone is asked why it is quiet. It does exclude the
        # backfill, which has no such grace to grant — see the constant.
        #
        # The backfill exclusion here is *not* the baseline's arithmetic, and
        # OD-20 asked for that to be measured rather than assumed. Production,
        # 2026-09-17: **0** orders where Postgres holds a NULL
        # `manager_comment` and DuckDB holds one, and 10,192 NULL comments in
        # DuckDB across the backfills' 730-day window, of which **26** are
        # website orders — the only ones a KeyCRM re-fetch could fill. One run
        # today is ~26 rows against this threshold of 1,000. The exclusion is
        # about what the count means: a repair somebody deliberately started
        # is not the content comparison having stopped discriminating, which
        # is the only thing this finding can honestly name. See
        # `core.pg_order_versions.NOT_THE_WRITER` for the whole argument.
        recent = await conn.fetchval(
            ORDER_VERSIONS_RECENT_SQL, now - timedelta(hours=24),
        )
        # Orders the archive does not cover at all. Should be impossible: the
        # baseline in revision 0010 seeded every row `bronze.orders` held, and
        # every row written since goes through `write_orders`, which captures
        # in the same transaction. That is exactly why it is worth asking — an
        # invariant with no known way to break is the one whose breach nobody
        # would otherwise notice.
        uncovered = await conn.fetch(
            f"SELECT o.id FROM bronze.orders o "
            f"WHERE NOT EXISTS (SELECT 1 FROM {ORDER_VERSIONS_TABLE} v "
            f"                  WHERE v.order_id = o.id) "
            f"ORDER BY o.id LIMIT $1",
            max_samples + 1,
        )
        uncovered_total = 0
        if uncovered:
            uncovered_total = await conn.fetchval(
                f"SELECT count(*) FROM bronze.orders o "
                f"WHERE NOT EXISTS (SELECT 1 FROM {ORDER_VERSIONS_TABLE} v "
                f"                  WHERE v.order_id = o.id)"
            )

    stale_after = now - timedelta(hours=stall_hours)
    if total == 0:
        issues.append(IntegrityIssue(
            check_name="order_versions_empty",
            table_name=ORDER_VERSIONS_TABLE,
            severity=Severity.CRITICAL,
            count=0,
            description=(
                "The order-version archive is empty. Revision 0010 seeds a "
                "baseline row for every order in bronze.orders as part of the "
                "migration itself, so an empty table means either the "
                "migration did not run its seed or the table has been emptied "
                "since. Nothing re-derives this: KeyCRM has no history."
            ),
        ))
    elif newest is None or newest < stale_after:
        age = "never" if newest is None else newest.isoformat()
        issues.append(IntegrityIssue(
            check_name="order_versions_stalled",
            table_name=ORDER_VERSIONS_TABLE,
            severity=Severity.CRITICAL,
            count=0,
            description=(
                f"No order version has been written since {age}, more than "
                f"{stall_hours}h ago. This is not a quiet day: production "
                f"creates ~48 orders a day and a new order has no previous "
                f"version to match, so it always writes one. Every transition "
                f"since then is lost and cannot be recovered — KeyCRM serves "
                f"current state only. Check the mirror: the capture runs "
                f"inside write_orders, so a failing mirror stops the archive."
            ),
        ))

    if recent is not None and recent > flood_per_day:
        issues.append(IntegrityIssue(
            check_name="order_versions_flooding",
            table_name=ORDER_VERSIONS_TABLE,
            severity=Severity.WARN,
            count=recent,
            description=(
                f"{recent} versions written in the last 24h, against an "
                f"expected ~100 and a threshold of {flood_per_day}. The "
                f"content comparison has probably stopped discriminating: the "
                f"05:15 status refresh offers ~1,400 ids every morning with "
                f"force_update=True, and if those are landing wholesale then "
                f"this table is recording observations rather than changes — "
                f"which is what took bronze_order_events to 43 GB."
            ),
        ))

    if uncovered_total:
        issues.append(IntegrityIssue(
            check_name="order_versions_missing",
            table_name=ORDER_VERSIONS_TABLE,
            severity=Severity.CRITICAL,
            count=uncovered_total,
            sample_ids=tuple(int(r[0]) for r in uncovered[:max_samples]),
            description=(
                f"{uncovered_total} order(s) in bronze.orders have no version "
                f"at all. Every order written since revision 0010 is captured "
                f"in the same transaction as its header, and the migration "
                f"seeded a baseline for everything that predated it, so this "
                f"should not be reachable. Something is writing bronze.orders "
                f"without going through write_orders."
            ),
        ))

    return issues


# ─── The buyer landing: delta-fed like orders, small enough to read whole ────
#
# `bronze.buyers` is one parsed Buyer batch handed to two stores in the same
# call (`core/pg_buyers.py`) — landing in the exact sense of the catalogue.
# But it is *fed* like orders: the sync only ever fetches buyers missing from
# the table, so there is no catalogue re-ship to license the retired category.
# DuckDB never deletes a buyer, and after `backfill_buyers` has carried
# history across, a row missing from Postgres has exactly one meaning: lost.
# That is `full_replace=True` used for its semantics, stated out loud.

_BUYERS_ORIGIN = (
    "Both sides are written from the same parsed Buyer batch in the same "
    "call, and DuckDB never deletes a buyer — after the backfill, a row "
    "missing here was lost by the mirror, not retired by KeyCRM."
)

def _buyers_spec():
    from core.pg_buyers import BUYER_COLUMNS

    return MirroredTable(
        pg_table="bronze.buyers",
        dk_table="buyers",
        columns=BUYER_COLUMNS,
        numeric=("loyalty_discount", "loyalty_amount"),
        key_columns=("id",),
        synced_column="synced_at",
        origin_note=_BUYERS_ORIGIN,
        full_replace=True,
    )


def _contacts_spec():
    from core.pg_buyers import CONTACT_COLUMNS

    return MirroredTable(
        pg_table="bronze.buyer_contacts",
        dk_table="buyer_contacts",
        columns=CONTACT_COLUMNS,
        key_columns=("buyer_id", "contact_type", "value"),
        # No timestamp of its own; the reader below joins the owning buyer's
        # `synced_at`, because contacts ship in the same transaction as their
        # buyer and are in flight exactly when the buyer is.
        synced_column=None,
        origin_note=_BUYERS_ORIGIN,
        full_replace=True,
    )


async def reconcile_buyers(
    store,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = MIRROR_GRACE_MINUTES,
    max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Compare the buyer landing, row by row, tolerance zero.

    Gated on `backfilled_at` like orders and for the same reason: the mirror
    ships deltas, so until history has crossed, every buyer older than the
    mirror looks exactly like a lost one. One `mirror_backfill_pending`
    instead of twenty thousand CRITICALs.
    """
    from core import chain_latch, pg_landing
    from core.pg import get_pool, require_revision
    from core.pg_buyers import BUYER_UNIT, BUYERS_STATE, CONTACT_COLUMNS

    if not pg_landing.enabled():
        return []

    # Stood down with `mirror_buyers` once a write chain owns the buyers or
    # their contacts (DN-22b), both together — `reconcile_mirror`'s
    # arrangement: the local answer before either store is read, the owner
    # rows after `require_revision()`.
    issues, held = _landing_stood_down(BUYER_UNIT)
    if held:
        return issues

    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    issues, held = _landing_stood_down(BUYER_UNIT, await chain_latch.read_owners(pool))
    if held:
        return issues
    watermarks = await fetch_watermarks(pool)

    buyers_spec = _buyers_spec()
    contacts_spec = _contacts_spec()

    wm = watermarks.get(BUYERS_STATE)
    if wm and wm.get("last_ok_at") and not wm.get("backfilled_at"):
        return [IntegrityIssue(
            check_name="mirror_backfill_pending",
            table_name=BUYERS_STATE,
            severity=Severity.INFO,
            count=1,
            description=(
                "bronze.buyers is receiving deltas but history has not been "
                "backfilled; row-level comparison is suppressed until "
                "backfill_buyers stamps backfilled_at, or every historical "
                "buyer would read as lost."
            ),
        )]

    async with store.connection() as conn:
        dk_buyers, dk_buyers_synced = fetch_duckdb_rows(conn, buyers_spec)
        # Contacts, with the owning buyer's clock attached.
        contact_rows = conn.execute(
            """
            SELECT c.buyer_id, c.contact_type, c.value, c.is_primary, b.synced_at
            FROM buyer_contacts c
            JOIN buyers b ON b.id = c.buyer_id
            """
        ).fetchall()

    dk_contacts: Dict[Any, Tuple[Any, ...]] = {}
    dk_contacts_synced: Dict[Any, Optional[datetime]] = {}
    for row in contact_rows:
        key = (row[0], row[1], row[2])
        dk_contacts[key] = _normalise_row(
            tuple(row[:4]), contacts_spec.columns, contacts_spec.numeric
        )
        dk_contacts_synced[key] = row[4]

    issues: List[IntegrityIssue] = []
    pg_buyers = await fetch_pg_rows(pool, buyers_spec)
    issues += compare_table(
        buyers_spec, dk_buyers, dk_buyers_synced, pg_buyers,
        watermarks.get(buyers_spec.pg_table),
        now=now, grace_minutes=grace_minutes, max_samples=max_samples,
    )
    pg_contacts = await fetch_pg_rows(pool, contacts_spec)
    issues += compare_table(
        contacts_spec, dk_contacts, dk_contacts_synced, pg_contacts,
        watermarks.get(contacts_spec.pg_table),
        now=now, grace_minutes=grace_minutes, max_samples=max_samples,
    )

    # The hourly ids-diff heals a lost buyer before this comparison can see
    # it — which is the point, and also the review's objection: a report-only
    # check that only ever measures the already-repaired is measuring
    # nothing. So a heal leaves a trace here. Process-local by design: the
    # healer and this check share a process, and a restart between them costs
    # one finding (the WARNING log line survives).
    from core.pg_buyers import last_heal

    if last_heal and (now - last_heal["at"]).total_seconds() < 24 * 3600:
        issues.append(IntegrityIssue(
            check_name="mirror_selfhealed_rows",
            table_name="bronze.buyers",
            severity=Severity.INFO,
            count=int(last_heal["shipped"]),
            description=(
                f"the hourly ids-diff re-shipped {last_heal['shipped']} "
                f"buyer(s) at {last_heal['at'].isoformat()} — rows the mirror "
                f"had lost and this comparison would otherwise never have "
                f"seen. One heal is housekeeping; a heal every day is a "
                f"leak wearing a bandage."
            ),
        ))
    return issues
