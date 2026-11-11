"""The checks of the `mirror_landing` layer (daily, 07:30 Kyiv).

Until step 14's S14-12 this was Reconciliation A — Postgres set against DuckDB,
table by table, at a tolerance of zero: the landing mirror, the orders by
bucketed fingerprint, Silver, the UTM verdicts, Gold through the channel
mapping, the order-level expenses, the replicated operational tables, the
buyers, the SMS tables and the dashboard's users. Every chain writes Postgres
since S14-00, so there was no second copy left to compare, and S14-12 deleted
those comparisons (the plan's appendix A names what replaced each).

What stays asks Postgres about itself, or Postgres against the one other store
that still has a copy:

* `reconcile_order_utm_completeness` — every order the UTM parser reads has a
  current verdict (DN-16);
* `pg_gold_internal_check` — Gold's per-source rows add up to its roll-up;
* `reconcile_buyer_completeness` — every landed buyer has a verdict, and every
  buyer an order names exists;
* `reconcile_order_versions` — the order archive is still being written;
* `reconcile_bot_state` — `data/bot.db` against its copy in `app.*`, while the
  bot still writes SQLite.

The job adds the ClickHouse comparisons (`core.ch_silver`,
`core.ch_history`). Report only: nothing here repairs anything. (It added
`reconcile_customer_profile` too, until the owner's decision O6 of
2026-11-11 stopped rebuilding and checking `app.customer_profile`.)

The catalogue specs and `fetch_duckdb_rows` stayed for one more reader,
`core.pg_landing.carry_retired_catalogue`, until step 14's S14-13 deleted it
and them.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import (
    Any, Dict, List, Mapping, Optional, Sequence, Tuple,
)

from core.data_quality import IntegrityIssue, Severity
from core.pg_bot_state import (
    AUTHORIZED_COLUMNS,
    MILESTONE_COLUMNS,
    PREFERENCE_COLUMNS,
    REPORT_HISTORY_COLUMNS,
)
from core import pg_order_versions as _versions

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


# What a disagreement means for a table that is *copied* rather than mirrored,
# so that a finding does not send its reader hunting a shared write path that
# does not exist.
_COPIED_FROM_SQLITE = (
    "This table is copied out of data/bot.db through a type conversion "
    "(SQLite has no boolean and no timezone), so a disagreement is either the "
    "copy not having run or the conversion — check the column named above "
    "against core/pg_bot_state._convert before anything else."
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


# ─── Gold: the two grains of one Postgres table, against each other ─────────
#
# `gold.daily_revenue` holds both grains: a row per source and a roll-up
# (`source_id IS NULL`). The roll-up is stored, not derived, because three of
# its measures are distinct counts and adding the per-source rows up
# overstates them — one buyer who used two channels in a day. What can be
# asked is whether the four additive measures of the per-source rows add up to
# the roll-up, and that is the only check that sees the rows of sources 3 and
# 5 at all. Until step 14's S14-12 DuckDB's Gold was also set against this
# table through the channel mapping (`reconcile_gold`); ClickHouse's own
# derivation is the independent check of the values now (`core.ch_silver`).

GOLD_PG_TABLE = "gold.daily_revenue"
_GOLD_ROLLUP_MEASURES: Tuple[str, ...] = (
    "revenue", "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count", "returns_revenue",
    "avg_order_value",
)

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


async def pg_gold_internal_check(*, max_samples: int = 10) -> List[IntegrityIssue]:
    """`gold_rollup_mismatch` over Postgres alone — no DuckDB read (DN-28).

    Asked by `dq_mirror_landing` on every run. Until step 13 it rode
    `compare_gold`, behind a read of DuckDB's Gold; from step 13 it was asked
    in `reconcile_gold`'s place while the DuckDB comparisons stood down, and
    since step 14's S14-12, which deleted that comparison, unconditionally —
    one verdict a run, under the same condition name, so a page carries over.

    Not gated on the landing mirror: whether the grains of one table agree does
    not depend on what feeds it. An empty table finds nothing, and saying Gold
    was never built is the derivation's own job and the canary's.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    pg_rollup, pg_fine = await fetch_pg_gold(pool)
    return _gold_internal_findings(pg_rollup, pg_fine, max_samples=max_samples)


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
    for (day, sales_type, _source), row in fine.items():
        acc = sums.setdefault((day, sales_type), {c: Decimal(0) for c in _GOLD_ADDITIVE})
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



# How far behind an hourly copy into Postgres may legitimately be: the
# interval plus half of it again. `data/bot.db`'s copy runs inside the hourly
# `replicate_operational`, so its grace is this (`BOT_DB_GRACE_MINUTES`). Raise
# the job's interval and this must move with it, or the check starts reporting
# the schedule instead of the data. The replicated operational tables were
# compared against DuckDB at the same grace until step 14's S14-12.
OPERATIONAL_GRACE_MINUTES = 90


# ─── The UTM classification's completeness: Postgres alone (DN-16) ───────────
#
# `reconcile_order_utm` asked whether Postgres held the verdicts DuckDB held,
# until step 14's S14-12 deleted it. That was never the question /traffic
# depends on, and it stopped meaning anything at step 9: once the parse runs
# inside Postgres there is no DuckDB copy to agree with. What
# the tab needs is that every order the parser reads has a verdict, and a
# current one — because an order with no row does not read as missing. It falls
# through the COALESCE to organic or unattributed, on the chart and in the
# Monday traffic report, with nothing on either to say so.
#
# **The population is the parser's own predicate**, `INCREMENTAL_SELECT_SQL`
# in core/pg_utm_parse.py: an order in `bronze.orders` whose `manager_comment`
# is neither NULL nor empty, with no row or with `updated_at > parsed_at`.
# `parsed_at` is the order's `updated_at` at the moment its comment was read,
# not the wall clock, so a finished parse leaves the two equal and a later edit
# compares greater. An order with no comment never gets a row, by design, and
# is not asked about.
#
# **Read from `bronze.orders`, the table the parser reads.** The parse is
# `core/pg_utm_parse.py`, the derivation's last step, so a missing or stale
# verdict has one cause left: a parse that has not run since the order landed,
# or did not finish. Until step 14's S14-15 the parser read DuckDB's `orders`,
# and this check read `bronze.orders` as the only copy of that input Postgres
# held. The two normally held the same comment, but not always — DuckDB kept a
# stored comment over a NULL payload only on its UPDATE path, so an order it
# INSERTed afresh (after `POST /api/duckdb/purge-orders`, retired by OD-10 on
# 2026-09-30, or a restore from an export older than a backfill) could hold
# NULL there and its comment here. No parse could clear such a finding, and its
# lever was putting the comment back in DuckDB. That cause went with DuckDB's
# parse, and the lever with it (`_ORDER_UTM_LEVER_REASONS` below says so).
#
# **The grace is absolute, on `bronze.orders.mirrored_at`.** An order and its
# verdict arrive in the same derivation: Postgres derives on its own signal,
# no sooner than the floor (`KS_PG_SILVER_INTERVAL_S`, 600 s by default) after
# its last attempt, and the parse is that derivation's last step
# (`parse_incremental_locked`, run once the derivation has let
# `PG_LAYER_LOCK` go). So the grace is resolved from that variable when the
# check runs, through `pg_warehouse_dq.silver_grace_minutes()`: the floor plus
# the twins' ten-minute margin, twenty minutes at the default. A constant here
# would go on paging the old floor's width the day somebody raised it, and the
# floor and the grace would then be two numbers somebody has to remember to
# move together. (Until step 14's S14-15 the verdict waited for DuckDB's next
# warehouse tick and then the ship's floor instead — the gap #213 accepted.)
# `deploy/daily_soak/11_utm_gap.sql` asks the same question by hand with
# fifteen minutes: right for a reading, and a page on a slow tick here.
#
# **`mirrored_at` is when the row was last written, not when the order
# arrived**, and that is the grace's blind spot. The orders upsert
# (`pg_landing._write_order_rows`, which chain 3 runs) stamps it on every
# write, identical rewrites included: the 05:15 status refresh force-writes
# ~1,400 unchanged orders, and the comment backfills re-stamp every order they
# fill in. A verdict missing for days therefore reads as in flight — INFO,
# counted, not paged — for one grace after such a rewrite. Not changed here:
# the stamp belongs to the orders writer, and other checks read it too. The
# scheduled run does not meet the 05:15 case, since 07:30 is two hours past
# that window; a backfill run by hand in the grace before 07:30 would hide the
# orders it re-stamps for that one morning, as INFO.
#
# **Not the parse's watermark** (`meta.mirror_state`, `silver.order_utm`),
# which is the other clock available. A parse that stops being *called*
# records no failure — a tick that cannot read the derivation's signal returns
# before it, a scheduler that has stopped runs nothing — so
# `failures_since_ok` stays at zero, only `last_ok_at` ages, and an age names
# no order. (The parse runs after the derivation whatever the derivation did,
# so a Gold fault does not stop it.) The watermark is also the wrong
# shape: the incremental parse stamps it on every success, zero rows included,
# so "the parser ran since the order arrived" does not mean "it read that
# order". (Until step 14's S14-15 it was the ship's watermark, the copy out of
# DuckDB, and the same two objections held.)
#
# **Inside the grace is INFO, counted, never nothing.** The plan asked for the
# width of the two-clocks gap to be measured before step 9's flip, and a daily
# count of orders caught in flight at 07:30 is that measurement; INFO rides the
# digest and never summons it, so a busy morning costs a line and not a page.
#
# Report only, like everything in this module: no finding here repairs
# anything or reaches `validation_passed`. The lever is the parse, and a check
# that re-parsed what it found missing would hide that the parse broke. It
# reads Postgres alone and takes no store.

# The table the verdicts live in. Until step 14's S14-12 the name came off the
# daily comparison's spec (`ORDER_UTM_TABLE`), deleted with that comparison.
ORDER_UTM_PG_TABLE = "silver.order_utm"

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
# The parser reads bronze.orders, so the only cause left is a parse that has
# not run or did not finish, and both refresh routes run it. Until step 14's
# S14-15 a second text stood here for `KS_UTM_PARSE=duckdb` — copy the comment
# into DuckDB, then the ship — and this one was `_ORDER_UTM_LEVER_REASONS_POSTGRES`;
# the words of the findings are what production published under `postgres`.
_ORDER_UTM_LEVER_REASONS = (
    "The alert's levers, in order. Read {table}'s row in meta.mirror_state "
    "first: the parse records every failure there before it raises "
    "(PG_LAYER_LOCK not free within LOCK_WAIT_S, a lock_timeout on its "
    "advisory key, a schema revision mismatch, a comment the classifier "
    "raised on) and every refused full parse, and its next successful run "
    "clears it. Then POST /api/traffic/refresh, which runs the incremental "
    "parse whether or not anything is marked dirty, or POST "
    "/api/warehouse/refresh, whose derivation ends with the same parse under "
    "KS_PG_DERIVE=own. Copying a comment into DuckDB, the lever under "
    "KS_UTM_PARSE=duckdb, moves nothing here: this parser does not read "
    "DuckDB."
)


def order_utm_completeness_findings(
    row: Mapping[str, Any], *, grace_minutes: int,
) -> List[IntegrityIssue]:
    """The findings one aggregate row of `ORDER_UTM_COMPLETENESS_SQL` makes.

    Pure, so what each count means is testable without a server; the SQL that
    produces the counts is tested against a real one.

    It took `parsed_in_postgres`, the `KS_UTM_PARSE` mode, until step 14's
    S14-15: the words differed by mode and the counts never did. These are
    the words production published under `postgres`, byte for byte.
    """
    reasons = _ORDER_UTM_LEVER_REASONS
    issues: List[IntegrityIssue] = []
    table = ORDER_UTM_PG_TABLE

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
                "unattributed, with nothing on screen to say so. "
                + ("Under KS_UTM_PARSE=postgres the parser reads bronze.orders "
                   "itself, so the parse has not reached them: its last run "
                   "failed, or none has run since they landed. ")
                + reasons.format(table=table)
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
                "own updated_at when its comment was read, so "
                + ("the parse has not caught up with an edit: under "
                   "KS_UTM_PARSE=postgres it reads bronze.orders itself, so "
                   "its last run failed or none has run since the edit "
                   "landed. ")
                + "/traffic shows the previous classification for these. "
                + reasons.format(table=table)
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
                f"{table}. Not a defect yet: "
                + ("under KS_UTM_PARSE=postgres the verdict follows on the "
                   "next derivation, whose last step parses bronze.orders, ")
                + "and /traffic "
                "shows these without their current verdict until it lands. "
                "The clock is the row's last write, so a rewrite of an "
                "unchanged order (the 05:15 refresh, a backfill) puts one long "
                "without a verdict here for one grace too. "
                + ("Counted because that width is how far the parse inside "
                   "Postgres trails the orders it reads.")
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
    let it outlive `reconcile_order_utm` (deleted in step 14's S14-12) once the
    parse moved there.

    `grace_minutes=None` — what the job passes — is the derivation's floor
    plus the twins' margin, read from `KS_PG_SILVER_INTERVAL_S` now rather
    than at import, so the two move together. A floor that is not a number raises
    here, and the job's `check` names the error on the run.
    """
    from core.pg import get_pool, require_revision
    from core.pg_warehouse_dq import silver_grace_minutes

    if not configured():
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


# ─── Buyer completeness: two questions no comparison can answer ─────────────
#
# `reconcile_buyers` compared DuckDB with Postgres, so it saw a buyer that one
# copy had and the other lacked. It could not see a buyer BOTH lack, and it had
# nothing to compare once chain 4 made Postgres the only writer — step 14's
# S14-12 deleted it. These two read Postgres alone, so they held in every
# state of the chain, and ran with the flag off so that their false-positive
# rate was measured before the flip (chain 4, PR-1, step 4).
#
# - `buyers_without_verdict`: a buyer that landed more than 90 minutes ago with
#   no gender verdict of the current RULES_VERSION. The derivation is an hourly
#   tick, so 90 minutes is one missed tick. A human override counts as a
#   verdict whatever its version: nothing rewrites it, by design.
# - `buyers_missing_for_orders`: a buyer an order names that bronze.buyers does
#   not hold, or holds with no name — exactly what the buyer sync selects —
#   whose earliest order is more than a day old. The sync is hourly and the
#   measured lag from order to buyer has a median of 37 minutes, so a day is
#   not lag. The day is a starting point: this check, run with the flag off, is
#   what says whether it is the right one.
#
# WARN, both: neither loses anything, but both are a writer that stopped, and
# under chain 4 there is only one writer. Ids only in the samples.

BUYER_VERDICT_GRACE_MINUTES = 90
BUYER_MISSING_AFTER_HOURS = 24

BUYER_COMPLETENESS_SQL = """
WITH no_verdict AS (
    SELECT b.id, b.mirrored_at
    FROM bronze.buyers b
    LEFT JOIN app.buyer_gender g ON g.buyer_id = b.id
    WHERE b.mirrored_at < $1
      AND (g.buyer_id IS NULL
           OR (NOT g.override_by_human AND g.rules_version < $2))
),
missing AS (
    SELECT o.buyer_id AS id, min(o.ordered_at) AS first_order_at
    FROM silver.orders o
    LEFT JOIN bronze.buyers b ON b.id = o.buyer_id
    WHERE o.buyer_id IS NOT NULL
      AND (b.id IS NULL OR b.full_name IS NULL OR b.full_name = '')
    GROUP BY o.buyer_id
    HAVING min(o.ordered_at) < $3
)
SELECT
    (SELECT count(*) FROM no_verdict)                             AS without_verdict,
    (SELECT (array_agg(id ORDER BY id))[1:$4] FROM no_verdict)    AS without_verdict_ids,
    (SELECT min(mirrored_at) FROM no_verdict)                     AS without_verdict_since,
    (SELECT count(*) FROM missing)                                AS missing,
    (SELECT (array_agg(id ORDER BY id))[1:$4] FROM missing)       AS missing_ids,
    (SELECT min(first_order_at) FROM missing)                     AS missing_since
"""


def buyer_completeness_findings(
    row: Mapping[str, Any], *, verdict_grace_minutes: int = BUYER_VERDICT_GRACE_MINUTES,
    missing_after_hours: int = BUYER_MISSING_AFTER_HOURS,
) -> List[IntegrityIssue]:
    """The findings one aggregate row of `BUYER_COMPLETENESS_SQL` makes. Pure."""
    issues: List[IntegrityIssue] = []

    def _ids(key: str) -> Tuple[int, ...]:
        return tuple(int(i) for i in (row[key] or ()))

    def _since(key: str) -> str:
        value = row[key]
        return value.isoformat() if isinstance(value, datetime) else "unknown"

    without = int(row["without_verdict"] or 0)
    if without:
        issues.append(IntegrityIssue(
            check_name="buyers_without_verdict",
            table_name="app.buyer_gender",
            severity=Severity.WARN,
            count=without,
            sample_ids=_ids("without_verdict_ids"),
            description=(
                f"{without} buyer(s) landed in bronze.buyers more than "
                f"{verdict_grace_minutes} minutes ago (the oldest at "
                f"{_since('without_verdict_since')}) with no gender verdict of "
                "the current rules. The derivation is an hourly tick, so at "
                "least one tick has passed them by. They are in no gendered "
                "SMS audience — `gender IN (...)` never matches NULL — and "
                "nothing on the page says so. The derivation never fails its "
                "job — it logs 'gender derivation failed' in web's log and "
                "/api/jobs shows nothing — and a failed ship to Postgres is "
                "stamped in meta.mirror_state for app.buyer_gender "
                "(last_error, failures_since_ok). Under chain 4 "
                "(KS_WRITE_BUYERS) the verdicts are written in Postgres "
                "directly, nothing ships them, and nothing is stamped there: "
                "its log line reads 'gender derivation failed (postgres)'. A "
                "RULES_VERSION bump deployed shortly before this ran explains "
                "a large count by itself."
            ),
        ))

    missing = int(row["missing"] or 0)
    if missing:
        issues.append(IntegrityIssue(
            check_name="buyers_missing_for_orders",
            table_name="bronze.buyers",
            severity=Severity.WARN,
            count=missing,
            sample_ids=_ids("missing_ids"),
            description=(
                f"{missing} buyer(s) are named by orders in silver.orders and "
                "absent from bronze.buyers, or present with no name — what the "
                "hourly buyer sync selects — with their first order more than "
                f"{missing_after_hours} hours old (the oldest at "
                f"{_since('missing_since')}). The sync is not reaching them: "
                "read `buyer_sync` in /api/health for the step's own state. "
                "An id KeyCRM will not serve stays here until it does."
            ),
        ))
    return issues


async def buyer_completeness_row(
    conn, *, now: datetime, rules_version: int, max_samples: int = 10,
    verdict_grace_minutes: int = BUYER_VERDICT_GRACE_MINUTES,
    missing_after_hours: int = BUYER_MISSING_AFTER_HOURS,
) -> Mapping[str, Any]:
    """One aggregate row, on a connection the caller holds — so a test can seed
    a scenario inside a transaction it rolls back: the query reads the whole of
    bronze.buyers and silver.orders."""
    return await conn.fetchrow(
        BUYER_COMPLETENESS_SQL,
        now - timedelta(minutes=int(verdict_grace_minutes)),
        int(rules_version),
        now - timedelta(hours=int(missing_after_hours)),
        int(max_samples),
    )


async def reconcile_buyer_completeness(
    *, now: Optional[datetime] = None, max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Does every landed buyer have a current verdict, and does every buyer an
    order names exist? Postgres alone; reports only; no store.

    Asked whenever there is a Postgres (`configured()`, `KS_PG_DSN`) since
    step 14's S14-13; until then while the landing mirror was on
    (`pg_landing.enabled()`, a constant True since S14-00). Until S14-11 it
    was also asked
    whenever chain 4 wrote the buyers in Postgres (`pg_buyers_write.mode()`),
    the one Postgres-side check that the chain's writer is alive, which must
    not switch off with a mirror flag the chain does not consult (the plan
    critic's b2); the chain writes Postgres, a constant, so the gate is the
    mirror's alone."""
    from core.gender import RULES_VERSION
    from core.pg import get_pool, require_revision

    if not configured():
        return []
    now = now or datetime.now(timezone.utc)
    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        row = await buyer_completeness_row(
            conn, now=now, rules_version=RULES_VERSION, max_samples=max_samples)
    return buyer_completeness_findings(row)


# ─── The bot's own state: the third store, and the one with no backup ────────
#
# This compares **SQLite** against Postgres, because `data/bot.db` is the third store in this system and
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

    from core.bot_prefs import BOT_DB_PATH
    from core.bot_store import ENGINE_ENV
    from core.pg import get_pool, require_revision
    from core.pg_bot_state import BUSY_TIMEOUT_SECONDS

    if not configured():
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
    (DN-22b). The stand-down stopped the sync's mirror (which step 14's S14-09
    deleted with DuckDB's order writer), never the capture: the chain's writer
    ships through `write_orders`' core, which captures in the same
    transaction, so a day with no version is still a broken writer — and this
    is the one liveness check on the one table nothing can rebuild, at the
    moment its writer changes hands.
    """
    from core.pg import get_pool, require_revision

    if not configured():
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

