"""Data Quality framework — multi-layered.

Layered design (so each layer can be tested, deployed, and reasoned about
independently):

    Layer 1 — Internal integrity        (pure DB scans, cheap, 6h)
    Layer 2 — Source reconciliation     (vs KeyCRM API, daily)
    Layer 3 — Statistical anomaly       (rolling baseline, daily) — future
    Layer 4 — Surface (health endpoint, Telegram digest)

This module owns the **pure functions** of Layers 1 and 2 — the parts that
do not perform I/O. They are heavily unit-tested. The scheduler is the
**orchestrator** that performs I/O (DuckDB reads, KeyCRM API calls,
Telegram alerts) by calling these pure functions.

Vocabulary
----------
- **Issue**: a Layer-1 integrity violation (orphan FK, duplicate PK).
- **Discrepancy**: a Layer-2 mismatch between DuckDB and KeyCRM.
- **Severity**: CRITICAL / WARN / INFO. CRITICAL pages admins; WARN goes
  into the morning digest; INFO is logged only.
"""
from __future__ import annotations

import html as html_module
import importlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import (
    Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Set, Tuple,
)

from core.rollback_lever import ROLLBACK_LEVER

logger = logging.getLogger(__name__)


# ─── Severity & classes ───────────────────────────────────────────────────────


class Severity(str, Enum):
    """Three-tier severity for both Issues and Discrepancies."""
    CRITICAL = "CRITICAL"  # paged immediately
    WARN = "WARN"          # collected into daily digest
    INFO = "INFO"          # logged only

    def rank(self) -> int:
        return {"INFO": 0, "WARN": 1, "CRITICAL": 2}[self.value]


class DiscrepancyClass(str, Enum):
    """How a (month, source) cell differs between DuckDB and KeyCRM.

    The class drives both severity and the runbook for resolving it.
    """
    # Order is in KeyCRM but missing from DuckDB. Sync gap.
    MISSING_IN_DK = "MISSING_IN_DK"
    # Order is in DuckDB but not in KeyCRM. Ghost; needs triage.
    MISSING_IN_KC = "MISSING_IN_KC"
    # Same order ID on both sides, different values (status, revenue, qty).
    VALUE_MISMATCH = "VALUE_MISMATCH"
    # Specifically: KeyCRM marks order as returned, DuckDB hasn't caught up.
    STATUS_DRIFT = "STATUS_DRIFT"
    # Aggregate (month, source, metric) differs but we don't know which orders.
    # Used when reconciliation runs at rollup-only granularity.
    TOTAL_DRIFT = "TOTAL_DRIFT"


# ─── Data containers ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class IntegrityIssue:
    """A Layer-1 integrity violation."""
    check_name: str          # e.g. "pk_uniqueness_orders"
    table_name: str
    severity: Severity
    count: int               # number of violating rows
    sample_ids: Tuple[int, ...] = ()  # up to 10 example IDs for triage
    description: str = ""
    # The page conditions a finding says this run did not re-examine, for
    # `unverified_conditions` to hold rather than resolve. Set only where the
    # finding itself is what knows them (`sync_watermarks_unwatched`, whose
    # entities depend on what the run could read); never persisted — the
    # journal writes the six fields above.
    unverified: Tuple[str, ...] = ()


@dataclass(frozen=True)
class Discrepancy:
    """A Layer-2 reconciliation discrepancy at (month, source, metric)."""
    month: str               # 'YYYY-MM'
    source_id: int
    diff_class: DiscrepancyClass
    field: str               # 'orders' | 'qty' | 'revenue' | 'returns_count' | 'returns_revenue'
    dk_value: float
    kc_value: float
    severity: Severity = Severity.WARN
    order_ids: Tuple[int, ...] = ()  # IDs involved (for MISSING_IN_DK etc.)

    @property
    def diff_abs(self) -> float:
        return self.dk_value - self.kc_value

    @property
    def diff_pct(self) -> Optional[float]:
        """Percentage diff relative to KC (the source of truth).
        None when kc_value is 0 (division undefined; absolute diff is the
        only useful signal in that case)."""
        if self.kc_value == 0:
            return None
        return 100.0 * (self.dk_value - self.kc_value) / self.kc_value


# ─── Rollup type (input to classify_discrepancies) ────────────────────────────

# A rollup maps (month_yyyy_mm, source_id) → dict of metrics.
# This is the shape both the DuckDB aggregator and the KeyCRM aggregator must
# produce. Decoupling format from source means the classifier is pure.
Rollup = Dict[Tuple[str, int], Dict[str, float]]


# ─── Severity thresholds (tunable) ────────────────────────────────────────────

# Material discrepancy thresholds. Below these, we don't generate a
# Discrepancy at all — typical case is rounding noise or in-flight order
# updates we couldn't watermark out.
#
# Rules:
#   - (abs, pct) tuple. abs is a floor (UAH or row count); pct is a
#     relative ceiling for large bases.
#   - A diff is material iff diff_abs > abs AND, when pct > 0, also
#     diff_pct > pct. The AND combines a noise-floor with a relative-
#     noise filter: small drifts on huge bases (e.g. ₴500 on ₴1M, 0.05%)
#     are real-world rounding, not signal.
#
# Rationale per field:
#   - orders: abs=0. Any non-zero diff is material. One missed order is
#     the whole point of reconciliation.
#   - qty: abs=1. Line-item recombinations can shift qty by 1 across
#     status updates; allow that noise.
#   - revenue: 100 UAH floor + 0.5% ceiling. Below 100 UAH is rounding;
#     above floor but below 0.5% of KC is noise on large totals.
#   - returns_count: abs=0. Returns are high-impact; one missed return
#     can cost real money downstream.
#   - returns_revenue: 50 UAH floor + 0.5% ceiling.
MATERIAL_THRESHOLDS = {
    "orders": (0, 0.0),
    "qty": (1, 0.0),
    "revenue": (100, 0.5),
    "returns_count": (0, 0.0),
    "returns_revenue": (50, 0.5),
}


# ─── Pure: classify_discrepancies ─────────────────────────────────────────────


def is_material(field_name: str, dk: float, kc: float) -> bool:
    """Return True iff the (dk, kc) diff for this field exceeds the
    material threshold. Below threshold = noise to ignore."""
    if field_name not in MATERIAL_THRESHOLDS:
        raise ValueError(f"unknown field for materiality check: {field_name!r}")
    abs_t, pct_t = MATERIAL_THRESHOLDS[field_name]
    diff_abs = abs(dk - kc)
    if diff_abs <= abs_t:
        return False
    # Beyond abs threshold — but if pct check exists and diff is tiny relative
    # to KC, still ignore (handles cases where KC is huge and we drifted by
    # less than pct_t).
    if pct_t > 0 and kc != 0:
        diff_pct = 100.0 * diff_abs / abs(kc)
        if diff_pct <= pct_t:
            return False
    return True


def _severity_for_field(field_name: str, dk: float, kc: float) -> Severity:
    """Severity for a (field, dk, kc) cell. CRITICAL when we lost
    visibility into orders entirely; WARN for value drift."""
    if field_name in ("orders", "returns_count"):
        # If counts disagree, we are missing or hallucinating orders.
        # Treat as CRITICAL even at small counts because each diff IS
        # a missing/extra order — actionable, not noise.
        return Severity.CRITICAL
    # Revenue/qty diffs without count diffs are typically status changes
    # or in-flight updates. WARN.
    return Severity.WARN


def classify_discrepancies(
    dk: Rollup,
    kc: Rollup,
    *,
    fields: Tuple[str, ...] = ("orders", "qty", "revenue", "returns_count", "returns_revenue"),
) -> List[Discrepancy]:
    """Pure classifier. Compare two rollups, return list of material discrepancies.

    The input rollups must have the same shape. Empty cells are treated as 0s
    (which is correct when a (month, source) has no orders on one side — that
    IS a 0 vs N discrepancy worth reporting).

    Args:
        dk: DuckDB rollup. Keys = (month, source_id). Values = dict[field, value].
        kc: KeyCRM rollup, same shape.
        fields: which metrics to compare. Default: all five.

    Returns:
        List of Discrepancy. Empty list means full agreement (within thresholds).
        Ordered: CRITICAL first, then WARN, then by (month, source_id).
    """
    all_keys = set(dk.keys()) | set(kc.keys())
    discrepancies: List[Discrepancy] = []

    for key in all_keys:
        month, source_id = key
        dk_cell = dk.get(key, {})
        kc_cell = kc.get(key, {})

        # If one side has no row at all, classify as missing-side.
        # When DK has the row but KC does not → orders DK reports that KC
        # never returned. Could be ghost or KeyCRM cache lag. CRITICAL.
        dk_has = bool(dk_cell)
        kc_has = bool(kc_cell)

        if dk_has and not kc_has:
            # All KC fields = 0. Surface as MISSING_IN_KC for orders/returns
            # so the runbook ("investigate ghost orders") is unambiguous.
            for f in fields:
                dv = float(dk_cell.get(f, 0))
                if dv == 0:
                    continue
                if not is_material(f, dv, 0):
                    continue
                discrepancies.append(Discrepancy(
                    month=month, source_id=source_id,
                    diff_class=DiscrepancyClass.MISSING_IN_KC,
                    field=f, dk_value=dv, kc_value=0,
                    severity=Severity.CRITICAL,
                ))
            continue

        if kc_has and not dk_has:
            for f in fields:
                kv = float(kc_cell.get(f, 0))
                if kv == 0:
                    continue
                if not is_material(f, 0, kv):
                    continue
                discrepancies.append(Discrepancy(
                    month=month, source_id=source_id,
                    diff_class=DiscrepancyClass.MISSING_IN_DK,
                    field=f, dk_value=0, kc_value=kv,
                    severity=Severity.CRITICAL,
                ))
            continue

        # Both sides have data. Compare field-by-field.
        for f in fields:
            dv = float(dk_cell.get(f, 0))
            kv = float(kc_cell.get(f, 0))
            if not is_material(f, dv, kv):
                continue
            sev = _severity_for_field(f, dv, kv)
            # If the count diff is non-zero AND it's a count field, classify
            # as TOTAL_DRIFT (we know the aggregate is off but not which orders).
            # The orchestrator can later upgrade this to MISSING_IN_DK by
            # cross-referencing actual order IDs.
            cls = DiscrepancyClass.TOTAL_DRIFT
            discrepancies.append(Discrepancy(
                month=month, source_id=source_id,
                diff_class=cls,
                field=f, dk_value=dv, kc_value=kv,
                severity=sev,
            ))

    # Stable, severity-first ordering.
    discrepancies.sort(
        key=lambda d: (-d.severity.rank(), d.month, d.source_id, d.field)
    )
    return discrepancies


# ─── Pure: aggregate severity for a run ───────────────────────────────────────


def overall_severity(
    issues: List[IntegrityIssue],
    discrepancies: List[Discrepancy],
) -> Severity:
    """Combine layer-1 and layer-2 outputs into a single run severity."""
    max_rank = Severity.INFO.rank()
    for i in issues:
        max_rank = max(max_rank, i.severity.rank())
    for d in discrepancies:
        max_rank = max(max_rank, d.severity.rank())
    for s in (Severity.CRITICAL, Severity.WARN, Severity.INFO):
        if s.rank() == max_rank:
            return s
    return Severity.INFO  # unreachable


def summarize_discrepancies(discrepancies: List[Discrepancy]) -> Dict[str, int]:
    """Per-class counts. Stable shape (zero entries included) so the schema
    stays consistent across runs."""
    out: Dict[str, int] = {c.value: 0 for c in DiscrepancyClass}
    for d in discrepancies:
        out[d.diff_class.value] += 1
    return out


# ─── Layer 1: Internal integrity checks ───────────────────────────────────────

# `KNOWN_STATUS_IDS` and `KNOWN_SOURCE_IDS` stood here until step 14's
# S14-14, with the four generic checks over DuckDB's landing that read them
# (`_pk_uniqueness_check`, `_fk_orphan_check`, `_null_constraint_check`,
# `_value_domain_check`). The ids moved, byte for byte, to
# `core/pg_warehouse_dq.py`, whose order-landing twins are their one reader.


# Per-entity freshness thresholds (hours) and the severity when breached.
# orders syncs every ~60s so a multi-hour gap means the pipeline is dead →
# no new revenue lands → CRITICAL. catalog (categories/expense_types) syncs only
# in the weekly full_sync, so its threshold tolerates a healthy week but fires on
# a stall (the confirmed 45-day-stale incident that NOTHING alerted on).
FRESHNESS_THRESHOLDS: Dict[str, Tuple[float, "Severity"]] = {
    "orders":        (6,   Severity.CRITICAL),
    "products":      (48,  Severity.WARN),
    "buyers":        (48,  Severity.WARN),
    "offers":        (48,  Severity.WARN),
    "stocks":        (48,  Severity.WARN),
    "managers":      (192, Severity.WARN),
    "categories":    (192, Severity.WARN),
    "expense_types": (192, Severity.WARN),
}

# Entities whose `last_sync_*` VALUE is the source's clock, not the sync's: the
# order step stores KeyCRM's newest `updated_at` (`SyncService._orders_step`),
# because that is where its next window starts. It stands still whenever
# nobody touches an order, so judged at 6 h it paged every night quiet for six
# hours before 07:00 Kyiv — five CRITICALs from 2026-09-21 to 10-29, each the
# length of that night's gap in order activity, with the sync landing batches
# the whole time. These are judged by when the sync last wrote the row
# (`chain_stamps`, `meta.chain_watermarks.updated_at`), which the order step
# rewrites on every tick that lands a batch.
JUDGED_BY_LAST_WRITE: FrozenSet[str] = frozenset({"orders"})


def _freshness_check(
    now: Optional[datetime] = None,
    *,
    chain_watermarks: Optional[Dict[str, str]] = None,
    chain_stamps: Optional[Dict[str, str]] = None,
    frozen_rows: Optional[Mapping[str, str]] = None,
) -> List[IntegrityIssue]:
    """Flag sync_metadata entities whose last_sync_* is older than its threshold.

    Catches silent sync-pipeline stalls — e.g. categories/expense_types going
    45 days stale while orders kept current — which neither the warehouse
    validation nor the reconciliation checks cover.

    A watermark whose write chain has moved to Postgres is read from
    `chain_watermarks` — `{key: value}` out of `meta.chain_watermarks`, read by
    the caller, which is async where this is not. DuckDB's copy of that key
    stopped moving at the switch, so judging it would page "offers stalled" two
    days after a switch that changed nothing. Called without it while a chain
    has moved, the moved entities are left out and the check says so, rather
    than judging a frozen copy or going quiet.

    A moved key Postgres does not hold yet is "never synced" — unless its
    chain declares `CHAIN_WATERMARK_INHERITS_DUCKDB`, in which case DuckDB's
    frozen stamp stands in, at the same threshold, until the first sync under
    the flag writes one. That was for a chain whose sync is rarer than this
    check: chain 6a's dictionary moves on the Sunday full sync, so a mid-week
    flip would otherwise have filed "never synced" on every run for up to a
    week; chains 6 and 6a declared it until step 14's S14-09, their flips long
    past. Chain 3 declares it for its sync window, and the store's getter
    answers the same stand-in (`DuckDBStore.get_last_sync_time`), so this
    judges what the sync reads.

    `chain_stamps` is `{key: when the row was last written}` out of the same
    table (`read_values_and_stamps`). An entity in `JUDGED_BY_LAST_WRITE`
    whose row Postgres holds is judged by that stamp instead of its value,
    because its value is KeyCRM's clock. Without a stamp — a caller that
    brought none, or the inherited stand-in, which is frozen on both clocks —
    its value is judged, as before.

    DuckDB's `sync_metadata` is never read here: its rows are `frozen_rows`,
    the same table as Postgres last copied it
    (`core.pg_frozen_metadata.frozen_sync_rows`, read by the async caller),
    and None there means the copy could not be read (`_judge_freshness`).
    That was the `KS_DUCKDB=off` branch since stage 5's PR-5, and the only
    one since step 14's S14-14, which deleted the read of DuckDB's own table
    under `on` with the rest of the DuckDB scan: web has run `off` whatever
    the variable says since S14-00.
    """
    return _judge_freshness(frozen_rows, now, chain_watermarks=chain_watermarks,
                            chain_stamps=chain_stamps, from_the_copy=True)


def _judge_freshness(
    rows: Optional[Mapping[str, str]],
    now: Optional[datetime] = None,
    *,
    chain_watermarks: Optional[Dict[str, str]] = None,
    chain_stamps: Optional[Dict[str, str]] = None,
    from_the_copy: bool = False,
) -> List[IntegrityIssue]:
    """`_freshness_check`'s verdict over rows already read — pure.

    `rows` are `{key: value}` of DuckDB's `sync_metadata`, its `last_sync_*`
    keys only. With `from_the_copy` False they are DuckDB's own — what
    `_freshness_check` read under `on` until step 14's S14-14; it hands
    every call the copy since.

    `from_the_copy` is `KS_DUCKDB=off`: the rows are Postgres's copy of that
    table, and None means it could not be read. Two things then follow from
    Postgres alone. **The bootstrap gate** reads both Postgres sources — the
    copy and `chain_watermarks` — so "no sync history at all" is said only of
    a Postgres that holds none, and never of a copy that failed to read. And
    an entity whose verdict would rest on the copy — a key Postgres does not
    hold, of a chain that inherits, or of one that has not moved — is
    **unwatched** when the copy is unreadable, rather than "never synced"."""
    from core.write_chains import WRITE_CHAINS, sync_keys

    issues: List[IntegrityIssue] = []
    if now is None:
        now = datetime.now().astimezone()

    copy_unread = from_the_copy and rows is None
    seen = {k[len("last_sync_"):]: v for k, v in (rows or {}).items()}
    # No sync history at all → fresh/bootstrap install, not a stall. Skip to
    # avoid spurious "never synced" issues before the first sync completes.
    if not from_the_copy and not seen:
        return []

    # Every chain's key lives in `meta.chain_watermarks` — since step 14's
    # S14-11 asked of nothing (`write_chains.sync_keys()`); until then a key
    # moved only once its chain wrote Postgres, and a chain whose KS_WRITE_*
    # was not understood left its keys unjudged.
    moved_keys = sync_keys()
    moved = {key[len("last_sync_"):] for key in moved_keys}
    # Under `off` the same gate, over everything Postgres holds: the copy read
    # and empty, and no watermark of a moved chain — read and empty too, when
    # any chain has moved. A read that failed is not an empty one.
    if from_the_copy and not copy_unread and not seen and not (
            moved_keys and chain_watermarks is None) and not any(
            (chain_watermarks or {}).values()):
        return []
    inherits = {k[len("last_sync_"):] for c in WRITE_CHAINS
                if getattr(c, "CHAIN_WATERMARK_INHERITS_DUCKDB", False)
                for k in getattr(c, "CHAIN_SYNC_KEYS", ())}
    # Entities whose watermark cannot be judged this run: every moved one when
    # the caller brought no watermarks.
    blind = set(moved) if chain_watermarks is None else set()
    # Under `off` with the copy unreadable: every entity whose verdict would
    # rest on the copy. A moved key Postgres holds is judged as it stands, and
    # a moved key of a chain that does not inherit is "never synced" without
    # it — the copy was never its stand-in.
    copy_blind: Set[str] = set()
    if copy_unread:
        copy_blind = {e for e in FRESHNESS_THRESHOLDS
                      if e not in moved
                      or (e in inherits and chain_watermarks is not None
                          and not chain_watermarks.get(f"last_sync_{e}"))}
        blind |= copy_blind
    # Only what this check judges. A moved key with no threshold — the search
    # index's cursor, chain 3's since OD-15 — is watched in no mode, so naming
    # it here would say that a stall of it went unwatched this run.
    blind &= set(FRESHNESS_THRESHOLDS)
    if blind:
        reasons = []
        if blind - copy_blind:
            reasons.append(
                "Their chain writes Postgres and this check was called without "
                "`chain_watermarks=` from core.pg_chain_watermarks.read_values.")
        if copy_blind & blind:
            reasons.append(
                "KS_DUCKDB=off, and DuckDB's sync_metadata as Postgres copied it "
                "(app.sync_metadata) could not be read, so the stamp they would "
                "be judged by is not in hand.")
        issues.append(IntegrityIssue(
            check_name="sync_watermarks_unwatched",
            table_name="sync_metadata",
            severity=Severity.WARN,
            count=len(blind),
            sample_ids=(),
            description=(
                f"the watermarks of {', '.join(sorted(blind))} are not judged this "
                "run, so a stall of those syncs is NOT being watched. "
                + " ".join(reasons)
            ),
            # Every entity named above, which under `off` with the copy
            # unreadable is more than the moved keys `unverified_conditions`
            # derives on its own: a delivered page of an entity no chain moved
            # must not be announced resolved by a run that did not look.
            unverified=tuple(f"freshness_{e}" for e in sorted(blind)),
        ))
    inherited: Set[str] = set()
    # Entity → its value, for those judged by the row's stamp instead.
    by_last_write: Dict[str, str] = {}
    for entity in moved:
        frozen = seen.pop(entity, None)
        if chain_watermarks is not None:
            value = chain_watermarks.get(f"last_sync_{entity}")
            stamp = (chain_stamps or {}).get(f"last_sync_{entity}")
            if value and stamp and entity in JUDGED_BY_LAST_WRITE:
                seen[entity] = stamp
                by_last_write[entity] = value
            elif value:
                seen[entity] = value
            elif frozen and entity in inherits:
                seen[entity] = frozen
                inherited.add(entity)

    for entity, (max_hours, sev) in FRESHNESS_THRESHOLDS.items():
        if entity in blind:
            continue
        raw = seen.get(entity)
        if not raw:
            where = ("meta.chain_watermarks" if entity in moved
                     else "app.sync_metadata (DuckDB's, as Postgres copied it)"
                     if from_the_copy else "sync_metadata")
            issues.append(IntegrityIssue(
                check_name=f"freshness_{entity}",
                table_name=entity,
                severity=sev,
                count=1,
                description=f"{where} has no last_sync_{entity} — entity never synced",
            ))
            continue
        try:
            ts = datetime.fromisoformat(raw)
        except (ValueError, TypeError):
            issues.append(IntegrityIssue(
                check_name=f"freshness_{entity}",
                table_name=entity,
                severity=Severity.WARN,
                count=1,
                description=f"unparseable last_sync_{entity}={raw!r}",
            ))
            continue
        if ts.tzinfo is None:
            ts = ts.astimezone()
        age_h = (now - ts).total_seconds() / 3600
        if age_h > max_hours:
            issues.append(IntegrityIssue(
                check_name=f"freshness_{entity}",
                table_name=entity,
                severity=sev,
                count=int(age_h),
                description=(
                    f"{entity} last synced {age_h:.1f}h ago "
                    f"(threshold {max_hours}h) — sync may be stalled"
                    + (" (when the sync last wrote meta.chain_watermarks; "
                       "the value, KeyCRM's newest updated_at, is "
                       f"{by_last_write[entity]})"
                       if entity in by_last_write else "")
                    + (" (DuckDB's stamp from before the switch"
                       + (", as Postgres copied it" if from_the_copy else "")
                       + ": no sync under the flag has written "
                       "meta.chain_watermarks yet)"
                       if entity in inherited else "")
                ),
            ))
    return issues


# How far back the inventory continuity check looks. Long enough that a hole
# is still visible after a few days of not reading Telegram; short enough that
# gaps which are already permanent stop being reported forever, since there is
# nothing left to do about them.
INVENTORY_CONTINUITY_WINDOW_DAYS = 30


def _inventory_snapshot_continuity_check(
    now: Optional[datetime] = None,
    *,
    calendar: "Optional[Tuple[Optional[date], FrozenSet[date]]]" = None,
) -> List[IntegrityIssue]:
    """Flag days with no per-SKU inventory snapshot.

    `inventory_sku_history` is written once a day from *current* stock, and
    KeyCRM serves current stock only. A day the job did not run is therefore a
    day that cannot be reconstructed from anything, ever — unlike almost
    everything else here, it is not re-fetchable at any price.

    It has already happened. Between 2026-01-27 and 2026-08-09 the table holds
    170 of 195 calendar days; twenty-one of the twenty-five missing ones are a
    single unbroken run. Nothing reported it at the time and nothing reports it
    now: a failed job increments an in-memory counter, and a container restart
    clears even that.

    Yesterday is the unit that matters. Today may legitimately have no snapshot
    yet (the job runs at 01:00), and gaps older than the window are permanent —
    reporting them forever would be noise, which is its own failure mode.
    """
    if now is None:
        now = datetime.now().astimezone()
    today = now.date()
    yesterday = today - timedelta(days=1)

    # This check follows the writer: the snapshot is taken into
    # `app.inventory_sku_history` (chain 1; since step 14's S14-11 asked of no
    # flag), and reading DuckDB here would file a finding for every day since
    # the switch — about snapshots taken correctly, in the other store.
    #
    # The facts arrive pre-read rather than being fetched here, because this
    # runs inside the **sync** `check_internal_integrity` while a Postgres read
    # is async. `core.pg_inventory_write.read_snapshot_calendar` is the reader
    # and `_run_dq_integrity` is the caller that knows which engine to ask.
    # Below this line the check does not know or care which store answered —
    # `compare_table`'s arrangement, and the reason there is one body and not
    # two.
    #
    # The guard stays, narrowed to what it now protects: a caller that
    # supplied nothing. That is a forgotten call site, not a
    # migration, and it must still be loud — this is the one check standing
    # over the one table that cannot be reconstructed at any price.
    if calendar is None:
        return [IntegrityIssue(
            check_name="inventory_continuity_unwatched",
            table_name="inventory_sku_history",
            severity=Severity.WARN,
            count=1,
            sample_ids=(),
            description=(
                "The per-SKU snapshot is written to "
                "app.inventory_sku_history, and this check was "
                "called without a pre-read calendar — so it is NOT watching "
                "the only table here that cannot be reconstructed from any "
                "source. The caller must pass `calendar=` from "
                "core.pg_inventory_write.read_snapshot_calendar."
            ),
        )]

    first_day, present = calendar

    if first_day is None:
        # No history at all → fresh install, not a stall. Same reasoning as
        # _freshness_check: do not accuse a bootstrap of losing data.
        return []
    # A snapshot cannot be missing from before the first one ever taken.
    window_start = max(first_day, today - timedelta(days=INVENTORY_CONTINUITY_WINDOW_DAYS))
    if window_start > yesterday:
        return []

    # The calendar is enumerated here rather than in SQL, so the two
    # engines cannot disagree about what a day is. `generate_series` exists
    # in both and spells differently in each; a Python range spells the
    # same everywhere and is the same arithmetic.
    span_days = (yesterday - window_start).days
    missing = [
        d for d in (window_start + timedelta(days=i) for i in range(span_days + 1))
        if d not in present
    ]

    if not missing:
        return []

    # Yesterday missing means the job is failing now and today is at risk too.
    # Older gaps inside the window are already unrecoverable: worth stating,
    # not worth waking anyone.
    stale_now = yesterday in missing
    span = f"{window_start.isoformat()}..{yesterday.isoformat()}"
    if stale_now:
        description = (
            f"no inventory snapshot for yesterday ({yesterday.isoformat()}); "
            f"{len(missing)} day(s) missing in {span}. Stock history is built "
            f"from current stock only — a missed day cannot be backfilled."
        )
    else:
        description = (
            f"{len(missing)} day(s) with no inventory snapshot in {span} "
            f"(most recent {missing[-1].isoformat()}); permanently unrecoverable, "
            f"reported so the count is a number rather than a silence."
        )

    return [IntegrityIssue(
        check_name="inventory_snapshot_gaps",
        table_name="inventory_sku_history",
        severity=Severity.WARN if stale_now else Severity.INFO,
        count=len(missing),
        description=description,
    )]


# `_orders_without_line_items_check` and `_attribution_coverage_check` stood
# here until step 14's S14-14: DuckDB's halves of `pg_orders_without_line_items`
# and `pg_attribution_coverage_website`. What the twins read from them — the
# `min_total` default, and the attribution thresholds the twin read out of
# the signature — moved, byte for byte, to `core/pg_warehouse_dq.py`, with
# the five-week outage the attribution check was written for.


def classify_order_discrepancies(
    dk: Dict[int, Dict[str, Any]],
    kc: Dict[int, Dict[str, Any]],
    *,
    fields: Tuple[str, ...] = (
        "status_id", "source_id", "manager_id", "buyer_id",
        "grand_total", "order_date", "n_lines", "qty", "line_amount",
    ),
    money_fields: frozenset = frozenset({"grand_total", "line_amount"}),
    money_tolerance: float = 0.01,
    max_ids: int = 50,
) -> List[Discrepancy]:
    """Compare two order-id → facts maps, order by order.

    Rollups net out anything that offsets — a status wrong on one order and
    right on another, a line item lost here and gained there, a manager
    reassigned — so the monthly comparison can be perfectly green while
    individual orders disagree. This sees those.

    Results are grouped into one Discrepancy per (month, source, field) so they
    fit the existing diffs table, with `dk_value` carrying the number of orders
    that differ and `order_ids` carrying handles for triage.
    """
    grouped: Dict[Tuple[str, int, str, DiscrepancyClass], List[int]] = {}

    def _add(order_id: int, facts: Dict[str, Any], field_name: str,
             klass: DiscrepancyClass) -> None:
        order_date = facts.get("order_date")
        month = order_date.strftime("%Y-%m") if order_date is not None else "unknown"
        source_id = int(facts.get("source_id") or 0)
        grouped.setdefault((month, source_id, field_name, klass), []).append(order_id)

    for order_id in sorted(set(kc) - set(dk)):
        _add(order_id, kc[order_id], "order", DiscrepancyClass.MISSING_IN_DK)
    for order_id in sorted(set(dk) - set(kc)):
        _add(order_id, dk[order_id], "order", DiscrepancyClass.MISSING_IN_KC)

    for order_id in sorted(set(dk) & set(kc)):
        dk_facts, kc_facts = dk[order_id], kc[order_id]
        for field_name in fields:
            dk_value, kc_value = dk_facts.get(field_name), kc_facts.get(field_name)
            if field_name in money_fields:
                differs = abs(float(dk_value or 0) - float(kc_value or 0)) > money_tolerance
            else:
                differs = dk_value != kc_value
            if not differs:
                continue
            klass = (DiscrepancyClass.STATUS_DRIFT if field_name == "status_id"
                     else DiscrepancyClass.VALUE_MISMATCH)
            _add(order_id, kc_facts, field_name, klass)

    out: List[Discrepancy] = []
    for (month, source_id, field_name, klass), ids in sorted(
        grouped.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])
    ):
        # A per-order difference is material by construction: there is no
        # rounding noise in "this order exists on one side only" or "its status
        # is 12 here and 19 there".
        severity = (
            Severity.CRITICAL
            if klass in (DiscrepancyClass.MISSING_IN_DK,
                         DiscrepancyClass.MISSING_IN_KC,
                         DiscrepancyClass.STATUS_DRIFT)
            else Severity.WARN
        )
        out.append(Discrepancy(
            month=month,
            source_id=source_id,
            diff_class=klass,
            field=field_name,
            dk_value=float(len(ids)),
            kc_value=0.0,
            severity=severity,
            order_ids=tuple(ids[:max_ids]),
        ))
    return out


# `_headline_vs_line_items_check`, `_status_group_agreement_check`,
# `_goods_shipped_without_sale_check`, `_gold_cell_values_check` and
# `_silver_arc_check` stood here until step 14's S14-14 — the DuckDB checks
# over Silver, Gold and the order tables, stood down since step 13 and
# chain 3. Their Postgres twins are `core/pg_warehouse_dq.py`'s, and
# `_SILVER_ROW_COLUMNS` and `_silver_row_differs`, which the row-values twin
# renders with, moved there byte for byte. Gold's recompute is ClickHouse's
# (`ch_silver.compare_gold_cells`) and `pg_gold_internal_check`'s.


def check_internal_integrity(
    *,
    inventory_calendar: "Optional[Tuple[Optional[date], FrozenSet[date]]]" = None,
    chain_watermarks: Optional[Dict[str, str]] = None,
    chain_stamps: Optional[Dict[str, str]] = None,
    frozen_sync_rows: Optional[Mapping[str, str]] = None,
    raised_out: Optional[List[str]] = None,
) -> List[IntegrityIssue]:
    """The Layer-1 checks this module still runs: the sync watermarks'
    freshness and the per-SKU snapshot calendar, both judged over facts the
    integrity job pre-read from Postgres. Returns list of issues (empty =
    clean). Pure: no connection, no I/O.

    `chain_watermarks` and `chain_stamps` are `meta.chain_watermarks`' values
    and when each row was last written, which the freshness check judges in
    place of a value that is the source's clock (`JUDGED_BY_LAST_WRITE`).
    `frozen_sync_rows` is DuckDB's `sync_metadata` as Postgres last copied it
    (`_freshness_check`). `inventory_calendar` is
    `core.pg_inventory_write.read_snapshot_calendar`'s answer.

    Until step 14's S14-14 this was the DuckDB scan — PK uniqueness, the
    orphan, NULL and domain checks, KeyCRM's status group against the return
    list, the line-item checks, the landing→Silver arc, attribution coverage
    and the Gold recompute — every one of them stood down in production since
    step 13 and chain 3, under a connection that refused (`KS_DUCKDB=off`).
    Their Postgres twins (`core/pg_warehouse_dq.py`) and the chain
    invariants (`core/pg_chain_invariants.py`) are judged by the integrity
    job beside this, outside it.
    """
    issues: List[IntegrityIssue] = []

    # A check that raises is named, never swallowed: a dropped, renamed or
    # moved table must not make its check vanish while the run persists as a
    # clean success. `error_message` is not touched: the other checks ran,
    # and the layer's age, the canary and paging stay where they were.
    raised: List[str] = []

    def guarded(name: str, run) -> List[IntegrityIssue]:
        try:
            return run()
        except Exception as exc:  # noqa: BLE001 — named in `integrity_check_raised`
            logger.error("integrity check %s raised: %s: %s",
                         name, type(exc).__name__, exc)
            raised.append(f"{name} ({type(exc).__name__})")
            if raised_out is not None:
                raised_out.append(name)
            return []

    # Freshness — catch silent sync-pipeline stalls (e.g. categories 45d stale).
    issues += _freshness_check(chain_watermarks=chain_watermarks,
                               chain_stamps=chain_stamps,
                               frozen_rows=frozen_sync_rows)

    # A missed inventory snapshot is the one loss here with no second chance:
    # the API serves current stock, so yesterday's is gone the moment yesterday
    # is. Twenty-five days went missing in 2026 without anything saying so.
    issues += guarded("inventory_snapshot_continuity",
                      lambda: _inventory_snapshot_continuity_check(
                          calendar=inventory_calendar))

    if raised:
        issues.append(IntegrityIssue(
            check_name="integrity_check_raised",
            table_name="(integrity scan)",
            severity=Severity.WARN,
            count=len(raised),
            sample_ids=(),
            description=(
                f"{len(raised)} integrity check(s) raised and reported nothing: "
                + ", ".join(raised)
                + ". Their findings are absent from this run, not clean — "
                "the table each reads is missing, renamed or unreadable."
            ),
        ))

    return issues


# What each guarded check reports on. A run in which a check could not look —
# it raised, or it said it is not watching — has no verdict on these, so the
# run must neither fire nor resolve them: "✅ Resolved" posted for a page the
# check never re-examined is a recovery nobody verified. Keyed by the name
# `check_internal_integrity` guards each check under; a test derives every
# tuple from the check_name values the check itself emits, so the two cannot
# drift. The DuckDB checks' entries went with them in step 14's S14-14, and
# `ORDER_LANDING_CHECKS` and `DUCKDB_OFF_STOOD_DOWN`, the sets chain 3 and
# `KS_DUCKDB=off` stood them down by, with them.
GUARDED_CHECK_CONDITIONS: Dict[str, Tuple[str, ...]] = {
    "inventory_snapshot_continuity": ("inventory_snapshot_gaps",),
}


def unverified_conditions(raised: Sequence[str], issues: List[IntegrityIssue]) -> List[str]:
    """Conditions this run could not re-examine, from both kinds of blindness.

    `raised` are the guard names `check_internal_integrity` reported through
    `raised_out`. The `*_unwatched` findings are the other kind: the check ran
    but was not handed what it needs to look. A finding that knows which
    conditions it left unexamined names them in `unverified`, and those are
    held too.
    """
    names = set()
    for check in raised:
        names.update(GUARDED_CHECK_CONDITIONS.get(check, ()))
    found = {i.check_name for i in issues}
    if "inventory_continuity_unwatched" in found:
        names.add("inventory_snapshot_gaps")
    if "sync_watermarks_unwatched" in found:
        from core.write_chains import sync_keys

        # The judged entities only (`_freshness_check`'s `blind`): a moved key
        # with no threshold has no condition to hold.
        names.update(f"freshness_{key[len('last_sync_'):]}"
                     for key in sync_keys()
                     if key[len("last_sync_"):] in FRESHNESS_THRESHOLDS)
    # What the finding itself names. Under `on` that is a subset of the moved
    # keys above, so nothing changes there; under `off` with
    # `app.sync_metadata` unreadable it is also every entity no chain moved
    # (stage 5, PR-5), which the moved keys alone would leave to be resolved.
    for issue in issues:
        names.update(issue.unverified)
    return sorted(names)


def summarize_issues(issues: List[IntegrityIssue]) -> Dict[str, int]:
    """Counts by severity. Schema-stable across runs."""
    out: Dict[str, int] = {s.value: 0 for s in Severity}
    for i in issues:
        out[i.severity.value] += 1
    return out


# ─── Persistence (writes to data_quality_runs / _issues / _diffs) ─────────────


def _status_from_severity(sev: Severity) -> str:
    """Map run-level severity → status string written to data_quality_runs."""
    return {"CRITICAL": "CRITICAL", "WARN": "WARN", "INFO": "PASS"}[sev.value]


@dataclass(frozen=True)
class RunValues:
    """One run row's values, computed once.

    Chain 9 (OD-02 (c)) writes the journal to Postgres; until step 14's S14-10
    it then handed DuckDB the same row as a shadow, so the status, the counts
    and the duration were computed in one place rather than once per store —
    two computations of `duration_ms` a millisecond apart would have been a
    discrepancy every morning. DuckDB's own writer, `persist_run`, computed
    them here too, until S14-10 deleted it.
    """
    started_at: datetime
    ended_at: datetime
    as_of: datetime
    window_start: date
    window_end: date
    layer: str
    status: str
    integrity_issues_count: int
    discrepancies_count: int
    critical_count: int
    warn_count: int
    api_calls_used: int
    duration_ms: int
    error_message: Optional[str]


# The run row's columns in insert order — `RunValues`' fields, which is what
# `journal_rows` writes and the readers read back.
RUN_COLUMNS: Tuple[str, ...] = (
    "started_at", "ended_at", "as_of", "window_start", "window_end",
    "layer", "status",
    "integrity_issues_count", "discrepancies_count",
    "critical_count", "warn_count",
    "api_calls_used", "duration_ms", "error_message",
)


def run_values(
    *,
    started_at: datetime,
    ended_at: datetime,
    as_of: datetime,
    window_start: date,
    window_end: date,
    layer: str,
    issues: List[IntegrityIssue],
    discrepancies: List[Discrepancy],
    api_calls_used: int = 0,
    error_message: Optional[str] = None,
) -> RunValues:
    """The run row chain 9 writes, as values. Pure."""
    issue_sev = summarize_issues(issues)

    critical_count = (
        issue_sev.get("CRITICAL", 0)
        + sum(1 for d in discrepancies if d.severity == Severity.CRITICAL)
    )
    warn_count = (
        issue_sev.get("WARN", 0)
        + sum(1 for d in discrepancies if d.severity == Severity.WARN)
    )

    if error_message is not None:
        status = "FAILED"
    else:
        status = _status_from_severity(overall_severity(issues, discrepancies))

    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    return RunValues(
        started_at=started_at, ended_at=ended_at, as_of=as_of,
        window_start=window_start, window_end=window_end, layer=layer,
        status=status,
        integrity_issues_count=len(issues),
        discrepancies_count=len(discrepancies),
        critical_count=critical_count, warn_count=warn_count,
        api_calls_used=api_calls_used, duration_ms=duration_ms,
        error_message=error_message,
    )


@dataclass(frozen=True)
class JournalRows:
    """One run and its findings as the rows chain 9 writes (OD-02 (c)):
    Postgres commits these in one transaction. (DuckDB's shadow was given the
    very same tuples, `insert_journal_rows`, until step 14's S14-10.)"""
    run: Tuple[Any, ...]                      # (run_id, *RUN_COLUMNS)
    issues: Tuple[Tuple[Any, ...], ...]       # data_quality_issues' 7 columns
    diffs: Tuple[Tuple[Any, ...], ...]        # data_quality_diffs' 9 columns

    @property
    def run_id(self) -> int:
        return int(self.run[0])


def _require_aware(name: str, value: Any) -> None:
    """A naive timestamp reads as UTC in asyncpg, so a Kyiv wall clock
    would land three hours off (and in DuckDB, while it was the shadow, as
    Kyiv local time — three hours from Postgres's copy)."""
    if isinstance(value, datetime) and value.tzinfo is None:
        raise ValueError(
            f"{name}={value!r} is naive; the journal's two stores would read "
            "it in two different zones")


def journal_rows(
    run_id: int,
    values: RunValues,
    issues: Sequence[IntegrityIssue],
    discrepancies: Sequence[Discrepancy],
) -> JournalRows:
    """The rows of one run, as Postgres is handed them. Pure.

    The expressions DuckDB's `_persist_run_rows` wrote until step 14's S14-10,
    with two normalisations asyncpg needs: `dk_value`/`kc_value` as `float`
    (a `Decimal` from a rollup would otherwise reach float8 as a Decimal), and
    `count` as `int`. Raises on a naive timestamp (`_require_aware`).
    """
    for column in ("started_at", "ended_at", "as_of"):
        _require_aware(column, getattr(values, column))
    run = (int(run_id),) + tuple(getattr(values, c) for c in RUN_COLUMNS)
    issue_rows = tuple(
        (int(run_id), i.check_name, i.table_name, i.severity.value,
         int(i.count), json.dumps(list(i.sample_ids)), i.description)
        for i in issues
    )
    diff_rows = tuple(
        (int(run_id), d.month, int(d.source_id), d.diff_class.value, d.field,
         float(d.dk_value), float(d.kc_value), d.severity.value,
         json.dumps(list(d.order_ids)) if d.order_ids else None)
        for d in discrepancies
    )
    return JournalRows(run=run, issues=issue_rows, diffs=diff_rows)


# `severity` is text, so `ORDER BY severity DESC` ranked WARN, INFO, CRITICAL:
# the worst finding came last in the digest and was the first to fall past a
# LIMIT or into "…+N more". Ranked explicitly, as `evidence_for_agent` does.
_WORST_FIRST = (
    "CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1 "
    "WHEN 'INFO' THEN 2 ELSE 3 END"
)


# The journal's three tables, as a hole each reader's text carries: Postgres's
# `app.*`, with `?` numbered (chain 9, OD-02 (c) — the readers follow the
# writer). The same texts served DuckDB's bare names until step 14's S14-10,
# which is why they keep the hole; only `postgres` renders them now.
JOURNAL_TABLES = {
    "postgres": {"runs": "app.data_quality_runs",
                 "issues": "app.data_quality_issues",
                 "diffs": "app.data_quality_diffs"},
}


def journal_sql(sql: str, engine: str = "postgres") -> str:
    """A reader's text for Postgres: its table holes filled and `?` numbered
    for asyncpg. `engine` stays a parameter so the rendering chain 9 runs is
    byte for byte the one it ran; any other engine is a `KeyError`."""
    from core.sql_dialect import numbered

    return numbered(sql.format(**JOURNAL_TABLES[engine]))


RUN_DIFFS_SQL = f"""
        SELECT month, source_id, diff_class, field,
               dk_value, kc_value, severity, order_ids
        FROM {{diffs}}
        WHERE run_id = ?
        ORDER BY {_WORST_FIRST}, month, source_id
        LIMIT ?
    """

RUN_ISSUES_SQL = f"""
        SELECT check_name, table_name, severity, count, sample_ids, description
        FROM {{issues}}
        WHERE run_id = ?
        ORDER BY {_WORST_FIRST}, check_name
        LIMIT ?
    """


def diff_dicts(rows) -> List[Dict[str, Any]]:
    """A run's discrepancies, shaped for the health endpoint and the digest."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "month": r[0], "source_id": int(r[1]),
            "diff_class": r[2], "field": r[3],
            "dk_value": float(r[4]), "kc_value": float(r[5]),
            "severity": r[6],
            "order_ids": json.loads(r[7]) if r[7] else [],
        })
    return out


def issue_dicts(rows) -> List[Dict[str, Any]]:
    """A run's findings, shaped for the health endpoint and the digest."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "check_name": r[0], "table_name": r[1],
            "severity": r[2], "count": int(r[3] or 0),
            "sample_ids": json.loads(r[4]) if r[4] else [],
            "description": r[5],
        })
    return out


# ─── What to do about it ─────────────────────────────────────────────────────
#
# Rule 1 of the alerts charter is "a page is an action". A CRITICAL that names
# a check and a count still leaves the reader to work out which lever exists,
# and the levers are not guessable: some of these conditions are repaired by a
# job that is already running, some by one admin POST, and some — the archive
# and the stock movements — must never be "repaired" at all, because the only
# record of the fact is the thing that would be overwritten.
#
# Matched by longest prefix, because half of the check names are generated
# (`fk_orphan_<child>_<fk>`, `freshness_<entity>`, `pk_uniqueness_<table>`) and
# a table keyed on exact names would silently miss every one of them.
REMEDIATION: Tuple[Tuple[str, str], ...] = (
    ("mirror_", "Wait for the hourly re-ship; check meta.mirror_state. Never copy rows by hand"),
    ("mirror_never_shipped", "Normal before Sunday (weekly sync writes it); after — a defect"),
    # Two layers share this check name — orders and, since revision 0020,
    # expenses — and REMEDIATION keys on the name alone, so the lever has to
    # name the table it applies to rather than assume one of them.
    # Since step 14's S14-13 there is no backfill route: Postgres is the
    # store of record, so a table without its history is restored from the
    # nightly dump, which carries `meta.mirror_state.backfilled_at` with it.
    ("mirror_backfill_pending",
     "Restore the table named from the nightly Postgres dump; "
     "never set backfilled_at by hand"),
    # `mirror_stood_down`, `order_owner_row_without_marker` and
    # `owner_row_without_marker` had their lines here until step 14's S14-12
    # deleted the comparisons that filed them.
    ("ch_", "Wait for the hourly ch_sync; stuck — check KS_CH_URL and the grant"),
    ("ch_history_", "Lost in PG and CH at once is unrepairable — a human decides"),
    ("order_versions_", "Do not repair: the archive is the only chronicle. Check the writer is alive"),
    ("gold_", "POST /api/warehouse/refresh rebuilds Silver and Gold — Postgres too under KS_PG_DERIVE=own"),
    ("customer_profile_", "Rebuilt on the same tick; survives one — suspect Silver"),
    ("freshness_", "The sync, not the warehouse: see the sync block in /api/health"),
    ("integrity_check_raised",
     "A check's table is gone or renamed — find which in the ERROR log; its findings are absent, not clean"),
    # The ownership latch: owner rows standing with no marker (the chain
    # invariants since step 14's S14-12). Not repairable by a job — a human
    # puts the marker back. `chain_shipper_overwrote` and
    # `chain_owner_unregistered` went with the daily comparison.
    ("chain_latch_disagrees", f"{ROLLBACK_LEVER} (CLAUDE.md)"),
    ("sync_watermarks_unwatched",
     "The integrity job must pre-read meta.chain_watermarks, or the chain's flag goes back"),
    # The standing watch on a chain's own tables (DN-07). Nothing here is
    # repairable by a job, and the last two must never be: an initial burst has
    # already recorded the wrong deltas, a reset first_seen_at has already lost
    # the dates, and a missed snapshot day cannot be photographed again.
    ("chain_sequence_behind",
     "Raise the sequence above MAX(id) with setval before the next write — one insert away from a duplicate id"),
    ("chain_required_column_null",
     "Find the writer that stopped supplying the column; the Postgres table has no default by design"),
    ("chain_initial_movement_burst",
     "Do not repair: those deltas were computed against zero. Check bronze.offer_stocks survived the last sync"),
    ("chain_first_seen_reset",
     "Do not repair: the dates are gone. Check the status rebuild's carry-forward, then restore from the nightly dump"),
    ("chain_daily_rollup_missing",
     "A missed day is gone for good; check the hourly stocks sync reached record_inventory_snapshot"),
    ("chain_snapshot_rows_short",
     "The status table was empty when the snapshot was taken; check the rebuild ran first"),
    ("chain_watermark_stale",
     "The sync, not the warehouse: see the sync block in /api/health, then the KeyCRM errors in the web log"),
    # Chain 6a's dictionary. KeyCRM serves it only to the full sync, so that is
    # the lever for both; the second also needs the writer that skipped the
    # shared parse found first, or the sync writes the key straight back.
    ("chain_dictionary_empty",
     "Run a full sync — the only path KeyCRM serves expense types to; until then /expenses shows every cost as Other"),
    ("chain_name_unresolved",
     "Find the write that skipped core.landing_rows.expense_type_rows, then run a full sync to rewrite the names"),
    # Chain 4's buyers. Nothing deletes a buyer and every writer writes the
    # three tables together, so an orphan is a write that went round them.
    ("chain_buyer_orphan_rows",
     "Find the write that skipped core.pg_buyers_write; never delete a buyer's contacts to clear it"),
    ("chain_buyer_contact_missing",
     "POST /api/sync/sync-all-buyers rewrites every buyer and its contacts; find what deleted the list first"),
    # Chain 3's orders: an order and its costs are written together, so an
    # orphaned cost is an order refused or a write that went round the chain.
    ("chain_expense_orphans",
     "Read the web log for the order ids the chain refused; a refused order is re-offered for 24 h, then re-fetch it by id"),
    # Chain 5's classification. Only the sync can re-land managers; only a
    # human can decide an interval, and nothing here may re-derive one.
    ("chain_managers_empty",
     "Find what deleted bronze.managers; the next manager sync lands them, but the intervals are not re-fetchable — restore those from the nightly dump"),
    ("chain_manager_open_interval",
     "Per id: a forward classification through POST /api/managers/{id}/retail-status closes the extra open interval; none open needs one"),
    ("chain_manager_unclassified",
     "Find the write that skipped core.pg_managers_write.upsert_managers; its next run seeds the baseline"),
    ("chain_manager_intervals_broken",
     "A human decides each overlap or gap per id — it changes past sales_type; never let a job rewrite history"),
    ("chain_manager_retail_disagrees",
     "Reclassify the manager through POST /api/managers/{id}/retail-status, which sets both in one transaction"),
    # Chain 6's catalogue (OD-15 (a)). Never re-ship it out of DuckDB: DuckDB's
    # copy froze at the flip, and the chain's writer is the only one.
    ("chain_catalogue_empty",
     "POST /api/jobs/full_sync_weekly/trigger lands both tables; until then every Postgres order line reads Unknown"),
    ("chain_catalogue_written_around",
     "Find the writer that is not pg_catalogue_write; delete a stray id or restore that one row from DuckDB, mirrored_at too; never re-ship the catalogue"),
    ("chain_catalogue_rows_lost",
     "Nothing deletes this table: find the statement; the next hourly products sync restores what KeyCRM serves"),
    ("chain_catalogue_retired",
     "Not a defect: KeyCRM retired them, and the dashboard keeps their names"),
    ("chain_catalogue_short_write",
     "Read the last products sync in the web log (pages fetched vs the catalogue); the next hourly write heals it"),
    # Chain 7b-3's four tables. Recomputable, so the lever is the writer; what
    # to read first is the job's own result in /api/jobs.
    ("chain_goal_tables_incomplete",
     "POST /api/goals/recalculate stores all three tables at once; if it recurs, read seasonality_calc in /api/jobs"),
    ("chain_goal_tables_stale",
     "Read seasonality_calc in /api/jobs (skipped? error?), then POST /api/jobs/seasonality_calc/trigger"),
    ("chain_forecast_stale",
     "Read revenue_prediction_train in /api/jobs (rejected? predictions_stored false?), then trigger it again"),
    # Chains 9, 10, 11a and 11b (OD-02 (c)). Chain 9 writes a run and its
    # findings in one transaction, so an orphan is a write that went round it.
    ("chain_orphan_children",
     "Find the write that skipped core.dq_journal; never delete the findings to clear it"),
    ("chain_samples_stale",
     "grep the web log for \"sample not persisted\"; the watchdog still judges capacity without history"),
    ("chain_retention_unbounded",
     "Check the watchdog tick reaches its prune (core/watchdog_samples.py); never delete samples by hand"),
    ("chain_report_week_missing",
     "Read the report job's result in /api/jobs and write_chains.<chain>.pending in /api/health; never insert the row by hand unless the message went out"),
    ("chain_invariants_unwatched",
     "Nothing else watches these tables: read the reason, then check KS_PG_DSN and that the integrity job still reads the facts"),
    # The Postgres twins (step 8a). The lever depends on who derives.
    ("pg_silver_", "Postgres Silver: under KS_PG_DERIVE=own POST /api/warehouse/refresh; under piggyback it follows the DuckDB tick"),
    ("pg_attribution_coverage_", "Not ours to repair: check the shop's order-comment template; rule out a stale silver.order_utm copy first"),
    ("pg_headline_vs_line_items", "Standing (certificates); the signal is growth, not level"),
    ("pg_goods_shipped_without_sale", "By design (bloggers/seeding); the signal is growth"),
    ("pg_warehouse_", "The Postgres twins did not look: read the reason, then check KS_PG_DSN and PG_LAYER_LOCK holders"),
    # The two findings that page (`mirror_reconciliation._ORDER_UTM_LEVER_REASONS`
    # carries the reasons, because this is the single "→" line of a Telegram
    # alert). The parse writes every failure into its watermark row before it
    # raises, and both routes run it. Until step 14's S14-15 this line was
    # `KS_UTM_PARSE`'s to choose: under `duckdb` it sent the reader to copy a
    # comment into DuckDB and to the ship, and `REMEDIATION_UTM_PARSED_IN_POSTGRES`
    # held this line, which `remediation_for` swapped in under `postgres`.
    ("pg_order_utm_",
     "Read meta.mirror_state for silver.order_utm: the parse writes its error there. "
     "Then POST /api/traffic/refresh or /api/warehouse/refresh"),
    ("buyers_without_verdict",
     "Grep web's log for 'gender derivation failed'; unless chain 4 writes the buyers, read meta.mirror_state for app.buyer_gender"),
    ("buyers_missing_for_orders", "Read buyer_sync in /api/health: the step's failures and retry window"),
    ("pg_silver_arc_unwatched", "The Postgres twins did not look: read the reason in the finding"),
    ("pg_attribution_coverage_unwatched", "The orders mirror is failing or silent: see mirror freshness in /api/health"),
    ("pg_line_items_unwatched", "The Postgres twins did not look: read the reason in the finding"),
    # Step 8b. The finding says which of two things it is: a row no rebuild has
    # reached yet wants the generic Silver lever above; a row an error-free
    # rebuild already covered means the rebuild is not producing what bronze
    # holds, and rebuilding again would only repeat it.
    ("pg_silver_row_values",
     "Silver kept old values: POST /api/warehouse/refresh. If a rebuild already covered them, compare silver.orders with bronze.orders for those ids"),
    ("pg_silver_row_values_unwatched",
     "The Silver recompute did not finish: read the reason; a spent hold budget means it outgrew HOLD_BUDGET_S"),
    # DN-14. Not a rebuild: the run that saw the write already rebuilt it.
    # The lever is the writer that did not mark, or the mark that was dropped.
    ("pg_signal_missed",
     "An orders write raised no derivation mark: check meta.mirror_state for dropped marks, else find the writer in the web log at the runs' started_at"),
    ("pg_derivation_signal_unwatched",
     "The derivation journal could not be read: read the reason in the finding"),
    # DN-23: DuckDB's landing checks, read over bronze.*. Each twin's lever is
    # its DuckDB check's, pointed at the copy the twin read.
    ("pg_orders_without_line_items",
     "halfwritten_repair re-fetches within 2h; one cycle is fine. Longer: compare order_products for the ids"),
    ("pg_fk_orphan_", "Re-ship the parent orders to bronze.orders first; never delete the line items"),
    ("pg_not_null_", "Find the writer that sent bronze.orders a row with no ordered_at, then re-fetch the ids"),
    ("pg_value_domain_", "A new KeyCRM id: check its status group, then register it in KNOWN_STATUS_IDS/KNOWN_SOURCE_IDS"),
    ("pg_status_group_vs_return_list", "The source's status group wins over the legacy list"),
    ("pg_order_landing_unwatched",
     "The Postgres twins did not read bronze.orders: read the reason in the finding"),
    # The canary's, not a check's: a backup marker past its limit (S14-00).
    ("backup_evidence_stale",
     "On the host run the script named: deploy/pg_pitr_drill.sh, "
     "deploy/pg_restore_drill.sh --from-remote or deploy/pg_offsite.sh"),
    ("inventory_snapshot_gaps", "A missed day is gone for good; check the snapshot job"),
    ("ch_reconcile_pending", "Wait for a fresh ch_sync — never reconcile a lagging copy"),
    # OD-08 (a). The generic `gold_` line would send the reader to a rebuild,
    # which re-aggregates nothing in a second engine.
    ("gold_values_unwatched",
     "Set KS_CH_URL, or clear the ch_* finding beside it; then "
     "POST /api/jobs/dq_mirror_landing/trigger to re-check Gold"),
)

DEFAULT_REMEDIATION = (
    "No lever written down — see CLAUDE.md «How a failure reaches a human», add one"
)

# Что находка значит для читателя — по-русски, коротко. Имя проверки в
# скобках остаётся ключом для поиска; без перевода алерт говорит на языке
# кода, а страница, которую не понять за три секунды, — шум.
HUMAN_CHECK_NAMES: Dict[str, str] = {
    "mirror_missing_rows": "rows missing from the copy",
    "mirror_orphan_rows": "extra rows in the copy",
    "mirror_row_values": "rows differ between copies",
    "mirror_retired_rows": "retired in KeyCRM, copy remembers",
    "mirror_pruned_rows": "aged out of DuckDB, copy still holds them",
    "inventory_continuity_unwatched": "snapshot gaps no longer watched",
    "sync_watermarks_unwatched": "sync stalls no longer watched",
    "chain_latch_disagrees": "the two copies of a chain's latch disagree",
    "integrity_check_raised": "integrity checks crashed",
    "mirror_never_shipped": "table never shipped",
    "mirror_backfill_pending": "history not carried over yet",
    "mirror_failing": "mirror failing",
    "pg_silver_missing_rows": "orders missing from Postgres Silver",
    "pg_silver_orphan_rows": "ghost rows in Postgres Silver",
    "pg_attribution_coverage_website": "website orders lost their tags (Postgres)",
    "pg_headline_vs_line_items": "order total ≠ line items (Postgres)",
    "pg_goods_shipped_without_sale": "shipments without a sale (Postgres)",
    "pg_silver_arc_unwatched": "Postgres Silver not examined",
    "pg_attribution_coverage_unwatched": "Postgres attribution not examined",
    "pg_line_items_unwatched": "Postgres line items not examined",
    "pg_silver_row_values": "stale values in Postgres Silver",
    "pg_silver_row_values_unwatched": "Postgres Silver values not examined",
    "pg_signal_missed": "orders written with no derivation mark",
    "pg_derivation_signal_unwatched": "derivation journal not examined",
    "pg_orders_without_line_items": "orders without line items (Postgres)",
    "pg_fk_orphan_order_products_order_id": "line items with no order (Postgres)",
    "pg_not_null_orders_ordered_at": "orders with no date (Postgres)",
    "pg_value_domain_orders_status_id": "unknown order status (Postgres)",
    "pg_value_domain_orders_source_id": "unknown order source (Postgres)",
    "pg_status_group_vs_return_list": "status group ≠ return list (Postgres)",
    "pg_order_landing_unwatched": "Postgres order landing not examined",
    "pg_warehouse_unwatched": "Postgres twins did not look",
    "pg_order_utm_missing": "orders with no traffic verdict (Postgres)",
    "pg_order_utm_stale": "traffic verdict older than its order (Postgres)",
    "pg_order_utm_in_flight": "traffic verdicts still on their way",
    "buyers_without_verdict": "buyers with no gender verdict",
    "buyers_missing_for_orders": "orders whose buyer never arrived",
    "order_versions_stalled": "version archive went silent",
    "ch_engines_gold_mismatch": "two engines' Gold differ",
    "ch_silver_roundtrip": "ClickHouse copy differs",
    "ch_reconcile_pending": "ClickHouse copy lagging",
    "gold_values_unwatched": "Gold not re-checked by a second engine",
    "freshness_orders": "orders not arriving",
    "chain_managers_empty": "no managers in Postgres",
    "chain_manager_open_interval": "a manager without exactly one classification in force",
    "chain_manager_unclassified": "managers with no classification",
    "chain_manager_intervals_broken": "a manager's classification history overlaps or gaps",
    "chain_manager_retail_disagrees": "a manager's retail flag ≠ its classification",
}


def human_check_name(name: str) -> str:
    label = HUMAN_CHECK_NAMES.get(name)
    return f"{label} ({name})" if label else name


def remediation_for(check_names: Iterable[str]) -> List[str]:
    """The distinct "what to do" lines for a set of check names.

    Longest prefix wins, so `mirror_never_shipped` gets its own sentence
    rather than the generic `mirror_` one. Deduplicated and order-stable: an
    alert naming eight `fk_orphan_*` checks should say the one useful thing
    once. Pure since step 14's S14-15, which removed the `KS_UTM_PARSE` mode
    it read to pick the `pg_order_utm_` line.
    """
    lines: List[str] = []
    for name in check_names:
        best = ""
        chosen = DEFAULT_REMEDIATION
        for prefix, line in REMEDIATION:
            if name.startswith(prefix) and len(prefix) > len(best):
                best, chosen = prefix, line
        if chosen not in lines:
            lines.append(chosen)
    return lines


def machine_attempts_note(now: Optional[datetime] = None) -> Optional[str]:
    """What the self-healing machinery has already tried in the last 24 h.

    Rule 5 of the charter: an alert must not *trigger* a repair, but it owes
    the reader the count. Without it the honest reading of a CRITICAL is "and
    nothing is being done", which was false for every mirror finding — the
    hourly ids-diffs had usually already re-shipped rows by the time the
    07:30 comparison spoke.

    Reads the in-process heal ledger of the ClickHouse archive. Returns None
    when it has not run, which is the common case and must not produce an
    empty line. The buyers' ledger (`core.pg_buyers`, the buyers ids-diff)
    was the other until step 14's S14-13 deleted the diff with the module.
    """
    reference = now or datetime.now(timezone.utc)
    parts: List[str] = []
    for label, module in (
        ("archive", "core.ch_history"),
    ):
        try:
            heal = importlib.import_module(module).last_heal
        except Exception:  # pragma: no cover - import failure is not an alert
            continue
        if not heal or "at" not in heal:
            continue
        age = (reference - heal["at"]).total_seconds()
        if 0 <= age < 24 * 3600:
            parts.append(
                f"{label}: auto re-shipped {int(heal.get('shipped', 0))} "
                f"rows {int(age // 60)}m ago"
            )
    if not parts:
        return None
    return "🤖 " + "; ".join(parts)


def format_alert_message(
    layer: str,
    severity: Severity,
    issues: List[IntegrityIssue],
    discrepancies: List[Discrepancy],
    *,
    window: Optional[Tuple[date, date]] = None,
    max_lines: int = 12,
    machine_note: Optional[str] = None,
) -> str:
    """Три строки, по-русски: что случилось · сколько · что делать.

    Формат — ответ на прямую правку владельца 30.08: «коротко — ясно и по
    сути». Обоснования рычагов живут в CLAUDE.md; детали — в журнале и у
    агента-диагноста, который приходит вторым сообщением. Чистая функция
    (режим `KS_UTM_PARSE`, выбиравший рычаг находок `pg_order_utm_`, ушёл в
    S14-15 шага 14).

    Пример:
        🚨 <b>Сверка копий: строки расходятся между копиями — 891</b>
        • товар снят в KeyCRM, копия помнит — 1
        → Жди часовой пере-шип; смотри meta.mirror_state
    """
    icon = {"CRITICAL": "🚨", "WARN": "⚠️", "INFO": "ℹ️"}[severity.value]
    titles = {
        "integrity": "Integrity",
        "mirror_landing": "Mirror check",
        "reconciliation_pg": "Postgres vs KeyCRM",
        "reconciliation_ch": "ClickHouse vs KeyCRM",
    }
    title = titles.get(layer, layer)

    lines: List[str] = []
    shown = sorted(issues, key=lambda i: (-i.severity.rank(), -i.count))
    if shown:
        first = shown[0]
        head = f"{icon} <b>{title}: {human_check_name(first.check_name)} — {first.count}</b>"
        lines.append(head)
        for i in shown[1:4]:
            lines.append(f"• {human_check_name(i.check_name)} — {i.count}")
        if len(shown) > 4:
            lines.append(f"• …+{len(shown) - 4} more")
    elif discrepancies:
        total_ids = sum(len(d.order_ids) for d in discrepancies)
        lines.append(
            f"{icon} <b>{title}: {len(discrepancies)} mismatches, "
            f"~{total_ids or len(discrepancies)} orders</b>"
        )
        for d in discrepancies[:3]:
            lines.append(
                f"• {d.month}/src{d.source_id} {d.field}: "
                f"{d.dk_value:.0f}≠{d.kc_value:.0f}"
            )
        if len(discrepancies) > 3:
            lines.append(f"• …+{len(discrepancies) - 3} more")
    else:
        lines.append(f"{icon} <b>{title}: {severity.value}</b>")

    if severity is not Severity.INFO:
        actions = remediation_for(i.check_name for i in shown)
        if not actions and discrepancies:
            actions = ["Hand-check one order against KeyCRM before any rebuilds"]
        for a in actions[:2]:
            lines.append(f"→ {a}")
        if machine_note:
            lines.append(machine_note)

    return "\n".join(lines)


_RUN_SELECT = """
        SELECT run_id, started_at, ended_at, as_of, window_start, window_end,
               layer, status, integrity_issues_count, discrepancies_count,
               critical_count, warn_count, api_calls_used, duration_ms, error_message
        FROM {runs}"""

# By id, not by `started_at`. The baseline (`pg_dq_journal_write.baseline_run`)
# bounds on `run_id`, and a reconstruction that ranks by one key while its
# bound uses another is right only while the two never disagree. Verified on
# all 350 production rows: identical ranking today.
LATEST_RUN_SQL = _RUN_SELECT + """
        ORDER BY run_id DESC
        LIMIT 1
    """
LATEST_RUN_OF_LAYER_SQL = _RUN_SELECT + """
        WHERE layer = ?
        ORDER BY run_id DESC
        LIMIT 1
    """
RUN_BY_ID_SQL = _RUN_SELECT + " WHERE run_id = ?"


def run_dict(row) -> Optional[Dict[str, Any]]:
    """One run row, shaped for the health endpoint, the digest and the catch-up."""
    if not row:
        return None
    return {
        "run_id": int(row[0]),
        "started_at": row[1].isoformat() if row[1] else None,
        "ended_at": row[2].isoformat() if row[2] else None,
        "as_of": row[3].isoformat() if row[3] else None,
        "window_start": row[4].isoformat() if row[4] else None,
        "window_end": row[5].isoformat() if row[5] else None,
        "layer": row[6],
        "status": row[7],
        "integrity_issues_count": int(row[8] or 0),
        "discrepancies_count": int(row[9] or 0),
        "critical_count": int(row[10] or 0),
        "warn_count": int(row[11] or 0),
        "api_calls_used": int(row[12] or 0),
        "duration_ms": int(row[13] or 0),
        "error_message": row[14],
    }


# Layers a run-age watchdog is expected to see. A layer that never appears
# here would be watched by nobody.
# The layers with a cadence, a digest section and an age. `mirror_landing` is
# the 07:30 job (it was step 05's Reconciliation A, Postgres against DuckDB,
# until step 14's S14-12), and it belongs here for three separate reasons: the digest
# renders one section per layer, `dq_journal.last_success_ages_pg` reports one
# age per layer, and the scheduler's catch-up reads those ages to decide what
# it missed.
# A layer absent from this tuple has a null age forever, which the catch-up
# reads as "never succeeded" and re-queues on every single restart.
#
# It was opted into `bot/canary.py`'s `DQ_MAX_AGE_S` on 28.08, once the layer
# had a track record and had grown the step-2/5/6 comparisons; the worry it
# was held back for — the canary's first probe firing 90 s after a restart,
# before the catch-up run finishes — only bites when the layer is already past
# 30 h at that restart, which is a genuine outage worth one page.
#
# `reconciliation`, DuckDB's arm of the 05:30 job, left in step 14's S14-12:
# it stood down from chain 3's flip (2026-10-15) and was written by nothing,
# so its age was published marked `stood_down` and judged by nobody.
WATCHED_LAYERS: Tuple[str, ...] = (
    "integrity", "mirror_landing", "reconciliation_pg",
    # The third arm of the source reconciliation: ClickHouse against the same
    # 05:30 snapshot. Its own layer for reconciliation_pg's reason — an arm
    # that stops running must not hide behind a fresh sibling.
    "reconciliation_ch",
)


# ─── Evidence for the diagnostician ─────────────────────────────────────────
#
# The alert body carries a count. The finding carries the answer — its
# `description` names the offending columns, and on 2026-09-01 that one line
# ("Columns: updated_at (891), reserve (3), last_sale_date (3)") was the whole
# diagnosis, sitting in `data_quality_issues` where nothing could reach it.
#
# The agent's runbook told it so: the DQ runs live in DuckDB, DuckDB is held
# by the web container, and the agent's allowlist is read-only shell —
# `psql -U ks_readonly`, `docker logs`, `curl` — with nothing that executes
# code. It was sent to /api/health instead, which publishes ages and no
# findings, and it twice produced a plausible, wrong story and advised
# waiting.
#
# The answer is not to widen what the agent may run. It is to put the evidence
# where its existing SELECT already reaches: `app.alert_events.context`, a
# column revision 0012 created for exactly this and nothing has ever written.

# Bounds, because this row is read by a person at 03:00 and by an agent on a
# 300-second budget. Neither is served by an unbounded blob, and a run with
# three thousand findings must not turn one INSERT into a megabyte.
EVIDENCE_MAX_FINDINGS = 12
EVIDENCE_MAX_DETAIL_CHARS = 400
EVIDENCE_MAX_BYTES = 8192


def evidence_for_agent(
    layer: str,
    issues: Sequence[IntegrityIssue],
    discrepancies: Sequence[Discrepancy] = (),
    *,
    run_id: Optional[int] = None,
) -> dict:
    """What the alert could not say, in the shape a SELECT can read.

    Ordered worst-first — CRITICAL before WARN, then by count — so that when
    the bounds bite it is the trailing noise that goes, never the finding the
    reader was sent here for.

    Pure: builds a dict and touches nothing. The caller decides whether it
    reaches the ledger, and a failure there must never cost an alert.
    """
    rank = {Severity.CRITICAL: 0, Severity.WARN: 1, Severity.INFO: 2}
    ordered = sorted(
        issues,
        key=lambda i: (rank.get(i.severity, 3), -i.count, i.check_name),
    )

    findings = [
        {
            "check": i.check_name,
            "table": i.table_name,
            "severity": i.severity.value,
            "count": i.count,
            # Already capped at ten by the emitters; listed because "which
            # rows" is the second question after "which columns".
            "samples": list(i.sample_ids),
            "detail": (i.description or "")[:EVIDENCE_MAX_DETAIL_CHARS],
        }
        for i in ordered[:EVIDENCE_MAX_FINDINGS]
    ]

    payload: dict = {"layer": layer, "findings": findings}
    if run_id is not None:
        # Lets the reader join back to data_quality_runs once somebody can
        # open DuckDB — the ledger row is a pointer, not a replacement.
        payload["run_id"] = run_id
    if len(issues) > len(findings):
        payload["findings_total"] = len(issues)

    if discrepancies:
        by_class: dict = {}
        for d in discrepancies:
            name = d.diff_class.value
            by_class[name] = by_class.get(name, 0) + 1
        payload["discrepancies"] = {"total": len(discrepancies), "by_class": by_class}

    # Belt and braces: drop findings from the tail until it fits. Worst-first
    # ordering is what makes this safe to do bluntly.
    import json as _json

    while findings and len(
        _json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ) > EVIDENCE_MAX_BYTES:
        findings.pop()
        payload["findings_total"] = len(issues)

    return payload


def alert_fingerprint(
    layer: str,
    severity: Severity,
    issues: List[IntegrityIssue],
    discrepancies: List[Discrepancy],
) -> str:
    """A stable key naming *which* problem this alert is about.

    Built from the identities of the findings — check names, and the
    (field, class) pairs of the discrepancies — never from their counts or
    amounts, which move on every run. Two runs reporting the same broken
    things get the same key and the second is throttled; a new kind of
    breakage gets a new key and is delivered at once, instead of hiding
    behind the cooldown of the one already being reported.
    """
    parts = sorted({i.check_name for i in issues})
    parts += sorted({f"{d.field}/{d.diff_class.value}" for d in discrepancies})
    body = ",".join(parts) if parts else "clean"
    return f"dq:{layer}:{severity.value}:{body}"


PREVIOUS_RUN_ID_SQL = """
        SELECT run_id FROM {runs}
        WHERE layer = ? AND run_id < ?
          AND error_message IS NULL AND status <> 'FAILED'
        ORDER BY run_id DESC
        LIMIT 1
    """

BASELINE_RUN_ID_SQL = """
            SELECT run_id FROM {runs}
            WHERE layer = ? AND run_id < ?
              AND error_message IS NULL AND status <> 'FAILED'
              AND ended_at IS NOT NULL AND ended_at <= ?
            ORDER BY run_id DESC
            LIMIT 1
        """


# ─── Daily digest ─────────────────────────────────────────────────────────────

# How stale a layer's newest verdict may be before the digest calls it out.
# One cycle plus grace, same reasoning as the canary's thresholds.
DIGEST_MAX_AGE_HOURS = {
    "integrity": 12,
    # Daily at 07:30, so 30h is one cycle plus six hours of grace — the
    # same arithmetic as the reconciliation arms below.
    "mirror_landing": 30,
    # The two arms of `dq_reconciliation` (daily 05:30), so they share that
    # job's cadence and therefore its limit. Each is a layer of its own so
    # that an arm which stopped cannot hide behind a fresh sibling.
    "reconciliation_ch": 30,
    "reconciliation_pg": 30,
}

# How long a standing finding may go unmentioned before the digest says it
# again. Long enough that a known problem stops being daily wallpaper, short
# enough that it cannot be forgotten.
DIGEST_RESTATE_AFTER = timedelta(days=7)


@dataclass
class DigestSection:
    """One layer's contribution to the daily digest."""
    layer: str
    run: Optional[Dict[str, Any]]           # newest successful run, if any
    issues: List[Dict[str, Any]] = field(default_factory=list)
    diffs: List[Dict[str, Any]] = field(default_factory=list)
    previous_issues: List[Dict[str, Any]] = field(default_factory=list)
    previous_diffs: List[Dict[str, Any]] = field(default_factory=list)
    age_hours: Optional[float] = None       # age of `run`, None if never ran


def _delta_note(check_name: str, count: int, previous: List[Dict[str, Any]]) -> str:
    """«новое», «=», или «+12».

    «=» вместо слова: дельта — самое частое, что читатель сканирует, и один
    символ читается быстрее слова. Логика `news` в build_digest сверяется с
    этим значением — меняются вместе.
    """
    for p in previous:
        if p["check_name"] == check_name:
            diff = count - int(p["count"])
            if diff == 0:
                return "="
            return f"{diff:+d}"
    return "new"


def _diff_signature(diffs: List[Dict[str, Any]]) -> frozenset:
    """What a set of discrepancies looks like, for telling two runs apart.

    Month, source, field and class say which drift it is; the two values say
    how far it has drifted. A run reporting the same drift at the same
    distance has told the reader nothing new.

    Measured over 2026-07-20…08-15, that case is rare: the recon numbers move
    on nearly every run while the warehouse catches up, and each morning is
    genuinely a different figure. What this earns instead is the other
    direction — an empty set against a non-empty one, so a drift that healed
    is said out loud rather than left to be inferred from silence.

    Order and `order_ids` are left out: the sample is capped at `max_ids` and
    reshuffles between runs without anything actually moving.
    """
    return frozenset(
        (d["month"], d["source_id"], d["field"], d["diff_class"],
         round(float(d["dk_value"]), 2), round(float(d["kc_value"]), 2))
        for d in diffs
    )


def build_digest(
    sections: List[DigestSection],
    *,
    max_issue_lines: int = 8,
    max_diff_lines: int = 6,
    last_sent_at: Optional[datetime] = None,
    now: Optional[datetime] = None,
    restate_after: timedelta = DIGEST_RESTATE_AFTER,
) -> Optional[str]:
    """Render the daily digest, or None when there is nothing to say.

    WARN findings are persisted on every run and alerted on none — only
    CRITICAL is pushed at the moment it happens. Two standing WARNs worth
    ₴5.6M had therefore never been said out loud to anyone. This is the
    surface for them: with a delta so a growing problem reads differently
    from a known one.

    "Nothing to say" includes a standing finding that has not moved. The
    delta was already being computed and then ignored by the send decision,
    so `headline_vs_line_items: 414 (unchanged)` went out every morning from
    2026-08-10 to 08-15 — the exact wallpaper this module refuses to make of
    INFO findings, applied to WARN ones instead.

    A finding is news when it is new, when it moved, when it healed, or when
    a layer went stale or silent; otherwise the reader already knows, and
    `restate_after` brings it back once a week so known does not decay into
    forgotten. Note what this does not do: nothing here decides a finding is
    unimportant. A number that moves by one is news the same morning it
    moves, and a layer that stopped running is news every morning it stays
    down — replayed over 2026-07-20…08-15, the only mornings this silences
    are the six above.

    `last_sent_at` is when the digest last actually reached a human, and
    `now` defaults to the current time. Passing neither restates on every
    call, which is what a caller with no memory of previous sends deserves.

    Returns None when there is no news and nothing due for restatement, so a
    quiet day stays quiet. Liveness is the canary's job, not the digest's.
    """
    body: List[str] = []
    news = False           # something a reader does not already know
    standing = False       # a finding worth restating on the weekly beat

    for s in sorted(sections, key=lambda x: x.layer):
        if s.run is None:
            body.append(f"<b>{s.layer}</b> — no successful run")
            news = True
            continue

        limit = DIGEST_MAX_AGE_HOURS.get(s.layer)
        stale = limit is not None and s.age_hours is not None and s.age_hours > limit
        when = (s.run.get("started_at") or "")[:16].replace("T", " ")
        head = f"<b>{s.layer}</b> · {s.run.get('status')} · {when}"
        if stale:
            head += f" · ⏳ silent {s.age_hours:.0f}h"
            news = True
        body.append(head)

        if s.issues:
            # INFO findings are content, never a reason to write. Goods shipped
            # to bloggers is a number worth reading beside a real problem and
            # not worth a message of its own — a digest that arrives every day
            # regardless is one that stops being read on the day it matters.
            for i in s.issues[:max_issue_lines]:
                note = _delta_note(i["check_name"], int(i["count"]), s.previous_issues)
                if i.get("severity") != "INFO":
                    standing = True
                    if note != "=":
                        news = True
                # Одна строка на находку, человеческим именем и с дельтой.
                # 200-символьные описания владелец назвал визуальным шумом —
                # их дом теперь журнал и дашборд, не телефон.
                body.append(
                    f"• {human_check_name(i['check_name'])}: "
                    f"{i['count']:,} ({note})"
                )
            if len(s.issues) > max_issue_lines:
                body.append(f"  …+{len(s.issues) - max_issue_lines} more")
                # A finding past the cut has no line and so no delta of its
                # own. Suppressing on a "quiet" the reader cannot see would be
                # a guess; say the digest and let them scroll.
                if any(i.get("severity") != "INFO" for i in s.issues[max_issue_lines:]):
                    news = True

        # Compared even when the list is empty today: a drift that healed is
        # the one change a reader must not have to infer from the absence of
        # a message. Safe to trust here in a way a vanished *issue* is not —
        # failed runs never reach the digest, and the recon layer reports its
        # discrepancies as one set, where an integrity check can be skipped
        # out of a run that still succeeds and read as fixed.
        if _diff_signature(s.diffs) != _diff_signature(s.previous_diffs):
            news = True

        if s.diffs:
            standing = True
            for d in s.diffs[:max_diff_lines]:
                body.append(
                    f"• {d['month']} / src={d['source_id']}: {d['field']} "
                    f"DK={d['dk_value']:,.0f} KC={d['kc_value']:,.0f} ({d['diff_class']})"
                )
            if len(s.diffs) > max_diff_lines:
                body.append(f"  …+{len(s.diffs) - max_diff_lines} more")

        if not s.issues and not s.diffs and not stale:
            body.append("• clean")

    if news:
        return "\n".join(["📋 <b>Data quality</b>", ""] + body)

    if not standing:
        return None

    now = now or datetime.now(timezone.utc)
    if last_sent_at is not None:
        since = last_sent_at
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        if now - since < restate_after:
            return None
        days = max(1, int((now - since).days))
        footer = f"<i>Unchanged for {days}d; weekly restatement.</i>"
    else:
        footer = "<i>Standing findings; quiet on unchanged days.</i>"
    return "\n".join(["📋 <b>Data quality</b>", ""] + body + ["", footer])


def last_success_sql(n_layers: int, engine: str = "postgres") -> str:
    """The layer ages' statement for `n_layers` layers
    (`pg_dq_journal_write.last_success_ages`)."""
    placeholders = ", ".join("?" for _ in range(n_layers))
    return journal_sql(f"""
        SELECT layer,
               MAX(started_at) AS last_success_at,
               EXTRACT(EPOCH FROM (now() - MAX(started_at))) AS age_seconds
        FROM {{runs}}
        WHERE layer IN ({placeholders})
          AND error_message IS NULL
          AND status <> 'FAILED'
        GROUP BY layer
    """, engine)


def success_ages(rows, layers: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """`{layer: {last_success_at, age_seconds}}`, one entry per requested
    layer — a layer that never succeeded reads both None
    (`dq_journal.last_success_ages_pg` says why)."""
    found = {
        r[0]: {
            "last_success_at": r[1].isoformat() if r[1] else None,
            "age_seconds": int(r[2]) if r[2] is not None else None,
        }
        for r in rows
    }
    return {
        layer: found.get(layer, {"last_success_at": None, "age_seconds": None})
        for layer in layers
    }
