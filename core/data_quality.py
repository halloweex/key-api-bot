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

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

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

# Known order statuses (KeyCRM-defined; new IDs require explicit registration
# so we don't silently accept upstream changes).
# Verified against KeyCRM on 2026-08-09. Statuses 3, 4, 8, 10, 11, 18 and 24
# were missing, so a perfectly ordinary parcel «Зібрано для самовивозу» (24)
# would have been reported as an unknown status. Status 20 was listed under
# "return/cancel family" and is nothing of the kind — it is «Прибув у
# відділення», KeyCRM group 4, and it belongs to revenue. That comment cost
# two sessions a phantom ₴265,230.78 discrepancy.
KNOWN_STATUS_IDS = frozenset({
    1,   # new
    2,   # presence_confirmed
    3,   # waiting_for_email_response
    4,   # waiting_for_prepayment
    8,   # delivered_to_delivery
    9,   # delivered
    10,  # departing
    11,  # in_transit
    12,  # completed
    20,  # Прибув у відділення
    24,  # Зібрано для самовивозу
    # Lost / cancel group (KeyCRM status_group_id = 6) — excluded from revenue
    15,  # not_available
    18,  # did_not_arrange_price
    19,  # canceled
    21,  # Помилка доставки
    22,  # Повернено
    23,  # Повертається
})

# Known sources (active + deprecated). New source IDs from KeyCRM should
# fire an integrity warning so we route them explicitly.
KNOWN_SOURCE_IDS = frozenset({1, 2, 3, 4, 5})


def _pk_uniqueness_check(conn, table: str, pk_col: str = "id") -> List[IntegrityIssue]:
    """Verify PK column has no duplicates. DuckDB enforces PK constraints
    at INSERT time but historic data from upserts before the constraint
    existed can still violate.
    """
    row = conn.execute(
        f'SELECT COUNT(*) - COUNT(DISTINCT "{pk_col}") FROM "{table}"'
    ).fetchone()
    dupes = int(row[0] or 0)
    if dupes == 0:
        return []
    samples = conn.execute(f"""
        SELECT "{pk_col}"
        FROM "{table}"
        GROUP BY "{pk_col}"
        HAVING COUNT(*) > 1
        LIMIT 10
    """).fetchall()
    return [IntegrityIssue(
        check_name=f"pk_uniqueness_{table}",
        table_name=table,
        severity=Severity.CRITICAL,
        count=dupes,
        sample_ids=tuple(int(r[0]) for r in samples if r[0] is not None),
        description=f"{dupes} duplicate {pk_col} value(s) in {table}",
    )]


def _fk_orphan_check(
    conn,
    child_table: str,
    child_fk: str,
    parent_table: str,
    parent_pk: str = "id",
) -> List[IntegrityIssue]:
    """Verify every child.fk has a matching parent.pk."""
    row = conn.execute(f"""
        SELECT COUNT(*) FROM "{child_table}" c
        WHERE c."{child_fk}" IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM "{parent_table}" p WHERE p."{parent_pk}" = c."{child_fk}"
          )
    """).fetchone()
    orphans = int(row[0] or 0)
    if orphans == 0:
        return []
    samples = conn.execute(f"""
        SELECT DISTINCT c."{child_fk}" FROM "{child_table}" c
        WHERE c."{child_fk}" IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM "{parent_table}" p WHERE p."{parent_pk}" = c."{child_fk}"
          )
        LIMIT 10
    """).fetchall()
    return [IntegrityIssue(
        check_name=f"fk_orphan_{child_table}_{child_fk}",
        table_name=child_table,
        severity=Severity.CRITICAL,
        count=orphans,
        sample_ids=tuple(int(r[0]) for r in samples if r[0] is not None),
        description=(
            f"{orphans} {child_table}.{child_fk} value(s) reference "
            f"non-existent {parent_table}.{parent_pk}"
        ),
    )]


def _null_constraint_check(
    conn, table: str, column: str, severity: Severity = Severity.CRITICAL,
) -> List[IntegrityIssue]:
    """Flag NULL values in columns that must be populated for analytics."""
    row = conn.execute(
        f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" IS NULL'
    ).fetchone()
    nulls = int(row[0] or 0)
    if nulls == 0:
        return []
    return [IntegrityIssue(
        check_name=f"not_null_{table}_{column}",
        table_name=table,
        severity=severity,
        count=nulls,
        description=f"{nulls} row(s) with NULL {column} in {table}",
    )]


def _value_domain_check(
    conn, table: str, column: str, known_set: frozenset, severity: Severity,
) -> List[IntegrityIssue]:
    """Flag values that are not in the registered known domain."""
    known_list = ", ".join(str(v) for v in sorted(known_set))
    row = conn.execute(f"""
        SELECT COUNT(*), STRING_AGG(DISTINCT CAST("{column}" AS VARCHAR), ',')
        FROM "{table}"
        WHERE "{column}" NOT IN ({known_list})
    """).fetchone()
    unknown = int(row[0] or 0)
    if unknown == 0:
        return []
    unknown_vals = (row[1] or "")[:200]
    return [IntegrityIssue(
        check_name=f"value_domain_{table}_{column}",
        table_name=table,
        severity=severity,
        count=unknown,
        description=(
            f"{unknown} row(s) in {table} have {column} not in known set "
            f"{{{known_list}}}. Unknown values seen: {unknown_vals}"
        ),
    )]


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


def _freshness_check(conn, now: Optional[datetime] = None) -> List[IntegrityIssue]:
    """Flag sync_metadata entities whose last_sync_* is older than its threshold.

    Catches silent sync-pipeline stalls — e.g. categories/expense_types going
    45 days stale while orders kept current — which neither the warehouse
    validation nor the reconciliation checks cover.
    """
    issues: List[IntegrityIssue] = []
    if now is None:
        now = datetime.now().astimezone()

    rows = conn.execute(
        "SELECT key, value FROM sync_metadata WHERE key LIKE 'last_sync_%'"
    ).fetchall()
    seen = {k[len("last_sync_"):]: v for k, v in rows}
    # No sync history at all → fresh/bootstrap install, not a stall. Skip to
    # avoid spurious "never synced" issues before the first sync completes.
    if not seen:
        return []

    for entity, (max_hours, sev) in FRESHNESS_THRESHOLDS.items():
        raw = seen.get(entity)
        if not raw:
            issues.append(IntegrityIssue(
                check_name=f"freshness_{entity}",
                table_name=entity,
                severity=sev,
                count=1,
                description=f"sync_metadata has no last_sync_{entity} — entity never synced",
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
                ),
            ))
    return issues


# How far back the inventory continuity check looks. Long enough that a hole
# is still visible after a few days of not reading Telegram; short enough that
# gaps which are already permanent stop being reported forever, since there is
# nothing left to do about them.
INVENTORY_CONTINUITY_WINDOW_DAYS = 30


def _inventory_snapshot_continuity_check(
    conn, now: Optional[datetime] = None,
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

    row = conn.execute(
        "SELECT MIN(date), MAX(date) FROM inventory_sku_history"
    ).fetchone()
    if not row or row[0] is None:
        # No history at all → fresh install, not a stall. Same reasoning as
        # _freshness_check: do not accuse a bootstrap of losing data.
        return []

    first_day = row[0]
    # A snapshot cannot be missing from before the first one ever taken.
    window_start = max(first_day, today - timedelta(days=INVENTORY_CONTINUITY_WINDOW_DAYS))
    if window_start > yesterday:
        return []

    missing = [r[0] for r in conn.execute(
        """
        WITH cal AS (
            SELECT UNNEST(GENERATE_SERIES(?::DATE, ?::DATE, INTERVAL 1 DAY))::DATE AS d
        )
        SELECT cal.d
        FROM cal
        LEFT JOIN (SELECT DISTINCT date FROM inventory_sku_history) h ON h.date = cal.d
        WHERE h.date IS NULL
        ORDER BY cal.d
        """,
        [window_start, yesterday],
    ).fetchall()]

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


def _orders_without_line_items_check(
    conn, severity: "Severity" = None, min_total: float = 0.01,
) -> List[IntegrityIssue]:
    """Flag orders that carry revenue but hold no line items at all.

    An order billed for money must have sold something. When the header lands
    and the products do not, every revenue figure is right and every product,
    brand and category figure is short — and the order looks complete, so no
    delta sync ever goes back for it. 552 orders were in this state as of
    2026-08, worth ₴1,422,610.30, 437 of them from a single month.
    """
    severity = severity or Severity.WARN
    row = conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(o.grand_total), 0)
        FROM orders o
        LEFT JOIN (SELECT DISTINCT order_id FROM order_products) li
               ON li.order_id = o.id
        WHERE li.order_id IS NULL AND o.grand_total >= ?
    """, [min_total]).fetchone()
    count = int(row[0] or 0)
    if count == 0:
        return []
    sample = tuple(r[0] for r in conn.execute("""
        SELECT o.id FROM orders o
        LEFT JOIN (SELECT DISTINCT order_id FROM order_products) li
               ON li.order_id = o.id
        WHERE li.order_id IS NULL AND o.grand_total >= ?
        ORDER BY o.grand_total DESC LIMIT 10
    """, [min_total]).fetchall())
    return [IntegrityIssue(
        check_name="orders_without_line_items",
        table_name="order_products",
        severity=severity,
        count=count,
        sample_ids=sample,
        description=(
            f"{count} order(s) worth {float(row[1] or 0):,.2f} have no line "
            "items. Revenue counts them, product/brand/category breakdowns "
            "cannot. Repairable by re-fetching those ids from KeyCRM."
        ),
    )]

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


def _headline_vs_line_items_check(
    conn, severity: "Severity" = None,
) -> List[IntegrityIssue]:
    """Flag completed orders billed at zero that still carry line items.

    KeyCRM lets an order be saved with grand_total = 0 while its line items
    stand. Revenue reads grand_total; the product, brand and category pages
    read line items. The two therefore disagree by exactly these orders —
    **418 of them, ₴865,918**, measured against the nightly backup on
    2026-08-20 — and nothing said so.

    That figure was misquoted as ₴4.16M across 1 177 orders from 2026-08-09
    onward, including in the list of decisions waiting on the owner. It is this
    same query with `internal` left in, and `internal` alone is 780 orders and
    ₴3.37M of deliberate blogger seeding — already counted, as INFO, by
    `_goods_shipped_without_sale_check`. Four fifths of the alarming number was
    work behaving exactly as intended.

    The "invoiced separately" reading is unsupported: none of the 418 has a
    sibling order from the same buyer at a matching amount within 14 days.
    Switching the product pages to line items would therefore not double count
    — but nothing shows the money arrived either. Three quarters of the value
    is the wholesale manager on Telegram, and the share of line-item value is
    growing: 0.49 % (2024), 0.61 % (2025), 0.93 % (2026).

    The gap is created upstream and cannot be fixed here, but it can stop being
    invisible.

    `internal` is excluded and counted separately by
    `_goods_shipped_without_sale_check`: shipping product with no money owed
    is that role's job, not a fault, and a check that reports intended work as
    a defect is one people stop reading.
    """
    severity = severity or Severity.WARN
    row = conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(li.amount), 0)
        FROM silver_orders s
        JOIN (SELECT order_id, SUM(price_sold * quantity) AS amount
              FROM order_products GROUP BY order_id) li ON li.order_id = s.id
        WHERE NOT s.is_return AND s.is_active_source
          AND s.grand_total = 0 AND li.amount > 0
          AND s.sales_type <> 'internal'
    """).fetchone()
    count = int(row[0] or 0)
    if count == 0:
        return []
    amount = float(row[1] or 0)
    sample = tuple(r[0] for r in conn.execute("""
        SELECT s.id FROM silver_orders s
        JOIN (SELECT order_id, SUM(price_sold * quantity) AS amount
              FROM order_products GROUP BY order_id) li ON li.order_id = s.id
        WHERE NOT s.is_return AND s.is_active_source
          AND s.grand_total = 0 AND li.amount > 0
          AND s.sales_type <> 'internal'
        ORDER BY li.amount DESC LIMIT 10
    """).fetchall())
    return [IntegrityIssue(
        check_name="headline_vs_line_items",
        table_name="silver_orders",
        severity=severity,
        count=count,
        sample_ids=sample,
        description=(
            f"{count} order(s) have grand_total = 0 but {amount:,.2f} in line "
            "items. Revenue figures read grand_total and product/brand/category "
            "figures read line items, so the two disagree by this amount."
        ),
    )]


def _status_group_agreement_check(
    conn, severity: "Severity" = None,
) -> List[IntegrityIssue]:
    """Compare KeyCRM's own status grouping against our hardcoded id list.

    Revenue excludes the lost/cancel group. We learn that group from the
    order payload now, but a hardcoded list of status ids still backs up rows
    synced before the column existed — and a list can only describe the
    statuses that existed when it was written. Status 20 appeared on
    2026-07-09 and nothing noticed for a month.

    This is the check that would have. It says nothing while the two agree,
    and names the status the moment they part company.
    """
    from core.models import LOST_STATUS_GROUP_ID, OrderStatus

    severity = severity or Severity.WARN
    listed = tuple(int(s) for s in OrderStatus.return_statuses())
    listed_sql = ", ".join(str(s) for s in listed)
    rows = conn.execute(f"""
        SELECT status_id, status_group_id, COUNT(*) AS n,
               COALESCE(SUM(grand_total), 0) AS amount
        FROM orders
        WHERE status_group_id IS NOT NULL
          AND (status_group_id = {LOST_STATUS_GROUP_ID})
              <> (status_id IN ({listed_sql}))
        GROUP BY status_id, status_group_id
        ORDER BY amount DESC
    """).fetchall()
    if not rows:
        return []

    total = sum(int(r[2]) for r in rows)
    amount = sum(float(r[3]) for r in rows)
    detail = "; ".join(
        f"status {int(r[0])} is KeyCRM group {int(r[1])} but our list says "
        f"{'excluded' if int(r[0]) in listed else 'revenue'} "
        f"({int(r[2])} orders, {float(r[3]):,.2f})"
        for r in rows[:5]
    )
    return [IntegrityIssue(
        check_name="status_group_vs_return_list",
        table_name="orders",
        severity=severity,
        count=total,
        sample_ids=tuple(int(r[0]) for r in rows[:10]),
        description=(
            f"{total} order(s) worth {amount:,.2f} are classified differently by "
            f"KeyCRM's status group and by our status-id list. {detail}. "
            "The group is the source's own answer; the list is our copy of it."
        ),
    )]


def _goods_shipped_without_sale_check(
    conn, severity: "Severity" = None,
) -> List[IntegrityIssue]:
    """Count product that left with no money attached — deliberately.

    An influence manager ships cosmetics to bloggers: the order carries line
    items and a grand_total of zero, forever. That is the job, not a fault,
    which is why these orders are kept out of `headline_vs_line_items` — but
    the amount is worth knowing. It is marketing spend measured at the price
    the goods would otherwise have sold for, and until now nobody counted it.

    INFO on purpose: it belongs in the daily digest as a figure, never as a
    warning about something needing repair.
    """
    severity = severity or Severity.INFO
    row = conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(li.amount), 0)
        FROM silver_orders s
        JOIN (SELECT order_id, SUM(price_sold * quantity) AS amount
              FROM order_products GROUP BY order_id) li ON li.order_id = s.id
        WHERE NOT s.is_return AND s.is_active_source
          AND s.grand_total = 0 AND li.amount > 0
          AND s.sales_type = 'internal'
    """).fetchone()
    count = int(row[0] or 0)
    if count == 0:
        return []
    amount = float(row[1] or 0)
    sample = tuple(r[0] for r in conn.execute("""
        SELECT s.id FROM silver_orders s
        JOIN (SELECT order_id, SUM(price_sold * quantity) AS amount
              FROM order_products GROUP BY order_id) li ON li.order_id = s.id
        WHERE NOT s.is_return AND s.is_active_source
          AND s.grand_total = 0 AND li.amount > 0
          AND s.sales_type = 'internal'
        ORDER BY li.amount DESC
        LIMIT 10
    """).fetchall())
    return [IntegrityIssue(
        check_name="goods_shipped_without_sale",
        table_name="silver_orders",
        severity=severity,
        count=count,
        sample_ids=sample,
        description=(
            f"{count} internal order(s) shipped {amount:,.2f} of goods with no "
            "sale attached — seeding and gifts, priced at what they would have "
            "sold for. Expected, and counted here so it is a number rather "
            "than a silence."
        ),
    )]


# Every Gold column the rebuild writes, and how closely a recomputed value has
# to match. Counts must be exact; money is DECIMAL(12,2) and avg_order_value is
# a division, so both get a cent of slack.
_GOLD_CELL_COLUMNS = (
    ("revenue", 0.01),
    ("orders_count", 0),
    ("unique_customers", 0),
    ("new_customers", 0),
    ("returning_customers", 0),
    ("instagram_revenue", 0.01),
    ("telegram_revenue", 0.01),
    ("shopify_revenue", 0.01),
    ("instagram_orders", 0),
    ("telegram_orders", 0),
    ("shopify_orders", 0),
    ("returns_count", 0),
    ("returns_revenue", 0.01),
    ("avg_order_value", 0.01),
)


def _gold_cell_values_check(
    conn, severity: "Severity" = None, max_samples: int = 10,
) -> List[IntegrityIssue]:
    """Recompute every Gold cell from Silver and compare all fourteen columns.

    The warehouse validation asserts three scalars and, since the cell guard,
    that the *set* of (date, sales_type) cells matches. It says nothing about
    thirteen of the fourteen values inside a cell: order counts, unique and
    new and returning customers, per-source revenue and orders, returns,
    average order value. PR #41 was a bug in exactly one of those columns —
    the new-customer baseline — and it was found by eye.

    Deliberately **report-only**, and placed here rather than inline in the
    refresh to make that structural. Integrity findings never touch
    `validation_passed`, so this cannot drive `mark_warehouse_dirty` and it
    cannot start a rebuild loop: fourteen columns across ~2 000 cells failing
    every two minutes on a 7 GB host is the outcome the design debate vetoed.
    It runs with the rest of the integrity scan, four times a day, and reaches
    a human through the daily digest.

    What it cannot see: a lie that arrived from Bronze. Gold is rebuilt from
    Silver in the same tick, so a consistent wrong value is reproduced on both
    sides of this comparison. It covers rebuild faults and the unasserted
    columns — nothing more. Reconciliation against KeyCRM is the only check
    that sees the rest.
    """
    from core.duckdb_store import GOLD_REVENUE_SELECT_SQL

    severity = severity or Severity.WARN
    expected = GOLD_REVENUE_SELECT_SQL.format(date_filter="order_date IS NOT NULL")

    diffs = []
    for column, tolerance in _GOLD_CELL_COLUMNS:
        if tolerance:
            diffs.append(
                f"ABS(COALESCE(e.{column}, 0) - COALESCE(g.{column}, 0)) > {tolerance}"
            )
        else:
            diffs.append(f"COALESCE(e.{column}, -1) <> COALESCE(g.{column}, -1)")

    rows = conn.execute(f"""
        WITH expected AS ({expected})
        SELECT e.date, e.sales_type,
               {", ".join(f"e.{c} AS e_{c}, g.{c} AS g_{c}" for c, _ in _GOLD_CELL_COLUMNS)}
        FROM expected e
        JOIN gold_daily_revenue g
          ON g.date = e.date AND g.sales_type = e.sales_type
        WHERE {" OR ".join(diffs)}
        ORDER BY e.date DESC
        LIMIT ?
    """, [max_samples]).fetchall()

    if not rows:
        return []

    # Name the columns that actually moved, not just the cells.
    offenders: Dict[str, int] = {}
    for row in rows:
        for idx, (column, tolerance) in enumerate(_GOLD_CELL_COLUMNS):
            e_val, g_val = row[2 + idx * 2], row[3 + idx * 2]
            if e_val is None and g_val is None:
                continue
            differs = (
                abs(float(e_val or 0) - float(g_val or 0)) > tolerance
                if tolerance else (e_val != g_val)
            )
            if differs:
                offenders[column] = offenders.get(column, 0) + 1

    worst = ", ".join(
        f"{c} ({n})" for c, n in sorted(offenders.items(), key=lambda kv: -kv[1])[:5]
    )
    return [IntegrityIssue(
        check_name="gold_cell_values",
        table_name="gold_daily_revenue",
        severity=severity,
        count=len(rows),
        sample_ids=tuple(),
        description=(
            f"{len(rows)} Gold cell(s) disagree with a recompute from Silver "
            f"(showing at most {max_samples}). Columns: {worst}. "
            "Report only — Gold is rebuilt from Silver, so this catches rebuild "
            "faults and the columns nothing else asserts, not a wrong value that "
            "arrived from Bronze."
        ),
    )]


def check_internal_integrity(conn) -> List[IntegrityIssue]:
    """Run all Layer-1 integrity checks. Returns list of issues (empty = clean).

    Cheap by design: only DB scans, no external I/O. Suitable for running
    every few hours alongside the heavier reconciliation job.

    Adding new checks: follow the per-check function pattern so each is
    individually testable with a fixture DuckDB.
    """
    issues: List[IntegrityIssue] = []

    # PK uniqueness on critical tables.
    issues += _pk_uniqueness_check(conn, "orders", "id")
    issues += _pk_uniqueness_check(conn, "order_products", "id")
    issues += _pk_uniqueness_check(conn, "products", "id")
    issues += _pk_uniqueness_check(conn, "buyers", "id")
    issues += _pk_uniqueness_check(conn, "categories", "id")

    # FK orphans (DuckDB doesn't enforce FK; we validate manually).
    issues += _fk_orphan_check(conn, "order_products", "order_id", "orders", "id")

    # NULL constraints — required for analytics queries to work.
    issues += _null_constraint_check(conn, "orders", "ordered_at")
    issues += _null_constraint_check(conn, "orders", "source_id")
    issues += _null_constraint_check(conn, "orders", "status_id")

    # Value domains — surface upstream changes (new KeyCRM status/source IDs).
    issues += _value_domain_check(
        conn, "orders", "status_id", KNOWN_STATUS_IDS, Severity.WARN,
    )

    # Our copy of "what counts as revenue" against KeyCRM's own grouping.
    try:
        issues += _status_group_agreement_check(conn)
    except Exception as exc:  # status_group_id predates some schemas
        logger.debug("status_group agreement check skipped: %s", exc)
    issues += _value_domain_check(
        conn, "orders", "source_id", KNOWN_SOURCE_IDS, Severity.WARN,
    )

    # Freshness — catch silent sync-pipeline stalls (e.g. categories 45d stale).
    issues += _freshness_check(conn)

    # Cross-metric consistency — revenue and product pages read different
    # columns, so orders billed at zero make them disagree without saying so.
    try:
        issues += _headline_vs_line_items_check(conn)
    except Exception as exc:  # silver_orders may not exist yet on a fresh DB
        logger.debug("headline_vs_line_items check skipped: %s", exc)

    # Fourteen Gold columns against a recompute from Silver. Report-only by
    # construction: an integrity finding cannot reach validation_passed.
    try:
        issues += _gold_cell_values_check(conn)
    except Exception as exc:  # gold_daily_revenue may not exist yet
        logger.debug("gold_cell_values check skipped: %s", exc)

    # The same shape, deliberately: goods leaving with no sale is the whole
    # job of an influence manager. Counted, not warned about.
    try:
        issues += _goods_shipped_without_sale_check(conn)
    except Exception as exc:
        logger.debug("goods_shipped_without_sale check skipped: %s", exc)

    # An order with revenue and no products is a half-written order. The header
    # makes it look complete, so nothing goes back for it on its own.
    issues += _orders_without_line_items_check(conn)

    # A missed inventory snapshot is the one loss here with no second chance:
    # the API serves current stock, so yesterday's is gone the moment yesterday
    # is. Twenty-five days went missing in 2026 without anything saying so.
    try:
        issues += _inventory_snapshot_continuity_check(conn)
    except Exception as exc:  # inventory_sku_history predates some schemas
        logger.debug("inventory_snapshot_continuity check skipped: %s", exc)

    return issues


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


def persist_run(
    conn,
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
) -> int:
    """Insert one run + its child issues/diffs in a single transaction.

    Returns: the new run_id.

    The connection must already be in the caller's transaction context
    (we don't open/close — the store wrapper handles that).
    """
    summary = summarize_discrepancies(discrepancies)
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

    row = conn.execute("""
        INSERT INTO data_quality_runs (
            started_at, ended_at, as_of, window_start, window_end,
            layer, status,
            integrity_issues_count, discrepancies_count,
            critical_count, warn_count,
            api_calls_used, duration_ms, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING run_id
    """, [
        started_at, ended_at, as_of, window_start, window_end,
        layer, status,
        len(issues), len(discrepancies),
        critical_count, warn_count,
        api_calls_used, duration_ms, error_message,
    ]).fetchone()
    run_id = int(row[0])

    if issues:
        conn.executemany("""
            INSERT INTO data_quality_issues
              (run_id, check_name, table_name, severity, count, sample_ids, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (run_id, i.check_name, i.table_name, i.severity.value,
             i.count, json.dumps(list(i.sample_ids)), i.description)
            for i in issues
        ])

    if discrepancies:
        conn.executemany("""
            INSERT INTO data_quality_diffs
              (run_id, month, source_id, diff_class, field,
               dk_value, kc_value, severity, order_ids)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (run_id, d.month, d.source_id, d.diff_class.value, d.field,
             d.dk_value, d.kc_value, d.severity.value,
             json.dumps(list(d.order_ids)) if d.order_ids else None)
            for d in discrepancies
        ])

    return run_id


def fetch_run_diffs(conn, run_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    """Read all discrepancies for a run. For health/UI surface and digest."""
    rows = conn.execute("""
        SELECT month, source_id, diff_class, field,
               dk_value, kc_value, severity, order_ids
        FROM data_quality_diffs
        WHERE run_id = ?
        ORDER BY severity DESC, month, source_id
        LIMIT ?
    """, [run_id, limit]).fetchall()
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


def fetch_run_issues(conn, run_id: int, limit: int = 100) -> List[Dict[str, Any]]:
    rows = conn.execute("""
        SELECT check_name, table_name, severity, count, sample_ids, description
        FROM data_quality_issues
        WHERE run_id = ?
        ORDER BY severity DESC, check_name
        LIMIT ?
    """, [run_id, limit]).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "check_name": r[0], "table_name": r[1],
            "severity": r[2], "count": int(r[3] or 0),
            "sample_ids": json.loads(r[4]) if r[4] else [],
            "description": r[5],
        })
    return out


def format_alert_message(
    layer: str,
    severity: Severity,
    issues: List[IntegrityIssue],
    discrepancies: List[Discrepancy],
    *,
    window: Optional[Tuple[date, date]] = None,
    max_lines: int = 12,
) -> str:
    """Build a Telegram-friendly summary. Pure function — no I/O.

    Shape:
        🚨 Data Quality CRITICAL (reconciliation)
        Window: 2026-02 .. 2026-05
        ── Issues (1) ──
        • fk_orphan_order_products_order_id: 3 orphans (sample: 88888)
        ── Discrepancies (2) ──
        • 2026-04 / src=1: orders DK=565 KC=566 (MISSING_IN_DK)
        ...
    """
    icon = {"CRITICAL": "🚨", "WARN": "⚠️", "INFO": "ℹ️"}[severity.value]
    lines: List[str] = [f"{icon} *Data Quality {severity.value}* ({layer})"]
    if window:
        lines.append(f"Window: {window[0].isoformat()} .. {window[1].isoformat()}")

    if issues:
        lines.append(f"── Issues ({len(issues)}) ──")
        for i in issues[:max_lines // 2]:
            samples = (
                f" (sample: {', '.join(str(s) for s in i.sample_ids[:3])})"
                if i.sample_ids else ""
            )
            lines.append(f"• {i.check_name}: {i.count}{samples}")
        if len(issues) > max_lines // 2:
            lines.append(f"  …and {len(issues) - max_lines // 2} more")

    if discrepancies:
        lines.append(f"── Discrepancies ({len(discrepancies)}) ──")
        for d in discrepancies[:max_lines]:
            lines.append(
                f"• {d.month} / src={d.source_id}: {d.field} "
                f"DK={d.dk_value:.0f} KC={d.kc_value:.0f} ({d.diff_class.value})"
            )
        if len(discrepancies) > max_lines:
            lines.append(f"  …and {len(discrepancies) - max_lines} more")

    return "\n".join(lines)


def fetch_latest_run(conn, layer: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Read the most recent run row. Used by health endpoint."""
    where = ""
    params: List[Any] = []
    if layer is not None:
        where = "WHERE layer = ?"
        params.append(layer)
    row = conn.execute(f"""
        SELECT run_id, started_at, ended_at, as_of, window_start, window_end,
               layer, status, integrity_issues_count, discrepancies_count,
               critical_count, warn_count, api_calls_used, duration_ms, error_message
        FROM data_quality_runs
        {where}
        ORDER BY started_at DESC
        LIMIT 1
    """, params).fetchone()
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
WATCHED_LAYERS: Tuple[str, ...] = ("integrity", "reconciliation")


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


def fetch_previous_run(conn, layer: str, before_run_id: int) -> Optional[Dict[str, Any]]:
    """The run of `layer` immediately preceding `before_run_id`, if any.

    Used by the digest to say whether a standing problem is growing. Failed
    runs are skipped: their zero counts would read as "fixed, then broke
    again" and turn every 429 into a fake recovery.
    """
    row = conn.execute("""
        SELECT run_id FROM data_quality_runs
        WHERE layer = ? AND run_id < ?
          AND error_message IS NULL AND status <> 'FAILED'
        ORDER BY run_id DESC
        LIMIT 1
    """, [layer, before_run_id]).fetchone()
    if not row:
        return None
    return fetch_latest_run_by_id(conn, int(row[0]))


def fetch_latest_run_by_id(conn, run_id: int) -> Optional[Dict[str, Any]]:
    """Read one run row by id, in the same shape as `fetch_latest_run`."""
    row = conn.execute("""
        SELECT run_id, started_at, ended_at, as_of, window_start, window_end,
               layer, status, integrity_issues_count, discrepancies_count,
               critical_count, warn_count, api_calls_used, duration_ms, error_message
        FROM data_quality_runs WHERE run_id = ?
    """, [run_id]).fetchone()
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


# ─── Daily digest ─────────────────────────────────────────────────────────────

# How stale a layer's newest verdict may be before the digest calls it out.
# One cycle plus grace, same reasoning as the canary's thresholds.
DIGEST_MAX_AGE_HOURS = {"reconciliation": 30, "integrity": 12}

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
    """'new', 'unchanged', or '+12 since the last run'."""
    for p in previous:
        if p["check_name"] == check_name:
            diff = count - int(p["count"])
            if diff == 0:
                return "unchanged"
            return f"{diff:+d} since the last run"
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
            body.append(f"*{s.layer}* — no successful run on record")
            news = True
            continue

        limit = DIGEST_MAX_AGE_HOURS.get(s.layer)
        stale = limit is not None and s.age_hours is not None and s.age_hours > limit
        when = (s.run.get("started_at") or "")[:16].replace("T", " ")
        head = f"*{s.layer}* · {s.run.get('status')} · {when}"
        if stale:
            head += f" · ⏳ {s.age_hours:.0f}h old (>{limit}h)"
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
                    if note != "unchanged":
                        news = True
                body.append(f"• {i['check_name']}: {i['count']:,} ({note})")
                desc = (i.get("description") or "").strip()
                if desc:
                    body.append(f"  ↳ {desc[:200]}")
            if len(s.issues) > max_issue_lines:
                body.append(f"  …and {len(s.issues) - max_issue_lines} more")
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
                body.append(f"  …and {len(s.diffs) - max_diff_lines} more")

        if not s.issues and not s.diffs and not stale:
            body.append("• clean")

    if news:
        return "\n".join(["📋 *Data quality digest*", ""] + body)

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
        footer = (
            f"_Nothing has changed since the last digest {days}d ago. "
            "Repeated weekly so a standing finding is not forgotten; "
            "the days in between stay quiet._"
        )
    else:
        footer = (
            "_Standing findings, restated. The digest is quiet on days "
            "nothing changes._"
        )
    return "\n".join(["📋 *Data quality digest*", ""] + body + ["", footer])


def fetch_last_success_ages(
    conn, layers: Tuple[str, ...] = WATCHED_LAYERS,
) -> Dict[str, Dict[str, Any]]:
    """Age of the last **successful** run per layer, for the run-age watchdog.

    Keyed on `error_message IS NULL`, not on the mere existence of a row:
    `persist_run` writes a row on the failure path too, so a plain
    "when did this job last run" watchdog stayed silent through all 57
    consecutive `429 Too Many Attempts` failures of the reconciliation job.
    A run that produced no verdict is not a run.

    Returns one entry per requested layer — a layer that has never succeeded
    reports `last_success_at=None, age_seconds=None`, so the consumer can
    tell "never ran" apart from "ran recently" instead of seeing a missing key.
    """
    placeholders = ", ".join("?" for _ in layers)
    rows = conn.execute(f"""
        SELECT layer,
               MAX(started_at) AS last_success_at,
               EXTRACT(EPOCH FROM (now() - MAX(started_at))) AS age_seconds
        FROM data_quality_runs
        WHERE layer IN ({placeholders})
          AND error_message IS NULL
          AND status <> 'FAILED'
        GROUP BY layer
    """, list(layers)).fetchall()

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
