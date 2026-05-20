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
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


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
KNOWN_STATUS_IDS = frozenset({
    1, 2, 9, 12, 15,           # active
    19, 20, 21, 22, 23,         # return/cancel family
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
    issues += _value_domain_check(
        conn, "orders", "source_id", KNOWN_SOURCE_IDS, Severity.WARN,
    )

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
