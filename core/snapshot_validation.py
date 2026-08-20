"""Validation for a logical snapshot, before it is allowed to leave the machine.

A file copy fails loudly: it is the wrong size, or it will not open. A logical
export fails *quietly* — it can succeed, weigh the right amount, and contain
zero rows in the tables the backup exists for. That is not hypothetical here.
`backup_database` validated with `SELECT COUNT(*) FROM orders > 0` and reported
success nightly while four of the six tables the runbook named were empty; the
check never looked at them, so nobody found out from the check.

So the rule this module encodes: **empty is allowed, silently empty is not.**

Three tiers, because the tables differ in what emptiness means:

  MUST_BE_NONEMPTY   zero rows is a broken export, full stop
  MONOTONE           append-only ledgers; a count that fell means rows were
                     lost between snapshots, which no correct path produces
  MAY_BE_EMPTY       legitimately empty, but the emptiness must be *declared*
                     in the manifest and named in the daily line, so that a
                     feature quietly losing its data is visible as a change

The evaluator is pure. The caller supplies this snapshot's counts and the
previous snapshot's, and gets back a verdict.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


# Zero rows here means the export is broken, not that the business is quiet.
MUST_BE_NONEMPTY = frozenset({
    "orders",
    "order_products",
    "products",
    "buyers",
    "expense_types",
    "sync_metadata",
    "users",
    "role_permissions",
})

# Append-only ledgers. Rows are added and never deleted by any correct path, so
# a count that fell between snapshots means loss — either upstream, or in the
# export itself. Deliberately does not include tables that are rebuilt or
# pruned by design (disk_samples, memory_samples, the derived layers).
MONOTONE = frozenset({
    "orders",
    "order_products",
    "expenses",
    "stock_movements",
    "inventory_sku_history",
    "buyer_contacts",
    "sms_campaign_members",
    "marketing_optouts",
    "reconciliation_log",
    "data_quality_runs",
    "warehouse_refreshes",
})

# Legitimately empty, and the point is that this is written down. Each of these
# is a live feature whose table is empty because nobody has used it yet — which
# is a different fact from "the export dropped it", and only stays a different
# fact if the manifest says so out loud.
MAY_BE_EMPTY = frozenset({
    "revenue_goals",
    "manual_expenses",
    "bronze_order_events",
    "marketing_optouts",
})

# user_preferences, celebrated_milestones and report_history were listed here
# until 2026-08-20. They are not empty tables any more; they are not tables in
# this database at all. Their home is the bot's SQLite, which now rides in the
# off-site archive in its own right — see deploy/offsite_parquet.sh.


# Derived layers. The manifest counts every table, but these are deliberately
# not exported — the app rebuilds them from bronze in seconds, which is most of
# why the snapshot is small enough to ship nightly. An empty one is the design
# working, so warning about it is noise, and noise is what this whole module
# exists to keep out of the daily line.
DERIVED = frozenset({
    "silver_orders", "silver_order_utm",
    "gold_daily_revenue", "gold_daily_products",
    "gold_daily_traffic",
    # Kept for snapshots taken before the table was dropped; its DDL is gone.
    # See the matching note in scripts/compact_duckdb.py.
    "gold_product_pairs",
})


@dataclass(frozen=True)
class SnapshotVerdict:
    """Whether a snapshot may be shipped, and what to say about it."""
    ok: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    empty_tables: List[str] = field(default_factory=list)

    def summary(self) -> str:
        if not self.ok:
            return f"REJECTED: {'; '.join(self.errors)}"
        parts = [f"{len(self.empty_tables)} declared-empty"]
        if self.warnings:
            parts.append(f"{len(self.warnings)} warning(s)")
        return "ok — " + ", ".join(parts)


def validate_snapshot(
    counts: Dict[str, int],
    *,
    previous_counts: Optional[Dict[str, int]] = None,
    checksums: Optional[Dict] = None,
    previous_checksums: Optional[Dict] = None,
) -> SnapshotVerdict:
    """Decide whether this snapshot is fit to be the copy that survives.

    Args:
        counts: table name → row count, from the export manifest.
        previous_counts: the same from the last accepted snapshot, or None on
            the first run — where there is nothing to compare and monotonicity
            cannot be judged, rather than assumed.
        checksums: manifest checksums (total_revenue, orders_date_range, ...).
        previous_checksums: the same from the last accepted snapshot.

    Returns:
        SnapshotVerdict. `ok` False means do not ship this.
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not counts:
        return SnapshotVerdict(ok=False, errors=["manifest carries no table counts"])

    # Tier 1 — must not be empty.
    for table in sorted(MUST_BE_NONEMPTY):
        if table not in counts:
            errors.append(f"{table}: absent from the manifest entirely")
        elif counts[table] <= 0:
            errors.append(f"{table}: 0 rows, and it can never legitimately be 0")

    # Tier 2 — append-only ledgers must not shrink.
    if previous_counts:
        for table in sorted(MONOTONE):
            now = counts.get(table)
            before = previous_counts.get(table)
            if now is None or before is None:
                continue
            if now < before:
                errors.append(
                    f"{table}: {before:,} → {now:,} rows. This table is only "
                    f"ever appended to, so a fall means rows were lost"
                )

    # Tier 3 — empty is fine, silence is not.
    empty_tables = sorted(t for t, n in counts.items() if n <= 0 and t not in DERIVED)
    for table in empty_tables:
        if (table not in MAY_BE_EMPTY and table not in MUST_BE_NONEMPTY
                and table not in DERIVED):
            # Not classified either way: report it rather than pick a side. An
            # unclassified empty table is exactly the case the old validator
            # walked past.
            warnings.append(f"{table}: 0 rows and not classified — is that expected?")

    # Money is the checksum an operator actually recognises.
    if checksums and previous_checksums:
        now_rev = checksums.get("total_revenue")
        before_rev = previous_checksums.get("total_revenue")
        if isinstance(now_rev, (int, float)) and isinstance(before_rev, (int, float)):
            if now_rev < before_rev * 0.99:
                errors.append(
                    f"total_revenue fell {before_rev:,.0f} → {now_rev:,.0f} "
                    f"(more than 1%); returns move this a little, not this much"
                )

        now_range = (checksums.get("orders_date_range") or [None, None])[1]
        before_range = (previous_checksums.get("orders_date_range") or [None, None])[1]
        if now_range and before_range and str(now_range) < str(before_range):
            errors.append(
                f"newest order went backwards: {before_range} → {now_range}"
            )

    return SnapshotVerdict(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        empty_tables=empty_tables,
    )
