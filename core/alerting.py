"""The vocabulary of alert conditions — step 01 of the alerts rework.

Until 29.08 the system had no notion of "a condition" separate from "a
throttle bucket", and the buckets were unusable as identities: the canary
joins every failing check into one compound key (``canary:a,b,c``), and the
data-quality fingerprint carries the severity *and the set* of check names —
one check joining the set mints a new key and orphans the old one, so nothing
keyed that way can ever be marked resolved. The canary even hand-rolls a
workaround (forgetting keys that stopped failing), which is what a broken
identity looks like in code.

This module is the fix's foundation: **one condition — one key — no severity**,
declared here and pinned by a test that enumerates every name any emitter can
actually produce (``tests/unit/test_condition_registry.py``). Severity is an
attribute of an occurrence, never of the identity: a finding that escalates
WARN→CRITICAL is the same condition getting worse, not a new condition.

Two kinds, declared and never inferred:

- **condition** — a state that holds and can clear: a stale mirror, a failing
  validation, a certificate about to expire. Gets a lifecycle
  (fired → … → resolved) when the ledger lands (step 03), and only these may
  ever escalate or emit a "✅ resolved".
- **event** — a fact about the past that nothing can un-happen: an OOM kill,
  a failed backup, a rejected model. An event never resolves, and letting one
  escalate would page about 03:00 forever.

What this module deliberately is NOT yet:

- It does not route, throttle, or send — the Gate (step 02) will consume it.
- DQ finding keys carry no table subject: ``mirror_missing_rows`` on
  ``bronze.products`` and on ``bronze.orders`` are one key today. The ledger
  stores the subject as its own column, so refining the series grain there
  does not change this vocabulary.
- The canary's ``mirror_failing:bronze.orders`` and the daily comparison's
  ``mirror_failing`` are two names for the same underlying condition, found
  by two watchers in two processes. They stay distinct strings here (each is
  what its emitter can actually produce) and are unified at the ledger,
  where cross-process dedup lives.
- Reconciliation *discrepancies* (MISSING_IN_DK and friends) are not reified
  as condition keys anywhere in the code yet; they enter the registry when
  the Gate starts reifying them (step 02).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, FrozenSet


class Kind(str, Enum):
    CONDITION = "condition"  # holds and can clear → lifecycle, may escalate
    EVENT = "event"          # a fact about the past → reported once, never resolves


@dataclass(frozen=True)
class ConditionSpec:
    kind: Kind
    # For conditions: one sentence on what clears it — the emitter that can
    # observe the clearing is the one that will call resolve (step 04).
    # Empty for events, by definition.
    clears: str = ""


C, E = Kind.CONDITION, Kind.EVENT


def _c(clears: str) -> ConditionSpec:
    return ConditionSpec(Kind.CONDITION, clears)


_EVENT = ConditionSpec(Kind.EVENT)


# ─── The registry ───────────────────────────────────────────────────────────
#
# Exact keys only. Lookup is exact-match, never prefix: prefix dispatch is how
# a future check inherits a behaviour nobody chose for it (the review's live
# example: a prefix map for `dq_` would have muted a duplicate-primary-key
# CRITICAL because the bot could not reach nginx). The REMEDIATION table in
# core/data_quality.py keeps its longest-prefix matching because its worst
# mistake is a wrong sentence; here the worst mistake will be a wrong action.

REGISTRY: Dict[str, ConditionSpec] = {
    # ── canary (bot container, every 15 min) ──
    "health_unreachable": _c("the next probe reaches /api/health"),
    "health_http": _c("the next probe gets a 200"),
    "health_status": _c("the app reports status=healthy again"),
    "cert_expiring": _c("the cert is renewed past the warning window"),
    "cert_unreachable": _c("the TLS handshake succeeds again"),
    "dq_block_missing": _c("health payload carries a data_quality block again"),
    "dq_missing:reconciliation": _c("the layer reports freshness again"),
    "dq_missing:integrity": _c("the layer reports freshness again"),
    "dq_missing:mirror_landing": _c("the layer reports freshness again"),
    "dq_never:reconciliation": _c("the layer's first successful run"),
    "dq_never:integrity": _c("the layer's first successful run"),
    "dq_never:mirror_landing": _c("the layer's first successful run"),
    "dq_stale:reconciliation": _c("a successful run inside the age limit"),
    "dq_stale:integrity": _c("a successful run inside the age limit"),
    "dq_stale:mirror_landing": _c("a successful run inside the age limit"),
    "mirror_block_missing": _c("health payload carries a mirrors block again"),
    "mirror_missing:bronze.orders": _c("the table reports freshness again"),
    "mirror_never:bronze.orders": _c("the table's first successful shipment"),
    "mirror_stale:bronze.orders": _c("a shipment inside the age limit"),
    "mirror_failing:bronze.orders": _c("failures_since_ok back to zero"),
    # The canary's guaranteed-fallback bucket for a result with no keys.
    # Registered so the completeness test sees it; firing it is a bug.
    "unkeyed": _EVENT,

    # ── warehouse validator and its escalation ladder (web, every 2 min) ──
    "warehouse:validation_retrying": _c("a tick passes validation"),
    "warehouse:validation_rebuilding": _c("a tick passes validation"),
    "warehouse:validation_unfixed": _c("a tick passes validation"),
    "warehouse:sales_type_partition": _c("Gold's known types cover Silver again"),
    "warehouse:refresh_errored": _c("a refresh completes without raising"),
    "warehouse:refresh_errored_exhausted": _c("a refresh completes without raising"),
    "warehouse:backup_preflight": _EVENT,   # one refused attempt, that night's fact
    "warehouse:backup_failed": _EVENT,      # one failed copy/validation

    # ── resource watchdogs (web) ──
    "disk:WARN": _c("usage back under the warning threshold"),
    "disk:CRITICAL": _c("usage back under the critical threshold"),
    "memory:WARN": _c("working set back under the warning threshold"),
    "memory:CRITICAL": _c("working set back under the critical threshold"),
    # memory's OOM-kill alert is deliberately unkeyed (each kill is its own
    # fact, throttling them would hide a second kill) and so has no entry.

    # ── bronze (staging mode only — dead in this deployment, kept honest) ──
    "bronze:invariant_violated": _c("table size matches the mode invariant"),
    "bronze:backlog": _c("the backlog drains under the threshold"),

    # ── prediction (web, Mon+Thu 03:30; only retail is trained) ──
    "prediction:retrain_rejected:retail": _EVENT,

    # ── TurboSMS webhook rejections (web) ──
    "turbosms:webhook:secret_unset": _c("the secret is configured"),
    "turbosms:webhook:client_disconnected": _EVENT,
    "turbosms:webhook:malformed_body": _EVENT,
    "turbosms:webhook:no_event_id": _EVENT,
    "turbosms:webhook:no_signature": _EVENT,
    "turbosms:webhook:bad_signature": _EVENT,
    "turbosms:webhook:no_message_id": _EVENT,
    "turbosms:webhook:event_rebound": _EVENT,

    # ── data-quality findings: the landing mirror against DuckDB ──
    "mirror_missing_rows": _c("the hourly ids-diff re-ships the rows"),
    "mirror_orphan_rows": _c("the rows are retired or restored"),
    "mirror_row_values": _c("the next full shipment overwrites the drift"),
    "mirror_retired_rows": _c("the source serves the row again, or never"),
    "mirror_never_shipped": _c("the table's first successful shipment"),
    "mirror_backfill_pending": _c("the backfill finishes with nothing left"),
    "mirror_buckets_disagree": _c("the fingerprinted buckets agree again"),
    "mirror_disabled": _c("KS_PG_DSN is configured"),
    "mirror_failing": _c("failures_since_ok back to zero"),

    # ── data-quality findings: Silver / Gold arcs ──
    "silver_missing_rows": _c("the next rebuild carries the rows"),
    "silver_orphan_rows": _c("the next rebuild drops the orphans"),
    "silver_row_values": _c("the next rebuild recomputes the values"),
    "gold_cell_values": _c("the next rebuild recomputes the cells"),
    "gold_missing_cells": _c("the next rebuild restores the cells"),
    "gold_orphan_cells": _c("the next rebuild drops the orphans"),
    "gold_rollup_mismatch": _c("fine rows add up to the roll-up again"),
    "customer_profile_mismatch": _c("the next витрина rebuild on the same tick"),

    # ── data-quality findings: ClickHouse copies and derivations ──
    "ch_silver_unreachable": _c("ClickHouse answers again"),
    "ch_silver_sync_failed": _c("the next hourly ship succeeds"),
    "ch_silver_roundtrip": _c("the copy reads back equal"),
    "ch_gold_unreachable": _c("ClickHouse answers again"),
    "ch_gold_ship_failed": _c("the next hourly ship succeeds"),
    "ch_gold_row_missing": _c("the next ship carries the row"),
    "ch_gold_row_extra": _c("the next ship drops the row"),
    "ch_gold_value_mismatch": _c("the engines' aggregations agree again"),
    "ch_engines_gold_missing": _c("both engines hold the cell"),
    "ch_engines_gold_extra": _c("the extra cell disappears on re-derivation"),
    "ch_engines_gold_mismatch": _c("the engines' aggregations agree again"),
    "ch_history_unreachable": _c("ClickHouse answers again"),
    "ch_history_buckets": _c("a human repairs the archive copy"),

    # ── data-quality findings: the order-version archive (report-only) ──
    "order_versions_stalled": _c("a version row lands again"),
    "order_versions_flooding": _c("the write rate returns to ~daily scale"),
    "order_versions_missing": _c("a human repairs the archive"),
    "order_versions_empty": _c("the seed baseline lands"),

    # ── data-quality findings: business-shape checks ──
    "orders_without_line_items": _c("halfwritten_repair re-fetches them"),
    "headline_vs_line_items": _c("never fully — the standing 436 are certificates"),
    "status_group_vs_return_list": _c("the stored group and the legacy list agree"),
    "goods_shipped_without_sale": _c("by design it never fully clears"),
    "inventory_snapshot_gaps": _c("cannot heal — a missed day is missed forever"),

    # ── self-heal trails (INFO riders on the digest, facts about past heals) ──
    "mirror_selfhealed_rows": _EVENT,
    "ch_history_selfhealed": _EVENT,
}

# Integrity checks whose names are generated from (table, column) call sites.
# The completeness test extracts the actual call arguments from the AST, so a
# new `_pk_uniqueness_check(conn, "offers")` fails the suite until its key is
# added here — the registry stays the single place a human declared intent.
for _table in ("orders", "order_products", "products", "buyers", "categories"):
    REGISTRY[f"pk_uniqueness_{_table}"] = _c("the duplicate rows are gone")
REGISTRY["fk_orphan_order_products_order_id"] = _c("the parent order lands or the orphans go")
for _col in ("ordered_at", "source_id", "status_id"):
    REGISTRY[f"not_null_orders_{_col}"] = _c("the null rows are repaired")
for _col in ("source_id", "status_id"):
    REGISTRY[f"value_domain_orders_{_col}"] = _c("the unknown values are mapped or fixed")
for _entity in ("orders", "products", "buyers", "offers", "stocks",
                "managers", "categories", "expense_types"):
    REGISTRY[f"freshness_{_entity}"] = _c("a sync moves the entity's watermark")


# Keys whose emitter exists but has not landed on the branch yet — the SMS
# session's tree carries `_note_rejection("event_rebound", ...)` uncommitted,
# so the committed tree cannot emit it while the local tree can. The
# completeness test skips these in its reverse direction only; delete the
# entry here the moment the emitter's commit lands, or it becomes the exact
# stale documentation the reverse direction exists to forbid.
PENDING_EMITTERS: FrozenSet[str] = frozenset({"turbosms:webhook:event_rebound"})


# Channel messages that ride the same throttle machinery but are not
# conditions: the digest is a scheduled report, the recovery notice is a
# lifecycle message *about* conditions. Neither may ever grow a lifecycle of
# its own.
EXCLUDED_MESSAGE_KEYS: FrozenSet[str] = frozenset({"dq:digest", "canary:recovery"})


def spec_for(key: str) -> ConditionSpec:
    """Exact-match lookup. An unregistered key is an EVENT — the do-nothing
    default: it gets reported and throttled like today, but no lifecycle, no
    escalation, and (in later steps) no trigger can attach to it until a human
    registers it. Defaulting to the inert kind is the whole point of the
    exact-match rule."""
    return REGISTRY.get(key, _EVENT)


def is_condition(key: str) -> bool:
    return spec_for(key).kind is Kind.CONDITION
