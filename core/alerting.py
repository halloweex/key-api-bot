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
from typing import Dict, FrozenSet, Sequence


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
    # The alerting machinery watching itself: consecutive transport failures
    # published in /api/health and judged by the canary — the one subsystem
    # that had no dead-man's switch, which is how the certificate alert died
    # unnoticed. Rule 3 applies to the block itself: absent is a failure.
    "alerting_transport_failing": _c("a send reaches at least one admin again"),
    "alerting_block_missing": _c("health payload carries the alerting block again"),
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
    # Per container, because the two processes have different limits (web 7g,
    # bot 512m) and different failure stories — one key for both would make
    # the series lie about which cgroup is starving.
    "memory:web:WARN": _c("working set back under the warning threshold"),
    "memory:web:CRITICAL": _c("working set back under the critical threshold"),
    "memory:bot:WARN": _c("working set back under the warning threshold"),
    "memory:bot:CRITICAL": _c("working set back under the critical threshold"),
    # The OOM-kill alerts are deliberately unkeyed (each kill is its own
    # fact, throttling them would hide a second kill) and so have no entry.

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
    "ch_reconcile_pending": _c("silver ships fresh again (hourly ch_sync)"),
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
# conditions: the digest is a scheduled report. (canary:recovery lived here
# until step 07, when the canary's bespoke recovery died in favour of the
# standard per-key "✅ Resolved".)
EXCLUDED_MESSAGE_KEYS: FrozenSet[str] = frozenset({"dq:digest"})


# ─── The Gate — step 02 ─────────────────────────────────────────────────────
#
# One door for the web container's emitters. What it owns is the *policy*:
# whether this raise becomes a message, and what standing-condition context
# rides along. What it does not own is the channel — delivery goes through
# `bot.main.send_admin_message(pre_throttled=True)`, so the kill switch, the
# signature, the clamp and the plain-text degradation stay in one place.
#
# The cooldown grows with the condition's age, and the number is measured,
# not chosen: production history holds three multi-week episodes of the
# warehouse validator failing ~545 ticks a day — a standing CRITICAL against
# a flat 30-minute cooldown is 48 messages a day for weeks, which is how one
# June condition delivered 404 copies. A condition that has been standing an
# hour is no longer news; it becomes one daily reminder that says how long
# and how many repeats it stands for. Events never escalate their cooldown —
# each occurrence is its own fact.
#
# Decision state survives restarts (step 03): a JSON snapshot per container
# on the ./data volume, written atomically and loaded tolerantly. Wall clock,
# not monotonic — a monotonic timestamp is meaningless across restarts, and
# durability is what this state is for. What the file protects: last_sent, so
# a deploy inside a standing condition's daily cooldown does not re-page; what
# it deliberately lets happen: a bucket whose last_attempt has gone stale
# reads as a new incident, so after a long outage the first alert of each
# kind still lands — the charter's restart rule, now a property of the data
# rather than of amnesia.

import json as _json
import os as _os
import tempfile as _tempfile
import time as _time
from pathlib import Path as _Path


@dataclass
class _BucketState:
    first_seen: float
    last_attempt: float
    # None, not 0.0: a bucket that was decided but never delivered must retry
    # on the next raise, and a zero would read as "sent at epoch".
    last_sent: "float | None" = None
    suppressed: int = 0


def _default_state_path() -> "_Path | None":
    """`data/alert-gate-{role}.json`; both containers mount ./data, so the
    role (KS_ROLE: web|bot) keeps them from clobbering each other. An empty
    KS_ALERT_GATE_STATE_DIR disables persistence — the test suite's setting,
    so two thousand tests do not take turns rewriting one real file."""
    root = _os.getenv("KS_ALERT_GATE_STATE_DIR", "data")
    if not root.strip():
        return None
    role = _os.getenv("KS_ROLE", "web").strip() or "web"
    return _Path(root) / f"alert-gate-{role}.json"


class AlertGate:
    BASE_COOLDOWN_S = 1800.0      # the loud phase: repeats every 30 min
    LOUD_PHASE_S = 3600.0         # for the condition's first hour
    STANDING_COOLDOWN_S = 86400.0  # then one reminder a day
    INCIDENT_RESET_S = 3600.0     # an hour of quiet = the next raise is news
    _SAVE_DEBOUNCE_S = 60.0

    def __init__(self, state_path: "_Path | None" = None) -> None:
        self._state: Dict[str, _BucketState] = {}
        # condition_key -> {"group": str|None, "first_delivered": float}.
        # The resolution gate (step 04): a "✅ resolved" may only follow a
        # fired notice that actually reached someone — otherwise the first
        # thing a human hears about a condition is that it went away.
        self._delivered: Dict[str, Dict] = {}
        # Buckets whose send is in flight right now. `decide` commits nothing
        # — the cooldown is paid by `record_delivery`, after the transport
        # returns — so between the two a second raise for the same bucket used
        # to look fresh and go out too: the two-minute refresh and a manual
        # one failing validation inside one Telegram round trip produced two
        # messages and two agent diagnoses. Process-local and deliberately not
        # on `_BucketState`: that dataclass is persisted, and a marker that
        # survived a crash would silence its bucket for good.
        self._in_flight: set = set()
        self._path = state_path
        self._last_save = 0.0
        self._dirty = False
        if self._path is not None:
            self._load()

    # ── persistence (tolerant on both ends) ──

    def _load(self) -> None:
        try:
            raw = _json.loads(self._path.read_text())
            if "buckets" in raw:
                self._state = {
                    k: _BucketState(**v) for k, v in raw["buckets"].items()
                }
                self._delivered = dict(raw.get("delivered", {}))
            else:
                # The step-03 format: a bare bucket map. One deploy's worth of
                # tolerance costs four lines; a crash on the old file would
                # cost the history the file exists to keep.
                self._state = {
                    k: _BucketState(**v) for k, v in raw.items()
                }
        except FileNotFoundError:
            pass
        except Exception as exc:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "alert gate state unreadable (%s); starting empty", exc,
            )
            self._state = {}

    def _save(self, now: float, *, force: bool = False) -> None:
        if self._path is None or not self._dirty:
            return
        if not force and (now - self._last_save) < self._SAVE_DEBOUNCE_S:
            return
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = _json.dumps({
                "buckets": {k: vars(v) for k, v in self._state.items()},
                "delivered": self._delivered,
            })
            fd, tmp = _tempfile.mkstemp(
                dir=str(self._path.parent), prefix=self._path.name,
            )
            with _os.fdopen(fd, "w") as handle:
                handle.write(payload)
            _os.replace(tmp, self._path)
            self._last_save = now
            self._dirty = False
        except Exception as exc:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "alert gate state not saved (%s); continuing in memory", exc,
            )

    def decide(
        self, bucket: str, *, has_condition: bool, now: "float | None" = None,
    ) -> "tuple[bool, str]":
        """(should_send, context_suffix) — without committing the cooldown.

        The cooldown is committed by `record_delivery`, so an attempt that the
        transport could not deliver does not buy silence — the lesson of the
        disk watchdog, whose 24h slot used to be consumed by a message nobody
        received.
        """
        now = _time.time() if now is None else now
        st = self._state.get(bucket)
        if st is not None and (now - st.last_attempt) > self.INCIDENT_RESET_S:
            # Quiet long enough that this is a new incident, not a repeat.
            st = None
        if st is None:
            self._state[bucket] = _BucketState(first_seen=now, last_attempt=now)
            self._dirty = True
            self._save(now)
            return True, ""

        st.last_attempt = now
        self._dirty = True
        self._save(now)
        age = now - st.first_seen
        standing = has_condition and age >= self.LOUD_PHASE_S
        cooldown = self.STANDING_COOLDOWN_S if standing else self.BASE_COOLDOWN_S
        if st.last_sent is not None and (now - st.last_sent) < cooldown:
            st.suppressed += 1
            return False, ""

        parts = []
        if standing:
            parts.append(f"standing {int(age // 3600)}h")
        if st.suppressed:
            parts.append(f"repeats ×{st.suppressed}")
        if standing:
            parts.append("next reminder in 24h")
        return True, ("\n⏳ " + " · ".join(parts) if parts else "")

    def claim(self, bucket: str) -> bool:
        """Mark the send `decide` admitted as in flight; False if one already is.

        Separate from `decide` so that the cooldown logic stays a pure
        function of the bucket's history: a second raise that loses the claim
        is the same news as the one in flight and rides its repeat counter,
        exactly as a raise inside the cooldown would.
        """
        if bucket in self._in_flight:
            st = self._state.get(bucket)
            if st is not None:
                st.suppressed += 1
            return False
        self._in_flight.add(bucket)
        return True

    def release(self, bucket: str) -> None:
        """The send that `claim` admitted has returned, delivered or not.
        Must run on every exit of that send — a bucket left in flight would
        be silent until the process restarts."""
        self._in_flight.discard(bucket)

    def record_delivery(self, bucket: str, *, now: "float | None" = None) -> int:
        """Commit the cooldown; returns the swallowed count this delivery
        flushed, which is what the archive records against the series."""
        now = _time.time() if now is None else now
        st = self._state.get(bucket)
        if st is None:
            return 0
        flushed = st.suppressed
        st.last_sent = now
        st.suppressed = 0
        self._dirty = True
        self._save(now, force=True)
        return flushed

    def note_delivered_conditions(
        self, keys: "Sequence[str]", group: "str | None",
        *, now: "float | None" = None,
    ) -> None:
        """Remember which conditions have a delivered fired-notice, so their
        clearing may be announced. Events are excluded by kind: nothing about
        the past ever "goes away"."""
        now = _time.time() if now is None else now
        for key in keys:
            if not is_condition(key):
                continue
            entry = self._delivered.get(key)
            if entry is None:
                self._delivered[key] = {"group": group, "first_delivered": now}
            else:
                entry["group"] = group
        self._dirty = True
        self._save(now, force=True)

    def restore_delivered(
        self, entries: "Dict[str, float]", group: "str | None",
        *, now: "float | None" = None,
    ) -> None:
        """Put back what `take_resolved` took, keeping the original
        `first_delivered`, when the resolution could not be recorded. A key
        that re-fired in between keeps its newer entry."""
        now = _time.time() if now is None else now
        for key, first in entries.items():
            if key not in self._delivered:
                self._delivered[key] = {"group": group, "first_delivered": first}
        if entries:
            self._dirty = True
            self._save(now, force=True)

    def take_resolved(
        self, group: str, still_firing: "Sequence[str]" = (),
        *, now: "float | None" = None,
    ) -> Dict[str, float]:
        """Pop and return {key: first_delivered} for the group's conditions
        that are no longer firing. Popping is the idempotence: one resolved
        notice per delivered fired-cycle, never a stream of them."""
        now = _time.time() if now is None else now
        firing = set(still_firing)
        taken: Dict[str, float] = {}
        for key in list(self._delivered):
            entry = self._delivered[key]
            if entry.get("group") == group and key not in firing:
                taken[key] = float(entry.get("first_delivered") or now)
                del self._delivered[key]
        if taken:
            self._dirty = True
            self._save(now, force=True)
        return taken

    def reset(self) -> None:
        self._state.clear()
        self._delivered.clear()
        self._in_flight.clear()
        self._dirty = False


_gate = AlertGate(state_path=_default_state_path())


def reset_gate() -> None:
    """For tests and for a deliberate re-arm."""
    _gate.reset()


async def raise_alert(
    text: str,
    *,
    conditions: "Sequence[str]",
    bucket: "str | None",
    parse_mode: str = "HTML",
    group: "str | None" = None,
    spool_as: "str | None" = None,
    evidence: "dict | None" = None,
) -> int:
    """Raise an alert about the named conditions. Returns admins reached.

    `conditions` are canonical keys from the REGISTRY — the vocabulary this
    module declares. An unregistered key is tolerated (it gets the inert
    EVENT default) but logged, because it means somebody added an alert
    without declaring what it is about, and the completeness test will say
    so louder.

    `bucket` names the dedup identity — usually the single condition, for the
    DQ layers today still the fingerprint. `None` means no dedup at all: the
    caller has already decided this occurrence must go (the OOM path).

    `evidence` is what the three-line message had no room for, archived into
    `app.alert_events.context`. It exists because the diagnostician reads
    Postgres and the DQ findings live in a DuckDB it cannot open: the answer
    to the 2026-09-01 incident was one sentence in a finding nothing could
    reach. Best-effort by construction — it rides the archive's
    fire-and-forget task, so a malformed or oversized payload costs a ledger
    column and never an alert.
    """
    import logging as _logging

    log = _logging.getLogger(__name__)
    for key in conditions:
        if key not in REGISTRY and key not in EXCLUDED_MESSAGE_KEYS:
            log.warning("raise_alert: unregistered condition key %r", key)

    suffix = ""
    if bucket is not None:
        has_condition = any(is_condition(k) for k in conditions)
        should_send, suffix = _gate.decide(bucket, has_condition=has_condition)
        if not should_send:
            log.debug("Alert suppressed by gate (bucket=%s)", bucket)
            return 0
        if not _gate.claim(bucket):
            log.debug("Alert already in flight (bucket=%s)", bucket)
            return 0

    from bot.main import send_admin_message

    try:
        delivered = await send_admin_message(
            text + suffix, parse_mode, pre_throttled=True,
        )
        if delivered:
            swallowed = _gate.record_delivery(bucket) if bucket is not None else 0
            _gate.note_delivered_conditions(conditions, group)
            from core.alert_archive import record_fired

            record_fired(
                conditions, message=text + suffix,
                delivered=delivered, swallowed=swallowed, evidence=evidence,
            )
            if bucket is not None and suffix == "":
                # A fresh incident (an empty suffix is the first fire of a
                # bucket, including one returning after the quiet-hour reset) —
                # the moment worth a diagnosis. Reminders and standing repeats
                # never re-summon the agent; the host runner adds a daily budget
                # on top.
                from core.alert_agent_spool import drop_task

                drop_task(conditions, bucket, text)
            elif spool_as is not None:
                # The bucket-less emitters — memory keeps its own pre-Gate
                # cooldown as the pre-OOM exception — still deserve a
                # diagnostician. `spool_as` names the task explicitly, and the
                # upstream cooldown is what keeps this from re-summoning the
                # agent every tick.
                from core.alert_agent_spool import drop_task

                drop_task(list(conditions) or [spool_as], spool_as, text)
    finally:
        # Whatever the transport did — delivered, returned 0, or raised into
        # a caller that swallows it — the bucket is no longer in flight.
        if bucket is not None:
            _gate.release(bucket)
    return delivered


def _age(seconds: float) -> str:
    hours = int(seconds // 3600)
    if hours < 1:
        return f"{int(seconds // 60)}m"
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d {hours % 24}h"


async def resolve_group(
    group: str, still_firing: "Sequence[str]" = (),
) -> int:
    """Announce that a group's delivered conditions have cleared.

    Called by the emitter that can observe the clearing — the `clears` column
    of the REGISTRY names which one that is — on every healthy pass, with
    whatever is *still* firing excluded. Gated on delivery by construction:
    only conditions whose fired notice reached someone are in the map, so
    "✅ resolved" can never be the first a human hears of a condition. One
    notice per fired-cycle: taking a key out of the map is the idempotence.

    Returns admins reached (0: nothing to resolve, or nothing deliverable).
    """
    import logging as _logging
    import time as _t

    taken = _gate.take_resolved(group, still_firing)
    if not taken:
        return 0

    now = _t.time()
    lines = [
        f"• {key} — stood {_age(now - first)}"
        for key, first in sorted(taken.items())
    ]
    text = "✅ Resolved:\n" + "\n".join(lines)

    # The ledger first, the notice second. A resolve whose fire-and-forget
    # write missed its one-second budget left the series firing for good —
    # the key was already out of the map and nothing retried — so the digest
    # showed the condition standing for months and the escalator fired a
    # phantom six hours later. If the row cannot be written, the keys go
    # back and the next healthy pass tries again; nothing is announced that
    # the ledger does not hold.
    from core.alert_archive import write_resolved_now

    if not await write_resolved_now(list(taken), delivered=None, message=text):
        _gate.restore_delivered(taken, group)
        _logging.getLogger(__name__).warning(
            "resolved notice for %s held back: the ledger could not record it; "
            "retrying on the next healthy pass", sorted(taken),
        )
        return 0

    from bot.main import send_admin_message

    delivered = await send_admin_message(text, pre_throttled=True)
    if not delivered:
        _logging.getLogger(__name__).info(
            "resolved notice for %s reached nobody (suppressed or failed); "
            "the series is closed in the archive", sorted(taken),
        )
    return delivered


def spec_for(key: str) -> ConditionSpec:
    """Exact-match lookup. An unregistered key is an EVENT — the do-nothing
    default: it gets reported and throttled like today, but no lifecycle, no
    escalation, and (in later steps) no trigger can attach to it until a human
    registers it. Defaulting to the inert kind is the whole point of the
    exact-match rule."""
    return REGISTRY.get(key, _EVENT)


def is_condition(key: str) -> bool:
    return spec_for(key).kind is Kind.CONDITION
