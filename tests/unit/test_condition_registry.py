"""Every key an emitter can produce is registered, and nothing else is.

The registry in core/alerting.py is only worth having if it cannot drift from
the code: an unregistered key would silently get the inert default, and a
stale entry would document a condition nobody can raise. So this test
*computes* the emittable set from the emitters themselves — AST for literals
and generator call-sites, imports for the enumerable families — and compares
both directions. Adding a check or an alert without registering it fails the
suite; that failure is the registration prompt.

AST, not grep: a name in a comment or a docstring must not count (the
project's standing rule — assert on structure, not prose).
"""
from __future__ import annotations

import ast
from pathlib import Path

from core.alerting import (
    EXCLUDED_MESSAGE_KEYS,
    PENDING_EMITTERS,
    REGISTRY,
    Kind,
    spec_for,
)

ROOT = Path(__file__).resolve().parents[2]


def _parse(path: str) -> ast.AST:
    return ast.parse((ROOT / path).read_text())


def _const_str(node: ast.AST) -> "str | None":
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


# ─── Collectors, one per family of emitted names ────────────────────────────

def collect_static_check_names() -> set:
    """Every literal check_name=... across the modules that build findings."""
    names = set()
    for path in ("core/data_quality.py", "core/mirror_reconciliation.py",
                 "core/ch_silver.py", "core/ch_gold.py", "core/ch_history.py",
                 "core/pg_order_versions.py", "core/pg_vitrina.py",
                 # The scheduler builds two gating findings of its own:
                 # mirror_backfill_pending (the PG arm) and
                 # ch_reconcile_pending (the ClickHouse arm).
                 "core/scheduler.py"):
        for node in ast.walk(_parse(path)):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "check_name" and _const_str(kw.value) is not None:
                        names.add(kw.value.value)
    return names


def collect_generated_check_names() -> set:
    """Integrity checks named from (table, column) at their call sites."""
    patterns = {
        "_pk_uniqueness_check": "pk_uniqueness_{0}",
        "_fk_orphan_check": "fk_orphan_{0}_{1}",
        "_null_constraint_check": "not_null_{0}_{1}",
        "_value_domain_check": "value_domain_{0}_{1}",
    }
    names = set()
    for node in ast.walk(_parse("core/data_quality.py")):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in patterns):
            args = [a.value for a in node.args[1:]
                    if isinstance(a, ast.Constant) and isinstance(a.value, str)]
            names.add(patterns[node.func.id].format(*args))
    assert names, "the integrity call sites moved — update the collector"
    return names


def collect_freshness_names() -> set:
    from core.data_quality import FRESHNESS_THRESHOLDS

    return {f"freshness_{entity}" for entity in FRESHNESS_THRESHOLDS}


def collect_literal_send_keys() -> set:
    """Every literal key=... passed to a send/throttle call, all emitters."""
    keys = set()
    for path in ("core/scheduler.py", "core/duckdb_store.py", "bot/main.py",
                 "core/prediction_service.py", "web/routes/api/webhooks.py"):
        for node in ast.walk(_parse(path)):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    # `key=` is the legacy throttle argument; `bucket=` and
                    # `conditions=[...]` are the Gate's (step 02). All three
                    # name conditions when they are literals.
                    if kw.arg in ("key", "bucket") and _const_str(kw.value) is not None:
                        keys.add(kw.value.value)
                    if kw.arg == "conditions" and isinstance(kw.value, ast.List):
                        for element in kw.value.elts:
                            value = _const_str(element)
                            if value is not None:
                                keys.add(value)
                # _send_warehouse_alert(msg, "warehouse:...") passes the key
                # positionally as the second argument.
                func = node.func
                if (isinstance(func, ast.Attribute)
                        and func.attr == "_send_warehouse_alert"
                        and len(node.args) == 2):
                    value = _const_str(node.args[1])
                    if value is not None:
                        keys.add(value)
            # The validator picks its key by assigning validation_alert_key
            # and sends it two hundred lines later — an assignment, not a
            # call argument, so the call-scanner above cannot see it.
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if getattr(target, "id", "") == "validation_alert_key":
                        value = _const_str(node.value)
                        if value is not None:
                            keys.add(value)
    return keys


def collect_family_keys() -> set:
    """Keys built at runtime from enumerable inputs, family by family.

    Each family here mirrors one f-string in the code; the inputs are imported
    from the same constants the code reads, so a new severity level or webhook
    kind extends this set without the test knowing the string by heart.
    """
    keys = set()

    # disk: "disk:" + alert.severity.value — WARN/CRITICAL only (INFO never
    # constructs an alert; evaluate_* return None below WARN).
    keys |= {"disk:WARN", "disk:CRITICAL"}

    # memory: f"memory:web:{level}" in the scheduler, "memory:bot:{level}"
    # in bot/memory_watch.py — one key per container, because the two have
    # different limits and different failure stories. OOM stays unkeyed.
    keys |= {f"memory:{role}:{level}"
             for role in ("web", "bot") for level in ("WARN", "CRITICAL")}

    # prediction: f"prediction:retrain_rejected:{sales_type}" — enumerate the
    # sales_type literals actually passed to train() anywhere in the codebase.
    trained = set()
    for path in ("core/scheduler.py", "core/prediction_service.py",
                 "web/routes/api/margin.py"):
        try:
            tree = _parse(path)
        except FileNotFoundError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "sales_type" and _const_str(kw.value) is not None:
                        func = node.func
                        name = getattr(func, "attr", getattr(func, "id", ""))
                        if name in {"train", "retrain"}:
                            trained.add(kw.value.value)
    trained.add("retail")  # train()'s own default
    keys |= {f"prediction:retrain_rejected:{st}" for st in trained}

    # turbosms: f"turbosms:webhook:{kind}" — kinds are the literal first args
    # of _note_rejection(...) plus the kind = "..." assignments feeding it.
    kinds = set()
    for node in ast.walk(_parse("web/routes/api/webhooks.py")):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "_note_rejection" and node.args):
            value = _const_str(node.args[0])
            if value is not None:
                kinds.add(value)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if getattr(target, "id", "") == "kind":
                    value = _const_str(node.value)
                    if value is not None:
                        kinds.add(value)
    assert kinds, "webhook rejection kinds moved — update the collector"
    keys |= {f"turbosms:webhook:{k}" for k in kinds}
    return keys


def collect_canary_keys() -> set:
    """The canary's failure keys: literal fail(...) calls plus the f-string
    families over the two threshold dicts it actually iterates."""
    from bot.canary import DQ_MAX_AGE_S, MIRROR_MAX_AGE_S

    keys = set()
    for node in ast.walk(_parse("bot/canary.py")):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "fail" and node.args):
            value = _const_str(node.args[0])
            if value is not None:
                keys.add(value)
    for layer in DQ_MAX_AGE_S:
        keys |= {f"dq_missing:{layer}", f"dq_never:{layer}", f"dq_stale:{layer}"}
    for table in MIRROR_MAX_AGE_S:
        keys |= {f"mirror_missing:{table}", f"mirror_never:{table}",
                 f"mirror_stale:{table}", f"mirror_failing:{table}"}
    keys.add("dq_block_missing")
    keys.add("mirror_block_missing")
    keys.add("unkeyed")  # the guaranteed-fallback bucket in decide()
    # check_alerting_health returns its keys as tuple literals rather than
    # through fail(); a declared family, like disk's and memory's.
    keys |= {"alerting_block_missing", "alerting_transport_failing"}
    return keys


def emittable() -> set:
    return (collect_static_check_names() | collect_generated_check_names()
            | collect_freshness_names() | collect_literal_send_keys()
            | collect_family_keys() | collect_canary_keys())


# ─── The two directions ─────────────────────────────────────────────────────

class TestCompleteness:
    def test_every_emittable_key_is_registered(self):
        """The direction that guards the future: a new check or alert lands in
        the registry, with a declared kind, before it can ship."""
        missing = emittable() - set(REGISTRY) - EXCLUDED_MESSAGE_KEYS
        assert not missing, (
            f"unregistered condition keys: {sorted(missing)} — declare each "
            "in core/alerting.py with a kind (condition or event)"
        )

    def test_every_registered_key_is_emittable(self):
        """The direction that keeps the registry honest: an entry nobody can
        emit is documentation of a condition that does not exist."""
        stale = set(REGISTRY) - emittable() - PENDING_EMITTERS
        assert not stale, (
            f"registry entries no emitter produces: {sorted(stale)} — delete "
            "them or fix the collector that should have found them"
        )

    def test_pending_emitters_are_registered_and_stay_small(self):
        """PENDING is a coordination valve for the shared branch, not a
        loophole: every pending key must already be registered, and the set
        must not quietly grow into a second registry."""
        assert PENDING_EMITTERS <= set(REGISTRY)
        assert len(PENDING_EMITTERS) <= 3

    def test_excluded_keys_are_not_registered(self):
        """The digest and the recovery notice are channel messages, not
        conditions; registering one would hand it a lifecycle."""
        assert not EXCLUDED_MESSAGE_KEYS & set(REGISTRY)


class TestTheModel:
    def test_no_key_carries_a_severity(self):
        """Severity inside the identity is what made WARN→CRITICAL mint a new
        series and orphan the old one. The two watchdog families carry their
        level as the *subject* (a WARN-level breach and a CRITICAL-level
        breach are different thresholds, hence different conditions) — but no
        DQ key may embed Severity."""
        for key in REGISTRY:
            if key.startswith(("disk:", "memory:")):
                continue
            assert not any(
                s in key for s in (":CRITICAL", ":WARN", ":INFO")
            ), key

    def test_unregistered_keys_default_to_the_inert_kind(self):
        spec = spec_for("something_nobody_declared")
        assert spec.kind is Kind.EVENT
        assert spec.clears == ""

    def test_every_condition_says_what_clears_it(self):
        """The `clears` sentence is step 04's worksheet: the emitter that can
        observe the clearing is the one that will call resolve."""
        for key, spec in REGISTRY.items():
            if spec.kind is Kind.CONDITION:
                assert spec.clears, f"{key} is a condition with no clearing note"
            else:
                assert not spec.clears, f"{key} is an event; events never clear"

    def test_the_facts_that_cannot_unhappen_are_events(self):
        """The review's finding 3, pinned: an OOM kill, a failed backup, a
        rejected model would otherwise escalate forever."""
        for key in ("warehouse:backup_preflight", "warehouse:backup_failed",
                    "prediction:retrain_rejected:retail",
                    "mirror_selfhealed_rows", "ch_history_selfhealed"):
            assert spec_for(key).kind is Kind.EVENT, key
