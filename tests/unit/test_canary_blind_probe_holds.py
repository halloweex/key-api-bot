"""A canary probe that read nothing announces nothing resolved (L4-02).

`canary_job` resolves the `canary` group on every tick with what is still
failing, and the Gate announces every other delivered key cleared. A probe
that could not judge a key — web did not answer (a blip `defer_flaky` holds
back), or answered with a block null — reported it as not failing: a standing
`dq_stale:mirror_landing` was announced "✅ Resolved" by the blind probe and
paged again as a new incident by the next one. The held set now covers every
delivered key judged from the payload (no payload) or from the missing block
(`BLOCK_KEY_PREFIXES`).
"""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from bot import canary
from bot.canary import (
    BLOCK_KEY_PREFIXES, JUDGED_WITHOUT_PAYLOAD, defer_flaky, format_alert,
    run_canary, unjudged_keys,
)

ROOT = Path(__file__).resolve().parents[2]


def _ok(mirror_landing_age: int = 3600) -> dict:
    return {
        "status": "healthy", "sync": {"seconds_since_sync": 30},
        "data_quality": {"integrity": {"age_seconds": 3600},
                         "reconciliation_pg": {"age_seconds": 3600},
                         "reconciliation_ch": {"age_seconds": 3600},
                         "mirror_landing": {"age_seconds": mirror_landing_age}},
        "mirrors": {"bronze.orders": {"age_seconds": 60, "failures_since_ok": 0,
                                      "failing": False}},
        "alerting": {"consecutive_transport_failures": 0}, "derivation": {},
        "write_chains": {"pg_orders_write": {"sync_step": {
            "consecutive_failures": 0, "last_ok_age_s": 30, "last_attempt_age_s": 30}}},
        "read_fallback_mode": {"refused": {}},
        "buyer_sync": {"consecutive_failures": 0, "last_ok_age_s": 60,
                       "last_attempt_age_s": 60},
        "backups": {},
    }


DELIVERED = ["dq_stale:mirror_landing", "mirror_failing:bronze.orders",
             "alerting_transport_failing", "derivation_marks_failing",
             "report_ledger_pending", "health_status", "health_unreachable",
             "cert_expiring"]


class TestTheHeldSet:
    def test_no_payload_holds_everything_judged_from_one(self):
        """Mutation: drop the `delivered` loop — none of these is held."""
        held = unjudged_keys(None, delivered=DELIVERED)
        for key in DELIVERED:
            assert (key in held) == (key not in JUDGED_WITHOUT_PAYLOAD), key

    @pytest.mark.parametrize("block,prefixes", BLOCK_KEY_PREFIXES)
    def test_a_missing_block_holds_its_families_only(self, block, prefixes):
        payload = _ok()
        payload[block] = None
        held = unjudged_keys(payload, delivered=DELIVERED)
        for key in DELIVERED:
            assert (key in held) == key.startswith(prefixes), (block, key)

    def test_a_whole_payload_holds_nothing_new(self):
        assert unjudged_keys(_ok(), delivered=DELIVERED) == unjudged_keys(_ok())

    def test_without_deliveries_the_old_list_stands(self):
        assert unjudged_keys(None) == ["read_refused", "buyer_sync_stalled",
                                       "buyer_sync_stalled_chain",
                                       "orders_sync_failing", "backup_evidence_stale"]


def _returned_keys(fn: ast.AST) -> "set[str]":
    """The literal key (or the literal head of an f-string key) opening each
    `(key, message)` tuple a check builds — returned, or appended to the list
    it returns."""
    keys = set()
    for tup in ast.walk(fn):
        if isinstance(tup, ast.Tuple) and len(tup.elts) == 2:
            head = tup.elts[0]
            if isinstance(head, ast.Constant) and isinstance(head.value, str):
                keys.add(head.value)
            elif isinstance(head, ast.JoinedStr) and isinstance(
                    head.values[0], ast.Constant):
                keys.add(head.values[0].value)
    return keys


def _blocks_read(fn: ast.AST) -> "set[str]":
    return {c.args[0].value for c in ast.walk(fn)
            if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
            and c.func.attr == "get" and c.args
            and isinstance(c.args[0], ast.Constant) and isinstance(c.args[0].value, str)}


class TestTheTableFollowsTheChecks:
    def test_every_payload_key_has_a_hold(self):
        """Every key a check judges from the payload is held by one rule or
        another when its block is missing: the table, the named list, or it
        is the block-missing page itself."""
        tree = ast.parse((ROOT / "bot/canary.py").read_text())
        named = set(unjudged_keys(None))
        prefixes = tuple(p for _, ps in BLOCK_KEY_PREFIXES for p in ps)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.FunctionDef) and fn.name.startswith("check_")):
                continue
            if fn.name in ("check_health", "check_cert_expiry"):
                continue
            for key in _returned_keys(fn):
                assert (key.endswith("_block_missing") or key in named
                        or key.startswith(prefixes)), (fn.name, key)

    def test_each_block_is_read_by_the_check_that_judges_its_families(self):
        tree = ast.parse((ROOT / "bot/canary.py").read_text())
        checks = [fn for fn in ast.walk(tree)
                  if isinstance(fn, ast.FunctionDef) and fn.name.startswith("check_")]
        for block, prefixes in BLOCK_KEY_PREFIXES:
            owners = [fn.name for fn in checks
                      if block in _blocks_read(fn)
                      and any(k.startswith(prefixes) for k in _returned_keys(fn))]
            assert owners, f"no check reads {block!r} and emits {prefixes}"


class TestThroughTheJob:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("second", ["blip", "null-blocks"])
    async def test_a_blind_probe_neither_resolves_nor_repages(self, second):
        """The reviewer's three probes, through `canary_job`'s own logic: a
        standing page, a blind probe, the same page again."""
        import core.alerting as alerting

        gate = alerting.AlertGate(state_path=None)
        sent: list = []

        async def send(text, *a, **k):
            sent.append(text)
            return 1

        partial = _ok(31 * 3600)
        partial["data_quality"] = None
        partial["mirrors"] = None
        probes = [_ok(31 * 3600), None if second == "blip" else partial, _ok(31 * 3600)]
        previous: set = set()
        with patch.object(alerting, "_gate", gate), \
             patch("bot.main.send_admin_message", new=send), \
             patch("core.alert_archive.write_resolved_now", new=AsyncMock(return_value=True)), \
             patch("core.alert_archive.record_fired", return_value=None), \
             patch.object(canary, "check_cert_expiry", new=AsyncMock(return_value=(90, None))):
            for answer in probes:
                def handler(request, answer=answer):
                    if answer is None:
                        raise httpx.ConnectError("refused")
                    return httpx.Response(200, json=answer)

                result = await run_canary(
                    "http://dash.invalid",
                    client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
                defer, previous = defer_flaky(list(result.failure_keys), previous)
                if result.failures and not defer:
                    keys = list(result.failure_keys)
                    await alerting.raise_alert(
                        format_alert(result, "http://dash.invalid"), conditions=keys,
                        bucket="canary:" + ",".join(sorted(keys)), group="canary")
                await alerting.resolve_group(
                    "canary", still_firing=[*result.failure_keys, *result.unjudged_keys])

        resolved = [t for t in sent if t.startswith("✅")]
        assert not any("dq_stale:mirror_landing" in t for t in resolved), resolved
        paged = [t for t in sent if not t.startswith("✅")
                 and "mirror_landing" in t]
        assert len(paged) == 1, "the standing page was paged again as new"
        assert "dq_stale:mirror_landing" in gate.delivered_groups()
