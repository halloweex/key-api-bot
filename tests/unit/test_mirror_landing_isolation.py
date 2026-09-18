"""One reconciliation that raises must not silence the other fifteen.

`dq_mirror_landing` runs sixteen checks in one job — landing, orders, Silver,
UTM and its completeness, expenses, Gold, the five irreplaceable tables, the
bot's state, the order archive, buyers, SMS, dashboard users, the vitrina,
ClickHouse and its archive. It used to wrap all of them in ONE try, so an
exception anywhere skipped every check after it, and the alert gate's
`and not error_message` then withheld the page for any CRITICAL already found
above the failure.

What these pin, and what they deliberately do not change:

- every check still runs after one raises;
- `error_message` names EVERY check that raised, not only the first;
- the run is still failed — `error_message` is set — so run age, the digest's
  delta and alert resolution treat it exactly as before;
- a CRITICAL found by a check that completed still pages.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.data_quality import IntegrityIssue, Severity

# Every check, in the order the job runs it. `reconcile_mirror` is the landing
# comparison; the rest are named as they are imported.
CHECKS_BY_MODULE = {
    "core.mirror_reconciliation": [
        "reconcile_mirror", "reconcile_orders", "reconcile_silver",
        "reconcile_order_utm", "reconcile_order_utm_completeness",
        "reconcile_expenses", "reconcile_gold",
        "reconcile_operational", "reconcile_bot_state",
        "reconcile_order_versions", "reconcile_buyers", "reconcile_sms",
        "reconcile_dashboard_users",
    ],
    "core.pg_vitrina": ["reconcile_customer_profile"],
    "core.ch_silver": ["reconcile_clickhouse"],
    "core.ch_history": ["reconcile_ch_history"],
}
ALL_CHECKS = [c for names in CHECKS_BY_MODULE.values() for c in names]


def check_order() -> list:
    """The `check("…")` names in `_run_dq_mirror_landing`, in source order.

    Parsed, not grepped: a string search matches a comment as readily as a call,
    and the comments in that function name every one of these checks.
    """
    from core.scheduler import BackgroundScheduler

    tree = ast.parse(textwrap.dedent(
        inspect.getsource(BackgroundScheduler._run_dq_mirror_landing)))
    calls = [
        (node.lineno, node.args[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name) and node.func.id == "check"
        and node.args and isinstance(node.args[0], ast.Constant)
    ]
    return [name for _, name in sorted(calls)]


class TestEveryCheckIsIsolated:
    def test_all_sixteen_run_through_check_and_each_exactly_once(self):
        assert sorted(check_order()) == sorted(ALL_CHECKS)
        assert len(check_order()) == len(set(check_order())) == 16


def _critical(name):
    return IntegrityIssue(
        check_name=name, table_name="app.stock_movements",
        severity=Severity.CRITICAL, count=1, sample_ids=(55716,),
        description="a movement Postgres does not hold",
    )


async def _run(outcomes):
    """Run the real job with every check stubbed to `outcomes[name]`.

    An outcome is a list of issues, or an Exception to raise. Returns what the
    job persisted, what it paged, and whether it announced a recovery.
    """
    from core.scheduler import BackgroundScheduler

    persisted, paged, resolved = {}, [], []

    @asynccontextmanager
    async def _conn():
        yield MagicMock()

    store = MagicMock()
    store.connection = _conn

    def _persist(conn, **kw):
        persisted.update(kw)
        return 1

    patches = [
        patch("core.mirror_reconciliation.configured", return_value=True),
        patch("core.mirror_reconciliation.read_duckdb_side", return_value={}),
        patch("core.duckdb_store.get_store", new=AsyncMock(return_value=store)),
        patch("core.data_quality.persist_run", side_effect=_persist),
        patch("core.data_quality.machine_attempts_note", return_value=None),
        patch("core.data_quality.evidence_for_agent", return_value={}),
    ]
    for module, names in CHECKS_BY_MODULE.items():
        for name in names:
            outcome = outcomes.get(name, [])
            mock = (AsyncMock(side_effect=outcome) if isinstance(outcome, Exception)
                    else AsyncMock(return_value=list(outcome)))
            patches.append(patch(f"{module}.{name}", new=mock))

    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)

    async def _page(layer, message, key=None, conditions=(), evidence=None):
        paged.append(list(conditions))
        return True

    async def _resolve(layer, issues, error_message):
        # The real method returns early on error_message; record whether it
        # would have announced anything.
        if not error_message:
            resolved.append([i.check_name for i in issues])

    scheduler._send_dq_alert_throttled = _page
    scheduler._resolve_dq_layer = _resolve

    for p_ in patches:
        p_.start()
    try:
        await scheduler._run_dq_mirror_landing()
    finally:
        for p_ in reversed(patches):
            p_.stop()
    return persisted, paged, resolved


class TestOneRaisingCheckSilencesNothing:
    @pytest.mark.asyncio
    async def test_the_checks_after_it_still_run_and_their_findings_are_kept(self):
        """The shape that used to lose them: Silver raises early, and the lost
        stock movement is found three checks later by the operational one."""
        persisted, _, _ = await _run({
            "reconcile_silver": RuntimeError("silver.orders is gone"),
            "reconcile_operational": [_critical("mirror_lost_rows")],
        })
        assert [i.check_name for i in persisted["issues"]] == ["mirror_lost_rows"]

    @pytest.mark.asyncio
    async def test_the_run_is_still_failed_and_names_every_check_that_raised(self):
        persisted, _, _ = await _run({
            "reconcile_silver": RuntimeError("silver.orders is gone"),
            "reconcile_gold": ValueError("gold.daily_revenue is gone"),
        })
        message = persisted["error_message"]
        assert message and message.startswith("2 check(s) raised")
        assert "reconcile_silver: RuntimeError" in message
        assert "reconcile_gold: ValueError" in message

    @pytest.mark.asyncio
    async def test_a_critical_from_a_completed_check_still_pages(self):
        """Withholding it because an unrelated check raised is exactly how one
        flaky round-trip used to silence a lost row."""
        _, paged, _ = await _run({
            "reconcile_silver": RuntimeError("transient"),
            "reconcile_operational": [_critical("mirror_lost_rows")],
        })
        assert paged == [["mirror_lost_rows"]]

    @pytest.mark.asyncio
    async def test_a_run_with_any_unverified_check_announces_no_recovery(self):
        """Unchanged, and it must stay unchanged: a check that did not run proves
        nothing, so the run cannot clear a page it never re-examined."""
        _, _, resolved = await _run({"reconcile_operational": RuntimeError("down")})
        assert resolved == []

    @pytest.mark.asyncio
    async def test_a_clean_run_is_exactly_what_it_was(self):
        persisted, paged, resolved = await _run({})
        assert persisted["error_message"] is None
        assert persisted["issues"] == [] and paged == []
        assert resolved == [[]]
