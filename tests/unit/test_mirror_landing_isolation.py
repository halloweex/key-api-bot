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
        "reconcile_expenses", "reconcile_gold", "pg_gold_internal_check",
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
    def test_all_seventeen_run_through_check_and_each_exactly_once(self):
        """Sixteen every run, and `pg_gold_internal_check` only while Postgres
        alone derives the warehouse (DN-28) — see the class below."""
        assert sorted(check_order()) == sorted(ALL_CHECKS)
        assert len(check_order()) == len(set(check_order())) == 17


def _critical(name):
    return IntegrityIssue(
        check_name=name, table_name="app.stock_movements",
        severity=Severity.CRITICAL, count=1, sample_ids=(55716,),
        description="a movement Postgres does not hold",
    )


async def _run(outcomes, mocks=None):
    """Run the real job with every check stubbed to `outcomes[name]`.

    An outcome is a list of issues, or an Exception to raise. Returns what the
    job persisted, what it paged, and whether it announced a recovery. Pass a
    dict as `mocks` to get each check's stub back by name, to ask how the job
    called it.
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
            if mocks is not None:
                mocks[name] = mock
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


class TestTheUtmCompletenessCheckRunsAsProductionRunsIt:
    """The AST walk above finds the label; only running the job runs the
    lambda under it. That lambda is where the production grace is decided:
    `reconcile_order_utm_completeness` resolves it from the ship's floor when
    it is given none, and every test of the check itself passes one or relies
    on that default — so a keyword added here would move the grace the job
    pages on with nothing watching (DN-16)."""

    @pytest.mark.asyncio
    async def test_its_finding_reaches_the_run_and_it_is_called_with_nothing(self):
        mocks = {}
        missing = IntegrityIssue(
            check_name="pg_order_utm_missing", table_name="silver.order_utm",
            severity=Severity.CRITICAL, count=1, sample_ids=(301,),
            description="an order with a comment and no verdict",
        )
        persisted, paged, _ = await _run(
            {"reconcile_order_utm_completeness": [missing]}, mocks=mocks)

        assert [i.check_name for i in persisted["issues"]] == ["pg_order_utm_missing"]
        assert persisted["error_message"] is None
        assert paged == [["pg_order_utm_missing"]]
        mocks["reconcile_order_utm_completeness"].assert_awaited_once_with()


class TestTheGoldRollUpStandsAloneUnderPostgres:
    """DN-28: `gold_rollup_mismatch` is asked of Postgres by itself while
    Postgres alone derives, and `reconcile_gold` leaves it out on the same
    predicate. In this build the predicate is never true, so production runs
    exactly the sixteen it ran before."""

    @pytest.mark.asyncio
    async def test_while_duckdb_derives_it_is_not_asked(self, monkeypatch):
        from core import warehouse_cutover

        monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.DUCKDB)
        mocks = {}
        await _run({}, mocks=mocks)
        mocks["pg_gold_internal_check"].assert_not_awaited()
        mocks["reconcile_gold"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_under_postgres_it_is_asked_once_and_its_finding_pages(
        self, monkeypatch,
    ):
        from core import warehouse_cutover

        monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.POSTGRES)
        mocks = {}
        mismatch = IntegrityIssue(
            check_name="gold_rollup_mismatch", table_name="gold.daily_revenue",
            severity=Severity.CRITICAL, count=1, description="fine rows do not add up",
        )
        persisted, paged, _ = await _run(
            {"pg_gold_internal_check": [mismatch]}, mocks=mocks)
        mocks["pg_gold_internal_check"].assert_awaited_once_with()
        assert [i.check_name for i in persisted["issues"]] == ["gold_rollup_mismatch"]
        assert paged == [["gold_rollup_mismatch"]]

    @pytest.mark.asyncio
    async def test_it_raising_fails_the_run_like_any_other_check(self, monkeypatch):
        from core import warehouse_cutover

        monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.POSTGRES)
        persisted, _, resolved = await _run(
            {"pg_gold_internal_check": RuntimeError("gold.daily_revenue is gone")})
        assert "pg_gold_internal_check: RuntimeError" in persisted["error_message"]
        assert resolved == []

    def test_it_sits_right_after_reconcile_gold(self):
        order = check_order()
        assert order.index("pg_gold_internal_check") == order.index("reconcile_gold") + 1
