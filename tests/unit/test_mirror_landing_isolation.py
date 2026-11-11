"""One check that raises must not silence the other six.

`dq_mirror_landing` runs seven checks in one job — the UTM verdicts'
completeness, Gold's two grains, the bot's state, the order archive, the
buyers' completeness, ClickHouse and its archive. (Seventeen until step 14's
S14-12 deleted the comparisons against DuckDB; eight until the owner's
decision O6 of 2026-11-11 stopped checking the vitrina.) It used to wrap all
of them in ONE try, so an exception anywhere skipped every check after it, and
the alert gate's `and not error_message` then withheld the page for any
CRITICAL already found above the failure.

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
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import IntegrityIssue, Severity


# Every check, in the order the job runs it, named as it is imported.
CHECKS_BY_MODULE = {
    "core.mirror_reconciliation": [
        "reconcile_order_utm_completeness", "pg_gold_internal_check",
        "reconcile_bot_state", "reconcile_order_versions",
        "reconcile_buyer_completeness",
    ],
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
    def test_all_seven_run_through_check_and_each_exactly_once(self):
        assert check_order() == ALL_CHECKS
        assert len(check_order()) == len(set(check_order())) == 7

    def test_nothing_reads_duckdb(self):
        """The comparisons against DuckDB went in step 14's S14-12; the job
        asks the store for nothing."""
        from core.scheduler import BackgroundScheduler

        tree = ast.parse(textwrap.dedent(
            inspect.getsource(BackgroundScheduler._run_dq_mirror_landing)))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        names |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        modules = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
        assert not {"get_store", "store"} & names
        assert not {m for m in modules if m and "duckdb" in m}


def _critical(name):
    return IntegrityIssue(
        check_name=name, table_name="app.stock_movements",
        severity=Severity.CRITICAL, count=1, sample_ids=(55716,),
        description="a movement Postgres does not hold",
    )


async def _run(outcomes, mocks=None, held=None, hook=None, returned=None):
    """Run the real job with every check stubbed to `outcomes[name]`.

    An outcome is a list of issues, or an Exception to raise. Returns what the
    job persisted, what it paged, and whether it announced a recovery. Pass a
    dict as `mocks` to get each check's stub back by name, to ask how the job
    called it, a list as `held` to collect the conditions each resolution
    was told it could not re-examine (`unverified`), and a callable as `hook`
    to break something after the stubs are in place and before the job runs,
    and a list as `returned` to get what the job returned.
    """
    from core.scheduler import BackgroundScheduler

    persisted, paged, resolved = {}, [], []

    # What the job hands the journal's door. Until step 14's S14-10 this
    # module routed chain 9 back to DuckDB and caught DuckDB's `persist_run`
    # behind the door instead.
    async def _journal_run(**kw):
        persisted.update(kw)
        return 1

    patches = [
        patch("core.mirror_reconciliation.configured", return_value=True),
        patch("core.dq_journal.journal_run", new=_journal_run),
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

    async def _resolve(layer, issues, error_message, unverified=()):
        # The real method returns early on error_message; record whether it
        # would have announced anything.
        if held is not None:
            held.append(list(unverified))
        if not error_message:
            resolved.append([i.check_name for i in issues])

    scheduler._send_dq_alert_throttled = _page
    scheduler._resolve_dq_layer = _resolve

    for p_ in patches:
        p_.start()
    try:
        if hook is not None:
            hook()
        result = await scheduler._run_dq_mirror_landing()
        if returned is not None:
            returned.append(result)
    finally:
        for p_ in reversed(patches):
            p_.stop()
    return persisted, paged, resolved


class TestOneRaisingCheckSilencesNothing:
    @pytest.mark.asyncio
    async def test_the_checks_after_it_still_run_and_their_findings_are_kept(self):
        """The shape that used to lose them: an early check raises, and a
        stalled archive is found three checks later."""
        persisted, _, _ = await _run({
            "reconcile_order_utm_completeness": RuntimeError("silver.order_utm is gone"),
            "reconcile_order_versions": [_critical("order_versions_stalled")],
        })
        assert [i.check_name for i in persisted["issues"]] == ["order_versions_stalled"]

    @pytest.mark.asyncio
    async def test_the_run_is_still_failed_and_names_every_check_that_raised(self):
        persisted, _, _ = await _run({
            "reconcile_bot_state": RuntimeError("data/bot.db is gone"),
            "pg_gold_internal_check": ValueError("gold.daily_revenue is gone"),
        })
        message = persisted["error_message"]
        assert message and message.startswith("2 check(s) raised")
        assert "reconcile_bot_state: RuntimeError" in message
        assert "pg_gold_internal_check: ValueError" in message

    @pytest.mark.asyncio
    async def test_a_critical_from_a_completed_check_still_pages(self):
        """Withholding it because an unrelated check raised is exactly how one
        flaky round-trip used to silence a lost row."""
        _, paged, _ = await _run({
            "reconcile_order_utm_completeness": RuntimeError("transient"),
            "reconcile_order_versions": [_critical("order_versions_stalled")],
        })
        assert paged == [["order_versions_stalled"]]

    @pytest.mark.asyncio
    async def test_a_run_with_any_unverified_check_announces_no_recovery(self):
        """Unchanged, and it must stay unchanged: a check that did not run proves
        nothing, so the run cannot clear a page it never re-examined."""
        _, _, resolved = await _run({"reconcile_buyer_completeness": RuntimeError("down")})
        assert resolved == []

    @pytest.mark.asyncio
    async def test_a_clean_run_is_exactly_what_it_was(self):
        persisted, paged, resolved = await _run({})
        assert persisted["error_message"] is None
        assert persisted["issues"] == [] and paged == []
        assert resolved == [[]]

    @pytest.mark.asyncio
    async def test_clickhouse_raising_files_gold_unwatched(self):
        """OD-08: a raise leaves the comparison nothing to return, so the job
        files `gold_values_unwatched` itself — CRITICAL, it pages."""
        persisted, paged, _ = await _run(
            {"reconcile_clickhouse": RuntimeError("ClickHouse refused")})
        assert [i.check_name for i in persisted["issues"]] == ["gold_values_unwatched"]
        assert persisted["issues"][0].severity is Severity.CRITICAL
        assert paged == [["gold_values_unwatched"]]


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


class TestTheGoldRollUpIsAskedEveryRun:
    """DN-28: `gold_rollup_mismatch` is asked of Postgres by itself. It stood
    beside `reconcile_gold` while that ran, and on every run since step 13
    stood the comparison down; step 14's S14-12 deleted the comparison, so
    it is asked unconditionally."""

    @pytest.mark.asyncio
    async def test_it_is_asked_once_and_its_finding_pages(self):
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
    async def test_it_raising_fails_the_run_like_any_other_check(self):
        persisted, _, resolved = await _run(
            {"pg_gold_internal_check": RuntimeError("gold.daily_revenue is gone")})
        assert "pg_gold_internal_check: RuntimeError" in persisted["error_message"]
        assert resolved == []


class TestTheRunSaysWhichChecksItAsked:
    """Absent findings cannot say whether a check ran. The step-13 rehearsal
    ran into exactly that (its P5 could not fail), so the job names what it
    asked, raise or verdict, on the one line a reader outside the process
    has."""

    @pytest.mark.asyncio
    async def test_it_names_every_check_in_order(self):
        returned = []
        await _run({}, returned=returned)
        assert returned[0]["checks_run"] == check_order() == ALL_CHECKS

    @pytest.mark.asyncio
    async def test_a_check_that_raised_was_still_asked(self):
        returned = []
        await _run({"pg_gold_internal_check": ValueError("boom")}, returned=returned)
        assert "pg_gold_internal_check" in returned[0]["checks_run"]

    def test_it_is_on_the_completion_line(self):
        """`extra=result` is how the formatter puts it on the line; a key a
        LogRecord already has would raise there instead."""
        import logging

        source = inspect.getsource(
            __import__("core.scheduler", fromlist=["BackgroundScheduler"])
            .BackgroundScheduler._run_dq_mirror_landing)
        assert 'logger.info("Mirror reconciliation complete", extra=result)' in source
        assert '"checks_run": list(checks_run)' in source
        assert "checks_run" not in logging.LogRecord(
            "x", logging.INFO, "f", 1, "m", None, None).__dict__
