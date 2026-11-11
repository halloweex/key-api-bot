"""An integrity run that could see nothing is a failed run, and blindness is
news every morning it stands (the owner's decision O10 (a)+(c), 2026-11-11).

Until then a run in which every Postgres twin and the chain invariants filed
only their `*_unwatched` findings was journalled a success: the layer's age
moved, so the canary's 12 h on `integrity` (`dq_stale:integrity`) could
never fire however long Postgres stayed unreadable to them, and the digest
restated the blindness only on its weekly beat. Now such a run carries
`error_message` and moves no age, and a blindness finding is news on every
digest it appears in.

What is blind is read off the run's own findings, never a hand list: the
twins count their blind groups in their findings
(`pg_warehouse_dq.looked_at_nothing`), the chain invariants name the part
each blindness covers (`pg_chain_invariants.looked_at_nothing`), and the
digest's blindness names are read off the modules that file them
(`data_quality.blindness_names`), held here to every hold function in
`core/` by an AST walk.
"""
from __future__ import annotations

import ast
import asyncio
import itertools
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import data_quality as dq
from core import pg_chain_invariants as inv
from core import pg_warehouse_dq as twins
from core.data_quality import DigestSection, IntegrityIssue, Severity
from tests.unit.test_pg_warehouse_dq import _facts

ROOT = Path(__file__).resolve().parents[2]

# Each group's judge, by the module name the job's lambdas call at run time.
JUDGES = {
    "silver_arc": "_silver_arc_check",
    "attribution": "_attribution_check",
    "line_items": "_line_items_check",
    "derivation_signal": "_signal_check",
    "order_landing": "_order_landing_check",
    "silver_row_values": "_row_values_check",
}


def _boom(*_a, **_k):
    raise KeyError("a field the verdict expected")


# ─── The twins ────────────────────────────────────────────────────────────────


class TestTheTwinsLookedAtNothing:
    def test_the_judges_are_the_groups(self):
        assert set(JUDGES) == set(twins.GROUPS)

    def test_every_combination_of_unread_and_raised_groups(self, monkeypatch):
        """Each group unread, raised while judged, or looked: 3^6 runs. The
        twins looked at nothing exactly when no group looked. Mutation: count
        the collapsed finding once (`blind += 1`), or call any blind group
        nothing (`blind >= 1`)."""
        for states in itertools.product(("unread", "raised", "looked"),
                                        repeat=len(twins.GROUPS)):
            plan = dict(zip(twins.GROUPS, states))
            with monkeypatch.context() as m:
                for group, state in plan.items():
                    if state == "raised":
                        m.setattr(twins, JUDGES[group], _boom)
                facts = _facts(**{g: twins.Unwatched(f"{g} unread")
                                  for g, s in plan.items() if s == "unread"})
                issues = twins.check_pg_warehouse(facts)
            expected = "looked" not in states
            assert twins.looked_at_nothing(issues) is expected, plan

    def test_a_whole_blindness_and_no_facts_saw_nothing(self):
        assert twins.looked_at_nothing(
            twins.check_pg_warehouse(twins.Facts.blind("Postgres refused")))
        assert twins.looked_at_nothing(twins.check_pg_warehouse(None))

    def test_a_clean_run_saw_everything(self):
        assert twins.check_pg_warehouse(_facts()) == []
        assert not twins.looked_at_nothing([])


# ─── The chain invariants ────────────────────────────────────────────────────


def _expenses_read():
    return inv.Expenses(
        allocator=inv.Allocator("app.manual_expenses", "app.manual_expenses_id_seq", 2, 1),
        nulls=inv.Nulls("app.manual_expenses", {"created_at": 0}))


class TestTheChainsLookedAtNothing:
    WATCHED = ("pg_expenses_write", "pg_goals_write")

    def _judged(self, facts):
        return inv.looked_at_nothing(facts, inv.judge(facts))

    def test_a_whole_blindness_saw_nothing(self):
        assert self._judged(inv.Facts.blind(self.WATCHED, "Postgres refused"))

    def test_a_raising_verdict_saw_nothing(self, monkeypatch):
        monkeypatch.setattr(inv, "check_chain_invariants", _boom)
        assert self._judged(inv.Facts(watched=self.WATCHED))

    def test_every_part_blind_saw_nothing(self):
        """Each watched chain's group, the markers and the watermarks, each
        unread on its own. Mutation: drop a part from what is looked at."""
        facts = inv.Facts(
            watched=self.WATCHED,
            expenses=inv.Unwatched("refused"), goals=inv.Unwatched("refused"),
            latch=inv.Unwatched("no directory"),
            watermarks_unread=inv.Unwatched("refused"))
        assert self._judged(facts)

    @pytest.mark.parametrize("seen", ["expenses", "latch", "watermarks"])
    def test_one_part_read_saw_something(self, seen):
        """Mutation: judge on any blindness finding at all."""
        parts = dict(expenses=inv.Unwatched("refused"), goals=inv.Unwatched("refused"),
                     latch=inv.Unwatched("no directory"),
                     watermarks_unread=inv.Unwatched("refused"))
        if seen == "expenses":
            parts["expenses"] = _expenses_read()
        elif seen == "latch":
            parts["latch"] = inv.Latch()
        else:
            parts["watermarks_unread"] = None
        facts = inv.Facts(watched=self.WATCHED, **parts)
        assert any(i.check_name == inv.UNWATCHED for i in inv.judge(facts))
        assert not self._judged(facts)

    def test_nothing_watched_is_nothing_to_see(self):
        assert self._judged(inv.Facts(watched=()))


# ─── The job ─────────────────────────────────────────────────────────────────


@pytest.fixture
def job(monkeypatch):
    """The integrity job, its journal a list (`tests/journal_fake.py`), its
    Postgres pre-reads refused, the scan clean unless a test says otherwise."""
    from core.scheduler import BackgroundScheduler
    from tests.journal_fake import install_journal

    journal = install_journal(monkeypatch)
    scheduler = BackgroundScheduler()
    sent = AsyncMock(return_value=True)
    monkeypatch.setattr(scheduler, "_send_dq_alert_throttled", sent)
    refused = AsyncMock(side_effect=RuntimeError("no store in this test"))
    with ExitStack() as stack:
        for target in ("core.pg_inventory_write.read_snapshot_calendar",
                       "core.pg_chain_watermarks.read_values_and_stamps",
                       "core.pg_frozen_metadata.frozen_sync_rows"):
            stack.enter_context(patch(target, refused))
        stack.enter_context(patch("core.alerting.resolve_group", AsyncMock(return_value=0)))
        yield scheduler, journal, sent


def _run(scheduler, twin_facts, chain_facts, scan=()):
    with patch.object(twins, "read_facts", AsyncMock(return_value=twin_facts)), \
         patch.object(inv, "read_facts", AsyncMock(return_value=chain_facts)), \
         patch("core.data_quality.check_internal_integrity", return_value=list(scan)):
        return asyncio.run(scheduler._run_dq_integrity())


def _integrity_age():
    from core import dq_journal

    return asyncio.run(dq_journal.last_success_ages_pg(["integrity"]))["integrity"]


ALL_TWINS_BLIND = twins.Facts.blind("Postgres refused the snapshot")
WATCHED = tuple(sorted(inv.watched_chains()))
ALL_CHAINS_BLIND = inv.Facts.blind(WATCHED, "Postgres refused the read")


class TestTheJob:
    def test_a_run_that_saw_nothing_is_failed_and_moves_no_age(self, job):
        """Mutation: journal `error_message` alone again (`error_message=
        error_message`), and the blind run moves the age."""
        from core.scheduler import BLIND_INTEGRITY_RUN

        scheduler, journal, _ = job
        good = _run(scheduler, _facts(), ALL_CHAINS_BLIND)
        assert good["error"] is None
        before = _integrity_age()
        assert before["last_success_at"] == journal.run(good["run_id"])["started_at"]

        blind = _run(scheduler, ALL_TWINS_BLIND, ALL_CHAINS_BLIND)

        assert blind["error"] == BLIND_INTEGRITY_RUN
        run = journal.run(blind["run_id"])
        assert run["error_message"] == BLIND_INTEGRITY_RUN
        assert run["status"] == "FAILED"
        found = {i["check_name"] for i in journal.issues(blind["run_id"])}
        assert {twins.WHOLE_UNWATCHED, inv.UNWATCHED} <= found
        assert _integrity_age()["last_success_at"] == before["last_success_at"]

    def test_one_twin_seeing_is_a_success(self, job):
        scheduler, journal, _ = job
        five_blind = _facts(**{g: twins.Unwatched("refused")
                               for g in twins.GROUPS if g != "silver_arc"})
        result = _run(scheduler, five_blind, ALL_CHAINS_BLIND)
        assert result["error"] is None
        assert journal.run(result["run_id"])["error_message"] is None
        assert _integrity_age()["last_success_at"] is not None

    def test_one_chain_part_seeing_is_a_success(self, job):
        scheduler, journal, _ = job
        partly = inv.Facts(watched=("pg_expenses_write",), expenses=_expenses_read(),
                           latch=inv.Unwatched("no directory"),
                           watermarks_unread=inv.Unwatched("refused"))
        result = _run(scheduler, ALL_TWINS_BLIND, partly)
        assert result["error"] is None
        assert journal.run(result["run_id"])["error_message"] is None

    def test_the_scan_still_pages_in_a_blind_run(self, job):
        """The scan beside them did look: its CRITICAL pages as it did
        before, though the run is journalled failed."""
        scheduler, _, sent = job
        stalled = IntegrityIssue(check_name="freshness_orders", table_name="sync_metadata",
                                 severity=Severity.CRITICAL, count=1)
        _run(scheduler, ALL_TWINS_BLIND, ALL_CHAINS_BLIND, scan=[stalled])
        assert sent.await_count == 1
        assert sent.await_args.kwargs["conditions"] == ["freshness_orders"]


# ─── The digest ──────────────────────────────────────────────────────────────


NOW = datetime(2026, 11, 11, 6, 0, tzinfo=timezone.utc)


def _section(name, *, severity="WARN"):
    finding = {"check_name": name, "table_name": "t", "severity": severity, "count": 1}
    return DigestSection(
        layer="integrity",
        run={"status": "WARN", "started_at": (NOW - timedelta(hours=5)).isoformat()},
        issues=[finding], previous_issues=[dict(finding)], age_hours=5.0)


class TestTheDigest:
    @pytest.mark.parametrize("name", [twins.UNWATCHED_NAMES["line_items"],
                                      twins.WHOLE_UNWATCHED, inv.UNWATCHED])
    def test_a_standing_blindness_is_news_on_two_consecutive_days(self, name):
        """Unchanged (`=`) and the digest sent the morning before: still
        news, both mornings. Mutation: drop the blindness clause, and the
        second morning is silent until the weekly beat."""
        for day in (0, 1):
            now = NOW + timedelta(days=day)
            message = dq.build_digest([_section(name)], last_sent_at=now - timedelta(days=1),
                                      now=now)
            assert message is not None, (name, day)
            assert "(=)" in message

    def test_a_standing_known_problem_still_waits_for_the_weekly_beat(self):
        message = dq.build_digest([_section("pg_headline_vs_line_items")],
                                  last_sent_at=NOW - timedelta(days=1), now=NOW)
        assert message is None


def _hold_functions():
    """Every function in `core/` whose name says it reads which conditions a
    run could not re-examine, with its module."""
    import importlib

    for path in sorted((ROOT / "core").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and "unverified" in node.name:
                yield importlib.import_module(f"core.{path.stem}"), node


def _compared_names(module, fn: ast.FunctionDef) -> set:
    """The finding names `fn` compares a finding against: string literals
    and module-level string constants on either side of a comparison."""
    names = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Compare):
            for side in (node.left, *node.comparators):
                if isinstance(side, ast.Constant) and isinstance(side.value, str):
                    names.add(side.value)
                elif isinstance(side, ast.Name) and isinstance(
                        getattr(module, side.id, None), str):
                    names.add(getattr(module, side.id))
    return names


class TestTheBlindnessNames:
    def test_every_hold_function_reads_only_blindness_names(self):
        """A new finding that makes a run hold its conditions is blindness;
        the digest must know it by name. Mutation: drop a module's name from
        `blindness_names` (`ch_history.HISTORY_UNREACHABLE`)."""
        blindness = dq.blindness_names()
        walked = list(_hold_functions())
        assert {m.__name__ for m, _ in walked} >= {
            "core.data_quality", "core.pg_chain_invariants", "core.ch_silver",
            "core.ch_history", "core.pg_warehouse_dq"}
        for module, fn in walked:
            missing = _compared_names(module, fn) - blindness
            assert not missing, (module.__name__, fn.name, sorted(missing))

    def test_the_twins_file_theirs_by_constant(self):
        """`unverified_pg_conditions` reads what `check_pg_warehouse` held,
        not names; what it filed for the holding are these."""
        assert set(twins.UNWATCHED_NAMES.values()) | {twins.WHOLE_UNWATCHED} <= (
            dq.blindness_names())

    def test_every_blindness_name_is_a_registered_condition(self):
        from core.alerting import REGISTRY

        assert dq.blindness_names() <= set(REGISTRY)
