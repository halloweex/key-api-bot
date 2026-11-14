"""The analysts' captures' foundation: the switch, the spec, the two helpers
on a fake connection, and the watch the integrity job runs.

The helpers on a real PostgreSQL — a capture that raises leaves the writer's
row committed — are `tests/integration/test_analytics_captures_pg.py`. Each
test names the mutation it kills.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core import analytics_captures as cap
from core import ga4_capture
from core.data_quality import Severity

NOW = datetime(2026, 11, 13, 7, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _switch_unset(monkeypatch):
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    cap._warned_values.clear()


# ─── The switch ───────────────────────────────────────────────────────────────


class TestTheSwitch:
    @pytest.mark.parametrize("value", ["off", "OFF", " off ", "0", "false", "False"])
    def test_off_stops_the_captures(self, monkeypatch, value):
        monkeypatch.setenv(cap.SWITCH_ENV, value)
        assert cap.captures_on() is False

    @pytest.mark.parametrize("value", ["", "on", "1", "true", "TRUE"])
    def test_on_and_unset_run_them(self, monkeypatch, value, caplog):
        monkeypatch.setenv(cap.SWITCH_ENV, value)
        with caplog.at_level(logging.WARNING, logger=cap.__name__):
            assert cap.captures_on() is True
        assert not caplog.records

    def test_unset_is_on(self):
        assert cap.captures_on() is True

    def test_an_unknown_value_is_on_and_warns_once(self, monkeypatch, caplog):
        """Not a store selector: a typo must not take a writer down, so it
        does not raise. Kills: a raise, a silent typo, or one warning per
        capture."""
        monkeypatch.setenv(cap.SWITCH_ENV, "of")
        with caplog.at_level(logging.WARNING, logger=cap.__name__):
            assert cap.captures_on() is True
            assert cap.captures_on() is True
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


# ─── The spec ─────────────────────────────────────────────────────────────────


class TestTheSpec:
    def test_seven_captures_each_with_a_module_and_a_kind(self):
        assert len(cap.CAPTURES) == 7
        for table, spec in cap.CAPTURES.items():
            assert table.startswith("app."), table
            assert spec.writer.startswith("core."), table
            assert spec.kind in (cap.IN_TRANSACTION, cap.OWN_TRANSACTION, cap.JOB), table

    def test_the_kinds_are_the_designs(self):
        """§3.3: four ride their writer's transaction, the two content diffs
        run on their own, the funnel is a job."""
        kinds = {t: s.kind for t, s in cap.CAPTURES.items()}
        assert {t for t, k in kinds.items() if k == cap.IN_TRANSACTION} == {
            "app.offer_cost_daily", "app.revenue_prediction_vintages",
            "app.revenue_goal_history", "app.expense_deletions"}
        assert {t for t, k in kinds.items() if k == cap.OWN_TRANSACTION} == {
            "app.catalogue_versions", "app.order_channel_verdicts"}
        assert {t for t, k in kinds.items() if k == cap.JOB} == {"app.ga4_funnel"}

    def test_only_the_content_diffs_carry_a_self_check(self):
        assert {t for t, s in cap.CAPTURES.items() if s.self_checked} == {
            "app.catalogue_versions", "app.order_channel_verdicts"}

    def test_the_watermark_row_is_named_by_the_table(self):
        assert cap.state_row("app.offer_cost_daily") == "capture.app.offer_cost_daily"

    def test_write_chains_reads_the_writers_off_the_spec(self):
        from core.write_chains import CAPTURE_TABLES

        assert CAPTURE_TABLES == {t: s.writer for t, s in cap.CAPTURES.items()}


# ─── The helpers, on a fake connection ────────────────────────────────────────


class _Savepoint:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.depth += 1
        self.conn.log.append("SAVEPOINT" if self.conn.depth > 1 else "BEGIN")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.log.append(("ROLLBACK" if exc_type else "RELEASE")
                             + (" TO SAVEPOINT" if self.conn.depth > 1 else ""))
        self.conn.depth -= 1
        return False


class _Conn:
    """Records statements; raises on any statement containing `fail_on`."""

    def __init__(self, *, in_transaction=True, fail_on=None, raise_with=RuntimeError):
        self.depth = 1 if in_transaction else 0
        self.log = []
        self.sql = []
        self.timeouts = []
        self.fail_on = fail_on
        self.raise_with = raise_with

    def is_in_transaction(self):
        return self.depth > 0

    def transaction(self):
        return _Savepoint(self)

    async def execute(self, sql, *args, timeout=None):
        self.sql.append((sql, args))
        self.timeouts.append(timeout)
        if self.fail_on and self.fail_on in sql:
            raise self.raise_with("boom")
        return "INSERT 0 3"


class _Mute(_Conn):
    """A connection that cannot say whether it is inside a transaction."""

    def is_in_transaction(self):
        raise AttributeError("is_in_transaction")


async def _never(conn):
    raise AssertionError("the capture ran on a connection that could not answer")


CAPTURE_SQL = "INSERT INTO app.revenue_goal_history (period_type) VALUES ($1)"
GOALS = "app.revenue_goal_history"


def _stamped(conn, *, ok):
    marker = "last_ok_at        = now()" if ok else "failures_since_ok + 1"
    return [args for sql, args in conn.sql if "meta.mirror_state" in sql and marker in sql]


class TestCaptureInTransaction:
    def test_kept_and_stamped_ok_inside_a_savepoint(self):
        conn = _Conn()
        assert asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL, "monthly")) is True
        assert conn.log == ["SAVEPOINT", "RELEASE TO SAVEPOINT"]
        assert conn.sql[0] == (CAPTURE_SQL, ("monthly",))
        assert _stamped(conn, ok=True) == [("capture.app.revenue_goal_history", 3)]

    def test_a_capture_that_raises_rolls_back_alone_and_is_stamped(self, caplog):
        """Kills: the error reaching the writer (its transaction would roll
        back), or a failure nobody stamps."""
        conn = _Conn(fail_on="revenue_goal_history")
        with caplog.at_level(logging.ERROR, logger=cap.__name__):
            assert asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL)) is False
        assert conn.log[:2] == ["SAVEPOINT", "ROLLBACK TO SAVEPOINT"]
        failed = _stamped(conn, ok=False)
        assert failed and failed[0][0] == "capture.app.revenue_goal_history"
        assert failed[0][1].startswith("RuntimeError: boom")
        assert not _stamped(conn, ok=True)
        assert [r.args[0] for r in caplog.records if r.levelno == logging.ERROR] == [GOALS]

    @pytest.mark.parametrize("fail_on", [None, "revenue_goal_history"])
    def test_every_statement_it_runs_is_bounded(self, fail_on):
        """Inside a writer's transaction a capture waiting on a lock holds the
        writer, and every lock it took, for as long as it waits: without a
        bound of its own, KS_PG_TIMEOUT (30 s), and twice that when the stamp
        row is what is held. Kills: any statement of the capture, the stamps
        included, run without asyncpg's per-call timeout."""
        conn = _Conn(fail_on=fail_on)
        asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL))
        assert len(conn.timeouts) == 2
        assert conn.timeouts == [cap.CAPTURE_TIMEOUT_S] * 2
        assert 0 < cap.CAPTURE_TIMEOUT_S <= 5

    def test_a_stamp_that_fails_costs_the_stamp_only(self):
        conn = _Conn(fail_on="meta.mirror_state")
        assert asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL)) is False

    def test_the_switch_off_executes_nothing(self, monkeypatch):
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        conn = _Conn()
        assert asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL)) is False
        assert conn.sql == [] and conn.log == []

    def test_outside_a_transaction_it_captures_nothing_and_says_so(self):
        """An in-transaction capture handed a connection with no open
        transaction would commit apart from its writer. Kills: capturing
        anyway."""
        conn = _Conn(in_transaction=False)
        assert asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL)) is False
        assert CAPTURE_SQL not in [s for s, _ in conn.sql]
        assert _stamped(conn, ok=False)

    def test_a_table_not_in_the_spec_is_refused(self):
        conn = _Conn()
        assert asyncio.run(cap.capture_in_transaction(conn, "app.elsewhere", CAPTURE_SQL)) is False
        assert conn.sql == []

    def test_a_connection_that_cannot_say_whether_it_is_inside_never_raises(self):
        """Kills: `is_in_transaction()` asked outside the guard — a
        connection that cannot answer would raise into the writer."""
        conn = _Mute()
        assert asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL)) is False
        assert asyncio.run(cap.run_own_transaction(VERSIONS, _never, conn=conn)) is False
        assert CAPTURE_SQL not in [s for s, _ in conn.sql]
        assert [args[0] for args in _stamped(conn, ok=False)] == [
            "capture.app.revenue_goal_history", "capture.app.catalogue_versions"]

    def test_a_cancellation_goes_through(self):
        conn = _Conn(fail_on="revenue_goal_history", raise_with=asyncio.CancelledError)
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(cap.capture_in_transaction(conn, GOALS, CAPTURE_SQL))


VERSIONS = "app.catalogue_versions"


class TestRunOwnTransaction:
    def test_kept_and_stamped_with_its_rows(self):
        conn = _Conn(in_transaction=False)

        async def capture(c):
            await c.execute("INSERT INTO app.catalogue_versions (product_id) VALUES (1)")
            return 2

        assert asyncio.run(cap.run_own_transaction(VERSIONS, capture, conn=conn)) is True
        assert conn.log == ["BEGIN", "RELEASE"]
        assert _stamped(conn, ok=True) == [("capture.app.catalogue_versions", 2)]

    def test_a_refused_self_check_is_stamped_as_one(self):
        conn = _Conn(in_transaction=False)

        async def capture(c):
            raise cap.CaptureRefused("would retire 600 of 1000 products")

        assert asyncio.run(cap.run_own_transaction(VERSIONS, capture, conn=conn)) is False
        failed = _stamped(conn, ok=False)
        assert failed and failed[0][1].startswith(cap.SELF_CHECK_MARK)

    def test_any_other_raise_is_a_failure(self):
        conn = _Conn(in_transaction=False)

        async def capture(c):
            raise ValueError("bad row")

        assert asyncio.run(cap.run_own_transaction(VERSIONS, capture, conn=conn)) is False
        failed = _stamped(conn, ok=False)
        assert failed and failed[0][1].startswith("ValueError: bad row")

    def test_inside_a_transaction_it_refuses(self):
        """It would become a savepoint of the caller's — not its own
        transaction. Kills: running anyway."""
        conn = _Conn(in_transaction=True)
        ran = AsyncMock(return_value=1)
        assert asyncio.run(cap.run_own_transaction(VERSIONS, ran, conn=conn)) is False
        ran.assert_not_awaited()

    def test_the_switch_off_runs_nothing(self, monkeypatch):
        monkeypatch.setenv(cap.SWITCH_ENV, "false")
        ran = AsyncMock(return_value=1)
        assert asyncio.run(cap.run_own_transaction(VERSIONS, ran, conn=_Conn(in_transaction=False))) is False
        ran.assert_not_awaited()

    def test_no_pool_is_a_logged_failure_not_a_raise(self):
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=RuntimeError("down"))):
            assert asyncio.run(cap.run_own_transaction(VERSIONS, AsyncMock())) is False


# ─── The watch ────────────────────────────────────────────────────────────────


def _ok(age=timedelta(minutes=5)):
    return cap.Stamp(last_ok_at=NOW - age, failures_since_ok=0, last_error=None)


def _all_ok(**overrides):
    stamps = {t: _ok() for t in cap.CAPTURES}
    stamps.update(overrides)
    return cap.Facts(now=NOW, stamps={t: s for t, s in stamps.items() if s is not None})


def _names(facts):
    return sorted(i.check_name for i in cap.judge(facts))


OLDEST = ga4_capture.Period("day", date(2025, 9, 14), date(2025, 9, 14))


def _behind(missing, **overrides):
    return dataclasses.replace(_all_ok(**overrides), ga4_backlog=(missing, OLDEST))


class TestTheWatch:
    def test_every_capture_fresh_is_quiet(self):
        assert cap.judge(_all_ok()) == []

    def test_a_failing_capture_is_a_warn_named_for_its_table(self):
        facts = _all_ok(**{"app.offer_cost_daily": cap.Stamp(
            NOW - timedelta(hours=30), 2, "NotNullViolationError: price")})
        (issue,) = cap.judge(facts)
        assert issue.check_name == "capture_failed:app.offer_cost_daily"
        assert issue.table_name == "app.offer_cost_daily"
        assert issue.severity == Severity.WARN
        assert issue.count == 2

    def test_a_failing_capture_is_not_also_stalled(self):
        """Kills: one fault filed twice."""
        facts = _all_ok(**{"app.offer_cost_daily": cap.Stamp(None, 1, "boom")})
        assert _names(facts) == ["capture_failed:app.offer_cost_daily"]

    def test_a_refused_self_check_has_its_own_name(self):
        facts = _all_ok(**{"app.catalogue_versions": cap.Stamp(
            NOW, 1, cap.SELF_CHECK_MARK + "CaptureRefused: 600 tombstones")})
        assert _names(facts) == ["capture_self_check_failed:app.catalogue_versions"]

    def test_the_mark_on_a_capture_without_a_self_check_is_a_failure(self):
        facts = _all_ok(**{"app.offer_cost_daily": cap.Stamp(
            NOW, 1, cap.SELF_CHECK_MARK + "x")})
        assert _names(facts) == ["capture_failed:app.offer_cost_daily"]

    @pytest.mark.parametrize("table", sorted(t for t, s in cap.CAPTURES.items()
                                              if s.max_age is not None))
    def test_past_its_cadence_a_capture_is_stalled(self, table):
        limit = cap.CAPTURES[table].max_age
        assert _names(_all_ok(**{table: _ok(limit - timedelta(minutes=1))})) == []
        assert _names(_all_ok(**{table: _ok(limit + timedelta(minutes=1))})) == [
            f"capture_stalled:{table}"]

    def test_never_stamped_is_stalled(self):
        """The first due period after the deploy says so too, by design: an
        unstarted history is not started. Kills: a missing row read as
        healthy."""
        assert _names(_all_ok(**{"app.ga4_funnel": None})) == ["capture_stalled:app.ga4_funnel"]

    @pytest.mark.parametrize("table", sorted(t for t, s in cap.CAPTURES.items()
                                              if s.max_age is None))
    def test_an_on_demand_capture_is_never_stalled(self, table):
        """A goal moves when a human types one; an expense delete when KeyCRM
        drops one. Kills: a quiet month filed as a stall."""
        assert _names(_all_ok(**{table: None})) == []
        assert _names(_all_ok(**{table: _ok(timedelta(days=90))})) == []

    def test_a_ga4_backlog_is_a_warn_that_names_the_next_to_go(self):
        """Kills: periods inside GA4's horizon left unkept with nobody told —
        each that ages out is gone for good (the design's §9)."""
        (issue,) = cap.judge(_behind(3))
        assert (issue.check_name, issue.table_name, issue.severity, issue.count) == (
            cap.GA4_BACKFILL, "app.ga4_funnel", Severity.WARN, 3)

    def test_no_backlog_and_no_ga4_are_quiet(self):
        """None is GA4 not configured: nothing is captured there, and its
        stamp already says so. Kills: a backlog filed with nothing missing."""
        assert cap.judge(dataclasses.replace(_all_ok(), ga4_backlog=(0, None))) == []
        assert _all_ok().ga4_backlog is None and cap.judge(_all_ok()) == []

    def test_the_switch_off_is_said_once_and_stalls_nothing(self):
        """Kills: every capture filed as stalled while an operator switched
        them off on purpose."""
        (issue,) = cap.judge(cap.Facts(switched_off=True))
        assert issue.check_name == cap.SWITCHED_OFF
        assert issue.table_name == cap.WHOLE_PART

    def test_the_switch_off_is_a_warn_that_holds_what_it_did_not_judge(self):
        """An INFO never summons a digest nor is restated in one, so an
        operator who switched the captures off was never told; and a finding
        held by nothing reads as healed. Kills: the switch-off filed INFO, or
        without `unverified` — a `capture_failed` standing when it went off
        would be resolved by a run that never looked at it."""
        facts = dataclasses.replace(
            _all_ok(**{"app.offer_cost_daily": cap.Stamp(None, 2, "boom")}),
            switched_off=True)
        (issue,) = cap.judge(facts)
        assert (issue.check_name, issue.severity) == (cap.SWITCHED_OFF, Severity.WARN)
        assert "capture_failed:app.offer_cost_daily" in issue.unverified
        assert set(issue.unverified) == set(cap.conditions()) - {
            cap.SWITCHED_OFF, cap.UNWATCHED}

    def test_the_switch_off_still_judges_the_ga4_backlog_it_read(self):
        """Reading GA4's backlog captures nothing; a period that leaves the
        horizon while the switch is off is lost for good. Kills: the backlog
        not judged, or held, while off."""
        facts = dataclasses.replace(_behind(4), switched_off=True)
        names = _names(facts)
        assert names == sorted([cap.SWITCHED_OFF, cap.GA4_BACKFILL])
        off = next(i for i in cap.judge(facts) if i.check_name == cap.SWITCHED_OFF)
        assert cap.GA4_BACKFILL not in off.unverified
        # Not read (GA4 not configured, or the read failed): held, not judged.
        off_blind = cap.judge(cap.Facts(switched_off=True))[0]
        assert cap.GA4_BACKFILL in off_blind.unverified

    def test_the_switch_off_with_the_stamps_unread_says_both(self):
        names = _names(cap.Facts(switched_off=True, unwatched="refused"))
        assert names == sorted([cap.SWITCHED_OFF, cap.UNWATCHED])

    def test_unreadable_stamps_hold_every_condition(self):
        """A run that read nothing resolves nothing. Kills: blindness read as
        clean."""
        (issue,) = cap.judge(cap.Facts(unwatched="ConnectionRefusedError: refused"))
        assert issue.check_name == cap.UNWATCHED
        assert issue.severity == Severity.WARN
        assert set(issue.unverified) == set(cap.conditions())

    def test_nothing_is_ever_critical(self):
        """D8 (b): counted and digested, never a page. The integrity job pages
        on CRITICAL alone."""
        scenarios = [_all_ok(**{t: cap.Stamp(None, 1, cap.SELF_CHECK_MARK)}) for t in cap.CAPTURES]
        scenarios += [_all_ok(**{t: None}) for t in cap.CAPTURES]
        scenarios += [cap.Facts(switched_off=True), cap.Facts(unwatched="x"), _behind(500)]
        for facts in scenarios:
            assert all(i.severity != Severity.CRITICAL for i in cap.judge(facts))

    def test_conditions_are_exactly_what_the_watch_can_file(self):
        """The REGISTRY family is `conditions()`; this holds it to `judge`.
        Kills: a name registered that nothing files, or filed unregistered."""
        filed = set()
        for t in cap.CAPTURES:
            filed |= set(_names(_all_ok(**{t: cap.Stamp(None, 1, "boom")})))
            filed |= set(_names(_all_ok(**{t: cap.Stamp(None, 1, cap.SELF_CHECK_MARK)})))
            filed |= set(_names(_all_ok(**{t: None})))
        filed |= set(_names(cap.Facts(switched_off=True)))
        filed |= set(_names(cap.Facts(unwatched="x")))
        filed |= set(_names(_behind(1)))
        assert filed == set(cap.conditions())


class TestTheNamesAreRegisteredWithALever:
    def test_registered_as_conditions(self):
        from core.alerting import REGISTRY, Kind

        for name in cap.conditions():
            assert REGISTRY[name].kind is Kind.CONDITION, name

    def test_each_has_a_lever_of_its_own(self):
        from core.data_quality import DEFAULT_REMEDIATION, remediation_for

        for name in cap.conditions():
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name

    def test_the_blindness_is_news_every_morning(self):
        """The switch off included: every day it stands is a day missing from
        every history. Kills: it left out, to wait a week to be restated."""
        from core.data_quality import blindness_names

        assert {cap.UNWATCHED, cap.SWITCHED_OFF} <= blindness_names()

    def test_the_switch_off_alone_reaches_the_digest(self):
        """Kills: a finding that never produces a message — the INFO it was."""
        from core.data_quality import DigestSection, build_digest

        issues = [{"check_name": i.check_name, "count": i.count,
                   "severity": i.severity.value}
                  for i in cap.judge(cap.Facts(switched_off=True))]
        section = DigestSection(
            layer="integrity", run={"started_at": NOW.isoformat(), "status": "PASS"},
            issues=issues, previous_issues=issues, age_hours=2.0)
        assert build_digest([section], last_sent_at=NOW - timedelta(days=1),
                            now=NOW) is not None

    def test_every_name_reads_as_words(self):
        """The digest prints the label beside the key; a key alone is not
        the alert format's human words. Kills: a capture added to `CAPTURES`
        with no history name, or a family with no words."""
        from core.data_quality import HUMAN_CHECK_NAMES, human_check_name

        for name in cap.conditions():
            assert HUMAN_CHECK_NAMES.get(name), name
            assert human_check_name(name) != name
        assert len({HUMAN_CHECK_NAMES[n] for n in cap.conditions()}) == len(cap.conditions())

    def test_none_is_a_retired_name(self):
        """Rule 15: no name an image from before step 14 treats as retired."""
        from tests.unit.test_rollback_condition_names import ALL_RETIRED

        assert not set(cap.conditions()) & ALL_RETIRED


# ─── The integrity job ────────────────────────────────────────────────────────


@pytest.fixture
def job(monkeypatch):
    """The integrity job with its journal a list, its other Postgres reads
    refused and the twins and chains blind — the captures' findings alone."""
    from core import pg_chain_invariants as inv
    from core import pg_warehouse_dq as twins
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
        stack.enter_context(patch.object(
            twins, "read_facts", AsyncMock(return_value=twins.Facts.blind("test"))))
        stack.enter_context(patch.object(
            inv, "read_facts", AsyncMock(return_value=inv.Facts.blind((), "test"))))
        stack.enter_context(patch("core.data_quality.check_internal_integrity",
                                  return_value=[]))
        yield scheduler, journal, sent


class TestTheIntegrityJob:
    def test_a_failed_capture_is_journalled_and_pages_nobody(self, job):
        """Kills: the watch not wired into the job, or its finding paged."""
        scheduler, journal, sent = job
        facts = _all_ok(**{"app.revenue_goal_history": cap.Stamp(NOW, 1, "boom")})
        with patch.object(cap, "read_facts", AsyncMock(return_value=facts)):
            result = asyncio.run(scheduler._run_dq_integrity())
        found = {i["check_name"]: i for i in journal.issues(result["run_id"])}
        assert found["capture_failed:app.revenue_goal_history"]["severity"] == "WARN"
        sent.assert_not_awaited()

    def test_a_standing_critical_pages_alike_whatever_the_captures_do(self, job):
        """D8 (b): the captures never page, and never ride a page either. With
        a CRITICAL standing, a capture that stamps between two runs must not
        change the page's Gate bucket (the Gate would deliver it again as a
        new incident) nor appear in its body. Kills: the page built from every
        finding the run filed."""
        from core.data_quality import IntegrityIssue

        scheduler, _journal, sent = job
        critical = IntegrityIssue(check_name="freshness_orders", table_name="orders",
                                  severity=Severity.CRITICAL, count=1, description="x")
        runs = [_all_ok(**{"app.ga4_funnel": None,
                           "app.revenue_prediction_vintages": cap.Stamp(None, 1, "boom")}),
                _all_ok(**{"app.revenue_prediction_vintages": None}),
                cap.Facts(switched_off=True)]
        with patch("core.data_quality.check_internal_integrity", return_value=[critical]):
            for facts in runs:
                with patch.object(cap, "read_facts", AsyncMock(return_value=facts)):
                    asyncio.run(scheduler._run_dq_integrity())
        assert sent.await_count == len(runs)
        keys = {call.args[2] for call in sent.await_args_list}
        assert len(keys) == 1
        names = set(cap.conditions())
        for call in sent.await_args_list:
            assert not any(n in call.args[1] or n in call.args[2] for n in names)
            assert not set(call.kwargs["conditions"]) & names

    def test_a_read_that_raises_is_blindness_not_a_crash(self, job):
        scheduler, journal, _ = job
        with patch.object(cap, "read_facts", AsyncMock(side_effect=KeyError("x"))):
            result = asyncio.run(scheduler._run_dq_integrity())
        assert cap.UNWATCHED in {i["check_name"] for i in journal.issues(result["run_id"])}
