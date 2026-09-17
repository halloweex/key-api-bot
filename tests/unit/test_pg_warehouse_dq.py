"""The Postgres twins of the Silver checks, judged without a database (step 8a).

The facts are read by `tests/integration/test_pg_warehouse_dq.py` against a
real Postgres. Here: the verdicts, the blindness rules, and what a blind run
holds rather than resolves.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import date

import pytest

from core import pg_warehouse_dq as twins
from core.data_quality import IntegrityIssue, Severity
from core.pg_warehouse_dq import Attribution, Facts, LineItems, SilverArc, Unwatched, Watermark

WM = Watermark(today=date(2026, 9, 17), in_flight=2, backfilled=True,
               landing_age_s=120, landing_failures=0)
ARC_CLEAN = SilverArc(0, 0, 0.0, None, (), 0, 0.0, ())
ATT_OK = Attribution(orders=100, tagged=30, base_orders=400, base_tagged=120)
LI_NONE = LineItems(0, 0.0, (), 0, 0.0, ())


def _names(issues):
    return sorted(i.check_name for i in issues)


def _duck(name, count, severity=Severity.WARN):
    return IntegrityIssue(check_name=name, table_name="silver_orders", severity=severity, count=count)


class TestTheFlag:
    def test_default_off(self, monkeypatch):
        monkeypatch.delenv(twins.ENV, raising=False)
        assert twins.flag() == (False, None)

    def test_on(self, monkeypatch):
        monkeypatch.setenv(twins.ENV, "on")
        assert twins.flag() == (True, None)

    def test_a_typo_is_off_with_an_error(self, monkeypatch):
        monkeypatch.setenv(twins.ENV, "yes")
        enabled, error = twins.flag()
        assert enabled is False and "yes" in error


class TestTheWindows:
    def test_default_floor(self, monkeypatch):
        monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)
        assert (twins.silver_grace_minutes(), twins.page_after_minutes()) == (20, 80)

    def test_they_follow_the_floor(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "1200")
        assert (twins.silver_grace_minutes(), twins.page_after_minutes()) == (30, 90)


class TestTheSilverArc:
    def test_missing_inside_the_repair_window_is_warn(self):
        arc = SilverArc(3, 0, 900.0, 1800, (1, 2, 3), 0, 0.0, ())
        (issue,) = twins._silver_arc_check(arc)
        assert (issue.check_name, issue.severity, issue.count) == ("pg_silver_missing_rows", Severity.WARN, 3)
        assert issue.sample_ids == (1, 2, 3)

    def test_missing_past_the_repair_window_is_critical(self):
        arc = SilverArc(3, 1, 900.0, 6000, (1, 2, 3), 0, 0.0, ())
        (issue,) = twins._silver_arc_check(arc)
        assert issue.severity == Severity.CRITICAL and "1 of them" in issue.description

    def test_orphans(self):
        (issue,) = twins._silver_arc_check(SilverArc(0, 0, 0.0, None, (), 2, 50.0, (9, 8)))
        assert (issue.check_name, issue.severity) == ("pg_silver_orphan_rows", Severity.WARN)

    def test_clean(self):
        assert twins._silver_arc_check(ARC_CLEAN) == []


class TestAttribution:
    def test_below_the_floor(self):
        (issue,) = twins._attribution_check(Attribution(100, 10, 400, 40), WM)
        assert issue.check_name == "pg_attribution_coverage_website" and issue.count == 90

    def test_halved_against_the_baseline(self):
        (issue,) = twins._attribution_check(Attribution(100, 20, 400, 200), WM)
        assert "50%" in issue.description

    def test_healthy(self):
        assert twins._attribution_check(ATT_OK, WM) == []

    def test_a_quiet_week_with_a_healthy_mirror_is_quiet(self):
        assert twins._attribution_check(Attribution(12, 0, 60, 20), WM) == []

    @pytest.mark.parametrize("landing", [
        dict(landing_failures=3), dict(landing_age_s=None), dict(landing_age_s=9 * 3600)])
    def test_a_quiet_week_with_a_silent_mirror_is_not_watched(self, landing):
        wm = Watermark(**{**WM.__dict__, **landing})
        (issue,) = twins._attribution_check(Attribution(12, 0, 60, 20), wm)
        assert issue.check_name == "pg_attribution_coverage_unwatched"

    def test_the_thresholds_are_the_duckdb_checks_own(self):
        """Read from its signature, so changing them there changes them here."""
        from core.data_quality import _attribution_coverage_check

        src = inspect.getsource(twins._attribution_check)
        assert "inspect.signature(_attribution_coverage_check)" in src
        d = inspect.signature(_attribution_coverage_check).parameters
        assert d["min_orders"].default == 40 and d["floor_pct"].default == 15.0


class TestLineItems:
    LI = LineItems(5, 500.0, (1,), 7, 700.0, (2,))

    def test_agreeing_with_duckdb_files_nothing(self):
        issues = twins._line_items_check(
            self.LI, WM, duckdb_issues=[_duck("headline_vs_line_items", 5),
                                        _duck("goods_shipped_without_sale", 6, Severity.INFO)],
            duckdb_looked=frozenset({"headline_vs_line_items", "goods_shipped_without_sale"}))
        assert issues == []   # 7 against 6 is inside the 2 orders in flight

    def test_a_difference_beyond_in_flight_is_named(self):
        issues = twins._line_items_check(
            self.LI, WM, duckdb_issues=[_duck("headline_vs_line_items", 1)],
            duckdb_looked=frozenset({"headline_vs_line_items", "goods_shipped_without_sale"}))
        (issue,) = issues
        assert issue.check_name == "pg_line_items_disagree" and issue.count == 2

    def test_when_duckdb_did_not_look_the_twins_stand_in(self):
        issues = twins._line_items_check(self.LI, WM, duckdb_issues=[], duckdb_looked=frozenset())
        by = {i.check_name: i for i in issues}
        assert set(by) == {"pg_headline_vs_line_items", "pg_goods_shipped_without_sale"}
        assert by["pg_headline_vs_line_items"].severity == Severity.WARN
        assert by["pg_goods_shipped_without_sale"].severity == Severity.INFO


def _facts(**groups):
    base = dict(silver_arc=ARC_CLEAN, attribution=ATT_OK, line_items=LI_NONE)
    base.update(groups)
    return Facts(whole=None, watermark=WM, **base)


class TestBlindness:
    def test_a_blind_snapshot_files_one_finding(self):
        issues = twins.check_pg_warehouse(Facts.blind("KS_PG_DSN is not set"))
        assert _names(issues) == ["pg_warehouse_unwatched"]

    def test_no_facts_at_all(self):
        assert _names(twins.check_pg_warehouse(None)) == ["pg_warehouse_unwatched"]

    def test_one_blind_group_names_itself_and_the_rest_are_judged(self):
        issues = twins.check_pg_warehouse(_facts(
            attribution=Unwatched("timeout"),
            silver_arc=SilverArc(1, 1, 10.0, 9000, (5,), 0, 0.0, ())))
        assert _names(issues) == ["pg_attribution_coverage_unwatched", "pg_silver_missing_rows"]

    def test_two_blind_groups_collapse(self):
        issues = twins.check_pg_warehouse(_facts(
            attribution=Unwatched("a"), line_items=Unwatched("b")))
        assert _names(issues) == ["pg_warehouse_unwatched"]

    def test_a_judge_that_raises_is_named_and_held(self, monkeypatch):
        def boom(*_a, **_k):
            raise ValueError("bad")
        monkeypatch.setattr(twins, "_silver_arc_check", boom)
        raised = []
        issues = twins.check_pg_warehouse(_facts(), raised_out=raised)
        assert _names(issues) == ["pg_silver_arc_unwatched"] and raised == ["silver_arc"]


class TestWhatABlindRunHolds:
    def test_switched_off_holds_nothing(self):
        """Stood down, not blind: a page from while they were on resolves once
        rather than standing for as long as the flag stays off."""
        assert twins.unverified_pg_conditions(ran=False, flag_invalid=False) == []

    def test_a_flag_not_understood_holds_everything(self):
        assert set(twins.unverified_pg_conditions(ran=False, flag_invalid=True)) == twins.all_conditions()

    def test_a_run_holds_exactly_what_could_not_look(self):
        assert twins.unverified_pg_conditions(
            ran=True, flag_invalid=False, held=["pg_line_items_disagree"]) == ["pg_line_items_disagree"]

    def test_whole_blindness_holds_every_group(self):
        held = []
        twins.check_pg_warehouse(Facts.blind("x"), held_out=held)
        assert set(held) == twins.all_conditions()

    def test_a_collapsed_finding_holds_only_the_blind_groups(self):
        """One line for two blind groups — but silver_arc, read clean in the same
        run, still announces its recovery. Found reviewing #215."""
        held = []
        issues = twins.check_pg_warehouse(
            _facts(attribution=Unwatched("a"), line_items=Unwatched("b")), held_out=held)
        assert _names(issues) == ["pg_warehouse_unwatched"]
        assert set(held) == set(twins.GUARD_CONDITIONS["attribution"]) | set(twins.GUARD_CONDITIONS["line_items"])
        assert not set(held) & set(twins.GUARD_CONDITIONS["silver_arc"])

    def test_a_judge_that_raises_holds_its_group(self, monkeypatch):
        def boom(*_a, **_k):
            raise ValueError("bad")
        monkeypatch.setattr(twins, "_line_items_check", boom)
        held = []
        twins.check_pg_warehouse(_facts(), held_out=held)
        assert sorted(held) == sorted(twins.GUARD_CONDITIONS["line_items"])

    def test_every_guard_has_an_unwatched_name_and_every_condition_is_registered(self):
        from core.alerting import REGISTRY

        assert set(twins.GUARD_CONDITIONS) == set(twins.UNWATCHED_NAMES) == set(twins.GROUPS)
        names = (twins.all_conditions() | set(twins.UNWATCHED_NAMES.values())
                 | {twins.WHOLE_UNWATCHED, twins.FLAG_INVALID})
        assert names <= set(REGISTRY)


class TestReadFactsNeverRaises:
    """It promises not to, and the job relies on it for the DuckDB half's sake.
    Found reviewing #215: a floor that is not a number raised past it."""

    def test_a_floor_that_is_not_a_number_is_blindness(self, monkeypatch):
        import asyncio
        from unittest.mock import AsyncMock, patch

        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "10m")
        with patch("core.pg.get_pool", AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", AsyncMock()):
            facts = asyncio.run(twins.read_facts())
        assert facts.whole is not None and "ValueError" in facts.whole.reason


class TestResolveOnlyAPrefix:
    def test_only_prefix_leaves_every_other_key_delivered(self, tmp_path):
        from core.alerting import AlertGate

        gate = AlertGate(state_path=tmp_path / "gate.json")
        gate._delivered.update({
            "pg_silver_missing_rows": {"group": "dq:integrity", "first_delivered": 1.0},
            "silver_missing_rows": {"group": "dq:integrity", "first_delivered": 1.0},
        })
        taken = gate.take_resolved("dq:integrity", [], now=2.0, only_prefix="pg_")
        assert set(taken) == {"pg_silver_missing_rows"}
        assert set(gate._delivered) == {"silver_missing_rows"}


class TestTheJobWiring:
    def _tree(self):
        from core.scheduler import BackgroundScheduler

        return ast.parse(textwrap.dedent(inspect.getsource(BackgroundScheduler._run_dq_integrity)))

    def test_facts_are_read_only_when_the_flag_is_on(self):
        tree = self._tree()
        reads = [n for n in ast.walk(tree) if isinstance(n, ast.If)
                 and any(isinstance(c, ast.Attribute) and c.attr == "read_facts"
                         for c in ast.walk(n))]
        assert reads, "read_facts is not called under a condition"
        assert any(isinstance(n.test, ast.Name) and n.test.id == "pg_on"
                   or (isinstance(n.test, ast.Name) and n.test.id == "pg_flag_error")
                   for n in reads)

    def test_the_twins_conditions_reach_the_resolution(self):
        calls = [n for n in ast.walk(self._tree()) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute) and n.func.attr == "_resolve_dq_layer"]
        assert calls
        assert "pg_unverified" in ast.unparse(calls[0])

    def test_a_postgres_critical_pages_when_the_duckdb_half_failed(self):
        ifs = [n for n in ast.walk(self._tree()) if isinstance(n, ast.If)
               and "Severity.CRITICAL" in ast.unparse(n.test)]
        assert any("pg_critical" in ast.unparse(n.test) for n in ifs)


class TestKyivsToday:
    def test_today_is_computed_in_kyiv_never_current_date(self):
        """Structural on purpose: a two-engine test of the date passes whenever
        the gate runs outside 21:00–24:00 UTC, the only hours Kyiv's date and
        Postgres' CURRENT_DATE disagree — and the 01:00 Kyiv run is always in
        them."""
        sql = twins._WATERMARK_SQL
        assert "AT TIME ZONE 'Europe/Kyiv'" in sql
        for text in (twins._WATERMARK_SQL, twins._ATTRIBUTION_SQL, twins._MISSING_SQL,
                     twins._ORPHAN_SQL, twins._LINE_ITEMS_SQL):
            assert "CURRENT_DATE" not in text.upper()
