"""The Postgres twins of the Silver checks, judged without a database (step 8a).

The facts are read by `tests/integration/test_pg_warehouse_dq.py` against a
real Postgres. Here: the verdicts, the blindness rules, and what a blind run
holds rather than resolves.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import date, datetime, timedelta, timezone

import pytest

from core import pg_warehouse_dq as twins
from core.data_quality import IntegrityIssue, Severity
from core.pg_warehouse_dq import (
    Attribution, DerivationJournal, Facts, JournalRun, LineItems, RowValues, SilverArc,
    Unwatched, Watermark,
)

WM = Watermark(today=date(2026, 9, 17), in_flight=2, backfilled=True,
               landing_age_s=120, landing_failures=0)
ARC_CLEAN = SilverArc(0, 0, 0.0, None, (), 0, 0.0, ())
ATT_OK = Attribution(orders=100, tagged=30, base_orders=400, base_tagged=120)
LI_NONE = LineItems(0, 0.0, (), 0, 0.0, ())
REBUILT = datetime(2026, 9, 17, 6, 50, tzinfo=timezone.utc)


def _rv(**kw):
    base = dict(reported=0, covered=0, abandoned=0, in_flight=0, columns=(), sample=(),
                oldest_age_s=None, rebuilt_at=REBUILT, compared=48000, elapsed_ms=230)
    base.update(kw)
    return RowValues(**base)


RV_CLEAN = _rv()
JOURNAL_QUIET = DerivationJournal(since=REBUILT, runs=())


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
    base = dict(silver_arc=ARC_CLEAN, attribution=ATT_OK, line_items=LI_NONE,
                derivation_signal=JOURNAL_QUIET, silver_row_values=RV_CLEAN)
    base.update(groups)
    return Facts(whole=None, watermark=WM, **base)


class TestTheRowValues:
    """Step 8b's verdict over pre-read counts. What decides covered, abandoned
    and in flight is SQL, and `tests/integration/test_pg_warehouse_dq.py` reads
    it from a real Postgres; here, what each count becomes."""

    def test_nothing_stale_files_nothing(self):
        assert twins._row_values_check(RV_CLEAN) == []

    def test_rows_only_in_flight_file_nothing(self):
        """Inside the settle grace a change has not had its rebuild yet, and
        filing it would put a WARN in most daytime runs' digest."""
        assert twins._row_values_check(_rv(in_flight=3)) == []

    def test_rows_inside_the_repair_window_warn(self):
        (issue,) = twins._row_values_check(_rv(
            reported=2, columns=(("grand_total", 2),), sample=(7, 5), oldest_age_s=1800))
        assert (issue.check_name, issue.severity, issue.count, issue.sample_ids) == (
            "pg_silver_row_values", Severity.WARN, 2, (7, 5))
        assert "repair window" in issue.description and "grand_total (2)" in issue.description

    def test_a_covered_row_is_critical_and_names_the_rebuild(self):
        (issue,) = twins._row_values_check(_rv(
            reported=1, covered=1, columns=(("is_new_customer", 1),), sample=(42,)))
        assert issue.severity == Severity.CRITICAL and issue.sample_ids == (42,)
        assert REBUILT.isoformat(timespec="seconds") in issue.description
        assert "another would repeat it" in issue.description

    def test_an_abandoned_row_is_critical_and_says_nothing_is_coming(self):
        (issue,) = twins._row_values_check(_rv(reported=1, abandoned=1, rebuilt_at=None))
        assert issue.severity == Severity.CRITICAL
        assert "no rebuild is coming" in issue.description

    def test_the_count_is_every_reported_row_not_the_sample(self):
        """DuckDB's own check counts only the rows it samples; this one counts
        them all, so a count above ten is a count, not a cap."""
        (issue,) = twins._row_values_check(_rv(
            reported=37, covered=30, sample=tuple(range(10))))
        assert issue.count == 37 and len(issue.sample_ids) == 10

    def test_in_flight_rides_along_uncounted(self):
        (issue,) = twins._row_values_check(_rv(reported=1, in_flight=4))
        assert issue.count == 1 and "4 more" in issue.description

    LOADED = datetime(2026, 9, 17, 7, 20, tzinfo=timezone.utc)

    def test_rows_only_a_previous_processs_rebuild_had_are_held_as_info(self):
        """After a restart, until this process has rebuilt, a row the last
        rebuild's snapshot held may be that rebuild's fault or a rule the deploy
        changed. The finding says so and pages nobody; the condition is held."""
        loaded = self.LOADED.isoformat(timespec="seconds")
        (issue,) = twins._row_values_check(_rv(
            held=3, rule_loaded_at=self.LOADED, columns=(("is_active_source", 3),),
            sample=(9, 8, 7)))
        assert (issue.severity, issue.count, issue.sample_ids) == (Severity.INFO, 3, (9, 8, 7))
        assert f"loaded the Silver rule ({loaded})" in issue.description
        assert "Held" in issue.description
        assert "another would repeat it" not in issue.description
        (plain,) = twins._row_values_check(_rv(reported=3, columns=(("grand_total", 3),)))
        assert "Silver rule" not in plain.description

    def test_held_rows_hold_the_condition_and_nothing_else_does(self):
        """The group looked, so it is not blind — but a page standing for rows
        it could not judge must not be announced resolved."""
        held = []
        (issue,) = twins.check_pg_warehouse(_facts(silver_row_values=_rv(held=2)), held_out=held)
        assert issue.check_name == "pg_silver_row_values" and held == ["pg_silver_row_values"]
        for rv in (RV_CLEAN, _rv(reported=1, covered=1), _rv(reported=1), _rv(in_flight=4)):
            held = []
            twins.check_pg_warehouse(_facts(silver_row_values=rv), held_out=held)
            assert held == [], rv

    def test_held_rows_beside_waiting_ones_ride_a_warning(self):
        (issue,) = twins._row_values_check(_rv(reported=1, held=2, rule_loaded_at=self.LOADED))
        assert (issue.severity, issue.count) == (Severity.WARN, 3)
        assert "repair window" in issue.description and "Held" in issue.description

    def test_overdue_rows_are_critical_and_say_no_rebuild_ran_under_the_rule(self):
        """Held past the repair window after the rule loaded, with still no
        rebuild of this process's own: a derivation that never ran again.
        Neither a rebuild at fault nor 'no rebuild since' — one did run, under
        the code before."""
        (issue,) = twins._row_values_check(_rv(
            reported=2, overdue=2, rule_loaded_at=self.LOADED))
        assert issue.severity == Severity.CRITICAL
        assert "no rebuild has run under that rule since" in issue.description
        assert "another would repeat it" not in issue.description
        assert "no Silver rebuild begun since" not in issue.description

    def test_a_budget_spent_on_the_recompute_blinds_it_alone(self):
        held = []
        issues = twins.check_pg_warehouse(_facts(silver_row_values=Unwatched(
            f"the {twins.HOLD_BUDGET_S} s hold budget was spent before this group was read")),
            held_out=held)
        assert _names(issues) == ["pg_silver_row_values_unwatched"]
        assert held == ["pg_silver_row_values"]


class TestTheRowValuesSql:
    """Structure, parsed or rendered — never grepped out of prose."""

    def test_the_recompute_is_the_shared_text_rendered_for_postgres(self):
        """Rule 1: a Postgres copy of pass 2 would be a third statement of it."""
        from core.duckdb_store import silver_recompute_ctes
        from core.sql_dialect import POSTGRES

        sql = twins._row_values_sql()
        assert silver_recompute_ctes(POSTGRES) in sql
        assert "FROM bronze.orders o" in silver_recompute_ctes(POSTGRES)

    def test_the_duckdb_check_reads_the_same_function(self):
        from core import data_quality

        tree = ast.parse(textwrap.dedent(inspect.getsource(data_quality._silver_arc_check)))
        called = {n.func.id for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert {"silver_recompute_ctes", "_silver_row_differs"} <= called

    def test_every_silver_column_is_compared_and_counted(self):
        """A column Silver gains and the twin does not compare is a column that
        drifts in silence."""
        from core.data_quality import _SILVER_ROW_COLUMNS
        from core.pg_silver import SILVER_COLUMNS

        compared = [c for c, _ in _SILVER_ROW_COLUMNS]
        assert set(compared) | {"id"} == set(SILVER_COLUMNS)
        sql = twins._row_values_sql()
        for column in compared:
            assert f"AS d_{column}" in sql and f"AS n_{column}" in sql

    def test_the_rebuild_is_dated_by_the_journal_and_silvers_own_watermark(self):
        """The layer and the watermark's row are bind parameters, and both are
        the rebuild's own: the journal for error-free runs of the warehouse
        layer, and the row `rebuild_silver` stamps — the only record a
        piggyback rebuild leaves."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock, patch

        from core.data_quality import _SILVER_ROW_COLUMNS
        from core.pg_derivation import DROPPED_MARK_MARGIN, LAYER
        from core import pg_silver
        from core.pg_silver import SILVER_TABLE

        row = {"reported": 0, "covered": 0, "abandoned": 0, "in_flight": 0, "sample": None,
               "oldest_age_s": None, "rebuilt_at": None, "compared": 0, "held": 0,
               "overdue": 0, **{f"n_{c}": 0 for c, _ in _SILVER_ROW_COLUMNS}}
        first = datetime(2026, 9, 17, 7, 21, tzinfo=timezone.utc)
        for rebuilt in (None, first):
            conn = MagicMock(fetchrow=AsyncMock(return_value=row))
            with patch.object(pg_silver, "FIRST_REBUILT_AT", rebuilt):
                facts = asyncio.run(twins._read_row_values(conn, 20, 80))
            sql, *args = conn.fetchrow.await_args.args
            assert "error IS NULL" in sql and "layer = $4" in sql
            assert "FROM meta.mirror_state" in sql and "table_name = $5" in sql
            assert args == [20, 80, DROPPED_MARK_MARGIN, LAYER, SILVER_TABLE,
                            rebuilt, pg_silver.RULE_LOADED_AT]
            assert (facts.first_rebuilt_at, facts.rule_loaded_at) == (
                rebuilt, pg_silver.RULE_LOADED_AT)

    def test_the_snapshot_runs_without_jit(self):
        """JIT compilation never paid for itself on the recompute, and past
        jit_optimize_above_cost it was ~0.9 s of a ~1.3 s read, all of it under
        PG_LAYER_LOCK. Parsed out of `read_facts`: the SET must be a statement
        the snapshot executes."""
        tree = ast.parse(textwrap.dedent(inspect.getsource(twins.read_facts)))
        executed = [ast.literal_eval(n.args[0]) for n in ast.walk(tree)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "execute" and n.args
                    and isinstance(n.args[0], ast.Constant)]
        assert "SET LOCAL jit = off" in executed

    def test_the_recompute_is_read_last(self):
        """A hold budget spent on the heavy group must not blind the cheap ones
        queued behind it."""
        assert twins.GROUPS[-1] == "silver_row_values"
        tree = ast.parse(textwrap.dedent(inspect.getsource(twins.read_facts)))
        (readers,) = [n.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "readers" for t in n.targets)]
        assert [ast.literal_eval(e.elts[0]) for e in readers.elts] == list(twins.GROUPS)


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
                     twins._ORPHAN_SQL, twins._LINE_ITEMS_SQL, twins._row_values_sql()):
            assert "CURRENT_DATE" not in text.upper()


class TestTheLevers:
    def test_the_row_values_twin_has_its_own_lever(self):
        """Longest prefix: without its own entry `pg_silver_` would answer
        "rebuild", which is the wrong advice for a row a rebuild already covered."""
        from core.data_quality import remediation_for

        (row_values,) = remediation_for(["pg_silver_row_values"])
        (unwatched,) = remediation_for(["pg_silver_row_values_unwatched"])
        (generic,) = remediation_for(["pg_silver_missing_rows"])
        assert len({row_values, unwatched, generic}) == 3
        assert "compare silver.orders with bronze.orders" in row_values
        assert "HOLD_BUDGET_S" in unwatched


# ─── DN-14 ────────────────────────────────────────────────────────────────────

T0 = datetime(2026, 9, 17, 0, 0, tzinfo=timezone.utc)


def _at(minutes):
    return T0 + timedelta(minutes=minutes)


def _run(id, *, minutes, seen, water, trigger="heartbeat"):
    """A journal row: started `minutes` after T0, having seen `seen` marks and
    bronze mirrored up to `water` minutes after T0 (None: no mark recorded)."""
    return JournalRun(id=id, trigger=trigger, started_at=_at(minutes), requested_seen=seen,
                      high_water=None if water is None else _at(water))


def _journal(*runs, since_minutes=0):
    return DerivationJournal(since=_at(since_minutes), runs=tuple(runs))


class TestTheSignalMissed:
    """`pg_signal_missed` over pre-read journal rows. The rows are read by
    `tests/integration/test_pg_warehouse_dq.py` from a real journal the real
    derivation wrote."""

    def test_bronze_moved_on_and_the_mark_did_not_is_a_warning(self):
        journal = _journal(_run(1, minutes=0, seen=7, water=-5),
                           _run(2, minutes=60, seen=7, water=30))
        (issue,) = twins._signal_check(journal)
        assert (issue.check_name, issue.severity, issue.count, issue.sample_ids) == (
            "pg_signal_missed", Severity.WARN, 1, (2,))
        assert "requested still at 7" in issue.description
        assert _at(30).isoformat(timespec="seconds") in issue.description

    def test_an_equal_high_water_mark_is_nothing(self):
        """A quiet hour: nothing written, nothing marked."""
        assert twins._signal_check(_journal(_run(1, minutes=0, seen=7, water=-5),
                                            _run(2, minutes=60, seen=7, water=-5))) == []

    def test_a_first_tick_run_is_nothing(self):
        """A boot sync configured before DN-05b wrote with no mark by design,
        and the process's first run is what rebuilt it."""
        journal = _journal(_run(1, minutes=0, seen=7, water=-5),
                           _run(2, minutes=60, seen=7, water=30, trigger="first_tick"))
        assert twins._signal_check(journal) == []

    def test_the_run_after_a_first_tick_is_judged_against_it(self):
        """The exclusion is the first run's pair only: what the boot sync wrote
        is inside its high-water mark, so the next pair starts from there."""
        journal = _journal(_run(1, minutes=0, seen=7, water=-5),
                           _run(2, minutes=60, seen=7, water=30, trigger="first_tick"),
                           _run(3, minutes=120, seen=7, water=90))
        (issue,) = twins._signal_check(journal)
        assert issue.sample_ids == (3,)

    def test_a_mark_that_moved_is_nothing(self):
        assert twins._signal_check(_journal(_run(1, minutes=0, seen=7, water=-5),
                                            _run(2, minutes=12, seen=8, water=10,
                                                 trigger="signal"))) == []

    def test_a_run_before_dn14_says_nothing_either_way(self):
        assert twins._signal_check(_journal(_run(1, minutes=0, seen=7, water=None),
                                            _run(2, minutes=60, seen=7, water=30))) == []

    def test_only_runs_inside_the_lookback_are_judged(self):
        """The run before the lookback is the first judged run's predecessor,
        and is not judged itself."""
        journal = _journal(_run(1, minutes=0, seen=7, water=-10),
                           _run(2, minutes=60, seen=7, water=30),
                           _run(3, minutes=120, seen=7, water=90), since_minutes=90)
        (issue,) = twins._signal_check(journal)
        assert (issue.count, issue.sample_ids) == (1, (3,))
        journal = _journal(_run(1, minutes=0, seen=7, water=-10),
                           _run(2, minutes=60, seen=7, water=30), since_minutes=50)
        (issue,) = twins._signal_check(journal)
        assert issue.sample_ids == (2,)

    def test_every_pair_counts_and_the_sample_leads_with_the_latest(self):
        journal = _journal(*(_run(i, minutes=60 * i, seen=7, water=60 * i - 5)
                             for i in range(1, 14)))
        (issue,) = twins._signal_check(journal)
        assert issue.count == 12
        assert issue.sample_ids == tuple(range(13, 3, -1))

    def test_an_unreadable_journal_is_named_and_held(self):
        held = []
        issues = twins.check_pg_warehouse(_facts(derivation_signal=Unwatched("timeout")),
                                          held_out=held)
        assert _names(issues) == ["pg_derivation_signal_unwatched"]
        assert held == ["pg_signal_missed"]

    def test_it_reaches_the_run_through_the_twins(self):
        journal = _journal(_run(1, minutes=0, seen=7, water=-5),
                           _run(2, minutes=60, seen=7, water=30))
        assert _names(twins.check_pg_warehouse(_facts(derivation_signal=journal))) == [
            "pg_signal_missed"]

    def test_it_has_a_lever_of_its_own(self):
        from core.data_quality import DEFAULT_REMEDIATION, remediation_for

        for name in ("pg_signal_missed", "pg_derivation_signal_unwatched"):
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name
        assert "dropped marks" in remediation_for(["pg_signal_missed"])[0]


def _pairing(facts, **kw):
    import json

    issue = twins.pairing_record(facts, **kw)
    assert (issue.check_name, issue.severity, issue.count) == (
        "pg_twin_pairing", Severity.INFO, 1)
    return json.loads(issue.description)


DUCK_LOOKED = frozenset({"silver_arc", "attribution_coverage", "headline_vs_line_items",
                         "goods_shipped_without_sale"})


class TestThePairingRecord:
    """Parsed, never grepped: the soak reads this JSON, so the JSON is what
    is asserted on."""

    LI = LineItems(5, 500.0, (1,), 7, 700.0, (2,))

    def test_with_duckdb_findings_it_carries_both_sides_and_the_standalone_verdict(self):
        """DuckDB's headline and goods findings are present, so the real run
        compares and files neither twin (7 against 6 is inside the 2 orders in
        flight). The record still says what each engine counted, and what the
        twins would have filed on their own."""
        duck = [_duck("headline_vs_line_items", 5),
                _duck("goods_shipped_without_sale", 6, Severity.INFO),
                _duck("silver_missing_rows", 3, Severity.CRITICAL)]
        facts = _facts(line_items=self.LI,
                       silver_arc=SilverArc(3, 3, 300.0, 9000, (4, 5, 6), 0, 0.0, ()))
        real = twins.check_pg_warehouse(facts, duckdb_issues=duck, duckdb_looked=DUCK_LOOKED)
        assert not {"pg_headline_vs_line_items", "pg_goods_shipped_without_sale"} & set(
            _names(real))

        record = _pairing(facts, duckdb_issues=duck, duckdb_looked=DUCK_LOOKED,
                          duckdb_attribution={"orders": 90, "tagged": 27,
                                              "base_orders": 380, "base_tagged": 114})

        assert record["version"] == twins.PAIRING_VERSION and record["in_flight"] == 2
        d, p = record["duckdb"], record["postgres"]
        assert (d["headline_vs_line_items"], p["headline_vs_line_items"]) == (5, 5)
        assert (d["goods_shipped_without_sale"], p["goods_shipped_without_sale"]) == (6, 7)
        assert (d["silver_missing_rows"], p["silver_missing_rows"]) == (3, 3)
        assert (d["silver_orphan_rows"], p["silver_orphan_rows"]) == (0, 0)
        assert (d["attribution"]["pct"], p["attribution"]["pct"]) == (30.0, 30.0)
        assert (d["attribution"]["base_pct"], p["attribution"]["base_pct"]) == (30.0, 30.0)
        assert d["looked"] == sorted(DUCK_LOOKED) and p["unwatched"] == {}
        assert record["standalone"]["pg_headline_vs_line_items"] == 5
        assert record["standalone"]["pg_goods_shipped_without_sale"] == 7
        assert record["standalone"]["pg_silver_missing_rows"] == 3
        assert "pg_line_items_disagree" not in record["standalone"]

    def test_a_duckdb_guard_that_did_not_look_is_none_not_zero(self):
        record = _pairing(_facts(line_items=self.LI), duckdb_issues=[],
                          duckdb_looked=frozenset({"silver_arc"}))
        d = record["duckdb"]
        assert d["silver_missing_rows"] == 0
        assert d["headline_vs_line_items"] is None and d["attribution"] is None
        assert record["postgres"]["headline_vs_line_items"] == 5

    def test_the_duckdb_half_failing_leaves_one_side(self):
        record = _pairing(_facts(line_items=self.LI), duckdb_issues=[],
                          duckdb_looked=frozenset())
        assert record["duckdb"]["looked"] == []
        assert set(record["standalone"]) == {"pg_headline_vs_line_items",
                                             "pg_goods_shipped_without_sale"}

    def test_a_blind_group_is_none_with_its_reason(self):
        record = _pairing(_facts(attribution=Unwatched("timeout")),
                          duckdb_looked=DUCK_LOOKED,
                          duckdb_attribution={"orders": 90, "tagged": 27})
        p = record["postgres"]
        assert p["attribution"] is None and p["unwatched"] == {"attribution": "timeout"}
        assert record["duckdb"]["attribution"]["pct"] == 30.0
        assert record["duckdb"]["attribution"]["base_pct"] is None
        assert record["standalone"] == {"pg_attribution_coverage_unwatched": 1}

    def test_a_blind_snapshot_is_recorded_not_skipped(self):
        record = _pairing(Facts.blind("KS_PG_DSN is not set"), duckdb_looked=DUCK_LOOKED)
        assert record["postgres"]["unwatched"] == {"*": "KS_PG_DSN is not set"}
        assert record["in_flight"] is None
        assert record["standalone"] == {"pg_warehouse_unwatched": len(twins.GROUPS)}

    def test_a_signal_miss_rides_the_standalone_as_the_real_run_files_it(self):
        journal = _journal(_run(1, minutes=0, seen=7, water=-5),
                           _run(2, minutes=60, seen=7, water=30))
        record = _pairing(_facts(derivation_signal=journal), duckdb_looked=DUCK_LOOKED)
        assert record["standalone"] == {"pg_signal_missed": 1}

    def test_it_never_raises(self, monkeypatch):
        def boom(*_a, **_k):
            raise ValueError("bad")
        monkeypatch.setattr(twins, "_pairing", boom)
        record = _pairing(_facts())
        assert record == {"version": twins.PAIRING_VERSION, "error": "ValueError: bad"}

    def test_the_standalone_evaluation_reads_nothing(self):
        """Pure over what was read: no await anywhere in the record's path."""
        for fn in (twins.pairing_record, twins._pairing, twins._coverage):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            assert not [n for n in ast.walk(tree) if isinstance(n, (ast.Await, ast.AsyncFunctionDef))]

    def test_it_is_an_event_with_a_human_name(self):
        from core.alerting import REGISTRY, Kind
        from core.data_quality import HUMAN_CHECK_NAMES

        assert REGISTRY["pg_twin_pairing"].kind is Kind.EVENT
        assert REGISTRY["pg_signal_missed"].kind is Kind.CONDITION
        for name in ("pg_twin_pairing", "pg_signal_missed", "pg_derivation_signal_unwatched"):
            assert name in HUMAN_CHECK_NAMES


class TestThePairingIsWired:
    def _tree(self):
        from core.scheduler import BackgroundScheduler

        return ast.parse(textwrap.dedent(inspect.getsource(BackgroundScheduler._run_dq_integrity)))

    def test_it_is_filed_with_the_twins_findings_only_when_they_ran(self):
        """Inside the `elif pg_on` branch, appended to `pg_issues`, fed the
        DuckDB half's own findings, what it looked at and what it measured."""
        (branch,) = [n for n in ast.walk(self._tree()) if isinstance(n, ast.If)
                     and isinstance(n.test, ast.Name) and n.test.id == "pg_on"]
        calls = [n for n in ast.walk(branch) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute) and n.func.attr == "pairing_record"]
        (call,) = calls
        kw = {k.arg: ast.unparse(k.value) for k in call.keywords}
        assert kw["duckdb_issues"] == "issues" and kw["duckdb_looked"] == "duckdb_looked"
        assert "attribution_coverage" in kw["duckdb_attribution"]
        appends = [n for n in ast.walk(branch) if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Attribute) and n.func.attr == "append"
                   and ast.unparse(n.func.value) == "pg_issues"]
        assert appends

    def test_the_duckdb_scan_hands_over_what_it_measured(self):
        scans = [n for n in ast.walk(self._tree()) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == "check_internal_integrity"]
        (scan,) = scans
        assert "pairing_out" in {k.arg for k in scan.keywords}
