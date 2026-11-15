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
    Attribution, DerivationJournal, Facts, JournalRun, LineItems, OrderLanding, RowValues,
    SilverArc, Unwatched, Watermark,
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


class TestWhatMovedFromTheScan:
    """What the twins read from the DuckDB checks until step 14's S14-14
    deleted them, moved here byte for byte. Each literal is what the code
    rendered before the move, on the base commit (rule 12): a difference is a
    change to a check, not a refactor."""

    def test_the_known_ids(self):
        assert sorted(twins.KNOWN_STATUS_IDS) == [1, 2, 3, 4, 8, 9, 10, 11, 12, 15, 18, 19, 20, 21, 22, 23, 24]
        assert sorted(twins.KNOWN_SOURCE_IDS) == [1, 2, 3, 4, 5]

    def test_the_silver_row_comparison(self):
        assert [list(c) for c in twins._SILVER_ROW_COLUMNS] == [['source_id', 0], ['status_id', 0], ['grand_total', 0.01], ['ordered_at', 0], ['buyer_id', 0], ['manager_id', 0], ['order_date', 0], ['is_return', 0], ['sales_type', 0], ['is_active_source', 0], ['source_name', 0], ['is_new_customer', 0], ['buyer_first_order_date', 0], ['promocode', 0]]
        assert {c: twins._silver_row_differs(c, t)
                for c, t in twins._SILVER_ROW_COLUMNS} == {'source_id': 'r.source_id IS DISTINCT FROM s.source_id', 'status_id': 'r.status_id IS DISTINCT FROM s.status_id', 'grand_total': 'ABS(COALESCE(r.grand_total, 0) - COALESCE(s.grand_total, 0)) > 0.01', 'ordered_at': 'r.ordered_at IS DISTINCT FROM s.ordered_at', 'buyer_id': 'r.buyer_id IS DISTINCT FROM s.buyer_id', 'manager_id': 'r.manager_id IS DISTINCT FROM s.manager_id', 'order_date': 'r.order_date IS DISTINCT FROM s.order_date', 'is_return': 'r.is_return IS DISTINCT FROM s.is_return', 'sales_type': 'r.sales_type IS DISTINCT FROM s.sales_type', 'is_active_source': 'r.is_active_source IS DISTINCT FROM s.is_active_source', 'source_name': 'r.source_name IS DISTINCT FROM s.source_name', 'is_new_customer': 'r.is_new_customer IS DISTINCT FROM s.is_new_customer', 'buyer_first_order_date': 'r.buyer_first_order_date IS DISTINCT FROM s.buyer_first_order_date', 'promocode': 'r.promocode IS DISTINCT FROM s.promocode'}

    def test_the_row_values_statement_renders_as_before(self):
        """Re-recorded under the owner's decision of 2026-11-14 (a sale to
        the company's own legal entity is not revenue): the recompute carries
        Silver's rule, and `is_active_source` gained its buyer half — the 70
        characters of `\\n` + 16 spaces + `AND (o.buyer_id IS NULL OR
        o.buyer_id NOT IN (17358))` and nothing else; it was 8703,
        da06407885f9234570f2654aac326bb1fa558e9e7c0dc509f71cc47300fef2c2."""
        import hashlib

        sql = twins._row_values_sql()
        assert (len(sql), hashlib.sha256(sql.encode()).hexdigest()) == (
            8773, "c5ab88916e93b71c7d20835d7dc72a1031878bfed9d47e39682a5b92bb17bf64")

    def test_the_thresholds(self):
        """The attribution check's defaults and the landing check's
        `min_total`, as the DuckDB checks' signatures gave them."""
        assert twins._ATTRIBUTION_DEFAULTS == {
            "window_days": 7, "baseline_days": 28, "floor_pct": 15.0,
            "halved_against_baseline": 0.5, "min_orders": 40}
        assert twins._WITHOUT_ITEMS_MIN_TOTAL == 0.01

    def test_the_twins_statements_render_as_before(self):
        import hashlib

        assert {name: hashlib.sha256(getattr(twins, name).encode()).hexdigest()
                for name in ['_ATTRIBUTION_SQL', '_HEADERS_SQL', '_JOURNAL_SQL', '_LINE_ITEMS_SQL', '_MISSING_SQL', '_ORPHAN_ITEMS_SQL', '_ORPHAN_SQL', '_STATUS_GROUP_SQL', '_WATERMARK_SQL', '_WITHOUT_ITEMS_SQL']} == {'_WATERMARK_SQL': 'cd8f2d71adf21772800f97b245d093cfdf6d2328f1ead06877cd1c765f7a5127', '_MISSING_SQL': '862849ffecc029a736957b1c2913721254f6204b0b6bb31c3a160b590a5c525b', '_ORPHAN_SQL': '81219f8e958028c273e112c29731a9e484ab71ed43e3a409ffde276ac6fdee1f', '_ATTRIBUTION_SQL': '7deeead8c05b11452b806a1334b23c176649fc894c5a42a419555c97f06d2797', '_LINE_ITEMS_SQL': 'db37046a8bdff79e345f4aef85671007096552d673f4c1a94eb02ddae60c5beb', '_WITHOUT_ITEMS_SQL': '7802387b06c77df69f8c042e9b05d759991c41fcd04fa8783dd5f54e47df3a56', '_ORPHAN_ITEMS_SQL': 'ece2ec904b132fa6f9c2964afa2ea94fc1a316dd9425a03d21c326a77484a26e', '_HEADERS_SQL': '4d305a09201bcfc59d37a7757eeb1d70e96879dda250f352cb8f8924fb24de4c', '_STATUS_GROUP_SQL': 'efb9bb6e39f528b49ca20f4b3f2894c764ef2cd840b5c771cf8061ff92080091', '_JOURNAL_SQL': 'a015f77b56b73bad9248f1a4077b5575f15b8bbc1b22bafc6d2218765b2f182b'}

    def test_nothing_is_left_behind_in_the_scan(self):
        from core import data_quality as dq

        for name in ("KNOWN_STATUS_IDS", "KNOWN_SOURCE_IDS", "_SILVER_ROW_COLUMNS",
                     "_silver_row_differs", "_attribution_coverage_check",
                     "_orders_without_line_items_check"):
            assert not hasattr(dq, name), name


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

    def test_no_baseline_still_honours_the_floor(self):
        """Carried from the DuckDB check's tests (deleted with it in step 14's
        S14-14): too few orders before the window is no trend, not a pass."""
        (issue,) = twins._attribution_check(Attribution(100, 10, 10, 1), WM)
        assert issue.count == 90 and "Over the previous" not in issue.description

    def test_a_slow_slide_within_half_is_not_news(self):
        """30% to 20%: above the floor, and not halved."""
        assert twins._attribution_check(Attribution(100, 20, 400, 120), WM) == []

    @pytest.mark.parametrize("landing", [
        dict(landing_failures=3), dict(landing_age_s=None), dict(landing_age_s=9 * 3600)])
    def test_a_quiet_week_with_a_silent_mirror_is_not_watched(self, landing):
        wm = Watermark(**{**WM.__dict__, **landing})
        (issue,) = twins._attribution_check(Attribution(12, 0, 60, 20), wm)
        assert issue.check_name == "pg_attribution_coverage_unwatched"



class TestLineItems:
    LI = LineItems(5, 500.0, (1,), 7, 700.0, (2,))
    # 2026-11-14 on production, by sales type (the retail and exhibition
    # amounts are this test's: production measured their sum).
    NOV_14 = LineItems(
        461, 1_023_472.67, (9, 8), 0, 0.0, (),
        headline_by_type=(("b2b", 350, 809_553.00), ("exhibition", 5, 12_000.00),
                          ("retail", 106, 201_919.67)))

    def test_the_twins_file_both(self):
        issues = twins._line_items_check(self.LI)
        by = {i.check_name: i for i in issues}
        assert set(by) == {"pg_headline_vs_line_items", "pg_goods_shipped_without_sale"}
        assert by["pg_headline_vs_line_items"].severity == Severity.INFO
        assert by["pg_goods_shipped_without_sale"].severity == Severity.INFO

    def test_the_headline_is_info_under_its_own_name(self):
        """The owner's decision of 2026-11-14 explains every one of these
        orders, so it is content in the digest, never a reason to send one.
        Kills: the WARN back (a month of digests sent for nothing), or the
        name changed — rule 15, and the Gate and the ledger key on it."""
        (issue,) = twins._line_items_check(self.NOV_14)
        assert (issue.check_name, issue.severity, issue.count) == (
            "pg_headline_vs_line_items", Severity.INFO, 461)

    def test_the_description_says_what_each_part_is(self):
        """Certificate redemptions (retail and exhibition) and test shipments
        to b2b partners, each with its count and amount, and the decision
        that says so. Kills: a finding that states a disagreement and not
        what it is — which is what kept it a WARN nobody could act on."""
        (issue,) = twins._line_items_check(self.NOV_14)
        text = issue.description
        assert "461 order(s) have grand_total = 0 but 1,023,472.67" in text
        assert "owner's decision of 2026-11-14" in text
        assert ("111 worth 213,919.67 (exhibition 5, retail 106) are certificate "
                "redemptions") in text
        assert "350 worth 809,553.00 (b2b) are test shipments to b2b partners" in text
        assert "VALUE_MISMATCH" in text

    def test_a_sales_type_no_decision_covers_is_named(self):
        """Silver's CASE yields none today; if it ever does, the finding says
        so instead of filing it under certificates. Kills: an unknown type
        folded silently into an explained part."""
        li = LineItems(3, 30.0, (), 0, 0.0, (),
                       headline_by_type=(("retail", 2, 20.0), ("wholesale2", 1, 10.0)))
        (issue,) = twins._line_items_check(li)
        assert "1 worth 10.00 (wholesale2) are of a sales type no decision covers" in (
            issue.description)
        assert "2 worth 20.00 (retail) are certificate redemptions" in issue.description

    def test_the_kinds_are_the_decisions(self):
        assert twins.ZERO_TOTAL_KINDS == {
            "retail": twins.CERTIFICATE_REDEMPTION,
            "exhibition": twins.CERTIFICATE_REDEMPTION,
            "b2b": twins.PARTNER_TEST_SHIPMENT,
        }

    def test_the_rows_fold_into_the_two_findings(self):
        """One row per sales type out of `_LINE_ITEMS_SQL`: internal is the
        goods finding, the rest the headline, whose sample is the ten largest
        across its types in the statement's own order. Kills: a type's rows
        lost, internal counted in the headline, or the sample taken per type."""
        rows = [
            {"sales_type": "internal", "n": 7, "amount": 700.0,
             "sample": [2], "sample_amounts": [700.0]},
            {"sales_type": "retail", "n": 4, "amount": 40.0,
             "sample": [11, 10] + list(range(100, 108)),
             "sample_amounts": [9.0, 9.0] + [1.0] * 8},
            {"sales_type": "b2b", "n": 2, "amount": 60.0,
             "sample": [5, 12], "sample_amounts": [50.0, 9.0]},
        ]
        li = twins.line_items_from_rows(rows)
        assert (li.headline, li.headline_amount, li.goods, li.goods_amount,
                li.goods_sample) == (6, 100.0, 7, 700.0, (2,))
        assert li.headline_sample == (5, 12, 11, 10, 107, 106, 105, 104, 103, 102)
        assert li.headline_by_type == (("b2b", 2, 60.0), ("retail", 4, 40.0))
        assert twins.line_items_from_rows([]) == LineItems(0, 0.0, (), 0, 0.0, ())


def _facts(**groups):
    base = dict(silver_arc=ARC_CLEAN, attribution=ATT_OK, line_items=LI_NONE,
                derivation_signal=JOURNAL_QUIET, order_landing=OrderLanding(),
                own_entity=twins.OwnEntity(), silver_row_values=RV_CLEAN)
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
        from core.sql_dialect import POSTGRES
        from core.warehouse_sql import silver_recompute_ctes

        sql = twins._row_values_sql()
        assert silver_recompute_ctes(POSTGRES) in sql
        assert "FROM bronze.orders o" in silver_recompute_ctes(POSTGRES)

    def test_every_silver_column_is_compared_and_counted(self):
        """A column Silver gains and the twin does not compare is a column that
        drifts in silence."""
        from core.pg_silver import SILVER_COLUMNS

        compared = [c for c, _ in twins._SILVER_ROW_COLUMNS]
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

        from core.pg_derivation import DROPPED_MARK_MARGIN, LAYER
        from core import pg_silver
        from core.pg_silver import SILVER_TABLE

        row = {"reported": 0, "covered": 0, "abandoned": 0, "in_flight": 0, "sample": None,
               "oldest_age_s": None, "rebuilt_at": None, "compared": 0, "held": 0,
               "overdue": 0, **{f"n_{c}": 0 for c, _ in twins._SILVER_ROW_COLUMNS}}
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
    def test_a_run_holds_exactly_what_could_not_look(self):
        assert twins.unverified_pg_conditions(
            ["pg_signal_missed", "pg_signal_missed"]) == ["pg_signal_missed"]
        assert twins.unverified_pg_conditions() == []

    def test_whole_blindness_holds_every_group(self):
        held = []
        twins.check_pg_warehouse(Facts.blind("x"), held_out=held)
        assert set(held) == {c for cs in twins.GUARD_CONDITIONS.values() for c in cs}

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
        names = ({c for cs in twins.GUARD_CONDITIONS.values() for c in cs}
                 | set(twins.UNWATCHED_NAMES.values())
                 | {twins.WHOLE_UNWATCHED})
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

    def test_facts_are_read_on_every_run(self):
        """No flag since step 14's S14-14: `read_facts` is called outside any
        `if`, and its findings are judged with no DuckDB scan beside them."""
        tree = self._tree()
        conditional = {id(m) for n in ast.walk(tree) if isinstance(n, ast.If)
                       for m in ast.walk(n)}
        reads = [n for n in ast.walk(tree) if isinstance(n, ast.Attribute)
                 and n.attr == "read_facts"]
        assert reads and not {id(n) for n in reads} & conditional
        judged = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Attribute) and n.func.attr == "check_pg_warehouse"]
        (call,) = judged
        assert {k.arg for k in call.keywords} == {"held_out"}

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

    def test_it_is_a_condition_with_a_human_name(self):
        from core.alerting import REGISTRY, Kind
        from core.data_quality import HUMAN_CHECK_NAMES

        assert REGISTRY["pg_signal_missed"].kind is Kind.CONDITION
        for name in ("pg_signal_missed", "pg_derivation_signal_unwatched"):
            assert name in HUMAN_CHECK_NAMES




# ─── DN-23: the order-landing twins ───────────────────────────────────────────

def _landing(ol):
    return twins._order_landing_check(ol)


class TestTheLandingTwinsStandingIn:
    """Each twin files what DuckDB's check would have, under its own `pg_`
    name and DuckDB's severity — every run since chain 3 stood DuckDB's
    checks down, and the only way since step 14's S14-14 deleted them."""

    def test_clean_files_nothing(self):
        assert _landing(OrderLanding()) == []

    def test_an_order_without_line_items_warns_and_leaves_the_in_flight_out(self):
        (issue,) = _landing(OrderLanding(without_items=3, without_items_in_flight=2,
                                         without_items_amount=900.0,
                                         without_items_sample=(7, 5, 3)))
        assert (issue.check_name, issue.severity, issue.count, issue.sample_ids) == (
            "pg_orders_without_line_items", Severity.WARN, 3, (7, 5, 3))
        assert "900.00" in issue.description and "2 more" in issue.description

    def test_orders_only_in_flight_are_not_filed(self):
        """Inside the settle grace a header may be ahead of its line items."""
        assert _landing(OrderLanding(without_items_in_flight=4)) == []

    def test_a_line_item_with_no_order_is_critical_and_samples_the_orders(self):
        (issue,) = _landing(OrderLanding(orphan_items=5, orphan_sample=(42, 41)))
        assert (issue.check_name, issue.severity, issue.count, issue.sample_ids) == (
            "pg_fk_orphan_order_products_order_id", Severity.CRITICAL, 5, (42, 41))

    def test_an_order_with_no_date_is_critical(self):
        (issue,) = _landing(OrderLanding(null_ordered_at=1, null_ordered_at_sample=(9,)))
        assert (issue.check_name, issue.severity, issue.sample_ids) == (
            "pg_not_null_orders_ordered_at", Severity.CRITICAL, (9,))

    def test_a_status_keycrm_added_is_named(self):
        """The new-KeyCRM-status detector: status 20 went unnoticed a month."""
        (issue,) = _landing(OrderLanding(unknown_status=3, unknown_status_values=(25, 977),
                                         unknown_status_sample=(8, 6, 4)))
        assert (issue.check_name, issue.severity, issue.count, issue.sample_ids) == (
            "pg_value_domain_orders_status_id", Severity.WARN, 3, (8, 6, 4))
        assert "Unknown values seen: 25, 977" in issue.description

    def test_a_source_nobody_registered_is_named(self):
        (issue,) = _landing(OrderLanding(unknown_source=2, unknown_source_values=(6,)))
        assert (issue.check_name, issue.severity, issue.count) == (
            "pg_value_domain_orders_source_id", Severity.WARN, 2)
        assert "Unknown values seen: 6" in issue.description

    def test_the_group_and_the_list_disagreeing_both_ways(self):
        (issue,) = _landing(OrderLanding(status_groups=((20, 6, 3, 300.0), (19, 4, 1, 50.0))))
        assert (issue.check_name, issue.severity, issue.count, issue.sample_ids) == (
            "pg_status_group_vs_return_list", Severity.WARN, 4, (20, 19))
        assert "status 20 is KeyCRM group 6 but our list says revenue" in issue.description
        assert "status 19 is KeyCRM group 4 but our list says excluded" in issue.description

    def test_every_twin_files_in_one_run_in_order(self):
        ol = OrderLanding(without_items=1, orphan_items=1, null_ordered_at=1,
                          unknown_status=1, unknown_source=1,
                          status_groups=((20, 6, 1, 10.0),))
        assert [i.check_name for i in _landing(ol)] == list(twins.LANDING_TWINS)


class TestTheLandingBlindness:
    def test_one_blind_landing_group_names_itself_and_holds_every_twin(self):
        held = []
        issues = twins.check_pg_warehouse(
            _facts(order_landing=Unwatched("the 20 s hold budget was spent")), held_out=held)
        assert _names(issues) == ["pg_order_landing_unwatched"]
        assert sorted(held) == sorted(twins.GUARD_CONDITIONS["order_landing"])

    def test_every_twin_is_a_condition_of_the_group(self):
        assert tuple(twins.GUARD_CONDITIONS["order_landing"]) == twins.LANDING_TWINS

    def test_it_is_read_before_the_recompute(self):
        """A budget the recompute spends must not blind the landing twins."""
        assert twins.GROUPS.index("order_landing") < twins.GROUPS.index("silver_row_values")


class TestTheLandingRead:
    def _conn(self):
        from unittest.mock import AsyncMock, MagicMock

        row = {"n": 0, "in_flight": 0, "amount": 0, "sample": None, "null_ordered_at": 0,
               "null_sample": None, "unknown_status": 0, "status_values": None,
               "status_sample": None, "unknown_source": 0, "source_values": None,
               "source_sample": None}
        return MagicMock(fetchrow=AsyncMock(return_value=row), fetch=AsyncMock(return_value=[]))

    def test_the_thresholds_are_bound_as_parameters(self):
        """DuckDB's own thresholds (`TestWhatMovedFromTheScan`), bound —
        not copied into the SQL."""
        import asyncio
        from decimal import Decimal

        from core.models import LOST_STATUS_GROUP_ID, OrderStatus

        conn = self._conn()
        facts = asyncio.run(twins._read_order_landing(conn, WM, 20))
        assert facts == OrderLanding()
        calls = {c.args[0]: c.args[1:] for c in conn.fetchrow.await_args_list}
        assert calls[twins._WITHOUT_ITEMS_SQL] == (Decimal("0.01"), 20)
        assert calls[twins._ORPHAN_ITEMS_SQL] == ()
        assert calls[twins._HEADERS_SQL] == (sorted(twins.KNOWN_STATUS_IDS),
                                             sorted(twins.KNOWN_SOURCE_IDS))
        (group_call,) = conn.fetch.await_args_list
        assert group_call.args == (twins._STATUS_GROUP_SQL, LOST_STATUS_GROUP_ID,
                                   sorted(int(s) for s in OrderStatus.return_statuses()))

    def test_history_not_backfilled_is_blindness_and_reads_nothing(self):
        import asyncio

        conn = self._conn()
        wm = Watermark(**{**WM.__dict__, "backfilled": False})
        facts = asyncio.run(twins._read_order_landing(conn, wm, 20))
        assert isinstance(facts, Unwatched) and "backfilled" in facts.reason
        assert conn.fetchrow.await_count == 0 and conn.fetch.await_count == 0


class TestTheLandingNames:
    def test_each_twin_is_a_pg_name_of_its_own(self):
        """Each is the DuckDB check's name prefixed — told apart from it, and
        from the names the images before step 14 retire, by the prefix
        (`tests/unit/test_rollback_condition_names.py`)."""
        assert twins.LANDING_TWINS == (
            "pg_orders_without_line_items", "pg_fk_orphan_order_products_order_id",
            "pg_not_null_orders_ordered_at", "pg_value_domain_orders_status_id",
            "pg_value_domain_orders_source_id", "pg_status_group_vs_return_list")


class TestTheLandingLevers:
    def test_every_landing_condition_has_a_lever_and_a_human_name(self):
        from core.data_quality import DEFAULT_REMEDIATION, HUMAN_CHECK_NAMES, remediation_for

        names = set(twins.GUARD_CONDITIONS["order_landing"]) | {
            twins.UNWATCHED_NAMES["order_landing"]}
        for name in names:
            assert remediation_for([name]) != [DEFAULT_REMEDIATION], name
            assert name in HUMAN_CHECK_NAMES, name
        # Its own line: the lever names the copy it read.
        (pg,) = remediation_for(["pg_fk_orphan_order_products_order_id"])
        assert "bronze.orders" in pg


class TestTheJobFilesTheTwins:
    """The integrity job end to end, the twins' facts handed in: they file on
    their own, with no DuckDB scan beside them since step 14's S14-14 (until
    then the scan stood the Silver checks down and the twins stood in). The
    run it journals goes through chain 9's door into a list
    (`tests/journal_fake.py`) — a DuckDB journal until step 14's S14-10."""

    LI = LineItems(5, 500.0, (1,), 7, 700.0, (2,))

    @pytest.fixture
    def job(self, tmp_path, monkeypatch):
        from unittest.mock import AsyncMock, patch

        from core.store import AnalyticsStore
        from core.scheduler import BackgroundScheduler

        for env in ("KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY", "KS_WRITE_GOALS"):
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setenv("KS_DQ_PG_WAREHOUSE", "on")
        from tests.journal_fake import install_journal

        journal = install_journal(monkeypatch)
        store = AnalyticsStore()
        scheduler = BackgroundScheduler()
        monkeypatch.setattr(scheduler, "_send_dq_alert_throttled",
                            AsyncMock(return_value=True))
        facts = _facts(line_items=self.LI)
        with patch("core.store.get_store", new=AsyncMock(return_value=store)), \
             patch("core.alerting.resolve_group", AsyncMock(return_value=0)), \
             patch.object(twins, "read_facts", AsyncMock(return_value=facts)):
            yield scheduler, journal

    def _run(self, scheduler, journal):
        import asyncio

        result = asyncio.run(scheduler._run_dq_integrity())
        return {r["check_name"]: r for r in journal.issues(result["run_id"])}

    def test_the_twins_file_on_their_own(self, job):
        persisted = self._run(*job)
        assert persisted["pg_headline_vs_line_items"]["count"] == 5
        assert persisted["pg_goods_shipped_without_sale"]["count"] == 7
        assert "integrity_check_raised" not in persisted
        assert "pg_twin_pairing" not in persisted
