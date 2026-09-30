"""OD-08 (a): ClickHouse is required for the parallel period after step 13.

Once Postgres alone derives Silver and Gold, the mirror-landing comparison of
the two engines' Gold (`core.ch_silver.reconcile_clickhouse`) is the only
independent re-aggregation of Gold left. It used to stand down in silence
without `KS_CH_URL`; a run that did not compare now says so as
`gold_values_unwatched` — WARN while DuckDB's own Gold is still compared,
CRITICAL once it is not. And `reconciliation_ch`, the ClickHouse arm of the
05:30 comparison against KeyCRM, is paged on by the canary like its Postgres
sibling, with a catch-up that reads its age.

Every guard here has a named mutation in the test that catches it. The
production payload at the bottom proves one point in time, the evening of
2026-09-30, and nothing more: whether the canary would have paged over the
last fourteen days is a question about history, which
`deploy/stage4_soak/22_reconciliation_ch_history.sql` answers on the host
(DN-21's bar), and which the OD-08 review found unasked.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from core import ch_silver, warehouse_cutover
from core.data_quality import IntegrityIssue, Severity

UNWATCHED = "gold_values_unwatched"


@pytest.fixture
def duckdb_derives(monkeypatch):
    """Production today: DuckDB derives, and its Gold is compared."""
    monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.DUCKDB)
    monkeypatch.setattr(warehouse_cutover, "_held", False)


@pytest.fixture
def postgres_alone(monkeypatch):
    """After step 13: Postgres alone derives."""
    monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.POSTGRES)
    monkeypatch.setattr(warehouse_cutover, "_held", False)


def _names(issues):
    return [i.check_name for i in issues]


def _unwatched(issues):
    found = [i for i in issues if i.check_name == UNWATCHED]
    assert len(found) <= 1, found
    return found[0] if found else None


def _cell(day: int, revenue: str = "100.00"):
    """One Gold roll-up cell in GOLD_COLUMNS order."""
    return (date(2026, 9, day), "retail", None, Decimal(revenue), 1, 1, 0, 1,
            0, Decimal("0.00"), Decimal(revenue))


def _stub_the_wire(monkeypatch, *, pg_cells, ch_cells, ship=None, readback=None):
    """Replace every network call `reconcile_clickhouse` makes."""
    monkeypatch.setattr(ch_silver, "configured", lambda: True)
    monkeypatch.setattr(ch_silver, "_fetch_pg_silver", AsyncMock(return_value=[]))
    monkeypatch.setattr(ch_silver, "_ship_silver_rows",
                        AsyncMock(side_effect=ship) if ship else AsyncMock())
    monkeypatch.setattr(ch_silver, "_mark_ok", AsyncMock())
    monkeypatch.setattr(ch_silver, "_mark_failed", AsyncMock())
    monkeypatch.setattr(ch_silver, "_derive_gold", AsyncMock(return_value=len(ch_cells)))
    monkeypatch.setattr(ch_silver, "_execute",
                        AsyncMock(side_effect=readback) if readback
                        else AsyncMock(return_value=""))
    monkeypatch.setattr(ch_silver, "_fetch_pg_gold_cells", AsyncMock(return_value=pg_cells))
    monkeypatch.setattr(ch_silver, "_fetch_ch_gold_cells", AsyncMock(return_value=ch_cells))


# ─── The comparison says when it did not compare ────────────────────────────

class TestTheComparisonNamesItsOwnBlindness:
    """Mutation: drop any one of the `gold_values_unwatched(...)` returns in
    `reconcile_clickhouse` and the test for that cause fails."""

    @pytest.mark.asyncio
    async def test_without_ks_ch_url_it_files_instead_of_standing_down(
        self, monkeypatch, duckdb_derives,
    ):
        monkeypatch.setattr(ch_silver, "configured", lambda: False)
        issues = await ch_silver.reconcile_clickhouse()
        assert _names(issues) == [UNWATCHED]
        assert "KS_CH_URL is not set" in issues[0].description

    @pytest.mark.asyncio
    async def test_a_failed_ship_is_the_stale_copy(self, monkeypatch, duckdb_derives):
        """The comparison ships what it compares, so a ship that fails leaves
        ClickHouse with the last good copy — and no comparison at all."""
        _stub_the_wire(monkeypatch, pg_cells=[_cell(1)], ch_cells=[_cell(1)],
                       ship=RuntimeError("Code: 241. Memory limit exceeded"))
        issues = await ch_silver.reconcile_clickhouse()
        assert _names(issues) == ["ch_silver_sync_failed", UNWATCHED]
        assert "RuntimeError" in _unwatched(issues).description
        ch_silver._fetch_pg_gold_cells.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_read_back(self, monkeypatch, duckdb_derives):
        _stub_the_wire(monkeypatch, pg_cells=[_cell(1)], ch_cells=[_cell(1)],
                       readback=httpx.ConnectError("connection refused"))
        issues = await ch_silver.reconcile_clickhouse()
        assert _names(issues) == ["ch_silver_unreachable", UNWATCHED]

    @pytest.mark.asyncio
    async def test_two_empty_golds_compare_nothing(self, monkeypatch, duckdb_derives):
        """Mutation: drop the empty-cells branch and this files nothing."""
        _stub_the_wire(monkeypatch, pg_cells=[], ch_cells=[])
        issues = await ch_silver.reconcile_clickhouse()
        assert _names(issues) == [UNWATCHED]
        assert "empty" in issues[0].description

    @pytest.mark.asyncio
    async def test_a_comparison_that_ran_is_silent_about_itself(
        self, monkeypatch, postgres_alone,
    ):
        """Production's normal morning: cells on both sides, equal. Under
        either mode no blindness is filed — only what the comparison found."""
        _stub_the_wire(monkeypatch, pg_cells=[_cell(1), _cell(2)],
                       ch_cells=[_cell(1), _cell(2)])
        assert await ch_silver.reconcile_clickhouse() == []

        _stub_the_wire(monkeypatch, pg_cells=[_cell(1), _cell(2)],
                       ch_cells=[_cell(1), _cell(2, "100.01")])
        issues = await ch_silver.reconcile_clickhouse()
        assert _names(issues) == ["ch_engines_gold_mismatch"]


class TestItsSeverityFollowsWhoElseLooks:
    """Mutation: make `gold_unwatched_severity` return WARN unconditionally
    and the two CRITICAL tests fail; CRITICAL unconditionally, the WARN one."""

    def test_warn_while_duckdb_gold_is_still_compared(self, duckdb_derives):
        assert ch_silver.gold_values_unwatched("x").severity is Severity.WARN

    def test_critical_once_postgres_alone_derives(self, postgres_alone):
        issue = ch_silver.gold_values_unwatched("x")
        assert issue.severity is Severity.CRITICAL
        assert "no second engine checked" in issue.description

    def test_critical_while_the_way_back_holds_duckdbs_comparison_down(
        self, monkeypatch,
    ):
        """DuckDB derives again but `reconcile_gold` still stands down (the
        hold): ClickHouse is again the only independent check that runs."""
        monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.DUCKDB)
        monkeypatch.setattr(warehouse_cutover, "_held", True)
        assert warehouse_cutover.duckdb_derives()
        assert ch_silver.gold_values_unwatched("x").severity is Severity.CRITICAL

    def test_unreadable_predicate_is_critical_and_still_files(self, monkeypatch):
        """Mutation: let the predicate's raise escape and this raises."""
        def boom():
            raise RuntimeError("cannot tell")

        monkeypatch.setattr(warehouse_cutover, "warehouse_checks_stand_down", boom)
        assert ch_silver.gold_values_unwatched("x").severity is Severity.CRITICAL

    def test_same_predicate_the_job_stands_reconcile_gold_down_on(self):
        """The severity must follow what the job actually did, so it reads the
        job's own predicate — never a second opinion of it."""
        import ast
        import inspect
        import textwrap
        from core.scheduler import BackgroundScheduler

        def calls(fn):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            return {n.func.attr for n in ast.walk(tree)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}

        predicate = "warehouse_checks_stand_down"
        assert predicate in calls(ch_silver.gold_unwatched_severity)
        assert predicate in calls(BackgroundScheduler._run_dq_mirror_landing)


# ─── The job files what the comparison cannot ───────────────────────────────

def _critical(name):
    return IntegrityIssue(check_name=name, table_name="gold.daily_revenue",
                          severity=Severity.CRITICAL, count=1, description="d")


class TestTheJobFilesWhatTheComparisonCannotReturn:
    """A raise leaves `reconcile_clickhouse` nothing to return. Mutation:
    drop the `if not gold_compared` block in `_run_dq_mirror_landing` and the
    first three fail."""

    @pytest.mark.asyncio
    async def test_a_raise_is_filed_and_the_run_still_fails(self, duckdb_derives):
        from tests.unit.test_mirror_landing_isolation import _run

        persisted, paged, resolved = await _run(
            {"reconcile_clickhouse": RuntimeError("lock wait")})
        issue = _unwatched(persisted["issues"])
        assert issue is not None and issue.severity is Severity.WARN
        assert "reconcile_clickhouse: RuntimeError" in issue.description
        assert "reconcile_clickhouse: RuntimeError" in persisted["error_message"]
        # WARN while DuckDB still compares: the digest's, not a page.
        assert paged == [] and resolved == []

    @pytest.mark.asyncio
    async def test_after_step_13_a_raise_pages_although_the_run_failed(
        self, postgres_alone,
    ):
        from tests.unit.test_mirror_landing_isolation import _run

        persisted, paged, _ = await _run(
            {"reconcile_clickhouse": RuntimeError("lock wait")})
        assert _unwatched(persisted["issues"]).severity is Severity.CRITICAL
        assert paged == [[UNWATCHED]]

    @pytest.mark.asyncio
    async def test_a_job_that_never_reached_the_comparison(self, duckdb_derives):
        from tests.unit.test_mirror_landing_isolation import _run

        def break_the_import():
            # `from core.ch_silver import reconcile_clickhouse` now raises
            # between the checks, where only `setup:` can catch it.
            del ch_silver.reconcile_clickhouse

        persisted, _, _ = await _run({}, hook=break_the_import)
        issue = _unwatched(persisted["issues"])
        assert issue is not None
        assert "never reached the comparison" in issue.description
        assert "setup: ImportError" in issue.description

    @pytest.mark.asyncio
    async def test_a_comparison_that_returned_is_not_second_guessed(
        self, duckdb_derives,
    ):
        """What the comparison returned is the verdict — the job adds nothing,
        clean or not."""
        from tests.unit.test_mirror_landing_isolation import _run

        persisted, _, _ = await _run({})
        assert persisted["issues"] == []
        persisted, paged, _ = await _run(
            {"reconcile_clickhouse": [_critical("ch_engines_gold_mismatch")]})
        assert _names(persisted["issues"]) == ["ch_engines_gold_mismatch"]
        assert paged == [["ch_engines_gold_mismatch"]]


class TestABlindRunResolvesNothingItDidNotSee:
    """Mutation: resolve the layer without `unverified` and a delivered
    `ch_engines_gold_mismatch` would be announced resolved by the first run
    that could not look."""

    @pytest.mark.asyncio
    async def test_blind_run_holds_the_comparisons_conditions(
        self, monkeypatch, duckdb_derives,
    ):
        """KS_CH_URL set, so the archive half counted its buckets and only
        the Gold half's conditions are held."""
        from tests.unit.test_mirror_landing_isolation import _run

        monkeypatch.setenv("KS_CH_URL", "http://127.0.0.1:1")
        held = []
        await _run({"reconcile_clickhouse": [ch_silver.gold_values_unwatched("x")]},
                   held=held)
        assert held == [list(ch_silver.GOLD_WATCH_CONDITIONS)]

    @pytest.mark.asyncio
    async def test_a_run_that_compared_holds_nothing(self, monkeypatch, duckdb_derives):
        from tests.unit.test_mirror_landing_isolation import _run

        monkeypatch.setenv("KS_CH_URL", "http://127.0.0.1:1")
        held = []
        await _run({}, held=held)
        assert held == [[]]

    def test_the_held_conditions_are_the_comparisons_own(self):
        """Derived from what `reconcile_clickhouse` and `compare_gold_cells`
        actually emit, so a check added there cannot be left unheld."""
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(ch_silver))
        emitted = set()
        for fn in ast.walk(tree):
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and fn.name in (
                "reconcile_clickhouse", "compare_gold_cells",
            ):
                for node in ast.walk(fn):
                    if isinstance(node, ast.Call):
                        for kw in node.keywords:
                            if kw.arg == "check_name" and isinstance(kw.value, ast.Constant):
                                emitted.add(kw.value.value)
        # The WARNs about ClickHouse itself clear on their own; what the
        # comparison alone re-examines is every CRITICAL it can file.
        criticals = {"ch_silver_roundtrip", "ch_engines_gold_missing",
                     "ch_engines_gold_extra", "ch_engines_gold_mismatch"}
        assert criticals <= emitted
        assert set(ch_silver.GOLD_WATCH_CONDITIONS) == criticals


class TestABlindArchiveRunResolvesNothingEither:
    """The OD-08 review's third finding: the same outage one call further
    down. `reconcile_ch_history` answers `ch_history_unreachable` (WARN), or
    [] without KS_CH_URL, and neither is a CRITICAL — so a delivered
    `ch_history_buckets`, which resolves only when "a human repairs the
    archive copy", was announced resolved by a run that could not read the
    copy. Mutation: drop `history_unverified_conditions` from the job's
    `unverified` and the first two fail."""

    OUTAGE = {
        "reconcile_clickhouse": [
            IntegrityIssue(check_name="ch_silver_unreachable",
                           table_name="silver.orders", severity=Severity.WARN,
                           count=1, description="ConnectError"),
        ],
        "reconcile_ch_history": [
            IntegrityIssue(check_name="ch_history_unreachable",
                           table_name="history.order_versions",
                           severity=Severity.WARN, count=1,
                           description="ConnectError"),
        ],
    }

    @pytest.mark.asyncio
    async def test_an_unreadable_copy_holds_the_archive_page(
        self, monkeypatch, postgres_alone,
    ):
        """The reviewer's scenario, after step 13."""
        from tests.unit.test_mirror_landing_isolation import _run

        monkeypatch.setenv("KS_CH_URL", "http://127.0.0.1:1")
        outage = dict(self.OUTAGE)
        outage["reconcile_clickhouse"] = outage["reconcile_clickhouse"] + [
            ch_silver.gold_values_unwatched("a read-back failed (ConnectError)")]
        held = []
        await _run(outage, held=held)
        assert "ch_history_buckets" in held[0]
        assert set(ch_silver.GOLD_WATCH_CONDITIONS) <= set(held[0])

    @pytest.mark.asyncio
    async def test_without_ks_ch_url_the_archive_page_is_held(
        self, monkeypatch, duckdb_derives,
    ):
        """No KS_CH_URL: the archive half returns [], which is not a count."""
        from tests.unit.test_mirror_landing_isolation import _run

        monkeypatch.delenv("KS_CH_URL", raising=False)
        held = []
        await _run({"reconcile_clickhouse": [
            ch_silver.gold_values_unwatched("KS_CH_URL is not set")]}, held=held)
        assert "ch_history_buckets" in held[0]

    @pytest.mark.asyncio
    async def test_a_counted_archive_holds_nothing_of_its_own(
        self, monkeypatch, duckdb_derives,
    ):
        """KS_CH_URL set and the buckets counted: a delivered
        `ch_history_buckets` that the count no longer finds does resolve.
        Mutation: hold it unconditionally and this fails."""
        from tests.unit.test_mirror_landing_isolation import _run

        monkeypatch.setenv("KS_CH_URL", "http://127.0.0.1:1")
        held = []
        await _run({"reconcile_ch_history": []}, held=held)
        assert "ch_history_buckets" not in held[0]

    def test_a_comparison_that_raised_or_was_never_reached_holds(self, monkeypatch):
        """`compared` False: the job's `check` caught a raise, or the job never
        got that far. Mutation: drop `not compared` and this fails."""
        from core import ch_history

        monkeypatch.setenv("KS_CH_URL", "http://127.0.0.1:1")
        assert ch_history.history_unverified_conditions([], compared=False) == [
            "ch_history_buckets"]
        assert ch_history.history_unverified_conditions([], compared=True) == []

    @pytest.mark.asyncio
    async def test_the_unreachable_name_is_the_one_the_comparison_files(
        self, monkeypatch,
    ):
        """The hold reads a check name; this proves it is the name the
        comparison actually files when ClickHouse does not answer."""
        from core import ch_history

        monkeypatch.setattr(ch_history, "configured", lambda: True)
        monkeypatch.setattr(ch_history, "_ch_max_id", AsyncMock(
            side_effect=httpx.ConnectError("connection refused")))
        issues = await ch_history.reconcile_ch_history()
        assert _names(issues) == [ch_history.HISTORY_UNREACHABLE]
        assert ch_history.history_unverified_conditions(issues, compared=True) == [
            "ch_history_buckets"]

    def test_the_held_conditions_are_every_critical_the_archive_files(self):
        """Derived from what `reconcile_ch_history` emits, so a CRITICAL
        added there cannot be left unheld."""
        import ast
        import inspect
        from core import ch_history

        tree = ast.parse(inspect.getsource(ch_history.reconcile_ch_history))
        critical = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                kws = {k.arg: k.value for k in node.keywords}
                name, sev = kws.get("check_name"), kws.get("severity")
                if (isinstance(name, ast.Constant) and isinstance(sev, ast.Attribute)
                        and sev.attr == "CRITICAL"):
                    critical.add(name.value)
        assert critical == {"ch_history_buckets"}
        assert set(ch_history.HISTORY_WATCH_CONDITIONS) == critical


# ─── A ClickHouse that stays down keeps paging (OD-08 review) ───────────────
#
# The 05:30 arm gates a copy more than 3 h old instead of blaming it. That run
# compared nothing against KeyCRM, and it was written as a success — status
# WARN, error_message NULL — which `fetch_last_success_ages` counts. So a
# ClickHouse that stayed down paged `dq_stale:reconciliation_ch` at most once,
# and the next morning's gated run reset the age: the canary announced the
# page resolved while ClickHouse was still down.

async def _arm(copy_age, *, ch_down=True):
    """The real `_reconcile_clickhouse`, ClickHouse refusing every query."""
    from core.scheduler import BackgroundScheduler

    now = datetime.now(timezone.utc)
    query = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
    with patch("core.ch_common.configured", return_value=True), \
         patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=AsyncMock()), \
         patch("core.mirror_reconciliation.fetch_watermarks", new=AsyncMock(
             return_value={"clickhouse.silver_orders": {
                 "last_ok_at": now - copy_age}})), \
         patch("core.reconciliation_io.pg_ids_updated_since",
               new=AsyncMock(return_value=set())), \
         patch("core.reconciliation_io.clickhouse_orders_in_window", new=query):
        scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
        result = await scheduler._reconcile_clickhouse(
            {}, window_start=date(2026, 7, 1), window_end=date(2026, 9, 30),
            as_of=now, inflight_ids=set())
    return result, query.await_count


class TestADeadClickHouseKeepsPaging:
    """Mutation: return `"error": None` from the stale gate again and all
    three fail — the gated run is a success, the second morning reads green,
    and it announces the layer's pages resolved."""

    @pytest.mark.asyncio
    async def test_a_stale_copy_compares_nothing_and_says_so(self):
        result, queried = await _arm(timedelta(hours=25, minutes=30))
        assert queried == 0                        # gated, not blamed
        assert result["discrepancies"] == []
        assert _names(result["issues"]) == ["ch_reconcile_pending"]
        assert result["error"].startswith("ch_reconcile_pending: ")
        assert "25.5 h ago" in result["error"]

    @pytest.mark.asyncio
    async def test_the_second_morning_of_an_outage_still_pages(self, tmp_path):
        """The reviewer's two mornings, persisted through the real
        `_persist_ch_reconciliation` into a real store and judged by the real
        canary. Day 1: ClickHouse died at 04:00, the copy is 1.5 h old, the
        comparison raises. Day 2: still down, the copy is 25.5 h old."""
        from bot import canary
        from core.data_quality import fetch_last_success_ages, persist_run
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        store = DuckDBStore(db_path=tmp_path / "t.duckdb")
        await store.connect()
        try:
            now = datetime.now(timezone.utc)
            async with store.connection() as conn:   # the last good morning
                persist_run(conn, started_at=now - timedelta(hours=54),
                            ended_at=now - timedelta(hours=54), as_of=now,
                            window_start=date(2026, 7, 1),
                            window_end=date(2026, 9, 30),
                            layer="reconciliation_ch", issues=[],
                            discrepancies=[])

            scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
            resolve = AsyncMock()
            with patch("core.duckdb_store.get_store",
                       new=AsyncMock(return_value=store)), \
                 patch("core.alerting.resolve_group", new=resolve):
                for copy_age, started in (
                    (timedelta(hours=1, minutes=30), now - timedelta(hours=30)),
                    (timedelta(hours=25, minutes=30), now - timedelta(minutes=5)),
                ):
                    result, _ = await _arm(copy_age)
                    assert result["error"], result
                    await scheduler._persist_ch_reconciliation(
                        result, started_at=started, as_of=started,
                        window_start=date(2026, 7, 1),
                        window_end=date(2026, 9, 30))

            async with store.connection() as conn:
                ages = fetch_last_success_ages(conn)
            failures, _ = canary.check_dq_freshness(
                {"data_quality": ages},
                {"reconciliation_ch": canary.DQ_MAX_AGE_S["reconciliation_ch"]})
            assert [k for k, _ in failures] == ["dq_stale:reconciliation_ch"]
            assert ages["reconciliation_ch"]["age_seconds"] >= 54 * 3600 - 60
            # Neither blind morning announced anything resolved.
            resolve.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_fresh_copy_that_compares_is_still_a_success(self):
        """The gate did not become a blanket failure: a copy under 3 h is
        compared, and a clean comparison carries no error."""
        from core.scheduler import BackgroundScheduler

        now = datetime.now(timezone.utc)
        with patch("core.ch_common.configured", return_value=True), \
             patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool", new=AsyncMock()), \
             patch("core.mirror_reconciliation.fetch_watermarks", new=AsyncMock(
                 return_value={"clickhouse.silver_orders": {
                     "last_ok_at": now - timedelta(minutes=40)}})), \
             patch("core.reconciliation_io.pg_ids_updated_since",
                   new=AsyncMock(return_value=set())), \
             patch("core.reconciliation_io.clickhouse_orders_in_window",
                   new=AsyncMock(return_value={})):
            scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
            result = await scheduler._reconcile_clickhouse(
                {}, window_start=date(2026, 7, 1), window_end=date(2026, 9, 30),
                as_of=now, inflight_ids=set())
        assert result == {"issues": [], "discrepancies": [], "error": None}


# ─── Its alert names a lever ────────────────────────────────────────────────

class TestItsAlert:
    def test_a_registered_condition(self):
        """Exact-match registry: unregistered, it would be delivered as an
        inert EVENT with no resolve."""
        from core.alerting import is_condition

        for key in (UNWATCHED, "dq_missing:reconciliation_ch",
                    "dq_never:reconciliation_ch", "dq_stale:reconciliation_ch"):
            assert is_condition(key), key

    def test_its_own_lever_not_the_gold_rebuild(self):
        """Mutation: remove the REMEDIATION entry and the longest prefix left
        is `gold_` — a warehouse rebuild, which re-aggregates nothing in a
        second engine."""
        from core.data_quality import remediation_for

        line = remediation_for([UNWATCHED])[0]
        assert "KS_CH_URL" in line
        assert "dq_mirror_landing/trigger" in line
        assert remediation_for([UNWATCHED]) != remediation_for(["gold_cell_values"])

    def test_the_page_reads_in_three_lines(self, postgres_alone):
        from core.data_quality import format_alert_message

        msg = format_alert_message(
            "mirror_landing", Severity.CRITICAL,
            [ch_silver.gold_values_unwatched("KS_CH_URL is not set")], [])
        lines = msg.split("\n")
        assert lines[0] == ("🚨 <b>Mirror check: Gold not re-checked by a second "
                            "engine (gold_values_unwatched) — 1</b>")
        assert lines[1].startswith("→ Set KS_CH_URL")
        assert len(lines) == 2


# ─── The diagnostic agent reads the ClickHouse runbook ──────────────────────

def _family_for(*conditions):
    """`family_for` from deploy/agent/run_agent.sh, run by bash as the host
    runs it — the function only, extracted the way the review did."""
    import subprocess
    from pathlib import Path

    script = Path(__file__).resolve().parents[2] / "deploy/agent/run_agent.sh"
    body = subprocess.run(
        ["sed", "-n", "/^family_for()/,/^}/p", str(script)],
        capture_output=True, text=True, check=True).stdout
    assert body.startswith("family_for()"), body
    out = subprocess.run(
        ["bash", "-c", body + '\nfor c in "$@"; do family_for "$c"; done', "_",
         *conditions], capture_output=True, text=True, check=True).stdout
    return dict(zip(conditions, out.split()))


class TestTheAgentReadsTheClickHouseRunbook:
    """The review's fourth finding: `gold_values_unwatched` went to the
    'default' runbook, which knows nothing of ClickHouse, and the mirror
    runbook it belongs to still called KS_CH_URL optional. After step 13 a
    CRITICAL `gold_values_unwatched` would have been diagnosed with no
    ClickHouse guidance at all. Mutation: drop `gold_values_unwatched` from
    the mirror line of `family_for` and the first test fails."""

    @staticmethod
    def _emitted_by_the_clickhouse_comparisons():
        import ast
        import inspect
        from core import ch_history

        names = set()
        for module in (ch_silver, ch_history):
            for node in ast.walk(ast.parse(inspect.getsource(module))):
                if isinstance(node, ast.Call):
                    for kw in node.keywords:
                        if kw.arg == "check_name" and isinstance(kw.value, ast.Constant):
                            names.add(kw.value.value)
        return names

    def test_every_clickhouse_condition_reaches_the_mirror_runbook(self):
        """Derived: every check name the two ClickHouse comparisons file, and
        the canary's three keys for the arm."""
        emitted = self._emitted_by_the_clickhouse_comparisons()
        assert UNWATCHED in emitted and "ch_history_buckets" in emitted
        keys = sorted(emitted | {f"dq_{kind}:reconciliation_ch"
                                 for kind in ("stale", "never", "missing")}
                      | {"ch_reconcile_pending"})
        families = _family_for(*keys)
        assert {k: f for k, f in families.items() if f != "mirror"} == {}

    def test_the_mirror_runbook_no_longer_calls_clickhouse_optional(self):
        from pathlib import Path

        text = (Path(__file__).resolve().parents[2]
                / "deploy/agent/runbooks/mirror.md").read_text(encoding="utf-8")
        header = text.splitlines()[0]
        assert UNWATCHED in header
        optional = [line for line in text.splitlines()
                    if "KS_CH_URL" in line and "опционал" in line]
        assert optional == []
        assert "OD-08 (a)" in text
        assert "table_name LIKE 'clickhouse.%'" in text


# ─── reconciliation_ch joins the canary ─────────────────────────────────────

def _ch_layer_aged(age_seconds):
    from tests.unit.test_canary import _healthy_payload

    payload = _healthy_payload()
    payload["data_quality"]["reconciliation_ch"]["age_seconds"] = age_seconds
    return payload


class TestTheCanaryPagesReconciliationCh:
    """Through the default thresholds on purpose: the watch is the entry in
    DQ_MAX_AGE_S. Mutation: drop it and the first four fail."""

    def test_silent_31h_fails(self):
        from bot import canary

        failures, ages = canary.check_dq_freshness(_ch_layer_aged(31 * 3600))
        assert [k for k, _ in failures] == ["dq_stale:reconciliation_ch"]
        assert ages["reconciliation_ch"] == 31 * 3600

    def test_absent_from_the_block_fails(self):
        from bot import canary

        payload = _ch_layer_aged(3600)
        del payload["data_quality"]["reconciliation_ch"]
        failures, _ = canary.check_dq_freshness(payload)
        assert [k for k, _ in failures] == ["dq_missing:reconciliation_ch"]

    def test_never_succeeded_fails(self):
        """What a web without KS_CH_URL looks like: `_reconcile_clickhouse`
        returns None and no run is ever written."""
        from bot import canary

        failures, _ = canary.check_dq_freshness(_ch_layer_aged(None))
        assert [k for k, _ in failures] == ["dq_never:reconciliation_ch"]

    @pytest.mark.asyncio
    async def test_pages_through_run_canary_as_a_warning(self):
        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _mock_transport

        payload = _ch_layer_aged(31 * 3600)
        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(lambda r: httpx.Response(200, json=payload)) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "warn"
        assert result.failure_keys == ["dq_stale:reconciliation_ch"]

    def test_29h_passes(self):
        from bot import canary

        failures, _ = canary.check_dq_freshness(_ch_layer_aged(29 * 3600))
        assert failures == []

    def test_same_limit_as_its_postgres_sibling_and_the_digest(self):
        from bot import canary
        from core.data_quality import DIGEST_MAX_AGE_HOURS

        assert canary.DQ_MAX_AGE_S["reconciliation_ch"] == \
            canary.DQ_MAX_AGE_S["reconciliation_pg"] == 30 * 3600
        assert DIGEST_MAX_AGE_HOURS["reconciliation_ch"] * 3600 == \
            canary.DQ_MAX_AGE_S["reconciliation_ch"]


# ─── The first probe after a start never beats the catch-up ─────────────────

class TestNoLayerPagesBeforeItsCatchUp:
    """The canary's first probe is 90 s after the bot starts; the catch-up runs
    minutes after web starts. A layer between the catch-up's limit and the
    canary's at a restart is refreshed before it can page — but only if some
    catch-up reads that layer's age. Mutation: take `reconciliation_ch` out of
    `CATCHUP_SIBLING_LAYERS` and the first test fails; widen the catch-up's
    limit to 30 h and the second."""

    @staticmethod
    def _catchup_limit_by_layer():
        from core.scheduler import CATCHUP_CHECKS, CATCHUP_SIBLING_LAYERS

        limits = {}
        for job_id, (layer, max_age_s, _delay) in CATCHUP_CHECKS.items():
            for name in (layer, *CATCHUP_SIBLING_LAYERS.get(job_id, ())):
                limits[name] = max_age_s
        return limits

    def test_every_paged_layer_is_read_by_a_catch_up(self):
        from bot import canary

        assert set(canary.DQ_MAX_AGE_S) <= set(self._catchup_limit_by_layer())

    def test_every_catch_up_fires_before_its_page(self):
        from bot import canary

        limits = self._catchup_limit_by_layer()
        for layer, page_after in canary.DQ_MAX_AGE_S.items():
            assert limits[layer] < page_after, layer


def _ages(**by_layer):
    return {layer: {"last_success_at": None if age is None else "2026-09-30T05:30:00+03:00",
                    "age_seconds": age}
            for layer, age in by_layer.items()}


class TestTheReconciliationCatchUpReadsItsArms:
    @pytest.fixture
    def stores(self, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://u:p@127.0.0.1:1/x")
        monkeypatch.setenv("KS_CH_URL", "http://127.0.0.1:1")

    @staticmethod
    async def _queued(monkeypatch, ages):
        from tests.unit.test_scheduler_catchup import _scheduler_with

        scheduler = _scheduler_with(monkeypatch, ages)
        await scheduler._schedule_catchup_runs()
        return set(scheduler._scheduler.added)

    @pytest.mark.asyncio
    async def test_an_arm_that_failed_its_last_run_is_caught_up(self, monkeypatch, stores):
        """The DuckDB half ran at 05:30, the ClickHouse arm raised; a deploy
        at 08:00 finds the arm at 26.5 h. Without the catch-up it pages at
        11:30 though one run would have cleared it."""
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600, reconciliation_pg=3 * 3600,
            reconciliation_ch=int(26.5 * 3600)))
        assert "dq_reconciliation_catchup" in queued

    @pytest.mark.asyncio
    async def test_an_arm_that_never_succeeded_is_caught_up(self, monkeypatch, stores):
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600, reconciliation_ch=None))
        assert "dq_reconciliation_catchup" in queued

    @pytest.mark.asyncio
    async def test_the_postgres_arm_too(self, monkeypatch, stores):
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600, reconciliation_pg=27 * 3600))
        assert "dq_reconciliation_catchup" in queued

    @pytest.mark.asyncio
    async def test_fresh_arms_queue_nothing(self, monkeypatch, stores):
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600, reconciliation_pg=3 * 3600,
            reconciliation_ch=3 * 3600))
        assert queued == set()

    @pytest.mark.asyncio
    async def test_a_host_without_clickhouse_does_not_loop(self, monkeypatch, stores):
        """No KS_CH_URL: the arm is never written, so its null age would queue
        a KeyCRM re-fetch on every start. Mutation: drop the `_writes_layer`
        condition and this queues."""
        monkeypatch.delenv("KS_CH_URL")
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600, reconciliation_ch=None))
        assert queued == set()

    @pytest.mark.asyncio
    async def test_clickhouse_without_postgres_does_not_loop(self, monkeypatch, stores):
        """KS_CH_URL but no KS_PG_DSN: `_reconcile_clickhouse` returns None on
        the Postgres half of its gate as well (its watermark and exclusion set
        live in Postgres), so the layer is never written here either. The
        review's surviving mutation: gate `reconciliation_ch` on
        `ch_common.configured()` alone and this queues a KeyCRM re-fetch on
        every start."""
        from core.scheduler import _writes_layer

        monkeypatch.delenv("KS_PG_DSN")
        assert not _writes_layer("reconciliation_ch")
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600, reconciliation_ch=None))
        assert queued == set()

    @pytest.mark.asyncio
    async def test_an_arm_the_reader_did_not_report_is_no_evidence(
        self, monkeypatch, stores,
    ):
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=3 * 3600, integrity=3600))
        assert queued == set()

    @pytest.mark.asyncio
    async def test_an_unreadable_configuration_leaves_the_job_layer_to_decide(
        self, monkeypatch, stores,
    ):
        from core import ch_common

        def boom():
            raise RuntimeError("env unreadable")

        monkeypatch.setattr(ch_common, "configured", boom)
        queued = await self._queued(monkeypatch, _ages(
            reconciliation=30 * 3600, integrity=3600, reconciliation_ch=None))
        assert queued == {"dq_reconciliation_catchup"}


# ─── Production, one evening ────────────────────────────────────────────────
#
# /api/health as production published it on 2026-09-30 (correlation id
# dropped; the endpoint is public and carries no host, user or text). KS_CH_URL
# is set there, and that evening reconciliation_ch's last success was the same
# 05:30 run as the DuckDB and Postgres halves. One snapshot: it shows the
# canary's shape agrees with production's payload, not that no morning of the
# last fourteen would have paged. That is the history check's question, and
# `TestTheHistoryIsAskedForEveryPagedArm` holds that it exists.

PRODUCTION_HEALTH_2026_09_30 = {
    "status": "healthy",
    "version": "3.0.258",
    "uptime_seconds": 2253,
    "duckdb": {"status": "connected", "latency_ms": 0.0, "orders": 48377,
               "products": 1017, "categories": 28, "managers": None,
               "db_size_mb": 184.76},
    "sync": None,
    "migrations": {"status": "ok", "applied": 26, "total": 26, "pending": [],
                   "failed": []},
    "data_quality": {
        "integrity": {"last_success_at": "2026-09-30T19:00:00.076452+03:00",
                      "age_seconds": 12975},
        "reconciliation": {"last_success_at": "2026-09-30T05:30:00.001799+03:00",
                           "age_seconds": 61575},
        "mirror_landing": {"last_success_at": "2026-09-30T07:30:00.001573+03:00",
                           "age_seconds": 54375},
        "reconciliation_pg": {"last_success_at": "2026-09-30T05:30:00.001799+03:00",
                              "age_seconds": 61575},
        "reconciliation_ch": {"last_success_at": "2026-09-30T05:30:00.001799+03:00",
                              "age_seconds": 61575},
    },
    "alerting": {"consecutive_transport_failures": 0, "last_delivery_at": None},
    "buyer_sync": {"last_attempt_age_s": None, "last_ok_age_s": 2250,
                   "ever_ok": False, "consecutive_failures": 0,
                   "last_error_class": None, "last_selected": 0,
                   "last_written": 0, "unreadable_birthdays": 0,
                   "retry_in_s": None},
    "write_chains": {
        "pg_inventory_write": {
            "env": "KS_WRITE_INVENTORY", "mode": "postgres", "error": None,
            "latched": True, "latched_at": "2026-09-30T13:33:23.853917+00:00",
            "mismatch": False, "unmet_precondition": None,
            "preflight": {"ok": None, "reasons": [
                "chain 1 already writes Postgres; the preflight is the question "
                "asked before the flip"]},
            "sync_step": {"failures_since_ok": 0, "last_ok_at": None,
                          "last_failure_at": None, "last_failed_step": None,
                          "last_error": None, "retry_in_s": None},
        },
        "pg_expenses_write": {"env": "KS_WRITE_EXPENSES", "mode": "postgres",
                              "error": None, "latched": False, "latched_at": None,
                              "mismatch": False, "unmet_precondition": None},
        "pg_goals_write": {"env": "KS_WRITE_GOALS", "mode": "duckdb",
                           "error": None, "latched": False, "latched_at": None,
                           "mismatch": False, "unmet_precondition": None},
        "pg_expense_types_write": {"env": "KS_WRITE_EXPENSE_TYPES",
                                   "mode": "duckdb", "error": None,
                                   "latched": False, "latched_at": None,
                                   "mismatch": False, "unmet_precondition": None},
    },
    "derivation": {"mode": "own", "error": None, "signal_unreadable_ticks": 0,
                   "marks_dropped_unhealed": 0, "last_mark_drop_age_s": None},
    "read_fallbacks": {},
    "read_fallback_mode": {"mode": "duckdb", "error": None, "misconfigured": []},
    "warehouse_writer_mode": {"mode": "duckdb", "value": "duckdb", "error": None,
                              "preconditions_unmet": [], "held": False,
                              "held_for_s": None, "reclassify_needed": False},
    "utm_parse": {"mode": "duckdb", "error": None},
    "mirrors": {
        "bronze.orders": {"last_ok_at": "2026-09-30T19:27:05.233538+00:00",
                          "age_seconds": 549, "failures_since_ok": 0,
                          "failing": False, "max_age_s": None},
        "silver.orders": {"last_ok_at": "2026-09-30T19:32:05.547543+00:00",
                          "age_seconds": 249, "failures_since_ok": 0,
                          "failing": False, "max_age_s": 5400},
        "gold.daily_revenue": {"last_ok_at": "2026-09-30T19:32:06.792512+00:00",
                               "age_seconds": 248, "failures_since_ok": 0,
                               "failing": False, "max_age_s": 5400},
    },
}


async def _judge(payload):
    from bot import canary
    from tests.unit.test_canary import DASHBOARD, _mock_transport

    future = datetime.now(timezone.utc) + timedelta(days=60)
    cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
    async with _mock_transport(lambda r: httpx.Response(200, json=payload)) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=cert):
            return await canary.run_canary(DASHBOARD, client=client)


class TestNothingNewPagesInProduction:
    @pytest.mark.asyncio
    async def test_production_health_is_green_with_reconciliation_ch_watched(self):
        result = await _judge(PRODUCTION_HEALTH_2026_09_30)
        assert (result.severity, result.failure_keys) == ("ok", [])
        assert result.dq_ages["reconciliation_ch"] == 61575

    @pytest.mark.asyncio
    async def test_a_deploy_at_the_worst_moment_of_a_healthy_day(self):
        """The oldest a passing reconciliation_ch ever is when a bot starts:
        a minute before the next 05:30 run. Far inside 30 h."""
        import copy

        payload = copy.deepcopy(PRODUCTION_HEALTH_2026_09_30)
        for layer in ("reconciliation", "reconciliation_pg", "reconciliation_ch"):
            payload["data_quality"][layer]["age_seconds"] = 24 * 3600 - 60
        result = await _judge(payload)
        assert (result.severity, result.failure_keys) == ("ok", [])

    @pytest.mark.asyncio
    async def test_a_blind_comparison_today_is_the_digests_not_a_page(
        self, duckdb_derives,
    ):
        """Production runs DuckDB's derivation (`warehouse_writer_mode.mode`
        above), so the mirror-landing job files a ClickHouse outage as a WARN
        for the 09:00 digest and pages nobody over it. An outage that costs
        the 05:30 comparison against KeyCRM pages, but through the canary's
        `reconciliation_ch` watch, 30 h after the last run that compared
        (`TestADeadClickHouseKeepsPaging`)."""
        from tests.unit.test_mirror_landing_isolation import _run

        assert PRODUCTION_HEALTH_2026_09_30["warehouse_writer_mode"]["mode"] == "duckdb"
        persisted, paged, _ = await _run(
            {"reconcile_clickhouse": [ch_silver.gold_values_unwatched("down")]})
        assert _unwatched(persisted["issues"]).severity is Severity.WARN
        assert paged == []


class TestTheHistoryIsAskedForEveryPagedArm:
    """The OD-08 review's second finding: DN-21 put `reconciliation_pg` under
    the canary only on a history read over fourteen days, and nothing asked
    the same of `reconciliation_ch`. Every arm of the 05:30 job the canary
    pages on has a stage-4 soak check that reads its history against the
    canary's own limit. Mutation: delete 22_reconciliation_ch_history.sql and
    this fails."""

    def test_every_paged_arm_has_its_history_check(self):
        import re
        from pathlib import Path

        from bot.canary import DQ_MAX_AGE_S
        from core.scheduler import CATCHUP_SIBLING_LAYERS

        soak = Path(__file__).resolve().parents[2] / "deploy/stage4_soak"
        judged = {}
        for path in sorted(soak.glob("*.sql")):
            code = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
            if "canary_max_age" not in code:
                continue
            m = re.search(r"layer_runs AS \(.*?r\.layer = '(\w+)'", code, flags=re.S)
            if m:
                judged[m.group(1)] = path.name
        arms = [a for a in CATCHUP_SIBLING_LAYERS["dq_reconciliation"]
                if a in DQ_MAX_AGE_S]
        assert arms == ["reconciliation_pg", "reconciliation_ch"]
        assert {a: judged.get(a) for a in arms} == {
            "reconciliation_pg": "20_reconciliation_pg_history.sql",
            "reconciliation_ch": "22_reconciliation_ch_history.sql",
        }
