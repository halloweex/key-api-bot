"""`scripts/goals_semantics_dryrun.py` — chain 7b-2's flip precondition.

It runs the real goal methods twice over one in-memory copy of a backup,
under `KS_GOALS_HISTORY=bridge` and `=silver`, and exits 0 only when nothing
differs. Proved here on a backup written for the purpose:

  * the three-year history reads clean — 0, and the backup's bytes unchanged;
  * an order KeyCRM groups as lost whose status is not on the list is counted
    by the bridge alone: the script names the cause, shows the numbers it
    moved, and exits 1 — so the silver side really ran Silver (mutation:
    compute both sides under `bridge`, and the numbers here agree);
  * no read leaves the copy, whatever `KS_READ_GOALS` says in the
    environment (mutation: drop the forced `duckdb`, and a read is routed —
    counted as a fallback here);
  * a missing backup, or one without a goal table, is refused with 2;
  * each path to DIFFERENCES on its own: a growth cap — a list leaf — moving
    alone, and an order counted by one side only while no number moves, on
    either side (mutations MXp, MXq, MXq2).
  * where `reconcile_silver` does not run, its silence is no verdict: a
    backup recording Postgres as the warehouse writer is refused, and
    `KS_WRITE_WAREHOUSE` other than duckdb — like `KS_MIRROR_LANDING` off —
    is a reason, read by the real `postgres_half` from its environment
    (mutations MW1–MW6).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from core import read_fallback
from core.duckdb_store import DuckDBStore
from tests.unit.test_goals_off_duckdb_silver import _seed_history

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from scripts import goals_semantics_dryrun as dryrun  # noqa: E402

KYIV = ZoneInfo("Europe/Kyiv")
TODAY = "2026-10-01"


def _backup(tmp_path, *extra) -> Path:
    path = tmp_path / "analytics-backup.duckdb"

    async def build():
        store = DuckDBStore(db_path=path)
        await store.connect()
        try:
            await _seed_history(store)
            async with store.connection() as conn:
                for sql, params in extra:
                    conn.execute(sql, params)
            if extra:
                # No ids: a full rebuild of Silver, which then holds them.
                await store.refresh_warehouse_layers(trigger="manual")
        finally:
            await store.close()

    asyncio.run(build())
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def _clean_counts(monkeypatch):
    monkeypatch.delenv("KS_GOALS_HISTORY", raising=False)
    read_fallback.reset_counts()
    yield
    read_fallback.reset_counts()


NOW = datetime(2026, 10, 1, 9, 0, tzinfo=ZoneInfo("UTC"))


def _state(**overrides):
    """What `read_postgres_state` returns for a healthy morning: a journal
    copy 20 min old, the 07:30 Kyiv run clean, nothing against Silver."""
    state = {
        "taken_at": NOW, "role": "ks_readonly",
        "journal_ok_at": NOW - timedelta(minutes=20), "journal_failures": 0,
        "run": {"run_id": 4242, "started_at": NOW - timedelta(hours=4, minutes=30),
                "error_message": None},
        "findings": [],
    }
    state.update(overrides)
    return state


@pytest.fixture(autouse=True)
def _postgres_clean(request, monkeypatch):
    """The Postgres half answers clean unless a test says otherwise: these
    tests are about the backup half, and the Postgres one has its own below
    and in `tests/integration/test_goals_semantics_dryrun_pg.py`."""
    if getattr(request.cls, "REAL_POSTGRES_HALF", False):
        return

    async def clean(dsn=None):
        return dryrun.judge_postgres(_state(), landing_on=True)

    monkeypatch.setattr(dryrun, "postgres_half", clean)


class TestTheDryRun:
    def test_the_history_reads_clean_and_the_backup_is_untouched(
        self, tmp_path, capsys,
    ):
        backup = _backup(tmp_path)
        before = _sha(backup)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 0
        out = capsys.readouterr().out
        assert "CLEAN" in out and "no difference" in out
        assert _sha(backup) == before, "the dry run changed the backup"

    def test_a_difference_is_named_with_its_cause_and_its_numbers(
        self, tmp_path, capsys,
    ):
        # Lost by KeyCRM's group, status 1 not on the list: the bridge counts
        # it, Silver does not (the return rule — 0 such orders in production).
        backup = _backup(tmp_path, (
            "INSERT INTO orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id) VALUES (900001, 1, 1, 6, 250000, ?, 1)",
            [datetime(2025, 3, 10, 12, tzinfo=KYIV)]))
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        import json

        report = json.loads(capsys.readouterr().out)
        assert report["clean"] is False
        retail = report["orders"]["retail"]
        assert retail["bridge"] == retail["silver"] + 1
        assert list(retail["bridge_only"]) == ["return rule: status 1, group 6"]
        paths = [d["path"] for d in report["differences"]]
        assert any(p.startswith("seasonality/retail/3/") for p in paths), paths[:10]
        assert any(p.startswith("monday_job/seasonal/3/") for p in paths)

    def test_no_read_leaves_the_copy(self, tmp_path, monkeypatch, capsys):
        backup = _backup(tmp_path)
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://ks_app:x@127.0.0.1:9/ks")
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 0
        assert read_fallback.counts() == {}, "a goal read was routed off the copy"

    @pytest.mark.parametrize("latched", [True, False], ids=["latched", "flagged_and_ready"])
    def test_chain_7b3_stays_on_the_copy(self, tmp_path, monkeypatch, capsys,
                                         latched):
        """Run as the web service, the dry run has `KS_PG_DSN` and web's
        `./data`. With chain 7b-3 latched, or flagged with everything it
        needs, the Monday job's store and the smart goal's read would go to
        production Postgres, and the first write would latch the chain from a
        one-off container. `measure` pins both answers to DuckDB.

        "Everything it needs" cannot happen for real inside `measure`, which
        sets `KS_READ_GOALS=duckdb` and so leaves clause 2 unmet; it is stood
        in for by `unmet_precondition` answering None, or the unlatched case
        would pass with the pins or without them. Mutation: drop either pin
        and both cases fail here — the store reaches the dry run's raising
        pool, or the read its raising fetch. The pool itself, the second
        wall, is `test_the_writers_pool_is_a_wall_of_its_own`'s."""
        from core import chain_latch, pg_forecast_write

        backup = _backup(tmp_path)
        reached = []

        async def _recording_pool():
            reached.append("pool")
            raise RuntimeError("reached production Postgres")

        monkeypatch.setattr(pg_forecast_write, "_pool", _recording_pool)
        monkeypatch.setenv(pg_forecast_write.WRITE_ENV, "postgres")
        monkeypatch.setenv("KS_READ_FORECAST_INPUT", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://ks_app:x@127.0.0.1:9/ks")
        monkeypatch.setattr(read_fallback, "_mode", read_fallback.OFF)
        if latched:
            chain_latch.latch(pg_forecast_write.CHAIN, pg_forecast_write.WRITE_ENV)
        else:
            monkeypatch.setattr(pg_forecast_write, "unmet_precondition", lambda: None)
            # Outside the dry run, this is a chain that writes Postgres.
            assert pg_forecast_write.writes_postgres() is True
            assert pg_forecast_write.reads_postgres() is True
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 0
        assert reached == []
        assert chain_latch.latched(pg_forecast_write.CHAIN) is latched
        assert read_fallback.counts() == {}

    def test_the_writers_pool_is_a_wall_of_its_own(self, monkeypatch):
        """Inside the pins, either writer reached by a route that never asks
        `writes_postgres` raises the dry run's own error — before it acquires
        a connection, so before it latches — and never reaches the pool
        beneath. Mutation: the `_pool` patch dropped from `held_off_chain_7b3`
        — the writer reaches that pool."""
        from datetime import timezone

        from core import chain_latch, pg_forecast_write

        reached = []

        async def _recording_pool():
            reached.append("pool")
            raise RuntimeError("reached production Postgres")

        monkeypatch.setattr(pg_forecast_write, "_pool", _recording_pool)
        now = datetime.now(timezone.utc)
        with dryrun.held_off_chain_7b3():
            assert pg_forecast_write.writes_postgres() is False
            assert pg_forecast_write.reads_postgres() is False
            with pytest.raises(RuntimeError, match="in-memory copy only"):
                asyncio.run(pg_forecast_write.persist_goal_tables(
                    [(1, 1.0, 3, 1000.0, 900.0, 1100.0, "high")],
                    (0.1, None, None, 0), {}, [], now))
            with pytest.raises(RuntimeError, match="in-memory copy only"):
                asyncio.run(pg_forecast_write.store_predictions(
                    [{"date": "2026-10-08", "predicted_revenue": 1.0}],
                    "retail", {}, now))
        assert reached == []
        assert not chain_latch.latched(pg_forecast_write.CHAIN)

    def test_a_missing_backup_is_refused(self, tmp_path, capsys):
        assert dryrun.main(["--backup", str(tmp_path / "none.duckdb")]) == 2
        assert "refused" in capsys.readouterr().err

    def test_a_backup_without_a_goal_table_is_refused(self, tmp_path, capsys):
        import duckdb

        path = tmp_path / "partial.duckdb"
        conn = duckdb.connect(str(path))
        conn.execute("CREATE TABLE orders (id INTEGER)")
        conn.close()
        assert dryrun.main(["--backup", str(path)]) == 2
        assert "silver_orders" in capsys.readouterr().err


class TestEveryVerdictPathCounts:
    """The two ways the dry run reaches DIFFERENCES, each pinned on its own
    (review of 7b-2: both mutations below passed the tests above, which only
    ever produced a difference in a number *and* in the orders at once)."""

    def test_a_list_leaf_is_compared(self):
        """The twelve growth caps per sales type are a list. Mutation MXp:
        skip lists in `_differences`, and a cap that moved reads clean."""
        bridge = {"caps/retail": [0.25, 0.3, 0.35], "nested": [{"a": [1.0, 2.0]}]}
        silver = {"caps/retail": [0.25, 0.31, 0.35], "nested": [{"a": [1.0, 2.5]}]}
        assert dryrun._differences(bridge, silver) == [
            ("caps/retail[1]", 0.3, 0.31),
            ("nested[0]/a[1]", 2.0, 2.5),
        ]
        # A list that changed length is one difference, not a silent pass.
        assert dryrun._differences({"x": [1.0]}, {"x": [1.0, 2.0]}) == [
            ("x", [1.0], [1.0, 2.0])]

    def test_a_cap_alone_moving_fails_the_run(self, tmp_path, monkeypatch, capsys):
        """End to end: the silver side's answers differ in one cap and in
        nothing else — exit 1, the cap named by its path."""
        import json

        from core import pg_goals_read

        backup = _backup(tmp_path)
        real = dryrun.answers

        async def one_cap_moved(conn, today):
            out = await real(conn, today)
            if pg_goals_read.history_mode() == "silver":
                out["caps/retail"][3] = round(out["caps/retail"][3] + 0.01, 6)
            return out

        monkeypatch.setattr(dryrun, "answers", one_cap_moved)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        report = json.loads(capsys.readouterr().out)
        assert [d["path"] for d in report["differences"]] == ["caps/retail[3]"]
        assert all(not v["bridge_only"] and not v["silver_only"]
                   for v in report["orders"].values())

    @pytest.mark.parametrize("status,group,side", [
        (1, 6, "bridge_only"),    # lost by KeyCRM's group, status not listed
        (19, 4, "silver_only"),   # status listed, KeyCRM's group says not lost
    ])
    def test_orders_that_differ_fail_the_run_when_no_number_moves(
        self, tmp_path, capsys, status, group, side,
    ):
        """An order counted by one side only, worth nothing: no number moves,
        and the flip would still change which orders count. Mutation MXq:
        `Report.clean` reads the numbers alone, and both exit 0; MXq2: it
        reads `bridge_only` alone, and the second does."""
        import json

        backup = _backup(tmp_path, (
            "INSERT INTO orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id) VALUES (900001, 1, ?, ?, 0, ?, 1)",
            [status, group, datetime(2025, 3, 10, 12, tzinfo=KYIV)]))
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        report = json.loads(capsys.readouterr().out)
        assert report["differences"] == [] and report["clean"] is False
        retail = report["orders"]["retail"]
        assert list(retail[side]) == [f"return rule: status {status}, group {group}"]
        other = "silver_only" if side == "bridge_only" else "bridge_only"
        assert retail[other] == {}


# ─── The Postgres half: the engine the flip switches to ────────────────────

class TestThePostgresVerdict:
    """`judge_postgres` over the state read out of Postgres' copy of the
    quality journal (review of 7b-2: the gate compared DuckDB with DuckDB and
    never asked about Postgres Silver). Each reason names the mutation that
    removes it; each must make the verdict unclean on its own."""

    def _reasons(self, landing_on=True, **overrides):
        verdict = dryrun.judge_postgres(_state(**overrides), landing_on=landing_on)
        return verdict, verdict.reasons

    def test_a_healthy_morning_is_clean(self):
        verdict, reasons = self._reasons()
        assert reasons == [] and verdict.clean
        assert (verdict.run_id, verdict.run_age_s, verdict.journal_copy_age_s) == (
            4242, int(timedelta(hours=4, minutes=30).total_seconds()), 20 * 60)

    def test_a_finding_against_silver_is_not_proved(self):
        """Mutation: drop the findings reason — Postgres Silver missing
        orders reads clean."""
        verdict, reasons = self._reasons(findings=[
            ("mirror_missing_rows", "critical", 12),
            ("mirror_row_values", "critical", 3)])
        assert not verdict.clean
        assert reasons == [
            "the latest mirror_landing run (4242) filed 2 finding(s) against "
            "silver.orders: mirror_missing_rows (critical, 12), "
            "mirror_row_values (critical, 3)"]

    def test_a_run_older_than_the_canarys_limit(self):
        """Mutation: drop the age reason — yesterday's yesterday reads clean."""
        _, reasons = self._reasons(run={"run_id": 4242, "started_at": NOW - timedelta(hours=30),
                                        "error_message": None})
        assert reasons == ["the latest mirror_landing run (4242) is 30 h old (limit 30)"]
        _, reasons = self._reasons(run={"run_id": 4242, "started_at": NOW - timedelta(hours=29, minutes=59),
                                        "error_message": None})
        assert reasons == []

    def test_the_limits_are_the_journals_own(self):
        """One definition with chain 1's preflight, the second pinned there
        to the canary's `mirror_landing` limit."""
        from bot import canary
        from core import pg_inventory_write

        table, copy_within, verdict_within = dryrun._journal_limits()
        assert table == "app.data_quality_runs"
        assert copy_within == pg_inventory_write.PREFLIGHT_JOURNAL_COPY_WITHIN
        assert verdict_within.total_seconds() == canary.DQ_MAX_AGE_S["mirror_landing"]

    def test_no_run(self):
        """Mutation: return clean when there is no run."""
        verdict, reasons = self._reasons(run=None)
        assert reasons == ["no mirror_landing run in the quality journal"]
        assert verdict.run_id is None

    @pytest.mark.parametrize("ok_at,failures,expected", [
        (None, 0, "the quality journal has never been copied into Postgres"),
        (NOW - timedelta(minutes=5), 2, "the copy of the quality journal is failing (2 in a row)"),
        (NOW - timedelta(minutes=75), 0, "the copy of the quality journal is 75 min old (limit 75)"),
    ], ids=["never", "failing", "stale"])
    def test_a_journal_copy_that_cannot_be_trusted(self, ok_at, failures, expected):
        """Mutation: drop any one branch — a copy that stopped keeps showing
        an older clean run for as long as anybody looks."""
        _, reasons = self._reasons(journal_ok_at=ok_at, journal_failures=failures)
        assert len(reasons) == 1 and reasons[0].startswith(expected), reasons

    @pytest.mark.parametrize("error,expected", [
        ("1 check(s) raised — reconcile_silver: ConnectionError: gone",
         "failed in reconcile_silver"),
        ("1 check(s) raised — setup: RuntimeError: x", "failed between its checks"),
        ("boom", "its error does not say which check raised"),
        ("2 check(s) raised — reconcile_gold: E: x", "its error does not say"),
    ], ids=["silver_raised", "setup", "unparseable", "fewer_than_counted"])
    def test_a_run_that_may_not_have_compared_silver(self, error, expected):
        """Mutation: read a failed run as compared."""
        _, reasons = self._reasons(run={"run_id": 4242, "started_at": NOW - timedelta(hours=2),
                                        "error_message": error})
        assert len(reasons) == 1 and expected in reasons[0], reasons

    def test_a_run_that_failed_elsewhere_still_compared_silver(self):
        verdict, reasons = self._reasons(run={
            "run_id": 4242, "started_at": NOW - timedelta(hours=2),
            "error_message": "1 check(s) raised — reconcile_sms: TimeoutError: x"})
        assert reasons == [] and verdict.clean
        assert verdict.notes and "reconcile_sms" in verdict.notes[0]

    def test_the_landing_mirror_off_is_silence_not_a_verdict(self):
        """`reconcile_silver` returns [] with the landing mirror off.
        Mutation: ignore `landing_on`."""
        _, reasons = self._reasons(landing_on=False)
        assert len(reasons) == 1 and reasons[0].startswith("KS_MIRROR_LANDING is off")


class TestTheGate:
    """Exit 0 only when both halves are clean and the Postgres one was
    asked. Mutations: `Report.clean` ignoring `postgres` (an unproved
    Postgres reads CLEAN), `--backup-only` exiting 0."""

    def test_postgres_not_proved_fails_a_clean_backup(
        self, tmp_path, monkeypatch, capsys,
    ):
        import json

        async def missing_rows(dsn=None):
            return dryrun.judge_postgres(
                _state(findings=[("mirror_missing_rows", "critical", 30)]),
                landing_on=True)

        monkeypatch.setattr(dryrun, "postgres_half", missing_rows)
        backup = _backup(tmp_path)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        report = json.loads(capsys.readouterr().out)
        assert report["backup_clean"] is True and report["clean"] is False
        assert report["postgres"]["findings"] == [
            {"check_name": "mirror_missing_rows", "severity": "critical", "count": 30}]

    def test_the_text_says_what_was_not_proved(self, tmp_path, monkeypatch, capsys):
        async def stale(dsn=None):
            return dryrun.judge_postgres(_state(run=None), landing_on=True)

        monkeypatch.setattr(dryrun, "postgres_half", stale)
        backup = _backup(tmp_path)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 1
        out = capsys.readouterr().out
        assert "NOT PROVED: no mirror_landing run" in out
        assert "DIFFERENCES" in out and "CLEAN —" not in out

    def test_the_backup_alone_is_never_the_gate(self, tmp_path, monkeypatch, capsys):
        async def must_not_be_asked(dsn=None):
            raise AssertionError("--backup-only asked Postgres")

        monkeypatch.setattr(dryrun, "postgres_half", must_not_be_asked)
        backup = _backup(tmp_path)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY,
                            "--backup-only"]) == 3
        out = capsys.readouterr().out
        assert "CLEAN ON THE BACKUP ALONE" in out and "not the flip's gate" in out

    def test_differences_on_the_backup_stay_1_under_backup_only(
        self, tmp_path, capsys,
    ):
        backup = _backup(tmp_path, (
            "INSERT INTO orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id) VALUES (900001, 1, 1, 6, 0, ?, 1)",
            [datetime(2025, 3, 10, 12, tzinfo=KYIV)]))
        assert dryrun.main(["--backup", str(backup), "--today", TODAY,
                            "--backup-only"]) == 1


class TestTheDoor:
    """The real `postgres_half`, with no server: the door's refusals."""

    REAL_POSTGRES_HALF = True

    @pytest.fixture(autouse=True)
    def _no_readonly_login(self, monkeypatch):
        from scripts import utm_reclassify_dryrun as door

        monkeypatch.delenv(door.DSN_ENV, raising=False)
        monkeypatch.delenv(door.PASSWORD_ENV, raising=False)

    def test_no_read_only_login_is_refused_before_the_backup(
        self, tmp_path, monkeypatch, capsys,
    ):
        """And `KS_PG_DSN` — the application's read-write login — is not a
        way in: set, it is still a refusal. Mutation: fall back to it."""
        monkeypatch.setenv("KS_PG_DSN", "postgresql://ks_app:x@127.0.0.1:9/ks")

        def no_backup_read(*a, **k):
            raise AssertionError("the backup was read before the refusal")

        monkeypatch.setattr(dryrun, "measure", no_backup_read)
        assert dryrun.main(["--backup", str(tmp_path / "x.duckdb")]) == 2
        err = capsys.readouterr().err
        assert "refused: no read-only login" in err
        assert "KS_PG_READONLY_DSN" in err and "KS_READONLY_PASSWORD" in err

    def test_an_unreachable_server_is_a_refusal(self, capsys):
        assert dryrun.main(["--backup", "/nonexistent.duckdb", "--dsn",
                            "postgresql://ks_readonly:x@127.0.0.1:9/ks"]) == 2
        assert "Postgres could not be reached" in capsys.readouterr().err

    def test_the_script_never_names_the_application_dsn(self):
        """Read off the module, not its docstring: no string the code
        evaluates is `KS_PG_DSN`."""
        import ast

        tree = ast.parse(Path(dryrun.__file__).read_text())
        docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                      if isinstance(n, (ast.Module, ast.FunctionDef,
                                        ast.AsyncFunctionDef, ast.ClassDef))
                      and n.body and isinstance(n.body[0], ast.Expr)
                      and isinstance(n.body[0].value, ast.Constant)}
        named = [n.value for n in ast.walk(tree)
                 if isinstance(n, ast.Constant) and isinstance(n.value, str)
                 and id(n) not in docstrings and "KS_PG_DSN" in n.value]
        assert named == []


# ─── Where reconcile_silver does not run at all ─────────────────────────────

def _record_writer(path: Path, value: str) -> None:
    """Write the warehouse writer record into a built backup, the way
    `core.warehouse_cutover._write_writer` does — after the build, so no
    refresh of the build's own reads it and moves the module's state."""
    import duckdb

    from core.warehouse_cutover import WRITER_KEY

    conn = duckdb.connect(str(path))
    try:
        conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value) VALUES (?, ?)",
                     [WRITER_KEY, value])
    finally:
        conn.close()


class TestAfterTheWarehouseSwitch:
    """`reconcile_silver` does not run while the warehouse checks stand down —
    Postgres alone derives, or a way back is owed its first validated full
    DuckDB tick — and then the journal's silence about `silver.orders` is not
    a verdict, and DuckDB's `silver_orders` the backup half reads is frozen.
    The backup's recorded writer says which, by `read_writer`'s rules.
    Mutations: skip the check (MW4); refuse any record at all (MW5); copy no
    record out of the backup (MW6)."""

    @pytest.mark.parametrize("record", [
        json.dumps({"writer": "postgres", "since": "2026-11-02T04:00:00+00:00",
                    "resolved": True}),
        "not the JSON this build writes",
    ], ids=["switched", "unreadable"])
    def test_a_backup_recording_postgres_is_refused(self, tmp_path, capsys, record):
        backup = _backup(tmp_path)
        _record_writer(backup, record)
        before = _sha(backup)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 2
        err = capsys.readouterr().err
        assert "records postgres as the warehouse writer" in err, err
        assert "reconcile_silver stands down" in err
        assert _sha(backup) == before

    def test_a_way_back_that_completed_is_answered(self, tmp_path, capsys):
        backup = _backup(tmp_path)
        _record_writer(backup, json.dumps(
            {"writer": "duckdb", "since": "2026-11-09T04:00:00+00:00"}))
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 0
        assert "CLEAN —" in capsys.readouterr().out

    @pytest.mark.parametrize("value,stands_down", [
        (None, False), ("", False), ("duckdb", False), (" DuckDB ", False),
        ("postgres", True), ("Postgres ", True), ("postgress", True),
    ])
    def test_the_switch_in_this_environment(self, value, stands_down):
        """`KS_WRITE_WAREHOUSE` read as web reads it. A value web does not
        understand runs as duckdb — unless a switch came first, when it takes
        the way back and holds the checks down — so it is not trusted.
        Mutation MW1: ignore `warehouse_value`."""
        verdict = dryrun.judge_postgres(_state(), landing_on=True,
                                        warehouse_value=value)
        if stands_down:
            assert len(verdict.reasons) == 1, verdict.reasons
            assert verdict.reasons[0].startswith(
                f"KS_WRITE_WAREHOUSE={value!r} in this environment")
        else:
            assert verdict.clean, verdict.reasons


class TestTheEnvironmentItRunsIn:
    """The real `postgres_half` judges with the environment it runs in —
    web's, run as the web service — and not with constants. A login that
    connects, can write nothing, and reads a clean journal. Mutations:
    `landing_on=True` (MW2), `warehouse_value` not passed (MW3)."""

    REAL_POSTGRES_HALF = True

    @pytest.fixture(autouse=True)
    def _a_clean_read_only_journal(self, monkeypatch):
        from scripts import utm_reclassify_dryrun as door

        class _Conn:
            async def close(self):
                pass

        async def connect(dsn=None):
            return _Conn()

        async def writes_nothing(conn, role=None):
            return []

        async def clean_state(conn):
            return _state()

        monkeypatch.setattr(dryrun, "connect_readonly", connect)
        monkeypatch.setattr(door, "write_privileges", writes_nothing)
        monkeypatch.setattr(dryrun, "read_postgres_state", clean_state)
        monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
        monkeypatch.delenv("KS_WRITE_WAREHOUSE", raising=False)

    def test_as_production_runs_it_is_clean(self):
        verdict = asyncio.run(dryrun.postgres_half())
        assert verdict.clean, verdict.reasons

    @pytest.mark.parametrize("env,value,head", [
        ("KS_MIRROR_LANDING", "0", "KS_MIRROR_LANDING is off"),
        ("KS_WRITE_WAREHOUSE", "postgres", "KS_WRITE_WAREHOUSE='postgres'"),
    ])
    def test_a_switch_that_stands_reconcile_silver_down(
        self, monkeypatch, env, value, head,
    ):
        monkeypatch.setenv(env, value)
        verdict = asyncio.run(dryrun.postgres_half())
        assert len(verdict.reasons) == 1 and verdict.reasons[0].startswith(head), \
            verdict.reasons
