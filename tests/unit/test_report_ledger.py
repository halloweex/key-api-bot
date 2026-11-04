"""Chains 11a/11b: the report ledgers, written to and read from Postgres alone.

The parts that need no database; the real-Postgres half is
`tests/integration/test_report_ledger_pg.py`. OD-16 (a): at least once,
never twice. Until step 14's S14-10 the ledgers had a DuckDB shadow, a gate
that adopted a week only DuckDB held and a `duckdb` branch behind their flags;
their tests went with them. Every guard names, in its docstring, the mutation
it catches.
"""
from __future__ import annotations

import ast
import pathlib
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from core import pg_traffic_ledger_write, pg_weekly_ledger_write, report_ledger

ROOT = pathlib.Path(__file__).resolve().parents[2]
WEEK = date(2026, 9, 21)
LEDGERS = [(report_ledger.WEEKLY, pg_weekly_ledger_write),
           (report_ledger.TRAFFIC, pg_traffic_ledger_write)]


@pytest.fixture
def flags(monkeypatch):
    """No chain's flag; the retry delays zeroed. A flag routes nothing since
    S14-00; it is set only to show that."""
    from core import write_chains

    for c in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(c.WRITE_ENV, raising=False)
    monkeypatch.setattr(report_ledger, "RETRY_DELAYS", (0, 0, 0))
    yield monkeypatch


@pytest.fixture
def no_duckdb(monkeypatch):
    """Any reach for DuckDB's store fails the test: the doors take no store
    and open no connection since S14-10."""
    from core import duckdb_store

    boom = AsyncMock(side_effect=AssertionError("the ledger reached for DuckDB"))
    monkeypatch.setattr(duckdb_store, "get_store", boom)

    def connection(self):
        raise AssertionError("the ledger opened a DuckDB connection")

    monkeypatch.setattr(duckdb_store.DuckDBStore, "connection", connection)
    return boom


class FakePostgres:
    """The chain module's two Postgres calls over a dict, refusing on demand."""

    def __init__(self, monkeypatch, chain):
        self.rows, self.refuse, self.calls = {}, 0, []

        async def mark_sent(week_start, sales_type, revenue, orders, sent_at):
            self.calls.append("mark_sent")
            if self.refuse:
                self.refuse -= 1
                raise ConnectionRefusedError("down")
            self.rows[(week_start, sales_type)] = dict(
                week_start=week_start, sales_type=sales_type, revenue=revenue,
                orders=orders, sent_at=sent_at)

        async def find_sent(week_start, sales_type):
            self.calls.append("find_sent")
            return self.rows.get((week_start, sales_type))

        monkeypatch.setattr(chain, "mark_sent", mark_sent)
        monkeypatch.setattr(chain, "find_sent", find_sent)


# ─── The one door ─────────────────────────────────────────────────────────────

# The reports' DuckDB statements for the ledger (`already_sent`, `mark_sent`,
# `fetch_sent`), deleted in S14-10; their names stay so one coming back to a
# report module, or an import of one, is caught. The report modules are no
# longer allowed SQL over the ledgers at all.
_REPORT_MODULES = {"core.weekly_report", "core.traffic_report"}
_LEDGER_NAMES = {"already_sent", "mark_sent", "fetch_sent"}
_ALLOWED = {"core/report_ledger.py",
            "core/pg_weekly_ledger_write.py", "core/pg_traffic_ledger_write.py"}
_SQL_ALLOWED = _ALLOWED | {
    "core/migrations.py", "core/mirror_reconciliation.py", "core/pg_operational.py",
    "core/pg_chain_invariants.py", "core/snapshot_validation.py",
    "core/duckdb_store.py",
}
_LEDGER_SQL = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE|FROM|INTO)\b[\s\S]*\b"
                         r"(weekly_report_sends|traffic_report_sends)\b", re.IGNORECASE)


def _docstrings(tree) -> set:
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                          ast.AsyncFunctionDef)) and n.body:
            first = n.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                out.add(id(first.value))
    return out


def _offenders_in(tree, rel):
    out = []
    docs = _docstrings(tree)
    for n in ast.walk(tree):
        if (isinstance(n, ast.ImportFrom) and n.module in _REPORT_MODULES
                and rel not in _ALLOWED):
            for alias in n.names:
                if alias.name in _LEDGER_NAMES:
                    out.append(f"{rel}:{n.lineno} imports {n.module}.{alias.name}")
        if (isinstance(n, ast.Attribute) and n.attr in _LEDGER_NAMES
                and isinstance(n.value, ast.Name)
                and n.value.id in {"weekly_report", "traffic_report"}
                and rel not in _ALLOWED):
            out.append(f"{rel}:{n.lineno} reaches {n.value.id}.{n.attr}")
        if (isinstance(n, ast.Constant) and isinstance(n.value, str)
                and id(n) not in docs and rel not in _SQL_ALLOWED
                and _LEDGER_SQL.search(n.value)):
            out.append(f"{rel}:{n.lineno} spells SQL over a ledger")
    return out


class TestTheOneDoor:
    def test_nothing_goes_round_the_router(self):
        """Mutation: `from core.weekly_report import mark_sent` back in the
        job — the delivery would be recorded in DuckDB alone, the gate reads
        Postgres, and the week is sent again tomorrow."""
        offenders = []
        for d in ("core", "web", "bot", "scripts"):
            for path in sorted((ROOT / d).rglob("*.py")):
                rel = str(path.relative_to(ROOT))
                offenders += _offenders_in(ast.parse(path.read_text(encoding="utf-8")), rel)
        assert offenders == []

    def test_the_walk_sees_the_shape(self):
        tree = ast.parse("from core.weekly_report import mark_sent, build_report\n"
                         "x = 'SELECT 1 FROM traffic_report_sends'\n")
        assert len(_offenders_in(tree, "core/scheduler.py")) == 2

    def test_the_reports_ledger_statements_are_gone(self):
        """S14-10 deleted them, and the ledger names only its chain module.
        Mutation: put `mark_sent(conn, ...)` back in `core/weekly_report.py`,
        or `Ledger.report_module` back in `core/report_ledger.py`."""
        import dataclasses

        from core import traffic_report, weekly_report

        for module in (weekly_report, traffic_report):
            assert not {n for n in _LEDGER_NAMES if hasattr(module, n)}, module.__name__
        assert [f.name for f in dataclasses.fields(report_ledger.Ledger)] == ["chain_module"]
        assert not hasattr(report_ledger, "_shadow")


# ─── At least once, never twice ──────────────────────────────────────────────


class TestTheDoor:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger,chain", LEDGERS)
    @pytest.mark.parametrize("value", [None, "duckdb", "postgres", "pgsql-typo"])
    async def test_a_delivery_is_recorded_in_postgres_alone(
            self, flags, no_duckdb, ledger, chain, value):
        """Whatever the flag says. Mutation: route by the flag again, or hand
        the row to DuckDB after the Postgres commit."""
        if value is not None:
            flags.setenv(chain.WRITE_ENV, value)
        pg = FakePostgres(flags, chain)
        before = datetime.now(timezone.utc)
        assert await report_ledger.mark_sent(ledger, WEEK, "retail", 10.004, 3) == "recorded"
        row = pg.rows[(WEEK, "retail")]
        assert row["revenue"] == Decimal("10.00") and row["orders"] == 3
        assert before <= row["sent_at"] <= datetime.now(timezone.utc)
        assert await report_ledger.already_sent(ledger, WEEK, "retail") is True
        assert await report_ledger.already_sent(ledger, WEEK, "b2b") is False
        assert pg.calls == ["mark_sent", "find_sent", "find_sent"]
        assert not no_duckdb.await_count

    @pytest.mark.asyncio
    async def test_a_record_that_fails_is_retried_then_spooled(self, flags, no_duckdb):
        """Mutation: give up after one attempt, or raise instead of spooling —
        the job would report a delivered week as a failure, and tomorrow's
        tick would send it again."""
        chain = pg_weekly_ledger_write
        pg = FakePostgres(flags, chain)
        pg.refuse = 3
        out = await report_ledger.mark_sent(report_ledger.WEEKLY, WEEK, "retail", 10.0, 3)
        assert out == "pending" and pg.calls == ["mark_sent"] * 3
        assert report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")
        assert chain.pending() == {"count": 1, "weeks": ["2026-09-21__retail"],
                                   "unreadable": []}
        assert pg.rows == {}

    @pytest.mark.asyncio
    async def test_a_record_refused_twice_lands_on_the_third_attempt(self, flags, no_duckdb):
        """Mutation: stop retrying at the first refusal — a Postgres restart of
        a few seconds after the delivery would spool every week it overlaps
        and page `report_ledger_pending` for a blip the retry exists to
        absorb. (The fixture zeroes the delays, so it is the attempts that
        are counted, not the ~12 s.) The three attempts carry one `sent_at`."""
        chain = pg_weekly_ledger_write
        pg = FakePostgres(flags, chain)
        stamps = []
        real = chain.mark_sent

        async def mark_sent(week_start, sales_type, revenue, orders, sent_at):
            stamps.append(sent_at)
            await real(week_start, sales_type, revenue, orders, sent_at)

        flags.setattr(chain, "mark_sent", mark_sent)
        pg.refuse = 2
        out = await report_ledger.mark_sent(report_ledger.WEEKLY, WEEK, "retail", 10.0, 3)
        assert out == "recorded" and pg.calls == ["mark_sent"] * 3
        assert not report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")
        assert len(set(stamps)) == 1 and pg.rows[(WEEK, "retail")]["sent_at"] == stamps[0]

    @pytest.mark.asyncio
    async def test_a_spooled_week_is_sent_and_drains_with_its_original_sent_at(
            self, flags, no_duckdb):
        """Mutation: a gate that ignores the spool (the week is sent twice),
        or a drain that stamps now() (the row would say the message went out
        when Postgres came back)."""
        chain = pg_weekly_ledger_write
        pg = FakePostgres(flags, chain)
        pg.refuse = 3
        await report_ledger.mark_sent(report_ledger.WEEKLY, WEEK, "retail", 10.0, 3)
        spooled_at = report_ledger._spooled(chain.CHAIN)[0]["sent_at"]

        # Still down at the next tick: the gate still says sent.
        pg.refuse = 1
        assert await report_ledger.already_sent(report_ledger.WEEKLY, WEEK, "retail")
        assert report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")

        # Back: the gate drains it, then says sent from Postgres.
        assert await report_ledger.already_sent(report_ledger.WEEKLY, WEEK, "retail")
        assert not report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")
        assert pg.rows[(WEEK, "retail")]["sent_at"] == spooled_at
        assert chain.pending() == {"count": 0, "weeks": [], "unreadable": []}

    @pytest.mark.asyncio
    async def test_a_week_postgres_does_not_hold_is_not_sent(self, flags, no_duckdb):
        """No spool and no row: the gate answers False from Postgres alone —
        it no longer asks DuckDB to adopt a week only DuckDB held (skipped in
        production under KS_DUCKDB=off since 2026-10-16, deleted in S14-10).
        Mutation: put the adoption back — `no_duckdb` fails."""
        pg = FakePostgres(flags, pg_traffic_ledger_write)
        assert await report_ledger.already_sent(report_ledger.TRAFFIC, WEEK, "retail") is False
        assert pg.calls == ["find_sent"]

    @pytest.mark.asyncio
    async def test_postgres_unreachable_at_the_gate_refuses(self, flags, no_duckdb):
        """No answer is not "not sent". Mutation: read a refusal as False."""
        chain = pg_weekly_ledger_write
        flags.setattr(chain, "find_sent", AsyncMock(side_effect=ConnectionRefusedError()))
        with pytest.raises(ConnectionRefusedError):
            await report_ledger.already_sent(report_ledger.WEEKLY, WEEK, "retail")

    @pytest.mark.asyncio
    async def test_the_spool_is_written_where_and_as_it_always_was(self, flags):
        """Every image before step 14 drains this directory, so a rollback
        lands what this build spooled: the path, the name and the JSON keys
        are pinned. Mutation: rename a key, the file, or the directory."""
        import json

        went_out = datetime(2026, 9, 28, 6, 30, 1, 5, tzinfo=timezone.utc)
        path = report_ledger.spool(pg_weekly_ledger_write.CHAIN, WEEK, "retail",
                                   Decimal("1234.57"), 7, went_out)
        assert path == (report_ledger.SPOOL_DIR / "pg_weekly_ledger_write"
                        / "2026-09-21__retail.json")
        assert report_ledger.SPOOL_DIR.name == "report-ledger-pending"
        assert json.loads(path.read_text(encoding="utf-8")) == {
            "week_start": "2026-09-21", "sales_type": "retail", "revenue": "1234.57",
            "orders": 7, "sent_at": "2026-09-28T06:30:01.000005+00:00"}

    def test_the_spool_dir_is_the_data_dirs(self):
        """`./data` is the directory both containers mount and the web
        container can write; the suite moves it to a tmp dir, so the module's
        own assignment is read. Mutation: move the spool anywhere else."""
        tree = ast.parse((ROOT / "core" / "report_ledger.py").read_text(encoding="utf-8"))
        (value,) = [ast.unparse(n.value) for n in tree.body
                    if isinstance(n, ast.AnnAssign) and getattr(n.target, "id", "") == "SPOOL_DIR"]
        assert value == "DATA_DIR / 'report-ledger-pending'"

    @pytest.mark.asyncio
    async def test_a_value_either_store_reads_two_ways_is_refused_before_the_latch(
            self, flags):
        chain = pg_weekly_ledger_write
        pool = AsyncMock(side_effect=AssertionError("asked for a pool"))
        flags.setattr("core.pg.get_pool", pool)
        for bad in (dict(revenue=10.0), dict(revenue=Decimal("10.005")),
                    dict(sent_at=datetime(2026, 9, 28, 9, 30)), dict(orders=True)):
            kw = dict(week_start=WEEK, sales_type="retail", revenue=Decimal("10.00"),
                      orders=3, sent_at=datetime.now(timezone.utc))
            kw.update(bad)
            with pytest.raises(ValueError):
                await chain.mark_sent(**kw)
        assert not pool.await_count
        assert not chain.chain_latch.marker_path(chain.CHAIN).exists()


@pytest_asyncio.fixture
async def warehouse(monkeypatch):
    """Postgres' Gold and Silver, emptied, for the report's reads — Postgres'
    alone since step 14's S14-07 — while the ledger under test is faked."""
    import os

    from tests.integration.pg_tab_seed import tab_in_postgres

    if not os.getenv("KS_PG_DSN"):
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")

    async def _nothing(conn, pool):
        pass

    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _nothing) as pool:
        yield pool


class TestTheJobSendsOnce:
    """Through the job the scheduler runs, with the sales report's own wiring."""

    @pytest.mark.asyncio
    async def test_postgres_refusing_after_delivery_never_sends_twice(
            self, flags, warehouse, tmp_path, monkeypatch):
        from tests.unit.test_weekly_report import TestSchedulerJob, _store

        chain = pg_weekly_ledger_write
        flags.setenv(chain.WRITE_ENV, "postgres")
        pg = FakePostgres(flags, chain)
        pg.refuse = 3
        store = await _store(tmp_path)
        try:
            job = TestSchedulerJob()
            week_start = await job._seed(warehouse, complete=True)
            sent = []
            scheduler = job._wire(monkeypatch, store, sent, tmp_path)
            first = await scheduler._run_weekly_report()
            assert first["sent"] is True and first["ledger"] == "pending"
            second = await scheduler._run_weekly_report()
            assert second == {"sent": False, "reason": "already_sent",
                              "week": week_start.isoformat()}
            assert len(sent) == 1
            assert (week_start, "retail") in pg.rows        # drained by the gate
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_traffic_floor_is_asked_before_the_ledger(
            self, flags, warehouse, tmp_path, monkeypatch):
        """A week before `KS_TRAFFIC_REPORT_FIRST_WEEK` is skipped without
        asking the ledger, so a Postgres that is down cannot turn "not this
        week" into a failed job. Mutation: move the floor after the gate —
        the job then raises on the refusal, every day, for a week that was
        never going to be sent."""
        from core.traffic_report import FIRST_WEEK_ENV
        from tests.unit.test_traffic_report import (
            TestTheScheduledJob, _job_store, _seed_gold,
        )

        chain = pg_traffic_ledger_write
        flags.setenv(chain.WRITE_ENV, "postgres")
        flags.setattr(chain, "find_sent", AsyncMock(side_effect=ConnectionRefusedError()))
        store = await _job_store(tmp_path)
        try:
            week_start = await _seed_gold(store)
            monkeypatch.setenv(FIRST_WEEK_ENV, (week_start + timedelta(days=7)).isoformat())
            job = TestTheScheduledJob()
            scheduler = job._wire(monkeypatch, store, tmp_path)
            sent = job._capture(monkeypatch)
            result = await scheduler._run_traffic_report()
            assert result["reason"] == "before_first_week", result
            assert sent["rich"] == [] and sent["text"] == []
            assert not chain.find_sent.await_count
        finally:
            await store.close()


# ─── The watch and the canary ────────────────────────────────────────────────


class TestTheStandingWatch:
    def test_the_week_is_due_from_wednesday(self):
        from core.pg_chain_invariants import ledger_week_due

        monday, tuesday, wednesday = date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)
        assert ledger_week_due(monday, None) == (date(2026, 9, 21), False)
        assert ledger_week_due(tuesday, None) == (date(2026, 9, 21), False)
        assert ledger_week_due(wednesday, None) == (date(2026, 9, 21), True)
        assert ledger_week_due(wednesday, date(2026, 9, 28)) == (date(2026, 9, 21), False)

    def test_a_missing_week_warns_and_a_null_sent_at_pages(self):
        """Mutation: drop either rule."""
        from core import pg_chain_invariants as inv

        led = inv.Ledger(nulls=inv.Nulls(table="app.weekly_report_sends",
                                         counts={"sent_at": 1}),
                         latched_at=datetime(2026, 9, 25, tzinfo=timezone.utc),
                         week_start=WEEK, due=True, has_week=False)
        facts = inv.Facts(watched=(pg_weekly_ledger_write.CHAIN,),
                          now=datetime(2026, 9, 30, tzinfo=timezone.utc),
                          weekly_ledger=led)
        names = {i.check_name: i for i in inv.check_chain_invariants(facts)}
        assert set(names) == {inv.REPORT_WEEK_MISSING, inv.COLUMN_NULL}
        quiet = inv.Facts(watched=facts.watched, now=facts.now,
                          weekly_ledger=inv.Ledger(nulls=inv.Nulls(
                              table="app.weekly_report_sends", counts={"sent_at": 0}),
                              week_start=WEEK, due=True, has_week=True))
        assert inv.check_chain_invariants(quiet) == []

    def test_each_chain_names_its_report_and_floor(self):
        from core.scheduler import WEEKLY_REPORT_SALES_TYPE
        from core.traffic_report import TRAFFIC_SALES_TYPE

        assert pg_weekly_ledger_write.report_sales_type() == WEEKLY_REPORT_SALES_TYPE
        assert pg_traffic_ledger_write.report_sales_type() == TRAFFIC_SALES_TYPE
        assert pg_weekly_ledger_write.first_week() is None


class TestTheCanary:
    def test_a_spooled_week_warns_and_names_itself(self):
        """Mutation: drop `check_report_ledger_pending` from the probe."""
        from bot import canary

        payload = {"write_chains": {
            "pg_weekly_ledger_write": {"pending": {"count": 1, "weeks": ["2026-09-21__retail"]}},
            "pg_traffic_ledger_write": {"pending": {"count": 0, "weeks": []}}}}
        ((key, message),) = canary.check_report_ledger_pending(payload)
        assert key == "report_ledger_pending" and "2026-09-21__retail" in message
        assert canary.check_report_ledger_pending({"write_chains": {}}) == []
        assert canary.check_report_ledger_pending({}) == []

    def test_the_health_block_carries_the_spool(self, flags):
        """`_pending_entries` publishes `pending` for a chain that declares
        it, and nothing else since S14-10 (`shadow_failures` and
        `shadow_stood_down` went with the shadow)."""
        from web.routes.api.health import _pending_entries

        report_ledger.spool(pg_traffic_ledger_write.CHAIN, WEEK, "retail",
                            Decimal("1.00"), 1, datetime.now(timezone.utc))
        block = {"pg_traffic_ledger_write": {}, "pg_weekly_ledger_write": {},
                 "pg_dq_journal_write": {}}
        _pending_entries(block)
        assert block["pg_traffic_ledger_write"]["pending"]["count"] == 1
        assert block["pg_weekly_ledger_write"]["pending"]["count"] == 0
        assert block["pg_dq_journal_write"] == {}

    @pytest.mark.asyncio
    async def test_the_published_block_carries_it(self, flags, monkeypatch):
        """Through `_write_chains_block`, what `/api/health` actually publishes.
        Mutation: drop the `_pending_entries(block)` call — the test above
        would still pass, calling the helper itself, and the canary would
        never see a spooled week."""
        from web.routes.api import health

        monkeypatch.setattr(health, "_inventory_sync_step", AsyncMock(return_value=None))
        report_ledger.spool(pg_weekly_ledger_write.CHAIN, WEEK, "retail",
                            Decimal("1.00"), 1, datetime.now(timezone.utc))
        block = await health._write_chains_block()
        assert block["pg_weekly_ledger_write"]["pending"]["count"] == 1
        # The registry's `shadow` went in S14-11 with every flag field, the
        # two counters beside it with the shadow in S14-10: `pending` alone.
        assert set(block["pg_weekly_ledger_write"]) == {"pending"}

    @pytest.mark.asyncio
    async def test_the_probe_warns_on_it(self):
        """Through `run_canary`, the probe the bot runs every 15 min, on an
        otherwise healthy payload. Mutation: drop `check_report_ledger_pending`
        from the probe — the function above would still answer, and nobody
        would be told that a delivered week is recorded only on local disk."""
        import httpx
        from unittest.mock import patch

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload.setdefault("write_chains", {})["pg_weekly_ledger_write"] = {
            "env": "postgres", "mode": "postgres", "error": None, "latched": True,
            "latched_at": "2026-09-28T06:30:00+00:00", "mismatch": False,
            "unmet_precondition": None, "shadow": True,
            "pending": {"count": 1, "weeks": ["2026-09-21__retail"]}}
        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(lambda request: httpx.Response(200, json=payload)) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert "report_ledger_pending" in result.failure_keys
        assert result.severity == "warn"
        assert "2026-09-21__retail" in canary.format_alert(result, DASHBOARD)


# ─── A spool outlives the flag that wrote it ─────────────────────────────────


class TestASpoolOutlivesItsFlag:
    """A spooled row is a delivery that happened, whatever any flag says. The
    review of chains 11a/11b reproduced two ordinary ways back walking past
    it — the flag put back to `duckdb` after a record that never latched, and
    a copy-back — and the week went out a second time. Neither way back
    exists since step 14 (S14-00, S14-04), and S14-10 deleted the `duckdb`
    gate they reached; these hold the spool to counting under every flag."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [None, "duckdb", "pgsql-typo"])
    async def test_a_spooled_week_counts_as_sent_while_postgres_refuses(
            self, flags, no_duckdb, value):
        """Mutation: count the week only once it has landed — a Postgres that
        refused the drain would turn a delivered week into a second send."""
        chain = pg_weekly_ledger_write
        if value is not None:
            flags.setenv(chain.WRITE_ENV, value)
        pg = FakePostgres(flags, chain)
        pg.refuse = 1
        report_ledger.spool(chain.CHAIN, WEEK, "retail", Decimal("5.00"), 1,
                            datetime.now(timezone.utc))
        assert await report_ledger.already_sent(report_ledger.WEEKLY, WEEK, "retail")
        assert report_ledger.is_spooled(chain.CHAIN, WEEK, "retail")
        assert pg.calls == ["mark_sent"]                 # the drain, refused

    @pytest.mark.asyncio
    async def test_an_unreadable_file_counts_by_its_name_and_stays(self, flags, no_duckdb):
        """No drain lands a file that will not parse, and none deletes it."""
        chain = pg_traffic_ledger_write
        pg = FakePostgres(flags, chain)
        path = report_ledger._spool_path(chain.CHAIN, WEEK, "retail")
        path.parent.mkdir(parents=True)
        path.write_text("{not json", encoding="utf-8")
        assert await report_ledger.already_sent(report_ledger.TRAFFIC, WEEK, "retail")
        assert path.exists() and pg.rows == {}
        assert chain.pending() == {"count": 1, "weeks": ["2026-09-21__retail"],
                                   "unreadable": ["2026-09-21__retail"]}

    @pytest.mark.asyncio
    async def test_through_the_job_the_week_goes_out_once_whatever_the_flag(
            self, flags, warehouse, tmp_path, monkeypatch):
        """End to end with the sales report's own wiring: delivered, the
        record refused three times and spooled, the flag put back to
        `duckdb`, then a tick with Postgres still refusing and one with it
        back. Mutation: drop the spool branch from the gate — `sent` holds
        two."""
        from tests.unit.test_weekly_report import TestSchedulerJob, _store

        chain = pg_weekly_ledger_write
        pg = FakePostgres(flags, chain)
        pg.refuse = 3
        store = await _store(tmp_path)
        try:
            job = TestSchedulerJob()
            week_start = await job._seed(warehouse, complete=True)
            sent = []
            scheduler = job._wire(monkeypatch, store, sent, tmp_path)
            first = await scheduler._run_weekly_report()
            assert first["sent"] is True and first["ledger"] == "pending"
            flags.setenv(chain.WRITE_ENV, "duckdb")
            pg.refuse = 1
            quiet = {"sent": False, "reason": "already_sent",
                     "week": week_start.isoformat()}
            assert await scheduler._run_weekly_report() == quiet   # spooled
            assert report_ledger.is_spooled(chain.CHAIN, week_start, "retail")
            assert await scheduler._run_weekly_report() == quiet   # drained
            assert not report_ledger.is_spooled(chain.CHAIN, week_start, "retail")
            assert await scheduler._run_weekly_report() == quiet   # Postgres's row
            assert len(sent) == 1
            assert (week_start, "retail") in pg.rows
        finally:
            await store.close()
