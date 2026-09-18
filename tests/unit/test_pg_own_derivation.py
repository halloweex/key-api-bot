"""Chain 2, steps 3–6 without a database: the mode, the schedule, the wiring.

The behaviour against a real Postgres is in
`tests/integration/test_pg_own_derivation.py`. What is here is what a real
server cannot tell you: that the mode is never read on the write path, that
every writer of a derivation source raises the signal, and that the watchdog
judges what web declares.
"""
from __future__ import annotations

import ast
import asyncio
import html
import inspect
import pathlib
import re
import textwrap
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_derivation

REPO = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def mode(monkeypatch):
    """Set the cached mode the way the scheduler does, and put it back."""
    before = (pg_derivation._mode, pg_derivation._mode_error)

    def set_mode(value):
        if value is None:
            monkeypatch.delenv(pg_derivation.ENV, raising=False)
        else:
            monkeypatch.setenv(pg_derivation.ENV, value)
        return pg_derivation.configure_mode()

    yield set_mode
    pg_derivation._mode, pg_derivation._mode_error = before


class TestTheMode:
    def test_default_is_piggyback(self, mode):
        assert mode(None) == "piggyback" and not pg_derivation.owns()

    def test_own(self, mode):
        assert mode("own") == "own" and pg_derivation.owns()

    def test_an_unknown_value_falls_back_and_says_so(self, mode):
        """Not a raise: refusing to start would take the dashboard down over a
        derivation setting. Today's behaviour, and the reason on record."""
        assert mode("owned") == "piggyback"
        assert not pg_derivation.owns()
        assert "owned" in pg_derivation.mode_error()

    def test_the_write_path_never_reads_the_environment(self, mode, monkeypatch):
        mode("own")
        monkeypatch.setenv(pg_derivation.ENV, "garbage")
        assert pg_derivation.owns() is True
        for fn in (pg_derivation.owns, pg_derivation.mark_if_owned):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            reads = [n for n in ast.walk(tree)
                     if isinstance(n, ast.Attribute) and n.attr in ("getenv", "environ")]
            assert reads == [], fn.__name__


class TestTheSchedule:
    NOW = datetime(2026, 9, 17, 3, 0, tzinfo=timezone.utc)
    FLOOR = timedelta(minutes=10)

    def due(self, **kw):
        args = dict(requested=5, built=5, built_at=self.NOW - timedelta(minutes=5),
                    last_attempt=self.NOW - timedelta(minutes=5), now=self.NOW,
                    floor=self.FLOOR, first_tick=False)
        args.update(kw)
        return pg_derivation.due(**args)

    def test_nothing_owed_is_quiet(self):
        assert self.due() == (False, "nothing owed")

    def test_owed_runs_past_the_floor(self):
        assert self.due(requested=6, last_attempt=self.NOW - timedelta(minutes=11)) == (True, "signal")

    def test_owed_waits_inside_the_floor(self):
        assert self.due(requested=6) == (False, "floor")

    def test_the_first_tick_of_a_process_runs(self):
        assert self.due(first_tick=True, last_attempt=self.NOW - timedelta(hours=1)) == (True, "first_tick")

    def test_a_quiet_hour_still_runs(self):
        assert self.due(built_at=self.NOW - timedelta(minutes=61),
                        last_attempt=self.NOW - timedelta(minutes=61)) == (True, "heartbeat")

    def test_never_built_runs(self):
        assert self.due(built_at=None, last_attempt=None) == (True, "heartbeat")

    def test_force_skips_the_floor(self):
        assert self.due(force=True) == (True, "manual")


def _rendered_strings(tree: ast.AST, names: dict) -> list:
    """Every string in a module, f-strings rendered with its module constants."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.append(node.value)
        elif isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    parts.append(str(v.value))
                elif isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name):
                    parts.append(names.get(v.value.id, "{}"))
                else:
                    parts.append("{}")
            out.append("".join(parts))
    return out


class TestEveryWriterOfASourceRaisesTheSignal:
    WRITE = re.compile(
        r"(INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE)\s+("
        + "|".join(re.escape(t) for t in pg_derivation.SOURCE_TABLES) + r")\b")

    def _writers(self):
        found = set()
        for root in ("core", "web", "bot", "scripts", "deploy"):
            for path in (REPO / root).rglob("*.py"):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                names = {t.id: n.value.value for n in tree.body
                         if isinstance(n, ast.Assign) and isinstance(n.value, ast.Constant)
                         and isinstance(n.value.value, str)
                         for t in n.targets if isinstance(t, ast.Name)}
                if any(self.WRITE.search(s) for s in _rendered_strings(tree, names)):
                    found.add(".".join(path.relative_to(REPO).with_suffix("").parts))
        return found

    def test_the_walk_finds_exactly_the_registered_modules(self):
        """Walks the code rather than trusting the list: a fourth writer of
        `bronze.orders` that forgets the mark is a derivation that silently
        stops seeing its changes."""
        found = self._writers()
        assert found, "the walk found no writer at all — it is not looking"
        assert found == set(pg_derivation.MARK_SITES), found

    @pytest.mark.parametrize("module,function", sorted(pg_derivation.MARK_SITES.items()))
    def test_each_registered_writer_marks_inside_its_transaction(self, module, function):
        import importlib

        fn = getattr(importlib.import_module(module), function)
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        transactions = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncWith)
                        and any(isinstance(i.context_expr, ast.Call)
                                and isinstance(i.context_expr.func, ast.Attribute)
                                and i.context_expr.func.attr == "transaction"
                                for i in n.items)]
        assert transactions, f"{module}.{function} has no transaction"
        marks = [n for t in transactions for n in ast.walk(t)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                 and n.func.id == "mark_if_owned"]
        assert marks, f"{module}.{function} does not raise the signal inside its transaction"


class TestTheDuckDBTickUnderOwn:
    def _run(self):
        from core.scheduler import BackgroundScheduler

        silver, gold = AsyncMock(return_value={"rows": 1}), AsyncMock(return_value={"rows": 1, "cells": 1})
        profile, ship = AsyncMock(return_value={"rows": 1}), AsyncMock(return_value={"rows": 1})
        with patch("core.pg_vitrina.rebuild_customer_profile", profile), \
             patch("core.pg_order_utm.ship_order_utm", ship), \
             patch("core.duckdb_store.get_store", AsyncMock(return_value=object())):
            asyncio.run(BackgroundScheduler()._rebuild_pg_layers(silver, gold))
        return silver, gold, profile, ship

    def test_under_own_it_only_ships_utm(self, mode):
        mode("own")
        silver, gold, profile, ship = self._run()
        assert not silver.await_count and not gold.await_count and not profile.await_count
        assert ship.await_count == 1

    def test_under_piggyback_it_is_unchanged(self, mode):
        mode(None)
        silver, gold, profile, ship = self._run()
        assert (silver.await_count, gold.await_count, profile.await_count, ship.await_count) == (1, 1, 1, 1)


class TestTheJobIsRegisteredOnlyUnderOwn:
    def _jobs(self):
        from core.scheduler import BackgroundScheduler

        added = {}

        class FakeScheduler:
            def add_job(self, func, **kwargs):
                added[kwargs.get("id")] = kwargs

            def get_job(self, job_id):
                return None

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler()
        asyncio.run(scheduler._register_jobs())
        return added

    def test_own(self, mode):
        mode("own")
        assert "pg_warehouse_derive" in self._jobs()

    def test_piggyback(self, mode):
        mode(None)
        assert "pg_warehouse_derive" not in self._jobs()


class TestTheWatchdogJudgesWhatWebDeclares:
    def _payload(self, **derived):
        entry = {"age_seconds": 60, "failures_since_ok": 0, "failing": False}
        block = {"bronze.orders": dict(entry)}
        block.update(derived)
        return {"mirrors": block}

    def test_a_declared_table_past_its_limit_is_stale(self):
        from bot.canary import check_mirror_freshness

        failures, ages = check_mirror_freshness(self._payload(**{
            "silver.orders": {"age_seconds": 6000, "failures_since_ok": 0,
                              "failing": False, "max_age_s": 5400}}))
        assert ("mirror_stale:silver.orders" in [k for k, _ in failures])
        assert ages["silver.orders"] == 6000

    def test_a_declared_table_inside_its_limit_is_quiet(self):
        from bot.canary import check_mirror_freshness

        failures, _ = check_mirror_freshness(self._payload(**{
            "gold.daily_revenue": {"age_seconds": 600, "failures_since_ok": 0,
                                   "failing": False, "max_age_s": 5400}}))
        assert failures == []

    def test_a_table_without_a_declared_limit_is_not_judged(self):
        from bot.canary import check_mirror_freshness

        failures, ages = check_mirror_freshness(self._payload(**{
            "silver.orders": {"age_seconds": 999999, "failures_since_ok": 0, "failing": False}}))
        assert failures == [] and "silver.orders" not in ages


class TestHealthDeclaresTheDerivedTablesUnderOwn:
    def _mirrors(self):
        from web.routes.api import health

        async def fake_fetch(pool, tables=("bronze.orders",), now=None):
            return {t: {"last_ok_at": None, "age_seconds": 10,
                        "failures_since_ok": 0, "failing": False} for t in tables}

        health._mirror_cache["data"] = None
        with patch("core.mirror_reconciliation.fetch_mirror_freshness", fake_fetch), \
             patch("core.pg.get_pool", AsyncMock(return_value=object())):
            try:
                return asyncio.run(health._mirror_freshness())
            finally:
                health._mirror_cache["data"] = None

    def test_own(self, mode):
        mode("own")
        data = self._mirrors()
        assert set(data) == {"bronze.orders", "silver.orders", "gold.daily_revenue"}
        assert data["silver.orders"]["max_age_s"] == pg_derivation.DERIVED_MAX_AGE_S
        assert "max_age_s" not in data["bronze.orders"]

    def test_piggyback(self, mode):
        mode(None)
        assert set(self._mirrors()) == {"bronze.orders"}


class TestATypoIsVisible:
    """Found reviewing #210: an unknown KS_PG_DERIVE fell back to piggyback and
    left one log line — while the comment promised /api/health would say so."""

    def test_health_publishes_the_mode_and_the_error(self, mode):
        from web.routes.api.health import _derivation_mode

        mode("owned")
        block = _derivation_mode()
        assert block["mode"] == "piggyback" and "owned" in block["error"]
        mode("own")
        assert _derivation_mode() == {"mode": "own", "error": None}

    def test_the_canary_pages_on_the_error(self):
        from bot.canary import check_derivation_mode

        assert [k for k, _ in check_derivation_mode(
            {"derivation": {"mode": "piggyback", "error": "KS_PG_DERIVE='owned' ..."}})] == [
            "derivation_mode_invalid"]

    def test_the_canary_is_quiet_on_a_valid_mode_or_an_older_web(self):
        from bot.canary import check_derivation_mode

        assert check_derivation_mode({"derivation": {"mode": "own", "error": None}}) == []
        assert check_derivation_mode({}) == []


# ─── DN-05a: every failure path of the own derivation is recorded ─────────────

class _Pool:
    """A pool whose connection is never really used: every read and write the
    job makes through it is patched out below."""

    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _verdict(passed: bool = True) -> dict:
    return {"passed": passed, "row_count_match": passed, "checksum_match": True,
            "cells_match": True, "rollup_match": True, "partition_exhaustive": True,
            "missing_cells": 0, "extra_cells": 0, "rollup_mismatch_cells": 0,
            "unknown_sales_types": []}


def _bounded(coro, timeout=10):
    """Every run takes `_heavy_job_lock` and PG_LAYER_LOCK; a mutation that
    left one held must fail the test, not hang the suite."""
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


@pytest.fixture
def job(mode, monkeypatch):
    """A scheduler under own whose process has already derived once, so a tick
    with nothing owed is quiet — a web container past its first minute. Every
    piece of process state the job keeps is put back afterwards."""
    from core.scheduler import BackgroundScheduler

    mode("own")
    monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)   # the 600 s floor
    before = (BackgroundScheduler._pg_derive_ran, BackgroundScheduler._pg_derive_started,
              pg_derivation._signal_failures)
    BackgroundScheduler._pg_derive_ran = True
    BackgroundScheduler._pg_derive_started = None
    pg_derivation._signal_failures = 0
    yield BackgroundScheduler()
    (BackgroundScheduler._pg_derive_ran, BackgroundScheduler._pg_derive_started,
     pg_derivation._signal_failures) = before


def _alert_keys(raise_alert):
    return [c.kwargs["conditions"][0] for c in raise_alert.await_args_list]


class TestASignalThatCannotBeRead:
    """A tick that could not read the signal returned before the derivation and
    wrote nothing anywhere; only the derived tables' 90-minute age noticed.
    SchemaVersionError across a deploy window is exactly this case."""

    def _ticks(self, job, outcomes):
        """One tick per outcome: an exception is a read that fails, None a read
        that finds nothing owed. Returns the alert and resolve mocks."""
        now = datetime.now(timezone.utc)
        reads = [o if isinstance(o, Exception) else (5, 5, now) for o in outcomes]
        raise_alert, resolve = AsyncMock(return_value=1), AsyncMock(return_value=0)
        with patch("core.pg.get_pool", AsyncMock(return_value=_Pool())), \
             patch("core.pg.require_revision", AsyncMock()), \
             patch.object(pg_derivation, "read_owed", AsyncMock(side_effect=reads)), \
             patch.object(pg_derivation, "last_attempt_at", AsyncMock(return_value=now)), \
             patch("core.alerting.raise_alert", raise_alert), \
             patch("core.alerting.resolve_group", resolve):
            self.results = [_bounded(job._run_pg_derivation()) for _ in outcomes]
        return raise_alert, resolve

    @staticmethod
    def _failing(n):
        return [ConnectionError("gone")] * n

    def test_five_in_a_row_page_exactly_once(self, job):
        raise_alert, _ = self._ticks(job, self._failing(5))
        assert _alert_keys(raise_alert) == ["warehouse_pg:signal_unreadable"]
        assert raise_alert.await_args.kwargs["group"] == "warehouse_pg"
        assert "5 ticks in a row" in raise_alert.await_args.args[0]
        assert pg_derivation.signal_unreadable_ticks() == 5
        assert all(r["skipped"] for r in self.results)

    def test_four_page_nobody(self, job):
        raise_alert, _ = self._ticks(job, self._failing(4))
        assert raise_alert.await_count == 0
        assert pg_derivation.signal_unreadable_ticks() == 4

    def test_a_read_resets_the_count(self, job):
        raise_alert, _ = self._ticks(job, self._failing(4) + [None] + self._failing(4))
        assert raise_alert.await_count == 0
        assert pg_derivation.signal_unreadable_ticks() == 4

    def test_past_five_every_tick_is_handed_to_the_gate(self, job):
        """The gate, not the emitter, decides what reaches a phone — a loud half
        hour, then a daily reminder — and a send the transport dropped is
        retried by the next tick instead of lost."""
        raise_alert, _ = self._ticks(job, self._failing(7))
        assert _alert_keys(raise_alert) == ["warehouse_pg:signal_unreadable"] * 3

    def test_readable_again_resolves_only_that_page(self, job):
        """Nothing owed, so no run resolves the group; without this the page
        would stand until the heartbeat's run, up to an hour later."""
        _a, resolve = self._ticks(job, self._failing(5) + [None])
        assert resolve.await_count == 1
        assert resolve.await_args.args == ("warehouse_pg",)
        assert resolve.await_args.kwargs == {"only_prefix": "warehouse_pg:signal_unreadable"}
        assert pg_derivation.signal_unreadable_ticks() == 0

    def test_a_read_after_a_short_blip_resolves_nothing(self, job):
        _a, resolve = self._ticks(job, self._failing(3) + [None])
        assert resolve.await_count == 0

    def test_a_schema_version_error_counts_like_any_other(self, job):
        """The deploy-window case: `require_revision` raising, not the read."""
        from core.pg import SchemaVersionError

        raise_alert = AsyncMock(return_value=1)
        with patch("core.pg.get_pool", AsyncMock(return_value=_Pool())), \
             patch("core.pg.require_revision",
                   AsyncMock(side_effect=SchemaVersionError("0033 is not 0034"))), \
             patch("core.alerting.raise_alert", raise_alert):
            for _ in range(pg_derivation.SIGNAL_UNREADABLE_TICKS):
                _bounded(job._run_pg_derivation())
        assert _alert_keys(raise_alert) == ["warehouse_pg:signal_unreadable"]
        assert "SchemaVersionError" in raise_alert.await_args.args[0]

    def _page(self, job, error):
        raise_alert = AsyncMock(return_value=1)
        with patch("core.pg.get_pool", AsyncMock(return_value=_Pool())), \
             patch("core.pg.require_revision", AsyncMock(side_effect=error)), \
             patch("core.alerting.raise_alert", raise_alert):
            for _ in range(pg_derivation.SIGNAL_UNREADABLE_TICKS):
                _bounded(job._run_pg_derivation())
        assert raise_alert.await_count == 1
        return raise_alert.await_args.args[0].splitlines()

    def test_the_page_carries_the_errors_words(self, job):
        """The class alone was read as a diagnosis, and `current_revision`
        turns a timed-out read into "never been migrated" — on a database that
        is migrated. What the error said is what tells the two apart."""
        from core.pg import SchemaVersionError

        headline, detail, lever = self._page(job, SchemaVersionError(
            "meta.alembic_version is absent or empty: this database has never "
            "been migrated. Run `alembic upgrade head`."))
        assert headline.startswith("Postgres derivation: signal unreadable — 5 ticks")
        assert detail == ("SchemaVersionError: meta.alembic_version is absent or empty: "
                          "this database has never been migrated. Run `alembic upgrade head`.")
        assert lever.startswith("→")

    def test_the_words_are_one_line_cut_and_escaped(self, job):
        """Escaped after the cut, so the cut cannot split an entity; one line,
        so an asyncpg traceback cannot become the page."""
        said = "<b>\n" + "x" * 300
        _h, detail, _l = self._page(job, ConnectionError(said))
        assert "<b>" not in detail
        assert html.unescape(detail) == "ConnectionError: " + ("<b> " + "x" * 300)[:120] + "…"


class TestAReadThatNeverReturns:
    """A Postgres frozen rather than gone: the read neither answers nor fails,
    and under `max_instances=1` the tick holding it skipped every tick behind
    it, so the count never moved. The read is cut off at the bound and counts
    like any other failure."""

    def _ticks(self, job, monkeypatch, read_owed, n=pg_derivation.SIGNAL_UNREADABLE_TICKS):
        monkeypatch.setenv("KS_PG_TIMEOUT", "0.05")          # a bound of 0.1 s
        raise_alert = AsyncMock(return_value=1)
        with patch("core.pg.get_pool", AsyncMock(return_value=_Pool())), \
             patch("core.pg.require_revision", AsyncMock()), \
             patch.object(pg_derivation, "read_owed", read_owed), \
             patch.object(pg_derivation, "last_attempt_at", AsyncMock(return_value=None)), \
             patch("core.alerting.raise_alert", raise_alert):
            results = [_bounded(job._run_pg_derivation(), timeout=3) for _ in range(n)]
        return results, raise_alert

    def test_the_bound_is_two_command_timeouts(self, monkeypatch):
        monkeypatch.setenv("KS_PG_TIMEOUT", "30")
        assert pg_derivation.signal_read_bound_s() == 60
        monkeypatch.delenv("KS_PG_TIMEOUT")
        assert pg_derivation.signal_read_bound_s() == 60

    def test_it_counts_and_pages(self, job, monkeypatch):
        async def never(conn):
            await asyncio.Event().wait()

        results, raise_alert = self._ticks(job, monkeypatch, never)
        assert all(r == {"skipped": True, "reason": "signal unreadable: the signal read "
                                                   "did not return within 0.1 s"}
                   for r in results), results
        assert _alert_keys(raise_alert) == ["warehouse_pg:signal_unreadable"]
        assert "TimeoutError: the signal read did not return" in raise_alert.await_args.args[0]

    def test_even_when_its_cancellation_hangs_too(self, job, monkeypatch):
        """asyncpg's shape, measured on a paused container: cancelled, the read
        waits in the pool's release on a cancel request the server never
        answers. A bare `wait_for` waits for that as well; the tick must not.

        The unwinding hangs for five seconds, not for ever: far past the
        0.1 s bound, and short enough that a mutation which waits for it still
        fails `_bounded`'s three seconds instead of hanging the suite. A
        second cancel — asyncio.run's own, at the end — ends it at once."""
        async def stuck(conn):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(5)           # the release, on a frozen server
                raise

        results, raise_alert = self._ticks(job, monkeypatch, stuck)
        assert all(r["skipped"] for r in results)
        assert pg_derivation.signal_unreadable_ticks() == 5
        assert raise_alert.await_count == 1

    def test_a_timeout_of_the_reads_own_is_not_relabelled(self, job, monkeypatch):
        """A query past KS_PG_TIMEOUT raises TimeoutError inside the read; that
        is its own failure and keeps its own words."""
        results, _a = self._ticks(
            job, monkeypatch, AsyncMock(side_effect=TimeoutError("the query's own")), n=1)
        assert results == [{"skipped": True, "reason": "signal unreadable: the query's own"}]


class TestHealthPublishesTheCount:
    def test_under_own_it_is_the_process_count(self, job):
        from web.routes.api.health import _signal_reads

        assert _signal_reads() == {"signal_unreadable_ticks": 0}
        pg_derivation.note_signal_unreadable()
        pg_derivation.note_signal_unreadable()
        assert _signal_reads() == {"signal_unreadable_ticks": 2}

    def test_under_piggyback_it_is_null(self, mode):
        """The counting job is not registered there; a zero would claim a watch
        nobody keeps."""
        from web.routes.api.health import _signal_reads

        mode(None)
        assert _signal_reads() == {"signal_unreadable_ticks": None}

    def test_the_block_carries_it(self):
        tree = ast.parse((REPO / "web/routes/api/health.py").read_text(encoding="utf-8"))
        block = next(n for n in ast.walk(tree)
                     if isinstance(n, ast.AsyncFunctionDef) and n.name == "_derivation_block")
        called = {n.func.id for n in ast.walk(block)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert "_signal_reads" in called


_STEPS = ("silver", "gold", "profile", "validate", "complete", "record_run")


def _derive(job, *, reads=((3, 2, None),), journal_attempt=None, verdict=None,
            raising=()):
    """One tick through `_run_pg_derivation` with every Postgres step patched.
    `raising` names the steps that raise; `reads` are what successive signal
    reads return or raise. Returns the result and the mocks."""
    values = {"silver": {"rows": 1}, "gold": {"rows": 1, "cells": 1},
              "profile": {"rows": 1}, "validate": verdict or _verdict(),
              "complete": None, "record_run": 1}
    mocks = {name: (AsyncMock(side_effect=RuntimeError(f"{name} boom")) if name in raising
                    else AsyncMock(return_value=values[name])) for name in _STEPS}
    mocks["record_failure"] = AsyncMock()
    mocks["clear_tail"] = AsyncMock(return_value=False)
    with patch("core.pg.get_pool", AsyncMock(return_value=_Pool())), \
         patch("core.pg.require_revision", AsyncMock()), \
         patch("core.pg_silver.rebuild_silver", mocks["silver"]), \
         patch("core.pg_gold.rebuild_gold", mocks["gold"]), \
         patch("core.pg_vitrina.rebuild_customer_profile", mocks["profile"]), \
         patch("core.pg_landing._record_failure", mocks["record_failure"]), \
         patch.object(pg_derivation, "read_owed", AsyncMock(side_effect=list(reads) * 2)), \
         patch.object(pg_derivation, "last_attempt_at", AsyncMock(return_value=journal_attempt)), \
         patch.object(pg_derivation, "validate", mocks["validate"]), \
         patch.object(pg_derivation, "complete", mocks["complete"]), \
         patch.object(pg_derivation, "record_run", mocks["record_run"]), \
         patch.object(pg_derivation, "heal_dropped_marks", AsyncMock(return_value=False)), \
         patch.object(pg_derivation, "clear_tail_failures", mocks["clear_tail"]), \
         patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
         patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
        return _bounded(job._run_pg_derivation()), mocks


def _recorded(mocks):
    return [c.args[0] for c in mocks["record_failure"].await_args_list]


class TestTheFloorOutlivesAnUnwritableJournal:
    """The floor read the journal alone, so a run whose row could not be
    written was followed by another on the next tick, and the next — every
    minute, for as long as Postgres could not take the row."""

    def test_record_run_raising_holds_the_next_tick_inside_the_floor(self, job):
        first, _m = _derive(job, raising=("record_run",))
        assert first["status"] == "error" and first["stage"] == "validation"
        # The journal holds nothing and the debt is still owed; the floor holds.
        second, again = _derive(job, raising=("record_run",))
        assert second == {"skipped": True, "reason": "floor"}
        assert again["silver"].await_count == 0

    def test_the_process_stamp_is_the_runs_own_start(self, job):
        from core.scheduler import BackgroundScheduler

        before = datetime.now(timezone.utc)
        _derive(job)
        assert before <= BackgroundScheduler._pg_derive_started <= datetime.now(timezone.utc)

    def test_the_journal_still_carries_the_floor_across_a_restart(self, job):
        """A new process has no stamp of its own; the journal is what keeps a
        deploy from resetting the floor."""
        recent = datetime.now(timezone.utc) - timedelta(minutes=2)
        result, _m = _derive(job, journal_attempt=recent)
        assert result == {"skipped": True, "reason": "floor"}

    def test_an_older_journal_row_does_not_undercut_this_process(self, job):
        """Production's journal is never empty (RUNS_KEPT is 20 000). A row
        older than the floor beside a newer run of this process whose own row
        could not be written: the later of the two holds. The earlier is
        exactly the journal-only floor, and runs every minute."""
        long_ago = datetime.now(timezone.utc) - timedelta(minutes=11)
        first, _m = _derive(job, journal_attempt=long_ago, raising=("record_run",))
        assert first["status"] == "error" and first["stage"] == "validation"
        second, again = _derive(job, journal_attempt=long_ago)
        assert second == {"skipped": True, "reason": "floor"}
        assert again["silver"].await_count == 0

    def test_past_the_floor_it_runs(self, job):
        from core.scheduler import BackgroundScheduler

        long_ago = datetime.now(timezone.utc) - timedelta(minutes=11)
        BackgroundScheduler._pg_derive_started = long_ago
        result, _m = _derive(job, journal_attempt=long_ago)
        assert result["status"] == "success"


class TestEveryStageIsRecorded:
    """Silver and Gold were recorded against their watermarks; the profile and
    the run's tail were not, so a failure there left every row reading clean."""

    def test_the_profile_raising_is_recorded_against_its_table(self, job):
        from core.pg_vitrina import PROFILE_TABLE

        result, mocks = _derive(job, raising=("profile",))
        assert result["stage"] == PROFILE_TABLE == "app.customer_profile"
        assert _recorded(mocks) == [PROFILE_TABLE]

    @pytest.mark.parametrize("step", ["validate", "complete", "record_run"])
    def test_the_tail_raising_is_recorded_against_the_journal(self, job, step):
        result, mocks = _derive(job, raising=(step,))
        assert result["stage"] == "validation"
        assert _recorded(mocks) == [pg_derivation.RUNS_TABLE]
        assert mocks["clear_tail"].await_count == 0

    @pytest.mark.parametrize("step,table", [("silver", "silver.orders"),
                                            ("gold", "gold.daily_revenue")])
    def test_silver_and_gold_are_recorded_as_before(self, job, step, table):
        _r, mocks = _derive(job, raising=(step,))
        assert _recorded(mocks) == [table]

    def test_the_read_inside_the_lock_is_not_recorded_on_the_signal(self, job):
        """The signal's row counts dropped marks, and the canary pages on it by
        that name; the journal row and the alert carry this failure."""
        result, mocks = _derive(job, reads=((3, 2, None), RuntimeError("read")))
        assert result["stage"] == "signal"
        assert _recorded(mocks) == []

    def test_a_completed_run_clears_the_tail(self, job):
        result, mocks = _derive(job)
        assert result["status"] == "success"
        assert mocks["clear_tail"].await_count == 1 and _recorded(mocks) == []

    def test_a_run_that_fails_validation_still_clears_it(self, job):
        """Not passed is a finding, journaled and alerted as `validation_failed`;
        the tail itself completed."""
        result, mocks = _derive(job, verdict=_verdict(passed=False))
        assert result["validation_passed"] is False
        assert mocks["clear_tail"].await_count == 1


class _Conn:
    def __init__(self, status="UPDATE 1", error=None):
        self.status, self.error, self.calls = status, error, []

    async def execute(self, sql, *args):
        self.calls.append((" ".join(sql.split()), args))
        if self.error:
            raise self.error
        return self.status


class TestClearingTheTail:
    def test_it_touches_only_a_journal_row_that_owes_something(self):
        conn = _Conn(status="UPDATE 1")
        assert asyncio.run(pg_derivation.clear_tail_failures(conn)) is True
        sql, args = conn.calls[0]
        assert args == (pg_derivation.RUNS_TABLE,) and args[0] == "meta.derivation_runs"
        assert "failures_since_ok > 0" in sql
        # Both stamps together: the table's constraint forbids a success after
        # the last attempt, and equal stamps are what it calls healthy.
        assert "last_attempted_at = now()" in sql and "last_ok_at = now()" in sql

    def test_nothing_owed_is_false(self):
        assert asyncio.run(pg_derivation.clear_tail_failures(_Conn(status="UPDATE 0"))) is False

    def test_it_never_raises(self):
        """The run has already succeeded; a clear that failed leaves a count
        the next completed run clears."""
        conn = _Conn(error=ConnectionError("gone"))
        assert asyncio.run(pg_derivation.clear_tail_failures(conn)) is False


class TestTheProfileStampsItsWatermark:
    def test_in_the_transaction_that_rebuilds_it(self):
        """A failure is recorded against `app.customer_profile`; only a success
        stamped here makes `failures_since_ok` mean "still failing"."""
        from core.pg_vitrina import rebuild_customer_profile

        tree = ast.parse(textwrap.dedent(inspect.getsource(rebuild_customer_profile)))
        transaction = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncWith)
                           and any(isinstance(i.context_expr, ast.Call)
                                   and getattr(i.context_expr.func, "attr", "") == "transaction"
                                   for i in n.items))
        stamped = [n for n in ast.walk(transaction) if isinstance(n, ast.Call)
                   and getattr(n.func, "attr", "") == "execute" and n.args
                   and isinstance(n.args[0], ast.Name) and n.args[0].id == "_WATERMARK_OK"]
        assert stamped, "rebuild_customer_profile does not stamp its watermark in its transaction"
        assert [a.id for a in stamped[0].args[1:2]] == ["PROFILE_TABLE"]
