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
