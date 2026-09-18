"""DN-05b without a database: a dropped mark heals, and is published until it does.

`mark` records a drop in `meta.mirror_state` and nothing used to write that row
again, so one lock timeout read as failing for ever. What is here: the heal is
asked for only after a validated rebuild, `/api/health` publishes the count, and
the canary pages only where healing is demonstrably not happening — the latest
drop older than a heartbeat's rebuild, or marks failing ten times in a row —
judged from one probe with no memory between probes. The heal's SQL against
a real server is in `tests/integration/test_pg_own_derivation.py`.
"""
from __future__ import annotations

import ast
import asyncio
import pathlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from bot import canary
from core import pg_derivation

REPO = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def mode(monkeypatch):
    before = (pg_derivation._mode, pg_derivation._mode_error)

    def set_mode(value):
        if value is None:
            monkeypatch.delenv(pg_derivation.ENV, raising=False)
        else:
            monkeypatch.setenv(pg_derivation.ENV, value)
        return pg_derivation.configure_mode()

    yield set_mode
    pg_derivation._mode, pg_derivation._mode_error = before


class _Conn:
    def __init__(self, row=None, error=None, status="UPDATE 1"):
        self.row, self.error, self.status = row, error, status
        self.calls = []

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if self.error:
            raise self.error
        return self.row

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        if self.error:
            raise self.error
        return self.status


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


# ─── The heal is asked for only after a validated rebuild ───────────────────

def _validation(passed: bool) -> dict:
    return {"passed": passed, "row_count_match": passed, "checksum_match": True,
            "cells_match": True, "rollup_match": True, "partition_exhaustive": True,
            "missing_cells": 0, "extra_cells": 0, "rollup_mismatch_cells": 0,
            "unknown_sales_types": []}


class TestTheJobHealsOnlyAValidatedRun:
    def _derive(self, *, passed=True, gold_raises=False):
        from core.scheduler import BackgroundScheduler

        heal = AsyncMock(return_value=True)
        gold = (AsyncMock(side_effect=RuntimeError("boom")) if gold_raises
                else AsyncMock(return_value={"rows": 1, "cells": 1}))
        # Everything the job keeps on the class or the module, put back: the
        # run stamps `_pg_derive_started`, and a stamp left behind is a hidden
        # ten-minute floor for whichever test drives the job next.
        before = (BackgroundScheduler._pg_derive_ran, BackgroundScheduler._pg_derive_started,
                  pg_derivation._signal_failures)
        try:
            with patch("core.pg_silver.rebuild_silver", AsyncMock(return_value={"rows": 1})), \
                 patch("core.pg_gold.rebuild_gold", gold), \
                 patch("core.pg_vitrina.rebuild_customer_profile",
                       AsyncMock(return_value={"rows": 1})), \
                 patch("core.pg_landing._record_failure", AsyncMock()), \
                 patch.object(pg_derivation, "read_owed", AsyncMock(return_value=(3, 2, None))), \
                 patch.object(pg_derivation, "validate", AsyncMock(return_value=_validation(passed))), \
                 patch.object(pg_derivation, "complete", AsyncMock()), \
                 patch.object(pg_derivation, "record_run", AsyncMock(return_value=1)), \
                 patch.object(pg_derivation, "heal_dropped_marks", heal), \
                 patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
                 patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                started_after = datetime.now(timezone.utc)
                result = asyncio.run(
                    BackgroundScheduler()._derive_pg_layers(_Pool(_Conn()), "signal"))
        finally:
            (BackgroundScheduler._pg_derive_ran, BackgroundScheduler._pg_derive_started,
             pg_derivation._signal_failures) = before
        return result, heal, started_after

    def test_a_validated_run_heals_with_its_own_start(self):
        result, heal, before = self._derive(passed=True)
        assert result["status"] == "success"
        assert heal.await_count == 1
        started = heal.await_args.args[1]
        assert before <= started <= datetime.now(timezone.utc)

    def test_a_run_that_fails_validation_does_not(self):
        result, heal, _ = self._derive(passed=False)
        assert result["validation_passed"] is False
        assert heal.await_count == 0

    def test_an_errored_run_does_not(self):
        result, heal, _ = self._derive(gold_raises=True)
        assert result["status"] == "error"
        assert heal.await_count == 0

    def test_the_process_state_is_left_as_it_was_found(self):
        """A run stamps the class. Left behind, the stamp skipped the next test
        that drove the job with `floor`, depending only on file order."""
        from core.scheduler import BackgroundScheduler

        def state():
            return (BackgroundScheduler._pg_derive_ran,
                    BackgroundScheduler._pg_derive_started,
                    pg_derivation._signal_failures)

        before = state()
        self._derive(passed=True)
        assert state() == before


class TestTheHealItself:
    def test_it_bounds_by_the_start_less_the_margin(self):
        conn = _Conn(status="UPDATE 1")
        started = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)
        assert asyncio.run(pg_derivation.heal_dropped_marks(conn, started)) is True
        sql, args = conn.calls[0]
        assert args == (pg_derivation.SIGNAL_TABLE, started, pg_derivation.DROPPED_MARK_MARGIN)
        assert pg_derivation.DROPPED_MARK_MARGIN == timedelta(seconds=30)
        # Both stamps move together: the table's constraint forbids a success
        # after the last attempt.
        assert "last_attempted_at = now()" in " ".join(sql.split())
        assert "last_ok_at = now()" in " ".join(sql.split())

    def test_nothing_to_heal_is_false(self):
        conn = _Conn(status="UPDATE 0")
        assert asyncio.run(pg_derivation.heal_dropped_marks(
            conn, datetime.now(timezone.utc))) is False

    def test_it_never_raises(self):
        """The rebuild has succeeded and settled its debt by then; a heal that
        failed costs a stale count the next validated run clears."""
        conn = _Conn(error=ConnectionError("gone"))
        assert asyncio.run(pg_derivation.heal_dropped_marks(
            conn, datetime.now(timezone.utc))) is False


# ─── /api/health publishes the count and the age of the latest drop ────────

class TestHealthPublishesTheDrops:
    def _read(self, conn=None, pool_error=None):
        from web.routes.api import health

        health._marks_cache.update(data=None, expires_at=0)
        get_pool = (AsyncMock(side_effect=pool_error) if pool_error
                    else AsyncMock(return_value=_Pool(conn)))
        try:
            with patch("core.pg.get_pool", get_pool):
                return asyncio.run(health._derivation_block()), get_pool
        finally:
            health._marks_cache.update(data=None, expires_at=0)

    def test_under_own_it_reads_the_count_and_ages_the_latest_drop(self, mode):
        mode("own")
        dropped = datetime.now(timezone.utc) - timedelta(minutes=80)
        conn = _Conn(row={"failures_since_ok": 3, "last_attempted_at": dropped})
        block, _ = self._read(conn)
        assert set(block) == {"mode", "error", "signal_unreadable_ticks",
                              "marks_dropped_unhealed", "last_mark_drop_age_s"}
        assert block["marks_dropped_unhealed"] == 3
        assert 80 * 60 <= block["last_mark_drop_age_s"] <= 80 * 60 + 5
        assert conn.calls[0][1] == (pg_derivation.SIGNAL_TABLE,)

    def test_a_healed_row_has_no_age(self, mode):
        """A heal moves `last_attempted_at` too; read as a drop, it would age
        into a page an hour after every heal."""
        mode("own")
        healed = datetime.now(timezone.utc) - timedelta(hours=5)
        block, _ = self._read(_Conn(row={"failures_since_ok": 0, "last_attempted_at": healed}))
        assert (block["marks_dropped_unhealed"], block["last_mark_drop_age_s"]) == (0, None)

    def test_a_signal_that_never_dropped_a_mark_has_no_row_and_reads_zero(self, mode):
        mode("own")
        block, _ = self._read(_Conn(row=None))
        assert (block["marks_dropped_unhealed"], block["last_mark_drop_age_s"]) == (0, None)

    def test_unreadable_is_null_not_zero(self, mode):
        mode("own")
        block, _ = self._read(pool_error=RuntimeError("no dsn"))
        assert (block["marks_dropped_unhealed"], block["last_mark_drop_age_s"]) == (None, None)

    def test_under_piggyback_it_is_not_asked(self, mode):
        """Nothing marks and nothing heals there, so a count left from an
        earlier soak would page for ever."""
        mode(None)
        block, get_pool = self._read(_Conn(row={"failures_since_ok": 5,
                                                "last_attempted_at": datetime.now(timezone.utc)}))
        assert (block["marks_dropped_unhealed"], block["last_mark_drop_age_s"]) == (None, None)
        assert get_pool.await_count == 0

    def test_the_endpoint_carries_the_block(self):
        tree = ast.parse((REPO / "web/routes/api/health.py").read_text(encoding="utf-8"))
        handler = next(n for n in ast.walk(tree)
                       if isinstance(n, ast.AsyncFunctionDef) and n.name == "health_check")
        awaited = {n.value.func.id for n in ast.walk(handler)
                   if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
                   and isinstance(n.value.func, ast.Name)}
        assert "_derivation_block" in awaited


# ─── The canary: only where healing is demonstrably not happening ──────────

LIMIT = canary.DERIVATION_HEAL_WITHIN_S
PERSISTENT = canary.DERIVATION_MARKS_PERSISTENT


def _payload(count, age):
    from tests.unit.test_canary import _healthy_payload

    payload = _healthy_payload()
    payload["derivation"] = {"mode": "own", "error": None,
                             "marks_dropped_unhealed": count,
                             "last_mark_drop_age_s": age}
    return payload


def _keys(count, age):
    return [k for k, _ in canary.check_derivation_marks(_payload(count, age))]


class TestTheLimitIsTheHeartbeatPlusFifteen:
    def test_pinned_to_core_pg_derivation(self):
        """Written out in `bot/canary.py` because nothing under `bot/` may
        import `core.pg*`; this is what keeps the two from drifting."""
        assert LIMIT == pg_derivation.HEARTBEAT.total_seconds() + 15 * 60

    def test_it_clears_the_floor_the_tick_and_the_heal_margin(self):
        floor = timedelta(seconds=600)   # KS_PG_SILVER_INTERVAL_S default
        tick = timedelta(minutes=1)
        worst = (pg_derivation.HEARTBEAT + floor + tick
                 + pg_derivation.DROPPED_MARK_MARGIN)
        assert worst.total_seconds() < LIMIT


class TestAnOldDrop:
    def test_just_past_the_limit_pages(self):
        assert _keys(1, LIMIT + 1) == ["derivation_marks_failing"]

    def test_at_the_limit_is_nothing(self):
        assert _keys(1, LIMIT) == []

    def test_a_young_drop_is_nothing(self):
        assert _keys(3, 60) == []

    def test_the_message_says_how_long_and_how_many(self):
        (_key, message), = canary.check_derivation_marks(_payload(2, 2 * 3600))
        assert "2h" in message and "2" in message


class TestMarksFailingPersistently:
    def test_nine_is_nothing(self):
        assert _keys(PERSISTENT - 1, 60) == []

    def test_ten_pages_however_young_the_latest(self):
        assert PERSISTENT == 10
        assert _keys(PERSISTENT, 60) == ["derivation_marks_failing"]

    def test_with_no_age_only_the_count_can_fire(self):
        assert _keys(PERSISTENT - 1, None) == []
        assert _keys(PERSISTENT, None) == ["derivation_marks_failing"]


class TestNullIsNeverJudged:
    def test_zero_or_null_count_is_nothing_whatever_the_age(self):
        assert _keys(0, LIMIT * 10) == []
        assert _keys(None, LIMIT * 10) == []

    def test_an_older_web_or_no_block(self):
        assert canary.check_derivation_marks({"derivation": {"mode": "own"}}) == []
        assert canary.check_derivation_marks({}) == []
        assert canary.check_derivation_marks(None) == []

    def test_a_bool_is_not_a_number(self):
        assert _keys(True, LIMIT * 10) == []


class TestRunCanary:
    async def _probe(self, count, age):
        payload = _payload(count, age)

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                return await canary.run_canary("https://x.test", client=client)

    @pytest.mark.asyncio
    async def test_a_drop_nothing_has_healed_files_a_warning_on_one_probe(self):
        result = await self._probe(2, LIMIT + 60)
        assert result.failure_keys == ["derivation_marks_failing"]
        assert result.severity == "warn"
        assert "meta.derivation_runs" in canary.format_alert(result, "https://x.test")

    @pytest.mark.asyncio
    async def test_a_drop_on_its_way_out_is_green(self):
        result = await self._probe(2, 300)
        assert result.ok and result.failure_keys == []

    def test_the_canary_keeps_no_state_for_it(self):
        """The rule is judged from one payload, so neither the canary nor the
        job that runs it carries anything between probes."""
        import inspect

        assert list(inspect.signature(canary.check_derivation_marks).parameters) == ["payload"]
        assert "previous_marks_dropped" not in inspect.signature(canary.run_canary).parameters
        tree = ast.parse((REPO / "bot/main.py").read_text(encoding="utf-8"))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert {name for name in names if name.startswith("canary_prev_")} == {
            "canary_prev_failures"}
