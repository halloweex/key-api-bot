"""DN-19: Postgres writes its own `silver.order_utm`.

The derivation's last step parses the table out of `bronze.orders`, and the
four re-parse doors parse in Postgres through `reparse_router`. Until step
14's S14-15 `KS_UTM_PARSE` chose between that and a copy of DuckDB's parse
(`core/pg_order_utm.py`, deleted with the mode); the tests of the mode, of
the copy, of the DuckDB tick and of the `utm_parse` health block and its
canary check went with them.

What is pinned here: the router's two branches, the incremental under the
layer lock, the derivation's last step and its isolation from the
derivation's own faults, and that `/api/health` watches the table with a
limit of its own which the canary judges. The doors' walks are in
`tests/unit/test_utm_parse_doors.py`; the parse against a real Postgres in
`tests/integration/test_pg_own_derivation.py`.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_utm_parse
# The derivation's own harness: one tick through `_run_pg_derivation` with
# every Postgres step patched, and the fixtures it runs under.
from tests.unit.test_pg_own_derivation import _derive, job, mode  # noqa: F401

UTM = pg_utm_parse.UTM_TABLE


def _bounded(coro, timeout=10):
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


# ─── The router ───────────────────────────────────────────────────────────────

class _Engines:
    """Every way the router can reach the table, patched and counted."""

    def __init__(self, *, parse_raises=None):
        self.full = AsyncMock(return_value={"rows": 3, "replaced": 3})
        self.incremental = AsyncMock(
            side_effect=parse_raises, return_value={"parsed": 1, "rows": 3})

    def __enter__(self):
        self._patches = [
            patch.object(pg_utm_parse, "parse_full", self.full),
            patch.object(pg_utm_parse, "parse_incremental_locked", self.incremental),
            patch("core.mirror_reconciliation.configured", return_value=True),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._patches):
            p.stop()


class TestTheRouter:
    def test_it_parses_what_is_new(self):
        with _Engines() as e:
            assert _bounded(pg_utm_parse.reparse_router()) == {"parsed": 1, "rows": 3}
        assert e.incremental.await_count == 1 and e.full.await_count == 0

    def test_full_replaces_the_table_and_forwards_force(self):
        with _Engines() as e:
            _bounded(pg_utm_parse.reparse_router(full=True))
            _bounded(pg_utm_parse.reparse_router(full=True, force=True))
        assert [c.kwargs for c in e.full.await_args_list] == [
            {"force": False}, {"force": True}]
        assert e.incremental.await_count == 0

    def test_it_never_raises(self):
        """The door reads the answer; the parse has already written its
        failure into the watermark."""
        with _Engines(parse_raises=RuntimeError("pg is down")):
            out = _bounded(pg_utm_parse.reparse_router())
        assert out == {"error": "RuntimeError: pg is down"}

    def test_it_stands_down_without_a_dsn(self):
        with _Engines() as e, \
                patch("core.mirror_reconciliation.configured", return_value=False):
            out = _bounded(pg_utm_parse.reparse_router())
        assert "skipped" in out
        assert e.incremental.await_count == e.full.await_count == 0


class TestTheIncrementalUnderTheLayerLock:
    def test_it_holds_the_lock_across_the_parse(self):
        from core import pg_silver

        seen = []

        async def parse(pool=None):
            seen.append(pg_silver.PG_LAYER_LOCK.locked())
            return {"parsed": 0}

        with patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()), \
             patch.object(pg_utm_parse, "parse_incremental", parse):
            _bounded(pg_utm_parse.parse_incremental_locked())
            assert not pg_silver.PG_LAYER_LOCK.locked()
        assert seen == [True]

    def test_a_lock_that_is_never_free_is_recorded_and_raised(self):
        """Two of its callers are HTTP handlers, and the ClickHouse shippers
        hold the same lock across network I/O."""
        recorded = AsyncMock()
        parse = AsyncMock()

        async def run():
            from core import pg_silver

            async with pg_silver.PG_LAYER_LOCK:             # held by somebody else
                await pg_utm_parse.parse_incremental_locked()

        with patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()), \
             patch.object(pg_utm_parse, "LOCK_WAIT_S", 0.05), \
             patch.object(pg_utm_parse, "parse_incremental", parse), \
             patch("core.pg_landing._record_failure", recorded):
            with pytest.raises(asyncio.TimeoutError):
                _bounded(run())
        assert parse.await_count == 0
        (table, error), _kw = recorded.await_args
        assert table == UTM and "incremental UTM parse did not run" in error


# ─── The derivation's last step ───────────────────────────────────────────────

def _derive_with_parse(job, parse, **kw):
    """`_derive` with the parse patched in, and the alerts counted."""
    from core.scheduler import BackgroundScheduler

    alert = AsyncMock()
    with patch.object(BackgroundScheduler, "_pg_derivation_alert", alert):
        result, mocks = _derive(job, utm=parse, **kw)
    return result, mocks, alert


@pytest.fixture
def utm_postgres(job):
    """`job`, whose derivation ends with the parse — unconditionally since
    step 14's S14-15, which removed KS_UTM_PARSE."""
    return job


class TestTheDerivationsLastStep:
    def test_the_parse_runs_after_the_journal_row(self, utm_postgres):
        from core.scheduler import BackgroundScheduler

        order = []
        parse = AsyncMock(side_effect=lambda pool=None: order.append("parse")
                          or {"parsed": 2, "rows": 9})
        journaled = BackgroundScheduler._derive_and_journal_pg_layers

        async def derivation(self, pool, trigger):
            result = await journaled(self, pool, trigger)
            order.append("derivation, journaled")
            return result

        with patch.object(BackgroundScheduler, "_derive_and_journal_pg_layers", derivation):
            result, mocks, alert = _derive_with_parse(utm_postgres, parse)
        assert order == ["derivation, journaled", "parse"]
        assert mocks["record_run"].await_count == 1
        assert result["status"] == "success"
        assert result["order_utm"] == {"parsed": 2, "rows": 9}
        assert parse.await_args.args and parse.await_args.args[0] is not None, (
            "the parse must use the derivation's pool")
        assert alert.await_count == 0

    def test_the_parse_takes_the_layer_lock_after_the_derivation_let_it_go(
            self, utm_postgres):
        """The derivation holds `PG_LAYER_LOCK`, and its last step takes it
        again through the real `parse_incremental_locked`. `asyncio.Lock` is
        not reentrant: called inside the derivation's `async with`, the parse
        would wait out `LOCK_WAIT_S` on its own caller and record a failure on
        every run. The module docstring once said the derivation holds the
        lock for the parse; this is what makes that sentence impossible to
        follow quietly."""
        from core import pg_silver
        from core.scheduler import BackgroundScheduler

        held = []

        async def parse(pool=None):
            held.append(pg_silver.PG_LAYER_LOCK.locked())
            return {"parsed": 1, "rows": 9}

        with patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()), \
             patch.object(pg_utm_parse, "LOCK_WAIT_S", 0.05), \
             patch.object(pg_utm_parse, "parse_incremental", parse), \
             patch.object(BackgroundScheduler, "_pg_derivation_alert", AsyncMock()):
            result, mocks = _derive(utm_postgres, utm=pg_utm_parse.parse_incremental_locked)
        assert held == [True], "the parse ran without the lock, or never took it"
        assert result["order_utm"] == {"parsed": 1, "rows": 9}
        assert mocks["record_failure"].await_count == 0

    def test_a_gold_fault_does_not_stop_the_parse(self, utm_postgres):
        """After step 9 this parse is the only classifier. Inside the
        derivation's try a persistent Gold fault would have stopped it too,
        and /traffic would have filed every new order as organic for the
        length of an unrelated outage."""
        parse = AsyncMock(return_value={"parsed": 1, "rows": 9})
        result, mocks, alert = _derive_with_parse(utm_postgres, parse, raising=("gold",))
        assert result["status"] == "error" and result["stage"] == "gold.daily_revenue"
        assert mocks["record_run"].await_count == 1, "the failed run is journaled first"
        assert parse.await_count == 1
        assert result["order_utm"] == {"parsed": 1, "rows": 9}

    def test_a_parse_fault_is_not_the_derivations(self, utm_postgres):
        """Its failure is its own: in `silver.order_utm`'s watermark, written
        by the parse, and judged by the canary from there. The derivation it
        followed succeeded and says so."""
        parse = AsyncMock(side_effect=RuntimeError("bad comment"))
        result, mocks, alert = _derive_with_parse(utm_postgres, parse)
        assert result["status"] == "success" and result["validation_passed"] is True
        assert result["order_utm"] == {"status": "error",
                                       "error": "RuntimeError: bad comment"}
        assert mocks["record_failure"].await_count == 0
        assert alert.await_count == 0


# ─── /api/health and the canary ───────────────────────────────────────────────

class TestHealthPublishesIt:
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

    def test_the_table_is_watched_with_its_own_limit(self):
        data = self._mirrors()
        assert set(data) == {"bronze.orders", "silver.orders",
                             "gold.daily_revenue", UTM}
        assert data[UTM]["max_age_s"] == pg_utm_parse.MAX_AGE_S

    def test_the_limit_is_inside_the_canarys_ceiling(self):
        from bot import canary

        assert 0 < pg_utm_parse.MAX_AGE_S <= canary.DECLARED_MAX_AGE_CEILING_S


class TestTheCanaryJudgesIt:
    def test_a_stale_or_failing_parse_is_judged_by_the_declared_limit(self):
        from bot.canary import check_mirror_freshness

        entry = {"age_seconds": pg_utm_parse.MAX_AGE_S + 60, "failures_since_ok": 2,
                 "failing": True, "max_age_s": pg_utm_parse.MAX_AGE_S}
        failures, ages = check_mirror_freshness({"mirrors": {
            "bronze.orders": {"age_seconds": 60, "failures_since_ok": 0, "failing": False},
            UTM: entry}})
        keys = [k for k, _ in failures]
        assert f"mirror_failing:{UTM}" in keys and f"mirror_stale:{UTM}" in keys
        assert ages[UTM] == entry["age_seconds"]
