"""DN-19: `KS_UTM_PARSE` — who writes Postgres' `silver.order_utm`.

Under the default, `duckdb`, nothing changes: the tick ships DuckDB's parse
and the four re-parse doors ship through `ship_after_reparse`. Under
`postgres` (which needs `KS_PG_DERIVE=own`) the derivation's last step parses
the table out of `bronze.orders`, the doors parse in Postgres through
`reparse_router`, and nothing ships.

What is pinned here: the mode (read once, before the boot sync, never raising,
downgraded to `duckdb` with a published error when it cannot be honoured), the
router's two branches, the derivation's last step and its isolation from the
derivation's own faults, the tick's silence, the boot, and what `/api/health`
and the canary make of all of it. The structural walks — every door goes
through the router, no ship is reachable under `postgres` — are in
`tests/unit/test_order_utm_shipping.py`; the parse against a real Postgres in
`tests/integration/test_pg_own_derivation.py`.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import textwrap
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import pg_derivation, pg_utm_parse, read_fallback
# The derivation's own harness: one tick through `_run_pg_derivation` with
# every Postgres step patched, and the fixtures it runs under.
from tests.unit.test_pg_own_derivation import _derive, job, mode  # noqa: F401

UTM = pg_utm_parse.UTM_TABLE


@pytest.fixture
def modes(monkeypatch):
    """Set KS_PG_DERIVE and KS_UTM_PARSE the way `configure_modes` reads
    them — the derivation first — and put every cache back afterwards."""
    before = (pg_derivation._mode, pg_derivation._mode_error,
              pg_utm_parse._mode, pg_utm_parse._mode_error)

    def set_modes(derive=None, utm=None):
        for env, value in ((pg_derivation.ENV, derive), (pg_utm_parse.ENV, utm)):
            if value is None:
                monkeypatch.delenv(env, raising=False)
            else:
                monkeypatch.setenv(env, value)
        pg_derivation.configure_mode()
        return pg_utm_parse.configure_mode()

    yield set_modes
    (pg_derivation._mode, pg_derivation._mode_error,
     pg_utm_parse._mode, pg_utm_parse._mode_error) = before


def _bounded(coro, timeout=10):
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


# ─── The mode ─────────────────────────────────────────────────────────────────

class TestTheMode:
    def test_unset_is_duckdb(self, modes):
        assert modes() == "duckdb"
        assert not pg_utm_parse.parses_in_postgres()
        assert pg_utm_parse.mode_error() is None

    def test_postgres_beside_own(self, modes):
        assert modes(derive="own", utm="postgres") == "postgres"
        assert pg_utm_parse.parses_in_postgres()
        assert pg_utm_parse.mode_error() is None

    def test_postgres_without_own_runs_as_duckdb_and_says_why(self, modes):
        """The tick would stop shipping and no derivation would parse: the
        table would freeze under an OK watermark. So it is refused — as
        `duckdb`, never by raising."""
        assert modes(utm="postgres") == "duckdb"
        assert not pg_utm_parse.parses_in_postgres()
        error = pg_utm_parse.mode_error()
        assert "KS_PG_DERIVE" in error and "'piggyback'" in error

    @pytest.mark.parametrize("value", ["postgress", "pg", "on"])
    def test_an_unknown_value_runs_as_duckdb_and_says_so(self, modes, value):
        assert modes(derive="own", utm=value) == "duckdb"
        assert f"KS_UTM_PARSE={value!r}" in pg_utm_parse.mode_error()

    def test_case_and_blanks_are_forgiven(self, modes):
        assert modes(derive="own", utm="  Postgres ") == "postgres"
        assert modes(derive="own", utm="") == "duckdb"
        assert pg_utm_parse.mode_error() is None

    def test_the_error_is_logged_once_per_cause(self, modes, caplog):
        """Web's startup and the scheduler both configure: one ERROR per cause
        is what a reader of the log can use."""
        with caplog.at_level(logging.ERROR, logger="core.pg_utm_parse"):
            modes(utm="bogus")
            modes(utm="bogus")
        assert len([r for r in caplog.records if "bogus" in r.getMessage()]) == 1

    def test_the_write_path_never_reads_the_environment(self, modes, monkeypatch):
        """The tick, the derivation and the router ask on every run; a typo
        evaluated there would change the writer mid-process."""
        modes(derive="own", utm="postgres")
        monkeypatch.setenv(pg_utm_parse.ENV, "duckdb")
        assert pg_utm_parse.parses_in_postgres() is True
        for fn in (pg_utm_parse.parses_in_postgres, pg_utm_parse.mode,
                   pg_utm_parse.reparse_router):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            reads = [n for n in ast.walk(tree)
                     if isinstance(n, ast.Attribute) and n.attr in ("getenv", "environ")]
            assert reads == [], fn.__name__

    def test_configure_modes_reads_it_after_the_derivation(self, modes, monkeypatch):
        """Both caches empty, as in a fresh process: `postgres` is honoured only
        because `KS_PG_DERIVE` was read first. Read the other way round, the
        derivation would still be unconfigured and the parse refused."""
        from core.runtime_modes import configure_modes

        modes()   # registers the restore; the caches are emptied next
        monkeypatch.setenv(pg_derivation.ENV, "own")
        monkeypatch.setenv(pg_utm_parse.ENV, "postgres")
        pg_derivation._mode = pg_derivation._mode_error = None
        pg_utm_parse._mode = pg_utm_parse._mode_error = None
        fallback = (read_fallback._mode, read_fallback._mode_error,
                    list(read_fallback._misconfigured))
        try:
            read = configure_modes()
        finally:
            (read_fallback._mode, read_fallback._mode_error,
             read_fallback._misconfigured) = fallback
        assert read[pg_utm_parse.ENV] == "postgres"
        assert pg_utm_parse.parses_in_postgres()


# ─── The router ───────────────────────────────────────────────────────────────

class _Engines:
    """Every way the router can reach the table, patched and counted."""

    def __init__(self, *, parse_raises=None):
        self.ship_after = AsyncMock(return_value={"rows": 3})
        self.ship = AsyncMock(return_value={"rows": 3})
        self.full = AsyncMock(return_value={"rows": 3, "replaced": 3})
        self.incremental = AsyncMock(
            side_effect=parse_raises, return_value={"parsed": 1, "rows": 3})

    def __enter__(self):
        self._patches = [
            patch("core.pg_order_utm.ship_after_reparse", self.ship_after),
            patch("core.pg_order_utm.ship_order_utm", self.ship),
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
    def test_duckdb_ships_as_before_and_forwards_force(self, modes):
        modes(derive="own")
        store = object()
        with _Engines() as e:
            assert _bounded(pg_utm_parse.reparse_router(store)) == {"rows": 3}
            _bounded(pg_utm_parse.reparse_router(store, full=True, force=True))
        assert [c.args for c in e.ship_after.await_args_list] == [(store,), (store,)]
        assert [c.kwargs for c in e.ship_after.await_args_list] == [
            {"force": False}, {"force": True}]
        assert e.full.await_count == e.incremental.await_count == 0

    def test_postgres_parses_what_is_new_and_ships_nothing(self, modes):
        modes(derive="own", utm="postgres")
        with _Engines() as e:
            assert _bounded(pg_utm_parse.reparse_router(object())) == {
                "parsed": 1, "rows": 3}
        assert e.incremental.await_count == 1 and e.full.await_count == 0
        assert e.ship_after.await_count == e.ship.await_count == 0

    def test_postgres_full_replaces_the_table_and_forwards_force(self, modes):
        modes(derive="own", utm="postgres")
        with _Engines() as e:
            _bounded(pg_utm_parse.reparse_router(object(), full=True))
            _bounded(pg_utm_parse.reparse_router(object(), full=True, force=True))
        assert [c.kwargs for c in e.full.await_args_list] == [
            {"force": False}, {"force": True}]
        assert e.incremental.await_count == 0
        assert e.ship_after.await_count == e.ship.await_count == 0

    def test_postgres_never_raises(self, modes):
        """The DuckDB half of the door has succeeded and must be reported; the
        parse has already written its failure into the watermark."""
        modes(derive="own", utm="postgres")
        with _Engines(parse_raises=RuntimeError("pg is down")) as e:
            out = _bounded(pg_utm_parse.reparse_router(object()))
        assert out == {"error": "RuntimeError: pg is down"}
        assert e.ship_after.await_count == 0

    def test_postgres_stands_down_without_a_dsn(self, modes):
        modes(derive="own", utm="postgres")
        with _Engines() as e, \
                patch("core.mirror_reconciliation.configured", return_value=False):
            out = _bounded(pg_utm_parse.reparse_router(object()))
        assert "skipped" in out
        assert e.incremental.await_count == e.ship_after.await_count == 0


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
    with patch.object(pg_utm_parse, "parse_incremental_locked", parse), \
         patch.object(BackgroundScheduler, "_pg_derivation_alert", alert):
        result, mocks = _derive(job, **kw)
    return result, mocks, alert


@pytest.fixture
def utm_postgres(job, monkeypatch):
    """`job` is own already; this adds KS_UTM_PARSE=postgres on top of it."""
    before = (pg_utm_parse._mode, pg_utm_parse._mode_error)
    monkeypatch.setenv(pg_utm_parse.ENV, "postgres")
    assert pg_utm_parse.configure_mode() == "postgres"
    yield job
    pg_utm_parse._mode, pg_utm_parse._mode_error = before


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
            result, mocks = _derive(utm_postgres)
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

    def test_under_duckdb_the_result_is_what_it_was(self, job):
        """Byte for byte: no parse, no key."""
        parse = AsyncMock()
        result, _m, _a = _derive_with_parse(job, parse)
        assert parse.await_count == 0
        assert "order_utm" not in result
        assert result["status"] == "success"


# ─── The DuckDB tick ──────────────────────────────────────────────────────────

class TestTheTick:
    def _run(self):
        from core.scheduler import BackgroundScheduler

        silver = AsyncMock(return_value={"rows": 1})
        gold = AsyncMock(return_value={"rows": 1, "cells": 1})
        profile, ship = AsyncMock(return_value={"rows": 1}), AsyncMock(return_value={"rows": 1})
        with patch("core.pg_vitrina.rebuild_customer_profile", profile), \
             patch("core.pg_order_utm.ship_order_utm", ship), \
             patch("core.duckdb_store.get_store", AsyncMock(return_value=object())):
            _bounded(BackgroundScheduler()._rebuild_pg_layers(silver, gold))
        return silver, gold, profile, ship

    def test_under_postgres_it_does_nothing_in_postgres(self, modes):
        modes(derive="own", utm="postgres")
        silver, gold, profile, ship = self._run()
        assert (silver.await_count, gold.await_count, profile.await_count,
                ship.await_count) == (0, 0, 0, 0)

    def test_under_own_and_duckdb_it_still_ships(self, modes):
        modes(derive="own")
        *_derived, ship = self._run()
        assert ship.await_count == 1

    def test_a_refused_postgres_ships_as_before(self, modes):
        """`postgres` without own ran as duckdb: the tick keeps the copy
        /traffic has always had."""
        modes(utm="postgres")
        silver, gold, profile, ship = self._run()
        assert (silver.await_count, gold.await_count, profile.await_count,
                ship.await_count) == (1, 1, 1, 1)


# ─── The boot ─────────────────────────────────────────────────────────────────

@pytest.fixture
def boot(monkeypatch):
    """web's `startup_event` with what it would do to the world stubbed, the
    real `init_and_sync` inside it, then the scheduler's first warehouse tick
    — the one that follows a boot sync, which marks the warehouse dirty.
    Returns what the boot sync saw and every Postgres step the tick reached."""
    import web.main as main
    from core import pg_silver
    from core.scheduler import BackgroundScheduler

    saved = (pg_derivation._mode, pg_derivation._mode_error,
             pg_utm_parse._mode, pg_utm_parse._mode_error,
             read_fallback._mode, read_fallback._mode_error,
             list(read_fallback._misconfigured),
             BackgroundScheduler._pg_silver_last_at,
             BackgroundScheduler._pg_layers_pending)

    def run(derive=None, utm=None):
        for env, value in ((pg_derivation.ENV, derive), (pg_utm_parse.ENV, utm)):
            if value is None:
                monkeypatch.delenv(env, raising=False)
            else:
                monkeypatch.setenv(env, value)
        # A fresh process: nothing configured yet.
        pg_derivation._mode = pg_derivation._mode_error = None
        pg_utm_parse._mode = pg_utm_parse._mode_error = None

        seen = {}
        store = MagicMock()
        store.get_stats = AsyncMock(return_value={
            "orders": 1, "products": 1, "categories": 1, "db_size_mb": 1})
        store.backfill_sms_campaign_record = AsyncMock(return_value=False)
        store.refresh_sku_inventory_status = AsyncMock(return_value=0)
        store.peek_warehouse_dirty = AsyncMock(return_value=(True, None, "stamp"))
        store.refresh_warehouse_layers = AsyncMock(return_value={"status": "success"})
        store.clear_warehouse_dirty = AsyncMock()

        async def incremental_sync():
            seen["parses_in_postgres"] = pg_utm_parse.parses_in_postgres()
            seen["owns"] = pg_derivation.owns()

        sync = MagicMock()
        sync.incremental_sync = AsyncMock(side_effect=incremental_sync)
        steps = {name: AsyncMock(return_value={"rows": 1, "cells": 1})
                 for name in ("silver", "gold", "profile", "ship")}

        monkeypatch.setattr(main, "validate_config", MagicMock())
        monkeypatch.setattr(main, "init_database", MagicMock())
        monkeypatch.setattr(main, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(main, "start_scheduler", AsyncMock())
        monkeypatch.setattr(main, "_register_event_handlers", MagicMock())
        monkeypatch.setattr("core.prediction_service.get_prediction_service",
                            MagicMock(return_value=MagicMock(is_ready=True)))
        monkeypatch.setattr("core.sync_service.get_store", AsyncMock(return_value=store))
        monkeypatch.setattr("core.sync_service.get_sync_service",
                            AsyncMock(return_value=sync))
        monkeypatch.setattr("core.sync_service.init_meilisearch",
                            AsyncMock(return_value=False))
        monkeypatch.setattr("core.duckdb_store.get_store", AsyncMock(return_value=store))
        monkeypatch.setattr("core.mirror_reconciliation.configured", lambda: True)
        monkeypatch.setattr("core.pg_silver.rebuild_silver", steps["silver"])
        monkeypatch.setattr("core.pg_gold.rebuild_gold", steps["gold"])
        monkeypatch.setattr("core.pg_vitrina.rebuild_customer_profile", steps["profile"])
        monkeypatch.setattr("core.pg_order_utm.ship_order_utm", steps["ship"])
        monkeypatch.setattr(pg_silver, "PG_LAYER_LOCK", asyncio.Lock())
        BackgroundScheduler._pg_silver_last_at = None
        BackgroundScheduler._pg_layers_pending = False

        async def boot_then_tick():
            await main.startup_event()
            return await BackgroundScheduler()._run_warehouse_refresh()

        seen["tick"] = _bounded(boot_then_tick())
        assert sync.incremental_sync.await_count == 1, "the boot sync did not run"
        return seen, steps

    yield run
    (pg_derivation._mode, pg_derivation._mode_error,
     pg_utm_parse._mode, pg_utm_parse._mode_error,
     read_fallback._mode, read_fallback._mode_error,
     read_fallback._misconfigured,
     BackgroundScheduler._pg_silver_last_at,
     BackgroundScheduler._pg_layers_pending) = saved


class TestTheBoot:
    def test_under_postgres_the_boot_ships_no_utm_copy(self, boot):
        """The mode is known before the boot sync writes an order, and the
        tick that follows the boot copies nothing over the parse."""
        seen, steps = boot(derive="own", utm="postgres")
        assert seen["parses_in_postgres"] is True and seen["owns"] is True
        assert steps["ship"].await_count == 0
        assert seen["tick"]["status"] == "success"

    def test_under_the_default_the_same_boot_ships_once(self, boot):
        """The control: the harness can see a ship when there is one."""
        seen, steps = boot()
        assert seen["parses_in_postgres"] is False
        assert steps["ship"].await_count == 1


# ─── /api/health and the canary ───────────────────────────────────────────────

class TestHealthPublishesIt:
    def test_the_block(self, modes):
        from web.routes.api.health import _utm_parse_mode

        modes()
        assert _utm_parse_mode() == {"mode": "duckdb", "error": None}
        modes(derive="own", utm="postgres")
        assert _utm_parse_mode() == {"mode": "postgres", "error": None}
        modes(utm="postgres")
        block = _utm_parse_mode()
        assert block["mode"] == "duckdb" and "KS_PG_DERIVE" in block["error"]

    def test_the_response_model_keeps_it(self):
        """`/api/health` has a response model, and a field it does not declare
        is dropped on the way out — the canary would read nothing."""
        from web.schemas import HealthResponse

        assert "utm_parse" in HealthResponse.model_fields

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

    def test_under_postgres_the_table_is_watched_with_its_own_limit(self, modes):
        modes(derive="own", utm="postgres")
        data = self._mirrors()
        assert set(data) == {"bronze.orders", "silver.orders",
                             "gold.daily_revenue", UTM}
        assert data[UTM]["max_age_s"] == pg_utm_parse.MAX_AGE_S

    def test_under_duckdb_it_is_not(self, modes):
        """The row is the ship's then, and the daily comparison judges it, as
        before; publishing it would page on a ship's cadence."""
        modes(derive="own")
        assert UTM not in self._mirrors()

    def test_the_limit_is_inside_the_canarys_ceiling(self):
        from bot import canary

        assert 0 < pg_utm_parse.MAX_AGE_S <= canary.DECLARED_MAX_AGE_CEILING_S


class TestTheCanaryJudgesIt:
    def test_an_error_is_a_failure_key(self):
        from bot.canary import check_utm_parse_mode

        payload = {"utm_parse": {"mode": "duckdb",
                                 "error": "KS_UTM_PARSE='pg' is not one of ..."}}
        assert [k for k, _ in check_utm_parse_mode(payload)] == ["utm_parse_mode_invalid"]

    def test_absent_or_clean_is_quiet(self):
        from bot.canary import check_utm_parse_mode

        assert check_utm_parse_mode({}) == []
        assert check_utm_parse_mode({"utm_parse": {"mode": "postgres", "error": None}}) == []

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

    @pytest.mark.asyncio
    async def test_the_wiring_warns_and_names_the_lever(self):
        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["utm_parse"] = {
            "mode": "duckdb",
            "error": "KS_UTM_PARSE='postgres' needs KS_PG_DERIVE='own', and it is "
                     "'piggyback'; running as 'duckdb'"}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "warn"
        assert result.failure_keys == ["utm_parse_mode_invalid"]
        assert "KS_UTM_PARSE" in canary._what_to_do(result)


class TestAnInvalidValueStartsWeb:
    """The startup itself: a typo in KS_UTM_PARSE, or `postgres` before own,
    must not be able to stop order intake. Then `/api/health`, which is where
    the canary reads it."""

    @pytest.mark.parametrize("derive,utm,said", [
        ("own", "postgress", "KS_UTM_PARSE='postgress'"),
        (None, "postgres", "KS_PG_DERIVE"),
    ])
    def test_it_runs_as_duckdb_with_a_health_error(self, boot, derive, utm, said):
        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter

        seen, steps = boot(derive=derive, utm=utm)
        assert seen["parses_in_postgres"] is False
        assert steps["ship"].await_count == 1, "the copy /traffic has always had"
        limiter.reset()
        try:
            published = TestClient(app).get("/api/health").json()["utm_parse"]
        finally:
            limiter.reset()
        assert published["mode"] == "duckdb"
        assert said in published["error"]
