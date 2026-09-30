"""DN-29, step 13b: `KS_WRITE_WAREHOUSE=postgres` stops DuckDB deriving.

The plumbing it stands on — the mode read in `configure_modes`, the stood-down
checks, the precondition evaluator — is DN-28's and is pinned in
`tests/unit/test_warehouse_cutover.py`. What is pinned here is the switch:

- the mode answers `postgres` only when every precondition holds, and keeps
  its verdict for the life of the process;
- under it nothing of DuckDB's derivation runs — no `warehouse_refresh` job, no
  call of `refresh_warehouse_layers` anywhere in production (walked, not
  listed), no dirty mark, no DuckDB half of rebuild-silver — and the DuckDB
  checks and the three mirror-landing comparisons stand down;
- the writer is recorded, the `warehouse` group is closed once, and the way
  back marks a full DuckDB rebuild and holds the checks until one validates.

The same promises against a real Postgres and the real boot are in
`tests/integration/test_warehouse_writer.py`.
"""
from __future__ import annotations

import ast
import asyncio
import json
import logging
import pathlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import warehouse_cutover as wc
from core.pg import REQUIRED_REVISION
# The derivation's own fixtures: a scheduler under KS_PG_DERIVE=own.
from tests.unit.test_pg_own_derivation import job, mode  # noqa: F401

REPO = pathlib.Path(__file__).resolve().parents[2]

# Every precondition of the switch met, as environment variables.
MET_ENV = {
    "KS_PG_DERIVE": "own",
    "KS_DQ_PG_WAREHOUSE": "on",
    "KS_UTM_PARSE": "postgres",
    "KS_READ_FALLBACK": "off",
    "KS_PG_DSN": "postgresql://ks_app:x@db:5432/ks",
    **{name: "postgres" for name in wc.WAREHOUSE_READERS},
    "KS_READ_COHORTS": "clickhouse",
    "KS_CH_URL": "http://ch:8123",
}


@pytest.fixture(autouse=True)
def _other_modes_put_back(monkeypatch):
    """`configure_modes()` and the admin routes set the other cached modes;
    recording them here has monkeypatch put them back after the test."""
    from core import pg_derivation, pg_utm_parse, read_fallback

    for module, names in ((pg_derivation, ("_mode", "_mode_error")),
                          (pg_utm_parse, ("_mode", "_mode_error")),
                          (read_fallback, ("_mode", "_mode_error", "_misconfigured"))):
        for name in names:
            monkeypatch.setattr(module, name, getattr(module, name))


@pytest.fixture
def met(monkeypatch):
    """The environment with every precondition met and the revision read on
    its own connection answering the one this build requires. Returns the
    reader, so a test can count or change what it answers."""
    for name, value in MET_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
    reader = AsyncMock(return_value=REQUIRED_REVISION)
    monkeypatch.setattr(wc, "_revision_on_its_own_connection", reader)
    # A read that failed is asked again; not across seconds in a unit test.
    monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (0.0, 0.0))
    # A build where OD-10 has been answered; today's has not (TestTheOd10Doors).
    monkeypatch.setattr(wc, "OD10_DOORS", ())
    return reader


@pytest.fixture
def postgres(met, monkeypatch):
    """A process configured under `postgres` with everything met."""
    monkeypatch.setenv(wc.ENV, "postgres")
    assert wc.configure_mode() == wc.POSTGRES
    return met


def _store(tmp_path, name="w.duckdb"):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / name)
    asyncio.run(store.connect())
    return store


def _metadata(store, key):
    async def read():
        async with store.connection() as conn:
            return conn.execute(
                "SELECT value, updated_at FROM sync_metadata WHERE key = ?", [key]
            ).fetchone()
    return asyncio.run(read())


def _restart():
    """What a new process has: nothing read, nothing settled."""
    for name, value in (("_value", None), ("_mode", None), ("_mode_error", None),
                        ("_unmet", ()), ("_decided_for", None), ("_settled", False),
                        ("_held", False), ("_reclassify_needed", False),
                        ("_writer_record", None), ("_settle_lock", None)):
        setattr(wc, name, value)


# ─── The mode ────────────────────────────────────────────────────────────────


class TestTheMode:
    def test_every_precondition_met_is_postgres(self, postgres):
        assert wc.writes_postgres() and not wc.duckdb_derives()
        assert wc.preconditions_unmet() == ()
        postgres.assert_awaited_once_with(MET_ENV["KS_PG_DSN"])

    def test_one_unmet_runs_as_duckdb_and_names_it(self, met, monkeypatch, caplog):
        """OD-09 (b): never a raise — web is the only syncer."""
        monkeypatch.setenv(wc.ENV, "postgres")
        monkeypatch.setenv("KS_UTM_PARSE", "duckdb")
        with caplog.at_level(logging.CRITICAL, logger="core.warehouse_cutover"):
            assert wc.configure_mode() == wc.DUCKDB
        assert wc.duckdb_derives() and wc.stood_down_duckdb_checks() == frozenset()
        assert [u.key for u in wc.preconditions_unmet()] == ["utm_parse_postgres"]
        assert "utm_parse_postgres" in caplog.text

    def test_a_revision_behind_the_build_runs_as_duckdb(self, met, monkeypatch):
        monkeypatch.setenv(wc.ENV, "postgres")
        met.return_value = "0032_manual_goal_ids"
        assert wc.configure_mode() == wc.DUCKDB
        assert [u.key for u in wc.preconditions_unmet()] == ["pg_revision"]

    def test_a_revision_read_that_hangs_is_bounded_and_unmet(self, met, monkeypatch):
        async def hang(_dsn):
            await asyncio.sleep(10)

        monkeypatch.setattr(wc, "_revision_on_its_own_connection", hang)
        monkeypatch.setattr(wc, "REVISION_READ_TIMEOUT_S", 0.05)
        monkeypatch.setenv(wc.ENV, "postgres")
        assert wc.configure_mode() == wc.DUCKDB
        (unmet,) = wc.preconditions_unmet()
        assert unmet.key == "pg_revision" and "did not answer" in unmet.detail

    def test_a_worker_that_outlives_the_bound_does_not_hold_the_caller(
        self, met, monkeypatch,
    ):
        """What a name that does not resolve does: `asyncio.run` waits for the
        resolver thread past the read's own bound. Modelled as a gather that
        blocks its thread; the caller returns at twice the bound, unmet."""
        import threading
        import time

        release = threading.Event()
        asked = []

        async def stuck(env, *, own_connection=False):
            asked.append(own_connection)
            release.wait(5)
            return {"revision": REQUIRED_REVISION, "revision_error": None,
                    "unread": False}

        monkeypatch.setattr(wc, "_read_postgres", stuck)
        monkeypatch.setattr(wc, "REVISION_READ_TIMEOUT_S", 0.05)
        monkeypatch.setenv(wc.ENV, "postgres")
        started = time.monotonic()
        try:
            assert wc.configure_mode() == wc.DUCKDB
        finally:
            release.set()
        assert time.monotonic() - started < 2
        # A worker left stuck is a read that failed, and is asked again.
        assert asked == [True] * wc.REVISION_READ_ATTEMPTS
        # Only what was not read: the registry and the Alert Gate are this
        # process's own, read on the caller's thread, and nobody failed them.
        assert [u.key for u in wc.preconditions_unmet()] == ["pg_revision"]

    def test_a_read_that_failed_is_asked_again_before_the_verdict(self, met, monkeypatch):
        """After a flip a start that runs as duckdb IS the way back — a full
        DuckDB rebuild, a new `since`, the warehouse group closed again. A
        Postgres a few seconds slower than web to come up must not cost that."""
        met.side_effect = [OSError("refused"), asyncio.TimeoutError(), REQUIRED_REVISION]
        monkeypatch.setenv(wc.ENV, "postgres")
        assert wc.configure_mode() == wc.POSTGRES
        assert met.await_count == 3 and wc.preconditions_unmet() == ()

    def test_a_read_still_failing_after_the_last_ask_is_unmet(self, met, monkeypatch):
        met.side_effect = OSError("refused")
        monkeypatch.setenv(wc.ENV, "postgres")
        assert wc.configure_mode() == wc.DUCKDB
        assert met.await_count == wc.REVISION_READ_ATTEMPTS == 3
        assert [u.key for u in wc.preconditions_unmet()] == ["pg_revision"]

    def test_an_answer_is_not_asked_again(self, met, monkeypatch):
        """A revision this build does not require is an answer; asking again
        would only hold the start."""
        met.return_value = "0032_manual_goal_ids"
        monkeypatch.setenv(wc.ENV, "postgres")
        assert wc.configure_mode() == wc.DUCKDB
        met.assert_awaited_once()

    def test_it_waits_between_asks_and_is_bounded(self, met, monkeypatch):
        slept = []
        monkeypatch.setattr("time.sleep", slept.append)
        monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (2.0, 5.0))
        met.side_effect = OSError("refused")
        monkeypatch.setenv(wc.ENV, "postgres")
        wc.configure_mode()
        assert slept == [2.0, 5.0]
        assert wc.start_bound_s() == 3 * 2 * wc.REVISION_READ_TIMEOUT_S + 7.0 <= 40

    def test_it_can_be_configured_from_inside_a_running_loop(self, met, monkeypatch):
        """Where web calls it: `startup_event` is a coroutine. The revision is
        read on a loop of the worker's own, never the caller's."""
        monkeypatch.setenv(wc.ENV, "postgres")

        async def startup():
            return wc.configure_mode()

        assert asyncio.run(startup()) == wc.POSTGRES

    def test_the_verdict_is_kept_for_the_process(self, postgres):
        """web's startup and the scheduler both configure. A second read that
        found Postgres slow must not hand a boot that ran under postgres a
        scheduler that runs under duckdb."""
        postgres.side_effect = OSError("gone")
        assert wc.configure_mode() == wc.POSTGRES
        assert wc.configure_mode() == wc.POSTGRES
        postgres.assert_awaited_once()

    def test_the_default_asks_nothing(self, met, monkeypatch):
        monkeypatch.delenv(wc.ENV, raising=False)
        assert wc.configure_mode() == wc.DUCKDB
        met.assert_not_awaited()
        assert wc.preconditions_unmet() == ()

    def test_configure_modes_reaches_it(self, met, monkeypatch):
        from core.runtime_modes import configure_modes

        monkeypatch.setenv(wc.ENV, "postgres")
        assert configure_modes()[wc.ENV] == wc.POSTGRES


class TestWhatStandsDown:
    def test_under_postgres_the_checks_and_the_comparisons(self, postgres):
        assert wc.warehouse_checks_stand_down()
        assert wc.stood_down_duckdb_checks() == wc.STOOD_DOWN_WHEN_POSTGRES

    def test_held_on_the_way_back_while_duckdb_derives(self, monkeypatch):
        monkeypatch.setattr(wc, "_held", True)
        assert wc.duckdb_derives() and wc.warehouse_checks_stand_down()
        assert wc.stood_down_duckdb_checks() == wc.STOOD_DOWN_WHEN_POSTGRES

    def test_the_default_stands_nothing_down(self):
        assert wc.duckdb_derives() and not wc.warehouse_checks_stand_down()
        assert wc.stood_down_duckdb_checks() == frozenset()

    def test_a_held_scan_skips_the_checks_over_frozen_tables(self, tmp_path, monkeypatch):
        from core import data_quality as dq

        monkeypatch.setattr(wc, "_held", True)
        for fn in ("_silver_arc_check", "_attribution_coverage_check",
                   "_gold_cell_values_check", "_headline_vs_line_items_check",
                   "_goods_shipped_without_sale_check"):
            monkeypatch.setattr(dq, fn, MagicMock(side_effect=AssertionError(fn)))
        store = _store(tmp_path)
        try:
            async def scan():
                async with store.connection() as conn:
                    return dq.check_internal_integrity(conn)
            issues = asyncio.run(scan())
        finally:
            asyncio.run(store.close())
        assert "integrity_check_raised" not in {i.check_name for i in issues}


# ─── Nothing of DuckDB's derivation runs under postgres ──────────────────────


class TestTheDirtyMark:
    def test_under_postgres_it_writes_nothing(self, tmp_path, postgres):
        store = _store(tmp_path)
        try:
            asyncio.run(store.mark_warehouse_dirty([1, 2]))
            asyncio.run(store.mark_warehouse_dirty(None))
            assert _metadata(store, "warehouse_dirty") is None
        finally:
            asyncio.run(store.close())

    def test_the_default_marks_as_before(self, tmp_path):
        store = _store(tmp_path)
        try:
            asyncio.run(store.mark_warehouse_dirty([1, 2]))
            assert json.loads(_metadata(store, "warehouse_dirty")[0]) == [1, 2]
        finally:
            asyncio.run(store.close())


def _jobs():
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


class TestTheScheduler:
    def test_under_postgres_the_refresh_job_is_not_registered(self, postgres):
        jobs = _jobs()
        assert "warehouse_refresh" not in jobs and "incremental_sync" in jobs

    def test_the_default_registers_it(self):
        assert "warehouse_refresh" in _jobs()

    def test_a_tick_that_finds_its_way_in_returns_before_the_peek(self, postgres):
        from core.scheduler import BackgroundScheduler

        store = MagicMock()
        store.peek_warehouse_dirty = AsyncMock(side_effect=AssertionError("peeked"))
        with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)):
            result = asyncio.run(BackgroundScheduler()._run_warehouse_refresh())
        assert result == {"skipped": True, "reason": "KS_WRITE_WAREHOUSE=postgres"}

    def test_the_start_settles_before_it_registers(self):
        """So a scheduler started without web's boot — or after a boot whose
        settle raised — marks the way back before the refresh job exists."""
        from core.scheduler import BackgroundScheduler

        tree = ast.parse((REPO / "core/scheduler.py").read_text(encoding="utf-8"))
        (start,) = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
                    and n.name == "start"]
        order = [c.func.attr for c in ast.walk(start) if isinstance(c, ast.Call)
                 and isinstance(c.func, ast.Attribute)
                 and c.func.attr in ("_settle_warehouse_writer", "_register_jobs")]
        assert order[:2] == ["_settle_warehouse_writer", "_register_jobs"]
        assert callable(BackgroundScheduler._settle_warehouse_writer)


class TestTheMirrorLandingComparisons:
    """`reconcile_silver`, `reconcile_order_utm` and `reconcile_gold` set
    DuckDB's copies against Postgres', and stand down when DuckDB's are
    frozen; the Gold roll-up question is asked of Postgres alone instead."""

    COMPARED = ("reconcile_silver", "reconcile_order_utm", "reconcile_gold")

    @staticmethod
    def _run():
        from tests.unit.test_mirror_landing_isolation import _run

        mocks = {}
        asyncio.run(_run({}, mocks=mocks))
        return mocks

    def test_under_postgres(self, postgres):
        mocks = self._run()
        for name in self.COMPARED:
            mocks[name].assert_not_awaited()
        mocks["pg_gold_internal_check"].assert_awaited_once_with()
        mocks["reconcile_orders"].assert_awaited_once()

    def test_held_on_the_way_back(self, monkeypatch):
        monkeypatch.setattr(wc, "_held", True)
        mocks = self._run()
        for name in self.COMPARED:
            mocks[name].assert_not_awaited()
        mocks["pg_gold_internal_check"].assert_awaited_once_with()

    def test_the_default_compares(self):
        mocks = self._run()
        for name in self.COMPARED:
            mocks[name].assert_awaited_once()
        mocks["pg_gold_internal_check"].assert_not_awaited()


# ─── The admin levers ────────────────────────────────────────────────────────


def _unwrap(fn):
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


class _Scheduler:
    def __init__(self):
        self._heavy_job_lock = asyncio.Lock()
        self.derived = []

    async def _run_pg_derivation(self, *, trigger=None, force=False):
        self.derived.append((trigger, force))
        return {"status": "success", "trigger": trigger}


class TestTheAdminLevers:
    def _call(self, route):
        from core import pg_derivation
        from web.routes.api import admin

        pg_derivation.configure_mode()   # put back by `_other_modes_put_back`
        scheduler = _Scheduler()
        store = MagicMock()
        store.refresh_warehouse_layers = AsyncMock(return_value={"status": "success"})
        store.connection = MagicMock(side_effect=AssertionError("DuckDB touched"))
        store.mark_warehouse_dirty = AsyncMock()
        with patch("core.scheduler.get_scheduler", return_value=scheduler), \
             patch.object(admin, "get_store", AsyncMock(return_value=store)):
            result = asyncio.run(_unwrap(getattr(admin, route))(MagicMock(), admin={}))
        return result, store, scheduler

    def test_refresh_under_postgres_derives_postgres_alone(self, postgres):
        result, store, scheduler = self._call("refresh_warehouse")
        store.refresh_warehouse_layers.assert_not_awaited()
        assert result["status"] == "skipped" and "KS_WRITE_WAREHOUSE" in result["reason"]
        assert scheduler.derived == [("manual", True)]

    def test_rebuild_silver_under_postgres_skips_its_duckdb_half(self, postgres):
        result, store, scheduler = self._call("rebuild_silver_from_scratch")
        store.mark_warehouse_dirty.assert_not_awaited()
        assert result["status"] == "skipped"
        assert scheduler.derived == [("manual", True)]

    def test_refresh_under_the_default_is_unchanged(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DERIVE", raising=False)
        result, store, scheduler = self._call("refresh_warehouse")
        store.refresh_warehouse_layers.assert_awaited_once_with(trigger="manual")
        assert result == {"status": "success"} and scheduler.derived == []


# ─── The sync paths ──────────────────────────────────────────────────────────


class _Client:
    """KeyCRM with nothing in its catalogue and one order — served to the
    status refresh, which pages `order` itself instead of asking the date
    filter the other two paths are stubbed through."""

    async def paginate(self, endpoint, params=None, page_size=50):
        if endpoint == "order":
            yield [{"id": 1, "status_id": 1}]


class _FullSyncStore:
    """What `full_sync` asks of the store. The derivation it must not reach
    raises; everything else answers."""

    def __init__(self):
        self.refreshed = []

    async def upsert_categories(self, rows):
        return 0

    async def upsert_expense_types(self, rows):
        return 0

    async def upsert_products(self, rows):
        return 0

    async def set_last_sync_time(self, key, timestamp=None):
        return None

    async def refresh_sku_inventory_status(self):
        return 0

    async def record_sku_inventory_snapshot(self):
        return False

    async def refresh_warehouse_layers(self, trigger=None, changed_order_ids=None):
        self.refreshed.append(trigger)
        return {"status": "success"}

    async def get_latest_order_time(self):
        return None

    async def checkpoint(self):
        return None


def _service(store, orders=()):
    from core import sync_service as sync_module

    service = sync_module.SyncService(store)
    service.sync_managers = AsyncMock(return_value=0)
    service.sync_offers = AsyncMock(return_value=0)
    service.sync_stocks = AsyncMock(return_value=0)
    service._fetch_orders_with_date_filter = AsyncMock(return_value=list(orders))
    service._upsert_orders_with_expenses = AsyncMock(return_value=(len(orders), 0))
    return service


def _sync(coro_fn):
    from core import sync_service as sync_module

    async def client():
        return _Client()

    with patch.object(sync_module, "get_async_client", new=client), \
         patch.object(sync_module, "mirror_categories", new=AsyncMock()), \
         patch.object(sync_module, "mirror_products", new=AsyncMock()):
        return asyncio.run(coro_fn())


class TestTheSyncPaths:
    """The three production calls in `core/sync_service.py`. The walk below
    finds them by shape; these prove the shape does what it says."""

    @pytest.mark.parametrize("path", ["full_sync", "sync_today", "refresh_order_statuses"])
    def test_under_postgres_none_of_them_refreshes(self, postgres, path):
        store = _FullSyncStore()
        service = _service(store, orders=[{"id": 1, "status_id": 1}])
        _sync(lambda: getattr(service, path)(**({"days_back": 1} if path != "sync_today" else {})))
        assert store.refreshed == []

    @pytest.mark.parametrize("path, trigger", [
        ("full_sync", "full_sync"), ("sync_today", "sync_today"),
        ("refresh_order_statuses", "status_refresh")])
    def test_the_default_refreshes_as_before(self, path, trigger):
        store = _FullSyncStore()
        service = _service(store, orders=[{"id": 1, "status_id": 1}])
        _sync(lambda: getattr(service, path)(**({"days_back": 1} if path != "sync_today" else {})))
        assert store.refreshed == [trigger]

    def test_the_boot_settles_before_it_reads_or_syncs(self, monkeypatch):
        from core import sync_service as sync_module

        calls = []
        store = MagicMock()
        store.get_stats = AsyncMock(side_effect=lambda: calls.append("stats") or {
            "orders": 1, "products": 1})
        store.refresh_sku_inventory_status = AsyncMock(return_value=0)
        service = MagicMock()
        service.incremental_sync = AsyncMock(side_effect=lambda: calls.append("sync"))
        monkeypatch.setattr(sync_module, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(sync_module, "get_sync_service", AsyncMock(return_value=service))
        monkeypatch.setattr(sync_module, "init_meilisearch", AsyncMock(return_value=False))
        monkeypatch.setattr(wc, "settle_writer",
                            AsyncMock(side_effect=lambda s: calls.append("settle")))
        asyncio.run(sync_module.init_and_sync())
        assert calls == ["settle", "stats", "sync"]

    def test_a_settle_that_raises_costs_the_boot_nothing(self, monkeypatch, caplog):
        from core import sync_service as sync_module

        store = MagicMock()
        store.get_stats = AsyncMock(return_value={"orders": 1, "products": 1})
        store.refresh_sku_inventory_status = AsyncMock(return_value=0)
        service = MagicMock()
        service.incremental_sync = AsyncMock()
        monkeypatch.setattr(sync_module, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(sync_module, "get_sync_service", AsyncMock(return_value=service))
        monkeypatch.setattr(sync_module, "init_meilisearch", AsyncMock(return_value=False))
        monkeypatch.setattr(wc, "settle_writer", AsyncMock(side_effect=OSError("disk")))
        asyncio.run(sync_module.init_and_sync())
        service.incremental_sync.assert_awaited_once()
        assert "not settled at boot" in caplog.text


# ─── No production call of refresh_warehouse_layers bypasses the predicate ───

PRODUCTION_ROOTS = ("core", "web", "bot", "scripts", "deploy")
PREDICATE = "duckdb_derives"


def _name(node) -> "str | None":
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _asks(test) -> bool:
    """`duckdb_derives()`, however it was reached."""
    return isinstance(test, ast.Call) and _name(test.func) == PREDICATE


def _asks_not(test) -> bool:
    return (isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not)
            and _asks(test.operand))


def _returns(body) -> bool:
    return bool(body) and isinstance(body[-1], (ast.Return, ast.Raise))


def _guarded_calls(tree: ast.Module, callee: str):
    """`(lineno, guarded)` for every call of `callee` in the module.

    Guarded is one of three shapes, and only these: the call sits in the
    body of an `if <...>.duckdb_derives():` (never its else), or in the else
    of an `if not <...>.duckdb_derives():`, or the function it sits in
    returns under `if not <...>.duckdb_derives():` in a statement of its own
    body that comes before the one holding the call."""
    return [(line, guarded) for line, guarded, _fn in _guarded_nodes(
        tree, lambda node: isinstance(node, ast.Call) and _name(node.func) == callee)]


def _guarded_nodes(tree: ast.Module, match):
    """`(lineno, guarded, enclosing function name or None)` for every node
    `match` accepts, guarded as `_guarded_calls` says."""
    found = []

    def visit(node, ifs, fn):
        for field, value in ast.iter_fields(node):
            children = value if isinstance(value, list) else [value]
            for child in children:
                if not isinstance(child, ast.AST):
                    continue
                inner_ifs = ifs
                if isinstance(node, ast.If) and (
                        (field == "body" and _asks(node.test))
                        or (field == "orelse" and _asks_not(node.test))):
                    inner_ifs = ifs + (node,)
                inner_fn = child if isinstance(child, (ast.FunctionDef,
                                                       ast.AsyncFunctionDef)) else fn
                if match(child):
                    found.append((child.lineno, bool(inner_ifs) or _early_return(fn, child),
                                  fn.name if fn is not None else None))
                visit(child, inner_ifs if inner_fn is fn else (), inner_fn)

    def _early_return(fn, call) -> bool:
        if fn is None:
            return False
        for stmt in fn.body:
            if stmt.lineno <= call.lineno <= (stmt.end_lineno or stmt.lineno) \
                    and not (isinstance(stmt, ast.If) and _asks_not(stmt.test)):
                return False
            if isinstance(stmt, ast.If) and _asks_not(stmt.test) and _returns(stmt.body):
                return True
        return False

    visit(tree, (), None)
    return found


def _production_calls(callee: str):
    out = []
    for root in PRODUCTION_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for lineno, guarded in _guarded_calls(tree, callee):
                out.append((f"{path.relative_to(REPO)}:{lineno}", guarded))
    return out


class TestEveryRefreshStandsBehindThePredicate:
    def test_no_production_call_bypasses_it(self):
        calls = _production_calls("refresh_warehouse_layers")
        assert calls, "the walk found no call at all — is it looking?"
        bypass = [where for where, guarded in calls if not guarded]
        assert bypass == [], (
            f"{bypass} call refresh_warehouse_layers without asking "
            "warehouse_cutover.duckdb_derives(): under KS_WRITE_WAREHOUSE=postgres "
            "DuckDB would derive again beside Postgres")

    def test_the_walk_finds_the_sites_the_plan_named(self):
        """sync_service's three, admin's one, and the scheduler's tick — a
        walk that found none of them would pass on a codebase it cannot read."""
        where = {w.rsplit(":", 1)[0] for w, _ in
                 _production_calls("refresh_warehouse_layers")}
        assert {"core/sync_service.py", "web/routes/api/admin.py",
                "core/scheduler.py"} <= where
        assert len(_production_calls("refresh_warehouse_layers")) == 5

    @pytest.mark.parametrize("source, guarded", [
        ("async def f(s):\n    await s.refresh_warehouse_layers()\n", False),
        ("async def f(s):\n    if wc.duckdb_derives():\n"
         "        await s.refresh_warehouse_layers()\n", True),
        ("async def f(s):\n    if wc.duckdb_derives():\n        pass\n"
         "    else:\n        await s.refresh_warehouse_layers()\n", False),
        ("async def f(s):\n    if not wc.duckdb_derives():\n        return 1\n"
         "    await s.refresh_warehouse_layers()\n", True),
        ("async def f(s):\n    if not wc.duckdb_derives():\n        log()\n"
         "    await s.refresh_warehouse_layers()\n", False),
        ("async def f(s):\n    await s.refresh_warehouse_layers()\n"
         "    if not wc.duckdb_derives():\n        return 1\n", False),
        ("async def f(s):\n    if wc.writes_postgres():\n"
         "        await s.refresh_warehouse_layers()\n", False),
        ("async def f(s):\n    if wc.duckdb_derives():\n"
         "        async def g():\n            await s.refresh_warehouse_layers()\n", False),
        ("async def f(s):\n    if not wc.duckdb_derives():\n        pass\n"
         "    else:\n        await s.refresh_warehouse_layers()\n", True),
        ("async def f(s):\n    if not wc.duckdb_derives():\n"
         "        await s.refresh_warehouse_layers()\n", False),
    ])
    def test_the_walk_reads_each_shape(self, source, guarded):
        ((_line, found),) = _guarded_calls(ast.parse(source), "refresh_warehouse_layers")
        assert found is guarded


# The tick is the one DuckDB UTM parse the refresh walk above already guards —
# every call of `refresh_warehouse_layers` stands behind the predicate.
UTM_TICK = "refresh_warehouse_layers"


def _deletes_duckdb_utm(node) -> bool:
    return (isinstance(node, ast.Constant) and isinstance(node.value, str)
            and "DELETE FROM silver_order_utm" in node.value)


def _parses_duckdb_utm(node) -> bool:
    return isinstance(node, ast.Call) and _name(node.func) == "refresh_utm_silver_layer"


def _production_nodes(match):
    out = []
    for root in PRODUCTION_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for lineno, guarded, fn in _guarded_nodes(tree, match):
                out.append((f"{path.relative_to(REPO)}:{lineno}", guarded, fn))
    return out


class TestEveryDuckdbUtmParseStandsBehindThePredicate:
    """Under postgres DuckDB derives no UTM verdict at all — not in the tick,
    and not at the four doors that re-parse outside it. Walked, like the
    refresh, over every production root."""

    def test_no_door_parses_or_empties_duckdb_utm_under_postgres(self):
        found = [(where, guarded) for where, guarded, fn in
                 _production_nodes(lambda n: _parses_duckdb_utm(n) or _deletes_duckdb_utm(n))
                 if fn != UTM_TICK]
        bypass = [where for where, guarded in found if not guarded]
        assert bypass == [], (
            f"{bypass} parse or empty DuckDB's silver_order_utm without asking "
            "warehouse_cutover.duckdb_derives(): under KS_WRITE_WAREHOUSE=postgres "
            "a DuckDB failure there would stand in front of the table /traffic reads")

    def test_the_walk_finds_the_doors(self):
        """Three routes, the CLI, and the way back's own DELETE — a walk that
        found none would pass on a codebase it cannot read."""
        parses = {where.rsplit(":", 1)[0] for where, _g, fn in
                  _production_nodes(_parses_duckdb_utm) if fn != UTM_TICK}
        assert parses == {"web/routes/api/traffic.py", "scripts/backfill_utm.py"}
        assert len([1 for _w, _g, fn in _production_nodes(_parses_duckdb_utm)
                    if fn != UTM_TICK]) == 4
        deletes = {where.rsplit(":", 1)[0] for where, _g, _fn in
                   _production_nodes(_deletes_duckdb_utm)}
        assert deletes == {"web/routes/api/traffic.py", "scripts/backfill_utm.py",
                           "core/warehouse_cutover.py"}
        (tick,) = [fn for _w, _g, fn in _production_nodes(_parses_duckdb_utm)
                   if fn == UTM_TICK]
        assert tick == UTM_TICK


# ─── The recorded writer ─────────────────────────────────────────────────────


def _writer(store):
    row = _metadata(store, wc.WRITER_KEY)
    return None if row is None else json.loads(row[0])


class TestTheRecordedWriter:
    def test_the_default_records_nothing_and_marks_nothing(self, tmp_path):
        store = _store(tmp_path)
        try:
            asyncio.run(wc.settle_writer(store))
            assert _writer(store) is None
            assert _metadata(store, "warehouse_dirty") is None
            assert not wc.held()
        finally:
            asyncio.run(store.close())

    def test_the_first_start_under_postgres_records_it_and_closes_the_group(
        self, tmp_path, postgres,
    ):
        store = _store(tmp_path)
        resolve = AsyncMock(return_value=1)
        try:
            with patch("core.alerting.resolve_group", resolve):
                asyncio.run(wc.settle_writer(store))
            record = _writer(store)
            assert record["writer"] == "postgres" and record["resolved"] is True
            resolve.assert_awaited_once_with("warehouse", note=wc.RESOLVE_NOTE)
        finally:
            asyncio.run(store.close())

    def test_the_resolve_runs_once_across_two_restarts(self, tmp_path, postgres):
        store = _store(tmp_path)
        resolve = AsyncMock(return_value=1)
        try:
            with patch("core.alerting.resolve_group", resolve):
                for _ in range(3):
                    _restart()
                    assert wc.configure_mode() == wc.POSTGRES
                    asyncio.run(wc.settle_writer(store))
            assert resolve.await_count == 1
        finally:
            asyncio.run(store.close())

    def test_a_resolve_the_ledger_could_not_record_is_tried_again(
        self, tmp_path, postgres,
    ):
        """`resolve_group` gives the keys back to the gate when the ledger
        refuses them; the next start must not believe it is done."""
        from core.alerting import _gate

        store = _store(tmp_path)
        try:
            _gate.note_delivered_conditions(["warehouse:validation_retrying"], "warehouse")
            with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=False)):
                asyncio.run(wc.settle_writer(store))
            assert _writer(store)["resolved"] is False
            _restart()
            wc.configure_mode()
            with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
                 patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
                asyncio.run(wc.settle_writer(store))
            assert _writer(store)["resolved"] is True
            (text,) = send.await_args.args[:1]
            assert "warehouse:validation_retrying" in text and wc.RESOLVE_NOTE in text
        finally:
            asyncio.run(store.close())

    def test_the_writer_is_recorded_before_the_group_is_resolved(
        self, tmp_path, postgres, monkeypatch,
    ):
        """A resolve that raises must still leave the way back armed: the
        record is what a later start under duckdb reads to owe a full
        rebuild, and DuckDB has stopped deriving from this start on."""
        store = _store(tmp_path)
        try:
            with patch("core.alerting.resolve_group",
                       AsyncMock(side_effect=RuntimeError("ledger gone"))):
                asyncio.run(wc.settle_writer(store))
            assert _writer(store) == {**_writer(store), "writer": "postgres",
                                      "resolved": False}
            _restart()
            monkeypatch.delenv(wc.ENV)
            wc.configure_mode()
            asyncio.run(wc.settle_writer(store))
            assert wc.held() and _metadata(store, "warehouse_dirty")[0] == "full"
        finally:
            asyncio.run(store.close())

    def test_the_way_back_marks_a_full_rebuild_and_holds_the_checks(
        self, tmp_path, postgres, monkeypatch,
    ):
        store = _store(tmp_path)
        try:
            with patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                asyncio.run(wc.settle_writer(store))
            assert _writer(store)["writer"] == "postgres"

            # The id list pending at the switch, abandoned as it stands.
            async def seed():
                async with store.connection() as conn:
                    conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)"
                                 " VALUES ('warehouse_dirty', '[7, 8]', CURRENT_TIMESTAMP)")
            asyncio.run(seed())
            _restart()
            monkeypatch.delenv(wc.ENV)
            assert wc.configure_mode() == wc.DUCKDB
            asyncio.run(wc.settle_writer(store))
            assert _metadata(store, "warehouse_dirty")[0] == "full"
            assert wc.held() and wc.warehouse_checks_stand_down()
            assert wc.reclassify_needed(), "silver_order_utm is empty"
            assert _writer(store)["writer"] == "postgres", "released before a tick validated"
        finally:
            asyncio.run(store.close())

    def test_a_non_empty_utm_table_needs_no_reclassify(self, tmp_path, monkeypatch):
        store = _store(tmp_path)
        try:
            async def seed():
                async with store.connection() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                        "VALUES (?, ?, CURRENT_TIMESTAMP)",
                        [wc.WRITER_KEY, json.dumps({"writer": "postgres", "resolved": True})])
                    conn.execute("INSERT INTO silver_order_utm (order_id) VALUES (1)")
            asyncio.run(seed())
            asyncio.run(wc.settle_writer(store))
            assert wc.held() and not wc.reclassify_needed()
        finally:
            asyncio.run(store.close())

    def test_a_value_this_build_did_not_write_is_read_as_postgres(self, tmp_path, caplog):
        """The cautious reading costs one full rebuild; the other skips one."""
        store = _store(tmp_path)
        try:
            async def seed():
                async with store.connection() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                        "VALUES (?, 'postgres', CURRENT_TIMESTAMP)", [wc.WRITER_KEY])
            asyncio.run(seed())
            asyncio.run(wc.settle_writer(store))
            assert wc.held() and _metadata(store, "warehouse_dirty")[0] == "full"
            assert "did not write" in caplog.text
        finally:
            asyncio.run(store.close())

    def test_settling_twice_in_one_process_does_it_once(self):
        store = MagicMock()
        store.connection = MagicMock(side_effect=AssertionError("read twice"))
        wc._settled = True
        asyncio.run(wc.settle_writer(store))


def _orders(*ids, comment=None):
    return [{
        "id": oid, "source_id": 1, "status_id": 12, "grand_total": "100.00",
        "ordered_at": "2026-04-01T10:00:00+00:00",
        "created_at": "2026-04-01T09:00:00+00:00",
        "updated_at": "2026-04-01T10:00:00+00:00",
        "buyer": None, "manager": None, "manager_comment": comment, "promocode": None,
        "products": [{"id": oid * 1000 + 1, "product_id": 1, "name": "p",
                      "quantity": 1, "price_sold": "100.00"}],
    } for oid in ids]


class TestTheHoldEndsOnAValidatedFullTick:
    @pytest.fixture
    def held_store(self, tmp_path):
        store = _store(tmp_path)
        asyncio.run(store.upsert_orders(_orders(1, 2, comment="utm_source=instagram")))

        async def seed():
            async with store.connection() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                    "VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [wc.WRITER_KEY, json.dumps({"writer": "postgres", "resolved": True})])
        asyncio.run(seed())
        asyncio.run(wc.settle_writer(store))
        assert wc.held()
        yield store
        asyncio.run(store.close())

    def test_a_full_tick_that_validates_releases_it(self, held_store):
        result = asyncio.run(held_store.refresh_warehouse_layers(trigger="dirty_flag"))
        assert result["validation_passed"] is True
        assert not wc.held() and not wc.warehouse_checks_stand_down()
        assert _writer(held_store)["writer"] == "duckdb"
        assert not wc.reclassify_needed(), "the tick's own parse refilled the verdicts"

    def test_an_incremental_tick_does_not(self, held_store):
        """Only a full rebuild answers for the whole of a Silver as old as the
        switch; an incremental one rewrites the orders it was handed."""
        asyncio.run(held_store.refresh_warehouse_layers(trigger="manual"))
        assert not wc.held()   # Silver is populated now

        async def seed():
            async with held_store.connection() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                    "VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [wc.WRITER_KEY, json.dumps({"writer": "postgres", "resolved": True})])
        asyncio.run(seed())
        _restart()
        asyncio.run(wc.settle_writer(held_store))
        assert wc.held()
        result = asyncio.run(held_store.refresh_warehouse_layers(
            trigger="dirty_flag", changed_order_ids=[1]))
        assert result["validation_passed"] is True
        assert wc.held() and _writer(held_store)["writer"] == "postgres"

    def test_a_tick_that_fails_validation_does_not(self, held_store):
        released = asyncio.run(wc.note_refresh(
            held_store, silver_mode="full", validation_passed=False))
        assert released is False and wc.held()


# ─── The retired Gold comparison's own conditions hold the switch ────────────


class TestTheRetiredComparisonsConditions:
    def test_they_are_what_compare_gold_names(self):
        """Read out of the source: a name `compare_gold` learns later is a
        page the first run after the switch would announce resolved."""
        tree = ast.parse((REPO / "core/mirror_reconciliation.py").read_text(encoding="utf-8"))
        (fn,) = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name == "compare_gold"]
        named = {kw.value.value for n in ast.walk(fn) if isinstance(n, ast.Call)
                 for kw in n.keywords
                 if kw.arg == "check_name" and isinstance(kw.value, ast.Constant)}
        assert named == wc.RETIRED_COMPARISON_CONDITIONS
        assert "gold_rollup_mismatch" not in wc.retired_conditions()

    def test_an_open_page_under_one_holds_the_switch(self):
        from core.alerting import _gate

        _gate.note_delivered_conditions(["gold_missing_cells"], "dq:mirror_landing")
        facts = asyncio.run(wc.gather_facts({}))
        assert facts.open_retired == {"gold_missing_cells": "dq:mirror_landing"}


# ─── Published, and judged ───────────────────────────────────────────────────


class TestPublishedAndJudged:
    def test_health_publishes_the_keys_and_never_the_details(self, met, monkeypatch):
        from web.routes.api.health import _warehouse_writer_mode

        monkeypatch.setenv(wc.ENV, "postgres")
        monkeypatch.setenv("KS_READ_GOLD", "duckdb")
        wc.configure_mode()
        block = _warehouse_writer_mode()
        assert block["mode"] == "duckdb"
        assert block["preconditions_unmet"] == ["reader:KS_READ_GOLD"]
        assert "KS_READ_GOLD is" not in repr(block) and "needs" not in repr(block), (
            "a detail quotes the environment")
        assert (block["held"], block["reclassify_needed"]) == (False, False)

    def test_the_public_endpoint_carries_it(self, met, monkeypatch):
        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter

        monkeypatch.setenv(wc.ENV, "postgres")
        monkeypatch.setenv("KS_UTM_PARSE", "duckdb")
        wc.configure_mode()
        limiter.reset()
        try:
            body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
        block = body["warehouse_writer_mode"]
        assert block["preconditions_unmet"] == ["utm_parse_postgres"]
        assert "held" in block and "reclassify_needed" in block

    def test_the_canary_pages_on_it_and_is_quiet_otherwise(self):
        from bot.canary import check_warehouse_preconditions

        block = {"mode": "duckdb", "value": "postgres", "error": None,
                 "preconditions_unmet": ["pg_revision", "utm_parse_postgres"]}
        ((key, message),) = check_warehouse_preconditions({"warehouse_writer_mode": block})
        assert key == "warehouse_preconditions_unmet"
        assert "pg_revision" in message and "utm_parse_postgres" in message
        assert check_warehouse_preconditions({}) == []
        assert check_warehouse_preconditions({"warehouse_writer_mode": {
            **block, "preconditions_unmet": []}}) == []
        assert check_warehouse_preconditions({"warehouse_writer_mode": {
            "mode": "duckdb", "error": None}}) == []

    @pytest.mark.asyncio
    async def test_the_wiring_pages_critical_and_names_its_lever(self):
        from datetime import datetime, timedelta, timezone

        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["warehouse_writer_mode"] = {
            "mode": "duckdb", "value": "postgres", "error": None,
            "preconditions_unmet": ["read_fallback_off"]}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "critical"
        assert result.failure_keys == ["warehouse_preconditions_unmet"]
        assert "cutover.unmet" in canary._what_to_do(result)

    def test_it_is_a_registered_condition(self):
        from core.alerting import Kind, spec_for

        assert spec_for("warehouse_preconditions_unmet").kind is Kind.CONDITION

    def test_the_status_page_says_what_settling_found(self, monkeypatch):
        monkeypatch.setattr(wc, "_held", True)
        monkeypatch.setattr(wc, "_writer_record", {"writer": "postgres"})
        status = wc.status()
        assert status["held"] is True and status["writer"] == {"writer": "postgres"}
        assert status["stood_down_duckdb_checks"] == sorted(wc.STOOD_DOWN_WHEN_POSTGRES)


class TestTheResolveNote:
    def test_it_is_one_more_line_under_the_list(self):
        from core.alerting import _gate, resolve_group

        _gate.note_delivered_conditions(["warehouse:refresh_errored"], "warehouse")
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
             patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
            asyncio.run(resolve_group("warehouse", note="retired"))
        text = send.await_args.args[0]
        assert text.startswith("✅ Resolved:") and text.endswith("\nretired")

    def test_without_it_nothing_changes(self):
        from core.alerting import _gate, resolve_group

        _gate.note_delivered_conditions(["warehouse:refresh_errored"], "warehouse")
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
             patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
            asyncio.run(resolve_group("warehouse"))
        assert send.await_args.args[0].splitlines()[-1].startswith("• warehouse:")


# ─── What a settle that could not finish still owes ──────────────────────────


def _seed(store, **rows):
    """sync_metadata rows, by key; a dict value is written as the JSON the
    cutover writes."""
    async def seed():
        async with store.connection() as conn:
            for key, value in rows.items():
                conn.execute(
                    "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                    "VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [key, json.dumps(value) if isinstance(value, dict) else value])
    asyncio.run(seed())


class TestAPageTheWayBackDeliveredIsClosed:
    def test_the_next_start_under_postgres_closes_it(self, tmp_path, postgres, monkeypatch):
        """A way back that never validated leaves the record saying `postgres`,
        resolved — and its DuckDB ticks can page the group. Back under
        postgres nothing else would ever resolve that page."""
        from core.alerting import _gate, delivered_conditions

        store = _store(tmp_path)
        try:
            with patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                asyncio.run(wc.settle_writer(store))
            assert _writer(store)["resolved"] is True

            _restart()
            monkeypatch.delenv(wc.ENV)
            wc.configure_mode()
            asyncio.run(wc.settle_writer(store))
            assert wc.held()
            # The owed full tick crashed once, and paged.
            _gate.note_delivered_conditions(["warehouse:refresh_errored"], "warehouse")

            _restart()
            monkeypatch.setenv(wc.ENV, "postgres")
            assert wc.configure_mode() == wc.POSTGRES
            with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
                 patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
                asyncio.run(wc.settle_writer(store))
            assert "warehouse:refresh_errored" not in delivered_conditions()
            (text,) = send.await_args.args[:1]
            assert "warehouse:refresh_errored" in text and wc.RESOLVE_NOTE in text
            assert _writer(store)["resolved"] is True
        finally:
            asyncio.run(store.close())

    def test_with_nothing_open_a_resolved_record_asks_nothing(self, tmp_path, postgres):
        store = _store(tmp_path)
        resolve = AsyncMock(return_value=0)
        try:
            _seed(store, **{wc.WRITER_KEY: {"writer": "postgres", "resolved": True}})
            with patch("core.alerting.resolve_group", resolve):
                asyncio.run(wc.settle_writer(store))
            assert resolve.await_count == 0
        finally:
            asyncio.run(store.close())


class TestASettleThatCouldNotFinishIsAskedAgain:
    def test_a_resolve_the_ledger_refused_leaves_the_process_unsettled(
        self, tmp_path, postgres,
    ):
        """Asked again in the same process — the derivation tick's call —
        rather than at the next restart."""
        from core.alerting import _gate

        store = _store(tmp_path)
        try:
            _gate.note_delivered_conditions(["warehouse:validation_retrying"], "warehouse")
            with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=False)):
                asyncio.run(wc.settle_writer(store))
            assert _writer(store)["resolved"] is False
            with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
                 patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
                asyncio.run(wc.settle_writer(store))
            assert _writer(store)["resolved"] is True
            assert wc.RESOLVE_NOTE in send.await_args.args[0]
        finally:
            asyncio.run(store.close())

    def test_under_postgres_the_derivation_tick_records_what_the_start_could_not(
        self, tmp_path, postgres, job, monkeypatch,
    ):
        """The only job that ticks under postgres. Without it a process whose
        boot and start could not write the record runs unrecorded, and a way
        back after it owes DuckDB nothing: an incremental tick over the id list
        abandoned at the switch."""
        from tests.unit.test_pg_own_derivation import _derive

        store = _store(tmp_path)
        try:
            _seed(store, warehouse_dirty="[1, 2]")
            real = wc._write_writer
            monkeypatch.setattr(wc, "_write_writer", AsyncMock(side_effect=OSError("disk")))
            for _ in ("boot", "scheduler start"):
                with pytest.raises(OSError):
                    asyncio.run(wc.settle_writer(store))
            monkeypatch.setattr(wc, "_write_writer", real)

            with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)):
                _derive(job)
            assert _writer(store)["writer"] == "postgres"

            _restart()
            monkeypatch.delenv(wc.ENV)
            wc.configure_mode()
            asyncio.run(wc.settle_writer(store))
            assert wc.held() and _metadata(store, "warehouse_dirty")[0] == "full"
        finally:
            asyncio.run(store.close())

    def test_on_the_way_back_the_first_refresh_tick_settles_before_its_peek(
        self, tmp_path, monkeypatch,
    ):
        """Boot and start could not read the record: the first tick must still
        find the rebuild owed in full and the checks held before it peeks, or
        it runs incremental over a Silver as old as the switch."""
        from core.scheduler import BackgroundScheduler

        class _Peeked(Exception):
            pass

        store = _store(tmp_path)
        try:
            _seed(store, warehouse_dirty="[7, 8]",
                  **{wc.WRITER_KEY: {"writer": "postgres", "resolved": True}})
            real = wc.read_writer
            monkeypatch.setattr(wc, "read_writer", AsyncMock(side_effect=OSError("disk")))
            for _ in ("boot", "scheduler start"):
                with pytest.raises(OSError):
                    asyncio.run(wc.settle_writer(store))
            monkeypatch.setattr(wc, "read_writer", real)
            assert not wc.held()

            seen = {}

            async def peek():
                async with store.connection() as conn:
                    seen["dirty"] = conn.execute(
                        "SELECT value FROM sync_metadata WHERE key = 'warehouse_dirty'"
                    ).fetchone()[0]
                seen["held"] = wc.held()
                raise _Peeked

            monkeypatch.setattr(store, "peek_warehouse_dirty", peek)
            with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)):
                with pytest.raises(_Peeked):
                    asyncio.run(BackgroundScheduler()._run_warehouse_refresh())
            assert seen == {"dirty": "full", "held": True}
        finally:
            asyncio.run(store.close())

    def test_a_release_that_raises_asks_for_another_full_tick(self, tmp_path, monkeypatch):
        """The hold outlives a record it could not write only if the next tick
        is full again; the job clears the flag it peeked, so the re-mark is
        what keeps it."""
        from core.scheduler import BackgroundScheduler

        store = _store(tmp_path)
        try:
            asyncio.run(store.upsert_orders(_orders(1, 2, comment="utm_source=instagram")))
            _seed(store, **{wc.WRITER_KEY: {"writer": "postgres", "resolved": True}})
            asyncio.run(wc.settle_writer(store))
            assert wc.held()
            monkeypatch.setattr(wc, "_write_writer", AsyncMock(side_effect=OSError("disk")))
            with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
                 patch.object(BackgroundScheduler, "_rebuild_postgres_layers", AsyncMock()):
                result = asyncio.run(BackgroundScheduler()._run_warehouse_refresh())
            assert result["status"] == "success" and result["validation_passed"] is True
            assert wc.held()
            assert _metadata(store, "warehouse_dirty")[0] == "full"
        finally:
            asyncio.run(store.close())


# ─── The status page ─────────────────────────────────────────────────────────


class TestTheStatusPage:
    def _get(self, tmp_path):
        from web.routes.api import admin

        tmp_path.mkdir(parents=True, exist_ok=True)
        store = _store(tmp_path)
        try:
            with patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                asyncio.run(store.refresh_warehouse_layers(trigger="dirty_flag"))
            with patch.object(admin, "get_store", AsyncMock(return_value=store)), \
                 patch.object(admin, "_pg_derivation_status",
                              AsyncMock(return_value={"mode": "own", "owed": False})), \
                 patch.object(wc, "readiness", AsyncMock(return_value={"mode": wc.mode()})):
                return asyncio.run(_unwrap(admin.get_warehouse_status)(MagicMock()))
        finally:
            asyncio.run(store.close())

    def test_under_postgres_the_frozen_duckdb_refresh_is_not_the_answer(
        self, tmp_path, monkeypatch,
    ):
        """DuckDB's last tick is as old as the switch; an operator reading the
        page after the flip must not take it for the warehouse's state."""
        body_before = self._get(tmp_path / "a")
        assert body_before["last_trigger"] == "dirty_flag"

        monkeypatch.setattr(wc, "_mode", wc.POSTGRES)
        body = self._get(tmp_path / "b")
        assert "last_refresh" not in body and "validation_passed" not in body
        assert body["writer"] == "postgres" and body["postgres"]["mode"] == "own"
        assert body["duckdb_frozen"]["last_trigger"] == "dirty_flag"
        assert body["cutover"] == {"mode": "postgres"}

    def test_the_default_answers_as_before(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KS_PG_DERIVE", raising=False)
        from core import pg_derivation

        pg_derivation.configure_mode()
        body = self._get(tmp_path)
        assert body["last_trigger"] == "dirty_flag" and body["validation_passed"] is True
        assert "postgres" not in body and "duckdb_frozen" not in body
        assert "writer" not in body


# ─── The UTM doors under postgres ────────────────────────────────────────────


def _broken_duckdb():
    """A DuckDB that raises on everything — what a door under postgres must
    not need."""
    store = MagicMock()
    store.connection = MagicMock(side_effect=RuntimeError("duckdb is gone"))
    store.refresh_utm_silver_layer = AsyncMock(side_effect=RuntimeError("duckdb parse died"))
    return store


class TestTheUtmDoorsUnderPostgres:
    def _door(self, name, store, router):
        from web.routes.api import traffic

        with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
             patch.object(traffic, "reparse_router", router):
            return asyncio.run(_unwrap(getattr(traffic, name))(request=None, user={}))

    def test_the_reclassify_parses_postgres_alone_whatever_duckdb_does(
        self, postgres, caplog,
    ):
        """The review's reproduction: a DuckDB parse that raised left the
        Postgres reclassify — what /traffic reads — unrun, behind a 500."""
        store = _broken_duckdb()
        router = AsyncMock(return_value={"rows": 5, "replaced": 4, "duration_ms": 1})
        result = self._door("reclassify_traffic", store, router)
        assert result == {"success": True, "utm_records": 5, "engine": "postgres"}
        router.assert_awaited_once_with(store, full=True)
        store.refresh_utm_silver_layer.assert_not_awaited()
        assert "will not re-parse them" in caplog.text, "the refused note is logged"

    def test_the_refresh_parses_postgres_alone_and_only_adds(self, postgres):
        store = _broken_duckdb()
        router = AsyncMock(return_value={"parsed": 2, "rows": 9, "duration_ms": 1})
        result = self._door("refresh_traffic_data", store, router)
        assert result == {"success": True, "utm_orders_parsed": 2, "engine": "postgres"}
        router.assert_awaited_once_with(store, full=False)
        store.refresh_utm_silver_layer.assert_not_awaited()

    @pytest.mark.parametrize("answer, status", [
        ({"error": "LockNotAvailableError: lock timeout"}, 500),
        ({"refused": "0 rows would replace 9"}, 409),
    ])
    def test_postgres_failing_is_the_doors_answer(self, postgres, answer, status):
        """There is no DuckDB half that succeeded for it to report instead."""
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as raised:
            self._door("reclassify_traffic", _broken_duckdb(), AsyncMock(return_value=answer))
        assert raised.value.status_code == status

    def test_the_reclassify_notes_the_record_and_leaves_duckdb_alone(
        self, tmp_path, postgres,
    ):
        store = _store(tmp_path)
        try:
            with patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
                asyncio.run(wc.settle_writer(store))
            _one_utm_row(store)
            router = AsyncMock(return_value={"rows": 1, "replaced": 1})
            self._door("reclassify_traffic", store, router)
            record = _writer(store)
            assert record["writer"] == "postgres" and record["resolved"] is True
            assert wc.UTM_REPARSED in record
            assert _utm_rows(store) == 1, "DuckDB's verdicts are left as they stand"
        finally:
            asyncio.run(store.close())

    def test_a_note_before_the_first_settle_records_postgres_unresolved(
        self, tmp_path, postgres,
    ):
        """The settle that would have written it raised; its retry then finds
        the record unresolved and closes the group."""
        store = _store(tmp_path)
        try:
            asyncio.run(wc.note_utm_reparsed_in_postgres(store))
            record = _writer(store)
            assert record["writer"] == "postgres" and record["resolved"] is False
            assert wc.UTM_REPARSED in record
        finally:
            asyncio.run(store.close())

    def test_the_default_reclassify_is_unchanged(self, tmp_path):
        store = _store(tmp_path)
        try:
            asyncio.run(store.upsert_orders(_orders(1, 2, comment="utm_source=instagram")))
            router = AsyncMock(return_value={"shipped": 2})
            result = self._door("reclassify_traffic", store, router)
            assert result == {"success": True, "utm_records": 2}
            router.assert_awaited_once_with(store, full=True)
            assert _utm_rows(store) == 2
            assert _writer(store) is None, "the default writes no record"
        finally:
            asyncio.run(store.close())


def _one_utm_row(store) -> None:
    async def seed():
        async with store.connection() as conn:
            conn.execute("INSERT INTO silver_order_utm (order_id) VALUES (1)")
    asyncio.run(seed())


def _utm_rows(store) -> int:
    async def count():
        async with store.connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM silver_order_utm").fetchone()[0]
    return asyncio.run(count())


class TestTheBackfillDoorsUnderPostgres:
    """The `manager_comment` backfill task and the CLI: the comments still land
    in DuckDB's `orders` and ship to `bronze.orders` — landing, which the
    switch does not move — and the verdicts are Postgres' alone."""

    @pytest.mark.asyncio
    async def test_the_task_parses_postgres_alone(self, tmp_path, postgres):
        from tests.unit.test_comment_backfill_survives_a_ship_fault import (
            _Client, _two_orders)
        from web.routes.api import traffic

        store = await _two_orders(tmp_path)
        scheduler = MagicMock()
        scheduler._heavy_job_lock = asyncio.Lock()
        before = dict(traffic._backfill_status)

        async def ship(_store, ids, *, version_kind, **_kw):
            return {"orders_shipped": len(ids)}

        router = AsyncMock(return_value={"parsed": 1, "rows": 2})
        try:
            with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
                 patch("core.keycrm.get_async_client", AsyncMock(return_value=_Client())), \
                 patch("core.scheduler.get_scheduler", return_value=scheduler), \
                 patch("core.pg_backfill.ship_orders_by_id", new=ship), \
                 patch.object(store, "refresh_utm_silver_layer",
                              new=AsyncMock(side_effect=AssertionError("DuckDB parsed"))), \
                 patch.object(traffic, "reparse_router", router), \
                 patch("asyncio.sleep", new=AsyncMock()):
                await asyncio.wait_for(traffic._run_backfill_inner(1), timeout=20)
            result = dict(traffic._backfill_status["result"])

            router.return_value = {"error": "RuntimeError: postgres went away"}
            async with store.connection() as conn:
                conn.execute("UPDATE orders SET manager_comment = NULL WHERE id = 2")
            with patch.object(traffic, "get_store", AsyncMock(return_value=store)), \
                 patch("core.keycrm.get_async_client", AsyncMock(return_value=_Client())), \
                 patch("core.scheduler.get_scheduler", return_value=scheduler), \
                 patch("core.pg_backfill.ship_orders_by_id", new=ship), \
                 patch.object(store, "refresh_utm_silver_layer",
                              new=AsyncMock(side_effect=AssertionError("DuckDB parsed"))), \
                 patch.object(traffic, "reparse_router", router), \
                 patch("asyncio.sleep", new=AsyncMock()):
                await asyncio.wait_for(traffic._run_backfill_inner(1), timeout=20)
            failed = dict(traffic._backfill_status["result"])
        finally:
            traffic._backfill_status.clear()
            traffic._backfill_status.update(before)
            await store.close()

        assert result["status"] == "success" and result["utm_records_parsed"] == 1
        assert "pg_parse_error" not in result
        router.assert_awaited_with(store)
        assert failed["status"] == "partial"
        assert failed["pg_parse_error"].startswith("RuntimeError")

    @pytest.mark.asyncio
    async def test_the_cli_parses_postgres_alone_and_notes_it(self, tmp_path, postgres):
        import scripts.backfill_utm as cli
        from tests.unit.test_comment_backfill_survives_a_ship_fault import (
            _Client, _two_orders)

        store = await _two_orders(tmp_path, name="cli.duckdb")
        async with store.connection() as conn:
            conn.execute("INSERT INTO silver_order_utm (order_id) VALUES (1)")

        async def ship(_store, ids, *, version_kind, **_kw):
            return {"orders_shipped": len(ids)}

        router = AsyncMock(return_value={"rows": 2, "replaced": 1})
        try:
            with patch("core.duckdb_store.get_store", AsyncMock(return_value=store)), \
                 patch("core.keycrm.get_async_client", AsyncMock(return_value=_Client())), \
                 patch("core.runtime_modes.configure_modes", MagicMock()), \
                 patch("core.pg_backfill.ship_orders_by_id", new=ship), \
                 patch.object(store, "refresh_utm_silver_layer",
                              new=AsyncMock(side_effect=AssertionError("DuckDB parsed"))), \
                 patch("core.pg_utm_parse.reparse_router", router):
                await asyncio.wait_for(cli.backfill_utm(days_back=1), timeout=20)
            async with store.connection() as conn:
                kept = conn.execute("SELECT COUNT(*) FROM silver_order_utm").fetchone()[0]
                raw = conn.execute("SELECT value FROM sync_metadata WHERE key = ?",
                                   [wc.WRITER_KEY]).fetchone()
        finally:
            await store.close()

        router.assert_awaited_once_with(store, full=True, force=False)
        assert kept == 1, "DuckDB's silver_order_utm was emptied under postgres"
        assert wc.UTM_REPARSED in json.loads(raw[0])


class TestTheWayBackReparsesWhatPostgresAloneReclassified:
    @pytest.fixture
    def reclassified(self, tmp_path):
        """DuckDB holding a verdict under the old rules, and a record saying a
        reclassify ran in Postgres alone since."""
        store = _store(tmp_path)
        asyncio.run(store.upsert_orders(_orders(1, 2, comment="utm_source=instagram")))
        asyncio.run(store.refresh_utm_silver_layer())

        async def seed():
            async with store.connection() as conn:
                conn.execute("UPDATE silver_order_utm SET platform = 'old-rules'")
                conn.execute(
                    "INSERT OR REPLACE INTO sync_metadata (key, value, updated_at) "
                    "VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [wc.WRITER_KEY, json.dumps({
                        "writer": "postgres", "resolved": True,
                        wc.UTM_REPARSED: "2026-09-20T10:00:00+00:00"})])
        asyncio.run(seed())
        yield store
        asyncio.run(store.close())

    def test_the_way_back_empties_duckdbs_verdicts_and_the_owed_tick_reparses(
        self, reclassified,
    ):
        asyncio.run(wc.settle_writer(reclassified))
        assert wc.held() and _utm_rows(reclassified) == 0
        assert _metadata(reclassified, "warehouse_dirty")[0] == "full"

        result = asyncio.run(reclassified.refresh_warehouse_layers(trigger="dirty_flag"))
        assert result["validation_passed"] is True and result["utm_orders_parsed"] == 2
        assert not wc.held() and not wc.reclassify_needed()

        async def platforms():
            async with reclassified.connection() as conn:
                return {r[0] for r in conn.execute(
                    "SELECT DISTINCT platform FROM silver_order_utm").fetchall()}
        assert "old-rules" not in asyncio.run(platforms())

    def test_without_the_note_the_verdicts_stay(self, tmp_path):
        """What the note buys: the incremental parse re-reads only what changed."""
        store = _store(tmp_path)
        try:
            asyncio.run(store.upsert_orders(_orders(1, comment="utm_source=instagram")))
            asyncio.run(store.refresh_utm_silver_layer())
            _seed(store, **{wc.WRITER_KEY: {"writer": "postgres", "resolved": True}})
            asyncio.run(wc.settle_writer(store))
            assert wc.held() and _utm_rows(store) == 1
        finally:
            asyncio.run(store.close())


# ─── The doors OD-10 has not decided ─────────────────────────────────────────

import re  # noqa: E402

# What the switch freezes in DuckDB, as SQL names it: Silver, the order-lines
# view over it, Gold and the UTM verdicts — and the dialect holes that render
# to one of them, which count because the walk cannot tell which engine a
# rendering is for.
FROZEN_BY_THE_SWITCH = re.compile(
    r"\b(silver_orders|silver_order_lines|gold_daily_revenue|silver_order_utm)\b"
    r"|\{(silver_orders|order_lines|gold_daily_revenue|order_utm)\}")


def _docstrings(tree: ast.Module) -> set:
    return {id(node.body[0].value) for node in ast.walk(tree)
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef))
            and node.body and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)}


def _frozen_readers_in(tree: ast.Module) -> set:
    """Every function in `tree` holding a string — SQL, an f-string's text,
    never a docstring — that names a table the switch freezes, where the
    string does not stand behind `duckdb_derives()` in one of the shapes the
    refresh walk accepts. A module-level string counts under None."""
    docstrings = _docstrings(tree)
    return {fn for _line, guarded, fn in _guarded_nodes(tree, lambda node: (
        isinstance(node, ast.Constant) and isinstance(node.value, str)
        and id(node) not in docstrings and FROZEN_BY_THE_SWITCH.search(node.value)))
        if not guarded}


def _unguarded_duckdb_readers(root: str = "web") -> set:
    """`path:function` for every such function under `root`."""
    found = set()
    for path in sorted((REPO / root).rglob("*.py")):
        for fn in _frozen_readers_in(ast.parse(path.read_text(encoding="utf-8"))):
            found.add(f"{path.relative_to(REPO)}:{fn}")
    return found


class TestTheDoorsOd10HasNotDecided:
    """The review's reading: admin doors read DuckDB's Silver with no switch,
    so after the flip they answer from a Silver as old as it, and after a
    compaction from none. OD-10 blocks step 13, and the switch waits on it:
    `od10_doors` is unmet while `OD10_DOORS` is not empty, and the list is
    what a walk of `web/` finds — nobody keeps it."""

    def test_the_list_is_exactly_what_the_walk_finds(self):
        """A door retired, ported or put behind the predicate must leave the
        list, and a door added must join it — either way this fails until it
        does."""
        found = _unguarded_duckdb_readers()
        listed = {where for where, _label in wc.OD10_DOORS}
        assert found == listed, (
            f"read DuckDB's frozen Silver with no switch and not listed: "
            f"{sorted(found - listed)}; listed and no longer found: "
            f"{sorted(listed - found)}")

    def test_the_reviews_four_and_the_plans_detail_endpoints_are_there(self):
        labels = " ".join(label for _, label in wc.OD10_DOORS)
        for route in ("/api/buyers/stats", "/api/debug/stale-returns",
                      "/api/debug/order-status", "/api/duckdb/purge-orders",
                      "/api/buyers/{id}", "/api/orders/{id}", "/api/products/{id}"):
            assert route in labels

    @pytest.mark.parametrize("source, readers", [
        # SQL counts; a docstring naming the table does not.
        ('def f(conn):\n    """Reads silver_orders."""\n'
         '    conn.execute("SELECT 1 FROM silver_orders")\n', {"f"}),
        ('def f(conn):\n    """Reads silver_orders."""\n    return 1\n', set()),
        # The view over Silver is Silver; so is an f-string's text.
        ('def f(conn, i):\n    conn.execute(f"SELECT * FROM silver_order_lines '
         'WHERE id = {i}")\n', {"f"}),
        ('def f(conn):\n    conn.execute("SELECT 1 FROM gold_daily_revenue")\n', {"f"}),
        ('def f(conn):\n    conn.execute("DELETE FROM silver_order_utm")\n', {"f"}),
        # A dialect hole could render to DuckDB; the walk cannot tell.
        ('def f(conn):\n    conn.execute("SELECT 1 FROM {silver_orders}")\n', {"f"}),
        # Postgres names another table, and landing is not frozen.
        ('def f(conn):\n    conn.execute("SELECT 1 FROM silver.orders")\n', set()),
        ('def f(conn):\n    conn.execute("SELECT 1 FROM orders")\n', set()),
        # Behind the predicate, in each shape the refresh walk accepts.
        ('def f(conn):\n    if not wc.duckdb_derives():\n        raise X()\n'
         '    conn.execute("SELECT 1 FROM silver_orders")\n', set()),
        ('def f(conn):\n    if wc.duckdb_derives():\n'
         '        conn.execute("SELECT 1 FROM silver_orders")\n', set()),
        ('def f(conn):\n    if wc.duckdb_derives():\n        pass\n    else:\n'
         '        conn.execute("SELECT 1 FROM silver_orders")\n', {"f"}),
        ('def f(conn):\n    if not wc.duckdb_derives():\n        log()\n'
         '    conn.execute("SELECT 1 FROM silver_orders")\n', {"f"}),
    ])
    def test_the_walk_reads_each_shape(self, source, readers):
        assert _frozen_readers_in(ast.parse(source)) == readers

    def test_the_walk_finds_the_doors_it_names(self):
        """A walk that found nothing would pass on a codebase it cannot read."""
        assert "web/routes/api/admin.py:get_buyer_stats" in _unguarded_duckdb_readers()

    def test_the_start_reads_the_list(self, met, monkeypatch):
        """`_local_facts` hands `OD10_DOORS` to the evaluator — the list is
        not a document: while it is not empty, `postgres` runs as duckdb."""
        monkeypatch.setattr(wc, "OD10_DOORS", (
            ("web/routes/api/admin.py:get_buyer_stats", "GET /api/buyers/stats"),))
        monkeypatch.setenv(wc.ENV, "postgres")
        assert wc.configure_mode() == wc.DUCKDB
        (unmet,) = wc.preconditions_unmet()
        assert unmet.key == "od10_doors" and "/api/buyers/stats" in unmet.detail

    def test_the_status_page_reads_it_too(self, met, monkeypatch):
        monkeypatch.setattr(wc, "OD10_DOORS", (
            ("web/routes/api/admin.py:get_buyer_stats", "GET /api/buyers/stats"),))
        ready = asyncio.run(wc.readiness())
        (door,) = [u for u in ready["unmet"] if u["key"] == "od10_doors"]
        assert "/api/buyers/stats" in door["detail"]

    def test_this_build_is_not_switchable_and_names_every_door(self, monkeypatch):
        """Today's list, as the walk finds it — not `met`, which stands in a
        build where OD-10 is answered: `postgres` runs as duckdb, and the one
        unmet item names every door, so the page is the list of what is left
        to do."""
        for name, value in MET_ENV.items():
            monkeypatch.setenv(name, value)
        monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
        monkeypatch.setattr(wc, "_revision_on_its_own_connection",
                            AsyncMock(return_value=REQUIRED_REVISION))
        assert wc.OD10_DOORS, "OD-10 answered: this test has done its job — delete it"
        monkeypatch.setenv(wc.ENV, "postgres")
        assert wc.configure_mode() == wc.DUCKDB
        (unmet,) = wc.preconditions_unmet()
        assert unmet.key == "od10_doors"
        for _where, label in wc.OD10_DOORS:
            assert label in unmet.detail
        assert f"{len(wc.OD10_DOORS)} door(s)" in unmet.detail
