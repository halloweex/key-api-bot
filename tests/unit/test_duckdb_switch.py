"""`KS_DUCKDB` and its tripwire — the week of silence's first detector (OD-17 (a)).

Stage 5 may begin only after seven days in which nothing opened the DuckDB
file, and that has to be shown by running web with `KS_DUCKDB=off`, not by
reading the code (core/duckdb_switch.py). What is held here:

- the mode: unset is `on`, today; a value nobody understands runs `on` and
  says so; a process that never configured reads it on its first open;
- the opener: under `off` it refuses before the driver runs — the file is not
  created, and the store's `connect()` and `backup_database()` refuse before
  they touch the disk beside the file —
  counts at the raise, names the site (a task's coroutine when the walk would
  otherwise name whatever turns the loop), bounds the sites, and publishes no
  exception text;
- the walk: `open_file` is the only reach for a driver function in `core/`,
  `web/` and `bot/` — `connect`, and every function that runs on the default
  connection — and the host-side tools that reach it directly are exactly the
  exemptions written down here;
- the weekly compaction, the one scheduled host process that opens the live
  file, opens it through the switch;
- the store under `off` (stage 5, PR-1): `get_store()` publishes it
  unconnected and the tripwire stands at `connection()`; under `on` it is
  connected before it is published, as ever;
- web's startup under `off` boots on that store without asking DuckDB's
  counts, survives a boot sync that failed or was refused, and a route that
  reaches the file answers 503 naming `duckdb`; `/api/health` publishes the
  sites, and the canary pages them CRITICAL and keeps the watch only while
  web runs `off`.

What still reaches the file under `off`, in the state production will be in,
is `tests/unit/test_duckdb_off_sweep.py`'s.

Each guard names the mutation that makes it fail.
"""
from __future__ import annotations

import ast
import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from bot import canary
from core import duckdb_switch
from core.alerting import REGISTRY, Kind
from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401  (fixture)
from tests.unit.test_weekly_compact_wrapper import fake_bin  # noqa: F401  (fixture)

REPO = Path(__file__).resolve().parents[2]


def _off(monkeypatch):
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    duckdb_switch.configure_mode()


# ─── The mode ────────────────────────────────────────────────────────────────

class TestTheMode:
    def test_unset_is_on_and_says_nothing(self, caplog):
        with caplog.at_level(logging.WARNING, logger="core.duckdb_switch"):
            assert duckdb_switch.configure_mode() == "on"
        assert duckdb_switch.value() is None and duckdb_switch.mode_error() is None
        assert caplog.text == ""

    @pytest.mark.parametrize("raw, mode", [("on", "on"), ("off", "off"),
                                           (" OFF ", "off"), ("On", "on")])
    def test_the_two_values_read_the_way_every_switch_reads(self, monkeypatch, raw, mode):
        monkeypatch.setenv(duckdb_switch.ENV, raw)
        assert duckdb_switch.configure_mode() == mode
        assert duckdb_switch.mode_error() is None

    def test_off_says_so_once_at_critical(self, monkeypatch, caplog):
        monkeypatch.setenv(duckdb_switch.ENV, "off")
        with caplog.at_level(logging.CRITICAL, logger="core.duckdb_switch"):
            duckdb_switch.configure_mode()
            duckdb_switch.configure_mode()
        assert caplog.text.count("must not open the DuckDB file") == 1

    def test_a_value_nobody_understands_runs_on_and_is_published(self, monkeypatch, caplog):
        """OD-09: web is the only syncer, so a typo must not stop it — and
        whoever typed `of` believes the week is running, so it is said."""
        monkeypatch.setenv(duckdb_switch.ENV, "of")
        with caplog.at_level(logging.ERROR, logger="core.duckdb_switch"):
            assert duckdb_switch.configure_mode() == "on"
            duckdb_switch.configure_mode()
        assert duckdb_switch.value() == "of"
        assert "KS_DUCKDB='of'" in duckdb_switch.mode_error()
        assert caplog.text.count("is not one of") == 1
        assert duckdb_switch.health_block()["error"] == duckdb_switch.mode_error()

    def test_an_unconfigured_process_reads_the_switch_on_its_first_open(
        self, monkeypatch, tmp_path,
    ):
        """A script that never called `configure_modes()` must not be the one
        process that opens the file under `off`.
        Mutation: make `mode()` return `_mode or ON` without configuring."""
        monkeypatch.setenv(duckdb_switch.ENV, "off")
        assert duckdb_switch._mode is None  # the conftest fixture forgot it
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.open_file(tmp_path / "never.duckdb")
        assert not (tmp_path / "never.duckdb").exists()

    def test_configure_modes_reads_it_before_the_boot_sync(self, monkeypatch):
        from core.runtime_modes import configure_modes

        monkeypatch.setenv(duckdb_switch.ENV, "off")
        modes = configure_modes()
        assert modes[duckdb_switch.ENV] == "off"
        assert list(modes)[0] == duckdb_switch.ENV, (
            "first: the boot sync's first act is to open the file")


# ─── The opener ──────────────────────────────────────────────────────────────

class TestTheOpener:
    def test_on_opens_the_file_as_ever(self, tmp_path):
        con = duckdb_switch.open_file(tmp_path / "a.duckdb")
        try:
            assert con.execute("SELECT 42").fetchone() == (42,)
        finally:
            con.close()
        assert duckdb_switch.opened() == {}

    def test_off_refuses_before_the_driver_and_creates_nothing(self, monkeypatch, tmp_path):
        """Mutation: delete `guard()` from `open_file`."""
        _off(monkeypatch)
        target = tmp_path / "a.duckdb"
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff) as raised:
            duckdb_switch.open_file(target, read_only=True)
        assert not target.exists() and list(tmp_path.iterdir()) == []
        site = f"{__name__}:test_off_refuses_before_the_driver_and_creates_nothing"
        assert raised.value.site == site and raised.value.count == 1
        assert list(duckdb_switch.opened()) == [site]

    def test_a_refusal_the_caller_swallows_is_still_counted(self, monkeypatch, tmp_path):
        """Dozens of callers wrap `get_store()` in `except Exception`; the
        count is taken at the raise so none of them can hide an open.
        Mutation: move `_tally` after the raise in `guard()`."""
        _off(monkeypatch)
        for _ in range(3):
            try:
                duckdb_switch.open_file(tmp_path / "a.duckdb")
            except Exception:
                pass
        [(site, entry)] = duckdb_switch.opened().items()
        assert entry["count"] == 3
        assert datetime.fromisoformat(entry["last_at"]) <= datetime.now(timezone.utc)

    def test_it_logs_critical_naming_the_site(self, monkeypatch, tmp_path, caplog):
        _off(monkeypatch)
        with caplog.at_level(logging.CRITICAL, logger="core.duckdb_switch"):
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                duckdb_switch.open_file(tmp_path / "a.duckdb")
        assert "DuckDB opened while KS_DUCKDB=off" in caplog.text
        assert "test_it_logs_critical_naming_the_site" in caplog.text

    def test_the_store_is_plumbing_and_the_caller_is_the_site(self, monkeypatch, tmp_path):
        """`DuckDBStore.connect()` is on every path, so naming it would name
        nothing: the site is whoever asked for the store."""
        from core.duckdb_store import DuckDBStore

        _off(monkeypatch)
        target = tmp_path / "store.duckdb"

        async def asks_for_the_store():
            await DuckDBStore(db_path=target).connect()

        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            asyncio.run(asks_for_the_store())
        assert not target.exists()
        assert list(duckdb_switch.opened()) == [f"{__name__}:asks_for_the_store"]

    def test_a_refused_connect_creates_no_directory(self, monkeypatch, tmp_path):
        """The switch is `connect()`'s first act: before it, a refused connect
        had already made `data/` and `duckdb_tmp/` (stage 5, PR-0).
        Mutation: move `duckdb_switch.guard()` below `DB_DIR.mkdir` in
        `DuckDBStore.connect`."""
        from core.duckdb_store import DuckDBStore

        data = tmp_path / "data"
        monkeypatch.setattr("core.duckdb_store.DB_DIR", data)
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            asyncio.run(DuckDBStore(db_path=data / "analytics.duckdb").connect())
        assert list(tmp_path.iterdir()) == []
        # `asyncio.run` made `connect()` itself the task, so the site is it.
        assert duckdb_switch.opened()["core.duckdb_store:connect (task)"]["count"] == 1, (
            "counted once: connect's refusal, not open_file's as well")

    def test_a_refused_backup_touches_nothing_beside_the_file(self, monkeypatch, tmp_path):
        """The backup is the other reach for the file that went to the disk
        before the switch: `backups/` made, stale temp copies deleted, the
        file stat'ed and the disk measured — and an alert sent if that came
        out short — all before `connection()` refused. Under off the switch
        is its first act too, so a frozen file's neighbours stay exactly as
        they were (review of stage 5's PR-0). Mutation: move
        `duckdb_switch.guard()` below `dest.mkdir` in `backup_database`."""
        from core.duckdb_store import DuckDBStore

        data = tmp_path / "data"
        data.mkdir()
        frozen = data / "analytics.duckdb"
        frozen.write_bytes(b"frozen")
        left_by_a_kill = data / "elsewhere" / ".analytics-20261009-020000.duckdb.tmp"
        left_by_a_kill.parent.mkdir()
        left_by_a_kill.write_bytes(b"half a copy")
        _off(monkeypatch)
        store = DuckDBStore(db_path=frozen)
        alert = AsyncMock()
        monkeypatch.setattr(store, "_send_warehouse_alert", alert)

        # Beside the file, as the daily job asks; and into a directory that
        # holds a temp copy a kill left, which the backup would clear first.
        for dest_dir in (None, left_by_a_kill.parent):
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                asyncio.run(store.backup_database(dest_dir=dest_dir))
        assert sorted(p.name for p in data.iterdir()) == ["analytics.duckdb", "elsewhere"]
        assert left_by_a_kill.read_bytes() == b"half a copy"
        alert.assert_not_awaited()
        assert sum(e["count"] for e in duckdb_switch.opened().values()) == 2

    def test_on_still_makes_both_directories(self, monkeypatch, tmp_path):
        from core.duckdb_store import DuckDBStore

        data = tmp_path / "data"
        monkeypatch.setattr("core.duckdb_store.DB_DIR", data)

        async def opens():
            store = DuckDBStore(db_path=data / "analytics.duckdb")
            await store.connect()
            await store.close()

        asyncio.run(opens())
        assert (data / "analytics.duckdb").exists() and (data / "duckdb_tmp").is_dir()
        assert duckdb_switch.opened() == {}

    def test_a_refusal_inside_a_task_names_the_task_not_the_loop(self, monkeypatch, tmp_path):
        """The sync hands the store's own methods to `asyncio.ensure_future`,
        so the task's coroutine is plumbing and the walk went past it to
        whatever turns the loop: this test function here, uvicorn in
        production — never the read that was refused (stage 5, PR-0).
        Mutation: drop the `left_the_task` branch in `_site`."""
        from core.duckdb_store import DuckDBStore

        _off(monkeypatch)
        store = DuckDBStore(db_path=tmp_path / "a.duckdb")

        async def sync():
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                await asyncio.ensure_future(store.get_last_sync_time("orders"))

        asyncio.run(sync())
        assert list(duckdb_switch.opened()) == [
            "core.duckdb_store:get_last_sync_time (task)"]

    def test_an_application_frame_that_turns_the_loop_is_still_the_site(
        self, monkeypatch, tmp_path,
    ):
        """A tool of ours that runs the loop on the store's coroutine says
        more than the coroutine does, and is named as it always was."""
        from core.duckdb_store import DuckDBStore

        _off(monkeypatch)
        scope = {"__name__": "scripts.some_tool", "asyncio": asyncio,
                 "store": DuckDBStore(db_path=tmp_path / "a.duckdb")}
        exec("def main():\n    asyncio.run(store.connect())\n", scope)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            scope["main"]()
        assert list(duckdb_switch.opened()) == ["scripts.some_tool:main"]

    def test_a_task_of_our_own_is_named_plainly(self, monkeypatch, tmp_path):
        """A task whose coroutine is a caller, not the store, is found by the
        walk before it leaves the task: no `(task)`."""
        from core.duckdb_store import DuckDBStore

        _off(monkeypatch)
        store = DuckDBStore(db_path=tmp_path / "a.duckdb")

        async def reads():
            async with store.connection():
                pass

        async def boot():
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                await asyncio.ensure_future(reads())

        asyncio.run(boot())
        assert list(duckdb_switch.opened()) == [f"{__name__}:reads"]

    def test_on_the_singleton_is_connected_before_it_is_published(
        self, monkeypatch, tmp_path,
    ):
        """`on` is as it always was: `get_store()` connects, then publishes.
        Mutation: publish before `connect()` (the store a failed connect left
        behind used to be handed to web's second attempt)."""
        import core.duckdb_store as store_module

        monkeypatch.setattr(store_module, "DB_DIR", tmp_path)
        published_at_connect = []
        real = store_module.DuckDBStore.connect

        async def connect(self, **kwargs):
            published_at_connect.append(store_module._store_instance)
            await real(self, **kwargs)

        monkeypatch.setattr(store_module.DuckDBStore, "connect", connect)

        async def get():
            store = await store_module.get_store()
            try:
                assert store._connection is not None
            finally:
                await store_module.close_store()

        asyncio.run(get())
        assert published_at_connect == [None]

    def test_on_a_failed_connect_leaves_no_singleton(self, monkeypatch):
        import core.duckdb_store as store_module

        monkeypatch.setattr(store_module.DuckDBStore, "connect",
                            AsyncMock(side_effect=OSError("disk")))
        with pytest.raises(OSError):
            asyncio.run(store_module.get_store())
        assert store_module._store_instance is None

    def test_off_the_singleton_is_published_unconnected(self, duckdb_off):
        """Stage 5, PR-1: under off `get_store()` touches no file and refuses
        nothing — about 150 callers hold the store only as the host of a
        router that goes to Postgres — and the tripwire stands at
        `connection()`, naming who reached for the file. Mutation: connect in
        `get_store()` under off again (the first caller is refused and no
        singleton is published)."""
        import core.duckdb_store as store_module

        async def a_router_host():
            return await store_module.get_store()

        async def a_reader():
            store = await store_module.get_store()
            async with store.connection():
                pass

        store = asyncio.run(a_router_host())
        assert store is store_module._store_instance and store._connection is None
        assert duckdb_switch.opened() == {}
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            asyncio.run(a_reader())
        assert list(duckdb_switch.opened()) == [f"{__name__}:a_reader"]
        assert store_module._store_instance is store and store._connection is None
        assert not duckdb_off.exists()
        asyncio.run(store_module.close_store())
        assert store_module._store_instance is None

    def test_off_a_checkpoint_of_the_unconnected_store_says_so(self, monkeypatch, caplog):
        import core.duckdb_store as store_module

        _off(monkeypatch)

        async def checkpoint():
            await (await store_module.get_store()).checkpoint()

        with caplog.at_level(logging.INFO, logger="core.duckdb_store"):
            asyncio.run(checkpoint())
        assert "checkpoint skipped: KS_DUCKDB=off" in caplog.text
        assert duckdb_switch.opened() == {}

    def test_sites_are_bounded_and_the_rest_are_other(self, monkeypatch, tmp_path):
        """Published, so bounded: a counter keyed on anything says where it
        stops. Mutation: drop the `len(_opened) >= MAX_SITES` branch."""
        _off(monkeypatch)
        extra = 5
        for i in range(duckdb_switch.MAX_SITES + extra):
            scope = {"__name__": f"fake.module{i}", "open_file": duckdb_switch.open_file,
                     "path": tmp_path / "x.duckdb"}
            exec(f"def caller{i}():\n    open_file(path)\n", scope)
            with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                scope[f"caller{i}"]()
        opened = duckdb_switch.opened()
        assert len(opened) == duckdb_switch.MAX_SITES + 1
        assert opened[duckdb_switch.OTHER]["count"] == extra

    def test_the_health_block_carries_no_exception_text(self, monkeypatch, tmp_path):
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff) as raised:
            duckdb_switch.open_file(tmp_path / "a.duckdb")
        block = duckdb_switch.health_block()
        assert set(block) == {"mode", "value", "error", "opened_while_off"}
        assert block["mode"] == "off" and block["value"] == "off"
        for entry in block["opened_while_off"].values():
            assert set(entry) == {"count", "last_at"}
        assert str(raised.value) not in json.dumps(block)
        assert str(tmp_path) not in json.dumps(block)

    def test_reset_forgets_everything(self, monkeypatch, tmp_path):
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.open_file(tmp_path / "a.duckdb")
        duckdb_switch.reset()
        monkeypatch.delenv(duckdb_switch.ENV)
        assert duckdb_switch.opened() == {} and duckdb_switch.mode() == "on"

    def test_reset_counts_forgets_the_counts_and_keeps_the_mode(self, monkeypatch, tmp_path):
        """What a sweep calls between its steps. Mutation: make it `reset()`
        — the mode is read again, from an environment that no longer says
        `off`, and the next open is let through."""
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.open_file(tmp_path / "a.duckdb")
        duckdb_switch.reset_counts()
        monkeypatch.delenv(duckdb_switch.ENV)
        assert duckdb_switch.opened() == {}
        assert duckdb_switch.mode() == "off" and duckdb_switch.value() == "off"
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.open_file(tmp_path / "a.duckdb")
        assert not (tmp_path / "a.duckdb").exists()


# ─── The walk: one opener ────────────────────────────────────────────────────

def _driver_functions() -> frozenset:
    """Every function on the driver module, read off the driver itself.

    `connect` opens a file; every other one — `execute`, `sql`, `query`,
    `read_parquet`, `default_connection`... — runs on the module's default
    connection, an in-memory database that `ATTACH '<file>'` turns into an
    open of any file at all. So the walk forbids them all, not `connect`
    alone. What the application may name is the driver's classes (its
    exceptions, the connection type) and its plain values: neither opens
    anything."""
    import inspect

    import duckdb

    return frozenset(
        name for name in dir(duckdb)
        if not name.startswith("_") and callable(getattr(duckdb, name))
        and not inspect.isclass(getattr(duckdb, name)))


DRIVER_FUNCTIONS = _driver_functions()
_IMPORTERS = frozenset({"import_module", "__import__"})


def _is_driver_name(value) -> bool:
    return isinstance(value, str) and (value == "duckdb" or value.startswith("duckdb."))


def _called(node) -> str | None:
    """The name a call is made by: `f(...)` and `x.f(...)` both give `f`."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    return func.attr if isinstance(func, ast.Attribute) else None


def driver_references(source: str) -> list:
    """Every reach for a driver function in a module, as the dotted name of
    the function holding it (`<module>` at top level).

    References, not only calls: `opener = duckdb.connect` hands the driver on
    just as well. A function on the driver is reached as an attribute of the
    driver, through `getattr` on it (a name the walk cannot read counts as
    one), or bound by `from duckdb import <function> [as f]`; and
    `from duckdb import *` counts where it stands.

    The driver itself is any of:
    - a name an import bound to it: `import duckdb [as x]`, `import a, duckdb`,
      `import duckdb.sub`, and `from <anything> import duckdb [as d]` —
      because every module that imports the driver re-exports it. A star
      import from anywhere may bring it in the same way, so after one the bare
      name `duckdb` is the driver;
    - `<anything>.duckdb` where a driver function or `getattr` follows it
      (`s.duckdb.connect`), and `<a module>.duckdb` anywhere — a module being
      a name an import bound, or an attribute of one — so `args.duckdb`, a
      command-line flag, is not;
    - `importlib.import_module('duckdb')`, `__import__('duckdb')`,
      `sys.modules['duckdb']`, `getattr(<a module>, 'duckdb')`.

    Handed on as a value anywhere but the object of an attribute or of
    `getattr` (`x = duckdb`, `f(duckdb)`), the driver counts too: no later
    line can be traced through.

    A program held as text — a string, an f-string or a `+` concatenation
    that is itself Python mentioning the driver, run elsewhere by `python -c`
    or a subprocess — is read the same way, and every reach inside it counts
    where the string stands: it runs in another process, past the switch's
    in-process count, and a read-only open of it changes no byte the file's
    hash could see (batch-E review; the step-13 rehearsal's D1 DELETE is
    such a program, and opens through `open_file`). Text that does not parse
    counts once if it imports the driver.

    What it cannot read, and does not pretend to: a module named by a value
    (`import_module(name)`), the driver reached through an object's
    attribute set at run time, and anything that is not Python (`ATTACH` in a
    SQL file, a `duckdb` CLI in a shell script)."""
    tree = ast.parse(source)
    parents = {child: node for node in ast.walk(tree)
               for child in ast.iter_child_nodes(node)}
    imported, modules, functions = set(), set(), set()
    starred = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                imported.add(bound)
                if alias.name == "duckdb" or (
                        alias.name.startswith("duckdb.") and alias.asname is None):
                    modules.add(bound)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    modules.add("duckdb")
                    if node.module == "duckdb":
                        starred.append(node)
                    continue
                bound = alias.asname or alias.name
                imported.add(bound)
                if alias.name == "duckdb":
                    modules.add(bound)
                elif node.module == "duckdb" and alias.name in DRIVER_FUNCTIONS:
                    functions.add(bound)

    def sys_modules_key(expr):
        if (isinstance(expr, ast.Subscript) and isinstance(expr.value, ast.Attribute)
                and expr.value.attr == "modules" and isinstance(expr.slice, ast.Constant)):
            return expr.slice.value
        return None

    def is_getattr(expr) -> bool:
        return _called(expr) == "getattr" and len(expr.args) >= 2

    def a_module(expr) -> bool:
        if isinstance(expr, ast.Name):
            return expr.id in imported or expr.id in modules
        if isinstance(expr, ast.Attribute):
            return a_module(expr.value)
        return _called(expr) in _IMPORTERS or sys_modules_key(expr) is not None

    def the_driver(expr, *, followed: bool) -> bool:
        """`followed`: a driver function or `getattr` is applied to `expr`."""
        if isinstance(expr, ast.Name):
            return expr.id in modules
        if isinstance(expr, ast.Attribute) and expr.attr == "duckdb":
            return followed or a_module(expr.value)
        if _called(expr) in _IMPORTERS:
            return (bool(expr.args) and isinstance(expr.args[0], ast.Constant)
                    and _is_driver_name(expr.args[0].value))
        if is_getattr(expr):
            looked_up = expr.args[1]
            return (isinstance(looked_up, ast.Constant) and looked_up.value == "duckdb"
                    and a_module(expr.args[0]))
        return _is_driver_name(sys_modules_key(expr))

    def reaches(node) -> bool:
        if (isinstance(node, ast.Attribute) and node.attr in DRIVER_FUNCTIONS
                and the_driver(node.value, followed=True)):
            return True
        if is_getattr(node) and the_driver(node.args[0], followed=True):
            looked_up = node.args[1]
            return not (isinstance(looked_up, ast.Constant)
                        and looked_up.value not in DRIVER_FUNCTIONS)
        if (isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
                and node.id in functions):
            return True
        ctx = getattr(node, "ctx", None)
        if ctx is not None and not isinstance(ctx, ast.Load):
            return False
        if the_driver(node, followed=False):
            # The driver as a value: fine as the object of an attribute or of
            # getattr, judged above; anywhere else it is handed on.
            parent = parents.get(node)
            if isinstance(parent, ast.Attribute) and parent.value is node:
                return False
            if is_getattr(parent) and parent.args[0] is node:
                return False
            return True
        return False

    found = []
    for node in [*starred, *(n for n in ast.walk(tree) if reaches(n)),
                 *_reaches_in_programs(tree)]:
        scope, names = node, []
        while scope in parents:
            scope = parents[scope]
            if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(scope.name)
        found.append(".".join(reversed(names)) or "<module>")
    return found


def _string_text(node) -> str | None:
    """The text a string expression holds, every hole as `{}`: a literal, an
    f-string, or a `+` concatenation with a literal in it. None otherwise."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "{}"
                       for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _string_text(node.left), _string_text(node.right)
        if left is None and right is None:
            return None
        return (left if left is not None else "{}") + (right if right is not None else "{}")
    return None


_DRIVER_IMPORT_IN_TEXT = re.compile(
    r"(?m)(?:^|;)\s*(?:import\s+(?:[\w.]+\s*,\s*)*duckdb\b|from\s+duckdb\s+import\b)")


def _reaches_in_programs(tree: ast.AST) -> list:
    """One string node per reach for the driver inside a program it holds."""
    out, inside = [], set()
    for node in ast.walk(tree):
        if node in inside:
            continue
        text = _string_text(node)
        if text is None:
            continue
        # Breadth-first, so the whole expression is read before its parts.
        inside.update(d for d in ast.walk(node) if d is not node)
        if "duckdb" not in text:
            continue
        try:
            reaches = driver_references(text)
        except (SyntaxError, ValueError):
            reaches = ["<text>"] if _DRIVER_IMPORT_IN_TEXT.search(text) else []
        out += [node] * len(reaches)
    return out


def _walk(*trees: str) -> dict:
    out: dict = {}
    for top in trees:
        for path in sorted((REPO / top).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            for scope in driver_references(path.read_text(encoding="utf-8")):
                out.setdefault((rel, scope), 0)
                out[(rel, scope)] += 1
    return out


# The one opener in the application.
OPENER = ("core/duckdb_switch.py", "open_file")

# Host-side tools that reach the driver directly, past the switch. What covers
# them is the hourly sha256 of the file (deploy/duckdb_silence_check.sh) — and
# that sees only a WRITE to the live file: a read-only open changes no byte,
# and nothing sees it. So an entry here must be a tool a person runs by hand,
# or one that never opens the live file. The one scheduled tool that does, the
# weekly compaction, opens it through the switch (TestTheCompaction), and is
# not here. Each key must still be found by the walk, and the walk must find
# nothing else.
HOST_TOOLS = {
    ("scripts/compact_duckdb.py", "phase3_validate"):
        "validates the NEW file the compaction built, never the live one; "
        "unreachable under off (phase 1 is refused first)",
    ("scripts/weekly_report_preview.py", "live_report"):
        "an operator's preview on a laptop copy, read-only",
    ("deploy/ark_freeze.py", "freeze"):
        "the Ark is frozen from the file read-only, before the week starts",
    ("deploy/ark_freeze.py", "verify"):
        "verifies the frozen copy and an in-memory probe, never the live file",
    ("deploy/dq_history.py", "<module>"):
        "reads a backup under data/backups, read-only",
    ("deploy/duckdb_table_fates_check.py", "read_catalogue"):
        "the table-fate manifest's check, run by hand: the newest nightly "
        "backup, read-only; it refuses the live file by name, by inode and "
        "with a WAL beside it",
    ("deploy/step13_rehearsal/probe.py", "duckdb_facts"):
        "the step-13 rehearsal, run by hand before the warehouse flip: a copy "
        "of a backup in a throwaway stack, read-only, never the live file. Its "
        "one read-write open, D1's DELETE, is a program run with `python -c` "
        "and opens through `open_file`",
}


class TestOneOpener:
    def test_the_application_has_one_opener(self):
        """`core/`, `web/` and `bot/` run in the two containers; every DuckDB
        connection they make goes through `open_file`, or the switch has a
        hole nobody can see. Mutation: in `DuckDBStore.connect`, open with
        `duckdb.connect(str(self.db_path))` again."""
        found = _walk("core", "web", "bot")
        assert found == {OPENER: 1}, found

    def test_the_kill_guards_exemptions_are_host_tools(self):
        """The kill guard (`test_duckdb_open_guard.py`) exempts a read-write
        open from `open_file`'s CHECKPOINT by name; any such open also reaches
        the driver past the switch, so it is a host tool here too, under the
        same (file, function) — the two lists say one thing about each tool.
        Neither may exempt the application. Mutation: exempt a read-write
        open in the guard's list and not here."""
        from tests.unit.test_duckdb_open_guard import EXEMPT, OPENER as GUARD

        assert GUARD == OPENER, "the two walks must hold one opener"
        assert set(EXEMPT) <= set(HOST_TOOLS), sorted(set(EXEMPT) - set(HOST_TOOLS))
        for path, _ in [*EXEMPT, *HOST_TOOLS]:
            assert path.split("/")[0] in ("scripts", "deploy"), path

    def test_the_host_tools_are_exactly_the_written_exemptions(self):
        """Mutation: delete one entry, or add `duckdb.connect` to another
        script — either way the two sets differ."""
        found = _walk("scripts", "deploy")
        assert set(found) == set(HOST_TOOLS), (
            f"new: {sorted(set(found) - set(HOST_TOOLS))}, "
            f"gone: {sorted(set(HOST_TOOLS) - set(found))}")

    @pytest.mark.parametrize("source, expected", [
        ("import duckdb\nduckdb.connect('x')", ["<module>"]),
        ("import duckdb as d\ndef f():\n    return d.connect('x')", ["f"]),
        ("import glob, duckdb\nc = duckdb.connect('x')", ["<module>"]),
        ("from duckdb import connect\nconnect('x')", ["<module>"]),
        ("from duckdb import connect as c\nclass A:\n    def m(self):\n        c('x')",
         ["A.m"]),
        ("from duckdb import *\ndef f():\n    return connect", ["<module>"]),
        ("import duckdb\nopener = duckdb.connect", ["<module>"]),
        ("import duckdb\nopener = getattr(duckdb, 'connect')", ["<module>"]),
        ("import duckdb\ndef f(n):\n    return getattr(duckdb, n)", ["f"]),
        ("def f():\n    import duckdb\n    return duckdb.connect(':memory:')", ["f"]),
        # The four spellings the first walk let past (review of 02.10): the
        # driver re-exported by the store, reached through the store's
        # module, imported by its name, and the default connection, which
        # `ATTACH` points at any file.
        ("from core.duckdb_store import duckdb as d\ndef f(p):\n    return d.connect(p)",
         ["f"]),
        ("import core.duckdb_store as s\ndef f(p):\n    return s.duckdb.connect(p)", ["f"]),
        ("import importlib\ndef f(p):\n    return importlib.import_module('duckdb').connect(p)",
         ["f"]),
        ("import duckdb\ndef f():\n    duckdb.execute(\"ATTACH 'x.duckdb' AS live\")", ["f"]),
        ("import duckdb\nduckdb.sql('SELECT 1')", ["<module>"]),
        ("import sys\ndef g():\n    return sys.modules['duckdb'].sql('x')", ["g"]),
        ("d = __import__('duckdb')", ["<module>"]),
        ("import duckdb\nhanded_on = duckdb", ["<module>"]),
        ("from core import duckdb_store\nx = duckdb_store.duckdb", ["<module>"]),
        ("import core.duckdb_store\ndef f(p):\n    return core.duckdb_store.duckdb.sql(p)",
         ["f"]),
        ("import core.duckdb_store as s\ndef f(p):\n    return getattr(s, 'duckdb').connect(p)",
         ["f"]),
        ("def f(m, p):\n    return m.duckdb.connect(p)", ["f"]),
        ("from core.duckdb_store import *\ndef f(p):\n    return duckdb.connect(p)", ["f"]),
        # What may be named: the driver's classes and plain values.
        ("import duckdb\ntry:\n    pass\nexcept duckdb.IOException:\n    pass", []),
        ("import duckdb\ndef f(c: duckdb.DuckDBPyConnection):\n    return duckdb.__version__",
         []),
        ("import sqlite3\nsqlite3.connect('x')", []),
        ("class A:\n    def m(self):\n        self.duckdb = 1", []),
        ("import os\nmode = os.getenv('KS_READ_X', 'duckdb')", []),
        # A command-line flag named like the driver is not the driver
        # (scripts/backfill_gender.py's `--duckdb`): only a module's
        # attribute is, unless a driver function follows it.
        ("import argparse\nargs = argparse.ArgumentParser().parse_args()\n"
         "if args.duckdb:\n    use = args.duckdb", []),
        ("from core.duckdb_store import duckdb\ntry:\n    pass\n"
         "except duckdb.IOException:\n    pass", []),
        # A program held as text, run in another process (batch-E review):
        # past the in-process count, and a read-only open changes no byte the
        # hash could see — so the static walk is the only one that can.
        ("_PROG = \"import duckdb as d, sys; print(d.connect(sys.argv[1]))\"",
         ["<module>"]),
        ("_PEEK = \"import duckdb, sys; "
         "print(duckdb.connect(sys.argv[1], read_only=True).execute('x').fetchone())\"",
         ["<module>"]),
        ("import subprocess\ndef f(q):\n"
         "    subprocess.run(['python', '-c', f'import duckdb; duckdb.sql({q!r})'])",
         ["f"]),
        ("_DELETE_ONE = r'''\nimport sys\nimport duckdb as ddb\n"
         "con = ddb.connect(sys.argv[1])\n'''", ["<module>"]),
        ("X = 'import duckdb\\n  con = duckdb.connect(p'", ["<module>"]),   # does not parse
        # D1 as it is: the driver for its exception class, the file through
        # the opener — no reach.
        ("_DELETE_ONE = r'''\nimport sys\nimport duckdb\n"
         "from core.duckdb_switch import open_file\ncon = open_file(sys.argv[1])\n"
         "try:\n    con.execute('DELETE FROM t')\nexcept duckdb.FatalException:\n"
         "    pass\n'''", []),
        ("doc = 'every `duckdb.connect` in core goes through open_file'", []),
    ])
    def test_the_walk_reads_every_spelling(self, source, expected):
        """Mutation: drop any one rule from `driver_references` — the module
        re-exported, reached as an attribute, imported by name, or a function
        other than `connect` — and its row fails."""
        assert driver_references(source) == expected

    def test_the_four_bypasses_of_the_review_are_each_found(self):
        """The review of 02.10 put these four functions in `core/` and the
        walk passed them, while each opened a file under `off` with nothing
        counted. Each must now be one finding, in its own function."""
        source = (
            "import importlib\n\n\n"
            "def via_reexport(p):\n"
            "    from core.duckdb_store import duckdb as d\n"
            "    return d.connect(p)\n\n\n"
            "def via_module_attr(p):\n"
            "    import core.duckdb_store as s\n"
            "    return s.duckdb.connect(p)\n\n\n"
            "def via_import_module(p):\n"
            "    return importlib.import_module('duckdb').connect(p)\n\n\n"
            "def via_default_connection(p):\n"
            "    import duckdb\n"
            "    duckdb.execute(f\"ATTACH '{p}' AS live\")\n"
            "    return duckdb.sql('SELECT count(*) FROM live.t').fetchone()[0]\n")
        assert sorted(driver_references(source)) == sorted([
            "via_reexport", "via_module_attr", "via_import_module",
            "via_default_connection", "via_default_connection"])

    def test_every_function_on_the_driver_counts_not_connect_alone(self):
        """Read off the driver: `execute` and `sql` run on the default
        connection, which `ATTACH` points at any file. Classes are not
        functions — the application names `duckdb.IOException`."""
        assert {"connect", "execute", "sql", "default_connection",
                "read_parquet"} <= DRIVER_FUNCTIONS
        assert not {"IOException", "DuckDBPyConnection", "ConstraintException"} & DRIVER_FUNCTIONS

    def test_the_backup_validation_goes_through_the_opener(self):
        """The one read-only open in the store, of a copy: still DuckDB, still
        refused under off."""
        source = (REPO / "core" / "duckdb_store.py").read_text(encoding="utf-8")
        assert source.count("duckdb_switch.open_file(") == 2


# ─── The weekly compaction: scheduled, so through the switch ─────────────────

class TestTheCompaction:
    """Every Sunday the host cron runs the compaction in a sidecar started with
    the whole `.env`, and its phase 1 opens the LIVE file read-only — a read
    changes no byte, so the hourly hash cannot see it, and the switch is the
    only detector there is. It opens through `open_file`, so under `off` the
    sidecar is refused before the driver runs, and the cron's abort quotes
    why (review of 02.10: phase 1 used to connect directly, and a week of
    silence could pass a Sunday in which the file was opened)."""

    @pytest.fixture
    def live(self, tmp_path, monkeypatch):
        import duckdb

        from scripts import compact_duckdb as compact

        data = tmp_path / "data"
        data.mkdir()
        for name, value in (("DATA_DIR", data), ("SOURCE_DB", data / "analytics.duckdb"),
                            ("NEW_DB", data / "analytics_clean.duckdb"),
                            ("EXPORT_DIR", data / "export_parquet"),
                            ("MANIFEST_PATH", data / "export_parquet" / "_manifest.json")):
            monkeypatch.setattr(compact, name, value)
        con = duckdb.connect(str(compact.SOURCE_DB))
        # Phase 1 checksums `orders`, so the smallest file it can export has one.
        con.execute("CREATE TABLE orders (id INTEGER, ordered_at TIMESTAMP, "
                    "grand_total DECIMAL(12, 2), status_id INTEGER, source_id INTEGER)")
        con.execute("INSERT INTO orders VALUES (1, TIMESTAMP '2026-09-01 10:00', 100, 1, 1)")
        con.close()
        return compact

    @staticmethod
    def _bytes(path: Path) -> tuple:
        return path.read_bytes(), path.stat().st_mtime_ns

    def test_under_off_phase_one_is_refused_before_the_driver(self, live, monkeypatch):
        """Mutation: open with `duckdb.connect(str(SOURCE_DB), read_only=True)`
        in `phase1_export` again — the driver is reached and nothing is
        counted (and the walk above finds a host tool it was not told of)."""
        import duckdb

        before = self._bytes(live.SOURCE_DB)
        reached = []
        real = duckdb.connect
        monkeypatch.setattr(duckdb, "connect",
                            lambda *a, **k: reached.append(a) or real(*a, **k))
        _off(monkeypatch)
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            live.phase1_export()
        assert reached == []
        assert list(duckdb_switch.opened()) == ["scripts.compact_duckdb:phase1_export"]
        assert self._bytes(live.SOURCE_DB) == before
        assert not live.EXPORT_DIR.exists()

    def test_under_on_phase_one_reads_as_ever(self, live):
        manifest = live.phase1_export()
        assert manifest["tables"] == ["orders"] and duckdb_switch.opened() == {}

    def test_the_cron_says_why_and_that_its_line_is_left_over(
        self, live, monkeypatch, capsys, tmp_path, fake_bin,
    ):
        """The whole sidecar under `off`: exit 1, and the line the wrapper
        quotes in "Compact aborted" names the switch and the cron line to
        remove. Mutation: drop the `except DuckDBOpenedWhileOff` in `main` —
        a traceback, whose last quotable line says neither."""
        from tests.unit.test_weekly_compact_wrapper import _run as wrapper_run

        monkeypatch.setenv("FORCE_COMPACT", "1")  # a laptop's free disk is no question here
        _off(monkeypatch)
        with pytest.raises(SystemExit) as exit_:
            live.main()
        assert exit_.value.code == 1
        printed = capsys.readouterr().out
        assert "PHASE 2" not in printed

        run = wrapper_run(tmp_path / "wrapper", fake_bin, printed, 1, "")
        first = run.notify.splitlines()[0]
        assert first.startswith(
            "🚨 Compact aborted: compact failed (exit 1): KS_DUCKDB=off: the weekly "
            "compaction is refused before it opens the DuckDB file"), first
        assert "Remove weekly_compact.sh from root's crontab" in first, first


class TestTheNightlySnapshot:
    """`deploy/snapshot_export.py` — the nightly off-site's sidecar, also
    started with the whole `.env` — runs the compaction's phase 1 against a
    hard link to the newest backup. Routing phase 1 through the switch refuses
    it too under `off`; it must say so in a line, not die in a traceback, and
    leave nothing behind for daily_offsite.sh to ship."""

    @pytest.fixture
    def snapshot(self, tmp_path, monkeypatch):
        import importlib.util

        import duckdb

        monkeypatch.syspath_prepend(str(REPO / "scripts"))
        import compact_duckdb as compact

        data = tmp_path / "data"
        data.mkdir()
        for name, value in (("DATA_DIR", data), ("SOURCE_DB", data / "analytics.duckdb"),
                            ("EXPORT_DIR", data / "export_parquet"),
                            ("MANIFEST_PATH", data / "export_parquet" / "_manifest.json")):
            monkeypatch.setattr(compact, name, value)
        con = duckdb.connect(str(compact.SOURCE_DB))
        con.execute("CREATE TABLE orders (id INTEGER)")
        con.close()
        spec = importlib.util.spec_from_file_location(
            "snapshot_export_under_test", REPO / "deploy" / "snapshot_export.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        monkeypatch.setattr(module, "PREVIOUS_PATH", data / ".last_snapshot.json")
        return module, compact

    def test_under_off_it_is_refused_in_a_line_and_ships_nothing(
        self, snapshot, monkeypatch, capsys,
    ):
        """Mutation: drop the `except DuckDBOpenedWhileOff` in its `main` — the
        refusal leaves as a traceback, not exit 1 with the line."""
        import duckdb

        module, compact = snapshot
        reached = []
        real = duckdb.connect
        monkeypatch.setattr(duckdb, "connect",
                            lambda *a, **k: reached.append(a) or real(*a, **k))
        _off(monkeypatch)
        with pytest.raises(SystemExit) as exit_:
            module.main()
        assert exit_.value.code == 1
        printed = capsys.readouterr().out
        assert ("KS_DUCKDB=off: the nightly DuckDB snapshot is refused before it opens "
                "the backup") in printed, printed
        assert "Remove daily_offsite.sh from root's crontab" in printed
        assert reached == []
        assert list(duckdb_switch.opened()) == ["compact_duckdb:phase1_export"]
        assert not compact.EXPORT_DIR.exists() and not module.PREVIOUS_PATH.exists()


# ─── Web: it starts, and it says so ──────────────────────────────────────────

class TestTheStartup:
    def _stub(self, monkeypatch):
        import web.main as main

        monkeypatch.setattr(main, "validate_config", MagicMock())
        monkeypatch.setattr(main, "init_database", MagicMock())
        monkeypatch.setattr(main, "start_scheduler", AsyncMock())
        monkeypatch.setattr(main, "_register_event_handlers", MagicMock())
        monkeypatch.setattr("core.prediction_service.get_prediction_service",
                            MagicMock(return_value=MagicMock(is_ready=True)))
        return main

    def _off_boot(self, monkeypatch, data, init_and_sync):
        """web's real `startup_event` under off (`duckdb_off`, whose directory
        is `data`), around the boot sync it is given; the campaign restore
        recorded rather than written. Returns (main, restore)."""
        import core.duckdb_store as store_module

        main = self._stub(monkeypatch)
        restore = AsyncMock(return_value=False)
        monkeypatch.setattr(store_module.DuckDBStore, "backfill_sms_campaign_record",
                            restore)
        monkeypatch.setattr(main, "init_and_sync", init_and_sync)
        # web's logger does not propagate (core.observability), so it is read
        # where it is called.
        monkeypatch.setattr(main, "logger", MagicMock())
        asyncio.run(main.startup_event())
        main.start_scheduler.assert_awaited_once()
        assert not data.exists(), "the file, the WAL and the spill directory untouched"
        return main, restore

    def test_off_boots_without_opening_the_file(self, monkeypatch, duckdb_off):
        """Stage 5, PR-1: the boot sync runs on the unconnected store, the
        counts web used to log are not asked for, and the campaign restore —
        the SMS router's, Postgres under KS_SMS_STORE=postgres — still runs.
        Mutation: call `get_stats()` in the success branch under off again."""
        import core.duckdb_store as store_module

        hosts = []

        async def boot_sync(**_):
            hosts.append(await store_module.get_store())

        main, restore = self._off_boot(monkeypatch, duckdb_off, boot_sync)
        assert duckdb_switch.opened() == {}
        [store] = hosts
        assert store._connection is None
        restore.assert_awaited_once()
        said = " ".join(str(c.args[0]) for c in main.logger.info.call_args_list)
        assert "KS_DUCKDB=off, so DuckDB is not opened" in said
        assert "Dashboard ready (KS_DUCKDB=off)" in said
        assert "all queries use DuckDB" not in said

    def test_off_a_failed_boot_sync_does_not_end_the_startup(self, monkeypatch, duckdb_off):
        """The generic handler asked `get_stats()` before anything else, and
        under off that raised a second refusal out of the handler, uncaught:
        any failed boot sync — KeyCRM down, Postgres down — was a crash loop
        of the only process that syncs orders. Mutation: drop the
        `duckdb_switch.is_off()` branch in that handler."""

        async def boot_sync(**_):
            raise OSError("KeyCRM is down")

        main, restore = self._off_boot(monkeypatch, duckdb_off, boot_sync)
        assert duckdb_switch.opened() == {}
        restore.assert_awaited_once()
        warned = " ".join(str(c.args[0]) for c in main.logger.warning.call_args_list)
        assert "boot sync failed under KS_DUCKDB=off" in warned

    def test_off_a_refused_boot_sync_is_contained_and_counted(self, monkeypatch, duckdb_off):
        """A path stage 5 has not decoupled yet — or orders still held in
        DuckDB, with chain 3 off — reaches for the file inside the boot sync:
        counted where it was refused, and web starts all the same, on the
        unconnected store. Mutation: delete the `except
        duckdb_switch.DuckDBOpenedWhileOff` branch in `startup_event`."""
        import core.duckdb_store as store_module

        async def boot_sync(**_):
            async with (await store_module.get_store()).connection():
                pass

        main, restore = self._off_boot(monkeypatch, duckdb_off, boot_sync)
        assert list(duckdb_switch.opened()) == [f"{__name__}:boot_sync"]
        said = " ".join(str(c.args[0]) for c in main.logger.critical.call_args_list)
        assert "the boot sync reached for the DuckDB file and was refused" in said
        restore.assert_awaited_once()

    def test_on_is_the_path_it_always_was(self, monkeypatch):
        main = self._stub(monkeypatch)
        store = MagicMock()
        store.get_stats = AsyncMock(return_value={
            "orders": 1, "products": 1, "categories": 1, "db_size_mb": 1})
        store.backfill_sms_campaign_record = AsyncMock(return_value=False)
        monkeypatch.setattr(main, "init_and_sync", AsyncMock())
        monkeypatch.setattr(main, "get_store", AsyncMock(return_value=store))
        asyncio.run(main.startup_event())
        store.backfill_sms_campaign_record.assert_awaited_once()
        store.get_stats.assert_awaited_once()
        assert duckdb_switch.opened() == {}

    @pytest.mark.parametrize("orders, ends", [(0, True), (5, False)])
    def test_on_a_failed_boot_sync_still_asks_duckdb_first(self, orders, ends, monkeypatch):
        """Under `on` the handler is what it was: the store is asked for its
        counts, and an empty DuckDB with chain 3 off ends the startup."""
        main = self._stub(monkeypatch)
        store = MagicMock()
        store.get_stats = AsyncMock(return_value={"orders": orders})
        store.backfill_sms_campaign_record = AsyncMock(return_value=False)
        monkeypatch.setattr(main, "init_and_sync", AsyncMock(side_effect=OSError("down")))
        monkeypatch.setattr(main, "get_store", AsyncMock(return_value=store))
        if ends:
            with pytest.raises(OSError):
                asyncio.run(main.startup_event())
        else:
            asyncio.run(main.startup_event())
            main.start_scheduler.assert_awaited_once()
        store.get_stats.assert_awaited_once()


class TestTheBootSync:
    """`init_and_sync` under off asks the store the orders are written to
    whether there is history, and never DuckDB's counts (stage 5, PR-1).
    Under `on` the order is pinned in `test_warehouse_writer.py`
    (settle, stats, sync)."""

    def _boot(self, monkeypatch, held):
        from core import sync_service as mod

        store = MagicMock()
        store.get_stats = AsyncMock(side_effect=AssertionError("DuckDB's counts"))
        store.orders_held = AsyncMock(return_value=held)
        store.refresh_sku_inventory_status = AsyncMock(return_value=0)
        service = MagicMock()
        service.full_sync = AsyncMock()
        service.incremental_sync = AsyncMock()
        monkeypatch.setattr(mod, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(mod, "get_sync_service", AsyncMock(return_value=service))
        monkeypatch.setattr(mod, "init_meilisearch", AsyncMock(return_value=False))
        monkeypatch.setattr(mod.warehouse_cutover, "settle_writer", AsyncMock())
        asyncio.run(mod.init_and_sync())
        store.get_stats.assert_not_awaited()
        store.orders_held.assert_awaited_once()
        return service

    def test_off_history_held_is_an_incremental_sync(self, monkeypatch, duckdb_off):
        """Mutation: ask `get_stats()` under off again."""
        service = self._boot(monkeypatch, held=46_000)
        service.incremental_sync.assert_awaited_once()
        service.full_sync.assert_not_awaited()

    def test_off_no_history_is_a_full_sync(self, monkeypatch, duckdb_off):
        service = self._boot(monkeypatch, held=0)
        service.full_sync.assert_awaited_once()
        service.incremental_sync.assert_not_awaited()


class TestTheFullSync:
    """`full_sync` checkpoints after each 30-day chunk. Under off the store is
    unconnected and its checkpoint a no-op already; skipped by name, so the
    no-op is not one connection away from a CHECKPOINT (stage 5, PR-1)."""

    def _chunked(self, store):
        from tests.unit.test_warehouse_writer import _service, _sync

        service = _service(store, orders=[{"id": 1, "status_id": 1}])
        _sync(lambda: service.full_sync(days_back=45))

    def _store(self):
        from tests.unit.test_warehouse_writer import _FullSyncStore

        store = _FullSyncStore()
        store.checkpoint = AsyncMock()
        return store

    def test_on_every_chunk_is_checkpointed(self):
        store = self._store()
        self._chunked(store)
        assert store.checkpoint.await_count == 2

    def test_off_no_chunk_is(self, duckdb_off):
        """Mutation: drop the `is_off()` guard around the chunk's checkpoint."""
        store = self._store()
        self._chunked(store)
        store.checkpoint.assert_not_awaited()


class TestARefusalOnAnHTTPPath:
    """Stage 5, PR-1: a route that reaches for the file under off answers
    503 naming `duckdb`, beside `ReadUnavailable` — not the 500 it fell
    through to. The refusal is counted where it was raised, and the site
    stays on /api/health: a response is public to the page."""

    def test_it_is_a_503_naming_the_surface(self, admin_client, monkeypatch):  # noqa: F811
        from fastapi.routing import APIRoute

        from web.main import app

        _off(monkeypatch)

        async def reaches_for_the_file():
            from core.duckdb_store import get_store

            async with (await get_store()).connection():
                pass

        # First, ahead of the SPA's catch-all, and taken out again after.
        route = APIRoute("/api/_test_reaches_for_duckdb", reaches_for_the_file,
                         methods=["GET"])
        app.router.routes.insert(0, route)
        try:
            response = admin_client.get(route.path)
        finally:
            app.router.routes.remove(route)
        assert response.status_code == 503, response.text
        assert response.json() == {
            "error": "Service Unavailable",
            "detail": "This page needs the DuckDB file, which is switched off.",
            "surface": "duckdb"}
        assert list(duckdb_switch.opened()) == [f"{__name__}:reaches_for_the_file"]
        assert "reaches_for_the_file" not in response.text


class TestWhatWebPublishes:
    def test_on_publishes_the_mode_and_nothing_opened(self, admin_client):  # noqa: F811
        health = admin_client.get("/api/health").json()
        assert health["duckdb_switch"] == {"mode": "on", "value": None,
                                           "error": None, "opened_while_off": {}}
        assert canary.check_duckdb_switch(health) == []
        assert canary.check_duckdb_mode(health) == []
        assert canary.duckdb_silent(health) is None

    def test_off_publishes_every_site_that_reached_for_the_file(
        self, admin_client, monkeypatch,  # noqa: F811
    ):
        """Health itself still counts DuckDB's tables (`get_stats`, until
        stage 5's PR-3), so under off its first answer already carries a
        site — the tripwire proving itself. The 60 s stats cache is emptied
        first: a count cached under `on` by an earlier request is not a reach
        for the file. Mutation: drop `duckdb_switch` from `health_check`'s
        answer."""
        from web.routes.api import health as health_routes

        monkeypatch.setitem(health_routes._stats_cache, "data", None)
        monkeypatch.setitem(health_routes._stats_cache, "expires_at", 0)
        _off(monkeypatch)
        health = admin_client.get("/api/health").json()
        block = health["duckdb_switch"]
        assert block["mode"] == "off"
        assert list(block["opened_while_off"]) == [
            "web.routes.api.health:health_check"], block
        assert "tried to open" not in json.dumps(block)
        [(key, _line)] = canary.check_duckdb_switch(health)
        assert key == "duckdb_opened_while_off"
        assert canary.duckdb_silent(health) is False


# ─── The canary ──────────────────────────────────────────────────────────────

def _entry(count=1):
    return {"count": count,
            "last_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


async def _probe(payload):
    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=cert):
            return await canary.run_canary(DASHBOARD, client=client)


class TestTheCanary:
    def test_both_keys_are_registered_conditions(self):
        for key in ("duckdb_opened_while_off", "duckdb_mode_invalid"):
            assert REGISTRY[key].kind is Kind.CONDITION, key

    def test_an_absent_or_empty_block_says_nothing(self):
        for payload in (None, {}, {"duckdb_switch": None},
                        {"duckdb_switch": {"mode": "off", "opened_while_off": {}}}):
            assert canary.check_duckdb_switch(payload) == []

    def test_an_open_under_off_pages_naming_the_sites(self):
        payload = {"duckdb_switch": {"mode": "off", "opened_while_off": {
            "web.routes.api.health:health_check": _entry(4)}}}
        [(key, line)] = canary.check_duckdb_switch(payload)
        assert key == "duckdb_opened_while_off"
        assert line.startswith(canary.DUCKDB_OPENED_LINE)
        assert "web.routes.api.health:health_check ×4" in line

    @pytest.mark.asyncio
    async def test_it_pages_critical_with_its_own_title_and_lever_first(self):
        """Mutation: make the severity `warn` in `run_canary`."""
        payload = _healthy_payload()
        payload["duckdb_switch"] = {"mode": "off", "value": "off", "error": None,
                                    "opened_while_off": {"core.x:y": _entry()}}
        result = await _probe(payload)
        assert result.severity == "critical"
        assert result.failure_keys == ["duckdb_opened_while_off"]
        assert canary._title(result) == "DuckDB opened while off"
        assert canary._what_to_do(result).startswith("KS_DUCKDB=off and web opened DuckDB")
        assert result.duckdb_silent is False

    @pytest.mark.asyncio
    async def test_its_lever_outranks_the_outage_it_causes(self):
        """Under off today web cannot work, so health is degraded too — the
        cause is the switch, and its lever comes first."""
        payload = _healthy_payload()
        payload["status"] = "degraded"
        payload["duckdb_switch"] = {"mode": "off", "value": "off", "error": None,
                                    "opened_while_off": {"core.x:y": _entry()}}
        result = await _probe(payload)
        assert "duckdb_opened_while_off" in result.failure_keys
        assert canary._what_to_do(result).startswith("KS_DUCKDB=off")

    @pytest.mark.asyncio
    async def test_a_typo_warns_and_is_not_silence(self):
        payload = _healthy_payload()
        payload["duckdb_switch"] = {"mode": "on", "value": "of",
                                    "error": "KS_DUCKDB='of' is not one of ('on', 'off')",
                                    "opened_while_off": {}}
        result = await _probe(payload)
        assert result.severity == "warn"
        assert result.failure_keys == ["duckdb_mode_invalid"]
        assert result.duckdb_silent is None

    @pytest.mark.parametrize("block, silent", [
        (None, None),
        ({"mode": "on", "opened_while_off": {}}, None),
        ({"mode": "off", "opened_while_off": {}}, True),
        ({"mode": "off", "opened_while_off": {"a:b": {"count": 1}}}, False),
    ])
    def test_the_watch_is_said_only_under_off(self, block, silent):
        payload = {} if block is None else {"duckdb_switch": block}
        assert canary.duckdb_silent(payload) is silent

    @pytest.mark.asyncio
    async def test_production_today_writes_no_watch(self):
        """`on`, the default: the probe reads the block and has nothing to say
        about the week. Mutation: make `duckdb_silent` answer for `on`."""
        payload = _healthy_payload()
        payload["duckdb_switch"] = {"mode": "on", "value": None, "error": None,
                                    "opened_while_off": {}}
        result = await _probe(payload)
        assert result.ok and result.duckdb_silent is None
        assert "duckdb_opened_while_off" not in result.unjudged_keys

    def test_the_lever_is_one_line_of_at_most_150(self):
        actions = dict(canary._ACTIONS)
        for key in ("duckdb_opened_while_off", "duckdb_mode_invalid"):
            assert "\n" not in actions[key] and len(actions[key]) <= 150, key


def _canary_job() -> ast.AsyncFunctionDef:
    tree = ast.parse((REPO / "bot" / "main.py").read_text(encoding="utf-8"))
    [job] = [n for n in ast.walk(tree)
             if isinstance(n, ast.AsyncFunctionDef) and n.name == "canary_job"]
    return job


class TestTheBotWritesTheWatch:
    def test_once_per_probe_and_only_when_web_runs_off(self):
        """Parsed: one `record_watch` under the week's key, with the probe's
        reading, the canary's gap and web's uptime, guarded by `is not None`
        and nothing else. Mutation: drop the guard (a web running `on` would
        write the row every probe)."""
        job = _canary_job()
        calls = [n for n in ast.walk(job) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == "record_watch"
                 and n.args and ast.unparse(n.args[0]) == "DUCKDB_SWITCH_WATCH_KEY"]
        assert len(calls) == 1
        kwargs = {k.arg: ast.unparse(k.value) for k in calls[0].keywords}
        assert kwargs == {"clean": "result.duckdb_silent",
                          "gap_s": "DUCKDB_SWITCH_WATCH_GAP_S",
                          "web_uptime_s": "result.web_uptime_s"}
        parents = {c: n for n in ast.walk(job) for c in ast.iter_child_nodes(n)}
        node, guards = calls[0], []
        while node is not job:
            node = parents[node]
            if isinstance(node, ast.If):
                guards.append(ast.unparse(node.test))
        assert guards == ["result.duckdb_silent is not None"], guards

    def test_the_watch_key_is_not_an_alert(self):
        assert canary.DUCKDB_SWITCH_WATCH_KEY.startswith("watch:")
        assert canary.DUCKDB_SWITCH_WATCH_KEY not in REGISTRY
        assert canary.DUCKDB_SWITCH_WATCH_GAP_S == canary.READ_FALLBACK_WATCH_GAP_S
