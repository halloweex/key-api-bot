"""The analytics store without DuckDB (step 14, S14-18).

S14-18 took the DuckDB half out of the store — the connection, the schema and
its migrations, the checkpoint, the nightly backup, the FATAL bookkeeping, the
counts — deleted the switch (`core/duckdb_switch.py`) with its health block,
its three pages and its watch, and deleted the compaction and the nightly
snapshot. What is held here:

* `connection()`, kept one release, refuses: `StoreRemoved`, a
  `ReadUnavailable` naming `duckdb`, counted where it is raised, answered as
  the 503 every refused read gets, and paged by the canary as `read_refused`.
  It depends on nothing DuckDB — the handler the switch's refusal had is gone.
* the store holds nothing DuckDB, its modules load no driver, and the deleted
  modules and scripts are gone;
* rule 3: junk, or nothing, in the variables S14-18 stopped reading
  (`KS_DUCKDB`, `DUCKDB_MEMORY_LIMIT`) — import, configure and `/api/health`
  answer as with production's values;
* rule 5: nothing in the code deletes or renames the DuckDB file, its
  backups, the export or the Arks, and the chain markers and the report
  spool are touched by the two functions that always did and no other;
* rule 8: the conditions S14-18 removed are no condition and no lever.

Every guard names, in its docstring, the mutation it catches.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from core import read_fallback
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

REPO = Path(__file__).resolve().parents[2]
TREES = ("core", "web", "bot", "scripts")


@pytest.fixture
def counts():
    read_fallback.reset_counts()
    yield read_fallback
    read_fallback.reset_counts()


# ─── the stub ────────────────────────────────────────────────────────────────

class TestTheConnectionStub:
    def test_it_is_a_refusal_naming_duckdb(self, counts):
        """Mutation: raise anything but a `ReadUnavailable`, or name another
        surface — the route would answer 500, or the page say another tab."""
        from core.store import AnalyticsStore, StoreRemoved

        assert issubclass(StoreRemoved, read_fallback.ReadUnavailable)
        with pytest.raises(StoreRemoved) as caught:
            AnalyticsStore().connection()
        assert caught.value.surface == "duckdb"
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert "S14-18" in str(caught.value.__cause__)

    def test_it_is_counted_where_it_is_raised(self, counts):
        """Counted at the raise, as `refuse` counts: a caller that swallows it
        still leaves the count. Mutation: raise without counting."""
        from core.store import AnalyticsStore, StoreRemoved

        store = AnalyticsStore()
        for _ in range(2):
            try:
                store.connection()
            except StoreRemoved:
                pass
        assert counts.refusals()["duckdb"]["count"] == 2

    def test_the_old_spelling_meets_it_too(self, counts):
        """`async with store.connection() as conn:` — the shape every DuckDB
        caller had — raises the refusal before the block runs."""
        from core.duckdb_store import DuckDBStore, StoreRemoved

        async def caller():
            async with DuckDBStore().connection() as conn:  # noqa: F841
                raise AssertionError("the block ran")

        with pytest.raises(StoreRemoved):
            asyncio.run(caller())

    def test_a_route_that_reaches_it_answers_the_counted_503_and_pages(
            self, admin_client, counts, monkeypatch):  # noqa: F811
        """What a caller nobody found would cost: the 503 naming `duckdb`, its
        count on `/api/health`, and `read_refused` from the canary — never a
        500. The handler the switch's refusal had (`DuckDBOpenedWhileOff`) is
        gone, so the stub must not need it. Mutation: delete the stub (an
        `AttributeError`, a 500), or have it raise the old exception."""
        from bot import canary
        from core.store import AnalyticsStore

        async def reaches_duckdb(self):
            async with self.connection():
                return []

        monkeypatch.setattr(AnalyticsStore, "get_all_managers", reaches_duckdb)
        response = admin_client.get("/api/managers")
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "duckdb"

        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"]["refused"]["duckdb"]["count"] == 1
        assert [k for k, _ in canary.check_read_refusals(health)] == ["read_refused"]

    def test_web_main_has_no_handler_of_the_switch_s(self):
        """Mutation: put `duckdb_opened_while_off_handler` back."""
        from web.main import app

        names = {getattr(h, "__name__", "") for h in app.exception_handlers.values()}
        assert "duckdb_opened_while_off_handler" not in names
        assert "query_timeout_handler" not in names
        assert "read_unavailable_handler" in names


# ─── the store holds nothing DuckDB ──────────────────────────────────────────

GONE_FROM_THE_STORE = (
    "connect", "close", "_fetch_one", "_fetch_all", "_offload", "_init_schema",
    "_build_view", "_run_migrations", "_record_migration", "schema_status",
    "_read_schema_status", "_create_inventory_views", "checkpoint",
    "backup_database", "_send_warehouse_alert", "fatal_status",
    "_drop_if_invalidated", "_read_index_short", "_record_index_short",
    "get_stats", "db_path", "_connection", "_lock", "_executor",
)


class TestTheStore:
    def test_nothing_of_duckdb_is_left_on_it(self):
        """Mutation: put any of the deleted methods back."""
        from core.store import AnalyticsStore

        store = AnalyticsStore()
        for name in GONE_FROM_THE_STORE:
            assert not hasattr(store, name), name

    def test_the_shim_is_the_store_and_nothing_else(self):
        """`core/duckdb_store.py` re-exports the store under its old name for
        one release, and nothing more — no driver, no path, no schema.
        Mutation: define anything in the shim again."""
        from core import duckdb_store, store

        assert duckdb_store.DuckDBStore is store.AnalyticsStore
        assert duckdb_store.get_store is store.get_store
        assert duckdb_store.close_store is store.close_store
        tree = ast.parse(Path(duckdb_store.__file__).read_text(encoding="utf-8"))
        defined = [n for n in tree.body
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
        assert defined == []
        for gone in ("DB_PATH", "DB_DIR", "duckdb", "SILVER_ORDERS_DDL",
                     "StoreClosedError", "_memory_limit", "INDEX_SHORT_MARKER"):
            assert not hasattr(duckdb_store, gone), gone

    def test_one_store_per_process_and_close_forgets_it(self):
        from core import store

        async def run():
            first = await store.get_store()
            again = await store.get_store()
            await store.close_store()
            fresh = await store.get_store()
            return first, again, fresh

        first, again, fresh = asyncio.run(run())
        assert first is again and fresh is not first

    def test_the_store_and_web_load_no_driver(self, tmp_path):
        """In an interpreter of its own, with no `.env` to read: importing
        the store, its shim, web and the scheduler loads no `duckdb`.
        Mutation: import `duckdb` anywhere on that path."""
        code = ("import sys\n"
                "import core.store, core.duckdb_store, core.scheduler, web.main\n"
                "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] == 'duckdb')))\n")
        done = _python(tmp_path, "import json\n" + code, {})
        assert done.returncode == 0, done.stderr[-3000:]
        assert json.loads(done.stdout.strip().splitlines()[-1]) == []

    @pytest.mark.parametrize("module", [
        "core.duckdb_switch", "core.migrations", "core.duckdb_sequences",
        "core.snapshot_validation", "core.pg_sms"])
    def test_the_deleted_modules_are_gone(self, module):
        import importlib.util

        assert importlib.util.find_spec(module) is None

    @pytest.mark.parametrize("path", [
        "scripts/compact_duckdb.py", "scripts/weekly_compact.sh",
        "deploy/compact_floor_check.py", "deploy/daily_offsite.sh",
        "deploy/snapshot_export.py", "deploy/offsite_parquet.sh"])
    def test_the_deleted_host_tools_are_gone(self, path):
        """Their cron lines were retired on 2026-10-16 (the plan's H4 checks
        `crontab -l` before the deploy); the pinned image keeps the
        compaction for a restore."""
        assert not (REPO / path).exists()

    def test_the_two_dead_entries_are_gone(self):
        """`SyncService.sync_today` and `auth_service.get_user_role` had no
        caller (the stage 5 entry-point walk listed them dead)."""
        from core.sync_service import SyncService
        from web.services import auth_service

        assert not hasattr(SyncService, "sync_today")
        assert not hasattr(auth_service, "get_user_role")

    def test_the_row_dump_cut_is_gone_from_the_log_handler(self):
        """A DuckDB FATAL's row dump cannot be printed by a process that
        opens no DuckDB, so `setup_logging` installs no filter for it.
        Mutation: install `RowDumpFilter` again."""
        import logging

        from core import observability

        for gone in ("RowDumpFilter", "cut_row_dumps", "without_row_dump", "ROW_DUMP"):
            assert not hasattr(observability, gone), gone
        root = logging.getLogger()
        saved = list(root.handlers), root.level
        try:
            observability.setup_logging("INFO")
            assert all(not h.filters for h in root.handlers)
        finally:
            root.handlers[:], _ = saved[0], root.setLevel(saved[1])

    def test_no_mode_is_configured(self):
        """`configure_modes()` names no mode since S14-18 (`KS_DUCKDB` was the
        last). Mutation: read `KS_DUCKDB` there again."""
        from core.runtime_modes import configure_modes

        assert configure_modes() == {}


# ─── rule 3: the variables S14-18 stopped reading ────────────────────────────

STOPPED_READING = ("KS_DUCKDB", "DUCKDB_MEMORY_LIMIT")

_START = """
import asyncio, json, sys
from fastapi.testclient import TestClient
from core.runtime_modes import configure_modes
from core.store import get_store
import web.main
from web.routes.api import health

modes = configure_modes()
store = asyncio.run(get_store())
for value in vars(health).values():
    if isinstance(value, dict) and "expires_at" in value:
        value["expires_at"] = 0
response = TestClient(web.main.app).get("/api/health")
body = response.json()
print(json.dumps({"modes": modes, "status_code": response.status_code,
                  "status": body.get("status"), "switch": "duckdb_switch" in body,
                  "driver": "duckdb" in sys.modules}))
"""


def _python(tmp_path: Path, code: str, extra: dict) -> subprocess.CompletedProcess:
    """`code` in an interpreter of its own, run from a scratch directory, with
    test secrets and no `.env` of the checkout to inherit."""
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
           "TZ": "UTC", "PYTHONPATH": str(REPO), "PYTHONDONTWRITEBYTECODE": "1",
           "DASHBOARD_SECRET_KEY": "s14-18", "BOT_TOKEN": "1:s14-18",
           "KEYCRM_API_KEY": "s14-18", "KS_ALERTS_DISABLED": "1",
           "KS_ALERT_GATE_STATE_DIR": "", "KS_INSTANCE": "s14-18", **extra}
    return subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env,
                          capture_output=True, text=True, timeout=180)


@pytest.mark.parametrize("how", ["junk", "absent", "production"])
def test_a_variable_it_stopped_reading_changes_nothing(tmp_path, how):
    """Rule 3: junk in either, both at once, or neither — the modules
    import, the modes configure, the store is built and `/api/health` answers
    200 healthy with no switch block, and no driver is loaded. A junk
    `DUCKDB_MEMORY_LIMIT` used to be read by the store's open alone, which
    nothing reaches any more. Mutation: read either for anything again."""
    extra = {"production": {"KS_DUCKDB": "off", "DUCKDB_MEMORY_LIMIT": "4GB"},
             "junk": {"KS_DUCKDB": "junk-s14", "DUCKDB_MEMORY_LIMIT": "junk-s14"},
             "absent": {}}[how]
    done = _python(tmp_path, _START, extra)
    assert done.returncode == 0, done.stderr[-3000:]
    result = json.loads(done.stdout.strip().splitlines()[-1])
    assert result == {"modes": {}, "status_code": 200, "status": "healthy",
                      "switch": False, "driver": False}, result


# ─── rule 5: what the code may delete ────────────────────────────────────────

_DELETING = {"unlink", "rename", "replace", "remove", "rmtree", "move", "rmdir",
             "truncate", "removedirs"}
# Words a function that deletes or renames would have to name to reach the
# DuckDB file, its backups, the export or the Arks.
_DUCKDB_PLACES = ("analytics", "DUCKDB_FILE_NAME", "DB_PATH", "DB_DIR",
                  "export_parquet", "backups", "/ark", "BACKUP_ARK", ".duckdb")
# The two that touch the other protected places, and why they may.
ALLOWED = {
    ("core/chain_latch.py", "release"): (
        "the chain marker's own remover; no caller since S14-04 deleted the "
        "copy-back (tests only)"),
    ("core/report_ledger.py", "drain"): (
        "lands a spooled week in Postgres, then removes that spooled file — "
        "the spool's design (OD-16 (a))"),
}


def _deleters():
    for top in TREES:
        for path in sorted((REPO / top).rglob("*.py")):
            if "frontend" in path.parts:
                continue
            rel = path.relative_to(REPO).as_posix()
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                         and isinstance(n.func, ast.Attribute)
                         and n.func.attr in _DELETING
                         # `str.replace` is not a rename: only a path's, os's
                         # or shutil's `replace` moves a file.
                         and (n.func.attr != "replace" or len(n.args) < 2)]
                if calls:
                    yield rel, fn


class TestNothingDeletesWhatRule5Keeps:
    def test_no_deleting_function_names_the_duckdb_file_or_its_copies(self):
        """Mutation: write a function that unlinks `DATA_DIR / DUCKDB_FILE_NAME`,
        a backup or the export."""
        offenders = []
        for rel, fn in _deleters():
            body = ast.unparse(fn)
            hit = [w for w in _DUCKDB_PLACES if w in body]
            if hit:
                offenders.append(f"{rel}:{fn.name} {hit}")
        assert offenders == [], offenders

    def test_the_markers_and_the_spool_are_touched_by_the_two_that_always_did(self):
        """Mutation: delete a marker or a spooled file from anywhere else."""
        touching = set()
        for rel, fn in _deleters():
            body = ast.unparse(fn)
            if any(w in body for w in ("MARKER_DIR", "write-chain-owners", "SPOOL_DIR",
                                       "report-ledger-pending")):
                touching.add((rel, fn.name))
        assert touching <= set(ALLOWED), touching - set(ALLOWED)
        # The two modules that own the places delete in those two functions
        # alone (the spool's `drain` reaches its files through `_spooled`).
        owners = {(rel, fn.name) for rel, fn in _deleters()
                  if rel in ("core/chain_latch.py", "core/report_ledger.py")}
        assert owners == set(ALLOWED), owners

    def test_the_walk_sees_a_deletion(self, tmp_path):
        source = ("from core.paths import DATA_DIR, DUCKDB_FILE_NAME\n"
                  "def wipe():\n    (DATA_DIR / DUCKDB_FILE_NAME).unlink()\n")
        fn = ast.parse(source).body[1]
        assert any(isinstance(n, ast.Call) and n.func.attr == "unlink"
                   for n in ast.walk(fn) if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Attribute))
        assert "DUCKDB_FILE_NAME" in ast.unparse(fn)


# ─── rule 8: the conditions S14-18 removed ───────────────────────────────────

REMOVED_CONDITIONS = ("duckdb_opened_while_off", "duckdb_mode_invalid",
                      "duckdb_off_held_back")
REMOVED_EVENTS = ("warehouse:backup_preflight", "warehouse:backup_failed")


class TestTheRemovedConditions:
    def test_none_is_registered_or_has_a_lever(self):
        """Mutation: put any of them back in the REGISTRY or the canary's
        levers and titles without its emitter."""
        from bot import canary
        from core.alerting import REGISTRY

        for key in (*REMOVED_CONDITIONS, *REMOVED_EVENTS):
            assert key not in REGISTRY, key
        levers = {k for k, _ in canary._ACTIONS} | {k for k, _ in canary._CRITICAL_TITLES}
        assert not levers & set(REMOVED_CONDITIONS)
        assert set(REMOVED_CONDITIONS).isdisjoint(canary.unjudged_keys(None))

    def test_nothing_emits_them(self):
        """Parsed: no string in the code names one as a key to raise."""
        found = []
        for top in TREES:
            for path in sorted((REPO / top).rglob("*.py")):
                if "frontend" in path.parts:
                    continue
                for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                    if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                            and node.value in (*REMOVED_CONDITIONS, *REMOVED_EVENTS)):
                        found.append(f"{path.relative_to(REPO)}:{node.lineno}")
        assert found == [], found
