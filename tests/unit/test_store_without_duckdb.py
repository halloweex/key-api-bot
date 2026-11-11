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
* rule 5: nothing in the code — Python or shell, `deploy/` included —
  deletes, renames or replaces the DuckDB file, its backups, the export or
  the Arks but the Ark shipping's cleanup of its own work, and the chain
  markers and the report spool are touched by the four functions that always
  did and no other (`tests/deletion_walk.py`);
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
from tests import deletion_walk
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
#
# The walk is `tests/deletion_walk.py`, which says what it reads and what it
# cannot. Every mutation case below runs through the same two functions the
# guards run — over the real tree with one file added or changed — never
# through a hand-made `ast.parse`. The review of S14-18 found the first form
# blind to four of them (a from-import, a module constant, `deploy/`, a helper
# naming the marker); each is a case here.

# Every deletion, rename or replacement that reaches a protected place, with
# the places it reaches and why it may. Equality, not a subset: a new one, a
# stale entry and a place that moved all fail.
ALLOWED_PY = {
    ("core/chain_latch.py", "latch"): (
        ("MARKER_DIR", "write-chain-owners"),
        "writes the chain's marker at its first Postgres write, atomically "
        "over its own path — the one write rule 5 allows (decision 3)"),
    ("core/chain_latch.py", "release"): (
        ("MARKER_DIR", "write-chain-owners"),
        "the marker's own remover; no caller since S14-04 deleted the "
        "copy-back (tests only)"),
    ("core/report_ledger.py", "spool"): (
        ("SPOOL_DIR", "report-ledger-pending"),
        "writes a delivered week Postgres refused, atomically — the spool's "
        "design (OD-16 (a))"),
    ("core/report_ledger.py", "drain"): (
        ("SPOOL_DIR", "report-ledger-pending"),
        "lands a spooled week in Postgres, then removes that spooled file "
        "(OD-16 (a))"),
}
# The Ark's own shipping (stage 5's PR-11, before step 14) cleans up after
# itself inside the Ark root: the work directory `.ship.*` it made, the copy
# it fetched back, and an Ark this run froze and that failed. Never an Ark a
# run did not make.
_ARK = "deploy/ark_ship.sh"
ALLOWED_SHELL = {
    (_ARK, 'rm -rf "${OUT_ROOT:?}/$s"'): (
        ("/root/ark",), "a freeze that failed: the stamps it made, never one in `$had`"),
    (_ARK, 'rm -rf "$ARK_DIR"'): (
        ("/root/ark", "BACKUP_ARK"),
        "the Ark this run just froze, when the source moved under it"),
    (_ARK, 'rm -f "$plain"'): (
        ("/root/ark", "BACKUP_ARK"), "the plain tarball in the run's work directory"),
    (_ARK, 'rm -f "$WORK/$enc"'): (
        ("/root/ark", "BACKUP_ARK"), "the encrypted tarball in the work directory, shipped"),
    (_ARK, 'rm -f "$back/$enc"'): (
        ("/root/ark", "BACKUP_ARK"), "the copy fetched back to verify"),
    (_ARK, 'rm -f "$back/ark.tar.gz"'): (
        ("/root/ark",), "the copy fetched back, decrypted"),
    (_ARK, 'rm -rf "$back"'): (
        ("/root/ark",), "the fetched-back directory inside the work directory"),
}


class TestNothingDeletesWhatRule5Keeps:
    def test_the_python_deletions_are_exactly_the_allowed(self):
        """Mutation: delete, rename or replace the DuckDB file, a backup, the
        export, an Ark, a marker or a spooled file from anywhere else."""
        found = deletion_walk.python_deletions()
        assert found == {k: places for k, (places, _) in ALLOWED_PY.items()}, found

    def test_the_shell_deletions_are_exactly_the_allowed(self):
        """Mutation: `rm`, `mv` or `ln -f` over those places in a script."""
        found = deletion_walk.shell_deletions()
        assert found == {k: places for k, (places, _) in ALLOWED_SHELL.items()}, found

    def test_the_walks_read_the_trees_they_say(self):
        """`deploy/` is walked for both languages, and the walk is not empty.
        Mutation: drop `deploy` from either list."""
        assert "deploy" in deletion_walk.PYTHON_TREES
        assert "deploy" in deletion_walk.SHELL_TREES
        modules, deleters = deletion_walk._tree()
        assert any(rel.startswith("deploy/") for rel in modules)
        assert ("func", "core/atomic_file.py", "write_atomic") in deleters

    # One case per shape, each a file the walk did not see before.
    PY_CASES = {
        "from-import, module constant, loop variable": (
            "core/mut_b.py",
            "from os import remove\n"
            "from core.paths import DATA_DIR\n"
            "NIGHTLY = DATA_DIR / 'backups'\n"
            "def prune(keep):\n"
            "    for old in sorted(NIGHTLY.glob('*'))[:-keep]:\n"
            "        remove(old)\n",
            ("core/mut_b.py", "prune"), {"backups"}),
        "an alias of shutil, a constant behind a parameter's `or`": (
            "core/disk_monitor.py", None,  # appended to the real module
            ("core/disk_monitor.py", "_mut_wipe"), {"export_parquet"}),
        "deploy/, a path built with +": (
            "deploy/mut_c.py",
            "import os\n"
            "def swap(path):\n"
            "    os.rename(path + '/analytics.duckdb', path + '/analytics.old')\n",
            ("deploy/mut_c.py", "swap"), {"analytics", ".duckdb"}),
        "a helper of another module naming the marker": (
            "core/mut_e.py",
            "from core import chain_latch\n"
            "def forget(chain):\n"
            "    chain_latch.marker_path(chain).unlink()\n",
            ("core/mut_e.py", "forget"), {"MARKER_DIR", "write-chain-owners"}),
        "inline, the control": (
            "core/mut_d.py",
            "import shutil\n"
            "from core.paths import DATA_DIR\n"
            "def drop():\n"
            "    shutil.rmtree(DATA_DIR / 'export_parquet')\n",
            ("core/mut_d.py", "drop"), {"export_parquet"}),
        "a wrapper, seen at its caller": (
            "core/mut_f.py",
            "from core.paths import DATA_DIR, DUCKDB_FILE_NAME\n"
            "def _rm(p):\n"
            "    p.unlink()\n"
            "def wipe():\n"
            "    _rm(DATA_DIR / DUCKDB_FILE_NAME)\n",
            ("core/mut_f.py", "wipe"), {"DUCKDB_FILE_NAME", "analytics", ".duckdb"}),
        "self, as the class assigns it": (
            "core/mut_g.py",
            "from core.duckdb_constants import DB_PATH\n"
            "class Holder:\n"
            "    def __init__(self):\n"
            "        self._file = DB_PATH\n"
            "    def drop(self):\n"
            "        self._file.unlink()\n",
            ("core/mut_g.py", "Holder.drop"), {"DB_PATH", "analytics", ".duckdb"}),
        "a subprocess handed rm": (
            "scripts/mut_h.py",
            "import subprocess\n"
            "from core.paths import DATA_DIR\n"
            "BACKUPS = DATA_DIR / 'backups'\n"
            "def purge():\n"
            "    subprocess.run(['rm', '-rf', str(BACKUPS)], check=True)\n",
            ("scripts/mut_h.py", "purge"), {"backups"}),
        "a rename onto the file": (
            "core/mut_i.py",
            "from core.paths import DATA_DIR, DUCKDB_FILE_NAME\n"
            "def put(tmp):\n"
            "    tmp.replace(DATA_DIR / DUCKDB_FILE_NAME)\n",
            ("core/mut_i.py", "put"), {"DUCKDB_FILE_NAME", "analytics", ".duckdb"}),
        "the spool, through the private helper of its module": (
            "web/mut_j.py",
            "from core.report_ledger import _spool_dir\n"
            "def clear():\n"
            "    for p in _spool_dir('weekly').glob('*.json'):\n"
            "        p.unlink()\n",
            ("web/mut_j.py", "clear"), {"SPOOL_DIR", "report-ledger-pending"}),
        "module-level code": (
            "scripts/mut_k.py",
            "import os\n"
            "from core.paths import DATA_DIR\n"
            "os.remove(DATA_DIR / 'analytics.duckdb.wal')\n",
            ("scripts/mut_k.py", "<module>"), {"analytics", ".duckdb"}),
    }
    _APPENDED = ("\n\nimport shutil as _mut_shutil\n"
                 "from core.paths import DATA_DIR as _MUT_DATA\n"
                 "_MUT_EXPORT = _MUT_DATA / 'export_parquet'\n"
                 "def _mut_wipe(target=None):\n"
                 "    _mut_shutil.rmtree(target or _MUT_EXPORT)\n")

    @pytest.mark.parametrize("shape", sorted(PY_CASES))
    def test_the_python_walk_sees_each_shape(self, shape):
        rel, source, unit, places = self.PY_CASES[shape]
        if source is None:
            source = (REPO / rel).read_text(encoding="utf-8") + self._APPENDED
        found = deletion_walk.python_deletions({rel: source})
        assert unit in found, (shape, found)
        assert places <= set(found[unit]), (shape, found[unit])

    def test_the_python_walk_is_not_fooled_by_names_alike(self):
        """`list.remove`, `str.replace`, `datetime.replace` and an alert
        whose evidence names the backups are not deletions there — or the
        guard would be an allowlist of noise."""
        source = ("from core.paths import DATA_DIR\n"
                  "def f(items, when):\n"
                  "    items.remove('analytics.duckdb')\n"
                  "    name = 'analytics.duckdb'.replace('a', 'b')\n"
                  "    return when.replace(tzinfo=None), name\n"
                  "def g(bucket):\n"
                  "    from core.atomic_file import write_atomic\n"
                  "    write_atomic(DATA_DIR / 'alert-tasks' / f'{bucket}.json', '')\n"
                  "def h():\n"
                  "    g('disk:backups:export_parquet:analytics.duckdb')\n")
        assert deletion_walk.python_deletions({"core/mut_neg.py": source}) == {}

    SHELL_CASES = {
        "a variable": ("D=/opt/key-api-bot/data\nrm -f \"$D/analytics.duckdb\"\n",
                       {".duckdb"}),
        "a default, the directory itself": ("mv \"${DATA:-data}/backups\" /tmp/old\n",
                                            {"backups"}),
        "ln -f": ("ln -sf /dev/null data/export_parquet\n", {"export_parquet"}),
        "find -delete": ("ARK=/root/ark/key-api-bot\nfind \"$ARK\" -name '*.gpg' -delete\n",
                         {"/root/ark"}),
        "down a pipe": ("ls -1t data/backups/analytics-*.duckdb | tail -n +3 | xargs -r rm -f\n",
                        {".duckdb"}),
        "a continued line in a subshell": (
            "( cd /opt/key-api-bot && \\\n  rm -rf data/write-chain-owners )\n",
            {"write-chain-owners"}),
    }

    @pytest.mark.parametrize("shape", sorted(SHELL_CASES))
    def test_the_shell_walk_sees_each_shape(self, shape):
        text, places = self.SHELL_CASES[shape]
        found = deletion_walk.shell_deletions({"deploy/mut.sh": text})
        assert found and places <= set().union(*found.values()), (shape, found)

    def test_the_shell_walk_leaves_postgres_s_own_backups(self):
        """`backups/postgres` and `backups/pg_wal` are Postgres's: their
        retention is not rule 5's business."""
        text = ("rm -f \"backups/postgres/_drill-$STAMP.dump\"\n"
                "WAL_DIR=\"$(pwd)/backups/pg_wal\"\n"
                "for seg in \"$WAL_DIR\"/*.gz; do rm -f \"/tmp/x/$(basename \"$seg\")\"; done\n"
                "docker rm -f -v ks-pg\n")
        assert deletion_walk.shell_deletions({"deploy/mut.sh": text}) == {}


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
