"""Step 14's S14-01: the warehouse rule and three helpers, in engine-free modules.

Silver's projection and Gold's measures lived in `core/duckdb_store.py`, so
every Postgres derivation tick imported the DuckDB store lazily — and with it
the `duckdb` driver and pandas — to render a string (`pg_silver.pass1_sql`
was the first line of the tick to do it). S14-01 moved them, with
`UpsertResult`, the data directory and the atomic file writer, to modules that
name no DuckDB module:

    silver_* and GOLD_MEASURES       → core/warehouse_sql.py
    UpsertResult                     → core/pg_orders_write.py
    duckdb_constants.DB_DIR          → core/paths.py::DATA_DIR
    chain_latch._write_marker/_fsync_dir → core/atomic_file.py

Three things are held here:

* **every rendering is the bytes the code before the move produced** — a
  changed one would change the stored Silver or Gold at the next derivation.
  The literals are `tests/warehouse_sql_57fd7c22.py`, captured from 57fd7c22
  by running that module as a script against it;
* **every old name is the same object as the new one**, for the one release
  the re-exports live;
* **the derivation path imports no DuckDB module**: none of the modules S14-01
  repointed names one, and a fresh interpreter that imports them and renders
  every statement they own has neither `duckdb` nor `core.duckdb_store` nor
  pandas loaded. `tests/integration/test_derivation_loads_no_duckdb.py` holds
  the same of a whole derivation tick against a real Postgres.
"""
from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests.warehouse_sql_57fd7c22 import RENDERINGS, render

ROOT = Path(__file__).resolve().parents[2]


# ─── The bytes ───────────────────────────────────────────────────────────────

class TestEveryRenderingIsTheBytesFrom57fd7c22:
    def test_the_literal_covers_what_the_plan_names(self):
        """Pass 1, pass 2 with and without the buyer filter, the recompute,
        and Gold for Postgres and ClickHouse — a literal that lost a key would
        pass the parametrised test below by having nothing to compare."""
        assert set(RENDERINGS) == {
            "silver_sales_type_case(POSTGRES)",
            "silver_select_sql(POSTGRES)",
            "pg_silver.pass1_sql()",
            "pg_silver.pass2_sql()",
            "silver_pass2_sql(dialect=POSTGRES)",
            'silver_pass2_sql("?,?,?", dialect=POSTGRES)',
            "silver_recompute_ctes(POSTGRES)",
            "pg_warehouse_dq._row_values_sql()",
            "pg_gold.select_sql()",
            "pg_gold.insert_sql()",
            "ch_silver.derive_gold_sql()[0]",
            "ch_silver.derive_gold_sql()[1]",
            "GOLD_MEASURES",
        }
        assert set(render()) == set(RENDERINGS)

    @pytest.mark.parametrize("name", sorted(RENDERINGS))
    def test_the_rendering_is_unchanged(self, name):
        """Mutation: any one character in a moved body — a space in the
        sales-type CASE, the measures' order — fails the keys that carry it."""
        assert render()[name] == RENDERINGS[name], name

    def test_the_renderings_come_from_the_new_home(self):
        """`render()` takes the shared names from `core.warehouse_sql` when it
        exists; this tree must be one where it does, or the comparison above
        would be with the re-export rather than the definition."""
        import core.warehouse_sql as home

        assert render()["GOLD_MEASURES"] == tuple(home.GOLD_MEASURES.items())
        assert home.silver_select_sql.__module__ == "core.warehouse_sql"

    def test_the_defaults_are_still_duckdb(self):
        """`dialect=None` → DUCKDB until S14-07 removes that dialect."""
        from core.sql_dialect import DUCKDB
        from core.warehouse_sql import (silver_pass2_sql, silver_recompute_ctes,
                                        silver_sales_type_case, silver_select_sql)

        assert silver_sales_type_case() == silver_sales_type_case(DUCKDB)
        assert silver_select_sql() == silver_select_sql(DUCKDB)
        assert silver_pass2_sql() == silver_pass2_sql(dialect=DUCKDB)
        assert silver_recompute_ctes() == silver_recompute_ctes(DUCKDB)


# ─── The re-exports ──────────────────────────────────────────────────────────

SHARED_SQL = (
    "silver_sales_type_case", "silver_select_sql", "silver_pass2_sql",
    "silver_recompute_ctes", "GOLD_MEASURES", "_GOLD_EARNING_ROWS",
    "_GOLD_RETURNED_ROWS", "_GOLD_REVENUE_EXPR", "_GOLD_ORDERS_EXPR",
)


class TestTheOldNamesAreTheNewObjects:
    @pytest.mark.parametrize("name", SHARED_SQL)
    def test_duckdb_store_re_exports_the_warehouse_sql(self, name):
        """Mutation: a copy left behind in `core/duckdb_store.py` — the two
        would be two homes of one rule, which is what #101 proved diverges."""
        from core import duckdb_store, warehouse_sql

        assert getattr(duckdb_store, name) is getattr(warehouse_sql, name)

    def test_upsert_result(self):
        from core import duckdb_store, pg_orders_write

        assert duckdb_store.UpsertResult is pg_orders_write.UpsertResult
        assert pg_orders_write.UpsertResult.__module__ == "core.pg_orders_write"

    def test_the_data_directory(self):
        from core import duckdb_constants, paths

        assert duckdb_constants.DB_DIR is paths.DATA_DIR
        assert paths.DATA_DIR == ROOT / "data"
        assert duckdb_constants.DB_PATH == paths.DATA_DIR / "analytics.duckdb"

    def test_the_atomic_writer(self):
        from core import atomic_file, chain_latch

        assert chain_latch._write_marker is atomic_file.write_atomic
        assert chain_latch._fsync_dir is atomic_file.fsync_dir

    def test_the_files_beside_the_database_did_not_move(self):
        """The paths read from `DATA_DIR` are the paths production holds:
        `data/` is `/app/data` in both images, and an image from before step 14
        reads the same files. Read out of the source, because the suite's own
        fixtures redirect these attributes into a tmp_path. The disk
        watchdog's heartbeat is read by host cron
        (`deploy/offsite_check.sh`), so its path is as fixed as the rest."""
        sources = {
            "core/chain_latch.py": ('MARKER_DIR: Path = DATA_DIR / "write-chain-owners"',),
            "core/report_ledger.py": ('SPOOL_DIR: Path = DATA_DIR / "report-ledger-pending"',),
            "core/warehouse_cutover.py":
                ('UTM_MARKER: Path = DATA_DIR / "warehouse-utm-reparsed-at"',),
            "core/backup_evidence.py": ("return base if base is not None else DATA_DIR",),
            "core/scheduler.py": ("from core.paths import DATA_DIR",
                                  "str(DATA_DIR),",
                                  'health_dir = DATA_DIR / "health"',
                                  '(health_dir / "watchdog_last_sample")'),
        }
        for rel, lines in sources.items():
            text = (ROOT / rel).read_text(encoding="utf-8")
            for line in lines:
                assert line in text, (rel, line)

    def test_the_writer_is_whole_or_absent(self, tmp_path):
        """What it was in `chain_latch`: the file appears complete, no temp
        file is left beside it, and a payload the write cannot finish leaves
        nothing at all."""
        from core.atomic_file import write_atomic

        target = tmp_path / "deep" / "marker"
        write_atomic(target, json.dumps({"a": 1}))
        assert json.loads(target.read_text(encoding="utf-8")) == {"a": 1}
        assert [p.name for p in target.parent.iterdir()] == ["marker"]

        with pytest.raises(TypeError):
            write_atomic(tmp_path / "other", object())  # not a str: write raises
        assert [p.name for p in tmp_path.iterdir()] == ["deep"]

    def test_the_writer_pushes_the_file_then_the_rename_to_the_disk(
            self, tmp_path, monkeypatch):
        """The property the module exists for: a rename that never reached the
        disk unlatches a chain on the next power cut. The file's bytes are
        fsynced before the rename, and the directory holding the new entry
        after it, once each. Recorded by inode, as
        `test_chain_latch::test_the_removal_is_as_durable_as_the_creation`
        does for the release: the temp file keeps its inode across the rename,
        so the file is named the same before and after it. Mutations: the
        closing `fsync_dir(path.parent)` deleted, or moved above `os.replace`;
        the file's own `os.fsync` deleted."""
        import os

        from core.atomic_file import write_atomic

        events: list = []
        real_fsync, real_replace = os.fsync, os.replace

        def fsync(fd):
            events.append(("fsync", os.fstat(fd).st_ino))
            return real_fsync(fd)

        def replace(src, dst):
            events.append(("replace", os.stat(src).st_ino))
            return real_replace(src, dst)

        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(os, "replace", replace)
        target = tmp_path / "deep" / "marker"
        write_atomic(target, json.dumps({"a": 1}))
        monkeypatch.undo()

        file_ino = target.stat().st_ino
        assert events == [("fsync", file_ino), ("replace", file_ino),
                          ("fsync", target.parent.stat().st_ino)]
        assert file_ino != target.parent.stat().st_ino


# ─── No DuckDB module on the derivation path ─────────────────────────────────

# The modules S14-01 repointed, and the three it created. None of them may
# name a DuckDB module, at any depth of a function: the lazy import inside
# `pg_silver.pass1_sql` is exactly what put the driver on every tick.
NEUTRAL = (
    "core/warehouse_sql.py", "core/paths.py", "core/atomic_file.py",
    "core/pg_silver.py", "core/pg_gold.py", "core/ch_silver.py",
    "core/pg_orders_write.py", "core/pg_warehouse_dq.py",
    "core/chain_latch.py", "core/report_ledger.py", "core/backup_evidence.py",
    "core/warehouse_cutover.py",
)

FORBIDDEN = ("duckdb", "core.duckdb_store")


def _named_modules(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            yield node.module
            if node.module == "core":
                for alias in node.names:
                    yield f"core.{alias.name}"
        elif (isinstance(node, ast.Call) and node.args
              and isinstance(node.args[0], ast.Constant)
              and isinstance(node.args[0].value, str)
              and ((isinstance(node.func, ast.Attribute)
                    and node.func.attr == "import_module")
                   or (isinstance(node.func, ast.Name)
                       and node.func.id in ("import_module", "__import__")))):
            yield node.args[0].value


def _forbidden(name: str) -> bool:
    return any(name == f or name.startswith(f + ".") for f in FORBIDDEN)


class TestNoDuckDBModuleOnTheDerivationPath:
    def test_the_walk_sees_every_spelling(self):
        """The walk itself, on the spellings a lazy import takes."""
        for source in ("import duckdb", "from core.duckdb_store import x",
                       "from core import duckdb_store",
                       "def f():\n    from core.duckdb_store import silver_select_sql",
                       "importlib.import_module('core.duckdb_store')",
                       "__import__('duckdb')"):
            assert any(_forbidden(m) for m in _named_modules(ast.parse(source))), source
        assert not any(_forbidden(m) for m in _named_modules(ast.parse(
            "from core import duckdb_switch, duckdb_constants")))

    @pytest.mark.parametrize("rel", NEUTRAL)
    def test_the_module_names_no_duckdb_module(self, rel):
        tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"), filename=rel)
        found = sorted({m for m in _named_modules(tree) if _forbidden(m)})
        assert found == [], f"{rel} imports {found}"

    def test_a_fresh_interpreter_renders_everything_without_duckdb(self):
        """Import the five modules the plan names, render every statement they
        own, take the order writer's no-row path, and look. Then import the
        DuckDB store in the same interpreter, so a check that could not see
        the driver cannot pass. Fails on S14-00's tree: `pass1_sql` loads it."""
        program = r"""
import asyncio, json, sys
sys.path.insert(0, sys.argv[1])
import core.pg_silver, core.pg_gold, core.ch_silver, core.pg_orders_write, core.pg_warehouse_dq
from core.sql_dialect import POSTGRES
from core import warehouse_sql
core.pg_silver.pass1_sql(); core.pg_silver.pass2_sql()
warehouse_sql.silver_pass2_sql("?", dialect=POSTGRES)
core.pg_gold.select_sql(); core.pg_gold.insert_sql()
core.ch_silver.derive_gold_sql()
core.pg_warehouse_dq._row_values_sql()
result, expenses = asyncio.run(core.pg_orders_write.upsert_orders_with_expenses([]))
assert (result.count, result.changed_ids, expenses) == (0, [], 0), result
watched = ("duckdb", "core.duckdb_store", "pandas")
before = sorted(m for m in watched if m in sys.modules)
import core.duckdb_store
after = sorted(m for m in watched if m in sys.modules)
print(json.dumps({"before": before, "after": after}))
"""
        done = subprocess.run(
            [sys.executable, "-I", "-c", program, str(ROOT)],
            capture_output=True, text=True, timeout=120, cwd=str(ROOT))
        assert done.returncode == 0, done.stderr[-2000:]
        seen = json.loads(done.stdout.strip().splitlines()[-1])
        assert seen["before"] == [], seen
        assert seen["after"] == ["core.duckdb_store", "duckdb", "pandas"], seen
