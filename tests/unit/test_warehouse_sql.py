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
* **the old home holds none of them**: `core.duckdb_store` re-exported them
  until S14-18 and was a shim of the store until the final stage deleted it;
* **the derivation path imports no DuckDB module**: none of the modules S14-01
  repointed names one, and a fresh interpreter that imports them and renders
  every statement they own has neither `duckdb` nor the store nor pandas
  loaded. `tests/integration/test_derivation_loads_no_duckdb.py` holds
  the same of a whole derivation tick against a real Postgres.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.warehouse_sql_57fd7c22 import RENDERINGS, render

ROOT = Path(__file__).resolve().parents[2]


# ─── The bytes ───────────────────────────────────────────────────────────────

class TestEveryRenderingIsTheBytesFrom57fd7c22:
    def test_the_literal_covers_what_the_plan_names(self):
        """Silver's rebuild, the recompute, and Gold for Postgres and
        ClickHouse — a literal that lost a key would pass the parametrised
        test below by having nothing to compare. (Pass 1 and pass 2, with and
        without the buyer filter, until the review's A23 made the rebuild one
        INSERT, re-recorded under the owner's decision O1 (a).)"""
        assert set(RENDERINGS) == {
            "silver_sales_type_case(POSTGRES)",
            "silver_select_sql(POSTGRES)",
            "pg_silver.insert_sql()",
            "silver_recompute_ctes(POSTGRES)",
            "pg_warehouse_dq._row_values_sql()",
            "pg_gold.select_sql()",
            "pg_gold.insert_sql()",
            "ch_silver.derive_gold_sql()[0]",
            "ch_silver.derive_gold_sql()[1]",
            "GOLD_MEASURES",
        }
        assert set(render()) == set(RENDERINGS)


# ─── The re-exports ──────────────────────────────────────────────────────────


class TestTheOldNamesAreTheNewObjects:

    def test_the_data_directory(self):
        from core import duckdb_constants, paths

        assert duckdb_constants.DB_DIR is paths.DATA_DIR
        assert paths.DATA_DIR == ROOT / "data"
        assert duckdb_constants.DB_PATH == paths.DATA_DIR / "analytics.duckdb"

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

# `core/warehouse_cutover.py` was one too, and the UTM re-parse marker's path
# (`data/warehouse-utm-reparsed-at`) among the files above, until step 14's
# S14-15 deleted the module: nothing writes or reads the marker in this build,
# and the file stays on disk for an image from before step 14.
