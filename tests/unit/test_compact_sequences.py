"""The weekly compaction's sequence restore, run for real.

Every test here drives the real phase1_export, phase2_import and
phase3_validate of scripts/compact_duckdb.py against real DuckDB files in a
temporary directory, and judges the clean file with a raw `duckdb.connect` —
never through DuckDBStore, whose boot migration 0027 would repair a lagging
sequence and hide exactly what these tests exist to see.

What they pin, each measured on DuckDB 1.5.5 before it was written:

* the restore used to burn with an unfetched `SELECT nextval(..) FROM
  range(n)`, which stops at 129,024 values whatever n is, so a table whose
  MAX(id) reached 129,025 went live handing out ids below that MAX;
* the same unfetched burn never reached the WAL, and survived only because the
  CHECKPOINT after it happened to have the imports to write;
* phase 3 checked no sequence at all, so both of those passed validation;
* the source's `last_value` is its *next* value after a clean close (restoring
  to it + 1 skipped an id every week) and the checkpoint counter after a WAL
  replay, where only the rendered `START` carries the true next value;
* MAX(id) stays a floor, because explicit ids never move a sequence and an
  archive written before 2026-09-17 recorded the checkpoint counter.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

import compact_duckdb as compact  # noqa: E402
import core.duckdb_sequences as duckdb_sequences  # noqa: E402
import core.migrations as migrations  # noqa: E402
from core.duckdb_sequences import advance_to, next_value  # noqa: E402
from core.duckdb_store import DuckDBStore  # noqa: E402
from core.migrations import SEQUENCE_ID_COLUMNS  # noqa: E402
from tests.unit.test_duckdb_sequences import _MINIMAL_ROW  # noqa: E402

TABLE_OF = {seq: table for seq, table, _ in SEQUENCE_ID_COLUMNS}
COLUMN_OF = {table: col for _, table, col in SEQUENCE_ID_COLUMNS}

# Rows each table gets below its top id. Enough to put the top id in a dense
# run, few enough that a whole compaction stays well under a second.
BLOCK = 20

# Where production stood on the 2026-09-17 backup, rounded to Sunday: the top
# id of each table and the value its sequence would hand out next. Every
# source next is MAX + 1 except reconciliation_seq, which stood 17 above its
# table, and manual_expenses, empty and never used (last_value NULL).
PRODUCTION_SHAPE = {
    "warehouse_refreshes": (75_453, 75_454),
    "reconciliation_log": (2_597, 2_614),
    "data_quality_runs": (720, 721),
    "stock_movements": (57_090, 57_091),
    "buyer_contacts": (34_460, 34_461),
    "manual_expenses": (0, None),
}
PRODUCTION_NEXT = {
    "warehouse_refreshes": 75_454,
    "reconciliation_log": 2_614,
    "data_quality_runs": 721,
    "stock_movements": 57_091,
    "buyer_contacts": 34_461,
    "manual_expenses": 1,
}

ANSI = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def data(tmp_path, monkeypatch):
    """Point the compaction, and DuckDBStore's own mkdir, at a temp directory."""
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setattr(compact, "DATA_DIR", d)
    monkeypatch.setattr(compact, "SOURCE_DB", d / "analytics.duckdb")
    monkeypatch.setattr(compact, "NEW_DB", d / "analytics_clean.duckdb")
    monkeypatch.setattr(compact, "EXPORT_DIR", d / "export_parquet")
    monkeypatch.setattr(compact, "MANIFEST_PATH", d / "export_parquet" / "_manifest.json")
    monkeypatch.setattr("core.duckdb_store.DB_DIR", d)
    monkeypatch.delenv("COMPACT_AUTO_SWAP", raising=False)
    return d


def _insert(conn, table: str, n: int) -> int:
    """One row with a DEFAULT id; `n` only keeps UNIQUE constraints apart."""
    columns, values = _MINIMAL_ROW[table]
    return conn.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({values.format(n=n)}) "
        f"RETURNING {COLUMN_OF[table]}"
    ).fetchone()[0]


def _fill(conn, shape: dict) -> None:
    """Give each table ids up to its top through its own sequence, then put the
    sequence at the source-next the shape asks for."""
    for seq, table, col in SEQUENCE_ID_COLUMNS:
        top, source_next = shape[table]
        if top:
            first = max(1, top - BLOCK + 1)
            advance_to(conn, seq, first - 1)
            for n in range(first, top + 1):
                assert _insert(conn, table, n) == n
        if source_next is not None:
            advance_to(conn, seq, source_next - 1)


def _build_source(path: Path, shape: dict) -> None:
    async def build():
        store = DuckDBStore(db_path=path)
        await store.connect()
        try:
            async with store.connection() as conn:
                _fill(conn, shape)
        finally:
            await store.close()  # a clean close, as web's shutdown does

    asyncio.run(build())


def _build_explicit_source(path: Path, ids) -> None:
    """Every table holds exactly `ids`, written explicitly, and no sequence has
    ever been used — the state an import or a backfill with its own ids leaves,
    where the source's position says nothing and only MAX(id) does."""
    _build_source(path, {t: (0, None) for t in COLUMN_OF})
    conn = duckdb.connect(str(path))
    try:
        for _, table, col in SEQUENCE_ID_COLUMNS:
            columns, values = _MINIMAL_ROW[table]
            for n in ids:
                conn.execute(
                    f"INSERT INTO {table} ({col}, {columns}) "
                    f"VALUES ({n}, {values.format(n=n)})"
                )
    finally:
        conn.close()


def _advance_short(real, sequence: str):
    """advance_to that leaves `sequence` handing out its floor instead of
    floor + 1 — one id short, the smallest miss there is — and every other
    sequence where the real one puts it."""
    def advance(conn, name, floor, **kwargs):
        return real(conn, name, floor - 1 if name == sequence else floor, **kwargs)
    return advance


def _compact() -> dict:
    manifest = compact.phase1_export()
    compact.phase2_import(manifest)
    compact.phase3_validate(manifest)
    return manifest


def _next_values(path: Path) -> dict:
    """{table: next id} read from the file alone, with nothing to repair it."""
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return {table: next_value(conn, seq) for seq, table, _ in SEQUENCE_ID_COLUMNS}
    finally:
        conn.close()


def _default_inserts(path: Path) -> dict:
    """{table: id a DEFAULT insert gets} — raises on a primary-key collision."""
    conn = duckdb.connect(str(path))
    try:
        got = {table: _insert(conn, table, 9_000_000) for _, table, _ in SEQUENCE_ID_COLUMNS}
        dupes = conn.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT id) FROM reconciliation_log"
        ).fetchone()[0]
        assert dupes == 0, "reconciliation_log has no key; a lagging sequence duplicates silently"
        return got
    finally:
        conn.close()


def _out(capsys) -> str:
    return ANSI.sub("", capsys.readouterr().out)


class TestTheCleanFileHandsOutTheRightIds:
    @pytest.mark.parametrize("top", [129_025, 300_000])
    def test_a_table_past_the_old_cap_is_not_handed_its_own_ids(self, data, top, capsys):
        """(a) The unfetched burn stopped at 129,024: MAX 129,025 collided on
        five tables and duplicated in reconciliation_log, and MAX 300,000 landed
        silently in a hole below MAX. Phase 3 passed both."""
        _build_source(compact.SOURCE_DB, {t: (top, top + 1) for t in COLUMN_OF})

        _compact()

        expected = {t: top + 1 for t in COLUMN_OF}
        assert _next_values(compact.NEW_DB) == expected
        assert _default_inserts(compact.NEW_DB) == expected
        assert "ALL VALIDATIONS PASSED" in _out(capsys)

    def test_production_shape_hands_out_exactly_the_sources_next_value(self, data, capsys):
        """(b) No skipped id, and a sequence standing above its table keeps its
        position: reconciliation_seq hands out 2,614, not MAX + 1 = 2,598."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)

        _compact()

        assert _next_values(compact.NEW_DB) == PRODUCTION_NEXT
        assert _default_inserts(compact.NEW_DB) == PRODUCTION_NEXT

    def test_each_restore_line_reports_the_position_read_back(self, data, capsys):
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)

        _compact()

        out = _out(capsys)
        assert ("sequence warehouse_refresh_seq: next 75,454 (warehouse_refreshes.id "
                "MAX 75,453, source next 75,454, burned 75,453)") in out
        assert ("sequence reconciliation_seq: next 2,614 (reconciliation_log.id "
                "MAX 2,597, source next 2,614, burned 2,613)") in out
        # Never used: last_value is NULL, and the rendered START still says 1.
        assert ("sequence seq_manual_expenses_id: next 1 (manual_expenses.id MAX 0, "
                "source next 1, burned 0)") in out
        assert "sequence reconciliation_seq: next 2,614 > reconciliation_log.id MAX 2,597" in out

    def test_a_source_replayed_from_its_wal_hands_out_its_true_next_value(
        self, data, tmp_path, capsys
    ):
        """web killed without a checkpoint: phase 1's read-only open replays the
        WAL, and last_value is the counter as of the last checkpoint. Here the
        WAL burned past the rows it added, so MAX(id) is no floor either: the
        ids between MAX and the true next value were handed out, and recording
        last_value alone gave a file that hands them out again (250,004)."""
        live = tmp_path / "live.duckdb"
        _build_source(live, {t: (50_000, 50_001) for t in COLUMN_OF})  # closes clean
        conn = duckdb.connect(str(live))
        conn.execute("PRAGMA disable_checkpoint_on_shutdown")
        for seq, table, _ in SEQUENCE_ID_COLUMNS:
            advance_to(conn, seq, 250_000)  # inserts that failed, or rolled back
            for n in range(250_001, 250_004):
                assert _insert(conn, table, n) == n
            advance_to(conn, seq, 300_003)
        # A crash copy: the file and its WAL, taken while the writer holds them.
        shutil.copy(live, compact.SOURCE_DB)
        shutil.copy(f"{live}.wal", f"{compact.SOURCE_DB}.wal")
        conn.close()

        manifest = _compact()

        # The engine facts this depends on, so a change in either is seen here.
        assert {s: manifest["seq_readings"][s] for s in TABLE_OF} == {
            s: {"last_value": 50_001, "rendered_start": 300_004} for s in TABLE_OF
        }
        expected = {t: 300_004 for t in COLUMN_OF}
        assert _next_values(compact.NEW_DB) == expected
        assert _default_inserts(compact.NEW_DB) == expected
        assert {s: manifest["seq_values"][s] for s in TABLE_OF} == {s: 300_004 for s in TABLE_OF}
        out = _out(capsys)
        assert ("warehouse_refresh_seq: next 300,004 (stored counter 50,001, as of the "
                "last checkpoint; the WAL replay moved it on)") in out
        assert ("sequence warehouse_refresh_seq: next 300,004 > warehouse_refreshes.id "
                "MAX 250,003 (source next 300,004)") in out

    @pytest.mark.parametrize("rendering", [None, 1], ids=["unreadable", "reads-low"])
    def test_a_rendering_that_fails_costs_nothing_on_a_clean_source(
        self, data, monkeypatch, capsys, rendering
    ):
        """The rendered START is not a documented interface. If it stops being
        readable, or reads low, the stored counter is recorded — never the
        lower reading — and a cleanly closed source still restores exactly."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        monkeypatch.setattr(compact, "_rendered_start", lambda sql: rendering)

        manifest = _compact()

        assert {TABLE_OF[s]: manifest["seq_values"][s] for s in TABLE_OF} == PRODUCTION_NEXT
        assert _next_values(compact.NEW_DB) == PRODUCTION_NEXT
        assert re.search(r"WARN +warehouse_refresh_seq: next 75,454 from the stored "
                         r"counter", _out(capsys))

    def test_explicit_ids_on_never_used_sequences_are_floored_on_max_id(self, data, capsys):
        """No source position to go on — every sequence still says 1 — so the
        MAX(id) floor alone decides, past the old burn's cap."""
        _build_explicit_source(compact.SOURCE_DB, (1, 64_000, 129_025))

        manifest = _compact()

        assert {s: manifest["seq_readings"][s]["last_value"] for s in TABLE_OF} == {
            s: None for s in TABLE_OF
        }
        expected = {t: 129_026 for t in COLUMN_OF}
        assert _next_values(compact.NEW_DB) == expected
        assert _default_inserts(compact.NEW_DB) == expected
        assert "ALL VALIDATIONS PASSED" in _out(capsys)


class TestTheRestoreIsDurable:
    def test_positions_survive_when_the_final_checkpoint_has_nothing_else_to_write(
        self, data, monkeypatch, capsys
    ):
        """(c) The unfetched burn reached disk only because the CHECKPOINT after
        it still had the schema and the imports to write. Checkpoint those away
        first and every sequence reopened at 1, with phase 3 passing."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        real = compact.restore_sequences
        wal_before_restore = []

        def checkpoint_first(conn, *args, **kwargs):
            conn.execute("CHECKPOINT")
            wal = Path(f"{compact.NEW_DB}.wal")
            wal_before_restore.append(wal.stat().st_size if wal.exists() else 0)
            return real(conn, *args, **kwargs)

        monkeypatch.setattr(compact, "restore_sequences", checkpoint_first)

        _compact()

        assert wal_before_restore == [0], "the injected checkpoint left work behind"
        assert _next_values(compact.NEW_DB) == PRODUCTION_NEXT
        assert _default_inserts(compact.NEW_DB) == PRODUCTION_NEXT


class TestPhase3RefusesAFileThatWouldCollide:
    def test_a_sequence_left_behind_fails_validation_by_name(self, data, monkeypatch, capsys):
        """(d) advance_to made a no-op: phase 2 completes and logs where each
        sequence really stands, phase 3 reads the closed file and refuses."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        monkeypatch.setattr(duckdb_sequences, "advance_to", lambda *a, **k: 0)
        manifest = compact.phase1_export()
        compact.phase2_import(manifest)
        assert "sequence warehouse_refresh_seq: next 1 (" in _out(capsys)

        with pytest.raises(SystemExit) as exit_:
            compact.phase3_validate(manifest)

        assert exit_.value.code == 1
        out = _out(capsys)
        assert ("sequence warehouse_refresh_seq: hands out 1 but "
                "warehouse_refreshes.id MAX is 75,453") in out
        assert "sequence data_quality_run_seq: hands out 1 but data_quality_runs.run_id MAX is 720" in out
        assert "ALL VALIDATIONS PASSED" not in out
        fail_line = [line for line in out.splitlines() if "ERROR  FAIL: " in line]
        assert len(fail_line) == 1 and "warehouse_refresh_seq" in fail_line[0]
        assert out.rstrip().splitlines()[-1] == fail_line[0]

    def test_a_sequence_that_hands_out_exactly_max_fails(self, data, monkeypatch, capsys):
        """The boundary, and the SEQ-1 shape itself: next == MAX. The source's
        position cannot catch it — never used, it says 1 — so this is the only
        check between that file and a primary-key error on the first insert."""
        _build_explicit_source(compact.SOURCE_DB, (1, 64_000, 129_025))
        monkeypatch.setattr(duckdb_sequences, "advance_to",
                            _advance_short(advance_to, "warehouse_refresh_seq"))
        manifest = compact.phase1_export()
        compact.phase2_import(manifest)
        assert "sequence warehouse_refresh_seq: next 129,025 (" in _out(capsys)

        with pytest.raises(SystemExit) as exit_:
            compact.phase3_validate(manifest)

        assert exit_.value.code == 1
        out = _out(capsys)
        assert out.rstrip().splitlines()[-1].endswith(
            "ERROR  FAIL: sequence warehouse_refresh_seq: hands out 129,025 but "
            "warehouse_refreshes.id MAX is 129,025")

    def test_a_sequence_one_below_the_sources_next_value_fails(
        self, data, monkeypatch, capsys
    ):
        """The other boundary. reconciliation_seq stands 17 above its table, so
        MAX(id) cannot catch it: ids between the two were handed out by the
        source, and the tables that ship to Postgres by `id > MAX` may already
        hold them there."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        monkeypatch.setattr(duckdb_sequences, "advance_to",
                            _advance_short(advance_to, "reconciliation_seq"))
        manifest = compact.phase1_export()
        compact.phase2_import(manifest)

        with pytest.raises(SystemExit) as exit_:
            compact.phase3_validate(manifest)

        assert exit_.value.code == 1
        out = _out(capsys)
        assert out.rstrip().splitlines()[-1].endswith(
            "ERROR  FAIL: sequence reconciliation_seq: hands out 2,613, below the "
            "source's next value 2,614")

    def test_the_swap_does_not_happen(self, data, monkeypatch, capsys):
        """Through main() with auto-swap on, as the sidecar runs it."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        before = hashlib.sha256(compact.SOURCE_DB.read_bytes()).hexdigest()
        monkeypatch.setattr(duckdb_sequences, "advance_to", lambda *a, **k: 0)
        monkeypatch.setenv("COMPACT_AUTO_SWAP", "1")
        swaps = []
        monkeypatch.setattr(compact, "atomic_swap", lambda d: swaps.append(d))

        with pytest.raises(SystemExit) as exit_:
            compact.main()

        assert exit_.value.code == 1
        # Why it exited, not only that it did: a disk-space abort exits 1 too.
        assert "ERROR  FAIL: sequence warehouse_refresh_seq: hands out 1 but" in _out(capsys)
        assert swaps == []
        assert hashlib.sha256(compact.SOURCE_DB.read_bytes()).hexdigest() == before
        assert not (data / "analytics.duckdb.old").exists()

    def test_a_wal_left_beside_the_clean_file_fails(self, data, monkeypatch, capsys):
        """atomic_swap moves the database file alone; a WAL left beside it is
        deleted by the wrapper, and whatever it held never goes live."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        manifest = compact.phase1_export()
        compact.phase2_import(manifest)

        real_connect = duckdb.connect

        class NoCheckpoint:
            def __init__(self, conn):
                self._conn = conn

            def execute(self, sql, *args):
                if sql.strip().upper() == "CHECKPOINT":
                    return None
                return self._conn.execute(sql, *args)

            def close(self):
                self._conn.close()

        def leaky(database, read_only=False, **kwargs):
            conn = real_connect(database, read_only=read_only, **kwargs)
            if read_only:
                return conn
            conn.execute("PRAGMA disable_checkpoint_on_shutdown")
            return NoCheckpoint(conn)

        monkeypatch.setattr(duckdb, "connect", leaky)

        with pytest.raises(SystemExit):
            compact.phase3_validate(manifest)

        assert re.search(r"clean DB left a WAL after close \([\d,]+ bytes\)", _out(capsys))


class TestTheMapAndTheSchemaAgree:
    def test_an_entry_whose_table_is_gone_is_skipped_with_a_warning(
        self, data, monkeypatch, capsys
    ):
        """(e) The DuckDB exit drops tables one write chain at a time. An entry
        left behind must cost a warning, not every Sunday's compaction."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        monkeypatch.setattr(
            migrations, "SEQUENCE_ID_COLUMNS",
            SEQUENCE_ID_COLUMNS + (("seq_gone_id", "gone_table", "id"),),
        )

        _compact()

        out = _out(capsys)
        assert re.search(r"WARN +sequence seq_gone_id: gone_table is not in this "
                         r"version's schema — skipped", out)
        assert re.search(r"WARN +sequence seq_gone_id: gone_table is not in this "
                         r"version's schema — not checked", out)
        assert "ALL VALIDATIONS PASSED" in out
        assert _next_values(compact.NEW_DB) == PRODUCTION_NEXT

    def test_sequences_outside_the_map_warn_and_keep_the_sources_position(
        self, data, monkeypatch, capsys
    ):
        """One the schema still declares keeps where the source left it; one
        only the source holds is reported once. Neither fails the run."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        conn = duckdb.connect(str(compact.SOURCE_DB))
        conn.execute("CREATE SEQUENCE seq_retired_id START 1")
        conn.execute("SELECT max(nextval('seq_retired_id')) FROM range(40)").fetchall()
        conn.close()
        monkeypatch.setattr(
            migrations, "SEQUENCE_ID_COLUMNS",
            tuple(e for e in SEQUENCE_ID_COLUMNS if e[1] != "buyer_contacts"),
        )

        _compact()

        out = _out(capsys)
        assert re.search(r"WARN +sequence seq_buyer_contacts_id: not in "
                         r"SEQUENCE_ID_COLUMNS — next 34,461 from the source's "
                         r"position alone", out)
        assert re.search(r"WARN +sequence seq_buyer_contacts_id: not in "
                         r"SEQUENCE_ID_COLUMNS — nothing checks it", out)
        assert re.search(r"WARN +sequence seq_retired_id: in the source only", out)
        assert out.count("sequence seq_retired_id") == 1
        assert "ALL VALIDATIONS PASSED" in out
        assert _next_values(compact.NEW_DB)["buyer_contacts"] == 34_461

    @pytest.mark.asyncio
    async def test_every_sequence_default_in_the_schema_is_in_the_map(self, tmp_path, monkeypatch):
        """(f) Walks the schema DuckDBStore builds, not a list. A table added
        with an id sequence and no entry fails here, in CI, instead of going
        unrestored and unchecked on a Sunday; an entry whose table has gone
        fails here too, instead of warning every week."""
        monkeypatch.setattr("core.duckdb_store.DB_DIR", tmp_path)
        store = DuckDBStore(db_path=tmp_path / "schema.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                defaults = conn.execute(
                    "SELECT table_name, column_name, column_default FROM duckdb_columns() "
                    "WHERE column_default ILIKE '%nextval(%'"
                ).fetchall()
                declared = {r[0] for r in conn.execute(
                    "SELECT sequence_name FROM duckdb_sequences()"
                ).fetchall()}
        finally:
            await store.close()

        walked = set()
        for table, column, default in defaults:
            match = re.fullmatch(r"nextval\('([A-Za-z0-9_]+)'\)", default.strip())
            assert match, f"{table}.{column} has a DEFAULT this walk cannot read: {default}"
            walked.add((match.group(1), table, column))

        assert len(SEQUENCE_ID_COLUMNS) == len(set(SEQUENCE_ID_COLUMNS))
        assert walked == set(SEQUENCE_ID_COLUMNS)
        # Every sequence numbers some column, so phase 3 has nothing to warn about.
        assert declared == {seq for seq, _, _ in walked}


class TestColumnCoverage:
    def test_a_clean_run_reports_no_dropped_columns(self, data, capsys):
        """parquet_schema() lists the schema's root, `duckdb_schema`, and it was
        reported as a dropped column on every table, every Sunday."""
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)

        _compact()

        out = _out(capsys)
        assert "duckdb_schema" not in out
        assert "source columns not in target" not in out

    def test_a_column_the_target_lacks_is_still_reported(self, data, capsys):
        _build_source(compact.SOURCE_DB, PRODUCTION_SHAPE)
        conn = duckdb.connect(str(compact.SOURCE_DB))
        conn.execute("ALTER TABLE warehouse_refreshes ADD COLUMN legacy_note VARCHAR")
        conn.execute("UPDATE warehouse_refreshes SET legacy_note = 'x'")
        conn.close()
        manifest = compact.phase1_export()

        with pytest.raises(SystemExit):
            compact.phase2_import(manifest)

        out = _out(capsys)
        assert "warehouse_refreshes: source columns not in target: ['legacy_note']" in out
        assert "this table's import will fail" in out
        assert "duckdb_schema" not in out
        # The line weekly_compact.sh quotes, last and whole. DuckDB's message
        # runs on over two more lines ("Did you mean ..."); unflattened, the
        # last line of the run would be that hint, naming nothing.
        last = out.rstrip().splitlines()[-1]
        assert re.search(r'ERROR +FAIL: warehouse_refreshes: Binder Error: Table '
                         r'"warehouse_refreshes" does not have a column with name '
                         r'"legacy_note"', last), out


class TestTheScriptRunsAsTheSidecarRunsIt:
    def test_by_path_from_another_directory(self, tmp_path):
        """(g) `python /app/scripts/compact_duckdb.py` puts /app/scripts, not
        the working directory and not the app root, on sys.path. main()
        resolves the application imports before anything else, so reaching
        preflight proves they resolved."""
        if Path("/app/data/analytics.duckdb").exists():
            pytest.skip("a database sits where the script looks; not running it")
        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "COMPACT_AUTO_SWAP")}
        done = subprocess.run(
            [sys.executable, str(REPO / "scripts" / "compact_duckdb.py")],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
        )
        out = ANSI.sub("", done.stdout + done.stderr)

        assert "ModuleNotFoundError" not in out, out
        assert f"Application code: {REPO}" in out, out
        assert "Source DB not found: /app/data/analytics.duckdb" in out, out
        assert done.returncode == 1
