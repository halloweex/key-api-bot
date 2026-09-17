"""Advancing a DuckDB sequence past the ids its table already holds.

Migration 0027 used to DROP and re-CREATE these sequences. On DuckDB 1.5.5 a
sequence that backs a column DEFAULT cannot be dropped, the error went to
DEBUG, and the reset never happened. These tests run against real DuckDB,
because every property that matters here — what `duckdb_sequences()` reports,
what survives a reopen — is a fact about the engine and not about our code.
"""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pytest

from core.duckdb_sequences import advance_to, next_value
from core.duckdb_store import DuckDBStore
from core.migrations import SEQUENCE_ID_COLUMNS, _m0027_reset_sequences_after_compaction


def _table(conn, *, primary_key: bool = False) -> None:
    conn.execute("CREATE SEQUENCE s START 1")
    pk = " PRIMARY KEY" if primary_key else ""
    conn.execute(f"CREATE TABLE t (id BIGINT{pk} DEFAULT nextval('s'), v INTEGER)")


def _insert_default(conn) -> int:
    return conn.execute("INSERT INTO t (v) VALUES (0) RETURNING id").fetchone()[0]


class TestAdvanceTo:
    def test_a_sequence_behind_explicit_ids_moves_above_them(self):
        conn = duckdb.connect()
        _table(conn)
        conn.execute("INSERT INTO t SELECT i, i FROM range(1, 501) r(i)")
        assert next_value(conn, "s") == 1

        burned = advance_to(conn, "s", 500)

        assert burned == 500
        assert _insert_default(conn) == 501

    def test_a_sequence_already_ahead_is_not_moved_and_consumes_nothing(self):
        conn = duckdb.connect()
        _table(conn)
        conn.execute("SELECT max(nextval('s')) FROM range(900)").fetchall()
        before = next_value(conn, "s")
        assert before == 901

        assert advance_to(conn, "s", 500) == 0
        assert advance_to(conn, "s", 900) == 0  # at the floor is past it

        assert next_value(conn, "s") == before
        assert _insert_default(conn) == 901

    def test_reading_the_position_consumes_nothing(self):
        conn = duckdb.connect()
        _table(conn)
        for _ in range(3):
            assert next_value(conn, "s") == 1
        assert _insert_default(conn) == 1

    def test_a_sequence_used_before_a_reopen_is_not_read_one_ahead(self, tmp_path):
        """After a reopen, `last_value` is the *next* value, not the last one.

        This is the case a `last_value + 1` reading gets wrong: it believes the
        sequence is one further on than it is, burns one value too few, and the
        next insert collides with MAX(id). The primary key makes that loud.
        """
        path = str(tmp_path / "seq.duckdb")
        conn = duckdb.connect(path)
        _table(conn, primary_key=True)
        assert _insert_default(conn) == 1
        conn.execute("INSERT INTO t SELECT i, i FROM range(2, 501) r(i)")
        conn.close()

        conn = duckdb.connect(path)
        last_value = conn.execute(
            "SELECT last_value FROM duckdb_sequences() WHERE sequence_name = 's'"
        ).fetchone()[0]
        assert last_value == next_value(conn, "s") == 2  # the engine fact this guards

        advance_to(conn, "s", 500)

        assert _insert_default(conn) == 501
        conn.close()

    def test_the_new_position_survives_a_reopen(self, tmp_path):
        """A burn that is not read to the end is not written to the WAL."""
        path = str(tmp_path / "seq.duckdb")
        conn = duckdb.connect(path)
        _table(conn, primary_key=True)
        conn.execute("INSERT INTO t SELECT i, i FROM range(1, 501) r(i)")
        conn.close()

        conn = duckdb.connect(path)
        advance_to(conn, "s", 500)
        conn.close()  # nothing else written: no checkpoint to carry it

        conn = duckdb.connect(path)
        assert _insert_default(conn) == 501
        conn.close()

    def test_a_gap_larger_than_one_client_chunk_is_burned_whole(self):
        conn = duckdb.connect()
        conn.execute("CREATE SEQUENCE s START 1")
        assert advance_to(conn, "s", 300_000) == 300_000
        assert next_value(conn, "s") == 300_001

    def test_an_increment_above_one_lands_past_the_floor(self):
        conn = duckdb.connect()
        conn.execute("CREATE SEQUENCE s START 10 INCREMENT BY 3")
        # 10, 13, 16, 19 are burned; 22 is the first value above 20.
        assert advance_to(conn, "s", 20) == 4
        assert conn.execute("SELECT nextval('s')").fetchall() == [(22,)]

    def test_a_schema_qualified_name(self):
        conn = duckdb.connect()
        conn.execute("CREATE SCHEMA app")
        conn.execute("CREATE SEQUENCE app.s START 1")
        conn.execute("CREATE SEQUENCE s START 1")  # same name, other schema

        assert advance_to(conn, "app.s", 40) == 40

        assert next_value(conn, "app.s") == 41
        assert next_value(conn, "s") == 1

    @pytest.mark.parametrize("name", [
        "s'",
        "s; DROP TABLE t",
        "s'); DROP TABLE t; --",
        'a"b',
        "a.b.c",
        "a b",
        "",
    ])
    def test_a_name_that_is_not_a_plain_identifier_is_refused(self, name):
        conn = duckdb.connect()
        _table(conn)
        with pytest.raises(ValueError):
            advance_to(conn, name, 500)
        assert _insert_default(conn) == 1  # and nothing was consumed

    def test_a_missing_sequence_is_an_error_not_a_no_op(self):
        conn = duckdb.connect()
        with pytest.raises(LookupError):
            advance_to(conn, "nope", 10)

    def test_a_gap_above_the_limit_is_refused_before_burning(self):
        conn = duckdb.connect()
        conn.execute("CREATE SEQUENCE s START 1")
        with pytest.raises(ValueError):
            advance_to(conn, "s", 1_000, max_burn=999)
        assert next_value(conn, "s") == 1


# ─── Migration 0027 against the real schema ───────────────────────────────────

# One default insert per table in SEQUENCE_ID_COLUMNS: the columns it needs
# and the values to put in them. `{n}` keeps UNIQUE constraints satisfied.
_MINIMAL_ROW = {
    "warehouse_refreshes": ("trigger", "'test'"),
    "reconciliation_log": (
        "check_date, api_count, db_count, discrepancy, discrepancy_pct, status",
        "DATE '2026-09-01', 1, 1, 0, 0, 'ok'",
    ),
    "data_quality_runs": (
        "started_at, as_of, window_start, window_end, layer, status",
        "TIMESTAMPTZ '2026-09-01 00:00:00+00', TIMESTAMPTZ '2026-09-01 00:00:00+00', "
        "DATE '2026-08-01', DATE '2026-09-01', 'integrity', 'PASS'",
    ),
    "stock_movements": (
        "offer_id, movement_type, quantity_before, quantity_after, delta, "
        "reserve_before, reserve_after",
        "1, 'sale', 2, 1, -1, 0, 0",
    ),
    "buyer_contacts": ("buyer_id, contact_type, value", "1, 'phone', '{n}'"),
    "manual_expenses": (
        "expense_date, category, expense_type, amount",
        "DATE '2026-09-01', 'other', 'test', 1.00",
    ),
}


def _insert(conn, table: str, col: str, n: int, *, explicit_id: bool) -> int:
    columns, values = _MINIMAL_ROW[table]
    values = values.format(n=n)
    if explicit_id:
        columns, values = f"{col}, {columns}", f"{n}, {values}"
    return conn.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {col}"
    ).fetchone()[0]


class TestMigration0027:
    def test_every_entry_has_a_row_to_insert(self):
        assert {table for _, table, _ in SEQUENCE_ID_COLUMNS} == set(_MINIMAL_ROW)

    @pytest.mark.asyncio
    async def test_every_default_insert_lands_above_max_id_after_a_reboot(
        self, tmp_path: Path, caplog
    ):
        """The boot path: sequences used, rows copied in with high ids, reopen.

        Using each sequence once first matters — it is what puts a reopened
        sequence into the state where `last_value` reads one ahead.
        """
        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        highs = {}
        try:
            async with store.connection() as conn:
                for i, (_, table, col) in enumerate(SEQUENCE_ID_COLUMNS):
                    assert _insert(conn, table, col, 1_000_000, explicit_id=False) == 1
                    highs[table] = 1_000 + 137 * i
                    _insert(conn, table, col, highs[table], explicit_id=True)
        finally:
            await store.close()

        # connect() runs every migration, 0027 among them, exactly as a boot does.
        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        with caplog.at_level(logging.WARNING, logger="core.migrations"):
            await store.connect()
        try:
            assert not [r for r in caplog.records if "0027" in r.getMessage()]
            async with store.connection() as conn:
                for _, table, col in SEQUENCE_ID_COLUMNS:
                    max_id = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]
                    assert max_id == highs[table]
                    new_id = _insert(conn, table, col, 2_000_000, explicit_id=False)
                    assert new_id > max_id, table
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_running_it_again_moves_nothing(self, tmp_path: Path):
        store = DuckDBStore(db_path=tmp_path / "test.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                for i, (_, table, col) in enumerate(SEQUENCE_ID_COLUMNS):
                    _insert(conn, table, col, 500 + i, explicit_id=True)
            _m0027_reset_sequences_after_compaction(store)
            async with store.connection() as conn:
                positions = {s: next_value(conn, s) for s, _, _ in SEQUENCE_ID_COLUMNS}
            _m0027_reset_sequences_after_compaction(store)
            async with store.connection() as conn:
                assert {s: next_value(conn, s) for s, _, _ in SEQUENCE_ID_COLUMNS} == positions
        finally:
            await store.close()

    def test_one_failing_entry_is_a_warning_and_the_rest_still_move(self, caplog):
        conn = duckdb.connect()
        for seq, table, col in SEQUENCE_ID_COLUMNS[1:]:
            conn.execute(f"CREATE SEQUENCE {seq} START 1")
            conn.execute(f"CREATE TABLE {table} ({col} BIGINT DEFAULT nextval('{seq}'))")
            conn.execute(f"INSERT INTO {table} VALUES (77)")
        # The first entry's table and sequence do not exist at all.

        class _Store:
            _connection = conn

        with caplog.at_level(logging.WARNING, logger="core.migrations"):
            _m0027_reset_sequences_after_compaction(_Store())

        first_seq = SEQUENCE_ID_COLUMNS[0][0]
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and first_seq in warnings[0].getMessage()
        for seq, _, _ in SEQUENCE_ID_COLUMNS[1:]:
            assert next_value(conn, seq) == 78
