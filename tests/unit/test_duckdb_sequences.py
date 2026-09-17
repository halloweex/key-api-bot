"""Advancing a DuckDB sequence past the ids its table already holds.

Migration 0027 used to DROP and re-CREATE these sequences, with a peek that
consumed a value on every boot and failures logged at DEBUG. On DuckDB 1.5.5
that DROP raises DependencyException only in the session that created the
dependent table; after a reopen it works. These tests run against real DuckDB,
because every property that matters here — what `duckdb_sequences()` reports,
what survives a reopen — is a fact about the engine and not about our code.

0027 is the only net under the weekly compaction's own restore, so the last
classes are about the net itself: a sequence it cannot move is published where
/api/health reads it, a connect that fails before the migrations run cannot
leave behind a store that serves without them, and a view that cannot be built
neither skips the migrations nor keeps the store from connecting.
"""
from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

import core.duckdb_store as duckdb_store
import core.migrations as migrations
from core.duckdb_sequences import advance_to, next_value
from core.duckdb_store import DuckDBStore
from core.migrations import M0027_ID, SEQUENCE_ID_COLUMNS, _m0027_reset_sequences_after_compaction


@pytest.fixture(autouse=True)
def _data_dir_in_tmp(monkeypatch, tmp_path):
    """`connect()` creates DB_DIR, which is the repository's `data/` unless
    redirected; conftest redirects DB_PATH only."""
    monkeypatch.setattr(duckdb_store, "DB_DIR", tmp_path)


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

    def test_one_failing_entry_is_published_and_the_rest_still_move(self, caplog):
        conn = duckdb.connect()
        for seq, table, col in SEQUENCE_ID_COLUMNS[1:]:
            conn.execute(f"CREATE SEQUENCE {seq} START 1")
            conn.execute(f"CREATE TABLE {table} ({col} BIGINT DEFAULT nextval('{seq}'))")
            conn.execute(f"INSERT INTO {table} VALUES (77)")
        # The first entry's table and sequence do not exist at all.

        class _Store:
            _connection = conn
            _failed_migrations: list = []

        store = _Store()
        with caplog.at_level(logging.WARNING, logger="core.migrations"):
            _m0027_reset_sequences_after_compaction(store)

        first_seq = SEQUENCE_ID_COLUMNS[0][0]
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and first_seq in warnings[0].getMessage()
        assert [(f["id"], f["sequence"]) for f in store._failed_migrations] == [
            (M0027_ID, first_seq)
        ]
        assert first_seq in store._failed_migrations[0]["error"]
        for seq, _, _ in SEQUENCE_ID_COLUMNS[1:]:
            assert next_value(conn, seq) == 78

    @pytest.mark.asyncio
    async def test_a_sequence_it_cannot_move_turns_the_ledger_to_failed_on_a_real_boot(
        self, tmp_path: Path
    ):
        """Through `connect()`, which is what web's startup runs: the failure
        must reach `schema_status()`, the snapshot /api/health publishes and
        reads as "degraded", while every other sequence is still repaired."""
        path = tmp_path / "boot.duckdb"
        store = DuckDBStore(db_path=path)
        await store.connect()
        highs = {}
        try:
            async with store.connection() as conn:
                for i, (_, table, col) in enumerate(SEQUENCE_ID_COLUMNS):
                    highs[table] = 700 + 11 * i
                    _insert(conn, table, col, highs[table], explicit_id=True)
        finally:
            await store.close()

        broken_seq, broken_table, broken_col = SEQUENCE_ID_COLUMNS[1]
        real_advance_to = migrations.advance_to

        def advance_all_but_one(conn, seq_name, floor, **kwargs):
            if seq_name == broken_seq:
                raise RuntimeError("the position read back did not move")
            return real_advance_to(conn, seq_name, floor, **kwargs)

        store = DuckDBStore(db_path=path)
        with patch.object(migrations, "advance_to", advance_all_but_one):
            await store.connect()
        try:
            status = store.schema_status()
            assert status["status"] == "failed"
            assert [(f["id"], f["sequence"]) for f in status["failed"]] == [
                (M0027_ID, broken_seq)
            ]
            assert "the position read back did not move" in status["failed"][0]["error"]

            async with store.connection() as conn:
                for seq, table, col in SEQUENCE_ID_COLUMNS:
                    if seq == broken_seq:
                        assert next_value(conn, seq) <= highs[table]
                        continue
                    new_id = _insert(conn, table, col, 3_000_000, explicit_id=False)
                    assert new_id > highs[table], table
        finally:
            await store.close()


async def _file_with_sequences_behind(path: Path) -> dict:
    """A database whose every sequence is behind MAX(id), as a compaction
    whose own restore fell short would leave it."""
    store = DuckDBStore(db_path=path)
    await store.connect()
    highs = {}
    try:
        async with store.connection() as conn:
            for i, (_, table, col) in enumerate(SEQUENCE_ID_COLUMNS):
                highs[table] = 900 + 13 * i
                _insert(conn, table, col, highs[table], explicit_id=True)
                assert next_value(conn, SEQUENCE_ID_COLUMNS[i][0]) <= highs[table]
    finally:
        await store.close()
    return highs


def _assert_the_net_ran(conn, highs: dict) -> None:
    for _, table, col in SEQUENCE_ID_COLUMNS:
        new_id = _insert(conn, table, col, 4_000_000, explicit_id=False)
        assert new_id > highs[table], f"{table}: 0027 never ran"


def _count_migration_runs():
    real = DuckDBStore._run_migrations
    calls = []

    async def counting(self):
        calls.append(1)
        return await real(self)

    return patch.object(DuckDBStore, "_run_migrations", counting), calls


def _broken_table_ddl():
    """The statement that creates Silver, cut off mid-definition — a failure
    that leaves no usable schema, unlike a view."""
    return patch.object(
        duckdb_store, "SILVER_ORDERS_DDL", "CREATE TABLE IF NOT EXISTS silver_orders (",
    )


def _broken_migration_runner():
    """The ledger itself unreadable: `_run_migrations` raising, as opposed to
    one step inside it failing, which it catches and publishes."""
    async def boom(self):
        raise RuntimeError("schema_migrations could not be read")

    return patch.object(DuckDBStore, "_run_migrations", boom)


class TestAFailedConnectLeavesNothingBehind:
    """The boot net runs inside `connect()`, after the table DDL.

    Anything that raised before `_run_migrations` used to leave the store's
    connection set, and `connection()` reconnects only when it is None — so
    every later caller, web's degraded-mode startup included, got a store on
    which no migration had run, 0027 among them, and no failure anywhere.

    What still raises is what leaves no usable store: the table DDL and the
    migration runner itself. A view does not — `TestAViewThatCannotBeBuilt`.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("breakage, raised", [
        (_broken_table_ddl, duckdb.Error),
        (_broken_migration_runner, RuntimeError),
    ], ids=["table_ddl", "migration_runner"])
    async def test_a_failure_that_leaves_no_schema_raises_and_keeps_nothing(
        self, tmp_path, breakage, raised,
    ):
        path = tmp_path / "broken.duckdb"
        await _file_with_sequences_behind(path)

        store = DuckDBStore(db_path=path)
        with breakage():
            with pytest.raises(raised):
                await store.connect()

        assert store._connection is None
        assert store._executor is None
        assert store.schema_status()["status"] == "unknown"

    @pytest.mark.asyncio
    async def test_the_next_use_of_the_store_runs_the_whole_connect(self, tmp_path):
        path = tmp_path / "flaky.duckdb"
        highs = await _file_with_sequences_behind(path)

        store = DuckDBStore(db_path=path)
        with _broken_table_ddl():
            with pytest.raises(duckdb.Error):
                await store.connect()
        assert store._connection is None

        counting, calls = _count_migration_runs()
        with counting:
            try:
                async with store.connection() as conn:
                    assert len(calls) == 1, "the second use did not connect again"
                    assert store.schema_status()["status"] == "ok"
                    _assert_the_net_ran(conn, highs)
            finally:
                await store.close()

    @pytest.mark.asyncio
    async def test_the_singleton_is_not_published_until_connected(self, tmp_path):
        """web/main.py's startup catches a failed first sync and calls
        `get_store()` again to serve stale data. That second call must be a
        second, complete connect."""
        highs = await _file_with_sequences_behind(duckdb_store.DB_PATH)

        with _broken_table_ddl():
            with pytest.raises(duckdb.Error):
                await duckdb_store.get_store()
        assert duckdb_store._store_instance is None

        counting, calls = _count_migration_runs()
        with counting:
            store = await duckdb_store.get_store()
            try:
                assert len(calls) == 1
                assert store.schema_status()["status"] == "ok"
                async with store.connection() as conn:
                    _assert_the_net_ran(conn, highs)
            finally:
                await duckdb_store.close_store()


def _views_always_fail(which: str):
    """A view whose SQL fails on every attempt, as one reading a column the
    file does not have would."""
    if which == "inventory":
        async def boom(self):
            raise duckdb.BinderException(
                'Binder Error: Table "s" does not have a column named "later"'
            )

        return patch.object(DuckDBStore, "_create_inventory_views", boom)
    return patch.object(
        duckdb_store, "SILVER_ORDER_LINES_VIEW_SQL",
        "CREATE OR REPLACE VIEW silver_order_lines AS "
        "SELECT s.column_a_later_migration_adds FROM silver_orders s",
    )


class TestAViewThatCannotBeBuilt:
    """A view is built after the migrations, and failing to build one is
    published, not raised.

    Raised, it would take the whole store down: `connect()` is all-or-nothing,
    a view that fails on this file fails the same way on the next attempt, and
    web's startup stops on the second — a restart loop until a fix shipped,
    for a failure that costs the readers of one view.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("which", ["inventory", "silver_order_lines"])
    async def test_the_store_connects_with_every_migration_applied(
        self, tmp_path, which,
    ):
        path = tmp_path / "views.duckdb"
        highs = await _file_with_sequences_behind(path)

        store = DuckDBStore(db_path=path)
        with _views_always_fail(which):
            await store.connect()
        try:
            assert store._connection is not None
            status = store.schema_status()
            assert status["status"] == "failed"
            assert [f["id"] for f in status["failed"]] == [f"view:{which}"]
            assert "column" in status["failed"][0]["error"]
            assert status["pending"] == [], "a migration was skipped"

            async with store.connection() as conn:
                _assert_the_net_ran(conn, highs)
                # One broken view costs that view, not its neighbour.
                other = ("silver_order_lines" if which == "inventory"
                         else "v_sku_analysis")
                conn.execute(f"SELECT COUNT(*) FROM {other}").fetchall()
        finally:
            await store.close()

    def test_health_reads_degraded_while_the_store_serves(self, monkeypatch):
        """Through the endpoint the canary polls: DuckDB connected and
        answering, the status degraded, and the view named."""
        import asyncio

        from fastapi.testclient import TestClient

        from web.main import app
        from web.ratelimit import limiter
        from web.routes.api import health as health_routes

        asyncio.run(_file_with_sequences_behind(duckdb_store.DB_PATH))
        monkeypatch.setitem(health_routes._stats_cache, "data", None)
        monkeypatch.setitem(health_routes._stats_cache, "expires_at", 0)
        limiter.reset()

        try:
            with _views_always_fail("inventory"):
                body = TestClient(app).get("/api/health").json()
        finally:
            limiter.reset()
            asyncio.run(duckdb_store.close_store())

        assert body["duckdb"]["status"] == "connected"
        assert body["migrations"]["status"] == "failed"
        assert [f["id"] for f in body["migrations"]["failed"]] == ["view:inventory"]
        assert body["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_a_file_older_than_a_migration_its_view_reads_boots_clean(
        self, tmp_path,
    ):
        """A real reopen, no patching: a file from before 0020, whose
        `silver_orders` has no `promocode` — which `silver_order_lines` reads.

        With the views first, the view raised before 0020 could add the
        column, on every boot. Migrations first, the column is there by the
        time the view is built."""
        path = tmp_path / "before_0020.duckdb"
        store = DuckDBStore(db_path=path)
        await store.connect()
        await store.close()

        conn = duckdb.connect(str(path))
        conn.execute("DROP VIEW silver_order_lines")
        # DuckDB will not drop a column an index sits on; the schema script
        # puts them back on the reopen, before 0020 runs.
        for (index,) in conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'silver_orders'"
        ).fetchall():
            conn.execute(f"DROP INDEX {index}")
        conn.execute("ALTER TABLE silver_orders DROP COLUMN promocode")
        conn.execute(
            "DELETE FROM schema_migrations "
            "WHERE id = '0020_promocode_on_orders_and_silver'"
        )
        conn.close()

        store = DuckDBStore(db_path=path)
        await store.connect()
        try:
            status = store.schema_status()
            assert status["failed"] == [], status["failed"]
            assert status["status"] == "ok"
            assert status["pending"] == []
            async with store.connection() as conn:
                assert conn.execute(
                    "SELECT promocode FROM silver_order_lines LIMIT 0"
                ).fetchall() == []
        finally:
            await store.close()
