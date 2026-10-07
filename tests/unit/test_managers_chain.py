"""Chain 5 — the manager classification and stats, without a database.

What is pinned here, each with the mutation it must kill:

- the payload is read once, for both stores (`core.landing_rows.manager_row`);
- the stats body is one text, and its date is Kyiv's in both engines rather
  than the process's — a laptop or CI under UTC used to store a day early;
- the copy-back's fourth source derives chain 5's two tables from what the
  replicator ships and what the daily comparison compares.

Everything that needs Postgres is in
`tests/integration/test_managers_chain_writer.py`.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import date, datetime, timezone

import pytest

from core import landing_rows, sql_dialect
from core.sql_dialect import DUCKDB, POSTGRES


# ─── The one reading of a KeyCRM user ───────────────────────────────────────


class TestTheManagerRow:
    @pytest.mark.parametrize("payload,expected", [
        ({"id": 1, "name": "Олена", "email": "o@x", "status": "active"},
         (1, "Олена", "o@x", "active", False)),
        # `name` empty, `full_name` there: the fallback.
        ({"id": 2, "name": "", "full_name": "Петро"}, (2, "Петро", None, None, False)),
        # Neither: 'Unknown'.
        ({"id": 3}, (3, "Unknown", None, None, False)),
        # `full_name` present and null stays null — DuckDB's expression, which
        # the writers then refuse rather than invent a name.
        ({"id": 5, "name": None, "full_name": None}, (5, None, None, None, False)),
        # A listed id seeds retail; an unlisted one does not.
        ({"id": 4, "name": "R"}, (4, "R", None, None, True)),
    ])
    def test_every_edge_reads_as_it_always_did(self, payload, expected):
        """Mutation: change a fallback, or the seed, in the shared function."""
        assert tuple(landing_rows.manager_row(payload)) == expected

    def test_the_seed_is_the_constant(self):
        from core.duckdb_constants import RETAIL_MANAGER_IDS

        for mid in RETAIL_MANAGER_IDS:
            assert landing_rows.manager_row({"id": mid, "name": "x"}).is_retail is True

    def test_a_batch_keeps_keycrms_order(self):
        rows = landing_rows.manager_rows([{"id": 9, "name": "a"}, {"id": 1, "name": "b"},
                                          {"id": 9, "name": "c"}])
        assert [(r.id, r.name) for r in rows] == [(9, "a"), (1, "b"), (9, "c")]

    def test_duckdbs_writer_reads_it_through_the_shared_function(self):
        """Mutation: spell the row inline in DuckDB again — the two stores
        would then be one edit away from naming a manager differently."""
        from core.duckdb_store import DuckDBStore

        tree = ast.parse(textwrap.dedent(inspect.getsource(DuckDBStore.upsert_managers)))
        called = {getattr(n.func, "id", getattr(n.func, "attr", ""))
                  for n in ast.walk(tree) if isinstance(n, ast.Call)}
        assert "manager_rows" in called
        # Parsed, not grepped: the docstring names the constant on purpose.
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert "RETAIL_MANAGER_IDS" not in names
        reads = [n for n in ast.walk(tree) if isinstance(n, ast.Constant)
                 and n.value == "full_name"]
        assert not reads, "the payload is read in DuckDB's writer again"


# ─── The stats body ─────────────────────────────────────────────────────────


class TestTheStatsBody:
    def test_one_text_for_both_engines(self):
        """The two renders differ only by the names and the date's spelling."""
        duck, pg = (sql_dialect.manager_stats_sql(DUCKDB),
                    sql_dialect.manager_stats_sql(POSTGRES))
        assert "UPDATE managers m" in duck and "FROM orders" in duck
        assert "UPDATE bronze.managers m" in pg and "FROM bronze.orders" in pg
        undone = (pg.replace("bronze.managers", "managers")
                    .replace("bronze.orders", "orders")
                    .replace(POSTGRES.kyiv_date("ordered_at"), DUCKDB.kyiv_date("ordered_at")))
        assert undone == duck

    @pytest.mark.parametrize("dialect", [DUCKDB, POSTGRES], ids=lambda d: d.name)
    def test_the_date_is_kyivs_in_both(self, dialect):
        """Mutation: `DATE(ordered_at)` / `ordered_at::date` — the process's
        or the session's timezone, a day early for every order between 00:00
        and 03:00 Kyiv."""
        sql = sql_dialect.manager_stats_sql(dialect)
        assert sql.count("timezone('Europe/Kyiv', ordered_at)") == 2
        assert "DATE(ordered_at)" not in sql and "ordered_at::date" not in sql

    @pytest.mark.asyncio
    @pytest.mark.parametrize("session_tz", ["UTC", "Europe/Kyiv"])
    async def test_duckdb_stores_kyiv_dates_whatever_its_timezone(self, tmp_path, session_tz):
        """Orders at 22:30 UTC on 28.02 and 21:15 UTC on 31.07 are Kyiv's 01.03
        and 01.08 (chain 5's design, M3). Under `TZ=Europe/Kyiv` — production —
        the old spelling gave the same answer, so production does not move;
        under UTC it gave 28.02 and 31.07."""
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "stats.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                conn.execute(f"SET TimeZone = '{session_tz}'")
                conn.execute("INSERT INTO managers (id, name) VALUES (7, 'M')")
                conn.execute(
                    "INSERT INTO orders (id, source_id, status_id, grand_total, "
                    "ordered_at, manager_id) VALUES "
                    "(1, 1, 1, 10, TIMESTAMPTZ '2026-02-28 22:30:00+00', 7), "
                    "(2, 1, 1, 10, TIMESTAMPTZ '2026-07-31 21:15:00+00', 7)")
            from unittest.mock import AsyncMock, patch

            with patch("core.pg_replication.replicate_managers", new=AsyncMock()):
                await store.update_manager_stats()
            async with store.connection() as conn:
                row = conn.execute(
                    "SELECT first_order_date, last_order_date, order_count "
                    "FROM managers WHERE id = 7").fetchone()
                old = conn.execute(
                    "SELECT MIN(DATE(ordered_at)), MAX(DATE(ordered_at)) FROM orders"
                ).fetchone()
        finally:
            await store.close()
        assert row == (date(2026, 3, 1), date(2026, 8, 1), 2)
        if session_tz == "Europe/Kyiv":
            assert old == (date(2026, 3, 1), date(2026, 8, 1)), (
                "production's answer must not move")
        else:
            assert old == (date(2026, 2, 28), date(2026, 7, 31)), (
                "the case the old spelling got wrong")


# ─── The copy-back's fourth source ──────────────────────────────────────────


class TestTheReplicatedShapes:
    def test_the_shipping_shape_is_what_write_managers_writes(self):
        """`REPLICATED_SHAPES` is read by the copy-back as the way these two
        tables are carried; it must name the columns the replicator ships."""
        from core.pg_replication import (
            CLASSIFICATION_COLUMNS, MANAGER_COLUMNS, MANAGER_UNIT, REPLICATED_SHAPES,
        )

        assert tuple(s[0] for s in REPLICATED_SHAPES) == MANAGER_UNIT
        shapes = {s[0]: s for s in REPLICATED_SHAPES}
        assert shapes["bronze.managers"][1:3] == ("managers", MANAGER_COLUMNS)
        assert shapes["app.manager_classifications"][1:3] == (
            "manager_classifications", CLASSIFICATION_COLUMNS)

    def test_the_daily_specs_are_looked_up_not_restated(self):
        from core.mirror_reconciliation import MIRRORED_TABLES, REPLICATED_TABLES

        assert REPLICATED_TABLES and all(s in MIRRORED_TABLES for s in REPLICATED_TABLES)
        assert {s.pg_table for s in REPLICATED_TABLES} == {
            "bronze.managers", "app.manager_classifications"}
        assert all(s.full_replace for s in REPLICATED_TABLES)


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)
