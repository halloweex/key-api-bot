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


# ─── The copy-back: chain 5's two tables ────────────────────────────────────


def _fake_chain(**attrs):
    """A chain module declaring the unit, for the copy-back's derivations
    before (and independently of) the real one."""
    import types

    chain = types.ModuleType("core.pg_fake_managers_write")
    chain.CHAIN = "pg_fake_managers_write"
    chain.WRITE_ENV = "KS_WRITE_FAKE_MANAGERS"
    chain.CHAIN_TABLES = ("bronze.managers", "app.manager_classifications")
    chain.CHAIN_SYNC_KEYS = ("last_sync_managers",)
    for name, value in attrs.items():
        setattr(chain, name, value)
    return chain


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


class TestTheCopyBackDerivesTheUnit:
    def test_two_full_replace_specs_with_their_clocks(self):
        """Mutation: drop the replicated source — `chain_specs` raised
        `LookupError` for these two before chain 5 (design, M9)."""
        from core import chain_transfer
        from core.pg_replication import CLASSIFICATION_COLUMNS, MANAGER_COLUMNS

        managers, classifications = chain_transfer.chain_specs(_fake_chain())
        assert (managers.pg_table, managers.dk_table, managers.kind) == (
            "bronze.managers", "managers", "replicated")
        assert (classifications.pg_table, classifications.dk_table) == (
            "app.manager_classifications", "manager_classifications")
        assert managers.columns == MANAGER_COLUMNS
        assert classifications.columns == CLASSIFICATION_COLUMNS
        assert not managers.is_append and not managers.is_mirrored
        # `set_at` is carried by both stores as a value and dates a human's
        # decision; the managers carry no per-row clock both stores share.
        assert managers.clock == () and classifications.clock == ("set_at",)
        assert managers.compare.full_replace and classifications.compare.full_replace
        assert classifications.compare.key_columns == ("manager_id", "valid_from")
        assert chain_transfer.chain_sequences(_fake_chain()) == ()

    def test_a_table_in_no_source_still_raises(self):
        from core import chain_transfer

        with pytest.raises(LookupError, match="REPLICATED_SHAPES"):
            chain_transfer.chain_specs(_fake_chain(CHAIN_TABLES=("app.nowhere",)))


def _cspec(table):
    from core import chain_transfer

    (spec,) = [s for s in chain_transfer.chain_specs(_fake_chain())
               if s.pg_table == table]
    return spec


def _crow(spec, **values):
    return tuple(values.get(c) for c in spec.compare.columns)


T0 = _utc(2026, 10, 7, 9)


class TestTheHandoverOfAReplicatedTable:
    """`classify_handover` for `kind == "replicated"` (design §8)."""

    def _interval(self, spec, manager_id, valid_from, *, set_at=T0, retail=False):
        return _crow(spec, manager_id=manager_id, is_retail=retail,
                     valid_from=valid_from, set_at=set_at, note="n")

    def test_before_a_flip_a_key_only_postgres_holds_is_critical(self):
        """There is no next full replace after a flip: a ghost interval would
        stay in the source of truth and reclassify past orders. Mutation:
        leave it INFO (design, M10)."""
        from core.chain_transfer import classify_handover

        spec = _cspec("app.manager_classifications")
        ghost = self._interval(spec, 7, date(2026, 1, 1), retail=True)
        issues = classify_handover(spec, {}, {(7, date(2026, 1, 1)): ghost},
                                   moved_on=False)
        (issue,) = issues
        assert (issue.check_name, issue.severity.value) == (
            "handover_rows_ahead", "CRITICAL")
        assert "ghost interval" in issue.description

    def test_after_the_latch_it_is_the_size_of_the_copy(self):
        from core.chain_transfer import classify_handover

        spec = _cspec("app.manager_classifications")
        row = self._interval(spec, 7, date(2026, 1, 1))
        (issue,) = classify_handover(spec, {}, {(7, date(2026, 1, 1)): row},
                                     moved_on=True)
        assert (issue.check_name, issue.severity.value) == (
            "handover_rows_ahead", "INFO")

    @pytest.mark.parametrize("table", ["bronze.managers", "app.manager_classifications"])
    def test_the_pre_flip_lever_is_this_pairs_shipper(self, table):
        """`replicate_operational` never ships these tables. Mutation: the
        operational sentence."""
        from core.chain_transfer import classify_handover

        spec = _cspec(table)
        key = 7 if table == "bronze.managers" else (7, date(2026, 1, 1))
        row = (_crow(spec, id=7, name="M", is_retail=False, order_count=0)
               if table == "bronze.managers" else self._interval(spec, 7, date(2026, 1, 1)))
        for dk, pg in (({key: row}, {}), ({}, {key: row})):
            issues = classify_handover(spec, dk, pg, moved_on=False)
            assert issues and all(i.severity.value == "CRITICAL" for i in issues)
            for issue in issues:
                assert "replicate_managers" in issue.description
                assert "manager_stats/trigger" in issue.description
                assert "replicate_operational" not in issue.description

    def test_a_pre_flip_difference_is_refused_with_the_lever(self):
        from core.chain_transfer import classify_handover

        spec = _cspec("bronze.managers")
        dk = {7: _crow(spec, id=7, name="M", is_retail=True, order_count=1)}
        pg = {7: _crow(spec, id=7, name="M", is_retail=False, order_count=1)}
        (issue,) = classify_handover(spec, dk, pg, moved_on=False)
        assert (issue.check_name, issue.severity.value) == (
            "handover_rows_differ", "CRITICAL")
        assert "replicate_managers" in issue.description

    def test_after_the_latch_a_key_only_duckdb_holds_is_a_write_after_it(self):
        """The writer never removes a key, so this is DuckDB written after the
        latch, and the copy would delete it."""
        from core.chain_transfer import classify_handover

        spec = _cspec("app.manager_classifications")
        row = self._interval(spec, 7, date(2026, 5, 1))
        (issue,) = classify_handover(spec, {(7, date(2026, 5, 1)): row}, {},
                                     moved_on=True)
        assert (issue.check_name, issue.severity.value) == (
            "handover_rows_missing", "CRITICAL")
        assert "never removes a key" in issue.description
        assert "retail-status" in issue.description

    def test_after_the_latch_a_later_duckdb_decision_is_refused_by_its_clock(self):
        from core.chain_transfer import classify_handover

        spec = _cspec("app.manager_classifications")
        key = (7, date(2026, 5, 1))
        dk = {key: self._interval(spec, 7, date(2026, 5, 1), set_at=T0, retail=True)}
        pg = {key: self._interval(spec, 7, date(2026, 5, 1),
                                  set_at=T0.replace(hour=8), retail=False)}
        (issue,) = classify_handover(spec, dk, pg, moved_on=True)
        assert (issue.check_name, issue.severity.value) == (
            "handover_rows_newer_in_duckdb", "CRITICAL")
        # And the other way round it is the chain's later write: INFO.
        (issue,) = classify_handover(spec, pg, dk, moved_on=True)
        assert (issue.check_name, issue.severity.value) == ("handover_rows_differ", "INFO")

    def test_null_where_duckdb_says_not_null_is_still_refused(self):
        from core.chain_transfer import classify_handover

        spec = _cspec("bronze.managers")
        pg = {7: _crow(spec, id=7, name=None, is_retail=False, order_count=0)}
        issues = classify_handover(spec, {}, pg, moved_on=True, required=("id", "name"))
        assert ("handover_rows_unwritable", "CRITICAL") in {
            (i.check_name, i.severity.value) for i in issues}

    def test_the_operational_tables_are_judged_as_before(self):
        """The dispatch is by kind: an operational table's pre-flip key only
        Postgres holds stays INFO."""
        from core import pg_goals_write
        from core.chain_transfer import chain_specs, classify_handover

        (spec,) = chain_specs(pg_goals_write)
        row = _crow(spec, period_type="daily", goal_amount=1, is_custom=True,
                    updated_at=T0)
        (issue,) = classify_handover(spec, {}, {"daily": row}, moved_on=False)
        assert issue.severity.value == "INFO"
        assert "replicate_managers" not in issue.description


class _Conn:
    """A DuckDB connection that records what the copy-back asked of it."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchone(self):
        return (False,)

    def fetchall(self):
        return []


class _Store:
    def __init__(self, conn):
        self.conn = conn
        self.checkpoints = 0

    def connection(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False
        return _Ctx()

    async def checkpoint(self):
        self.checkpoints += 1


class TestTheCopyBackOwesAFullRebuild:
    """`sales_type` is materialised at rebuild time, and an incremental
    rebuild re-derives only the order ids it is handed — so a classification
    carried back owes DuckDB a full rebuild, in the copy's own transaction."""

    async def _execute(self, chain):
        from unittest.mock import AsyncMock, patch

        from core import chain_transfer

        conn = _Conn()
        store = _Store(conn)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
                patch("core.pg.require_revision", new=AsyncMock()), \
                patch.object(chain_transfer, "_owned_since",
                             new=AsyncMock(return_value="2026-10-07T09:00:00+00:00")), \
                patch.object(chain_transfer, "_handover_issues", new=AsyncMock(return_value=[])), \
                patch.object(chain_transfer, "_read_pg", new=AsyncMock(return_value=[])), \
                patch.object(chain_transfer, "_read_sync_keys",
                             new=AsyncMock(return_value={"last_sync_managers": "x"})), \
                patch.object(chain_transfer, "_read_pg_sequences", new=AsyncMock(return_value={})), \
                patch.object(chain_transfer, "verify", new=AsyncMock(return_value=[])), \
                patch.object(chain_transfer, "release_chain", new=AsyncMock(return_value=True)):
            plan = await chain_transfer.copy_back(store, chain, dry_run=False)
        return plan, [sql for sql, _p in conn.calls]

    @pytest.mark.asyncio
    async def test_marked_full_inside_the_transaction(self):
        """Mutation: skip the mark, or write it after the COMMIT."""
        plan, statements = await self._execute(
            _fake_chain(CHAIN_COPY_BACK_OWES_FULL_REBUILD=True))
        assert plan["released"] is True and plan["full_rebuild_owed"] is True
        begin = statements.index("BEGIN TRANSACTION")
        commit = statements.index("COMMIT")
        marks = [i for i, sql in enumerate(statements)
                 if "warehouse_dirty" in sql and "'full'" in sql]
        assert marks and all(begin < i < commit for i in marks), statements

    @pytest.mark.asyncio
    async def test_and_not_for_a_chain_that_does_not_declare_it(self):
        """Mutation: mark for every chain — chain 7a's goals are not read by
        Silver and owe no rebuild."""
        plan, statements = await self._execute(
            _fake_chain(CHAIN_TABLES=("app.revenue_goals",), CHAIN_SYNC_KEYS=()))
        assert "full_rebuild_owed" not in plan
        assert not [s for s in statements if "warehouse_dirty" in s]

    def test_the_released_runbook_names_the_shipper_and_the_rebuild(self):
        from core import chain_transfer

        text = "\n".join(chain_transfer._runbook(
            _fake_chain(), executed=True, released=True))
        assert "KS_WRITE_FAKE_MANAGERS=duckdb" in text
        assert "replicate_managers" in text and "manager_stats/trigger" in text
        assert "warehouse_dirty=full" in text
        assert "replicate_operational" not in text and "hourly copy" not in text

    def test_the_marker_only_steps_say_which_copy_resumes(self):
        from core import chain_transfer

        text = chain_transfer._marker_only(_fake_chain(), "pg_fake_managers_write",
                                           "2026-10-07T09:00:00+00:00")
        assert "--handover" in text and "replicate_managers" in text
        # And no other chain is told about it.
        from core import pg_goals_write

        other = chain_transfer._marker_only(pg_goals_write, "pg_goals_write", "x")
        assert "replicate_managers" not in other
