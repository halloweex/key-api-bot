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


# ─── The chain: flag, preconditions, registry ───────────────────────────────


@pytest.fixture
def flags(monkeypatch):
    from core import pg_managers_write

    monkeypatch.delenv(pg_managers_write.WRITE_ENV, raising=False)
    monkeypatch.setattr(pg_managers_write, "_unmet_warned", {})
    return monkeypatch


class _Chain3:
    """`core.pg_orders_write` as far as chain 5 asks it: `mode()`."""

    WRITE_ENV = "KS_WRITE_ORDERS"

    def __init__(self, mode="postgres", raises=None):
        self._mode, self._raises = mode, raises

    def mode(self):
        if self._raises:
            raise self._raises
        return self._mode


# Where the stand-in goes: a name of its own, never `core.pg_orders_write`.
# Chain 3 is in the tree, and its own `mode()` reads its module back out of
# `sys.modules` (`pg_orders_write._self`), so a stand-in at chain 3's name would
# answer for the REAL chain everywhere else in the process — the tick's order
# step asks `pg_orders_write.mode()` too, and got this object. Chain 5 asks
# chain 3 by the name in `_CHAIN3`, read when it asks, so pointing that at the
# stand-in fakes exactly what chain 5 asks and nothing chain 3 asks of itself.
_CHAIN3_STAND_IN = "tests.unit._chain3_as_chain5_asks_it"


def _stand_in_chain3(mp, value) -> None:
    """Chain 3 as chain 5 sees it: `value` is a `_Chain3`, or None for a
    build without chain 3."""
    import sys

    from core import pg_managers_write

    mp.setattr(pg_managers_write, "_CHAIN3", _CHAIN3_STAND_IN)
    mp.setitem(sys.modules, _CHAIN3_STAND_IN, value)


@pytest.fixture
def every_precondition_met(flags):
    """The four facts chain 5 waits on, each made true at its source — so a
    test that then breaks one exercises the real check, not a stub of it.
    Chain 3's answer is the one stand-in, at the name chain 5 asks
    (`_stand_in_chain3`); `TestTheRealChain3` asks the real one."""
    from core import read_fallback, warehouse_cutover
    from core.repositories import goals

    flags.setattr(goals, "SALES_TYPE_BRIDGE_TABLES", frozenset({"bronze.orders"}))
    flags.setattr(warehouse_cutover, "_mode", "postgres")
    _stand_in_chain3(flags, _Chain3())
    flags.setattr(read_fallback, "refusing", lambda: True)
    return flags


class TestTheFlag:
    def test_off_by_default_and_registered_standing_nothing_down(self, flags):
        """Mutation: default to postgres — production would move at deploy."""
        from core import pg_managers_write, write_chains

        assert pg_managers_write.env_writes_postgres() is False
        assert pg_managers_write.mode() == "duckdb"
        assert pg_managers_write in write_chains.WRITE_CHAINS
        state = write_chains.chain_modes()["pg_managers_write"]
        assert (state["mode"], state["error"], state["latched"]) == ("duckdb", None, False)
        assert not set(pg_managers_write.CHAIN_TABLES) & write_chains.stood_down_tables()
        assert write_chains.chain_for_sync_key("last_sync_managers") is pg_managers_write

    def test_a_typo_stops_this_chain_and_nothing_else(self, flags):
        """DN-01. Mutation: swallow the typo as duckdb — the writers would go
        on writing DuckDB, and the replica would ship over nothing."""
        from core import pg_managers_write, write_chains

        flags.setenv(pg_managers_write.WRITE_ENV, "postgress")
        with pytest.raises(RuntimeError, match=pg_managers_write.WRITE_ENV):
            pg_managers_write.env_writes_postgres()
        with pytest.raises(RuntimeError):
            pg_managers_write.writes_postgres()
        modes = write_chains.chain_modes()
        assert modes["pg_managers_write"]["mode"] is None
        assert "postgress" in modes["pg_managers_write"]["error"]
        assert set(pg_managers_write.CHAIN_TABLES) <= write_chains.stood_down_tables()
        assert all(state["mode"] == "duckdb" for name, state in modes.items()
                   if name != "pg_managers_write")
        assert pg_managers_write.mode() is None
        # Reads hand the choice back: neither copy moves on a typo.
        assert pg_managers_write.reads_postgres() is False

    def test_the_latch_outranks_any_flag(self, flags):
        from core import chain_latch, pg_managers_write

        flags.setenv(pg_managers_write.WRITE_ENV, "duckdb")
        chain_latch.latch(pg_managers_write.CHAIN, pg_managers_write.WRITE_ENV)
        assert pg_managers_write.writes_postgres() is True
        assert pg_managers_write.mode() == "postgres"
        assert pg_managers_write.reads_postgres() is True


class TestThePreconditions:
    def test_today_every_one_is_unmet(self, flags):
        from core import pg_managers_write

        unmet = pg_managers_write.unmet_precondition()
        for key in ("goals_bridge", "step13", "chain3", "read_fallback_off"):
            assert key in unmet, key

    def test_with_all_four_met_the_flag_moves_the_writes(self, every_precondition_met):
        from core import pg_managers_write

        assert pg_managers_write.unmet_precondition() is None
        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        assert pg_managers_write.writes_postgres() is True
        assert pg_managers_write.mode() == "postgres"
        assert pg_managers_write.reads_postgres() is True

    @pytest.mark.parametrize("broken", ["goals_bridge", "step13", "chain3",
                                        "read_fallback_off"])
    def test_any_one_unmet_holds_the_flag_on_duckdb(self, every_precondition_met, broken):
        """Mutation: remove any one of the four checks."""
        from core import pg_managers_write, read_fallback, warehouse_cutover
        from core.repositories import goals

        env = every_precondition_met
        if broken == "goals_bridge":
            env.setattr(goals, "SALES_TYPE_BRIDGE_TABLES",
                        frozenset({"bronze.orders", "app.manager_classifications"}))
        elif broken == "step13":
            env.setattr(warehouse_cutover, "_mode", "duckdb")
        elif broken == "chain3":
            _stand_in_chain3(env, _Chain3("duckdb"))
        else:
            env.setattr(read_fallback, "refusing", lambda: False)
        env.setenv(pg_managers_write.WRITE_ENV, "postgres")

        assert pg_managers_write.unmet_precondition().startswith(broken)
        assert pg_managers_write.writes_postgres() is False
        assert pg_managers_write.mode() == "duckdb"
        # And reads stay with the rows: a held flag writes DuckDB.
        assert pg_managers_write.reads_postgres() is False

    def test_the_bridge_is_named_while_it_holds_either_table(self, every_precondition_met):
        """Spelled `goals_bridge`, as chain 3 spells it, so the tripwire in
        test_goals_off_duckdb_silver.py finds the hold. Mutation: check the
        wrong table."""
        from core import pg_managers_write
        from core.repositories import goals

        for table in ("bronze.managers", "app.manager_classifications"):
            every_precondition_met.setattr(goals, "SALES_TYPE_BRIDGE_TABLES",
                                           frozenset({table}))
            assert "goals_bridge" in pg_managers_write.unmet_precondition()

    def test_a_build_without_chain_3_is_unmet_not_an_import_error(self, every_precondition_met):
        """Mutation: treat a missing chain 3 as met."""
        from core import pg_managers_write

        _stand_in_chain3(every_precondition_met, None)
        unmet = pg_managers_write.unmet_precondition()
        assert unmet == ("chain3: the orders chain is not in this build, and "
                         "update_manager_stats reads the orders it would freeze")

    def test_a_check_that_raises_is_unmet_and_nothing_escapes(self, every_precondition_met):
        """Mutation: let a raise propagate — the registry would read the whole
        question as unreadable, and `/api/health` would publish nothing."""
        from core import pg_managers_write, warehouse_cutover

        env = every_precondition_met

        def boom():
            raise RuntimeError("cache unreadable")

        env.setattr(warehouse_cutover, "writes_postgres", boom)
        _stand_in_chain3(env, _Chain3(raises=OSError("nope")))
        unmet = pg_managers_write.unmet_precondition()
        assert "step13: could not be read (RuntimeError)" in unmet
        assert "chain3: could not be read (OSError)" in unmet

    def test_it_never_asks_postgres(self, flags):
        """/api/health asks it through the registry with Postgres down."""
        from unittest.mock import AsyncMock, patch

        from core import pg_managers_write

        with patch("core.pg.get_pool", new=AsyncMock(side_effect=AssertionError)):
            pg_managers_write.unmet_precondition()


class TestTheRealChain3:
    """Chain 3 is in the tree: what chain 5 asks is the registered module,
    and its answer is the registry's. The tests above stand chain 3 in;
    these do not."""

    @pytest.fixture
    def others_met(self, flags):
        from core import read_fallback, warehouse_cutover
        from core.repositories import goals

        flags.setattr(goals, "SALES_TYPE_BRIDGE_TABLES", frozenset({"bronze.orders"}))
        flags.setattr(warehouse_cutover, "_mode", "postgres")
        flags.setattr(read_fallback, "refusing", lambda: True)
        flags.delenv("KS_WRITE_ORDERS", raising=False)
        return flags

    def test_chain_5_asks_the_registered_chain_3(self):
        import importlib

        from core import pg_managers_write, pg_orders_write, write_chains

        assert importlib.import_module(pg_managers_write._CHAIN3) is pg_orders_write
        assert pg_orders_write in write_chains.WRITE_CHAINS

    def test_chain_3_on_duckdb_holds_chain_5_and_is_one_reason(self, others_met):
        """Mutation: a `; ` inside the reason — `preflight` splits on it, and
        published a lever as a precondition of its own."""
        from core import pg_managers_write

        unmet = pg_managers_write.unmet_precondition()
        assert unmet.startswith("chain3: ") and "KS_WRITE_ORDERS runs as duckdb" in unmet
        assert unmet.split("; ") == [unmet]
        others_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        assert pg_managers_write.mode() == "duckdb"

    def test_chain_3_latched_meets_it(self, others_met):
        from core import chain_latch, pg_managers_write, pg_orders_write

        chain_latch.latch(pg_orders_write.CHAIN)
        assert pg_orders_write.mode() == "postgres"
        assert pg_managers_write.unmet_precondition() is None

    @pytest.mark.asyncio
    async def test_the_preflight_names_each_precondition_once(self, flags, monkeypatch):
        """Every reason is one entry, with the real chain 3 answering."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=AssertionError("Postgres was asked")))
        out = await pg_managers_write.preflight()
        assert [r.split(":")[0] for r in out["reasons"]] == [
            "goals_bridge", "step13", "chain3", "read_fallback_off"]

    @pytest.mark.asyncio
    async def test_a_reason_with_a_semicolon_is_still_one(self, flags, monkeypatch):
        """Structurally, not by every reason's wording: chain 3's goal-bridge
        reason carried a "; " and its preflight published the lever after it
        as a precondition of its own (batch-E review). Mutation: split the
        joined `unmet_precondition()` on "; " again."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=AssertionError("Postgres was asked")))
        monkeypatch.setattr(pg_managers_write, "_CHECKS", (
            ("goals_bridge", lambda: "goals_bridge: the bridge reads it; port 7b first"),
            ("step13", lambda: "step13: not in force")))
        out = await pg_managers_write.preflight()
        assert out["reasons"] == ["goals_bridge: the bridge reads it; port 7b first",
                                  "step13: not in force"]


# ─── The writers, as code ───────────────────────────────────────────────────


def _fn_tree(fn):
    return ast.parse(textwrap.dedent(inspect.getsource(fn)))


def _transaction_bodies(tree):
    return [n for n in ast.walk(tree) if isinstance(n, ast.AsyncWith)
            and any(isinstance(i.context_expr, ast.Call)
                    and getattr(i.context_expr.func, "attr", "") == "transaction"
                    for i in n.items)]


def _strings(node):
    return [n.value for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)]


class TestTheUpsert:
    def test_is_retail_is_never_in_the_conflict_update(self):
        """Measured on 17.2 (design, M4): writing the seed in DO UPDATE puts it
        over every classification a human made. Mutation: add
        `is_retail = EXCLUDED.is_retail`."""
        import re

        from core import pg_managers_write

        (sql,) = [s for s in _strings(_fn_tree(pg_managers_write.upsert_managers))
                  if s.startswith("INSERT INTO bronze.managers")]
        head, _, update = sql.partition("DO UPDATE SET")
        assert "is_retail" in head, "the seed is inserted for a new manager"
        targets = re.findall(r"(\w+)\s*=\s*EXCLUDED\.\w+", update)
        assert targets == ["name", "email", "status", "mirrored_at"]

    def test_it_seeds_every_manager_without_an_interval_not_only_the_batch(self):
        """`_m0006`'s twin. Mutation: narrow the seed to the batch's ids — a
        manager the replica carried without a baseline would never get one."""
        from core import pg_managers_write

        (seed,) = [s for s in _strings(_fn_tree(pg_managers_write.upsert_managers))
                   if s.startswith("INSERT INTO app.manager_classifications")]
        assert "FROM bronze.managers m" in seed and "NOT EXISTS" in seed
        assert "$2" not in seed and "ANY(" not in seed
        assert "DATE '1970-01-01'" in seed


class TestTheDerivationSignal:
    def test_the_two_that_change_what_silver_reads_mark_inside_their_transaction(self):
        """Mutation: drop either mark."""
        from core import pg_managers_write

        for fn in (pg_managers_write.upsert_managers,
                   pg_managers_write.set_manager_retail_status):
            marks = [n for t in _transaction_bodies(_fn_tree(fn)) for n in ast.walk(t)
                     if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "mark_if_owned"]
            assert marks, fn.__name__

    def test_the_stats_do_not(self):
        """Silver reads none of the three stats columns, so the stats owe no
        rebuild — the one daily Postgres Silver rebuild today's replicate
        causes goes. Mutation: mark there without revisiting this: a Silver
        that starts reading the columns must come back here."""
        from core import pg_managers_write
        from core.duckdb_store import silver_select_sql

        calls = {getattr(n.func, "id", getattr(n.func, "attr", ""))
                 for n in ast.walk(_fn_tree(pg_managers_write.update_manager_stats))
                 if isinstance(n, ast.Call)}
        assert "mark_if_owned" not in calls and "mark" not in calls
        import re

        silver = silver_select_sql()
        for column in ("first_order_date", "last_order_date", "order_count"):
            # `buyer_first_order_date` is the buyer's, not the manager's.
            assert not re.search(rf"\b{column}\b", silver), column


class TestOneLockForAllThree:
    WRITERS = ("upsert_managers", "update_manager_stats", "set_manager_retail_status")

    @pytest.mark.parametrize("name", WRITERS)
    def test_the_advisory_lock_comes_before_the_claim_and_every_table(self, name):
        """M5: without it two classifications of one manager leave two open
        intervals. Mutation: drop the lock, or take it after a statement."""
        from core import pg_managers_write

        tree = _fn_tree(getattr(pg_managers_write, name))
        (tx,) = _transaction_bodies(tree)
        order = []
        for n in ast.walk(tx):
            if isinstance(n, ast.Call):
                what = getattr(n.func, "attr", getattr(n.func, "id", ""))
                text = " ".join(_strings(n))
                if "pg_advisory_xact_lock" in text:
                    order.append(("lock", n.lineno))
                elif what == "claim":
                    order.append(("claim", n.lineno))
                elif "bronze." in text or "app." in text:
                    order.append(("table", n.lineno))
        order.sort(key=lambda o: o[1])
        assert order and order[0][0] == "lock", order
        locks = [n for n in ast.walk(tx) if isinstance(n, ast.Call)
                 and any(isinstance(a, ast.Name) and a.id == "CHAIN_LOCK_KEY"
                         for a in n.args)]
        assert locks, f"{name} does not lock CHAIN_LOCK_KEY"

    def test_the_key_is_no_other_chains(self):
        """Mutation: reuse chain 1's key — the two chains would serialise
        against each other for nothing."""
        import pkgutil
        import importlib

        import core
        from core import pg_managers_write

        keys = {}
        for info in pkgutil.iter_modules(core.__path__):
            if not info.name.startswith("pg_"):
                continue
            module = importlib.import_module(f"core.{info.name}")
            for attr in dir(module):
                if attr.endswith("LOCK_KEY") and isinstance(getattr(module, attr), int):
                    keys[f"{info.name}.{attr}"] = getattr(module, attr)
        mine = keys.pop("pg_managers_write.CHAIN_LOCK_KEY")
        assert mine == pg_managers_write.CHAIN_LOCK_KEY
        assert mine not in keys.values(), keys


# ─── The writers, against a recording connection ────────────────────────────


class _Tx:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.calls.append(("BEGIN", None))
        return self

    async def __aexit__(self, exc_type, *exc):
        self.conn.calls.append(("ROLLBACK" if exc_type else "COMMIT", None))
        return False


class _PgConn:
    """What a writer asks of its connection, recorded; reads answer from
    `answers` by the first words of the statement."""

    def __init__(self, answers=None):
        self.calls = []
        self.answers = answers or {}

    def transaction(self, **kw):
        return _Tx(self)

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        return "UPDATE 3" if sql.lstrip().startswith("UPDATE") else "OK"

    async def executemany(self, sql, rows):
        self.calls.append((sql, list(rows)))

    def _answer(self, sql):
        for head, value in self.answers.items():
            if head in sql:
                return value
        return None

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return self._answer(sql)

    async def fetchrow(self, sql, *args, **kw):
        self.calls.append((sql, args))
        return self._answer(sql)


class _PgPool:
    def __init__(self, conn):
        self.conn = conn
        self.acquired = 0

    def acquire(self, *args, **kwargs):
        pool = self

        class _Ctx:
            async def __aenter__(self_inner):
                pool.acquired += 1
                return pool.conn

            async def __aexit__(self_inner, *exc):
                return False
        return _Ctx()


@pytest.fixture
def pool(monkeypatch):
    from unittest.mock import AsyncMock

    conn = _PgConn()
    fake = _PgPool(conn)
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=fake))
    monkeypatch.setattr("core.pg.require_revision", AsyncMock())
    return fake


def _row(mid, name="M", email=None, status="active", retail=False):
    return landing_rows.ManagerRow(mid, name, email, status, retail)


class TestWhatPostgresWouldRefuse:
    @pytest.mark.asyncio
    async def test_a_refused_manager_is_skipped_before_the_latch(self, pool):
        """Mutation: move the refusal after `_latch` — a first batch refused
        whole would latch the chain with no owner row behind it."""
        from core import chain_latch, pg_managers_write

        rows = [_row(1), _row(2, name="a\x00b"), _row(2 ** 31), _row(3, email="\ud800")]
        written = await pg_managers_write.upsert_managers(rows, set_at=T0)
        assert written == 1
        (inserted,) = [args for sql, args in pool.conn.calls
                       if sql.startswith("INSERT INTO bronze.managers")]
        assert [r[0] for r in inserted] == [1]
        assert chain_latch.latched(pg_managers_write.CHAIN)

    @pytest.mark.asyncio
    async def test_a_batch_of_nothing_but_refusals_raises_and_latches_nothing(self, pool):
        from core import chain_latch, pg_managers_write

        with pytest.raises(pg_managers_write.ManagersRefused):
            await pg_managers_write.upsert_managers(
                [_row(2 ** 31), _row(4, name=None)], set_at=T0)
        assert pool.acquired == 0
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_an_empty_batch_asks_nothing_and_latches_nothing(self, pool):
        from core import chain_latch, pg_managers_write

        assert await pg_managers_write.upsert_managers([], set_at=T0) == 0
        assert pool.acquired == 0
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_a_manager_served_twice_is_written_once_in_id_order(self, pool):
        from core import pg_managers_write

        await pg_managers_write.upsert_managers(
            [_row(9, name="old"), _row(1), _row(9, name="new")], set_at=T0)
        (inserted,) = [args for sql, args in pool.conn.calls
                       if sql.startswith("INSERT INTO bronze.managers")]
        assert [(r[0], r[1]) for r in inserted] == [(1, "M"), (9, "new")]

    @pytest.mark.asyncio
    async def test_the_copy_back_clock_is_the_callers(self, pool):
        """`set_at` is the web container's clock, never Postgres's `now()`."""
        from core import pg_managers_write

        await pg_managers_write.upsert_managers([_row(1)], set_at=T0)
        (seed,) = [args for sql, args in pool.conn.calls
                   if sql.startswith("INSERT INTO app.manager_classifications")]
        assert seed == (T0,)
        with pytest.raises(ValueError, match="set_at"):
            await pg_managers_write.upsert_managers([_row(1)], set_at=None)


class TestAClassificationThatCannotLand:
    @pytest.mark.asyncio
    async def test_a_backdate_behind_the_latest_change_is_refused_before_the_latch(self, pool):
        """OD-C5-1 (a). DuckDB writes two open intervals here (design, M1).
        Mutation: drop the guard."""
        from core import chain_latch, pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": True, "latest": date(2026, 3, 1)}}
        with pytest.raises(pg_managers_write.BackdateBehindLatest) as raised:
            await pg_managers_write.set_manager_retail_status(
                7, True, date(2026, 1, 1), 1, None, set_at=T0)
        assert raised.value.latest == date(2026, 3, 1)
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()
        assert not [c for c in pool.conn.calls if c[0] == "BEGIN"]

    @pytest.mark.asyncio
    async def test_the_same_date_is_a_replacement_not_a_backdate(self, pool):
        from core import pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": True, "latest": date(2026, 3, 1)},
                             "SELECT MAX(valid_from)": date(2026, 3, 1)}
        await pg_managers_write.set_manager_retail_status(
            7, True, date(2026, 3, 1), 1, "same day", set_at=T0)
        statements = [sql for sql, _a in pool.conn.calls]
        assert any(s.startswith("DELETE FROM app.manager_classifications") for s in statements)
        assert ("COMMIT", None) in pool.conn.calls

    @pytest.mark.asyncio
    async def test_a_manager_postgres_does_not_hold_is_refused_before_the_latch(self, pool):
        from core import chain_latch, pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": False, "latest": None}}
        with pytest.raises(pg_managers_write.ManagerNotFound):
            await pg_managers_write.set_manager_retail_status(
                99, True, date(2026, 5, 1), 1, None, set_at=T0)
        assert not chain_latch.marker_path(pg_managers_write.CHAIN).exists()

    @pytest.mark.asyncio
    async def test_a_race_inside_the_lock_rolls_the_whole_write_back(self, pool):
        """A classification that landed between the read and the lock: the
        re-check inside raises, and the transaction rolls back."""
        from core import pg_managers_write

        pool.conn.answers = {"EXISTS": {"present": True, "latest": date(2026, 1, 1)},
                             "SELECT MAX(valid_from)": date(2026, 6, 1)}
        with pytest.raises(pg_managers_write.BackdateBehindLatest):
            await pg_managers_write.set_manager_retail_status(
                7, True, date(2026, 5, 1), 1, None, set_at=T0)
        assert ("ROLLBACK", None) in pool.conn.calls
        assert not [sql for sql, _a in pool.conn.calls
                    if sql.startswith(("DELETE", "UPDATE app", "UPDATE bronze"))]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("args", [
        (2 ** 31, True, date(2026, 5, 1), 1, None),
        (7, "yes", date(2026, 5, 1), 1, None),
        (7, True, None, 1, None),
        (7, True, date(2026, 5, 1), 1, "a\x00b"),
    ])
    async def test_what_postgres_would_refuse_is_refused_first(self, pool, args):
        """As `ClassificationRefused`, which the route answers 422: a bad
        request, not a Postgres that did not answer."""
        from core import pg_managers_write

        with pytest.raises(pg_managers_write.ClassificationRefused):
            await pg_managers_write.set_manager_retail_status(*args, set_at=T0)
        assert pool.acquired == 0


# ─── DuckDB stands down where the chain took over ───────────────────────────


class TestTheBootSeedStandsDownOnTheLatch:
    @pytest.mark.asyncio
    async def test_a_latched_chain_leaves_duckdbs_intervals_alone(self, tmp_path):
        """H3: a baseline seeded in DuckDB after the latch is a key only
        DuckDB holds, which the copy-back refuses on. Mutation: drop the guard."""
        from unittest.mock import AsyncMock, patch

        from core import chain_latch
        from core.duckdb_store import DuckDBStore

        path = tmp_path / "seed.duckdb"
        store = DuckDBStore(db_path=path)
        await store.connect()
        with patch("core.pg_replication.replicate_managers", new=AsyncMock()):
            await store.upsert_managers([{"id": 1, "name": "A"}, {"id": 4, "name": "B"}])
        await store.close()

        chain_latch.latch("pg_managers_write")
        store = DuckDBStore(db_path=path)
        await store.connect()                 # _m0006 runs here
        async with store.connection() as conn:
            latched = conn.execute(
                "SELECT COUNT(*) FROM manager_classifications").fetchone()[0]
        await store.close()
        assert latched == 0

        chain_latch.release("pg_managers_write")
        store = DuckDBStore(db_path=path)
        await store.connect()
        async with store.connection() as conn:
            seeded = conn.execute(
                "SELECT manager_id, is_retail FROM manager_classifications "
                "ORDER BY manager_id").fetchall()
        await store.close()
        assert seeded == [(1, False), (4, True)]


class TestTheStoreRoutes:
    @pytest.mark.asyncio
    async def test_get_all_managers_reads_postgres_under_the_chain_and_never_duckdb(self, flags):
        """No fallback: DuckDB's managers are frozen after the flip.
        Mutation: drop the routing in `get_all_managers`."""
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore

        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        rows = [{"id": 1, "name": "M", "synced_at": T0}]
        with patch("core.pg_managers_read.fetch_all_managers",
                   new=AsyncMock(return_value=rows)) as read:
            assert await store.get_all_managers() == rows
        read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_classification_marks_whichever_engine_derives(self, flags):
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore

        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        store.mark_warehouse_dirty = AsyncMock()
        with patch.object(pg_managers_write, "set_manager_retail_status",
                          new=AsyncMock()) as write:
            await store.set_manager_retail_status(7, True, date(2026, 5, 1), 3, "n")
        write.assert_awaited_once()
        assert write.await_args.args == (7, True, date(2026, 5, 1), 3, "n")
        assert write.await_args.kwargs["set_at"].tzinfo is not None
        store.mark_warehouse_dirty.assert_awaited_once_with(None)

    @pytest.mark.asyncio
    async def test_the_stats_and_the_upsert_route_too(self, flags):
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore

        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        with patch.object(pg_managers_write, "update_manager_stats",
                          new=AsyncMock(return_value=4)), \
                patch.object(pg_managers_write, "upsert_managers",
                             new=AsyncMock(return_value=2)) as upsert, \
                patch("core.pg_replication.replicate_managers",
                      new=AsyncMock(side_effect=AssertionError("no replica"))):
            assert await store.update_manager_stats() == 4
            assert await store.upsert_managers(
                [{"id": 1, "name": "A"}, {"id": 2, "full_name": "B"}]) == 2
        rows = upsert.await_args.args[0]
        assert [tuple(r) for r in rows] == [(1, "A", None, None, False),
                                           (2, "B", None, None, False)]


# ─── The sync step: contained under the chain, untouched without it ────────


def _tick(monkeypatch, *, upsert=None, managers_due=True):
    """The REAL `SyncService.incremental_sync`, with the store and KeyCRM
    faked around it (tests/unit/test_buyers_step_containment.py's shape)."""
    from datetime import timedelta
    from unittest.mock import AsyncMock, MagicMock
    from zoneinfo import ZoneInfo

    from bot.config import DEFAULT_TIMEZONE
    from core import sync_service as mod
    from core.sync_service import SyncService

    now = datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
    recent, stale = now - timedelta(minutes=1), now - timedelta(days=2)
    due = {"orders": recent, "products": recent, "buyers": recent,
           "managers": stale if managers_due else recent}

    store = MagicMock()
    store.get_last_sync_time = AsyncMock(side_effect=lambda k: due.get(k, stale))
    store.set_last_sync_time = AsyncMock()
    store.upsert_managers = upsert or AsyncMock(return_value=3)
    store.update_manager_stats = AsyncMock(return_value=3)
    store.mark_warehouse_dirty = AsyncMock()
    store.refresh_sku_inventory_status = AsyncMock(return_value=0)
    store.record_sku_inventory_snapshot = AsyncMock()
    store.record_inventory_snapshot = AsyncMock()

    async def users(*a, **kw):
        yield [{"id": 1, "name": "M"}]

    client = MagicMock()
    client.paginate = users
    monkeypatch.setattr(mod, "get_async_client", AsyncMock(return_value=client))

    svc = SyncService(store=store)
    svc._should_skip_sync = lambda: (False, None)
    svc._fetch_orders_with_date_filter = AsyncMock(return_value=[])
    svc.sync_offers = AsyncMock(return_value=5)
    svc.sync_stocks = AsyncMock(return_value=7)
    return svc, store


def _stamped(store, key="managers") -> bool:
    return any(c.args[:1] == (key,) for c in store.set_last_sync_time.await_args_list)


class TestTheManagersStep:
    @pytest.mark.asyncio
    async def test_under_the_chain_a_failure_is_contained_and_held(
            self, monkeypatch, every_precondition_met):
        """Mutation: remove the containment — the raise skipped the buyers,
        offers and stocks steps for the tick — or stamp on failure."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch,
                           upsert=AsyncMock(side_effect=OSError("postgres gone")))

        stats = await svc.incremental_sync()       # does not raise

        svc.sync_offers.assert_awaited_once()
        svc.sync_stocks.assert_awaited_once()
        assert not _stamped(store), "a failed step moved its watermark"
        assert stats["managers"] == 0
        assert all(isinstance(v, int) for v in stats.values()), stats
        health = svc.managers_step_health()
        assert (health["failures_since_ok"], health["last_error"]) == (1, "OSError")
        assert 590 <= health["retry_in_s"] <= 601

        # Inside the retry window nothing is asked of anyone.
        store.get_last_sync_time.reset_mock()
        await svc.incremental_sync()
        assert "managers" not in [c.args[0] for c in store.get_last_sync_time.await_args_list]
        assert store.upsert_managers.await_count == 1

    @pytest.mark.asyncio
    async def test_a_watermark_that_cannot_be_read_is_contained_too(
            self, monkeypatch, every_precondition_met):
        """Under the chain the getter reads `meta.chain_watermarks`."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch)
        others = store.get_last_sync_time.side_effect

        async def getter(key):
            if key == "managers":
                raise ConnectionError("pool gone")
            return others(key)

        store.get_last_sync_time = AsyncMock(side_effect=getter)
        await svc.incremental_sync()
        svc.sync_offers.assert_awaited()
        assert svc.managers_step_health()["last_error"] == "ConnectionError"

    @pytest.mark.asyncio
    async def test_a_typo_is_contained_without_asking_keycrm(self, monkeypatch, flags):
        """The chain's own getter raises on its typo (DN-01); the tick goes on."""
        from core import pg_managers_write

        flags.setenv(pg_managers_write.WRITE_ENV, "postgress")
        svc, store = _tick(monkeypatch)
        others = store.get_last_sync_time.side_effect

        async def getter(key):
            if key == "managers":
                pg_managers_write.writes_postgres()      # raises, as the store's does
            return others(key)

        from unittest.mock import AsyncMock

        store.get_last_sync_time = AsyncMock(side_effect=getter)
        await svc.incremental_sync()
        store.upsert_managers.assert_not_awaited()
        svc.sync_offers.assert_awaited()
        assert svc.managers_step_health()["last_error"] == "RuntimeError"

    @pytest.mark.asyncio
    async def test_a_success_under_the_chain_stamps_and_is_published(
            self, monkeypatch, every_precondition_met):
        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch)
        stats = await svc.incremental_sync()
        assert stats["managers"] == 3 and _stamped(store)
        health = svc.managers_step_health()
        assert health["failures_since_ok"] == 0 and health["last_ok_at"]

    @pytest.mark.asyncio
    async def test_the_full_sync_is_not_taken_down_either(
            self, monkeypatch, every_precondition_met):
        """`full_sync` calls `sync_managers` first and directly, not through
        the tick's step: one classification store must not cost a week's
        orders. Mutation: let `sync_managers` re-raise under the chain."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        every_precondition_met.setenv(pg_managers_write.WRITE_ENV, "postgres")
        svc, store = _tick(monkeypatch,
                           upsert=AsyncMock(side_effect=OSError("postgres gone")))
        assert await svc.sync_managers() == 0
        assert not _stamped(store)
        assert svc.managers_step_health()["last_error"] == "OSError"

    @pytest.mark.asyncio
    async def test_on_duckdb_a_failure_escapes_exactly_as_before(self, monkeypatch, flags):
        """Mutation: apply the containment in duckdb mode — production's
        behaviour would change with the flag off."""
        from unittest.mock import AsyncMock

        svc, store = _tick(monkeypatch,
                           upsert=AsyncMock(side_effect=RuntimeError("duckdb gone")))
        with pytest.raises(RuntimeError, match="duckdb gone"):
            await svc.sync_managers()
        assert svc.managers_step_health()["failures_since_ok"] == 0
        assert not _stamped(store)


# ─── Every read of the two tables follows the chain ─────────────────────────


def _read_walk():
    """`(routers that ask, [(file, line, call) a hole statement is handed to],
    [stray (file, line)])` over `core/` and `web/`.

    A statement is a string constant carrying one of the chain's holes, not in
    an f-string (the Silver CASE renders its names, it is not routed) and not a
    write: the stats UPDATE is the writer's own body. Chain 7a's walk
    (`test_goals_reads_follow_chain.py`) in miniature.
    """
    import pathlib

    from core import pg_managers_write

    repo = pathlib.Path(__file__).resolve().parents[2]
    holes = pg_managers_write.TABLE_HOLES
    trees = []
    for top in ("core", "web"):
        for path in sorted((repo / top).rglob("*.py")):
            rel = str(path.relative_to(repo))
            trees.append((rel, ast.parse(path.read_text(encoding="utf-8"))))

    def own_calls(fn):
        out, stack = set(), list(ast.iter_child_nodes(fn))
        while stack:
            node = stack.pop()
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                continue
            if isinstance(node, ast.Call):
                out.add(getattr(node.func, "attr", getattr(node.func, "id", "")))
            stack.extend(ast.iter_child_nodes(node))
        return out

    routers = {fn.name for _r, tree in trees for fn in ast.walk(tree)
               if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
               and "reads_the_chain" in own_calls(fn)}

    def is_statement(node):
        return (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and any(h in node.value for h in holes)
                and not node.value.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")))

    sites, stray = [], []
    for rel, tree in trees:
        in_fstrings = {id(v) for n in ast.walk(tree) if isinstance(n, ast.JoinedStr)
                       for v in ast.walk(n)}
        docs = {id(n.body[0].value) for n in ast.walk(tree)
                if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                  ast.AsyncFunctionDef))
                and n.body and isinstance(n.body[0], ast.Expr)
                and isinstance(n.body[0].value, ast.Constant)}
        constants = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and is_statement(node.value):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        constants[t.id] = node.value
        literals = [n for n in ast.walk(tree) if is_statement(n)
                    and id(n) not in in_fstrings and id(n) not in docs
                    and not (rel == "core/pg_managers_write.py")]
        handed = set()
        literal_ids = {id(lit) for lit in literals}
        for call in [n for n in ast.walk(tree) if isinstance(n, ast.Call)]:
            for arg in list(call.args) + [k.value for k in call.keywords]:
                # The statement as it is handed over: a literal, a module
                # constant, or either through a string method
                # (`_RETURN_ORDERS_SQL.replace(...)`).
                names = [n.id for n in ast.walk(arg)
                         if isinstance(n, ast.Name) and n.id in constants]
                if names or any(id(n) in literal_ids for n in ast.walk(arg)):
                    sites.append((rel, call.lineno,
                                  getattr(call.func, "attr", getattr(call.func, "id", ""))))
                    handed |= {id(n) for n in ast.walk(arg)}
                    handed |= {id(constants[name]) for name in names}
        for lit in literals:
            if id(lit) not in handed:
                stray.append((rel, lit.lineno))
    return routers, sites, stray


class TestEveryReadOfTheTablesFollowsTheChain:
    def test_the_walk_finds_the_reads_there_are(self):
        """Non-vacuity: the dashboard's returns list joins `{managers}`."""
        routers, sites, _stray = _read_walk()
        assert "_dashboard_run" in routers
        assert ("core/repositories/revenue.py", "_dashboard_run") in {
            (rel, name) for rel, _line, name in sites}

    def test_each_is_handed_to_a_router_that_asks(self):
        """Mutation: drop `reads_the_chain` from `_dashboard_run` — after the
        flip that join would name managers out of a DuckDB nothing writes."""
        routers, sites, _stray = _read_walk()
        wrong = [s for s in sites if s[2] not in routers]
        assert not wrong, wrong

    def test_none_is_left_where_the_walk_cannot_follow(self):
        _routers, _sites, stray = _read_walk()
        assert not stray, stray

    @pytest.mark.asyncio
    async def test_the_dashboard_join_reads_postgres_under_the_chain_without_a_fallback(
            self, flags):
        """`_RETURN_ORDERS_SQL` under the chain, with `KS_READ_DASHBOARD` at
        duckdb: Postgres, and a Postgres failure raises."""
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write
        from core.duckdb_store import DuckDBStore
        from core.repositories.revenue import _RETURN_ORDERS_SQL

        flags.setenv("KS_READ_DASHBOARD", "duckdb")
        chain_latch.latch(pg_managers_write.CHAIN)
        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        with patch("core.pg_dashboard_read.fetch",
                   new=AsyncMock(return_value=[(1,)])) as fetch:
            assert await store._dashboard_run(_RETURN_ORDERS_SQL.replace(
                "{where}", "TRUE"), [5]) == [(1,)]
        assert "bronze.managers" in fetch.await_args.args[0]
        with patch("core.pg_dashboard_read.fetch",
                   new=AsyncMock(side_effect=OSError("down"))):
            with pytest.raises(OSError):
                await store._dashboard_run("SELECT name FROM {managers}", [])


# ─── The route ──────────────────────────────────────────────────────────────


class _RouteStore:
    def __init__(self, raises=None):
        self.raises = raises
        self.set_calls = []

    async def get_all_managers(self):
        return [{"id": 34, "name": "M", "is_retail": False}]

    async def set_manager_retail_status(self, manager_id, is_retail, **kw):
        self.set_calls.append((manager_id, is_retail, kw))
        if self.raises:
            raise self.raises


@pytest.fixture
def route(monkeypatch, flags):
    import time as _time

    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.routes.api._deps import limiter
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    limiter.reset()
    admin = sorted(ADMIN_USER_IDS)[0]
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": str(admin), "first_name": "T", "last_name": "U", "username": "t",
         "auth_date": str(int(_time.time()))}, role="admin")))

    def with_store(store):
        async def get():
            return store
        monkeypatch.setattr("web.routes.api.admin.get_store", get)
        return client
    yield with_store
    limiter.reset()


class TestTheRoute:
    PATH = "/api/managers/34/retail-status?is_retail=true"

    def test_a_typo_is_409_and_nothing_is_written(self, route, flags):
        """Mutation: let it through — it would be a 500, or written to DuckDB."""
        from core import pg_managers_write

        flags.setenv(pg_managers_write.WRITE_ENV, "postgress")
        store = _RouteStore()
        res = route(store).post(self.PATH)
        assert res.status_code == 409 and pg_managers_write.WRITE_ENV in res.json()["detail"]
        assert store.set_calls == []

    def test_a_backdate_is_409_naming_the_latest_change(self, route, flags):
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(pg_managers_write.BackdateBehindLatest(
            34, date(2026, 1, 1), date(2026, 3, 1)))
        res = route(store).post(self.PATH + "&effective_from=2026-01-01")
        assert res.status_code == 409 and "2026-03-01" in res.json()["detail"]

    def test_a_postgres_failure_under_the_chain_is_503(self, route, flags):
        from unittest.mock import AsyncMock, patch

        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(OSError("connection refused"))
        with patch("core.pg_replication.replicate_managers", new=AsyncMock()):
            res = route(store).post(self.PATH)
        assert res.status_code == 503 and "OSError" in res.json()["detail"]
        assert "connection refused" not in res.text

    def test_on_duckdb_a_failure_is_what_it_always_was(self, route, flags):
        store = _RouteStore(RuntimeError("duckdb broke"))
        with pytest.raises(RuntimeError, match="duckdb broke"):
            route(store).post(self.PATH)

    def test_under_the_chain_the_replica_says_it_stood_down(self, route, flags, monkeypatch):
        from core import chain_latch, pg_managers_write

        monkeypatch.setenv("KS_MIRROR_LANDING", "on")
        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore()
        res = route(store).post(self.PATH)
        assert res.status_code == 200, res.text
        assert "stood down" in (res.json()["replica"]["skipped"] or "")


# ─── The standing watch, judged ─────────────────────────────────────────────


def _facts(**managers):
    from core import pg_chain_invariants as inv

    base = dict(nulls=inv.Nulls(table="app.manager_classifications",
                                counts={"set_at": 0}),
                managers=40, intervals=44)
    base.update(managers)
    return inv.Facts(watched=("pg_managers_write",), now=T0,
                     managers=inv.Managers(**base))


class TestTheStandingWatch:
    def _checks(self, facts):
        from core import pg_chain_invariants as inv

        return {(i.check_name, i.severity.value): i
                for i in inv.check_chain_invariants(facts)}

    def test_a_clean_shape_is_quiet(self):
        assert self._checks(_facts()) == {}

    @pytest.mark.parametrize("field,check,severity", [
        ("open_wrong", "chain_manager_open_interval", "CRITICAL"),
        ("broken", "chain_manager_intervals_broken", "WARN"),
        ("disagree", "chain_manager_retail_disagrees", "WARN"),
    ])
    def test_each_shape_is_its_own_finding(self, field, check, severity):
        """Mutation: remove or invert any predicate."""
        found = self._checks(_facts(**{field: (4, 7)}))
        assert set(found) == {(check, severity)}
        assert found[(check, severity)].sample_ids == (4, 7)

    def test_an_empty_table_is_critical(self):
        assert ("chain_managers_empty", "CRITICAL") in self._checks(_facts(managers=0))

    def test_unclassified_is_judged_only_from_the_latch(self):
        """Before it the replica carries such managers until DuckDB's next
        boot (M2); after it the writer seeds in the same transaction."""
        assert self._checks(_facts(unclassified=(5,))) == {}
        found = self._checks(_facts(unclassified=(5,), latched_at=T0))
        assert set(found) == {("chain_manager_unclassified", "WARN")}

    def test_a_missing_set_at_is_the_required_column(self):
        from core import pg_chain_invariants as inv

        found = self._checks(_facts(nulls=inv.Nulls(
            table="app.manager_classifications", counts={"set_at": 2})))
        assert ("chain_required_column_null", "CRITICAL") in found

    def test_a_group_that_could_not_be_read_is_unwatched_and_holds(self):
        from core import pg_chain_invariants as inv

        facts = inv.Facts(watched=("pg_managers_write",), now=T0,
                          managers=inv.Unwatched("ConnectionError: gone"))
        issues = inv.check_chain_invariants(facts)
        assert [i.check_name for i in issues] == [inv.UNWATCHED]
        assert set(inv.unverified_conditions(issues)) >= {
            inv.MANAGERS_EMPTY, inv.MANAGER_OPEN_INTERVAL, inv.MANAGER_UNCLASSIFIED,
            inv.MANAGER_INTERVALS_BROKEN, inv.MANAGER_RETAIL_DISAGREES}

    def test_every_condition_is_in_the_registry_and_has_a_lever(self):
        from core import pg_chain_invariants as inv
        from core.alerting import REGISTRY
        from core.data_quality import REMEDIATION

        levers = {prefix for prefix, _line in REMEDIATION}
        for name in (inv.MANAGERS_EMPTY, inv.MANAGER_OPEN_INTERVAL,
                     inv.MANAGER_UNCLASSIFIED, inv.MANAGER_INTERVALS_BROKEN,
                     inv.MANAGER_RETAIL_DISAGREES):
            assert name in REGISTRY and name in levers and name.startswith(inv.PREFIX)


# ─── The preflight and /api/health ──────────────────────────────────────────


_CLEAN_SHAPE = {"managers": 40, "intervals": 44, "unclassified": [], "open_wrong": [],
                "broken": [], "disagree": [], "set_at_null": 0, "orphans": []}


class _PreflightConn(_PgConn):
    def __init__(self, marks, shape):
        super().__init__()
        self.marks, self.shape = marks, shape

    async def fetch(self, sql, *args, **kw):
        self.calls.append((sql, args))
        return self.marks

    async def fetchrow(self, sql, *args, **kw):
        self.calls.append((sql, args))
        return self.shape


def _fresh(table):
    return {"table_name": table, "failures_since_ok": 0, "age_s": 3600.0}


class TestThePreflight:
    @pytest.mark.asyncio
    async def test_today_it_names_the_preconditions(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        conn = _PreflightConn([_fresh(t) for t in pg_managers_write.CHAIN_TABLES],
                              dict(_CLEAN_SHAPE))
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_PgPool(conn)))
        monkeypatch.setattr("core.pg.require_revision", AsyncMock())
        out = await pg_managers_write.preflight()
        assert out["ok"] is False
        assert [r.split(":")[0] for r in out["reasons"]] == [
            "goals_bridge", "step13", "chain3", "read_fallback_off"]
        # It only reads.
        assert not [c for c in conn.calls if c[0] in ("BEGIN", "COMMIT")]

    @pytest.mark.asyncio
    async def test_met_fresh_and_clean_is_ok(self, every_precondition_met, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        conn = _PreflightConn([_fresh(t) for t in pg_managers_write.CHAIN_TABLES],
                              dict(_CLEAN_SHAPE))
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_PgPool(conn)))
        monkeypatch.setattr("core.pg.require_revision", AsyncMock())
        assert await pg_managers_write.preflight() == {"ok": True, "reasons": []}

    @pytest.mark.asyncio
    async def test_a_stale_failing_copy_and_a_bad_shape_are_each_named(
            self, every_precondition_met, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        marks = [{"table_name": "bronze.managers", "failures_since_ok": 2, "age_s": 60.0},
                 {"table_name": "app.manager_classifications", "failures_since_ok": 0,
                  "age_s": 30 * 3600.0}]
        shape = dict(_CLEAN_SHAPE, unclassified=[5], open_wrong=[4, 7], broken=[2],
                     disagree=[6], set_at_null=1)
        conn = _PreflightConn(marks, shape)
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_PgPool(conn)))
        monkeypatch.setattr("core.pg.require_revision", AsyncMock())
        reasons = (await pg_managers_write.preflight())["reasons"]
        text = "\n".join(reasons)
        assert "bronze.managers is failing (2 in a row)" in text
        assert "app.manager_classifications was last copied 30 h ago" in text
        for fragment in ("no interval", "exactly one open", "overlap or leave a gap",
                         "disagrees", "no set_at"):
            assert fragment in text, fragment

    @pytest.mark.asyncio
    async def test_a_postgres_error_is_named_by_class_only(self, every_precondition_met,
                                                           monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        monkeypatch.setattr("core.pg.get_pool", AsyncMock(
            side_effect=OSError("password authentication failed for user x")))
        out = await pg_managers_write.preflight()
        assert out == {"ok": False, "reasons": ["postgres: OSError"]}

    @pytest.mark.asyncio
    async def test_the_question_is_over_once_the_chain_writes_postgres(self, flags):
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        assert await pg_managers_write.preflight() == {"ok": None, "reasons": []}


class TestHealthPublishesIt:
    @pytest.mark.asyncio
    async def test_preflight_and_sync_step_sit_under_chain_5s_entry(self, flags, monkeypatch):
        from unittest.mock import AsyncMock

        from core import pg_inventory_write, pg_managers_write
        from web.routes.api import health

        health._managers_preflight_cache.update(data=None, expires_at=0)
        health._preflight_cache.update(data=None, expires_at=0)
        monkeypatch.setattr(pg_managers_write, "preflight", AsyncMock(
            return_value={"ok": False, "reasons": ["step13: x"]}))
        monkeypatch.setattr(pg_inventory_write, "preflight", AsyncMock(
            return_value={"ok": True, "reasons": []}))
        monkeypatch.setattr(health, "_managers_sync_step", AsyncMock(
            return_value={"failures_since_ok": 0}))
        try:
            block = await health._write_chains_block()
        finally:
            health._managers_preflight_cache.update(data=None, expires_at=0)
            health._preflight_cache.update(data=None, expires_at=0)
        entry = block["pg_managers_write"]
        assert entry["preflight"] == {"ok": False, "reasons": ["step13: x"]}
        assert entry["sync_step"] == {"failures_since_ok": 0}
        assert entry["mode"] == "duckdb" and entry["unmet_precondition"] is None
        assert block["pg_inventory_write"]["preflight"]["ok"] is True
        assert "preflight" not in block["pg_goals_write"]

    @pytest.mark.asyncio
    async def test_the_two_preflights_cost_one_bound_not_two(self, flags, monkeypatch):
        """Concurrently: two hung preflights in a row would cost the canary's
        whole HEALTH_TIMEOUT_S."""
        import asyncio
        import time as _time

        from core import pg_inventory_write, pg_managers_write
        from web.routes.api import health

        async def hangs(*a, **kw):
            await asyncio.sleep(60)

        health._managers_preflight_cache.update(data=None, expires_at=0)
        health._preflight_cache.update(data=None, expires_at=0)
        monkeypatch.setattr(pg_managers_write, "preflight", hangs)
        monkeypatch.setattr(pg_inventory_write, "preflight", hangs)
        monkeypatch.setattr(health, "_PREFLIGHT_TIMEOUT_S", 0.3)
        started = _time.monotonic()
        try:
            block = await health._write_chains_block()
        finally:
            health._managers_preflight_cache.update(data=None, expires_at=0)
            health._preflight_cache.update(data=None, expires_at=0)
        assert _time.monotonic() - started < 0.55
        assert "did not answer" in block["pg_managers_write"]["preflight"]["reasons"][0]


# ─── The review of chain 5 (2026-10-08) ─────────────────────────────────────


class TestStep13sWayBackIsRefusedWhileTheChainIsLatched:
    """H1. After a flip any start that would run as duckdb is step 13's way
    back — an operator's unset, a typo, or a precondition unmet at the start —
    and the full DuckDB rebuild it owes would derive `sales_type` from the
    classification this chain froze at its latch. Refused: the start stays
    postgres (`warehouse_cutover`, THE WAY BACK, REFUSED)."""

    @pytest.fixture
    def cutover(self, flags):
        from core import warehouse_cutover as wc

        # A start that read nothing of Postgres: `pg_revision` and the rest
        # unmet whatever the environment says.
        flags.setattr(wc, "_gather_facts_blocking",
                      lambda env: wc.Facts(required_revision="x"))
        flags.delenv(wc.ENV, raising=False)
        return flags

    def test_a_start_with_a_precondition_unmet_stays_postgres(self, cutover):
        """The review's reproduction. Mutation: drop the refusal from the
        unmet branch of `configure_mode` — DuckDB derives again."""
        from core import chain_latch, pg_managers_write
        from core import warehouse_cutover as wc

        chain_latch.latch(pg_managers_write.CHAIN)
        cutover.setenv(wc.ENV, "postgres")
        cutover.delenv("KS_CH_URL", raising=False)
        assert wc.configure_mode() == "postgres"
        assert not wc.duckdb_derives()
        assert wc.way_back_refused() == ("pg_managers_write",)
        # What is unmet is still published, beside the refusal.
        assert "ch_url" in {u.key for u in wc.preconditions_unmet()}
        assert "step13" not in (pg_managers_write.unmet_precondition() or "")

    @pytest.mark.parametrize("value", [None, "duckdb", "postgrse"])
    def test_an_operators_way_back_is_refused_too(self, cutover, value):
        """Unset, `duckdb`, or a value this build does not understand.
        Mutation: drop the refusal from the `value != postgres` branch."""
        from core import chain_latch, pg_managers_write
        from core import warehouse_cutover as wc

        chain_latch.latch(pg_managers_write.CHAIN)
        if value is not None:
            cutover.setenv(wc.ENV, value)
        assert wc.configure_mode() == "postgres"
        assert not wc.duckdb_derives()
        assert wc.way_back_refused() == ("pg_managers_write",)
        if value == "postgrse":
            assert "running as 'postgres'" in wc.mode_error()

    @pytest.mark.parametrize("latched", [
        ("pg_orders_write",), ("pg_managers_write", "pg_orders_write")])
    @pytest.mark.parametrize("value", [None, "duckdb", "postgres"])
    def test_chain_3_refuses_it_too_and_both_are_named(self, cutover, latched, value):
        """Chain 3 in the tree: DuckDB derives Silver from the orders, so its
        latch refuses the way back exactly as chain 5's does, alone or beside
        it, and the refusal names every chain holding a source. Mutation: drop
        the order tables from `DUCKDB_DERIVATION_SOURCES`, or stop at the
        first latched chain."""
        from core import chain_latch
        from core import warehouse_cutover as wc

        for name in latched:
            chain_latch.latch(name)
        if value is not None:
            cutover.setenv(wc.ENV, value)
        assert wc.configure_mode() == "postgres"
        assert not wc.duckdb_derives()
        assert wc.way_back_refused() == latched

    @pytest.mark.parametrize("value", [None, "duckdb", "postgres", "postgrse"])
    def test_with_nothing_latched_nothing_moves(self, cutover, value):
        """Production today: nothing latched, so every value runs as it did —
        `postgres` with a precondition unmet included."""
        from core import warehouse_cutover as wc

        if value is not None:
            cutover.setenv(wc.ENV, value)
        assert wc.configure_mode() == "duckdb"
        assert wc.duckdb_derives()
        assert wc.way_back_refused() == ()
        if value == "postgrse":
            assert "running as 'duckdb'" in wc.mode_error()

    def test_only_a_chain_over_what_duckdb_derives_from_refuses_it(self, cutover):
        """Chain 4 latches without step 13, and DuckDB's Silver reads none of
        the buyers. Mutation: refuse on any latched chain, or on every
        `pg_derivation.SOURCE_TABLES` table (`bronze.buyers` is one)."""
        from core import chain_latch, write_chains
        from core import warehouse_cutover as wc

        others = [c for c in write_chains.WRITE_CHAINS
                  if not set(c.CHAIN_TABLES) & set(wc.DUCKDB_DERIVATION_SOURCES)]
        assert "pg_buyers_write" in {write_chains.chain_name(c) for c in others}
        for chain in others:
            chain_latch.latch(write_chains.chain_name(chain))
        assert wc.configure_mode() == "duckdb"
        assert wc.way_back_refused() == ()

    def test_every_chain_over_those_tables_holds_itself_behind_step_13(self, flags):
        """The refusal reads a latch as "step 13 was flipped". That holds only
        of a chain that cannot latch while DuckDB derives — chain 5 and chain
        3, both walked here. Mutation: drop `step13` from chain 5's checks, or
        from chain 3's."""
        from core import warehouse_cutover as wc
        from core import write_chains

        flags.setattr(wc, "_mode", "duckdb")
        declaring = [c for c in write_chains.WRITE_CHAINS
                     if set(c.CHAIN_TABLES) & set(wc.DUCKDB_DERIVATION_SOURCES)]
        assert declaring, "nothing declares a table DuckDB derives from"
        for chain in declaring:
            flags.setenv(chain.WRITE_ENV, "postgres")
            assert "step13" in (chain.unmet_precondition() or ""), chain.__name__
            name = write_chains.chain_name(chain)
            assert write_chains.chain_modes()[name]["mode"] == "duckdb", name

    def test_the_full_rebuild_is_not_owed_and_nothing_is_held(self, cutover, tmp_path):
        """What the refusal is for: no DuckDB rebuild over the frozen
        classification. Mutation: drop the refusal — the start marks the
        warehouse dirty in full and holds the checks for it."""
        import asyncio
        import json

        from core import chain_latch, pg_managers_write
        from core import warehouse_cutover as wc
        from tests.unit.test_warehouse_writer import _metadata, _seed, _store

        store = _store(tmp_path)
        try:
            _seed(store, **{wc.WRITER_KEY: {
                "writer": "postgres", "since": "2026-10-01T00:00:00+00:00",
                "resolved": True}})
            chain_latch.latch(pg_managers_write.CHAIN)
            assert wc.configure_mode() == "postgres"
            asyncio.run(wc.settle_writer(store))
            assert not wc.held()
            assert _metadata(store, "warehouse_dirty") is None
            assert json.loads(_metadata(store, wc.WRITER_KEY)[0])["writer"] == "postgres"
        finally:
            asyncio.run(store.close())

    def test_a_registry_that_cannot_be_read_refuses_nothing(self, cutover, caplog):
        """`configure_mode` never raises, and an unreadable latch must not
        stop DuckDB deriving on a deployment that never flipped. Mutation:
        let the error through."""
        from core import chain_latch
        from core import warehouse_cutover as wc

        def boom(chain):
            raise OSError("marker directory unreadable")

        cutover.setattr(chain_latch, "latched", boom)
        assert wc.configure_mode() == "duckdb"
        assert wc.way_back_refused() == ()
        assert "could not be read" in caplog.text

    def test_health_publishes_it_and_the_canary_judges_it(self, cutover):
        """Mutation: leave `way_back_refused` out of the health block — the
        canary would page "ran as duckdb", which it did not."""
        from bot import canary
        from core import chain_latch, pg_managers_write
        from core import warehouse_cutover as wc
        from core.alerting import Kind, spec_for
        from web.routes.api import health

        chain_latch.latch(pg_managers_write.CHAIN)
        cutover.setenv(wc.ENV, "postgres")
        wc.configure_mode()
        block = health._warehouse_writer_mode()
        assert block["mode"] == "postgres"
        assert block["way_back_refused"] == ["pg_managers_write"]
        assert wc.status()["way_back_refused"] == ["pg_managers_write"]
        payload = {"warehouse_writer_mode": block}
        ((key, message),) = canary.check_warehouse_way_back_refused(payload)
        assert key == "warehouse_way_back_refused"
        assert "pg_managers_write" in message and "pg_revision" in message
        assert canary.check_warehouse_preconditions(payload) == []
        assert spec_for(key).kind is Kind.CONDITION
        # An older web, or a start that refused nothing.
        assert canary.check_warehouse_way_back_refused({}) == []
        unrefused = dict(block, way_back_refused=[])
        assert canary.check_warehouse_way_back_refused(
            {"warehouse_writer_mode": unrefused}) == []
        assert [k for k, _ in canary.check_warehouse_preconditions(
            {"warehouse_writer_mode": unrefused})] == ["warehouse_preconditions_unmet"]

    @pytest.mark.parametrize("derive, says", [
        ("own", "; Postgres derives"),
        ("piggyback", "; NOTHING derives (KS_PG_DERIVE=piggyback, not own)"),
        (None, None),
    ])
    def test_the_page_says_whether_anything_derives(self, cutover, caplog, derive, says):
        """Postgres derives on its own signal only under KS_PG_DERIVE=own; under
        `piggyback` its rebuild rides the DuckDB tick a `postgres` start does
        not run, so a refusal leaves nothing deriving. The page must not say
        "Postgres still derives" then — and an unset KS_WRITE_WAREHOUSE
        evaluates no precondition, so it reads the `derivation` block.
        Mutation: drop the clause, or the log's half of it."""
        import logging

        from bot import canary
        from core import chain_latch, pg_derivation, pg_managers_write
        from core import warehouse_cutover as wc
        from web.routes.api import health

        cutover.setattr(pg_derivation, "_mode", derive)
        chain_latch.latch(pg_managers_write.CHAIN)
        with caplog.at_level(logging.CRITICAL, logger=wc.__name__):
            assert wc.configure_mode() == "postgres"      # unset: the operator's way back
        assert ("NOTHING derives" in caplog.text) is (derive != "own")
        payload = {"warehouse_writer_mode": health._warehouse_writer_mode()}
        if derive is not None:
            payload["derivation"] = health._derivation_mode()
        ((_key, message),) = canary.check_warehouse_way_back_refused(payload)
        if says is None:
            # An older web's payload: no claim either way.
            assert message.endswith("own what DuckDB derives from"), message
        else:
            assert message.endswith(says), message
        assert "unless KS_PG_DERIVE=own" in dict(canary._ACTIONS)["warehouse_way_back_refused"]

    @pytest.mark.asyncio
    async def test_the_canary_pages_it_with_its_own_lever_and_title(self):
        from datetime import timedelta
        from unittest.mock import patch

        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["warehouse_writer_mode"] = {
            "mode": "postgres", "value": "duckdb", "error": None,
            "preconditions_unmet": [], "held": False, "held_for_s": None,
            "reclassify_needed": False, "way_back_refused": ["pg_managers_write"]}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "critical"
        assert result.failure_keys == ["warehouse_way_back_refused"]
        assert "chain_copy_back" in canary._what_to_do(result)
        assert canary._title(result) == "Warehouse way back refused"


class TestTheStatsWaitForTheOrders:
    """The ways back run chain 5's before chain 3's. Between the two DuckDB
    writes the managers again while chain 3 still owns the orders, and
    DuckDB's `orders` stopped at chain 3's latch."""

    @staticmethod
    def _orders_chain():
        import types

        chain = types.ModuleType("core.pg_chain_orders_write")
        chain.WRITE_ENV = "KS_WRITE_CHAIN_ORDERS"
        chain.CHAIN_TABLES = ("bronze.orders", "bronze.order_products")
        chain.env_writes_postgres = lambda: True
        return chain

    @pytest.mark.asyncio
    async def test_they_hold_rather_than_go_backwards_and_nothing_is_shipped(
            self, flags, tmp_path):
        """Mutation: drop the guard in `update_manager_stats` — the review's
        reproduction: Postgres held 2026-10-06 and 500 orders, and after the
        recompute 2026-09-01 and 1."""
        from unittest.mock import AsyncMock, patch

        from core import write_chains
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "stats.duckdb")
        await store.connect()

        async def stats():
            async with store.connection() as conn:
                return conn.execute(
                    "SELECT first_order_date, last_order_date, order_count "
                    "FROM managers WHERE id = 34").fetchone()
        try:
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO managers (id, name, first_order_date, last_order_date, "
                    "order_count) VALUES (34, 'M', DATE '2025-01-01', DATE '2026-10-06', 500)")
                conn.execute(
                    "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                    "created_at, updated_at, manager_id) VALUES (980000001, 1, 1, 100, "
                    "TIMESTAMPTZ '2026-09-01 10:00:00+00', TIMESTAMPTZ '2026-09-01 10:00:00+00', "
                    "TIMESTAMPTZ '2026-09-01 10:00:00+00', 34)")
            registered = write_chains.WRITE_CHAINS
            replica = AsyncMock()
            with patch("core.pg_replication.replicate_managers", new=replica):
                flags.setattr(write_chains, "WRITE_CHAINS",
                              registered + (self._orders_chain(),))
                assert await store.update_manager_stats() == 0
                assert await stats() == (date(2025, 1, 1), date(2026, 10, 6), 500)
                replica.assert_not_awaited()

                # The control: with nobody owning the orders, they are counted.
                flags.setattr(write_chains, "WRITE_CHAINS", registered)
                assert await store.update_manager_stats() == 1
                assert await stats() == (date(2026, 9, 1), date(2026, 9, 1), 1)
                replica.assert_awaited_once()
        finally:
            await store.close()


class TestTheReviewsSmallerGuards:
    @pytest.mark.parametrize("key", ["goals_bridge", "step13", "chain3",
                                     "read_fallback_off"])
    def test_a_check_that_raises_is_keyed_by_its_precondition(
            self, every_precondition_met, key):
        """Every reason starts with the precondition's name, an unreadable one
        too. Mutation: derive the key from the function's name —
        `_read_fallback_unmet` read as "read_fallback"."""
        from core import pg_managers_write, read_fallback, warehouse_cutover
        from core.repositories import goals

        def boom(*_a):
            raise OSError("unreadable")

        class _Unreadable:
            __contains__ = boom

        env = every_precondition_met
        if key == "goals_bridge":
            env.setattr(goals, "SALES_TYPE_BRIDGE_TABLES", _Unreadable())
        elif key == "step13":
            env.setattr(warehouse_cutover, "writes_postgres", boom)
        elif key == "chain3":
            _stand_in_chain3(env, _Chain3(raises=OSError("x")))
        else:
            env.setattr(read_fallback, "refusing", boom)
        assert pg_managers_write.unmet_precondition() == f"{key}: could not be read (OSError)"

    def test_a_chain_whose_state_cannot_be_read_counts_as_moved(self, flags):
        """`sales_type_bridge_owners`: the louder answer for a chain over a
        bridge table. Mutation: read an unreadable state as not moved."""
        from core import write_chains
        from core.repositories.goals import sales_type_bridge_owners

        real = write_chains._chain_state

        def state(chain):
            if write_chains.chain_name(chain) == "pg_managers_write":
                raise RuntimeError("unreadable")
            return real(chain)

        flags.setattr(write_chains, "_chain_state", state)
        assert sales_type_bridge_owners() == {
            "pg_managers_write": ("app.manager_classifications", "bronze.managers")}

    @pytest.mark.asyncio
    async def test_an_empty_managers_table_is_a_reason_before_the_flip(
            self, every_precondition_met, monkeypatch):
        """A flip over an empty `bronze.managers` sends every manager's orders
        to `internal`. Mutation: drop the reason."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        conn = _PreflightConn([_fresh(t) for t in pg_managers_write.CHAIN_TABLES],
                              dict(_CLEAN_SHAPE, managers=0, intervals=0))
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_PgPool(conn)))
        monkeypatch.setattr("core.pg.require_revision", AsyncMock())
        assert await pg_managers_write.preflight() == {
            "ok": False, "reasons": ["shape: bronze.managers is empty"]}

    @pytest.mark.asyncio
    async def test_while_a_precondition_is_unmet_postgres_is_not_asked(self, flags,
                                                                       monkeypatch):
        """The default build: chain 3 on DuckDB, so the flag cannot move the
        writes, and `/api/health` asks Postgres nothing on this chain's behalf
        once a minute. Mutation: ask anyway (the review: a default-on change
        not gated by KS_WRITE_MANAGERS)."""
        from unittest.mock import AsyncMock

        from core import pg_managers_write

        get_pool = AsyncMock(side_effect=AssertionError("Postgres was asked"))
        monkeypatch.setattr("core.pg.get_pool", get_pool)
        out = await pg_managers_write.preflight()
        assert out["ok"] is False
        assert [r.split(":")[0] for r in out["reasons"]] == [
            "goals_bridge", "step13", "chain3", "read_fallback_off"]
        get_pool.assert_not_awaited()


class TestTheRouteAnswersWhatHappened:
    PATH = TestTheRoute.PATH

    def test_a_manager_gone_between_the_list_and_the_write_is_404(self, route, flags):
        """Mutation: drop the `ManagerNotFound` mapping — a 503 naming
        Postgres for a manager that is not there."""
        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        store = _RouteStore(pg_managers_write.ManagerNotFound("manager 34"))
        res = route(store).post(self.PATH)
        assert res.status_code == 404 and "34" in res.json()["detail"]

    def test_a_refusal_postgres_never_saw_is_422_not_503(self, route, flags, monkeypatch):
        """A note carrying NUL, refused by the writer before Postgres is asked
        (the review's reproduction answered 503 "Postgres did not answer").
        Mutation: drop the route's `ClassificationRefused` mapping."""
        from unittest.mock import AsyncMock

        from core import chain_latch, pg_managers_write

        chain_latch.latch(pg_managers_write.CHAIN)
        get_pool = AsyncMock(side_effect=AssertionError("Postgres was asked"))
        monkeypatch.setattr("core.pg.get_pool", get_pool)

        class _Writing(_RouteStore):
            async def set_manager_retail_status(self, manager_id, is_retail, **kw):
                await pg_managers_write.set_manager_retail_status(
                    manager_id, is_retail, kw["effective_from"], kw["set_by"],
                    kw["note"], set_at=T0)

        res = route(_Writing()).post(self.PATH + "&effective_from=2026-10-07&note=a%00b")
        assert res.status_code == 422, res.text
        assert "note carries NUL" in res.json()["detail"]
        assert "a\x00b" not in res.text
        get_pool.assert_not_awaited()


# ─── Nothing reads the frozen DuckDB copies by name ─────────────────────────

# DuckDB's own `managers` and `manager_classifications` spelled bare in a
# statement — a read or a write — outside the `{managers}`/`{classifications}`
# holes `_read_walk` follows. Not after a `.`: `bronze.managers` is Postgres's.
_BARE = r"\b(?:FROM|JOIN|UPDATE|INTO)\s+(?:managers|manager_classifications)(?![.\w])"

# Every function that names them, with the function that routes it and the
# question that function asks: the DuckDB half of a store method behind the
# chain's own answer, the boot seed behind the latch, and the replica's read
# behind the stand-down. A new site fails until it is added here with its
# router — the review's probe (`FROM managers` in a new module) is one.
_BARE_SITES = {
    ("core/duckdb_store.py", "upsert_managers"): ("upsert_managers", "writes_postgres"),
    ("core/duckdb_store.py", "set_manager_retail_status"):
        ("set_manager_retail_status", "writes_postgres"),
    ("core/duckdb_store.py", "get_all_managers"): ("get_all_managers", "reads_postgres"),
    ("core/migrations.py", "_m0006_seed_manager_classifications"):
        ("_m0006_seed_manager_classifications", "latched"),
    ("core/pg_replication.py", "read_managers"): ("replicate_managers", "tables_stood_down"),
}


def _bare_walk():
    """`({(file, function) naming a bare table}, {(file, function): its
    calls}, {called name: {(file, function) calling it}})` over `core/`,
    `web/`, `scripts/`, `bot/` and `deploy/`. A module-level constant counts
    at every function of its module that loads it."""
    import pathlib
    import re

    repo = pathlib.Path(__file__).resolve().parents[2]
    bare = re.compile(_BARE, re.I)
    found, calls, callers = set(), {}, {}
    for top in ("core", "web", "scripts", "bot", "deploy"):
        for path in sorted((repo / top).rglob("*.py")):
            rel = str(path.relative_to(repo))
            tree = ast.parse(path.read_text(encoding="utf-8"))
            docs = {id(n.body[0].value) for n in ast.walk(tree)
                    if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                      ast.AsyncFunctionDef))
                    and n.body and isinstance(n.body[0], ast.Expr)
                    and isinstance(n.body[0].value, ast.Constant)}
            functions = [n for n in ast.walk(tree)
                         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
            owner = {}
            for fn in functions:
                for node in ast.walk(fn):
                    owner.setdefault(id(node), fn.name)
            for fn in functions:
                names = {getattr(c.func, "attr", getattr(c.func, "id", ""))
                         for c in ast.walk(fn) if isinstance(c, ast.Call)}
                calls.setdefault((rel, fn.name), set()).update(names)
                for name in names:
                    callers.setdefault(name, set()).add((rel, fn.name))
            constants = {}
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Constant) and isinstance(node.value, str)
                        and id(node) not in docs and bare.search(node.value)):
                    continue
                if id(node) in owner:
                    found.add((rel, owner[id(node)]))
                    continue
                for stmt in tree.body:
                    if isinstance(stmt, ast.Assign) and any(
                            n is node for n in ast.walk(stmt.value)):
                        constants.update({t.id: node for t in stmt.targets
                                          if isinstance(t, ast.Name)})
                else:
                    found.add((rel, "<module>"))
            for fn in functions:
                if any(isinstance(n, ast.Name) and n.id in constants
                       for n in ast.walk(fn)):
                    found.add((rel, fn.name))
    return found, calls, callers


class TestNothingReadsTheFrozenCopiesByName:
    def test_every_site_is_known(self):
        """The review's finding: a statement written `FROM managers` instead
        of `FROM {managers}` passed every walk. Mutation: add one."""
        found, _calls, _callers = _bare_walk()
        assert found == set(_BARE_SITES), sorted(found ^ set(_BARE_SITES))

    def test_each_is_routed_by_the_chain(self):
        """Mutation: drop the routing from any of them — `get_all_managers`'
        `reads_postgres()`, say."""
        _found, calls, callers = _bare_walk()
        for (rel, function), (router, asks) in _BARE_SITES.items():
            assert asks in calls[(rel, router)], (rel, router, asks)
            if function != router:
                assert callers[function] == {(rel, router)}, (function, callers[function])

    def test_it_sees_a_statement_written_bare(self):
        """Non-vacuity, on the review's own probe."""
        import re

        bare = re.compile(_BARE, re.I)
        for sql, seen in (("SELECT id, name, is_retail FROM managers ORDER BY id", True),
                          ("SELECT manager_id FROM manager_classifications", True),
                          ("SELECT 1 FROM orders o JOIN managers m ON m.id = o.manager_id", True),
                          ("SELECT 1 FROM bronze.managers", False),
                          ("SELECT 1 FROM {managers}", False),
                          ("'baseline frozen from managers.is_retail'", False)):
            assert bool(bare.search(sql)) is seen, sql
