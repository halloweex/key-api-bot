"""Stage 5, PR-7: the scheduler's jobs and the quality checks under `KS_DUCKDB=off`.

The design's 2.3. Under `off` the jobs below no longer reach for the file:

- the integrity scan's DuckDB half **stands down rather than fails** — the
  three checks with no Postgres twin join the stood-down set
  (`DUCKDB_OFF_STOOD_DOWN`, owner's decision OD-S5-5), the freshness check and
  the snapshot continuity judge what Postgres holds, and the scan is handed a
  connection that refuses (`duckdb_switch.RefusedConnection`), so the run is a
  verdict with no `error_message` and the twins stand in alone;
- the mirror landing reads DuckDB only for what survives the stand-down, which
  under chains 5 and 6 is nothing;
- the hourly operational copy and its daily comparison stood down for the
  three tables no chain owns, which froze as history (`DUCKDB_HISTORY_TABLES`,
  OD-S5-4), until step 14's S14-12 and S14-13 deleted both; the hourly job
  that kept the copy's id asks no store;
- `dq_reconciliation` needed nothing beyond the unconnected store (chain 3
  took its DuckDB arm) — held here so it stays so.

Not registering `duckdb_checkpoint` and `db_backup` is held in
`test_scheduler_cadence.py`, the catch-up asking Postgres first in
`test_scheduler_catchup.py`; the whole process in the target state is
`test_duckdb_off_sweep.py`'s.

Every class has its control under `on`, where nothing changed, and every guard
names the mutation it catches.
"""
from __future__ import annotations

import ast
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import duckdb_switch
from core import data_quality as dq
from core.duckdb_store import DuckDBStore
from tests.unit.test_duckdb_off_sweep import target_state  # noqa: F401 — fixture
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: F401 — fixture

STAMP = "2026-10-01T08:00:00+00:00"
REPO = Path(__file__).resolve().parents[2]


def _owned_by_chains() -> frozenset:
    from core.write_chains import WRITE_CHAINS

    return frozenset(t for chain in WRITE_CHAINS for t in chain.CHAIN_TABLES)


def _every_owner_row() -> dict:
    """Production's owner rows once every chain is latched: one per table —
    with each chain's local marker beside them, as production's are. Since
    step 14's S14-11 the sweep's target state latches nothing, so the marker
    half is written here: owner rows with no marker are the copy's
    `chain_marker_lost` stamp, a different state."""
    from core import chain_latch
    from core.write_chains import WRITE_CHAINS, chain_name

    for chain in WRITE_CHAINS:
        chain_latch.latch(chain_name(chain), chain.WRITE_ENV)
    return {table: STAMP for table in _owned_by_chains()}


def _turn_on(monkeypatch) -> None:
    """The control: the same state, under `on`."""
    monkeypatch.setenv(duckdb_switch.ENV, "on")
    duckdb_switch.configure_mode()
    assert not duckdb_switch.is_off()


# ─── The connection that refuses ─────────────────────────────────────────────


def _reaches(conn):
    """A caller in the application that issues a statement."""
    return conn.execute("SELECT 1")


class TestTheRefusedConnection:
    def test_a_statement_is_refused_counted_and_named_after_its_caller(self, duckdb_off):
        """The class is plumbing to `_site()`: the refusal names who issued
        the statement. Mutation: refuse without `guard()` — nothing counted,
        and a check reaching for the file under off fails as a bare
        RuntimeError the switch never sees."""
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            _reaches(duckdb_switch.RefusedConnection())
        assert duckdb_switch.opened().keys() == {f"{__name__}:_reaches"}
        assert not duckdb_off.exists()

    def test_sql_is_refused_too(self, duckdb_off):
        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            duckdb_switch.RefusedConnection().sql("SELECT 1")
        assert len(duckdb_switch.opened()) == 1

    @pytest.mark.asyncio
    async def test_the_async_form_stands_where_store_connection_stood(self, duckdb_off):
        async with duckdb_switch.refused_connection() as conn:
            assert isinstance(conn, duckdb_switch.RefusedConnection)
        assert duckdb_switch.opened() == {}

    def test_under_on_there_is_still_no_connection_behind_it(self, monkeypatch):
        _turn_on(monkeypatch)
        with pytest.raises(RuntimeError, match="no DuckDB connection"):
            _reaches(duckdb_switch.RefusedConnection())
        assert duckdb_switch.opened() == {}


# ─── The integrity scan ──────────────────────────────────────────────────────


def _fresh_inputs():
    """What the job reads from Postgres for the scan: a full snapshot
    calendar, every chain's watermark fresh, and the copy of DuckDB's
    `sync_metadata` fresh too."""
    now = datetime.now().astimezone()
    today = now.date()
    days = frozenset(today - timedelta(days=i) for i in range(1, 40))
    fresh = now.isoformat()
    rows = {f"last_sync_{entity}": fresh for entity in dq.FRESHNESS_THRESHOLDS}
    return (min(days), days), dict(rows), dict(rows)


def _names(issues):
    return sorted(i.check_name for i in issues)


def _scan_pk_checks():
    """The integrity scan's `_pk_uniqueness_check` calls, in source order, as
    `(table, wrapped, guard)`: `wrapped` when the call sits in a lambda —
    handed on to a chain's stand-down, `landing(...)` — and `guard` the
    finding name an enclosing `if "<name>" not in stood_down:` skips it by."""
    tree = ast.parse((REPO / "core/data_quality.py").read_text(encoding="utf-8"))
    scan = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == "check_internal_integrity")
    in_lambda = {id(n) for lam in ast.walk(scan) if isinstance(lam, ast.Lambda)
                 for n in ast.walk(lam)}
    guard = {}
    for node in ast.walk(scan):
        test = getattr(node, "test", None)
        if (isinstance(node, ast.If) and isinstance(test, ast.Compare)
                and isinstance(test.left, ast.Constant)
                and [type(op) for op in test.ops] == [ast.NotIn]
                and getattr(test.comparators[0], "id", None) == "stood_down"):
            guard.update({id(n): test.left.value
                          for stmt in node.body for n in ast.walk(stmt)})
    calls = sorted((n for n in ast.walk(scan) if isinstance(n, ast.Call)
                    and getattr(n.func, "id", None) == "_pk_uniqueness_check"),
                   key=lambda n: (n.lineno, n.col_offset))
    return [(n.args[1].value, id(n) in in_lambda, guard.get(id(n))) for n in calls]


class TestTheScanUnderOff:
    def test_the_set_is_read_out_of_the_scan_s_bare_calls(self):
        """`DUCKDB_OFF_STOOD_DOWN` is exactly the scan's `_pk_uniqueness_check`
        calls no chain's stand-down takes — those not handed on as a lambda —
        each skipped by its own finding's name. That is the rule `off`'s
        verdict (PR-2) reads its retired conditions by, so the two sets
        cannot part: wrapped in lambdas, the three left this set standing and
        the verdict's empty. Mutation: wrap them again, add a fourth bare
        call, or guard one by another table's name."""
        bare = [(table, name) for table, wrapped, name in _scan_pk_checks()
                if not wrapped]
        assert {f"pk_uniqueness_{table}" for table, _ in bare} == dq.DUCKDB_OFF_STOOD_DOWN
        assert all(name == f"pk_uniqueness_{table}" for table, name in bare), bare

    def test_the_wrapped_ones_are_chain_3_s(self):
        """The other side of the same rule: every PK check handed on as a
        lambda is an order table's, stood down with chain 3. Mutation: hand
        one of the three to `landing(...)`."""
        wrapped = {f"pk_uniqueness_{table}"
                   for table, is_wrapped, _ in _scan_pk_checks() if is_wrapped}
        assert wrapped and wrapped <= dq.ORDER_LANDING_CHECKS
        assert not wrapped & dq.DUCKDB_OFF_STOOD_DOWN

    def test_in_the_target_state_nothing_in_the_scan_reaches_for_the_file(
            self, target_state):  # noqa: F811
        """Every check over DuckDB stands down in the target state — Silver,
        Gold and UTM with step 13, the order tables with chain 3, and the three
        without a twin under off — and the two that judge read Postgres. So a
        connection that refuses every statement is never asked. Mutation:
        drop `DUCKDB_OFF_STOOD_DOWN` from the default — `pk_uniqueness_products`
        reaches the connection, is refused and raises out of the scan."""
        calendar, watermarks, frozen = _fresh_inputs()
        issues = dq.check_internal_integrity(
            duckdb_switch.RefusedConnection(), inventory_calendar=calendar,
            chain_watermarks=watermarks, frozen_sync_rows=frozen)
        assert issues == [], _names(issues)
        assert duckdb_switch.opened() == {}

    def test_the_three_without_a_twin_stand_down_under_off_alone(self, duckdb_off):
        assert dq.duckdb_off_stood_down() == dq.DUCKDB_OFF_STOOD_DOWN == frozenset({
            "pk_uniqueness_products", "pk_uniqueness_buyers",
            "pk_uniqueness_categories"})

    def test_each_is_a_table_a_chain_took_to_postgres(self):
        """OD-S5-5's ground: DuckDB's copy is frozen and Postgres refuses a
        duplicate id itself. Derived from the registry, so a check named for
        a table no chain moved cannot be stood down here by accident."""
        moved = {t.split(".", 1)[1] for t in _owned_by_chains()}
        for name in dq.DUCKDB_OFF_STOOD_DOWN:
            assert name.removeprefix("pk_uniqueness_") in moved, name

    def test_a_check_that_still_reaches_is_refused_where_it_reached(self, duckdb_off):
        """Outside the target state — step 13 not met, so the Silver checks
        are not stood down — a check that reaches the refused connection is
        counted under its own name and reported raised, and the scan goes on.
        The point of handing the scan a refusing connection rather than none:
        a reach is caught in the act, never a None that fails unseen."""
        raised = []
        issues = dq.check_internal_integrity(
            duckdb_switch.RefusedConnection(), inventory_calendar=(None, frozenset()),
            chain_watermarks={}, frozen_sync_rows={}, raised_out=raised,
            stood_down=dq.ORDER_LANDING_CHECKS | dq.DUCKDB_OFF_STOOD_DOWN)
        assert "headline_vs_line_items" in raised
        assert "core.data_quality:_headline_vs_line_items_check" in duckdb_switch.opened()
        assert all(site.startswith("core.data_quality:")
                   for site in duckdb_switch.opened()), duckdb_switch.opened()
        assert "integrity_check_raised" in _names(issues)
        assert not duckdb_off.exists()

    @pytest.mark.asyncio
    async def test_under_on_the_three_run_as_they_always_have(self, monkeypatch, tmp_path):
        """The control: under on nothing is stood down by this, and the scan
        asks DuckDB's own products, buyers and categories. Mutation: stand
        them down whatever the switch says."""
        assert dq.duckdb_off_stood_down() == frozenset()
        asked = []
        real = dq._pk_uniqueness_check

        def spy(conn, table, column):
            asked.append(table)
            return real(conn, table, column)

        monkeypatch.setattr(dq, "_pk_uniqueness_check", spy)
        store = DuckDBStore(db_path=tmp_path / "on.duckdb")
        await store.connect()
        try:
            async with store.connection() as conn:
                dq.check_internal_integrity(conn, stood_down=None)
        finally:
            await store.close()
        assert {"products", "buyers", "categories"} <= set(asked)


class TestTheIntegrityJobUnderOff:
    """End to end through `_run_dq_integrity`, in the target state, with
    every Postgres read answered."""

    @staticmethod
    def _postgres(monkeypatch):
        from core import (
            dq_journal, pg_chain_invariants, pg_chain_watermarks,
            pg_frozen_metadata, pg_inventory_write, pg_warehouse_dq,
        )
        from tests.unit.test_pg_warehouse_dq import _facts

        calendar, watermarks, frozen = _fresh_inputs()
        monkeypatch.setattr(pg_inventory_write, "read_snapshot_calendar",
                            AsyncMock(return_value=calendar))
        monkeypatch.setattr(pg_chain_watermarks, "read_values",
                            AsyncMock(side_effect=lambda keys: {
                                k: v for k, v in watermarks.items() if k in keys}))
        # The integrity job reads each row's stamp beside its value; here the
        # rows were written when their values say.
        monkeypatch.setattr(pg_chain_watermarks, "read_values_and_stamps",
                            AsyncMock(side_effect=lambda keys: (
                                {k: v for k, v in watermarks.items() if k in keys},
                                {k: v for k, v in watermarks.items() if k in keys})))
        monkeypatch.setattr(pg_frozen_metadata, "frozen_sync_rows",
                            AsyncMock(return_value=frozen))
        monkeypatch.setattr(pg_warehouse_dq, "read_facts",
                            AsyncMock(return_value=_facts()))
        looked = []
        real_check = pg_warehouse_dq.check_pg_warehouse

        def check(facts, **kwargs):
            looked.append(kwargs["duckdb_looked"])
            return real_check(facts, **kwargs)

        monkeypatch.setattr(pg_warehouse_dq, "check_pg_warehouse", check)
        monkeypatch.setattr(pg_chain_invariants, "read_facts", AsyncMock(
            return_value=pg_chain_invariants.Facts.blind((), "not read in this test")))
        journalled = []

        async def journal_run(**kwargs):
            journalled.append(kwargs)
            return 11

        monkeypatch.setattr(dq_journal, "journal_run", journal_run)
        return journalled, looked

    @pytest.mark.asyncio
    async def test_the_run_is_a_verdict_and_the_twins_stand_alone(
            self, target_state, monkeypatch):  # noqa: F811
        """The design's proof for 2.3: no `error_message`, the file never
        reached, `duckdb_looked` empty so the twins file on their own.
        Mutation: hand the scan `store.connection()` under off — the store
        is unconnected, the open is refused, and the run is a failure that
        ages the layer until the canary pages at 12 h (the sweep's
        `_run_dq_integrity` row)."""
        from core.scheduler import BackgroundScheduler

        journalled, looked = self._postgres(monkeypatch)
        scheduler = BackgroundScheduler()
        scheduler._send_dq_alert_throttled = AsyncMock(return_value=True)
        scheduler._resolve_dq_layer = AsyncMock()
        with patch("core.duckdb_store.get_store", new=AsyncMock(return_value=DuckDBStore())):
            result = await scheduler._run_dq_integrity()

        assert result["error"] is None, result
        (run,) = journalled
        assert run["layer"] == "integrity" and run["error_message"] is None
        assert "integrity_check_raised" not in _names(run["issues"])
        # The job's own call; the pairing record asks again, standing alone.
        assert looked[0] == frozenset()
        scheduler._resolve_dq_layer.assert_awaited_once()
        assert scheduler._resolve_dq_layer.await_args.args[2] is None
        assert duckdb_switch.opened() == {}


# ─── The hourly job ──────────────────────────────────────────────────────────


class TestTheHourlyJobUnderOff:
    @pytest.mark.asyncio
    async def test_it_derives_the_gender_and_copies_the_bot_without_the_store(
            self, target_state):  # noqa: F811
        """`replicate_operational` keeps its id; since step 14's S14-13 its
        body is the gender derivation and the bot's state, neither of which
        asks the store. Until then the operational copy was here, standing
        down for every chain's tables and the three history tables under off
        (`DUCKDB_HISTORY_TABLES`, OD-S5-4), with the tests that it opened
        nothing; the copy went with `core.pg_operational`. Mutation: ask the
        store, or drop the derivation — new buyers would get no verdict."""
        from core.scheduler import BackgroundScheduler

        derive = AsyncMock(return_value={"decided": 2})
        bot = AsyncMock(return_value={"skipped": "KS_BOT_STORE=postgres"})
        with patch("core.pg_buyers_write.derive_gender_pg", new=derive), \
             patch("core.pg_bot_state.replicate_bot_state", new=bot), \
             patch("core.duckdb_store.get_store",
                   new=AsyncMock(side_effect=AssertionError("the store was asked"))):
            result = await BackgroundScheduler()._run_replicate_operational()
        assert result == {"gender": {"decided": 2},
                          "bot_state": {"skipped": "KS_BOT_STORE=postgres"}}
        derive.assert_awaited_once()
        bot.assert_awaited_once()
        assert duckdb_switch.opened() == {}

    @pytest.mark.asyncio
    async def test_a_failed_derivation_does_not_cost_the_bot_its_copy(
            self, target_state):  # noqa: F811
        from core.scheduler import BackgroundScheduler

        bot = AsyncMock(return_value={"rows": 3})
        with patch("core.pg_buyers_write.derive_gender_pg",
                   new=AsyncMock(side_effect=RuntimeError("pg down"))), \
             patch("core.pg_bot_state.replicate_bot_state", new=bot):
            result = await BackgroundScheduler()._run_replicate_operational()
        assert result["gender"] == {"error": "RuntimeError: pg down"}
        assert result["bot_state"] == {"rows": 3}


# ─── The operational comparison ──────────────────────────────────────────────
#
# `reconcile_operational` read neither the history nor, in the target state,
# anything out of DuckDB (`TestTheOperationalComparisonUnderOff`), until step
# 14's S14-12 deleted it.


# ─── The reconciliation against KeyCRM ───────────────────────────────────────


class TestTheReconciliationUnderOff:
    @pytest.mark.asyncio
    async def test_it_compares_postgres_without_reaching_the_file(
            self, target_state):  # noqa: F811
        """No DuckDB arm since step 14's S14-12 (chain 3 had stood it down):
        the store is never asked, the Postgres arm runs and is journalled,
        and nothing is refused (the design's 2.3). Mutation: ask the store."""
        from core.scheduler import BackgroundScheduler

        sync = AsyncMock()
        sync.repair_orders = AsyncMock(return_value={"repaired": 0, "remaining": 0})
        scheduler = BackgroundScheduler()
        scheduler._send_dq_alert_throttled = AsyncMock()
        scheduler._resolve_dq_layer = AsyncMock()
        scheduler._reconcile_postgres = AsyncMock(
            return_value={"issues": [], "discrepancies": [], "error": None})
        scheduler._persist_postgres_reconciliation = AsyncMock()
        scheduler._reconcile_clickhouse = AsyncMock(return_value=None)
        with patch("core.reconciliation_io.keycrm_orders_in_window",
                   new=AsyncMock(return_value=({}, 1, set()))), \
             patch("core.data_quality.machine_attempts_note", return_value=None), \
             patch("core.sync_service.get_sync_service", new=AsyncMock(return_value=sync)), \
             patch("core.duckdb_store.get_store",
                   new=AsyncMock(side_effect=AssertionError("the store was asked"))):
            result = await scheduler._run_dq_reconciliation()
        assert result["error"] is None, result
        scheduler._reconcile_postgres.assert_awaited_once()
        scheduler._persist_postgres_reconciliation.assert_awaited_once()
        assert duckdb_switch.opened() == {}
