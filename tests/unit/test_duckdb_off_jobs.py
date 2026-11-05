"""Stage 5, PR-7: the scheduler's jobs and the quality checks under `KS_DUCKDB=off`.

The design's 2.3. Under `off` the jobs below no longer reach for the file:

- the integrity scan's DuckDB half **stood down rather than failed** — the
  three checks with no Postgres twin joined the stood-down set
  (`DUCKDB_OFF_STOOD_DOWN`, owner's decision OD-S5-5), the freshness check and
  the snapshot continuity judged what Postgres holds, and the scan was handed
  a connection that refused (`duckdb_switch.RefusedConnection`), so the run
  was a verdict with no `error_message` and the twins stood in alone. Step
  14's S14-14 deleted the DuckDB half, the set and the connection: the job
  opens no DuckDB connection at all, which the class below still holds;
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

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from core import duckdb_switch
from core import data_quality as dq
from core.duckdb_store import DuckDBStore
from tests.unit.test_duckdb_off_sweep import target_state  # noqa: F401 — fixture
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: F401 — fixture



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
            looked.append(sorted(kwargs))
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
        reached, and the twins file on their own — since step 14's S14-14
        with no DuckDB scan to be handed what it looked at (`duckdb_looked`
        empty until then). Mutation: hand the scan `store.connection()` —
        the store is unconnected, the open is refused, and the run is a
        failure that ages the layer until the canary pages at 12 h."""
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
        # One call, handed nothing about DuckDB; no pairing record asks again.
        assert looked == [["held_out"]]
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
