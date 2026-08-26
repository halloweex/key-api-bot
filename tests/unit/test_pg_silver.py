"""Silver computed inside Postgres — the first derived table, not a copy.

Verified end to end before this file was written: throwaway
`postgres:17.2-alpine` at revision 0006, the production catalogue of orders
backfilled into `bronze.orders`, the classification replicated, then
`rebuild_silver()`.

    rebuild        46,446 rows in 2.19 s
    against DuckDB 0 only on either side, **0 differing rows** across all
                   fifteen columns
    sales_type     b2b 1189 · exhibition 178 · internal 2288 · retail 42791
                   — identical on both sides

What is pinned here is the wiring that result depended on, because every part
of it is silent when wrong: a column order that shifts values one place left,
a pass rendered for the wrong engine, a rebuild that leaves the two passes in
separate transactions.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import silver_select_sql
from core.pg_silver import SILVER_COLUMNS, SILVER_TABLE, pass1_sql, pass2_sql, rebuild_silver
from core.sql_dialect import POSTGRES

REPO = Path(__file__).resolve().parents[2]


class TestTheColumnOrderIsTheProjectionsOrder:
    def test_it_matches_the_migration(self):
        """A shift here writes `status_id` into `source_id` and raises nothing:
        both are integers."""
        ddl = (REPO / "migrations" / "versions" / "0006_silver_orders.py").read_text()
        body = ddl[ddl.index("CREATE TABLE silver.orders"):]
        declared = [
            m.group(1) for m in re.finditer(r"^\s{12}([a-z_]+)\s+[A-Z]", body, re.M)
        ]
        assert declared == list(SILVER_COLUMNS)

    def test_the_insert_names_every_column_explicitly(self):
        """Positional INSERT into a table whose column order changed later is
        the same failure with no test to catch it."""
        head = pass1_sql().splitlines()[0]
        for column in SILVER_COLUMNS:
            assert column in head


class TestBothPassesAreRenderedForPostgres:
    def test_pass_one_reads_bronze_and_casts_the_date(self):
        sql = pass1_sql()
        assert f"FROM {POSTGRES.orders} o" in sql
        assert "::date" in sql
        assert "DATE(timezone(" not in sql

    def test_pass_two_targets_the_schema_qualified_table(self):
        assert f"UPDATE {SILVER_TABLE} AS s SET" in pass2_sql()

    def test_pass_two_is_aliased_not_schema_qualified_in_predicates(self):
        """PostgreSQL reads `silver.orders.buyer_id` as a column of a column;
        both dialects address the target through one alias so the text stays
        identical apart from the table name."""
        sql = pass2_sql()
        assert "silver.orders.buyer_id" not in sql
        assert "WHERE s.buyer_id = fo.buyer_id" in sql

    def test_the_projection_is_the_shared_one(self):
        """Not a second Postgres-flavoured copy — rule 1, and #101 is the
        proof it takes under a day to bite."""
        assert silver_select_sql(POSTGRES) in pass1_sql()


class TestTheRebuildIsOneTransaction:
    def test_truncate_and_both_passes_share_a_transaction(self):
        """Between pass 1 and pass 2 every row carries `is_new_customer =
        FALSE`, which is not incomplete but wrong. Nothing may observe it."""
        source = inspect.getsource(rebuild_silver)
        assert "async with conn.transaction():" in source
        for step in ("TRUNCATE", "pass1_sql()", "pass2_sql()"):
            assert step in source
        assert source.index("conn.transaction()") < source.index("TRUNCATE")

    def test_it_moves_the_same_watermark_the_mirror_uses(self):
        """So Reconciliation A can tell 'not rebuilt yet' from 'rebuilt wrong'
        without a second mechanism to learn."""
        source = inspect.getsource(rebuild_silver)
        assert "_WATERMARK_OK" in source

    def test_it_raises_rather_than_reporting_success(self):
        """Unlike the mirror in the sync path: nothing reads this table yet, so
        there is no working system to protect by staying quiet."""
        source = inspect.getsource(rebuild_silver)
        assert "except" not in source


class TestTheRefreshHook:
    """Postgres Silver is recomputed after the DuckDB warehouse refresh.

    Three properties, each of which is silent when wrong: it cannot break the
    refresh, it does not run 720 times a day, and it does not report a fresh
    Postgres Silver beside a failed DuckDB one.
    """

    def _scheduler(self):
        from core.scheduler import BackgroundScheduler

        BackgroundScheduler._pg_silver_last_at = 0.0
        return BackgroundScheduler()

    @pytest.mark.asyncio
    async def test_it_rebuilds_after_a_successful_refresh(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver",
                   new=AsyncMock(return_value={"rows": 1})) as rebuild:
            await self._scheduler()._rebuild_postgres_silver({"status": "success"})
        rebuild.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_failed_refresh_is_not_followed_by_a_rebuild(self, monkeypatch):
        """A fresh Postgres Silver beside a failed DuckDB one invites reading
        the two as comparable when they are not."""
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver", new=AsyncMock()) as rebuild:
            await self._scheduler()._rebuild_postgres_silver({"status": "error"})
        rebuild.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_floor_stops_it_running_every_two_minutes(self, monkeypatch):
        """46,446 rows and 2.19 s every time, however small the change was."""
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "600")
        scheduler = self._scheduler()
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver",
                   new=AsyncMock(return_value={"rows": 1})) as rebuild:
            await scheduler._rebuild_postgres_silver({"status": "success"})
            await scheduler._rebuild_postgres_silver({"status": "success"})
        assert rebuild.await_count == 1

    @pytest.mark.asyncio
    async def test_a_postgres_fault_cannot_break_the_refresh(self, monkeypatch):
        """Rule 8: DuckDB is what the business looks at."""
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver",
                   side_effect=RuntimeError("postgres is down")):
            await self._scheduler()._rebuild_postgres_silver({"status": "success"})

    @pytest.mark.asyncio
    async def test_no_postgres_configured_does_nothing(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=False), \
             patch("core.pg_silver.rebuild_silver", new=AsyncMock()) as rebuild:
            await self._scheduler()._rebuild_postgres_silver({"status": "success"})
        rebuild.assert_not_called()

    def test_the_refresh_job_calls_it_outside_the_store_lock(self):
        """Awaiting a network round-trip under the DuckDB lock is how a sync
        becomes a stall."""
        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler._run_warehouse_refresh)
        # Position, not presence: the call's own comment names
        # `store.connection()` as the thing it stays outside of, so a grep for
        # that string matches the explanation. Sixth time in two days.
        assert source.index("refresh_warehouse_layers") < source.index(
            "await self._rebuild_postgres_silver"
        )
