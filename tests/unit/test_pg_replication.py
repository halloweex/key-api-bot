"""Replication, not mirroring — and the difference is the whole file.

Everything in `core/pg_landing.py` re-reads a KeyCRM payload: lose Postgres and
the next sync refills it. These two tables cannot work that way, because
`sales_type` is decided by a human and KeyCRM has never heard of the decision.

`upsert_managers` seeds `is_retail` from `RETAIL_MANAGER_IDS` **for managers it
has never seen** and never touches it again — the rule exists because
recomputing it every sync made the column unfixable, and eight managers sold
₴3.1M into the wrong bucket for months. A mirror that re-seeded from the same
constant would give the two stores different classifications, and the Gold
reconciliation that follows would be measuring the classification instead of
the migration.

So: copied from DuckDB, replaced whole, and pinned here.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_landing
from core.duckdb_constants import RETAIL_MANAGER_IDS
from core.duckdb_store import DuckDBStore
from core.pg_replication import (
    CLASSIFICATION_COLUMNS,
    CLASSIFICATIONS_TABLE,
    MANAGER_COLUMNS,
    MANAGERS_TABLE,
    read_managers,
    replicate_managers,
    write_managers,
)


@pytest.fixture(autouse=True)
def _clean():
    pg_landing.reset_health()
    yield
    pg_landing.reset_health()


async def _store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "mgr.duckdb")
    await store.connect()
    return store


class TestWhatItReads:
    @pytest.mark.asyncio
    async def test_it_reads_is_retail_as_stored_not_as_the_constant_says(self, tmp_path):
        """The crux. A human's decision outranks RETAIL_MANAGER_IDS, and this
        copy has to carry the decision or the two stores classify differently."""
        store = await _store(tmp_path)
        contrarian = RETAIL_MANAGER_IDS[0]          # seeded TRUE by the sync
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO managers (id, name, is_retail) VALUES (?, ?, FALSE)",
                [contrarian, "Reclassified by a human"],
            )
            conn.execute(
                "INSERT INTO managers (id, name, is_retail) VALUES (999, 'New', TRUE)"
            )
            managers, _ = read_managers(conn)

        by_id = {m[0]: m for m in managers}
        assert by_id[contrarian][MANAGER_COLUMNS.index("is_retail")] is False
        assert by_id[999][MANAGER_COLUMNS.index("is_retail")] is True

    @pytest.mark.asyncio
    async def test_it_reads_the_intervals_in_key_order(self, tmp_path):
        store = await _store(tmp_path)
        async with store.connection() as conn:
            conn.execute("INSERT INTO managers (id, name) VALUES (4, 'M')")
            for valid_from, retail in (
                (date(2026, 1, 1), True), (date(2026, 6, 1), False),
            ):
                conn.execute(
                    "INSERT INTO manager_classifications "
                    "(manager_id, is_retail, valid_from) VALUES (?, ?, ?)",
                    [4, retail, valid_from],
                )
            _, classifications = read_managers(conn)

        assert [c[CLASSIFICATION_COLUMNS.index("valid_from")] for c in classifications] == [
            date(2026, 1, 1), date(2026, 6, 1),
        ]


class TestItRefusesToEmptyTheTable:
    @pytest.mark.asyncio
    async def test_an_empty_manager_list_raises_instead_of_wiping(self):
        """Replacing a populated table with nothing takes `sales_type` with
        it: every order whose manager vanished falls through to `internal`,
        which is admin-only, and the dashboard empties for everyone else."""
        with pytest.raises(ValueError, match="empty manager list"):
            await write_managers([], [])


class TestItNeverRaises:
    @pytest.mark.asyncio
    async def test_a_write_failure_is_counted_against_both_tables(self, tmp_path):
        store = await _store(tmp_path)
        async with store.connection() as conn:
            conn.execute("INSERT INTO managers (id, name) VALUES (4, 'M')")

        with patch("core.pg_replication.write_managers",
                   side_effect=RuntimeError("pg is down")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            result = await replicate_managers(store)

        assert result["ok"] is False
        assert "pg is down" in result["error"]
        assert pg_landing.health()["failures"] == {
            MANAGERS_TABLE: 1, CLASSIFICATIONS_TABLE: 1,
        }

    @pytest.mark.asyncio
    async def test_a_read_failure_does_not_reach_the_caller(self, tmp_path):
        store = await _store(tmp_path)
        with patch("core.pg_replication.read_managers",
                   side_effect=RuntimeError("duckdb is busy")):
            result = await replicate_managers(store)
        assert result["ok"] is False and "duckdb is busy" in result["error"]

    @pytest.mark.asyncio
    async def test_the_kill_switch_skips_without_touching_postgres(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv(pg_landing.MIRROR_ENV, "0")
        store = await _store(tmp_path)
        with patch("core.pg_replication.write_managers", new=AsyncMock()) as w:
            result = await replicate_managers(store)
        w.assert_not_called()
        assert result["skipped"]

    @pytest.mark.asyncio
    async def test_a_success_reports_what_it_carried(self, tmp_path):
        store = await _store(tmp_path)
        async with store.connection() as conn:
            conn.execute("INSERT INTO managers (id, name) VALUES (4, 'M')")
            conn.execute(
                "INSERT INTO manager_classifications "
                "(manager_id, is_retail, valid_from) VALUES (4, TRUE, ?)",
                [date(2026, 1, 1)],
            )
        with patch("core.pg_replication.write_managers", new=AsyncMock()):
            result = await replicate_managers(store)

        assert result == {
            "managers": 1, "classifications": 1, "ok": True,
            "skipped": None, "error": None,
        }


class TestTheSyncAndTheEndpointBothCarryIt:
    @pytest.mark.asyncio
    async def test_a_manager_sync_replicates(self, tmp_path):
        """Otherwise a new manager exists in one store's `sales_type` and not
        the other's until somebody notices."""
        store = await _store(tmp_path)
        with patch("core.pg_replication.replicate_managers",
                   new=AsyncMock()) as rep:
            count = await store.upsert_managers([{"id": 4, "name": "M"}])
        assert count == 1
        rep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_sync_still_returns_its_count_when_postgres_is_down(
        self, tmp_path,
    ):
        """Rule 8: the step leaves the system working."""
        store = await _store(tmp_path)
        with patch("core.pg_replication.write_managers",
                   side_effect=RuntimeError("nope")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            assert await store.upsert_managers([{"id": 4, "name": "M"}]) == 1

    def test_the_classification_endpoint_replicates_too(self):
        """Waiting for the daily manager sync would leave Silver computed in
        Postgres carrying yesterday's answer for up to a day."""
        import inspect

        from web.routes.api.admin import set_manager_retail_status

        source = inspect.getsource(set_manager_retail_status)
        assert "replicate_managers" in source
        assert source.index("set_manager_retail_status(") < source.index(
            "replicate_managers(store)"
        )


class TestItHappensAtStartup:
    """The gap that made this necessary was measured, not imagined.

    Revision 0005 shipped, the deploy succeeded, and `bronze.managers` sat at
    **0 rows**: the only writer the sync reaches runs once a day and had
    already run that morning. Between a deploy and the next daily sync,
    Postgres held no classification at all — and four of the six branches of
    `silver_sales_type_case()` read it.
    """

    @pytest.mark.asyncio
    async def test_the_scheduler_replicates_on_start(self, tmp_path):
        from core.scheduler import BackgroundScheduler

        store = await _store(tmp_path)
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.duckdb_store.get_store", new=AsyncMock(return_value=store)), \
             patch("core.pg_replication.replicate_managers",
                   new=AsyncMock(return_value={"ok": True})) as rep:
            await BackgroundScheduler()._replicate_classification_once()
        rep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_postgres_configured_does_nothing(self, tmp_path):
        from core.scheduler import BackgroundScheduler

        with patch("core.mirror_reconciliation.configured", return_value=False), \
             patch("core.pg_replication.replicate_managers", new=AsyncMock()) as rep:
            await BackgroundScheduler()._replicate_classification_once()
        rep.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failure_cannot_stop_the_scheduler(self):
        """A scheduler that will not start because Postgres is down is a worse
        outcome than a stale copy."""
        from core.scheduler import BackgroundScheduler

        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.duckdb_store.get_store",
                   side_effect=RuntimeError("duckdb is locked")):
            await BackgroundScheduler()._replicate_classification_once()

    def test_start_calls_it(self):
        import inspect

        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler.start)
        assert "_replicate_classification_once" in source
