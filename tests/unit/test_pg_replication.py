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

Since step 14's S14-00 chain 5 writes both tables, a constant, so the copy
stands down: the tests of it carrying the rows, of the sync and the endpoint
running it and of the stats it shipped went with it (S14-13 deletes it).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_landing
from core.duckdb_constants import RETAIL_MANAGER_IDS
from core.duckdb_store import DuckDBStore
from core.pg_replication import (
    CLASSIFICATION_COLUMNS,
    MANAGER_COLUMNS,
    read_managers,
    replicate_managers,
    write_managers,
)


@pytest.fixture(autouse=True)
def _clean():
    pg_landing.reset_health()
    yield
    pg_landing.reset_health()


@pytest.fixture(autouse=True)
def _nothing_owns_the_classification(monkeypatch):
    """`replicate_managers` asks Postgres's owner rows before it reads DuckDB
    (the DN-22b review). These tests are about what it copies and how it
    fails, so Postgres answers that no write chain owns either table; the
    stand-down itself is pinned in `tests/unit/test_write_chains.py`."""
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=object()))
    monkeypatch.setattr("core.pg.require_revision", AsyncMock())
    monkeypatch.setattr("core.chain_latch.read_owners", AsyncMock(return_value={}))


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
    async def test_the_kill_switch_skips_without_touching_postgres(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv(pg_landing.MIRROR_ENV, "0")
        store = await _store(tmp_path)
        with patch("core.pg_replication.write_managers", new=AsyncMock()) as w:
            result = await replicate_managers(store)
        w.assert_not_called()
        assert result["skipped"]


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


