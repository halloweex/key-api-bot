"""Orders with revenue and no line items get their own job.

523 of them, ₴1,422,610.30. Revenue counts them because it reads `grand_total`;
every product, brand and category figure cannot, because those read line items.
Only a fetch by id fills them — a delta sync keyed on `updated_at` sees a
complete-looking header and moves on.

The repair used to live inside the reconciliation job, which failed 57 runs out
of 68 and died at ~120 s before reaching it. Nothing about this scan needs the
comparison, or its 90-day window; most of these orders are older than it.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core.duckdb_store import DuckDBStore
from core.scheduler import BackgroundScheduler


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _insert_order(conn, oid: int, grand_total: str = "1000.00"):
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, 4, 1, ?, ?, ?, ?, 10, NULL, NULL, NULL)
        """,
        [oid, grand_total] + ["2026-08-01T10:00:00+03:00"] * 3,
    )


def _insert_line_item(conn, oid: int):
    conn.execute(
        "INSERT INTO order_products (id, order_id, product_id, name, quantity, "
        "price_sold) VALUES (?, ?, 1, 'x', 1, 100.0)",
        [oid * 1000, oid],
    )


@pytest.fixture
def scheduler(monkeypatch):
    return BackgroundScheduler()


def _wire(monkeypatch, store, repair_mock):
    async def _get_store():
        return store

    async def _get_sync_service():
        svc = AsyncMock()
        svc.repair_orders = repair_mock
        return svc

    monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
    monkeypatch.setattr("core.sync_service.get_sync_service", _get_sync_service)


class TestHalfWrittenRepair:
    @pytest.mark.asyncio
    async def test_only_orders_missing_line_items_are_fetched(
        self, tmp_path, monkeypatch, scheduler,
    ):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1, "5000.00")     # empty, biggest
                _insert_order(conn, 2, "900.00")      # empty
                _insert_order(conn, 3, "7000.00")     # complete
                _insert_line_item(conn, 3)
                _insert_order(conn, 4, "0.00")        # no revenue → not our problem

            repair = AsyncMock(return_value={"repaired": 2, "failed": 0})
            _wire(monkeypatch, store, repair)

            out = await scheduler._run_halfwritten_repair()

            assert repair.await_args.args[0] == [1, 2], "biggest money first"
            assert out["candidates"] == 2
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_order_keycrm_serves_empty_is_not_asked_again(
        self, tmp_path, monkeypatch, scheduler,
    ):
        """Otherwise the same 200 ids burn 200 API calls every two hours,
        forever — and 429s are what started this whole investigation."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1)
                _insert_order(conn, 2)

            # The repair "succeeds" but the orders come back with no line items.
            repair = AsyncMock(return_value={"repaired": 2, "failed": 0})
            _wire(monkeypatch, store, repair)

            first = await scheduler._run_halfwritten_repair()
            assert first["still_empty"] == 2

            second = await scheduler._run_halfwritten_repair()
            assert second["candidates"] == 0
            assert repair.await_count == 1, "asked KeyCRM twice about the same ids"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_repaired_order_is_not_recorded_as_a_miss(
        self, tmp_path, monkeypatch, scheduler,
    ):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1)
                _insert_order(conn, 2)

            async def repair_and_fill(ids, **kwargs):
                async with store.connection() as conn:
                    _insert_line_item(conn, 1)
                return {"repaired": 1, "failed": 0}

            _wire(monkeypatch, store, repair_and_fill)

            out = await scheduler._run_halfwritten_repair()

            assert out["still_empty"] == 1  # only order 2
            async with store.connection() as conn:
                recorded = [r[0] for r in conn.execute(
                    "SELECT order_id FROM order_backfill_misses"
                ).fetchall()]
            assert recorded == [2]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_stale_record_comes_back_up_for_another_try(
        self, tmp_path, monkeypatch, scheduler,
    ):
        """Line items someone fixes upstream must not be invisible forever."""
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1)
                conn.execute(
                    "INSERT INTO order_backfill_misses (order_id, checked_at, reason) "
                    "VALUES (1, ?, 'old attempt')",
                    [datetime.now(timezone.utc) - timedelta(days=31)],
                )

            repair = AsyncMock(return_value={"repaired": 0, "failed": 0})
            _wire(monkeypatch, store, repair)

            out = await scheduler._run_halfwritten_repair()

            assert out["candidates"] == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_idle_run_costs_no_api_calls(
        self, tmp_path, monkeypatch, scheduler,
    ):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_order(conn, 1)
                _insert_line_item(conn, 1)

            repair = AsyncMock(return_value={"repaired": 0, "failed": 0})
            _wire(monkeypatch, store, repair)

            out = await scheduler._run_halfwritten_repair()

            assert out == {"candidates": 0, "repaired": 0, "still_empty": 0}
            repair.assert_not_awaited()
        finally:
            await store.close()


class TestReconciliationNoLongerCarriesIt:
    def test_the_job_is_registered_on_its_own_trigger(self, monkeypatch):
        added = {}

        class FakeScheduler:
            def add_job(self, func, **kwargs):
                added[kwargs.get("id")] = kwargs

            def get_job(self, job_id):
                return None

        scheduler = BackgroundScheduler()
        scheduler._scheduler = FakeScheduler()
        import asyncio
        asyncio.run(scheduler._register_jobs())

        assert "halfwritten_repair" in added
        assert added["halfwritten_repair"]["max_instances"] == 1

    def test_reconciliation_no_longer_scans_for_them(self):
        """The scan owes nothing to the comparison; it must not share its fate."""
        import inspect
        from core.scheduler import BackgroundScheduler as BS

        source = inspect.getsource(BS._run_dq_reconciliation)
        assert "order_products" not in source, (
            "the half-written scan is back inside the reconciliation job"
        )
        assert "REPAIR_BATCH_LIMIT" not in source
