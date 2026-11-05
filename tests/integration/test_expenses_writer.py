"""Chain 8: the three expense writers, against a real Postgres.

Every test runs through the repository methods — the path the /expenses form AND
the chat tool take — under `KS_WRITE_EXPENSES=postgres`, on a DuckDBStore that
was never connected and whose `connection` raises. A write that reached DuckDB
would fail the test instead of quietly landing in the store being left.
"""
from __future__ import annotations

import os
from datetime import date
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


@pytest_asyncio.fixture
async def writer(monkeypatch):
    from core.duckdb_store import DuckDBStore

    monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.manual_expenses")
        # The owner rows the latch writes on its first Postgres write. Left
        # behind, they read in the next module as a chain owning a table with
        # no local marker — a real CRITICAL, manufactured by a fixture.
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

    store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")

    def _no_duckdb(*_a, **_k):
        raise AssertionError("an expense write reached DuckDB under KS_WRITE_EXPENSES=postgres")

    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()), \
         patch.object(DuckDBStore, "connection", _no_duckdb):
        yield store, pool
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.manual_expenses")
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
    await pool.close()


async def _row(pool, expense_id):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM app.manual_expenses WHERE id = $1", expense_id)


class TestAdd:
    @pytest.mark.asyncio
    async def test_it_lands_in_postgres_with_a_time_and_the_callers_shape(self, writer):
        store, pool = writer
        out = await store.add_expense(date(2026, 9, 1), "marketing", "Facebook Ads",
                                      1250.50, note="september", platform="facebook")
        assert set(out) == {"id", "expense_date", "category", "expense_type", "amount",
                            "currency", "note", "created_at", "platform"}
        assert out["amount"] == 1250.50 and out["currency"] == "UAH"
        row = await _row(pool, out["id"])
        assert row is not None and row["platform"] == "facebook"
        # DuckDB defaults created_at and the Postgres column does not; a writer
        # that forgot it would record every typed expense with no time at all.
        assert row["created_at"] is not None

    @pytest.mark.asyncio
    async def test_the_allocator_starts_above_a_row_carried_in_before_the_switch(self, writer):
        """Revision 0031's setval saw an empty table. An expense typed before
        the switch arrives afterwards carrying DuckDB's id; the next id handed
        out here must not collide with it."""
        store, pool = writer
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.manual_expenses (id, expense_date, category, "
                "expense_type, amount, currency, created_at) "
                "VALUES (500, '2026-08-01', 'marketing', 'Google Ads', 10, 'UAH', now())")
        out = await store.add_expense(date(2026, 9, 2), "marketing", "TikTok Ads", 20)
        assert out["id"] > 500


class TestUpdate:
    @pytest.mark.asyncio
    async def test_named_fields_change_and_the_rest_stay(self, writer):
        store, pool = writer
        added = await store.add_expense(date(2026, 9, 3), "marketing", "Facebook Ads", 100)
        out = await store.update_expense(added["id"], amount=175.25, note="corrected")
        assert out["amount"] == 175.25 and out["note"] == "corrected"
        assert out["category"] == "marketing" and out["updated_at"] is not None
        row = await _row(pool, added["id"])
        assert float(row["amount"]) == 175.25 and row["expense_type"] == "Facebook Ads"

    @pytest.mark.asyncio
    async def test_nothing_to_set_and_no_such_row_are_both_none(self, writer):
        store, _ = writer
        added = await store.add_expense(date(2026, 9, 4), "other", "Misc", 5)
        assert await store.update_expense(added["id"]) is None
        assert await store.update_expense(987654, amount=1) is None


class TestDelete:
    @pytest.mark.asyncio
    async def test_a_deletion_stays_a_deletion(self, writer):
        """The replica is a full replace so a withdrawn spend cannot survive as a
        ghost still dividing the ROAS. The writer keeps that promise directly."""
        store, pool = writer
        added = await store.add_expense(date(2026, 9, 5), "marketing", "Facebook Ads", 300)
        assert await store.delete_expense(added["id"]) is True
        assert await _row(pool, added["id"]) is None
        assert await store.delete_expense(added["id"]) is False
