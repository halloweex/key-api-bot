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


class TestTheHourlyShipperCannotRollTheWriterBack:
    """The most dangerous state for a write chain, proven end to end.

    `replicate_operational` full-replaces `app.manual_expenses` out of DuckDB
    every hour. Under `KS_WRITE_EXPENSES=postgres` DuckDB has stopped receiving
    these rows, so a replace would roll back every expense typed since the
    switch — once an hour, looking healthy in between. The shipper must stand
    down for this table.

    Two tests, and the second is what gives the first its meaning.
    `replicate_operational` never raises: a run that failed internally wipes
    nothing, so "the row survived" alone could pass for the wrong reason. Both
    therefore require a run with no `error`, and the control proves the replace
    is real by showing the same row wiped once the chain no longer owns the
    table — which since DN-06 means releasing the latch, not clearing the flag.
    """

    @pytest_asyncio.fixture
    async def empty_duckdb_and_postgres(self, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv("KS_PG_DSN", DSN)
        store = DuckDBStore(db_path=tmp_path / "empty.duckdb")
        await store.connect()                     # schema only; manual_expenses empty
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.manual_expenses")
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.manual_expenses")
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await pool.close()
        await store.close()

    @staticmethod
    async def _typed_in_postgres(pool):
        from core import pg_expenses_write
        row = await pg_expenses_write.add_expense(
            date(2026, 9, 10), "marketing", "Facebook Ads", 4200, "UAH", "typed after switch", "facebook")
        return row[0]

    @pytest.mark.asyncio
    async def test_under_the_flag_the_typed_expense_survives_the_hourly_replace(
        self, empty_duckdb_and_postgres, monkeypatch,
    ):
        from core.pg_operational import replicate_operational

        store, pool = empty_duckdb_and_postgres
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
        expense_id = await self._typed_in_postgres(pool)

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert "app.manual_expenses" in result.get("stood_down", []), result
        # Reported as stood down, never as replaced: the log line is what an
        # operator reads to learn whether the copy touched the table.
        assert "app.manual_expenses" not in result["replaced"], result
        assert await _row(pool, expense_id) is not None, "the hourly replace rolled it back"

    @pytest.mark.asyncio
    async def test_control_without_the_flag_the_same_replace_wipes_it(
        self, empty_duckdb_and_postgres, monkeypatch,
    ):
        """Proves the replace is real — so the survival above is the stand-down
        working, not a run that quietly did nothing.

        Removing the variable is no longer enough to get here, and that is the
        point of DN-06: the first typed expense LATCHES the chain, and from then
        on the writes, the shipper and the comparison all follow the latch
        rather than the flag (OD-19 (a)). So the control releases both copies of
        it, which is what `scripts/chain_copy_back.py` will do after it has
        carried the rows back to DuckDB — and only then does the replace bite.
        """
        from core import chain_latch
        from core.pg_operational import replicate_operational

        store, pool = empty_duckdb_and_postgres
        monkeypatch.setenv("KS_WRITE_EXPENSES", "postgres")
        expense_id = await self._typed_in_postgres(pool)
        monkeypatch.delenv("KS_WRITE_EXPENSES", raising=False)   # DuckDB is the writer again
        chain_latch.release("pg_expenses_write")
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert "app.manual_expenses" not in result.get("stood_down", [])
        assert await _row(pool, expense_id) is None, "the replace out of an empty DuckDB did not run"


class TestAFlagNotUnderstoodStandsDownOnlyItsChain:
    """DN-01. A typo in KS_WRITE_EXPENSES used to raise inside the shipper's
    registry call and take the hourly copy of every stage-3 table with it. Now
    that chain stands down — its Postgres row cannot be overwritten — its table
    is stamped failing, and everything else ships."""

    @pytest_asyncio.fixture
    async def stores(self, tmp_path, monkeypatch):
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv("KS_PG_DSN", DSN)
        for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
            monkeypatch.delenv(env, raising=False)
        store = DuckDBStore(db_path=tmp_path / "typo.duckdb")
        await store.connect()
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.manual_expenses")
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
            await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = 'app.manual_expenses'")
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool, monkeypatch
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.manual_expenses")
            await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
            await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = 'app.manual_expenses'")
        await pool.close()
        await store.close()

    @pytest.mark.asyncio
    async def test_the_row_survives_the_rest_ships_and_the_failure_is_stamped(self, stores):
        from core import pg_expenses_write
        from core.pg_operational import replicate_operational

        store, pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        row = await pg_expenses_write.add_expense(
            date(2026, 9, 17), "marketing", "Facebook Ads", 99, "UAH", "typed", "facebook")
        env.setenv("KS_WRITE_EXPENSES", "postgrse")                 # the typo

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert "app.manual_expenses" in result["stood_down"]
        assert "pg_expenses_write" in result["chain_flag_errors"]
        assert "app.manual_expenses" not in result["replaced"]
        assert "bronze.expense_types" in result["replaced"]         # the rest shipped
        assert await _row(pool, row[0]) is not None, "the typo let the copy wipe the row"
        async with pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT failures_since_ok, last_error FROM meta.mirror_state WHERE table_name = 'app.manual_expenses'")
        # Two stamps now, because the typed row also latched the chain and the
        # variable no longer says `postgres` — both are true and both are
        # recorded. `last_error` keeps the typo: correcting it ends the
        # mismatch too, where releasing the latch would leave the typo.
        assert state["failures_since_ok"] >= 2 and "postgrse" in state["last_error"]
        assert list(result["chain_flag_mismatch"]) == ["pg_expenses_write"]

    @pytest.mark.asyncio
    async def test_stood_down_tables_are_not_even_read_out_of_duckdb(self, stores):
        """After stage 5 those DuckDB tables will not exist. With both chains
        writing Postgres and their DuckDB tables gone, the copy still runs."""
        from core.pg_operational import replicate_operational

        store, _pool, env = stores
        env.setenv("KS_WRITE_EXPENSES", "postgres")
        env.setenv("KS_WRITE_INVENTORY", "postgres")
        async with store.connection() as conn:
            views = [r[0] for r in conn.execute(
                "SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()]
            for view in views:
                conn.execute(f"DROP VIEW IF EXISTS {view}")
            for table in ("manual_expenses", "offers", "offer_stocks", "stock_movements",
                          "sku_inventory_status", "inventory_sku_history", "inventory_history"):
                conn.execute(f"DROP TABLE IF EXISTS {table}")

        result = await replicate_operational(store)

        assert "error" not in result, result
        assert result["movements_appended"] is None and result["sku_history_appended"] is None
