"""What is left of `core.pg_landing` after the mirror: the statement chain
3's `_write_rows` runs, and the failure stamp every writer shares.

Exercised against a real server before this was written — postgres:17.2-alpine
with the real initdb scripts, revision 0002 applied as `ks_app`.

Since step 14's S14-13 the mirror itself (`_mirror`, `mirror_categories`,
`mirror_products`, its process-local health and the sync's two call sites)
is gone, and with it the tests of its kill switch, of a failure becoming an
outcome and of the sync calling it. The statement and `_record_failure`
stay, byte for byte, because the write chains run them.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from core import pg_landing
from core.landing_rows import EXPENSE_COLUMNS
from tests.helpers.recon_clickhouse import kept_mirror_state
from tests.integration.pg_tab_seed import DSN, needs_postgres

# A synthetic expense id no fixture uses.
EXPENSE_ID = 990_771


# `TestTheConstant` held `enabled()` and `mirror_on()` to a constant True for
# any `KS_MIRROR_LANDING` until step 14's S14-15 deleted both with their last
# consumer, step 13's `evaluate_preconditions` (rule 16).


class TestTheStatement:
    def test_it_upserts_on_the_primary_key(self):
        sql = pg_landing._statement("bronze.categories", ("id", "name", "parent_id"))
        assert "INSERT INTO bronze.categories (id, name, parent_id)" in sql
        assert "VALUES ($1, $2, $3)" in sql
        assert "ON CONFLICT (id) DO UPDATE" in sql

    def test_the_key_is_not_assigned_to_itself(self):
        sql = pg_landing._statement("bronze.products", ("id", "name"))
        assert "id = EXCLUDED.id" not in sql
        assert "name = EXCLUDED.name" in sql

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_row_written_again_carries_the_second_write_s_instant(self):
        """`mirrored_at` is this copy's own fact — the instant of the write
        that last carried the row, which the chain invariants read
        (`_EXPENSE_ORPHANS_SQL`) — so writing a row again moves it, with the
        row's values, instead of leaving the instant it first arrived."""
        row = dict.fromkeys(EXPENSE_COLUMNS)
        row.update(id=EXPENSE_ID, order_id=990_770, expense_type_id=1,
                   amount=Decimal("10.50"), status="paid")
        conn = await asyncpg.connect(DSN)
        try:
            async with kept_mirror_state("bronze.expenses"):
                await conn.execute("DELETE FROM bronze.expenses WHERE id = $1", EXPENSE_ID)
                try:
                    written = []
                    for amount in (Decimal("10.50"), Decimal("12.00")):
                        row["amount"] = amount
                        async with conn.transaction():   # the caller's, as chain 3's
                            await pg_landing._write_rows(
                                conn, "bronze.expenses", EXPENSE_COLUMNS,
                                [tuple(row[c] for c in EXPENSE_COLUMNS)])
                        written.append(tuple(await conn.fetchrow(
                            "SELECT amount, mirrored_at FROM bronze.expenses WHERE id = $1",
                            EXPENSE_ID)))
                finally:
                    await conn.execute("DELETE FROM bronze.expenses WHERE id = $1", EXPENSE_ID)
        finally:
            await conn.close()
        (first_amount, first_at), (second_amount, second_at) = written
        assert (first_amount, second_amount) == (Decimal("10.50"), Decimal("12.00"))
        assert second_at > first_at


class TestTheFailureStamp:
    """`_record_failure`, which every writer that stamps `meta.mirror_state`
    shares (chain 3's order step, the sync's catalogue steps, the derived
    layers): best effort, and it says whether it was written."""

    @pytest.mark.asyncio
    async def test_a_failure_that_could_not_be_written_says_so(self):
        """A caller stamping many tables stops at the first stamp that could
        not be written, so the answer must be False only when it was not."""
        with patch("core.pg.get_pool", side_effect=ConnectionRefusedError()):
            assert await pg_landing._record_failure("app.stock_movements", "boom") is False

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_failure_written_is_counted_beside_the_last_success(self):
        """Written, it says True, and the state row reads failing: the count
        goes up by one per failure, the error is the latest, and the last
        success stays where it was — the watermark no longer reads healthy."""
        table = "app.stock_movements"
        succeeded = datetime.now(timezone.utc) - timedelta(hours=3)
        async with kept_mirror_state(table):
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
            try:
                await pool.execute(
                    "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at,"
                    " failures_since_ok, last_rows) VALUES ($1, $2, $2, 0, 5)", table, succeeded)
                with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
                    written = [await pg_landing._record_failure(table, error)
                               for error in ("boom", "boom again")]
                state = await pool.fetchrow(
                    "SELECT last_ok_at, failures_since_ok, last_error, last_attempted_at"
                    " FROM meta.mirror_state WHERE table_name = $1", table)
            finally:
                await pool.close()
        assert written == [True, True]
        assert (state["last_ok_at"], state["failures_since_ok"], state["last_error"]) == (
            succeeded, 2, "boom again")
        assert state["last_attempted_at"] > succeeded
