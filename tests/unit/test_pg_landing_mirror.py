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

from unittest.mock import AsyncMock, patch

import pytest

from core import pg_landing


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

    def test_the_bookkeeping_column_is_refreshed(self):
        """`mirrored_at` is this copy's own fact, so the mirror sets it rather
        than leaving the value from whenever the row first arrived."""
        sql = pg_landing._statement("bronze.products", ("id", "name"))
        assert "mirrored_at = now()" in sql


class TestTheFailureStamp:
    """`_record_failure`, which every writer that stamps `meta.mirror_state`
    shares (chain 3's order step, the sync's catalogue steps, the derived
    layers): best effort, and it says whether it was written."""

    @pytest.mark.asyncio
    async def test_recording_a_failure_says_whether_it_was_written(self):
        """A caller stamping many tables stops at the first stamp that could
        not be written, so the answer must be False only when it was not."""
        with patch("core.pg.get_pool", side_effect=ConnectionRefusedError()):
            assert await pg_landing._record_failure("app.stock_movements", "boom") is False

        conn = AsyncMock()

        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        class _Pool:
            def acquire(self):
                return _Acquire()

        with patch("core.pg.get_pool", new=AsyncMock(return_value=_Pool())):
            assert await pg_landing._record_failure("app.stock_movements", "boom") is True
        assert conn.execute.await_args.args[1:] == ("app.stock_movements", "boom")
