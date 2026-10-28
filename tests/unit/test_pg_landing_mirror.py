"""The mirror, and the one property everything else depends on: it never raises.

Exercised against a real server before this was written — postgres:17.2-alpine
with the real initdb scripts, revision 0002 applied as `ks_app`:

  write        2 categories and 2 products land, Cyrillic and NULLs intact,
               price back as Decimal('700.00'); watermark ok, last_rows 2
  idempotent   same payload again leaves 2 rows and updates a changed name
  bad data     `name = NULL` -> NotNullViolationError, no exception escapes,
               0 rows left behind (the transaction rolled back), watermark
               shows failures_since_ok = 1 with the message
  unreachable  ConnectionRefusedError, same handling
  kill switch  KS_MIRROR_LANDING=0 skips without touching Postgres

What the suite pins is the behaviour that has no server: the flag, the
statement, and that every path returns an outcome instead of throwing.

Since step 14's S14-00 the flag is a constant (on) and every table the mirror
ships is a write chain's, so it stands down for all of them; the tests of the
kill switch, of a failure becoming an outcome, of the failure counts and of
the parse through the mirror went with that path (S14-13 deletes the mirror).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from core import pg_landing
from core.pg_landing import (
    MIRROR_ENV,
    enabled,
    health,
    mirror_categories,
    reset_health,
)


@pytest.fixture(autouse=True)
def _clean():
    reset_health()
    yield
    reset_health()


class TestTheKillSwitch:
    def test_it_is_on_by_default(self, monkeypatch):
        """Off by default would ship dead code, which this repository has a
        scar from: a Redis client lived here for months without executing in
        any environment and was then blamed for the dashboard's latency."""
        monkeypatch.delenv(MIRROR_ENV, raising=False)
        assert enabled() is True


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


class TestItNeverRaises:
    """The property the failure policy is built on. Charter rule 8: a Postgres
    fault must not be able to stop a sync of the store the business reads."""

    @pytest.mark.asyncio
    async def test_a_failure_to_record_the_failure_is_also_swallowed(self, monkeypatch):
        """If Postgres is down, the attempt to write "Postgres is down" fails
        too. That must not be the thing that finally raises."""
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", side_effect=ConnectionRefusedError()), \
             patch("core.pg.get_pool", side_effect=ConnectionRefusedError()):
            out = await mirror_categories([{"id": 1, "name": "X"}])
        assert out.ok is False

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


class TestFailuresAreVisible:
    def test_health_is_readable_with_postgres_down(self):
        """It is process-local on purpose — the durable record is the
        watermark, and the watermark is unreachable exactly when it matters."""
        assert set(health()) == {"enabled", "failures", "last_error"}


class TestTheSyncCallsIt:
    def test_all_three_catalogue_sites_mirror(self):
        import inspect
        from core import sync_service

        src = inspect.getsource(sync_service)
        assert src.count("await mirror_categories(") == 1
        assert src.count("await mirror_products(") == 2, (
            "full sync and the hourly incremental both write products"
        )
