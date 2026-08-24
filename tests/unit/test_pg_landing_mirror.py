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
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from core import pg_landing
from core.pg_landing import (
    MIRROR_ENV,
    MirrorOutcome,
    enabled,
    health,
    mirror_categories,
    mirror_products,
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

    @pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", ""])
    def test_it_can_be_turned_off_in_one_deploy(self, monkeypatch, value):
        monkeypatch.setenv(MIRROR_ENV, value)
        assert enabled() is False

    @pytest.mark.asyncio
    async def test_off_means_postgres_is_not_touched(self, monkeypatch):
        monkeypatch.setenv(MIRROR_ENV, "0")
        with patch("core.pg.get_pool", new=AsyncMock()) as pool:
            out = await mirror_categories([{"id": 1, "name": "Care"}])
        pool.assert_not_awaited()
        assert out.ok is False and out.attempted is False
        assert MIRROR_ENV in out.skipped


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
    @pytest.mark.parametrize("boom", [
        ConnectionRefusedError("no route"),
        RuntimeError("KS_PG_DSN is not set"),
        Exception("something nobody predicted"),
    ])
    async def test_any_failure_becomes_an_outcome(self, monkeypatch, boom):
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", side_effect=boom), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            out = await mirror_products([{"id": 1, "name": "X"}])
        assert isinstance(out, MirrorOutcome)
        assert out.ok is False
        assert out.attempted is True
        assert str(boom)[:10] in out.error

    @pytest.mark.asyncio
    async def test_a_failure_to_record_the_failure_is_also_swallowed(self, monkeypatch):
        """If Postgres is down, the attempt to write "Postgres is down" fails
        too. That must not be the thing that finally raises."""
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", side_effect=ConnectionRefusedError()), \
             patch("core.pg.get_pool", side_effect=ConnectionRefusedError()):
            out = await mirror_categories([{"id": 1, "name": "X"}])
        assert out.ok is False


class TestFailuresAreVisible:
    @pytest.mark.asyncio
    async def test_they_are_counted_per_table(self, monkeypatch):
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", side_effect=RuntimeError("nope")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            await mirror_categories([{"id": 1, "name": "X"}])
            await mirror_categories([{"id": 1, "name": "X"}])
            await mirror_products([{"id": 1, "name": "Y"}])
        assert health()["failures"] == {
            "bronze.categories": 2, "bronze.products": 1,
        }

    @pytest.mark.asyncio
    async def test_a_success_clears_the_count(self, monkeypatch):
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", side_effect=RuntimeError("nope")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            await mirror_categories([{"id": 1, "name": "X"}])
        assert health()["failures"]["bronze.categories"] == 1

        with patch("core.pg_landing._write", new=AsyncMock()):
            out = await mirror_categories([{"id": 1, "name": "X"}])
        assert out.ok is True
        assert health()["failures"] == {}

    @pytest.mark.asyncio
    async def test_the_error_is_logged_at_error_not_debug(self, monkeypatch, caplog):
        """The 2026-08-09 shape: an ALTER failed inside try/except with a
        DEBUG log, and a one-off became a rebuild every two minutes."""
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", side_effect=RuntimeError("nope")), \
             patch("core.pg_landing._record_failure", new=AsyncMock()):
            with caplog.at_level("ERROR", logger="core.pg_landing"):
                await mirror_products([{"id": 1, "name": "X"}])
        assert any(r.levelname == "ERROR" for r in caplog.records)

    def test_health_is_readable_with_postgres_down(self):
        """It is process-local on purpose — the durable record is the
        watermark, and the watermark is unreachable exactly when it matters."""
        assert set(health()) == {"enabled", "failures", "last_error"}


class TestItReadsThePayloadThroughTheSharedParser:
    @pytest.mark.asyncio
    async def test_brand_comes_from_landing_rows(self, monkeypatch):
        """Not a second reading. Rule 1 — and brand is the column the owner's
        supplier figures are built on."""
        monkeypatch.setenv(MIRROR_ENV, "1")
        captured = {}

        async def _capture(table, columns, rows):
            captured["table"], captured["cols"], captured["rows"] = table, columns, rows

        with patch("core.pg_landing._write", new=_capture):
            await mirror_products([{
                "id": 100, "name": "Serum", "min_price": 0, "price": 700.0,
                "custom_fields": [{"uuid": "CT_1001", "value": ["NEOGEN", "ANUA"]}],
            }])

        assert captured["table"] == "bronze.products"
        assert captured["cols"] == ("id", "name", "category_id", "brand", "sku", "price")
        # `value[0]` and `min_price or price`, exactly as DuckDB gets them.
        assert captured["rows"] == [(100, "Serum", None, "NEOGEN", None, 700.0)]

    @pytest.mark.asyncio
    async def test_an_empty_payload_still_moves_the_watermark(self, monkeypatch):
        """A sync that found nothing is a successful sync, and a watermark
        that stops moving on quiet days reads as a stalled mirror."""
        monkeypatch.setenv(MIRROR_ENV, "1")
        with patch("core.pg_landing._write", new=AsyncMock()) as w:
            out = await mirror_categories([])
        assert out.ok is True and out.rows == 0
        w.assert_awaited_once()


class TestTheSyncCallsIt:
    def test_all_three_catalogue_sites_mirror(self):
        import inspect
        from core import sync_service

        src = inspect.getsource(sync_service)
        assert src.count("await mirror_categories(") == 1
        assert src.count("await mirror_products(") == 2, (
            "full sync and the hourly incremental both write products"
        )
