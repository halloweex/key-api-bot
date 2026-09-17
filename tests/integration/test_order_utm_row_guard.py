"""The UTM row guard against a real Postgres (DN-04).

`ship_order_utm` replaces `silver.order_utm` whole, so whatever DuckDB holds at
the read is what `/traffic` and the weekly traffic report read a transaction
later. A reclassify's DELETE with its re-parse still running, the same in
`scripts/backfill_utm.py`, or a warehouse tick whose parse raised between two
write batches all leave DuckDB short — and a row missing in Postgres does not
read as missing: the order falls through the COALESCE to unattributed or
organic.

`tests/unit/test_order_utm_shipping.py` pins the rule and the wiring against a
fake pool. This proves the part a fake cannot: that a refusal leaves the real
table untouched, that `meta.mirror_state` counts it, and that the next honest
ship clears it through the same watermark path as ever.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import pg_order_utm
from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

TABLE = pg_order_utm.UTM_TABLE


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {TABLE}")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = $1", TABLE)


async def _pg_count(pool) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(f"SELECT count(*) FROM {TABLE}")


async def _watermark(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
            "FROM meta.mirror_state WHERE table_name = $1", TABLE)


async def _duckdb_holds(store, n: int) -> None:
    """Make DuckDB hold exactly ids 1..n — by deleting above n when it holds
    more, which is the shape a reclassify's DELETE leaves behind."""
    async with store.connection() as conn:
        conn.execute("DELETE FROM silver_order_utm WHERE order_id > ?", [n])
        conn.execute(
            "INSERT INTO silver_order_utm (order_id, utm_source, utm_medium, "
            "utm_campaign, traffic_type, platform, parsed_at) "
            "SELECT i, 'fbads', 'paid', 'spring', 'paid_confirmed', 'facebook', "
            "TIMESTAMPTZ '2026-09-07 06:00:00+00' FROM range(1, ?) t(i) "
            "WHERE i NOT IN (SELECT order_id FROM silver_order_utm)",
            [n + 1],
        )


@pytest_asyncio.fixture
async def stores(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "utm_guard.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield store, pool
    finally:
        await _clean(pool)
        await pool.close()
        await store.close()


async def _seed_postgres_with_1000(store, pool):
    """Through the shipper itself, so the starting watermark is the one a
    production ship leaves: `last_ok_at` set, `failures_since_ok` zero."""
    await _duckdb_holds(store, 1000)
    first = await pg_order_utm.ship_order_utm(store)
    assert first["rows"] == 1000, first
    assert await _pg_count(pool) == 1000
    wm = await _watermark(pool)
    assert wm["failures_since_ok"] == 0 and wm["last_ok_at"] is not None
    return wm


class TestAShrinkStaysInDuckDB:
    @pytest.mark.asyncio
    async def test_a_tenth_of_the_table_is_refused_and_counted(self, stores):
        store, pool = stores
        before = await _seed_postgres_with_1000(store, pool)

        await _duckdb_holds(store, 100)
        result = await pg_order_utm.ship_order_utm(store)

        assert result["duckdb_rows"] == 100
        assert result["postgres_rows"] == 1000
        assert result["refused"].startswith("refused:")
        assert await _pg_count(pool) == 1000, "the refusal wrote to Postgres"

        after = await _watermark(pool)
        assert after["failures_since_ok"] == 1
        assert "100" in after["last_error"] and "1000" in after["last_error"]
        # A refusal is a failure, not a success: the last good ship stands.
        assert after["last_ok_at"] == before["last_ok_at"]
        assert after["last_rows"] == 1000

    @pytest.mark.asyncio
    async def test_a_second_refusal_counts_again(self, stores):
        """`failures_since_ok` is what `mirror_failing` reports, so a guard
        that keeps refusing must keep counting."""
        store, pool = stores
        await _seed_postgres_with_1000(store, pool)
        await _duckdb_holds(store, 100)

        await pg_order_utm.ship_order_utm(store)
        await pg_order_utm.ship_order_utm(store)

        assert (await _watermark(pool))["failures_since_ok"] == 2
        assert await _pg_count(pool) == 1000

    @pytest.mark.asyncio
    async def test_within_the_floor_it_ships_and_clears_the_count(self, stores):
        store, pool = stores
        await _seed_postgres_with_1000(store, pool)

        await _duckdb_holds(store, 100)
        await pg_order_utm.ship_order_utm(store)          # refused, counted
        assert (await _watermark(pool))["failures_since_ok"] == 1

        await _duckdb_holds(store, 950)
        result = await pg_order_utm.ship_order_utm(store)

        assert result["rows"] == 950 and "refused" not in result
        assert await _pg_count(pool) == 950
        wm = await _watermark(pool)
        assert wm["failures_since_ok"] == 0
        assert wm["last_error"] is None
        assert wm["last_rows"] == 950


class TestAFailedParse:
    @pytest.mark.asyncio
    async def test_it_is_refused_until_forced(self, stores):
        """Driven through the real parser, not by setting the attribute: an
        order with a comment and no UTM row, and a parse that raises on it."""
        store, pool = stores
        await _seed_postgres_with_1000(store, pool)

        stamp = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, "
                "ordered_at, updated_at, buyer_id, manager_comment) "
                "VALUES (5001, 4, 1, 100, ?, ?, 10, ?)",
                [stamp, stamp, "utm_source: fbads; utm_campaign: spring"],
            )
        with patch.object(DuckDBStore, "_parse_utm_from_comment",
                          side_effect=RuntimeError("bad comment")):
            with pytest.raises(RuntimeError):
                await store.refresh_utm_silver_layer()
        assert store.last_utm_parse_error == "RuntimeError: bad comment"

        refused = await pg_order_utm.ship_order_utm(store)
        assert "refused" in refused
        assert refused["duckdb_rows"] == refused["postgres_rows"] == 1000
        wm = await _watermark(pool)
        assert wm["failures_since_ok"] == 1
        assert "bad comment" in wm["last_error"]

        forced = await pg_order_utm.ship_order_utm(store, force=True)
        assert forced["rows"] == 1000 and forced.get("forced") is True
        assert (await _watermark(pool))["failures_since_ok"] == 0


class TestForceAndFirstShip:
    @pytest.mark.asyncio
    async def test_force_ships_the_shrink(self, stores):
        store, pool = stores
        await _seed_postgres_with_1000(store, pool)

        await _duckdb_holds(store, 100)
        result = await pg_order_utm.ship_order_utm(store, force=True)

        assert result["rows"] == 100
        assert await _pg_count(pool) == 100
        wm = await _watermark(pool)
        assert wm["failures_since_ok"] == 0 and wm["last_rows"] == 100

    @pytest.mark.asyncio
    @pytest.mark.parametrize("n", [0, 7, 1000])
    async def test_an_empty_postgres_takes_any_first_ship(self, stores, n):
        """Nothing to protect yet, so DuckDB's count does not matter, zero
        included. The opposite case — Postgres full, DuckDB empty, which is a
        store's first tick after a Sunday compact if anything ships before the
        parse — is the tenth-of-the-table refusal above, taken to its end."""
        store, pool = stores
        await _duckdb_holds(store, n)

        result = await pg_order_utm.ship_order_utm(store)

        assert result["rows"] == n and "refused" not in result
        assert await _pg_count(pool) == n
        wm = await _watermark(pool)
        assert wm["failures_since_ok"] == 0 and wm["last_rows"] == n
