"""gold_daily_traffic must be rebuilt for the dates that moved, not all of them.

The call site used to pass affected_dates=None unconditionally, so every one of
~240 warehouse refreshes a day rewrote the whole table. DuckDB cannot reclaim
the row versions that leaves while a writer is live, and the table reached 3.86M
stored rows behind 5 781 live ones — 667x amplification, and the largest single
contributor to the ~90 MB a day the database file grew.

The reason given for the full rebuild was real: UTM parsing can touch dates
outside affected_dates. These tests pin the answer to it — ask which ones.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore, UTM_DATE_LOOKUP_LIMIT


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "traffic.duckdb")
    await s.connect()
    return s


async def _silver_row(store: DuckDBStore, order_id: int, day: date) -> None:
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES (?, 1, 12, 100.0, NULL, NULL, NULL, ?, FALSE, 'retail',
                      TRUE, 'Instagram', FALSE, NULL, NULL)
            """,
            [order_id, day],
        )


# `TestTrafficRebuildScope` lived here and is gone with `gold_daily_traffic`.
#
# It pinned `_traffic_rebuild_dates`, the helper that worked out which dates
# the traffic Gold had to be rebuilt for — machinery that existed because that
# layer's full DELETE+INSERT on every two-minute tick was the single largest
# contributor to the database file's growth. The layer had no reader left once
# `/traffic` moved to Postgres, so it was retired, and the cheapest rebuild of
# a table nobody reads is no rebuild.
#
# The two classes below are unrelated and stay: one is the catalogue dirty
# flag, the other is `gold_daily_products` carrying no index.



class TestCatalogDirtyScope:
    """A catalog change widens one scope, not four.

    silver_orders reads only `orders`; gold_daily_revenue reads only
    silver_orders; gold_daily_traffic reads silver_orders and silver_order_utm.
    None of the three can be changed by a product rename. Only
    gold_daily_products joins products and categories.
    """

    @pytest.mark.asyncio
    async def test_the_flag_survives_until_a_refresh_consumes_it(self, tmp_path):
        store = await _make_store(tmp_path)

        assert await store._consume_catalog_dirty() is False

        await store.mark_catalog_dirty()
        assert await store._consume_catalog_dirty() is True

        # Consumed once, gone. Otherwise every later refresh would keep
        # widening its scope for a rename that already landed.
        assert await store._consume_catalog_dirty() is False

    @pytest.mark.asyncio
    async def test_marking_twice_still_consumes_once(self, tmp_path):
        store = await _make_store(tmp_path)
        await store.mark_catalog_dirty()
        await store.mark_catalog_dirty()
        assert await store._consume_catalog_dirty() is True
        assert await store._consume_catalog_dirty() is False

    @pytest.mark.asyncio
    async def test_catalog_and_order_dirtiness_are_independent(self, tmp_path):
        """Two axes, two flags. Consuming one must not clear the other."""
        store = await _make_store(tmp_path)

        await store.mark_catalog_dirty()
        await store.mark_warehouse_dirty([11, 22])

        assert await store._consume_catalog_dirty() is True

        is_dirty, ids = await store.consume_warehouse_dirty()
        assert is_dirty is True
        assert sorted(ids) == [11, 22], (
            "consuming the catalog flag swallowed the order scope"
        )


class TestGoldProductsCarriesNoIndex:
    """One ART index costs exactly what six do, and these six bought nothing.

    In DuckDB 1.5.5 a single index — a PRIMARY KEY counts — disables row-group
    vacuuming for the whole table. Measured on a clone at the real size, a full
    DELETE+INSERT rebuild leaks 0.000 MB/cycle with no index and 1.171 MB with
    any number of them. At ~45 full rebuilds a day that was 53 MB/day, the
    largest single contributor to the growth behind the weekly compaction.

    Against the real query shapes the six saved nothing measurable: four of
    five unchanged, the fifth a tenth of a millisecond. 87 906 rows is nothing
    to a columnar scan.
    """

    @pytest.mark.asyncio
    async def test_a_fresh_database_creates_none(self, tmp_path):
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            rows = conn.execute(
                "SELECT index_name FROM duckdb_indexes() "
                "WHERE table_name = 'gold_daily_products'"
            ).fetchall()
        assert rows == [], (
            f"gold_daily_products grew an index again: {[r[0] for r in rows]}. "
            "Measure the rebuild cost and the query benefit on both sides "
            "before adding one — see the note in _init_schema."
        )

    @pytest.mark.asyncio
    async def test_an_existing_index_is_migrated_away(self, tmp_path):
        """A database created before this change must lose them on connect."""
        store = await _make_store(tmp_path)
        async with store.connection() as conn:
            conn.execute(
                "CREATE INDEX idx_gold_prod_brand ON gold_daily_products(brand)"
            )
            assert conn.execute(
                "SELECT COUNT(*) FROM duckdb_indexes() "
                "WHERE table_name = 'gold_daily_products'"
            ).fetchone()[0] == 1

        await store._run_migrations()

        async with store.connection() as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM duckdb_indexes() "
                "WHERE table_name = 'gold_daily_products'"
            ).fetchone()[0] == 0
