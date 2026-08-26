"""Comparing two *computations* of Silver, not two copies of a payload.

Everything else in `mirror_reconciliation` compares a row that was written
twice. This compares `silver_select_sql(POSTGRES)` over `bronze.orders` against
the same projection over DuckDB's own landing — which is the thing the parallel
period exists to prove, and the only check that would notice the two engines
disagreeing about a rule rather than about a byte.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_store import DuckDBStore
from core.mirror_reconciliation import (
    BUCKET_SIZE,
    SILVER_GRACE_MINUTES,
    SILVER_TABLES,
    disagreeing_buckets,
    duckdb_fingerprint_sql,
    fingerprints,
    postgres_fingerprint_sql,
    reconcile_silver,
)

SPEC = SILVER_TABLES[0]
NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
WHEN = "2026-08-20T12:00:00+00:00"


def _payload(order_id):
    return {
        "id": order_id, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": WHEN, "created_at": WHEN,
        "updated_at": WHEN, "buyer": {"id": 500 + order_id}, "manager": {"id": 4},
        "manager_comment": None, "promocode": None,
        "products": [{"name": "Товар", "quantity": 1, "price_sold": "50.00",
                      "offer": {"product_id": 700}}],
    }


async def _store(tmp_path: Path, ids):
    store = DuckDBStore(db_path=tmp_path / "silver.duckdb")
    await store.connect()
    with patch("core.pg_landing.mirror_orders", new=AsyncMock()), \
         patch("core.pg_replication.replicate_managers", new=AsyncMock()):
        await store.upsert_orders([_payload(i) for i in ids])
    await store.refresh_warehouse_layers(trigger="manual")
    async with store.connection() as conn:
        conn.execute("UPDATE orders SET synced_at = ?", [NOW - timedelta(days=1)])
    return store


class TestTheFingerprintCoversEverySilverColumn:
    def test_it_sums_all_fifteen(self):
        """A column left out of the fingerprint is a column the check cannot
        see change — and `sales_type` is one of them."""
        from core.pg_silver import SILVER_COLUMNS

        assert tuple(c for c, _ in SPEC.fields) == tuple(SILVER_COLUMNS)
        assert duckdb_fingerprint_sql(SPEC).count("SUM(") == 15

    def test_booleans_are_counted_not_rendered(self):
        """DuckDB writes `true` and Postgres writes `t`; summing their lengths
        would report a difference in every bucket forever."""
        for sql in (duckdb_fingerprint_sql(SPEC), postgres_fingerprint_sql(SPEC)):
            assert "SUM(CASE WHEN is_return THEN 1 ELSE 0 END)" in sql
            assert "LENGTH(COALESCE(is_return" not in sql

    def test_dates_are_epochs_on_both_sides(self):
        assert "epoch(order_date)::BIGINT" in duckdb_fingerprint_sql(SPEC)
        assert "EXTRACT(EPOCH FROM order_date)::bigint" in postgres_fingerprint_sql(SPEC)

    @pytest.mark.asyncio
    async def test_duckdb_computes_it_over_real_silver(self, tmp_path):
        store = await _store(tmp_path, [1, 2, 1500])
        async with store.connection() as conn:
            print_ = fingerprints(conn, SPEC)
        assert sorted(print_) == [0, 1]
        assert print_[0][0] == 2 and print_[1][0] == 1


class TestTheGraceWindowMatchesTheRebuildFloor:
    def test_it_is_wider_than_the_ten_minute_floor(self):
        """Postgres rebuilds Silver on a floor, so it is *legitimately* behind
        by up to that long. A fifteen-minute grace would report every order
        touched in the last ten minutes as a defect."""
        assert SILVER_GRACE_MINUTES == 20

    def test_the_grace_reads_the_orders_timestamp(self):
        """Silver carries no timestamp of its own on either side; a Silver row
        is only ever as fresh as the landing row it was projected from."""
        assert "LEFT JOIN orders o ON o.id = s.id" in SPEC.dk_rows_sql
        assert "o.synced_at" in SPEC.dk_rows_sql


class TestWhatItFinds:
    @staticmethod
    def _fingerprint(spec, rows):
        """The fingerprint's definition in Python, owing nothing to either SQL.

        Local rather than shared with the orders test: Silver adds booleans and
        dates, and a helper that grew a branch per caller stops being an
        independent definition of anything."""
        from datetime import date as _date

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        out = {}
        for row in rows.values():
            bucket = int(row[0]) // BUCKET_SIZE
            acc = out.setdefault(bucket, [0] + [Decimal(0)] * len(spec.fields))
            acc[0] += 1
            for n, (column, kind) in enumerate(spec.fields, start=1):
                v = row[spec.columns.index(column)]
                if kind == "bool":
                    acc[n] += 1 if v else 0
                elif kind == "date":
                    acc[n] += 0 if v is None else int(
                        (datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
                         - epoch).total_seconds())
                elif kind == "ts":
                    if v is None:
                        continue
                    v = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
                    acc[n] += (v - epoch) // timedelta(microseconds=1)
                elif kind == "text":
                    acc[n] += len(v or "")
                else:
                    acc[n] += Decimal(str(v or 0))
        return {b: tuple(v) for b, v in out.items()}

    async def _run(self, store, pg_rows, watermarks):
        async def _fp(pool, spec):
            return self._fingerprint(spec, pg_rows)

        async def _bucket(pool, spec, bucket):
            return {i: r for i, r in pg_rows.items() if int(r[0]) // BUCKET_SIZE == bucket}

        with patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value=watermarks)), \
             patch("core.mirror_reconciliation.pg_fingerprints", new=_fp), \
             patch("core.mirror_reconciliation._read_pg_bucket", new=_bucket):
            return await reconcile_silver(store, now=NOW)

    def _pg(self, conn):
        from core.mirror_reconciliation import _normalise_row

        cols = ", ".join(SPEC.columns)
        return {
            int(r[0]): _normalise_row(r, SPEC.columns, SPEC.numeric)
            for r in conn.execute(f"SELECT {cols} FROM silver_orders").fetchall()
        }

    def _marks(self, ok=True):
        stamp = NOW - timedelta(hours=2)
        return {"silver.orders": {
            "table_name": "silver.orders", "last_ok_at": stamp if ok else None,
            "last_attempted_at": stamp, "failures_since_ok": 0,
            "last_error": None, "last_rows": 3, "backfilled_at": None,
        }}

    @pytest.mark.asyncio
    async def test_two_identical_computations_are_silent(self, tmp_path):
        store = await _store(tmp_path, [1, 2, 3])
        async with store.connection() as conn:
            pg = self._pg(conn)
        assert await self._run(store, pg, self._marks()) == []

    @pytest.mark.asyncio
    async def test_a_never_rebuilt_postgres_reports_that_and_no_rows(self, tmp_path):
        """No backfill gate here — `rebuild_silver` writes the table whole, so
        the only question is whether it has ever run."""
        store = await _store(tmp_path, [1, 2, 3])
        issues = await self._run(store, {}, self._marks(ok=False))
        assert [i.check_name for i in issues] == ["mirror_never_shipped"]

    @pytest.mark.asyncio
    async def test_a_sales_type_that_differs_is_critical(self, tmp_path):
        """The column every dashboard endpoint filters on, and the one a
        divergence in the two renderings of the projection would move."""
        store = await _store(tmp_path, [1, 2, 3])
        async with store.connection() as conn:
            pg = self._pg(conn)
        victim = sorted(pg)[0]
        row = list(pg[victim])
        row[SPEC.columns.index("sales_type")] = "b2b"
        pg[victim] = tuple(row)

        issues = await self._run(store, pg, self._marks())
        found = next(i for i in issues if i.check_name == "mirror_row_values")
        assert found.sample_ids == (victim,)
        assert "sales_type" in found.description

    @pytest.mark.asyncio
    async def test_a_row_postgres_never_computed_is_missing(self, tmp_path):
        store = await _store(tmp_path, [1, 2, 3])
        async with store.connection() as conn:
            pg = self._pg(conn)
        gone = sorted(pg)[1]
        pg.pop(gone)

        issues = await self._run(store, pg, self._marks())
        found = next(i for i in issues if i.check_name == "mirror_missing_rows")
        assert found.sample_ids == (gone,)
