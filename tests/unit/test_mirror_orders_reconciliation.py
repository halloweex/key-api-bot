"""Reconciliation A for orders: fingerprint first, drill down second.

The catalogue is compared by pulling both sides whole. Orders are 46,487 rows
and 147,648 line items, and materialising 194,000 tuples from two databases
every morning, in a container that already gives DuckDB a 4 GB ceiling, is a
memory incident waiting for a busy day. So both sides are folded into one
fingerprint per 1,000-id bucket, and only the buckets that disagree are opened.

Two things here are worth more than the plumbing.

The fingerprint that DuckDB computes in SQL is checked against the same
fingerprint computed independently in Python — if the SQL and the definition
ever part company, the comparison silently stops comparing.

And the gate: `backfilled_at`. `last_ok_at` licenses a tolerance of zero for
the catalogue because a catalogue mirror ships the whole table. The orders
mirror ships a delta, so until history has been carried across, every order
older than the mirror looks exactly like a lost one.
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
    ORDER_TABLES,
    _Divergence,
    compare_bucket,
    disagreeing_buckets,
    duckdb_fingerprint_sql,
    fingerprints,
    postgres_fingerprint_sql,
    reconcile_orders,
)

ORDERS, LINE_ITEMS = ORDER_TABLES
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
WHEN = "2026-08-20T12:00:00+00:00"


def _payload(order_id, *, total="100.00", products=2, comment=None):
    return {
        "id": order_id, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": total, "ordered_at": WHEN, "created_at": WHEN,
        "updated_at": WHEN, "buyer": {"id": 500 + order_id},
        "manager": {"id": 4}, "manager_comment": comment, "promocode": None,
        "products": [
            {"name": f"Товар {i}", "quantity": i + 1, "price_sold": "50.00",
             "offer": {"product_id": 700 + i}}
            for i in range(products)
        ],
    }


def _epoch_us(value):
    if value is None:
        return 0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (value - EPOCH) // timedelta(microseconds=1)


def python_fingerprint(spec, rows):
    """The fingerprint's definition, in Python, owing nothing to either SQL.

    `rows` is {id: tuple in spec.columns order}. Buckets come from the same
    column the SQL groups on.
    """
    bucket_at = spec.columns.index(spec.bucket_column)
    out = {}
    for row in rows.values():
        bucket = int(row[bucket_at]) // BUCKET_SIZE
        acc = out.setdefault(bucket, [0] + [Decimal(0)] * len(spec.fields))
        acc[0] += 1
        for n, (column, kind) in enumerate(spec.fields, start=1):
            value = row[spec.columns.index(column)]
            if kind == "ts":
                acc[n] += _epoch_us(value)
            elif kind == "text":
                acc[n] += len(value or "")
            else:
                acc[n] += Decimal(str(value or 0))
    return {b: tuple(v) for b, v in out.items()}


async def _store(tmp_path: Path, ids, **kw) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "recon.duckdb")
    await store.connect()
    with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
        await store.upsert_orders([_payload(i, **kw) for i in ids])
    # `synced_at` defaults to the wall clock, and the grace window is measured
    # against it. Pinning it a day before `NOW` keeps every row out of the
    # in-flight window no matter what time the suite runs at.
    async with store.connection() as conn:
        conn.execute("UPDATE orders SET synced_at = ?", [NOW - timedelta(days=1)])
    return store


def _read_dk(store_conn, spec):
    """Both stores' shared columns, as the mirror would have shipped them."""
    from core.mirror_reconciliation import _normalise_row

    cols = ", ".join(spec.columns)
    rows = store_conn.execute(f"SELECT {cols} FROM {spec.dk_table}").fetchall()
    return {int(r[0]): _normalise_row(r, spec.columns, spec.numeric) for r in rows}


class TestTheFingerprint:
    @pytest.mark.asyncio
    async def test_duckdb_agrees_with_the_definition(self, tmp_path):
        """If the SQL and the definition part company, the check stops
        checking and says nothing."""
        store = await _store(tmp_path, [1, 2, 1500, 1501], comment="utm_source=ig")
        async with store.connection() as conn:
            for spec in ORDER_TABLES:
                assert fingerprints(conn, spec) == python_fingerprint(
                    spec, _read_dk(conn, spec)
                )

    @pytest.mark.asyncio
    async def test_it_buckets_by_a_thousand_ids(self, tmp_path):
        store = await _store(tmp_path, [1, 999, 1000, 2500])
        async with store.connection() as conn:
            print_ = fingerprints(conn, ORDERS)
        assert sorted(print_) == [0, 1, 2]
        assert print_[0][0] == 2      # ids 1 and 999
        assert print_[1][0] == 1
        assert print_[2][0] == 1

    @pytest.mark.asyncio
    async def test_line_items_bucket_by_their_order(self, tmp_path):
        """So an order and its items always land in the same bucket."""
        store = await _store(tmp_path, [1, 1500])
        async with store.connection() as conn:
            print_ = fingerprints(conn, LINE_ITEMS)
        assert sorted(print_) == [0, 1]

    def test_the_two_dialects_ask_the_same_question(self):
        dk, pg = duckdb_fingerprint_sql(ORDERS), postgres_fingerprint_sql(ORDERS)
        # One COUNT and one SUM per column, on both sides.
        assert dk.count("SUM(") == pg.count("SUM(") == len(ORDERS.fields)
        # Timestamps as integer microseconds, never as a float epoch.
        assert "epoch_us(ordered_at)" in dk
        assert "(EXTRACT(EPOCH FROM ordered_at) * 1000000)::bigint" in pg
        # Text by length, which is what makes an equal-length edit invisible.
        assert "SUM(LENGTH(COALESCE(manager_comment, '')))" in dk


class TestWhichBucketsGetOpened:
    def test_agreement_opens_nothing(self):
        both = {0: (2, Decimal(3)), 1: (1, Decimal(9))}
        assert disagreeing_buckets(both, dict(both)) == []

    def test_a_moved_value_opens_its_bucket_only(self):
        dk = {0: (2, Decimal(3)), 1: (1, Decimal(9))}
        pg = {0: (2, Decimal(3)), 1: (1, Decimal(8))}
        assert disagreeing_buckets(dk, pg) == [1]

    def test_a_bucket_missing_from_one_side_counts(self):
        assert disagreeing_buckets({0: (1,), 5: (1,)}, {0: (1,)}) == [5]
        assert disagreeing_buckets({0: (1,)}, {0: (1,), 5: (1,)}) == [5]

    def test_an_int_and_a_decimal_of_equal_value_agree(self):
        """DuckDB returns HUGEINT where asyncpg returns Decimal."""
        assert disagreeing_buckets({0: (2, 5)}, {0: (2, Decimal(5))}) == []


class TestTheDrillDown:
    def _rows(self, ids):
        return {i: (i, 1, 12, 4, Decimal("100.00")) for i in ids}

    def test_a_row_only_in_duckdb_is_missing(self):
        found = _Divergence()
        compare_bucket(
            ORDERS, self._rows([1, 2]), {1: NOW - timedelta(days=1)},
            self._rows([1]), found, now=NOW, grace_minutes=15,
        )
        assert found.missing == [2] and not found.orphans

    def test_a_row_written_seconds_ago_is_in_flight_not_missing(self):
        found = _Divergence()
        compare_bucket(
            ORDERS, self._rows([1, 2]), {2: NOW - timedelta(minutes=2)},
            self._rows([1]), found, now=NOW, grace_minutes=15,
        )
        assert found.missing == []

    def test_a_row_only_in_postgres_is_an_orphan(self):
        found = _Divergence()
        compare_bucket(
            ORDERS, self._rows([1]), {}, self._rows([1, 7]), found,
            now=NOW, grace_minutes=15,
        )
        assert found.orphans == [7]

    def test_a_disagreement_names_the_column(self):
        dk = {1: (1, 1, 12, 4, Decimal("100.00"))}
        pg = {1: (1, 1, 20, 4, Decimal("100.00"))}
        found = _Divergence()
        compare_bucket(ORDERS, dk, {1: NOW - timedelta(days=1)}, pg, found,
                       now=NOW, grace_minutes=15)
        assert found.differing == [1]
        assert found.offenders == {"status_id": 1}

    def test_a_status_change_is_seen_even_though_updated_at_did_not_move(self):
        """KeyCRM does not bump `updated_at` on a status change — which is why
        the comparison is not keyed on it."""
        stamp = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
        dk = {1: (1, 1, 12, 4, Decimal("100.00"), stamp)}
        pg = {1: (1, 1, 20, 6, Decimal("100.00"), stamp)}
        found = _Divergence()
        compare_bucket(ORDERS, dk, {1: NOW - timedelta(days=1)}, pg, found,
                       now=NOW, grace_minutes=15)
        assert set(found.offenders) == {"status_id", "status_group_id"}


class TestTheBackfillGate:
    """`last_ok_at` cannot license a tolerance of zero for a delta mirror."""

    def _pg_side(self, dk_conn, *, drop=()):
        rows = {
            spec.pg_table: _read_dk(dk_conn, spec) for spec in ORDER_TABLES
        }
        for spec_table, ids in drop:
            for i in ids:
                rows[spec_table].pop(i, None)
        return rows

    async def _run(self, store, *, watermarks, pg_rows, **kw):
        async def _fp(pool, spec):
            return python_fingerprint(spec, pg_rows[spec.pg_table])

        async def _bucket(pool, spec, bucket):
            at = spec.columns.index(spec.bucket_column)
            return {
                i: r for i, r in pg_rows[spec.pg_table].items()
                if int(r[at]) // BUCKET_SIZE == bucket
            }

        with patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value=watermarks)), \
             patch("core.mirror_reconciliation.pg_fingerprints", new=_fp), \
             patch("core.mirror_reconciliation._read_pg_bucket", new=_bucket):
            return await reconcile_orders(store, now=NOW, **kw)

    def _marks(self, *, backfilled=True):
        stamp = NOW - timedelta(hours=2)
        return {
            spec.pg_table: {
                "table_name": spec.pg_table, "last_ok_at": stamp,
                "last_attempted_at": stamp, "failures_since_ok": 0,
                "last_error": None, "last_rows": 1,
                "backfilled_at": stamp if backfilled else None,
            }
            for spec in ORDER_TABLES
        }

    @pytest.mark.asyncio
    async def test_without_a_backfill_it_reports_the_gap_not_the_rows(self, tmp_path):
        """Otherwise the first run files 46,000 CRITICALs for a job nobody ran."""
        store = await _store(tmp_path, list(range(1, 30)))
        async with store.connection() as conn:
            pg_rows = self._pg_side(conn, drop=[("bronze.orders", range(1, 25))])

        issues = await self._run(
            store, watermarks=self._marks(backfilled=False), pg_rows=pg_rows,
        )
        names = {i.check_name for i in issues}
        assert "mirror_backfill_pending" in names
        assert "mirror_missing_rows" not in names
        pending = next(i for i in issues if i.check_name == "mirror_backfill_pending")
        assert pending.count == 24

    @pytest.mark.asyncio
    async def test_with_a_backfill_two_identical_stores_are_silent(self, tmp_path):
        store = await _store(tmp_path, list(range(1, 30)))
        async with store.connection() as conn:
            pg_rows = self._pg_side(conn)

        assert await self._run(
            store, watermarks=self._marks(), pg_rows=pg_rows,
        ) == []

    @pytest.mark.asyncio
    async def test_with_a_backfill_a_lost_order_is_critical(self, tmp_path):
        store = await _store(tmp_path, list(range(1, 30)))
        async with store.connection() as conn:
            pg_rows = self._pg_side(conn, drop=[("bronze.orders", [17])])

        issues = await self._run(store, watermarks=self._marks(), pg_rows=pg_rows)
        found = next(i for i in issues if i.check_name == "mirror_missing_rows")
        assert found.table_name == "bronze.orders"
        assert found.sample_ids == (17,)

    @pytest.mark.asyncio
    async def test_a_dropped_line_item_is_found_too(self, tmp_path):
        """The failure a plain upsert would have left behind, in reverse."""
        store = await _store(tmp_path, [1, 2, 3])
        async with store.connection() as conn:
            pg_rows = self._pg_side(conn, drop=[("bronze.order_products", [2001])])

        issues = await self._run(store, watermarks=self._marks(), pg_rows=pg_rows)
        found = next(
            i for i in issues
            if i.check_name == "mirror_missing_rows"
            and i.table_name == "bronze.order_products"
        )
        assert found.sample_ids == (2001,)

    @pytest.mark.asyncio
    async def test_a_whole_table_apart_is_one_finding_not_a_scan(self, tmp_path):
        store = await _store(tmp_path, [i * 1000 for i in range(1, 8)])
        async with store.connection() as conn:
            pg_rows = self._pg_side(conn)
        pg_rows["bronze.orders"] = {}

        issues = await self._run(
            store, watermarks=self._marks(), pg_rows=pg_rows, max_buckets=2,
        )
        found = next(i for i in issues if i.check_name == "mirror_buckets_disagree")
        assert found.count == 7
        assert found.severity.value == "CRITICAL"

    @pytest.mark.asyncio
    async def test_a_failing_mirror_is_reported_before_any_of_this(self, tmp_path):
        store = await _store(tmp_path, [1])
        async with store.connection() as conn:
            pg_rows = self._pg_side(conn)
        marks = self._marks()
        marks["bronze.orders"]["failures_since_ok"] = 4
        marks["bronze.orders"]["last_error"] = "UniqueViolationError"

        issues = await self._run(store, watermarks=marks, pg_rows=pg_rows)
        found = next(i for i in issues if i.check_name == "mirror_failing")
        assert found.count == 4 and "UniqueViolation" in found.description


class TestTheGateIsActuallyReadable:
    """A gate the check cannot see is a check that never runs."""

    def test_the_watermark_query_selects_backfilled_at(self):
        """Left out of the SELECT, `backfilled_at` reads None forever and the
        row-level comparison is suppressed permanently — silently, and looking
        exactly like a healthy PASS."""
        import inspect

        from core.mirror_reconciliation import fetch_watermarks

        assert "backfilled_at" in inspect.getsource(fetch_watermarks)

    def test_the_migration_adds_it_nullable(self):
        import importlib.util
        import inspect
        from pathlib import Path as _Path

        repo = _Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location(
            "_rev0004", repo / "migrations" / "versions" / "0004_backfilled_at.py"
        )
        rev = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rev)
        up = inspect.getsource(rev.upgrade)

        assert "ADD COLUMN backfilled_at TIMESTAMPTZ" in up
        # No DEFAULT: a default would backdate every existing table into
        # "history has been carried across" without anything having carried it.
        assert "DEFAULT" not in up
        assert rev.down_revision == "0003_landing_orders"

    def test_the_backfill_is_what_writes_it(self):
        import inspect

        from core import pg_backfill

        source = inspect.getsource(pg_backfill.backfill_orders)
        assert "if remaining == 0:" in source
        assert "_mark_backfilled" in source
