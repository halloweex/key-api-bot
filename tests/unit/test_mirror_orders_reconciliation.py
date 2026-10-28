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

Since step 14's S14-00 chain 3 writes the orders, a constant, so DuckDB holds
none to fingerprint and the comparison stands down: the tests that wrote
orders into DuckDB — the fingerprint against its definition and the backfill
gate — went with that path. The pure parts stay until S14-12.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from core.mirror_reconciliation import (
    ORDER_TABLES,
    _Divergence,
    compare_bucket,
    disagreeing_buckets,
    duckdb_fingerprint_sql,
    postgres_fingerprint_sql,
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


# The bucket is an expression per dialect from 2026-08-27, because
# `inventory_sku_history` buckets by the day rather than by an id. These two
# specs still divide an integer column, and this is the Python spelling of it —
# named here rather than read off the spec, so the reimplementation keeps
# owing nothing to the SQL it is checking.
_BUCKET_COLUMN = {"bronze.orders": "id", "bronze.order_products": "order_id"}


class TestTheFingerprint:
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
