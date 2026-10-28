"""Comparing two *computations* of Silver, not two copies of a payload.

Everything else in `mirror_reconciliation` compares a row that was written
twice. This compares `silver_select_sql(POSTGRES)` over `bronze.orders` against
the same projection over DuckDB's own landing — which is the thing the parallel
period exists to prove, and the only check that would notice the two engines
disagreeing about a rule rather than about a byte.

Since step 14's S14-00 Postgres alone derives Silver (step 13, a constant), so
the comparison stands down and DuckDB writes no Silver to compare: the tests
that built one went with it (S14-12 deletes the comparison).
"""
from __future__ import annotations

from datetime import datetime, timezone
from core.mirror_reconciliation import (
    SILVER_GRACE_MINUTES,
    SILVER_TABLES,
    duckdb_fingerprint_sql,
    postgres_fingerprint_sql,
)

SPEC = SILVER_TABLES[0]
NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
WHEN = "2026-08-20T12:00:00+00:00"


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


