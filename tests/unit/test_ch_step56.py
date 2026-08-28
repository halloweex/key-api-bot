"""Steps 5–6: silver into ClickHouse, Gold derived there, the archive inherited.

The load-bearing assertion in this file is the no-third-dialect contract:
`derive_gold_sql` must render every expression of `GOLD_MEASURES` verbatim —
the same text Postgres and DuckDB render — because that is what dissolves the
"рукописная проекция №3" objection. If someone rewrites a measure for
ClickHouse by hand, this test names the divergence before the daily
comparison has to.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from core import ch_history, ch_silver
from core.ch_silver import (
    SILVER_COLUMNS,
    _SILVER_DDL,
    compare_gold_cells,
    derive_gold_sql,
    parse_tsv,
    render_tsv,
)
from core.data_quality import Severity


def _silver_row(**overrides):
    base = {
        "id": 1001, "source_id": 1, "status_id": 20,
        "grand_total": Decimal("1834.50"),
        "ordered_at": datetime(2026, 8, 27, 12, 30, 5, tzinfo=timezone.utc),
        "buyer_id": 7, "manager_id": None,
        "order_date": date(2026, 8, 27), "is_return": False,
        "sales_type": "retail", "is_active_source": True,
        "source_name": "Instagram", "is_new_customer": True,
        "buyer_first_order_date": date(2026, 8, 27), "promocode": None,
    }
    base.update(overrides)
    return tuple(base[c] for c in SILVER_COLUMNS)


def _gold_cell(**overrides):
    base = {
        "date": date(2026, 8, 27), "sales_type": "retail", "source_id": 1,
        "revenue": Decimal("1834.50"), "orders_count": 1,
        "unique_customers": 1, "new_customers": 1, "returning_customers": 0,
        "returns_count": 0, "returns_revenue": Decimal("0.00"),
        "avg_order_value": Decimal("1834.50"),
    }
    base.update(overrides)
    order = ("date", "sales_type", "source_id", "revenue", "orders_count",
             "unique_customers", "new_customers", "returning_customers",
             "returns_count", "returns_revenue", "avg_order_value")
    return tuple(base[c] for c in order)


class TestSilverWireFormat:
    def test_round_trip_is_identity(self):
        rows = [
            _silver_row(),
            _silver_row(id=1002, is_return=True, buyer_id=None,
                        promocode="WELCOME", ordered_at=None),
            _silver_row(id=1003, sales_type="exhibition", source_id=5,
                        is_new_customer=False),
        ]
        assert parse_tsv(render_tsv(rows).decode("utf-8")) == rows

    def test_bools_render_as_clickhouse_bools(self):
        line = render_tsv([_silver_row()]).decode("utf-8")
        cells = line.strip().split("\t")
        assert cells[SILVER_COLUMNS.index("is_return")] == "false"
        assert cells[SILVER_COLUMNS.index("is_new_customer")] == "true"

    def test_datetimes_render_without_offset(self):
        # DateTime('UTC') refuses '+00:00'; the renderer writes bare seconds.
        line = render_tsv([_silver_row()]).decode("utf-8")
        assert "+00" not in line
        assert "2026-08-27 12:30:05" in line

    def test_ddl_declares_exactly_the_shipped_columns_in_order(self):
        declared = re.findall(
            r"^\s{4}(\w+)\s+(?:Int32|Decimal|Nullable|Date\b|Bool|"
            r"LowCardinality|String)",
            _SILVER_DDL, re.MULTILINE,
        )
        assert tuple(declared) == SILVER_COLUMNS


class TestNoThirdDialect:
    def test_every_measure_renders_verbatim(self):
        from core.duckdb_store import GOLD_MEASURES

        fine, rollup = derive_gold_sql()
        for name, expr in GOLD_MEASURES.items():
            assert expr in fine, f"{name} was rewritten for ClickHouse"
            assert expr in rollup, f"{name} was rewritten in the roll-up"

    def test_rollup_carries_null_source_and_coarser_grouping(self):
        fine, rollup = derive_gold_sql()
        assert "GROUP BY order_date, sales_type, source_id" in fine
        assert "GROUP BY order_date, sales_type\n" in rollup
        assert "sales_type, NULL," in rollup


class TestTheEngineVerdict:
    def test_identical_cells_no_findings(self):
        cells = [_gold_cell(), _gold_cell(source_id=None, sales_type="b2b")]
        assert compare_gold_cells(cells, list(cells)) == []

    def test_one_cent_on_avg_is_the_licensed_difference(self):
        pg = [_gold_cell()]
        ch = [_gold_cell(avg_order_value=Decimal("1834.51"))]
        assert compare_gold_cells(pg, ch) == []

    def test_two_cents_on_avg_is_not(self):
        pg = [_gold_cell()]
        ch = [_gold_cell(avg_order_value=Decimal("1834.52"))]
        (issue,) = compare_gold_cells(pg, ch)
        assert issue.check_name == "ch_engines_gold_mismatch"

    def test_a_cent_anywhere_else_is_a_defect(self):
        pg = [_gold_cell()]
        ch = [_gold_cell(revenue=Decimal("1834.51"))]
        (issue,) = compare_gold_cells(pg, ch)
        assert issue.severity is Severity.CRITICAL
        assert "revenue" in issue.description

    def test_uniq_counts_compare_exact(self):
        pg = [_gold_cell(unique_customers=48)]
        ch = [_gold_cell(unique_customers=47)]
        (issue,) = compare_gold_cells(pg, ch)
        assert "unique_customers" in issue.description

    def test_missing_and_extra_cells_are_named(self):
        pg = [_gold_cell(), _gold_cell(sales_type="b2b")]
        ch = [_gold_cell(), _gold_cell(sales_type="internal")]
        names = sorted(i.check_name for i in compare_gold_cells(pg, ch))
        assert names == ["ch_engines_gold_extra", "ch_engines_gold_missing"]


class _FakePool:
    def __init__(self, rows):
        self._rows = rows

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                conn = AsyncMock()
                conn.fetch = AsyncMock(return_value=pool._rows)
                return conn

            async def __aexit__(self, *a):
                return False

        return _Ctx()


class TestTheArchiveCopy:
    def test_microseconds_survive_the_render(self):
        row = (1, 1001, datetime(2026, 8, 27, 19, 27, 25, 123456,
                                 tzinfo=timezone.utc), "baseline",
               1, 20, 4, Decimal("10.00"), None, 7, None, None, None, None)
        assert b"19:27:25.123456" in ch_history.render_tsv([row])

    @pytest.mark.asyncio
    async def test_unconfigured_stands_down(self, monkeypatch):
        monkeypatch.delenv(ch_silver.URL_ENV, raising=False)
        assert await ch_history.ship_history() == {
            "skipped": f"{ch_silver.URL_ENV} is not set"
        }
        assert await ch_history.reconcile_ch_history() == []

    @pytest.mark.asyncio
    async def test_equal_buckets_mean_silence(self, monkeypatch):
        monkeypatch.setenv(ch_silver.URL_ENV, "http://ch:8123")

        async def fake_execute(sql, *, body=None):
            if "max(id)" in sql:
                return "20000\n"
            return "1\t10000\n2\t123\n"

        monkeypatch.setattr(ch_history, "_execute", fake_execute)
        import core.pg as pg

        monkeypatch.setattr(
            pg, "get_pool",
            AsyncMock(return_value=_FakePool([(1, 10000), (2, 123)])),
        )
        assert await ch_history.reconcile_ch_history() == []

    @pytest.mark.asyncio
    async def test_a_short_bucket_is_critical(self, monkeypatch):
        monkeypatch.setenv(ch_silver.URL_ENV, "http://ch:8123")

        async def fake_execute(sql, *, body=None):
            if "max(id)" in sql:
                return "20000\n"
            return "1\t9999\n2\t123\n"

        monkeypatch.setattr(ch_history, "_execute", fake_execute)
        import core.pg as pg

        monkeypatch.setattr(
            pg, "get_pool",
            AsyncMock(return_value=_FakePool([(1, 10000), (2, 123)])),
        )
        (issue,) = await ch_history.reconcile_ch_history()
        assert issue.check_name == "ch_history_buckets"
        assert issue.severity is Severity.CRITICAL
        assert issue.sample_ids == (1,)


class TestTheColumnListsHaveOneHome:
    """The review's finding: the copies restated their column lists, so a
    revision 0012 adding a column to Postgres would silently never reach
    ClickHouse while every round-trip check stayed green — the copy would be
    verifying itself against its own stale list. Identity, not equality: the
    modules must share the object, not agree today."""

    def test_silver_copy_shares_the_projection_write_order(self):
        from core import ch_silver, pg_silver

        assert ch_silver.SILVER_COLUMNS is pg_silver.SILVER_COLUMNS

    def test_gold_copy_shares_the_cell_shape(self):
        from core import ch_gold, pg_gold

        assert ch_gold.COLUMNS is pg_gold.GOLD_COLUMNS

    def test_type_maps_cover_exactly_the_shared_columns(self):
        from core import ch_history, ch_silver

        assert set(ch_silver._TYPES) == set(ch_silver.SILVER_COLUMNS)
        assert set(ch_history._TYPES) == set(ch_history.COLUMNS)
