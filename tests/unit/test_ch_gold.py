"""Step 4 of «Одна бронза»: the ClickHouse copy of Gold.

The module under test is a copy pipeline, so the tests are about the ways a
copy goes wrong: rows lost or invented crossing the wire, values changed by
the wire format, a short INSERT swapped into place, and a comparison that
quietly compares nothing. The wire format itself (TSV both directions) gets a
round-trip test because it is the one place where every column's type is
decided by hand.

No network anywhere: `_execute` is replaced with fakes that record SQL, which
also lets the EXCHANGE-refusal test assert on what was *not* executed.
"""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from core import ch_gold
from core.ch_gold import (
    COLUMNS,
    _TABLE_DDL,
    compare_rows,
    parse_tsv,
    render_tsv,
)
from core.data_quality import Severity


def _row(**overrides):
    """One gold cell with plausible defaults, positionally ordered."""
    base = {
        "date": date(2026, 8, 27),
        "sales_type": "retail",
        "source_id": 1,
        "revenue": Decimal("1834807.24"),
        "orders_count": 52,
        "unique_customers": 48,
        "new_customers": 7,
        "returning_customers": 41,
        "returns_count": 2,
        "returns_revenue": Decimal("1200.00"),
        "avg_order_value": Decimal("35284.75"),
    }
    base.update(overrides)
    return tuple(base[c] for c in COLUMNS)


class TestTheWireFormat:
    def test_round_trip_is_identity(self):
        rows = [
            _row(),
            _row(sales_type="b2b", source_id=None, revenue=Decimal("0.00")),
            _row(date=date(2024, 1, 1), sales_type="exhibition", source_id=5),
        ]
        assert parse_tsv(render_tsv(rows).decode("utf-8")) == rows

    def test_null_source_id_is_backslash_n(self):
        line = render_tsv([_row(source_id=None)]).decode("utf-8")
        assert "\\N" in line.split("\t")[2]

    def test_decimals_survive_as_decimals(self):
        (row,) = parse_tsv(render_tsv([_row()]).decode("utf-8"))
        assert isinstance(row[COLUMNS.index("revenue")], Decimal)
        assert row[COLUMNS.index("avg_order_value")] == Decimal("35284.75")

    def test_escaping_is_written_even_for_data_that_never_needs_it(self):
        hostile = _row(sales_type="re\ttail\\n")
        (back,) = parse_tsv(render_tsv([hostile]).decode("utf-8"))
        assert back == hostile

    def test_empty_set_renders_empty_body(self):
        assert render_tsv([]) == b""
        assert parse_tsv("") == []


class TestTheDDLAndTheColumnListAgree:
    """Structure, not prose: the DDL is parsed, not grepped for a comment."""

    def test_ddl_declares_exactly_the_shipped_columns_in_order(self):
        declared = re.findall(
            r"^\s{4}(\w+)\s+(?:Date|LowCardinality|Nullable|Decimal|Int32)",
            _TABLE_DDL, re.MULTILINE,
        )
        assert tuple(declared) == COLUMNS


class TestTheComparison:
    def test_identical_sets_produce_no_findings(self):
        rows = [_row(), _row(sales_type="b2b", source_id=None)]
        assert compare_rows(rows, list(rows)) == []

    def test_a_lost_row_is_critical(self):
        shipped = [_row(), _row(sales_type="b2b")]
        (issue,) = compare_rows(shipped, shipped[:1])
        assert issue.check_name == "ch_gold_row_missing"
        assert issue.severity is Severity.CRITICAL
        assert issue.count == 1
        assert issue.sample_ids == (20260827,)

    def test_an_invented_row_is_critical(self):
        shipped = [_row()]
        (issue,) = compare_rows(shipped, shipped + [_row(sales_type="b2b")])
        assert issue.check_name == "ch_gold_row_extra"
        assert issue.severity is Severity.CRITICAL

    def test_one_cent_of_drift_is_a_finding(self):
        # The tolerance elsewhere in this system exists because two engines
        # compute. Here nothing computes, so the tolerance is exactly zero.
        shipped = [_row()]
        readback = [_row(avg_order_value=Decimal("35284.76"))]
        (issue,) = compare_rows(shipped, readback)
        assert issue.check_name == "ch_gold_value_mismatch"
        assert "avg_order_value" in issue.description
        assert "35284.75" in issue.description

    def test_the_rollup_and_the_fine_row_are_different_cells(self):
        # (date, type, NULL) and (date, type, 1) must never shadow each other.
        rollup = _row(source_id=None)
        fine = _row(source_id=1)
        assert compare_rows([rollup, fine], [fine, rollup]) == []


class TestTheShipper:
    @pytest.mark.asyncio
    async def test_a_short_staging_refuses_to_exchange(self, monkeypatch):
        executed = []

        async def fake_execute(sql, *, body=None):
            executed.append(sql)
            if sql.startswith("SELECT count()"):
                return "1\n"  # staging holds 1 row; two were supplied
            return ""

        monkeypatch.setenv(ch_gold.URL_ENV, "http://ch:8123")
        monkeypatch.setattr(ch_gold, "_execute", fake_execute)
        monkeypatch.setattr(
            ch_gold, "_fetch_pg_rows",
            AsyncMock(return_value=[_row(), _row(sales_type="b2b")]),
        )
        failed = AsyncMock()
        monkeypatch.setattr(ch_gold, "_watermark_failed", failed)
        monkeypatch.setattr(ch_gold, "_watermark_ok", AsyncMock())

        result = await ch_gold.ship_gold()

        assert "error" in result and "refusing" in result["error"]
        assert not any(sql.startswith("EXCHANGE") for sql in executed)
        failed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_clean_ship_exchanges_and_moves_the_watermark(self, monkeypatch):
        executed = []

        async def fake_execute(sql, *, body=None):
            executed.append(sql)
            if sql.startswith("SELECT count()"):
                return "2\n"
            return ""

        monkeypatch.setenv(ch_gold.URL_ENV, "http://ch:8123")
        monkeypatch.setattr(ch_gold, "_execute", fake_execute)
        monkeypatch.setattr(
            ch_gold, "_fetch_pg_rows",
            AsyncMock(return_value=[_row(), _row(sales_type="b2b")]),
        )
        ok = AsyncMock()
        monkeypatch.setattr(ch_gold, "_watermark_ok", ok)

        result = await ch_gold.ship_gold()

        assert result == {"rows": 2}
        assert any(sql.startswith("EXCHANGE TABLES") for sql in executed)
        ok.assert_awaited_once_with(2)

    @pytest.mark.asyncio
    async def test_unconfigured_stands_down_without_touching_anything(self, monkeypatch):
        monkeypatch.delenv(ch_gold.URL_ENV, raising=False)
        fetch = AsyncMock()
        monkeypatch.setattr(ch_gold, "_fetch_pg_rows", fetch)

        result = await ch_gold.ship_gold()

        assert "skipped" in result
        fetch.assert_not_awaited()


class TestTheReconciliation:
    @pytest.mark.asyncio
    async def test_unconfigured_returns_no_findings(self, monkeypatch):
        monkeypatch.delenv(ch_gold.URL_ENV, raising=False)
        assert await ch_gold.reconcile_ch_gold() == []

    @pytest.mark.asyncio
    async def test_a_ship_failure_is_one_warn_not_an_exception(self, monkeypatch):
        # It rides dq_mirror_landing, where a raise fails the whole layer —
        # an optional store must not silence the mandatory comparisons.
        monkeypatch.setenv(ch_gold.URL_ENV, "http://ch:8123")
        monkeypatch.setattr(
            ch_gold, "_fetch_pg_rows",
            AsyncMock(side_effect=RuntimeError("connection refused")),
        )
        monkeypatch.setattr(ch_gold, "_watermark_failed", AsyncMock())

        (issue,) = await ch_gold.reconcile_ch_gold()

        assert issue.check_name == "ch_gold_ship_failed"
        assert issue.severity is Severity.WARN

    @pytest.mark.asyncio
    async def test_compares_the_shipped_snapshot_not_a_fresh_read(self, monkeypatch):
        # The shipped rows and the readback disagree on one cell; the finding
        # must surface it even though nothing re-reads Postgres.
        shipped = [_row(), _row(sales_type="b2b", source_id=None)]
        readback = [shipped[0], _row(sales_type="b2b", source_id=None,
                                     revenue=Decimal("999.99"))]

        monkeypatch.setenv(ch_gold.URL_ENV, "http://ch:8123")
        monkeypatch.setattr(ch_gold, "_fetch_pg_rows", AsyncMock(return_value=shipped))
        monkeypatch.setattr(ch_gold, "_ship", AsyncMock())
        monkeypatch.setattr(ch_gold, "_watermark_ok", AsyncMock())
        monkeypatch.setattr(ch_gold, "_fetch_ch_rows", AsyncMock(return_value=readback))

        (issue,) = await ch_gold.reconcile_ch_gold()

        assert issue.check_name == "ch_gold_value_mismatch"
        assert "revenue" in issue.description

    @pytest.mark.asyncio
    async def test_a_failed_readback_is_warn_after_a_clean_ship(self, monkeypatch):
        monkeypatch.setenv(ch_gold.URL_ENV, "http://ch:8123")
        monkeypatch.setattr(ch_gold, "_fetch_pg_rows", AsyncMock(return_value=[_row()]))
        monkeypatch.setattr(ch_gold, "_ship", AsyncMock())
        monkeypatch.setattr(ch_gold, "_watermark_ok", AsyncMock())
        monkeypatch.setattr(
            ch_gold, "_fetch_ch_rows",
            AsyncMock(side_effect=RuntimeError("read timed out")),
        )

        (issue,) = await ch_gold.reconcile_ch_gold()

        assert issue.check_name == "ch_gold_unreachable"
        assert issue.severity is Severity.WARN
