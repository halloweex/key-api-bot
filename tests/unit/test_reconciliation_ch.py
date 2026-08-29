"""The third arm: ClickHouse's silver against the KeyCRM snapshot.

Two copies can agree perfectly and both be wrong — the owner's closing
criterion for Postgres, applied to the third store. Same snapshot, zero
extra API calls, own layer, and two guards this file pins because each was
found by thinking before shipping: the watermark exclusion computed where
updated_at lives (skipping it ghosts the ~1,400 orders the 05:15 refresh
force-rewrites), and the staleness gate (reconciling a lagging copy would
measure the ship and call it a lie).
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import DiscrepancyClass, classify_order_discrepancies
from core.reconciliation_io import CH_HEADER_FIELDS


def _facts(status=12, source=1, manager=4, buyer=7, total=100.0,
           d=date(2026, 8, 1)):
    return {"status_id": status, "source_id": source, "manager_id": manager,
            "buyer_id": buyer, "grand_total": total, "order_date": d}


class TestHeaderComparison:
    def test_the_fields_are_the_header_slice_and_nothing_line_level(self):
        assert "n_lines" not in CH_HEADER_FIELDS
        assert "qty" not in CH_HEADER_FIELDS
        assert "line_amount" not in CH_HEADER_FIELDS
        assert "grand_total" in CH_HEADER_FIELDS
        assert "status_id" in CH_HEADER_FIELDS

    def test_identical_sides_are_clean(self):
        ch = {1: _facts(), 2: _facts(total=50)}
        kc = {1: _facts(), 2: _facts(total=50)}
        assert classify_order_discrepancies(ch, kc, fields=CH_HEADER_FIELDS) == []

    def test_a_lost_order_is_missing_in_dk(self):
        kc = {1: _facts(), 2: _facts()}
        ch = {1: _facts()}
        [d] = classify_order_discrepancies(ch, kc, fields=CH_HEADER_FIELDS)
        assert d.diff_class is DiscrepancyClass.MISSING_IN_DK
        assert d.order_ids == (2,)

    def test_a_ghost_is_missing_in_kc(self):
        kc = {1: _facts()}
        ch = {1: _facts(), 3: _facts()}
        [d] = classify_order_discrepancies(ch, kc, fields=CH_HEADER_FIELDS)
        assert d.diff_class is DiscrepancyClass.MISSING_IN_KC

    def test_a_status_change_is_seen(self):
        """The transition KeyCRM does not bump updated_at for — the whole
        reason force_update exists — must be visible to this arm too."""
        kc = {1: _facts(status=20)}
        ch = {1: _facts(status=12)}
        [d] = classify_order_discrepancies(ch, kc, fields=CH_HEADER_FIELDS)
        assert d.field == "status_id"

    def test_money_keeps_its_cent_tolerance(self):
        kc = {1: _facts(total=100.005)}
        ch = {1: _facts(total=100.0)}
        assert classify_order_discrepancies(ch, kc, fields=CH_HEADER_FIELDS) == []


class TestTheArmGates:
    def _sched(self):
        from core.scheduler import BackgroundScheduler

        return BackgroundScheduler()

    @pytest.mark.asyncio
    async def test_no_clickhouse_means_none_not_a_clean_run(self, monkeypatch):
        monkeypatch.delenv("KS_CH_URL", raising=False)
        result = await self._sched()._reconcile_clickhouse(
            {}, window_start=date(2026, 8, 1), window_end=date(2026, 8, 29),
            as_of=datetime.now(timezone.utc), inflight_ids=set(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_a_stale_copy_is_gated_not_blamed(self, monkeypatch):
        """A copy three hours behind would fail the comparison for being a
        copy, not for being wrong."""
        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        old = datetime.now(timezone.utc) - timedelta(hours=5)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value={
                       "clickhouse.silver_orders": {"last_ok_at": old},
                   })):
            result = await self._sched()._reconcile_clickhouse(
                {}, window_start=date(2026, 8, 1),
                window_end=date(2026, 8, 29),
                as_of=datetime.now(timezone.utc), inflight_ids=set(),
            )
        [issue] = result["issues"]
        assert issue.check_name == "ch_reconcile_pending"
        assert issue.severity.value == "WARN"
        assert result["discrepancies"] == []

    @pytest.mark.asyncio
    async def test_a_fresh_copy_is_compared_with_the_watermark_set_excluded(
        self, monkeypatch,
    ):
        """Both sides shrink by the same exclusion set, computed in Postgres
        where updated_at lives — ClickHouse has no such column."""
        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        fresh = datetime.now(timezone.utc) - timedelta(minutes=20)
        kc = {1: _facts(), 2: _facts(), 99: _facts()}  # 99: force-rewritten
        ch = {1: _facts(), 2: _facts(status=19)}       # 99 excluded, 2 differs

        with patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value={
                       "clickhouse.silver_orders": {"last_ok_at": fresh},
                   })), \
             patch("core.reconciliation_io.pg_ids_updated_since",
                   new=AsyncMock(return_value={99})), \
             patch("core.reconciliation_io.clickhouse_orders_in_window",
                   new=AsyncMock(return_value=ch)) as chx:
            result = await self._sched()._reconcile_clickhouse(
                kc, window_start=date(2026, 8, 1),
                window_end=date(2026, 8, 29),
                as_of=datetime.now(timezone.utc), inflight_ids={7},
            )
        assert chx.await_args.kwargs["exclude_ids"] == {7, 99}
        assert result["error"] is None
        [d] = result["discrepancies"]
        assert d.field == "status_id"  # 99 never became a phantom ghost

    @pytest.mark.asyncio
    async def test_a_raising_arm_reports_the_error_never_a_clean_run(
        self, monkeypatch,
    ):
        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        with patch("core.pg.get_pool",
                   new=AsyncMock(side_effect=ConnectionError("refused"))):
            result = await self._sched()._reconcile_clickhouse(
                {}, window_start=date(2026, 8, 1),
                window_end=date(2026, 8, 29),
                as_of=datetime.now(timezone.utc), inflight_ids=set(),
            )
        assert "ConnectionError" in result["error"]


class TestExtractorShape:
    def test_the_sql_carries_window_sources_and_exclusions(self):
        """Pin the three clauses a rewrite must not drop."""
        import inspect

        from core import reconciliation_io

        src = inspect.getsource(reconciliation_io.clickhouse_orders_in_window)
        assert "BETWEEN" in src
        assert "source_id IN" in src
        assert "NOT IN" in src
        assert "FROM silver.orders" in src
