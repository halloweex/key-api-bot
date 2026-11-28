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
from tests.helpers.recon_clickhouse import (
    fresh_clickhouse_copies,
    kept_mirror_state,
    needs_clickhouse,
)
from tests.integration.pg_tab_seed import DSN, derive, needs_postgres, seed, tab_in_postgres


def _facts(status=12, source=1, manager=4, buyer=7, total=100.0,
           d=date(2026, 8, 1)):
    return {"status_id": status, "source_id": source, "manager_id": manager,
            "buyer_id": buyer, "grand_total": total, "order_date": d}


# One KeyCRM order with a line, as the snapshot fetches it.
ORDER = {"id": 1, "source_id": 1, "status_id": 12,
         "ordered_at": "2026-08-01T09:00:00+00:00",
         "updated_at": "2026-08-01T09:00:00+00:00", "grand_total": "100.00",
         "manager": {"id": 4}, "buyer": {"id": 7},
         "products": [{"name": "Товар", "quantity": 2, "price_sold": "50.00",
                       "offer": {"product_id": 701}}]}


class TestTheCopyHasNoLines:
    """ClickHouse's silver is order-grain, while the KeyCRM snapshot's facts
    carry the lines too (`n_lines`, `qty`, `line_amount`): set against them,
    every order the copy holds would read as differing. Driven through the
    arm, with the snapshot built by its own parse and ClickHouse's answer
    read by the arm's own extractor. Mutation: compare every field the
    snapshot carries."""

    async def _compare(self, monkeypatch, copy_total: str):
        from core.reconciliation_io import _process_batch
        from core.scheduler import BackgroundScheduler

        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        kc, inflight = {}, set()
        _process_batch([ORDER], kc, datetime(2026, 8, 4, tzinfo=timezone.utc),
                       date(2026, 8, 1), date(2026, 8, 29), inflight)
        assert kc[1]["n_lines"] == 1          # the snapshot does carry the lines
        fresh = datetime.now(timezone.utc) - timedelta(minutes=20)
        copy = f"1\t12\t1\t4\t7\t{copy_total}\t2026-08-01\n"
        with patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value={
                       "clickhouse.silver_orders": {"last_ok_at": fresh}})), \
             patch("core.reconciliation_io.pg_ids_updated_since",
                   new=AsyncMock(return_value=set())), \
             patch("core.ch_common.execute", new=AsyncMock(return_value=copy)):
            return await BackgroundScheduler()._reconcile_clickhouse(
                kc, window_start=date(2026, 8, 1), window_end=date(2026, 8, 29),
                as_of=datetime.now(timezone.utc) - timedelta(hours=2),
                inflight_ids=inflight)

    @pytest.mark.asyncio
    async def test_the_lines_the_copy_does_not_hold_are_not_compared(self, monkeypatch):
        result = await self._compare(monkeypatch, "100")
        assert result["error"] is None
        assert result["discrepancies"] == []

    @pytest.mark.asyncio
    async def test_a_hryvnia_on_the_header_is_a_finding(self, monkeypatch):
        result = await self._compare(monkeypatch, "99")
        [d] = result["discrepancies"]
        assert (d.diff_class, d.field, d.order_ids) == (
            DiscrepancyClass.VALUE_MISMATCH, "grand_total", (1,))


class TestHeaderComparison:
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
        # Not blamed, and not a verdict either: nothing was compared, so the
        # run is written as one that did not run (OD-08 review) and cannot
        # reset the layer's age.
        assert result["error"].startswith("ch_reconcile_pending: ")

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
            # The job's watermark, two hours back: the copy shipped after it.
            result = await self._sched()._reconcile_clickhouse(
                kc, window_start=date(2026, 8, 1),
                window_end=date(2026, 8, 29),
                as_of=datetime.now(timezone.utc) - timedelta(hours=2),
                inflight_ids={7},
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


# ─── The extractor against the copy itself ──────────────────────────────────

UTC = timezone.utc
EXCLUDED = 17


async def _window_edges(conn, pool):
    """Bronze in Postgres, Silver derived from it: what the hourly ship sends."""
    await seed(conn, orders=[
        # (id, source_id, status_id, grand_total, ordered_at, buyer, manager, promocode)
        (11, 1, 12, 100.00, datetime(2026, 6, 10, 9, tzinfo=UTC), 900, 4, None),
        # Opencart: synced and reconciled, though never revenue.
        (12, 3, 12, 40.00, datetime(2026, 6, 11, 9, tzinfo=UTC), 901, None, None),
        # A source this system does not sync.
        (13, 6, 12, 70.00, datetime(2026, 6, 12, 9, tzinfo=UTC), 902, None, None),
        (14, 1, 12, 20.00, datetime(2026, 5, 20, 9, tzinfo=UTC), 903, None, None),
        # 1 July in Kyiv, 30 June in UTC — and the other way round.
        (15, 1, 12, 30.00, datetime(2026, 6, 30, 22, 30, tzinfo=UTC), 904, None, None),
        (16, 1, 12, 60.00, datetime(2026, 5, 31, 21, 30, tzinfo=UTC), 905, None, None),
        (EXCLUDED, 1, 12, 90.00, datetime(2026, 6, 15, 9, tzinfo=UTC), 906, None, None),
    ])
    await derive(pool)


@needs_postgres
@needs_clickhouse
@pytest.mark.asyncio
async def test_the_copy_answers_its_window_its_sources_and_its_exclusions(monkeypatch):
    """Silver shipped by the real hourly tick, then June asked of the copy
    with one id excluded: the orders of June by the Kyiv date, on a source
    this system syncs, less the exclusion — each with its header facts.
    Mutation: drop the window, the source filter or the exclusion."""
    from core import ch_silver
    from core.reconciliation_io import clickhouse_orders_in_window

    async with kept_mirror_state(ch_silver.SILVER_STATE, ch_silver.GOLD_STATE), \
            tab_in_postgres(DSN, monkeypatch, _window_edges):
        await fresh_clickhouse_copies()
        assert "error" not in await ch_silver.ch_silver_sync()
        got = await clickhouse_orders_in_window(
            date(2026, 6, 1), date(2026, 6, 30), exclude_ids={EXCLUDED})

    def facts(source, manager, buyer, total, day):
        return {"status_id": 12, "source_id": source, "manager_id": manager,
                "buyer_id": buyer, "grand_total": total, "order_date": day}

    assert got == {
        11: facts(1, 4, 900, 100.0, date(2026, 6, 10)),
        12: facts(3, None, 901, 40.0, date(2026, 6, 11)),
        16: facts(1, None, 905, 60.0, date(2026, 6, 1)),
    }
