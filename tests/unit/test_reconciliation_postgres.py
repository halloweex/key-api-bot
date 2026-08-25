"""Reconciling Postgres against KeyCRM — the other half of the closing criterion.

Reconciliation A proves the two stores agree with each other. That is not the
same as being right: two copies can agree perfectly and both disagree with the
source, which is exactly the hole the warehouse validation has always had — it
cannot see a lie that arrives from Bronze, because Gold is built from Silver in
the same tick and reproduces it on both sides.

So the owner's criterion for closing step 05 is two things: zero differences
between the stores, **and** a reconciliation against KeyCRM.

The property that makes this affordable is that it costs **nothing**. The
KeyCRM fetch has already happened for the DuckDB comparison; this is handed the
same orders and the same rollup. Doubling KeyCRM traffic for a second opinion
would be a poor trade on any job and a very poor one on this one, which spent
57 runs out of 84 failing on 429s.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import DiscrepancyClass, Severity
from core.reconciliation_io import (
    duckdb_orders_in_window,
    postgres_orders_in_window,
    rollup_from_orders,
)
from core.scheduler import BackgroundScheduler

AS_OF = datetime(2026, 8, 24, 3, 0, tzinfo=timezone.utc)
START, END = date(2026, 5, 26), date(2026, 8, 24)

def _body(fn):
    """A function's source with its docstring removed.

    These tests compare the two extractors' SQL, and prose that *explains* an
    expression contains the same words as the expression.

    Sliced by line number from the AST, not by `source.replace(fn.__doc__, "")`
    — **that trick is version-dependent and fails silently.** Python 3.13
    started dedenting `__doc__`, so from 3.13 on the string no longer matches
    the indented literal in the source, `replace` finds nothing, and the test
    compares prose it meant to exclude. It passed on 3.12 here and failed on
    the 3.14.7 production runs, which is the only reason anyone found out."""
    source = textwrap.dedent(inspect.getsource(fn))
    node = ast.parse(source).body[0]
    first = node.body[0] if node.body else None
    is_doc = (
        isinstance(first, ast.Expr)
        and isinstance(getattr(first, "value", None), ast.Constant)
        and isinstance(first.value.value, str)
    )
    if not is_doc:
        return source
    lines = source.splitlines(keepends=True)
    return "".join(lines[: first.lineno - 1] + lines[first.end_lineno :])


DK_SQL = _body(duckdb_orders_in_window)
PG_SQL = _body(postgres_orders_in_window)


def _facts(order_id, **over):
    base = {
        "status_id": 12, "source_id": 1, "manager_id": 4, "buyer_id": 500,
        "grand_total": 1000.0, "order_date": date(2026, 8, 20),
        "n_lines": 2, "qty": 3, "line_amount": 1000.0,
    }
    base.update(over)
    return {order_id: base}


class TestTheTwoExtractorsAskTheSameQuestion:
    """Drift between the two sides is drift the comparison invents."""

    def test_the_kyiv_date_is_written_identically(self):
        """The one expression here worth doubting. A day's difference on one
        side moves orders between months and invents a whole rollup cell."""
        fragment = "AT TIME ZONE 'Europe/Kyiv'"
        assert DK_SQL.count(fragment) == PG_SQL.count(fragment) == 2

    def test_both_filter_to_the_active_sources(self):
        assert "o.source_id IN ({" in DK_SQL
        assert "o.source_id IN ({sources})" in PG_SQL

    def test_both_honour_the_watermark_the_same_way(self):
        assert "o.updated_at IS NULL OR o.updated_at <" in DK_SQL
        assert "o.updated_at IS NULL OR o.updated_at <" in PG_SQL

    def test_both_exclude_the_in_flight_ids(self):
        """KeyCRM's cut is the wider one, so both sides must honour its list."""
        assert "list_contains" in DK_SQL
        assert "o.id = ANY($4::bigint[])" in PG_SQL

    def test_line_items_are_aggregated_by_the_same_expression(self):
        fragment = "ROUND(SUM(price_sold * quantity), 2) AS amount"
        assert fragment in DK_SQL and fragment in PG_SQL

    def test_the_postgres_side_reads_the_mirror_and_not_the_warehouse(self):
        assert "bronze.orders" in PG_SQL and "bronze.order_products" in PG_SQL
        assert "FROM orders o" not in PG_SQL

    def test_both_produce_the_same_shape(self):
        """`rollup_from_orders` is the only place a rollup is computed, and it
        can only be that if both extractors hand it the same dict."""
        for source in (DK_SQL, PG_SQL):
            for key in ("status_id", "source_id", "manager_id", "buyer_id",
                        "grand_total", "order_date", "n_lines", "qty",
                        "line_amount"):
                assert f'"{key}":' in source


class TestItCostsNothing:
    @pytest.mark.asyncio
    async def test_it_never_calls_keycrm(self):
        """It is handed the snapshot the DuckDB comparison already paid for."""
        kc = _facts(1)
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value={
                       "bronze.orders": {"backfilled_at": AS_OF}})), \
             patch("core.reconciliation_io.postgres_orders_in_window",
                   new=AsyncMock(return_value=_facts(1))), \
             patch("core.keycrm.KeyCRMClient") as client:
            result = await BackgroundScheduler()._reconcile_postgres(
                kc, rollup_from_orders(kc),
                window_start=START, window_end=END,
                as_of=AS_OF, inflight_ids=set(),
            )
        client.assert_not_called()
        assert result["discrepancies"] == []
        assert result["error"] is None

    @pytest.mark.asyncio
    async def test_the_persisted_run_records_zero_api_calls(self):
        captured = {}

        def _persist(conn, **kw):
            captured.update(kw)
            return 1

        class _Conn:
            async def __aenter__(self): return object()
            async def __aexit__(self, *a): return False

        class _Store:
            def connection(self): return _Conn()

        with patch("core.data_quality.persist_run", new=_persist), \
             patch("core.duckdb_store.get_store",
                   new=AsyncMock(return_value=_Store())):
            await BackgroundScheduler()._persist_postgres_reconciliation(
                {"issues": [], "discrepancies": [], "error": None},
                started_at=AS_OF, as_of=AS_OF,
                window_start=START, window_end=END,
            )

        assert captured["layer"] == "reconciliation_pg"
        assert captured["api_calls_used"] == 0


class TestTheGate:
    @pytest.mark.asyncio
    async def test_without_a_backfill_it_reports_that_and_compares_nothing(self):
        """Before history is carried across, every old order is 'missing' from
        the source's point of view — which says nothing about KeyCRM."""
        kc = _facts(1)
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value={
                       "bronze.orders": {"backfilled_at": None}})), \
             patch("core.reconciliation_io.postgres_orders_in_window",
                   new=AsyncMock()) as reader:
            result = await BackgroundScheduler()._reconcile_postgres(
                kc, rollup_from_orders(kc),
                window_start=START, window_end=END,
                as_of=AS_OF, inflight_ids=set(),
            )

        reader.assert_not_called()
        assert [i.check_name for i in result["issues"]] == ["mirror_backfill_pending"]
        assert result["discrepancies"] == []

    @pytest.mark.asyncio
    async def test_no_postgres_configured_is_silence_not_a_clean_run(self):
        """A run persisted here would tell the watchdog the layer is healthy."""
        with patch("core.mirror_reconciliation.configured", return_value=False):
            assert await BackgroundScheduler()._reconcile_postgres(
                {}, {}, window_start=START, window_end=END,
                as_of=AS_OF, inflight_ids=set(),
            ) is None


class TestWhatItActuallyFinds:
    async def _run(self, kc, pg):
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool", new=AsyncMock(return_value=object())), \
             patch("core.pg.require_revision", new=AsyncMock()), \
             patch("core.mirror_reconciliation.fetch_watermarks",
                   new=AsyncMock(return_value={
                       "bronze.orders": {"backfilled_at": AS_OF}})), \
             patch("core.reconciliation_io.postgres_orders_in_window",
                   new=AsyncMock(return_value=pg)):
            return await BackgroundScheduler()._reconcile_postgres(
                kc, rollup_from_orders(kc),
                window_start=START, window_end=END,
                as_of=AS_OF, inflight_ids=set(),
            )

    @pytest.mark.asyncio
    async def test_two_stores_agreeing_with_the_source_is_silence(self):
        kc = _facts(1)
        assert (await self._run(kc, _facts(1)))["discrepancies"] == []

    @pytest.mark.asyncio
    async def test_an_order_keycrm_has_and_postgres_does_not(self):
        result = await self._run(_facts(1), {})
        classes = {d.diff_class for d in result["discrepancies"]}
        assert DiscrepancyClass.MISSING_IN_DK in classes

    @pytest.mark.asyncio
    async def test_a_status_that_differs_from_the_source(self):
        """The case Reconciliation A cannot see: both stores can carry the
        same wrong status and agree with each other perfectly."""
        result = await self._run(_facts(1), _facts(1, status_id=19))
        assert any(
            d.diff_class == DiscrepancyClass.STATUS_DRIFT
            for d in result["discrepancies"]
        )

    @pytest.mark.asyncio
    async def test_a_failure_is_captured_not_raised(self):
        """`dq_reconciliation` must still deliver the DuckDB verdict."""
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg.get_pool", side_effect=RuntimeError("pg is down")):
            result = await BackgroundScheduler()._reconcile_postgres(
                _facts(1), {}, window_start=START, window_end=END,
                as_of=AS_OF, inflight_ids=set(),
            )
        assert "pg is down" in result["error"]
        assert result["discrepancies"] == []


class TestItIsWatchedOnItsOwn:
    def test_the_layer_has_its_own_age(self):
        """One layer over two comparisons would give them one age between
        them, and a Postgres half that stopped would hide behind a fresh
        DuckDB one."""
        from core.data_quality import DIGEST_MAX_AGE_HOURS, WATCHED_LAYERS

        assert "reconciliation_pg" in WATCHED_LAYERS
        assert DIGEST_MAX_AGE_HOURS["reconciliation_pg"] == 30

    def test_it_does_not_repair(self):
        """The DuckDB layer re-fetches what it is missing. Postgres has a
        different answer — the backfill — and re-fetching here would repair
        the wrong store."""
        source = inspect.getsource(
            BackgroundScheduler._persist_postgres_reconciliation
        )
        assert "repair_orders" not in source


class TestFloatOrderIsNotADiscrepancy:
    """Measured on 90 days of production, not imagined.

    The two extractors returned *identical* per-order facts — `dk == pg` was
    True — and their rollups still compared unequal, because the databases
    return rows in different orders and float addition is not associative. The
    largest gap was 2.1e-09 on a 1,834,807.24 revenue cell.

    So the tolerance in `classify_discrepancies` is load-bearing here, and this
    is the test that fails if somebody decides exact equality would be tidier.
    """

    def test_two_nanohryvnia_is_not_a_finding(self):
        from core.data_quality import classify_discrepancies

        cell = ("2026-08", 4)
        left = {cell: {"orders": 900, "qty": 2500, "revenue": 1834807.2400000012,
                       "returns_count": 3, "returns_revenue": 1200.0}}
        right = {cell: {"orders": 900, "qty": 2500, "revenue": 1834807.2400000033,
                        "returns_count": 3, "returns_revenue": 1200.0}}
        assert classify_discrepancies(left, right) == []

    def test_a_hryvnia_still_is(self):
        """The tolerance must not have swallowed the thing it exists beside."""
        from core.data_quality import classify_discrepancies

        cell = ("2026-08", 4)
        left = {cell: {"orders": 900, "qty": 2500, "revenue": 1834807.24,
                       "returns_count": 3, "returns_revenue": 1200.0}}
        right = {cell: {"orders": 901, "qty": 2500, "revenue": 1834807.24,
                        "returns_count": 3, "returns_revenue": 1200.0}}
        assert classify_discrepancies(left, right) != []
