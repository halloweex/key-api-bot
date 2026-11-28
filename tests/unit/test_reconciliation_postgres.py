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

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import DiscrepancyClass
from core.reconciliation_io import rollup_from_orders
from core.scheduler import BackgroundScheduler
from tests.helpers.dq_journal_pg import pool  # noqa: F401
from tests.helpers.orders_sync import (
    keycrm_serving_orders,
    order_payload,
    orders_held,
    orders_in_postgres,
)
from tests.integration.pg_tab_seed import needs_postgres


AS_OF = datetime(2026, 8, 24, 3, 0, tzinfo=timezone.utc)
START, END = date(2026, 5, 26), date(2026, 8, 24)

def _facts(order_id, **over):
    base = {
        "status_id": 12, "source_id": 1, "manager_id": 4, "buyer_id": 500,
        "grand_total": 1000.0, "order_date": date(2026, 8, 20),
        "n_lines": 2, "qty": 3, "line_amount": 1000.0,
    }
    base.update(over)
    return {order_id: base}


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
        """Through the journal's door, `dq_journal.journal_run` (until step
        14's S14-10 through DuckDB's `persist_run` behind it, the chain
        routed back to DuckDB for the test)."""
        captured = {}

        async def journal_run(**kw):
            captured.update(kw)
            return 1

        with patch("core.dq_journal.journal_run", new=journal_run):
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

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_missing_order_is_re_fetched_once(self, pool, monkeypatch):  # noqa: F811
        """KeyCRM holds an order Postgres lacks. The job persists the Postgres
        verdict, then re-fetches by id what it found missing — once: the
        persistence repairs nothing, or the order would be asked for twice.
        KeyCRM at its transport, the 05:30 job real, Postgres the test
        database."""
        missing = 990_861
        created = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=10)
        monkeypatch.delenv("KS_CH_URL", raising=False)
        async with orders_in_postgres():
            await pool.execute(
                "INSERT INTO meta.mirror_state (table_name, backfilled_at)"
                " VALUES ('bronze.orders', now()) ON CONFLICT (table_name)"
                " DO UPDATE SET backfilled_at = now()")
            keycrm = keycrm_serving_orders(monkeypatch, [order_payload(missing, created=created)])
            result = await BackgroundScheduler()._run_dq_reconciliation()
            held = await orders_held()
            found = {r["diff_class"] for r in await pool.fetch(
                "SELECT d.diff_class FROM app.data_quality_diffs d"
                " JOIN app.data_quality_runs r USING (run_id)"
                " WHERE r.layer = 'reconciliation_pg'")}

        assert "MISSING_IN_DK" in found
        assert keycrm.by_id == [missing]
        assert result["repair"]["repaired"] == 1
        assert set(held) == {missing}


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
