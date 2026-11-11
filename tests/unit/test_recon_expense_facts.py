"""The 05:30 reconciliation compares each order's expenses (owner decision O3 (a)).

Until O3 (a) order expenses were compared with nothing: the KeyCRM snapshot
asked for `products,manager,buyer` only, so neither side of the comparison
held a cost. These pin the pure half — what the snapshot asks for, what it
records per order, and how two sides are classified. The real comparison, on a
real server and through the job, is `tests/integration/test_recon_compares_expenses.py`.
Each test names the mutation it kills.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from core.data_quality import DiscrepancyClass, Severity
from core.reconciliation_io import (
    EXPENSE_FIELDS,
    _process_batch,
    classify_expense_discrepancies,
    keycrm_expense_facts,
)

WATERMARK = datetime(2026, 8, 4, tzinfo=timezone.utc)
WINDOW_START, WINDOW_END = date(2026, 6, 1), date(2026, 6, 30)


def _order(oid, **extra):
    o = {"id": oid, "source_id": 1, "status_id": 12,
         "ordered_at": "2026-06-10T09:00:00+00:00",
         "updated_at": "2026-06-10T09:00:00+00:00",
         "grand_total": "100.00", "products": []}
    o.update(extra)
    return o


def _snapshot(*orders):
    out, inflight = {}, set()
    _process_batch(list(orders), out, WATERMARK, WINDOW_START, WINDOW_END, inflight)
    return out


class TestWhatTheSnapshotRecords:
    def test_a_list_is_counted_and_summed_in_cents(self):
        facts = keycrm_expense_facts([{"id": 1, "amount": "10.50"},
                                      {"id": 2, "amount": 3},
                                      {"id": 3, "amount": 0.1},
                                      {"id": 4}])
        assert facts == {"n_expenses": 4, "expense_amount": Decimal("13.60")}
        assert isinstance(facts["expense_amount"], Decimal)

    def test_each_amount_rounds_as_the_column_does(self):
        """`NUMERIC(12,2)` rounds half away from zero, each row on its own.
        Mutation: sum first and round once, or round half to even."""
        assert keycrm_expense_facts([{"amount": "0.005"}, {"amount": "0.005"}]) == \
            {"n_expenses": 2, "expense_amount": Decimal("0.02")}
        assert keycrm_expense_facts([{"amount": "0.125"}])["expense_amount"] == \
            Decimal("0.13")

    def test_an_empty_list_is_no_expense_at_all(self):
        assert keycrm_expense_facts([]) == {"n_expenses": 0,
                                            "expense_amount": Decimal("0")}

    @pytest.mark.parametrize("carried", [None, {"id": 1}, "x", 7],
                             ids=["null", "object", "text", "number"])
    def test_anything_but_a_list_is_nothing_to_compare(self, carried):
        assert keycrm_expense_facts(carried) is None

    @pytest.mark.parametrize("amount", ["ten", True, "NaN", "Infinity"])
    def test_an_amount_that_is_not_a_number_is_nothing_to_compare(self, amount):
        assert keycrm_expense_facts([{"amount": amount}]) is None

    def test_the_order_s_facts_carry_them_only_when_the_payload_did(self):
        """Mutation: record an absent key as no expenses — every order of a
        snapshot fetched without them would read as having lost its costs."""
        orders = _snapshot(_order(1, expenses=[{"id": 9, "amount": "10.00"}]),
                           _order(2, expenses=[]), _order(3))
        assert (orders[1]["n_expenses"], orders[1]["expense_amount"]) == (1, Decimal("10.00"))
        assert (orders[2]["n_expenses"], orders[2]["expense_amount"]) == (0, Decimal("0"))
        assert not set(EXPENSE_FIELDS) & set(orders[3])

    @pytest.mark.asyncio
    async def test_both_passes_ask_keycrm_for_the_expenses(self):
        """Same pages, a larger payload. Mutation: drop `expenses` from either
        pass's include — the orders only that pass reaches would carry none."""
        from core import reconciliation_io

        asked = []

        class _Client:
            async def connect(self):
                pass

            async def close(self):
                pass

            async def paginate(self, resource, params=None, page_size=50):
                asked.append(dict(params or {}))
                if False:
                    yield []

        with patch("core.keycrm.KeyCRMClient", _Client):
            await reconciliation_io.keycrm_orders_in_window(
                WINDOW_START, WINDOW_END, watermark=WATERMARK)
        assert len(asked) == 2
        for params in asked:
            assert "expenses" in params["include"].split(","), params


def _facts(n, amount, **over):
    f = {"status_id": 12, "source_id": 1, "order_date": date(2026, 6, 10),
         "n_expenses": n, "expense_amount": Decimal(amount)}
    f.update(over)
    return f


class TestTheClassification:
    def _one(self, dk, kc):
        return {(d.diff_class, d.field): d for d in classify_expense_discrepancies(dk, kc)}

    def test_an_expense_keycrm_removed(self):
        out = self._one({1: _facts(2, "17.75")}, {1: _facts(1, "10.50")})
        assert set(out) == {(DiscrepancyClass.VALUE_MISMATCH, "n_expenses"),
                            (DiscrepancyClass.VALUE_MISMATCH, "expense_amount")}
        assert all(d.order_ids == (1,) and d.severity == Severity.WARN
                   and (d.month, d.source_id) == ("2026-06", 1) for d in out.values())

    def test_an_expense_postgres_lacks(self):
        out = self._one({1: _facts(1, "10.50")}, {1: _facts(2, "13.50")})
        assert set(f for _, f in out) == {"n_expenses", "expense_amount"}

    def test_a_kopeck_is_a_difference(self):
        """Tolerance zero. Mutation: compare the sum as money with the order
        comparison's 0.01 tolerance, or as a float."""
        out = self._one({1: _facts(1, "10.50")}, {1: _facts(1, "10.51")})
        assert set(out) == {(DiscrepancyClass.VALUE_MISMATCH, "expense_amount")}

    def test_the_same_sum_in_another_scale_is_not(self):
        assert self._one({1: _facts(1, "10.50")}, {1: _facts(1, "10.5")}) == {}

    def test_an_order_keycrm_served_without_its_expenses_is_not_compared(self):
        """Mutation: compare an order whose snapshot facts carry no expense
        fields — every expense would read as missing from KeyCRM."""
        kc = {1: {"status_id": 12, "source_id": 1, "order_date": date(2026, 6, 10)}}
        assert self._one({1: _facts(3, "30.00")}, kc) == {}

    def test_an_order_on_one_side_only_is_not_an_expense_finding(self):
        """The header comparison names it MISSING_IN_DK or MISSING_IN_KC; this
        one compares what both sides hold."""
        assert classify_expense_discrepancies({}, {1: _facts(1, "10.00")}) == []
        assert classify_expense_discrepancies({1: _facts(1, "10.00")}, {}) == []


class TestTheRepairTakesThemLast:
    @pytest.mark.asyncio
    async def test_after_the_missing_and_the_drifted(self):
        """An order whose expenses differ is re-fetched in the same by-id
        batch, after every order missing or drifted: the batch is bounded,
        and a cost moves no revenue. Mutations: leave them out of the batch,
        or sort them in among the drifts."""
        from unittest.mock import AsyncMock

        from core.data_quality import Discrepancy
        from core.scheduler import BackgroundScheduler

        def d(klass, field, *ids, severity=Severity.WARN):
            return Discrepancy(month="2026-09", source_id=4, diff_class=klass,
                               field=field, dk_value=float(len(ids)), kc_value=0.0,
                               severity=severity, order_ids=tuple(ids))

        pg = {"issues": [], "error": None, "discrepancies": [
            d(DiscrepancyClass.VALUE_MISMATCH, "expense_amount", 1, 30),
            d(DiscrepancyClass.VALUE_MISMATCH, "n_expenses", 1),
            d(DiscrepancyClass.STATUS_DRIFT, "status_id", 20, severity=Severity.CRITICAL),
            d(DiscrepancyClass.VALUE_MISMATCH, "grand_total", 25),
            d(DiscrepancyClass.MISSING_IN_DK, "order", 40, severity=Severity.CRITICAL),
            # An order both drifted and with its expenses differing goes once,
            # with the drifts.
            d(DiscrepancyClass.VALUE_MISMATCH, "expense_amount", 20),
        ]}
        sync = AsyncMock()
        sync.repair_orders = AsyncMock(return_value={"repaired": 5, "remaining": 0})
        sched = BackgroundScheduler()
        sched._reconcile_postgres = AsyncMock(return_value=pg)
        sched._persist_postgres_reconciliation = AsyncMock()
        sched._reconcile_clickhouse = AsyncMock(return_value=None)
        with patch("core.reconciliation_io.keycrm_orders_in_window",
                   new=AsyncMock(return_value=({}, 3, set()))), \
             patch("core.sync_service.get_sync_service",
                   new=AsyncMock(return_value=sync)):
            result = await sched._run_dq_reconciliation()
        assert result["error"] is None
        assert sync.repair_orders.await_args.args[0] == [40, 20, 25, 1, 30]
