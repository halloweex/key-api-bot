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
    EXPENSES_LEFT_AS_STORED,
    _process_batch,
    classify_expense_discrepancies,
    keycrm_expense_facts,
    left_as_stored,
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


def _counted(facts):
    """The two compared fields of a snapshot's facts."""
    return facts["n_expenses"], facts["expense_amount"]


class TestWhatTheSnapshotRecords:
    def test_a_list_is_counted_and_summed_in_cents(self):
        facts = keycrm_expense_facts([{"id": 1, "amount": "10.50"},
                                      {"id": 2, "amount": 3},
                                      {"id": 3, "amount": 0.1},
                                      {"id": 4}], 7)
        assert _counted(facts) == (4, Decimal("13.60"))
        assert isinstance(facts["expense_amount"], Decimal)
        assert facts[EXPENSES_LEFT_AS_STORED] == frozenset()

    def test_each_amount_rounds_as_the_column_does(self):
        """`NUMERIC(12,2)` rounds half away from zero, each row on its own.
        Mutation: sum first and round once, or round half to even."""
        assert _counted(keycrm_expense_facts(
            [{"id": 1, "amount": "0.005"}, {"id": 2, "amount": "0.005"}], 7)) == \
            (2, Decimal("0.02"))
        assert keycrm_expense_facts([{"id": 1, "amount": "0.125"}], 7)["expense_amount"] == \
            Decimal("0.13")

    def test_an_empty_list_is_no_expense_at_all(self):
        assert _counted(keycrm_expense_facts([], 7)) == (0, Decimal("0"))

    @pytest.mark.parametrize("carried", [None, {"id": 1}, "x", 7],
                             ids=["null", "object", "text", "number"])
    def test_anything_but_a_list_is_nothing_to_compare(self, carried):
        assert keycrm_expense_facts(carried, 7) is None

    @pytest.mark.parametrize("entry", ["x", 7, None, ["id", 1]],
                             ids=["text", "number", "null", "list"])
    def test_an_entry_the_parse_cannot_read_is_nothing_to_compare(self, entry):
        """The writer refuses the whole order then (`_UNPARSEABLE`), and so
        writes and deletes none of its expenses. Mutation: count the entry,
        or skip it and compare the rest."""
        assert keycrm_expense_facts([{"id": 1, "amount": "1.00"}, entry], 7) is None

    def test_the_order_s_facts_carry_them_only_when_the_payload_did(self):
        """Mutation: record an absent key as no expenses — every order of a
        snapshot fetched without them would read as having lost its costs."""
        orders = _snapshot(_order(1, expenses=[{"id": 9, "amount": "10.00"}]),
                           _order(2, expenses=[]), _order(3))
        # Order 1's expense is counted, so the parse was handed the order's
        # id: without it every expense is refused (`order_id` is NOT NULL).
        assert _counted(orders[1]) == (1, Decimal("10.00"))
        assert _counted(orders[2]) == (0, Decimal("0"))
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


_OK = {"id": 1, "expense_type_id": 1, "amount": "10.50", "status": "paid"}


class TestOnlyWhatChain3Stores:
    """C-1: KeyCRM's side counts what chain 3 would store, never the list's
    length. With an entry the writer drops (no id) or refuses
    (`expense_refusal`) counted, the order read VALUE_MISMATCH every morning
    and was re-fetched every morning, and the re-fetch is refused the same
    way. Mutation killed by every case: count `len(list)`, or sum every
    amount the list carries."""

    @pytest.mark.parametrize("entry, left", [
        ({"id": 2, "expense_type_id": 1, "amount": None}, {2}),
        ({"expense_type_id": 1, "amount": "5.00"}, set()),
        ({"id": None, "expense_type_id": 1, "amount": "5.00"}, set()),
        ({"id": 2, "expense_type_id": 1, "amount": "5.00", "description": "a\x00b"}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": "5.00", "status": "paid\x00"}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": "10000000000.00"}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": "-9999999999.995"}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": "ten"}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": True}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": "NaN"}, {2}),
        ({"id": 2, "expense_type_id": 1, "amount": "Infinity"}, {2}),
        ({"id": "abc", "expense_type_id": 1, "amount": "5.00"}, set()),
        ({"id": "2", "expense_type_id": 1, "amount": "5.00"}, {2}),
        ({"id": 2 ** 31, "expense_type_id": 1, "amount": "5.00"}, {2 ** 31}),
        ({"id": 2, "expense_type_id": "x", "amount": "5.00"}, {2}),
    ], ids=["amount null", "id absent", "id null", "NUL in description",
            "NUL in status", "amount out of range", "amount rounds out of range",
            "amount is text", "amount is a boolean", "amount NaN",
            "amount infinite", "id is text", "id is a numeric string",
            "id past int32", "type id not an integer"])
    def test_an_entry_the_chain_does_not_write_is_left_out(self, entry, left):
        facts = keycrm_expense_facts([_OK, entry], 7)
        assert _counted(facts) == (1, Decimal("10.50"))
        # Its id, when it names one, is left out of Postgres' side too: the
        # row a re-fetch keeps as it is. Mutation: leave nothing out there.
        assert facts[EXPENSES_LEFT_AS_STORED] == frozenset(left)

    def test_one_id_given_twice_counts_once_as_the_last(self):
        """The writer keeps the last of one id. Mutation: count both."""
        facts = keycrm_expense_facts([dict(_OK), dict(_OK, amount="2.00")], 7)
        assert _counted(facts) == (1, Decimal("2.00"))

    def test_the_last_of_one_id_refused_is_none_written(self):
        """The last one given is refused, so the writer writes neither and
        keeps the stored row. Mutation: dedupe after the refusal."""
        facts = keycrm_expense_facts([dict(_OK), dict(_OK, amount=None)], 7)
        assert _counted(facts) == (0, Decimal("0"))
        assert facts[EXPENSES_LEFT_AS_STORED] == frozenset({1})

    def test_a_float_counts_as_asyncpg_writes_it(self):
        """asyncpg sends a float's binary value (`Decimal(obj)`), and
        `NUMERIC(12,2)` rounds that: 2.675 is 2.67499… and is stored as 2.67,
        1.005 as 1.00. Text is read as written: "2.675" is 2.68. Read from
        its text, a float summed to a cent above what Postgres holds, and the
        order read VALUE_MISMATCH every morning (C2R-1). Mutations: read a
        float from its text, or a text through a float."""
        assert Decimal(2.675) < Decimal("2.675")
        floats = keycrm_expense_facts(
            [dict(_OK, amount=2.675), dict(_OK, id=2, amount=1.005)], 7)
        assert _counted(floats) == (2, Decimal("3.67"))
        text = keycrm_expense_facts(
            [dict(_OK, amount="2.675"), dict(_OK, id=2, amount="1.005")], 7)
        assert _counted(text) == (2, Decimal("3.69"))

    def test_an_absent_amount_is_written_as_zero_and_counted(self):
        """`expense_row` writes an absent amount as 0; a null one is refused.
        Mutation: read the two alike."""
        facts = keycrm_expense_facts([_OK, {"id": 2, "expense_type_id": 1}], 7)
        assert _counted(facts) == (2, Decimal("10.50"))

    def test_the_order_s_own_id_is_what_the_parse_is_handed(self):
        """An order id the writer cannot write an expense under refuses every
        expense of it. Mutation: hand the parse the int()'d id."""
        assert _counted(keycrm_expense_facts([_OK], "7")) == (0, Decimal("0"))

    def test_it_is_the_writer_s_choice_not_a_copy(self):
        """The writer's own functions decide; nothing here restates them.
        Mutation: a copy of `expense_refusal`'s rule, or of the parse."""
        from core import landing_rows, pg_orders_write

        with patch.object(pg_orders_write, "expense_refusal", return_value="refused"):
            assert _counted(keycrm_expense_facts([_OK], 7)) == (0, Decimal("0"))
        real = landing_rows.expense_row
        with patch.object(landing_rows, "expense_row",
                          side_effect=lambda oid, e: real(oid, dict(e, amount="1.00"))):
            assert _counted(keycrm_expense_facts([_OK], 7)) == (1, Decimal("1.00"))

    def test_left_as_stored_names_only_the_orders_that_have_any(self):
        kc = {1: {"n_expenses": 1, EXPENSES_LEFT_AS_STORED: frozenset({11})},
              2: {"n_expenses": 0, EXPENSES_LEFT_AS_STORED: frozenset()},
              3: {"status_id": 12}}
        assert left_as_stored(kc) == {1: frozenset({11})}


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
