"""The 05:30 reconciliation compares each order's discount (revision 0038).

Revision 0038 lands KeyCRM's discount with every order and versions none of
it, so this comparison is the only place a discount that drifts from KeyCRM
is seen. These pin the pure half: what the snapshot records per order, as
chain 3 would store it; when a figure is compared at all; how two sides are
classified; and where the repair puts the orders it names. The real
comparison, on a real server and through the job, is
`tests/integration/test_recon_compares_discounts.py`. Each test names the
mutation it kills.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import Discrepancy, DiscrepancyClass, Severity
from core.reconciliation_io import (
    DISCOUNT_FIELDS,
    REPAIRED_LAST,
    _process_batch,
    carries_discounts,
    classify_discount_discrepancies,
    keycrm_discount_facts,
)

WATERMARK = datetime(2026, 8, 4, tzinfo=timezone.utc)
WINDOW_START, WINDOW_END = date(2026, 6, 1), date(2026, 6, 30)
HEADER_KEYS = ("products_total", "total_discount", "discount_amount",
               "discount_percent", "discount_data")
LINE_KEYS = ("price", "total_discount", "discount_percent", "discount_amount",
             "loyalty_discount", "individual_discount")


def _line(per_unit="389", qty=2, **extra):
    """980 → 591 is 389 off EACH unit (revision 0038's example)."""
    line = {"name": "Товар", "quantity": qty, "price_sold": "591", "price": "980",
            "total_discount": per_unit, "discount_percent": "39.6939",
            "discount_amount": 0, "loyalty_discount": 0, "individual_discount": 0}
    line.update(extra)
    return line


def _order(oid=7, **extra):
    """Two units at 591, the order 10 % off: 1182.00 before it, 118.20 off,
    1063.80 to pay."""
    o = {"id": oid, "source_id": 4, "status_id": 12,
         "ordered_at": "2026-06-10T09:00:00+00:00",
         "updated_at": "2026-06-10T09:00:00+00:00",
         "grand_total": "1063.80", "products_total": "1182.00",
         "total_discount": "118.20", "discount_amount": 0, "discount_percent": 10,
         "discount_data": [], "products": [_line()]}
    o.update(extra)
    return o


def _old_shape(o):
    """The payload without any discount key, header or line."""
    o = {k: v for k, v in o.items() if k not in HEADER_KEYS}
    o["products"] = [{k: v for k, v in p.items() if k not in LINE_KEYS}
                     for p in o.get("products", [])]
    return o


def _snapshot(*orders):
    out, inflight = {}, set()
    _process_batch(list(orders), out, WATERMARK, WINDOW_START, WINDOW_END, inflight)
    return out


def _discount(facts):
    return {f: facts[f] for f in DISCOUNT_FIELDS if f in facts}


class TestWhatTheSnapshotRecords:
    def test_the_three_figures_of_an_order(self):
        assert _discount(_snapshot(_order())[7]) == {
            "products_total": Decimal("1182.00"),
            "total_discount": Decimal("118.20"),
            "line_discount": Decimal("778.0000"),
        }

    def test_a_line_s_discount_is_per_unit_times_quantity(self):
        """Mutation: sum the per-unit figures — 389 against the 778 off."""
        facts = keycrm_discount_facts(_order(products=[_line("389", 2), _line("10.5", 3)]))
        assert facts["line_discount"] == Decimal("778") + Decimal("31.5")

    def test_each_figure_rounds_as_its_column_does(self):
        """Half away from zero, to the column's scale, each line's unit on
        its own — what `_discount` lands. Mutations: compare KeyCRM's figure
        as served (0.125 against a stored 0.13, a mismatch every morning),
        round half to even (0.12), or round the line's sum once (100.0001
        where the column holds 33.3334 × 3 = 100.0002). A float is read by
        its text, as the parse reads it."""
        facts = keycrm_discount_facts(_order(
            products_total=1182.005, total_discount="0.125",
            products=[_line("33.33335", 3)]))
        assert facts == {"products_total": Decimal("1182.01"),
                         "total_discount": Decimal("0.13"),
                         "line_discount": Decimal("100.0002")}

    def test_an_order_with_no_lines_has_no_line_discount_to_speak_of(self):
        assert keycrm_discount_facts(_order(products=[]))["line_discount"] == 0

    def test_a_line_whose_discount_is_null_makes_the_order_s_null(self):
        """The column then holds NULL on that line, and the order's figure
        is None on both sides. Mutation: skip the line and sum the rest — a
        line the store lost (an image below 0038 rewrote it) would hide
        behind its order's other lines."""
        facts = keycrm_discount_facts(_order(products=[_line(), _line(None)]))
        assert facts["line_discount"] is None

    @pytest.mark.parametrize("value", ["abc", "", True, float("nan"), 10 ** 12, "1e30"],
                             ids=["text", "empty", "bool", "nan", "too-large", "exp"])
    def test_a_figure_the_column_cannot_hold_is_none_as_stored(self, value):
        """`_discount` lands it as NULL and the order with it; the snapshot
        records the same None. Mutation: record it as served, or drop the
        order's facts."""
        facts = keycrm_discount_facts(_order(total_discount=value,
                                             products=[_line(value)]))
        assert facts["total_discount"] is None
        assert facts["line_discount"] is None
        assert facts["products_total"] == Decimal("1182.00")

    def test_a_null_the_payload_carries_is_compared_as_none(self):
        facts = keycrm_discount_facts(_order(products_total=None))
        assert "products_total" in facts and facts["products_total"] is None


class TestOnlyWhatThePayloadCarries:
    """A figure the payload does not carry is not compared: a snapshot
    without the key must not read as every discount gone, nor send the
    repair to overwrite what the backfill landed. Mutation killed by each:
    record an absent key as None."""

    def test_a_payload_of_the_old_shape_carries_none(self):
        assert _discount(_snapshot(_old_shape(_order()))[7]) == {}

    def test_each_header_figure_on_its_own(self):
        o = _order()
        del o["total_discount"]
        assert set(keycrm_discount_facts(o)) == {"products_total", "line_discount"}

    def test_lines_without_the_key_carry_no_line_discount(self):
        o = _order(products=[_line(), {k: v for k, v in _line().items()
                                       if k != "total_discount"}])
        assert "line_discount" not in keycrm_discount_facts(o)

    def test_no_products_list_carries_no_line_discount(self):
        o = _order()
        del o["products"]
        assert "line_discount" not in keycrm_discount_facts(o)

    def test_the_lines_are_the_lines_the_snapshot_asks_for(self):
        """`line_discount` is read off the lines the snapshot embeds. Mutation:
        drop `products` from the include — every line's discount would go
        uncompared without a word."""
        from core.reconciliation_io import SNAPSHOT_INCLUDE

        assert "products" in SNAPSHOT_INCLUDE.split(",")

    @pytest.mark.parametrize("shape", [
        {"manager_comment": "utm\x00"},
        {"products": [_line(name="Тов\x00ар")]},
        {"grand_total": "not a number"},
    ], ids=["nul-in-comment", "nul-in-a-line", "unparseable"])
    def test_an_order_chain_3_refuses_whole_carries_none(self, shape):
        """C2R-2's rule for expenses: chain 3 neither writes such an order
        nor its re-fetch, so a discount that differs would be named and
        re-fetched every morning for nothing. Mutation: compare it."""
        facts = _snapshot(_order(**shape)).get(7)
        if facts is not None:
            assert _discount(facts) == {}

    def test_only_orders_carrying_a_figure_are_asked_of_postgres(self):
        kc = _snapshot(_order(1), _old_shape(_order(2)), _order(3))
        assert carries_discounts(kc, [1, 2, 3, 4]) == [1, 3]


def _facts(products_total="1182.00", total_discount="118.20", lines="778.0000"):
    def dec(v):
        return None if v is None else Decimal(v)
    return {"products_total": dec(products_total), "total_discount": dec(total_discount),
            "line_discount": dec(lines)}


def _kc(**figures):
    return {"status_id": 12, "source_id": 4, "order_date": date(2026, 6, 10),
            **_facts(**figures)}


class TestTheComparison:
    def _one(self, dk, kc):
        return {(d.diff_class, d.field): d for d in classify_discount_discrepancies(dk, kc)}

    def test_the_same_figures_in_another_scale_are_no_finding(self):
        assert self._one({1: _facts("1182", "118.2", "778")}, {1: _kc()}) == {}

    def test_a_kopeck_on_the_header_is_a_difference(self):
        out = self._one({1: _facts(total_discount="118.21")}, {1: _kc()})
        assert set(out) == {(DiscrepancyClass.VALUE_MISMATCH, "total_discount")}
        (d,) = out.values()
        assert d.severity == Severity.WARN
        assert d.order_ids == (1,)
        assert (d.month, d.source_id) == ("2026-06", 4)

    def test_a_ten_thousandth_on_the_lines_is_a_difference(self):
        """Tolerance zero. Mutation: compare as money with the order
        comparison's 0.01 tolerance, or as a float."""
        out = self._one({1: _facts(lines="778.0001")}, {1: _kc()})
        assert set(out) == {(DiscrepancyClass.VALUE_MISMATCH, "line_discount")}

    @pytest.mark.parametrize("field", DISCOUNT_FIELDS)
    def test_a_null_against_a_figure_is_a_difference(self, field):
        """Both ways: the store lost it (a writer below 0038), or KeyCRM
        dropped it. Mutation: read NULL as zero."""
        nulled = {"products_total": {"products_total": None},
                  "total_discount": {"total_discount": None},
                  "line_discount": {"lines": None}}[field]
        assert set(self._one({1: _facts(**nulled)}, {1: _kc()})) == {
            (DiscrepancyClass.VALUE_MISMATCH, field)}
        assert set(self._one({1: _facts()}, {1: _kc(**nulled)})) == {
            (DiscrepancyClass.VALUE_MISMATCH, field)}

    def test_null_against_null_is_no_finding(self):
        assert self._one({1: _facts(None, None, None)}, {1: _kc(
            products_total=None, total_discount=None, lines=None)}) == {}

    def test_a_figure_keycrm_did_not_carry_is_not_compared(self):
        kc = _kc()
        del kc["line_discount"]
        assert self._one({1: _facts(lines=None)}, {1: kc}) == {}

    def test_the_identity_of_the_three_is_never_compared(self):
        """`products_total - total_discount = grand_total` is KeyCRM's data,
        broken on 81 of 50 723 orders (shipping and the like), and a gift
        certificate's redemption is an order discount there. Both sides
        agreeing, nothing is named whatever the arithmetic. Mutation: check
        the identity on either side."""
        kc = {1: {**_kc(), "grand_total": 1163.80},          # 100 of shipping
              2: {**_kc(total_discount="1182.00"), "grand_total": 0.0}}  # a certificate
        dk = {1: _facts(), 2: _facts(total_discount="1182.00")}
        assert self._one(dk, kc) == {}

    def test_an_order_on_one_side_only_is_not_a_discount_finding(self):
        """The header comparison names it MISSING_IN_DK or MISSING_IN_KC."""
        assert classify_discount_discrepancies({}, {1: _kc()}) == []
        assert classify_discount_discrepancies({1: _facts()}, {}) == []


class TestTheRepairTakesThemLast:
    def test_the_discount_is_repaired_last_with_the_expenses(self):
        assert set(DISCOUNT_FIELDS) <= set(REPAIRED_LAST)
        assert {"n_expenses", "expense_amount"} <= set(REPAIRED_LAST)
        assert not {"status_id", "grand_total", "line_amount", "n_lines",
                    "qty"} & set(REPAIRED_LAST)

    @pytest.mark.asyncio
    async def test_after_the_missing_and_the_drifted(self):
        """An order whose discount differs is re-fetched in the same by-id
        batch, after every order missing or drifted — the batch is bounded,
        and a discount moves no revenue figure — and an order both drifted
        and with its discount differing goes once, with the drifts.
        Mutations: leave them out of the batch, or sort them in among the
        drifts."""
        from core.scheduler import BackgroundScheduler

        def d(klass, field, *ids, severity=Severity.WARN):
            return Discrepancy(month="2026-09", source_id=4, diff_class=klass,
                               field=field, dk_value=float(len(ids)), kc_value=0.0,
                               severity=severity, order_ids=tuple(ids))

        pg = {"issues": [], "error": None, "discrepancies": [
            d(DiscrepancyClass.VALUE_MISMATCH, "line_discount", 1, 30),
            d(DiscrepancyClass.VALUE_MISMATCH, "total_discount", 1, 35),
            d(DiscrepancyClass.VALUE_MISMATCH, "expense_amount", 33),
            d(DiscrepancyClass.STATUS_DRIFT, "status_id", 20, severity=Severity.CRITICAL),
            d(DiscrepancyClass.VALUE_MISMATCH, "grand_total", 25),
            d(DiscrepancyClass.MISSING_IN_DK, "order", 40, severity=Severity.CRITICAL),
            d(DiscrepancyClass.VALUE_MISMATCH, "products_total", 20),
        ]}
        sync = AsyncMock()
        sync.repair_orders = AsyncMock(return_value={"repaired": 7, "remaining": 0})
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
        assert sync.repair_orders.await_args.args[0] == [40, 20, 25, 1, 30, 33, 35]
