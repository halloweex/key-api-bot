"""`landed_orders` refuses an unreadable payload by id and parses the rest (L1-01).

A page of KeyCRM orders is parsed payload by payload: one the parse cannot
read — a line item's `price_sold` null, a `grand_total` that is not a
number, a `quantity` null, a `buyer` that is not an object — is listed in
`LandedOrders.refused` with its reason, and is in neither the headers nor the
line items. Until this, its `TypeError` left the parse and took the page.

What it keeps: an order with no `ordered_at` is still dropped silently with
its lines, duplicates are still left to the writer, and `orders, products =`
still unpacks the two lists.
"""
from __future__ import annotations

import pytest

from core.landing_rows import LINE_ITEM_ID_STRIDE, landed_orders

T = "2026-11-01T12:00:00+00:00"


def _payload(oid: int, **over) -> dict:
    p = {
        "id": oid, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": T, "created_at": T, "updated_at": T,
        "buyer": {"id": 1}, "manager": {"id": 4}, "manager_comment": None,
        "promocode": None,
        "products": [{"name": "x", "quantity": 1, "price_sold": "100.00",
                      "offer": {"product_id": 7}}],
    }
    p.update(over)
    return p


def _line(oid: int, **over) -> list:
    item = {"name": "x", "quantity": 1, "price_sold": "100.00", "offer": {"product_id": 7}}
    item.update(over)
    return [item]


BAD = {
    "price_sold None": {"products": _line(0, price_sold=None)},
    "quantity None": {"products": _line(0, quantity=None)},
    "quantity abc": {"products": _line(0, quantity="abc")},
    "grand_total abc": {"grand_total": "abc"},
    "grand_total None": {"grand_total": None},
    "buyer a string": {"buyer": "nobody"},
    "status_group_id abc": {"status_group_id": "abc"},
}


class TestOnePayloadIsOneOrder:
    @pytest.mark.parametrize("name", sorted(BAD))
    def test_the_bad_one_is_refused_and_the_good_one_lands(self, name):
        landed = landed_orders([_payload(1), _payload(2, **BAD[name]), _payload(3)])
        assert [r.id for r in landed.orders] == [1, 3]
        assert {p.order_id for p in landed.products} == {1, 3}
        assert [oid for oid, _ in landed.refused] == [2]
        assert landed.refused[0][1].startswith("unparseable: ")

    def test_a_payload_that_is_not_an_object_is_refused_without_an_id(self):
        landed = landed_orders([_payload(1), "garbage"])  # type: ignore[list-item]
        assert [r.id for r in landed.orders] == [1]
        assert [oid for oid, _ in landed.refused] == [None]

    def test_header_only_refuses_the_same_payloads(self):
        landed = landed_orders([_payload(1), _payload(2, grand_total="abc")],
                               with_products=False)
        assert [r.id for r in landed.orders] == [1]
        assert landed.products == []
        assert [oid for oid, _ in landed.refused] == [2]


class TestWhatItKeeps:
    def test_an_order_without_ordered_at_is_dropped_with_its_lines_not_refused(self):
        landed = landed_orders([_payload(1), _payload(2, ordered_at=None)])
        assert [r.id for r in landed.orders] == [1]
        assert {p.order_id for p in landed.products} == {1}
        assert landed.refused == ()

    def test_duplicates_are_left_to_the_writer(self):
        landed = landed_orders([_payload(1, grand_total="1.00"),
                                _payload(1, grand_total="2.00")])
        assert [r.grand_total for r in landed.orders] == [1.0, 2.0]

    def test_it_still_unpacks_into_the_two_lists(self):
        orders, products = landed_orders([_payload(5)])
        assert [r.id for r in orders] == [5]
        assert [p.id for p in products] == [5 * LINE_ITEM_ID_STRIDE]

    def test_a_clean_page_refuses_nothing(self):
        assert landed_orders([_payload(1), _payload(2)]).refused == ()
