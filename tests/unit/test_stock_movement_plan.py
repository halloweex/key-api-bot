"""The stock-movement classification, pinned before it gains a second caller.

`plan_stock_movements` was lifted out of `DuckDBStore.upsert_stocks` unchanged.
It moves because stage 4 gives it a Postgres caller, and because of all the
rules in the inventory chain this is the one that must not have two homes:

A stock movement is computed once or never. KeyCRM serves current stock and has
no history endpoint, so the previous quantity exists only in whichever store
last wrote it — and a movement not recorded at the moment it happened cannot be
recovered by any backfill. Two implementations would not fail loudly. They
would disagree about whether a change was `stock_in`, `stock_out` or
`reserve_change`, and the wrong verdict would be written into the only record
there will ever be.

These are characterisation tests: they describe what the DuckDB implementation
did, including the parts that look like oversights and are not.
"""
from __future__ import annotations

import pytest

from core.landing_rows import (
    MOVEMENT_IN,
    MOVEMENT_INITIAL,
    MOVEMENT_OUT,
    MOVEMENT_RESERVE,
    STOCK_MOVEMENT_COLUMNS,
    plan_stock_movements,
)


def _stock(offer_id, quantity=0, reserve=0):
    return {"id": offer_id, "quantity": quantity, "reserve": reserve}


class TestTheFourVerdicts:
    def test_a_new_offer_with_stock_is_initial_against_zero(self):
        [m] = plan_stock_movements(
            [_stock(1, quantity=5, reserve=2)], current={}, product_map={1: 77})
        assert m.movement_type == MOVEMENT_INITIAL
        assert (m.quantity_before, m.quantity_after, m.delta) == (0, 5, 5)
        assert (m.reserve_before, m.reserve_after) == (0, 2)
        assert m.product_id == 77

    def test_a_new_offer_with_no_stock_produces_nothing(self):
        """Not an oversight. Otherwise every catalogue sync writes one row per
        new SKU, and a zero-to-zero 'movement' is not a fact about anything."""
        assert plan_stock_movements(
            [_stock(1)], current={}, product_map={}) == []

    def test_more_stock_is_stock_in(self):
        [m] = plan_stock_movements(
            [_stock(1, quantity=9)], current={1: (4, 0)}, product_map={})
        assert m.movement_type == MOVEMENT_IN
        assert m.delta == 5

    def test_less_stock_is_stock_out(self):
        [m] = plan_stock_movements(
            [_stock(1, quantity=1)], current={1: (4, 0)}, product_map={})
        assert m.movement_type == MOVEMENT_OUT
        assert m.delta == -3

    def test_only_the_reserve_moving_is_a_reserve_change(self):
        """`delta == 0` must not be called `stock_in`: the column is read as a
        direction, and zero has none."""
        [m] = plan_stock_movements(
            [_stock(1, quantity=4, reserve=3)], current={1: (4, 1)},
            product_map={})
        assert m.movement_type == MOVEMENT_RESERVE
        assert m.delta == 0
        assert (m.reserve_before, m.reserve_after) == (1, 3)

    def test_nothing_moving_produces_nothing(self):
        assert plan_stock_movements(
            [_stock(1, quantity=4, reserve=1)], current={1: (4, 1)},
            product_map={}) == []


class TestTheDangerousEdges:
    def test_an_unknown_offer_still_records_the_movement(self):
        """`product_id` comes from a lookup that can miss — an offer whose
        `offers` row has not arrived yet. Losing the denormalised product is
        survivable; losing the movement is not, so it is written with None and
        the column is nullable for this reason."""
        [m] = plan_stock_movements(
            [_stock(1, quantity=5)], current={}, product_map={})
        assert m.product_id is None
        assert m.movement_type == MOVEMENT_INITIAL

    def test_an_empty_delta_base_relabels_everything_as_initial(self):
        """The failure this chain's ordering exists to prevent, asserted so it
        is understood rather than discovered: if the store that holds the
        previous stock stops being read, every changed offer looks new."""
        stocks = [_stock(1, quantity=9), _stock(2, quantity=3)]
        against_truth = plan_stock_movements(
            stocks, current={1: (4, 0), 2: (3, 0)}, product_map={})
        against_nothing = plan_stock_movements(
            stocks, current={}, product_map={})
        assert [m.movement_type for m in against_truth] == [MOVEMENT_IN]
        assert [m.movement_type for m in against_nothing] == [
            MOVEMENT_INITIAL, MOVEMENT_INITIAL]

    def test_missing_quantity_and_reserve_default_to_zero(self):
        """KeyCRM omits them rather than sending null on some payloads."""
        [m] = plan_stock_movements(
            [{"id": 1}], current={1: (4, 1)}, product_map={})
        assert (m.quantity_after, m.reserve_after) == (0, 0)
        assert m.movement_type == MOVEMENT_OUT


class TestItIsPure:
    def test_it_touches_neither_argument(self):
        current = {1: (4, 0)}
        product_map = {1: 77}
        stocks = [_stock(1, quantity=9)]
        plan_stock_movements(stocks, current=current, product_map=product_map)
        assert current == {1: (4, 0)}
        assert product_map == {1: 77}
        assert stocks == [_stock(1, quantity=9)]

    def test_the_column_order_is_the_tuple_order(self):
        """Both engines bind positionally from `STOCK_MOVEMENT_COLUMNS`. If the
        NamedTuple's field order and this tuple ever disagree, every movement
        is written with its columns shuffled — and silently, because they are
        all integers except one."""
        [m] = plan_stock_movements(
            [_stock(1, quantity=5)], current={}, product_map={1: 77})
        assert tuple(m._fields) == STOCK_MOVEMENT_COLUMNS
        assert tuple(m) == (1, 77, MOVEMENT_INITIAL, 0, 5, 5, 0, 0)


class TestTheDuckDBWriterUsesIt:
    def test_upsert_stocks_does_not_classify_for_itself(self):
        """By tree: the repository may read the previous state and write rows,
        but the verdict must come from the shared planner."""
        import ast
        import inspect

        from core.repositories import inventory

        src = inspect.getsource(inventory.InventoryMixin.upsert_stocks)
        tree = ast.parse(src.lstrip())
        calls = {
            n.func.id for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
        assert "plan_stock_movements" in calls
        literals = {
            n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
        }
        for verdict in (MOVEMENT_IN, MOVEMENT_OUT, MOVEMENT_RESERVE,
                        MOVEMENT_INITIAL):
            assert verdict not in literals, (
                f"{verdict!r} is spelled in the repository again — the "
                "classification has grown a second home"
            )
