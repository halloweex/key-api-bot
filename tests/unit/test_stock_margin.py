"""What `/api/stock-margin` tells an admin, on rows small enough to check by hand.

`core.stock_margin` decides everything the block says — which rows are goods,
each margin, the flags, the tiles — from the query's raw rows, so none of this
needs a database. The query itself is held by
tests/integration/test_stock_margin_postgres.py, and who may ask by
tests/integration/test_stock_margin_permissions.py.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from core import stock_margin as sm

STAMP = datetime(2026, 11, 26, 17, 58, tzinfo=timezone.utc)


def row(offer_id, name="Cream", price=600, cost=300, *, qty=10, reserve=0,
        units=0, revenue=0, moved=90, brand="Brand", last=None, stamp=STAMP):
    """One row in `ROW_COLUMNS` order, typed as asyncpg returns it. By default
    one unit a day leaves the stock, so nothing is slow unless a test says so."""
    return (offer_id, offer_id * 10, f"SKU-{offer_id}", name, brand, "Creams", "Face",
            qty, reserve,
            None if price is None else Decimal(str(price)),
            None if cost is None else Decimal(str(cost)),
            units, Decimal(str(revenue)), moved,
            last, stamp)


def one(*args, **kwargs):
    return sm.item(row(*args, **kwargs))


class TestWhatCountsAsGoods:
    @pytest.mark.parametrize("name", [
        "Gift Certificate Подарунковий сертифікат",
        "Подарочный сертификат",
        "Gift certificate",
    ])
    def test_a_certificate_in_any_of_the_three_languages(self, name):
        assert sm.kind_of(name, 1000) == sm.CERTIFICATE

    @pytest.mark.parametrize("name,price", [
        ("Сироватка 10ml Тестер", 450),
        ("SERUM 10 мл ТЕСТЕР", 450),
        ("Mini tester", 80),
        ("Пакунок-сюрприз", 1),
        ("Картка", 0.5),
    ])
    def test_testers_and_giveaways_at_one_hryvnia_are_promo(self, name, price):
        assert sm.kind_of(name, price) == sm.PROMO

    def test_a_price_of_zero_is_missing_data_not_a_giveaway(self):
        assert sm.kind_of("Крем", 0) == sm.GOODS
        it = one(1, price=0, cost=None)
        assert (it["kind"], it["costed"], it["flags"]) == (sm.GOODS, False, [sm.NO_PRICE])

    def test_an_ordinary_product_is_goods(self):
        assert sm.kind_of("Крем для обличчя", 600) == sm.GOODS


class TestOneOffer:
    def test_the_figures_of_a_plain_offer(self):
        it = one(1, price=600, cost=300, qty=10, reserve=2)
        assert it["available"] == 8
        assert (it["margin"], it["markup"], it["unit_profit"]) == (0.5, 1.0, 300.0)
        assert (it["retail_value"], it["cost_value"], it["profit"]) == (4800.0, 2400.0, 2400.0)
        assert it["costed"] is True and it["flags"] == []

    def test_available_never_goes_below_zero(self):
        it = one(1, qty=1, reserve=3)
        assert it["available"] == 0 and it["retail_value"] == 0.0
        assert it["flags"] == [sm.RESERVED]

    def test_the_realized_margin_and_the_discount(self):
        it = one(1, price=600, cost=300, units=4, revenue=2000)
        assert it["avg_price_90d"] == 500.0
        assert it["discount_90d"] == round(1 - 500 / 600, 6)
        assert it["realized_margin_90d"] == 0.4

    @pytest.mark.parametrize("cost", [None, 0])
    def test_no_cost_is_no_margin_not_a_zero(self, cost):
        it = one(1, cost=cost)
        assert (it["margin"], it["profit"], it["cost_value"]) == (None, None, None)
        assert it["flags"] == [sm.NO_COST]

    def test_cost_at_the_price_and_above_it(self):
        assert one(1, price=100, cost=100)["flags"] == [sm.COST_OVER_PRICE]
        above = one(2, price=100, cost=120)
        assert above["margin"] == -0.2 and above["flags"] == [sm.COST_OVER_PRICE]

    def test_a_margin_above_85_percent_asks_to_check_the_cost(self):
        it = one(1, price=1000, cost=100)
        assert it["margin"] == 0.9 and it["flags"] == [sm.HIGH_MARGIN_FLAG]

    def test_sold_below_cost(self):
        it = one(1, price=600, cost=300, units=2, revenue=400)
        assert it["realized_margin_90d"] == -0.5
        assert it["flags"] == [sm.SOLD_BELOW_COST]

    def test_only_given_away(self):
        it = one(1, units=3, revenue=0)
        assert it["realized_margin_90d"] is None
        assert it["flags"] == [sm.ONLY_FREE]

    def test_days_of_supply_count_every_channel(self):
        """A wholesale sale empties the same shelf as a retail one."""
        it = one(1, qty=90, units=0, moved=30)
        assert it["days_of_supply"] == 270.0
        assert it["realized_margin_90d"] is None and not it["slow"]

    def test_slow_when_nothing_left_the_stock(self):
        it = one(1, qty=5, moved=0)
        assert it["slow"] and it["days_of_supply"] is None
        assert it["flags"] == [sm.SLOW]

    def test_slow_past_a_year_of_stock_and_not_at_it(self):
        assert one(1, qty=400, moved=90)["slow"]
        assert not one(2, qty=365, moved=90)["slow"]

    def test_a_certificate_and_a_tester_carry_no_margin_and_no_flags(self):
        cert = one(1, name="Gift Certificate", price=1000, cost=40, moved=0)
        tester = one(2, name="Сироватка 10 мл Тестер", price=450, cost=30, moved=0)
        for it in (cert, tester):
            assert it["costed"] is False
            assert (it["margin"], it["profit"], it["slow"]) == (None, None, False)
            assert it["flags"] == []


class TestTheTiles:
    def test_the_margin_is_weighted_by_money_not_averaged(self):
        out = sm.build([row(1, price=1000, cost=400, qty=100), row(2, price=100, cost=90, qty=1)])
        g = out["goods"]
        assert g["margin"] == round(60_010 / 100_100, 6)       # a simple mean would say 0.35
        assert g["median_margin"] == 0.35
        assert (g["retail"], g["cost"], g["profit"]) == (100_100.0, 40_090.0, 60_010.0)

    def test_certificates_and_testers_stay_out_of_the_goods(self):
        out = sm.build([
            row(1, price=600, cost=300),
            row(2, name="Gift Certificate", price=1000, cost=40, qty=5),
            row(3, name="Крем 3 мл Тестер", price=80, cost=30, qty=4),
            row(4, name="Пакунок", price=1, cost=400, qty=2),
        ])
        assert out["goods"]["positions"] == 1
        assert out["goods"]["profit"] == 3000.0
        assert out["excluded"] == {
            "certificate": {"positions": 1, "available": 5, "retail_value": 5000.0},
            "promo": {"positions": 2, "available": 6, "retail_value": 322.0},
        }

    def test_the_ninety_day_figures_count_a_free_unit(self):
        out = sm.build([
            row(1, price=600, cost=300, units=4, revenue=2000),
            row(2, price=100, cost=50, units=2, revenue=0),
        ])
        g = out["goods"]
        assert g["sold"] == 2
        assert (g["revenue_90d"], g["list_90d"], g["cogs_90d"]) == (2000.0, 2600.0, 1300.0)
        assert g["discount_90d"] == round(1 - 2000 / 2600, 6)
        assert g["list_margin_90d"] == 0.5
        assert g["realized_margin_90d"] == 0.35

    def test_slow_profit_and_the_problems_count_each_offer_once(self):
        out = sm.build([
            row(1, price=600, cost=300, qty=10, moved=0),                 # slow: nothing left
            row(2, price=600, cost=None),                                 # no cost
            row(3, price=600, cost=300, units=2, revenue=400),            # below cost
            row(4, price=1000, cost=100, units=2, revenue=100),           # high margin and below cost
            row(5, price=0, cost=None),                                   # no price
            row(6, price=600, cost=300, qty=400, moved=90),               # slow: 400 days
        ])
        g = out["goods"]
        assert (g["slow"], g["slow_profit"]) == (2, 3_000.0 + 120_000.0)
        assert g["problems"] == {
            sm.NO_PRICE: 1, sm.NO_COST: 1, sm.COST_OVER_PRICE: 0,
            sm.HIGH_MARGIN_FLAG: 1, sm.SOLD_BELOW_COST: 2, sm.ONLY_FREE: 0,
        }
        assert g["problem_items"] == 4

    def test_buckets_are_judged_on_the_margin_as_shown(self):
        """40,0 % is "40–50 %"; 49,996 %, shown as 50,0 %, is "50–60 %"."""
        out = sm.build([
            row(1, price=100, cost=120),            # −20 %
            row(2, price=100, cost=100),            # 0 %
            row(3, price=100, cost=80),             # 20 %
            row(4, price=100, cost=60),             # 40 %
            row(5, price=100_000, cost=50_004),     # 49,996 %
            row(6, price=100, cost=50),             # 50 %
            row(7, price=100, cost=40),             # 60 %
        ])
        assert [b["count"] for b in out["goods"]["buckets"]] == [1, 1, 1, 0, 1, 2, 1]
        assert [(b["from"], b["to"]) for b in out["goods"]["buckets"]] == list(sm.BUCKETS)

    def test_nothing_in_stock(self):
        out = sm.build([])
        g = out["goods"]
        assert out["items"] == [] and out["snapshot_at"] is None
        assert (g["positions"], g["margin"], g["realized_margin_90d"], g["median_margin"]) == (0, None, None, None)
        assert [b["count"] for b in g["buckets"]] == [0] * len(sm.BUCKETS)

    def test_goods_without_any_cost_give_no_margin_and_no_division(self):
        g = sm.build([row(1, cost=None), row(2, cost=None)])["goods"]
        assert (g["margin"], g["markup"], g["discount_90d"]) == (None, None, None)
        assert g["problems"][sm.NO_COST] == 2

    def test_the_snapshot_is_the_newest_stamp(self):
        later = datetime(2026, 11, 26, 18, 58, tzinfo=timezone.utc)
        out = sm.build([row(1), row(2, stamp=later)])
        assert out["snapshot_at"] == later.isoformat()

    def test_dates_travel_as_iso_text(self):
        it = one(1, last=date(2026, 11, 20))
        assert it["last_sale_date"] == "2026-11-20"
