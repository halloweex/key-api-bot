"""The margin of what is on the shelf now: one row per offer in stock.

Asked of ``GET /api/stock-margin`` (``web/routes/api/stock_margin.py``), which
only an admin may open (owner's decision, 2026-11-26). The query
(``MarginMixin.get_stock_margin_rows``) returns raw rows; everything a reader
is told — which rows are goods, the margin, what to check — is decided here,
in plain Python with no database, so the tests can state it exactly.

WHAT COUNTS AS GOODS

Gift certificates are a prepayment, not goods: a certificate's purchase price
is the card it is printed on, so it would read as a margin of nearly 100 %.
Testers and offers priced at ₴1 are given away, and a tester's
card often carries the full-size price. Both are reported apart (``kind``) and
stay out of every headline figure. An offer whose price is 0 is *missing
data*, not a giveaway: it stays among the goods and is flagged, so somebody
sets its price in KeyCRM. (No production figure is written here or in the
tests: the repository is public and these are purchase prices.)

THE FIGURES

* margin = (price − cost) / price, from the KeyCRM card as it is now;
  markup = (price − cost) / cost.
* Money on the shelf is counted on available units, quantity − reserve
  floored at 0, as ``/inventory`` counts it.
* The stock's margin is weighted by money: all the profit on the shelf over
  all of it at list price. A simple mean would let a ₴40 sachet weigh as
  much as a best-seller.
* The 90-day figures are the ``/margin`` tab's: retail lines, returns
  excluded, against today's purchase price — the cost on the day of sale is
  not kept anywhere. A unit given away for nothing counts, its cost was
  spent; so the discount is "discounts and gifts" together.
* Days of supply divide available units by what left the stock in the same
  90 days through every channel — retail, wholesale, the offline shop, the
  exhibition — because all of them empty the same shelf.
* "Slow" is a costed offer with stock and either no unit out in 90 days or
  more than a year of supply.
"""
from __future__ import annotations

import math
import statistics
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence

GOODS, CERTIFICATE, PROMO = "goods", "certificate", "promo"
KINDS = (GOODS, CERTIFICATE, PROMO)

WINDOW_DAYS = 90
LOW_MARGIN = 0.30      # the one threshold the page colours a margin by
HIGH_MARGIN = 0.85     # above it the purchase price looks mistyped
SLOW_DAYS = 365

# Flags, in the order a row shows them. The first six are problems — the
# "Проверить" count is the goods that carry any of them; the last two only
# describe the row.
NO_PRICE = "no_price"
NO_COST = "no_cost"
COST_OVER_PRICE = "cost_over_price"
HIGH_MARGIN_FLAG = "high_margin"
SOLD_BELOW_COST = "sold_below_cost"
ONLY_FREE = "only_free"
SLOW = "slow"
RESERVED = "reserved"
PROBLEM_FLAGS = (NO_PRICE, NO_COST, COST_OVER_PRICE, HIGH_MARGIN_FLAG,
                 SOLD_BELOW_COST, ONLY_FREE)
FLAGS = PROBLEM_FLAGS + (SLOW, RESERVED)

# The margin distribution, judged on the margin as the page shows it (a
# tenth of a percent) with the lower edge included: 40.0 % is "40–50 %", and
# 49.96 %, shown as "50.0 %", is "50–60 %". Judged on the raw value the two
# disagreed with the row beside them.
BUCKETS = ((None, 0.0), (0.0, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5),
           (0.5, 0.6), (0.6, None))

_CERTIFICATE_WORDS = ("сертифікат", "сертификат", "certificate")
_TESTER_WORDS = ("тестер", "tester")

# The columns `MarginMixin.get_stock_margin_rows` selects, in its order.
ROW_COLUMNS = (
    "offer_id", "product_id", "sku", "name", "brand", "category",
    "root_category", "quantity", "reserve", "price", "purchased_price",
    "retail_units", "retail_revenue", "moved_units", "last_sale_date",
    "updated_at",
)


def kind_of(name: Optional[str], price: Optional[float]) -> str:
    n = (name or "").lower()
    if any(w in n for w in _CERTIFICATE_WORDS):
        return CERTIFICATE
    if any(w in n for w in _TESTER_WORDS) or (price is not None and 0 < price <= 1):
        return PROMO
    return GOODS


def shown(ratio: float) -> float:
    """A ratio as the page shows it, to a tenth of a percent, half up."""
    return math.floor(ratio * 1000 + 0.5) / 1000


def bucket_of(margin: float) -> int:
    x = shown(margin)
    for i, (lo, hi) in enumerate(BUCKETS):
        if (lo is None or x >= lo) and (hi is None or x < hi):
            return i
    raise AssertionError(f"no bucket for {margin}")  # the edges cover the line


def _num(v: Any) -> Optional[float]:
    if v is None:
        return None
    return float(v) if isinstance(v, (Decimal, int, float)) else float(str(v))


def _money(v: Optional[float]) -> Optional[float]:
    return None if v is None else round(v, 2)


def _ratio(v: Optional[float]) -> Optional[float]:
    return None if v is None else round(v, 6)


def item(row: Sequence[Any]) -> Dict[str, Any]:
    """One offer as the page reads it."""
    r = dict(zip(ROW_COLUMNS, row))
    price = _num(r["price"]) or 0.0
    purchased = _num(r["purchased_price"])
    cost = purchased if purchased is not None and purchased > 0 else None
    quantity, reserve = int(r["quantity"] or 0), int(r["reserve"] or 0)
    available = max(0, quantity - reserve)
    kind = kind_of(r["name"], price)
    costed = kind == GOODS and cost is not None and price > 0
    # The cost as a number for the arithmetic below. Every use is behind
    # `costed`, which implies a cost, but a type checker cannot see through it.
    unit_cost = cost if cost is not None else 0.0

    units = _num(r["retail_units"]) or 0.0
    revenue = _num(r["retail_revenue"]) or 0.0
    moved = _num(r["moved_units"]) or 0.0
    margin = (price - unit_cost) / price if costed else None
    avg_price = revenue / units if units > 0 else None
    realized = (revenue - unit_cost * units) / revenue if costed and units > 0 and revenue > 0 else None
    days = available / (moved / WINDOW_DAYS) if available > 0 and moved > 0 else None
    slow = costed and available > 0 and (moved <= 0 or (days is not None and days > SLOW_DAYS))

    flags: List[str] = []
    if kind == GOODS:
        if price <= 0:
            flags.append(NO_PRICE)
        elif cost is None:
            flags.append(NO_COST)
        elif cost >= price:
            flags.append(COST_OVER_PRICE)
        elif margin is not None and margin > HIGH_MARGIN:
            flags.append(HIGH_MARGIN_FLAG)
        if realized is not None and realized < 0:
            flags.append(SOLD_BELOW_COST)
        if units > 0 and revenue <= 0:
            flags.append(ONLY_FREE)
    if slow:
        flags.append(SLOW)
    if available == 0:
        flags.append(RESERVED)

    last_sale = r["last_sale_date"]
    brand = (r["brand"] or "").strip() or None
    return {
        "offer_id": r["offer_id"],
        "product_id": r["product_id"],
        "sku": r["sku"],
        "name": r["name"] or "",
        "brand": brand,
        "category": r["category"],
        "root_category": r["root_category"],
        "kind": kind,
        "quantity": quantity,
        "reserve": reserve,
        "available": available,
        "price": _money(price) if price > 0 else None,
        "cost": _money(cost),
        "costed": costed,
        "margin": _ratio(margin),
        "markup": _ratio((price - unit_cost) / unit_cost) if costed else None,
        "unit_profit": _money(price - unit_cost) if costed else None,
        "retail_value": _money(available * price) if price > 0 else None,
        "cost_value": _money(available * unit_cost) if costed else None,
        "profit": _money(available * (price - unit_cost)) if costed else None,
        "units_90d": units,
        "revenue_90d": _money(revenue),
        "avg_price_90d": _money(avg_price),
        "discount_90d": _ratio(1 - avg_price / price) if avg_price is not None and price > 0 else None,
        "realized_margin_90d": _ratio(realized),
        "moved_90d": moved,
        "days_of_supply": None if days is None else round(days, 1),
        "slow": slow,
        "last_sale_date": last_sale.isoformat() if isinstance(last_sale, date) else last_sale,
        "flags": flags,
    }


def summary(goods: List[Dict[str, Any]]) -> Dict[str, Any]:
    """The tiles: goods only, and only the costed ones for anything priced."""
    costed = [g for g in goods if g["costed"]]
    sold = [g for g in costed if g["units_90d"] > 0]
    slow = [g for g in goods if g["slow"]]
    retail = sum(g["available"] * g["price"] for g in costed)
    cost = sum(g["available"] * g["cost"] for g in costed)
    profit = retail - cost
    revenue = sum(g["revenue_90d"] for g in sold)
    at_list = sum(g["price"] * g["units_90d"] for g in sold)
    cogs = sum(g["cost"] * g["units_90d"] for g in sold)
    margins = [g["margin"] for g in costed]
    buckets = [0] * len(BUCKETS)
    for m in margins:
        buckets[bucket_of(m)] += 1
    problems = {f: sum(1 for g in goods if f in g["flags"]) for f in PROBLEM_FLAGS}
    return {
        "positions": len(goods),
        "costed": len(costed),
        "reserved": sum(1 for g in goods if g["available"] == 0),
        "retail_all": _money(sum(g["retail_value"] or 0 for g in goods)),
        "retail": _money(retail),
        "cost": _money(cost),
        "profit": _money(profit),
        "margin": _ratio(profit / retail) if retail else None,
        "markup": _ratio(profit / cost) if cost else None,
        "median_margin": _ratio(statistics.median(margins)) if margins else None,
        "sold": len(sold),
        "revenue_90d": _money(revenue),
        "list_90d": _money(at_list),
        "cogs_90d": _money(cogs),
        "discount_90d": _ratio(1 - revenue / at_list) if at_list else None,
        "list_margin_90d": _ratio((at_list - cogs) / at_list) if at_list else None,
        "realized_margin_90d": _ratio((revenue - cogs) / revenue) if revenue else None,
        "slow": len(slow),
        "slow_profit": _money(sum(g["profit"] for g in slow)),
        "problem_items": sum(1 for g in goods if any(f in g["flags"] for f in PROBLEM_FLAGS)),
        "problems": problems,
        "buckets": [{"from": lo, "to": hi, "count": n} for (lo, hi), n in zip(BUCKETS, buckets)],
    }


def excluded(items: Iterable[Dict[str, Any]], kind: str) -> Dict[str, Any]:
    rows = [i for i in items if i["kind"] == kind]
    return {
        "positions": len(rows),
        "available": sum(i["available"] for i in rows),
        "retail_value": _money(sum(i["retail_value"] or 0 for i in rows)),
    }


def build(rows: Iterable[Sequence[Any]]) -> Dict[str, Any]:
    """The whole answer of ``/api/stock-margin``."""
    raw = list(rows)
    items = [item(r) for r in raw]
    stamps = [r[ROW_COLUMNS.index("updated_at")] for r in raw]
    stamps = [s for s in stamps if isinstance(s, datetime)]
    goods = [i for i in items if i["kind"] == GOODS]
    return {
        "snapshot_at": max(stamps).isoformat() if stamps else None,
        "window_days": WINDOW_DAYS,
        "thresholds": {"low_margin": LOW_MARGIN, "high_margin": HIGH_MARGIN,
                       "slow_days": SLOW_DAYS},
        "goods": summary(goods),
        "excluded": {k: excluded(items, k) for k in (CERTIFICATE, PROMO)},
        "items": items,
    }
