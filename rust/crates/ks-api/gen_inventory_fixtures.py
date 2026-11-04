"""Regenerate the CPython answers the inventory arithmetic is held to.

Run from the repository root, in the Python production runs and with the
locked dependencies — it imports `core.repositories.inventory` and
serialises as the app does, `jsonable_encoder` then orjson:

    docker run --rm -v "$PWD:/app" -w /app -e TZ=UTC <python:3.14-slim + requirements-dev.lock> \\
        python rust/crates/ks-api/gen_inventory_fixtures.py

Writes `tests/fixtures/inventory_semantics.jsonl` beside this file,
deterministic (fixed seed): whole answers of `get_dead_stock_deep` — the NPV
decision, the quadrant matrix, the Pareto shares, cost quality, the GMROI
quartiles, the liquidation queue — of `get_inventory_turnover` and of
`get_brand_rotation`, each method itself with its reads answered by the
line's rows. A numeric is given to Rust as the float Python makes of it, or,
where Python cuts a `Decimal` with `int()` or hands it on, as its text.
`cargo test` checks the port against every line without Python.
"""
from __future__ import annotations

import asyncio
import json
import random
import struct
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))

import orjson  # noqa: E402
from fastapi.encoders import jsonable_encoder  # noqa: E402

from core.repositories.inventory import InventoryMixin  # noqa: E402

TIERS = ("hot", "healthy", "warm", "cold", "frozen")


def _bits(x: Optional[float]) -> Optional[str]:
    return None if x is None else struct.pack(">d", x).hex()


def _f(d: Optional[Decimal]) -> Optional[str]:
    """What `float()` makes of a numeric, as its bits; a NULL stays one."""
    return None if d is None else _bits(float(d))


def _text(d: Optional[Decimal]) -> Optional[str]:
    """A numeric as Postgres writes it: fixed point, never an exponent."""
    return None if d is None else format(d, "f")


def _answer(value: Any) -> str:
    return orjson.dumps(jsonable_encoder(value), option=orjson.OPT_NON_STR_KEYS).decode()


class _Stub(InventoryMixin):
    """The repository with each read answering the case's rows."""

    def __init__(self, rows: Any) -> None:
        self.rows = rows

    async def _inventory_rows(self, sql: str, params: Any = (), **extra: Any) -> List[tuple]:
        return self.rows

    async def _inventory_batch(self, queries: Any, **extra: Any) -> List[List[tuple]]:
        return self.rows


def _money(rng: random.Random, lo: int = 0, hi: int = 900_000) -> Decimal:
    return Decimal(rng.randint(lo * 100, hi * 100)) / 100


def _maybe(rng: random.Random, value: Any, *others: Any) -> Any:
    return rng.choice((value, value, value, *others))


def _quotient(a: int, b: int) -> Decimal:
    """A numeric division as a view leaves one: many digits."""
    return Decimal(a) / Decimal(b) if b else Decimal(0)


def _deep_rows(rng: random.Random) -> List[tuple]:
    rows = []
    for offer_id in sorted(rng.sample(range(1, 400), rng.randint(0, 14))):
        available = _maybe(rng, rng.randint(0, 300), None, 0)
        price = _maybe(rng, _money(rng, 1, 3_000), None, Decimal("0.00"))
        qty90 = _maybe(rng, rng.randint(0, 200), None, 0)
        adv90 = None if qty90 is None else _quotient(qty90, 90)
        dos = rng.choice((
            None, Decimal(365), Decimal(180), Decimal("365.0000000001"),
            _quotient(available or 0, qty90 or 1) * 90, Decimal(rng.randint(0, 900)),
            Decimal("179.99999999999999999999")))
        unit_cost = _maybe(rng, _quotient(rng.randint(100, 90_000), 37), None, Decimal(0))
        rows.append((
            offer_id, _maybe(rng, f"KS-{offer_id:04d}", None), _maybe(rng, f"Товар {offer_id}", None),
            _maybe(rng, "COSRX", None, "", "Етуде"), _maybe(rng, "Креми", None),
            available, price, rng.choice((None, Decimal(0), _money(rng, 1, 2_000))),
            unit_cost, rng.choice(("actual", "fallback", None)),
            _maybe(rng, _money(rng), None, Decimal(0)), _maybe(rng, _money(rng), None, Decimal(0)),
            _maybe(rng, rng.randint(0, 500), None), _maybe(rng, rng.randint(0, 900), None),
            None,
            _maybe(rng, rng.randint(0, 80), None), qty90,
            _maybe(rng, _money(rng, 0, 90_000), None), _maybe(rng, _money(rng, 0, 250_000), None),
            _maybe(rng, _quotient(rng.randint(0, 80), 30), None), adv90, dos,
            rng.choice(TIERS), _maybe(rng, _quotient(rng.randint(0, 900), 300), None),
            rng.choice(("A", "B", "C")),
            _maybe(rng, _money(rng, -50_000, 400_000), None, Decimal(0)),
            rng.choice((None, Decimal(0), Decimal("-0.5"), _quotient(rng.randint(1, 99), 100),
                        _quotient(rng.randint(100, 900), 100)))))
    return rows


DEEP_COLUMNS = ("offer_id", "sku", "name", "brand", "category_name", "available", "price",
                "purchased_price", "effective_unit_cost", "cost_quality", "sale_value",
                "cost_basis", "days_since_sale", "days_in_stock", None, "qty_sold_30d",
                "qty_sold_90d", "revenue_30d", "revenue_90d", "avg_daily_sales_30d",
                "avg_daily_sales_90d", "days_of_supply", "velocity_tier",
                "velocity_ratio_30_90", "abc_class", "annual_gross_profit", "gmroi")


def _deep_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(120):
        rows = _deep_rows(rng)
        carrying = rng.choice((0.25, 0.05, 0.5, round(rng.uniform(0.05, 0.5), 3)))
        discount = rng.choice((0.5, 0.2, 0.8, round(rng.uniform(0.2, 0.8), 3)))
        limit = rng.choice((None, 100, 5, 1))
        answer = asyncio.run(_Stub(rows).get_dead_stock_deep(
            limit=limit, carrying_rate=carrying, liquidation_discount=discount))
        encoded = [{n: (_f(v) if isinstance(v, Decimal) else v)
                    for n, v in zip(DEEP_COLUMNS, row) if n is not None} for row in rows]
        yield {"kind": "deep", "carrying_rate": _bits(carrying), "discount": _bits(discount),
               "limit": limit, "rows": encoded, "json": _answer(answer)}


def _turnover_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(150):
        rev = (_maybe(rng, _money(rng, 0, 9_000_000), Decimal(0)),
               rng.choice((0, 1, 7, 30, 61, 90)))
        stock = (_maybe(rng, _money(rng, 0, 20_000_000), Decimal(0)),
                 _maybe(rng, _money(rng, 0, 9_000_000), Decimal(0)),
                 rng.randint(0, 40_000), rng.randint(0, 900))
        abc = []
        for cls in rng.sample(("A", "B", "C"), rng.randint(0, 3)):
            abc.append((cls, rng.randint(0, 300), _maybe(rng, rng.randint(0, 9_000), None),
                        _maybe(rng, _money(rng), None), _maybe(rng, _money(rng), None),
                        _maybe(rng, _quotient(rng.randint(0, 10_000), 97), None, Decimal(0)),
                        _maybe(rng, _quotient(rng.randint(0, 10_000), 89), None, Decimal(0))))
        st = (rng.randint(0, 90), rng.randint(0, 90), rng.randint(0, 90),
              _maybe(rng, _quotient(rng.randint(1, 4_000), 77), None),
              _maybe(rng, rng.uniform(0, 900), None, 0.0, 90.5, 91.5))
        excess = []
        for offer_id in sorted(rng.sample(range(1, 400), rng.randint(0, 8))):
            excess.append((offer_id, f"KS-{offer_id:04d}", _maybe(rng, "Товар", None),
                           _maybe(rng, "COSRX", None), _maybe(rng, "Креми", None),
                           _maybe(rng, rng.randint(0, 300), None, 0),
                           _maybe(rng, _money(rng), None),
                           rng.choice((None, Decimal("90.99999999999999999999"),
                                       _quotient(rng.randint(1, 90_000), 7),
                                       Decimal(rng.randint(91, 2_000)))),
                           _maybe(rng, _quotient(rng.randint(0, 100), 3), None),
                           _maybe(rng, _quotient(rng.randint(0, 50), 90), None, Decimal(0)),
                           _maybe(rng, _money(rng), None)))
        params = {"days": rng.randint(7, 90), "lead_time": rng.randint(1, 90),
                  "safety_multiplier": rng.choice((1.5, 1.0, 3.0, 1.1, 2.25,
                                                   round(rng.uniform(1, 3), 2))),
                  "buffer_days": rng.randint(0, 30), "max_acceptable_days": rng.randint(30, 365)}
        answer = asyncio.run(_Stub([[rev], [stock], abc, [st], excess]).get_inventory_turnover(
            params["days"], lead_time_days=params["lead_time"],
            safety_multiplier=params["safety_multiplier"], buffer_days=params["buffer_days"],
            max_acceptable_days=params["max_acceptable_days"]))
        yield {
            "kind": "turnover",
            "params": {**params, "safety_multiplier": _bits(params["safety_multiplier"])},
            "revenue": _f(rev[0]), "revenue_days": rev[1],
            "stock": [_f(stock[0]), _f(stock[1]), stock[2], stock[3]],
            "abc": [[r[0], r[1], r[2], _f(r[3]), _f(r[4]), _f(r[5]), _f(r[6])] for r in abc],
            "st": [st[0], st[1], st[2], _f(st[3]), _bits(st[4])],
            "excess": [[r[0], r[1], r[2], r[3], r[4], r[5], _f(r[6]), _text(r[7]), _f(r[8]),
                        _f(r[9]), _f(r[10])] for r in excess],
            "json": _answer(answer),
        }


def _brand_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(120):
        rows = []
        for brand in rng.sample(("COSRX", "—", "Етуде Хаус", "MISSHA", "Some Brand", " x "),
                                rng.randint(0, 6)):
            skus = rng.randint(1, 20)
            rows.append((brand, skus, rng.randint(0, skus), _maybe(rng, rng.randint(0, 900), None),
                         _maybe(rng, _money(rng, 0, 500_000), None, Decimal(0)),
                         _maybe(rng, _money(rng, 0, 900_000), None, Decimal(0)),
                         _maybe(rng, _money(rng, 0, 300_000), None, Decimal(0)),
                         _maybe(rng, Decimal(rng.randint(0, 5_000)), None, Decimal(0)),
                         _maybe(rng, _money(rng, -10_000, 200_000), None)))
        answer = asyncio.run(_Stub(rows).get_brand_rotation(min_skus=1))
        yield {"kind": "brands",
               "rows": [[r[0], r[1], r[2], r[3], _f(r[4]), _f(r[5]), _f(r[6]), _text(r[7]),
                         _f(r[8])] for r in rows],
               "json": _answer(answer)}


def main() -> None:
    rng = random.Random(20261029)
    records: List[Dict[str, Any]] = [{"kind": "made_by", "python": sys.version.split()[0]}]
    records += [*_deep_cases(rng), *_turnover_cases(rng), *_brand_cases(rng)]
    out = HERE / "tests" / "fixtures" / "inventory_semantics.jsonl"
    out.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records),
                   encoding="utf-8")
    print(f"{out}: {len(records)} lines")


if __name__ == "__main__":
    main()
