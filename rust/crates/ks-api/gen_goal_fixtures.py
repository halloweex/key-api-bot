"""Regenerate the CPython answers the goal arithmetic is held to.

Run from the repository root, in the Python production runs and with the
locked dependencies — it imports `core.repositories.goals` and serialises
with orjson, as the app does:

    docker run --rm -v "$PWD:/app" -w /app <python:3.14-slim + requirements-dev.lock> \\
        python rust/crates/ks-api/gen_goal_fixtures.py

Writes `tests/fixtures/goal_semantics.jsonl` beside this file, deterministic
(fixed seed): `round()` to an int, `_recency_weighted`, `_growth_cap`, and
whole smart goals — `GoalsMixin.generate_smart_goals` itself, its five reads
answered with the numbers on the line, as the bytes orjson writes them.
`cargo test` checks the port against every line without Python.
"""
from __future__ import annotations

import asyncio
import json
import random
import struct
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))

import orjson  # noqa: E402

from core.repositories import goals as g  # noqa: E402

# The instant every smart goal is computed at; the Rust test passes the same.
NOW = datetime(2026, 10, 21, 9, 0, tzinfo=timezone.utc)


class _Clock(datetime):
    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
        return NOW.astimezone(tz) if tz is not None else NOW


def _bits(x: float) -> str:
    return struct.pack(">d", x).hex()


def _num(v: Any) -> Dict[str, Any]:
    return {"i": v} if isinstance(v, int) else {"f": _bits(v)}


class _Stub(g.GoalsMixin):
    """`generate_smart_goals` with each read answering the case's number."""

    def __init__(self, case: Dict[str, Any]) -> None:
        self.case = case

    async def _get_ml_forecast_total(self, target_year: int, target_month: int,
                                     sales_type: str = "retail") -> float:
        return self.case["ml"]

    async def _dynamic_growth_cap(self, target_month: int, sales_type: str = "retail") -> float:
        return self.case["cap"]

    async def _last_year_month_revenue(self, target_year: int, target_month: int,
                                       sales_type: str) -> float:
        return self.case["last_year"]

    async def _recent_three_month_average(self, sales_type: str) -> float:
        return self.case["recent"]

    async def _goal_tables_run(self, sql: str, params: Any = None) -> List[Tuple[Any, ...]]:
        return self.case["rows"]


def _dec(rng: random.Random, lo: float, hi: float, places: int) -> Decimal:
    return Decimal(str(round(rng.uniform(lo, hi), places)))


def _money(rng: random.Random) -> float:
    """What `float(SUM(numeric))` hands the blend: a sum of kopecks."""
    return float(Decimal(rng.randint(10_000_000, 900_000_000)) / 100)


def _maybe(rng: random.Random, value: Any, *others: Any) -> Any:
    return rng.choice((value, value, value, *others))


def _round_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(300):
        k = rng.randint(-2000, 2000)
        x = rng.choice((k + 0.5, k - 0.5, rng.uniform(-1e4, 1e4),
                        rng.uniform(0, 5e8) / 1e5, float(k)))
        yield {"kind": "round_int", "x": _bits(x), "i": round(x)}


def _recency_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(200):
        rates = [rng.uniform(-0.6, 1.4) for _ in range(rng.randint(1, 7))]
        yield {"kind": "recency", "rates": [_bits(r) for r in rates],
               "out": _bits(g._recency_weighted(rates))}


def _cap_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(200):
        avg = _maybe(rng, rng.uniform(-0.5, 0.8), None)
        std = _maybe(rng, rng.uniform(0, 0.4), None, 0.0)
        count = rng.choice((0, 1, 1, 2, 3, 5))
        yield {"kind": "growth_cap",
               "avg": None if avg is None else _bits(avg),
               "std": None if std is None else _bits(std),
               "count": count, "out": _bits(g._growth_cap((avg, std, count)))}


def _smart_cases(rng: random.Random) -> Iterator[Dict[str, Any]]:
    for _ in range(400):
        year, month = rng.randint(2024, 2027), rng.randint(1, 12)
        case: Dict[str, Any] = {
            "ml": rng.choice((0.0, 0.0, _money(rng))),
            "cap": rng.choice((0.35, 0.1, 0.5, rng.uniform(0.1, 0.5))),
            "last_year": rng.choice((0, _money(rng), _money(rng))),
            "recent": rng.choice((0, _money(rng), _money(rng))),
        }
        rows: List[Tuple[Any, ...]] = []
        seasonal: Optional[List[Any]] = None
        if rng.random() < 0.7:
            seasonal = [
                _maybe(rng, _dec(rng, 0.5, 1.6, 4), None, Decimal("0.0000")),
                _maybe(rng, _dec(rng, 1e5, 6e6, 2), None, Decimal("0.00")),
                _maybe(rng, _dec(rng, -0.2, 0.6, 4), None, Decimal("0.0000")),
                rng.choice(("high", "medium", "low", "", None, "very high")),
            ]
            rows.append(("seasonal", None, *seasonal))
        growth: Dict[str, Any] = {"row": rng.random() < 0.7}
        if growth["row"]:
            value = _maybe(rng, _dec(rng, -0.1, 0.5, 4), None)
            growth["value"] = None if value is None else str(value)
            rows.append(("growth", None, value, None, None, None))
        weeks = sorted(rng.sample(range(1, 6), rng.choice((0, 0, 3, 4, 5, 5))))
        weekly = [[week, str(_dec(rng, 0.05, 0.4, 4))] for week in weeks]
        # The engine returns them in no order; the blend sorts by week.
        for week, weight in rng.sample(weekly, len(weekly)):
            rows.append(("weekly", week, Decimal(weight), None, None, None))
        rng.shuffle(rows)
        case["rows"] = rows
        answer = asyncio.run(_Stub(case).generate_smart_goals(year, month, "retail"))
        yield {
            "kind": "smart", "year": year, "month": month,
            "ml": _bits(case["ml"]), "cap": _bits(case["cap"]),
            "last_year": _num(case["last_year"]), "recent": _num(case["recent"]),
            "seasonal": None if seasonal is None else [
                None if v is None else str(v) for v in seasonal[:3]] + [seasonal[3]],
            "growth": growth, "weekly": weekly,
            "json": orjson.dumps(answer, option=orjson.OPT_NON_STR_KEYS).decode(),
        }


def main() -> None:
    g.datetime = _Clock  # type: ignore[misc]
    rng = random.Random(20261029)
    records: List[Dict[str, Any]] = [{"kind": "made_by", "python": sys.version.split()[0]}]
    records += [*_round_cases(rng), *_recency_cases(rng), *_cap_cases(rng),
                *_smart_cases(rng)]
    out = HERE / "tests" / "fixtures" / "goal_semantics.jsonl"
    out.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records),
                   encoding="utf-8")
    print(f"{out}: {len(records)} lines")


if __name__ == "__main__":
    main()
