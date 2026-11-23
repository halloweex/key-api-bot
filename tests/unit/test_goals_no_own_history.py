"""A sales type with no history of its own has no automatic goal.

The owner's decision of 2026-11-19. Until then `generate_smart_goals`, finding
no month a year earlier, no complete month with 25 days of sales and no
forecast, built the goal from the month's average in the shared seasonality
tables — retail's history, whatever sales type was asked. Production froze
November 2026 that way on 2026-11-18: ₴5.7M for the offline store, open since
the 12th (₴21 140 on three days of sales by the 18th), and the same for the
exhibition (six days in September 2025), beside retail's ₴4.7M — retail's
November average ₴4 399 005.46 × (1 + 0.2911), read from production that day.

Pinned here without a database, each named by the mutation it kills:

- every sales type but retail with no signal of its own answers no goal, in
  every period, and says why (`no_history`);
- retail with no signal still answers its own month average; retail with no
  history at all answers no goal, where a ₴3M default stood;
- one signal of its own gives any sales type its goal, as before;
- `get_smart_goals` says there is none, and a goal typed by hand still fills
  the period it was typed for;
- the freeze keeps a no-goal answer at once, and gives it back as it went in.

The same over a real Postgres is `tests/integration/test_goal_freeze_pg.py`.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from core import goal_freeze
from core.duckdb_constants import KNOWN_SALES_TYPES
from core.repositories import goals as goals_repo
from core.repositories.goals import GOAL_TABLES_SALES_TYPE, NO_HISTORY, GoalsMixin
from tests.helpers.goal_freeze import ledger  # noqa: F401 — fixture

KYIV = ZoneInfo("Europe/Kyiv")

# The shared tables' November as production read them on 2026-11-19: the
# month's average and YoY are retail's (`GOAL_TABLES_SALES_TYPE`).
NOVEMBER = [("seasonal", None, 1.3556, 4_399_005.46, 0.2911, "medium"),
            ("growth", None, 0.5019, None, None, None)] + [
            ("weekly", week, weight, None, None, None)
            for week, weight in ((1, 0.23), (2, 0.23), (3, 0.23), (4, 0.23), (5, 0.08))]

# Every sales type the goal routes accept but retail.
NOT_RETAIL = sorted(set(goal_freeze.SALES_TYPES) - {GOAL_TABLES_SALES_TYPE})


class _History(GoalsMixin):
    """The reads a smart goal makes, answered from memory: the three
    signals, the shared tables, the frozen month (none) and the goals typed
    by hand. Any other read fails the test."""

    def __init__(self, *, last_year=0.0, recent=0.0, ml=0.0, shared=None, typed=()):
        self.last_year = last_year
        self.recent = recent
        self.ml = ml
        self.shared = list(NOVEMBER if shared is None else shared)
        self.typed = list(typed)

    async def _get_ml_forecast_total(self, *a, **k):
        return self.ml

    async def _dynamic_growth_cap(self, *a, **k):
        return 0.35

    async def _last_year_month_revenue(self, *a, **k):
        return self.last_year

    async def _recent_three_month_average(self, *a, **k):
        return self.recent

    async def _goal_tables_run(self, sql, params=None):
        return list(self.shared)

    async def _goals_run(self, sql, params=None):
        if sql is goals_repo._FROZEN_GOAL_SQL:
            return []
        if sql is goals_repo._STORED_GOALS_SQL:
            return list(self.typed)
        raise AssertionError(f"an unexpected read: {' '.join(sql.split())[:100]}")


def _goal(store: _History, sales_type: str, month: int = 11) -> dict:
    return asyncio.run(store.generate_smart_goals(2026, month, sales_type))


def _assert_no_goal(answer: dict) -> None:
    assert answer["monthly"]["goal"] is None
    assert answer["monthly"]["calculationMethod"] == NO_HISTORY == "no_history"
    assert answer["monthly"]["blendWeights"] is None
    assert answer["weekly"]["goal"] is None
    assert answer["weekly"]["breakdown"] == {}
    assert answer["daily"]["goal"] is None


class TestNoHistoryOfItsOwn:
    def test_the_rule_covers_every_sales_type_but_retail(self):
        assert set(NOT_RETAIL) == (set(KNOWN_SALES_TYPES) | {"all"}) - {"retail"}
        assert {"offline", "exhibition"} <= set(NOT_RETAIL)

    @pytest.mark.parametrize("sales_type", NOT_RETAIL)
    def test_no_signal_is_no_goal_in_every_period(self, sales_type):
        """Kills: retail's month average standing in — production's ₴5.7M
        for the offline store and the exhibition in November 2026."""
        answer = _goal(_History(), sales_type)
        _assert_no_goal(answer)
        # What was read is still said; only the goal is absent.
        assert answer["monthly"]["historicalAvg"] == 4_399_005.46
        assert answer["daily"]["daysInMonth"] == 30
        assert answer["weekly"]["weights"]

    def test_retail_keeps_its_own_month_average(self):
        """The shared tables are retail's history, so with no signal retail
        still has a goal from them: ₴4 399 005.46 × 1.2911, to the ₴100 000.
        It is the number the offline store and the exhibition were given."""
        answer = _goal(_History(), "retail")
        assert answer["monthly"]["calculationMethod"] == "historical_avg"
        assert answer["monthly"]["goal"] == 5_700_000
        assert answer["daily"]["goal"] == 190_000
        assert answer["weekly"]["goal"] == 1_350_000
        assert sum(answer["weekly"]["breakdown"].values()) == 5_700_000

    def test_retail_with_no_history_at_all_has_none_either(self):
        """Kills: the ₴3M default ("fallback") for a month with no signal and
        no row in the shared tables — invented the same way."""
        _assert_no_goal(_goal(_History(shared=[]), "retail"))

    @pytest.mark.parametrize("sales_type,signal,method,goal", [
        # The offline store's first full month, at ₴80 000: × November's 1.3556.
        ("offline", {"recent": 80_000.0}, "recent_trend", 100_000),
        # The exhibition a year after a fair in the same month: × 1.2911.
        ("exhibition", {"last_year": 266_059.0}, "yoy_growth", 300_000),
        # Production's b2b, its November 2025 alone: 1 369 644 × 1.2911.
        ("b2b", {"last_year": 1_369_644.0}, "yoy_growth", 1_800_000),
        ("all", {"ml": 7_000_000.0}, "ml_forecast", 7_000_000),
    ])
    def test_one_signal_of_its_own_gives_the_goal(self, sales_type, signal, method, goal):
        """Kills: the rule reaching past "no signal" — a sales type with a
        history of its own keeps the goal it had."""
        answer = _goal(_History(**signal), sales_type)
        assert answer["monthly"]["calculationMethod"] == method
        assert answer["monthly"]["goal"] == goal


class TestTheAnswerServed:
    def test_get_smart_goals_says_there_is_none(self):
        served = asyncio.run(_History().get_smart_goals("offline", year=2026, month=11))
        for period in ("daily", "weekly", "monthly"):
            assert served[period]["amount"] is None, period
            assert served[period]["suggestedAmount"] is None, period
            assert served[period]["isCustom"] is False, period
        assert served["monthly"]["calculationMethod"] == "no_history"
        assert served["weekly"]["weeklyBreakdown"] == {}
        assert served["monthly"]["frozenAt"] is None

    def test_a_goal_typed_by_hand_still_fills_its_period(self):
        """`app.revenue_goals` is one row per period for every sales type; a
        typed monthly goal overrides the absent one as it overrides any."""
        typed = [("monthly", 300_000.0, True, None, None)]
        served = asyncio.run(_History(typed=typed).get_smart_goals("offline", year=2026, month=11))
        assert served["monthly"]["amount"] == 300_000.0
        assert served["monthly"]["isCustom"] is True
        assert served["monthly"]["suggestedAmount"] is None
        assert served["daily"]["amount"] is None


class TestTheFreeze:
    def test_no_goal_comes_back_as_it_went_in(self):
        answer = _goal(_History(), "offline")
        text = goal_freeze.encode(answer)
        assert json.loads(text)["monthly"]["goal"] is None
        assert goal_freeze.decode(text) == answer

    def test_it_is_not_held_back_for_a_forecast(self):
        """No ML signal, so nothing to wait for: `_short_forecast` answers
        before it would ask the store (whose read would fail this test)."""
        answer = _goal(_History(), "exhibition")
        at = datetime(2026, 12, 1, 0, 30, tzinfo=KYIV)
        assert asyncio.run(goal_freeze._short_forecast(_History(), "exhibition", answer, at)) is None

    @pytest.mark.asyncio
    async def test_the_month_is_frozen_with_no_goal_where_there_is_no_history(self, ledger):  # noqa: F811
        result = await goal_freeze.freeze_month(_History(), now=datetime(2026, 12, 1, 0, 30, tzinfo=KYIV))
        assert result == {"month": "2026-12", "frozen": list(goal_freeze.SALES_TYPES), "already": []}
        ((_year, _month, answers, _at),) = ledger["kept"]
        kept = {sales_type: json.loads(text) for sales_type, text in answers}
        assert kept["retail"]["monthly"]["goal"] == 5_700_000
        for sales_type in NOT_RETAIL:
            assert kept[sales_type]["monthly"]["goal"] is None, sales_type
            assert kept[sales_type]["monthly"]["calculationMethod"] == "no_history"
