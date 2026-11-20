"""The month's automatic goal, frozen on its 1st (revision 0037), without a
database: what is frozen, when, by whom, and what the two GETs read.

The owner's decision of 2026-11-18. The smart goal moved inside its month as
its ML term drifted toward the actual (a median 4-6 %, ₴0.5M in July 2026),
and a past month's goal moved after the fact. `core.goal_freeze` keeps each
month's answer once; `GoalsMixin.smart_goals_of_month` reads it. Pinned here,
each named by the mutation it kills:

- the sales types frozen are exactly the ones the goal routes accept;
- an answer comes back from JSON as it went in, week numbers included;
- a frozen month is answered frozen and computes nothing; a month with no
  row is computed live, as before; a typed goal still overrides; both GETs
  read through the one door, and a row frozen by this revision's code (a
  literal, never to be edited) goes on being read;
- the freeze: once per month and sales type, every answer computed before
  any row, never before 00:30 on the 1st, the month Kyiv's;
- the job: registered daily at 00:30 and after a start, clear of the busy
  windows, a refusal answered with nothing written;
- the table has one writer, reached from the job alone.

The server's half — the revision applied, the first freeze winning on a
real table, the frozen answer surviving a change of every live input — is
`tests/integration/test_goal_freeze_pg.py`.
"""
from __future__ import annotations

import asyncio
import copy
import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core import goal_freeze, read_fallback
from core.repositories import goals as goals_repo
from core.repositories.goals import GoalsMixin
from tests.helpers.goal_freeze import BUSY, ledger  # noqa: F401

KYIV = ZoneInfo("Europe/Kyiv")
TABLE = "app.frozen_month_goals"

# A December 2026 retail answer as revision 0037's `generate_smart_goals`
# builds it, encoded as `core.goal_freeze.encode` stores it. NEVER EDIT THIS:
# it stands for every row frozen in production by this revision's code, and
# a reader changed later must go on answering from it — a key added to the
# answer afterwards is absent here, as it is from those rows.
FROZEN_BY_0037 = (
    '{"targetYear": 2026, "targetMonth": 12, "monthly": {"goal": 4200000, '
    '"lastYearRevenue": 3612400.25, "recent3MonthAvg": 3905118.75, '
    '"historicalAvg": 3500000.0, "yoyGoal": 4274914.46, "recentGoal": 4233929.75, '
    '"mlForecastGoal": 4180250.5, "growthRate": 0.1834, "growthCap": 0.35, '
    '"seasonalityIndex": 1.0842, "confidence": "high", "calculationMethod": '
    '"yoy_growth", "blendWeights": {"yoy": 0.4, "recent": 0.3, "ml": 0.3}}, '
    '"weekly": {"goal": 950000, "breakdown": {"1": 970000, "2": 940000, '
    '"3": 920000, "4": 1010000, "5": 360000}, "weights": {"1": 0.2301, '
    '"2": 0.2245, "3": 0.2198, "4": 0.241, "5": 0.0846}}, "daily": {"goal": '
    '140000, "daysInMonth": 31}, "metadata": {"overallYoY": 0.2112, '
    '"monthlyYoY": 0.1834, "calculatedAt": "2026-12-01T00:30:02.114857+02:00"}}'
)
FROZEN_AT = datetime(2026, 11, 30, 22, 30, 2, tzinfo=timezone.utc)  # 00:30:02 Kyiv, 1 Dec


class _Answers(GoalsMixin):
    """`generate_smart_goals` over fixed history reads — the live answer in
    today's exact shape, with nothing but the arithmetic run."""

    def __init__(self, ml=4_180_250.5):
        self.ml = ml

    async def _get_ml_forecast_total(self, *a, **k):
        return self.ml

    async def _dynamic_growth_cap(self, *a, **k):
        return 0.35

    async def _last_year_month_revenue(self, *a, **k):
        return 3_612_400.25

    async def _recent_three_month_average(self, *a, **k):
        return 3_905_118.75

    async def _goal_tables_run(self, sql, params=None):
        return [("seasonal", None, 1.0842, 3_500_000.0, 0.1834, "high"),
                ("growth", None, 0.2112, None, None, None)] + [
                ("weekly", w, weight, None, None, None)
                for w, weight in ((1, 0.2301), (2, 0.2245), (3, 0.2198),
                                  (4, 0.2410), (5, 0.0846))]


def _live(ml=4_180_250.5) -> dict:
    return asyncio.run(_Answers(ml).generate_smart_goals(2026, 12, "retail"))


class _Reader(GoalsMixin):
    """`/goals`' two reads answered from memory — the frozen row or none, and
    the stored goals — and the live computation recorded."""

    def __init__(self, frozen=None, stored=(), live=None):
        self.frozen = frozen
        self.stored = list(stored)
        self.live = live
        self.asked = []
        self.computed = []

    async def _goals_run(self, sql, params=None):
        self.asked.append((sql, list(params or [])))
        if sql is goals_repo._FROZEN_GOAL_SQL:
            return [self.frozen] if self.frozen else []
        if sql is goals_repo._STORED_GOALS_SQL:
            return list(self.stored)
        raise AssertionError(f"an unexpected read: {' '.join(sql.split())[:100]}")

    async def generate_smart_goals(self, target_year, target_month, sales_type="retail"):
        self.computed.append((target_year, target_month, sales_type))
        if self.live is None:
            raise AssertionError("a frozen month was computed live")
        return copy.deepcopy(self.live)


# ─── What is frozen ─────────────────────────────────────────────────────────

class TestTheSalesTypes:
    def test_they_are_every_sales_type_the_goal_routes_accept(self):
        """Kills: a list typed by hand that misses one (`offline` arrived on
        2026-11-15) — that type's goal would stay live beside frozen ones."""
        from core.duckdb_constants import KNOWN_SALES_TYPES
        from core.validators import ValidationError, validate_sales_type

        assert len(set(goal_freeze.SALES_TYPES)) == len(goal_freeze.SALES_TYPES)
        assert set(goal_freeze.SALES_TYPES) == set(KNOWN_SALES_TYPES) | {"all"}
        for sales_type in goal_freeze.SALES_TYPES:
            assert validate_sales_type(sales_type) == sales_type
        with pytest.raises(ValidationError):
            validate_sales_type("wholesale")


class TestTheRoundTrip:
    def test_a_live_answer_comes_back_as_it_went_in(self):
        """Through JSONB's text: the same numbers, and the week maps keyed by
        integers again, as the live answer has them. Kills: `decode` without
        the week numbers — `weeklyBreakdown` would change its key type the
        day a month is frozen."""
        smart = _live()
        back = goal_freeze.decode(goal_freeze.encode(smart))
        assert back == smart
        assert all(type(k) is int for k in back["weekly"]["breakdown"])
        assert all(type(k) is int for k in back["weekly"]["weights"])

    def test_a_nan_is_refused_before_anything_is_stored(self):
        """Postgres refuses NaN in JSON; `encode` refuses it first, naming
        nothing stored. Kills: `allow_nan` left at its default."""
        smart = _live()
        smart["monthly"]["growthRate"] = float("nan")
        with pytest.raises(ValueError):
            goal_freeze.encode(smart)

    def test_each_decode_is_a_fresh_answer(self):
        """A reader adds `frozenAt` to what it decoded. Kills: a cached dict
        shared between two requests."""
        first = goal_freeze.decode(FROZEN_BY_0037)
        first["monthly"]["frozenAt"] = "x"
        assert "frozenAt" not in goal_freeze.decode(FROZEN_BY_0037)["monthly"]


# ─── What the GETs read ─────────────────────────────────────────────────────

class TestTheReader:
    def test_a_frozen_month_answers_the_frozen_goal_and_computes_nothing(self):
        """Kills: the frozen row read and then ignored, or the live answer
        computed beside it (a GET that costs the eight reads it used to)."""
        reader = _Reader(frozen=(FROZEN_BY_0037, FROZEN_AT))
        smart = asyncio.run(reader.get_smart_goals("retail", year=2026, month=12))

        assert reader.computed == []
        assert smart["monthly"]["amount"] == 4_200_000
        assert smart["monthly"]["suggestedAmount"] == 4_200_000
        assert smart["monthly"]["mlForecastGoal"] == 4_180_250.5
        assert smart["monthly"]["isCustom"] is False
        assert smart["daily"]["amount"] == 140_000
        assert smart["weekly"]["amount"] == 950_000
        assert smart["weekly"]["weeklyBreakdown"] == {
            1: 970_000, 2: 940_000, 3: 920_000, 4: 1_010_000, 5: 360_000}
        assert smart["monthly"]["frozenAt"] == "2026-12-01T00:30:02+02:00"
        assert smart["metadata"]["calculatedAt"] == "2026-12-01T00:30:02.114857+02:00"

    def test_it_asks_for_the_month_and_the_sales_type(self):
        """Kills: a read keyed on the month alone — one sales type's goal
        answered for every other."""
        reader = _Reader(frozen=(FROZEN_BY_0037, FROZEN_AT))
        asyncio.run(reader.get_smart_goals("b2b", year=2026, month=12))
        assert (goals_repo._FROZEN_GOAL_SQL, [2026, 12, "b2b"]) in reader.asked

    def test_a_month_with_no_row_is_computed_live_as_before(self):
        """Every month before revision 0037, and a month whose freeze has not
        run yet. Kills: a missing row read as no goal, or frozen by the GET."""
        live = goal_freeze.decode(FROZEN_BY_0037)
        live["monthly"]["goal"] = 3_900_000
        reader = _Reader(live=live)
        smart = asyncio.run(reader.get_smart_goals("all", year=2026, month=10))

        assert reader.computed == [(2026, 10, "all")]
        assert smart["monthly"]["amount"] == 3_900_000
        assert smart["monthly"]["frozenAt"] is None

    def test_a_typed_goal_still_overrides_a_frozen_one(self):
        """A goal a human typed on /goals wins over the automatic one, frozen
        or not. Kills: the custom override applied only to a live answer."""
        stored = [("daily", 150_000, False, 140_000, 1.1),
                  ("monthly", 7_777_700, True, 4_100_000, 1.1)]
        reader = _Reader(frozen=(FROZEN_BY_0037, FROZEN_AT), stored=stored)
        smart = asyncio.run(reader.get_smart_goals("retail", year=2026, month=12))

        assert smart["monthly"]["amount"] == 7_777_700
        assert smart["monthly"]["isCustom"] is True
        assert smart["monthly"]["suggestedAmount"] == 4_200_000
        # A stored goal that is not custom does not override.
        assert smart["daily"]["amount"] == 140_000
        assert smart["daily"]["isCustom"] is False

    def test_the_forecast_door_answers_the_frozen_month_too(self):
        """`/api/goals/forecast` and `/api/goals/smart` read one door, so one
        month cannot have two goals. Kills: the forecast left on
        `generate_smart_goals` for a frozen month."""
        frozen = _Reader(frozen=(FROZEN_BY_0037, FROZEN_AT))
        answer = asyncio.run(frozen.get_month_goal(2026, 12, "retail"))
        assert frozen.computed == []
        expected = goal_freeze.decode(FROZEN_BY_0037)
        expected["monthly"]["frozenAt"] = "2026-12-01T00:30:02+02:00"
        assert answer == expected

        live = _Reader(live=_live())
        answer = asyncio.run(live.get_month_goal(2027, 2, "retail"))
        assert live.computed == [(2027, 2, "retail")]
        assert answer["monthly"]["frozenAt"] is None
        assert answer["monthly"]["goal"] == _live()["monthly"]["goal"]

    def test_a_row_frozen_by_revision_0037s_code_is_still_read(self):
        """The literal above, through both GETs, every field of the answer
        they build. Kills: a reader changed to need a key that rows frozen
        before the change do not carry — a 500 on every such month."""
        reader = _Reader(frozen=(FROZEN_BY_0037, FROZEN_AT))
        smart = asyncio.run(reader.get_smart_goals("retail", year=2026, month=12))
        monthly = smart["monthly"]
        assert (monthly["lastYearRevenue"], monthly["recent3MonthAvg"],
                monthly["growthRate"], monthly["seasonalityIndex"],
                monthly["confidence"], monthly["calculationMethod"],
                monthly["blendWeights"]) == (
            3_612_400.25, 3_905_118.75, 0.1834, 1.0842, "high", "yoy_growth",
            {"yoy": 0.4, "recent": 0.3, "ml": 0.3})
        assert monthly["basedOnAverage"] == 3_500_000.0
        assert smart["daily"]["basedOnAverage"] == pytest.approx(3_500_000.0 / 31)
        assert smart["metadata"]["overallYoY"] == 0.2112
        forecast = asyncio.run(reader.get_month_goal(2026, 12, "retail"))
        assert forecast["weekly"]["breakdown"][4] == 1_010_000

    def test_the_forecast_route_goes_through_the_door(self, admin_client, monkeypatch):  # noqa: F811
        """Kills: the route calling `generate_smart_goals` again."""
        store = AsyncMock()
        store.get_month_goal = AsyncMock(return_value={"monthly": {"frozenAt": None}})
        store.generate_smart_goals = AsyncMock(side_effect=AssertionError("live, past the door"))
        monkeypatch.setattr("web.routes.api.goals.get_store", AsyncMock(return_value=store))

        response = admin_client.get("/api/goals/forecast",
                                    params={"year": 2026, "month": 12, "sales_type": "b2b"})

        assert response.status_code == 200, response.text
        assert store.get_month_goal.await_args.args == (2026, 12, "b2b")
        store.generate_smart_goals.assert_not_awaited()


# ─── The freeze ─────────────────────────────────────────────────────────────

class _Store:
    """The repository as the freeze sees it: answers counted, one refusal
    where asked, and a stored forecast that reaches the month's end unless
    `short` says how many days it holds for a sales type."""

    def __init__(self, refuse_at=None, short=None, ml=4_180_250.5):
        self.asked = []
        self.refuse_at = refuse_at
        self.short = dict(short or {})
        self.ml = ml
        self.days_asked = []

    async def generate_smart_goals(self, year, month, sales_type):
        self.asked.append((year, month, sales_type))
        if sales_type == self.refuse_at:
            raise read_fallback.refuse("goals", OSError("postgres is gone"))
        answer = await _Answers(ml=self.ml).generate_smart_goals(year, month, sales_type)
        answer["monthly"]["goal"] = 1_000_000 + 100_000 * len(self.asked)
        return answer

    async def forecast_days_stored(self, sales_type, start, end):
        self.days_asked.append((sales_type, start, end))
        return self.short.get(sales_type, (end - start).days + 1)


def _at(*args) -> datetime:
    return datetime(*args, tzinfo=KYIV)


class TestTheFreeze:
    @pytest.mark.asyncio
    async def test_it_freezes_every_sales_type_once_from_0030_on_the_1st(self, ledger):
        store = _Store()
        at = _at(2026, 12, 1, 0, 30)
        result = await goal_freeze.freeze_month(store, now=at)

        assert store.asked == [(2026, 12, s) for s in goal_freeze.SALES_TYPES]
        ((year, month, answers, stamped),) = ledger["kept"]
        assert (year, month, stamped) == (2026, 12, at)
        assert [s for s, _ in answers] == list(goal_freeze.SALES_TYPES)
        # Kept as `encode` writes it: the answer each sales type computed.
        assert [json.loads(text)["monthly"]["goal"] for _, text in answers] == [
            1_000_000 + 100_000 * n for n in range(1, 7)]
        assert result == {"month": "2026-12", "frozen": list(goal_freeze.SALES_TYPES),
                          "already": []}

    @pytest.mark.asyncio
    async def test_a_month_frozen_already_computes_nothing(self, ledger):
        """Days 2 to 31, every run: one read, and nothing computed. Kills: a
        run that recomputes, or one that writes over a frozen goal."""
        ledger["held"] = set(goal_freeze.SALES_TYPES)
        store = _Store()
        result = await goal_freeze.freeze_month(store, now=_at(2026, 12, 9, 0, 30))

        assert store.asked == [] and ledger["kept"] == []
        assert result == {"month": "2026-12", "frozen": [],
                          "already": sorted(goal_freeze.SALES_TYPES)}

    @pytest.mark.asyncio
    async def test_only_what_the_month_lacks_is_computed(self, ledger):
        ledger["held"] = {"retail", "all"}
        store = _Store()
        result = await goal_freeze.freeze_month(store, now=_at(2026, 12, 2, 9, 0))

        assert [s for _, _, s in store.asked] == ["b2b", "internal", "exhibition", "offline"]
        assert result["frozen"] == ["b2b", "internal", "exhibition", "offline"]
        assert result["already"] == ["all", "retail"]

    @pytest.mark.asyncio
    async def test_every_answer_is_computed_before_any_row_is_written(self, ledger):
        """A refusal at the fourth sales type writes nothing: the month stays
        whole and live, and the next run asks again. Kills: a row written per
        answer as it comes — half a month frozen from two moments' inputs."""
        store = _Store(refuse_at="exhibition")
        with pytest.raises(read_fallback.ReadUnavailable):
            await goal_freeze.freeze_month(store, now=_at(2026, 12, 1, 6, 0))
        assert len(store.asked) == 4
        assert ledger["kept"] == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hour,minute", [(0, 0), (0, 5), (0, 29)])
    async def test_not_before_0030_on_the_1st(self, ledger, hour, minute):
        """The month that has just ended may not all be in Silver yet. Kills:
        a start at 00:05 on a 1st freezing the month without its last day."""
        store = _Store()
        result = await goal_freeze.freeze_month(store, now=_at(2026, 12, 1, hour, minute))
        assert "skipped" in result and result["month"] == "2026-12"
        assert store.asked == [] and ledger["kept"] == []

    @pytest.mark.asyncio
    async def test_any_other_hour_of_any_other_day_freezes(self, ledger):
        """The deploy day's start freezes the month in progress (November
        2026). Kills: a guard that waits for a 1st."""
        result = await goal_freeze.freeze_month(_Store(), now=_at(2026, 11, 18, 0, 5))
        assert result["frozen"] == list(goal_freeze.SALES_TYPES)

    @pytest.mark.asyncio
    async def test_the_month_is_kyivs(self, ledger):
        """22:30 UTC on 30 November is 00:30 on 1 December in Kyiv. Kills: the
        month taken from UTC — November frozen again, or December a day late."""
        result = await goal_freeze.freeze_month(
            _Store(), now=datetime(2026, 11, 30, 22, 30, tzinfo=timezone.utc))
        assert result["month"] == "2026-12"
        assert ledger["kept"][0][:2] == (2026, 12)
        early = await goal_freeze.freeze_month(
            _Store(), now=datetime(2026, 11, 30, 22, 10, tzinfo=timezone.utc))
        assert "skipped" in early and early["month"] == "2026-12"

    @pytest.mark.asyncio
    async def test_an_answer_on_part_of_a_forecast_waits(self, ledger):
        """No training for 30 days and more: on the 1st the stored forecast
        holds 24 of December's 31 days, and the ML signal summed them alone.
        Kept, that goal would stand too low all month; it is read live
        instead, named under `waiting`, and the others are frozen. Kills:
        the freeze keeping whatever `generate_smart_goals` returned."""
        store = _Store(short={"retail": 24})
        at = _at(2026, 12, 1, 0, 30)
        result = await goal_freeze.freeze_month(store, now=at)

        rest = [s for s in goal_freeze.SALES_TYPES if s != "retail"]
        assert result["frozen"] == rest
        assert result["waiting"] == {
            "retail": "the stored forecast holds 24 of the 31 days to the month's end"}
        assert result["already"] == []
        ((_year, _month, answers, _stamp),) = ledger["kept"]
        assert [s for s, _ in answers] == rest
        # Counted from the freeze's Kyiv day to the month's last.
        assert ("retail", date(2026, 12, 1), date(2026, 12, 31)) in store.days_asked

    @pytest.mark.asyncio
    async def test_it_freezes_once_a_training_reaches_the_months_end(self, ledger):
        """The run after the training: only what the month lacks is computed,
        and frozen from that run's inputs."""
        ledger["held"] = {s for s in goal_freeze.SALES_TYPES if s != "retail"}
        store = _Store()
        result = await goal_freeze.freeze_month(store, now=_at(2026, 12, 3, 6, 30))

        assert store.asked == [(2026, 12, "retail")]
        assert store.days_asked == [("retail", date(2026, 12, 3), date(2026, 12, 31))]
        assert result["frozen"] == ["retail"] and "waiting" not in result

    @pytest.mark.asyncio
    async def test_an_answer_without_the_ml_signal_is_kept_and_asks_nothing(self, ledger):
        """No row at all in the window: the signal is absent, not short, and
        the blend gave its weight to the other two. Kills: a month that waits
        for good while no forecast exists."""
        store = _Store(ml=0.0, short={"retail": 0})
        result = await goal_freeze.freeze_month(store, now=_at(2026, 12, 1, 0, 30))

        assert result["frozen"] == list(goal_freeze.SALES_TYPES)
        assert "waiting" not in result and store.days_asked == []

    @pytest.mark.asyncio
    async def test_without_postgres_it_stands_down(self, ledger, monkeypatch):
        """A dev host with no DSN: no read, no refusal counted, no page."""
        monkeypatch.delenv("KS_PG_DSN")
        store = _Store()
        assert await goal_freeze.freeze_month(store) == {"skipped": "KS_PG_DSN is not set"}
        assert store.asked == []


# ─── The job ────────────────────────────────────────────────────────────────

def _registered():
    from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    return s


def _in_kyiv() -> CronTrigger:
    """The registered trigger's fields in Kyiv's zone. Like every cron job
    here it carries the process's zone — Europe/Kyiv in production, UTC in CI
    (`tests/unit/test_scheduler_cadence.py`) — so its firings are read on a
    copy made in Kyiv, as `tests/unit/test_capture_ga4.py` reads its own."""
    from core.scheduler import SCHEDULER_TIMEZONE

    job = _registered()._scheduler.get_job("month_goal_freeze")
    fields = {f.name: str(f) for f in job.trigger.fields}
    return CronTrigger(hour=fields["hour"], minute=fields["minute"],
                       day=fields["day"], day_of_week=fields["day_of_week"],
                       timezone=SCHEDULER_TIMEZONE)


class TestTheJob:
    def test_it_is_registered_at_half_past_its_hours_and_a_few_minutes_after_a_start(self):
        before = datetime.now(timezone.utc)
        job = _registered()._scheduler.get_job("month_goal_freeze")
        assert job is not None
        assert isinstance(job.trigger, CronTrigger)
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert (fields["hour"], fields["minute"], fields["day"], fields["day_of_week"]) == (
            ",".join(str(h) for h in goal_freeze.RUN_HOURS), "30", "*", "*")
        assert goal_freeze.RUN_HOURS[0] == goal_freeze.FREEZE_AT.hour
        assert int(fields["minute"]) == goal_freeze.FREEZE_AT.minute
        delay = timedelta(seconds=goal_freeze.FIRST_RUN_DELAY_S)
        assert before + delay - timedelta(seconds=5) < job.next_run_time \
            < before + delay + timedelta(seconds=30)
        assert job.func.__wrapped__.__name__ == "_run_month_goal_freeze"

    def test_after_the_first_run_it_follows_the_cron(self):
        """The first run is the start's, off the grid; the next is the next
        half past one of its hours, and after a start between midnight and
        00:30 on a 1st, which `freeze_month` turns away, it is 00:30. Kills: a
        first run that replaced the hourly ones, or a trigger that fires once."""
        first = datetime(2026, 11, 18, 14, 7, tzinfo=KYIV)
        trigger = _in_kyiv()
        assert trigger.get_next_fire_time(first, first) == datetime(
            2026, 11, 18, 14, 30, tzinfo=KYIV)
        late = datetime(2026, 11, 18, 23, 31, tzinfo=KYIV)
        assert trigger.get_next_fire_time(late, late) == datetime(
            2026, 11, 19, 0, 30, tzinfo=KYIV)
        early = datetime(2026, 12, 1, 0, 9, tzinfo=KYIV)
        assert goal_freeze.too_early(early)
        assert trigger.get_next_fire_time(early, early) == datetime(
            2026, 12, 1, 0, 30, tzinfo=KYIV)
        assert not goal_freeze.too_early(datetime(2026, 12, 1, 0, 30, tzinfo=KYIV))

    def test_a_year_of_firings_misses_every_busy_window(self):
        """Read in Kyiv across both clock changes. Kills: a time moved into a
        window the schedule keeps clear, or into the DST hours."""
        from core.scheduler import SCHEDULER_TIMEZONE

        trigger = _in_kyiv()
        at = datetime(2026, 3, 1, tzinfo=SCHEDULER_TIMEZONE)
        fired = []
        while at < datetime(2027, 3, 1, tzinfo=SCHEDULER_TIMEZONE):
            at = trigger.get_next_fire_time(None, at + timedelta(minutes=1))
            fired.append(at.astimezone(SCHEDULER_TIMEZONE))
        starts = {t.hour * 60 + t.minute for t in fired}
        assert starts == {h * 60 + 30 for h in goal_freeze.RUN_HOURS}
        assert not [w for w in BUSY for m in starts if w[0] <= m <= w[1]]
        by_day: dict = {}
        for t in fired:
            by_day.setdefault(t.date(), []).append(t)
        # Every day of the year, both clock changes' included, runs every
        # hour of the list, 00:30 first.
        assert len(by_day) >= 365
        assert {len(v) for d, v in by_day.items() if d < at.date()} == {len(goal_freeze.RUN_HOURS)}
        assert {(v[0].hour, v[0].minute) for v in by_day.values()} == {(0, 30)}

    @pytest.mark.asyncio
    async def test_a_refusal_defers_the_month_with_nothing_written(
        self, ledger, fresh_read_fallback, monkeypatch,  # noqa: F811
    ):
        """DN-20c: no request waits for this job, so it answers the refusal
        itself — skipped, the reason and the surface in its result — and
        writes nothing. Kills: the refusal let out as the job's exception,
        or an answer that wrote the sales types computed before it."""
        from core.scheduler import BackgroundScheduler

        store = _Store(refuse_at="b2b")
        monkeypatch.setattr("core.store.get_store", AsyncMock(return_value=store))
        # Whatever day the suite runs on, 00:10 on a 1st included.
        monkeypatch.setattr(goal_freeze, "too_early", lambda at: False)

        result = await BackgroundScheduler()._run_month_goal_freeze()

        assert result == {"skipped": True, "reason": read_fallback.READ_UNAVAILABLE,
                          "surface": "goals"}
        assert ledger["kept"] == []

    @pytest.mark.asyncio
    async def test_a_clean_run_is_the_freezes_answer(self, ledger, monkeypatch):
        from core.scheduler import BackgroundScheduler

        monkeypatch.setattr("core.store.get_store", AsyncMock(return_value=_Store()))
        monkeypatch.setattr(goal_freeze, "too_early", lambda at: False)
        result = await BackgroundScheduler()._run_month_goal_freeze()
        assert result["frozen"] == list(goal_freeze.SALES_TYPES)


# ─── One writer ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def walk():
    from tests.helpers.write_walk import _CAPTURE_WALKED, _Walk

    return _Walk(_CAPTURE_WALKED)


class TestOneWriter:
    """Read off the repository walk `tests/unit/test_write_chains.py` keeps,
    over every tree a container runs."""

    def test_the_table_has_one_writer(self, walk):
        """Kills: a second writer of the frozen goals anywhere — a route that
        froze a month on a GET above all."""
        writers = {key for key, targets in walk.writers().items() if TABLE in targets}
        assert writers == {("core/goal_freeze.py", "_keep")}

    def test_the_writer_is_reached_from_the_job_alone(self, walk):
        """`_keep` from `freeze_month`, and `freeze_month` from the job: no
        request handler between them. Kills: a route or a GET reaching it."""
        assert walk.callers.get(("core/goal_freeze.py", "_keep")) == {
            ("core/goal_freeze.py", "freeze_month")}
        assert walk.callers.get(("core/goal_freeze.py", "freeze_month")) == {
            ("core/scheduler.py", "_run_month_goal_freeze")}
