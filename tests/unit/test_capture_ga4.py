"""The GA4 funnel capture (`core/ga4_capture.py`): which periods, which rows, the job.

No test reaches Google. GA4 and its token endpoint are an
``httpx.MockTransport`` (`FakeGA4`), the seam `tests/unit/test_ga4_funnel.py`
uses, and the key is generated here. Postgres is faked here and real in
`tests/integration/test_capture_ga4_pg.py`, which imports `FakeGA4` and
`ga4_key`.

Each test is named by what it pins. The mutations they kill are in their
docstrings where the name does not say it.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import httpx
import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from core import analytics_captures as cap
from core import ga4
from core import ga4_capture as gc
from tests import migration_replay as replay
from web.services import ga4_funnel_service as svc

TODAY = date(2026, 11, 13)

# ─── The fake GA4 ─────────────────────────────────────────────────────────────

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
KEY_INFO = {
    "type": "service_account",
    "client_email": "ga4-reader@example.iam.gserviceaccount.com",
    "private_key": _KEY.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode(),
    "token_uri": "https://oauth2.example/token",
}

CHANNELS = {"Paid Social": 0.5, "Organic Social": 0.3, "Direct": 0.15, "Unassigned": 0.05}
DEVICES = {"mobile": 0.8, "desktop": 0.2}
_STEP_SHARE = (1.0, 0.6, 0.1, 0.05, 0.03)


def people(scope: str, start: date, end: date, share: float = 1.0) -> List[int]:
    """What the fake answers: a function of the request alone, so a test can
    say what a stored row must hold."""
    entered = (1000 + start.toordinal() % 97 + 10 * (end - start).days) * (8 if scope == "ua" else 10)
    return [int(entered * share * s) for s in _STEP_SHARE]


def _table(rows: Dict[str, List[int]], breakdown: str) -> dict:
    return {"funnelTable": {
        "dimensionHeaders": [{"name": "funnelStepName"}, {"name": breakdown}],
        "metricHeaders": [{"name": "activeUsers"}, {"name": "funnelStepCompletionRate"}],
        "rows": [
            {"dimensionValues": [{"value": f"{i + 1}. {ga4.COLUMNS[i]}"}, {"value": key}],
             "metricValues": [{"value": str(n)}, {"value": "0.5"}]}
            for key, counts in rows.items() for i, n in enumerate(counts)
        ],
    }}


class FakeGA4:
    """The token endpoint and GA4's runFunnelReport. Records each funnel asked
    as (start, end, breakdown dimension, scope); `fail_at` answers 429 on that
    funnel request (1-based); `empty` periods are answered with no rows."""

    def __init__(self, *, fail_at: Optional[int] = None,
                 empty: Set[Tuple[date, date]] = frozenset()) -> None:
        self.fail_at = fail_at
        self.empty = set(empty)
        self.asked: List[Tuple[date, date, str, str]] = []

    def periods(self) -> List[Tuple[date, date]]:
        """The periods asked, in order, each once."""
        return list(dict.fromkeys((s, e) for s, e, _, _ in self.asked))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "oauth2.example":
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        body = json.loads(request.content)
        span = body["dateRanges"][0]
        start, end = date.fromisoformat(span["startDate"]), date.fromisoformat(span["endDate"])
        breakdown = body["funnelBreakdown"]["breakdownDimension"]["name"]
        scope = "ua" if "dimensionFilter" in body else "all"
        self.asked.append((start, end, breakdown, scope))
        if self.fail_at is not None and len(self.asked) == self.fail_at:
            return httpx.Response(429, json={"error": {"message": "Exhausted property tokens per hour"}})
        if (start, end) in self.empty:
            return httpx.Response(200, json=_table({}, breakdown))
        shares = CHANNELS if breakdown == svc.CHANNEL else DEVICES
        rows = {"RESERVED_TOTAL": people(scope, start, end)}
        rows.update({key: people(scope, start, end, share) for key, share in shares.items()})
        return httpx.Response(200, json=_table(rows, breakdown))

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


@pytest.fixture
def ga4_key(monkeypatch, tmp_path):
    """A configured GA4: a generated key file and a property id."""
    key = tmp_path / "ga4-reader.json"
    key.write_text(json.dumps(KEY_INFO))
    monkeypatch.setenv("KS_GA4_KEY_FILE", str(key))
    monkeypatch.setenv("KS_GA4_PROPERTY_ID", "123")
    ga4._reset_token()
    yield key
    ga4._reset_token()


# ─── Which periods ────────────────────────────────────────────────────────────


def _of(grain: str, today: date = TODAY) -> List[gc.Period]:
    return [p for p in gc.due_periods(today) if p.grain == grain]


class TestWhichPeriods:

    def test_the_horizon_and_the_last_closed_day(self):
        """Today 2026-11-13: GA4 holds funnels from 2025-09-13 (14 months back,
        by the day), the margin starts at the 14th, and D-3 is the 10th."""
        assert svc.earliest_day(TODAY) == date(2025, 9, 13)
        assert gc.first_capturable_day(TODAY) == date(2025, 9, 14)
        assert gc.last_closed_day(TODAY) == date(2026, 11, 10)
        days = _of("day")
        assert days[0] == gc.Period("day", date(2025, 9, 14), date(2025, 9, 14))
        assert days[-1] == gc.Period("day", date(2026, 11, 10), date(2026, 11, 10))
        assert len(days) == (date(2026, 11, 10) - date(2025, 9, 14)).days + 1

    def test_the_edge_day_is_never_a_period(self):
        """Kills: a margin of zero, which would store the day GA4 may be
        deleting while it is asked."""
        edge = svc.earliest_day(TODAY)
        assert all(p.start > edge for p in gc.due_periods(TODAY))

    def test_a_day_closes_three_days_later(self):
        for today in (date(2026, 11, 13), date(2027, 1, 1), date(2026, 3, 1)):
            last = max(p.end for p in gc.due_periods(today))
            assert last == today - timedelta(days=3)

    def test_weeks_are_whole_iso_weeks_inside_the_horizon(self):
        weeks = _of("week")
        assert weeks[0] == gc.Period("week", date(2025, 9, 15), date(2025, 9, 21))
        assert weeks[-1] == gc.Period("week", date(2026, 11, 2), date(2026, 11, 8))
        for w in weeks:
            assert w.start.isoweekday() == 1 and (w.end - w.start).days == 6
        assert [b.start - a.start for a, b in zip(weeks, weeks[1:])] == [timedelta(days=7)] * (len(weeks) - 1)

    def test_a_week_closes_on_its_sunday_plus_three(self):
        sunday = date(2026, 11, 15)
        week = gc.Period("week", date(2026, 11, 9), sunday)
        assert week in gc.due_periods(sunday + timedelta(days=3))
        assert week not in gc.due_periods(sunday + timedelta(days=2))

    def test_the_week_across_new_year_is_one_iso_week(self):
        """ISO week 1 of 2026 runs 2025-12-29 to 2026-01-04."""
        weeks = [w for w in _of("week") if w.start <= date(2026, 1, 4) <= w.end]
        assert weeks == [gc.Period("week", date(2025, 12, 29), date(2026, 1, 4))]
        assert date(2025, 12, 29).isocalendar()[1] == 1

    def test_months_are_whole_calendar_months_inside_the_horizon(self):
        """September 2025 began before the horizon and is never asked; October
        2026 is closed; November 2026 is not."""
        months = _of("month")
        assert months[0] == gc.Period("month", date(2025, 10, 1), date(2025, 10, 31))
        assert months[-1] == gc.Period("month", date(2026, 10, 1), date(2026, 10, 31))
        assert len(months) == 13
        for m in months:
            assert m.start.day == 1 and (m.end + timedelta(days=1)).day == 1
            assert m.start.month == m.end.month

    def test_a_month_closes_on_its_last_day_plus_three(self):
        november = gc.Period("month", date(2026, 11, 1), date(2026, 11, 30))
        assert november in gc.due_periods(date(2026, 12, 3))
        assert november not in gc.due_periods(date(2026, 12, 2))

    def test_february_of_a_leap_year_ends_on_the_29th(self):
        february = gc.Period("month", date(2028, 2, 1), date(2028, 2, 29))
        assert february in gc.due_periods(date(2028, 3, 3))
        assert february not in gc.due_periods(date(2028, 3, 2))

    def test_a_month_is_asked_until_its_first_day_reaches_the_margin(self):
        """October 2025 is due on 2026-11-30, the last day its first day is
        past the margin, and never after: GA4 would answer it short."""
        october = gc.Period("month", date(2025, 10, 1), date(2025, 10, 31))
        assert october in gc.due_periods(date(2026, 11, 30))
        assert october not in gc.due_periods(date(2026, 12, 1))
        assert _of("month", date(2026, 12, 1))[0].start == date(2025, 11, 1)

    def test_oldest_first_and_a_day_before_its_week_before_its_month(self):
        """2026-06-01 is a Monday, so a day, a week and a month start on it."""
        due = gc.due_periods(TODAY)
        assert [p.start for p in due] == sorted(p.start for p in due)
        on_june_first = [p.grain for p in due if p.start == date(2026, 6, 1)]
        assert on_june_first == ["day", "week", "month"]

    @pytest.mark.parametrize("utc, kyiv", [
        # Spring forward: 2026-03-29 03:00 EET becomes 04:00 EEST.
        (datetime(2026, 3, 28, 21, 59, tzinfo=timezone.utc), date(2026, 3, 28)),
        (datetime(2026, 3, 28, 22, 0, tzinfo=timezone.utc), date(2026, 3, 29)),
        (datetime(2026, 3, 29, 20, 59, tzinfo=timezone.utc), date(2026, 3, 29)),
        (datetime(2026, 3, 29, 21, 0, tzinfo=timezone.utc), date(2026, 3, 30)),
        # Fall back: 2026-10-25 04:00 EEST becomes 03:00 EET.
        (datetime(2026, 10, 24, 20, 59, tzinfo=timezone.utc), date(2026, 10, 24)),
        (datetime(2026, 10, 24, 21, 0, tzinfo=timezone.utc), date(2026, 10, 25)),
        (datetime(2026, 10, 25, 21, 59, tzinfo=timezone.utc), date(2026, 10, 25)),
        (datetime(2026, 10, 25, 22, 0, tzinfo=timezone.utc), date(2026, 10, 26)),
    ])
    def test_today_is_kyivs_across_both_clock_changes(self, utc, kyiv):
        """Kyiv's midnight is 22:00 UTC in winter and 21:00 in summer. Kills: a
        UTC "today", which would close a day two or three hours early."""
        assert gc.kyiv_today(utc) == kyiv

    def test_the_weeks_holding_a_clock_change_are_seven_dates(self):
        weeks = {w.start: w for w in gc.due_periods(date(2027, 4, 15)) if w.grain == "week"}
        assert weeks[date(2026, 10, 19)].end == date(2026, 10, 25)
        assert weeks[date(2027, 3, 22)].end == date(2027, 3, 28)

    def test_the_values_are_revision_0036s(self):
        """The grains, scopes and breakdowns the job writes are exactly the
        CHECK lists of `app.ga4_funnel`. Read off the revision, not copied."""
        rev = replay.load(replay.VERSIONS / "0036_analytics_captures.py")
        ddl = next(s for s in rev.STATEMENTS if "app.ga4_funnel" in s)
        checks = {name: set(re.findall(r"'(\w+)'", values))
                  for name, values in re.findall(r"(\w+)\s+TEXT\s+NOT NULL CHECK \(\1 IN \(([^)]*)\)\)", ddl)}
        assert checks == {
            "grain": set(gc.GRAINS),
            "country_scope": set(svc.SCOPES),
            "breakdown": {"total"} | {name for name, _ in gc.BREAKDOWNS},
        }
        steps = re.search(r"CHECK \(step BETWEEN (\d+) AND (\d+)\)", ddl)
        assert steps is not None
        assert (int(steps[1]), int(steps[2])) == (1, len(ga4.COLUMNS))


# ─── What is stored ───────────────────────────────────────────────────────────


class TestTheRows:
    CHANNEL_ANSWER = {
        ga4.TOTAL: dict(zip(ga4.COLUMNS, [100, 60, 10, 5, 3])),
        "Paid Social": dict(zip(ga4.COLUMNS, [50, 30, 5, 2, 1])),
        "Organic Social": dict(zip(ga4.COLUMNS, [40, 25, 4, 2, 2])),
        "Unassigned": dict(zip(ga4.COLUMNS, [10, 5, 1, 1, 0])),
    }
    DEVICE_ANSWER = {
        ga4.TOTAL: dict(zip(ga4.COLUMNS, [100, 60, 10, 5, 3])),
        "mobile": dict(zip(ga4.COLUMNS, [80, 50, 8, 4, 2])),
    }

    def _rows(self):
        return gc.rows_for("ua", self.CHANNEL_ANSWER, self.DEVICE_ANSWER)

    def test_five_steps_for_each_breakdown_value(self):
        by_key: Dict[Tuple[str, str], List[int]] = {}
        for scope, breakdown, value, _kind, _rule, step, users in self._rows():
            assert scope == "ua"
            by_key.setdefault((breakdown, value), []).append(step)
        assert by_key == {
            ("total", ""): [1, 2, 3, 4, 5],
            ("channel_group", "Organic Social"): [1, 2, 3, 4, 5],
            ("channel_group", "Paid Social"): [1, 2, 3, 4, 5],
            ("channel_group", "Unassigned"): [1, 2, 3, 4, 5],
            ("device", "mobile"): [1, 2, 3, 4, 5],
        }

    def test_the_total_comes_from_the_channel_funnel_and_once(self):
        """Kills: the device funnel's total stored too, which would collide
        with the channel funnel's on the primary key."""
        totals = [r for r in self._rows() if r[1] == "total"]
        assert [r[6] for r in totals] == [100, 60, 10, 5, 3]
        assert not [r for r in self._rows() if r[1] == "device" and r[2] == ga4.TOTAL]

    def test_a_step_is_the_number_ga4_gave_for_that_step(self):
        rows = {(r[1], r[2], r[5]): r[6] for r in self._rows()}
        assert rows[("channel_group", "Paid Social", 1)] == 50
        assert rows[("channel_group", "Paid Social", 5)] == 1
        assert rows[("device", "mobile", 3)] == 8

    def test_a_channel_group_carries_its_kind_and_the_rule_as_decided_now(self):
        for _scope, breakdown, value, kind, rule, _step, _users in self._rows():
            if breakdown == "channel_group":
                assert (kind, rule) == (svc.channel_type(value), svc.TRAFFIC_RULE)
            else:
                assert (kind, rule) == ("", "")
        kinds = {r[2]: r[3] for r in self._rows() if r[1] == "channel_group"}
        assert kinds == {"Paid Social": "cold", "Organic Social": "warm", "Unassigned": "other"}

    def test_no_total_from_ga4_is_five_zeros(self):
        """An empty answer is what the page reads as zeros; the five rows mark
        the period kept."""
        rows = gc.rows_for("all", {}, {})
        assert [(r[1], r[5], r[6]) for r in rows] == [("total", s, 0) for s in range(1, 6)]


class TestTheRuleVersion:
    # Every version of the rule, as (sorted COLD, sorted WARM). Append-only:
    # one entry per version, never edited. Moving a channel between cold and
    # warm adds an entry under a new, later TRAFFIC_RULE in the same commit;
    # rows already kept carry the old version, and that is how they say which
    # rule placed them. Editing an existing entry instead would let two rules
    # write one `traffic_rule` into `app.ga4_funnel` — the thing the column
    # exists to prevent, and a test that compared one literal could not see.
    RULES = {
        "2026-11-04": ((
            "Cross-network", "Display", "Paid Other", "Paid Shopping", "Paid Social",
            "Paid Video",
        ), (
            "Direct", "Email", "Mobile Push Notifications", "Organic Search",
            "Organic Shopping", "Organic Social", "Paid Search", "SMS",
        )),
    }

    def test_the_version_names_the_rule(self):
        """Kills: a channel moved between cold and warm with the version left
        as it was — every row kept since would claim the old rule."""
        assert self.RULES.get(ga4.TRAFFIC_RULE) == (
            tuple(sorted(ga4.COLD)), tuple(sorted(ga4.WARM))), (
            "COLD or WARM moved: name a new ga4.TRAFFIC_RULE and add it to "
            "RULES in the same commit, leaving the old entries as they are")

    def test_the_current_version_is_the_newest(self):
        """Kills: a rule change that went back to an old name, so that rows
        of two rules read as one. Versions are dates, so they order."""
        import datetime as dt

        for version in self.RULES:
            dt.date.fromisoformat(version)
        assert ga4.TRAFFIC_RULE == max(self.RULES)

    def test_no_two_versions_share_a_rule(self):
        """Kills: a new name for a rule that did not change, which splits one
        rule's rows across two `traffic_rule`s."""
        assert len(set(self.RULES.values())) == len(self.RULES)

    def test_the_route_and_the_capture_read_one_rule(self):
        """The rule lives in `core.ga4`, and /traffic's service re-exports it.
        Kills: a second copy in the service, which would let the page and the
        stored rows place a channel differently."""
        for name in ("COLD", "WARM", "TRAFFIC_RULE", "SCOPES", "CHANNEL", "DEVICE",
                     "channel_type", "earliest_day"):
            assert getattr(svc, name) is getattr(ga4, name), name

    def test_no_core_module_imports_web(self):
        """`core/` ships in the bot's image, which has no `web/`; the capture
        is in `core`, so the rule it shares with the route is in `core.ga4`.
        Kills: a `core` module reaching into `web` again."""
        import ast
        from pathlib import Path

        core = Path(gc.__file__).resolve().parent
        found = []
        for path in sorted(core.rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                names = ([a.name for a in node.names] if isinstance(node, ast.Import)
                         else [node.module or ""] if isinstance(node, ast.ImportFrom)
                         else [])
                found += [f"{path.name}: {n}" for n in names
                          if n == "web" or n.startswith("web.")]
        assert found == []


# ─── The throttle, as constants ───────────────────────────────────────────────


class TestTheThrottle:

    def test_a_period_costs_four_requests(self):
        assert len(svc.SCOPES) * len(gc.BREAKDOWNS) == 4

    def test_a_run_and_an_hour_stay_at_96_requests(self):
        """The arithmetic in the module's docstring: 24 periods x 4 requests,
        under 50 tokens each, is a third of the project's 14 000 an hour.
        Kills: a budget raised without the arithmetic redone."""
        assert gc.PERIODS_PER_RUN == gc.PERIODS_PER_HOUR == 24
        assert gc.PERIODS_PER_HOUR * 4 * 50 <= 14_000 * 0.35

    def test_the_backfill_outruns_the_horizon(self):
        """More periods a day than the edge takes, so oldest first never loses
        one: a day, a seventh of a week and a thirtieth of a month."""
        assert gc.PERIODS_PER_RUN > 1 + 1 / 7 + 1 / 28


# ─── The slack the watch judges the backlog by ────────────────────────────────


def _day(d: date) -> gc.Period:
    return gc.Period("day", d, d)


class TestTheSlack:
    """How many nights the capture could still fail and keep every period —
    what `analytics_captures._judge_ga4` turns into INFO or WARN."""

    def test_a_period_leaves_the_window_fourteen_months_on(self):
        """2025-10-05 can be kept by the run of 2026-12-04 and by none after.
        Kills: a leave day one off either way, which would move every slack."""
        oldest = _day(date(2025, 10, 5))
        assert gc.leaves_window_on(oldest) == date(2026, 12, 5)
        assert oldest in gc.due_periods(date(2026, 12, 4))
        assert oldest not in gc.due_periods(date(2026, 12, 5))

    @pytest.mark.parametrize("start", [
        date(2025, 12, 31), date(2025, 12, 29), date(2026, 1, 30), date(2026, 2, 28),
        date(2027, 2, 28), date(2025, 10, 31), date(2026, 3, 1)])
    def test_the_leave_day_is_where_the_horizon_passes_it(self, start):
        """Across the short months `ga4.earliest_day` clamps to: the run of
        the day before keeps the period, the run of the day does not."""
        leaves = gc.leaves_window_on(_day(start))
        assert gc.first_capturable_day(leaves - timedelta(days=1)) <= start
        assert gc.first_capturable_day(leaves) > start

    @pytest.mark.parametrize("utc, night", [
        (datetime(2026, 11, 13, 23, 0, tzinfo=timezone.utc), date(2026, 11, 14)),  # 01:00
        (datetime(2026, 11, 14, 0, 39, tzinfo=timezone.utc), date(2026, 11, 14)),  # 02:39
        (datetime(2026, 11, 14, 0, 40, tzinfo=timezone.utc), date(2026, 11, 15)),  # 02:40
        (datetime(2026, 11, 14, 5, 0, tzinfo=timezone.utc), date(2026, 11, 15)),   # 07:00
        (datetime(2026, 7, 13, 22, 0, tzinfo=timezone.utc), date(2026, 7, 14)),    # 01:00 EEST
        (datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc), date(2026, 7, 15)),     # 07:00 EEST
    ])
    def test_the_first_night_is_the_next_run_in_kyiv(self, utc, night):
        """The 01:00 integrity run comes before that night's 02:40 run, the
        07:00 one after it. Kills: counting from tomorrow always, which would
        make the 01:00 run a night more pessimistic than the 19:00 before it,
        and a backlog on the margin flap."""
        assert gc.first_night(utc) == night

    def test_the_run_time_is_the_jobs(self):
        assert (gc.RUN_AT.hour, gc.RUN_AT.minute) == (2, 40)

    def test_the_14_november_backlog_has_19_nights_to_spare(self):
        """As production had it: 471 left after the first night, the oldest
        2025-10-05, which the next run keeps and which 19 more runs could
        miss. Its leave day moves ~20 days a night, so the slack grows."""
        today = date(2026, 11, 14)
        backlog = gc.Backlog(tuple(gc.overdue_periods(today)[gc.PERIODS_PER_RUN:]),
                             gc.first_night(datetime(2026, 11, 14, 5, tzinfo=timezone.utc)))
        assert (backlog.missing, backlog.oldest) == (471, _day(date(2025, 10, 5)))
        slack = backlog.slack()
        assert slack == gc.Slack(spare=19, period=_day(date(2025, 10, 5)),
                                 leaves_on=date(2026, 12, 5), nights_left=20,
                                 nights_needed=1)
        tomorrow = date(2026, 11, 15)
        kept_one_more_night = gc.Backlog(
            tuple(gc.overdue_periods(tomorrow)[2 * gc.PERIODS_PER_RUN:]), tomorrow)
        later = kept_one_more_night.slack()
        assert later is not None and later.spare > slack.spare + 15

    def test_the_tightest_period_need_not_be_the_oldest(self):
        """Position, not age alone, sets a period's turn: the 25th waits for
        the second run. Kills: a slack read off the oldest period only."""
        start = date(2025, 10, 5)
        first = date(2026, 11, 15)
        slack = gc.nights_to_spare([_day(start)] * (gc.PERIODS_PER_RUN + 1), first)
        assert slack is not None
        assert (slack.spare, slack.nights_left, slack.nights_needed) == (18, 20, 2)

    def test_the_spare_is_the_failures_it_survives(self):
        """Fail `spare` runs, wherever they fall, and every period is still
        kept; fail one more at the start, and the tightest is lost. Simulated
        run by run against `due_periods`. Kills: a slack that promises nights
        the arithmetic of the run does not give."""
        today = date(2026, 11, 14)
        missing = list(gc.overdue_periods(today)[gc.PERIODS_PER_RUN:])
        first = date(2026, 11, 15)
        slack = gc.nights_to_spare(missing, first)
        assert slack is not None

        def lost(failed_runs):
            left, night, run = list(missing), first, 0
            while left and night < first + timedelta(days=60):
                due = set(gc.due_periods(night))
                gone = [p for p in left if p not in due and p.start < night]
                if gone:
                    return True
                run += 1
                if run not in failed_runs:
                    left = left[gc.PERIODS_PER_RUN:]
                night += timedelta(days=1)
            return False

        assert not lost(set(range(1, slack.spare + 1)))
        assert not lost(set(range(2, 2 * slack.spare + 2, 2)))
        assert lost(set(range(1, slack.spare + 2)))

    def test_nothing_missing_has_no_slack(self):
        assert gc.nights_to_spare([], date(2026, 11, 15)) is None
        assert gc.Backlog((), date(2026, 11, 15)).slack() is None
        assert gc.Backlog((), date(2026, 11, 15)).oldest is None


# ─── The job, with Postgres faked ─────────────────────────────────────────────


class _FakeStore:
    """`_read_kept` and `run_own_transaction`, as the job calls them."""

    def __init__(self, kept: Set[Tuple[str, date]] = frozenset(), spent: int = 0) -> None:
        self.kept = set(kept)
        self.spent = spent
        self.stamps: List[str] = []       # "ok" or the failure's text
        self.inserted: List[Tuple[str, date]] = []

    async def read_kept(self, first: date):
        return self.kept, self.spent

    async def run_own_transaction(self, table: str, capture, **_kw) -> bool:
        assert table == gc.TABLE

        class _Conn:
            async def execute(_self, sql, grain, start, end, *columns):
                assert "INSERT INTO app.ga4_funnel" in sql
                self.inserted.append((grain, start))
                return f"INSERT 0 {len(columns[0])}"

        try:
            await capture(_Conn())
        except Exception as exc:  # noqa: BLE001 — the helper's contract
            self.stamps.append(f"{type(exc).__name__}: {exc}")
            return False
        self.stamps.append("ok")
        return True


@pytest.fixture
def store(monkeypatch, ga4_key):
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@nowhere/ks")
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    fake = _FakeStore()
    monkeypatch.setattr(gc, "_read_kept", fake.read_kept)
    monkeypatch.setattr(gc, "run_own_transaction", fake.run_own_transaction)
    return fake


async def _run(google: FakeGA4, today: date = TODAY) -> dict:
    async with google.client() as client:
        return await gc.run_capture(client=client, today=today)


class TestTheJob:

    @pytest.mark.asyncio
    async def test_a_run_keeps_the_oldest_24_periods_with_four_funnels_each(self, store):
        google = FakeGA4()
        result = await _run(google)
        expected = gc.due_periods(TODAY)[:24]
        assert store.inserted == [(p.grain, p.start) for p in expected]
        assert google.periods() == [(p.start, p.end) for p in expected]
        assert result["captured"] == 24 and result["requests"] == 96 == len(google.asked)
        assert {(b, s) for _, _, b, s in google.asked} == {
            (svc.CHANNEL, "ua"), (svc.CHANNEL, "all"), (svc.DEVICE, "ua"), (svc.DEVICE, "all")}
        assert store.stamps == ["ok"] * 24
        assert result["oldest_missing"] == gc.due_periods(TODAY)[24].start.isoformat()

    @pytest.mark.asyncio
    async def test_a_kept_period_is_not_asked_again(self, store):
        due = gc.due_periods(TODAY)
        store.kept = {(p.grain, p.start) for p in due[:30]}
        google = FakeGA4()
        await _run(google)
        assert google.periods()[0] == (due[30].start, due[30].end)
        assert not set(google.periods()) & {(p.start, p.end) for p in due[:30]}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("spent, asked", [(0, 24), (20, 4), (24, 0), (30, 0)])
    async def test_the_hours_budget_is_what_the_last_hour_left(self, store, spent, asked):
        store.spent = spent
        google = FakeGA4()
        result = await _run(google)
        assert result["budget"] == asked == result["captured"]
        assert len(google.asked) == 4 * asked

    @pytest.mark.asyncio
    async def test_a_run_with_nothing_to_ask_still_stamps(self, store):
        """The stall watch reads the stamp's age: a run that had nothing to do
        is still a run."""
        store.spent = 24
        google = FakeGA4()
        result = await _run(google)
        assert store.stamps == ["ok"] and not store.inserted and not google.asked
        assert result["captured"] == 0

    @pytest.mark.asyncio
    async def test_a_ga4_error_stops_the_run_and_is_stamped(self, store):
        """The sixth request fails: the first period is kept, the second is
        not, nothing more is asked, and the failure goes to the stamp."""
        google = FakeGA4(fail_at=6)
        result = await _run(google)
        assert result["captured"] == 1 and result["requests"] == 6 == len(google.asked)
        assert store.stamps[0] == "ok"
        assert store.stamps[1].startswith("GA4Unavailable: GA4 answered 429")
        assert len(store.stamps) == 2
        assert "error" in result
        assert result["oldest_missing"] == gc.due_periods(TODAY)[1].start.isoformat()

    @pytest.mark.asyncio
    async def test_an_unreadable_store_asks_ga4_nothing(self, store, monkeypatch):
        async def down(first):
            raise OSError("connection refused")

        monkeypatch.setattr(gc, "_read_kept", down)
        google = FakeGA4()
        result = await _run(google)
        assert "error" in result and not google.asked and not store.stamps


class TestStandingDown:
    """Nothing asked of Google, nothing read or written, nothing stamped."""

    @pytest.fixture
    def untouchable(self, monkeypatch):
        async def must_not(*_a, **_kw):
            raise AssertionError("touched Postgres")

        monkeypatch.setattr(gc, "_read_kept", must_not)
        monkeypatch.setattr(gc, "run_own_transaction", must_not)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@nowhere/ks")
        monkeypatch.delenv(cap.SWITCH_ENV, raising=False)

    @pytest.mark.asyncio
    async def test_without_a_key(self, untouchable, monkeypatch):
        monkeypatch.delenv("KS_GA4_KEY_FILE", raising=False)
        monkeypatch.setenv("KS_GA4_PROPERTY_ID", "123")
        google = FakeGA4()
        assert await _run(google) == {"skipped": "GA4 is not configured"}
        assert not google.asked

    @pytest.mark.asyncio
    async def test_with_a_key_file_that_is_not_there(self, untouchable, monkeypatch, tmp_path):
        monkeypatch.setenv("KS_GA4_KEY_FILE", str(tmp_path / "absent.json"))
        monkeypatch.setenv("KS_GA4_PROPERTY_ID", "123")
        google = FakeGA4()
        assert await _run(google) == {"skipped": "GA4 is not configured"}
        assert not google.asked

    @pytest.mark.asyncio
    async def test_with_the_captures_switched_off(self, untouchable, ga4_key, monkeypatch):
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        google = FakeGA4()
        assert "skipped" in await _run(google)
        assert not google.asked

    @pytest.mark.asyncio
    async def test_without_postgres(self, untouchable, ga4_key, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        google = FakeGA4()
        assert "skipped" in await _run(google)
        assert not google.asked


# ─── The schedule ─────────────────────────────────────────────────────────────

#: Minutes of the Kyiv day the job must not start in: 05:00-05:50, 07:20-07:45,
#: 08:55-09:55, ten minutes either side of the 01/07/13/19 checks, and the
#: 03:00-04:59 hours a clock change can skip or repeat.
BUSY = [(300, 350), (440, 465), (535, 595), (180, 299)] + [
    (h * 60 - 10, h * 60 + 10) for h in (1, 7, 13, 19)]


def _registered():
    from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

    s = BackgroundScheduler()
    s._scheduler = AsyncIOScheduler(timezone=SCHEDULER_TIMEZONE)
    asyncio.run(s._register_jobs())
    return s


def _in_kyiv() -> CronTrigger:
    """The registered trigger's fields in Kyiv's zone. Like every cron job
    here it carries the process's zone, which is Europe/Kyiv in production and
    UTC in CI (`tests/unit/test_scheduler_cadence.py`), so the clock changes
    are read on a copy made in Kyiv."""
    from core.scheduler import SCHEDULER_TIMEZONE

    job = _registered()._scheduler.get_job("ga4_capture")
    fields = {f.name: str(f) for f in job.trigger.fields}
    return CronTrigger(hour=fields["hour"], minute=fields["minute"],
                       day_of_week=fields["day_of_week"], timezone=SCHEDULER_TIMEZONE)


class TestTheSchedule:

    def test_it_is_registered_daily_at_0240(self):
        job = _registered()._scheduler.get_job("ga4_capture")
        assert job is not None
        assert isinstance(job.trigger, CronTrigger)
        fields = {f.name: str(f) for f in job.trigger.fields}
        assert (fields["hour"], fields["minute"], fields["day_of_week"], fields["day"]) == (
            "2", "40", "*", "*")
        # One time: the backlog's slack counts its runs from `RUN_AT`.
        assert (fields["hour"], fields["minute"]) == (str(gc.RUN_AT.hour), str(gc.RUN_AT.minute))
        assert job.func.__wrapped__.__name__ == "_run_ga4_capture"

    def test_a_year_of_firings_misses_every_busy_window(self):
        """Read in Kyiv across both clock changes. Kills: a time moved into a
        window the brief keeps clear, or into the DST hours."""
        from core.scheduler import SCHEDULER_TIMEZONE

        trigger = _in_kyiv()
        at = datetime(2026, 3, 1, tzinfo=SCHEDULER_TIMEZONE)
        fired = []
        for _ in range(400):
            at = trigger.get_next_fire_time(None, at + timedelta(minutes=1))
            fired.append(at.astimezone(SCHEDULER_TIMEZONE))
        starts = {t.hour * 60 + t.minute for t in fired}
        assert starts == {2 * 60 + 40}
        assert not [w for w in BUSY for m in starts if w[0] <= m <= w[1]]
        # Once a calendar day, the two clock-change days included.
        assert len({t.date() for t in fired}) == 400

    def test_the_longest_gap_is_inside_the_stall_limit(self):
        """25 h across the night the clocks go back, under the 30 h the
        watch allows (`CAPTURES['app.ga4_funnel'].max_age`)."""
        from core.scheduler import BackgroundScheduler, SCHEDULER_TIMEZONE

        autumn = BackgroundScheduler._trigger_cadence_s(
            _in_kyiv(), datetime(2026, 10, 20, 12, tzinfo=SCHEDULER_TIMEZONE))
        assert autumn == 25 * 3600
        spec = cap.CAPTURES[gc.TABLE]
        assert spec.max_age is not None and autumn < spec.max_age.total_seconds()
        assert spec.writer == gc.__name__

    def test_the_job_is_the_capture(self, monkeypatch):
        from core.scheduler import BackgroundScheduler

        async def fake():
            return {"captured": 3}

        monkeypatch.setattr(gc, "run_capture", fake)
        assert asyncio.run(BackgroundScheduler()._run_ga4_capture()) == {"captured": 3}
