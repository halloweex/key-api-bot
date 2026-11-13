"""The GA4 site funnel, kept per closed period before Google's horizon drops it.

WHY

GA4 builds a funnel from events and keeps events fourteen months, rolling by
the day (`core.ga4.earliest_day`): an older funnel comes back empty.
The /traffic page asks GA4 live and keeps nothing, so every day that ages out
is a funnel nobody can ask for again. This job keeps each closed day, ISO week
and calendar month in `app.ga4_funnel` (revision 0036), once, as the page would
have shown it. It is that table's one writer (`core.analytics_captures.CAPTURES`,
held by the repository walk in `tests/unit/test_write_chains.py`).

WHICH PERIODS

Kyiv dates: the property counts days in Kyiv time. A day is closed from D-3
back, because GA4 is still counting the last two. A week (Monday to Sunday) or
a month is closed when its last day is. A period is asked only when its first
day lies more than a day inside the horizon (`EDGE_MARGIN_DAYS`), so the edge
day, which GA4 may be deleting while it is asked, is never stored half empty.
A day lost to the margin is a gap; a half-empty day kept forever would be a
wrong number.

WHAT IS STORED

Four funnels a period: {ua, all} x {channel group, device}, the route's own
scopes, breakdowns and steps (`core.ga4`, which `ga4_funnel_service` reads
too). The channel group funnel's total row is breakdown 'total'. The device
funnel's total row counts the same people, so it is not stored twice. A
channel group's `traffic_kind` is the cold/warm/other rule as it stands at
capture, and `traffic_rule` is that rule's version (`core.ga4.TRAFFIC_RULE`),
so rows kept under an older rule say which rule placed them. Rows of
different grains are never summed: GA4 counts a person once per range.

ONCE

A period is kept when its total rows exist in both scopes. It is never asked
again, and the insert is ON CONFLICT DO NOTHING, so even a race does not
rewrite one. A re-capture is a human's DELETE, never automatic.

THE THROTTLE, AND ITS ARITHMETIC

Oldest first, by the first day of the period, because that is the order in
which periods age out. At most `PERIODS_PER_RUN` periods a run and
`PERIODS_PER_HOUR` in any hour, counted from `captured_at` in the table, so a
manual trigger or a restart cannot add to a run's spend. A period costs four
requests, asked one at a time.

- Spend: 24 periods are 96 `runFunnelReport` requests. GA4's quota for a
  standard property is 40 000 tokens an hour, 14 000 an hour for one Cloud
  project, and 200 000 a day. Google says most requests cost under 10 tokens,
  so 96 requests are under 960 tokens, 7 % of the project's hour. At five
  times that, 50 tokens for a month broken down by channel, they are 4 800
  tokens: 34 % of the project's hour and 2.4 % of the day. The cost of a
  funnel request has not been measured: setting `returnPropertyQuota` on one
  would measure it.
- Beside the route: an uncached period on /traffic costs six requests, at most
  once an hour per period and scope, and at most six are in flight. This job
  runs at 02:40 Kyiv, when nobody has the page open, with one request in
  flight. The total is seven, under GA4's ten concurrent requests.
- Backfill: on the first run about 500 periods are inside the horizon (about
  425 days, 60 weeks, 13 months). The edge takes about 1.2 periods a day, and
  24 a day are kept, so the backlog is gone in about three weeks. Oldest first
  means the edge never reaches an uncaptured period while each run keeps more
  than the edge takes.

FAILURE

Each closed period still missing is counted by the integrity job
(`backlog`, `ga4_backfill_incomplete`, WARN): right after revision 0036
deploys that is the backfill working through its first three weeks, and after
it a nightly run that is not keeping up.

No GA4 key or property is a supported state (`ga4.configured`): the job stands
down with nothing asked, written or stamped. If production loses its key, the
stall watch (`capture_stalled:app.ga4_funnel`, 30 h) says so in the digest
before the first lost day ages out. A GA4 error stops the run. The periods
already kept stay, the failure is stamped as `capture_failed:app.ga4_funnel`
(WARN, owner decision D8 (b)), and the next run retries from the oldest
missing period. Nothing is asked of GA4 while `KS_ANALYTICS_CAPTURES` is off.

BOUND

About 170 rows a period: 5 steps x (1 total + about 12 channel groups + about
4 devices) x 2 scopes. A year has 429 periods (365 days, 52 weeks, 12
months), so about 73 k rows. Measured on postgres:17.2 on 2026-11-13, a year
of such rows is 72 930 rows and 12 MB with the primary key, 173 bytes a row.
It is a history kept on purpose, and that arithmetic is its bound.
"""
from __future__ import annotations

import asyncio
import calendar
import contextlib
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import httpx

from core import ga4
from core.analytics_captures import captures_on, run_own_transaction

logger = logging.getLogger(__name__)

TABLE = "app.ga4_funnel"
_KYIV_TZ = ZoneInfo("Europe/Kyiv")

#: Shortest first, the order revision 0036's CHECK lists them.
GRAINS = ("day", "week", "month")
#: The last closed day is D-3: GA4 is still counting D-1 and D-2.
CLOSED_AFTER_DAYS = 3
#: Days kept clear of GA4's horizon (see "WHICH PERIODS").
EDGE_MARGIN_DAYS = 1
#: The throttle (see "THE THROTTLE, AND ITS ARITHMETIC").
PERIODS_PER_RUN = 24
PERIODS_PER_HOUR = 24
#: Each scope's two funnels: (breakdown as stored, GA4's dimension).
BREAKDOWNS = (("channel_group", ga4.CHANNEL), ("device", ga4.DEVICE))
STATEMENT_TIMEOUT = "15s"
ACQUIRE_TIMEOUT_S = 20

_INSERT_SQL = """
INSERT INTO app.ga4_funnel
    (grain, period_start, period_end, country_scope, breakdown, breakdown_value,
     traffic_kind, traffic_rule, step, active_users)
SELECT $1, $2, $3, r.scope, r.breakdown, r.value, r.kind, r.rule, r.step, r.users
FROM unnest($4::text[], $5::text[], $6::text[], $7::text[], $8::text[],
            $9::smallint[], $10::integer[])
     AS r(scope, breakdown, value, kind, rule, step, users)
ON CONFLICT (grain, period_start, country_scope, breakdown, breakdown_value, step)
DO NOTHING
"""

# A period is kept when its total rows are there in every scope.
_KEPT_SQL = """
SELECT grain, period_start
FROM app.ga4_funnel
WHERE breakdown = 'total' AND step = 1 AND period_start >= $1
GROUP BY grain, period_start
HAVING count(DISTINCT country_scope) = $2
"""

# Periods kept in the last hour, whichever run kept them.
_SPENT_SQL = """
SELECT count(*) FROM (
    SELECT DISTINCT grain, period_start
    FROM app.ga4_funnel
    WHERE captured_at > now() - interval '1 hour'
) AS this_hour
"""


class Period(NamedTuple):
    grain: str
    start: date
    end: date


# (country_scope, breakdown, breakdown_value, traffic_kind, traffic_rule, step, active_users)
Row = Tuple[str, str, str, str, str, int, int]


# ─── Which periods ────────────────────────────────────────────────────────────


def kyiv_today(now: Optional[datetime] = None) -> date:
    """Today in Kyiv. `now` must be timezone-aware; the default is the clock."""
    return (now or datetime.now(_KYIV_TZ)).astimezone(_KYIV_TZ).date()


def last_closed_day(today: date) -> date:
    return today - timedelta(days=CLOSED_AFTER_DAYS)


def first_capturable_day(today: date) -> date:
    return ga4.earliest_day(today) + timedelta(days=EDGE_MARGIN_DAYS)


def _next_month(day: date) -> date:
    return date(day.year + day.month // 12, day.month % 12 + 1, 1)


def _month_end(first: date) -> date:
    return first.replace(day=calendar.monthrange(first.year, first.month)[1])


def due_periods(today: date) -> List[Period]:
    """Every closed period GA4 still holds whole, oldest first."""
    first, last = first_capturable_day(today), last_closed_day(today)
    out: List[Period] = []
    day = first
    while day <= last:
        out.append(Period("day", day, day))
        day += timedelta(days=1)
    monday = first + timedelta(days=(7 - first.weekday()) % 7)
    while monday + timedelta(days=6) <= last:
        out.append(Period("week", monday, monday + timedelta(days=6)))
        monday += timedelta(days=7)
    month = first if first.day == 1 else _next_month(first)
    while _month_end(month) <= last:
        out.append(Period("month", month, _month_end(month)))
        month = _next_month(month)
    rank = {grain: i for i, grain in enumerate(GRAINS)}
    return sorted(out, key=lambda p: (p.start, rank[p.grain]))


# ─── What is stored ───────────────────────────────────────────────────────────


def _steps(scope: str, breakdown: str, value: str, kind: str, rule: str,
           people: Dict[str, int]) -> List[Row]:
    return [(scope, breakdown, value, kind, rule, i + 1, int(people.get(column, 0)))
            for i, column in enumerate(ga4.COLUMNS)]


def rows_for(scope: str, by_channel: Dict[str, Dict[str, int]],
             by_device: Dict[str, Dict[str, int]]) -> List[Row]:
    """One scope's rows from its two parsed funnels (`ga4.parse_funnel`).

    The total always has its five rows, zeros when GA4 gave none, as the page
    reads it: they are what marks the period kept."""
    rows = _steps(scope, "total", ga4.TOTAL, "", "", by_channel.get(ga4.TOTAL, {}))
    for value in sorted(by_channel):
        if value != ga4.TOTAL:
            rows += _steps(scope, "channel_group", value, ga4.channel_type(value),
                           ga4.TRAFFIC_RULE, by_channel[value])
    for value in sorted(by_device):
        if value != ga4.TOTAL:
            rows += _steps(scope, "device", value, "", "", by_device[value])
    return rows


async def _ask(client: httpx.AsyncClient, period: Period, spent: List[int]) -> List[Row]:
    """The period's four funnels, one request at a time. `spent` counts them."""
    rows: List[Row] = []
    for scope, countries in ga4.SCOPES.items():
        answers: Dict[str, Dict[str, Dict[str, int]]] = {}
        for breakdown, dimension in BREAKDOWNS:
            token = await ga4.access_token(client)
            spent[0] += 1
            answers[breakdown] = await ga4.run_funnel(
                client, token, period.start, period.end, dimension, countries=countries)
        rows += rows_for(scope, answers["channel_group"], answers["device"])
    return rows


def _inserted(status: Any) -> int:
    try:
        return int(str(status).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


def _keeper(period: Period, rows: List[Row], kept: List[int]):
    async def capture(conn) -> int:
        status = await conn.execute(
            _INSERT_SQL, period.grain, period.start, period.end,
            *(list(column) for column in zip(*rows)))
        kept[0] = _inserted(status)
        return kept[0]
    return capture


async def _nothing(conn) -> int:
    """A run with nothing to ask still stamps: the stall watch reads the stamp."""
    return 0


async def _stamp_failure(exc: BaseException) -> None:
    """Stamp `exc` as this capture's failure, through the shared helper."""
    async def refuse(conn) -> int:
        raise exc
    await run_own_transaction(TABLE, refuse)


async def _kept(conn, first: date) -> Set[Tuple[str, date]]:
    """The periods already kept from `first` on, read on `conn`."""
    return {(r["grain"], r["period_start"])
            for r in await conn.fetch(_KEPT_SQL, first, len(ga4.SCOPES))}


async def _read_kept(first: date) -> Tuple[Set[Tuple[str, date]], int]:
    """The periods already kept from `first` on, and how many were kept in the last hour."""
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        await require_revision(conn=conn)
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            kept = await _kept(conn, first)
            spent = int(await conn.fetchval(_SPENT_SQL) or 0)
    return kept, spent


# ─── The backlog the integrity job watches ────────────────────────────────────


def overdue_periods(today: date) -> List[Period]:
    """The due periods a night's run should already have kept: due since
    yesterday at least. A period falls due at midnight and is kept at 02:40,
    so the 01:00 integrity run does not count the one that fell due an hour
    ago."""
    since = last_closed_day(today) - timedelta(days=1)
    return [p for p in due_periods(today) if p.end <= since]


async def backlog(conn, today: date) -> Tuple[int, Optional[Period]]:
    """How many overdue periods are not kept, and the oldest of them, read
    on `conn` (`analytics_captures.read_facts`, inside its transaction).
    Every one of them is still inside GA4's horizon; the oldest is the next
    to leave it."""
    kept = await _kept(conn, first_capturable_day(today))
    missing = [p for p in overdue_periods(today) if (p.grain, p.start) not in kept]
    return len(missing), (missing[0] if missing else None)


# ─── The job ──────────────────────────────────────────────────────────────────


async def run_capture(*, client: Optional[httpx.AsyncClient] = None,
                      today: Optional[date] = None) -> Dict[str, Any]:
    """Keep the oldest missing closed periods, within the throttle. Never
    raises, a cancellation apart: a fault is the result's `error` and, once
    Postgres can be reached, the capture's stamp."""
    if not captures_on():
        return {"skipped": "KS_ANALYTICS_CAPTURES is off"}
    if not ga4.configured():
        return {"skipped": "GA4 is not configured"}
    if not os.getenv("KS_PG_DSN", "").strip():
        return {"skipped": "KS_PG_DSN is not set"}

    day = today or kyiv_today()
    due = due_periods(day)
    try:
        kept, spent_this_hour = await _read_kept(first_capturable_day(day))
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — no store, no stamp: the stall watch says it
        logger.error("GA4 capture: could not read what is kept: %s: %s",
                     type(exc).__name__, exc)
        return {"error": f"{type(exc).__name__}: {exc}"}

    missing = [p for p in due if (p.grain, p.start) not in kept]
    budget = max(0, min(PERIODS_PER_RUN, PERIODS_PER_HOUR - spent_this_hour))
    todo = missing[:budget]
    result: Dict[str, Any] = {
        "due": len(due), "missing": len(missing), "budget": budget,
        "captured": 0, "rows": 0, "requests": 0,
    }
    if not todo:
        await run_own_transaction(TABLE, _nothing)
    else:
        spent = [0]
        try:
            # A client handed in (the tests' fake GA4) is used and left open.
            async with (httpx.AsyncClient() if client is None
                        else contextlib.nullcontext(client)) as http:
                for period in todo:
                    try:
                        rows = await _ask(http, period, spent)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 — stamped, the run stops
                        if not isinstance(exc, ga4.GA4Unavailable):
                            logger.exception("GA4 capture: %s %s failed", period.grain, period.start)
                        await _stamp_failure(exc)
                        result["error"] = f"{period.grain} {period.start}: {type(exc).__name__}: {exc}"
                        break
                    inserted = [0]
                    if not await run_own_transaction(TABLE, _keeper(period, rows, inserted)):
                        result["error"] = f"{period.grain} {period.start}: not written (see the log)"
                        break
                    result["captured"] += 1
                    result["rows"] += inserted[0]
        finally:
            result["requests"] = spent[0]

    # `todo` is the head of `missing`, kept in order until the first failure.
    left = missing[result["captured"]:]
    result["left"] = len(left)
    result["oldest_missing"] = left[0].start.isoformat() if left else None
    log = logger.warning if "error" in result else logger.info
    log("GA4 capture: %s", result)
    return result

