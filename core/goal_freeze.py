"""The month's automatic goal, frozen on its first day (revision 0037).

WHY

`GET /api/goals/smart` computed the month's automatic goal on every request
(`GoalsMixin.generate_smart_goals`), and 30-40 % of it is the ML signal: the
month's actual revenue so far plus the stored forecast of the rest, which
drifts toward the actual as the month goes. Measured, the goal moved within a
month by a median 4-6 %, and in July 2026 by about ₴0.5M. A past month's goal
changed after the fact too: "the last three complete months" are counted from
today, and the ML term is 0 for a month that has ended. The owner's decision
of 2026-11-18: freeze the month's automatic goal on the 1st.

WHAT

`freeze_month` computes `generate_smart_goals(year, month, sales_type)` for
the Kyiv month in progress, for every sales type the goal routes accept
(`SALES_TYPES`), and keeps each answer whole in `app.frozen_month_goals`. The
reader is `GoalsMixin.smart_goals_of_month`, behind `/api/goals/smart` and
`/api/goals/forecast`: the frozen answer for a month that has a row, the live
computation as before for one that has none — every month before this
revision, and a month whose freeze has not run yet. A goal typed on /goals
(`app.revenue_goals.is_custom`) overrides either, as it always did.

ONE WRITER, AND NEVER A GET

This module is the table's only writer — `tests/unit/test_goal_freeze.py`
walks the repository for it — and it is reached only from the scheduler's job
`month_goal_freeze`, never from a request (OD-14 (i): a GET never writes; a
month nobody froze is read live, not frozen by the asking). Insert-only, `ON
CONFLICT DO NOTHING`: the first freeze of a month wins, so a second run, a
manual trigger or two runs at once cannot move a goal that is frozen.

WHEN

At half past the hours of `RUN_HOURS`, 00:30 Kyiv (`FREEZE_AT`) the first
of them, and once `FIRST_RUN_DELAY_S` after a start. The start is what
freezes the month in progress on the day this ships — November 2026's goal
is the deploy day's, not the 1st's — and what freezes a month whose 1st the
process was down for, on the day it comes back. Hourly, so that a run
turned away — a refused read, a forecast short of the month (below) — is
asked again within the hour, not the next night; once the month is frozen a
run reads which sales types are and stops: one SELECT.

Never before `FREEZE_AT` on the 1st. "The last three complete months" counts
the month that has just ended, and its last orders reach Silver a few minutes
after midnight — the sync every minute, the derivation on its ten-minute
floor — so a freeze at 00:02 could miss them. A start in that half hour
leaves it to the 00:30 run, and the 00:30 run itself waits for the
derivation (below).

The month's answers are all computed before anything is written, and written
in one transaction, so the sales types of one month are frozen from one
moment's inputs. A refused read (`read_fallback.ReadUnavailable`, which the
job answers) leaves the month unfrozen and read live, and the next run asks
again; so does any other failure, which goes to the job's history.

THE MONTH THAT HAS JUST ENDED IS DERIVED FIRST

Every signal of the goal reads Silver, or Gold beside it, and "the last three
complete months" count the month that has just ended. `FREEZE_AT` gave the
derivation half an hour after midnight and nothing checked that it ran: a
derivation stuck since the last day's evening would have frozen the month on
a Silver short of that day's last hours, for the whole month (the forecast
review, 2026-11-20). So a month is frozen only once the derivation's journal,
`meta.derivation_runs`, holds a validated run — Silver and Gold rebuilt and
their checks passed — that started `DERIVED_AFTER` or more past the month's
first midnight in Kyiv. Each run rebuilds Silver and Gold whole from bronze
as it stood when the run began, and a failed one leaves the previous build
in place, so the Silver and Gold read after such a run hold the whole of the
last day. Until
then every sales type the month lacks is `waiting`, with the reason, nothing
is computed or written, and the next run asks again. On a quiet night the
derivation runs on its hourly heartbeat, so the month freezes by 01:30.

A GOAL RESTING ON PART OF A FORECAST WAITS

The ML signal is the month's actual so far plus the stored forecast of the
rest, summed over whatever rows there are (`_get_ml_forecast_total`). Each
training stores 60 days ahead, so on a 1st with no training for 30 days and
more the forecast stops short of the month's end, and the signal, and the
goal, come out too low — 4.0M where it was 4.2M with 24 days of 31 on the
review's fixture. Live, the next training corrected it; frozen, it would
stand all month. So an answer whose ML signal rests on fewer days than are
left (`_short_forecast`) is not kept: that sales type is read live, the
result names it under `waiting`, and the first run after a training that
reaches the month's end freezes it, from that run's inputs. A forecast with
no row at all is absent, not short — the blend gives its weight to the
other signals — and is kept, as is every answer without the ML signal.

NO HISTORY OF ITS OWN IS FROZEN AS NO GOAL

A sales type with no history of its own has no automatic goal
(`calculationMethod` `no_history`, `goal` null in every period; the owner's
decision of 2026-11-19), and that answer is kept like any other. It cannot
change inside the month: the month a year earlier and the complete months
before this one are settled by then, and no forecast is stored for such a
sales type. November 2026 was frozen before the rule — ₴5.7M for the
offline store and for the exhibition — and moves only when a person deletes
those rows and triggers the job (`.claude/CLAUDE.md`, "Revenue goals").

THE SHAPE IS THE CODE'S THAT FROZE IT

A row keeps `generate_smart_goals`' answer as that day's code built it. A
reader that needs a key added to the answer later must default it for the
rows frozen before: `tests/unit/test_goal_freeze.py` reads a row frozen by
this revision's code, kept there as a literal, and must go on reading it.
JSON has no integer keys, so the two week maps (`weekly.breakdown`,
`weekly.weights`) come back with their week numbers made integers again
(`decode`), as the live answer carries them.

BOUND

About one row per month per sales type: six sales types today, 72 rows a year
of about 1.5 KB each, under 150 KB a year. Append-only by design — the rows
are the record of the goal each month was measured against — and nothing
deletes one.
"""
from __future__ import annotations

import calendar
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from core.duckdb_constants import DEFAULT_TZ, KNOWN_SALES_TYPES

logger = logging.getLogger(__name__)

TABLE = "app.frozen_month_goals"

# Every sales type `/api/goals/smart` and `/api/goals/forecast` accept —
# `core.validators.validate_sales_type`'s set, `KNOWN_SALES_TYPES` and `all`
# — so every answer those routes can give for a frozen month is the frozen
# one. Derived rather than listed: a sales type added to `KNOWN_SALES_TYPES`
# is frozen from its first month. `internal` included: the routes accept it
# (for admins, `api_gate`), and a month frozen for the others but live for it
# would be the inconsistency this exists to remove.
SALES_TYPES: Tuple[str, ...] = (*KNOWN_SALES_TYPES, "all")

# The first run of the day, in Kyiv — and the earliest a month is frozen on
# its 1st (see "WHEN" above).
FREEZE_AT = time(0, 30)

# The hours whose half past the job fires at, `FREEZE_AT` the first. Left
# out, so that no run lands in a window the schedule keeps clear: 02:30-02:50
# (the GA4 capture), 03:00-04:59 (a clock change can skip or repeat them),
# 05:00-05:50, 07:20-07:45 and 08:55-09:55; every :30 kept is also clear of
# the 01/07/13/19 checks.
RUN_HOURS: Tuple[int, ...] = (0, 1, 6, 8, *range(10, 24))

# The first run after a start. A few minutes, out of the startup rush (the
# catch-ups go at 120-420 s, the operational job at 90 s): a deploy on a 1st
# freezes the month the same morning, a deploy on any other day costs one
# SELECT.
FIRST_RUN_DELAY_S = 240

# How long past the month's first midnight, in Kyiv, the derivation that a
# freeze reads must have started (see "THE MONTH THAT HAS JUST ENDED IS
# DERIVED FIRST" above). The sync asks KeyCRM every minute, so the last day's
# last orders are in bronze a minute or two after midnight; five minutes
# leave room for a slow fetch.
DERIVED_AFTER = timedelta(minutes=5)

# Which sales types the month already holds. The writer's own bookkeeping,
# read on its own connection.
_HELD_SQL = (
    "SELECT sales_type FROM app.frozen_month_goals "
    "WHERE year = $1 AND month = $2"
)

# When the last validated derivation of the warehouse started: Silver and
# Gold rebuilt whole from bronze as it stood then, and their checks passed.
# A run that raised has no verdict — `core.pg_derivation.record_run` writes
# `validation_passed` only on a run that got through its validation, its
# error NULL — so the verdict alone says both. $1 is `pg_derivation.LAYER`.
_DERIVED_SQL = (
    "SELECT max(started_at) FROM meta.derivation_runs "
    "WHERE layer = $1 AND validation_passed"
)

# One month's answer for one sales type. The first freeze wins: a row a
# concurrent run kept first is left as it is, and `RETURNING` says which rows
# this run kept. A module constant, so the repository walk finds this module
# as the table's one writer.
_FREEZE_SQL = (
    "INSERT INTO app.frozen_month_goals "
    "(year, month, sales_type, smart, frozen_at) "
    "VALUES ($1, $2, $3, $4::jsonb, $5) "
    "ON CONFLICT (year, month, sales_type) DO NOTHING "
    "RETURNING sales_type"
)

# The answer's two maps keyed by week number. JSON has no integer keys: they
# are stored as "1".."5" and made integers again on the way out.
_WEEK_MAPS: Tuple[Tuple[str, str], ...] = (("weekly", "breakdown"), ("weekly", "weights"))


def encode(smart: Dict[str, Any]) -> str:
    """The answer as the text the `::jsonb` cast takes — asyncpg has no JSONB
    codec registered here (`core.alert_actions._json`). `allow_nan=False`:
    Postgres refuses NaN in JSON, and a NaN in a goal is a defect better
    raised here, with nothing written, than stored as something else."""
    return json.dumps(smart, ensure_ascii=False, allow_nan=False)


def decode(stored: Any) -> Dict[str, Any]:
    """A stored answer as `generate_smart_goals` returned it: the JSONB text
    parsed, and the week maps keyed by integers again. A fresh dict on every
    call, so a reader may add to it."""
    smart = json.loads(stored) if isinstance(stored, (str, bytes)) else json.loads(json.dumps(stored))
    for outer, inner in _WEEK_MAPS:
        block = smart.get(outer)
        if isinstance(block, dict) and isinstance(block.get(inner), dict):
            block[inner] = {int(week): value for week, value in block[inner].items()}
    return smart


def too_early(at: datetime) -> bool:
    """Whether `at` is on a month's 1st before `FREEZE_AT`, in Kyiv."""
    local = at.astimezone(DEFAULT_TZ)
    return local.day == 1 and local.time() < FREEZE_AT


def not_derived(derived_at: Optional[datetime], at: datetime) -> Optional[str]:
    """Why `at`'s Kyiv month may not be frozen yet — the last validated
    derivation, which started at `derived_at` (aware; None when the journal
    holds none), started before `DERIVED_AFTER` past the month's first
    midnight — or None (see "THE MONTH THAT HAS JUST ENDED IS DERIVED FIRST"
    above)."""
    began = datetime.combine(at.astimezone(DEFAULT_TZ).date().replace(day=1), time(0),
                             tzinfo=DEFAULT_TZ)
    needed = began + DERIVED_AFTER
    if derived_at is not None and derived_at >= needed:
        return None
    last = ("none is journalled" if derived_at is None else
            f"the last started {derived_at.astimezone(DEFAULT_TZ):%Y-%m-%d %H:%M}")
    return (f"no validated derivation has started since {needed:%Y-%m-%d %H:%M} Kyiv "
            f"({last}): the month that has just ended may not all be in Silver yet")


async def _short_forecast(store, sales_type: str, smart: Dict[str, Any], at: datetime) -> Optional[str]:
    """Why `smart` may not be kept yet — its ML signal summed a forecast of
    fewer days than the month has left — or None (see "A GOAL RESTING ON
    PART OF A FORECAST WAITS" above). Asked only of an answer that carries
    the signal; the days are counted from `at`'s Kyiv day, the day
    `_get_ml_forecast_total` predicts from."""
    if not (smart.get("monthly") or {}).get("mlForecastGoal"):
        return None
    today = at.astimezone(DEFAULT_TZ).date()
    last = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
    days = (last - today).days + 1
    held = await store.forecast_days_stored(sales_type, today, last)
    if held >= days:
        return None
    return f"the stored forecast holds {held} of the {days} days to the month's end"


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def _held(pool, year: int, month: int) -> Set[str]:
    """The sales types `(year, month)` is frozen for already."""
    async with pool.acquire() as conn:
        return {row[0] for row in await conn.fetch(_HELD_SQL, year, month)}


async def _derived_at(pool) -> Optional[datetime]:
    """When the last validated derivation started, or None."""
    from core.pg_derivation import LAYER

    async with pool.acquire() as conn:
        return await conn.fetchval(_DERIVED_SQL, LAYER)


async def _keep(
    pool, year: int, month: int, answers: Sequence[Tuple[str, str]], at: datetime,
) -> List[str]:
    """Keep the month's encoded answers, in one transaction; the sales types
    this run froze, in `answers`' order."""
    frozen: List[str] = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            for sales_type, smart in answers:
                kept = await conn.fetchval(_FREEZE_SQL, year, month, sales_type, smart, at)
                if kept is not None:
                    frozen.append(kept)
    return frozen


async def freeze_month(store, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Freeze the Kyiv month in progress for every sales type not frozen yet.

    `store` is the repository (`generate_smart_goals`, `forecast_days_stored`);
    `now` is the clock that chooses the month and stamps `frozen_at` — the
    wall clock when left out. Returns what it did, for the job's history: the
    sales types `frozen` and those frozen `already`, and, when there are any,
    those `waiting`, with the reason — every one the month lacks while the
    derivation has not taken in the month that has just ended, or an answer
    resting on part of a forecast, not kept. A refused read raises
    `read_fallback.ReadUnavailable` with nothing written (the job answers
    it); any other failure raises with nothing written either.
    """
    if not os.getenv("KS_PG_DSN", "").strip():
        return {"skipped": "KS_PG_DSN is not set"}
    at = (now or datetime.now(DEFAULT_TZ)).astimezone(DEFAULT_TZ)
    year, month = at.year, at.month
    result: Dict[str, Any] = {"month": f"{year}-{month:02d}"}
    if too_early(at):
        result["skipped"] = (
            f"not before {FREEZE_AT:%H:%M} Kyiv on the 1st: the month that has "
            f"just ended may not all be in Silver yet")
        return result

    pool = await _pool()
    held = await _held(pool, year, month)
    due = [sales_type for sales_type in SALES_TYPES if sales_type not in held]
    answers: List[Tuple[str, str]] = []
    waiting: Dict[str, str] = {}
    underived = not_derived(await _derived_at(pool), at) if due else None
    if underived is not None:
        waiting = dict.fromkeys(due, underived)
    elif due:
        # Every answer before any row: a refusal at any read leaves the month
        # as it was, read live, and the next run asks again.
        computed = [(sales_type, await store.generate_smart_goals(year, month, sales_type))
                    for sales_type in due]
        encoded = [(sales_type, encode(smart)) for sales_type, smart in computed]
        for (sales_type, smart), kept in zip(computed, encoded):
            why = await _short_forecast(store, sales_type, smart, at)
            if why is None:
                answers.append(kept)
            else:
                waiting[sales_type] = why
    frozen = await _keep(pool, year, month, answers, at) if answers else []
    result["frozen"] = frozen
    result["already"] = sorted(held | ({s for s, _ in answers} - set(frozen)))
    if waiting:
        result["waiting"] = waiting
        logger.warning("Month goal freeze: %s waits, read live meanwhile — %s",
                       ", ".join(waiting), result)
    elif frozen:
        logger.info("Month goal freeze: %s", result)
    return result
