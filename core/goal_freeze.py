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

Daily at `FREEZE_AT` (00:30 Kyiv), and once `FIRST_RUN_DELAY_S` after a
start. The start is what freezes the month in progress on the day this ships
— November 2026's goal is the deploy day's, not the 1st's — and what freezes
a month whose 1st the process was down for, on the day it comes back. On the
other days a run reads which sales types are frozen and stops: one SELECT.

Never before `FREEZE_AT` on the 1st. "The last three complete months" counts
the month that has just ended, and its last orders reach Silver a few minutes
after midnight — the sync every minute, the derivation on its ten-minute
floor — so a freeze at 00:02 could miss them. A start in that half hour
leaves it to the 00:30 run.

The month's answers are all computed before anything is written, and written
in one transaction, so the sales types of one month are frozen from one
moment's inputs. A refused read (`read_fallback.ReadUnavailable`, which the
job answers) leaves the month unfrozen and read live, and the next run asks
again; so does any other failure, which goes to the job's history.

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

import json
import logging
import os
from datetime import datetime, time
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

# When the daily run fires, in Kyiv — and the earliest a month is frozen on
# its 1st (see "WHEN" above). Clear of the 01/07/13/19 checks, the 02:40 GA4
# capture and the 03:00-04:59 clock-change hours.
FREEZE_AT = time(0, 30)

# The first run after a start. A few minutes, out of the startup rush (the
# catch-ups go at 120-420 s, the operational job at 90 s): a deploy on a 1st
# freezes the month the same morning, a deploy on any other day costs one
# SELECT.
FIRST_RUN_DELAY_S = 240

# Which sales types the month already holds. The writer's own bookkeeping,
# read on its own connection.
_HELD_SQL = (
    "SELECT sales_type FROM app.frozen_month_goals "
    "WHERE year = $1 AND month = $2"
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


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def _held(pool, year: int, month: int) -> Set[str]:
    """The sales types `(year, month)` is frozen for already."""
    async with pool.acquire() as conn:
        return {row[0] for row in await conn.fetch(_HELD_SQL, year, month)}


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

    `store` is the repository (`generate_smart_goals`); `now` is the clock
    that chooses the month and stamps `frozen_at` — the wall clock when left
    out. Returns what it did, for the job's history. A refused read raises
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
    if due:
        # Every answer before any row: a refusal at any read leaves the month
        # as it was, read live, and the next run asks again.
        answers = [(sales_type, encode(await store.generate_smart_goals(year, month, sales_type)))
                   for sales_type in due]
        frozen = await _keep(pool, year, month, answers, at)
    else:
        frozen = []
    result["frozen"] = frozen
    result["already"] = sorted(held | (set(due) - set(frozen)))
    if frozen:
        logger.info("Month goal freeze: %s", result)
    return result
