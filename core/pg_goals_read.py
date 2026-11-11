"""Reading `/goals` from Postgres — the four of its thirteen that can move.

ONE ENGINE SINCE STEP 14 (S14-07)

Every goal read is Postgres': `GoalsMixin._goals_run` and `_goal_tables_run`
ask this module and nothing else, and a failure — a lost DSN line included —
is refused as a 503 naming `goals` (`read_fallback.refuse`). `KS_READ_GOALS`
chooses nothing now, and `KS_GOALS_HISTORY` is not read at all: the refusal
of any value but unset and `silver`, its `/api/health` block `goals_history`
and the canary's `goals_history_mode_invalid` went with S14-07. The line stays
in production's `.env` (`KS_GOALS_HISTORY=silver`), because images from before
7b-4 read it unset as the bridge. The paragraphs below are the history.

WHAT IS HERE AND WHAT IS NOT

`/goals` is the last tab untouched by the migration, and it splits cleanly:

  * **here** — `get_goals`, `get_smart_goals`, `get_historical_revenue` and
    `get_daily_revenue_for_dates`. They read `revenue_goals` and the revenue
    history, and Postgres has both: `app.revenue_goals` since revision 0017,
    Silver since 0005.
  * **since revision 0025** — `get_predictions`, over `revenue_predictions`,
    which that revision gave a Postgres home (#183).
  * **not here** — `generate_smart_goals`' reads of `seasonal_indices`,
    `weekly_patterns` and `growth_metrics`. Revision 0025 created those three
    in Postgres too, but only as an hourly replica of what DuckDB writes, so a
    routed read would answer up to an hour behind `POST /goals/recalculate`.
    They are read where they are written, through `_goal_tables_run`, which
    asks chain 7b-3 (`KS_WRITE_FORECAST`, `core/pg_forecast_write.py`) and
    never this flag: DuckDB while that chain writes DuckDB, Postgres with no
    fallback once it writes there. `fetch` below is what answers then.
  * **not here either** — the seven writes. This tab is the only one that
    writes from the interface, and `app.revenue_goals` is an hourly read
    replica: DuckDB is still the writer, so a write routed here would land in
    a copy and be overwritten within the hour. The one write that may move
    is `set_goal`, and it moves as a write chain rather than through this
    reader — `KS_WRITE_GOALS` (chain 7a, `core/pg_goals_write.py`), which
    stands the hourly replace down for the table in the same breath.

WHAT THE PORT REMOVES

Two hand-rolled copies of rules Silver already carries as columns. The bodies
read raw `orders` today and re-apply `status_id NOT IN (…)` for returns; Silver
has `is_return`, which prefers KeyCRM's status *group* — the thing that caught
status 20 in July. `get_daily_revenue_for_dates` also carries its own
`source_id IN (1, 2, 4)`, and `get_historical_revenue` **carries none**, so the
two disagreed about which channels count.

Measured before changing anything, on the production backup over 28 days: the
disagreement costs **nothing today** — there are no retail orders outside those
three sources in the window, and `/goals` totals ₴3,209,005.26, which is
`gold_daily_revenue` for the same window to the kopeck. So this is a latent
divergence being closed, not a number being corrected.

The Kyiv date stops being computed too: `silver.orders.order_date` *is* it.

THE TRAP THAT WOULD HAVE SHIPPED

The bodies bound their windows with `CURRENT_DATE`, and the two engines do not
agree on what day that is: the `web` container runs `TZ=Europe/Kyiv` and the
Postgres server answers in UTC, so **between 21:00 and midnight UTC they name
different days** — and every one of these windows would have moved by one on
whichever engine answered. `core.sql_dialect.TODAY_IN_KYIV` is the expression
both engines accept and both mean the same by, and it is what these bodies use
now. Invisible for twenty-one hours out of twenty-four, which is why it is
written down rather than left to a differential test that runs under one
timezone.

WHY IT FELL BACK, AND WHERE IT MUST NOT, UNTIL S14-07

Revenue history and Gold diverge nowhere: they are derived layers DuckDB keeps
current, so a fault costs the engine rather than the page —
`core/pg_gold_read.py`'s answer.

`app.revenue_goals` is not one of them once chain 7a writes Postgres. From the
flip, DuckDB's `revenue_goals` stops changing — nothing writes it and the
hourly replace stands down — so a fallback there would not show an older
answer to the same question but the goal as it stood before the flip. Those
reads therefore go to Postgres while the chain does, whatever this flag says,
and raise instead of falling back (`GoalsMixin._goals_run`,
`core/pg_goals_write.py`).

WHICH ORDERS THE CALCULATORS COUNT — SILVER, AND A RETIRED SWITCH (CHAIN 7b)

The goal calculators — seasonality, YoY, weekly patterns, the growth cap, and
the last-year and recent-months reads of a smart goal — read the whole order
history from `{silver_orders}` through `_goals_run`, so the engine is
`KS_READ_GOALS`' as for every other goal read, and DN-20's counting and
refusal cover it.

From DN-12 to chain 7b-4 they could also read it through a bridge: DuckDB
`orders` narrowed by `silver_sales_type_case` rendered over DuckDB's
`managers` and `manager_classifications`, right only while DuckDB held those
three — and step 13 freezes them. `KS_GOALS_HISTORY` chose between the two
(`bridge`, the default then, or `silver`). Production read Silver from
2026-10-07 08:20 UTC, after a dry run over that day's backup found every
number equal; 7b-4 deleted the bridge.

So the variable was retired, and from 7b-4 to step 14's S14-07 read only to
refuse: unset and `silver` both meant Silver, and any other value raised at
every history read — `bridge` included, with a message of its own. A value naming a history that no longer
exists is a configuration nobody can satisfy, and answering it from Silver in
silence would hide that. Refusing fails what reads the calculators' history:
the smart goal (the dashboard's goal widget), `/goals/smart`,
`/goals/seasonality`, `/goals/growth`, `/goals/weekly-patterns`,
`/goals/forecast`, `POST /goals/recalculate` and the Monday job.
`GET /goals`, `/goals/history` and setting or resetting a goal never read it —
they read a recent window of Silver, `is_active_source` included — and
answer as before. `/api/health` published the error, which the canary paged
as `goals_history_mode_invalid` (CRITICAL). Never at import or at startup, so
web, the only syncer, could not crash-loop on it. Since S14-07 nothing reads
the variable, so no value of it refuses anything.

**Retired is not removable.** This build reads nothing, but every image from
before 7b-4 reads unset as `bridge`. So production keeps the line
`KS_GOALS_HISTORY=silver` for as long as such an image can be a rollback
target.

It never was `KS_READ_GOALS` reused, nor named `KS_READ_*`: the step-13
readiness reads every such name as an engine switch that must equal
`postgres`, and this one named a row set.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one goal query against Postgres and return plain tuples.

    `sql` arrives with `?` markers, the shared bodies' convention, and is
    renumbered for asyncpg here rather than by every caller.
    """
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]
