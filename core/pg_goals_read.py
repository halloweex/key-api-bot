"""Reading `/goals` from Postgres — the four of its thirteen that can move.

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

WHY THIS FALLS BACK, AND WHERE IT MUST NOT

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

WHICH ORDERS THE CALCULATORS COUNT — A SECOND SWITCH (CHAIN 7b)

The goal calculators — seasonality, YoY, weekly patterns, the growth cap, and
the last-year and recent-months reads of a smart goal — read the whole order
history. Since DN-12 they read it through a bridge: DuckDB `orders` narrowed by
`silver_sales_type_case` rendered over DuckDB's `managers` and
`manager_classifications`. That is right only while DuckDB holds those three,
and step 13 freezes them.

`KS_GOALS_HISTORY` chooses the row set: `bridge` (the default, today's
numbers) or `silver` — `{silver_orders}` through `_goals_run`, so the engine
is `KS_READ_GOALS`' as for every other goal read, and DN-20's counting and
refusal cover it. Measured on the 2026-08-31 production copy before choosing:
the two select the same orders for every sales type, and every calculator and
every smart goal answered identically, on both engines.

It is not `KS_READ_GOALS` reused, for three reasons: that variable is already
`postgres` in production, so a reuse would move these reads at the deploy; it
names an engine, and this names which orders count; and putting an engine
switch back is every read port's rollback, which must never also change the
semantics. Nor is it named `KS_READ_*`: the step-13 readiness reads every such
name as an engine switch that must equal `postgres`.

An unknown value raises at the read, this module's rule above — never at
import or at startup, so web, the only syncer, cannot crash-loop on it.
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_GOALS"
_VALID = ("duckdb", "postgres")

HISTORY_ENV = "KS_GOALS_HISTORY"
BRIDGE = "bridge"
SILVER = "silver"
_HISTORY_VALID = (BRIDGE, SILVER)


def history_mode_of(raw: Optional[str]) -> str:
    """`raw` — a `KS_GOALS_HISTORY` value, None for unset — as the mode it
    names. Pure, so the step-13 readiness can ask it of an environment it was
    handed. An unknown value raises."""
    value = (raw or "").strip().lower() or BRIDGE
    if value not in _HISTORY_VALID:
        raise ValueError(
            f"{HISTORY_ENV}={value!r} — unknown history. Expected one of "
            f"{_HISTORY_VALID}; a typo here must stop the read, not quietly "
            f"count a different set of orders."
        )
    return value


def history_mode() -> str:
    """Which orders the goal calculators count: `bridge` (DuckDB `orders`
    through the DN-12 bridge) or `silver` (`{silver_orders}`, through the
    goal router). Read at every history read; an unknown value raises."""
    return history_mode_of(os.getenv(HISTORY_ENV))


def history_from_silver() -> bool:
    return history_mode() == SILVER


def history_state() -> dict:
    """`KS_GOALS_HISTORY` as a history read would take it now: `mode`, or
    None and the `error` that read would raise. Never raises.

    The raise is at the read, so a typo stops every goal history read — the
    dashboard's goal widget, `/goals/*`, the POST and the Monday job — and
    none of those pages anybody: a 500 on a page and a job error in a log.
    So `/api/health` publishes this, and the canary pages the error as
    `goals_history_mode_invalid`. The value is the variable's own, which is
    not a secret, as `read_fallback_mode` publishes its siblings'."""
    try:
        return {"mode": history_mode(), "error": None}
    except ValueError as exc:
        return {"mode": None, "error": str(exc)}


def enabled() -> bool:
    """Whether the goal reads should ask Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule. A typo in the variable
    deciding which engine answers should stop the read loudly rather than
    quietly serve the other store.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    """Postgres is configured at all. Without a DSN there is nothing to ask,
    and the flag alone must not turn a working tab into an error."""
    return bool(os.getenv("KS_PG_DSN", "").strip())


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
