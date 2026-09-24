"""Chain 7a: the revenue goals a human types, written to Postgres.

`revenue_goals` is three rows at most — `daily`, `weekly`, `monthly` — and
the amount in each is a number somebody typed on /goals, or a suggestion the
service computed and somebody accepted by resetting to it. KeyCRM has never
heard of either, so no rebuild produces them: of everything the goals chain
stores, these three rows are the only thing lost for good if they are lost
(`.planning/DUCKDB_EXIT_STAGE4_CHAINS.md`, chain 7). Everything else the chain
writes — seasonality, growth, weekly patterns, the forecast — is one
recalculation away, which is why this part moves first and alone (DN-25).

WHAT IT CLOSES

The page and the form disagree about which store holds a goal. The form's
POST wrote DuckDB, the page's GET reads Postgres under `KS_READ_GOALS`, and
`app.revenue_goals` was an hourly replica in between — so a goal typed at
10:05 was invisible until the 11:00 copy, and `set_goal` computed its
`calculated_goal` from Postgres while writing the row to DuckDB. Under
`KS_WRITE_GOALS=postgres` the write lands where the page reads, at once.

AND THE READS OF THE TABLE FOLLOW IT

Every read of `app.revenue_goals` — `get_goals`, `get_smart_goals`, the
/marketing target line — goes to Postgres while this chain does, whatever
`KS_READ_GOALS` or `KS_READ_MARKETING` say, and never falls back to DuckDB
(`reads_the_chain` below). The first version left them on those two flags,
chain 8's arrangement, and made "both at `postgres`" a precondition of the
flip that only whoever flipped it could check. That precondition does not
hold for long: putting a read flag back is the documented rollback of every
read port, so one routine rollback of /goals or /marketing after the latch
would have shown the pre-flip goal from DuckDB for good, with nothing
reporting it. The flags still choose the engine for everything else those
pages read — revenue history, Gold, the forecast — which DuckDB keeps
current.

No fallback, because after the flip DuckDB's copy is not an older answer to
the same question but a frozen one: nothing writes it and the hourly replace
no longer runs. A Postgres failure on these reads raises, so the page fails
where it can be seen instead of showing a goal nobody typed last.

THE ONE WRITER EVERY CALLER GOES THROUGH

`set_goal` is the table's only writer: `POST /api/goals` calls it directly and
`DELETE /api/goals/{period}` reaches it through `reset_goal_to_auto`. The
routing therefore lives inside `GoalsMixin.set_goal`, where both arrive —
chain 8's reason for putting it in the repository rather than the route.

THE FLAG GATES BOTH ENDS

Under the flag the write comes here AND the hourly `replicate_operational`
stands down for `app.revenue_goals`, and the daily comparison with it. The
replica is a full replace out of DuckDB, so a write here without the
stand-down would be rolled back within the hour, out of a DuckDB that never
saw it, looking healthy in between. Both ask
`core.write_chains.stood_down_tables()`, so they cannot disagree.

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) — after the
connection is in hand and before the statement runs. From the first goal
typed here the flag cannot move the writes back to DuckDB; only
`scripts/chain_copy_back.py goals` can, and it carries the three rows back and
compares them at zero before it releases anything.

ONE CLOCK FOR BOTH STORES

`updated_at` is supplied by the caller — the web container's clock, the same
`datetime.now(DEFAULT_TZ)` DuckDB's branch stamps — never Postgres' `now()`.
It is the column the copy-back's handover orders two versions of one goal by
(`chain_transfer._shared_clock`), and a version DuckDB stamped against one
written here must be compared on one clock, not on two machines' idea of it.
The Postgres column has no default, so a writer that forgot it would leave
the handover nothing to order by; `core/pg_chain_invariants.py` watches for
that.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_GOALS"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_goals_write"

# The tables this chain writes. `core.write_chains` reads this, and through it
# the hourly shipper, the daily comparison and the copy-back.
CHAIN_TABLES: Tuple[str, ...] = ("app.revenue_goals",)

# The period types `set_goal` accepts. The repository refuses anything else
# before it gets here; restated so this writer cannot be the one that
# widens the table's domain.
PERIOD_TYPES: Tuple[str, ...] = ("daily", "weekly", "monthly")

# The hole every shared statement that reads this chain's table carries —
# `core.sql_dialect.render_tables` fills it with `revenue_goals` or
# `app.revenue_goals`. `reads_the_chain` recognises a goal read by it.
TABLE_HOLE = "{revenue_goals}"


def env_writes_postgres() -> bool:
    """What `KS_WRITE_GOALS` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule, and chain 8's: a
    typo in the variable deciding which store holds a number nobody else can
    reconstruct must not quietly pick the other store. Since DN-01 it stops
    this chain and nothing else.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def writes_postgres() -> bool:
    """Whether this chain writes Postgres — the one answer every caller reads.

    The latch outranks the flag (OD-19 (a)), and a value nobody can read: once
    a goal has been written here, setting the variable back must not start a
    second writer beside the first.
    """
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres()


def reads_postgres() -> bool:
    """Whether a read of this chain's table must ask Postgres, whatever the
    page's own read flag says — `writes_postgres()` applied to reads.

    One difference, and only for a flag nobody can read on a chain that has
    never written here: the writer then refuses in both stores and the hourly
    replace stands down, so neither copy moves, and the page's own flag
    chooses between them as it did before this chain existed. Raising here
    instead would take the page down over a variable about writes; the typo
    is published by `core.write_chains.chain_modes()` and paged by the
    canary. A latched chain never reaches the question.
    """
    if chain_latch.latched(CHAIN):
        return True
    try:
        return env_writes_postgres()
    except RuntimeError:
        return False


def reads_the_chain(sql: str) -> bool:
    """Whether this statement reads the chain's table while the chain owns it.

    Decided from the body, `_expenses_run`'s arrangement for `{expenses}`: a
    router asks this of every statement it is handed, so a query that starts
    reading `{revenue_goals}` tomorrow follows the chain the day it does,
    through whichever router carries it. `tests/unit/test_goals_reads_follow_chain.py`
    walks `core/` for every such statement and fails if its router does not ask.
    """
    return TABLE_HOLE in sql and reads_postgres()


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Raises if the marker cannot be written, and the write is then not
    attempted. **Called after the connection is in hand, never before it** —
    `core/pg_expenses_write.py._latch` has the reasoning: the latch is
    permanent, so taking it for a write that never reaches Postgres spends a
    rollback that is still available.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def set_goal(
    period_type: str,
    amount: float,
    is_custom: bool,
    calculated_goal: float,
    growth_factor: float,
    updated_at: datetime,
) -> Tuple[Any, ...]:
    """Write one goal; returns the stored row in `GOAL_COLUMNS` order.

    The caller builds its answer from what it passed, as the DuckDB branch
    does, so the response of `POST /api/goals` does not change with the
    engine; the row is returned for the callers that want to see what landed.
    """
    if period_type not in PERIOD_TYPES:
        raise ValueError(f"Invalid period_type: {period_type}")

    pool = await _pool()
    stamp = _latch()
    async with pool.acquire() as conn:
        # The owner row and the goal land together or neither does, so a claim
        # can never outlive the write that earned it.
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            # `ON CONFLICT (period_type)` — one row per period, rewritten in
            # place, as DuckDB's statement does. Spelled here rather than in a
            # module constant so the latch guard's walk, which reads each
            # writer's own statements, finds this one. `mirrored_at` is this
            # store's own bookkeeping (revision 0017), moved with every write
            # because it records when this store wrote the row.
            row = await conn.fetchrow(
                "INSERT INTO app.revenue_goals "
                "(period_type, goal_amount, is_custom, calculated_goal, "
                " growth_factor, updated_at, mirrored_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, now()) "
                "ON CONFLICT (period_type) DO UPDATE SET "
                "goal_amount = EXCLUDED.goal_amount, "
                "is_custom = EXCLUDED.is_custom, "
                "calculated_goal = EXCLUDED.calculated_goal, "
                "growth_factor = EXCLUDED.growth_factor, "
                "updated_at = EXCLUDED.updated_at, "
                "mirrored_at = EXCLUDED.mirrored_at "
                "RETURNING period_type, goal_amount, is_custom, "
                "calculated_goal, growth_factor, updated_at",
                period_type, amount, is_custom, calculated_goal,
                growth_factor, updated_at,
            )
    return tuple(row)
