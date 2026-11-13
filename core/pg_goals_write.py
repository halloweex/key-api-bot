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
(`reads_the_chain`, until step 14's S14-11). The first version left them on those two flags,
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

**Since step 14's S14-05 the /marketing target line no longer asks
`reads_the_chain`.** `_marketing_run` has one path, Postgres, whatever
`KS_READ_MARKETING` says, and a failure there is the page's counted refusal
(`read_fallback.refuse("marketing", exc)`, a 503 naming `marketing`) rather
than a raw raise. `get_goals` and `get_smart_goals` asked it until S14-07
made `_goals_run` Postgres alone, and S14-11 deleted it.

THE ONE WRITER EVERY CALLER GOES THROUGH

`set_goal` is the table's only writer: `POST /api/goals` calls it directly and
`DELETE /api/goals/{period}` reaches it through `reset_goal_to_auto`. The
routing therefore lives inside `GoalsMixin.set_goal`, where both arrive —
chain 8's reason for putting it in the repository rather than the route.

THE FLAG GATED BOTH ENDS — UNTIL STEP 14

Under the flag the write came here AND the hourly `replicate_operational`
stood down for `app.revenue_goals`, and the daily comparison with it. The
replica is a full replace out of DuckDB, so a write here without the
stand-down would be rolled back within the hour, out of a DuckDB that never
saw it, looking healthy in between. Both asked
`core.write_chains.stood_down_tables()`, so they could not disagree; since
step 14's S14-11 both stand down on every chain's tables
(`write_chains.chain_tables()`), no flag read.

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) once a
connection has come out of the pool and before the transaction opens. From
the first goal typed here the flag cannot move the writes back to DuckDB; only
`scripts/chain_copy_back.py goals` could, carrying the three rows back and
comparing them at zero before it released anything (deleted in step 14's
S14-04; the way back since is an image from before step 14,
`core.rollback_lever`).

ONE CLOCK FOR BOTH STORES

`updated_at` is supplied by the caller — the web container's clock, the same
`datetime.now(DEFAULT_TZ)` DuckDB's branch stamps — never Postgres' `now()`.
It is the column the copy-back's handover orders two versions of one goal by
(`chain_transfer._shared_clock`; an image from before step 14 still orders
by it, since S14-04 deleted this build's), and a version DuckDB stamped against one
written here must be compared on one clock, not on two machines' idea of it.
The Postgres column has no default, so a writer that forgot it would leave
the handover nothing to order by; `core/pg_chain_invariants.py` watches for
that.

AND EVERY GOAL IS KEPT (revision 0036)

`ON CONFLICT (period_type)` overwrites, so until revision 0036 the goal in
force last month was gone the moment somebody typed this month's. Every write
now also appends the row it stored to `app.revenue_goal_history`, with what
the write was (`CHANGE_KINDS`): `set` for a goal typed on /goals,
`reset_to_auto` for the DELETE that stores the suggestion, `recalculated` for
a goal the service stores for itself — no caller does today: the Monday job
recalculates the seasonality tables and stores no goal. The capture is read
back from the row this transaction wrote, so the history holds what Postgres
kept, rounding included, and a period's latest version is the live row,
`updated_at` included (as its `set_at`).

Before the write overwrites the goal in force, that goal is appended too
when the history does not already end with it (`_GOAL_IN_FORCE_SQL`): the
goal stored before revision 0036 deployed, which the first write after it
would otherwise overwrite unrecorded and a goal never edited again would
never enter the history at all; one written while the captures were off, or
by an image at 0035 in a rollback's window; and one whose own version failed.
It says `set` for a custom goal and `reset_to_auto` for any other, with its
own `updated_at` as `set_at`.

It runs inside the goal's transaction under a SAVEPOINT of its own
(`core.analytics_captures.capture_in_transaction`): a goal the transaction
rolls back leaves no version, and a capture that fails rolls back alone,
stamps `capture.app.revenue_goal_history` in `meta.mirror_state` and leaves
the goal written — it never fails the write it rides on.
`KS_ANALYTICS_CAPTURES=off` skips it. A failure is a WARN in the 09:00 digest
(`capture_failed:app.revenue_goal_history`, owner decision D8 (b)); a goal is
set when a human sets one, so there is no stall to judge. When the goal in
force could not be kept, the new goal's version is not attempted either: its
ok stamp would clear the failure, and the next write keeps it as the goal in
force.

What the history does not hold yet is a goal in force that nobody has
written over since it was stored unrecorded — the goal stored before revision
0036, until the first write after it. So the goals in force over time are
`app.revenue_goal_history`, then the live row of `app.revenue_goals` where the
history does not end with it; the warehouse's `goal_history` reads both.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional, Tuple

from core import chain_latch
from core.analytics_captures import capture_in_transaction

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_GOALS"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_goals_write"

# The tables this chain writes. `core.write_chains` reads this, and through it
# the hourly shipper and the daily comparison (and the copy-back, until step
# 14's S14-04 deleted it).
CHAIN_TABLES: Tuple[str, ...] = ("app.revenue_goals",)

# The period types `set_goal` accepts. The repository refuses anything else
# before it gets here; restated so this writer cannot be the one that
# widens the table's domain.
PERIOD_TYPES: Tuple[str, ...] = ("daily", "weekly", "monthly")

# Revision 0036's capture: every goal written, in `CAPTURE_TABLE`, with what
# the write was. Not one of `CHAIN_TABLES` — a capture is declared in
# `core.analytics_captures.CAPTURES`, and claims no owner row.
CAPTURE_TABLE = "app.revenue_goal_history"
CHANGE_KINDS: Tuple[str, ...] = ("set", "recalculated", "reset_to_auto")

# The capture's statement: the row this transaction has just stored, read
# back, so the version is what Postgres kept and never what the caller meant
# to keep. A module constant, so the repository walk
# (`tests/unit/test_write_chains.py`, `TestTheCaptureTables`) finds this
# module as the table's one writer. Bounded by arithmetic: one row per goal a
# person writes or resets, tens a year; append-only, nothing deletes from it.
_GOAL_HISTORY_SQL = (
    "INSERT INTO app.revenue_goal_history "
    "(period_type, goal_amount, is_custom, calculated_goal, growth_factor, "
    " change_kind, set_at) "
    "SELECT period_type, goal_amount, is_custom, calculated_goal, "
    "growth_factor, $2, updated_at "
    "FROM app.revenue_goals WHERE period_type = $1"
)

# The goal in force, kept before the write overwrites it, when the period's
# latest version is not it: the goal stored before revision 0036 deployed,
# one written while the captures were off or by an image at 0035 during a
# rollback, or one whose own version failed. It says `set` for a custom goal
# and `reset_to_auto` for any other — the only two writes a stored goal can
# have come from — and its `set_at` is its own `updated_at`. Nothing when the
# history already ends with it, so it adds a row once per such goal: the same
# arithmetic as `_GOAL_HISTORY_SQL`'s.
_GOAL_IN_FORCE_SQL = (
    "INSERT INTO app.revenue_goal_history "
    "(period_type, goal_amount, is_custom, calculated_goal, growth_factor, "
    " change_kind, set_at) "
    "SELECT g.period_type, g.goal_amount, g.is_custom, g.calculated_goal, "
    "g.growth_factor, "
    "CASE WHEN g.is_custom THEN 'set' ELSE 'reset_to_auto' END, "
    "COALESCE(g.updated_at, now()) "
    "FROM app.revenue_goals g WHERE g.period_type = $1 "
    "AND NOT EXISTS ("
    "SELECT 1 FROM (SELECT * FROM app.revenue_goal_history h "
    "WHERE h.period_type = $1 ORDER BY h.id DESC LIMIT 1) l "
    "WHERE (l.goal_amount, l.is_custom, l.calculated_goal, l.growth_factor, l.set_at) "
    "IS NOT DISTINCT FROM "
    "(g.goal_amount, g.is_custom, g.calculated_goal, g.growth_factor, g.updated_at))"
)

# The hole every shared statement that reads this chain's table carries —
# `core.sql_dialect.render_tables` fills it with `revenue_goals` or
# `app.revenue_goals`. `tests/unit/test_goals_reads_follow_chain.py` walks
# every statement carrying it to a router that reads Postgres alone
# (`reads_the_chain` recognised a goal read by it until step 14's S14-11).
TABLE_HOLE = "{revenue_goals}"


# `env_writes_postgres`, `writes_postgres`, `reads_postgres` and
# `reads_the_chain` stood here until step 14's S14-11. S14-00 made the three
# answers constants — every goal is written and read in Postgres whatever
# `KS_WRITE_GOALS` says — S14-07 left no router that asked `reads_the_chain`,
# and S14-11 deleted them with the flag: `set_goal` writes here and asks
# nothing. `WRITE_ENV` stays a name (revision 0034's comments and an image
# from before step 14 read it), and the latch with it (decision 3).


# `NUMERIC(precision, scale)` of the columns a caller's number lands in —
# revision 0017's, and DuckDB's `DECIMAL`s beside them.
NUMERIC_COLUMNS = {
    "goal_amount": (12, 2),
    "calculated_goal": (12, 2),
    "growth_factor": (4, 2),
}


def _fits(column: str, value: Optional[float], *, nullable: bool) -> None:
    """Raise `ValueError` for a number Postgres would refuse in `column`.

    **Asked before the latch, because the latch is permanent.** A value
    Postgres rejects raises inside the write, after `_latch()` has written the
    marker: the chain is then latched for good, with no owner row behind it —
    a direction nothing files since step 14's S14-12 (the integrity layer's
    chain invariants judge only owner rows with no marker), and one an image
    from before step 14 pages as CRITICAL `chain_latch_disagrees` — for an
    input DuckDB would have refused without a trace. The
    route bounds `amount` only from below, so ₴10 000 000 000 is one typing
    slip away.

    Postgres' own rule lives in `core.pg_numeric.refusal`, which chain 4 asks
    of a buyer's loyalty figures too — one home, so the two chains cannot come
    to disagree about where a boundary lies.
    """
    from core.pg_numeric import refusal

    precision, scale = NUMERIC_COLUMNS[column]
    why = refusal(column, value, precision, scale, nullable=nullable)
    if why:
        raise ValueError(why)


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Raises if the marker cannot be written, and the write is then not
    attempted. **Called after the connection is in hand, never before it** —
    inside `pool.acquire()`, not merely after `_pool()`, since a wait for a
    connection can end without one (a refused reconnect after a restart, a
    cancellation). `core/pg_expenses_write.py._latch` has the reasoning:
    the latch is permanent, so taking it for a write that never reaches
    Postgres spends a rollback that is still available.
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
    change_kind: Optional[str] = None,
) -> Tuple[Any, ...]:
    """Write one goal; returns the stored row in `GOAL_COLUMNS` order.

    The caller builds its answer from what it passed, as the DuckDB branch
    does, so the response of `POST /api/goals` does not change with the
    engine; the row is returned for the callers that want to see what landed.

    `change_kind` is what the version in `app.revenue_goal_history` says the
    write was; left out, a custom goal is `set` and any other `recalculated`.
    The reset says `reset_to_auto` itself (`GoalsMixin.reset_goal_to_auto`).
    """
    if period_type not in PERIOD_TYPES:
        raise ValueError(f"Invalid period_type: {period_type}")
    kind = change_kind or ("set" if is_custom else "recalculated")
    if kind not in CHANGE_KINDS:
        # A caller's mistake, refused before the latch like every other
        # argument: the history must not gain a kind its readers do not know.
        raise ValueError(f"Invalid change_kind: {change_kind!r}")
    # Everything Postgres could refuse on input is refused here, before the
    # latch — see `_fits`. `is_custom` and `updated_at` have no default and
    # `core/pg_chain_invariants.py` pages on a NULL in either, so the writer
    # does not leave one.
    _fits("goal_amount", amount, nullable=False)
    _fits("calculated_goal", calculated_goal, nullable=True)
    _fits("growth_factor", growth_factor, nullable=True)
    if not isinstance(is_custom, bool):
        raise ValueError(f"is_custom={is_custom!r} is not a boolean")
    if updated_at is None:
        raise ValueError("updated_at must be supplied — it is the copy-back's clock")

    pool = await _pool()
    async with pool.acquire() as conn:
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        # The owner row and the goal land together or neither does, so a claim
        # can never outlive the write that earned it.
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            # Revision 0036: the goal in force first, before the upsert below
            # overwrites it, when the history does not already end with it.
            in_force_kept = await capture_in_transaction(
                conn, CAPTURE_TABLE, _GOAL_IN_FORCE_SQL, period_type)
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
            # Revision 0036: the version, under a savepoint of its own — a
            # capture that fails is rolled back and stamped alone, and the
            # goal above commits regardless. Not after a goal in force that
            # was not kept: its ok stamp would clear that failure, and this
            # goal is kept by the next write anyway, as the goal in force.
            if in_force_kept:
                await capture_in_transaction(
                    conn, CAPTURE_TABLE, _GOAL_HISTORY_SQL, period_type, kind)
    return tuple(row)
