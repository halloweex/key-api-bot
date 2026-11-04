"""Reading the `/expenses` tab from Postgres.

Six read methods, and they split cleanly in two — which is why this port is
smaller than it looks.

**Three need nothing new.** `list_expenses`, `get_ad_spend_by_platform` and
`get_expenses_summary` read only `manual_expenses`, and revision 0018 already
put that here: the ROAS calculation on `/traffic` had to be able to join it.
Half the tab moves with no migration at all.

**Two needed the landing.** `get_expense_summary` and `get_profit_analysis`
read `expenses` joined to orders, plus the type dictionary for its names.
Revision 0020 mirrors both — they come from KeyCRM, so they are `bronze` and
mirrored, the opposite of `manual_expenses` sitting in `app` because nothing
can re-derive a number a human typed.

`get_expense_types` is the sixth and reads the dictionary alone.

NO FALLBACK SINCE STEP 14 (S14-07)

`/expenses` reads Postgres. The flag that chose the engine (`KS_READ_EXPENSES`)
chooses nothing now: `_expenses_run` asks Postgres and nothing else, and a
failure — a lost DSN line included — is refused as a 503 naming `expenses`
(`read_fallback.refuse`), which is what `KS_READ_FALLBACK=off` made of it since
2026-10-09. So is a statement that reads `{expenses}` while `backfilled()` says
the history is not across: a NULL `meta.mirror_state.backfilled_at` leaves no
engine that could answer it. The paragraphs below are the history of the port.

WHAT THE PORT CHANGED IN BOTH ENGINES

The two summary methods joined `expenses` to *raw* `orders` and then narrowed
by `sales_type` with an `EXISTS` against `silver_orders`. The only facts they
took from the order were its Kyiv date and its source — and `silver.orders`
carries both as columns, plus `sales_type` itself. So the shared body reads
Silver directly and the EXISTS is gone: one join instead of two, the same
answer, and one fewer place where a date is converted at read time rather than
being stored converted.

Sound because Silver covers every order — 46,272 of 46,272, verified
2026-08-20 — so nothing is excluded that the old join admitted, and because
`_build_sales_type_filter` was already reading Silver rather than re-deriving
the classification from `manager_id`. That subquery had been the fix for a
real defect (#101 gave source 5 its own `sales_type` while the filter, knowing
only about managers, still counted those 176 orders as retail); reading the
column directly is the same fix taken one step further.

WHY IT FELL BACK, UNTIL S14-07

Nothing diverges: the reads write nothing, and every table is one Postgres
receives from KeyCRM in the same call DuckDB does or replicates from what
DuckDB holds. A fault costs the engine rather than the page —
`core/pg_inventory_read.py`'s answer.

The writes are a different matter and stay on DuckDB. `add_expense`,
`update_expense` and `delete_expense` write `manual_expenses` there, and the
hourly `replicate_operational` carries the result. That lag was harmless while
nothing read the copy; with this flag on, a human who types an expense and
looks at the page would not see it. So the three write paths replicate
immediately, the way an admin UTM reparse ships immediately — see
`core/repositories/expenses.py`.
"""
from __future__ import annotations

import logging
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


# Whether `bronze.expenses` has had its history carried across. A one-way
# latch: `meta.mirror_state.backfilled_at` is only ever set, never cleared, so
# once true it stays true and costs nothing thereafter.
_backfilled: bool = False
_checked_at: float = 0.0
_RECHECK_S = 60.0
# The failure the last check ended in, or None. While the recheck window
# holds, every call answers the question that check could not, so every one
# of them is refused because of it — and is counted as such.
_check_failed: "BaseException | None" = None


async def backfilled() -> bool:
    """Whether the mirrored expenses hold history, not just the last sync.

    WHY A READ PATH ASKS THIS AT ALL

    Because the alternative failed in production on 2026-09-07. The flag went
    on while Postgres held 65 of 15,020 expenses, and the tab answered from it
    without a murmur: an empty result is not an exception, so nothing fell back
    and nothing was logged. The summary showed almost no costs and the profit
    analysis showed almost pure profit.

    "Run the backfill first" had been written down, said out loud, and put in
    the finding's own text. None of that is a mechanism. `backfilled_at` is the
    fact, and a read that depends on history can simply ask for it.

    Only the two statements that touch `{expenses}` are gated —
    `_expenses_run` decides by looking at the body — so the three methods that
    read `manual_expenses` are unaffected, and so is the type dictionary, which
    is re-shipped whole every sync and has no history to be missing.

    Cheap: false is re-checked at most once a minute, and true is remembered
    for the life of the process.

    A watermark it cannot read is refused here, a 503 naming `expenses`: this
    read is not going to succeed either. Until step 14's S14-07 that was
    `read_fallback.fall_back`, which refused under `KS_READ_FALLBACK=off`;
    it is `refuse` now, the same count, line and cause.
    """
    global _backfilled, _checked_at, _check_failed
    import time as _time

    from core import read_fallback

    if _backfilled:
        return True
    now = _time.monotonic()
    if now - _checked_at < _RECHECK_S:
        if _check_failed is not None:
            raise read_fallback.refuse("expenses", _check_failed)
        return False
    _checked_at = now

    try:
        from core.pg import get_pool

        pool = await get_pool()
        async with pool.acquire() as conn:
            # The table's name written out (step 14, S14-07), not imported
            # from `core.pg_expense_backfill`, which goes with the backfills
            # in S14-13: an import failing here would be refused like any
            # other failure, and /expenses would answer 503 on every start.
            done = await conn.fetchval(
                "SELECT backfilled_at IS NOT NULL FROM meta.mirror_state "
                "WHERE table_name = $1",
                "bronze.expenses",
            )
    except Exception as exc:  # noqa: BLE001 — every failure is the refusal
        # It used to log under words of its own ("cannot read the backfill
        # watermark") that no grep for "falling back to DuckDB" could find
        # (DN-20a); then it was a counted fallback; now it is the refusal.
        _check_failed = exc
        raise read_fallback.refuse("expenses", exc)

    _check_failed = None
    _backfilled = bool(done)
    if not _backfilled:
        logger.warning(
            "expenses: bronze.expenses has no completed backfill, so history "
            "is missing — refused. The hourly ids-diff sets this on its own.")
    return _backfilled


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run one expense query against Postgres and return plain tuples.

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
