"""Chain 8: the money a human types, written to Postgres.

`manual_expenses` holds ad spend and other costs entered by hand — from the
/expenses form and from the chat tool. KeyCRM has never heard of these figures,
so no system can re-derive them: a row written nowhere is a row that never
existed. Production holds zero rows today (measured 2026-09-07 and again
2026-09-16), which is what made this the cheapest chain left to cut over — and
it stops being cheap the day the owner starts entering spend.

THE ONE WRITER EVERY CALLER GOES THROUGH

Three methods write the table: `add_expense`, `update_expense`,
`delete_expense`. `update_expense` has NO HTTP route — its only caller is
`core/chat_tools.py` — so a port that covered `web/routes/api/expenses.py`
would have left an editor still writing DuckDB. The routing therefore lives in
the repository methods themselves, which every caller reaches.

THE FLAG GATES BOTH ENDS

Under `KS_WRITE_EXPENSES=postgres` the three writes come here AND the hourly
`replicate_operational` stands down for `app.manual_expenses`. Inverting the
write without standing the shipper down would roll a typed amount back within
the hour, out of a frozen DuckDB, looking healthy in between — `replicate_sms`'
recorded failure. The daily comparison stands down with it, or every new row
would read as a discrepancy the check itself created. Both ask
`core.write_chains.stood_down_tables()`, so they cannot disagree.

A DELETION HAS TO STAY A DELETION

The replica is a full replace rather than an upsert precisely so that a
withdrawn spend disappears on both sides; an upsert would leave a ghost that
keeps dividing the ROAS, and no check would call that a discrepancy because the
row is present on both sides. Writing Postgres directly keeps a DELETE a DELETE.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_EXPENSES"

# The tables this chain writes. `core.write_chains` reads this, and through it
# both the hourly shipper and the daily comparison.
CHAIN_TABLES: Tuple[str, ...] = ("app.manual_expenses",)

# What `add_expense` returns and what `update_expense` returns — the column
# lists of the DuckDB statements, kept identical so the callers' dict-building
# does not change with the engine.
_ADD_RETURNING = ("id, expense_date, category, expense_type, amount, currency, "
                  "note, created_at, platform")
_UPDATE_RETURNING = ("id, expense_date, category, expense_type, amount, currency, "
                     "note, created_at, updated_at")

# The fields `update_expense` may set, in the order DuckDB's builder checks them.
_UPDATABLE = ("expense_date", "category", "expense_type", "amount", "currency", "note")


def writes_postgres() -> bool:
    """Whether this chain writes Postgres.

    An unrecognised value raises — `KS_BOT_STORE`'s rule: a typo in the variable
    deciding which store holds money nobody else can reconstruct must stop the
    process, not quietly pick the other store.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


async def _ensure_expense_id_floor(conn) -> None:
    """Put the allocator above every id already in the table.

    Revision 0031's `setval` runs against the table as it stood at migration
    time; an expense typed before the switch is shipped in afterwards, carrying
    DuckDB's id, and would collide with the first id the sequence hands out.
    `_ensure_movement_id_floor`'s arrangement exactly: idempotent, monotone (the
    greater of the sequence and MAX(id), so it never moves backwards), and run
    inside the writing transaction on every insert rather than once per process,
    because "once" is wrong across the cutover itself.
    """
    await conn.execute(
        "SELECT setval('app.manual_expenses_id_seq', GREATEST("
        "  (SELECT COALESCE(MAX(id), 0) FROM app.manual_expenses),"
        "  (SELECT last_value FROM app.manual_expenses_id_seq)"
        "), true)"
    )


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def add_expense(
    expense_date: date, category: str, expense_type: str, amount: float,
    currency: str, note: Optional[str], platform: Optional[str],
) -> Tuple[Any, ...]:
    """Insert one expense; returns the row DuckDB's `RETURNING` returned.

    `created_at` is supplied explicitly: DuckDB defaults it, and the Postgres
    column was built to receive it, so it has no default — omitting it would
    record every typed expense with no time at all.
    """
    pool = await _pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _ensure_expense_id_floor(conn)
            row = await conn.fetchrow(
                "INSERT INTO app.manual_expenses "
                "(expense_date, category, expense_type, amount, currency, note, "
                " platform, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, now()) "
                f"RETURNING {_ADD_RETURNING}",
                expense_date, category, expense_type, amount, currency, note, platform,
            )
    return tuple(row)


async def update_expense(expense_id: int, **fields: Any) -> Optional[Tuple[Any, ...]]:
    """Update the given fields of one expense; None if nothing to set or no row.

    Mirrors DuckDB's builder: a field passed as None is left alone, and with no
    fields at all nothing is written and None is returned.
    """
    sets: List[str] = []
    params: List[Any] = []
    for name in _UPDATABLE:
        value = fields.get(name)
        if value is not None:
            params.append(value)
            sets.append(f"{name} = ${len(params)}")
    if not sets:
        return None
    sets.append("updated_at = now()")
    params.append(expense_id)

    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE app.manual_expenses SET {', '.join(sets)} "
            f"WHERE id = ${len(params)} RETURNING {_UPDATE_RETURNING}",
            *params,
        )
    return tuple(row) if row else None


async def delete_expense(expense_id: int) -> bool:
    """Delete one expense. A deletion stays a deletion — see the module note."""
    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM app.manual_expenses WHERE id = $1 RETURNING id",
            expense_id,
        )
    return row is not None
