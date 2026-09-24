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

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write here latches the chain (`core/chain_latch.py`, OD-19 (a)) once a
connection has come out of the pool and before the transaction opens. From the
first typed expense onward the flag cannot move these writes back to DuckDB —
only `scripts/chain_copy_back.py` can — because a flag flip after a row has
landed here starts a second writer rather than undoing anything.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any, List, Optional, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_EXPENSES"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_expenses_write"

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


def env_writes_postgres() -> bool:
    """What `KS_WRITE_EXPENSES` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule: a typo in the variable
    deciding which store holds money nobody else can reconstruct must stop the
    process, not quietly pick the other store. Since DN-01 it stops this chain
    and nothing else.
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

    The latch outranks the flag (OD-19 (a)): once an expense has been written
    to Postgres, setting the variable back to `duckdb` must not start a second
    writer beside the first. It also outranks a value nobody can read, so a
    latched chain keeps routing to the store that holds its rows while the typo
    is published, paged and stamped — see `core/chain_latch.py`.
    """
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres()


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Raises if the marker cannot be written, and the write is then not attempted:
    a row in Postgres that no marker records is how the next boot comes to
    believe DuckDB is still the writer.

    **Called after the connection is in hand, never before it.** The latch is
    permanent — only `scripts/chain_copy_back.py` releases it — so taking it for
    a write that never reaches Postgres spends the rollback this chain still
    has. Postgres being unreachable, a wait for a connection that ends without
    one (below) and `require_revision` raising because `web` came up ahead of
    `migrate` are all ordinary events, and today's production state
    (`KS_WRITE_EXPENSES=postgres` since 2026-09-17 08:33 UTC,
    `app.manual_expenses` at zero rows) is exactly the state one of them would
    have latched away with nothing written.

    "In hand" means out of the pool — inside `pool.acquire()`, not merely after
    `_pool()`. `core.pg.get_pool` sets no acquire timeout, so a pool with
    nothing free — the Sunday full sync holding all five — is a wait, not a
    failure, and the write lands once a connection frees. What ends the wait
    without one: the connection it is handed back has died — a Postgres
    restart closed it, a reset that failed on release terminated it, five idle
    minutes closed it — and the reconnect the acquire makes is refused; a
    cancellation; a closed pool. A latch taken before the acquire was on disk
    for the whole wait, so each of those, and a process stopped mid-wait, kept
    it with nothing written. Nothing in web cancels these writers today:
    `RequestTimeoutMiddleware` answers 504 and lets the handler run on
    (Starlette's `BaseHTTPMiddleware` runs the app in a task of its own), so an
    expense can land after its 504. This chain latched between the two until
    2026-09-25; chain 6a's review counted the acquire first. With a connection
    in hand the remaining window is the statement itself failing, which is the
    case the two copies are designed to report rather than prevent.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


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
        # Inside the acquire, not before it: a wait for a connection that ends
        # without one — a refused reconnect after a restart, a cancellation —
        # is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
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
        stamp = _latch()
        # A transaction where DuckDB's original needed none: the owner row and
        # the write it records land together or neither does, so a claim can
        # never outlive the statement that earned it.
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
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
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            row = await conn.fetchrow(
                "DELETE FROM app.manual_expenses WHERE id = $1 RETURNING id",
                expense_id,
            )
    return row is not None
