"""Chain 11a: the sales report's send ledger, written to Postgres and shadowed in DuckDB.

`weekly_report_sends` is what keeps a daily-ticking job to one message a week:
six firings out of seven find their week recorded and go quiet. Lose a row
and every approved user gets the same report again; invent one and a week is
never reported. Nothing re-derives a row — it says a message went out.

THE SHAPE: SHADOW (owner decision OD-02 (c), 2026-10-01)

Under `KS_WRITE_WEEKLY_LEDGER=postgres` a delivery is recorded in Postgres
first and DuckDB is handed the same row right after (`core/shadow_writes.py`);
the hourly copy stands down and the daily comparison keeps comparing, facing
Postgres→DuckDB. The ledger behaviour around this module — the gate that also
counts a spooled week and adopts a row only DuckDB holds, the retried write
after delivery and its local spool (OD-16 (a): at-least-once, never a second
send) — lives in `core/report_ledger.py`, shared with chain 11b.

Two chains rather than one flag (the chain map: "two small steps"), because
the two reports deliver on their own days and either may be rolled back alone.
The first Postgres write latches the chain (OD-19 (a)); the way back is
`scripts/chain_copy_back.py weekly_ledger`.
"""
from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from core import chain_latch

WRITE_ENV = "KS_WRITE_WEEKLY_LEDGER"
CHAIN = "pg_weekly_ledger_write"
CHAIN_TABLES: Tuple[str, ...] = ("app.weekly_report_sends",)
CHAIN_SHADOW = True
TABLE = CHAIN_TABLES[0]


def report_sales_type() -> str:
    """The sales type the report records its weeks under — what the standing
    watch looks for (`chain_report_week_missing`)."""
    from core.scheduler import WEEKLY_REPORT_SALES_TYPE

    return WEEKLY_REPORT_SALES_TYPE


def first_week():
    """The earliest week the report may deliver, or None for no floor."""
    return None


def env_writes_postgres() -> bool:
    """What `KS_WRITE_WEEKLY_LEDGER` alone says; an unknown value raises
    (OD-18 (a)) — the gate then refuses, and nothing is sent."""
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def writes_postgres() -> bool:
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres()


def pending() -> Dict[str, Any]:
    """Delivered weeks whose row is still spooled — `/api/health` publishes
    it under this chain's entry and the canary pages `report_ledger_pending`.
    Local files, no I/O beyond the directory."""
    from core.report_ledger import pending_of

    return pending_of(CHAIN)


def _latch() -> str:
    """Inside `pool.acquire()`, never before it (`pg_expenses_write._latch`)."""
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def mark_sent(week_start: date, sales_type: str, revenue: Decimal,
                    orders: int, sent_at: datetime) -> None:
    """Record one delivery. `INSERT … ON CONFLICT DO UPDATE` — DuckDB's
    `INSERT OR REPLACE`. `sent_at` is the moment the message went out, carried
    by the caller through every retry and the spool."""
    from core.report_ledger import UPSERT_SQL, check_row

    check_row(week_start, sales_type, revenue, orders, sent_at)
    pool = await _pool()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.execute(UPSERT_SQL.format(table=TABLE),
                               week_start, sales_type, revenue, orders, sent_at)


async def find_sent(week_start: date, sales_type: str) -> Optional[Dict[str, Any]]:
    """The row Postgres holds for the week, or None. Reads only."""
    from core.report_ledger import FIND_SQL

    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(FIND_SQL.format(table=TABLE), week_start, sales_type)
    return dict(row) if row else None
