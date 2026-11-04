"""Chain 11b: the traffic report's send ledger, written to Postgres.

`traffic_report_sends` is what keeps a daily-ticking job to one message a week:
six firings out of seven find their week recorded and go quiet. Lose a row
and every approved user gets the same report again; invent one and a week is
never reported. Nothing re-derives a row — it says a message went out.

THE SHAPE: SHADOW, UNTIL STEP 14'S S14-10 (owner decision OD-02 (c), 2026-10-01)

Under `KS_WRITE_TRAFFIC_LEDGER=postgres` a delivery was recorded in Postgres
first and DuckDB was handed the same row right after (`core/shadow_writes.py`),
while the hourly copy stood down and the daily comparison compared the two,
facing Postgres→DuckDB. Since S14-00 the shadow stood down in web, which runs
`KS_DUCKDB=off`, and the comparison compared nothing (`compared_in_shadow()`
empty); S14-10 deleted both, and the row is Postgres's alone. The ledger
behaviour around this module — the gate that counts a spooled week as sent,
the retried write after delivery and its local spool (OD-16 (a):
at-least-once, never a second send) — lives in `core/report_ledger.py`,
shared with chain 11a.

Two chains rather than one flag (the chain map: "two small steps"), because
the two reports deliver on their own days and either may be rolled back alone.
The first Postgres write latches the chain (OD-19 (a)); the way back was
`scripts/chain_copy_back.py traffic_ledger` (deleted in step 14's S14-04;
the way back since is an image from before step 14, `core.rollback_lever`).
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from core import chain_latch

WRITE_ENV = "KS_WRITE_TRAFFIC_LEDGER"
CHAIN = "pg_traffic_ledger_write"
CHAIN_TABLES: Tuple[str, ...] = ("app.traffic_report_sends",)

TABLE = CHAIN_TABLES[0]


def report_sales_type() -> str:
    """The sales type the report records its weeks under — what the standing
    watch looks for (`chain_report_week_missing`)."""
    from core.traffic_report import TRAFFIC_SALES_TYPE

    return TRAFFIC_SALES_TYPE


def first_week():
    """The earliest week the report may deliver, or None for no floor."""
    from core.traffic_report import first_week

    return first_week()


# `env_writes_postgres`, `unmet_precondition` and `writes_postgres` stood
# here until step 14's S14-11, with `CHAIN_SHADOW` (the shape the registry
# published as `shadow` after S14-10 deleted the shadow itself) and
# `LOCKOUT_REVISION` (the revision a flip waited for — 0035, which
# `tests/unit/test_chain_lockout.py` keeps as a literal). S14-00 made the
# answers constants and S14-11 deleted them with the flag: the chain writes
# Postgres, a constant. `WRITE_ENV` stays a name (revision 0035's comments and
# an image from before step 14 read it).


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
