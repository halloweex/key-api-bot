"""Chain 6a: the expense-type dictionary, written to Postgres.

`bronze.expense_types` is the 27-row dictionary that names every order-level
cost on /expenses. KeyCRM serves it only to the weekly full sync, so until now
DuckDB was its SOURCE for Postgres: `upsert_expense_types` wrote DuckDB and the
hourly `replicate_operational` full-replaced the Postgres copy out of it
(`core/pg_operational.py`, the `EXPENSE_TYPES_TABLE` entry). That makes it the
one catalogue table in exactly chain 8's shape — one writer, one replicated
copy — and the cheapest of chain 6 to move: everything in it is re-fetchable
from KeyCRM, so the worst a mistake costs is a week of wrong names.

THE ONE WRITER, AND THE ONE PARSE

`ExpensesMixin.upsert_expense_types` is the only statement that writes this
dictionary, reached only from `SyncService.full_sync`. The routing lives in
that repository method, chain 8's arrangement, and it routes the rows AFTER
`core.landing_rows.expense_type_rows` has turned them into rows — so both
stores are handed the same resolved names. KeyCRM serves some names as
localisation keys (`dictionaries.expense_types.delivery`); a second copy of
that rule here would be two stores disagreeing about what an expense is
CALLED, with no comparison left to say which is right once the chain moves.

An upsert, never a delete, because DuckDB's `INSERT OR REPLACE` never deletes
either: a type KeyCRM stops serving stays named, so an old expense that
carries it does not fall into "Other".

THE FLAG GATES BOTH ENDS, AND THE WATERMARK

Under `KS_WRITE_EXPENSE_TYPES=postgres` the write comes here, and the hourly
`replicate_operational` and the daily `reconcile_operational` both stand down
for `bronze.expense_types` — `core.write_chains.stood_down_tables()` says so
to both. Invert the write without that and DuckDB overwrites Postgres once an
hour out of a dictionary it no longer receives: `replicate_sms`' recorded
failure of 2026-09-06, exactly. `last_sync_expense_types` moves with the chain
to `meta.chain_watermarks` (revision 0032), so the freshness check judges the
store that is actually written.

FAILURE POLICY: THIS ONE RAISES

Like chains 1 and 8, and unlike the mirrors: under the flag this IS the write,
and a failure swallowed here would be a full sync reporting a dictionary it
never stored. What the caller does with the raise is the caller's decision —
`full_sync` records it, leaves the watermark where it was and carries on with
products and orders, because one dictionary must not cost a week's orders
(DN-01's rule: a chain's fault stops that chain and nothing else).

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) after the
connection is in hand and before the statement runs. From the first dictionary
written here, only `scripts/chain_copy_back.py` can move the writes back to
DuckDB.

A PRECONDITION THE FLAG DOES NOT ENFORCE

Everything that reads the dictionary reads it through `_expenses_run`, which
asks Postgres only under `KS_READ_EXPENSES=postgres`. With that flag off, the
page reads DuckDB's copy — frozen from the switch — so a type KeyCRM adds
afterwards shows as "Other". Chain 8 made the same assumption and does not
enforce it either; turn the read flag on first.
"""
from __future__ import annotations

import logging
import os
from typing import List, Optional, Tuple

from core import chain_latch
from core.landing_rows import EXPENSE_TYPE_COLUMNS, ExpenseTypeRow

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_EXPENSE_TYPES"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_expense_types_write"

# The table this chain writes. `core.write_chains` reads this, and through it
# the hourly shipper, the daily comparison and the copy-back.
CHAIN_TABLES: Tuple[str, ...] = ("bronze.expense_types",)

# The one `sync_metadata` key that moves with it — what `full_sync` stamps
# after the dictionary lands. The store's getter and setter route it through
# `core.write_chains.chain_for_sync_key` to `meta.chain_watermarks`.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_expense_types",)

# How stale that watermark may be before `core.pg_chain_invariants` calls the
# chain stalled — None: not judged there at all. Its 90 minutes are
# arithmetic about the HOURLY stock sync; this key moves once a week, on the
# Sunday full sync, so any limit shorter than a week would page every run.
# `_freshness_check` already judges this entity at 192 h and reads the moved
# key from `meta.chain_watermarks`, and saying the same stall twice under two
# names would make one page read as two problems.
CHAIN_WATERMARK_MAX_AGE_MIN: Optional[int] = None


def env_writes_postgres() -> bool:
    """What `KS_WRITE_EXPENSE_TYPES` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule. Since DN-01 that
    stops this chain and nothing else: the registry stands it down, and
    `full_sync` contains the raise.
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

    The latch outranks the flag (OD-19 (a)), and outranks a value nobody can
    read. See `core/chain_latch.py`.
    """
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres()


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Called after the pool is in hand and `require_revision()` has passed —
    `pg_expenses_write._latch` has the reason: the latch is permanent, and a
    write that never reaches Postgres must not spend the rollback.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def upsert_expense_types(rows: List[ExpenseTypeRow]) -> int:
    """The dictionary, from the rows `core.landing_rows` parsed. Raises.

    One transaction: 27 rows land together or none do, so a failure leaves the
    names the page had rather than half of a new set.
    """
    if not rows:
        return 0
    columns = EXPENSE_TYPE_COLUMNS
    # Spelled in the function rather than hoisted to a constant, so the latch
    # guard in tests/unit/test_chain_latch.py sees a writer here — it finds
    # writers by the statements they carry. The columns are the shared
    # parse's, never restated. `mirrored_at` is Postgres's own bookkeeping
    # (revision 0020) and is stamped on update as well as insert, which is
    # what DuckDB's `INSERT OR REPLACE ... CURRENT_TIMESTAMP` does to
    # `synced_at`.
    sql = (
        f"INSERT INTO bronze.expense_types ({', '.join(columns)}, mirrored_at) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(columns) + 1))}, now()) "
        "ON CONFLICT (id) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")
        + ", mirrored_at = EXCLUDED.mirrored_at"
    )
    pool = await _pool()
    stamp = _latch()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.executemany(sql, [tuple(r) for r in rows])
    return len(rows)
