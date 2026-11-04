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
store that is actually written — and, until the first full sync under the flag
had written it there, DuckDB's last stamp (`CHAIN_WATERMARK_INHERITS_DUCKDB`,
until step 14's S14-09).

FAILURE POLICY: THIS ONE RAISES

Like chains 1 and 8, and unlike the mirrors: under the flag this IS the write,
and a failure swallowed here would be a full sync reporting a dictionary it
never stored. What the caller does with the raise is the caller's decision —
`full_sync` records it, leaves the watermark where it was and carries on with
products and orders, because one dictionary must not cost a week's orders
(DN-01's rule: a chain's fault stops that chain and nothing else).

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) once a
connection has come out of the pool and before the transaction opens. From the
first dictionary written here, only `scripts/chain_copy_back.py` could move the
writes back to DuckDB (deleted in step 14's S14-04; the way back since is an
image from before step 14, `core.rollback_lever`).

"Once a connection has come out of the pool", and not merely once the pool is
in hand. The acquire does not time out — `core.pg.get_pool` sets no acquire
timeout, so the Sunday full sync this runs inside holding all five connections
is a wait — but the wait can end without a connection: a Postgres restart
during it leaves a reconnect that is refused, and a cancellation ends it too
(`pg_expenses_write._latch`). A latch taken before it would move the chain with
nothing written, leaving the copy-back as the only way back (since step
14's S14-04 deleted it, a rollback to an image from before step 14) and
`chain_latch_disagrees` (CRITICAL) the next morning. Chains
1, 8 and 7a were written before that was counted and latched between the two
until 2026-09-25; every chain now latches inside the acquire, and
`tests/unit/test_chain_latch.py` holds each registered chain to it.

A PRECONDITION THE FLAG ENFORCED — UNTIL STEP 14

Everything that reads the dictionary reads it through `_expenses_run`, which
asked Postgres only under `KS_READ_EXPENSES=postgres`, so until step 14's
S14-00 `unmet_precondition()` held an unlatched chain on DuckDB until that
read flag was on (DN-27's rule for the buyers chain). S14-00 made the answer a
constant and S14-07 made the reader Postgres alone; S14-09 deleted the
answer, with the routing it fed: the repository's writer and `full_sync` ask
nothing. Kept: `WRITE_ENV` as a name (the registry still publishes the
variable until S14-11, through `env_writes_postgres`), the latch and its owner
row (decision 3: an image from before step 14 routes by them), and
`writes_postgres()`, a constant the registry's sync-key routing asks until
S14-11.
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
# the hourly shipper and the daily comparison (and the copy-back, until step
# 14's S14-04 deleted it).
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

# `CHAIN_WATERMARK_INHERITS_DUCKDB` stood here until step 14's S14-09: until
# the first full sync under the flag wrote the key to `meta.chain_watermarks`,
# `_freshness_check` judged DuckDB's frozen stamp instead of calling the
# dictionary never synced — a mid-week flip would otherwise have filed
# `freshness_expense_types` on every integrity run until Sunday. The flip is
# long past and production holds the key (H6, 2026-10-29); nothing in the sync
# reads it back, so an absent key would only be "never synced", which is then
# true (`tests/unit/test_absent_chain_keys.py`).


def env_writes_postgres() -> bool:
    """What `KS_WRITE_EXPENSE_TYPES` alone says. Routes nothing: since step
    14's S14-00 the chain writes Postgres whatever its flag says, and since
    S14-09 neither the repository's writer nor `full_sync` reads the flag.

    An unrecognised value raises — `KS_BOT_STORE`'s rule — and the registry
    publishes the raise as the chain's `error`, which a rollback to an image
    from before step 14 would route by (`core.write_chains._chain_state`).
    Last consumer: that registry entry — S14-11.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


def writes_postgres() -> bool:
    """Whether this chain writes Postgres: True, a constant since step 14's
    S14-00 — neither the flag, nor the latch, nor a precondition routes. The
    first write in a process still takes the latch (decision 3), which the
    images before step 14 route by. Last consumer: the store's sync-key
    getter and setter, which ask it of the chain that declares the key
    through the registry (`DuckDBStore.get_last_sync_time`) — S14-11.
    Nothing in this build asks it of this chain by name since S14-09."""
    return True


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Called with a connection already acquired, after `require_revision()` has
    passed — `pg_expenses_write._latch` has the reason: the latch is permanent,
    and a write that never reaches Postgres must not spend the rollback.
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
    async with pool.acquire() as conn:
        # Inside the acquire, not before it: the full sync this runs in holds
        # the pool, so this is where it waits, and a wait can end without a
        # connection (module docstring).
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.executemany(sql, [tuple(r) for r in rows])
    return len(rows)
