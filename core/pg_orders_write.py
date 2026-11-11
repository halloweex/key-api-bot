"""Chain 3: the orders, their line items, their expenses and the misses, in Postgres.

Every page reads the orders, and until this chain DuckDB was where they landed:
`DuckDBStore.upsert_orders` decided which rows to write, wrote them, and the
mirror (`core.pg_landing.mirror_orders`) shipped exactly those rows to Postgres;
`upsert_expenses_batch` and `mirror_expenses` did the same for the order-level
costs; `record_backfill_misses` remembered in DuckDB the ids KeyCRM could not
supply, and the hourly `replicate_operational` copied that ledger across.
Under `KS_WRITE_ORDERS=postgres` all four tables were written here and DuckDB's
copies froze; since step 14 they are written here alone, whatever the flag
says (PRECONDITIONS AND ROUTING, below).

OWNER DECISION OD-13 (a), 2026-10-01: A RAISING WRITER

There is no DuckDB fallback when Postgres is down. Under the chain a write
here IS the write: a failure raises, the sync's order step records it and
holds `last_sync_orders` where it was (`SyncService._orders_step_postgres`),
and the next tick re-fetches the same 24-hour window. Nothing is lost while
Postgres is away — KeyCRM still holds every order — and nothing lands in a
store nobody reads. The way back was `scripts/chain_copy_back.py orders`
(deleted in step 14's S14-04; the way back since is an image from before
step 14, `core.rollback_lever`), and the flip waited for evidence that
Postgres itself can be got back (`unmet_precondition`).

WHAT MOVES TOGETHER, AND WHY IN ONE TRANSACTION

- `bronze.orders` and `bronze.order_products` are one unit (DN-22a): a chain
  that takes the headers takes their line items, and they were never written
  in two transactions in Postgres.
- `bronze.expenses` comes from the same payload (`include=…,expenses`) at the
  same call site, so a tick's orders and their costs land together or neither
  does. `/expenses`' profit analysis joins the two; split across stores, one
  tick could land a header in one and its costs in the other.
- `app.order_backfill_misses` moves because the repair SELECTIONS move with
  the writes (`core.pg_orders_read`): the gap scan and the half-written scan
  read it with a 30-day `NOT EXISTS`, and that is the only thing stopping both
  repairs re-asking KeyCRM for the same unfetchable ids for ever. Read where
  the chain no longer writes it, it stops nothing.

`app.order_versions` is not a chain table. It has no DuckDB copy and no
shipper out of DuckDB, and its one writer — `pg_order_versions.capture_versions`
inside `pg_landing._write_order_rows` — is the same in both modes. So the
archive goes on across a flip and across a rollback with nothing to copy.
`bronze.expense_types` is chain 6a's.

THE ONE WRITER OF THE ORDER TABLES STAYS ONE WRITER

The rows come from `core.landing_rows`, the parse DuckDB was handed too until
step 14's S14-09 deleted its order writer, and are written by
`pg_landing._write_order_rows` and `pg_landing._write_rows` — the statements
every DuckDB-to-Postgres shipment runs. So the version is captured in the
row's own transaction, the `bronze.orders` watermark the canary's
`mirror_stale` reads keeps moving, and `order_versions_stalled` keeps meaning
what it says (DN-22b; `tests/unit/test_write_chains.py` walks for a second
writer). What only the chain does lives here: the decision, the latch
and its owner rows, and what to do with an order Postgres would refuse.

THE DECIDER READ IS THE WRITE DECISION

DuckDB decided INSERT, UPDATE, skip-if-unchanged and the header-only refresh's
deferral from one read of `(id, updated_at)` under its store lock. Here the
same `core.upsert_decider.should_update_order` decides from the same read,
taken `FOR UPDATE` inside the writing transaction — Postgres has no store lock,
only the transaction. Every caller already holds the scheduler's heavy lock,
so the row lock is defence in depth; `ORDER BY id` gives two writers one lock
order.

FAILURE POLICY

`upsert_orders_with_expenses` raises. Two things it does not raise for:

- **an order or expense Postgres would refuse** — NUL in text, a number over
  `NUMERIC(12,2)`, an id outside INTEGER, a NULL where the table says NOT
  NULL. Refused before the latch (`_refusal`) and counted in
  `UpsertResult.failed`, as DuckDB's "Skipped N poisoned orders" did; the 24-h
  window offers it again, and `reconciliation_pg` names it MISSING next
  morning;
- **a value it could not foresee** — a `DataError` from the batch rolls the
  transaction back and the batch is retried order by order on the same
  connection, each with its own claim, so one order cannot cost the other 499.

Connection errors, timeouts and cancellation raise at once.

EXPENSES FOLLOW KEYCRM (owner decision O2 (a), 2026-11-11)

Until then an order's expenses were upserted and never deleted, and every
expense in the payload was rewritten on every tick. So a cost KeyCRM removed
stayed in `bronze.expenses` for ever — in /expenses' costs and its profit —
and every tick of the 24-hour window rewrote every expense in it, so no tick
that carried one was ever idle. Now, for a payload carrying an `expenses` key
that holds a list, and for an order that is not refused:

- the rows of `bronze.expenses` under that order whose ids the payload no
  longer lists are deleted — all of them for an empty list — in the
  transaction that writes the order, after the owner claim, each logged at
  INFO with its id and amount and counted in `UpsertResult.expenses_deleted`.
  An id listed anywhere in the batch is never deleted (an expense KeyCRM moved
  to another order is rewritten under it instead). A payload without the key,
  or with something that is not a list under it, deletes nothing;
- an expense is written only when it is new or differs from the stored row in
  a written column, so a batch whose orders are in their state and whose
  expenses are already stored as listed reads that before the latch and
  returns, as an idle tick of orders always did; and `meta.mirror_state`'s
  `bronze.expenses` row is stamped only by a tick that wrote or deleted one.

The returned expense count keeps its meaning: the expenses in their state,
written or already correct, as `count` is for orders. Silver reads no
expenses, so the derivation mark stays `_write_order_rows`' alone.

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) inside
`pool.acquire()` and claims the four owner rows inside the writing
transaction, so the owner row shares an `xmin` with the row it was taken for.
From the first order written here only `scripts/chain_copy_back.py orders`
could move the writes back to DuckDB (deleted in step 14's S14-04; the way
back since is an image from before step 14, `core.rollback_lever`). A batch
with nothing to write — every order already in its state, every expense
already stored as listed and none to delete — reads that on the same
connection before the latch and returns, so an idle tick latches nothing.

PRECONDITIONS AND ROUTING — UNTIL STEP 14

Until step 14's S14-00 `KS_WRITE_ORDERS=postgres` moved the writes only once
the lock-out revision, step 13, chain 1, the backup evidence and no delivered
page under a retired landing check all held, and a process that once found
one unmet stayed held until it ended. S14-00 made the answers constants; S14-09
deleted them, with the routing they fed: the sync's order step, the store's
selections and the boot ask nothing, and DuckDB's order writers are gone. The
backup evidence is judged by the canary (`backup_evidence_stale`). Kept:
`WRITE_ENV` as a name (revision 0035's comments name it, and an image from
before step 14 reads it) and the latch and its owner rows (decision 3: that
image routes by them). `env_writes_postgres` and `writes_postgres()`, which
the registry read until then, went in S14-11 with the flag.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)


# What an order upsert did — the shape `upsert_orders_with_expenses` returns
# and `SyncService._upsert_orders_with_expenses` reads. It was DuckDB's
# (`core.duckdb_store`) until step 14's S14-01 moved it to its one remaining
# producer, unchanged; `core.duckdb_store` re-exported it until S14-18.
@dataclass(frozen=True)
class UpsertResult:
    """What an order upsert actually did.

    `count` used to be the whole return, and it counts a row that was already
    in the desired state as a success — correct for "did this work", useless
    for "what changed". The sync loop consumed it as the latter and marked the
    entire fetched window dirty on every cycle, which is how a warehouse with
    no new data rebuilt 583 dates every two minutes. `changed_ids` is the set
    of rows actually written, and it is what callers should drive work from.
    """

    count: int                  # rows in the desired state (written or already correct)
    changed_ids: List[int]      # rows this call actually wrote
    skipped_unchanged: int      # rows already in the desired state
    failed: int                 # rows rejected by a constraint/transaction error
    deferred_to_full_sync: int = 0  # unknown ids a header-only refresh left for a full writer
    # `(order id, reason)` for every refused order whose id is an integer —
    # unparseable, refused before the latch, or refused by Postgres. `failed`
    # counts these and any payload with no usable id. The repairs read it to
    # ledger a refused id rather than ask KeyCRM for it again (L1-10).
    refused: Tuple[Tuple[int, str], ...] = ()
    # Of `failed`, the payloads the parse could not read at all (a header, a
    # line or an expense), whatever their id. The order step fails on a batch
    # that landed nothing and holds any of these (`OrdersRefused`, AR-02).
    unparseable: int = 0
    # Order expenses deleted because the order's payload no longer lists them
    # (owner decision O2 (a), "EXPENSES FOLLOW KEYCRM" in the module
    # docstring). Each is logged at INFO with its id and amount.
    expenses_deleted: int = 0

    def __len__(self) -> int:
        return self.count


WRITE_ENV = "KS_WRITE_ORDERS"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_orders_write"

# The tables this chain writes (module docstring). `core.write_chains` reads
# this, and through it the hourly shipper (the misses), the backfills and the
# daily comparisons — and, until step 14 took them, the sync's orders and
# expenses mirrors (their last callers went in S14-09) and the copy-back
# (deleted in S14-04). The two order tables are `pg_landing.ORDER_UNIT` and
# pass as one.
CHAIN_TABLES: Tuple[str, ...] = (
    "bronze.orders", "bronze.order_products", "bronze.expenses",
    "app.order_backfill_misses")

# This chain's own writer stamps `meta.mirror_state.last_ok_at` on the three
# bronze tables (`pg_landing._write_order_rows` and `_write_rows` run the
# mirror's statement), and the canary's `mirror_stale:bronze.orders` and
# `order_versions_stalled` keep their meaning only because they do (module
# docstring). It declared them (`CHAIN_STAMPS_MIRROR_STATE`) for the daily
# comparison's `chain_shipper_overwrote`, which step 14's S14-12 deleted.

# The `sync_metadata` keys that move with it. The store's getter and setter
# route both through `core.write_chains.chain_for_sync_key` to
# `meta.chain_watermarks`, so the monotonicity guard reads the row it writes;
# the copy-back carried both into DuckDB and its release deleted both, until
# step 14's S14-04 deleted it.
#
# `last_sync_orders` — what the incremental sync stamps after a batch lands
# and the full sync stamps from `MAX(updated_at)`.
#
# `last_sync_meilisearch_pg` — the cursor of the search index built from
# Postgres (`core.pg_search_index_read.WATERMARK_KEY`), owner decision OD-15:
# its value is `MAX(mirrored_at)` over orders, buyers and products, a Postgres
# clock, and it moves with whichever of chains 3 and 6 lands last, which by
# the sequencing is this one. Absent from Postgres at the flip, DuckDB's
# frozen cursor stands in (`CHAIN_WATERMARK_INHERITS_DUCKDB` below), so the
# first index tick after the flip goes on from where the last one stopped
# instead of re-indexing everything. Neither `_freshness_check` nor the
# standing watch judges it: it is in no `FRESHNESS_THRESHOLDS` entry, and
# `CHAIN_WATERMARK_MAX_AGE_MIN` is None. `last_sync_meilisearch`, the cursor
# of the index built from DuckDB, was a DuckDB clock and stayed DuckDB's,
# never a chain's; it went with that index's frames in step 14's S14-07, and
# nothing reads it now (history).
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_orders", "last_sync_meilisearch_pg")

# Not judged by `core.pg_chain_invariants`: `last_sync_orders` holds KeyCRM's
# newest `updated_at`, not a sync time, and stands still overnight by design,
# and the search cursor stands still whenever nothing is mirrored.
# Freshness stays with `freshness_orders` — which judges when the step last
# wrote the row, never this value (`data_quality.JUDGED_BY_LAST_WRITE`) —
# and the order step's own health.
CHAIN_WATERMARK_MAX_AGE_MIN: Optional[int] = None
# Absent from Postgres until the first tick after the flip writes it, and
# `last_sync_orders` is where the order step's window STARTS. Read as absent
# it was "an hour ago", so the first tick fetched from 25 h back and every
# order KeyCRM updated between DuckDB's last stamp and then was never fetched
# by the incremental sync (the chain-3 review: a three-day-old DuckDB stamp
# skipped two days). Declared, the store's getter answers DuckDB's frozen
# stamp for the absent key (`DuckDBStore.get_last_sync_time`) — read, since
# `KS_DUCKDB=off`, from its copy in Postgres, `app.sync_metadata`
# (`core.pg_frozen_metadata`) — and `_freshness_check` judges the same value:
# one stand-in for both. For the search cursor the same stand-in is what keeps
# the first index tick after the flip incremental.
#
# KEPT IN STEP 14's S14-09, which removed it from chains 6 and 6a. Production
# holds both keys in `meta.chain_watermarks` (H6, 2026-10-29), so nothing
# reads the stand-in today; but a key that went missing — a row deleted by
# hand, a restore — would otherwise open the 25-hour window again for orders,
# and a full re-index for the cursor. Chains 6 and 6a lose nothing by an
# absent key: the hourly products step syncs at once, and the categories and
# expense types are never read back by the sync. S14-11 moves the stand-in
# into the getter for every chain key (`tests/unit/test_absent_chain_keys.py`).
CHAIN_WATERMARK_INHERITS_DUCKDB = True

# Per statement, inside every transaction here (DN-05a: the server is asked to
# give up, the client never cancels a statement in flight).
STATEMENT_TIMEOUT = "60s"

# How long an acquire may wait. The pool sets none (`core.pg.get_pool`), and
# the order step holds the heavy-job lock while it writes.
ACQUIRE_TIMEOUT_S = 10


# `env_writes_postgres` stood here until step 14's S14-11: S14-09 left it for
# the registry alone — `error` and `mismatch` in `/api/health` — and S14-11
# deleted it with the flag and that reader. `WRITE_ENV` stays a name
# (revision 0035's comments and an image from before step 14 read it).


# ─── What the chain's move stood down — constants since S14-00 ─────────────
#
# Until step 14's S14-00 the flag moved the writes only with the lock-out
# revision, step 13 and chain 1 in force, no delivered page under a check the
# flip retires, and the backup evidence fresh (OD-13 (a)). Production met all
# of them; their checks went with them, and S14-09 deleted the answers that
# stood in. The backup evidence is judged by the canary instead
# (`backup_evidence_stale`), so a drill that silently stopped running is still
# seen. What the move stood down stays named below until its readers go.


# `landing_checks_stood_down()` and `retired_conditions()` stood here until
# step 14's S14-14: the ten DuckDB integrity checks over the order tables that
# did not run once this chain had moved, and the conditions only they
# reported. S14-14 deleted the checks; the names are not reused while an image
# from before step 14 is the rollback target
# (`tests/unit/test_rollback_condition_names.py`, rule 15). DN-23's Postgres
# twins in `core/pg_warehouse_dq.py` are the checks of those tables.


# The DuckDB arm of `dq_reconciliation` paged under `dq:reconciliation`
# (`RECONCILIATION_GROUP`), and its layer `reconciliation` was neither written,
# paged on, caught up nor digested since the chain moved
# (`STOOD_DOWN_LAYERS`, `stood_down_layers()`). Step 14's S14-12 deleted the
# arm, the layer and the three names with it; the Postgres arm,
# `reconciliation_pg`, is the comparison against the source, and drives the
# repair.


# `writes_postgres` stood here until step 14's S14-11: a constant since
# S14-00, asked by the store's sync-key getter and setter alone after S14-09,
# which since S14-11 go to `meta.chain_watermarks` without asking.


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Called with a connection already acquired, after `require_revision()` has
    passed — `pg_expenses_write._latch` has the reason: the latch is permanent,
    and a write that never reaches Postgres must not spend the rollback."""
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ─── What Postgres would refuse ──────────────────────────────────────────────

_INT32 = (-2 ** 31, 2 ** 31 - 1)
_INT64 = (-2 ** 63, 2 ** 63 - 1)

# Revision 0003's and 0020's column types, by the rows' own field names.
_ORDER_INTS = ("id", "source_id", "status_id", "status_group_id", "buyer_id",
               "manager_id")
_ORDER_NOT_NULL = ("id", "source_id", "status_id", "grand_total")
_ORDER_TEXT = ("manager_comment", "promocode")
_PRODUCT_INTS = ("order_id", "product_id", "quantity")
_PRODUCT_NOT_NULL = ("id", "order_id", "name", "quantity", "price_sold")
_EXPENSE_INTS = ("id", "order_id", "expense_type_id")
_EXPENSE_NOT_NULL = ("id", "order_id", "amount")
_EXPENSE_TEXT = ("description", "status")
_MONEY = (12, 2)


def _text_refusal(column: str, value) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return f"{column} is a {type(value).__name__}, not text"
    if "\x00" in value:
        return f"{column} carries a NUL byte"
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return f"{column} is not valid UTF-8"
    return None


def _int_refusal(column: str, value, bounds=_INT32) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        return f"{column} is a {type(value).__name__}, not an integer"
    if not bounds[0] <= value <= bounds[1]:
        return f"{column} is outside its integer range"
    return None


def _money_refusal(column: str, value, *, nullable: bool) -> Optional[str]:
    """`pg_numeric.refusal` for a money column, reading a numeric string as
    the number it is: KeyCRM may serve an expense's `amount` as text, the
    shared parse passes it through, and both DuckDB's DECIMAL and asyncpg's
    NUMERIC take it (checked on asyncpg against PostgreSQL 17.2)."""
    from decimal import InvalidOperation

    from core.pg_numeric import refusal

    if isinstance(value, str):
        try:
            value = Decimal(value)
        except InvalidOperation:
            return f"{column} is text that is not a number"
    return refusal(column, value, *_MONEY, nullable=nullable)


def _row_refusal(row, ints, not_null, text, money, *, bounds=None) -> Optional[str]:
    bounds = bounds or {}
    for column in not_null:
        if getattr(row, column) is None:
            return f"{column} is NULL"
    for column in ints:
        why = _int_refusal(column, getattr(row, column), bounds.get(column, _INT32))
        if why:
            return why
    for column in text:
        why = _text_refusal(column, getattr(row, column))
        if why:
            return why
    for column in money:
        why = _money_refusal(column, getattr(row, column),
                             nullable=column not in not_null)
        if why:
            return why
    return None


def order_refusal(row) -> Optional[str]:
    """Why Postgres would refuse this header, or None. Names the column and the
    kind of value, never the value: a comment can carry a customer's words."""
    return _row_refusal(row, _ORDER_INTS, _ORDER_NOT_NULL, _ORDER_TEXT,
                        ("grand_total",))


def product_refusal(row) -> Optional[str]:
    return _row_refusal(row, ("id",) + _PRODUCT_INTS, _PRODUCT_NOT_NULL,
                        ("name",), ("price_sold",), bounds={"id": _INT64})


def expense_refusal(row) -> Optional[str]:
    return _row_refusal(row, _EXPENSE_INTS, _EXPENSE_NOT_NULL, _EXPENSE_TEXT,
                        ("amount",))


# ─── The decision ────────────────────────────────────────────────────────────

def _as_utc(value: Any) -> Any:
    """A timestamp as an aware UTC instant; a naive one is read as UTC, which
    is what `pd.to_datetime(..., utc=True)` did on the DuckDB path."""
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def decide(orders: Sequence[Any], existing: Mapping[int, Any], *,
           force_update: bool, skip_products: bool):
    """`(to_write, skipped_ids, deferred)` — DuckDB's decision, verbatim.

    `existing` is `{id: stored updated_at}` for the ids that exist. A
    header-only refresh never creates an order (`deferred`); an order whose
    `updated_at` has not moved past the stored one is skipped as already in
    its state; everything else is written. `core.upsert_decider` decides each
    row, on UTC instants."""
    from core.upsert_decider import should_update_order

    to_write, skipped, deferred = [], [], 0
    for row in orders:
        oid = int(row.id)
        if skip_products and oid not in existing:
            deferred += 1
            continue
        if oid in existing:
            stored, incoming = _as_utc(existing[oid]), _as_utc(row.updated_at)
            # DuckDB's path reads a payload's missing stamp through pandas as
            # NaT, which no comparison passes, so a stored order is never
            # rewritten by a payload without `updated_at` — whatever the
            # decider's own None rule says. Production is that path; found by
            # the grid in tests/unit/test_pg_orders_write.py.
            if incoming is None and stored is not None:
                skipped.append(oid)
                continue
            if not should_update_order(stored, incoming, force=force_update):
                skipped.append(oid)
                continue
        to_write.append(row)
    return to_write, skipped, deferred


async def _stored_stamps(conn, ids: Sequence[int], *, lock: bool) -> Dict[int, Any]:
    """`{id: updated_at}` for the ids Postgres holds — the decider's read.
    Locked `FOR UPDATE`, in id order, inside the writing transaction."""
    if not ids:
        return {}
    rows = await conn.fetch(
        "SELECT id, updated_at FROM bronze.orders WHERE id = ANY($1::int[]) "
        "ORDER BY id" + (" FOR UPDATE" if lock else ""), sorted(set(ids)))
    return {int(r["id"]): r["updated_at"] for r in rows}


# ─── The expenses: what is new or moved, and what KeyCRM dropped (O2 (a)) ────

_CENT = Decimal("0.01")

# The stored rows a batch's expenses are compared with: every row under an
# order whose payload lists its expenses (the deletion candidates), and every
# row whose id the payload carries (a row KeyCRM moved to another order is
# stored under the old one). `{cols}` is `landing_rows.EXPENSE_COLUMNS`, the
# columns the writer writes, so the comparison sees every one of them.
_EXPENSES_STORED_SQL = (
    "SELECT {cols} FROM bronze.expenses "
    "WHERE order_id = ANY($1::int[]) OR id = ANY($2::int[]) ORDER BY id")

# The deletion, with what it deleted for the log. Run only on ids the locked
# read inside the same transaction chose.
_EXPENSES_DELETE_SQL = (
    "DELETE FROM bronze.expenses WHERE id = ANY($1::int[]) "
    "RETURNING id, order_id, amount")


def _cents(value: Any) -> Any:
    """A money value as `NUMERIC(12,2)` stores it: the number asyncpg sends,
    rounded half away from zero to two places (`pg_numeric.refusal`'s rule).
    A value that is not a number is returned as it is, and so compares
    unequal.

    asyncpg's binary NUMERIC encoder sends `Decimal(obj)` for anything that
    is not already a Decimal, so a float is its exact binary value, never its
    text: 2.675 is 2.67499…, stored as 2.67, and 1.005 as 1.00, while the text
    "2.675" is stored as 2.68. Read from its text, a float with a half-cent
    tail differed from its own stored row on every tick — rewritten every
    time, and named VALUE_MISMATCH by the 05:30 run every morning (C2R-1)."""
    from decimal import ROUND_HALF_UP, InvalidOperation

    if value is None or isinstance(value, bool):
        return value
    try:
        exact = value if isinstance(value, Decimal) else Decimal(value)
        return exact.quantize(_CENT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return value


def _expense_values(row: Sequence[Any]) -> tuple:
    """An expense's written columns as `bronze.expenses` holds them, in
    `EXPENSE_COLUMNS` order — a parsed row or a stored one. Money in cents
    (Decimal, never float), timestamps as UTC instants."""
    (eid, order_id, type_id, amount, description, status,
     payment_date, created_at) = tuple(row)
    return (_int_or_none(eid), _int_or_none(order_id), type_id, _cents(amount),
            description, status, _as_utc(payment_date), _as_utc(created_at))


def expense_changes(costs: Sequence[Any], listed: Any, all_listed: Any,
                    stored: Sequence[Any]) -> Tuple[List[Any], List[Any]]:
    """`(to_write, to_delete)` for one group of a batch. Pure.

    `costs` are the parsed expenses the group writes; `listed` the ids of the
    orders whose payload carried an `expenses` list (and are not refused);
    `all_listed` every expense id any payload of the batch lists; `stored`
    the rows `_EXPENSES_STORED_SQL` read, in `EXPENSE_COLUMNS` order.

    An expense is written when it is new or differs in a written column. A
    stored row is deleted when its order is in `listed` and no payload of the
    batch lists its id."""
    from core.landing_rows import EXPENSE_COLUMNS

    rows = [tuple(r[c] for c in EXPENSE_COLUMNS) if not isinstance(r, tuple) else r
            for r in stored]
    held = {_int_or_none(r[0]): _expense_values(r) for r in rows}
    to_write = [e for e in costs
                if held.get(_int_or_none(e.id)) != _expense_values(e)]
    orders = {int(o) for o in listed}
    keep = {int(i) for i in all_listed}
    to_delete = [r for r in rows
                 if _int_or_none(r[1]) in orders and _int_or_none(r[0]) not in keep]
    return to_write, to_delete


async def _stored_expenses(conn, listed, costs, *, lock: bool) -> List[Any]:
    """The rows `expense_changes` compares with — locked `FOR UPDATE`, in id
    order, inside the writing transaction; unlocked before the latch."""
    from core.landing_rows import EXPENSE_COLUMNS

    order_ids = sorted({int(o) for o in listed})
    expense_ids = sorted({i for i in (_int_or_none(e.id) for e in costs)
                          if i is not None})
    if not order_ids and not expense_ids:
        return []
    sql = _EXPENSES_STORED_SQL.format(cols=", ".join(EXPENSE_COLUMNS))
    return list(await conn.fetch(sql + (" FOR UPDATE" if lock else ""),
                                 order_ids, expense_ids))


async def _already_in_state(conn, orders, expenses, listed=(), all_listed=(), *,
                            force_update: bool, skip_products: bool):
    """Read before the latch, unlocked: `(skipped_ids, deferred)` when this
    batch would write nothing — every order already in its state or deferred,
    every expense already stored as listed and none to delete — else None.
    Whatever it says, a batch that does write is decided again under the
    lock.

    A forced batch is asked too: the 05:15 refresh forces a header-only
    rewrite, and a header-only refresh never creates an order, so a forced
    batch of orders Postgres does not hold writes nothing — and, as the first
    write after a flip, used to take the latch and claim the owner rows
    anyway.

    Until O2 (a) any expense at all answered "must write": an order's costs
    move without its `updated_at`, and nothing compared them with what was
    stored, so every tick rewrote every expense of the 24-hour window."""
    skipped: List[int] = []
    deferred = 0
    if orders:
        existing = await _stored_stamps(conn, [r.id for r in orders], lock=False)
        to_write, skipped, deferred = decide(
            orders, existing, force_update=force_update, skip_products=skip_products)
        if to_write:
            return None
    if expenses or listed:
        stored = await _stored_expenses(conn, listed, expenses, lock=False)
        write, gone = expense_changes(expenses, listed, all_listed, stored)
        if write or gone:
            return None
    return (skipped, deferred)


def _is_data_error(exc: BaseException) -> bool:
    """A value one order carries, not a writer that is broken: `DataError`
    (SQLSTATE class 22) only, chain 4's rule — a NOT NULL or unique violation
    is a defect in what this module writes, and retrying order by order would
    hide it behind a skipped count."""
    import asyncpg

    return isinstance(exc, asyncpg.DataError)


def _one_by_one(orders, expenses, listed=()) -> List[Tuple[List[Any], List[Any], set]]:
    """A refused batch as groups of one: each order with its own expenses and
    its own expense listing, then the expenses and listings whose order is not
    in the batch, together."""
    by_order: Dict[Any, List[Any]] = {}
    for e in expenses:
        by_order.setdefault(e.order_id, []).append(e)
    listing = {int(o) for o in listed}
    groups = [([r], by_order.pop(r.id, []), {int(r.id)} & listing) for r in orders]
    leftover = [e for costs in by_order.values() for e in costs]
    rest = listing - {int(r.id) for r in orders}
    if leftover or rest:
        groups.append(([], leftover, rest))
    return groups


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ─── Which of an order's listed expenses this chain stores ──────────────────
#
# Pure functions, because two readers must agree on them: the writer below,
# and the 05:30 reconciliation (`reconciliation_io.keycrm_expense_facts` and
# `_process_batch`), which counts an order's KeyCRM expenses only as this
# chain would store them, and not at all for an order it refuses whole.
# Counting every entry of the list instead reported an order with one refused
# entry as VALUE_MISMATCH every morning and re-fetched it, and the re-fetch is
# refused the same way (C-1); comparing a refused order's expenses did the
# same to a held order whose list had since changed (C2R-2).

def order_with_lines_refusal(row: Any, lines: Iterable[Any]) -> Optional[str]:
    """Why Postgres would refuse this order: its header's reason, else the
    first of its line items' (`order_refusal`, `product_refusal`), or None."""
    return order_refusal(row) or next(
        filter(None, (product_refusal(p) for p in lines)), None)


def payload_refusal(payload: Dict[str, Any]) -> Optional[str]:
    """Why this chain refuses one KeyCRM order payload whole, or None.

    Read as the minute sync and the by-id repair write it, line items
    included (`landing_rows.landed_orders`): the parse cannot read it, or
    `order_with_lines_refusal` refuses it (an id that is not an integer
    among its reasons). Such an order is not written, and its payload's
    listing deletes none of its stored expenses. Two refusals are not
    foreseen here: an expense list the parse cannot read
    (`reconciliation_io.keycrm_expense_facts` answers None for it), and a
    value only Postgres itself refuses, which nothing foresees before the
    writer's latch either."""
    from core.landing_rows import landed_orders

    landed = landed_orders([payload], with_products=True)
    for _oid, why in landed.refused:
        return why
    for row in landed.orders:
        why = order_with_lines_refusal(row, landed.products)
        if why:
            return why
    return None


def listed_expense_ids(carried: Any) -> set:
    """Every expense id one order's `expenses` list names, read off the
    payload as it came: an entry that is not an object, or an id that is not a
    number, names none. A stored expense whose id is here is never deleted
    (O2 (a)), whether this chain writes the entry or refuses it."""
    if not isinstance(carried, list):
        return set()
    return {i for i in (_int_or_none(e.get("id")) if isinstance(e, dict) else None
                        for e in carried) if i is not None}


def stored_expenses(rows: Sequence[Any]) -> Tuple[List[Any], List[Tuple[Any, str]]]:
    """`(kept, refused)` of parsed expense rows (`landing_rows.expense_row`).

    An expense with no id is dropped without a word: nothing could name it.
    One id given twice is the last one given (`INSERT OR REPLACE`'s outcome on
    the DuckDB path). What `expense_refusal` refuses is in `refused` with the
    reason, and its stored row, if any, stays as it is. The rest is `kept`,
    which is what is written."""
    by_id = {e.id: e for e in rows if e.id is not None}
    kept: List[Any] = []
    refused: List[Tuple[Any, str]] = []
    for row in by_id.values():
        why = expense_refusal(row)
        if why:
            refused.append((row, why))
        else:
            kept.append(row)
    return kept, refused


async def upsert_orders_with_expenses(
    payloads: Sequence[Dict[str, Any]], *,
    force_update: bool = False,
    skip_products: bool = False,
):
    """Write a page of KeyCRM orders and the expenses they carry. Raises.

    Returns `(UpsertResult, expense_count)` — what
    `SyncService._upsert_orders_with_expenses` returned on the DuckDB path, so
    its callers read the same shape: `count` is written plus already in state,
    `changed_ids` what was written, `failed` what Postgres would refuse.
    """
    from core import pg_landing
    from core.landing_rows import (
        _UNPARSEABLE, EXPENSE_COLUMNS, expense_rows, landed_orders)
    from core.pg_order_versions import CHANGE

    landed = landed_orders(list(payloads), with_products=not skip_products)
    # A payload the parse could not read at all is refused here, by id, with
    # its expenses: before this it raised out of the parse, failed the order
    # step on every tick and held `last_sync_orders` behind it (L1-01).
    refusals: Dict[int, str] = {}
    unidentified = 0            # refused by the parse with no usable id
    unparsed: set = set()       # ids the parse refused
    for oid, why in landed.refused:
        logger.error("orders: order %r not written to Postgres: %s", oid, why)
        if _int_or_none(oid) is None:
            unidentified += 1
        else:
            refusals[int(oid)] = why
            unparsed.add(int(oid))
    # One row per order, the last one given — the DuckDB DataFrame's rule; the
    # API repeats an order across paginated pages. A refused id is refused in
    # every copy; an id that is not a number cannot be written at all.
    latest = {}
    for r in landed.orders:
        oid = _int_or_none(r.id)
        if oid is None:
            if r.id is not None:
                logger.error("orders: order %r not written to Postgres: "
                             "unparseable: the id is not an integer", r.id)
                unidentified += 1
            continue
        if oid not in refusals:
            latest[oid] = r
    # Every fetched order's expenses, as `upsert_expenses_batch` was handed
    # them, deduplicated by id keeping the last (`INSERT OR REPLACE`'s outcome).
    # Parsed order by order, for the same reason as the headers. Beside them,
    # which orders' payloads LIST their expenses — a list under the key, empty
    # included — and every expense id any payload of the batch lists, read off
    # the payload itself, its order refused or not: what KeyCRM no longer lists
    # under such an order is deleted (O2 (a)), an id it lists anywhere never,
    # and a payload without the key says nothing.
    parsed_expenses = []
    listing: set = set()
    all_listed: set = set()
    for p in payloads:
        if not isinstance(p, dict):
            continue
        carried = p.get("expenses")
        all_listed.update(listed_expense_ids(carried))
        if _int_or_none(p.get("id")) in refusals:
            continue
        if isinstance(carried, list) and _int_or_none(p.get("id")) is not None:
            listing.add(int(p["id"]))
        if not carried:
            continue
        try:
            parsed_expenses.extend(expense_rows([p]))
        except _UNPARSEABLE as exc:
            why = f"unparseable expenses: {type(exc).__name__}: {exc}"
            logger.error("orders: order %r not written to Postgres: %s", p.get("id"), why)
            oid = _int_or_none(p.get("id"))
            if oid is None:
                unidentified += 1
            else:
                refusals[oid] = why
                unparsed.add(oid)
                latest.pop(oid, None)
    expenses = [e for e in parsed_expenses if _int_or_none(e.order_id) not in refusals]

    lines: Dict[int, List[Any]] = {}
    for p in landed.products:
        line_order = _int_or_none(p.order_id)
        if line_order is not None:
            lines.setdefault(line_order, []).append(p)
    orders = []
    for row in latest.values():
        why = order_with_lines_refusal(row, lines.get(int(row.id), ()))
        if why:
            logger.error("orders: order %r not written to Postgres: %s", row.id, why)
            refusals[int(row.id)] = why
        else:
            orders.append(row)
    kept_expenses, refused_expenses = stored_expenses(expenses)
    for row, why in refused_expenses:
        logger.error("orders: expense %r not written to Postgres: %s", row.id, why)

    # The orders whose expense listing is acted on: not refused, by the parse
    # or by the checks above (O2 (a)).
    listed = {oid for oid in listing if oid not in refusals}

    def failed() -> int:
        return len(refusals) + unidentified

    def refused() -> Tuple[Tuple[int, str], ...]:
        return tuple(refusals.items())

    def unparseable() -> int:
        return len(unparsed) + unidentified

    if not orders and not kept_expenses and not listed:
        return UpsertResult(count=0, changed_ids=[], skipped_unchanged=0,
                            failed=failed(), refused=refused(),
                            unparseable=unparseable()), 0

    written: List[int] = []
    skipped: List[int] = []
    deferred = 0
    expense_count = 0
    expenses_written = 0
    deleted: List[Any] = []
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        idle = await _already_in_state(conn, orders, kept_expenses, listed, all_listed,
                                       force_update=force_update,
                                       skip_products=skip_products)
        if idle is not None:
            skipped, deferred = idle
            # Every expense is already stored as listed: in its state, which
            # is what the count says for the orders too.
            return UpsertResult(
                count=len(skipped), changed_ids=[], skipped_unchanged=len(skipped),
                failed=failed(), deferred_to_full_sync=deferred,
                refused=refused(), unparseable=unparseable()), len(kept_expenses)
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        # The whole batch in one transaction; if Postgres refuses a value
        # nothing above could foresee, the same statements again, order by
        # order, so one order cannot cost the rest (module docstring).
        groups = [(orders, kept_expenses, listed)]
        one_by_one = False
        while groups:
            batch, costs, group_listed = groups.pop(0)
            try:
                async with conn.transaction():
                    await conn.execute(
                        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                    # The owner rows and the rows land together or neither
                    # does, so a claim never outlives the write that earned it.
                    await chain_latch.claim(conn, CHAIN_TABLES, stamp)
                    # The decider's read, under lock, in the writing
                    # transaction: the read IS the write decision.
                    existing = await _stored_stamps(
                        conn, [r.id for r in batch], lock=True)
                    to_write, s, d = decide(batch, existing, force_update=force_update,
                                            skip_products=skip_products)
                    # And the expenses' read, under lock too: what is new or
                    # moved is written, what KeyCRM no longer lists deleted.
                    gone: List[Any] = []
                    changed: List[Any] = []
                    if costs or group_listed:
                        stored = await _stored_expenses(
                            conn, group_listed, costs, lock=True)
                        changed, stale = expense_changes(
                            costs, group_listed, all_listed, stored)
                        if stale:
                            gone = list(await conn.fetch(
                                _EXPENSES_DELETE_SQL,
                                sorted(int(r[0]) for r in stale)))
                        if changed or stale:
                            await pg_landing._write_rows(
                                conn, "bronze.expenses", EXPENSE_COLUMNS,
                                [tuple(e) for e in changed])
                    if to_write:
                        await pg_landing._write_order_rows(
                            conn, [tuple(r) for r in to_write],
                            [tuple(p) for r in to_write for p in lines.get(int(r.id), ())],
                            replace_products=not skip_products, version_kind=CHANGE)
            except Exception as exc:
                if not _is_data_error(exc):
                    raise
                if not one_by_one:
                    one_by_one = True
                    groups = _one_by_one(orders, kept_expenses, listed)
                    logger.warning(
                        "orders: a batch of %d order(s) was refused (%s); "
                        "writing it order by order", len(orders), type(exc).__name__)
                    continue
                name = (f"order {batch[0].id!r}" if batch
                        else f"expense(s) {[c.id for c in costs]!r}")
                logger.error("orders: %s refused by Postgres: %s",
                             name, type(exc).__name__)
                for r in batch:
                    refusals[int(r.id)] = f"Postgres refused: {type(exc).__name__}"
                continue
            written += [int(r.id) for r in to_write]
            skipped += s
            deferred += d
            expense_count += len(costs)
            expenses_written += len(changed)
            deleted += gone

    # Logged once the transaction that deleted them has committed (O2 (a)):
    # these lines are the measurement of what the stale rows were worth.
    for row in deleted:
        logger.info("orders: expense %s of order %s deleted, amount %s: KeyCRM "
                    "no longer lists it", row["id"], row["order_id"], row["amount"])
    logger.info(
        "Upserted %d/%d orders to Postgres (written=%d, skipped_unchanged=%d%s%s%s)",
        len(written) + len(skipped), len(latest), len(written), len(skipped),
        f", deferred_to_full_sync={deferred}" if deferred else "",
        f", refused={failed()}" if failed() else "",
        f", expenses_written={expenses_written}, expenses_deleted={len(deleted)}"
        if expenses_written or deleted else "")
    return UpsertResult(
        count=len(written) + len(skipped), changed_ids=written,
        skipped_unchanged=len(skipped), failed=failed(),
        deferred_to_full_sync=deferred, refused=refused(),
        unparseable=unparseable(), expenses_deleted=len(deleted)), expense_count


async def record_backfill_misses(misses: Mapping[int, str]) -> int:
    """Remember ids KeyCRM could not supply, so the repairs stop asking.

    `checked_at` is the web process's UTC clock, passed in — the clock
    DuckDB's `CURRENT_TIMESTAMP` was, and the one the copy-back's handover
    ordered the two stores' rows by (an image from before step 14 still does;
    S14-04 deleted this build's) — never Postgres's `now()`. The statement is
    spelled here so the latch walk sees a writer."""
    if not misses:
        return 0
    checked_at = _now_utc()
    rows = [(int(oid), checked_at, str(reason)[:200]) for oid, reason in misses.items()]
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.executemany(
                "INSERT INTO app.order_backfill_misses (order_id, checked_at, reason) "
                "VALUES ($1, $2, $3) ON CONFLICT (order_id) DO UPDATE SET "
                "checked_at = EXCLUDED.checked_at, reason = EXCLUDED.reason",
                rows)
    return len(rows)


# Said by a caller that restores comments from outside the web container,
# where the scheduler's heavy-job lock does not exist: a CLI runs in its own
# process, so `get_scheduler()._heavy_job_lock` there would be a brand-new
# lock guarding nothing. `tests/unit/test_heavy_lock_coverage.py` accepts
# this constant in place of the lock, which is why it is a constant a script
# prints rather than a sentence in a docstring. It lived in `core.pg_backfill`
# until step 14's S14-13 deleted that module; the sentence was about a DuckDB
# UPDATE shipped to Postgres, and is about this chain's write now.
WEB_MUST_BE_STOPPED = (
    "Stop the web container before running this. It restores order comments "
    "in Postgres without the scheduler's heavy-job lock, which lives in the "
    "web process and cannot be taken from here — and a sync tick landing "
    "between the read and the write leaves Postgres holding the older copy, "
    "with the version archive recording a transition that never happened."
)


async def restore_manager_comments(comments: Mapping[int, str]) -> List[int]:
    """Fill in `manager_comment` where Postgres holds NULL, from KeyCRM.

    The two comment backfills' one write (`POST /api/traffic/backfill-utm`,
    `scripts/backfill_utm.py`) since step 14's S14-09 deleted their DuckDB
    walk. The rows are read `FOR UPDATE` and written back through
    `pg_landing._write_order_rows` with the comment filled in, headers only,
    as `kind='backfill'` (OD-20 (b)) — exactly what `ship_orders_by_id` sent
    on the DuckDB path until then, so the chain module spells no UPDATE of
    its own against the order tables. Returns the ids restored:
    an order whose comment is no longer NULL is left alone."""
    from core import pg_landing
    from core.landing_rows import ORDER_COLUMNS
    from core.pg_order_versions import BACKFILL

    wanted = {int(k): v for k, v in comments.items() if v}
    if not wanted:
        return []
    for oid, text in wanted.items():
        why = _text_refusal("manager_comment", text)
        if why:
            raise ValueError(f"order {oid}: {why}")
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            stored = await conn.fetch(
                f"SELECT {', '.join(ORDER_COLUMNS)} FROM bronze.orders "
                "WHERE id = ANY($1::int[]) AND manager_comment IS NULL "
                "ORDER BY id FOR UPDATE", sorted(wanted))
            rows = []
            for r in stored:
                values = [r[c] for c in ORDER_COLUMNS]
                values[ORDER_COLUMNS.index("manager_comment")] = wanted[int(r["id"])]
                rows.append(tuple(values))
            if rows:
                await pg_landing._write_order_rows(
                    conn, rows, [], replace_products=False, version_kind=BACKFILL)
    return [int(r[0]) for r in rows]
