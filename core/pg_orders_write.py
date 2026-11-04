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

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) inside
`pool.acquire()` and claims the four owner rows inside the writing
transaction, so the owner row shares an `xmin` with the row it was taken for.
From the first order written here only `scripts/chain_copy_back.py orders`
could move the writes back to DuckDB (deleted in step 14's S14-04; the way
back since is an image from before step 14, `core.rollback_lever`). A batch
with nothing to write — every order already in its state and no expense —
reads that on the same connection before the latch and returns, so an idle
tick latches nothing.

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
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)


# What an order upsert did — the shape `upsert_orders_with_expenses` returns
# and `SyncService._upsert_orders_with_expenses` reads. It was DuckDB's
# (`core.duckdb_store`) until step 14's S14-01 moved it to its one remaining
# producer, unchanged; `core.duckdb_store` re-exports it for one release.
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
# Freshness stays with `freshness_orders` and the order step's own health.
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


def landing_checks_stood_down() -> frozenset:
    """The DuckDB integrity checks over the order tables that do not run once
    this chain has moved: on a frozen DuckDB they would compare old orders,
    and DN-23's Postgres twins stand in alone. Always the ten
    (`ORDER_LANDING_CHECKS`), a constant since step 14's S14-00 — otherwise
    they would knock on the refused DuckDB connection and page
    `duckdb_opened_while_off`. Never raises."""
    from core.data_quality import ORDER_LANDING_CHECKS

    return ORDER_LANDING_CHECKS


def retired_conditions() -> frozenset:
    """The conditions only the stood-down landing checks report — what nothing
    re-examines after the flip. A check that runs bare reports under its own
    name; `status_group_agreement` under its guard's conditions. The DuckDB
    arm of `dq_reconciliation` retired a whole group instead
    (`dq:reconciliation`), until step 14's S14-12 deleted the arm."""
    from core.data_quality import GUARDED_CHECK_CONDITIONS, ORDER_LANDING_CHECKS

    return frozenset(c for name in ORDER_LANDING_CHECKS
                     for c in GUARDED_CHECK_CONDITIONS.get(name, (name,)))


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


async def _already_in_state(conn, orders, expenses, *, force_update: bool,
                            skip_products: bool):
    """Read before the latch, unlocked: `(skipped_ids, deferred)` when this
    batch would write nothing — no expense, and every order already in its
    state or deferred — else None. Whatever it says, a batch that does write
    is decided again under the lock.

    A forced batch is asked too: the 05:15 refresh forces a header-only
    rewrite, and a header-only refresh never creates an order, so a forced
    batch of orders Postgres does not hold writes nothing — and, as the first
    write after a flip, used to take the latch and claim the owner rows
    anyway."""
    if expenses:
        return None
    if not orders:
        return ([], 0)
    existing = await _stored_stamps(conn, [r.id for r in orders], lock=False)
    to_write, skipped, deferred = decide(
        orders, existing, force_update=force_update, skip_products=skip_products)
    return None if to_write else (skipped, deferred)


def _is_data_error(exc: BaseException) -> bool:
    """A value one order carries, not a writer that is broken: `DataError`
    (SQLSTATE class 22) only, chain 4's rule — a NOT NULL or unique violation
    is a defect in what this module writes, and retrying order by order would
    hide it behind a skipped count."""
    import asyncpg

    return isinstance(exc, asyncpg.DataError)


def _one_by_one(orders, expenses) -> List[Tuple[List[Any], List[Any]]]:
    """A refused batch as groups of one: each order with its own expenses,
    then the expenses whose order is not in the batch, together."""
    by_order: Dict[Any, List[Any]] = {}
    for e in expenses:
        by_order.setdefault(e.order_id, []).append(e)
    groups = [([r], by_order.pop(r.id, [])) for r in orders]
    leftover = [e for costs in by_order.values() for e in costs]
    if leftover:
        groups.append(([], leftover))
    return groups


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
    from core.landing_rows import EXPENSE_COLUMNS, expense_rows, landed_orders
    from core.pg_order_versions import CHANGE

    landed = landed_orders(list(payloads), with_products=not skip_products)
    # One row per order, the last one given — the DuckDB DataFrame's rule; the
    # API repeats an order across paginated pages.
    latest = {int(r.id): r for r in landed.orders if r.id is not None}
    # Every fetched order's expenses, as `upsert_expenses_batch` was handed
    # them, deduplicated by id keeping the last (`INSERT OR REPLACE`'s outcome).
    expenses = {e.id: e for e in expense_rows(
        [p for p in payloads if p.get("expenses")]) if e.id is not None}

    lines: Dict[int, List[Any]] = {}
    for p in landed.products:
        lines.setdefault(int(p.order_id), []).append(p)
    refused: List[int] = []
    orders = []
    for row in latest.values():
        why = order_refusal(row) or next(
            filter(None, (product_refusal(p) for p in lines.get(int(row.id), ()))), None)
        if why:
            logger.error("orders: order %r not written to Postgres: %s", row.id, why)
            refused.append(int(row.id))
        else:
            orders.append(row)
    kept_expenses = []
    for row in expenses.values():
        why = expense_refusal(row)
        if why:
            logger.error("orders: expense %r not written to Postgres: %s", row.id, why)
        else:
            kept_expenses.append(row)
    if not orders and not kept_expenses:
        return UpsertResult(count=0, changed_ids=[], skipped_unchanged=0,
                            failed=len(refused)), 0

    written: List[int] = []
    skipped: List[int] = []
    deferred = 0
    expense_count = 0
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        idle = await _already_in_state(conn, orders, kept_expenses,
                                       force_update=force_update,
                                       skip_products=skip_products)
        if idle is not None:
            skipped, deferred = idle
            return UpsertResult(
                count=len(skipped), changed_ids=[], skipped_unchanged=len(skipped),
                failed=len(refused), deferred_to_full_sync=deferred), 0
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        # The whole batch in one transaction; if Postgres refuses a value
        # nothing above could foresee, the same statements again, order by
        # order, so one order cannot cost the rest (module docstring).
        groups = [(orders, kept_expenses)]
        one_by_one = False
        while groups:
            batch, costs = groups.pop(0)
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
                    if costs:
                        await pg_landing._write_rows(
                            conn, "bronze.expenses", EXPENSE_COLUMNS,
                            [tuple(e) for e in costs])
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
                    groups = _one_by_one(orders, kept_expenses)
                    logger.warning(
                        "orders: a batch of %d order(s) was refused (%s); "
                        "writing it order by order", len(orders), type(exc).__name__)
                    continue
                name = (f"order {batch[0].id!r}" if batch
                        else f"expense(s) {[c.id for c in costs]!r}")
                logger.error("orders: %s refused by Postgres: %s",
                             name, type(exc).__name__)
                refused.extend(int(r.id) for r in batch)
                continue
            written += [int(r.id) for r in to_write]
            skipped += s
            deferred += d
            expense_count += len(costs)

    logger.info(
        "Upserted %d/%d orders to Postgres (written=%d, skipped_unchanged=%d%s%s)",
        len(written) + len(skipped), len(latest), len(written), len(skipped),
        f", deferred_to_full_sync={deferred}" if deferred else "",
        f", refused={len(refused)}" if refused else "")
    return UpsertResult(
        count=len(written) + len(skipped), changed_ids=written,
        skipped_unchanged=len(skipped), failed=len(refused),
        deferred_to_full_sync=deferred), expense_count


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
