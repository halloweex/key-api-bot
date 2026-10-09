"""Chain 5: the manager classification and the managers' stats, written to Postgres.

`bronze.managers` and `app.manager_classifications` decide `sales_type`: four of
the six branches of `silver_sales_type_case` read them, and every dashboard
endpoint defaults to `retail`. KeyCRM knows a manager's name, email and status,
and nothing about whether they sell retail, wholesale, internally or to
bloggers — only a human does, through `POST /api/managers/{id}/retail-status`.
Until this chain DuckDB was where that decision lived and
`core.pg_replication.replicate_managers` full-replaced both tables into
Postgres out of it. Under `KS_WRITE_MANAGERS=postgres` (default `duckdb`) the
three writers write Postgres and DuckDB's copies freeze.

THE THREE WRITERS, AND THE ONE ORDER THEY TAKE THEIR LOCKS IN

- `upsert_managers` — the daily sync's managers, from the shared parse
  (`core.landing_rows.manager_row`). **`is_retail` is not in the conflict
  update**: it is a seed for a manager this store has never seen, and writing
  it on conflict reverts every human classification to the constant — measured
  on PostgreSQL 17.2 (chain 5's design, M4). The same transaction seeds a
  baseline interval for EVERY manager without one, `_m0006`'s Postgres twin: in
  DuckDB a manager synced today gets its baseline only at the next boot (M2);
  here it has one the moment it lands.
- `update_manager_stats` — first and last order date and the count, from
  `bronze.orders`, by the one body DuckDB runs too (`core.sql_dialect.
  manager_stats_sql`), with the order's date in Kyiv.
- `set_manager_retail_status` — close the open interval, replace a same-day
  one, open the new one and set the current answer, in ONE transaction: four
  autocommits once left a manager with no open interval, every order from that
  date on `internal`.

All three take `pg_advisory_xact_lock(CHAIN_LOCK_KEY)` first in their
transaction. Without it two classifications of one manager interleave and leave
two open intervals (measured, M5: 2 without, 1 with); it also orders the 03:00
stats UPDATE against a concurrent upsert, whose row locks would otherwise be
taken in two different orders. These writes happen a few times a day, so
serialising them costs nothing.

A BACKDATE BEHIND THE LATEST CHANGE IS REFUSED HERE (OD-C5-1 (a))

DuckDB accepts an `effective_from` earlier than the manager's latest change and
writes two open intervals and an overlap — and the `sales_type` it then gives
depends on the direction: a backdate to retail overrides the later non-retail
decision for good, because the CASE asks whether ANY retail interval covers
the order (measured, M1). This writer refuses it (`BackdateBehindLatest`, the
route's 409) before the latch and again inside the lock. DuckDB is left as it
is: changing what it accepts changes production, and that is its own decision.

THE DERIVATION SIGNAL

Silver reads `is_retail` and the intervals, so `upsert_managers` and
`set_manager_retail_status` raise it (`pg_derivation.mark_if_owned`) in their
own transactions. `update_manager_stats` does not: Silver reads none of the
three stats columns. Today's `write_managers` raises it after every replicate,
the one after the stats included — one Postgres Silver rebuild a day that
moves no number, and it goes with this chain.

ONE CLOCK FOR BOTH STORES

`set_at` is the copy-back's handover clock (`chain_transfer._shared_clock`
derives it from the daily spec), so it is the web container's clock passed in
by the caller — `datetime.now(timezone.utc)` — never Postgres' `now()`; DuckDB's
`CURRENT_TIMESTAMP` default reads the same host clock. Chain 7a's rule.

HELD ON DUCKDB UNTIL WHAT READS THE TABLES FOLLOWS

`unmet_precondition()` names four things; until all hold an unlatched chain
runs as duckdb whatever its flag says (DN-26's hook, generic in the registry).
`goals_bridge` was a fifth, while the goal calculators read DuckDB's
classification through the DN-12 sales-type bridge; chain 7b-4 deleted the
bridge, and the goal history is Silver's, which step 13 makes Postgres':

- `lockout` — this build requires `LOCKOUT_REVISION` (0035), which refuses
  every image built before the chain (`chain_latch.lockout_unmet`).
- `step13` — while DuckDB derives Silver it derives `sales_type` from a
  classification this chain would freeze.
- `chain3` — `update_manager_stats` reads the orders, and the copy-backs must
  run in reverse order of the flips. Asked of `core.pg_orders_write` by name,
  so a build without chain 3 reads as unmet rather than failing to import.
- `read_fallback_off` — a failed read must be a 503, never DuckDB's frozen
  classification.

Every fact is local — a code constant, a cached verdict, the environment — so
`/api/health` can ask it with Postgres down.

AND THE FIRST WRITE TAKES THE FLAG'S PLACE

Every write latches the chain (`core/chain_latch.py`, OD-19 (a)) inside
`pool.acquire()` and before its transaction, and claims both owner rows inside
that transaction. What can be refused is refused before the latch — a row
Postgres would reject, a manager that does not exist, a backdate — so a write
that could not have landed does not spend the rollback. From the first write
only `scripts/chain_copy_back.py managers` moves the writes back, and it owes
DuckDB a full warehouse rebuild when it does (`CHAIN_COPY_BACK_OWES_FULL_REBUILD`).
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Tuple

from core import chain_latch

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_MANAGERS"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block. One spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_managers_write"

# `pg_replication.MANAGER_UNIT`, whole: a classification is read against its
# managers, never alone, so the two change hands as one (DN-22b).
CHAIN_TABLES: Tuple[str, ...] = ("bronze.managers", "app.manager_classifications")

# The revision that refuses every image built before this chain; the flag
# moves nothing until this build requires it (`chain_latch.lockout_unmet`).
LOCKOUT_REVISION = "0035_batch_e_chains"

# The one `sync_metadata` key that moves with it — what `sync_managers` stamps.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_managers",)

# Not judged by `core.pg_chain_invariants`: the sync is daily, and
# `_freshness_check` judges `managers` at 192 h from `meta.chain_watermarks`.
#
# Deliberately NOT `CHAIN_WATERMARK_INHERITS_DUCKDB`. Without it the key is
# absent from Postgres right after the flip, `get_last_sync_time("managers")`
# reads None, and the first incremental tick — within a minute of the start —
# syncs the managers: the chain's first write, and its latch, happen inside the
# watched flip window. A first sync that fails is then "never synced", which is
# true.
CHAIN_WATERMARK_MAX_AGE_MIN: Optional[int] = None

# `core.chain_transfer.copy_back` marks DuckDB's warehouse dirty in full in the
# copy's own transaction: `sales_type` is materialised at rebuild time, and an
# incremental rebuild never re-derives an order whose classification moved.
CHAIN_COPY_BACK_OWES_FULL_REBUILD = True

# 'ks' in the high half and the chain's number in the low one, chain 1's
# convention (`pg_inventory_write.CHAIN_LOCK_KEY` is ...0001).
CHAIN_LOCK_KEY = 0x6B73_0000_0000_0005

STATEMENT_TIMEOUT = "15s"

# An admin's click must not hang on a full pool: the pool sets no acquire
# timeout (`core.pg.get_pool`), and the Sunday sync can hold all five.
ACQUIRE_TIMEOUT_S = 10

# The holes every shared statement reading these tables carries —
# `core.sql_dialect.render_tables` fills `{managers}`; `{classifications}` is
# the dialect's name for the intervals. `reads_the_chain` recognises a read by
# them, and `tests/unit/test_managers_chain.py` walks `core/` and `web/` for
# every such statement.
TABLE_HOLES: Tuple[str, ...] = ("{managers}", "{classifications}")

# How often the unmet-precondition warning may repeat for one reason: the
# incremental tick asks this chain's key every minute.
UNMET_WARN_EVERY_S = 3600.0
_unmet_warned: Dict[str, float] = {}

_INT32 = (-2 ** 31, 2 ** 31 - 1)


class ManagersRefused(ValueError):
    """Every manager in a batch was one Postgres would refuse. Nothing was
    written and nothing latched."""


class ManagerNotFound(LookupError):
    """No such manager in `bronze.managers`. Nothing written, nothing latched."""


class ClassificationRefused(ValueError):
    """A classification Postgres would refuse — a note carrying NUL, a
    `set_by` past INTEGER — or one that is not a classification at all. Raised
    before anything is asked of Postgres and before the latch: a bad request,
    which the route answers 422, never an outage. The message names the field
    and the kind of value, never the value."""


class BackdateBehindLatest(ValueError):
    """A classification dated before the manager's latest change (OD-C5-1 (a)).

    `latest` is that change's date, for the route's 409."""

    def __init__(self, manager_id: int, effective_from: date, latest: date):
        super().__init__(
            f"manager {manager_id} was last reclassified from {latest}; a "
            f"classification from {effective_from} would sit behind it and "
            "leave two open intervals. Classify from that date or later.")
        self.manager_id = manager_id
        self.effective_from = effective_from
        self.latest = latest


def env_writes_postgres() -> bool:
    """What `KS_WRITE_MANAGERS` alone says.

    An unrecognised value raises — `KS_BOT_STORE`'s rule. Since DN-01 that
    stops this chain and nothing else: the registry stands it down, the sync
    step contains it, the route answers 409 and the stats job fails visibly.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


# ─── Preconditions ───────────────────────────────────────────────────────────

# Chain 3's module, asked by name: a build without it is unmet, not an import
# error that would make the whole question unreadable.
_CHAIN3 = "core.pg_orders_write"


def _step13_unmet() -> Optional[str]:
    from core import warehouse_cutover

    if warehouse_cutover.writes_postgres():
        return None
    return ("step13: DuckDB still derives the warehouse (KS_WRITE_WAREHOUSE is "
            "not in force), and it would derive sales_type from a "
            "classification this chain freezes")


def _chain3_unmet() -> Optional[str]:
    import importlib

    try:
        chain3 = importlib.import_module(_CHAIN3)
    except ModuleNotFoundError as exc:
        if exc.name != _CHAIN3:
            raise
        return ("chain3: the orders chain is not in this build, and "
                "update_manager_stats reads the orders it would freeze")
    mode = chain3.mode()
    if mode == "postgres":
        return None
    return (f"chain3: the orders are not written to Postgres "
            f"({getattr(chain3, 'WRITE_ENV', 'KS_WRITE_ORDERS')} runs as {mode}), "
            "and update_manager_stats reads them: flip chain 3 first, so the "
            "copy-backs run in reverse")


def _read_fallback_unmet() -> Optional[str]:
    from core import read_fallback

    if read_fallback.refusing():
        return None
    return ("read_fallback_off: KS_READ_FALLBACK is not off, so a failed read "
            "of the classification would be answered from DuckDB's frozen copy")


# `(key, check)`: the key is the precondition's name, the word every reason
# starts with — spelled here, not derived from the function's name, which
# would read `_read_fallback_unmet` as "read_fallback".
_CHECKS = (
    (chain_latch.LOCKOUT_KEY, lambda: chain_latch.lockout_unmet(LOCKOUT_REVISION)),
    ("step13", _step13_unmet),
    ("chain3", _chain3_unmet),
    ("read_fallback_off", _read_fallback_unmet),
)


def unmet_precondition() -> Optional[str]:
    """Why `KS_WRITE_MANAGERS=postgres` must not move the writes yet, or None.

    Never raises and never asks Postgres: `/api/health` reads it through the
    registry and must still answer with Postgres down. A fact that cannot be
    read is unmet, because nobody can then say it holds. Every fact is stable
    for the life of a process — a constant, a verdict cached at start — so the
    answer cannot come good mid-process and flip the chain with nobody
    watching; a fact that could would need chain 3's `held_until_restart`.
    `unmet_reasons()` joined with "; ".
    """
    reasons = unmet_reasons()
    return "; ".join(reasons) if reasons else None


def unmet_reasons() -> List[str]:
    """Every unmet precondition, one entry each, in `_CHECKS` order; empty
    when none. What the preflight publishes — never the joined form split
    back apart, which only works while no reason carries a "; " of its own
    (chain 3's did, batch-E review). Never raises."""
    reasons: List[str] = []
    for key, check in _CHECKS:
        try:
            why = check()
        except Exception as exc:  # noqa: BLE001 — carried out, not swallowed
            # Keyed by the precondition's name, as the answer it stands for.
            why = f"{key}: could not be read ({type(exc).__name__})"
        if why:
            reasons.append(why)
    return reasons


def _warn_unmet(reason: str) -> None:
    now = time.monotonic()
    last = _unmet_warned.get(reason)
    if last is not None and now - last < UNMET_WARN_EVERY_S:
        return
    _unmet_warned[reason] = now
    logger.warning("%s=postgres, but the chain stays on DuckDB: %s", WRITE_ENV, reason)


def writes_postgres() -> bool:
    """Whether this chain writes Postgres — the one answer every caller reads.

    The latch outranks the flag (OD-19 (a)), and outranks a value nobody can
    read. Unlatched, the flag moves the writes only once `unmet_precondition()`
    holds. Raises on a flag nobody can read while unlatched; `mode()` is the
    question that never raises.
    """
    if chain_latch.latched(CHAIN):
        return True
    if not env_writes_postgres():
        return False
    unmet = unmet_precondition()
    if unmet:
        _warn_unmet(unmet)
        return False
    return True


def _self():
    return sys.modules[__name__]


def mode() -> Optional[str]:
    """Where this chain's writes go: "postgres", "duckdb", or None when
    `KS_WRITE_MANAGERS` is not understood and no latch overrides it. The
    registry's answer for this chain alone (`write_chains._chain_state`).
    Never raises."""
    from core import write_chains

    return write_chains._chain_state(_self())["mode"]


def reads_postgres() -> bool:
    """Whether a read of these tables must ask Postgres — wherever the writes
    go. Never raises.

    Chain 7a's arrangement with one difference: the flag alone is not where
    the writes go here, because a flag held by an unmet precondition writes
    DuckDB, and a read must follow the rows rather than the variable. A flag
    nobody can read on a chain that never wrote here hands the choice back
    (neither copy moves: the writers refuse and the replica stands down).
    """
    return chain_latch.latched(CHAIN) or mode() == "postgres"


def reads_the_chain(sql: str) -> bool:
    """Whether this statement reads the chain's tables while the chain owns
    them — decided from the body, `pg_goals_write.reads_the_chain`'s rule."""
    return any(hole in sql for hole in TABLE_HOLES) and reads_postgres()


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


# ─── What Postgres would refuse ──────────────────────────────────────────────


def _text_refusal(column: str, value, *, nullable: bool) -> Optional[str]:
    """Names the column and the kind of value, never the value itself."""
    if value is None:
        return None if nullable else f"{column} is NULL"
    if not isinstance(value, str):
        return f"{column} is a {type(value).__name__}, not text"
    if "\x00" in value:
        return f"{column} carries NUL, which Postgres refuses in text"
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        # A lone surrogate — what a JSON "\ud800" escape decodes to.
        return f"{column} is not valid UTF-8"
    return None


def _int_refusal(column: str, value, *, nullable: bool) -> Optional[str]:
    if value is None:
        return None if nullable else f"{column} is NULL"
    if isinstance(value, bool) or not isinstance(value, int):
        return f"{column} is a {type(value).__name__}, not an integer"
    if not _INT32[0] <= value <= _INT32[1]:
        return f"{column} is outside INTEGER's range"
    return None


def _refusal(row) -> Optional[str]:
    """Why Postgres would refuse this manager, or None. Asked before the latch:
    a first batch the server refused whole would leave the marker with no
    owner row behind it — CRITICAL `chain_latch_disagrees` the next morning."""
    return (_int_refusal("id", row.id, nullable=False)
            or _text_refusal("name", row.name, nullable=False)
            or _text_refusal("email", row.email, nullable=True)
            or _text_refusal("status", row.status, nullable=True)
            or (None if isinstance(row.is_retail, bool)
                else "is_retail is not a boolean"))


# ─── The writers ─────────────────────────────────────────────────────────────


async def upsert_managers(rows: Sequence[Any], *, set_at: datetime) -> int:
    """Write the sync's managers, and a baseline for every manager without one.
    Returns how many managers were written. Raises.

    `rows` are `core.landing_rows.ManagerRow`s. A row Postgres would refuse is
    skipped and its id logged; a batch of nothing but such rows raises
    `ManagersRefused` without latching. An empty batch writes nothing and
    latches nothing.
    """
    from core.pg_derivation import mark_if_owned

    if set_at is None:
        raise ValueError("set_at must be supplied — it is the copy-back's clock")
    kept: Dict[int, Any] = {}
    refused: List[Any] = []
    for row in rows:
        why = _refusal(row)
        if why:
            logger.error("managers: manager %r not written to Postgres: %s", row.id, why)
            refused.append(row.id)
            continue
        kept[row.id] = row          # the last version served, as DuckDB keeps
    if not kept:
        if refused:
            raise ManagersRefused(
                f"all {len(refused)} manager(s) would be refused by Postgres: {refused}")
        return 0
    # In id order, so every writer of these rows takes its locks the same way.
    ordered = [tuple(kept[i]) for i in sorted(kept)]

    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        # Inside the acquire, not before it: an acquire that ends without a
        # connection is a write that never reached Postgres (`_latch`).
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await conn.execute("SELECT pg_advisory_xact_lock($1)", CHAIN_LOCK_KEY)
            # The owner rows and the managers land together or neither does.
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            # `is_retail` is inserted for a manager this store has never seen
            # and is NOT in the conflict update: there it would put the seed
            # over every classification a human made (design, M4). Spelled in
            # the function, not a module constant, so the latch guard's walk
            # sees the writer.
            await conn.executemany(
                "INSERT INTO bronze.managers "
                "(id, name, email, status, is_retail, mirrored_at) "
                "VALUES ($1, $2, $3, $4, $5, now()) "
                "ON CONFLICT (id) DO UPDATE SET "
                "name = EXCLUDED.name, "
                "email = EXCLUDED.email, "
                "status = EXCLUDED.status, "
                "mirrored_at = EXCLUDED.mirrored_at",
                ordered,
            )
            # `_m0006`'s twin, for every manager without an interval — not
            # only this batch's: a manager the replica carried before the flip
            # without its baseline (DuckDB seeds only at a boot) gets one here.
            await conn.execute(
                "INSERT INTO app.manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to, set_by, set_at, "
                " note, mirrored_at) "
                "SELECT m.id, COALESCE(m.is_retail, FALSE), DATE '1970-01-01', "
                "       NULL, NULL, $1, 'baseline frozen from managers.is_retail', "
                "       now() "
                "FROM bronze.managers m "
                "WHERE NOT EXISTS (SELECT 1 FROM app.manager_classifications mc "
                "                  WHERE mc.manager_id = m.id) "
                "ORDER BY m.id "
                "ON CONFLICT (manager_id, valid_from) DO NOTHING",
                set_at,
            )
            # Silver reads `is_retail` and the intervals: it owes a rebuild.
            await mark_if_owned(conn)
    return len(ordered)


async def update_manager_stats() -> int:
    """First and last order date and the order count, from `bronze.orders`.
    Returns how many managers were updated. Raises.

    No derivation signal: Silver reads none of these columns. A test pins
    that, so a Silver that starts reading them has to come back here.
    """
    from core.sql_dialect import POSTGRES, manager_stats_sql

    sql = manager_stats_sql(POSTGRES)
    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await conn.execute("SELECT pg_advisory_xact_lock($1)", CHAIN_LOCK_KEY)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            status = await conn.execute(sql)
    return int(status.rsplit(" ", 1)[-1])


def duckdb_orders_frozen() -> FrozenSet[str]:
    """The order tables a write chain has taken, whose DuckDB copies therefore
    stopped at its latch — asked by `DuckDBStore.update_manager_stats` before
    it counts DuckDB's orders while this chain is on DuckDB. Never raises.

    The rollback order runs this chain's copy-back before chain 3's, so the
    managers come back to DuckDB while the orders are still chain 3's; counted
    from DuckDB's frozen orders, `last_order_date` and `order_count` would go
    backwards and the replica would ship them over Postgres's (the chain-5
    review). The stats are left as they stand instead — after a copy-back, the
    chain's own last ones — until the orders come back.

    The local answer (`pg_landing.order_tables_stood_down`), as the per-tick
    order mirror takes it: it asks only a chain that declares an order table —
    chain 3, whose flag and marker it reads, and which with its flag off and
    no marker answers that nothing is taken, so the stats count DuckDB's
    orders as they always did.
    An owner row whose marker was lost is the daily comparison's to page
    (`order_owner_row_without_marker`, CRITICAL); the stats are display-only.
    """
    from core.pg_landing import order_tables_stood_down

    return order_tables_stood_down()


def _classification_refusal(manager_id, is_retail, effective_from, set_by,
                            note) -> Optional[str]:
    if effective_from is None or not isinstance(effective_from, date) \
            or isinstance(effective_from, datetime):
        return "effective_from must be a date"
    if not isinstance(is_retail, bool):
        return "is_retail is not a boolean"
    return (_int_refusal("manager_id", manager_id, nullable=False)
            or _int_refusal("set_by", set_by, nullable=True)
            or _text_refusal("note", note, nullable=True))


async def _latest_change(conn, manager_id: int) -> Tuple[bool, Optional[date]]:
    """`(the manager exists, the date of its latest classification)` — read
    before the latch, so a refusal does not spend it."""
    row = await conn.fetchrow(
        "SELECT EXISTS (SELECT 1 FROM bronze.managers WHERE id = $1) AS present, "
        "       (SELECT MAX(valid_from) FROM app.manager_classifications "
        "         WHERE manager_id = $1) AS latest",
        manager_id)
    return bool(row["present"]), row["latest"]


async def _refuse_before_the_latch(conn, manager_id: int, effective_from: date) -> None:
    present, latest = await _latest_change(conn, manager_id)
    if not present:
        raise ManagerNotFound(f"manager {manager_id} is not in bronze.managers")
    if latest is not None and effective_from < latest:
        raise BackdateBehindLatest(manager_id, effective_from, latest)


async def set_manager_retail_status(
    manager_id: int,
    is_retail: bool,
    effective_from: date,
    set_by: Optional[int],
    note: Optional[str],
    *,
    set_at: datetime,
) -> None:
    """Classify a manager from a date, in one transaction. Raises.

    Forward-dated, as DuckDB's writer is: the open interval is closed at
    `effective_from`, a same-day interval is replaced rather than stacked, and
    `bronze.managers.is_retail` is set to the new current answer. A date before
    the manager's latest change is refused (`BackdateBehindLatest`, module
    docstring), and so is a manager Postgres does not hold
    (`ManagerNotFound`) — both before the latch — and a value Postgres would
    refuse (`ClassificationRefused`), before Postgres is asked anything.
    """
    from core.pg_derivation import mark_if_owned

    why = _classification_refusal(manager_id, is_retail, effective_from, set_by, note)
    if why:
        raise ClassificationRefused(why)
    if set_at is None:
        raise ValueError("set_at must be supplied — it is the copy-back's clock")

    pool = await _pool()
    async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
        # A read never needs the latch, and a refusal is a write that never
        # reaches Postgres: asked first, so neither spends it.
        await _refuse_before_the_latch(conn, manager_id, effective_from)
        stamp = _latch()
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            await conn.execute("SELECT pg_advisory_xact_lock($1)", CHAIN_LOCK_KEY)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            # Again under the lock: a classification that landed between the
            # read above and the lock would otherwise be stacked behind. On
            # the chain's very first write that costs a marker with no owner
            # row — `chain_latch_disagrees` the next morning — and it is
            # accepted: two admins classifying one manager in the same second.
            latest = await conn.fetchval(
                "SELECT MAX(valid_from) FROM app.manager_classifications "
                "WHERE manager_id = $1", manager_id)
            if latest is not None and effective_from < latest:
                raise BackdateBehindLatest(manager_id, effective_from, latest)
            # A manager with no interval at all keeps its history before
            # `effective_from` as the seed said, rather than losing it to
            # `internal`.
            await conn.execute(
                "INSERT INTO app.manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to, set_by, set_at, "
                " note, mirrored_at) "
                "SELECT m.id, COALESCE(m.is_retail, FALSE), DATE '1970-01-01', "
                "       NULL, NULL, $2, 'baseline frozen from managers.is_retail', "
                "       now() "
                "FROM bronze.managers m "
                "WHERE m.id = $1 AND NOT EXISTS ("
                "    SELECT 1 FROM app.manager_classifications mc "
                "    WHERE mc.manager_id = m.id) "
                "ON CONFLICT (manager_id, valid_from) DO NOTHING",
                manager_id, set_at)
            # Replaced, not stacked: the key is (manager_id, valid_from). The
            # only DELETE this chain has, and it is always followed by the
            # INSERT of the same key below — so the key set only grows, which
            # the copy-back and the restore drill rely on.
            await conn.execute(
                "DELETE FROM app.manager_classifications "
                "WHERE manager_id = $1 AND valid_from = $2",
                manager_id, effective_from)
            await conn.execute(
                "UPDATE app.manager_classifications "
                "SET valid_to = $2, mirrored_at = now() "
                "WHERE manager_id = $1 AND valid_to IS NULL AND valid_from < $2",
                manager_id, effective_from)
            await conn.execute(
                "INSERT INTO app.manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to, set_by, set_at, "
                " note, mirrored_at) "
                "VALUES ($1, $2, $3, NULL, $4, $5, $6, now())",
                manager_id, is_retail, effective_from, set_by, set_at, note)
            await conn.execute(
                "UPDATE bronze.managers SET is_retail = $2, mirrored_at = now() "
                "WHERE id = $1",
                manager_id, is_retail)
            # `sales_type` is decided from these rows: Silver owes a rebuild,
            # raised in the decision's own transaction.
            await mark_if_owned(conn)
    logger.info("Manager %s retail status set to %s from %s (Postgres)",
                manager_id, is_retail, effective_from)


# ─── The shape of the two tables, and the pre-flip question ─────────────────

# One read of everything the standing watch and the preflight judge, from
# M11's measurement (2.7 ms over seeded managers):
# - `unclassified`: managers with no interval at all;
# - `open_wrong`: managers with intervals and not exactly one open;
# - `broken`: within one manager's history, an interval that does not end
#   where the next begins (an overlap, an open one with a later row, a gap) or
#   that covers no day at all;
# - `disagree`: `bronze.managers.is_retail` against the latest open interval;
# - `set_at_null`: the copy-back's clock missing;
# - `orphans`: intervals of a manager `bronze.managers` does not hold.
SHAPE_SQL = """
WITH iv AS (
    SELECT manager_id, valid_from, valid_to,
           LEAD(valid_from) OVER (PARTITION BY manager_id ORDER BY valid_from)
               AS next_from
    FROM app.manager_classifications
), per AS (
    SELECT m.id, m.is_retail,
           count(c.manager_id) AS intervals,
           count(c.manager_id) FILTER (WHERE c.valid_to IS NULL) AS open_n,
           (array_agg(c.is_retail ORDER BY c.valid_from DESC)
                FILTER (WHERE c.valid_to IS NULL))[1] AS latest_open
    FROM bronze.managers m
    LEFT JOIN app.manager_classifications c ON c.manager_id = m.id
    GROUP BY m.id, m.is_retail
)
SELECT
    (SELECT count(*) FROM bronze.managers) AS managers,
    (SELECT count(*) FROM app.manager_classifications) AS intervals,
    COALESCE((SELECT array_agg(id ORDER BY id) FROM per WHERE intervals = 0),
             '{}'::int[]) AS unclassified,
    COALESCE((SELECT array_agg(id ORDER BY id) FROM per
              WHERE intervals > 0 AND open_n <> 1), '{}'::int[]) AS open_wrong,
    COALESCE((SELECT array_agg(DISTINCT manager_id ORDER BY manager_id) FROM iv
              WHERE (next_from IS NOT NULL AND valid_to IS DISTINCT FROM next_from)
                 OR (valid_to IS NOT NULL AND valid_to <= valid_from)),
             '{}'::int[]) AS broken,
    COALESCE((SELECT array_agg(id ORDER BY id) FROM per
              WHERE open_n > 0 AND latest_open IS DISTINCT FROM is_retail),
             '{}'::int[]) AS disagree,
    (SELECT count(*) FROM app.manager_classifications WHERE set_at IS NULL)
        AS set_at_null,
    COALESCE((SELECT array_agg(DISTINCT c.manager_id ORDER BY c.manager_id)
              FROM app.manager_classifications c
              WHERE NOT EXISTS (SELECT 1 FROM bronze.managers m
                                WHERE m.id = c.manager_id)),
             '{}'::int[]) AS orphans
"""

# How old the replica's last copy of either table may be before the flip.
# `replicate_managers` runs at web's start, on the daily manager sync and
# after the 03:00 stats job, so a healthy copy is under a day old.
PREFLIGHT_REPLICATED_WITHIN_S = 26 * 3600

# Per statement, for a reader that cannot SET LOCAL: asyncpg's own timeout.
PREFLIGHT_STATEMENT_TIMEOUT_S = 5


async def preflight() -> Dict[str, Any]:
    """Chain 5's answer to "may `KS_WRITE_MANAGERS` be switched on now?", for
    `/api/health` (`write_chains.pg_managers_write.preflight`). Never raises;
    bounded and cached by the route.

    `ok` is null once the chain writes Postgres — the question is over. Before
    that `reasons` names every unmet precondition (asked whatever the flag
    says, and alone while any is: Postgres is asked only once they all hold),
    a replica of either table failing or over a day old, and every
    shape the standing watch would file after the flip — or that
    `chain_copy_back.py managers --handover` refuses on: a manager with no
    interval, not exactly one open, a broken history, a current answer that
    disagrees with the open interval, a missing `set_at`. A Postgres error is
    named by its class only: the endpoint is public. Not a substitute for
    `--handover`, which is still the gate.
    """
    if mode() == "postgres":
        return {"ok": None, "reasons": []}
    reasons: List[str] = []
    unmet = unmet_reasons()
    if unmet:
        # Postgres is not asked while a local precondition is unmet: until
        # they all hold the flag cannot move the writes whatever the replica
        # or the shape say, and while chain 3 writes DuckDB — its default —
        # that holds for good, so `/api/health` asks Postgres nothing on this
        # chain's behalf by default. One reason per precondition, as
        # `unmet_reasons()` lists them (`TestTheRealChain3`). The shape is
        # then asked the minute after the last one clears, before anybody
        # sets the flag.
        return {"ok": False, "reasons": unmet}
    try:
        pool = await _pool()
        async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
            marks = await conn.fetch(
                "SELECT table_name, failures_since_ok, "
                "       extract(epoch FROM now() - last_ok_at) AS age_s "
                "FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                list(CHAIN_TABLES), timeout=PREFLIGHT_STATEMENT_TIMEOUT_S)
            shape = await conn.fetchrow(SHAPE_SQL, timeout=PREFLIGHT_STATEMENT_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 — an answer of its own
        reasons.append(f"postgres: {type(exc).__name__}")
        return {"ok": False, "reasons": reasons}
    state = {r["table_name"]: r for r in marks}
    for table in CHAIN_TABLES:
        row = state.get(table)
        if row is None or row["age_s"] is None:
            reasons.append(f"replica: {table} has never been copied by "
                           "replicate_managers")
        elif row["failures_since_ok"]:
            reasons.append(f"replica: {table} is failing "
                           f"({row['failures_since_ok']} in a row)")
        elif float(row["age_s"]) > PREFLIGHT_REPLICATED_WITHIN_S:
            reasons.append(f"replica: {table} was last copied "
                           f"{float(row['age_s']) / 3600:.0f} h ago (limit 26 h); "
                           "POST /api/jobs/manager_stats/trigger copies it now")
    for column, what in (
            ("unclassified", "manager(s) with no interval"),
            ("open_wrong", "manager(s) without exactly one open interval"),
            ("broken", "manager(s) whose intervals overlap or leave a gap"),
            ("disagree", "manager(s) whose is_retail disagrees with the open interval")):
        ids = list(shape[column] or [])
        if ids:
            reasons.append(f"shape: {len(ids)} {what}: {ids[:10]}")
    if shape["set_at_null"]:
        reasons.append(f"shape: {shape['set_at_null']} interval(s) with no set_at")
    if not shape["managers"]:
        reasons.append("shape: bronze.managers is empty")
    return {"ok": not reasons, "reasons": reasons}
