"""The inventory chain, writing Postgres instead of DuckDB.

STAGE 4 STARTS HERE, AND THIS CHAIN GOES FIRST FOR ONE REASON

`stock_movements` is computed as a delta against the *previous* contents of
`offer_stocks`, and KeyCRM serves current stock with no history endpoint. A
movement not recorded at the moment it happens cannot be recovered by any
backfill — not from the API, not from a copy, not from anywhere. Every other
chain in stage 4 loses something that can be recomputed or re-fetched; this one
loses the only record there will ever be. So it moves first, while the whole of
stage 3's machinery is fresh and the daily comparison is still watching.

EXACTLY ONE STORE WRITES, AND IT IS THE ONE THAT NAMES

Revision 0008 argued that `stock_movements.id` must be carried from DuckDB and
never generated in Postgres, because a second allocator would give one movement
two names. Revision 0030 adds the Postgres allocator anyway, and the rule is
not abandoned — it is restated with the condition it always had:

    exactly one store writes this table, and that store allocates the id.

`KS_WRITE_INVENTORY` is what makes "exactly one" true, and it gates BOTH ends.
Under `postgres` the writes come here *and* `replicate_operational` stands down
for these tables — because a full replace out of a frozen DuckDB would roll
back every movement recorded since the switch, once an hour, looking fine in
between. That is `replicate_sms`' recorded failure and the reason it stands
down as a whole.

WHAT IS SHARED, AND WHY THOSE TWO THINGS SPECIFICALLY

Nothing here reimplements a rule. The two that decide what gets written live in
one place each and both engines call them:

  * `core.landing_rows.plan_stock_movements` — the classification. Two copies
    would not raise; they would disagree about whether a change was `stock_in`,
    `stock_out` or `reserve_change`, and the wrong verdict would go into the
    only record of it.
  * `core.sql_dialect.sku_status_rebuild_select` — six tables joined into one
    row per offer, rendered for whichever engine is asking.

This module is the plumbing around them: read the previous state, hand it over,
write what comes back.

THE READ AND THE WRITE ARE ONE TRANSACTION, AND THAT IS NOT TIDINESS

`upsert_stocks` reads `offer_stocks` to learn the previous quantities and then
overwrites them. If anything else writes between those two statements the delta
is computed against a state that no longer exists, and the movements are wrong
in a way nothing downstream can detect. The DuckDB implementation holds a
transaction across both for the same reason; so does this one.

`first_seen_at` in the status rebuild has the same shape one level up: the
rebuild reads the table it is about to delete. Lose that carry-forward and
every SKU's first-seen date resets to its first order date or today — and the
hourly replication then propagates the reset to the other store, so both copies
lose it inside an hour.

"EXACTLY ONE" IS A LATCH, NOT A VARIABLE

`KS_WRITE_INVENTORY` decides which store writes until the first movement lands
here; after that the latch does (`core/chain_latch.py`, OD-19 (a)). Setting the
flag back would otherwise let DuckDB's `seq_stock_movements_id` reissue ids the
Postgres sequence has already handed out — one movement with two names, which
is the rule revision 0030 restated. Every writer below latches once a
connection is out of the pool and before its first write, and
`scripts/chain_copy_back.py` is the only way back.

FAILURE POLICY: THIS ONE RAISES

The other Postgres modules here never raise, because they are copies and the
original is still safe in DuckDB — `pg_landing`, `pg_replication` and
`pg_operational` all say so. Under `KS_WRITE_INVENTORY=postgres` that is no
longer true: this IS the write, and swallowing its failure would mean a sync
that reports success while the movement it should have recorded is gone. So
these functions propagate, and the caller decides.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import (
    Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple,
)

from core import chain_latch
from core.landing_rows import (
    OFFER_COLUMNS,
    STOCK_MOVEMENT_COLUMNS,
    OfferRow,
    StockMovement,
    plan_stock_movements,
)
from core.sql_dialect import POSTGRES, sku_status_rebuild_select

logger = logging.getLogger(__name__)

WRITE_ENV = "KS_WRITE_INVENTORY"

# What this chain is called in `core.write_chains`, in the local latch marker
# and in the `/api/health` block — one spelling, so a marker written by one
# release is still read by the next.
CHAIN = "pg_inventory_write"

# What DuckDB's DDL defaults `stock_movements.source` to. Carried explicitly
# here because the Postgres column has no default — it was built to receive the
# value, not to invent one.
MOVEMENT_SOURCE = "sync"

# The six tables this chain owns. `replicate_operational` reads this list to
# know what to stand down on, so the two cannot come to disagree about which
# tables DuckDB has stopped writing.
CHAIN_TABLES: Tuple[str, ...] = (
    "bronze.offers",
    "bronze.offer_stocks",
    "app.stock_movements",
    "app.sku_inventory_status",
    "app.inventory_sku_history",
    "app.inventory_history",
)

# The two `sync_metadata` keys the chain owns. Named rather than derived,
# because `sync_metadata` holds three families of key with three different
# owners and only these two move with this chain. `core.write_chains` reads
# them, and the store's getter and setter both route through it to
# `meta.chain_watermarks` — revision 0032 says why not `app.sync_metadata`.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_offers", "last_sync_stocks")

# The transaction-scoped advisory lock every rebuild and snapshot of this chain
# takes first (DN-24). 'ks' in the high half and the chain's number in the low,
# so `pg_locks` shows it as classid 0x6B730000 (1802698752), objid 1.
#
# WHY A DATABASE LOCK, WHEN THE SCHEDULER ALREADY HAS ONE
#
# The stock step runs under the scheduler's `_heavy_job_lock`; the 01:00
# `inventory_snapshot` job, its boot catch-up and `POST /api/inventory/snapshot`
# do not. In DuckDB that never mattered, because the store's own lock admits
# one writer. In Postgres two rebuilds can interleave: the second one's DELETE
# cannot see the rows the first is inserting, so its own INSERT then collides
# with them on `offer_id` and the whole rebuild raises. The snapshots have the
# same shape one level down — both callers pass the "already taken today?"
# guard before either has committed. Held for the transaction and released by
# COMMIT or ROLLBACK, so a writer that dies cannot leave it behind.
CHAIN_LOCK_KEY = 0x6B73_0000_0000_0001


def env_writes_postgres() -> bool:
    """What `KS_WRITE_INVENTORY` alone says.

    An unrecognised value raises, `KS_BOT_STORE`'s rule: a typo in the one
    variable that decides which store owns the only record of a stock change
    must stop the process, not quietly pick the other one. Since DN-01 it stops
    this chain and nothing else.
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
    read: once a movement has been recorded here, the id allocator has moved
    and no environment variable may move it back. See `core/chain_latch.py`.
    """
    if chain_latch.latched(CHAIN):
        return True
    return env_writes_postgres()


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Raises if the marker cannot be written, and the write is then not attempted:
    a movement in Postgres that no marker records is how the next boot comes to
    believe DuckDB still allocates these ids.

    **Called with a connection out of the pool** — after `require_revision()`
    has passed and inside `pool.acquire()`, `pg_expenses_write._latch`'s reason
    and more sharply here: the sync tick runs every minute, so a deploy that
    brings `web` up ahead of `migrate`, a Postgres restart, or a pool whose
    five connections are all in use meets a writer within seconds. Latched
    there, the flip of `KS_WRITE_INVENTORY` would become irreversible with not
    one movement written — the chain's whole pre-flip rehearsal spent on an
    error.
    """
    return chain_latch.latch(CHAIN, WRITE_ENV)


async def _chain_lock(conn) -> None:
    """Wait for any other rebuild or snapshot of this chain to commit.

    The first statement of each transaction that takes it — before the owner
    rows and before anything is read — so the second of two callers reads the
    state the first one committed, rather than one it is about to replace.
    """
    await conn.execute("SELECT pg_advisory_xact_lock($1)", CHAIN_LOCK_KEY)


def _insert(table: str, columns: Sequence[str]) -> str:
    values = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"


def _upsert(table: str, columns: Sequence[str], keys: Sequence[str]) -> str:
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in keys)
    return (f"{_insert(table, columns)} "
            f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}")


async def _ensure_movement_id_floor(conn) -> None:
    """Put the allocator above every id already in the table.

    Revision 0030 creates the sequence and seeds it, but a migration runs
    against an EMPTY table — the rows arrive afterwards, carried from DuckDB
    with ids up to 55,715 at the time of writing. Seeded at migration time the
    sequence therefore sits at 1, and the first allocated id collides with a
    row that is already there. That is not a theory: the first run of the
    verification harness caught exactly this, which is why this function
    exists and why the migration alone is not enough.

    Idempotent and monotone. It takes the greater of the sequence's own
    position and `MAX(id)`, so it can never move the allocator backwards, and
    a run where the sequence is already ahead is a no-op. `is_called=true` so
    the next `nextval` returns that value plus one.

    Called inside the writing transaction and only when there are movements to
    write. Once per batch rather than once per process, because "once" would
    be wrong across the cutover itself: while the flag is off the replication
    keeps shipping higher ids, and the process that later becomes the writer
    may have computed its floor before any of them arrived.
    """
    await conn.execute(
        "SELECT setval('app.stock_movements_id_seq', GREATEST("
        "  (SELECT COALESCE(MAX(id), 0) FROM app.stock_movements),"
        "  (SELECT last_value FROM app.stock_movements_id_seq)"
        "), true)"
    )


async def upsert_offers(rows: List[OfferRow]) -> int:
    """The offer→product map, from the same parsed rows DuckDB would take."""
    if not rows:
        return 0
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    # `bronze.offers` carries `synced_at` — revision 0029 copied DuckDB's own
    # column, because this table is REPLICATED rather than mirrored.
    # `bronze.offer_stocks` beside it does not: it is older, from revision
    # 0008, and carries `mirrored_at` instead. Two bookkeeping conventions in
    # one chain, and the column name is the only place that shows.
    sql = _upsert(
        "bronze.offers", OFFER_COLUMNS + ("synced_at",), ("id",))
    async with pool.acquire() as conn:
        # Inside the acquire, not before it: the sync tick meets an exhausted
        # pool as readily as a down one, and either is a write that never
        # reached Postgres (`_latch`).
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            await conn.executemany(sql, [(*r, None) for r in rows])
            await conn.execute(
                "UPDATE bronze.offers SET synced_at = now() "
                "WHERE id = ANY($1::int[])",
                [r.id for r in rows],
            )
    return len(rows)


async def upsert_stocks(stocks: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Stock levels and the movements they imply, in one transaction.

    Returns (stocks written, movements recorded).

    The previous state is read INSIDE the transaction that overwrites it. That
    is the whole correctness of the delta: read it outside and another writer
    between the two statements makes every movement in this batch wrong, in a
    way nothing downstream can detect.
    """
    if not stocks:
        return 0, 0
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            current: Dict[int, Tuple[int, int]] = {
                r["id"]: (r["quantity"], r["reserve"])
                for r in await conn.fetch(
                    "SELECT id, quantity, reserve FROM bronze.offer_stocks")
            }
            product_map: Dict[int, Optional[int]] = {
                r["id"]: r["product_id"]
                for r in await conn.fetch(
                    "SELECT id, product_id FROM bronze.offers")
            }

            movements = plan_stock_movements(
                stocks, current=current, product_map=product_map)

            # One instant for the whole batch, read from the database rather
            # than from this process: the movements of one sync happened
            # together, and a per-row `now()` would spread them across the
            # statement's execution for no gain.
            now = await conn.fetchval("SELECT now()")

            # `mirrored_at`, not `synced_at` — see `upsert_offers`. Stamped in
            # the statement rather than by a DDL default, because this table
            # has none: it was built to RECEIVE the stamp from the replication.
            await conn.executemany(
                _upsert(
                    "bronze.offer_stocks",
                    ("id", "sku", "price", "purchased_price", "quantity",
                     "reserve", "mirrored_at"),
                    ("id",),
                ),
                [
                    (s.get("id"), s.get("sku"), s.get("price"),
                     s.get("purchased_price"), s.get("quantity", 0),
                     s.get("reserve", 0), now)
                    for s in stocks
                ],
            )

            if movements:
                await _ensure_movement_id_floor(conn)
                # `id` is omitted — revision 0030's sequence allocates it, and
                # this is the only writer while the flag says so.
                #
                # `recorded_at` and `source` ARE supplied. DuckDB defaults them
                # (`CURRENT_TIMESTAMP` and `'sync'`); the Postgres table has no
                # defaults at all, because it was built to receive both from
                # the replication. A writer that omitted them would record every
                # movement with a NULL timestamp — and `recorded_at` is what
                # `sku_inventory_status.last_stock_out_at` reads.
                await conn.executemany(
                    _insert(
                        "app.stock_movements",
                        STOCK_MOVEMENT_COLUMNS + ("recorded_at", "source"),
                    ),
                    [(*m, now, MOVEMENT_SOURCE) for m in movements],
                )
    return len(stocks), len(movements)


async def rebuild_sku_inventory_status() -> int:
    """The /inventory status table, rebuilt whole.

    `ON COMMIT DROP` rather than DuckDB's `CREATE OR REPLACE`: the temp table
    belongs to this transaction and cannot outlive it, so the "left behind by
    an interrupted call" case the DuckDB version guards against cannot arise
    here at all.

    Serialised by `_chain_lock` (DN-24): the stock step and the 01:00 snapshot
    job both call this, only one of them holds the scheduler's heavy lock, and
    two rebuilds interleaved in Postgres end with the second one's INSERT
    colliding with the first one's rows.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await _chain_lock(conn)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            try:
                await conn.execute(
                    "CREATE TEMP TABLE _tmp_first_seen ON COMMIT DROP AS "
                    "SELECT offer_id, first_seen_at FROM app.sku_inventory_status"
                )
            except Exception as exc:                       # pragma: no cover
                # `REVOKE ALL ON DATABASE` takes TEMPORARY away with everything
                # else, and `postgres/initdb/10-roles-and-schemas.sql` grants it
                # back — but initdb only runs for a NEW cluster. On a database
                # that predates that line the grant is a one-off by hand, and
                # this is where somebody finds that out.
                raise RuntimeError(
                    "the /inventory status rebuild needs one temporary table "
                    "and ks_app cannot create it. Run, as a superuser:\n"
                    "    GRANT TEMPORARY ON DATABASE ks TO ks_app;\n"
                    f"(original error: {exc})"
                ) from exc
            await conn.execute("DELETE FROM app.sku_inventory_status")
            await conn.execute(sku_status_rebuild_select(POSTGRES))
            return await conn.fetchval(
                "SELECT COUNT(*) FROM app.sku_inventory_status")


async def record_sku_inventory_snapshot(today: Optional[date] = None) -> bool:
    """The daily per-SKU photograph. One per day; returns False if already taken.

    The guard and the insert are one transaction, unlike the DuckDB original
    where they are two autocommit statements — two ticks arriving together
    there can both pass the guard. One transaction is not enough on its own
    under READ COMMITTED: both could still pass the guard before either had
    committed, and the second would die on the primary key. `_chain_lock` is
    what makes the second one wait and then find today already taken.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await _chain_lock(conn)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            day = today or await conn.fetchval(
                "SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
            if await conn.fetchval(
                "SELECT 1 FROM app.inventory_sku_history WHERE date = $1 LIMIT 1",
                day,
            ):
                return False
            await conn.execute(
                "INSERT INTO app.inventory_sku_history "
                "(date, offer_id, quantity, reserve, price) "
                "SELECT $1, offer_id, quantity, reserve, price "
                "FROM app.sku_inventory_status",
                day,
            )
            return True


async def record_inventory_snapshot(
    force: bool = False, today: Optional[date] = None,
) -> bool:
    """The rolled-up daily total. Aggregates `offer_stocks` directly.

    Not from `sku_inventory_status`: the DuckDB original reads the stock table
    itself, so this one does too. They would usually agree, and "usually" is
    not a reason to change which table a stored number came from.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        stamp = _latch()
        async with conn.transaction():
            await _chain_lock(conn)
            await chain_latch.claim(conn, CHAIN_TABLES, stamp)
            day = today or await conn.fetchval(
                "SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
            exists = await conn.fetchval(
                "SELECT 1 FROM app.inventory_history WHERE date = $1", day)
            if exists:
                if not force:
                    return False
                await conn.execute(
                    "DELETE FROM app.inventory_history WHERE date = $1", day)
            await conn.execute(
                # `recorded_at` is supplied for `stock_movements`' reason: the
                # Postgres table has no default, DuckDB's has CURRENT_TIMESTAMP.
                "INSERT INTO app.inventory_history "
                "(date, total_quantity, total_value, total_reserve, sku_count, "
                " recorded_at) "
                "SELECT $1, "
                "       COALESCE(SUM(quantity - reserve), 0), "
                "       COALESCE(SUM((quantity - reserve) * price), 0), "
                "       COALESCE(SUM(reserve), 0), "
                "       COUNT(*), now() "
                "FROM bronze.offer_stocks",
                day,
            )
            return True


async def read_snapshot_calendar(
    window_floor: date,
) -> Tuple[Optional[date], FrozenSet[date]]:
    """The two facts the continuity check needs, from the store that writes.

    Returns `(first_day, present_days)` — the earliest snapshot ever taken, and
    every distinct day on or after `window_floor` that has one. Both come from
    one round trip because the check needs them together and a second call
    could straddle the 01:00 job.

    This exists because the check that guards `inventory_sku_history` runs in a
    **sync** function (`check_internal_integrity`) while Postgres reads are
    async. Rather than bridge a loop inside the check — which would put an
    event loop underneath twenty other checks that do not need one — the async
    job reads the facts and hands them over, and the check stays pure. That is
    `compare_table`'s arrangement exactly: it takes two already-read mappings
    and does not care which engine either came out of.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        first_day = await conn.fetchval(
            "SELECT MIN(date) FROM app.inventory_sku_history")
        if first_day is None:
            return None, frozenset()
        rows = await conn.fetch(
            "SELECT DISTINCT date FROM app.inventory_sku_history "
            "WHERE date >= $1",
            window_floor,
        )
    return first_day, frozenset(r["date"] for r in rows)


# ─── Before the flip (DN-24) ─────────────────────────────────────────────────
#
# Three things have to be true on the day `KS_WRITE_INVENTORY` is switched on,
# and each used to be a query somebody had to remember to run by hand. They are
# asked here, together, and published on /api/health as
# `write_chains.pg_inventory_write.preflight`, so the answer is one look away
# from the person about to recreate web.

# The six tables' last hourly copy must be younger than this. The flip is a
# recreate of web, and the margin to the hour is the time it is given to land
# in before the next stock sync writes DuckDB behind the copy — a row that
# `scripts/chain_copy_back.py --handover` would then refuse the flip on.
PREFLIGHT_REPLICATED_WITHIN = timedelta(minutes=50)

# The findings are read out of Postgres' copy of the quality journal, which is
# itself shipped hourly, and a copy that stopped would keep showing yesterday's
# clean run for as long as anybody looked. DN-03's limit for that copy, and the
# same row it reads.
PREFLIGHT_JOURNAL_COPY_WITHIN = timedelta(minutes=75)
JOURNAL_COPY = "app.data_quality_runs"

# The comparison whose findings count: `reconcile_operational`, inside the
# daily `mirror_landing` run. A verdict older than the canary's own limit for
# that layer is not a verdict about today — `tests/unit/test_inventory_preflip.py`
# pins the two together.
OPERATIONAL_LAYER = "mirror_landing"
PREFLIGHT_VERDICT_WITHIN = timedelta(hours=30)

# Which failures of that run leave chain 1 uncompared. The run holds every
# comparison of the layer, each in its own try, and when any raises
# `_run_dq_mirror_landing` sets `error_message` — after every other check has
# compared and filed — to
#
#     "<n> check(s) raised — <check>: <Class>: <text> | <check>: ..."
#
# with `setup` as the check for an exception between checks, which skips every
# check after it. So a failed run is not an uncompared chain: only its own
# comparison raising, or `setup`, is. `tests/unit/test_inventory_preflip.py`
# drives the real job, so a renamed check or a reworded message fails there.
OPERATIONAL_CHECK = "reconcile_operational"
_RAISED_HEAD = re.compile(r"(\d+) check\(s\) raised — ")
# The trailing space is looked at, not taken: an exception's text ending in
# "x: Y:" would otherwise eat the space of the " | " that starts the next entry.
_RAISED_ENTRY = re.compile(r"(?:^| \| )([a-z_]+): [A-Za-z_]\w*:(?= |$)")

# How many open findings the published answer names. The rest are counted.
_PREFLIGHT_NAMED = 5


def _minutes(delta: timedelta) -> int:
    return int(delta.total_seconds() // 60)


def _raised_checks(error_message: str) -> Optional[List[str]]:
    """The checks a failed `mirror_landing` run names as raised, in order, or
    None when its message is not in the job's format.

    It can only err towards blocking. An exception's own text holding
    " | name: Class:" adds a name that did not raise; a real entry cannot go
    missing, because the job itself starts each one after "— " or " | ". A
    message naming fewer entries than its count is not trusted at all.
    """
    head = _RAISED_HEAD.match(error_message)
    if head is None:
        return None
    names = [m.group(1) for m in _RAISED_ENTRY.finditer(error_message[head.end():])]
    if len(names) < int(head.group(1)):
        return None
    return list(dict.fromkeys(names))


def _uncompared_reason(run_id: int, raised: Optional[List[str]]) -> Optional[str]:
    """Why a failed run leaves chain 1 uncompared, or None when it does not."""
    run = f"the latest {OPERATIONAL_LAYER} run ({run_id})"
    if raised is None:
        return (f"{run} failed, and its error does not say which check raised, "
                "so chain 1's tables may not have been compared")
    if OPERATIONAL_CHECK in raised:
        return (f"{run} failed in {OPERATIONAL_CHECK}, so chain 1's tables "
                "were not compared")
    if "setup" in raised:
        return (f"{run} failed between its checks, so chain 1's tables may not "
                "have been compared")
    return None


async def preflight(
    pool=None, *, now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Whether chain 1 may be switched to Postgres now, and why not. Never raises.

    `ok` is True only when all three hold:

    * the writing role may create a temporary table — without it every status
      rebuild raises (`rebuild_sku_inventory_status`), and it is granted by
      initdb only on a cluster new enough to have the line;
    * each of the six tables was copied by `replicate_operational` within
      `PREFLIGHT_REPLICATED_WITHIN`, with no failure since;
    * the latest `mirror_landing` run — from a journal copy that is itself
      fresh — is recent, compared chain 1 (it may have failed elsewhere: a
      check that raised in another store is a `note`, not a reason), and
      filed nothing against the six tables or the chain.

    `ok` is None once the chain already writes Postgres: this is the question
    before the flip, and afterwards the copy it asks about stands down by
    design. A value of `KS_WRITE_INVENTORY` nobody understands is not "writes
    Postgres" — the chain is stood down, and the checks below say what that
    costs.

    `reasons` are sentences of this module's own; a Postgres error is named by
    its class only, because /api/health is public and a driver's message is
    not.
    """
    try:
        moved = writes_postgres()
    except RuntimeError:
        moved = False
    if moved:
        return {"ok": None, "reasons": [
            "chain 1 already writes Postgres; the preflight is the question "
            "asked before the flip"]}

    now = now or datetime.now(timezone.utc)
    reasons: List[str] = []
    notes: List[str] = []
    out: Dict[str, Any] = {
        "ok": False, "temporary": None, "replicated": {},
        "journal_copy_age_s": None, "operational_run": None, "reasons": reasons,
        "notes": notes,
    }
    try:
        if pool is None:
            from core.pg import get_pool, require_revision

            pool = await get_pool()
            await require_revision()
        async with pool.acquire() as conn:
            role, database, temporary = await conn.fetchrow(
                "SELECT current_user, current_database(), "
                "has_database_privilege(current_database(), 'TEMPORARY')")
            marks = {r["table_name"]: r for r in await conn.fetch(
                "SELECT table_name, last_ok_at, failures_since_ok "
                "FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                [*CHAIN_TABLES, JOURNAL_COPY])}
            run = await conn.fetchrow(
                "SELECT run_id, started_at, error_message "
                "FROM app.data_quality_runs WHERE layer = $1 "
                "ORDER BY run_id DESC LIMIT 1",
                OPERATIONAL_LAYER)
            findings = [] if run is None else await conn.fetch(
                "SELECT check_name, table_name FROM app.data_quality_issues "
                "WHERE run_id = $1 AND table_name = ANY($2::text[]) "
                "ORDER BY check_name, table_name",
                run["run_id"], [*CHAIN_TABLES, CHAIN])
    except Exception as exc:  # noqa: BLE001 — named by class, never raised
        reasons.append(f"Postgres could not be read ({type(exc).__name__})")
        return out

    # ── the temporary table the status rebuild needs ──
    out["temporary"] = bool(temporary)
    if not temporary:
        reasons.append(
            f"{role} cannot create a temporary table in {database}, so every "
            f"status rebuild would raise; as a superuser: "
            f"GRANT TEMPORARY ON DATABASE {database} TO {role}")

    # ── the six tables' last copy ──
    limit = _minutes(PREFLIGHT_REPLICATED_WITHIN)
    for table in CHAIN_TABLES:
        mark = marks.get(table)
        ok_at = mark["last_ok_at"] if mark else None
        failures = int(mark["failures_since_ok"]) if mark else 0
        age = now - ok_at if ok_at else None
        out["replicated"][table] = {
            "age_s": int(age.total_seconds()) if age is not None else None,
            "failures_since_ok": failures,
        }
        if ok_at is None:
            reasons.append(f"{table} has never been copied into Postgres")
        elif failures:
            reasons.append(f"the copy of {table} is failing ({failures} in a row)")
        elif age >= PREFLIGHT_REPLICATED_WITHIN:
            reasons.append(
                f"{table} was last copied {_minutes(age)} min ago (limit {limit})")

    # ── the journal copy the findings are read from ──
    journal = marks.get(JOURNAL_COPY)
    journal_ok_at = journal["last_ok_at"] if journal else None
    if journal_ok_at is not None:
        out["journal_copy_age_s"] = int((now - journal_ok_at).total_seconds())
    if journal_ok_at is None:
        reasons.append(
            "the quality journal has never been copied into Postgres, so open "
            "findings cannot be read")
    elif journal["failures_since_ok"]:
        reasons.append(
            f"the copy of the quality journal is failing "
            f"({int(journal['failures_since_ok'])} in a row)")
    elif now - journal_ok_at >= PREFLIGHT_JOURNAL_COPY_WITHIN:
        reasons.append(
            f"the copy of the quality journal is {_minutes(now - journal_ok_at)} "
            f"min old (limit {_minutes(PREFLIGHT_JOURNAL_COPY_WITHIN)})")

    # ── the latest operational comparison ──
    if run is None:
        reasons.append(f"no {OPERATIONAL_LAYER} run in the quality journal")
    else:
        age = now - run["started_at"]
        failed = run["error_message"] is not None
        # Check names only: the text after them is a driver's message.
        raised = _raised_checks(run["error_message"]) if failed else []
        out["operational_run"] = {
            "run_id": int(run["run_id"]),
            "age_s": int(age.total_seconds()),
            "failed": failed,
            "raised": raised,
            "findings": len(findings),
        }
        if age >= PREFLIGHT_VERDICT_WITHIN:
            reasons.append(
                f"the latest {OPERATIONAL_LAYER} run ({run['run_id']}) is "
                f"{int(age.total_seconds() // 3600)} h old (limit "
                f"{int(PREFLIGHT_VERDICT_WITHIN.total_seconds() // 3600)})")
        if failed:
            reason = _uncompared_reason(run["run_id"], raised)
            if reason is not None:
                reasons.append(reason)
            else:
                notes.append(
                    f"the latest {OPERATIONAL_LAYER} run ({run['run_id']}) failed "
                    f"in {', '.join(raised)}; chain 1's own comparison "
                    f"({OPERATIONAL_CHECK}) completed, so that does not count here")
        if findings:
            named = ", ".join(f"{f['check_name']} on {f['table_name']}"
                              for f in findings[:_PREFLIGHT_NAMED])
            more = len(findings) - _PREFLIGHT_NAMED
            reasons.append(
                f"{len(findings)} open finding(s) on chain 1 in "
                f"{OPERATIONAL_LAYER} run {run['run_id']}: {named}"
                + (f" and {more} more" if more > 0 else ""))

    out["ok"] = not reasons
    return out
