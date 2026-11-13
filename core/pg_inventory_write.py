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

Since step 14's S14-11 no variable decides anything: this build writes
Postgres, and `KS_WRITE_INVENTORY` is a name only (revision 0035's comments
and an image from before step 14 read it). Until then it decided which store
wrote until the first movement landed here; after that the latch did (`core/chain_latch.py`, OD-19 (a)). Setting the
flag back would otherwise let DuckDB's `seq_stock_movements_id` reissue ids the
Postgres sequence has already handed out — one movement with two names, which
is the rule revision 0030 restated. Every writer below latches once a
connection is out of the pool and before its first write, and
`scripts/chain_copy_back.py` was the only way back (deleted in step 14's
S14-04; the way back since is an image from before step 14,
`core.rollback_lever`).

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
from datetime import date
from typing import (
    Any, Dict, FrozenSet, List, Optional, Sequence, Tuple,
)

from core import chain_latch
from core.analytics_captures import capture_in_transaction
from core.landing_rows import (
    OFFER_COLUMNS,
    STOCK_MOVEMENT_COLUMNS,
    OfferRow,
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

# The analysts' cost history (revision 0036, `core.analytics_captures`).
# `bronze.offer_stocks.purchased_price` is overwritten by every stock sync and
# KeyCRM keeps no history of it, so this is the only record of what an offer
# cost on a past day. Written in the per-SKU photograph's transaction, under a
# SAVEPOINT of its own, dated with the photograph's own day: one row per offer
# `offer_stocks` holds, once a Kyiv day. Not in `CHAIN_TABLES`: a capture is
# not a chain's table (`core.write_chains.CAPTURE_TABLES`), and `claim` would
# stamp owner rows for it.
#
# DO NOTHING, never UPDATE: the row is the cost as the day's photograph found
# it, and nothing retakes a photograph once the day has one. A cost changed
# later that day is the next day's row; an UPDATE would replace a cost
# that was true for part of the day with a later one and keep no record that
# the first existed, which is the overwrite this table exists to undo. So no
# statement changes a stored row, and a re-run of a day adds only the offers
# its first run did not see.
#
# Bounded by arithmetic: one row per offer a day, ~920 offers on 2026-10-29,
# so about 336 000 rows a year. Measured on PostgreSQL 17.2 with its key:
# 90 bytes a row, 29 MB a year. It grows with the catalogue, never faster.
_COST_CAPTURE_SQL = """
INSERT INTO app.offer_cost_daily (date, offer_id, sku, purchased_price, price)
SELECT $1, s.id, s.sku, s.purchased_price, s.price
FROM bronze.offer_stocks s
ON CONFLICT (date, offer_id) DO NOTHING
"""


def _latch() -> str:
    """Take the local half of the latch before this process writes Postgres.

    Raises if the marker cannot be written, and the write is then not attempted:
    a movement in Postgres that no marker records is how the next boot comes to
    believe DuckDB still allocates these ids.

    **Called with a connection out of the pool** — after `require_revision()`
    has passed and inside `pool.acquire()`, `pg_expenses_write._latch`'s reason
    and more sharply here: the sync tick runs every minute, so a deploy that
    brings `web` up ahead of `migrate`, or a Postgres restart — which ends a
    writer's wait for a connection in a refused reconnect — meets a writer
    within seconds. A pool with nothing free is only a wait (no acquire
    timeout), but a latch taken before the acquire is on disk through all of
    it. Latched there, the flip of `KS_WRITE_INVENTORY` would become
    irreversible with not one movement written — the chain's whole pre-flip
    rehearsal spent on an error.
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


# ─── What Postgres would refuse ──────────────────────────────────────────────
#
# Asked of every row before the latch, chain 3's and chain 4's rule: one row
# Postgres refuses — a quantity KeyCRM served as null, a price over
# NUMERIC(12,2) — used to raise inside the one transaction that writes the
# whole catalogue of stock, so every offer kept its old quantity and no
# movement was recorded, every hour, until somebody read the log (L1-06).
# Revision 0013's and 0029's column types.
#
# Only what Postgres refuses, never more: a row refused here is a row main
# wrote, so the refusals below are asyncpg 0.31's own encoders written out,
# and `tests/integration/test_inventory_refusal_matches_postgres.py` holds
# each verdict to what the server answers for the same value. What they miss
# reaches Postgres and is written row by row (`_is_data_error`); what they
# refuse that Postgres would take is a stock row lost every hour (AR-01: a
# quantity of 3.0, which asyncpg writes as 3, was refused as "not an integer").

_INT32 = (-2 ** 31, 2 ** 31 - 1)
_MONEY = (12, 2)


def _as_int4(value: Any) -> int:
    """The integer asyncpg sends for `value` into an INTEGER column, or the
    error it raises (TypeError, ValueError, OverflowError). Its encoder
    (`pgproto/codecs/int.pyx`, `int4_encode`): anything but an int whose type
    has `__int__` goes through `int()` — so 3.0 is 3, 1.5 is 1, True is 1 and
    NaN or an infinity raise — and the result through `PyLong_AsLong`, which
    takes an `__index__` and refuses everything else, text included."""
    import operator

    if type(value) is not int and hasattr(type(value), "__int__"):
        value = int(value)
    return operator.index(value)


def _int_refusal(column: str, value: Any) -> Optional[str]:
    if value is None:
        return f"{column} is NULL"
    try:
        sent = _as_int4(value)
    except (TypeError, ValueError, OverflowError):
        return f"{column} is a {type(value).__name__} asyncpg cannot send as an integer"
    if not _INT32[0] <= sent <= _INT32[1]:
        return f"{column} is outside its integer range"
    return None


def _text_refusal(column: str, value: Any) -> Optional[str]:
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


def _money_refusal(column: str, value: Any) -> Optional[str]:
    """Why Postgres would refuse `value` in a nullable NUMERIC(12,2).

    asyncpg sends `Decimal(value)` for anything that is not a Decimal (its
    `numeric_encode_binary`): a numeric string is its number, True is 1, a
    float is its exact binary value. The server then rounds half away from
    zero to two places and refuses what does not fit, an infinity included —
    and stores NaN, which `pg_numeric.refusal` refuses on purpose for the
    chains that compared with DuckDB's DECIMAL. Not this one: main wrote a NaN
    cost, so the refusal does not lose the row for it."""
    from decimal import ROUND_HALF_UP, Decimal

    if value is None:
        return None
    try:
        sent = value if isinstance(value, Decimal) else Decimal(value)
    except (TypeError, ValueError, ArithmeticError):
        return f"{column} is a {type(value).__name__} asyncpg cannot send as a number"
    if sent.is_nan():
        return None
    precision, scale = _MONEY
    digits = precision - scale
    # The magnitude first: `quantize` raises on a number too long for the
    # decimal context (1e30), and an infinity has no magnitude to compare.
    if sent.is_infinite() or sent.adjusted() >= digits or abs(sent.quantize(
            Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)) >= 10 ** digits:
        return f"{column} does not fit NUMERIC({precision},{scale})"
    return None


def stock_refusal(stock: Any) -> Optional[str]:
    """Why Postgres would refuse this stock row, or None. A missing quantity
    or reserve is 0, as it always was; one KeyCRM served as null is not."""
    if not isinstance(stock, dict):
        return f"the row is a {type(stock).__name__}, not an object"
    return (_int_refusal("id", stock.get("id"))
            or _int_refusal("quantity", stock.get("quantity", 0))
            or _int_refusal("reserve", stock.get("reserve", 0))
            or _text_refusal("sku", stock.get("sku"))
            or _money_refusal("price", stock.get("price"))
            or _money_refusal("purchased_price", stock.get("purchased_price")))


def offer_refusal(row: OfferRow) -> Optional[str]:
    """Why Postgres would refuse this offer, or None (`bronze.offers.product_id`
    is NOT NULL)."""
    return (_int_refusal("id", row.id) or _int_refusal("product_id", row.product_id)
            or _text_refusal("sku", row.sku))


def _is_data_error(exc: BaseException) -> bool:
    """A value one row carries (SQLSTATE class 22), not a writer that is
    broken — chain 3's rule."""
    import asyncpg

    return isinstance(exc, asyncpg.DataError)


def _kept(rows: Sequence[Any], refusal, what: str, ident) -> List[Any]:
    kept, refused = [], []
    for row in rows:
        why = refusal(row)
        if why:
            refused.append((ident(row), why))
        else:
            kept.append(row)
    if refused:
        logger.error("inventory: %d %s row(s) not written to Postgres: %s",
                     len(refused), what,
                     "; ".join(f"{i!r}: {w}" for i, w in refused[:20]))
    return kept


def _stock_id(stock: Any) -> Any:
    return stock.get("id") if isinstance(stock, dict) else None


def _deduplicated(stocks: Sequence[Any]) -> List[Any]:
    """One row per offer, the last one given. Two pages fetched in parallel
    can both carry an offer when the list shifts between the requests, and
    each copy then planned its own movement off the same stored quantity —
    one sale of three recorded twice (L1-05)."""
    by_id: Dict[Any, Any] = {}
    unkeyed: List[Any] = []
    for stock in stocks:
        key = _stock_id(stock)
        try:
            by_id.pop(key, None)
            by_id[key] = stock
        except TypeError:          # an id that cannot be a key is refused later
            unkeyed.append(stock)
    return list(by_id.values()) + unkeyed


async def _write_offer_rows(conn, sql: str, rows: Sequence[OfferRow]) -> None:
    await conn.executemany(sql, [(*r, None) for r in rows])
    await conn.execute(
        "UPDATE bronze.offers SET synced_at = now() "
        "WHERE id = ANY($1::int[])",
        [r.id for r in rows],
    )


async def upsert_offers(rows: List[OfferRow]) -> int:
    """The offer→product map, from the same parsed rows DuckDB would take."""
    if not rows:
        return 0
    rows = _kept(rows, offer_refusal, "offer", lambda r: r.id)
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
    written = 0
    async with pool.acquire() as conn:
        # Inside the acquire, not before it: a wait for a connection that ends
        # in a refused reconnect is a write that never reached Postgres
        # (`_latch`).
        stamp = _latch()
        # The whole batch in one transaction; if Postgres refuses a value
        # nothing above foresaw, the same statements row by row, so one offer
        # cannot cost the rest.
        groups: List[List[OfferRow]] = [list(rows)]
        one_by_one = False
        while groups:
            batch = groups.pop(0)
            try:
                async with conn.transaction():
                    await chain_latch.claim(conn, CHAIN_TABLES, stamp)
                    await _write_offer_rows(conn, sql, batch)
            except Exception as exc:
                if not _is_data_error(exc):
                    raise
                if not one_by_one:
                    one_by_one = True
                    groups = [[r] for r in rows]
                    logger.warning("inventory: a batch of %d offer(s) was refused "
                                   "(%s); writing it row by row", len(rows),
                                   type(exc).__name__)
                    continue
                logger.error("inventory: offer %r refused by Postgres: %s",
                             batch[0].id, type(exc).__name__)
                continue
            written += len(batch)
    return written


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
    stocks = _kept(_deduplicated(stocks), stock_refusal, "stock", _stock_id)
    if not stocks:
        return 0, 0
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    written = moved = 0
    async with pool.acquire() as conn:
        stamp = _latch()
        # The whole batch in one transaction; on a value Postgres refuses that
        # `stock_refusal` could not foresee, row by row, each with its own read
        # of the previous state, so one offer cannot cost the rest.
        groups: List[List[Dict[str, Any]]] = [list(stocks)]
        one_by_one = False
        while groups:
            batch = groups.pop(0)
            try:
                async with conn.transaction():
                    await chain_latch.claim(conn, CHAIN_TABLES, stamp)
                    n = await _write_stock_rows(conn, batch)
            except Exception as exc:
                if not _is_data_error(exc):
                    raise
                if not one_by_one:
                    one_by_one = True
                    groups = [[s] for s in stocks]
                    logger.warning("inventory: a batch of %d stock row(s) was "
                                   "refused (%s); writing it row by row",
                                   len(stocks), type(exc).__name__)
                    continue
                logger.error("inventory: stock %r refused by Postgres: %s",
                             _stock_id(batch[0]), type(exc).__name__)
                continue
            written += len(batch)
            moved += n
    return written, moved


async def _write_stock_rows(conn, stocks: List[Dict[str, Any]]) -> int:
    """The read of the previous state, the plan, the upsert and the movements,
    on the caller's transaction. Returns the movements recorded."""
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
    return len(movements)


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

    The cost of every offer goes into `app.offer_cost_daily` in the same
    transaction, with the same `day` (`_COST_CAPTURE_SQL`). It rides the
    photograph and never fails it: a capture that raises rolls back alone,
    is stamped `capture.app.offer_cost_daily` in `meta.mirror_state`, and the
    photograph commits. A day already taken returns before it, so the
    capture runs once a day, with the photograph.
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
            await capture_in_transaction(
                conn, "app.offer_cost_daily", _COST_CAPTURE_SQL, day)
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


# ─── Before the flip (DN-24) — until step 14's S14-11 ───────────────────────
#
# `preflight()` asked, on every `/api/health`, the three questions somebody
# had to answer before `KS_WRITE_INVENTORY` was switched on — the TEMPORARY
# grant the status rebuild needs, the six tables' last hourly copy, the latest
# `mirror_landing` verdict on them — and answered `ok: null` once the chain
# wrote Postgres. Production latched the chain on 2026-09-30, and since
# S14-00 it writes Postgres whatever the flag says, so the answer had been
# `null` on every probe; S14-11 deleted it with the flag. The advisory lock
# above (DN-24's other half) stays: the 01:00 snapshot and the stock step
# still race without it.
