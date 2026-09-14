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
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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
# owners and only these two move with this chain.
CHAIN_SYNC_KEYS: Tuple[str, ...] = ("last_sync_offers", "last_sync_stocks")


def writes_postgres() -> bool:
    """Whether this chain writes Postgres.

    An unrecognised value raises, `KS_BOT_STORE`'s rule: a typo in the one
    variable that decides which store owns the only record of a stock change
    must stop the process, not quietly pick the other one.
    """
    value = (os.getenv(WRITE_ENV) or "duckdb").strip().lower()
    if value not in {"duckdb", "postgres"}:
        raise RuntimeError(
            f"{WRITE_ENV}={value!r} is not understood; expected "
            f"'duckdb' or 'postgres'"
        )
    return value == "postgres"


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
        async with conn.transaction():
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
        async with conn.transaction():
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
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        async with conn.transaction():
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
    there can both pass the guard. Postgres makes that free, so it is taken.
    """
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        async with conn.transaction():
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
        async with conn.transaction():
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


async def set_last_sync_time(key: str, value: str) -> None:
    """One `sync_metadata` key, stamped at this row's own moment.

    Refuses a key this chain does not own. `sync_metadata` holds three families
    with three different owners — the nine `last_sync_*` watermarks, the
    warehouse dirty flag and the digest marker — and stage 4 moves them at
    different times. A writer that could set any key would let this chain move
    a watermark belonging to a chain still writing DuckDB.
    """
    if key not in CHAIN_SYNC_KEYS:
        raise ValueError(
            f"{key!r} is not owned by the inventory chain; "
            f"expected one of {CHAIN_SYNC_KEYS}"
        )
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO app.sync_metadata (key, value, updated_at) "
            "VALUES ($1, $2, now()) "
            "ON CONFLICT (key) DO UPDATE SET "
            "value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            key, value,
        )
